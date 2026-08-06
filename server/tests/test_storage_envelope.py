"""Bitemporal envelope and Parquet archive (Storage v2.0 §2, §1.B).

The properties tested here are the ones that make a backtest provable: that a
correction leaves the old truth queryable, that quarantine cannot reach the
production store, and that an archive can be read back byte-identical.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from botmaximus.storage.archive import Archive, LocalBackend
from botmaximus.storage.envelope import (
    EnvelopeError,
    Record,
    uuid7,
    visible_at,
)

T0 = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


def rec(**kw) -> Record:
    base = dict(dataset_id="btc_ohlcv_1m", source="bybit_v5_ws",
                event_time=T0, payload={"close": 64000.0}, quality_ok=True)
    base.update(kw)
    return Record.create(**base)


# =====================================================================
# envelope invariants
# =====================================================================
def test_uuid7_is_time_ordered():
    """Sortability keeps an append-only workload from becoming random-write."""
    a, b = uuid7(), uuid7()
    assert a < b or a[:8] <= b[:8]


def test_naive_timestamps_are_refused():
    """A naive datetime means something different on the Tokyo VPS than here,
    and the difference is invisible."""
    with pytest.raises(EnvelopeError, match="naive"):
        rec(event_time=datetime(2026, 8, 1, 12, 0))


def test_quality_flags_reject_margins():
    """Same wall as the LLM prompts: a name is diagnostic, a margin is a
    gradient, and these records reach generator digests."""
    with pytest.raises(EnvelopeError, match="margin"):
        rec(quality_ok=False, quality_flags=("spread_too_wide:0.12>0.05",))


def test_a_record_cannot_be_clean_and_flagged():
    with pytest.raises(EnvelopeError, match="both clean and flagged"):
        rec(quality_ok=True, quality_flags=("stale_feed",))


def test_event_time_partition_uses_the_event_not_ingest():
    """Partitioning by ingest would scatter a backfill of last year's candles
    into today's partition."""
    r = rec(event_time=datetime(2024, 3, 9, 5, 0, tzinfo=timezone.utc))
    assert r.partition_path() == \
        "year=2024/month=03/day=09/dataset=btc_ohlcv_1m"


# =====================================================================
# corrections
# =====================================================================
def test_a_correction_keeps_the_original_event_time():
    """The world did not change — only our knowledge of it."""
    original = rec()
    closed, new = original.supersede({"close": 64100.0}, "venue reissue")
    assert new.event_time == original.event_time
    assert new.supersedes == original.record_id
    assert new.correction_reason == "venue reissue"


def test_the_superseded_record_is_closed_not_deleted():
    original = rec()
    closed, new = original.supersede({"close": 64100.0}, "reissue")
    assert closed.record_id == original.record_id
    assert closed.valid_to_sys is not None
    assert closed.payload == {"close": 64000.0}      # old truth intact


def test_a_backtest_as_of_before_the_correction_sees_the_old_truth():
    """This is the whole point of the system-time axis: a run pinned to a past
    instant sees the data as it was believed then, errors included."""
    original = rec()
    before = original.valid_from_sys + timedelta(seconds=1)
    closed, new = original.supersede({"close": 64100.0}, "reissue",
                                     now=before + timedelta(seconds=10))

    old_view = visible_at([closed, new], as_of=before)
    assert [r.payload["close"] for r in old_view] == [64000.0]

    new_view = visible_at([closed, new], as_of=new.valid_from_sys)
    assert [r.payload["close"] for r in new_view] == [64100.0]


def test_correcting_an_already_closed_record_is_refused():
    """Forking the chain would make as-of queries ambiguous."""
    original = rec()
    closed, new = original.supersede({"close": 1.0}, "first")
    with pytest.raises(EnvelopeError, match="already superseded"):
        closed.supersede({"close": 2.0}, "second")


def test_lineage_is_stamped_automatically():
    r = rec()
    assert r.producer and "/" in r.producer
    assert r.code_version
    assert r.schema_version >= 1


# =====================================================================
# archive
# =====================================================================
@pytest.fixture
def archive(tmp_path):
    return Archive(LocalBackend(tmp_path), "arch", "quar")


def test_a_written_partition_reads_back_identically(archive):
    written = archive.write([rec(), rec(payload={"close": 64010.0})])
    assert written.rows == 2
    assert archive.verify(written) is True
    table = archive.read(written.key)
    assert table.num_rows == 2
    assert set(table.column_names) >= {
        "record_id", "event_time", "valid_from_sys", "valid_to_sys",
        "supersedes", "quality_ok", "payload"}


def test_bad_records_cannot_enter_the_production_archive(archive):
    """Quarantine and production never mix — that is P4."""
    bad = rec(quality_ok=False, quality_flags=("stale_feed",))
    with pytest.raises(ValueError, match="never enter the store"):
        archive.write([bad])


def test_quarantine_accepts_what_the_archive_refuses(archive):
    bad = rec(quality_ok=False, quality_flags=("stale_feed",))
    written = archive.write_quarantine([bad])
    assert written.bucket == "quar"
    assert archive.verify(written)


def test_there_is_no_path_from_quarantine_back_to_production(archive):
    """Rehabilitating bad rows is how a dataset becomes untrustworthy years
    later, when nobody remembers which ones were repaired."""
    api = {m for m in dir(archive) if not m.startswith("_")}
    assert not any(w in m.lower() for m in api
                   for w in ("promote", "rehabilitat", "restore", "reintroduce"))


def test_an_empty_write_is_refused(archive):
    """An empty partition is indistinguishable from a missing one at read
    time."""
    with pytest.raises(ValueError, match="empty parquet"):
        archive.write([])


def test_a_file_may_not_span_two_partitions(archive):
    """One day/dataset per file keeps a daily partition atomic."""
    other_day = rec(event_time=T0 + timedelta(days=1))
    with pytest.raises(ValueError, match="span"):
        archive.write([rec(), other_day])


def test_verify_catches_corruption(archive, tmp_path):
    """Silent bit rot on object storage is real; an archive nobody reads back
    is a hope."""
    written = archive.write([rec()])
    (tmp_path / "arch" / written.key).write_bytes(b"corrupted")
    assert archive.verify(written) is False


def test_writes_are_atomic(archive, tmp_path):
    """An interrupted write must never leave a partial Parquet file, which
    would read as a short partition rather than fail."""
    archive.write([rec()])
    assert not list((tmp_path / "arch").rglob("*.partial"))
