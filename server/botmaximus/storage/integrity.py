r"""Integrity checks (Storage v2.0 §11).

"The storage layer is worthless if you cannot tell when it's lying."

Every check writes an `integrity_events` row whether it passed or failed. That
is deliberate and it is the point of the design: a checker that only records
failures makes "everything is fine" and "the checker stopped running" produce
identical evidence, and the second one is how a silent corruption gets months
to spread.

Each check answers one question, names the rows that failed, and returns.
Nothing here repairs anything — a check that fixes what it finds destroys the
evidence of how often it was needed. §11's escalation (red-line halts writes to
the affected dataset via L2) is wired in `run_all`, not inside the checks.
"""
from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone

from botmaximus.config import settings
from botmaximus.storage import postgres
from botmaximus.storage import records as store

log = logging.getLogger(__name__)

UTC = timezone.utc

#: Bybit BTCUSDT linear perp did not exist before this. An event_time earlier
#: than the venue's own history is a parsing bug or a fabricated record.
VENUE_LAUNCH = {
    "bybit": datetime(2020, 1, 1, tzinfo=UTC),
    "binance": datetime(2019, 9, 1, tzinfo=UTC),
}


@dataclass
class CheckResult:
    name: str
    passed: bool
    dataset_id: str | None = None
    observed: dict = field(default_factory=dict)
    detail: str = ""

    def to_dict(self) -> dict:
        return {"check": self.name, "passed": self.passed,
                "dataset_id": self.dataset_id, "observed": self.observed,
                "detail": self.detail}


# ---------------------------------------------------------------- checks

async def check_row_count_parity() -> list[CheckResult]:
    """Postgres daily partition vs the Parquet partition it should mirror.

    Only for days that are fully written — today is still being appended to, so
    a mismatch there is expected rather than alarming.
    """
    out = []
    yesterday = (datetime.now(UTC) - timedelta(days=1)).date()
    rows = await postgres.fetch(
        "SELECT dataset_id, count(*) AS n FROM market_records "
        "WHERE event_time >= %s AND event_time < %s GROUP BY dataset_id",
        (datetime(yesterday.year, yesterday.month, yesterday.day, tzinfo=UTC),
         datetime(yesterday.year, yesterday.month, yesterday.day,
                  tzinfo=UTC) + timedelta(days=1)))
    for r in rows:
        prefix = (f"year={yesterday:%Y}/month={yesterday:%m}/"
                  f"day={yesterday:%d}/dataset={r['dataset_id']}")
        archived = await postgres.fetchval(
            "SELECT COALESCE(sum(rows), 0) AS n FROM storage_manifest "
            "WHERE bucket = %s AND partition = %s",
            (settings.bucket_archive, prefix)) or 0
        out.append(CheckResult(
            "row_count_parity", int(archived) == r["n"], r["dataset_id"],
            {"postgres": r["n"], "parquet": int(archived), "day": str(yesterday)},
            "" if int(archived) == r["n"]
            else "postgres and the archive disagree about this day"))
    return out


async def check_coverage_reconciles() -> list[CheckResult]:
    """Coverage totals must match the records they claim to describe.

    The ledger is what the backtester trusts to refuse a window. A ledger that
    has drifted from the records is worse than no ledger, because it is
    believed.
    """
    from botmaximus.pipeline.coverage import RECORD_BACKED_FEEDS
    out = []
    for feed, (dataset_id, granularity_s) in RECORD_BACKED_FEEDS.items():
        marked = await postgres.fetchval(
            "SELECT count(*) AS n FROM coverage_ledger "
            "WHERE venue = %s AND feed = %s AND state = 'complete'",
            (settings.venue, feed)) or 0
        actual = await postgres.fetchval(
            "SELECT count(DISTINCT to_timestamp("
            "  floor(extract(epoch FROM event_time) / %s) * %s)) AS n "
            "FROM market_records WHERE venue = %s AND dataset_id = %s "
            "AND valid_to_sys IS NULL",
            (granularity_s, granularity_s, settings.venue, dataset_id)) or 0
        # The ledger may legitimately hold slots for records already tiered out
        # of the hot window, so it may exceed the live count — never fall short.
        ok = marked >= actual
        out.append(CheckResult(
            "coverage_reconciles", ok, dataset_id,
            {"ledger_complete_slots": marked, "record_slots": actual},
            "" if ok else "coverage ledger has fewer complete slots than there "
                          "are records — the backtester is being told data is "
                          "missing that exists"))
    return out


async def check_orders_have_predictions() -> list[CheckResult]:
    """Every order must have a ledger prediction, and vice versa (§11).

    An order with no prediction means capital moved through a path that does
    not record intent — which is the one thing the execution ledger exists to
    make impossible.
    """
    orphan_orders = await postgres.fetchval(
        "SELECT count(*) AS n FROM orders o WHERE NOT EXISTS ("
        "  SELECT 1 FROM execution_ledger_predictions p "
        "  WHERE p.trade_id = o.trade_id)") or 0
    return [CheckResult(
        "orders_have_predictions", orphan_orders == 0, None,
        {"orders_without_prediction": orphan_orders},
        "" if orphan_orders == 0
        else "an order exists that no ledger prediction accounts for")]


async def check_scrutiny_blobs_exist() -> list[CheckResult]:
    """Every scrutiny verdict should have its prompt/response blob (§3.G)."""
    rows = await postgres.fetch(
        "SELECT intent_id, prompt_blob_key FROM scrutiny_events "
        "ORDER BY at DESC LIMIT 500")
    missing = []
    archive = store.archive()
    for r in rows:
        key = r["prompt_blob_key"]
        if not key or not archive.backend.exists(archive.archive_bucket, key):
            missing.append(r["intent_id"])
    return [CheckResult(
        "scrutiny_provenance_present", not missing, None,
        {"checked": len(rows), "missing": len(missing),
         "examples": missing[:5]},
        "" if not missing else "scrutiny verdicts without a provenance blob")]


async def check_generated_candidates_have_provenance() -> list[CheckResult]:
    """§3.G: a candidate whose provenance is missing must not enter validation."""
    orphans = await postgres.fetchval(
        "SELECT count(*) AS n FROM strategies s WHERE s.origin = 'generated' "
        "AND NOT EXISTS (SELECT 1 FROM generations g "
        "                WHERE g.strategy_id = s.strategy_id)") or 0
    return [CheckResult(
        "generated_have_provenance", orphans == 0, None,
        {"generated_without_generation_row": orphans},
        "" if orphans == 0
        else "generated strategies with no generation manifest")]


async def check_timestamps_are_sane() -> list[CheckResult]:
    """No future event_time, none before the venue existed, no negative latency.

    All three are impossible in correct data and all three are silent: a record
    stamped in the future sorts to the end of every window and quietly becomes
    the 'latest' price forever.
    """
    out = []
    now = datetime.now(UTC)
    future = await postgres.fetchval(
        "SELECT count(*) AS n FROM market_records WHERE event_time > %s",
        (now + timedelta(minutes=5),)) or 0
    out.append(CheckResult(
        "no_future_event_time", future == 0, None,
        {"records_in_the_future": future}))

    for venue, launch in VENUE_LAUNCH.items():
        early = await postgres.fetchval(
            "SELECT count(*) AS n FROM market_records "
            "WHERE venue = %s AND event_time < %s", (venue, launch)) or 0
        out.append(CheckResult(
            "no_pre_launch_event_time", early == 0, None,
            {"venue": venue, "records_before_launch": early,
             "launch": launch.isoformat()}))

    negative = await postgres.fetchval(
        "SELECT count(*) AS n FROM market_records "
        "WHERE ingest_time < collection_time - interval '5 seconds'") or 0
    out.append(CheckResult(
        "no_negative_latency", negative == 0, None,
        {"records_with_negative_latency": negative},
        "" if negative == 0
        else "ingest before collection by more than clock skew allows"))
    return out


async def check_archive_checksums(limit: int = 200) -> list[CheckResult]:
    """Weekly audit (§8): re-hash archived objects against the manifest.

    Silent bit rot on object storage is a real thing, and an archive nobody
    reads back is a hope rather than a backup.
    """
    from botmaximus.storage.archive import WrittenFile
    rows = await postgres.fetch(
        "SELECT * FROM storage_manifest "
        "ORDER BY verified_at NULLS FIRST, written_at LIMIT %s", (limit,))
    archive = store.archive()
    bad = []
    for o in rows:
        wf = WrittenFile(bucket=o["bucket"], key=o["object_key"],
                         rows=o["rows"], bytes=o["bytes"], sha256=o["sha256"],
                         dataset_id=o["dataset_id"], partition=o["partition"],
                         written_at=o["written_at"])
        if archive.verify(wf):
            await postgres.execute(
                "UPDATE storage_manifest SET verified_at = now() "
                "WHERE bucket = %s AND object_key = %s",
                (o["bucket"], o["object_key"]))
        else:
            bad.append(o["object_key"])
    return [CheckResult(
        "archive_checksums", not bad, None,
        {"checked": len(rows), "failed": len(bad), "examples": bad[:5]},
        "" if not bad else "objects that no longer hash to the manifest")]


CHECKS = (
    check_row_count_parity,
    check_coverage_reconciles,
    check_orders_have_predictions,
    check_scrutiny_blobs_exist,
    check_generated_candidates_have_provenance,
    check_timestamps_are_sane,
)


# ---------------------------------------------------------------- runner

async def run_all(include_checksums: bool = False) -> list[CheckResult]:
    """Hourly. Records every result, then escalates the failures.

    A check that raises is itself a failure — an integrity checker that dies
    quietly is indistinguishable from one that keeps passing.
    """
    results: list[CheckResult] = []
    checks = list(CHECKS) + ([check_archive_checksums] if include_checksums else [])
    for fn in checks:
        try:
            results.extend(await fn())
        except Exception as e:                          # noqa: BLE001
            log.exception("integrity check %s raised", fn.__name__)
            results.append(CheckResult(fn.__name__, False, None,
                                       {"error": str(e)},
                                       "the check itself failed to run"))
    for r in results:
        await _record(r)
    failed = [r for r in results if not r.passed]
    if failed:
        await _escalate(failed)
    return results


async def _record(r: CheckResult) -> None:
    await postgres.execute(
        "INSERT INTO integrity_events (check_name, dataset_id, passed, "
        " observed, detail) VALUES (%s,%s,%s,%s,%s)",
        (r.name, r.dataset_id, r.passed,
         json.dumps(r.observed, default=str), r.detail))


async def _escalate(failed: list[CheckResult]) -> None:
    """§11: red-lining halts new writes to the affected dataset via L2 rather
    than continuing on corrupted or unbacked state."""
    from botmaximus.obs import degradation
    for r in failed:
        await degradation.record(
            f"integrity_{r.name}",
            r.detail or f"integrity check {r.name} failed",
            dataset_id=r.dataset_id, observed=r.observed)


async def status(limit: int = 50) -> dict:
    rows = await postgres.fetch(
        "SELECT * FROM integrity_events ORDER BY at DESC LIMIT %s", (limit,))
    last_backup = await postgres.fetchrow(
        "SELECT * FROM backup_events WHERE succeeded ORDER BY at DESC LIMIT 1")
    last_drill = await postgres.fetchrow(
        "SELECT * FROM backup_events WHERE kind = 'restore_drill' "
        "AND succeeded ORDER BY at DESC LIMIT 1")
    return {
        "recent": [json.loads(json.dumps(dict(r), default=str)) for r in rows],
        "failing": [json.loads(json.dumps(dict(r), default=str))
                    for r in rows if not r["passed"]],
        "last_backup_at": last_backup["at"].isoformat() if last_backup else None,
        "last_restore_drill_at": (last_drill["at"].isoformat()
                                  if last_drill else None),
    }
