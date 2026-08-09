r"""Nightly Parquet snapshots (Storage v2.0 §3.B, §3.I).

Two things live in Postgres as the system of record but also need to exist in
the permanent archive, for different reasons.

**The coverage ledger (§3.B).** Snapshotted so that *historical coverage state
is itself point-in-time queryable*. This is subtler than it sounds: the ledger
is mutable — a slot marked missing today can be marked complete tomorrow when a
backfill lands. So a backtest re-run months later against the same `as_of` would
consult a ledger that has since changed its mind, and "why did this window get
refused in April but pass in November?" becomes unanswerable. The nightly
snapshot is the answer: each day's ledger state, frozen and dated.

**The money records (§3.I).** Orders, fills, ledger and kills are ACID-critical
and Postgres stays their system of record. The nightly export exists so the
audit trail survives the VPS — object storage is independent of the box, and
these are the rows whose loss is least acceptable.

Neither snapshot is ever read back into Postgres. They flow outward, like
everything else (§4).
"""
from __future__ import annotations

import json
import logging
from datetime import date, datetime, timedelta, timezone

import pyarrow as pa

from botmaximus.storage import postgres
from botmaximus.storage import records as store

log = logging.getLogger(__name__)

UTC = timezone.utc

#: §3.I table → the timestamp column its daily slice is cut on.
MONEY_TABLES = {
    "orders": "placed_at",
    "fills": "fill_time",
    "execution_ledger_predictions": "decision_time",
    "execution_ledger_realizations": "reconciled_at",
    "execution_ledger_drift": "computed_at",
    "kill_events": "at",
}


def _rows_to_table(rows: list[dict]) -> pa.Table:
    """Columns as JSON text.

    Deliberately schema-agnostic: these tables gain columns over time (§10
    allows additive changes), and a typed Arrow schema per table would turn
    every column addition into a snapshot migration. The envelope-level
    question — which day, which table — is what the archive indexes on.
    """
    return pa.table({
        "row": pa.array([json.dumps(r, default=str, sort_keys=True)
                         for r in rows], pa.string()),
    })


async def snapshot_coverage(day: date | None = None) -> dict:
    """Freeze the coverage ledger as it stands (§3.B).

    The whole ledger, not just the day's slot: the point is to answer "what did
    the ledger say on this date", and that includes its opinion about older
    slots, which backfills keep revising.
    """
    day = day or (datetime.now(UTC) - timedelta(days=1)).date()
    rows = await postgres.fetch(
        "SELECT * FROM coverage_ledger ORDER BY venue, feed, slot")
    if not rows:
        return {"day": str(day), "rows": 0, "key": None}

    key = (f"coverage_snapshots/year={day:%Y}/month={day:%m}/day={day:%d}"
           f"/coverage.parquet")
    written = store.archive().write_blob(
        key, _rows_to_table([dict(r) for r in rows]),
        dataset_id="coverage_snapshot", partition=key.rsplit("/", 1)[0])
    await store._record_manifest(written)
    log.info("coverage snapshot %s: %d row(s)", day, len(rows))
    return {"day": str(day), "rows": len(rows), "key": key}


async def snapshot_money(day: date | None = None) -> list[dict]:
    """Export the previous day's money records (§3.I).

    Postgres remains the system of record; this is the copy that survives the
    VPS.
    """
    day = day or (datetime.now(UTC) - timedelta(days=1)).date()
    start = datetime(day.year, day.month, day.day, tzinfo=UTC)
    end = start + timedelta(days=1)

    out = []
    for table, ts_col in MONEY_TABLES.items():
        rows = await postgres.fetch(
            f"SELECT * FROM {table} WHERE {ts_col} >= %s AND {ts_col} < %s "
            f"ORDER BY {ts_col}", (start, end))
        if not rows:
            continue
        key = (f"money/{table}/year={day:%Y}/month={day:%m}/day={day:%d}"
               f"/{table}.parquet")
        written = store.archive().write_blob(
            key, _rows_to_table([dict(r) for r in rows]),
            dataset_id=f"money_{table}", partition=key.rsplit("/", 1)[0])
        await store._record_manifest(written)
        out.append({"table": table, "rows": len(rows), "key": key})
        log.info("money snapshot %s %s: %d row(s)", day, table, len(rows))
    return out


async def run_nightly(day: date | None = None) -> dict:
    """Both snapshots. Independent: one failing must not skip the other.

    A coverage snapshot that silently stopped because an unrelated ledger
    export raised would be discovered months later, by a backtest that could no
    longer explain its own refusal.
    """
    result: dict = {"day": str(day or (datetime.now(UTC) - timedelta(days=1)).date())}
    try:
        result["coverage"] = await snapshot_coverage(day)
    except Exception as e:                              # noqa: BLE001
        log.exception("coverage snapshot failed")
        result["coverage_error"] = str(e)
    try:
        result["money"] = await snapshot_money(day)
    except Exception as e:                              # noqa: BLE001
        log.exception("money snapshot failed")
        result["money_error"] = str(e)
    return result
