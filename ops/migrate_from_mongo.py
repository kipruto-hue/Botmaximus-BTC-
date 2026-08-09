#!/usr/bin/env python3
r"""One-off carryover from MongoDB into the two-store architecture (Phase 8).

The **only** remaining consumer of `pymongo` in this repository. When it has
run and its counts have been verified, pymongo comes out of the requirements.

    python ops/migrate_from_mongo.py --dry-run          # counts, writes nothing
    python ops/migrate_from_mongo.py --what trials
    python ops/migrate_from_mongo.py --what all

## What is actually at stake here

Most of this is convenience: OHLCV, funding and open interest are all
re-fetchable from the venue. Two things are not.

**The trial ledger.** §3.F makes `trials` the input to the deflated Sharpe's
multiple-testing correction, and the count is lifetime by construction. If this
migration under-counts, `n_trials` silently drops and every subsequent verdict
is flattered — which is precisely the no-op bug that the pre-C2 hardening
existed to fix. So the trial migration verifies by count and **fails loudly** on
a shortfall rather than reporting success.

**Bybit-era liquidations and order book.** Neither venue has a history
endpoint. If they are not carried over they are gone permanently.

## Venue tagging is not optional

Every record is written with the venue derived from its own `source`, never
from `settings.venue` — this process runs configured for Bybit while writing
Binance-era history, and a venue read from config would stamp a million
Binance candles `bybit` and merge two incompatible price series into one. The
old Mongo databases were separate *per venue*, so `--mongo-db` also carries an
explicit `--venue` and the two must agree.

## Where the history lands

Market history goes to **Parquet only**, not the Postgres hot window. The hot
window is 24h–7d by design (§3.A); loading two years into it would defeat the
tiering the architecture is built around. Decisions (trials, strategies,
verdicts) go to Postgres, because that is where decisions live.
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))

from botmaximus.storage import postgres                     # noqa: E402
from botmaximus.storage import records as store             # noqa: E402
from botmaximus.storage.envelope import Record              # noqa: E402
from botmaximus.storage.venues import require_venue         # noqa: E402

log = logging.getLogger("migrate")
UTC = timezone.utc

#: Mongo collection → dataset_id.
MARKET_COLLECTIONS = {
    "btc_ohlcv_1m": "btc_ohlcv_1m",
    "btc_funding_8h": "btc_funding_8h",
    "btc_oi_5m": "btc_oi_5m",
    "btc_funding": "btc_funding",
    "btc_open_interest": "btc_open_interest",
    "btc_liquidations": "btc_liquidation",
    "btc_orderbook": "btc_orderbook",
    "btc_price_ticks": "btc_price_tick",
}

#: Never re-fetchable on any venue. Losing these is permanent.
UNRECOVERABLE = ("btc_liquidations", "btc_orderbook", "btc_price_ticks")

BATCH = 20_000


def mongo_db(uri: str, name: str):
    from pymongo import AsyncMongoClient
    return AsyncMongoClient(uri, tz_aware=True)[name]


# ---------------------------------------------------------------- trials

async def migrate_trials(db, dry_run: bool) -> dict:
    """The one migration that must not under-count."""
    src = await db["trial_ledger"].count_documents({})
    log.info("trials: %d row(s) in mongo", src)
    if dry_run:
        return {"source": src, "migrated": 0, "verified": False}

    n = 0
    async for d in db["trial_ledger"].find({}):
        await postgres.execute(
            "INSERT INTO trials (sig_hash, config_hash, strategy_id, "
            " first_seen, last_seen, replays) VALUES (%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (sig_hash, config_hash) DO NOTHING",
            (d["sig_hash"], d["config_hash"], d.get("strategy_id", "unknown"),
             d.get("first_seen") or datetime.now(UTC),
             d.get("last_seen") or datetime.now(UTC),
             int(d.get("replays", 1))))
        n += 1

    final = await postgres.fetchval("SELECT count(*) AS n FROM trials") or 0
    if final < src:
        raise SystemExit(
            f"TRIAL LEDGER SHORTFALL: mongo had {src}, postgres has {final}. "
            f"Refusing to report success — a smaller lifetime trial count "
            f"silently weakens the deflated Sharpe correction on every verdict "
            f"from here on (§3.F).")
    log.info("trials: %d migrated, postgres now holds %d", n, final)
    return {"source": src, "migrated": n, "final": final, "verified": True}


# ---------------------------------------------------------------- strategies

async def migrate_strategies(db, dry_run: bool) -> dict:
    import json
    src = await db["strategies"].count_documents({})
    log.info("strategies: %d row(s) in mongo", src)
    if dry_run:
        return {"source": src, "migrated": 0}

    n = 0
    async for d in db["strategies"].find({}):
        defn = d.get("definition") or {}
        dhash = d.get("definition_hash") or _hash(defn)
        async with postgres.transaction() as conn:
            await conn.execute(
                "INSERT INTO strategy_definitions_blob "
                "(definition_hash, definition) VALUES (%s,%s) "
                "ON CONFLICT (definition_hash) DO NOTHING",
                (dhash, json.dumps(defn, default=str)))
            await conn.execute(
                "INSERT INTO strategies (strategy_id, version, "
                " definition_hash, lifecycle_state, origin, rationale, "
                " signature, warnings, created_at, updated_at, last_verdict) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (strategy_id, version) DO NOTHING",
                (d["strategy_id"], int(d.get("version", 1)), dhash,
                 d.get("lifecycle_state", "candidate"),
                 d.get("origin", "seed"), d.get("rationale"),
                 list(d.get("signature") or []), list(d.get("warnings") or []),
                 d.get("created_at") or datetime.now(UTC),
                 d.get("updated_at") or datetime.now(UTC),
                 json.dumps(d["last_verdict"], default=str)
                 if d.get("last_verdict") else None))
        n += 1

    # Lifecycle history: the audit trail of how each strategy got where it is.
    ev = 0
    async for d in db["strategy_events"].find({}):
        await postgres.execute(
            "INSERT INTO strategy_events (strategy_id, event, at, detail) "
            "VALUES (%s,%s,%s,%s)",
            (d.get("strategy_id", "unknown"), d.get("event", "unknown"),
             d.get("at") or datetime.now(UTC),
             json.dumps({k: v for k, v in d.items()
                         if k not in ("_id", "strategy_id", "event", "at")},
                        default=str)))
        ev += 1
    log.info("strategies: %d migrated, %d event(s)", n, ev)
    return {"source": src, "migrated": n, "events": ev}


def _hash(defn: dict) -> str:
    import hashlib
    import json
    return hashlib.sha256(
        json.dumps(defn, sort_keys=True, default=str).encode()).hexdigest()[:16]


# ---------------------------------------------------------------- market data

async def migrate_market(db, venue: str, dry_run: bool,
                         only: str | None = None) -> dict:
    """Market history → Parquet archive, day by day, venue tagged.

    Straight to the archive: two years in the Postgres hot window would defeat
    the tiering this architecture is built on (§3.A).
    """
    out: dict[str, dict] = {}
    for coll, dataset_id in MARKET_COLLECTIONS.items():
        if only and coll != only:
            continue
        total = await db[coll].count_documents({})
        if total == 0:
            continue
        flag = " (UNRECOVERABLE)" if coll in UNRECOVERABLE else ""
        log.info("%s: %d document(s)%s", coll, total, flag)
        if dry_run:
            out[coll] = {"source": total, "archived": 0}
            continue

        by_day: dict[str, list[Record]] = defaultdict(list)
        archived = skipped = 0
        async for d in db[coll].find({}).sort("event_time", 1):
            meta = d.get("meta") or {}
            source = meta.get("source") or venue
            try:
                rec = Record(
                    dataset_id=dataset_id,
                    source=source if source.startswith(venue) else venue,
                    event_time=d["event_time"],
                    payload=d.get("payload") or {},
                    symbol=meta.get("symbol"),
                    collection_time=d.get("collection_time") or d["event_time"],
                    ingest_time=d.get("ingest_time") or d["event_time"],
                    valid_from_sys=d.get("ingest_time") or d["event_time"],
                    # The gate that originally judged it, not today's.
                    quality_ok=bool(d.get("quality_ok", True)),
                    quality_gate_version=int(d.get("quality_gate_version", 1)),
                    annotations=tuple(d.get("quality_flags") or ()),
                )
            except Exception as e:                      # noqa: BLE001
                skipped += 1
                log.debug("skipped a %s record: %s", coll, e)
                continue
            if not rec.quality_ok:
                skipped += 1        # failed records were never production data
                continue
            by_day[rec.partition_path()].append(rec)
            if len(by_day[rec.partition_path()]) >= BATCH:
                archived += await _flush(by_day, rec.partition_path())

        for key in list(by_day):
            archived += await _flush(by_day, key)
        log.info("%s: archived %d, skipped %d", coll, archived, skipped)
        out[coll] = {"source": total, "archived": archived, "skipped": skipped}
    return out


async def _flush(by_day: dict, key: str) -> int:
    """Write one day/dataset file and index it.

    The manifest row is written here rather than left for later: the weekly
    checksum audit and the tier-out verification both reconcile against it, and
    an archived object with no manifest entry is invisible to both.
    """
    recs = by_day.pop(key, [])
    if not recs:
        return 0
    written = store.archive().write(recs, part=f"migrated-{recs[0].record_id[:8]}")
    await store._record_manifest(written)
    return written.rows


# ---------------------------------------------------------------- driver

async def _amain(args) -> int:
    require_venue(args.venue)
    postgres.ensure_compatible_event_loop()
    await postgres.open_pool()
    await postgres.bootstrap()
    db = mongo_db(args.mongo_uri, args.mongo_db)

    report: dict = {"venue": args.venue, "mongo_db": args.mongo_db,
                    "dry_run": args.dry_run}
    try:
        if args.what in ("all", "trials"):
            report["trials"] = await migrate_trials(db, args.dry_run)
        if args.what in ("all", "strategies"):
            report["strategies"] = await migrate_strategies(db, args.dry_run)
        if args.what in ("all", "market"):
            report["market"] = await migrate_market(db, args.venue,
                                                    args.dry_run, args.only)
    finally:
        await postgres.close()

    print("\n" + "=" * 72)
    for k, v in report.items():
        print(f"{k:>12}: {v}")
    print("=" * 72)
    if args.dry_run:
        print("DRY RUN — nothing was written.")
    return 0


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="Mongo → Postgres/Parquet carryover")
    ap.add_argument("--mongo-uri", default="mongodb://localhost:27017")
    ap.add_argument("--mongo-db", default="botmaximus_bybit",
                    help="botmaximus_bybit (Bybit era) | botmaximus (Binance era)")
    ap.add_argument("--venue", default="bybit",
                    help="MUST match the database's era; see the module docstring")
    ap.add_argument("--what", default="all",
                    choices=("all", "trials", "strategies", "market"))
    ap.add_argument("--only", help="a single mongo collection (market only)")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    postgres.ensure_compatible_event_loop()
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
