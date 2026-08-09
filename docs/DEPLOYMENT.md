# BOTMAXIMUS — Vultr deployment (Storage v2.0 §13)

The six-phase clean install, plus the carryover. **Each phase is verified before
the next begins**; if one fails, only that phase is fixed and retried.

Status legend: ✅ built and tested · ⏳ built, needs real infrastructure to verify

---

## Phase 0 — credentials (operator)

Everything below is blocked on these. Put them in `server/.env` (never
committed, never in a backup — §8):

```ini
POSTGRES_DSN=postgresql://botmaximus:<password>@127.0.0.1:5432/botmaximus
OBJECTSTORE_ENDPOINT=https://<region>.vultrobjects.com
OBJECTSTORE_ACCESS_KEY=...
OBJECTSTORE_SECRET_KEY=...
```

```bash
pip install boto3     # ObjectStorageBackend imports it lazily and says so
```

---

## Phase 1 — Object Storage online ⏳

Provision three buckets: `botmaximus-archive-tokyo`,
`botmaximus-quarantine-tokyo`, `botmaximus-mirror-singapore`. Encryption on.

**Verify:** with the endpoint configured, `storage/records.py` uses
`ObjectStorageBackend` instead of the local filesystem. There is deliberately no
silent fallback — if an endpoint is configured and unreachable, writes raise
rather than quietly landing on disk, because a local file the operator believes
is in Tokyo is worse than an error.

```bash
python -c "from botmaximus.storage import records; print(type(records.archive().backend).__name__)"
# expect ObjectStorageBackend
```

## Phase 2 — Postgres online ⏳

Postgres 16+ on the VPS, data directory on the encrypted block volume
(`VULTR_BLOCK_MOUNT`), separate from the VPS root so rebuilding the instance
does not take the decision record with it.

```bash
python -c "import asyncio;from botmaximus.storage import postgres as p; \
  p.ensure_compatible_event_loop(); \
  asyncio.run(p.open_pool()); asyncio.run(p.bootstrap())"
```

Enable continuous WAL archiving in `postgresql.conf` (the server's job, not a
script's — anything else races the checkpointer):

```
wal_level = replica
archive_mode = on
archive_command = 'aws s3 cp %p s3://botmaximus-archive-tokyo/wal/%f --endpoint-url $OBJECTSTORE_ENDPOINT'
```

**Verify:** `bootstrap()` is idempotent — run it twice, expect no error;
`SELECT count(*) FROM information_schema.tables WHERE table_schema='bmx'` = 35.

## Phase 3 — Collector online, dual-write ✅

`storage/records.py` writes Postgres + Parquet + manifest on every batch.
Postgres first and unconditionally; an archive failure defers (§7) and leaves no
manifest row, so tier-out will refuse to drop that day.

```bash
cd server && .venv/bin/python -m botmaximus.main      # port 8300
curl localhost:8300/api/health                        # {"postgres": true, ...}
curl localhost:8300/api/storage                       # §11 telemetry
```

## Phase 4 — Tier-out online ✅

Nightly 00:15 UTC (`storage/jobs.py:TierOutJob`). Verify → record → drop, in
that order, and never drop on a failed verification.

Roll out to `btc_ohlcv_1m` first:

```bash
python -c "import asyncio;from datetime import date;from botmaximus.storage import tiering; \
  print(asyncio.run(tiering.tier_out_day(date(2026,6,1))))"
```

**Note:** one partition holds every dataset (partitioned by `event_time` only),
so a day is only eligible once the **longest** hot window has passed — 30 days,
set by funding and liquidations. That keeps the partition count in the low tens.

## Phase 5 — Bitemporal query interface ✅

`records.read_as_of()` implements §6; `backtest/runner.py` pins `as_of` at run
start and threads it through every feed; `store.save_run` refuses without one,
and `backtest_runs.as_of` is `NOT NULL` so an unpinned run is unrecordable.

## Phase 6 — Backup and monitoring ⏳

```bash
python ops/pg_backup.py full            # pg_dump -Fc, gzip, upload
python ops/pg_backup.py drill           # restore to scratch, verify, drop
python ops/pg_backup.py mirror --days 90
```

Schedule: nightly `full`, hourly incrementals via WAL, `drill` every 30 days,
`mirror` daily, checksum audit weekly (already a supervised job).

**The drill is the one that matters.** §8: a backup that has never been restored
is not a backup. It restores into a throwaway database and counts rows in
`trials`, `strategies`, the ledger tables and `orders` — a drill that only
checked the dump was readable would pass against an empty database.

Targets: RPO ≤ 5 min, RTO ≤ 30 min.

---

## Phase 7 — carryover from Mongo ⏳

```bash
python ops/migrate_from_mongo.py --dry-run --mongo-db botmaximus_bybit --venue bybit
python ops/migrate_from_mongo.py --what trials --mongo-db botmaximus_bybit --venue bybit
python ops/migrate_from_mongo.py --what all     --mongo-db botmaximus_bybit --venue bybit
# Binance era, into the SAME store but tagged as a different venue:
python ops/migrate_from_mongo.py --what market  --mongo-db botmaximus --venue binance
```

Three things to hold onto:

1. **`--venue` must match the database's era.** Venue is derived from each
   record's own `source`, never from config, because this process runs
   configured for Bybit while writing Binance-era history. Get this wrong and a
   million Binance candles are stamped `bybit` and the two price series merge.
2. **The trial ledger is verified by count and fails loudly on a shortfall.**
   A smaller lifetime `n_trials` silently flatters every subsequent deflated
   Sharpe — the exact no-op the pre-C2 hardening fixed.
3. **Market history goes to Parquet, not the hot window.** Two years in
   Postgres would defeat the tiering the architecture is built on.

Bybit-era liquidations and order book have no history endpoint on any venue: if
they are not carried over they are gone permanently.

After verification, drop `pymongo` from requirements — this script is its last
consumer.

---

## Ordering on cutover day

Collectors first, everything else after. Liquidations and order book cannot be
backfilled, so every minute of collector downtime during the migration is a
permanent hole the coverage gate will correctly refuse to backtest across
forever after.

## What is verified today

- 478 tests green against a real Postgres 18, and against a **virgin database**,
  proving `schema.sql` bootstraps 35 relations from nothing.
- 352 green + 104 skipped with **no database at all**, so the suite stays
  runnable offline.
- Constraint behaviour proven by refusal: unknown venue, `quality_ok=false` in
  production, flags on a clean record, inverted system-time interval, a
  realization with no prediction, both malformed realization shapes, a bad
  lifecycle state, orphan provenance, and `backtest_runs` without `as_of`.
- Tier-out proven to **refuse** on a corrupted archive and on a row-count
  mismatch, and to re-archive a day that object storage never received.

Everything marked ⏳ above is written and unit-tested but has never run against
Vultr, because the credentials are not in this environment.
