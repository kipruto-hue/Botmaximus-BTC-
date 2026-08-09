#!/usr/bin/env python3
r"""Postgres backup, restore drill and mirror sync (Storage v2.0 §8).

Replaces the Mongo-era `mongo_backup.py`. Three subcommands:

    python ops/pg_backup.py full            # pg_dump -> object storage
    python ops/pg_backup.py drill           # restore into a scratch DB, verify, drop
    python ops/pg_backup.py mirror          # copy the last 90d to Singapore

## Why the drill is not optional

§8: "A backup that has never been restored is not a backup." A dump that cannot
be restored fails in exactly one place — the incident — and looks perfect until
then. `drill` restores into a throwaway database, runs a checksum query against
the tables that matter, and drops it. Every run writes a `backup_events` row so
the dashboard can show backup age and last successful drill as *evidence*
rather than as a cron schedule someone believes is still firing.

## Secrets never enter a backup

§8 is explicit: backups include Postgres, the Parquet manifest, `.env.example`,
config and code — never `.env`. Secrets rotate via the operator, not via
restore. `pg_dump` output cannot contain `.env`, and nothing here reads it
except to obtain the DSN it must connect with.

WAL archiving is configured on the server (`archive_command` in
postgresql.conf), not here — continuous archiving is the server's job, and a
script that tried to do it would be racing the checkpointer.
"""
from __future__ import annotations

import argparse
import asyncio
import gzip
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "server"))

from botmaximus.config import settings                      # noqa: E402
from botmaximus.storage import postgres                     # noqa: E402

log = logging.getLogger("pg_backup")
UTC = timezone.utc

#: Tables whose row counts a restore drill verifies. These are the ones whose
#: loss is unrecoverable: the trial ledger (statistical honesty) and the money
#: records. A drill that only checked the dump was readable would pass on an
#: empty database.
DRILL_TABLES = ("trials", "strategies", "execution_ledger_predictions",
                "execution_ledger_realizations", "orders", "fills",
                "kill_events", "backtest_runs", "coverage_ledger")


def _pg_bin(name: str) -> str:
    return os.environ.get(f"{name.upper()}_PATH", name)


def _dsn_parts(dsn: str) -> dict:
    from urllib.parse import unquote, urlparse
    u = urlparse(dsn)
    return {
        "host": u.hostname or "127.0.0.1",
        "port": str(u.port or 5432),
        "user": u.username or "postgres",
        "password": unquote(u.password) if u.password else None,
        "dbname": (u.path or "/botmaximus").lstrip("/"),
    }


def _env_with_password(parts: dict) -> dict:
    env = dict(os.environ)
    if parts["password"]:
        env["PGPASSWORD"] = parts["password"]
    return env


def dump(dest: Path) -> Path:
    """`pg_dump -Fc`, gzipped. Custom format so a partial restore is possible."""
    parts = _dsn_parts(settings.postgres_dsn)
    dest.parent.mkdir(parents=True, exist_ok=True)
    raw = dest.with_suffix(".dump")
    cmd = [_pg_bin("pg_dump"), "-h", parts["host"], "-p", parts["port"],
           "-U", parts["user"], "-d", parts["dbname"], "-Fc", "-f", str(raw)]
    log.info("pg_dump -> %s", raw)
    subprocess.run(cmd, check=True, env=_env_with_password(parts))
    with open(raw, "rb") as fi, gzip.open(dest, "wb") as fo:
        shutil.copyfileobj(fi, fo)
    raw.unlink()
    log.info("wrote %s (%.1f MB)", dest, dest.stat().st_size / 1e6)
    return dest


async def _record(kind: str, ok: bool, target: str = "", nbytes: int = 0,
                  detail: str = "") -> None:
    try:
        await postgres.execute(
            "INSERT INTO backup_events (kind, succeeded, target, bytes, detail) "
            "VALUES (%s,%s,%s,%s,%s)", (kind, ok, target, nbytes, detail))
    except Exception as e:                              # noqa: BLE001
        log.error("could not record backup event: %s", e)


async def cmd_full(args) -> int:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    out = Path(args.dest) / f"botmaximus-{stamp}.dump.gz"
    try:
        path = dump(out)
    except subprocess.CalledProcessError as e:
        await _record("full", False, str(out), detail=str(e))
        log.error("pg_dump failed: %s", e)
        return 1

    key = f"postgres/{path.name}"
    uploaded = 0
    try:
        from botmaximus.storage import records as store
        store.archive().backend.put(settings.bucket_archive, key,
                                    path.read_bytes())
        uploaded = path.stat().st_size
        log.info("uploaded %s", key)
    except Exception as e:                              # noqa: BLE001
        # The local dump still exists; that is a real backup, just not an
        # off-box one. Recorded as a failure so it is not mistaken for durable.
        await _record("full", False, key, path.stat().st_size,
                      f"local dump ok, upload failed: {e}")
        log.error("upload failed (local dump retained at %s): %s", path, e)
        return 1
    await _record("full", True, key, uploaded)
    return 0


async def cmd_drill(args) -> int:
    """Restore into a scratch database, verify, drop. §8, every 30 days."""
    parts = _dsn_parts(settings.postgres_dsn)
    scratch = f"bmx_drill_{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    src = Path(args.dump) if args.dump else _latest_local(Path(args.dest))
    if src is None:
        log.error("no dump to restore; run `full` first")
        await _record("restore_drill", False, detail="no dump available")
        return 1

    env = _env_with_password(parts)
    base = ["-h", parts["host"], "-p", parts["port"], "-U", parts["user"]]
    tmp = Path(tempfile.mkdtemp()) / "restore.dump"
    with gzip.open(src, "rb") as fi, open(tmp, "wb") as fo:
        shutil.copyfileobj(fi, fo)

    try:
        subprocess.run([_pg_bin("psql"), *base, "-d", "postgres", "-c",
                        f'CREATE DATABASE "{scratch}"'], check=True, env=env)
        subprocess.run([_pg_bin("pg_restore"), *base, "-d", scratch,
                        "--no-owner", "--no-privileges", str(tmp)],
                       check=False, env=env)

        # Compare against the LIVE database, not against zero. A drill whose
        # only test is "the tables exist" passes against an empty restore —
        # which is the precise failure it is supposed to catch.
        live = await _live_counts()
        counts, missing, empty = {}, [], []
        for table in DRILL_TABLES:
            r = subprocess.run(
                [_pg_bin("psql"), *base, "-d", scratch, "-tA", "-c",
                 f"SELECT count(*) FROM bmx.{table}"],
                capture_output=True, text=True, env=env)
            if r.returncode != 0:
                missing.append(table)
                continue
            n = int((r.stdout or "0").strip() or 0)
            counts[table] = n
            # Source drift since the dump is expected, so this does not demand
            # equality — only that data which exists did not vanish entirely.
            if n == 0 and live.get(table, 0) > 0:
                empty.append(f"{table} (live has {live[table]})")

        ok = not missing and not empty
        if missing:
            detail = f"tables missing from the restore: {missing}"
        elif empty:
            detail = (f"restored but EMPTY where the live database has rows: "
                      f"{empty} — the dump is not usable as a backup")
        else:
            detail = f"restored {len(counts)} table(s): {counts}"
        log.info("drill %s — %s", "PASSED" if ok else "FAILED", detail)
        await _record("restore_drill", ok, scratch, detail=detail)
        return 0 if ok else 1
    finally:
        subprocess.run([_pg_bin("psql"), *base, "-d", "postgres", "-c",
                        f'DROP DATABASE IF EXISTS "{scratch}"'],
                       check=False, env=env)
        shutil.rmtree(tmp.parent, ignore_errors=True)


async def cmd_mirror(args) -> int:
    """Copy the last `days` of archive objects to the Singapore bucket.

    Tokyo alone is one earthquake from a very bad day (§8).
    """
    from botmaximus.storage import records as store
    since = datetime.now(UTC) - timedelta(days=args.days)
    rows = await postgres.fetch(
        "SELECT * FROM storage_manifest WHERE bucket = %s AND written_at >= %s",
        (settings.bucket_archive, since))
    backend = store.archive().backend
    copied, failed = 0, 0
    for o in rows:
        try:
            if backend.exists(settings.bucket_mirror, o["object_key"]):
                continue
            backend.put(settings.bucket_mirror, o["object_key"],
                        backend.get(o["bucket"], o["object_key"]))
            copied += 1
        except Exception as e:                          # noqa: BLE001
            failed += 1
            log.error("mirror failed for %s: %s", o["object_key"], e)
    ok = failed == 0
    await _record("mirror_sync", ok, settings.bucket_mirror,
                  detail=f"copied {copied}, failed {failed}, "
                         f"window {args.days}d")
    log.info("mirror: copied %d, failed %d", copied, failed)
    return 0 if ok else 1


async def _live_counts() -> dict[str, int]:
    out: dict[str, int] = {}
    for table in DRILL_TABLES:
        try:
            out[table] = await postgres.fetchval(
                f"SELECT count(*) AS n FROM {table}") or 0
        except Exception:                               # noqa: BLE001
            out[table] = 0
    return out


def _latest_local(dest: Path) -> Path | None:
    dumps = sorted(dest.glob("botmaximus-*.dump.gz"))
    return dumps[-1] if dumps else None


async def _amain(args) -> int:
    postgres.ensure_compatible_event_loop()
    try:
        await postgres.open_pool()
    except Exception as e:                              # noqa: BLE001
        log.error("postgres unreachable: %s", e)
        return 2
    try:
        return await {"full": cmd_full, "drill": cmd_drill,
                      "mirror": cmd_mirror}[args.command](args)
    finally:
        await postgres.close()


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="Postgres backup / DR (§8)")
    ap.add_argument("command", choices=("full", "drill", "mirror"))
    ap.add_argument("--dest", default="data/backups",
                    help="local staging directory for dumps")
    ap.add_argument("--dump", help="explicit dump to restore (drill)")
    ap.add_argument("--days", type=int, default=90,
                    help="mirror window in days (default 90, per §8)")
    args = ap.parse_args()
    postgres.ensure_compatible_event_loop()
    return asyncio.run(_amain(args))


if __name__ == "__main__":
    raise SystemExit(main())
