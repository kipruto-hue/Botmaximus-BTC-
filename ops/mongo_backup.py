r"""BOTMAXIMUS database backup and restore.

The database is the most expensive asset in the system and the only one that
cannot be rebuilt. Liquidations and order book have no venue history endpoint at
all; open interest is capped at 30 days; the 1,051,302 1m candles took a deep
backfill to assemble. A disk failure without a backup is not a setback, it is
the permanent loss of every window the backtester is allowed to read.

## Why this is not `mongodump`

Two reasons, and the second is the real one:

1. The MongoDB Database Tools are not in the portable zip this project ships
   (neither is `mongosh`), so `mongodump` is not present on the operator's
   machine and would be one more thing to install and keep in step.
2. **The high-frequency series are Mongo time-series collections.** `mongodump`
   dumps those at the internal `system.buckets.*` level, which is faithful but
   version-coupled: the bucket layout is an implementation detail that has
   changed between server releases, and a restore into a different server
   version is exactly the situation a disaster recovery is *for*.

So this dumps **measurements** — the documents the application actually reads —
and `restore` recreates each time-series collection from its captured options
before inserting them back. The output is deliberately NOT mongorestore-format,
and the manifest says so in `format`, so nobody feeds it to the wrong tool and
discovers the mismatch during an emergency.

The cost of a hand-rolled format is that it can rot silently. `self-test` exists
to stop that: it round-trips a real dump into a scratch database and compares
counts, indexes and time-series options. Run it after touching this file.

## Off-machine or it doesn't count

A backup on the same disk as the database protects against exactly one failure
mode (fat-fingered `drop`) and not the one that takes the whole asset. `--dest`
should be a different physical device or a synced remote. `backup.ps1` defaults
to a local path only so that the first run works at all; that is a starting
point, not the finished job.

Usage (from the server venv, which already has pymongo):

    python ops/mongo_backup.py dump    --dest D:\botmaximus-backups
    python ops/mongo_backup.py verify  --path D:\botmaximus-backups\<stamp>
    python ops/mongo_backup.py restore --path D:\...\<stamp> --into botmaximus_restored
    python ops/mongo_backup.py prune   --dest D:\botmaximus-backups --keep 7
    python ops/mongo_backup.py self-test
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import bson
from pymongo import MongoClient

FORMAT = "botmaximus-measurements-v1"
DEFAULT_URI = "mongodb://localhost:27017"
DEFAULT_DB = "botmaximus"

#: Internal representations of time-series collections. Dumping these alongside
#: the measurements would double the size and restore them into an inconsistent
#: state — the buckets are derived, not source.
SKIP_PREFIXES = ("system.",)


def _log(msg: str) -> None:
    print(f"{datetime.now(timezone.utc):%H:%M:%S} {msg}", flush=True)


def _user_collections(db) -> list[dict]:
    return sorted(
        (c for c in db.list_collections()
         if not c["name"].startswith(SKIP_PREFIXES) and c.get("type") != "view"),
        key=lambda c: c["name"],
    )


def _indexes_for(db, name: str, options: dict) -> list[dict]:
    """Recreatable index specs. `_id_` is implicit and rejected on re-creation;
    a time-series collection's internal index cannot be created by name either."""
    out = []
    for ix in db[name].list_indexes():
        d = dict(ix)
        d.pop("v", None)
        d.pop("ns", None)
        if d.get("name") == "_id_":
            continue
        if options.get("timeseries") and d.get("name", "").startswith("_ts_"):
            continue
        d["key"] = list(d["key"].items())
        out.append(d)
    return out


# =====================================================================
# dump
# =====================================================================
def dump(uri: str, dbname: str, dest: Path, stamp: str | None = None) -> Path:
    client = MongoClient(uri)
    db = client[dbname]
    server = client.server_info().get("version", "?")

    stamp = stamp or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out = dest / f"{dbname}-{stamp}"
    # Write to a .partial directory and rename at the end, so an interrupted run
    # can never leave something that looks like a complete backup.
    staging = dest / f"{dbname}-{stamp}.partial"
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)

    entries = []
    for info in _user_collections(db):
        name = info["name"]
        options = info.get("options", {}) or {}
        path = staging / f"{name}.bson.gz"
        digest = hashlib.sha256()
        count = 0
        with gzip.open(path, "wb") as fh:
            for doc in db[name].find():
                blob = bson.encode(doc)
                fh.write(blob)
                digest.update(blob)
                count += 1
        entries.append({
            "collection": name,
            "count": count,
            "sha256": digest.hexdigest(),
            "bytes": path.stat().st_size,
            "options": json.loads(bson.json_util.dumps(options)),
            "indexes": json.loads(bson.json_util.dumps(_indexes_for(db, name, options))),
            "timeseries": bool(options.get("timeseries")),
        })
        _log(f"  {name:<26} {count:>9,} docs  {path.stat().st_size / 1e6:6.1f} MB")

    manifest = {
        "format": FORMAT,
        "database": dbname,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "server_version": server,
        "collections": entries,
        "total_documents": sum(e["count"] for e in entries),
    }
    (staging / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    if out.exists():
        shutil.rmtree(out)
    staging.rename(out)
    client.close()
    _log(f"dump complete: {out}  ({manifest['total_documents']:,} documents)")
    return out


# =====================================================================
# verify
# =====================================================================
def verify(path: Path) -> bool:
    """Re-read every file and re-derive its checksum. A backup that has never
    been read is a hypothesis, not a backup."""
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("format") != FORMAT:
        _log(f"FAIL unknown format {manifest.get('format')!r} — not ours")
        return False

    ok = True
    for e in manifest["collections"]:
        f = path / f"{e['collection']}.bson.gz"
        if not f.exists():
            _log(f"FAIL {e['collection']}: file missing")
            ok = False
            continue
        digest = hashlib.sha256()
        count = 0
        with gzip.open(f, "rb") as fh:
            for doc in bson.decode_file_iter(fh):
                digest.update(bson.encode(doc))
                count += 1
        if count != e["count"]:
            _log(f"FAIL {e['collection']}: {count:,} docs, manifest says {e['count']:,}")
            ok = False
        elif digest.hexdigest() != e["sha256"]:
            _log(f"FAIL {e['collection']}: checksum mismatch")
            ok = False
        else:
            _log(f"  ok {e['collection']:<26} {count:>9,} docs")
    _log("verify: PASS" if ok else "verify: FAIL")
    return ok


# =====================================================================
# restore
# =====================================================================
def restore(uri: str, path: Path, into: str, force: bool = False,
            batch: int = 2000) -> None:
    manifest = json.loads((path / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("format") != FORMAT:
        raise SystemExit(f"refusing: {manifest.get('format')!r} is not {FORMAT}")

    client = MongoClient(uri)
    db = client[into]
    existing = [c for c in db.list_collection_names() if not c.startswith(SKIP_PREFIXES)]
    if existing and not force:
        raise SystemExit(
            f"refusing: {into} already has {len(existing)} collections. Restore "
            f"into a fresh database name, or pass --force to drop them.")

    for e in manifest["collections"]:
        name = e["collection"]
        if force and name in existing:
            db.drop_collection(name)
        options = bson.json_util.loads(json.dumps(e["options"]))
        # Time-series options are collection-creation-time only: inserting into an
        # auto-created ordinary collection would "work" and silently lose the
        # storage semantics, the TTL, and the bucketing this data depends on.
        create_kw = {}
        if options.get("timeseries"):
            create_kw["timeseries"] = options["timeseries"]
            if "expireAfterSeconds" in options:
                create_kw["expireAfterSeconds"] = options["expireAfterSeconds"]
        db.create_collection(name, **create_kw)

        buf, total = [], 0
        with gzip.open(path / f"{name}.bson.gz", "rb") as fh:
            for doc in bson.decode_file_iter(fh):
                buf.append(doc)
                if len(buf) >= batch:
                    db[name].insert_many(buf, ordered=False)
                    total += len(buf)
                    buf = []
        if buf:
            db[name].insert_many(buf, ordered=False)
            total += len(buf)

        for ix in e["indexes"]:
            spec = bson.json_util.loads(json.dumps(ix))
            keys = [(k, v) for k, v in spec.pop("key")]
            spec.pop("clustered", None)
            db[name].create_index(keys, **spec)

        _log(f"  restored {name:<26} {total:>9,} docs")
        if total != e["count"]:
            raise SystemExit(f"{name}: restored {total} of {e['count']} — aborting")

    client.close()
    _log(f"restore complete into {into}")


# =====================================================================
# prune
# =====================================================================
def prune(dest: Path, keep: int) -> None:
    """Newest `keep` kept. Retention is a real risk control: an unbounded backup
    directory eventually fills the disk and takes the live database with it."""
    dumps = sorted((p for p in dest.iterdir()
                    if p.is_dir() and (p / "manifest.json").exists()),
                   key=lambda p: p.name, reverse=True)
    for old in dumps[keep:]:
        shutil.rmtree(old)
        _log(f"  pruned {old.name}")
    _log(f"prune: {min(len(dumps), keep)} kept, {max(0, len(dumps) - keep)} removed")


# =====================================================================
# self-test
# =====================================================================
def self_test(uri: str) -> bool:
    """Round-trip a real dump through a scratch database and compare. This is
    the only thing standing between a hand-rolled format and the discovery,
    during an actual recovery, that it never worked."""
    import random

    tag = f"bmx_selftest_{random.randint(1000, 9999)}"
    src, dst = f"{tag}_src", f"{tag}_dst"
    tmp = Path(__file__).parent / ".selftest"
    client = MongoClient(uri)
    ok = False
    try:
        db = client[src]
        # A time-series collection with a TTL — the case a naive dump gets wrong.
        db.create_collection(
            "ts_feed",
            timeseries={"timeField": "event_time", "metaField": "meta",
                        "granularity": "minutes"},
            expireAfterSeconds=3600,
        )
        now = datetime.now(timezone.utc)
        db["ts_feed"].insert_many([
            {"event_time": now, "meta": {"dataset_id": "x"}, "close": float(i)}
            for i in range(500)
        ])
        db["ts_feed"].create_index([("meta.dataset_id", 1), ("event_time", 1)])
        db["plain"].insert_many([{"k": i, "v": f"row-{i}"} for i in range(300)])
        db["plain"].create_index([("k", 1)], unique=True)

        out = dump(uri, src, tmp, stamp="selftest")
        if not verify(out):
            return False
        restore(uri, out, dst)

        rdb = client[dst]
        for coll, n in (("ts_feed", 500), ("plain", 300)):
            got = rdb[coll].count_documents({})
            if got != n:
                _log(f"FAIL {coll}: {got} != {n}")
                return False

        info = next(c for c in rdb.list_collections() if c["name"] == "ts_feed")
        opts = info.get("options", {})
        if not opts.get("timeseries"):
            _log("FAIL ts_feed came back as an ordinary collection")
            return False
        if opts["timeseries"].get("timeField") != "event_time":
            _log("FAIL ts_feed timeField not preserved")
            return False
        if opts.get("expireAfterSeconds") != 3600:
            _log(f"FAIL ts_feed TTL not preserved: {opts.get('expireAfterSeconds')}")
            return False
        names = {i["name"] for i in rdb["plain"].list_indexes()}
        if not any(n != "_id_" for n in names):
            _log("FAIL plain: indexes not restored")
            return False
        if not any(i.get("unique") for i in rdb["plain"].list_indexes()):
            _log("FAIL plain: unique constraint not restored")
            return False

        _log("self-test: PASS — measurements, time-series options, TTL and indexes all survived")
        ok = True
        return True
    finally:
        client.drop_database(src)
        client.drop_database(dst)
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)
        client.close()
        if not ok:
            _log("self-test: FAIL")


def main() -> int:
    ap = argparse.ArgumentParser(description="BOTMAXIMUS database backup/restore")
    ap.add_argument("--uri", default=DEFAULT_URI)
    sub = ap.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("dump", help="write a verified backup")
    d.add_argument("--dest", required=True, type=Path)
    d.add_argument("--db", default=DEFAULT_DB)
    d.add_argument("--keep", type=int, default=0, help="prune to N newest after dumping")
    d.add_argument("--no-verify", action="store_true")

    v = sub.add_parser("verify", help="re-read a backup and check every checksum")
    v.add_argument("--path", required=True, type=Path)

    r = sub.add_parser("restore", help="restore a backup into a database")
    r.add_argument("--path", required=True, type=Path)
    r.add_argument("--into", required=True)
    r.add_argument("--force", action="store_true", help="drop conflicting collections first")

    p = sub.add_parser("prune", help="keep only the N newest backups")
    p.add_argument("--dest", required=True, type=Path)
    p.add_argument("--keep", type=int, default=7)

    sub.add_parser("self-test", help="round-trip a dump through a scratch database")

    a = ap.parse_args()
    if a.cmd == "dump":
        out = dump(a.uri, a.db, a.dest)
        # Verify by default: an unverified backup is an assumption. A dump that
        # cannot be read back is worse than no dump, because it is trusted.
        if not a.no_verify and not verify(out):
            return 1
        if a.keep:
            prune(a.dest, a.keep)
        return 0
    if a.cmd == "verify":
        return 0 if verify(a.path) else 1
    if a.cmd == "restore":
        restore(a.uri, a.path, a.into, a.force)
        return 0
    if a.cmd == "prune":
        prune(a.dest, a.keep)
        return 0
    if a.cmd == "self-test":
        return 0 if self_test(a.uri) else 1
    return 1


if __name__ == "__main__":
    sys.exit(main())
