r"""Parquet archive — the immutable permanent record (Storage v2.0 §1.B).

Day-partitioned, envelope-carrying, append-only. Every file written is indexed
in the Postgres `storage_manifest` with a checksum, because silent bit rot on
object storage is a real thing and an archive nobody verifies is a hope.

## Two buckets that never mix

`archive` holds what passed the quality gate. `quarantine` holds what did not.
No hot path, no backtester and no strategy training ever reads the second one,
and there is deliberately **no rehabilitation path** — `write_quarantine` has no
counterpart that moves records back. If a record was bad enough to quarantine,
the decision to "clean it up and reintroduce it" is exactly the decision that
poisons a dataset years later, when nobody remembers which rows were repaired.

## Backends

`LocalBackend` writes to disk; `ObjectStorageBackend` writes to Vultr Object
Storage through the same three methods. Callers never learn which is in use —
that is what makes the local test suite meaningful evidence about production.

Writes are atomic: a temporary file is renamed into place, so an interrupted
write can never leave a partial Parquet file that reads as a short partition.
"""
from __future__ import annotations

import hashlib
import io
import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from botmaximus.storage.envelope import Record

log = logging.getLogger(__name__)

ARCHIVE = "archive"
QUARANTINE = "quarantine"


@dataclass(frozen=True)
class WrittenFile:
    bucket: str
    key: str
    rows: int
    bytes: int
    sha256: str
    dataset_id: str
    partition: str
    written_at: datetime


class StorageBackend:
    def put(self, bucket: str, key: str, data: bytes) -> None:
        raise NotImplementedError

    def get(self, bucket: str, key: str) -> bytes:
        raise NotImplementedError

    def exists(self, bucket: str, key: str) -> bool:
        raise NotImplementedError

    def list(self, bucket: str, prefix: str) -> list[str]:
        raise NotImplementedError


class LocalBackend(StorageBackend):
    """Filesystem backend. Used for tests and for a single-box deployment."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)

    def _path(self, bucket: str, key: str) -> Path:
        return self.root / bucket / key

    def put(self, bucket: str, key: str, data: bytes) -> None:
        p = self._path(bucket, key)
        p.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename: an interrupted write must never leave a partial
        # Parquet file, which would read as a short partition rather than fail.
        tmp = p.with_suffix(p.suffix + ".partial")
        tmp.write_bytes(data)
        tmp.replace(p)

    def get(self, bucket: str, key: str) -> bytes:
        return self._path(bucket, key).read_bytes()

    def exists(self, bucket: str, key: str) -> bool:
        return self._path(bucket, key).exists()

    def list(self, bucket: str, prefix: str) -> list[str]:
        base = self._path(bucket, prefix)
        if not base.exists():
            return []
        root = self.root / bucket
        return sorted(str(p.relative_to(root)).replace("\\", "/")
                      for p in base.rglob("*.parquet"))


class ObjectStorageBackend(StorageBackend):
    """Vultr Object Storage (S3-compatible).

    Constructed only when credentials are configured; there is no anonymous or
    best-effort mode, because a silent no-op write is indistinguishable from a
    successful one until the day you need the data.
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str,
                 region: str = "jp-tyo") -> None:
        try:
            import boto3
        except ImportError as e:                    # noqa: BLE001
            raise RuntimeError(
                "boto3 is required for the object-storage backend "
                "(`pip install boto3`)") from e
        self._s3 = boto3.client(
            "s3", endpoint_url=endpoint, aws_access_key_id=access_key,
            aws_secret_access_key=secret_key, region_name=region)

    def put(self, bucket: str, key: str, data: bytes) -> None:
        self._s3.put_object(Bucket=bucket, Key=key, Body=data)

    def get(self, bucket: str, key: str) -> bytes:
        return self._s3.get_object(Bucket=bucket, Key=key)["Body"].read()

    def exists(self, bucket: str, key: str) -> bool:
        from botocore.exceptions import ClientError
        try:
            self._s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False

    def list(self, bucket: str, prefix: str) -> list[str]:
        out, token = [], None
        while True:
            kw = {"Bucket": bucket, "Prefix": prefix}
            if token:
                kw["ContinuationToken"] = token
            resp = self._s3.list_objects_v2(**kw)
            out += [o["Key"] for o in resp.get("Contents", [])]
            if not resp.get("IsTruncated"):
                return sorted(out)
            token = resp.get("NextContinuationToken")


def records_to_table(records: list[Record]) -> pa.Table:
    """Envelope columns flattened; payload kept as a JSON string.

    The payload stays opaque on purpose: datasets have different shapes, and a
    per-dataset Arrow schema would make adding a feed a schema migration. The
    envelope is what queries filter on, and it is fully typed.
    """
    import json

    rows = [r.to_row() for r in records]
    return pa.table({
        "record_id": pa.array([r["record_id"] for r in rows], pa.string()),
        "dataset_id": pa.array([r["dataset_id"] for r in rows], pa.string()),
        "source": pa.array([r["source"] for r in rows], pa.string()),
        "symbol": pa.array([r["symbol"] for r in rows], pa.string()),
        "event_time": pa.array([r["event_time"] for r in rows],
                               pa.timestamp("us", tz="UTC")),
        "collection_time": pa.array([r["collection_time"] for r in rows],
                                    pa.timestamp("us", tz="UTC")),
        "ingest_time": pa.array([r["ingest_time"] for r in rows],
                                pa.timestamp("us", tz="UTC")),
        "valid_from_sys": pa.array([r["valid_from_sys"] for r in rows],
                                   pa.timestamp("us", tz="UTC")),
        "valid_to_sys": pa.array([r["valid_to_sys"] for r in rows],
                                 pa.timestamp("us", tz="UTC")),
        "supersedes": pa.array([r["supersedes"] for r in rows], pa.string()),
        "correction_reason": pa.array([r["correction_reason"] for r in rows],
                                      pa.string()),
        "producer": pa.array([r["producer"] for r in rows], pa.string()),
        "code_version": pa.array([r["code_version"] for r in rows], pa.string()),
        "schema_version": pa.array([r["schema_version"] for r in rows], pa.int32()),
        "config_hash": pa.array([r["config_hash"] for r in rows], pa.string()),
        "quality_flags": pa.array([",".join(r["quality_flags"]) for r in rows],
                                  pa.string()),
        "quality_ok": pa.array([r["quality_ok"] for r in rows], pa.bool_()),
        "quality_gate_version": pa.array([r["quality_gate_version"] for r in rows],
                                         pa.int32()),
        "payload": pa.array([json.dumps(r["payload"], default=str) for r in rows],
                            pa.string()),
    })


class Archive:
    def __init__(self, backend: StorageBackend, archive_bucket: str,
                 quarantine_bucket: str) -> None:
        self.backend = backend
        self.archive_bucket = archive_bucket
        self.quarantine_bucket = quarantine_bucket

    def write(self, records: list[Record], *, part: str = "000") -> WrittenFile:
        """Append a Parquet file to the archive. Clean records only."""
        bad = [r for r in records if not r.quality_ok]
        if bad:
            raise ValueError(
                f"{len(bad)} record(s) with quality_ok=False were handed to the "
                f"production archive. Failed records go to quarantine and never "
                f"enter the store the backtester reads.")
        return self._write(self.archive_bucket, records, part)

    def write_quarantine(self, records: list[Record], *,
                         part: str = "000") -> WrittenFile:
        """Bad data, kept for forensics and gate tuning.

        There is deliberately no `promote_from_quarantine`. Rehabilitating bad
        rows is how a dataset becomes untrustworthy years later, when nobody
        remembers which ones were repaired or why.
        """
        return self._write(self.quarantine_bucket, records, part)

    def _write(self, bucket: str, records: list[Record],
               part: str) -> WrittenFile:
        if not records:
            raise ValueError("refusing to write an empty parquet file — an "
                             "empty partition is indistinguishable from a "
                             "missing one at read time")
        partitions = {r.partition_path() for r in records}
        if len(partitions) != 1:
            raise ValueError(
                f"records span {len(partitions)} partitions; write one "
                f"day/dataset per file so a daily partition is atomic")
        partition = partitions.pop()

        table = records_to_table(records)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="zstd")
        data = buf.getvalue()

        key = f"{partition}/part-{part}.parquet"
        self.backend.put(bucket, key, data)
        return WrittenFile(
            bucket=bucket, key=key, rows=len(records), bytes=len(data),
            sha256=hashlib.sha256(data).hexdigest(),
            dataset_id=records[0].dataset_id, partition=partition,
            written_at=datetime.now(timezone.utc))

    def read(self, key: str, bucket: str | None = None) -> pa.Table:
        return pq.read_table(io.BytesIO(
            self.backend.get(bucket or self.archive_bucket, key)))

    def verify(self, written: WrittenFile) -> bool:
        """Re-read and re-hash. An archive that has never been read back is a
        hypothesis, not a backup — the same reasoning as the database dumps."""
        try:
            data = self.backend.get(written.bucket, written.key)
        except Exception as e:                      # noqa: BLE001
            log.error("archive verify: cannot read %s: %s", written.key, e)
            return False
        ok = hashlib.sha256(data).hexdigest() == written.sha256
        if not ok:
            log.error("archive verify: CHECKSUM MISMATCH on %s", written.key)
        return ok
