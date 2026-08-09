r"""Feature-set catalog and vector storage (Storage v2.0 §3.D).

Postgres holds the metadata — which features exist, at which version, produced
by which code. Parquet holds the computed vectors, day-partitioned, **one
directory per feature-set version**.

## Versions never overwrite each other

§3.D: *"Never recomputed on top of prior files — a schema change is a new
version directory, both versions coexist until the operator retires the old
one."*

That is not tidiness, it is the difference between a reproducible backtest and
a lie. A backtest recorded that it ran against feature-set v3. If v3's files
are later recomputed in place — a redefined indicator, a bug fix, a different
warmup — the run's recorded `as_of` still resolves, the config hash still
matches, and the numbers it produced can no longer be reproduced by anything.
The failure leaves no trace at all.

So `write_vectors` refuses to overwrite an existing day for an existing
version, and registering a changed definition under an existing version number
raises. Retiring a version sets `retired_at`; it never deletes.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass
from datetime import date, datetime, timezone

import pyarrow as pa

from botmaximus.storage import postgres
from botmaximus.storage import records as store

log = logging.getLogger(__name__)

UTC = timezone.utc


class FeatureVersionConflict(RuntimeError):
    """A definition change was offered under an existing version number, or a
    day's vectors already exist for that version."""


@dataclass(frozen=True)
class FeatureSet:
    version: int
    registry_hash: str
    code_version: str
    definition: dict
    parquet_prefix: str
    retired_at: datetime | None = None


def registry_hash(registry: dict | None = None) -> str:
    """Stable digest of the feature registry.

    Sorted before hashing: a dict has no guaranteed order across processes, and
    a hash that changes on restart would make every boot look like a new
    feature-set version.
    """
    from botmaximus.features.registry import FEATURE_REGISTRY
    reg = registry if registry is not None else FEATURE_REGISTRY
    blob = json.dumps(sorted(reg), sort_keys=True).encode()
    return hashlib.sha256(blob).hexdigest()[:16]


def prefix_for(version: int) -> str:
    return f"features/v{version}"


async def register(version: int, definition: dict,
                   code_version: str | None = None) -> FeatureSet:
    """Register a feature-set version. Idempotent for an identical definition.

    Offering a *different* definition under an existing version raises rather
    than updating — that is the in-place recompute §3.D forbids, arriving one
    layer up.
    """
    from botmaximus.storage.envelope import _code_version

    rhash = registry_hash()
    code_version = code_version or _code_version()
    existing = await postgres.fetchrow(
        "SELECT * FROM feature_sets WHERE feature_set_version = %s", (version,))
    if existing:
        if existing["registry_hash"] != rhash:
            raise FeatureVersionConflict(
                f"feature-set v{version} is already registered with registry "
                f"hash {existing['registry_hash']}, but the current registry "
                f"hashes to {rhash}. A changed definition is a NEW version "
                f"(§3.D) — recomputing v{version} in place would silently "
                f"change what every backtest that cites it measured.")
        return _as_set(existing)

    await postgres.execute(
        "INSERT INTO feature_sets (feature_set_version, registry_hash, "
        " code_version, definition, parquet_prefix) VALUES (%s,%s,%s,%s,%s)",
        (version, rhash, code_version, json.dumps(definition, default=str),
         prefix_for(version)))
    log.info("registered feature-set v%d (registry %s)", version, rhash)
    return FeatureSet(version, rhash, code_version, definition,
                      prefix_for(version))


async def current_version() -> int | None:
    """Highest non-retired version."""
    return await postgres.fetchval(
        "SELECT max(feature_set_version) AS v FROM feature_sets "
        "WHERE retired_at IS NULL")


async def retire(version: int) -> None:
    """Operator action. Marks the version retired; the files stay forever
    (§9 keeps features forever, and a retired version is still what some past
    backtest cites)."""
    await postgres.execute(
        "UPDATE feature_sets SET retired_at = now() "
        "WHERE feature_set_version = %s AND retired_at IS NULL", (version,))


def vector_key(version: int, day: date, name: str = "features") -> str:
    return (f"{prefix_for(version)}/year={day:%Y}/month={day:%m}"
            f"/day={day:%d}/{name}.parquet")


async def write_vectors(version: int, day: date, columns: dict[str, list],
                        *, overwrite: bool = False) -> str:
    """Write one day of computed feature vectors for one version.

    Refuses to overwrite by default. `overwrite=True` exists only for a
    same-day recompute before anything has cited the file — it is not a way to
    revise history, and a version whose day already exists should become a new
    version instead.
    """
    key = vector_key(version, day)
    archive = store.archive()
    if not overwrite and archive.backend.exists(archive.archive_bucket, key):
        raise FeatureVersionConflict(
            f"feature vectors already exist at {key}. Overwriting them would "
            f"change what every backtest citing feature-set v{version} "
            f"measured, with no trace. Register a new version instead (§3.D).")

    if not columns:
        raise ValueError("refusing to write an empty feature file — an empty "
                         "day is indistinguishable from a missing one at read "
                         "time")
    table = pa.table({k: pa.array(v) for k, v in columns.items()})
    written = archive.write_blob(key, table, dataset_id=f"features_v{version}",
                                partition=key.rsplit("/", 1)[0])
    await store._record_manifest(written)
    log.info("features v%d %s: %d row(s)", version, day, table.num_rows)
    return key


def read_vectors(version: int, day: date) -> pa.Table:
    return store.archive().read(vector_key(version, day))


async def catalog() -> list[dict]:
    rows = await postgres.fetch(
        "SELECT * FROM feature_sets ORDER BY feature_set_version")
    return [json.loads(json.dumps(dict(r), default=str)) for r in rows]


def _as_set(row) -> FeatureSet:
    return FeatureSet(row["feature_set_version"], row["registry_hash"],
                      row["code_version"], row["definition"],
                      row["parquet_prefix"], row["retired_at"])
