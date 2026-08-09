r"""LLM generation and scrutiny provenance (Storage v2.0 §3.G).

Structured fields go to Postgres (`generations`, `scrutiny_events`); the **full
prompt text and full response text** go to Parquet as immutable blobs. Large,
rarely read, and they must survive forever — which is exactly the shape the
archive is for and exactly the shape a relational row is not.

## Why this module exists at all

The generator used to write provenance to `data/generations/<id>.json`. §14 is
explicit: *"Do not write to local JSON/CSV files as a stand-in for a proper
store. If it isn't Postgres or Parquet, it does not get written."* That is not
pedantry. A local file lives on one box, is not in any backup, is not in the
manifest, is not checksummed, and disappears when the VPS is rebuilt — while
§3.G requires the opposite: *"a candidate whose provenance blob is missing must
not enter validation."* A provenance store that quietly evaporates turns that
guard into a coin flip.

## The blob and the row are written together

The row carries the pointer; the blob carries the text. Writing the row without
the blob would produce a candidate that passes the existence check against
nothing, so the blob is written **first** and the row records the key it
actually landed at. If the archive write fails, no row is written, and the
candidate is refused at validation — which is the correct outcome: unauditable
means unusable.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import pyarrow as pa

from botmaximus.storage import postgres
from botmaximus.storage import records as store

log = logging.getLogger(__name__)

UTC = timezone.utc


class ProvenanceUnavailable(RuntimeError):
    """The provenance blob could not be written or cannot be found.

    A candidate carrying this is refused at validation (§3.G) rather than run
    with a gap in its audit trail.
    """


@dataclass(frozen=True)
class ProvenanceRef:
    """What a proposal carries instead of a filesystem path."""
    generation_id: str
    blob_key: str

    def __str__(self) -> str:
        return f"{self.generation_id}@{self.blob_key}"


def blob_key(kind: str, ident: str, at: datetime | None = None) -> str:
    """Day-partitioned, same as every other archive object (§1.B)."""
    at = at or datetime.now(UTC)
    return (f"provenance/{kind}/year={at:%Y}/month={at:%m}/day={at:%d}"
            f"/{ident}.parquet")


def _blob_table(fields: dict[str, str]) -> pa.Table:
    """One row per text field. Columnar and opaque on purpose: prompts and
    responses have no fixed schema, and giving them one would make every prompt
    revision a schema migration."""
    return pa.table({
        "field": pa.array(list(fields), pa.string()),
        "text": pa.array([fields[k] for k in fields], pa.string()),
    })


async def record_generation(*, strategy_id: str, proposer: str,
                            brief: dict, raw_response: dict,
                            model_id: str | None = None,
                            prompt_version: str | None = None,
                            system_prompt_hash: str | None = None,
                            temperature: float | None = None,
                            top_p: float | None = None,
                            max_output_tokens: int | None = None,
                            seed: int | None = None,
                            parent_id: str | None = None,
                            lineage_depth: int = 0,
                            feature_registry_hash: str | None = None,
                            dsl_schema_hash: str | None = None,
                            generation_id: str | None = None,
                            at: datetime | None = None) -> ProvenanceRef:
    """Persist one generation. Blob first, then the row that points at it."""
    generation_id = generation_id or uuid.uuid4().hex
    at = at or datetime.now(UTC)
    key = blob_key("generation", generation_id, at)

    table = _blob_table({
        "brief": json.dumps(brief, sort_keys=True, default=str, indent=1),
        "raw_response": json.dumps(raw_response, default=str, indent=1),
    })
    try:
        written = store.archive().write_blob(
            key, table, dataset_id="generation_provenance",
            partition=key.rsplit("/", 1)[0])
        await store._record_manifest(written)
    except Exception as e:                              # noqa: BLE001
        raise ProvenanceUnavailable(
            f"could not archive the provenance blob for {strategy_id}: {e}. "
            f"Refusing to record a generation whose prompt and response are "
            f"not recoverable — §3.G requires the blob before validation.") from e

    await postgres.execute(
        "INSERT INTO generations (generation_id, strategy_id, at, proposer, "
        " model_id, prompt_version, system_prompt_hash, context_hash, "
        " temperature, top_p, max_output_tokens, seed, parent_id, "
        " lineage_depth, feature_registry_hash, dsl_schema_hash, "
        " provenance_blob_key) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (generation_id) DO NOTHING",
        (_as_uuid(generation_id), strategy_id, at, proposer, model_id,
         prompt_version, system_prompt_hash, brief_hash(brief), temperature,
         top_p, max_output_tokens, seed, parent_id, lineage_depth,
         feature_registry_hash, dsl_schema_hash, key))
    return ProvenanceRef(generation_id, key)


async def record_scrutiny_blob(intent_id: str, prompt: str, response: str,
                               at: datetime | None = None) -> str:
    """Full prompt and response for one scrutiny verdict (§3.G)."""
    key = blob_key("scrutiny", intent_id, at)
    written = store.archive().write_blob(
        key, _blob_table({"prompt": prompt, "response": response}),
        dataset_id="scrutiny_provenance", partition=key.rsplit("/", 1)[0])
    await store._record_manifest(written)
    await postgres.execute(
        "UPDATE scrutiny_events SET prompt_blob_key = %s WHERE intent_id = %s",
        (key, intent_id))
    return key


def blob_exists(key: str | None) -> bool:
    """Is the provenance actually there?

    This replaces `Path.exists()`. The distinction matters: a filesystem check
    passes on a box that happens to still have the file, while this asks the
    store that is backed up, mirrored and checksummed.
    """
    if not key:
        return False
    try:
        a = store.archive()
        return a.backend.exists(a.archive_bucket, key)
    except Exception:                                   # noqa: BLE001
        return False


async def read_blob(key: str) -> dict[str, str]:
    """Recover the original prompt and response. Audit path, rarely used."""
    table = store.archive().read(key)
    d = table.to_pydict()
    return dict(zip(d["field"], d["text"]))


def brief_hash(brief: dict) -> str:
    return hashlib.sha256(
        json.dumps(brief, sort_keys=True, default=str).encode()).hexdigest()[:16]


def _as_uuid(ident: str) -> str:
    """`generations.generation_id` is a uuid column; the generator mints hex."""
    try:
        return str(uuid.UUID(ident))
    except ValueError:
        return str(uuid.uuid5(uuid.NAMESPACE_OID, ident))
