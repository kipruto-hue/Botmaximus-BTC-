r"""The canonical bitemporal record envelope (Storage v2.0 §2).

Every record in either store carries these fields. The payload is
dataset-specific; the envelope never is.

## Why two time axes rather than one

`event_time` is when the fact was true in the world. `valid_from_sys` is when we
came to believe it. Keeping them separate is what kills the silent-revision
failure mode: when a venue reissues a funding rate for a settlement that already
happened, the correction lands with the **original** event_time and a **new**
system time. A backtest run last April asks "what did we know in April?" and
gets April's answer, not today's improved one.

Collapse those axes into one timestamp and every historical backtest quietly
becomes a lie — not because the code is wrong, but because the data now
describes a world that only exists in hindsight.

## The three invariants

1. **Supersession chains are append-only.** A correction is a *new* record
   pointing at the one it replaces; the replaced record gets `valid_to_sys` set
   and is never deleted. A record's history is the chain ending at it.
2. **`event_time` comes from the venue.** Never the local clock. An unstamped
   record is quarantined rather than stamped locally — a guessed event time is
   indistinguishable from a real one once written, which makes it worse than a
   missing record.
3. **Corrections are records, not edits.** There is no update path in this
   module, and `supersede()` returns a new envelope rather than mutating.

## quality_flags carries names, never margins

Same rule as the LLM prompt wall: a check name is diagnostic, a margin is a
gradient someone can climb. These records feed generator digests eventually,
and the coarsening has to hold at the point of writing, not at the point of
reading.
"""
from __future__ import annotations

import hashlib
import json
import os
import socket
import time
import uuid
from dataclasses import asdict, dataclass, field, replace
from datetime import datetime, timezone
from typing import Any

SCHEMA_VERSION = 1


def uuid7() -> str:
    """Time-ordered UUID: sortable by creation and globally unique.

    Sortability matters because these become Parquet row keys and Postgres
    primary keys; a random v4 scatters writes across the index and turns an
    append-only workload into a random-write one.
    """
    ms = int(time.time() * 1000)
    rand = uuid.uuid4().int & ((1 << 74) - 1)
    val = (ms << 80) | (0x7 << 76) | (rand & ((1 << 76) - 1))
    return str(uuid.UUID(int=val & ((1 << 128) - 1)))


def _code_version() -> str:
    """git SHA of the writer, resolved once. Unknown is recorded as unknown —
    never as a plausible-looking placeholder."""
    global _CODE_VERSION
    if _CODE_VERSION is not None:
        return _CODE_VERSION
    try:
        import subprocess
        from pathlib import Path
        out = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=Path(__file__).resolve().parents[3], capture_output=True,
            text=True, timeout=5)
        _CODE_VERSION = out.stdout.strip() or "unknown"
    except Exception:                               # noqa: BLE001
        _CODE_VERSION = "unknown"
    return _CODE_VERSION


_CODE_VERSION: str | None = None


def producer() -> str:
    return f"{socket.gethostname()}/{os.getpid()}"


class EnvelopeError(ValueError):
    """Raised when a record would violate an envelope invariant."""


@dataclass(frozen=True)
class Record:
    dataset_id: str
    source: str
    event_time: datetime
    payload: dict[str, Any]

    symbol: str | None = "BTCUSDT"
    record_id: str = field(default_factory=uuid7)
    collection_time: datetime | None = None
    ingest_time: datetime | None = None

    # system-time versioning
    valid_from_sys: datetime | None = None
    valid_to_sys: datetime | None = None
    supersedes: str | None = None
    correction_reason: str | None = None

    # lineage
    producer: str = field(default_factory=producer)
    code_version: str = field(default_factory=_code_version)
    schema_version: int = SCHEMA_VERSION
    config_hash: str | None = None

    # quality
    quality_flags: tuple[str, ...] = ()
    quality_ok: bool = False
    quality_gate_version: int = 1

    stage_latency_ms: dict[str, float] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for name in ("event_time", "collection_time", "ingest_time",
                     "valid_from_sys", "valid_to_sys"):
            v = getattr(self, name)
            if v is not None and v.tzinfo is None:
                raise EnvelopeError(
                    f"{name} is naive. Every timestamp is UTC-aware: a naive "
                    f"datetime means something different on the Tokyo VPS than "
                    f"on this desktop, and the difference is invisible.")
        for flag in self.quality_flags:
            if any(c.isdigit() or c in "<>" for c in str(flag)):
                raise EnvelopeError(
                    f"quality_flags carries a margin ({flag!r}). Check NAMES "
                    f"only — a margin is a gradient, and these records reach "
                    f"generator digests.")
        if self.quality_ok and self.quality_flags:
            raise EnvelopeError(
                "quality_ok is True but quality_flags is non-empty. A record "
                "cannot be both clean and flagged; that ambiguity is how bad "
                "data reaches production stores.")

    # ---- construction ----
    @classmethod
    def create(cls, dataset_id: str, source: str, event_time: datetime,
               payload: dict, *, now: datetime | None = None, **kw) -> "Record":
        now = now or datetime.now(timezone.utc)
        return cls(dataset_id=dataset_id, source=source, event_time=event_time,
                   payload=payload,
                   collection_time=kw.pop("collection_time", now),
                   ingest_time=kw.pop("ingest_time", now),
                   valid_from_sys=kw.pop("valid_from_sys", now), **kw)

    def supersede(self, payload: dict, reason: str,
                  now: datetime | None = None) -> tuple["Record", "Record"]:
        """A correction. Returns (closed_old, new_record).

        The old record is *closed*, not changed: its `valid_to_sys` is stamped
        so an as-of query before that instant still returns it. The new record
        keeps the original `event_time` — the world did not change, only our
        knowledge of it.
        """
        now = now or datetime.now(timezone.utc)
        if self.valid_to_sys is not None:
            raise EnvelopeError(
                f"record {self.record_id} was already superseded at "
                f"{self.valid_to_sys.isoformat()}; correcting a closed record "
                f"would fork the chain and make as-of queries ambiguous.")
        closed = replace(self, valid_to_sys=now)
        new = Record.create(
            dataset_id=self.dataset_id, source=self.source,
            event_time=self.event_time, payload=payload, now=now,
            symbol=self.symbol, supersedes=self.record_id,
            correction_reason=reason, config_hash=self.config_hash,
            quality_flags=self.quality_flags, quality_ok=self.quality_ok,
            quality_gate_version=self.quality_gate_version)
        return closed, new

    # ---- serialisation ----
    def to_row(self) -> dict:
        d = asdict(self)
        d["quality_flags"] = list(self.quality_flags)
        return d

    def payload_hash(self) -> str:
        blob = json.dumps(self.payload, sort_keys=True, default=str).encode()
        return hashlib.sha256(blob).hexdigest()[:16]

    def partition_path(self) -> str:
        """`year=/month=/day=/dataset=` — UTC date of the EVENT, not of ingest.

        Partitioning by ingest would scatter a backfill of last year's candles
        across today's partition, and a point-in-time read would have to scan
        everything to find them.
        """
        t = self.event_time.astimezone(timezone.utc)
        return (f"year={t.year:04d}/month={t.month:02d}/day={t.day:02d}"
                f"/dataset={self.dataset_id}")


def visible_at(records: list[Record], as_of: datetime) -> list[Record]:
    """The truth the system held at `as_of` (§6).

    `valid_from_sys <= as_of < valid_to_sys`. This is the whole point of the
    system-time axis: a backtest pinned to a past instant sees the data as it
    was believed then, including the errors, which is the only honest way to
    ask whether a strategy would have worked.
    """
    out = []
    for r in records:
        if r.valid_from_sys is None or r.valid_from_sys > as_of:
            continue
        if r.valid_to_sys is not None and r.valid_to_sys <= as_of:
            continue
        out.append(r)
    return out
