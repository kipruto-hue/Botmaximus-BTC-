"""Canonical record envelope (§3.1). Every record in the system takes this shape."""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def from_epoch_ms(ms: int) -> datetime:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc)


@dataclass
class Envelope:
    dataset_id: str
    source: str
    symbol: str
    event_time: datetime            # when the fact became true at the venue
    collection_time: datetime       # when we received it
    payload: dict[str, Any]
    ingest_time: datetime | None = None   # stamped by the writer at store time
    quality_flags: list[str] = field(default_factory=list)
    quality_ok: bool = True
    reaction_ref: Any = None
    stage_latency_ms: dict[str, float] = field(default_factory=dict)
    quarantine_reasons: list[str] = field(default_factory=list)
    backfill: bool = False  # fetched via REST to heal a gap, not received live

    def __post_init__(self) -> None:
        for name in ("event_time", "collection_time", "ingest_time"):
            t = getattr(self, name)
            if t is not None and t.tzinfo is None:
                raise ValueError(f"{name} must be timezone-aware UTC")

    def to_doc(self) -> dict[str, Any]:
        """Mongo document. `meta` is the time-series metaField; `event_time` the timeField."""
        return {
            "event_time": self.event_time,
            "meta": {"dataset_id": self.dataset_id, "source": self.source, "symbol": self.symbol},
            "collection_time": self.collection_time,
            "ingest_time": self.ingest_time,
            "payload": self.payload,
            "quality_flags": self.quality_flags,
            "quality_ok": self.quality_ok,
            "reaction_ref": self.reaction_ref,
            "stage_latency_ms": self.stage_latency_ms,
            "backfill": self.backfill,
        }

    def to_record(self, config_hash: str | None = None):
        """Convert to the §2 storage envelope.

        The two models disagree about one thing, and the mapping is where that
        is reconciled. This pipeline uses `quality_flags` for *advisory* marks
        on records that passed — `single_source` is appended to every record
        the gate ever sees — while Storage v2.0 §2 defines `quality_flags` as
        the names of checks a record FAILED, empty when clean.

        Read literally, every record would carry a flag, and §5 would send the
        entire feed to quarantine. So:

        - a record that failed carries all of its reasons in `quality_flags`
          and goes to quarantine;
        - a record that passed carries its advisory marks in `annotations`
          and reaches production with `quality_flags` empty.

        Nothing is discarded; the distinction between "this failed" and "this
        is worth knowing" is preserved instead of collapsed.
        """
        from botmaximus.storage.envelope import Record

        failed = bool(self.quarantine_reasons) or not self.quality_ok
        flags = tuple(self.quarantine_reasons) + tuple(self.quality_flags)
        return Record.create(
            dataset_id=self.dataset_id,
            source=self.source,
            event_time=self.event_time,
            payload=self.payload,
            symbol=self.symbol,
            collection_time=self.collection_time,
            ingest_time=self.ingest_time or utcnow(),
            config_hash=config_hash,
            quality_ok=not failed,
            quality_flags=flags if failed else (),
            annotations=() if failed else tuple(self.quality_flags),
            stage_latency_ms=dict(self.stage_latency_ms),
        )

    @property
    def freshness_ms(self) -> float | None:
        if self.ingest_time is None:
            return None
        return (self.ingest_time - self.event_time).total_seconds() * 1000
