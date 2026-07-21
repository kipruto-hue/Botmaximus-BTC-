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
        }

    @property
    def freshness_ms(self) -> float | None:
        if self.ingest_time is None:
            return None
        return (self.ingest_time - self.event_time).total_seconds() * 1000
