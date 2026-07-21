"""§9 instrumentation: rolling per-stage latency percentiles, end-to-end
freshness per dataset, counters, live price. Single in-memory source the
API reads; everything the dashboard shows comes from here.
"""
from __future__ import annotations

import time
from collections import deque
from datetime import datetime, timezone

from botmaximus.config import settings

STAGES = ("gather", "parse", "quality", "store")

BUDGETS_MS = {
    "btc_price_tick": settings.budget_price_tick_ms,
    "btc_ohlcv_1m": settings.budget_ohlcv_1m_ms,
}

FEED_LABELS = {
    "btc_price_tick": "BTC ticks",
    "btc_ohlcv_1m": "BTC OHLCV 1m",
}


def _pct(values: list[float], p: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(len(s) - 1, max(0, round(p / 100 * (len(s) - 1))))
    return s[idx]


class Telemetry:
    def __init__(self) -> None:
        self.started_at = time.time()
        self._stage: dict[tuple[str, str], deque[float]] = {}
        self._e2e: dict[str, deque[float]] = {}
        self._last_ingest: dict[str, datetime] = {}
        self._last_event: dict[str, datetime] = {}
        self.counts = {"stored": 0, "quarantined": 0, "gathered": 0}
        self.last_price: float | None = None
        self.last_price_time: datetime | None = None
        self.ws_connected = False
        self.ws_reconnects = 0

    # ---- recording ----
    def record_stage(self, dataset_id: str, stage: str, ms: float) -> None:
        self._stage.setdefault((dataset_id, stage), deque(maxlen=500)).append(ms)

    def record_stored(self, dataset_id: str, ingest_time: datetime, event_time: datetime) -> None:
        self.counts["stored"] += 1
        self._last_ingest[dataset_id] = ingest_time
        self._last_event[dataset_id] = event_time
        e2e = (ingest_time - event_time).total_seconds() * 1000
        self._e2e.setdefault(dataset_id, deque(maxlen=500)).append(e2e)

    def record_quarantined(self) -> None:
        self.counts["quarantined"] += 1

    def set_price(self, price: float, event_time: datetime) -> None:
        self.last_price = price
        self.last_price_time = event_time

    # ---- reading ----
    def freshness_ms(self, dataset_id: str) -> float | None:
        """Age of the newest stored record's event vs now — grows while a feed is silent."""
        last = self._last_event.get(dataset_id)
        if last is None:
            return None
        return (datetime.now(timezone.utc) - last).total_seconds() * 1000

    def snapshot(self) -> dict:
        feeds = []
        for ds, label in FEED_LABELS.items():
            stage_p50 = {
                st: round(_pct(list(self._stage.get((ds, st), [])), 50), 1) for st in STAGES
            }
            fresh = self.freshness_ms(ds)
            feeds.append({
                "dataset_id": ds,
                "name": label,
                "g": stage_p50["gather"],
                "p": stage_p50["parse"],
                "q": stage_p50["quality"],
                "s": stage_p50["store"],
                "p95_ms": round(_pct(list(self._e2e.get(ds, [])), 95), 1),
                "fresh": None if fresh is None else round(fresh),
                "budget": BUDGETS_MS[ds],
                "stale": fresh is not None and fresh > BUDGETS_MS[ds],
                "records": len(self._e2e.get(ds, [])),
            })
        all_e2e = [v for d in self._e2e.values() for v in d]
        return {
            "running": True,
            "uptime_s": round(time.time() - self.started_at),
            "ws_connected": self.ws_connected,
            "ws_reconnects": self.ws_reconnects,
            "price": self.last_price,
            "price_time": self.last_price_time.isoformat() if self.last_price_time else None,
            "counts": dict(self.counts),
            "e2e_p95_ms": round(_pct(all_e2e, 95), 1),
            "stale_feeds": sum(1 for f in feeds if f["stale"]),
            "feeds": feeds,
        }


telemetry = Telemetry()
