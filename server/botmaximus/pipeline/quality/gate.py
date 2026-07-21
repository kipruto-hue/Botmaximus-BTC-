"""Five-layer quality gate (§8). Mutates the envelope in place:

- hard failures (impossible quotes, lookahead violations) → quarantine_reasons,
  quality_ok=False → the writer routes to the quarantine collection
- disqualifying-but-storable issues (stale, phantom_suspect) → flag + quality_ok=False
- informational issues (illiquid_window, single_source) → flag only

A record with quality_ok=False must never feed a live decision (§1.3).
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from botmaximus.config import settings
from botmaximus.pipeline.envelope import Envelope
from botmaximus.pipeline.telemetry import BUDGETS_MS


class QualityGate:
    def __init__(self) -> None:
        self._prev_price: dict[str, float] = {}
        self._prev_event_time: dict[str, datetime] = {}

    def check(self, env: Envelope) -> Envelope:
        self._layer_timestamps(env)
        self._layer_sanity(env)
        self._layer_liquidity(env)
        self._layer_freshness(env)
        self._layer_source(env)

        if not env.quarantine_reasons:
            self._prev_price[env.dataset_id] = self._price_of(env)
            self._prev_event_time[env.dataset_id] = env.event_time
        return env

    @staticmethod
    def _price_of(env: Envelope) -> float:
        return env.payload.get("price") or env.payload.get("close") or 0.0

    def _layer_timestamps(self, env: Envelope) -> None:
        """Layer 4 (§8): lookahead protection is absolute — quarantine."""
        skew = timedelta(milliseconds=settings.clock_skew_tolerance_ms)
        if env.event_time > env.collection_time + skew:
            env.quarantine_reasons.append("lookahead_violation")
            env.quality_ok = False

    def _layer_sanity(self, env: Envelope) -> None:
        """Layer 2: phantom ticks and impossible quotes."""
        p = env.payload
        price = self._price_of(env)
        if price <= 0:
            env.quarantine_reasons.append("nonpositive_price")
            env.quality_ok = False
            return
        if env.dataset_id == "btc_ohlcv_1m":
            o, h, l, c = p["open"], p["high"], p["low"], p["close"]
            if not (h >= max(o, c) >= min(o, c) >= l > 0):
                env.quarantine_reasons.append("ohlc_incoherent")
                env.quality_ok = False
                return
        prev = self._prev_price.get(env.dataset_id)
        if prev and prev > 0:
            move_pct = abs(price - prev) / prev * 100
            if move_pct > settings.max_minute_move_pct:
                env.quality_flags.append("phantom_suspect")
                env.quality_ok = False

    def _layer_liquidity(self, env: Envelope) -> None:
        """Layer 3: illiquid/unsafe windows — flag, still stored."""
        if env.dataset_id == "btc_ohlcv_1m" and env.payload.get("volume", 0) <= 0:
            env.quality_flags.append("illiquid_window")
        prev_t = self._prev_event_time.get(env.dataset_id)
        if (
            env.dataset_id == "btc_price_tick"
            and prev_t is not None
            and (env.event_time - prev_t).total_seconds() * 1000 > settings.illiquid_gap_ms
        ):
            env.quality_flags.append("illiquid_window")

    def _layer_freshness(self, env: Envelope) -> None:
        """Layer 1: over the staleness budget → not usable for live decisions."""
        budget = BUDGETS_MS.get(env.dataset_id)
        if budget is None:
            return
        age_ms = (datetime.now(timezone.utc) - env.event_time).total_seconds() * 1000
        if age_ms > budget:
            env.quality_flags.append("stale")
            env.quality_ok = False

    def _layer_source(self, env: Envelope) -> None:
        """Layer 5: single-source critical data is flagged until a second venue exists."""
        env.quality_flags.append("single_source")
