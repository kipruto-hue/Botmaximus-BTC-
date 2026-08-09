r"""Layer D — Arbiter. Zero-or-more strategy signals in, **at most one**
`OrderIntent` out.

Deterministic and config-driven. There is no model here and no discretion: the
arbiter's whole job is to be the place where "two strategies both want to trade"
resolves the same way every time, and can be audited afterwards.

## Why opposite-direction signals stand aside

Two strategies disagreeing is not a weak signal to be netted — it is evidence
that the situation is outside at least one of their competence. Netting them
produces a small position justified by nothing; picking the higher score bets on
a score comparison that was never validated across strategies. Standing aside is
the only option whose failure mode is "missed a trade".

## Why there is no size stacking

Same-direction signals merge to **one** intent at the winner's strategy_id. Two
strategies agreeing does not authorise double size: `risk_per_trade_pct` is a
per-trade budget, and stacking would breach it while every individual check
still passed.

## Every non-empty input set produces a record

Including the ones that produce nothing. "Why didn't it trade?" is the question
this project will ask most often, and it is unanswerable from a log of trades
that happened.
"""
from __future__ import annotations

import logging
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from botmaximus.config import settings
from botmaximus.execution.session import Session
from botmaximus.obs import degradation
from botmaximus.risk.state import OrderIntent

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class StrategySignal:
    strategy_id: str
    direction: str                      # "LONG" | "SHORT"
    confidence: float                   # 0..1, the strategy's own conviction
    entry_price: float
    stop_price: float
    required_feeds: tuple[str, ...] = ()
    regime_scope: tuple[str, ...] = ()
    ds_sharpe: float = 1.0              # deflated Sharpe from the lifecycle record
    weight: float = 1.0                 # operator/lifecycle weight
    expected_edge_pct: float | None = None
    expected_cost_pct: float | None = None
    thesis: str = ""
    ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


@dataclass
class ArbiterDecision:
    intent: OrderIntent | None
    reason: str
    scores: dict = field(default_factory=dict)
    dropped: dict = field(default_factory=dict)


class Arbiter:
    def __init__(self, risk_core, session: Session | None = None,
                 cooldown_s: int | None = None) -> None:
        self.risk = risk_core
        self.session = session
        self.cooldown_s = (settings.arbiter_cooldown_s if cooldown_s is None
                           else cooldown_s)
        self._last_entry_monotonic: float | None = None

    def note_entry(self) -> None:
        """Called by the executor after a fill, to start the cooldown."""
        self._last_entry_monotonic = time.monotonic()

    async def decide(self, signals: list[StrategySignal],
                     regime: str | None = None) -> ArbiterDecision:
        if not signals:
            return ArbiterDecision(None, "no_signals")

        decision = self._decide(signals, regime)
        await self._record(signals, decision, regime)
        return decision

    # ---- the rules, in the order they can cheaply eliminate work ----
    def _decide(self, signals: list[StrategySignal],
                regime: str | None) -> ArbiterDecision:
        if self.risk.portfolio.open_positions:
            return ArbiterDecision(None, "position_open")

        if self._in_cooldown():
            return ArbiterDecision(None, "cooldown")

        if self.session is not None and not self.session.may_enter():
            return ArbiterDecision(None, "outside_window")

        scored: dict[str, float] = {}
        dropped: dict[str, str] = {}
        live: list[StrategySignal] = []
        for s in signals:
            fit = self._regime_fit(s, regime)
            if fit is None:
                # Never guess a regime. A strategy scoped to conditions we
                # cannot currently label is not eligible.
                dropped[s.strategy_id] = "regime_unknown"
                continue
            if fit == 0.0:
                dropped[s.strategy_id] = "out_of_regime_scope"
                continue
            score = s.weight * s.confidence * max(s.ds_sharpe, 0.0) * fit
            if score <= 0:
                dropped[s.strategy_id] = "nonpositive_score"
                continue
            scored[s.strategy_id] = score
            live.append(s)

        if not live:
            return ArbiterDecision(None, "all_stale", scored, dropped)

        directions = {s.direction for s in live}
        if len(directions) > 1:
            # Disagreement is evidence, not noise to be averaged away.
            return ArbiterDecision(None, "conflict", scored, dropped)

        winner = max(live, key=lambda s: scored[s.strategy_id])
        intent = OrderIntent(
            strategy_id=winner.strategy_id,
            direction=winner.direction,
            entry_price=winner.entry_price,
            stop_price=winner.stop_price,
            symbol=settings.symbol,
            expected_edge_pct=winner.expected_edge_pct,
            expected_cost_pct=winner.expected_cost_pct,
            thesis=winner.thesis,
            required_feeds=winner.required_feeds,
        )
        return ArbiterDecision(intent, "ok", scored, dropped)

    def _in_cooldown(self) -> bool:
        if self._last_entry_monotonic is None:
            return False
        return (time.monotonic() - self._last_entry_monotonic) < self.cooldown_s

    @staticmethod
    def _regime_fit(signal: StrategySignal, regime: str | None) -> float | None:
        """1.0 in scope, 0.0 out of scope, None when the regime is unknown.

        An unscoped strategy is in scope everywhere — that is what declaring no
        scope means — but an unknown regime with a scoped strategy is not a
        judgement call the arbiter is allowed to make.
        """
        if not signal.regime_scope:
            return 1.0
        if regime is None:
            return None
        return 1.0 if regime in signal.regime_scope else 0.0

    async def _record(self, signals: list[StrategySignal],
                      decision: ArbiterDecision, regime: str | None) -> None:
        import json

        try:
            from botmaximus.storage import postgres
            await postgres.execute(
                "INSERT INTO arbiter_events "
                "(at, regime, reason, inputs, scores, dropped, intent) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                (datetime.now(timezone.utc), regime, decision.reason,
                 json.dumps([asdict(s) for s in signals], default=str),
                 json.dumps(decision.scores, default=str),
                 json.dumps(decision.dropped, default=str),
                 json.dumps(asdict(decision.intent), default=str)
                 if decision.intent else None))
        except Exception as e:                          # noqa: BLE001
            await degradation.record(
                "arbiter_event_write_failed",
                f"could not persist an arbiter decision ({e}) — the decision "
                f"still stands but is not auditable")


async def ensure_indexes() -> None:
    """No-op: indexes are part of `schema.sql`."""
    return None
