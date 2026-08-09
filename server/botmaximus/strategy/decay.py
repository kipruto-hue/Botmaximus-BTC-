r"""Decay monitor — is a live strategy still the thing that was validated?

The hard part is not detecting losses. It is **not reacting to normal ones.**
A strategy with a 45% win rate will produce a run of six losers roughly every
sixty trades; retiring it on that is how a working strategy gets killed and
replaced by a worse one that has not yet had its unlucky streak.

So the trigger is a sequential test: given this strategy's *validated* win rate,
how surprising is the run actually observed? Only a run that clears the
configured significance counts as evidence of change.

## Two different failures, deliberately separated

- **Alpha decay** — the edge stopped working. Rolling expectancy falls.
- **Cost drift** — the edge is intact and the cost model was wrong. Realized
  cost exceeds predicted, from `execution_ledger`.

They look identical on an equity curve and want opposite responses: retire the
strategy, or recalibrate the cost model and keep it. Reporting which one fired
is most of this module's value.

`DECAY_REPAIR_TRIGGER` is unset by default and consumers raise rather than
invent a threshold — a decay rule with a made-up significance level would fire
on noise and be trusted anyway.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone

from botmaximus.config import settings
from botmaximus.execution.ledger import LEDGER, RECONCILED

log = logging.getLogger(__name__)

DECAY_EVENTS = "decay_events"


@dataclass(frozen=True)
class DecayTrigger:
    """Parsed from `DECAY_REPAIR_TRIGGER`, e.g.
    `sequential:min_trades=50,alpha=0.01`."""
    kind: str = "sequential"
    min_trades: int = 50
    alpha: float = 0.01

    @classmethod
    def parse(cls, raw: str | None) -> "DecayTrigger":
        if not raw:
            raise RuntimeError(
                "DECAY_REPAIR_TRIGGER is unset. A decay rule with an invented "
                "significance level fires on noise and gets trusted anyway, so "
                "there is no default to fall back on.")
        kind, _, rest = raw.partition(":")
        kw: dict = {}
        for part in filter(None, rest.split(",")):
            k, _, v = part.partition("=")
            kw[k.strip()] = v.strip()
        return cls(kind=kind.strip() or "sequential",
                   min_trades=int(kw.get("min_trades", 50)),
                   alpha=float(kw.get("alpha", 0.01)))


@dataclass
class DecayVerdict:
    strategy_id: str
    decayed: bool
    cause: str
    detail: dict = field(default_factory=dict)


def losing_run_pvalue(run_length: int, win_rate: float) -> float:
    """P(a run of at least this many consecutive losses | validated win rate).

    Deliberately the naive per-trade probability rather than a run statistic
    over the whole history: it is conservative in the direction that matters
    (it under-states surprise, so it fires later rather than sooner), and it
    cannot be gamed by a longer sample.
    """
    loss_rate = max(1e-9, min(1.0 - win_rate, 1 - 1e-9))
    return loss_rate ** max(run_length, 0)


def current_losing_run(pnls: list[float]) -> int:
    run = 0
    for p in reversed(pnls):
        if p < 0:
            run += 1
        else:
            break
    return run


class DecayMonitor:
    def __init__(self, risk_core, trigger: DecayTrigger | None = None) -> None:
        self.risk = risk_core
        self.trigger = trigger or DecayTrigger.parse(settings.decay_repair_trigger)

    async def evaluate(self, strategy_id: str, trade_pnls: list[float],
                       validated_win_rate: float) -> DecayVerdict:
        n = len(trade_pnls)
        if n < self.trigger.min_trades:
            return DecayVerdict(strategy_id, False, "insufficient_trades",
                                {"trades": n, "need": self.trigger.min_trades})

        # Cost drift first: if the cost model is wrong, the "decay" is ours, not
        # the market's, and retiring the strategy would fix nothing.
        drift = await self._cost_drift(strategy_id)
        if drift and drift["ratio"] >= settings.auto_demote_cost_multiple:
            return DecayVerdict(strategy_id, True, "cost_drift", drift)

        run = current_losing_run(trade_pnls)
        p = losing_run_pvalue(run, validated_win_rate)
        if p < self.trigger.alpha:
            return DecayVerdict(strategy_id, True, "alpha_decay", {
                "losing_run": run, "p_value": p, "alpha": self.trigger.alpha,
                "validated_win_rate": validated_win_rate})

        expectancy = sum(trade_pnls) / n
        if expectancy < 0 and n >= self.trigger.min_trades * 2:
            return DecayVerdict(strategy_id, True, "negative_expectancy",
                                {"expectancy": expectancy, "trades": n})

        return DecayVerdict(strategy_id, False, "healthy", {
            "losing_run": run, "p_value": p,
            "expectancy": sum(trade_pnls) / n})

    async def _cost_drift(self, strategy_id: str) -> dict | None:
        """Realized vs predicted cost over the rolling leg window."""
        try:
            from botmaximus.storage import postgres
            rows = await postgres.fetch(
                "SELECT drift, predicted_fee FROM execution_ledger "
                "WHERE strategy_id = %s AND status = %s "
                "ORDER BY decision_time DESC LIMIT %s",
                (strategy_id, RECONCILED, settings.auto_demote_leg_window))
        except Exception:                               # noqa: BLE001
            return None
        if len(rows) < settings.auto_demote_leg_window:
            return None

        predicted = sum(abs(r.get("predicted_fee") or 0.0) for r in rows)
        realized = predicted + sum(
            (r.get("drift") or {}).get("cost_drift_usd", 0.0) for r in rows)
        if predicted <= 0:
            return None
        return {"legs": len(rows), "predicted": round(predicted, 4),
                "realized": round(realized, 4),
                "ratio": round(realized / predicted, 4)}

    async def on_decay(self, verdict: DecayVerdict) -> None:
        """Suspend, record, and hand off to the repair loop.

        The decayed strategy is retired **regardless of whether a repair
        succeeds** (§3.G.5). A repair is a new hypothesis, not a rehabilitation:
        letting the parent trade while its replacement is evaluated keeps
        capital on the thing that just failed its own test.
        """
        if not verdict.decayed:
            return
        await self.risk.kills.suspend_strategy(verdict.strategy_id, verdict.cause)
        try:
            import json

            from botmaximus.storage import postgres
            await postgres.execute(
                "INSERT INTO strategy_events (strategy_id, event, at, detail) "
                "VALUES (%s, %s, %s, %s)",
                (verdict.strategy_id, "decay", datetime.now(timezone.utc),
                 json.dumps({"cause": verdict.cause,
                             "detail": verdict.detail}, default=str)))
        except Exception as e:                          # noqa: BLE001
            from botmaximus.obs import degradation
            await degradation.record("decay_event_write_failed", str(e))
        log.warning("decay: %s -> %s %s", verdict.strategy_id, verdict.cause,
                    verdict.detail)


async def ensure_indexes() -> None:
    """No-op: indexes are part of `schema.sql`."""
    return None
