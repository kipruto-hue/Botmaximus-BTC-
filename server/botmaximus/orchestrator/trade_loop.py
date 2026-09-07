r"""The TradeLoop — the coordinator the constitution assumed existed.

Every decision layer in this system was built and tested individually, and
nothing called them. Collectors ran, data landed, coverage advanced — and the
chain stopped there. `PaperEngine.execute` had no caller anywhere in the
running process. This module is the connector, and nothing more.

## What it is

Glue. It owns references to the Arbiter, Scrutiny Gate, Risk Core and Paper
Engine, listens for bar-close events, and walks a signal through the
constitutional order. It decides nothing itself: it does not size (Risk does),
does not choose between signals (the Arbiter does), does not veto (Scrutiny
does), does not place orders (the Paper Engine does), and does not persist
decisions (every layer already persists its own).

## Why the order is code and not a comment

    deterministic risk pre-check → Arbiter → Scrutiny → risk final approve
        → execute → ledger

That sequence is a safety property, not a style choice. Scrutiny sits *after*
the deterministic checks so it can only ever subtract, and the second risk pass
exists because the world moves during the scrutiny budget — a position may have
opened, a kill may have fired, equity may have changed. Written as prose in a
docstring, that order survives exactly until someone refactors in a hurry.
Written as one method with a test asserting the call sequence, reordering it
fails the suite.

## Why OFF is the default and not a bug

There is currently no strategy in `paper` state, because none has cleared
Gate 3. A loop that runs would faithfully do nothing. The danger was never that
trading starts silently; it is the opposite — that someone promotes a strategy,
waits for trades, and debugs the strategy, the arbiter and the risk core before
discovering the layer that would act on it was never running. So the loop
defaults off, says so at every boot, and says why.

## The gap this module refuses to hide

`SignalSource` is where a live strategy would produce a signal for the just
closed bar. Compiled DSL strategies evaluate against a historical
`MarketWindow` during backtest; **no live evaluator exists yet**. An empty pool
therefore yields no signals, which is correct — but so would a *populated* one,
which is not. Rather than return a bland "no_signals" and recreate the exact
silent absence this module was written to close, the source reports
`no_live_evaluator` whenever the pool is non-empty and nothing can evaluate it.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from botmaximus.config import settings

log = logging.getLogger(__name__)

UTC = timezone.utc

#: Lifecycle states whose strategies are entitled to trade.
TRADING_STATES = ("paper", "micro", "full")

DISABLED = "disabled"
SKIPPED = "skipped"
BLOCKED = "blocked"
EXECUTED = "executed"


@dataclass
class BarOutcome:
    """What happened on one bar. One row in `telemetry_events` per bar."""
    outcome: str
    reason: str
    stage: str | None = None
    bar_close_time: datetime | None = None
    signals_in: int = 0
    detail: dict = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {"outcome": self.outcome, "reason": self.reason,
                "stage": self.stage, "signals_in": self.signals_in,
                "bar_close_time": (self.bar_close_time.isoformat()
                                   if self.bar_close_time else None),
                **self.detail}


class SignalSource:
    """Where a live strategy would produce a signal for the closed bar.

    See the module docstring. This exists so the seam is named, tested and
    visible rather than implied — and so the day a live evaluator is built,
    there is one obvious place to put it.
    """

    async def signals_for_bar(self, bar) -> tuple[list, str | None]:
        """Returns (signals, reason_if_empty).

        The reason distinguishes "nothing wanted to trade" from "something
        might have, and we have no way to ask it".
        """
        from botmaximus.strategy import store as strategy_store

        pool = await strategy_store.list_population(list(TRADING_STATES))
        if not pool:
            return [], "no_strategies_in_trading_state"
        # A populated pool with no evaluator is the silent gap. Name it.
        log.warning(
            "[TradeLoop] %d strategy(ies) are in a trading state but no live "
            "evaluator exists, so no signal can be produced. Live signal "
            "generation from compiled DSL strategies is not built.", len(pool))
        return [], "no_live_evaluator"


class TradeLoop:
    """Walks one bar-close event through the constitutional order."""

    name = "trade-loop"

    def __init__(self, risk_core, arbiter, scrutiny, paper_engine,
                 decay_monitor=None, signal_source: SignalSource | None = None,
                 enabled: bool | None = None, session=None) -> None:
        self.risk = risk_core
        self.arbiter = arbiter
        self.scrutiny = scrutiny
        self.paper_engine = paper_engine
        self.decay = decay_monitor
        self.signals = signal_source or SignalSource()
        self._session = session
        self.enabled = (settings.trade_loop_enabled if enabled is None
                        else enabled)
        self.last_bar: datetime | None = None
        self.last_outcome: BarOutcome | None = None

    # ---- the mandated order, in one place ---------------------------
    async def handle_bar_close(self, bar) -> BarOutcome:
        """The whole loop. Every branch returns; there is no fall-through.

        Stage order here is the constitution's, and `test_trade_loop.py`
        asserts the exact call sequence — a reordering that looks harmless
        fails the suite rather than reaching production.
        """
        bar_time = getattr(bar, "event_time", None)
        self.last_bar = bar_time

        if not self.enabled:
            return await self._record(BarOutcome(
                DISABLED, "trade_loop_disabled", bar_close_time=bar_time))

        # 0. Global preconditions — deterministic, before anything is gathered.
        block = self.risk.kills.blocks_trading()
        if block:
            return await self._record(BarOutcome(
                SKIPPED, f"risk_halted:{block}", "preconditions", bar_time))

        if not self._within_window(bar_time):
            return await self._record(BarOutcome(
                SKIPPED, "outside_window", "preconditions", bar_time))

        # 1. Signals from strategies entitled to trade.
        signals, empty_reason = await self.signals.signals_for_bar(bar)
        if not signals:
            return await self._record(BarOutcome(
                SKIPPED, empty_reason or "no_signals", "signals", bar_time))

        # 2. Arbiter — at most one intent out, and it records its own refusals.
        decision = await self.arbiter.decide(signals)
        if decision.intent is None:
            return await self._record(BarOutcome(
                SKIPPED, f"arbiter:{decision.reason}", "arbiter", bar_time,
                len(signals)))
        intent = decision.intent

        # 3. Deterministic risk pre-check.
        pre = self.risk.pre_trade_check(self.risk.size_intent(intent))
        if _rejected(pre):
            return await self._record(BarOutcome(
                BLOCKED, _reason(pre), "risk_pre", bar_time, len(signals)))

        # 4. Scrutiny — the sole LLM call in this loop, on its own budget.
        #    Timeout, exception and malformed output all resolve to VETO
        #    inside the gate; nothing here can turn one into an approval.
        verdict = await self.scrutiny.review(intent, {})
        if verdict.verdict != "APPROVE":
            return await self._record(BarOutcome(
                BLOCKED, f"scrutiny:{verdict.reason}", "scrutiny", bar_time,
                len(signals)))

        # 5. Deterministic risk, again. The world moved during the scrutiny
        #    budget: a position may have opened, a kill may have fired, equity
        #    may have changed. Approving on the pre-check alone would act on a
        #    snapshot that is now stale.
        sized = self.risk.size_intent(intent)
        final = self.risk.pre_trade_check(sized)
        if _rejected(final):
            return await self._record(BarOutcome(
                BLOCKED, _reason(final), "risk_final", bar_time, len(signals)))

        # 6. Execute. The engine sizes again internally, re-checks, places the
        #    entry and its broker-side reduce-only stop, and writes the
        #    execution ledger itself — step 7 needs nothing from here.
        result = await self.paper_engine.execute(intent)
        self.arbiter.note_entry()
        return await self._record(BarOutcome(
            EXECUTED, str(result.get("status", "placed")), "execute", bar_time,
            len(signals), detail={"execution": result}))

    # ---- helpers ----------------------------------------------------
    def _within_window(self, at: datetime | None = None) -> bool:
        """§7.6 — the window is enforced at the loop, not only at the executor.

        Judged against the BAR's close time, not the wall clock. `may_enter()`
        defaults to `now`, and taking that default made the answer depend on
        when the loop happened to run: the same bar replayed at a different
        hour got a different decision, so the record was not reproducible and
        the window test passed or failed according to the time of day. A
        decision chain that cannot be replayed to the same answer is not a
        decision chain.

        An unset window means no window is configured. That is deliberately
        permissive here and deliberately *not* permissive in the executor,
        which treats it as a configuration error: the loop's job is to not
        evaluate outside hours, the executor's is to refuse to place an order
        it cannot justify.
        """
        from botmaximus.execution.session import Session
        session = self._session if self._session is not None else Session.from_settings()
        return True if session is None else session.may_enter(at)

    async def _record(self, outcome: BarOutcome) -> BarOutcome:
        """One row per bar-close event (§8). Silent success is a bug.

        Telemetry failure must not break the loop: losing the record of a
        decision is bad, and losing the ability to make the next one is worse.
        """
        import json

        self.last_outcome = outcome
        try:
            from botmaximus.storage import postgres
            await postgres.execute(
                "INSERT INTO telemetry_events (kind, label, reason, context) "
                "VALUES ('trade_loop_bar', %s, %s, %s)",
                (outcome.outcome, outcome.reason,
                 json.dumps(outcome.to_dict(), default=str)))
        except Exception as e:                          # noqa: BLE001
            log.error("[TradeLoop] could not record bar outcome (%s): %s",
                      outcome.reason, e)
        return outcome

    def status(self) -> dict:
        return {
            "enabled": self.enabled,
            "last_bar": self.last_bar.isoformat() if self.last_bar else None,
            "last_outcome": (self.last_outcome.to_dict()
                             if self.last_outcome else None),
        }


def _rejected(verdict) -> bool:
    """The risk core answers with an approval object or a Rejection."""
    from botmaximus.risk.state import Rejection
    return isinstance(verdict, Rejection)


def _reason(verdict) -> str:
    reasons = getattr(verdict, "reasons", None)
    return ",".join(reasons) if reasons else "rejected"
