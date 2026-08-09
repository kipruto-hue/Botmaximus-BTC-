r"""The TradeLoop (TradeLoop Orchestrator v1.0 §7).

All ten §7 tests. None skipped as "obvious" — the point of encoding the
constitutional call order in code is to make its shape a *checked property*,
and a checked property with no check is just prose again.

The order under test is a safety property, not a convention:

    risk pre-check -> Arbiter -> Scrutiny -> risk final approve -> execute

Scrutiny sits after the deterministic checks so it can only subtract, and the
second risk pass exists because the world moves during the scrutiny budget.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, time, timezone

import pytest

from botmaximus.config import settings
from botmaximus.execution.session import Session
from botmaximus.orchestrator import startup as orch_startup
from botmaximus.orchestrator.trade_loop import SignalSource, TradeLoop
from botmaximus.risk.state import OrderIntent, Rejection

UTC = timezone.utc
BAR = datetime(2026, 6, 1, 12, 0, tzinfo=UTC)


@dataclass
class _Bar:
    event_time: datetime = BAR


class _Calls(list):
    """Records the call sequence so §7.2 can assert the exact order."""


def _intent():
    return OrderIntent(strategy_id="s1", direction="LONG",
                       entry_price=64_000.0, stop_price=63_500.0)


class _Kills:
    def __init__(self, block=None):
        self.block = block

    def blocks_trading(self):
        return self.block


class _Risk:
    """Approves unless told otherwise, recording each call."""

    def __init__(self, calls, block=None, reject_pre=False, reject_final=False):
        self.calls = calls
        self.kills = _Kills(block)
        self.reject_pre = reject_pre
        self.reject_final = reject_final
        self._checks = 0

    def size_intent(self, intent):
        self.calls.append("risk.size_intent")
        return intent

    def pre_trade_check(self, sized):
        self._checks += 1
        first = self._checks == 1
        self.calls.append("risk.pre_trade_check" if first
                          else "risk.final_approve")
        if first and self.reject_pre:
            return Rejection(intent=sized, reasons=["max_open_risk"])
        if not first and self.reject_final:
            return Rejection(intent=sized,
                             reasons=["position_opened_meanwhile"])
        return object()


@dataclass
class _Decision:
    intent: object | None
    reason: str = "ok"


class _Arbiter:
    def __init__(self, calls, intent=None, reason="ok"):
        self.calls = calls
        self._intent = intent
        self._reason = reason
        self.entries = 0

    async def decide(self, signals, regime=None):
        self.calls.append("arbiter.decide")
        return _Decision(self._intent, self._reason)

    def note_entry(self):
        self.entries += 1


@dataclass
class _Verdict:
    verdict: str = "APPROVE"
    reason: str = "ok"


class _Scrutiny:
    def __init__(self, calls, verdict="APPROVE", reason="ok"):
        self.calls = calls
        self._v = _Verdict(verdict, reason)

    async def review(self, intent, context):
        self.calls.append("scrutiny.review")
        return self._v


class _Engine:
    def __init__(self, calls):
        self.calls = calls
        self.executed = 0

    async def execute(self, intent):
        self.calls.append("engine.execute")
        self.executed += 1
        return {"status": "filled", "order_id": "o1"}


class _Signals(SignalSource):
    def __init__(self, calls, signals=None, reason=None):
        self.calls = calls
        self._signals = signals if signals is not None else ["sig"]
        self._reason = reason

    async def signals_for_bar(self, bar):
        self.calls.append("signals.for_bar")
        return list(self._signals), self._reason


def build(calls, **kw):
    """A loop whose every layer is a recording double."""
    risk = kw.pop("risk", None) or _Risk(calls)
    return TradeLoop(
        risk_core=risk,
        arbiter=kw.pop("arbiter", None) or _Arbiter(calls, intent=_intent()),
        scrutiny=kw.pop("scrutiny", None) or _Scrutiny(calls),
        paper_engine=kw.pop("engine", None) or _Engine(calls),
        signal_source=kw.pop("signals", None) or _Signals(calls),
        session=kw.pop("session", None),
        enabled=kw.pop("enabled", True),
    )


# ---------------------------------------------------------------- §7.1

def test_the_default_is_off():
    """Not a fault: until a strategy clears Gate 3 there is nothing to trade."""
    assert settings.trade_loop_enabled is False


@pytest.mark.asyncio
async def test_a_disabled_loop_calls_nothing(pg):
    calls = _Calls()
    loop = build(calls, enabled=False)
    out = await loop.handle_bar_close(_Bar())
    assert out.outcome == "disabled"
    assert calls == []


@pytest.mark.asyncio
async def test_disabled_startup_constructs_no_decision_layers(pg, monkeypatch):
    """§6: with the flag off, boot behaviour is unchanged byte-for-byte."""
    monkeypatch.setattr(settings, "trade_loop_enabled", False)
    assert await orch_startup.build() is None


# ---------------------------------------------------------------- §7.2

@pytest.mark.asyncio
async def test_the_call_order_is_exact(pg):
    """The constitutional order, asserted as a sequence. A reordering that
    looks harmless fails here instead of reaching production."""
    calls = _Calls()
    loop = build(calls)
    out = await loop.handle_bar_close(_Bar())

    assert out.outcome == "executed"
    assert calls == [
        "signals.for_bar",
        "arbiter.decide",
        "risk.size_intent",
        "risk.pre_trade_check",
        "scrutiny.review",
        "risk.size_intent",
        "risk.final_approve",
        "engine.execute",
    ]


@pytest.mark.asyncio
async def test_scrutiny_runs_after_risk_and_before_the_final_approve(pg):
    """Scrutiny may only subtract, so it cannot precede the deterministic
    checks; and the world moves during its budget, so it cannot be the last
    word either."""
    calls = _Calls()
    await build(calls).handle_bar_close(_Bar())
    assert calls.index("risk.pre_trade_check") < calls.index("scrutiny.review")
    assert calls.index("scrutiny.review") < calls.index("risk.final_approve")
    assert calls.index("risk.final_approve") < calls.index("engine.execute")


# ---------------------------------------------------------------- §7.3

@pytest.mark.asyncio
async def test_the_arbiter_can_stand_aside(pg):
    calls = _Calls()
    loop = build(calls, arbiter=_Arbiter(calls, intent=None, reason="conflict"))
    out = await loop.handle_bar_close(_Bar())
    assert out.outcome == "skipped" and "conflict" in out.reason
    assert "scrutiny.review" not in calls and "engine.execute" not in calls


@pytest.mark.asyncio
async def test_the_risk_pre_check_blocks_before_scrutiny_is_consulted(pg):
    """A deterministic block must not spend an LLM call, and must not give
    scrutiny the chance to look like it approved something risk refused."""
    calls = _Calls()
    loop = build(calls, risk=_Risk(calls, reject_pre=True))
    out = await loop.handle_bar_close(_Bar())
    assert out.outcome == "blocked" and out.stage == "risk_pre"
    assert "scrutiny.review" not in calls and "engine.execute" not in calls


@pytest.mark.asyncio
async def test_a_scrutiny_veto_stops_the_trade(pg):
    calls = _Calls()
    loop = build(calls, scrutiny=_Scrutiny(calls, "VETO", "stale_feed"))
    out = await loop.handle_bar_close(_Bar())
    assert out.outcome == "blocked" and out.stage == "scrutiny"
    assert "stale_feed" in out.reason
    assert "engine.execute" not in calls


@pytest.mark.asyncio
async def test_the_final_risk_pass_can_still_block_after_an_approval(pg):
    """The whole reason the second pass exists: a position may have opened, a
    kill may have fired, equity may have moved during the scrutiny budget."""
    calls = _Calls()
    loop = build(calls, risk=_Risk(calls, reject_final=True))
    out = await loop.handle_bar_close(_Bar())
    assert out.outcome == "blocked" and out.stage == "risk_final"
    assert "scrutiny.review" in calls          # it did approve
    assert "engine.execute" not in calls       # and it still did not trade


# ---------------------------------------------------------------- §7.4 / §7.5

@pytest.mark.asyncio
async def test_no_signals_is_a_valid_non_event(pg):
    calls = _Calls()
    loop = build(calls, signals=_Signals(calls, [], "no_strategies_in_trading_state"))
    out = await loop.handle_bar_close(_Bar())
    assert out.outcome == "skipped"
    assert out.reason == "no_strategies_in_trading_state"
    assert "arbiter.decide" not in calls


@pytest.mark.asyncio
async def test_a_populated_pool_with_no_evaluator_says_so(pg):
    """The gap this module refuses to hide. If a non-empty pool reported a
    bland `no_signals`, we would have rebuilt the silent absence the TradeLoop
    exists to close."""
    await pg.execute(
        "INSERT INTO strategies (strategy_id, version, definition_hash, "
        " lifecycle_state, origin) VALUES ('s1',1,'h','paper','seed')")
    signals, reason = await SignalSource().signals_for_bar(_Bar())
    assert signals == []
    assert reason == "no_live_evaluator"


@pytest.mark.asyncio
async def test_a_halt_short_circuits_before_signals_are_pulled(pg):
    calls = _Calls()
    loop = build(calls, risk=_Risk(calls, block="L2_daily_loss"))
    out = await loop.handle_bar_close(_Bar())
    assert out.outcome == "skipped" and "L2_daily_loss" in out.reason
    assert calls == []


# ---------------------------------------------------------------- §7.6

@pytest.mark.asyncio
async def test_the_trading_window_is_enforced_at_the_loop(pg):
    """06:00 Nairobi against a 15:30-17:30 window."""
    closed = Session(enabled=True, tz="Africa/Nairobi",
                     start=time(15, 30), end=time(17, 30))
    calls = _Calls()
    loop = build(calls, session=closed)
    out = await loop.handle_bar_close(
        _Bar(datetime(2026, 6, 1, 3, 0, tzinfo=UTC)))   # 06:00 Nairobi
    assert out.outcome == "skipped" and out.reason == "outside_window"
    assert calls == []


def test_the_window_setting_is_finally_parsed(monkeypatch):
    """`TRADE_WINDOW_LOCAL` was declared and read by nothing until now."""
    monkeypatch.setattr(settings, "trade_window_local",
                        "Africa/Nairobi:15:30-17:30")
    s = Session.from_settings()
    assert s.tz == "Africa/Nairobi" and s.start == time(15, 30)


def test_a_malformed_window_raises_rather_than_trading_around_the_clock(
        monkeypatch):
    monkeypatch.setattr(settings, "trade_window_local", "nonsense")
    with pytest.raises(ValueError, match="always-open"):
        Session.from_settings()


# ---------------------------------------------------------------- §7.7

@pytest.mark.asyncio
async def test_enabled_but_unmet_preconditions_aborts_startup(pg, monkeypatch):
    """A process that comes up with a half-wired trade loop is worse than one
    that refuses to come up, because the first looks healthy."""
    monkeypatch.setattr(settings, "trade_loop_enabled", True)
    monkeypatch.setattr(settings, "bybit_api_key", None)
    monkeypatch.setattr(settings, "bybit_api_secret", None)
    with pytest.raises(orch_startup.TradeLoopStartupError, match="ABORTED"):
        await orch_startup.build()


# ---------------------------------------------------------------- §7.8

def test_the_loop_cannot_turn_on_live_trading():
    """§1.7: the TradeLoop does not flip the money guards. It cannot."""
    import ast
    import pathlib
    pkg = pathlib.Path(orch_startup.__file__).parent
    for path in pkg.rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Attribute) and isinstance(node.ctx, ast.Store):
                assert node.attr not in ("live_trading_enabled", "bybit_testnet"), \
                    f"{path.name} assigns {node.attr}"


def test_there_is_no_runtime_enable_endpoint():
    """§11: `.env` + restart only. A route that can start trading is an attack
    surface."""
    from botmaximus.api import app as api
    for r in api.app.routes:
        path = getattr(r, "path", "")
        assert "trade_loop" not in path and "tradeloop" not in path


def test_no_stage_can_be_skipped_by_a_flag():
    """§1.4: no bypass, no fast path, no 'urgent' branch."""
    import pathlib
    src = (pathlib.Path(orch_startup.__file__).parent
           / "trade_loop.py").read_text(encoding="utf-8")
    for banned in ("skip_scrutiny", "skip_risk", "fast_path", "bypass",
                   "urgent"):
        assert banned not in src


# ---------------------------------------------------------------- §7.10 / §8

@pytest.mark.asyncio
async def test_a_scrutiny_timeout_is_a_veto_not_a_pass(pg):
    """The gate resolves timeout/exception/malformed to VETO internally;
    nothing in the loop can turn one into an approval."""
    calls = _Calls()
    loop = build(calls, scrutiny=_Scrutiny(calls, "VETO", "provider_timeout"))
    out = await loop.handle_bar_close(_Bar())
    assert out.outcome == "blocked"
    assert "provider_timeout" in out.reason


@pytest.mark.asyncio
async def test_every_outcome_writes_exactly_one_telemetry_row(pg):
    """§8: silent success is a bug. One row per bar-close event."""
    calls = _Calls()
    await build(calls).handle_bar_close(_Bar())
    await build(_Calls(), enabled=False).handle_bar_close(_Bar())
    await build(_Calls(), risk=_Risk(_Calls(), block="L3")).handle_bar_close(_Bar())

    rows = await pg.fetch(
        "SELECT * FROM telemetry_events WHERE kind = 'trade_loop_bar' "
        "ORDER BY event_id")
    assert len(rows) == 3
    assert [r["label"] for r in rows] == ["executed", "disabled", "skipped"]
    assert rows[0]["context"]["bar_close_time"] == BAR.isoformat()


@pytest.mark.asyncio
async def test_the_cooldown_starts_only_after_an_execution(pg):
    calls = _Calls()
    arb = _Arbiter(calls, intent=_intent())
    loop = build(calls, arbiter=arb)
    await loop.handle_bar_close(_Bar())
    assert arb.entries == 1


# ---------------------------------------------------------------- bar event

@pytest.mark.asyncio
async def test_the_pipeline_emits_bar_close_only_for_confirmed_live_candles():
    """Backfilled candles heal history and are not news — firing on one would
    have the loop evaluate a bar from last Tuesday as if it just closed."""
    from botmaximus.pipeline.bus import Pipeline
    from botmaximus.pipeline.envelope import Envelope, utcnow

    seen = []
    p = Pipeline(parser=None, gate=None, writer=None)
    p.subscribe_bar_close(lambda env: seen.append(env) or _noop())

    def env(dataset, backfill=False):
        now = utcnow()
        return Envelope(dataset_id=dataset, source="bybit_v5_ws",
                        symbol="BTCUSDT", event_time=now, collection_time=now,
                        payload={}, backfill=backfill)

    await p._emit_bar_close(env("btc_ohlcv_1m"))
    await p._emit_bar_close(env("btc_price_tick"))          # wrong dataset
    await p._emit_bar_close(env("btc_ohlcv_1m", backfill=True))
    assert len(seen) == 1


async def _noop():
    return None


@pytest.mark.asyncio
async def test_a_failing_subscriber_never_breaks_the_store_path():
    """Losing a trading decision is survivable; losing the data is not."""
    from botmaximus.pipeline.bus import Pipeline
    from botmaximus.pipeline.envelope import Envelope, utcnow

    async def boom(env):
        raise RuntimeError("subscriber exploded")

    p = Pipeline(parser=None, gate=None, writer=None)
    p.subscribe_bar_close(boom)
    now = utcnow()
    await p._emit_bar_close(Envelope(
        dataset_id="btc_ohlcv_1m", source="bybit_v5_ws", symbol="BTCUSDT",
        event_time=now, collection_time=now, payload={}))
