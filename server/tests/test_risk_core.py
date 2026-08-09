"""Risk core acceptance tests (§4.6) — Gate 1.

Synthetic intents against a fake Mongo. The suite proves: oversized intents
rejected, stops beyond liquidation rejected, cumulative risk caps enforced,
L1/L2/L3 fire correctly, master kill blocks everything, kill state survives
a process restart, stale feeds block entries.
"""
from datetime import timedelta

import pytest

from botmaximus.config import settings
from botmaximus.pipeline.envelope import utcnow
from botmaximus.pipeline.telemetry import telemetry
from botmaximus.risk.core import RiskCore
from botmaximus.risk.kills import L3_RESET_TOKEN
from botmaximus.risk.state import OrderIntent, Rejection, SizedOrder


ENTRY = 66_000.0


def fresh_feeds():
    now = utcnow()
    telemetry._last_event["btc_price_tick"] = now
    telemetry._last_event["btc_ohlcv_1m"] = now


def intent(strategy="S1", direction="LONG", entry=ENTRY, stop=None, **kw):
    if stop is None:
        stop = entry - 330 if direction == "LONG" else entry + 330  # 0.5% stop
    return OrderIntent(strategy_id=strategy, direction=direction,
                       entry_price=entry, stop_price=stop, **kw)


async def make_core():
    core = RiskCore()
    await core.load()
    fresh_feeds()
    return core


# ---------------- sizing (§4.1) ----------------

@pytest.mark.asyncio
async def test_sizing_derives_qty_from_risk(pg):
    core = await make_core()
    sized = core.size_intent(intent())
    assert isinstance(sized, SizedOrder)
    max_risk = settings.starting_equity_paper * settings.risk_per_trade_pct / 100
    assert 0 < sized.risk_usd <= max_risk
    assert sized.qty * ENTRY == pytest.approx(sized.notional_usd, rel=1e-6)


@pytest.mark.asyncio
async def test_wrong_side_stop_rejected(pg):
    core = await make_core()
    out = core.size_intent(intent(stop=ENTRY + 100))  # stop above entry on a LONG
    assert isinstance(out, Rejection)
    assert "stop_on_wrong_side" in out.reasons


@pytest.mark.asyncio
async def test_tight_stop_implies_oversized_leverage_rejected(pg):
    core = await make_core()
    sized = core.size_intent(intent(stop=ENTRY - 33))  # 0.05% stop → huge notional
    assert isinstance(sized, SizedOrder)
    assert sized.implied_leverage > settings.leverage_cap
    verdict = await core.pre_trade_check(sized)
    assert isinstance(verdict, Rejection)
    assert "leverage_exceeds_cap" in verdict.reasons


@pytest.mark.asyncio
async def test_cumulative_open_risk_cap_enforced(pg):
    core = await make_core()
    approved = 0
    for i in range(6):
        sized = core.size_intent(intent(strategy=f"S{i}"))
        verdict = await core.pre_trade_check(sized)
        if isinstance(verdict, SizedOrder):
            core.register_open(verdict)
            approved += 1
        else:
            assert "total_open_risk_exceeds_cap" in verdict.reasons
    cap = settings.starting_equity_paper * settings.max_open_risk_pct / 100
    assert core.portfolio.open_risk_usd <= cap
    assert 0 < approved < 6                    # some approved, then the cap bit


@pytest.mark.asyncio
async def test_stop_beyond_liquidation_rejected(pg):
    core = await make_core()
    liq = RiskCore._liquidation_price("LONG", ENTRY)
    bad = SizedOrder(
        intent=intent(stop=liq * 0.99),        # stop past the liquidation price
        qty=0.01, notional_usd=660.0, risk_usd=10.0,
        implied_leverage=0.066, est_liquidation_price=liq,
    )
    verdict = await core.pre_trade_check(bad)
    assert isinstance(verdict, Rejection)
    assert "stop_beyond_liquidation_buffer" in verdict.reasons


# ---------------- kill stack (§4.3) ----------------

@pytest.mark.asyncio
async def test_l1_suspends_one_strategy_not_others(pg):
    core = await make_core()
    await core.kills.suspend_strategy("BAD", "decay breach")
    rej = await core.pre_trade_check(core.size_intent(intent(strategy="BAD")))
    ok = await core.pre_trade_check(core.size_intent(intent(strategy="GOOD")))
    assert isinstance(rej, Rejection) and any("L1" in r for r in rej.reasons)
    assert isinstance(ok, SizedOrder)


@pytest.mark.asyncio
async def test_l2_halts_new_entries(pg):
    core = await make_core()
    await core.kills.halt_portfolio("abnormal slippage")
    verdict = await core.pre_trade_check(core.size_intent(intent()))
    assert isinstance(verdict, Rejection)
    assert any("L2" in r for r in verdict.reasons)


@pytest.mark.asyncio
async def test_l2_fires_on_daily_loss_limit(pg):
    core = await make_core()
    loss = 1 - (settings.daily_loss_limit_pct + 0.5) / 100
    await core.update_equity(settings.starting_equity_paper * loss)
    assert core.kills.l2_halted is not None


@pytest.mark.asyncio
async def test_l3_fires_on_drawdown_breach_and_blocks_everything(pg):
    core = await make_core()
    dd = 1 - (settings.max_drawdown_kill_pct + 1) / 100
    await core.update_equity(settings.starting_equity_paper * dd)
    assert core.kills.l3_killed is not None
    verdict = await core.pre_trade_check(core.size_intent(intent()))
    assert isinstance(verdict, Rejection)
    assert any("L3" in r for r in verdict.reasons)


@pytest.mark.asyncio
async def test_operator_master_kill_and_token_reset(pg):
    core = await make_core()
    await core.kills.master_kill("operator")
    assert core.kills.blocks_trading()
    assert not await core.kills.reset_l3("wrong-token")   # stays killed
    assert core.kills.blocks_trading()
    assert await core.kills.reset_l3(L3_RESET_TOKEN)
    assert core.kills.blocks_trading() is None


@pytest.mark.asyncio
async def test_kill_state_survives_process_restart(pg):
    core1 = RiskCore()
    await core1.load()
    fresh_feeds()
    await core1.kills.master_kill("drawdown breach")

    core2 = RiskCore()                       # simulated restart, same store
    await core2.load()
    assert core2.kills.l3_killed is not None
    verdict = await core2.pre_trade_check(core2.size_intent(intent()))
    assert isinstance(verdict, Rejection)


@pytest.mark.asyncio
async def test_equity_and_peak_survive_restart(pg):
    core1 = RiskCore()
    await core1.load()
    fresh_feeds()
    await core1.update_equity(11_000)
    await core1.update_equity(10_500)

    core2 = RiskCore()
    await core2.load()
    assert core2.portfolio.peak_equity == 11_000
    assert core2.portfolio.equity == 10_500


# ---------------- data-dependency checks (§4.2) ----------------

@pytest.mark.asyncio
async def test_stale_feed_blocks_entry(pg):
    core = await make_core()
    telemetry._last_event["btc_price_tick"] = utcnow() - timedelta(minutes=5)
    try:
        verdict = await core.pre_trade_check(core.size_intent(intent()))
        assert isinstance(verdict, Rejection)
        assert any(r.startswith("feed_stale") for r in verdict.reasons)
    finally:
        fresh_feeds()


@pytest.mark.asyncio
async def test_edge_below_cost_margin_rejected(pg):
    core = await make_core()
    weak = intent(expected_edge_pct=0.10, expected_cost_pct=0.10)  # needs 1.5×
    verdict = await core.pre_trade_check(core.size_intent(weak))
    assert isinstance(verdict, Rejection)
    assert "edge_below_cost_margin" in verdict.reasons


@pytest.mark.asyncio
async def test_every_verdict_is_logged(pg):
    core = await make_core()
    await core.pre_trade_check(core.size_intent(intent()))
    events = await pg.fetch("SELECT * FROM risk_events ORDER BY event_id")
    assert len(events) == 1
    assert events[0]["kind"] in ("approved", "rejected")
    assert events[0]["strategy_id"] == "S1"
    # §2.7 wants the reason recoverable, not just the outcome.
    assert events[0]["detail"]["direction"] == "LONG"
