"""Backtest harness tests — Gate 2 (§5, §10). Proves the harness is
trustworthy: costs are never zero, gross/net diverge by exactly the friction,
the point-in-time view cannot leak the future, purge/embargo drops the right
samples, deflated Sharpe corrects for trials, and validation rejects both a
broken strategy and a coverage-incomplete window.
"""
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from botmaximus.backtest import data, metrics, store
from botmaximus.backtest.costs import CostModel, FundingPoint
from botmaximus.backtest.data import Bar, CoverageError, PointInTimeView
from botmaximus.backtest.engine import Backtester, BacktestResult, Trade
from botmaximus.backtest.regimes import build_regime_map, regime_lookup
from botmaximus.backtest.validation import purge_embargo, validate
from botmaximus.config import settings
from botmaximus.risk.core import RiskCore
from botmaximus.risk.state import OrderIntent

UTC = timezone.utc
T0 = datetime(2026, 7, 1, tzinfo=UTC)


def bar(i, o, h, l, c):
    t = T0 + timedelta(minutes=i)
    return Bar(open_time=t, close_time=t + timedelta(seconds=59, microseconds=999000),
               open=o, high=h, low=l, close=c, volume=10.0)


class FakeRiskDB(dict):
    class _C:
        async def find_one(self, *a, **k): return None
        async def replace_one(self, *a, **k): pass
        async def insert_one(self, *a, **k): pass
    def __missing__(self, key):
        self[key] = self._C()
        return self[key]


def make_risk(equity=10_000.0):
    r = RiskCore(FakeRiskDB())
    r.portfolio.equity = equity
    r.portfolio.peak_equity = equity
    return r


# ---------------- cost model (§5.3) ----------------

def test_slippage_is_never_zero_and_adverse():
    cm = CostModel(taker_fee_rate=0.0005, slippage_bps=1.0)
    assert cm.fill_price(100, "LONG", "entry") == pytest.approx(100.01)   # buy pays up
    assert cm.fill_price(100, "LONG", "exit") == pytest.approx(99.99)     # sell receives less
    assert cm.fill_price(100, "SHORT", "entry") == pytest.approx(99.99)   # sell to enter
    assert cm.fill_price(100, "SHORT", "exit") == pytest.approx(100.01)


def test_fee_charged_on_notional():
    cm = CostModel(taker_fee_rate=0.0005)
    assert cm.fee(1000) == pytest.approx(0.5)


def test_funding_charged_only_across_settlements():
    settle = datetime(2026, 7, 1, 8, 0, tzinfo=UTC)
    cm = CostModel(funding=[FundingPoint(time=settle, rate=0.0001)])
    entry = settle - timedelta(minutes=5)
    # held across the 08:00 settlement → long pays rate*notional
    cost = cm.funding_cost("LONG", qty=1.0, avg_price=100.0,
                           entry_time=entry, exit_time=settle + timedelta(minutes=5))
    assert cost == pytest.approx(0.01)
    # closed before the settlement → no funding
    assert cm.funding_cost("LONG", 1.0, 100.0, entry, settle - timedelta(minutes=1)) == 0.0
    # a short receives positive funding (negative cost)
    assert cm.funding_cost("SHORT", 1.0, 100.0, entry,
                           settle + timedelta(minutes=5)) == pytest.approx(-0.01)


# ---------------- point-in-time (§5.4) ----------------

def test_view_cannot_see_the_future():
    bars = [bar(i, 100, 101, 99, 100 + i) for i in range(20)]
    view = PointInTimeView(bars, 5)
    assert view.now is bars[5]
    seen = view.bars(100)
    assert seen[-1] is bars[5]              # never bar 6+
    assert len(seen) == 6
    assert view.closes(3) == [103, 104, 105]


def test_purge_embargo_drops_boundary_samples():
    drop = purge_embargo(n_samples=100, test_start=40, test_end=60, purge=5, embargo=5)
    assert drop == set(range(35, 40)) | set(range(60, 70))


# ---------------- engine mechanics ----------------

class _EnterLongOnce:
    id = "test-long-once"
    required_feeds = ["btc_ohlcv_1m"]

    def __init__(self, at_index):
        self._at = at_index
        self._fired = False

    def evaluate(self, view):
        if self._fired or view._i != self._at:
            return None
        self._fired = True
        c = view.now.close
        return OrderIntent(strategy_id=self.id, direction="LONG",
                           entry_price=c, stop_price=c * 0.99)


def test_rising_market_hits_target_net_below_gross():
    bars = [bar(i, 100, 100.5, 99.5, 100) for i in range(61)]         # flat warmup
    # entry fills at bar 61 open; then a steady climb to the target
    for i in range(61, 75):
        px = 100 + (i - 60) * 1.0
        bars.append(bar(i, px, px + 2, px - 0.2, px))
    bt = Backtester(CostModel(), make_risk(), time_stop_bars=10)
    res = bt.run(bars, _EnterLongOnce(60), warmup=60)
    assert len(res.trades) == 1
    t = res.trades[0]
    assert t.exit_reason == "target"
    assert t.gross_pnl > 0
    assert t.fees > 0
    assert t.net_pnl == pytest.approx(t.gross_pnl - t.fees - t.funding)
    assert t.net_pnl < t.gross_pnl


def test_falling_market_hits_stop():
    bars = [bar(i, 100, 100.5, 99.5, 100) for i in range(61)]
    for i in range(61, 75):
        px = 100 - (i - 60) * 1.0
        bars.append(bar(i, px, px + 0.2, px - 2, px))
    bt = Backtester(CostModel(), make_risk(), time_stop_bars=10)
    res = bt.run(bars, _EnterLongOnce(60), warmup=60)
    assert res.trades[0].exit_reason == "stop"
    assert res.trades[0].gross_pnl < 0


# ---------------- metrics (§5.5) ----------------

def test_deflated_sharpe_penalises_more_trials():
    rets = [0.01, -0.005, 0.012, 0.004, -0.002, 0.009, 0.006, -0.001, 0.008, 0.003] * 4
    one = metrics.deflated_sharpe(rets, n_trials=1, trial_sr_std=0.5)
    many = metrics.deflated_sharpe(rets, n_trials=200, trial_sr_std=0.5)
    assert many < one          # multiple-testing correction lowers confidence


def test_max_drawdown():
    assert metrics.max_drawdown_pct([100, 120, 90, 110]) == pytest.approx(25.0)


# ---------------- validation gate (§5.5) ----------------

def _trade(pnl, t):
    return Trade(strategy_id="s", direction="LONG", entry_time=t, exit_time=t,
                 entry_price=100, exit_price=100 + pnl, qty=1, exit_reason="target",
                 gross_pnl=pnl, fees=0.0, funding=0.0)


def _result(trades, equity=10_000.0):
    curve, eq = [], equity
    for t in trades:
        eq += t.net_pnl
        curve.append((t.exit_time, eq))
    return BacktestResult(trades=trades, equity_curve=curve, starting_equity=equity,
                          bars_evaluated=1000, warmup_bars=60)


def test_broken_strategy_is_rejected():
    trades = [_trade(-5, T0 + timedelta(hours=i)) for i in range(50)]
    v = validate(_result(trades), regime_of=lambda t: "uptrend")
    assert not v.passed
    assert "nonpositive_net_expectancy" in v.reasons


def test_too_few_trades_rejected():
    trades = [_trade(10, T0 + timedelta(hours=i)) for i in range(5)]
    v = validate(_result(trades), regime_of=lambda t: "uptrend")
    assert not v.passed
    assert any(r.startswith("insufficient_trades") for r in v.reasons)


def test_single_regime_winner_rejected_for_instability():
    # profitable, many trades, but ALL in one regime → fails stability
    trades = [_trade(10, T0 + timedelta(hours=i)) for i in range(60)]
    v = validate(_result(trades), regime_of=lambda t: "uptrend")
    assert any(r.startswith("unstable_across_regimes") for r in v.reasons)


# ---------------- regimes ----------------

def test_regime_map_labels_trend_and_range():
    up = [bar(i, 100 + i, 100 + i, 100 + i, 100 + i) for i in range(80)]
    rm = build_regime_map(up, lookback=60, flat_threshold=0.002)
    of = regime_lookup(rm)
    assert of(up[70].close_time) == "uptrend"
    assert of(up[10].close_time) == "range"      # inside warmup


# ---------------- coverage gate (§2.4) ----------------

@pytest.mark.asyncio
async def test_incomplete_coverage_window_refused():
    async def fake_summary(feed, start, end):
        return {"feed": feed, "missing_slots": 12, "expected_slots": 100,
                "first_gap": start.isoformat(), "window_start": start.isoformat(),
                "window_end": end.isoformat(), "complete_pct": 88.0}
    with patch("botmaximus.pipeline.coverage.summary", side_effect=fake_summary):
        with pytest.raises(CoverageError):
            await data.assert_coverage("btc_ohlcv_1m", T0, T0 + timedelta(hours=2))
        # allow_gaps returns the summary instead of raising
        s = await data.assert_coverage("btc_ohlcv_1m", T0, T0 + timedelta(hours=2),
                                       allow_gaps=True)
        assert s["missing_slots"] == 12


# ---------------- reproducibility (§5.6) ----------------

def test_config_hash_is_deterministic_and_order_independent():
    a = {"strategy_id": "x", "slippage_bps": 1.0, "latency_bars": 1}
    b = {"latency_bars": 1, "strategy_id": "x", "slippage_bps": 1.0}
    assert store.config_hash(a) == store.config_hash(b)
    c = {**a, "slippage_bps": 2.0}
    assert store.config_hash(c) != store.config_hash(a)
