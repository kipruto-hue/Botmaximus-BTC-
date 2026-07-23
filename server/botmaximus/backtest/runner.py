"""End-to-end backtest runner: coverage-gate → load → replay → validate →
persist. Ties the §5 pieces together for a single strategy over a window.

Pass C will drive this from the strategy lifecycle; for now it is exercised by
tests and callable ad hoc. Sizing goes through the real RiskCore (size_intent
is pure — no venue, no kills touched during a backtest).
"""
from __future__ import annotations

from datetime import datetime

from botmaximus.backtest import data, store
from botmaximus.backtest.costs import CostModel
from botmaximus.backtest.engine import Backtester
from botmaximus.backtest.regimes import build_regime_map, regime_lookup
from botmaximus.backtest.strategy import Strategy
from botmaximus.backtest.validation import validate
from botmaximus.config import settings


async def run_backtest(strategy: Strategy, start: datetime, end: datetime,
                       allow_gaps: bool = False, warmup: int = 60,
                       persist: bool = True) -> dict:
    # §2.4 — refuse phantom-coverage windows for every required feed
    coverage_summaries = {}
    for feed in strategy.required_feeds:
        coverage_summaries[feed] = await data.assert_coverage(feed, start, end, allow_gaps)

    bars = await data.load_ohlcv(start, end)
    if len(bars) <= warmup:
        raise data.CoverageError(f"only {len(bars)} bars — need > warmup {warmup}")
    funding = await data.load_funding(start, end)

    from botmaximus.db.mongo import get_db
    from botmaximus.risk.core import RiskCore
    risk = RiskCore(get_db())
    cost_model = CostModel(funding=funding)
    bt = Backtester(cost_model, risk)
    result = bt.run(bars, strategy, warmup=warmup)

    regime_of = regime_lookup(build_regime_map(bars))
    verdict = validate(result, regime_of)

    config = {
        "strategy_id": strategy.id,
        "window": [start.isoformat(), end.isoformat()],
        "taker_fee_rate": settings.taker_fee_rate,
        "slippage_bps": settings.slippage_bps,
        "latency_bars": settings.latency_bars,
        "time_stop_bars": result.params["time_stop_bars"],
        "min_trades": settings.bt_min_trades,
        "candidate_trials": settings.bt_candidate_trials,
    }
    doc = store.build_run_doc(strategy.id, config, coverage_summaries, result, verdict)
    if persist:
        await store.save_run(doc)

    return {
        "config_hash": doc["config_hash"],
        "verdict": {"passed": verdict.passed, "reasons": verdict.reasons},
        "metrics": verdict.metrics,
        "coverage": coverage_summaries,
    }
