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
    # §2.4 — refuse phantom-coverage windows for every required feed.
    # `required_feeds` may be DSL feed names (ohlcv/funding/…) or raw dataset
    # ids; both resolve to the dataset the coverage ledger tracks.
    coverage_summaries = {}
    for feed in strategy.required_feeds:
        dataset = data.FEED_DATASETS.get(feed, feed)
        coverage_summaries[feed] = await data.assert_coverage(
            dataset, start, end, allow_gaps)

    bars = await data.load_ohlcv(start, end)
    if len(bars) <= warmup:
        raise data.CoverageError(f"only {len(bars)} bars — need > warmup {warmup}")
    funding = await data.load_funding(start, end)

    from botmaximus.db.mongo import get_db
    from botmaximus.risk.core import RiskCore
    risk = RiskCore(get_db())
    cost_model = CostModel(funding=funding)
    # A compiled DSL strategy carries its own exit policy; anything else gets
    # the defaults, which are the pre-Pass-C behaviour.
    bt = Backtester(cost_model, risk, policy=getattr(strategy, "exit_policy", None))
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


async def run_dsl_backtest(defn, start: datetime, end: datetime,
                           allow_gaps: bool = False, warmup: int = 200,
                           persist: bool = True) -> dict:
    """Gate 3 path: a validated StrategyDefinition → compiled → replayed →
    judged. The strategy is compiled against the *same* MarketWindow the replay
    walks, so features and bars cannot disagree about what happened when.

    `warmup` defaults higher than the OHLCV-only path because higher-timeframe
    features need many 1m bars before their first value exists (a 1h ADX(14)
    needs 2×14 closed 1h bars ≈ 1,680 minutes), and trading on a warmup-thin
    feature is trading on an artefact.
    """
    from botmaximus.strategy.compiler import compile_strategy

    coverage_summaries = {}
    for feed in defn.required_feeds:
        dataset = data.FEED_DATASETS.get(feed, feed)
        coverage_summaries[feed] = await data.assert_coverage(
            dataset, start, end, allow_gaps)

    market = await data.load_window(defn.required_feeds, start, end)
    if len(market.bars) <= warmup:
        raise data.CoverageError(
            f"only {len(market.bars)} bars — need > warmup {warmup}")

    strategy = compile_strategy(defn, market)

    from botmaximus.db.mongo import get_db
    from botmaximus.risk.core import RiskCore
    risk = RiskCore(get_db())
    bt = Backtester(CostModel(funding=market.funding), risk,
                    policy=strategy.exit_policy)
    result = bt.run(market.bars, strategy, warmup=warmup)

    regime_of = regime_lookup(build_regime_map(market.bars))
    verdict = validate(result, regime_of)

    config = {
        "strategy_id": defn.id,
        "definition": defn.to_dict(),
        "window": [start.isoformat(), end.isoformat()],
        "warmup": warmup,
        "taker_fee_rate": settings.taker_fee_rate,
        "slippage_bps": settings.slippage_bps,
        "latency_bars": settings.latency_bars,
        "min_trades": settings.bt_min_trades,
        "candidate_trials": settings.bt_candidate_trials,
        # part of the hash: changing the confirmation window changes the trades,
        # so two runs under different values must not collide
        "regime_confirm_bars": settings.regime_invalidation_confirm_bars,
    }
    doc = store.build_run_doc(defn.id, config, coverage_summaries, result, verdict)
    if persist:
        await store.save_run(doc)

    exits: dict[str, int] = {}
    for t in result.trades:
        exits[t.exit_reason] = exits.get(t.exit_reason, 0) + 1

    return {
        "config_hash": doc["config_hash"],
        "verdict": {"passed": verdict.passed, "reasons": verdict.reasons},
        "metrics": verdict.metrics,
        "coverage": coverage_summaries,
        "exit_reasons": exits,
        "signals": sum(1 for s in strategy.signals if s),
    }
