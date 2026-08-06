r"""Prompt context builders for both roles (§2.C, §3.C).

Context is assembled **in priority order**, highest-value first, so that if
anything is ever truncated the schema and registry survive and the optional
digests are what is lost. Truncation that drops the feature registry produces
hallucinated features; truncation that drops last week's rejection digest
produces a slightly less well-aimed proposal.

Both builders finish by running `guards.assert_clean`, so an exclusion
violation raises here rather than being discovered in a prompt log later.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone

from botmaximus.config import settings
from botmaximus.llm import guards


@dataclass
class BuiltContext:
    role: str
    context: dict
    context_hash: str
    rendered: str
    decision_time: datetime | None = None
    notes: list[str] = field(default_factory=list)


# =====================================================================
# Generator (§2.C)
# =====================================================================
async def build_generator_context(db, n_requested: int,
                                  regime: str | None = None,
                                  analogs: list[dict] | None = None,
                                  events: list[dict] | None = None,
                                  now: datetime | None = None) -> BuiltContext:
    from botmaximus.backtest.regimes import REGIME_BUCKETS
    from botmaximus.features.registry import FEATURE_REGISTRY
    from botmaximus.llm import ledger_reads

    now = now or datetime.now(timezone.utc)

    pop = await ledger_reads.population_summary(db)
    failures = await ledger_reads.recent_failure_kinds(db)
    accepts = await ledger_reads.recent_acceptance_signatures(db)
    coverage = await ledger_reads.coverage_summary(
        db, ["btc_ohlcv_1m", "btc_funding_8h", "btc_oi_5m",
             "btc_orderbook", "btc_liquidation"])

    ctx = {
        # 1-3: non-negotiable. Unknown features are hallucination, so the
        # registry is never summarised or abbreviated.
        "dsl_schema_fields": _schema_fields(),
        "feature_registry": sorted(FEATURE_REGISTRY),
        "regime_buckets": REGIME_BUCKETS,
        # 4-5: where to aim.
        "current_regime": regime,
        "coverage_by_feed": coverage,
        "population": {
            "live_count": pop.live_count,
            "by_regime": pop.by_regime,
            "by_feature_family": pop.by_feature_family,
            "gaps": pop.gaps,
        },
        # 6-7: the coarsened feedback loop. Names and signatures only.
        "recent_failure_kinds": failures,
        "recent_acceptance_signatures": accepts,
        # 8-9: framing, bounded.
        "events": events or [],
        "analogs": [_scrub_analog(a) for a in (analogs or [])],
        # 10
        "requested_proposals": min(n_requested,
                                   settings.candidate_cap_per_cycle or n_requested),
        "symbol": settings.symbol,
    }

    guards.assert_clean(ctx, decision_time=now)
    return BuiltContext("generator", ctx, guards.context_hash(ctx),
                        _render(ctx), now)


def _schema_fields() -> list[str]:
    from botmaximus.strategy.schema import StrategyDefinition
    return sorted(StrategyDefinition.__dataclass_fields__)


# =====================================================================
# Scrutiny (§3.C)
# =====================================================================
async def build_scrutiny_context(db, intent, state: dict,
                                 analogs: list[dict],
                                 rationale: str = "",
                                 events: list[dict] | None = None,
                                 now: datetime | None = None) -> BuiltContext:
    """Compact by design — scrutiny is on the hot path with an 800ms budget.

    State arrives as **bucket labels, not raw numbers**: buckets are stable and
    auditable, and raw prices invite arithmetic that looks like analysis.
    """
    from botmaximus.llm import ledger_reads

    now = now or datetime.now(timezone.utc)
    digest = await ledger_reads.scrutiny_outcome_digest(db)

    ctx = {
        "intent": {
            "strategy_id": intent.strategy_id,
            "direction": intent.direction,
            "entry_price": intent.entry_price,
            "stop_price": intent.stop_price,
            "expected_edge_pct": intent.expected_edge_pct,
            "expected_cost_pct": intent.expected_cost_pct,
        },
        "state_buckets": {
            "regime": state.get("regime"),
            "spread": state.get("spread_bucket"),
            "volatility": state.get("vol_bucket"),
            "funding": state.get("funding_bucket"),
            "book_imbalance": state.get("imbalance_bucket"),
        },
        "analogs": [_scrub_analog(a) for a in analogs],
        "events": events or [],
        "recent_verdict_outcomes": digest,
        "strategy_rationale": rationale[:400],
    }

    guards.assert_clean(ctx, decision_time=now)
    return BuiltContext("scrutiny", ctx, guards.context_hash(ctx),
                        _render(ctx), now)


def _scrub_analog(a: dict) -> dict:
    """Analog text is data, never instructions (§5)."""
    out = dict(a)
    for k in ("rationale", "note", "description", "text"):
        if k in out:
            out[k] = guards.scrub_analog_text(out[k])
    return out


def _render(ctx: dict) -> str:
    import json
    return json.dumps(ctx, indent=1, default=str, sort_keys=False)
