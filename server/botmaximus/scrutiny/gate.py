r"""Layer E — the scrutiny gate.

Order of operations, and it is not negotiable (constitution §5.H):

    risk-core pre-check → arbiter → **scrutiny** → risk-core final approve → execute

Scrutiny sits *after* the risk core has already conditionally approved and
*before* the final check. It can only subtract. There is deliberately no path
by which an APPROVE overrides a deterministic risk block — the deterministic
checks run here first, and a failure among them short-circuits to VETO without
ever consulting the provider.

## Timeout defaults to VETO

The provider gets `scrutiny_latency_budget_ms`. Exceeding it is a VETO, not a
wait: a trade whose justification arrives late is a trade justified by
conditions that have already changed. Timeout, exception and malformed output
are counted separately in telemetry, because they mean different things — a
slow provider is a capacity problem, a raising one is a bug.
"""
from __future__ import annotations

import asyncio
import logging
from dataclasses import asdict
from datetime import datetime, timezone

from botmaximus.config import settings
from botmaximus.obs import degradation
from botmaximus.risk import freshness
from botmaximus.risk.state import OrderIntent
from botmaximus.scrutiny.provider import VETO, ScrutinyProvider, ScrutinyVerdict

log = logging.getLogger(__name__)

SCRUTINY_EVENTS = "scrutiny_events"


def build_provider() -> ScrutinyProvider:
    """`rules` in this build. `llm` exists but is unreachable — its constructor
    demands credentials this build does not use, so selecting it is a
    deliberate act rather than a config drift."""
    if settings.scrutiny_provider == "rules":
        from botmaximus.scrutiny.analog import RuleBasedAnalog
        return RuleBasedAnalog()
    if settings.scrutiny_provider == "llm":
        from botmaximus.scrutiny.provider import LLMProvider
        return LLMProvider()
    raise RuntimeError(
        f"unknown scrutiny_provider {settings.scrutiny_provider!r}")


class ScrutinyGate:
    def __init__(self, risk_core, provider: ScrutinyProvider | None = None) -> None:
        self.risk = risk_core
        self.provider = provider or build_provider()

    async def review(self, intent: OrderIntent, context: dict) -> ScrutinyVerdict:
        deterministic = self._deterministic_block(intent)
        if deterministic:
            verdict = ScrutinyVerdict(
                VETO, deterministic, "deterministic", "n/a", 0.0)
            await self._record(intent, verdict)
            return verdict

        try:
            verdict = await asyncio.wait_for(
                self.provider.evaluate({**context,
                                        "direction": intent.direction}),
                timeout=settings.scrutiny_latency_budget_ms / 1000)
        except asyncio.TimeoutError:
            await degradation.record(
                "scrutiny_timeout",
                f"provider exceeded {settings.scrutiny_latency_budget_ms}ms — "
                f"vetoing, because a justification that arrives late describes "
                f"conditions that have already changed",
                strategy_id=intent.strategy_id)
            verdict = ScrutinyVerdict(VETO, "provider_timeout",
                                      self.provider.name, self.provider.version,
                                      settings.scrutiny_latency_budget_ms)
        except Exception as e:                          # noqa: BLE001
            await degradation.record("scrutiny_provider_error", str(e),
                                     strategy_id=intent.strategy_id)
            verdict = ScrutinyVerdict(VETO, f"provider_error:{e}",
                                      self.provider.name, self.provider.version)
        else:
            if verdict.verdict not in ("APPROVE", "VETO"):
                await degradation.record(
                    "scrutiny_malformed_verdict", repr(verdict.verdict))
                verdict = ScrutinyVerdict(VETO, "malformed_verdict",
                                          self.provider.name,
                                          self.provider.version)

        await self._record(intent, verdict)
        return verdict

    def _deterministic_block(self, intent: OrderIntent) -> str | None:
        """Run before the provider and unoverridable by it."""
        block = self.risk.kills.blocks_trading()
        if block:
            return block
        suspended = self.risk.kills.strategy_suspended(intent.strategy_id)
        if suspended:
            return f"L1_strategy_suspended:{suspended}"
        stale = freshness.assert_fresh(intent.required_feeds
                                       or freshness.BASELINE_FEEDS)
        if stale:
            return ",".join(stale)
        edge = self.risk._check_edge_over_cost(intent)
        if edge:
            return ",".join(edge)
        return None

    async def _record(self, intent: OrderIntent, verdict: ScrutinyVerdict) -> None:
        doc = {
            "at": datetime.now(timezone.utc),
            "strategy_id": intent.strategy_id,
            "direction": intent.direction,
            **asdict(verdict),
        }
        try:
            from botmaximus.db.mongo import get_db
            await get_db()[SCRUTINY_EVENTS].insert_one(doc)
        except Exception as e:                          # noqa: BLE001
            await degradation.record("scrutiny_event_write_failed", str(e))


async def ensure_indexes() -> None:
    from botmaximus.db.mongo import get_db
    db = get_db()
    await db[SCRUTINY_EVENTS].create_index([("at", -1)])
    await db[SCRUTINY_EVENTS].create_index([("verdict", 1), ("at", -1)])
