r"""Rule-based analog engine — scrutiny provider v1.

Asks a narrow, answerable question: **when the market last looked like this, and
someone took this side, what happened next?** If the honest answer is "it went
badly, often", the trade is vetoed.

## Why analogs rather than a model

A model would need training, validation, and its own place in the trial ledger —
it would be a second strategy, and it would need to clear the same gate the
first one did. Retrieval over real history needs none of that: it makes no
prediction, it reports what happened. Its failure mode is being uninformative,
not being confidently wrong.

## Temporal isolation is not optional

Analogs are drawn strictly from before `now - embargo`. Without the embargo, the
"historical" windows would overlap the very bar being judged, and the gate would
be reading the answer off the exam paper. This is the same purge/embargo
discipline `validation.py` already applies to walk-forward folds.

## No evidence is a VETO

Fewer than `min_analogs` matches means the situation is unlike anything in two
years of history. That is a reason for caution, not a free pass — and treating
"no matching data" as approval is precisely how a gate becomes decorative.
"""
from __future__ import annotations

import hashlib
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from botmaximus.scrutiny.provider import APPROVE, VETO, ScrutinyProvider, ScrutinyVerdict

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class AnalogRules:
    """Every threshold in one place, hashed into `provider_version` so a verdict
    can always be traced to the ruleset that produced it."""
    min_analogs: int = 20
    adverse_move_pct: float = 3.0
    adverse_fraction: float = 0.75
    spread_percentile_veto: float = 90.0
    funding_veto_abs: float = 0.0005     # 5bps per 8h settlement
    embargo_s: int = 3600
    lookback_days: int = 730

    def version(self) -> str:
        blob = json.dumps(self.__dict__, sort_keys=True).encode()
        return "rules_v1:" + hashlib.sha256(blob).hexdigest()[:12]


def bucket_vol(v: float | None) -> str:
    if v is None:
        return "unknown"
    if v < 0.0005:
        return "vol_low"
    if v < 0.0015:
        return "vol_mid"
    return "vol_high"


def bucket_spread(s: float | None) -> str:
    if s is None:
        return "unknown"
    if s < 0.005:
        return "spr_tight"
    if s < 0.02:
        return "spr_mid"
    return "spr_wide"


def bucket_funding(f: float | None) -> str:
    if f is None:
        return "unknown"
    if abs(f) < 0.0001:
        return "fund_flat"
    return "fund_pos" if f > 0 else "fund_neg"


@dataclass(frozen=True)
class StateKey:
    regime: str
    vol: str
    spread: str
    funding: str
    direction: str

    def hashed(self) -> str:
        return hashlib.sha256(
            f"{self.regime}|{self.vol}|{self.spread}|{self.funding}|{self.direction}"
            .encode()).hexdigest()[:12]


class RuleBasedAnalog(ScrutinyProvider):
    name = "rules"

    def __init__(self, rules: AnalogRules | None = None,
                 bars_provider=None) -> None:
        self.rules = rules or AnalogRules()
        #: Injected so the gate is testable without a database; in production
        #: this reads the 1m series.
        self._bars_provider = bars_provider

    @property
    def version(self) -> str:
        return self.rules.version()

    async def evaluate(self, context: dict) -> ScrutinyVerdict:
        t0 = time.perf_counter()
        try:
            return await self._evaluate(context, t0)
        except Exception as e:                          # noqa: BLE001
            # A provider that raises must not take the trade with it, and must
            # not let it through either.
            log.exception("scrutiny provider raised")
            return self._veto(f"provider_exception:{e}", t0)

    async def _evaluate(self, context: dict, t0: float) -> ScrutinyVerdict:
        r = self.rules
        key = StateKey(
            regime=context.get("regime") or "unknown",
            vol=bucket_vol(context.get("volatility")),
            spread=bucket_spread(context.get("spread_pct")),
            funding=bucket_funding(context.get("funding_rate")),
            direction=context["direction"],
        )

        funding = context.get("funding_rate")
        if funding is not None and abs(funding) >= r.funding_veto_abs:
            paying = (funding > 0 and context["direction"] == "LONG") or \
                     (funding < 0 and context["direction"] == "SHORT")
            if paying:
                return self._veto(
                    f"funding_against_position:{funding:.6f}", t0, key)

        analogs = await self._find_analogs(context, key)
        if len(analogs) < r.min_analogs:
            # Missing evidence is not a pass.
            return self._veto(
                f"insufficient_analogs:{len(analogs)}<{r.min_analogs}", t0, key,
                {"analogs": len(analogs)})

        adverse = sum(1 for a in analogs if a["adverse_pct"] >= r.adverse_move_pct)
        frac = adverse / len(analogs)
        if frac >= r.adverse_fraction:
            return self._veto(
                f"analogs_adverse:{frac:.0%}>={r.adverse_fraction:.0%}", t0, key,
                {"analogs": len(analogs), "adverse": adverse})

        spread = context.get("spread_pct")
        if spread is not None:
            spreads = sorted(a["spread_pct"] for a in analogs
                             if a.get("spread_pct") is not None)
            if spreads:
                idx = int(len(spreads) * r.spread_percentile_veto / 100)
                p90 = spreads[min(idx, len(spreads) - 1)]
                if spread > p90:
                    return self._veto(
                        f"spread_above_p{r.spread_percentile_veto:.0f}", t0, key,
                        {"spread": spread, "p90": p90})

        return ScrutinyVerdict(
            APPROVE, f"analogs_ok:{len(analogs)} adverse={frac:.0%}",
            self.name, self.version,
            (time.perf_counter() - t0) * 1000,
            {"analogs": len(analogs), "adverse_fraction": round(frac, 4),
             "state_key_hash": key.hashed()})

    def _veto(self, reason: str, t0: float, key: StateKey | None = None,
              evidence: dict | None = None) -> ScrutinyVerdict:
        ev = dict(evidence or {})
        if key is not None:
            ev["state_key_hash"] = key.hashed()
        return ScrutinyVerdict(VETO, reason, self.name, self.version,
                               (time.perf_counter() - t0) * 1000, ev)

    async def _find_analogs(self, context: dict, key: StateKey) -> list[dict]:
        """Historical windows matching the state key, strictly before the
        embargo boundary."""
        if self._bars_provider is None:
            return []
        now = context.get("now") or datetime.now(timezone.utc)
        cutoff = now - timedelta(seconds=self.rules.embargo_s)
        start = now - timedelta(days=self.rules.lookback_days)
        return await self._bars_provider(key, start, cutoff, context)
