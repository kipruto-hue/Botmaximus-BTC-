r"""Layer C2 — the generator. Proposes strategies; judges nothing.

It emits `StrategyDefinition` JSON and nothing else. It never emits code, never
executes anything, never promotes, never allocates. Everything it produces goes
through the same validation path as a hand-written seed.

## Two rules that exist because of the audit

**Trial-per-attempt (A3).** Every proposal that reaches validation registers a
lifetime trial *first*, via `strategy.trials.record`, on the DSL path that feeds
`validate(n_trials=…)`. First proposals, retries after malformed output, and
repairs all count. Without this the deflated Sharpe is corrected for one trial
while a generator runs thousands — the gate would certify noise and report high
confidence, which is worse than having no gate because it looks like one.

A proposal rejected by the **diversity** check does *not* consume a trial: it
never entered the gate, so counting it would inflate the correction rather than
sharpen it.

**Coarsened feedback (A2).** The generator sees the *names* of failing checks —
`deflated_sharpe_below_threshold`, `insufficient_trades` — never the margins.
`deflated_sharpe_below_threshold:0.412<0.95` tells an optimiser exactly how far
to push, and a generator that receives it is no longer proposing hypotheses; it
is hill-climbing the validator. Full reasons are still stored for humans. The
coarsening happens here, at the boundary, so there is one place to audit.

## Provenance is a precondition, not a record

A candidate with no provenance file must not be validated. Reproducing a
strategy later requires the prompt, the response, the model id and the sampling
params; a strategy that cannot be reproduced cannot be audited, and an
unauditable strategy has no business holding a position.
"""
from __future__ import annotations

import hashlib
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from botmaximus.config import settings
from botmaximus.obs import degradation
from botmaximus.strategy import trials
from botmaximus.strategy.schema import StrategyDefinition
from botmaximus.strategy.validator import parse_and_validate, signature, similarity

log = logging.getLogger(__name__)

GENERATIONS_DIR = Path(__file__).resolve().parents[3] / "data" / "generations"


def coarsen(reasons: list[str]) -> list[str]:
    """Failing check NAMES only — never the margins (audit A2).

    `deflated_sharpe_below_threshold:0.412<0.95` -> `deflated_sharpe_below_threshold`
    """
    return sorted({r.split(":", 1)[0] for r in reasons})


@dataclass
class Proposal:
    definition: StrategyDefinition
    generation_id: str
    parent_id: str | None = None
    lineage_depth: int = 0
    provenance_path: Path | None = None


@dataclass
class GenerationOutcome:
    proposed: int = 0
    duplicates: int = 0
    malformed: int = 0
    accepted: list[Proposal] = field(default_factory=list)

    @property
    def acceptance_ratio(self) -> float:
        return len(self.accepted) / self.proposed if self.proposed else 0.0


class Proposer:
    """Produces raw candidate payloads (dicts). Knows nothing about judging."""

    name = "base"

    async def propose(self, brief: dict, n: int) -> list[dict]:
        raise NotImplementedError


class NullProposer(Proposer):
    """Deterministic seed-mutation proposer.

    Ships so that every layer downstream — trials, provenance, diversity,
    validation, lifecycle — can be exercised end to end without an LLM. Its
    proposals are structurally valid and economically unremarkable, which is
    exactly right for a smoke test: any *survivor* it produces is evidence of a
    leak in the gate, not evidence of an edge.
    """

    name = "null"

    def __init__(self, seed: int = 0) -> None:
        self._seed = seed

    async def propose(self, brief: dict, n: int) -> list[dict]:
        from botmaximus.strategy.seeds import SEED_PAYLOADS

        out = []
        for i in range(n):
            base = json.loads(json.dumps(SEED_PAYLOADS[i % len(SEED_PAYLOADS)]))
            base["id"] = f"gen_{self._seed}_{i}_{uuid.uuid4().hex[:6]}"
            base["origin"] = "generated"
            base["rationale"] = (
                base.get("rationale", "") +
                " [NullProposer mutation: emitted to exercise the validation "
                "path; carries no economic claim of its own.]")
            out.append(base)
        return out


class LLMProposer(Proposer):
    """GPT-5.5. Wired, guarded, and not called in this build (§0).

    The constructor demands credentials this build does not use, so the live
    path is unreachable by accident rather than by convention.

    Decoding parameters come from `llm/params.generator_profile()` — high
    temperature inside the DSL's hard grammar, which is the combination that
    gives novelty without incoherence. They are orchestrator-owned: nothing in
    a model response may change them.
    """

    name = "llm"

    def __init__(self, seed: int | None = None) -> None:
        from botmaximus.llm import params, prompts

        settings.require("openai_api_key", "generation_llm")
        self.model = settings.generation_llm
        self.profile = params.generator_profile(seed)
        self.system_prompt = prompts.generator()

    async def propose(self, brief: dict, n: int) -> list[dict]:
        from botmaximus.llm import client

        # Raises LLMUnavailable: the record is assembled in full so provenance
        # and parameter plumbing are exercised, and only the network step is
        # missing — on purpose.
        raise_record = client.LLMCallRecord(
            role="generator", model_id=self.model,
            prompt_version=self.system_prompt.version,
            system_prompt_hash=self.system_prompt.sha,
            context_hash="", profile_fingerprint=self.profile.fingerprint(),
            temperature=self.profile.temperature, top_p=self.profile.top_p,
            max_output_tokens=self.profile.max_output_tokens,
            stop_sequences=list(self.profile.stop_sequences),
            seed=self.profile.seed, n=self.profile.n)
        raise client.LLMUnavailable(
            f"LLM generation is not called in this build "
            f"(profile {raise_record.profile_fingerprint}, prompt "
            f"{raise_record.prompt_version}). The interface, parameter profile, "
            f"provenance capture and trial accounting are all in place so that "
            f"enabling it later swaps one implementation.")


class Generator:
    def __init__(self, proposer: Proposer | None = None,
                 generations_dir: Path | None = None) -> None:
        self.proposer = proposer or NullProposer()
        self.dir = generations_dir or GENERATIONS_DIR
        self.dir.mkdir(parents=True, exist_ok=True)

    # ---- brief -------------------------------------------------------
    async def build_brief(self, population: list[dict] | None = None,
                          recent_failures: list[list[str]] | None = None) -> dict:
        """What the proposer is told. Feedback is coarsened here, at the
        boundary, so there is exactly one place to audit for leakage."""
        from botmaximus.backtest.regimes import REGIME_BUCKETS
        from botmaximus.features.registry import FEATURE_REGISTRY

        coarse = sorted({name for reasons in (recent_failures or [])
                         for name in coarsen(reasons)})
        return {
            "features": sorted(FEATURE_REGISTRY),
            "regime_buckets": REGIME_BUCKETS,
            "population_ids": [p.get("strategy_id") for p in (population or [])],
            "recent_failure_kinds": coarse,     # NAMES only, never margins
            "symbol": settings.symbol,
        }

    # ---- generation --------------------------------------------------
    async def generate(self, n: int | None = None,
                       population: list[dict] | None = None,
                       recent_failures: list[list[str]] | None = None,
                       parent: StrategyDefinition | None = None,
                       lineage_depth: int = 0) -> GenerationOutcome:
        cap = n if n is not None else settings.candidate_cap_per_cycle
        if cap is None:
            raise RuntimeError(
                "CANDIDATE_CAP_PER_CYCLE is unset. It is not a cost control — "
                "the cap is what keeps the multiple-testing correction honest, "
                "so there is no safe default to invent.")

        if lineage_depth > settings.lineage_repair_cap:
            raise RuntimeError(
                f"lineage_depth {lineage_depth} exceeds LINEAGE_REPAIR_CAP "
                f"{settings.lineage_repair_cap} — this lineage is retired. "
                f"Repeated repairs against the same gate is chained Goodharting.")

        brief = await self.build_brief(population, recent_failures)
        raw = await self.proposer.propose(brief, cap)

        outcome = GenerationOutcome(proposed=len(raw))
        existing = await self._existing_signatures()

        for payload in raw:
            defn, res = parse_and_validate(payload)
            if defn is None or not res.ok:
                outcome.malformed += 1
                await degradation.record(
                    "generator_malformed_proposal",
                    "proposal failed DSL parsing/validation and was discarded",
                    reasons=(res.reasons if res else ["unparseable"])[:5])
                continue

            sig = signature(defn)
            if any(similarity(sig, other) >= settings.diversity_threshold
                   for other in existing):
                # Never entered the gate, so it does not consume a trial.
                outcome.duplicates += 1
                continue
            existing.append(sig)

            gen_id = uuid.uuid4().hex
            path = await self._write_provenance(
                gen_id, defn, brief, payload, parent, lineage_depth)
            outcome.accepted.append(Proposal(
                definition=defn, generation_id=gen_id,
                parent_id=parent.id if parent else None,
                lineage_depth=lineage_depth, provenance_path=path))

        log.info("generation: proposed=%d accepted=%d duplicates=%d malformed=%d",
                 outcome.proposed, len(outcome.accepted),
                 outcome.duplicates, outcome.malformed)
        return outcome

    async def _existing_signatures(self) -> list:
        try:
            from botmaximus.strategy import store
            pairs = await store.signatures_for_dedupe()
            return [sig for _, sig in pairs]
        except Exception:                               # noqa: BLE001
            return []

    async def _write_provenance(self, gen_id: str, defn: StrategyDefinition,
                                brief: dict, raw: dict,
                                parent: StrategyDefinition | None,
                                lineage_depth: int) -> Path:
        from botmaximus.features.registry import FEATURE_REGISTRY

        doc = {
            "generation_id": gen_id,
            "strategy_id": defn.id,
            "at": datetime.now(timezone.utc).isoformat(),
            "proposer": self.proposer.name,
            "model": settings.generation_llm,
            "brief": brief,
            "raw_response": raw,
            "parent_id": parent.id if parent else None,
            "lineage_depth": lineage_depth,
            "feature_registry_hash": hashlib.sha256(
                json.dumps(sorted(FEATURE_REGISTRY)).encode()).hexdigest()[:16],
            "brief_hash": hashlib.sha256(
                json.dumps(brief, sort_keys=True, default=str).encode()
            ).hexdigest()[:16],
        }
        path = self.dir / f"{gen_id}.json"
        path.write_text(json.dumps(doc, indent=2, default=str), encoding="utf-8")
        return path

    # ---- validation entry point --------------------------------------
    async def validate_proposal(self, proposal: Proposal, start, end,
                                warmup: int = 5000, persist: bool = True) -> dict:
        """The ONLY sanctioned path from generator to validator.

        Refuses without provenance, and registers the lifetime trial before the
        verdict exists. `run_dsl_backtest` does the trial registration and
        passes the count to `validate(n_trials=…)`; asserting the provenance
        precondition here keeps both invariants at one door.
        """
        if proposal.provenance_path is None or not proposal.provenance_path.exists():
            raise RuntimeError(
                f"{proposal.definition.id} has no provenance record — refusing "
                f"to validate. A strategy that cannot be reproduced cannot be "
                f"audited, and an unauditable strategy has no business holding "
                f"a position.")

        from botmaximus.backtest.runner import run_dsl_backtest
        return await run_dsl_backtest(proposal.definition, start, end,
                                      warmup=warmup, persist=persist)


async def trial_count() -> int:
    return await trials.count()
