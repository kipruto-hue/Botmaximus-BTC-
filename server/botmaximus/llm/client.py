r"""LLM call orchestration — retries, counters, provenance, and the guard that
keeps the live path unreachable in this build.

**GPT-5.5 is not called here.** Finish-the-System §0 wires the key slots, the
interface and the provenance capture, and stops. `call()` raises unless the
operator has explicitly configured the credentials, and no code path in this
build supplies them.

## Retries belong to the orchestrator (§2.D)

The model never asks for another attempt. "Try again with more effort" is not a
strategy, it is a way to spend money converting one bad sample into several. The
policy table below is fixed, and every content retry that ends in failure still
**counts as a trial** — a proposal that took three attempts consumed three looks
at the data, and the deflated Sharpe has to know that.

A high retry rate is a signal to lower `GEN_TEMPERATURE`, not to raise the cap.

## Scrutiny never retries

It is on the hot path with an 800ms budget. A retry inside the budget breaks the
latency guarantee; a retry outside it answers a question about a bar that has
already gone. Timeout is a VETO and the bar is done.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone

from botmaximus.config import settings
from botmaximus.llm.params import DecodingProfile, reject_model_parameter_suggestions
from botmaximus.obs import degradation

log = logging.getLogger(__name__)

#: Failure -> (retries allowed, counts as a trial once exhausted)
RETRY_POLICY = {
    "malformed_json": (1, True),
    "feature_registry_violation": (1, True),
    "empty_response": (1, True),
    "rate_limit": (3, False),          # not a strategy failure
    "server_error": (3, False),
    "timeout": (0, False),             # fail the cycle, log, move on
}


@dataclass
class LLMCallRecord:
    role: str
    model_id: str | None
    prompt_version: str
    system_prompt_hash: str
    context_hash: str
    profile_fingerprint: str
    temperature: float
    top_p: float
    max_output_tokens: int
    stop_sequences: list[str]
    seed: int | None
    n: int
    attempts: int = 1
    failures: list[str] = field(default_factory=list)
    at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    prompt_text: str = ""
    response_text: str = ""

    def to_doc(self) -> dict:
        d = dict(self.__dict__)
        d["at"] = self.at.isoformat()
        return d


class LLMUnavailable(RuntimeError):
    """Raised when a live LLM call is attempted in a build that must not."""


async def call(role: str, system_prompt, built_context, profile: DecodingProfile,
               *, allow_live: bool = False) -> LLMCallRecord:
    """Assemble the record and refuse to reach the network.

    Everything a real call would need is computed and returned, so the plumbing
    is exercised and provenance is complete. The network step is the only part
    that is missing, and it is missing on purpose.
    """
    record = LLMCallRecord(
        role=role,
        model_id=settings.generation_llm,
        prompt_version=system_prompt.version,
        system_prompt_hash=system_prompt.sha,
        context_hash=built_context.context_hash,
        profile_fingerprint=profile.fingerprint(),
        temperature=profile.temperature,
        top_p=profile.top_p,
        max_output_tokens=profile.max_output_tokens,
        stop_sequences=list(profile.stop_sequences),
        seed=profile.seed,
        n=profile.n,
        prompt_text=built_context.rendered,
    )

    if not allow_live:
        raise LLMUnavailable(
            f"{role}: GPT-5.5 is wired but not called in this build. The key "
            f"slot, the interface, the parameter profile and the provenance "
            f"record all exist and this record is complete apart from the "
            f"response. Enabling the live path is an operator decision.")

    settings.require("openai_api_key", "generation_llm")
    raise NotImplementedError(
        "live LLM transport is v2; this build ships the rule-based analog "
        "engine for scrutiny and the NullProposer for generation.")


async def note_failure(kind: str, role: str, **context) -> tuple[int, bool]:
    """Record an LLM failure and return its retry budget.

    Each kind gets its own counter (§5): a rate-limit is a capacity problem, a
    malformed response is a prompt problem, and a hallucinated feature means the
    registry section of the prompt is unclear. Collapsing them into one
    "llm_error" number would hide which of those is happening.
    """
    retries, counts_as_trial = RETRY_POLICY.get(kind, (0, True))
    await degradation.record(f"llm_{kind}", f"{role}: {kind}", role=role, **context)
    return retries, counts_as_trial


def check_parameter_suggestion(response_text: str, role: str) -> None:
    if reject_model_parameter_suggestions(response_text):
        degradation.record_sync(
            "llm_parameter_suggestion_ignored",
            f"{role} response referenced decoding parameters; ignored. The "
            f"model does not control its own sampling.")


def hallucination_rate_alert(hallucinated: int, total: int) -> str | None:
    """Above the threshold the fix is the prompt, not the model (§2.E)."""
    if total == 0:
        return None
    rate = hallucinated / total
    if rate > settings.llm_feature_hallucination_alert_rate:
        return (f"feature-hallucination rate {rate:.1%} exceeds "
                f"{settings.llm_feature_hallucination_alert_rate:.0%} — the "
                f"feature-registry section of the prompt is unclear. Fix the "
                f"prompt, not the model.")
    return None
