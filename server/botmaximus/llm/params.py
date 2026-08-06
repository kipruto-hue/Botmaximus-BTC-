r"""Decoding parameter profiles — two of them, deliberately not interchangeable.

§1: *Do not share a `TEMPERATURE` constant between them.* The two roles want
opposite behaviour, and a shared constant is how that distinction quietly dies:

- **Generator** wants exploration inside a safe grammar. Its structural
  constraints come from the DSL, so temperature can be high without producing
  nonsense — and if it is low the model converges on a few familiar templates
  that the diversity gate rejects, burning trials and raising the statistical
  bar for everything after.
- **Scrutiny** wants the same answer to the same setup. Consistency is the
  safety property. A veto layer that answers differently to identical inputs is
  a second noise source wearing a safety badge.

Every value is orchestrator-owned (§5). Nothing in a model response may change
one; `reject_model_parameter_suggestions` exists to make that refusal explicit
and countable rather than merely assumed.
"""
from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import asdict, dataclass

from botmaximus.config import settings

log = logging.getLogger(__name__)

#: Documented safe ranges (§2.A, §3.A). Outside these the failure modes are
#: known: too cold and the generator repeats itself, too hot and malformed-JSON
#: rate climbs; any scrutiny temperature above ~0.3 costs verdict consistency.
GEN_TEMPERATURE_RANGE = (0.7, 1.1)
GEN_TOP_P_RANGE = (0.9, 1.0)
SCR_TEMPERATURE_RANGE = (0.0, 0.3)


@dataclass(frozen=True)
class DecodingProfile:
    role: str
    temperature: float
    top_p: float
    max_output_tokens: int
    stop_sequences: tuple[str, ...]
    seed: int | None
    #: Always 1. Sampling several and picking a "best" hides which prompt+seed
    #: produced which output and destroys reproducibility (§6).
    n: int = 1

    def __post_init__(self) -> None:
        if self.n != 1:
            raise ValueError(
                "n must be 1. Sampling n>1 and choosing a winner is "
                "model-directed selection that cannot be audited or reproduced.")
        if self.max_output_tokens < 100:
            raise ValueError(
                "max_output_tokens too low — truncation is a silent corruption "
                "mode: the response parses as far as it got and the rest is "
                "simply missing.")

    def fingerprint(self) -> str:
        """Stamped into provenance so a verdict or proposal can always be tied
        to the exact decoding settings that produced it."""
        blob = json.dumps(asdict(self), sort_keys=True, default=str).encode()
        return hashlib.sha256(blob).hexdigest()[:16]

    def as_api_kwargs(self) -> dict:
        return {"temperature": self.temperature, "top_p": self.top_p,
                "max_output_tokens": self.max_output_tokens,
                "stop": list(self.stop_sequences), "seed": self.seed, "n": self.n}


def generator_profile(seed: int | None = None) -> DecodingProfile:
    t, p = settings.gen_temperature, settings.gen_top_p
    _assert_range("gen_temperature", t, GEN_TEMPERATURE_RANGE)
    _assert_range("gen_top_p", p, GEN_TOP_P_RANGE)
    return DecodingProfile(
        role="generator", temperature=t, top_p=p,
        max_output_tokens=settings.gen_max_output_tokens,
        stop_sequences=(settings.gen_stop_sequences,),
        seed=seed if seed is not None else settings.gen_seed,
    )


def scrutiny_profile() -> DecodingProfile:
    t = settings.scr_temperature
    _assert_range("scr_temperature", t, SCR_TEMPERATURE_RANGE)
    return DecodingProfile(
        role="scrutiny", temperature=t, top_p=settings.scr_top_p,
        max_output_tokens=settings.scr_max_output_tokens,
        stop_sequences=(settings.scr_stop_sequences,),
        #: Fixed, not rotated: identical inputs must give identical verdicts.
        seed=settings.scr_seed,
    )


def _assert_range(name: str, value: float, bounds: tuple[float, float]) -> None:
    lo, hi = bounds
    if not (lo <= value <= hi):
        raise ValueError(
            f"{name}={value} is outside the documented safe range {lo}-{hi}. "
            f"Moving it is an operator decision that needs a measured "
            f"calibration cycle behind it, not a nudge to make output look "
            f"better.")


PARAM_WORDS = ("temperature", "top_p", "top-p", "max_tokens", "max_output_tokens",
               "seed", "frequency_penalty", "presence_penalty", "sampling")


def reject_model_parameter_suggestions(response_text: str) -> bool:
    """The model does not control its own decoding (§5).

    Returns True when a response appears to be asking for a parameter change.
    The caller ignores the suggestion either way — this exists so the attempt is
    *counted* rather than silently discarded, because a model that starts asking
    is a signal about the prompt.
    """
    lowered = response_text.lower()
    hits = [w for w in PARAM_WORDS if w in lowered]
    if hits:
        log.warning("model response mentioned decoding parameters %s — ignored", hits)
        return True
    return False
