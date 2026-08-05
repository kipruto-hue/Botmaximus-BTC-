r"""Scrutiny provider interface and verdict type.

A provider answers one question — *is there evidence against this trade?* — and
may only answer `APPROVE` or `VETO`. It cannot change size, price, direction,
stop, target or strategy. That restriction is the whole design: a layer that can
*modify* an order is a second trading strategy wearing a safety badge, and its
judgement would never be validated the way a real strategy's is.

Two implementations:

- `RuleBasedAnalog` (this build) — retrieves historical windows resembling the
  present and vetoes when they went badly.
- `LLMProvider` (v2) — wired, guarded, and unreachable. Its constructor demands
  credentials that this build does not use, so selecting it is a deliberate act
  rather than a config drift.

**Every failure mode is a VETO**: timeout, exception, malformed output, no
evidence. A gate that approves when it cannot decide is not a gate.
"""
from __future__ import annotations

import abc
from dataclasses import dataclass, field
from typing import Any

APPROVE = "APPROVE"
VETO = "VETO"


@dataclass(frozen=True)
class ScrutinyVerdict:
    verdict: str                        # APPROVE | VETO
    reason: str
    provider: str
    provider_version: str
    latency_ms: float = 0.0
    evidence: dict[str, Any] = field(default_factory=dict)

    @property
    def approved(self) -> bool:
        return self.verdict == APPROVE


class ScrutinyProvider(abc.ABC):
    name = "base"

    @property
    @abc.abstractmethod
    def version(self) -> str:
        """Hash or tag identifying the exact ruleset / model + prompt.

        Recorded on every verdict so a future audit can ask *why did it decide
        that* and get an answer that is still true after the rules change.
        """

    @abc.abstractmethod
    async def evaluate(self, context: dict) -> ScrutinyVerdict:
        """Return a verdict. Must not raise; return a VETO instead."""


class LLMProvider(ScrutinyProvider):
    """v2. Wired but unreachable in this build (constitution §0, §11).

    Construction requires the credentials this build deliberately does not use,
    so reaching it is impossible by accident. The class exists now so that
    switching later swaps one implementation rather than rewriting the gate.
    """

    name = "llm"

    def __init__(self) -> None:
        from botmaximus.config import settings
        settings.require("openai_api_key", "generation_llm")
        self._model = settings.generation_llm

    @property
    def version(self) -> str:
        return f"llm:{self._model}"

    async def evaluate(self, context: dict) -> ScrutinyVerdict:
        raise NotImplementedError(
            "LLM scrutiny is v2. This build ships the rule-based analog engine; "
            "no code path selects this provider.")
