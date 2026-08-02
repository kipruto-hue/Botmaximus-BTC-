"""Strategy lifecycle: candidate → paper → micro → full → retired.

Two rules carry the safety weight:

- **Leaving `candidate` requires a passing validation verdict.** Not a rationale,
  not a generator's confidence, not a good-looking equity curve — the §5.5 gate,
  measured gate-off on coverage-complete windows. `promote()` cannot be called
  without one.
- **`retired` is terminal.** §7.4: a decayed strategy is retired and its repair
  is a *new* candidate that re-earns its way through the whole gate. There is no
  transition out of retired, so "just un-retire it" is not something a future
  caller can accidentally do.

Transitions are advance-one-step-at-a-time. Retirement is the exception — a
strategy can be retired from any live state, because that is the safe direction.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

from botmaximus.backtest.validation import ValidationVerdict

STATES = ("candidate", "paper", "micro", "full", "retired")

#: The only forward moves. Each is one rung; nothing skips paper.
_PROMOTIONS = {"candidate": "paper", "paper": "micro", "micro": "full"}


class LifecycleError(Exception):
    """An illegal transition. Raised, never logged-and-continued — a silent
    no-op here would leave a strategy in a state its caller does not expect."""


@dataclass(frozen=True)
class Transition:
    strategy_id: str
    from_state: str
    to_state: str
    reason: str
    at: datetime
    verdict: dict | None = None

    def to_doc(self) -> dict:
        return {
            "strategy_id": self.strategy_id,
            "from_state": self.from_state,
            "to_state": self.to_state,
            "reason": self.reason,
            "at": self.at,
            "verdict": self.verdict,
        }


def _now() -> datetime:
    return datetime.now(timezone.utc)


def next_state(current: str) -> str | None:
    return _PROMOTIONS.get(current)


def promote(strategy_id: str, current: str,
            verdict: ValidationVerdict | None) -> Transition:
    """Advance one rung. Leaving `candidate` demands a passing §5.5 verdict."""
    if current not in STATES:
        raise LifecycleError(f"unknown_state:{current}")
    if current == "retired":
        raise LifecycleError("retired_is_terminal")          # §7.4
    target = _PROMOTIONS.get(current)
    if target is None:
        raise LifecycleError(f"no_promotion_from:{current}")

    if current == "candidate":
        if verdict is None:
            raise LifecycleError("promotion_requires_validation_verdict")
        if not verdict.passed:
            raise LifecycleError(
                "promotion_requires_passing_verdict:" + ",".join(verdict.reasons))

    return Transition(strategy_id, current, target,
                      reason="validated" if current == "candidate" else "graduated",
                      at=_now(),
                      verdict=({"passed": verdict.passed, "reasons": verdict.reasons,
                                "metrics": verdict.metrics} if verdict else None))


def retire(strategy_id: str, current: str, reason: str) -> Transition:
    """Retire from any live state. Always available — this is the safe direction,
    and a kill path that can itself be blocked is not a kill path."""
    if current not in STATES:
        raise LifecycleError(f"unknown_state:{current}")
    if current == "retired":
        raise LifecycleError("already_retired")
    if not reason:
        raise LifecycleError("retirement_requires_reason")
    return Transition(strategy_id, current, "retired", reason=reason, at=_now())


def reject(strategy_id: str, verdict: ValidationVerdict) -> Transition:
    """A candidate that failed the gate. Recorded rather than deleted: the
    rejection reasons are the training signal for the Pass-C2 generator, and a
    population with no memory of what failed proposes it again."""
    if verdict.passed:
        raise LifecycleError("cannot_reject_a_passing_verdict")
    return Transition(strategy_id, "candidate", "retired",
                      reason="validation_failed:" + ",".join(verdict.reasons),
                      at=_now(),
                      verdict={"passed": False, "reasons": verdict.reasons,
                               "metrics": verdict.metrics})
