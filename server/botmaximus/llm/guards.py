r"""The exclusion wall: things that must never reach an LLM prompt.

§2.C and §3.C list what is deliberately withheld. Those lists are only worth
something if a violation *fails* rather than being noticed in review, so they
are enforced here and every prompt builder runs its output through
`assert_clean` before the text exists.

## What is withheld, and why each one matters

- **Validation margins.** `deflated_sharpe_below_threshold:0.412<0.95` tells an
  optimiser exactly how far to push. Names alone do not. This is the
  anti-Goodhart wall: the model cannot climb what it cannot measure precisely.
- **Profit and loss.** Feeding PnL to a model that generates strategies is a
  fast track to fitting recent noise — "what worked lately" is the noise
  fitter's paradise, and it looks like insight right up until it doesn't.
- **Kill state.** Scrutiny sits *after* the deterministic risk checks and can
  only subtract. A model that can see a block might reason about it; it must
  not be able to.
- **Raw prices where buckets belong.** Buckets are auditable and stable;
  raw numbers invite arithmetic that looks like analysis.
- **Full definitions of live strategies.** Showing the model exactly what
  passed produces imitations of it, which the diversity gate then rejects —
  wasting trials and narrowing the population toward one bet.

## Lookahead

Any record carrying an `event_time` at or after the decision bar is refused
outright. This is the same rule the point-in-time view enforces for features;
a prompt is just another way to read data, and it gets the same treatment.
"""
from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Any

#: Keys that must never appear anywhere in an assembled prompt context.
FORBIDDEN_KEYS = {
    "pnl", "net_pnl", "gross_pnl", "profit", "equity", "realized_pnl",
    "unrealized_pnl", "day_pnl", "drawdown", "drawdown_pct",
    "l1_suspended", "l2_halted", "l3_killed", "kill_state", "kills",
    "api_key", "api_secret", "secret", "token",
}

#: A validation reason carrying its margin, e.g. "deflated_sharpe:0.41<0.95".
#: Check NAMES are bare identifiers; anything with a comparator or a number
#: attached is a margin.
_MARGIN = re.compile(r"[A-Za-z_]+\s*[:=]?\s*-?\d+(\.\d+)?\s*[<>]=?\s*-?\d+(\.\d+)?")
_COMPARATOR = re.compile(r"[<>]=?")


class PromptLeak(Exception):
    """Raised when assembled context contains something withheld by design."""


def is_margin(text: str) -> bool:
    return bool(_MARGIN.search(text)) or bool(_COMPARATOR.search(text))


def assert_no_margins(names: list[str], field: str) -> None:
    bad = [n for n in names if is_margin(str(n))]
    if bad:
        raise PromptLeak(
            f"{field} contains validation margins {bad!r}. Only failing check "
            f"NAMES may reach the model: a margin tells an optimiser how far to "
            f"push, and a proposal fitted to the gate is worthless the moment "
            f"the gate changes.")


def assert_no_forbidden_keys(obj: Any, path: str = "context") -> None:
    """Walk the whole structure. A forbidden key nested three levels down is
    just as readable to the model as a top-level one."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            if str(k).lower() in FORBIDDEN_KEYS:
                raise PromptLeak(
                    f"{path}.{k} is withheld from prompts by design "
                    f"(see llm/guards.py for why this specific field matters).")
            assert_no_forbidden_keys(v, f"{path}.{k}")
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            assert_no_forbidden_keys(v, f"{path}[{i}]")


def assert_no_lookahead(records: list[dict], decision_time: datetime,
                        field: str = "event_time") -> None:
    """Refuse any record from at or after the decision bar.

    A prompt is just another way to read data. The point-in-time view already
    makes this structurally impossible for features; analogs and event calendars
    arrive by a different route and need the same rule applied explicitly.
    """
    for r in records:
        t = r.get(field)
        if t is None:
            continue
        if isinstance(t, str):
            try:
                t = datetime.fromisoformat(t)
            except ValueError:
                continue
        if t >= decision_time:
            raise PromptLeak(
                f"record {field}={t.isoformat()} is at or after the decision "
                f"bar {decision_time.isoformat()} — that is lookahead, and it "
                f"is no more acceptable in a prompt than in a feature.")


def assert_clean(context: dict, decision_time: datetime | None = None) -> dict:
    """One call every prompt builder makes before rendering text."""
    assert_no_forbidden_keys(context)
    for key in ("recent_failure_kinds", "recent_acceptance_signatures"):
        if key in context:
            assert_no_margins(list(context[key]), key)
    if decision_time is not None:
        for key in ("analogs", "events"):
            if key in context and isinstance(context[key], list):
                assert_no_lookahead(context[key], decision_time)
    return context


def scrub_analog_text(text: str) -> str:
    """Analog records are data, not instructions (§5 prompt-injection defence).

    Retrieved text describes past market conditions; it must never be able to
    direct a verdict. Wrapping rather than trusting keeps the boundary visible
    in the rendered prompt itself.
    """
    flat = " ".join(str(text).split())
    return f"[data]{flat}[/data]"


def context_hash(context: dict) -> str:
    import hashlib
    blob = json.dumps(context, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:16]
