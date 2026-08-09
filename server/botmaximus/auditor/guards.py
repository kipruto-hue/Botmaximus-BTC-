r"""The Auditor's exclusion wall (§1.6, §1.7, §3, §5.B).

## Why this is a second wall and not the existing one

`llm/guards.py` blocks `pnl`, `equity`, `drawdown` and kill state from reaching
a prompt. Those exist for good reasons — feeding PnL to a model that *writes
strategies* is a fast track to fitting recent noise, and Scrutiny sits after the
deterministic risk checks and may only subtract, so it must not be able to
reason about a block it can see.

But §4.A requires the Auditor to report PnL, and §4.A.4 requires it to report
kill events. Applying the generator's wall here would make the daily rundown
impossible to write.

The resolution is two walls, not a weakened one. The generator's wall exists to
stop a *generating* model optimising against what it can measure. The Auditor
generates no strategies and gates no trades — its output is a document with no
pathway to action (§1.2) — so the same fields carry a different risk. What still
must not reach it:

- **Secrets** (§1.7). Nothing changes here; a report that quotes an API key is
  a report that leaks one.
- **Live market data** (§1.6). The Auditor reports on stored facts. A current
  price in its context is the first step toward "the Auditor thinks BTC will go
  up", which §1.4 forbids outright.
- **Raw quarantined payloads** (§3). Aggregate counts and check names only.
- **Raw LLM prompt/response text from Generator or Scrutiny** (§3). Metadata is
  enough to report on; the blob is forensic material. The database withholds
  the blob keys too, so this is belt and braces.

Margins are still coarsened where §4.A.6 asks for "coarsened check names",
because a margin in a report is a margin an operator might optimise against
even if the model cannot.
"""
from __future__ import annotations

import re
from datetime import datetime
from typing import Any

#: Never, for any role.
SECRET_KEYS = {"api_key", "api_secret", "secret", "token", "password",
               "access_key", "secret_key", "postgres_dsn", "dsn"}

#: §1.6 — no live market state. These are the collector's in-process fields;
#: a stored `payload` from a ledger row is fine, a "current" anything is not.
LIVE_MARKET_KEYS = {"current_price", "live_price", "last_price", "best_bid",
                    "best_ask", "current_book", "orderbook_snapshot",
                    "current_funding", "current_oi", "live_tick"}

#: §3 — the two blob classes the Auditor may know exist but never read.
RAW_BLOB_KEYS = {"raw_response", "prompt", "response", "raw_payload",
                 "quarantined_payload", "prompt_blob", "provenance_blob",
                 "definition"}

FORBIDDEN_KEYS = SECRET_KEYS | LIVE_MARKET_KEYS | RAW_BLOB_KEYS

#: Phrases that would make a report forward-looking (§1.4) or imperative
#: (§5.A). Checked on OUTPUT, not input — the model can be told not to, and
#: then this counts how often it did anyway.
FORWARD_LOOKING = re.compile(
    r"\b("
    r"will\s+(?:rise|fall|likely|probably|recover|continue|reach)|"
    r"(?:is|are|seems?|appears?)\s+likely\s+to|likely\s+to|"
    r"expect(?:s|ed|ing)?\s+to|we\s+expect|"
    r"forecast(?:s|ed|ing)?|predict(?:s|ed|ion|ions)?|projected|"
    r"should\s+(?:buy|sell|enter|exit|rally|drop)|"
    r"going\s+to\s+(?:rise|fall|recover)|outlook\s+for|"
    r"next\s+(?:week|session|day)\s+(?:will|should)"
    r")\b", re.I)

IMPERATIVE = re.compile(
    r"(?m)^\s*(?:[-*]\s*)?(?:you (?:must|should)|"
    r"(?:do|run|set|change|promote|suspend|kill|halt|increase|decrease|"
    r"adjust|disable|enable)\s+(?:the|this|that|it)\b)", re.I)


class AuditorLeak(Exception):
    """Assembled context contains something the Auditor must never see."""


class AuditorOutputViolation(Exception):
    """The report itself broke a §1.4 / §5.A rule."""


def assert_clean(context: Any, path: str = "context") -> None:
    """Walk the assembled context. A forbidden key three levels down is just as
    readable to the model as a top-level one."""
    if isinstance(context, dict):
        for k, v in context.items():
            if str(k).lower() in FORBIDDEN_KEYS:
                raise AuditorLeak(
                    f"{path}.{k} is withheld from the Auditor. Secrets, live "
                    f"market state and raw prompt/quarantine blobs never reach "
                    f"a report (§1.6, §1.7, §3).")
            assert_clean(v, f"{path}.{k}")
    elif isinstance(context, (list, tuple)):
        for i, v in enumerate(context):
            assert_clean(v, f"{path}[{i}]")


def assert_no_lookahead(context: Any, window_end: datetime,
                        path: str = "context") -> None:
    """Nothing after the reporting window may appear in the context.

    A daily rundown that quotes an event from after its own window is not a
    report about yesterday; it is a report about now, wearing yesterday's date.
    """
    if isinstance(context, dict):
        for k, v in context.items():
            if isinstance(v, datetime) and v > window_end:
                raise AuditorLeak(
                    f"{path}.{k}={v.isoformat()} is after the reporting window "
                    f"ends ({window_end.isoformat()}).")
            assert_no_lookahead(v, window_end, f"{path}.{k}")
    elif isinstance(context, (list, tuple)):
        for i, v in enumerate(context):
            assert_no_lookahead(v, window_end, f"{path}[{i}]")


def check_output(prose: str) -> list[str]:
    """Return the §1.4/§5.A violations in a finished report.

    Returned rather than raised: a report that slipped one imperative is still
    worth storing with the violation recorded, and §8 wants these countable so
    a rising rate can trigger a prompt fix. The caller decides what to do —
    `assert_output_clean` is the strict door for tests and for the paths that
    should refuse.
    """
    out = []
    for m in FORWARD_LOOKING.finditer(prose):
        out.append(f"forward_looking:{m.group(0).strip().lower()}")
    for m in IMPERATIVE.finditer(prose):
        out.append(f"imperative:{m.group(0).strip().lower()}")
    return out


def assert_output_clean(prose: str) -> None:
    bad = check_output(prose)
    if bad:
        raise AuditorOutputViolation(
            f"report contains {len(bad)} violation(s) of the no-prediction / "
            f"no-imperative rules: {bad[:5]}. The Auditor reports what "
            f"happened and flags where to look; it does not forecast and it "
            f"does not instruct (§1.4, §5.A).")
