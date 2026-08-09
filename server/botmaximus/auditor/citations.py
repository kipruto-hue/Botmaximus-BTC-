r"""Citations and the numbers boundary (§1.3, §8).

§1.3 says the Auditor does not perform arithmetic: the query computes, the model
quotes. That is an instruction, and instructions to language models are
requests, not guarantees. This module turns it into something checkable.

## Two independent checks

**Citation verification.** Every citation is a `{table, record_id_or_query,
quoted_value}` triple. Re-running the query must return the quoted value. If it
does not, the report is wrong — and a report can be wrong quietly, which is the
whole problem. Sampling five per week (§8) catches drift within a cycle.

**Uncited numbers.** Every numeric token in the prose is extracted and matched
against the citations' quoted values. A number in the report that no citation
supports is precisely the fabrication §1.3 exists to prevent, and it is now a
count rather than a hope. This is the check that would catch a model quietly
computing a percentage instead of quoting one.

The second check is deliberately blunt. It will occasionally flag a legitimate
number — a date, a section heading, "the last 7 days". Those are allowed
through a small allowlist of window-derived values the composer supplies. A
check that is slightly noisy and always runs beats a precise one that nobody
wired up.
"""
from __future__ import annotations

import re
from dataclasses import asdict, dataclass, field
from datetime import datetime

from botmaximus.storage import postgres

#: Numbers as they appear in prose: 12, 3.5, 1,024, 45%, -0.7, $1,234.56
_NUMBER = re.compile(r"-?\$?\d[\d,]*(?:\.\d+)?%?")


@dataclass(frozen=True)
class Citation:
    table: str
    record_id_or_query: str
    quoted_value: str

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass
class VerificationResult:
    checked: int = 0
    mismatches: list[dict] = field(default_factory=list)
    unrunnable: list[dict] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.mismatches and not self.unrunnable


def normalise_number(token: str) -> str:
    """Strip presentation so `$1,234.50`, `1234.5` and `1,234.50` compare equal.

    Without this the check fails on formatting rather than on substance, and a
    check that cries wolf is a check that gets switched off.
    """
    t = token.strip().replace(",", "").replace("$", "").replace("%", "")
    t = t.rstrip(".")
    try:
        f = float(t)
    except ValueError:
        return t
    return f"{f:.10g}"


def numbers_in(text: str) -> list[str]:
    return [normalise_number(m.group(0)) for m in _NUMBER.finditer(text or "")]


def cited_numbers(citations: list[Citation]) -> set[str]:
    out: set[str] = set()
    for c in citations:
        out.update(numbers_in(str(c.quoted_value)))
        # A citation whose quoted value IS the number, unformatted.
        out.add(normalise_number(str(c.quoted_value)))
    return out


def uncited_numbers(prose: str, citations: list[Citation],
                    allow: set[str] | None = None) -> list[str]:
    """Numbers in the prose that no citation supports (§1.3).

    `allow` carries window-derived values the composer legitimately knows
    without a ledger read — the year, the day count, the report's own dates.
    Everything else has to come from a citation.
    """
    supported = cited_numbers(citations) | {normalise_number(a)
                                            for a in (allow or set())}
    return [n for n in numbers_in(prose) if n not in supported]


async def verify(citations: list[Citation]) -> VerificationResult:
    """Re-run each citation and compare (§8).

    Read-only by construction: only SELECT statements are executed, and
    anything else is refused rather than run. The Auditor's own role could not
    write anyway, but this function may be called from the operator-facing API
    under a different connection, and a "verify" endpoint that could be talked
    into running an UPDATE would be a hole in an otherwise sealed wall.
    """
    result = VerificationResult()
    for c in citations:
        q = (c.record_id_or_query or "").strip()
        if not _is_select(q):
            result.unrunnable.append(
                {**c.to_dict(), "reason": "not a single read-only SELECT"})
            continue
        try:
            row = await postgres.fetchrow(q)
        except Exception as e:                          # noqa: BLE001
            result.unrunnable.append({**c.to_dict(), "reason": str(e)})
            continue
        result.checked += 1
        actual = "" if row is None else str(next(iter(row.values())))
        if normalise_number(actual) != normalise_number(str(c.quoted_value)):
            result.mismatches.append({**c.to_dict(), "actual": actual})
    return result


def _is_select(q: str) -> bool:
    lowered = q.lower().lstrip()
    if not lowered.startswith("select"):
        return False
    # One statement only: a trailing `; DELETE ...` would otherwise ride along.
    if ";" in q.rstrip().rstrip(";"):
        return False
    banned = ("insert ", "update ", "delete ", "drop ", "alter ", "truncate ",
              "grant ", "revoke ", "create ", "copy ")
    return not any(b in lowered for b in banned)


def density(prose: str, citations: list[Citation]) -> float:
    """Citations per 100 words (§8). Low density means prose without evidence."""
    words = len((prose or "").split())
    if words == 0:
        return 0.0
    return round(len(citations) * 100.0 / words, 2)


def word_count(prose: str) -> int:
    return len((prose or "").split())


def within_bounds(report_type: str, words: int) -> bool:
    """§4 word bounds. Outside them is a signal, not a feature."""
    lo, hi = BOUNDS.get(report_type, (0, 10_000))
    return lo <= words <= hi


#: §4.A / §4.B / §4.C.
BOUNDS = {
    "daily": (400, 800),
    "weekly": (800, 1500),
    "incident": (300, 1000),
    "cascade": (300, 1500),
}
