r"""Scrutiny calibration (§3.E) — how the veto layer is judged.

The model is stateless, so calibration is a **prompt-level and threshold-level**
operation, never a model-level one. This module computes the three numbers the
operator reviews, and the review changes thresholds, `k`, or the system prompt.

**It never changes temperature.** Raising temperature to "let the model think
differently" is anti-consistency: the whole safety property of this layer is
that identical inputs give identical verdicts, and a calibration cycle that
attacks that property has misunderstood what it is calibrating.

## The three metrics, and why conviction is not among them

- **Veto precision** — of the trades vetoed, what fraction would have lost?
  Low precision means the gate is expensive: it is declining trades that were
  fine.
- **Veto recall** — of the losing setups, what fraction were vetoed? Low recall
  means the gate is decorative: the losses came through anyway.
- **Consistency** — same state key, same verdict. This is the safety property.
  A gate can have decent precision and recall and still be worthless if it
  answers differently to the same question, because then neither number
  predicts what it will do next time.

Conviction is deliberately excluded. It is the model's self-report, and §4's
first invariant is that only ground truth feeds back — never model self-reports.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from botmaximus.scrutiny.gate import SCRUTINY_EVENTS


@dataclass
class CalibrationReport:
    sample: int
    veto_precision: float | None
    veto_recall: float | None
    consistency: float | None
    provider_versions: list[str]
    verdict: str

    def to_dict(self) -> dict:
        return {
            "sample": self.sample,
            "veto_precision": self.veto_precision,
            "veto_recall": self.veto_recall,
            "consistency": self.consistency,
            "provider_versions": self.provider_versions,
            "verdict": self.verdict,
        }


async def join_outcome(db, intent_id: str, adverse: bool,
                       realized_return_pct: float | None = None) -> None:
    """Attach the realized outcome to a verdict once it is known.

    Called after a trade closes — and for vetoes, after the horizon the trade
    *would* have run. A veto whose counterfactual is never measured cannot be
    judged, and an unjudgeable gate drifts without anyone noticing.
    """
    await db[SCRUTINY_EVENTS].update_one(
        {"intent_id": intent_id},
        {"$set": {"realized_known": True,
                  "realized_adverse": bool(adverse),
                  "realized_return_pct": realized_return_pct,
                  "realized_at": datetime.now(timezone.utc)}})


async def report(db, days: int = 7) -> CalibrationReport:
    since = datetime.now(timezone.utc) - timedelta(days=days)
    rows = [d async for d in db[SCRUTINY_EVENTS].find(
        {"at": {"$gte": since}}, {"_id": 0})]

    known = [r for r in rows if r.get("realized_known")]
    versions = sorted({r.get("provider_version", "?") for r in rows})

    if not known:
        # An empty aggregate scores 0/0 and reads like a perfect gate. Say so.
        return CalibrationReport(
            sample=len(rows), veto_precision=None, veto_recall=None,
            consistency=_consistency(rows), provider_versions=versions,
            verdict=("No verdict has a realized outcome yet, so precision and "
                     "recall are UNKNOWN — not good. Only consistency is "
                     "measurable before outcomes land."))

    vetoes = [r for r in known if r.get("verdict") == "VETO"]
    approves = [r for r in known if r.get("verdict") == "APPROVE"]
    adverse_total = sum(1 for r in known if r.get("realized_adverse"))

    precision = (sum(1 for r in vetoes if r.get("realized_adverse")) / len(vetoes)
                 if vetoes else None)
    recall = (sum(1 for r in vetoes if r.get("realized_adverse")) / adverse_total
              if adverse_total else None)
    consistency = _consistency(rows)

    bad_approvals = sum(1 for r in approves if r.get("realized_adverse"))
    parts = []
    if precision is not None and precision < 0.5:
        parts.append(f"veto precision {precision:.0%}: the gate is declining "
                     f"mostly-fine trades and costing opportunity")
    if recall is not None and recall < 0.5:
        parts.append(f"veto recall {recall:.0%}: most losing setups came "
                     f"through anyway — the gate is closer to decorative")
    if consistency is not None and consistency < 0.95:
        parts.append(f"consistency {consistency:.0%}: identical states are "
                     f"getting different verdicts, which makes precision and "
                     f"recall unpredictive of future behaviour")
    if bad_approvals:
        parts.append(f"{bad_approvals} approvals went adverse")

    verdict = ("; ".join(parts) if parts else
               "precision, recall and consistency all within range")
    return CalibrationReport(len(known), precision, recall, consistency,
                             versions, verdict)


def _consistency(rows: list[dict]) -> float | None:
    """Fraction of repeated state keys that received the same verdict."""
    by_key: dict[str, set] = {}
    for r in rows:
        key = (r.get("evidence") or {}).get("state_key_hash") or r.get("state_key_hash")
        if not key:
            continue
        by_key.setdefault(key, set()).add(r.get("verdict"))
    repeated = [v for v in by_key.values() if v]
    if not repeated:
        return None
    consistent = sum(1 for v in repeated if len(v) == 1)
    return consistent / len(repeated)
