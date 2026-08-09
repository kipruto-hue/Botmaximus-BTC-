"""Predicted-vs-realized execution ledger (§2.3 of docs/STRUCTURAL_RISKS.md).

At ~5-minute holds with taker orders both sides, **friction is the dominant
term** — Gate 3 measured a seed whose friction was 538x its gross edge. Every
number the validation gate produces therefore rests on the cost model being
right, and the cost model is currently a 1bp slippage floor with no order-book
depth, no market impact, no partial fills and no queue position.

When paper execution starts losing money, there will be two explanations that
produce an identical equity curve:

    1. the edge decayed and the strategy should be retired
    2. the cost model was optimistic from the beginning

Those have opposite responses. Nothing in the system can currently tell them
apart, and by the time §7 decay diagnosis runs it will be guessing. This ledger
is the record that makes the question answerable: for every leg of every trade,
what the model *said would happen* beside what *did*.

## Built before it can be filled

Pass F does not exist, so nothing writes realizations yet. That is the point of
building it now — the predictions have to be recorded at decision time by
whatever places the order, and retrofitting that later means throwing away every
trade until the retrofit.

It also means the reports must distinguish **"calibrated, drift is near zero"**
from **"no realized fills, drift is unknown"**. Those are numerically identical
in a naive aggregate — an empty mean is 0.0 — and reporting the second as the
first would be a lie of exactly the kind this ledger exists to catch. Every
report carries `realized_legs`, and `status` is never `calibrated` at zero.

## Sign convention

Drift is always **cost-positive**: a positive number means reality was *worse*
than predicted. That way "are we underestimating costs?" is answered by the sign
alone, with no per-field reasoning about direction.
"""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

LEDGER = "execution_ledger"

#: Prediction recorded, no fill seen yet. Either still working, or lost.
PENDING = "pending"
#: Both sides present.
RECONCILED = "reconciled"
#: Explicitly closed without a fill (cancelled, rejected, expired).
UNFILLED = "unfilled"


@dataclass(frozen=True)
class Prediction:
    """What the cost model expects, recorded at decision time.

    `reference_price` is the mid the decision was made against — without it a
    realized fill cannot be turned into a realized slippage, only into an
    unattributable price difference.
    """
    trade_id: str
    strategy_id: str
    leg: str                        # "entry" | "exit"
    direction: str                  # "LONG" | "SHORT"
    symbol: str
    qty: float
    decision_time: datetime
    reference_price: float
    predicted_fill: float
    predicted_fee: float
    predicted_slippage_bps: float
    predicted_latency_ms: float
    predicted_funding: float = 0.0  # entry leg carries the whole-hold estimate

    def to_doc(self) -> dict:
        d = asdict(self)
        d["status"] = PENDING
        d["recorded_at"] = datetime.now(timezone.utc)
        return d


@dataclass(frozen=True)
class Realization:
    """What the venue actually did."""
    trade_id: str
    leg: str
    fill_time: datetime
    realized_fill: float
    realized_fee: float
    realized_qty: float
    venue_order_id: str | None = None
    realized_funding: float = 0.0
    partial: bool = False
    note: str = ""


@dataclass
class Drift:
    """One reconciled leg. Every field is cost-positive: worse than predicted."""
    trade_id: str
    strategy_id: str
    leg: str
    slippage_bps_predicted: float
    slippage_bps_realized: float
    slippage_bps_drift: float
    fee_drift: float
    latency_ms_drift: float
    qty_shortfall: float
    funding_drift: float
    cost_drift_usd: float
    partial: bool


def _adverse_bps(reference: float, fill: float, direction: str, leg: str) -> float:
    """Realized slippage in bps, signed so positive is always *against* us.

    A long entry buys — paying above the reference is adverse. A long exit
    sells — receiving below the reference is adverse. Shorts mirror both.
    """
    if reference <= 0:
        return 0.0
    buying = (direction == "LONG" and leg == "entry") or (direction == "SHORT" and leg == "exit")
    raw = (fill - reference) / reference * 10_000
    return raw if buying else -raw


def reconcile(pred: Prediction, real: Realization) -> Drift:
    slip_real = _adverse_bps(pred.reference_price, real.realized_fill,
                             pred.direction, pred.leg)
    latency_ms = (real.fill_time - pred.decision_time).total_seconds() * 1000

    # Price drift in dollars, signed cost-positive. Uses the filled quantity:
    # a half-filled order that slipped badly cost half as much as a full one.
    buying = (pred.direction == "LONG" and pred.leg == "entry") or \
             (pred.direction == "SHORT" and pred.leg == "exit")
    price_delta = (real.realized_fill - pred.predicted_fill) if buying else \
                  (pred.predicted_fill - real.realized_fill)
    cost_drift = price_delta * real.realized_qty
    cost_drift += real.realized_fee - pred.predicted_fee
    cost_drift += real.realized_funding - pred.predicted_funding

    return Drift(
        trade_id=pred.trade_id,
        strategy_id=pred.strategy_id,
        leg=pred.leg,
        slippage_bps_predicted=pred.predicted_slippage_bps,
        slippage_bps_realized=slip_real,
        slippage_bps_drift=slip_real - pred.predicted_slippage_bps,
        fee_drift=real.realized_fee - pred.predicted_fee,
        latency_ms_drift=latency_ms - pred.predicted_latency_ms,
        # Shortfall, not difference: over-filling is a different (venue) problem,
        # and clamping at zero keeps this readable as "how much did we not get".
        qty_shortfall=max(0.0, pred.qty - real.realized_qty),
        funding_drift=real.realized_funding - pred.predicted_funding,
        cost_drift_usd=cost_drift,
        partial=real.partial or real.realized_qty < pred.qty,
    )


# =====================================================================
# store
# =====================================================================
async def ensure_indexes() -> None:
    """No-op: keys and indexes are part of `schema.sql`."""
    return None


async def record_prediction(pred: Prediction) -> None:
    """Written before the order goes out.

    If the process dies between this and the fill, the leg has a prediction and
    no realization — which the view reports as `pending`. That is the honest
    state, and it shows up as an unreconciled leg rather than vanishing.
    """
    from botmaximus.storage import postgres
    await postgres.execute(
        "INSERT INTO execution_ledger_predictions "
        "(trade_id, leg, strategy_id, direction, symbol, qty, decision_time, "
        " reference_price, predicted_fill, predicted_fee, "
        " predicted_slippage_bps, predicted_latency_ms, predicted_funding) "
        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
        "ON CONFLICT (trade_id, leg) DO NOTHING",
        (pred.trade_id, pred.leg, pred.strategy_id, pred.direction, pred.symbol,
         pred.qty, pred.decision_time, pred.reference_price,
         pred.predicted_fill, pred.predicted_fee, pred.predicted_slippage_bps,
         pred.predicted_latency_ms, pred.predicted_funding))


async def record_realization(real: Realization) -> Drift | None:
    """Attach the venue's answer and reconcile.

    Returns None if no prediction was recorded — itself a defect worth seeing,
    not a reason to silently create one: a fill with no prediction means an
    order was placed outside the path that is supposed to record intent. The
    foreign key enforces the same thing at the database, so this cannot be
    bypassed by a different caller.

    The realization and its drift are written in one transaction: a reconciled
    leg with no drift row would read as calibrated rather than as half-written.
    """
    from botmaximus.storage import postgres
    doc = await postgres.fetchrow(
        "SELECT * FROM execution_ledger_predictions "
        "WHERE trade_id = %s AND leg = %s", (real.trade_id, real.leg))
    if doc is None:
        return None

    pred = Prediction(**{k: _num(doc[k])
                         for k in Prediction.__dataclass_fields__})
    drift = reconcile(pred, real)
    async with postgres.transaction() as conn:
        await conn.execute(
            "INSERT INTO execution_ledger_realizations "
            "(trade_id, leg, status, realized_fill, realized_fee, "
            " realized_funding, realized_latency_ms) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (trade_id, leg) DO NOTHING",
            (real.trade_id, real.leg, RECONCILED, real.realized_fill,
             real.realized_fee, real.realized_funding,
             (real.fill_time - pred.decision_time).total_seconds() * 1000))
        d = asdict(drift)
        await conn.execute(
            "INSERT INTO execution_ledger_drift "
            "(trade_id, leg, strategy_id, slippage_bps_predicted, "
            " slippage_bps_realized, slippage_bps_drift, fee_drift, "
            " latency_ms_drift, qty_shortfall, funding_drift, cost_drift_usd, "
            " partial) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (trade_id, leg) DO NOTHING",
            (d["trade_id"], d["leg"], d["strategy_id"],
             d["slippage_bps_predicted"], d["slippage_bps_realized"],
             d["slippage_bps_drift"], d["fee_drift"], d["latency_ms_drift"],
             d["qty_shortfall"], d["funding_drift"], d["cost_drift_usd"],
             d["partial"]))
    return drift


async def mark_unfilled(trade_id: str, leg: str, reason: str) -> None:
    """A cancelled or rejected order. Distinct from `pending` on purpose: one is
    a known outcome, the other is an open question."""
    from botmaximus.storage import postgres
    await postgres.execute(
        "INSERT INTO execution_ledger_realizations "
        "(trade_id, leg, status, unfilled_reason) VALUES (%s,%s,%s,%s) "
        "ON CONFLICT (trade_id, leg) DO UPDATE SET "
        "  status = EXCLUDED.status, unfilled_reason = EXCLUDED.unfilled_reason",
        (trade_id, leg, UNFILLED, reason))


def _num(v):
    """Postgres `numeric` arrives as Decimal; the cost arithmetic is float.

    Mixing them raises `unsupported operand type(s) for -: 'decimal.Decimal'
    and 'float'` deep inside `reconcile`, far from the column that caused it.
    """
    from decimal import Decimal
    return float(v) if isinstance(v, Decimal) else v


# =====================================================================
# calibration report
# =====================================================================
def _median(xs: list[float]) -> float:
    if not xs:
        return 0.0
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


async def calibration(strategy_id: str | None = None, limit: int = 5000) -> dict:
    """Is the cost model telling the truth?

    Reports `status` explicitly rather than letting an empty aggregate read as a
    healthy one: with no realized fills every mean is 0.0, which is exactly what
    a perfectly calibrated model looks like. The two are opposite conclusions.
    """
    from botmaximus.storage import postgres
    sql = "SELECT * FROM execution_ledger "
    params: tuple = (limit,)
    if strategy_id is not None:
        sql += "WHERE strategy_id = %s "
        params = (strategy_id, limit)
    sql += "ORDER BY decision_time DESC LIMIT %s"
    docs = await postgres.fetch(sql, params)

    pending = [d for d in docs if d.get("status") == PENDING]
    unfilled = [d for d in docs if d.get("status") == UNFILLED]
    drifts = [d["drift"] for d in docs if d.get("status") == RECONCILED and d.get("drift")]

    base = {
        "legs_total": len(docs),
        "realized_legs": len(drifts),
        "pending_legs": len(pending),
        "unfilled_legs": len(unfilled),
    }

    if not drifts:
        return {
            **base,
            "status": "no_realized_fills",
            "verdict": (
                "The cost model is UNVALIDATED. No leg has been reconciled against "
                "a venue fill, so every drift statistic here would be an artefact "
                "of an empty aggregate rather than evidence of accuracy. Pass F "
                "populates this; until then treat backtest costs as an assumption."
            ),
        }

    slip = [d["slippage_bps_drift"] for d in drifts]
    cost = [d["cost_drift_usd"] for d in drifts]
    lat = [d["latency_ms_drift"] for d in drifts]
    partials = sum(1 for d in drifts if d.get("partial"))
    total_drift = sum(cost)

    # Cost-positive convention: >0 means reality was more expensive than modelled.
    underestimating = total_drift > 0
    return {
        **base,
        "status": "calibrated" if abs(_median(slip)) < 1.0 and not underestimating
                  else "drifting",
        "slippage_bps_drift_median": round(_median(slip), 3),
        "slippage_bps_drift_mean": round(sum(slip) / len(slip), 3),
        "slippage_bps_drift_worst": round(max(slip), 3),
        "latency_ms_drift_median": round(_median(lat), 1),
        "cost_drift_usd_total": round(total_drift, 2),
        "cost_drift_usd_per_leg": round(total_drift / len(cost), 4),
        "partial_fill_legs": partials,
        "verdict": (
            f"Realized costs exceed predicted by ${total_drift:,.2f} across "
            f"{len(drifts)} legs. Backtest net P&L is optimistic by roughly this "
            f"much per equivalent trade count; recalibrate slippage_bps before "
            f"trusting another validation verdict."
            if underestimating else
            f"Realized costs are ${abs(total_drift):,.2f} BELOW predicted across "
            f"{len(drifts)} legs — the model is conservative. Safe for the gate, "
            f"but it may be rejecting strategies that would in fact clear it."
        ),
    }
