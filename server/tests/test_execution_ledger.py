"""The ledger that lets the decay loop tell "the edge decayed" from "the cost
model was always wrong". Those two produce the same equity curve and want
opposite responses, so the arithmetic has to be right in both directions and
the empty case must never read as a healthy one.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from botmaximus.backtest.costs import CostModel
from botmaximus.execution import ledger
from botmaximus.execution.ledger import Prediction, Realization, reconcile
T0 = datetime(2026, 8, 1, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def db(pg):
    """Kept under its old name so the tests below read unchanged; it is now a
    real, empty Postgres schema rather than a fake collection.

    Worth noting for these tests in particular: the three-table ledger enforces
    with a foreign key what the fake could only be trusted to do — a
    realization cannot exist without its prediction.
    """
    return pg


def _pred(**kw) -> Prediction:
    base = dict(
        trade_id="t1", strategy_id="s1", leg="entry", direction="LONG",
        symbol="BTCUSDT", qty=0.1, decision_time=T0, reference_price=100_000.0,
        predicted_fill=100_010.0, predicted_fee=10.0,
        predicted_slippage_bps=1.0, predicted_latency_ms=60_000.0,
    )
    base.update(kw)
    return Prediction(**base)


def _real(**kw) -> Realization:
    base = dict(trade_id="t1", leg="entry", fill_time=T0 + timedelta(seconds=60),
                realized_fill=100_010.0, realized_fee=10.0, realized_qty=0.1)
    base.update(kw)
    return Realization(**base)


# =====================================================================
# sign convention: positive always means reality was worse
# =====================================================================
def test_a_long_entry_filling_high_is_adverse():
    d = reconcile(_pred(), _real(realized_fill=100_050.0))
    assert d.slippage_bps_realized == pytest.approx(5.0)
    assert d.slippage_bps_drift == pytest.approx(4.0)
    assert d.cost_drift_usd > 0


def test_a_long_exit_filling_low_is_adverse():
    """A long sells to exit, so receiving *less* than reference is the loss.
    A naive (fill - reference) would score this as a gain."""
    p = _pred(leg="exit", predicted_fill=99_990.0)
    d = reconcile(p, _real(leg="exit", realized_fill=99_950.0))
    assert d.slippage_bps_realized == pytest.approx(5.0)
    assert d.cost_drift_usd > 0


def test_a_short_entry_filling_low_is_adverse():
    """A short sells to enter - the mirror of a long entry."""
    p = _pred(direction="SHORT", predicted_fill=99_990.0)
    d = reconcile(p, _real(realized_fill=99_950.0))
    assert d.slippage_bps_realized == pytest.approx(5.0)
    assert d.cost_drift_usd > 0


def test_a_short_exit_filling_high_is_adverse():
    p = _pred(direction="SHORT", leg="exit", predicted_fill=100_010.0)
    d = reconcile(p, _real(leg="exit", realized_fill=100_050.0))
    assert d.slippage_bps_realized == pytest.approx(5.0)
    assert d.cost_drift_usd > 0


def test_a_better_than_predicted_fill_is_negative_drift():
    """The model being conservative must be visible too - it means the gate may
    be rejecting strategies that would in fact clear it."""
    d = reconcile(_pred(), _real(realized_fill=99_990.0))
    assert d.cost_drift_usd < 0
    assert d.slippage_bps_drift < 0


# =====================================================================
# components
# =====================================================================
def test_fee_and_funding_drift_land_in_the_dollar_total():
    d = reconcile(_pred(predicted_funding=2.0),
                  _real(realized_fee=14.0, realized_funding=5.0))
    assert d.fee_drift == pytest.approx(4.0)
    assert d.funding_drift == pytest.approx(3.0)
    assert d.cost_drift_usd == pytest.approx(7.0)


def test_latency_drift_measures_decision_to_fill():
    d = reconcile(_pred(), _real(fill_time=T0 + timedelta(seconds=95)))
    assert d.latency_ms_drift == pytest.approx(35_000.0)


def test_a_partial_fill_is_flagged_and_scales_the_price_drift():
    """Half a fill that slipped badly cost half as much. Charging the full
    intended size would overstate drift and mis-calibrate the model."""
    d = reconcile(_pred(), _real(realized_qty=0.05, realized_fill=100_050.0))
    assert d.partial is True
    assert d.qty_shortfall == pytest.approx(0.05)
    assert d.cost_drift_usd == pytest.approx((100_050.0 - 100_010.0) * 0.05)


def test_overfill_is_not_reported_as_negative_shortfall():
    d = reconcile(_pred(), _real(realized_qty=0.2))
    assert d.qty_shortfall == 0.0


# =====================================================================
# the empty case - the one that must not lie
# =====================================================================
@pytest.mark.asyncio
async def test_calibration_refuses_to_call_an_empty_ledger_calibrated(db):
    await ledger.ensure_indexes()
    report = await ledger.calibration()
    assert report["status"] == "no_realized_fills"
    assert report["realized_legs"] == 0
    assert "UNVALIDATED" in report["verdict"]


@pytest.mark.asyncio
async def test_predictions_alone_do_not_count_as_calibration(db):
    """With predictions but no fills every drift statistic is an artefact of an
    empty aggregate - numerically identical to a perfect model."""
    await ledger.record_prediction(_pred())
    report = await ledger.calibration()
    assert report["status"] == "no_realized_fills"
    assert report["legs_total"] == 1
    assert report["pending_legs"] == 1


# =====================================================================
# store round-trip
# =====================================================================
@pytest.mark.asyncio
async def test_recording_a_fill_reconciles_the_leg(db):
    await ledger.record_prediction(_pred())
    drift = await ledger.record_realization(_real(realized_fill=100_050.0))
    assert drift is not None
    assert drift.cost_drift_usd > 0

    report = await ledger.calibration()
    assert report["status"] == "drifting"
    assert report["realized_legs"] == 1
    assert report["cost_drift_usd_total"] > 0
    assert "optimistic" in report["verdict"]


@pytest.mark.asyncio
async def test_a_fill_with_no_prediction_returns_none_rather_than_inventing_one(db):
    """An order placed outside the path that records intent is a defect. Making
    up a prediction to match would hide it and pollute the calibration."""
    assert await ledger.record_realization(_real()) is None


@pytest.mark.asyncio
async def test_a_pending_leg_stays_visible_in_the_report(db):
    await ledger.record_prediction(_pred())
    await ledger.record_prediction(_pred(trade_id="t2"))
    await ledger.record_realization(_real(trade_id="t2"))
    report = await ledger.calibration()
    assert report["pending_legs"] == 1
    assert report["realized_legs"] == 1


@pytest.mark.asyncio
async def test_unfilled_is_distinct_from_pending(db):
    await ledger.record_prediction(_pred())
    await ledger.mark_unfilled("t1", "entry", "cancelled by venue")
    report = await ledger.calibration()
    assert report["unfilled_legs"] == 1
    assert report["pending_legs"] == 0


@pytest.mark.asyncio
async def test_a_prediction_is_never_overwritten(db):
    """It is the record of what was believed at decision time. Rewriting it
    after seeing the fill would drive drift to zero by construction."""
    await ledger.record_prediction(_pred(predicted_fill=100_010.0))
    await ledger.record_prediction(_pred(predicted_fill=999_999.0))
    doc = await db.fetchrow(
        "SELECT * FROM execution_ledger_predictions "
        "WHERE trade_id = %s AND leg = %s", ("t1", "entry"))
    assert float(doc["predicted_fill"]) == 100_010.0


@pytest.mark.asyncio
async def test_a_conservative_model_is_reported_as_such(db):
    await ledger.record_prediction(_pred())
    await ledger.record_realization(_real(realized_fill=99_950.0))
    report = await ledger.calibration()
    assert report["cost_drift_usd_total"] < 0
    assert "conservative" in report["verdict"]


# =====================================================================
# one cost model, not two
# =====================================================================
def test_predictions_come_from_the_backtesters_own_cost_model():
    """If the ledger measured drift against a second implementation, every
    reading would be meaningless in a way that looks like a real result."""
    cm = CostModel()
    p = cm.predict_leg("t9", "s1", "entry", "LONG", 0.1, 100_000.0, T0)
    assert p.predicted_fill == cm.fill_price(100_000.0, "LONG", "entry")
    assert p.predicted_fee == cm.fee(0.1 * p.predicted_fill)
    assert p.predicted_slippage_bps == cm.slippage_bps
    assert p.predicted_fill > 100_000.0        # a long entry pays up
