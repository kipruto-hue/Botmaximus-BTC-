"""Shared fixtures.

The venue-constants fixture is autouse and deliberate. `venue.get()` raises
when the constants were never fetched, because a default would be exactly the
assumed-number bug that `execution/venue.py` exists to remove — so every test
that sizes an order has to say which venue it is sizing against.

The values below are the **real** Bybit BTCUSDT linear-perp numbers, read from
`/v5/market/instruments-info` and `/v5/market/risk-limit` on 2026-08-05. Pinning
real values rather than round test numbers means a test that passes here is
evidence about the venue we actually trade, and a venue change that breaks an
assumption shows up as a failing test instead of a surprise in production.
"""
from __future__ import annotations

import pytest

from botmaximus.execution import venue
from botmaximus.execution.venue import RiskTier, VenueConstants
from botmaximus.obs import degradation

#: Bybit BTCUSDT LinearPerpetual, captured live 2026-08-05.
BYBIT_BTCUSDT = VenueConstants(
    symbol="BTCUSDT",
    category="linear",
    qty_step=0.001,
    min_order_qty=0.001,
    max_order_qty=1500.0,
    max_market_qty=150.0,
    min_notional=5.0,            # Binance hardcode said 100.0
    tick_size=0.1,
    max_leverage=100.0,
    tiers=(
        RiskTier(limit_value=2_000_000.0, maint_margin=0.005,   # was 0.004
                 initial_margin=0.01, max_leverage=100.0),
        RiskTier(limit_value=2_600_000.0, maint_margin=0.0056,
                 initial_margin=0.0111, max_leverage=90.0),
    ),
    taker_fee_rate=0.00055,
    maker_fee_rate=0.0002,
    fee_source="test",
    host="test",
)


@pytest.fixture(autouse=True)
def venue_constants():
    venue.set_for_tests(BYBIT_BTCUSDT)
    yield BYBIT_BTCUSDT
    venue.set_for_tests(None)


@pytest.fixture(autouse=True)
def clean_degradation_counters():
    """Degradations are asserted on in several tests; a count leaking between
    tests would make those assertions depend on execution order."""
    degradation.reset_for_tests()
    yield
    degradation.reset_for_tests()
