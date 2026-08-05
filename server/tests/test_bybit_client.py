"""The client is the only module that can move money. Its guards are the last
line between this build and a live account, so they are tested as behaviour
rather than trusted as convention.

No test here touches the network: pybit's HTTP is replaced wholesale.
"""
from __future__ import annotations

import pytest

from botmaximus.config import Settings, settings
from botmaximus.execution import bybit_client as bc
from botmaximus.execution.bybit_client import BybitClient, VenueScopeError


class FakeHTTP:
    """Records calls; returns retCode 0 unless told otherwise."""

    def __init__(self, **kw):
        self.init_kwargs = kw
        self.calls: list[tuple[str, dict]] = []
        self.responses: dict[str, dict] = {}

    def _reply(self, name, **params):
        self.calls.append((name, params))
        return self.responses.get(name, {"retCode": 0, "result": {
            "orderId": "oid-1", "list": []}})

    def __getattr__(self, name):
        if name.startswith("_"):
            raise AttributeError(name)
        return lambda **params: self._reply(name, **params)


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(bc, "HTTP", FakeHTTP)
    monkeypatch.setattr(settings, "bybit_api_key", Settings(
        bybit_api_key="k").bybit_api_key)
    monkeypatch.setattr(settings, "bybit_api_secret", Settings(
        bybit_api_secret="s").bybit_api_secret)
    monkeypatch.setattr(settings, "bybit_testnet", True)
    return BybitClient()


# =====================================================================
# scope lock
# =====================================================================
def test_construction_requires_credentials(monkeypatch):
    monkeypatch.setattr(bc, "HTTP", FakeHTTP)
    monkeypatch.setattr(settings, "bybit_api_key", None)
    with pytest.raises(RuntimeError, match="missing required secret"):
        BybitClient()


def test_a_non_testnet_config_cannot_construct_a_client(monkeypatch, client):
    """The assert is belt-and-braces over the config guard: a future edit that
    loosens config still trips here."""
    monkeypatch.setattr(settings, "bybit_testnet", False)
    with pytest.raises(AssertionError, match="must not touch live"):
        BybitClient._assert_scope()


def test_inverse_category_is_refused(monkeypatch):
    monkeypatch.setattr(settings, "bybit_category", "inverse")
    with pytest.raises(VenueScopeError, match="coin-margined"):
        BybitClient._assert_scope()


def test_another_symbol_is_refused(monkeypatch):
    monkeypatch.setattr(settings, "symbol", "ETHUSDT")
    with pytest.raises(VenueScopeError, match="locked to BTCUSDT"):
        BybitClient._assert_scope()


def test_binance_venue_is_unreachable(monkeypatch):
    monkeypatch.setattr(settings, "venue", "binance")
    with pytest.raises(VenueScopeError, match="not bybit"):
        BybitClient._assert_scope()


def test_the_client_points_at_testnet(client):
    assert client._http.init_kwargs["testnet"] is True


# =====================================================================
# retCode handling
# =====================================================================
@pytest.mark.asyncio
async def test_a_nonzero_retcode_raises_rather_than_returning_empty(client):
    """V5 errors arrive with HTTP 200. Unchecked, a rate limit reads as 'no
    open orders' -- which is how a reconciler concludes a live position does
    not exist."""
    client._http.responses["get_open_orders"] = {
        "retCode": 10001, "retMsg": "param error", "result": {}}
    with pytest.raises(RuntimeError, match="10001"):
        await client.fetch_open_orders()


@pytest.mark.asyncio
async def test_rate_limits_retry_on_reads_and_record_a_degradation(client, monkeypatch):
    from botmaximus.obs import degradation

    calls = {"n": 0}
    ok = {"retCode": 0, "result": {"list": []}}

    def flaky(**params):
        calls["n"] += 1
        return {"retCode": 10006, "retMsg": "rate", "result": {}} \
            if calls["n"] == 1 else ok

    monkeypatch.setattr(client._http, "get_open_orders", flaky, raising=False)
    # The first backoff is 0.4s and is left real: patching asyncio.sleep here
    # would replace it for pytest-asyncio's own loop machinery too.
    assert await client.fetch_open_orders() == []
    assert calls["n"] == 2
    assert degradation.counts().get("bybit_read_retry") == 1


@pytest.mark.asyncio
async def test_order_placement_is_never_retried(client, monkeypatch):
    """A retry after an ambiguous response is how one intent becomes two
    positions."""
    calls = {"n": 0}

    def always_busy(**params):
        calls["n"] += 1
        return {"retCode": 10006, "retMsg": "busy", "result": {}}

    monkeypatch.setattr(client._http, "place_order", always_busy, raising=False)
    with pytest.raises(RuntimeError, match="10006"):
        await client.place_market("Buy", 0.01)
    assert calls["n"] == 1


# =====================================================================
# order construction
# =====================================================================
@pytest.mark.asyncio
async def test_every_order_carries_a_client_order_id(client):
    r = await client.place_market("Buy", 0.01)
    name, params = client._http.calls[-1]
    assert name == "place_order"
    assert params["orderLinkId"].startswith("bmx-")
    assert r["orderLinkId"] == params["orderLinkId"]


@pytest.mark.asyncio
async def test_maker_entry_is_post_only(client):
    """PostOnly is rejected rather than filled if it would cross -- which is the
    point: a maker order that crosses pays the taker fee it exists to avoid."""
    await client.place_limit_maker("Buy", 0.01, 64_000.0)
    _, params = client._http.calls[-1]
    assert params["timeInForce"] == "PostOnly"
    assert params["orderType"] == "Limit"


@pytest.mark.asyncio
async def test_the_stop_is_reduce_only_and_broker_side(client):
    """A stop that only exists in local memory is not a stop: it dies with the
    process, leaving an unprotected position."""
    await client.place_stop_market("Sell", 0.01, 63_000.0)
    _, params = client._http.calls[-1]
    assert params["reduceOnly"] is True
    assert params["triggerPrice"] == "63000.0"


@pytest.mark.asyncio
async def test_orders_never_leave_the_locked_scope(client):
    await client.place_market("Buy", 0.01)
    await client.place_limit_maker("Sell", 0.01, 64_000.0)
    for name, params in client._http.calls:
        if name == "place_order":
            assert params["symbol"] == "BTCUSDT"
            assert params["category"] == "linear"


# =====================================================================
# closing
# =====================================================================
@pytest.mark.asyncio
async def test_closing_derives_side_from_the_venue_not_local_state(client):
    """Local state is exactly what is suspect when a flatten is called.
    Closing in the wrong direction doubles the position."""
    client._http.responses["get_positions"] = {
        "retCode": 0, "result": {"list": [{"side": "Buy", "size": "0.05"}]}}
    await client.close_position_market()
    _, params = client._http.calls[-1]
    assert params["side"] == "Sell"          # opposite of the open position
    assert params["qty"] == "0.05"
    assert params["reduceOnly"] is True


@pytest.mark.asyncio
async def test_closing_when_flat_is_a_noop(client):
    client._http.responses["get_positions"] = {
        "retCode": 0, "result": {"list": [{"side": "Buy", "size": "0"}]}}
    assert await client.close_position_market() is None


# =====================================================================
# the live-money guard
# =====================================================================
def test_testnet_orders_are_allowed_while_live_trading_is_off():
    """live_trading_enabled gates risking real money, not trading at all --
    otherwise demo trading could never be proven."""
    Settings(bybit_api_key="k", bybit_api_secret="s",
             bybit_testnet=True, live_trading_enabled=False).require_trading()


def test_a_real_account_still_requires_the_explicit_flag():
    s = Settings(bybit_api_key="k", bybit_api_secret="s",
                 bybit_testnet=False, live_trading_enabled=False)
    with pytest.raises(RuntimeError, match="live venue"):
        s.require_trading()
