"""The freshness probe decides when a supervisor restarts the collector, on
both Windows and the VPS. Its failure modes are asymmetric and both are bad:
too eager and it restart-loops into a Binance rate-limit ban; too lax and a
silently-dead feed bleeds data that no backfill can recover.
"""
from __future__ import annotations

import io
import json
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ops"))
import healthcheck  # noqa: E402

HEALTHY_TELE = {
    "uptime_s": 900,
    "counts": {"stored": 12345},
    "ws_sources": {"binance": True, "binance-futures-market": True},
    "feeds": [
        {"dataset_id": "btc_ohlcv_1m", "fresh": 1200, "stale": False},
        {"dataset_id": "btc_price_tick", "fresh": 300, "stale": False},
    ],
}
HEALTHY_HEALTH = {"status": "ok", "ws_connected": True, "mongo": True}


def _serve(monkeypatch, tele=None, health=None, boom=None):
    def fake_urlopen(url, timeout=None):
        if boom is not None:
            raise boom
        payload = tele if url.endswith("/api/telemetry") else health
        body = io.BytesIO(json.dumps(payload).encode())
        body.__enter__ = lambda: body
        body.__exit__ = lambda *a: False
        return body
    monkeypatch.setattr(healthcheck.urllib.request, "urlopen", fake_urlopen)


def test_healthy_when_every_feed_is_inside_its_budget(monkeypatch):
    _serve(monkeypatch, HEALTHY_TELE, HEALTHY_HEALTH)
    code, reason = healthcheck.probe("http://x")
    assert code == 0
    assert "8 feeds" not in reason and "feeds fresh" in reason


def test_a_stale_feed_is_unhealthy_even_though_the_process_is_alive(monkeypatch):
    """The 2026-04-23 Binance routing failure: socket connected, SUBSCRIBE
    acked, nothing delivered. Liveness saw a healthy service for hours."""
    tele = json.loads(json.dumps(HEALTHY_TELE))
    tele["feeds"][0]["stale"] = True
    _serve(monkeypatch, tele, HEALTHY_HEALTH)
    code, reason = healthcheck.probe("http://x")
    assert code == 1
    assert "btc_ohlcv_1m" in reason


def test_a_downed_websocket_source_is_unhealthy(monkeypatch):
    tele = json.loads(json.dumps(HEALTHY_TELE))
    tele["ws_sources"]["binance-futures-market"] = False
    _serve(monkeypatch, tele, HEALTHY_HEALTH)
    code, reason = healthcheck.probe("http://x")
    assert code == 1
    assert "binance-futures-market" in reason


def test_mongo_down_is_unhealthy(monkeypatch):
    _serve(monkeypatch, HEALTHY_TELE, {**HEALTHY_HEALTH, "mongo": False})
    code, reason = healthcheck.probe("http://x")
    assert code == 1
    assert "mongo" in reason


def test_a_collector_that_has_stored_nothing_is_not_healthy(monkeypatch):
    """An empty snapshot has no stale feeds, so a naive 'any stale?' check
    would call a collector that has never ingested anything healthy."""
    tele = {"uptime_s": 600, "counts": {}, "ws_sources": {"binance": True},
            "feeds": [{"dataset_id": "btc_ohlcv_1m", "fresh": None, "stale": False}]}
    _serve(monkeypatch, tele, HEALTHY_HEALTH)
    code, reason = healthcheck.probe("http://x")
    assert code == 1
    assert "stored" in reason


def test_unreachable_api_is_distinguished_from_unhealthy(monkeypatch):
    """Exit 2, not 1: 'the API is gone' and 'the API says feeds are stale' want
    different operator responses, and the supervisor logs which one it saw."""
    _serve(monkeypatch, boom=OSError("connection refused"))
    code, reason = healthcheck.probe("http://x")
    assert code == 2
    assert "unreachable" in reason


def test_event_driven_feeds_are_never_stale_by_omission(monkeypatch):
    """Liquidations carry budget=None because silence there is genuinely not
    absence. telemetry marks them stale=False; the probe must not second-guess
    that and restart the collector for being quiet overnight."""
    tele = json.loads(json.dumps(HEALTHY_TELE))
    tele["feeds"].append({"dataset_id": "btc_liquidation", "fresh": 9_000_000,
                          "stale": False})
    _serve(monkeypatch, tele, HEALTHY_HEALTH)
    assert healthcheck.probe("http://x")[0] == 0


def test_malformed_response_is_unreachable_not_healthy(monkeypatch):
    def fake_urlopen(url, timeout=None):
        body = io.BytesIO(b"<html>gateway error</html>")
        body.__enter__ = lambda: body
        body.__exit__ = lambda *a: False
        return body
    monkeypatch.setattr(healthcheck.urllib.request, "urlopen", fake_urlopen)
    code, _ = healthcheck.probe("http://x")
    assert code == 2
