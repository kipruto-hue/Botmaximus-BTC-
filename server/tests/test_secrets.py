"""Secrets must not leak, and the paths that can move money must fail toward
the harmless state. Both are properties worth a test rather than a convention:
a leak is irreversible, and a default that reaches a live venue is the kind of
mistake that is only noticed afterwards.
"""
from __future__ import annotations

import pytest
from pydantic import SecretStr

from botmaximus.config import Settings, settings


def test_secrets_do_not_render_in_logs_or_tracebacks():
    """repr() is what ends up in an exception traceback, a settings dump, or a
    debug log line. A plain str field would put a live key in all three."""
    s = Settings(bybit_api_key="live-key-abc123",
                 bybit_api_secret="live-secret-xyz789",
                 openai_api_key="sk-real-key")
    blob = f"{s!r} {s} {s.bybit_api_key!r} {s.bybit_api_key}"
    for leaked in ("live-key-abc123", "live-secret-xyz789", "sk-real-key"):
        assert leaked not in blob


def test_the_real_value_is_still_reachable_at_the_point_of_use():
    s = Settings(bybit_api_key="live-key-abc123")
    assert s.bybit_api_key.get_secret_value() == "live-key-abc123"


def test_secret_fields_are_secretstr_not_str():
    for field in ("bybit_api_key", "bybit_api_secret",
                  "openai_api_key", "anthropic_api_key"):
        ann = Settings.model_fields[field].annotation
        assert SecretStr in getattr(ann, "__args__", (ann,)), field


# =====================================================================
# fail-loudly guards
# =====================================================================
def test_require_names_exactly_which_secret_is_missing():
    s = Settings(bybit_api_key="present", bybit_api_secret=None)
    with pytest.raises(RuntimeError) as e:
        s.require("bybit_api_key", "bybit_api_secret")
    assert "bybit_api_secret" in str(e.value)
    assert "bybit_api_key" not in str(e.value)      # the one that IS set


def test_require_passes_when_everything_is_present():
    s = Settings(bybit_api_key="k", bybit_api_secret="s")
    s.require("bybit_api_key", "bybit_api_secret")


def test_trading_is_refused_without_credentials():
    with pytest.raises(RuntimeError, match="missing required secret"):
        Settings(live_trading_enabled=True).require_trading()


def test_trading_is_refused_while_the_live_switch_is_off():
    """Credentials alone are not consent. Both switches are deliberate."""
    s = Settings(bybit_api_key="k", bybit_api_secret="s",
                 live_trading_enabled=False)
    with pytest.raises(RuntimeError, match="live_trading_enabled"):
        s.require_trading()


def test_trading_is_allowed_only_when_both_switches_are_set():
    Settings(bybit_api_key="k", bybit_api_secret="s",
             live_trading_enabled=True).require_trading()


# =====================================================================
# defaults fail toward harmless
# =====================================================================
def test_defaults_do_not_reach_a_live_venue():
    """A fresh checkout with no .env must be incapable of trading real money."""
    s = Settings(_env_file=None)
    assert s.bybit_testnet is True
    assert s.live_trading_enabled is False
    assert s.bybit_api_key is None
    with pytest.raises(RuntimeError):
        s.require_trading()


def test_the_running_config_is_not_accidentally_live():
    """Guards the actual loaded settings, so a .env edit that turns on live
    trading has to also turn on this test's failure -- a visible event."""
    if settings.live_trading_enabled:
        pytest.fail("live_trading_enabled is TRUE in the loaded configuration. "
                    "If that is intentional, this test is your notification.")
