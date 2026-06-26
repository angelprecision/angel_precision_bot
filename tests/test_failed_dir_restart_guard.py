import os

import pytest

from ap import restart_guard


def test_failed_dir_2d_15min_rejected_by_default(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)

    assert restart_guard.should_skip_on_restart({
        "ticker": "GS",
        "pattern_id": "FAILED_DIR_2D_15min",
        "source_scanner": "failed_dir_intraday_1tf",
        "backtest_match_source": "FALLBACK_DEFAULT",
    }) is True


def test_failed_dir_2d_multitimeframe_rejected_by_startswith(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)

    assert restart_guard.should_skip_on_restart({
        "ticker": "COIN",
        "pattern_id": "FAILED_DIR_2D_30min+60min",
        "source_scanner": "failed_dir_intraday_2tf",
        "score": 78,
    }) is True


def test_failed_dir_2u_15min_rejected_by_default(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)

    assert restart_guard.should_skip_on_restart({
        "ticker": "MSFT",
        "pattern_id": "FAILED_DIR_2U_15min",
    }) is True


def test_future_failed_dir_variant_rejected_by_prefix(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)

    assert restart_guard.should_skip_on_restart({
        "ticker": "TEST",
        "pattern_id": "FAILED_DIR_CUSTOM_NEW",
    }) is True


def test_pattern_fallback_fields_are_checked(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)

    assert restart_guard.should_skip_on_restart({"pattern": "FAILED_DIR_2D_60min"}) is True
    assert restart_guard.should_skip_on_restart({"strat_pattern": "FAILED_DIR_2U_30min"}) is True


def test_failed_dir_enabled_allows_pass_through_when_market_closed(monkeypatch):
    monkeypatch.setenv("FAILED_DIR_ENABLED", "1")
    monkeypatch.setattr(restart_guard, "_is_market_hours_now", lambda: False)

    assert restart_guard.should_skip_on_restart({
        "ticker": "GS",
        "pattern_id": "FAILED_DIR_2D_15min",
    }) is False


@pytest.mark.parametrize("value", ["true", "yes", "on", "1"])
def test_failed_dir_enabled_truthy_values_allow_pass_through(monkeypatch, value):
    monkeypatch.setenv("FAILED_DIR_ENABLED", value)
    monkeypatch.setattr(restart_guard, "_is_market_hours_now", lambda: False)

    assert restart_guard.should_skip_on_restart({
        "pattern_id": "FAILED_DIR_2U_30min",
    }) is False


def test_non_failed_dir_signal_preserves_existing_restart_guard_path(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)
    monkeypatch.setattr(restart_guard, "_is_market_hours_now", lambda: False)

    assert restart_guard.should_skip_on_restart({
        "ticker": "AAPL",
        "pattern_id": "1-2_2U",
    }) is False


def test_payload_pattern_id_prefers_pattern_id():
    payload = {
        "pattern_id": "FAILED_DIR_2D_15min",
        "pattern": "1-2_2U",
        "strat_pattern": "3-2-2",
    }

    assert restart_guard._payload_pattern_id(payload) == "FAILED_DIR_2D_15min"
    assert restart_guard._is_failed_dir_pattern(payload) is True
