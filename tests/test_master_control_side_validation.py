from __future__ import annotations

import os

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_master_control_side_validation",
)
os.environ.setdefault("ENCRYPTION_KEY", "angel-side-validation-test-key")


def _make_master_control(monkeypatch):
    import ap_master_control as mc

    monkeypatch.setattr(
        mc.APMasterControl,
        "_seed_dedup_from_db",
        lambda self, client_id="default": None,
    )
    control = mc.APMasterControl(
        mode="paper",
        score_floor=70,
        account_equity=25_000,
        position_manager=None,
        supabase_client=None,
    )
    # Force an immediate downstream block for valid-side alias cases so these
    # tests exercise evaluate() side normalization without touching broker,
    # selector, queue, OSM, watcher, fill, or order-monitor paths.
    control.exit_engine_down = True
    return control


def test_master_control_rejects_missing_side_without_defaulting_to_call(monkeypatch):
    control = _make_master_control(monkeypatch)
    signal = {
        "signal_id": "sig-missing-side",
        "ticker": "AAPL",
        "score": 80,
        "timeframe": "1d",
    }

    decision = control.evaluate(signal, client_id="test-client")

    assert decision.ok is False
    assert decision.reason_code == "INVALID_OR_MISSING_SIDE"
    assert signal.get("side") != "CALL"


def test_master_control_rejects_invalid_side(monkeypatch):
    control = _make_master_control(monkeypatch)
    signal = {
        "signal_id": "sig-invalid-side",
        "ticker": "AAPL",
        "score": 80,
        "side": "UNKNOWN",
        "timeframe": "1d",
    }

    decision = control.evaluate(signal, client_id="test-client")

    assert decision.ok is False
    assert decision.reason_code == "INVALID_OR_MISSING_SIDE"


def test_master_control_normalizes_bullish_alias_to_call(monkeypatch):
    control = _make_master_control(monkeypatch)
    signal = {
        "signal_id": "sig-bullish",
        "ticker": "AAPL",
        "score": 80,
        "side": "bullish",
        "timeframe": "1d",
    }

    control.evaluate(signal, client_id="test-client")

    assert signal["side"] == "CALL"
    assert signal["direction"] == "CALL"


def test_master_control_normalizes_bearish_alias_to_put(monkeypatch):
    control = _make_master_control(monkeypatch)
    signal = {
        "signal_id": "sig-bearish",
        "ticker": "AAPL",
        "score": 80,
        "side": "bearish",
        "timeframe": "1d",
    }

    control.evaluate(signal, client_id="test-client")

    assert signal["side"] == "PUT"
    assert signal["direction"] == "PUT"


# Direct unit coverage for the module-level helper. These tests do not
# instantiate APMasterControl and lock the canonical contract that future
# consolidation of the four divergent _normalize_side() implementations
# elsewhere in the codebase will converge onto.
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("CALL", "CALL"),
        ("call", "CALL"),
        ("  Call  ", "CALL"),
        ("BUY", "CALL"),
        ("long", "CALL"),
        ("CALLS", "CALL"),
        ("bullish", "CALL"),
        ("PUT", "PUT"),
        ("sell", "PUT"),
        ("SHORT", "PUT"),
        ("puts", "PUT"),
        ("BEARISH", "PUT"),
    ],
)
def test_normalize_signal_side_accepts_aliases(raw, expected):
    import ap_master_control as mc

    assert mc._normalize_signal_side(raw) == expected


@pytest.mark.parametrize(
    "raw",
    [None, "", "   ", "UNKNOWN", "neutral", "0", "123", "side", object()],
)
def test_normalize_signal_side_returns_none_for_invalid(raw):
    import ap_master_control as mc

    assert mc._normalize_signal_side(raw) is None
