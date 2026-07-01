from __future__ import annotations

from datetime import datetime

import ap
from ap.entry_metadata_guard import ZERO_UNDERLYING, validate_entry_metadata
from ap.master_control_metadata_guard import should_allow_daily_handoff_without_underlying


def _signal(**overrides):
    payload = {
        "client_id": "test@example.com",
        "execution_mode": "paper",
        "signal_id": "sig-1",
        "canonical_signal_id": "sig-1",
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "pattern": "2-3",
        "pattern_id": "2-3",
        "score": 82.0,
        "entry_trigger": 200.0,
        "target_price": 205.0,
        "stop_price": 197.5,
        "underlying_at_signal": 201.0,
    }
    payload.update(overrides)
    return payload


def _make_mc():
    from ap_master_control import APMasterControl

    mc = APMasterControl(mode="paper", client_id="test@example.com")
    mc._get_snapshot = lambda *args, **kwargs: {
        "_snapshot_ok": True,
        "open_count": 0,
        "open_positions": [],
        "closing_positions": [],
        "calls_open": 0,
        "puts_open": 0,
        "pending_entries": 0,
        "pending_entry_capital": 0.0,
        "capital_deployed": 0.0,
        "realized_pnl_today": 0.0,
        "trades_today": 0,
        "total_trades": 0,
        "daily_trades": 0,
        "intraday_trades": 0,
        "symbol_trades": {},
    }
    mc._has_durable_duplicate_signal = lambda **kwargs: (False, "", "")
    mc._run_intelligence = lambda signal: {
        "approved": True,
        "score": 80,
        "contracts": 1,
        "reasoning": "test",
        "_available": False,
    }
    mc._run_final_quality_gates = lambda **kwargs: None
    mc.sizer = None
    mc.feedback = None
    return mc


def test_validate_entry_metadata_zero_underlying_still_blocks_by_default():
    result = validate_entry_metadata(plan=_signal(underlying_at_signal=0))
    assert result.ok is False
    assert result.reason == ZERO_UNDERLYING


def test_daily_missing_underlying_after_hours_is_data_pending_candidate():
    allowed = should_allow_daily_handoff_without_underlying(
        _signal(underlying_at_signal=0),
        now=datetime(2026, 7, 1, 20, 15),
    )
    assert allowed is True


def test_intraday_missing_underlying_is_not_data_pending_candidate():
    allowed = should_allow_daily_handoff_without_underlying(
        _signal(timeframe="15m", underlying_at_signal=0),
        now=datetime(2026, 7, 1, 20, 15),
    )
    assert allowed is False


def test_regular_session_daily_missing_underlying_is_not_data_pending_candidate():
    allowed = should_allow_daily_handoff_without_underlying(
        _signal(underlying_at_signal=0),
        now=datetime(2026, 7, 1, 10, 15),
    )
    assert allowed is False


def test_master_control_daily_after_hours_missing_underlying_marks_data_pending(monkeypatch):
    ap.install_entry_metadata_safety_guards()
    mc = _make_mc()

    monkeypatch.setattr(
        "ap.master_control_metadata_guard._is_regular_session_et",
        lambda now=None: False,
    )

    signal = _signal(underlying_at_signal=0)
    decision = mc.evaluate(signal, client_id="test@example.com")

    assert decision.ok is True
    assert str(getattr(decision, "stage", "")).lower() == "approved"
    assert signal["underlying_data_pending"] is True
    assert signal["allowed_for_handoff"] is True
    assert signal["allowed_for_execution"] is False
    assert signal["metadata_validation_status"] == "DATA_PENDING"
    assert signal["metadata_validation_reason"] == ZERO_UNDERLYING
    assert signal["metadata"]["underlying_data_pending"] is True
    assert signal["metadata"]["allowed_for_execution"] is False


def test_master_control_intraday_missing_underlying_still_blocks_at_metadata_validation(monkeypatch):
    ap.install_entry_metadata_safety_guards()
    mc = _make_mc()

    monkeypatch.setattr(
        "ap.master_control_metadata_guard._is_regular_session_et",
        lambda now=None: False,
    )

    decision = mc.evaluate(
        _signal(timeframe="15m", underlying_at_signal=0),
        client_id="test@example.com",
    )

    assert decision.ok is False
    assert decision.stage == "metadata_validation"
    assert decision.reason == ZERO_UNDERLYING


def test_regular_session_daily_missing_underlying_does_not_mark_data_pending(monkeypatch):
    ap.install_entry_metadata_safety_guards()
    mc = _make_mc()

    monkeypatch.setattr(
        "ap.master_control_metadata_guard._is_regular_session_et",
        lambda now=None: True,
    )

    signal = _signal(underlying_at_signal=0)
    decision = mc.evaluate(signal, client_id="test@example.com")

    assert decision.ok is False
    assert decision.stage == "metadata_validation"
    assert decision.reason == ZERO_UNDERLYING
    assert "underlying_data_pending" not in signal
    assert "allowed_for_execution" not in signal
