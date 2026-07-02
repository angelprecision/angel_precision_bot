from __future__ import annotations

import importlib
import os
import sys
import types
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
import logging
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

FIXED_ET = datetime(2026, 6, 24, 9, 15, tzinfo=ZoneInfo("America/New_York"))
_UNSET = object()


def _signal() -> dict:
    return {
        "signal_id": "sig-paper-1",
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "timeframe": "1d",
        "score": 78.0,
        "pattern": "2-1-2",
        "tier": "A",
        "entry_trigger": 101.0,
        "created_at": "2026-06-23T20:00:00+00:00",
        "force_overnight_reeval_only": True,
        "do_not_queue_directly": True,
    }


def _job() -> dict:
    return {
        "id": 101,
        "signal_id": "sig-paper-1",
        "payload": _signal(),
        "_source": "trade_queue",
    }


def _plan() -> types.SimpleNamespace:
    return types.SimpleNamespace(
        plan_id="plan-paper-1",
        signal_id="sig-paper-1",
        ticker="AAPL",
        side="CALL",
        direction="CALL",
        score=78.0,
        timeframe="1d",
        entry_trigger=101.0,
        trigger_price=101.0,
        trigger_type="breach",
        prior_day_high=100.0,
        prior_day_low=95.0,
        pattern="2-1-2",
        tier="A",
        contract_symbol="AAPL260626C00100000",
        contracts=1,
        limit_price=1.25,
        max_position_usd=125.0,
        metadata={},
    )


class _CreateFailOSM:
    def create_entry_order(self, *_args, **_kwargs):
        raise RuntimeError("insert failed")


class _HappyOSM:
    def create_entry_order(self, *_args, **_kwargs):
        return "local-paper-1"

    def mark_entry_pending_trigger(self, _local_order_id: str) -> bool:
        return True


def _install_common_stubs(monkeypatch, *, snapshot_valid: bool = True, execution_mode: str = "PAPER"):
    fake_validator = types.ModuleType("ap.overnight_daily_validator")
    fake_validator.fetch_market_snapshot = lambda ticker, broker: {"last": 100.0}
    if snapshot_valid:
        fake_validator.validate_overnight_daily_signal = (
            lambda **kwargs: types.SimpleNamespace(valid=True, reason_code="", reason_text="")
        )
    else:
        fake_validator.validate_overnight_daily_signal = (
            lambda **kwargs: types.SimpleNamespace(
                valid=False,
                reason_code="SNAPSHOT_UNAVAILABLE",
                reason_text="snapshot unavailable",
            )
        )
    fake_validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", fake_validator)

    fake_auth = types.ModuleType("ap.authorization")
    fake_auth.is_live_broker = lambda broker: False
    fake_auth.broker_live_mode_known = lambda broker: True
    fake_auth.check_live_authorization = lambda client_id: None
    fake_auth.authorization_gate_enforced = lambda: False
    fake_auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    fake_auth.execution_mode_for_broker = lambda broker: execution_mode
    monkeypatch.setitem(sys.modules, "ap.authorization", fake_auth)


def _run(
    monkeypatch,
    *,
    osm,
    snapshot_valid: bool = True,
    duplicate_proof: bool = False,
    execution_mode: str = "PAPER",
    data_broker=_UNSET,
):
    _install_common_stubs(monkeypatch, snapshot_valid=snapshot_valid, execution_mode=execution_mode)
    ov = importlib.import_module("ap_overnight_reeval")
    ov = importlib.reload(ov)
    monkeypatch.setattr(ov, "_et_now", lambda: FIXED_ET)
    monkeypatch.setattr(ov, "_lifecycle_ok", False, raising=False)
    monkeypatch.setattr(ov, "_fetch_watching_signals", lambda client_id: [_job()])
    monkeypatch.setattr(ov, "_shared_watch_arm_failure_already_recorded", lambda *a, **k: duplicate_proof)

    rejected_calls: list[tuple[object, str, str]] = []
    error_calls: list[tuple[object, str, str]] = []
    waiting_calls: list[tuple[object, str, str]] = []
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda job_id, client_id, reason: rejected_calls.append((job_id, client_id, reason)))
    monkeypatch.setattr(ov, "_mark_job_error", lambda job_id, client_id, reason: error_calls.append((job_id, client_id, reason)))
    monkeypatch.setattr(ov, "_mark_job_watching_reason", lambda job_id, client_id, reason: waiting_calls.append((job_id, client_id, reason)))

    broker = MagicMock()
    broker.get_prior_day_levels.return_value = {"prior_day_high": 100.0, "prior_day_low": 95.0}
    if data_broker is _UNSET:
        data_broker = broker

    master_control = MagicMock()
    master_control.evaluate.return_value = types.SimpleNamespace(
        ok=True,
        plan=_plan(),
        reason="approved",
        score=78.0,
    )

    contract_selector = MagicMock()
    contract_selector.select.return_value = "AAPL260626C00100000"

    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result = ov.run_overnight_reeval(
        client_id="tradefluencehq@gmail.com",
        broker=broker,
        data_broker=data_broker,
        master_control=master_control,
        contract_selector=contract_selector,
        order_state_machine=osm,
        entry_watcher=entry_watcher,
        force=True,
    )
    return result, rejected_calls, error_calls, waiting_calls


def test_paper_rescue_duplicate_proof_becomes_terminal_duplicate_setup(monkeypatch):
    result, rejected, errors, waiting = _run(
        monkeypatch,
        osm=_HappyOSM(),
        duplicate_proof=True,
    )
    assert result["rejected"] == 1
    assert errors == []
    assert waiting == []
    assert rejected and rejected[0][2] == "duplicate_setup:same_session_watch_arm_or_terminal_failure_proof"


def test_paper_rescue_snapshot_unavailable_writes_explicit_waiting_reason(monkeypatch):
    result, rejected, errors, waiting = _run(
        monkeypatch,
        osm=_HappyOSM(),
        snapshot_valid=False,
    )
    assert result["skipped"] == 1
    assert rejected == []
    assert errors == []
    assert waiting and waiting[0][2] == "after_hours_deferred:overnight_snapshot_unavailable"


def test_paper_rescue_create_entry_failure_writes_terminal_materialization_error(monkeypatch):
    result, rejected, errors, waiting = _run(
        monkeypatch,
        osm=_CreateFailOSM(),
    )
    assert result["errors"] == 1
    assert rejected == []
    assert waiting == []
    assert errors
    assert errors[0][2] == "order_materialization_failed:create_entry_order:RuntimeError"


def test_overnight_reeval_uses_data_broker_for_market_data(monkeypatch):
    captured = {"snapshot_broker": None, "prior_level_broker": None}

    fake_validator = types.ModuleType("ap.overnight_daily_validator")

    def _fetch_market_snapshot(ticker, broker):
        captured["snapshot_broker"] = broker
        return {"last": 100.0}

    fake_validator.fetch_market_snapshot = _fetch_market_snapshot
    fake_validator.validate_overnight_daily_signal = (
        lambda **kwargs: types.SimpleNamespace(valid=True, reason_code="", reason_text="")
    )
    fake_validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", fake_validator)

    fake_auth = types.ModuleType("ap.authorization")
    fake_auth.is_live_broker = lambda broker: False
    fake_auth.broker_live_mode_known = lambda broker: True
    fake_auth.check_live_authorization = lambda client_id: None
    fake_auth.authorization_gate_enforced = lambda: False
    fake_auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    fake_auth.execution_mode_for_broker = lambda broker: "PAPER"
    monkeypatch.setitem(sys.modules, "ap.authorization", fake_auth)

    ov = importlib.import_module("ap_overnight_reeval")
    ov = importlib.reload(ov)
    monkeypatch.setattr(ov, "_et_now", lambda: FIXED_ET)
    monkeypatch.setattr(ov, "_lifecycle_ok", False, raising=False)
    monkeypatch.setattr(ov, "_fetch_watching_signals", lambda client_id: [_job()])
    monkeypatch.setattr(ov, "_shared_watch_arm_failure_already_recorded", lambda *a, **k: False)
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda *args, **kwargs: None)
    monkeypatch.setattr(ov, "_mark_job_error", lambda *args, **kwargs: None)
    monkeypatch.setattr(ov, "_mark_job_watching_reason", lambda *args, **kwargs: None)

    broker = MagicMock()
    broker.get_prior_day_levels.return_value = {"prior_day_high": 0.0, "prior_day_low": 0.0}

    data_broker = MagicMock()

    def _data_prior_levels(ticker):
        captured["prior_level_broker"] = data_broker
        return {"prior_day_high": 100.0, "prior_day_low": 95.0}

    data_broker.get_prior_day_levels.side_effect = _data_prior_levels

    master_control = MagicMock()
    master_control.evaluate.return_value = types.SimpleNamespace(
        ok=True,
        plan=_plan(),
        reason="approved",
        score=78.0,
    )

    contract_selector = MagicMock()
    contract_selector.select.return_value = "AAPL260626C00100000"

    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result = ov.run_overnight_reeval(
        client_id="tradefluencehq@gmail.com",
        broker=broker,
        data_broker=data_broker,
        master_control=master_control,
        contract_selector=contract_selector,
        order_state_machine=_HappyOSM(),
        entry_watcher=entry_watcher,
        force=True,
    )

    assert result["armed"] == 1
    assert captured["prior_level_broker"] is data_broker
    assert captured["snapshot_broker"] is data_broker


def test_paper_mode_without_data_broker_logs_warning(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    result, rejected, errors, waiting = _run(
        monkeypatch,
        osm=_HappyOSM(),
        data_broker=None,
    )
    assert result["armed"] == 1
    assert rejected == []
    assert errors == []
    assert waiting == []
    assert "OVERNIGHT_REEVAL_MARKET_DATA_BROKER_SELECTED" in caplog.text
    assert "data_broker_present=False" in caplog.text
    assert "PAPER_OVERNIGHT_DATA_BROKER_MISSING_USING_EXECUTION_BROKER" in caplog.text


def test_live_mode_without_data_broker_falls_back_without_paper_warning(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    result, rejected, errors, waiting = _run(
        monkeypatch,
        osm=_HappyOSM(),
        execution_mode="LIVE",
        data_broker=None,
    )
    assert result["armed"] == 1
    assert rejected == []
    assert errors == []
    assert waiting == []
    assert "OVERNIGHT_REEVAL_MARKET_DATA_BROKER_SELECTED" in caplog.text
    assert "execution_mode=LIVE" in caplog.text
    assert "PAPER_OVERNIGHT_DATA_BROKER_MISSING_USING_EXECUTION_BROKER" not in caplog.text


def test_callsites_pass_data_broker_when_available():
    app_src = (REPO_ROOT / "app.py").read_text()
    rescue_src = (REPO_ROOT / "ap_paper_rescue_restart_guard.py").read_text()
    runner_src = (REPO_ROOT / "client_runner.py").read_text()
    assert "data_broker=data_broker" in runner_src
    assert "data_broker=comps[\"data_broker\"]" in rescue_src
    assert app_src.count("data_broker=(") >= 2
