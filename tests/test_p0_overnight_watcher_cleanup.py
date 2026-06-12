from __future__ import annotations

import importlib
import logging
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

FIXED_ET = datetime(2026, 6, 12, 9, 15, tzinfo=ZoneInfo("America/New_York"))


class _FakeOrderStateMachine:
    def __init__(self):
        self.orders: dict[str, dict] = {}
        self.expire_calls: list[tuple[str, str]] = []
        self.cancel_calls: list[tuple[str, str]] = []
        self.transition_calls: list[tuple[str, str, dict]] = []

    def create_entry_order(self, plan, initial_status="CREATED", execution_mode=None):
        local_order_id = "local-ord-1"
        self.orders[local_order_id] = {
            "status": initial_status,
            "execution_mode": execution_mode,
            "contract": getattr(plan, "contract_symbol", None),
        }
        return local_order_id

    def mark_entry_pending_trigger(self, local_order_id: str) -> bool:
        self.orders.setdefault(local_order_id, {})["status"] = "PENDING_TRIGGER"
        return True

    def expire_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.expire_calls.append((local_order_id, reason))
        self.orders.setdefault(local_order_id, {})["status"] = "EXPIRED"
        self.orders[local_order_id]["last_error"] = reason
        return True

    def cancel_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.cancel_calls.append((local_order_id, reason))
        self.orders.setdefault(local_order_id, {})["status"] = "CANCELED"
        self.orders[local_order_id]["last_error"] = reason
        return True

    def transition(self, local_order_id: str, new_status: str, **kwargs) -> bool:
        self.transition_calls.append((local_order_id, new_status, kwargs))
        self.orders.setdefault(local_order_id, {})["status"] = new_status
        self.orders[local_order_id].update(kwargs)
        return True


def _install_reeval_stubs(monkeypatch):
    fake_validator = types.ModuleType("ap.overnight_daily_validator")
    fake_validator.fetch_market_snapshot = lambda ticker, broker: {"last": 100.0}
    fake_validator.validate_overnight_daily_signal = (
        lambda **kwargs: types.SimpleNamespace(
            valid=True,
            reason_code="",
            reason_text="",
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
    fake_auth.execution_mode_for_broker = lambda broker: "PAPER"
    monkeypatch.setitem(sys.modules, "ap.authorization", fake_auth)


def _make_plan():
    return types.SimpleNamespace(
        plan_id="plan-001",
        signal_id="sig-001",
        ticker="AAPL",
        side="CALL",
        direction="CALL",
        score=75.0,
        timeframe="1d",
        entry_trigger=101.0,
        trigger_price=101.0,
        trigger_type="breach",
        prior_day_high=100.0,
        prior_day_low=95.0,
        pattern="2-3",
        tier="A",
        contract_symbol="AAPL260619C00100000",
        contracts=1,
        limit_price=1.25,
        metadata={},
    )


def _make_signal():
    return {
        "signal_id": "sig-001",
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "timeframe": "1d",
        "score": 75.0,
        "pattern": "2-3",
        "tier": "A",
        "entry_trigger": 101.0,
        "created_at": "2026-06-11T20:00:00+00:00",
    }


def _run_reeval(monkeypatch, entry_watcher):
    import ap_overnight_reeval as ov

    _install_reeval_stubs(monkeypatch)
    monkeypatch.setattr(ov, "_et_now", lambda: FIXED_ET)
    monkeypatch.setattr(
        ov,
        "_fetch_watching_signals",
        lambda client_id: [{"id": "job-001", "signal_id": "sig-001", "payload": _make_signal()}],
    )

    broker = MagicMock()
    broker.get_prior_day_levels.return_value = {
        "prior_day_high": 100.0,
        "prior_day_low": 95.0,
    }

    master_control = MagicMock()
    master_control.evaluate.return_value = types.SimpleNamespace(
        ok=True,
        plan=_make_plan(),
        reason="approved",
        score=75.0,
    )

    contract_selector = MagicMock()
    contract_selector.select.return_value = "AAPL260619C00100000"

    osm = _FakeOrderStateMachine()
    result = ov.run_overnight_reeval(
        client_id="client-1",
        broker=broker,
        master_control=master_control,
        contract_selector=contract_selector,
        order_state_machine=osm,
        entry_watcher=entry_watcher,
        force=True,
    )
    return result, osm


def _make_pending_trigger_order(*, age_seconds: int) -> dict:
    created = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    return {
        "local_order_id": "pending-1",
        "broker_order_id": None,
        "status": "PENDING_TRIGGER",
        "symbol": "AAPL",
        "contract": "DEFERRED:AAPL",
        "position_id": None,
        "signal_id": "sig-001",
        "plan_id": "plan-001",
        "created_ts": created.isoformat(),
        "submitted_ts": None,
        "limit_price": 1.25,
        "price": 1.25,
        "fill_price": None,
        "score": 75.0,
        "tier": "A",
        "trigger_price": 101.0,
        "meta": {},
    }


def test_overnight_watch_returns_false_expires_pending_order(monkeypatch, caplog):
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = False
    entry_watcher._last_reject_reason = "armed_false"

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm = _run_reeval(monkeypatch, entry_watcher)

    assert result["errors"] == 1
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:armed_false")
    ]
    assert osm.orders["local-ord-1"]["status"] == "EXPIRED"
    assert "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_START" in caplog.text
    assert "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_DONE" in caplog.text


def test_overnight_watch_raises_expires_pending_order(monkeypatch, caplog):
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("watcher boom")

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm = _run_reeval(monkeypatch, entry_watcher)

    assert result["errors"] == 1
    assert len(osm.expire_calls) == 1
    local_order_id, reason = osm.expire_calls[0]
    assert local_order_id == "local-ord-1"
    assert reason == "overnight_watch_arm_failed:exception:watcher boom"
    assert osm.orders["local-ord-1"]["status"] == "EXPIRED"
    assert "OVERNIGHT_WATCH_ARM_EXCEPTION_CLEANUP_DONE" in caplog.text


def test_pending_trigger_watchdog_expires_orphan(monkeypatch, caplog):
    from ap import order_monitor as om_mod

    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 60)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

    osm = _FakeOrderStateMachine()
    monitor = om_mod.APOrderMonitor(
        client_id="client-1",
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
    )
    monitor._emit_order_event = MagicMock()
    monitor._get_active_entry_orders = MagicMock(
        return_value=[_make_pending_trigger_order(age_seconds=120)]
    )

    caplog.set_level(logging.INFO, logger="ap.order_monitor")
    monitor._check_entry_orders()

    assert len(osm.expire_calls) == 1
    local_order_id, reason = osm.expire_calls[0]
    assert local_order_id == "pending-1"
    assert reason.startswith(
        "PENDING_TRIGGER_ORPHAN_EXPIRED: no_broker_order_id no_submitted_ts age="
    )
    assert osm.orders["pending-1"]["status"] == "EXPIRED"
    assert "PENDING_TRIGGER_WATCHDOG_SEEN" in caplog.text
    assert "PENDING_TRIGGER_ORPHAN_EXPIRED" in caplog.text


def test_pending_trigger_watchdog_does_not_touch_recent_order(monkeypatch, caplog):
    from ap import order_monitor as om_mod

    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 300)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

    osm = _FakeOrderStateMachine()
    monitor = om_mod.APOrderMonitor(
        client_id="client-1",
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
    )
    monitor._emit_order_event = MagicMock()
    monitor._get_active_entry_orders = MagicMock(
        return_value=[_make_pending_trigger_order(age_seconds=45)]
    )

    caplog.set_level(logging.INFO, logger="ap.order_monitor")
    monitor._check_entry_orders()

    assert osm.expire_calls == []
    assert osm.transition_calls == []
    assert "PENDING_TRIGGER_WATCHDOG_SEEN" in caplog.text
    assert "PENDING_TRIGGER_ORPHAN_EXPIRED" not in caplog.text


def test_hard_70_floor_unchanged(monkeypatch):
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "true")
    monkeypatch.setenv("MIN_CLIENT_SCORE", "70")
    monkeypatch.setenv("ALLOW_CLIENT_TIER_B", "true")
    monkeypatch.setenv("DAILY_CLIENT_PATTERN_WHITELIST", "2-3,3-2-2,1-2_2D")
    monkeypatch.setenv("INTRADAY_CLIENT_PATTERN_WHITELIST", "")
    monkeypatch.setenv("ALLOW_FAILED_DIR_CLIENT", "false")
    monkeypatch.setenv("ENTRY_CONFIRM_SECONDS", "45")
    monkeypatch.setenv("MAX_CLIENT_TRADES_PER_DAY", "5")
    monkeypatch.setenv("MAX_CLIENT_DAILY_TRADES", "3")
    monkeypatch.setenv("MAX_CLIENT_INTRADAY_TRADES", "2")
    monkeypatch.setenv("MAX_CLIENT_SYMBOL_TRADES_PER_DAY", "1")
    monkeypatch.setenv("MAX_PRE_ENTRY_OPTION_FADE_PCT", "8")
    monkeypatch.setenv("MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT", "0.25")

    gate_mod = importlib.import_module("ap_hybrid_client_quality_gate")
    gate_mod = importlib.reload(gate_mod)

    signal = {
        "symbol": "AAPL",
        "direction": "CALL",
        "timeframe": "1d",
        "pattern": "2-3",
        "tier": "A",
        "trigger_price": 180.0,
        "stop_underlying": 175.0,
        "target_underlying": 190.0,
    }
    empty_snap = {
        "trades_today": 0,
        "daily_trades": 0,
        "intraday_trades": 0,
        "symbol_trades": {},
    }

    blocked = gate_mod.evaluate_client_quality_gate(
        {**signal, "score": 69},
        "client-1",
        empty_snap,
    )
    allowed = gate_mod.evaluate_client_quality_gate(
        {**signal, "score": 70},
        "client-1",
        empty_snap,
    )

    assert blocked.allowed is False
    assert blocked.block_reason == "client_score_below_70"
    assert allowed.allowed is True
