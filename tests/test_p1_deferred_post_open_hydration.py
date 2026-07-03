from __future__ import annotations

import os
import sys
import types
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")


def _make_order(**overrides):
    base = {
        "local_order_id": "local-123",
        "client_id": "client@example.com",
        "signal_id": "sig-123",
        "status": "PENDING_TRIGGER",
        "kind": "ENTRY",
        "symbol": "AAPL",
        "contract": "DEFERRED:AAPL",
        "qty": 2,
        "limit_price": 0.01,
        "reserved_cost": 0.0,
        "broker_order_id": None,
        "submitted_ts": None,
        "execution_mode": "paper",
        "direction": "CALL",
        "score": 88.0,
        "tier": "A",
        "trigger_price": 201.5,
        "meta": {
            "contracts": 2,
            "max_position_usd": 400.0,
            "timeframe": "1d",
            "pattern": "2-1-2",
            "execution_mode": "paper",
        },
    }
    base.update(overrides)
    return base


def _make_monitor(contract_selector=None):
    from ap.order_monitor import APOrderMonitor

    osm = MagicMock()
    osm.record_deferred_hydration_result.return_value = True
    broker = MagicMock()
    broker.data_broker = MagicMock()
    broker.data_broker.get_quote.return_value = {
        "last": 201.75,
        "quote_time": "2026-07-02T13:37:00Z",
    }
    return APOrderMonitor(
        client_id="client@example.com",
        broker=broker,
        order_state_machine=osm,
        position_manager=MagicMock(),
        contract_selector=contract_selector,
        client_mode="PAPER",
    )


def _enable_window(monkeypatch):
    monkeypatch.setattr("ap.contract_quote_revalidator.is_market_open", lambda now=None: True)
    monkeypatch.setattr(
        "ap.order_monitor.APOrderMonitor._is_within_deferred_hydration_window",
        lambda self: True,
    )
    monkeypatch.setattr("ap.order_monitor.DEFERRED_PREBREACH_HYDRATION_ENABLED", True)
    monkeypatch.setattr("ap.order_monitor.DEFERRED_HYDRATION_MAX_PER_CYCLE", 3)


class _HydrationCursor:
    def __init__(self, row: dict, update_rowcount: int):
        self._row = row
        self._update_rowcount = update_rowcount
        self.rowcount = 0
        self.executed: list[tuple[str, tuple | None]] = []

    def execute(self, sql, params=None):
        self.executed.append((sql, params))
        if "SELECT status, contract, broker_order_id, submitted_ts" in sql:
            self.rowcount = 1 if self._row else 0
        elif "UPDATE orders" in sql:
            self.rowcount = self._update_rowcount
        else:
            self.rowcount = 0
        return self

    def fetchone(self):
        return self._row


def _install_fake_osm_db(monkeypatch, cursor: _HydrationCursor):
    import ap.order_state_machine as osm_mod

    @contextmanager
    def fake_conn():
        yield cursor

    monkeypatch.setattr(osm_mod, "conn", fake_conn)
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn: fn())


def _make_osm():
    from ap.order_state_machine import APOrderStateMachine

    return APOrderStateMachine("client@example.com")


def test_hydrates_real_contract_without_broker_submit(monkeypatch):
    selector = MagicMock()

    def _select(plan):
        plan.contract_symbol = "AAPL260717C00200000"
        plan.limit_price = 1.23
        plan.contracts = 2
        return SimpleNamespace(
            contract_symbol="AAPL260717C00200000",
            execution_price_per_share=1.23,
            affordable_contracts=2,
        )

    selector.select.side_effect = _select
    selector.get_last_failure.return_value = None

    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)

    result = monitor._maybe_hydrate_deferred_order(_make_order())

    assert result["attempted"] is True
    assert result["success"] is True
    _, kwargs = monitor.osm.record_deferred_hydration_result.call_args
    assert kwargs["contract"] == "AAPL260717C00200000"
    assert kwargs["limit_price"] == 1.23
    assert kwargs["reserved_cost"] == 246.0
    assert kwargs["contract_selection_status"] == "HYDRATED_PRE_BREACH"
    assert monitor.broker.place_order.call_count == 0
    assert monitor.osm.submit_existing_entry.call_count == 0


def test_failed_hydration_preserves_pending_trigger_and_records_reason(monkeypatch):
    selector = MagicMock()
    selector.select.return_value = None
    selector.get_last_failure.return_value = {
        "reason_code": "NO_CHAIN_DATA",
        "stage": "chain_fetch",
        "explanation": "quotes not warm yet",
    }

    monitor = _make_monitor(contract_selector=selector)
    _enable_window(monkeypatch)

    order = _make_order()
    result = monitor._maybe_hydrate_deferred_order(order)

    assert result == {"attempted": True, "success": False, "reason": "NO_CHAIN_DATA"}
    assert order["status"] == "PENDING_TRIGGER"
    _, kwargs = monitor.osm.record_deferred_hydration_result.call_args
    assert kwargs["contract_selection_status"] == "HYDRATION_DATA_PENDING"
    assert kwargs["selector_audit"]["reason_code"] == "NO_CHAIN_DATA"


def test_breach_uses_hydrated_order_row_without_rerunning_deferred_selector(monkeypatch):
    from ap_execution_core import APExecutionCore
    import ap.execution as execution_mod

    approved_plan = SimpleNamespace(
        contract_symbol="DEFERRED:AAPL",
        limit_price=0.01,
        contracts=1,
        max_position_usd=1.0,
        trigger_price=201.5,
        side="CALL",
        direction="CALL",
        execution_mode="paper",
        client_id="client@example.com",
        signal_id="sig-bridge-1",
        ticker="AAPL",
        tier="A",
        score=88.0,
        metadata={
            "contract_deferred": True,
            "execution_mode": "paper",
            "timeframe": "1d",
            "pattern": "2-1-2",
        },
    )
    watched = SimpleNamespace(
        ticker="AAPL",
        trigger_price=201.5,
        entry_trigger=201.5,
        stop_level=None,
        target_price=None,
        last_quote_ask=201.8,
        last_quote_bid=201.7,
        signal={
            "signal_id": "sig-bridge-1",
            "client_id": "client@example.com",
            "local_order_id": "local-123",
            "_approved_plan": approved_plan,
        },
    )

    hydrated_row = {
        "local_order_id": "local-123",
        "client_id": "client@example.com",
        "signal_id": "sig-bridge-1",
        "status": "PENDING_TRIGGER",
        "execution_mode": "paper",
        "contract": "AAPL260717C00200000",
        "limit_price": 2.50,
        "qty": 3,
        "reserved_cost": 750.0,
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "contract_deferred": False,
            "contract_selection_status": "HYDRATED_PRE_BREACH",
            "contract_materialized_source": "prebreach_hydration",
        },
    }

    osm = MagicMock()
    osm.get_order.return_value = hydrated_row
    osm.submit_existing_entry.return_value = {
        "ok": True,
        "local_order_id": "local-123",
        "broker_order_id": None,
    }

    core = object.__new__(APExecutionCore)
    core.broker = MagicMock()
    core.order_state_machine = osm
    core.contract_selector = MagicMock()
    core.store = MagicMock()
    core.paper = True
    core.mode = "PAPER"
    core.client_id = "client@example.com"
    core.client_email = "client@example.com"
    core.email = "client@example.com"
    core.execution_mode = "PAPER"
    core._max_positions = 7
    core._breach_risk_check = MagicMock(return_value=True)
    core._cleanup_pending_entry_order = MagicMock()
    core._emit_breach_diag = MagicMock()
    core._alert_degraded = MagicMock()
    core.master_control = SimpleNamespace(
        revalidate_exposure=MagicMock(return_value=SimpleNamespace(ok=True))
    )

    monkeypatch.setattr(execution_mod, "_refresh_ask_at_submit", lambda broker, contract: (
        2.50,
        0,
        True,
        "ok",
        {
            "submit_bid": 2.40,
            "submit_mid": 2.45,
            "submit_ask": 2.50,
            "submit_last": 2.48,
            "spread_pct": 0.04,
        },
    ))

    fake_confirm_module = types.SimpleNamespace(
        check_entry_confirmation=lambda **kwargs: types.SimpleNamespace(
            passed=True,
            fail_reason="",
            metadata={},
            to_meta=lambda **meta_kwargs: {"confirmation_passed": True},
        )
    )
    monkeypatch.setitem(sys.modules, "ap_entry_confirmation", fake_confirm_module)
    monkeypatch.setattr("ap_execution_core.funnel.inc", lambda *args, **kwargs: None)

    core._on_entry_trigger(watched)

    core.contract_selector.select.assert_not_called()
    osm.submit_existing_entry.assert_called_once()
    submit_kwargs = osm.submit_existing_entry.call_args.kwargs
    assert submit_kwargs["local_order_id"] == "local-123"
    assert submit_kwargs["limit_price"] > 0.01
    assert submit_kwargs["plan"].contract_symbol == "AAPL260717C00200000"
    assert submit_kwargs["plan"].contracts == 3
    assert submit_kwargs["plan"].max_position_usd == 750.0
    assert submit_kwargs["plan"].metadata["contract_deferred"] is False
    assert submit_kwargs["plan"].metadata["contract_materialized_source"] == "prebreach_hydration"
    assert submit_kwargs["plan"].signal_id == "sig-bridge-1"
    assert submit_kwargs["plan"].client_id == "client@example.com"
    assert submit_kwargs["plan"].execution_mode == "paper"
    assert hydrated_row["broker_order_id"] is None
    assert watched.signal["local_order_id"] == "local-123"
    assert watched.signal["client_id"] == "client@example.com"

def _build_hydrated_breach_harness():
    from ap_execution_core import APExecutionCore

    approved_plan = SimpleNamespace(
        contract_symbol="DEFERRED:AAPL",
        limit_price=0.01,
        contracts=1,
        max_position_usd=1.0,
        trigger_price=201.5,
        side="CALL",
        direction="CALL",
        execution_mode="paper",
        client_id="client@example.com",
        signal_id="sig-bridge-2",
        ticker="AAPL",
        tier="A",
        score=88.0,
        metadata={
            "contract_deferred": True,
            "execution_mode": "paper",
            "timeframe": "1d",
            "pattern": "2-1-2",
        },
    )
    watched = SimpleNamespace(
        ticker="AAPL",
        trigger_price=201.5,
        entry_trigger=201.5,
        stop_level=None,
        target_price=None,
        last_quote_ask=201.8,
        last_quote_bid=201.7,
        signal={
            "signal_id": "sig-bridge-2",
            "client_id": "client@example.com",
            "local_order_id": "local-123",
            "_approved_plan": approved_plan,
        },
    )
    hydrated_row = {
        "local_order_id": "local-123",
        "client_id": "client@example.com",
        "signal_id": "sig-bridge-2",
        "status": "PENDING_TRIGGER",
        "execution_mode": "paper",
        "contract": "AAPL260717C00200000",
        "limit_price": 2.50,
        "qty": 3,
        "reserved_cost": 750.0,
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "contract_deferred": False,
            "contract_selection_status": "HYDRATED_PRE_BREACH",
            "contract_materialized_source": "prebreach_hydration",
        },
    }
    osm = MagicMock()
    osm.get_order.return_value = hydrated_row
    osm.submit_existing_entry.return_value = {
        "ok": True,
        "local_order_id": "local-123",
        "broker_order_id": None,
    }
    core = object.__new__(APExecutionCore)
    core.broker = MagicMock()
    core.order_state_machine = osm
    core.contract_selector = MagicMock()
    core.store = MagicMock()
    core.paper = True
    core.mode = "PAPER"
    core.client_id = "client@example.com"
    core.client_email = "client@example.com"
    core.email = "client@example.com"
    core.execution_mode = "PAPER"
    core._max_positions = 7
    core._breach_risk_check = MagicMock(return_value=True)
    core._cleanup_pending_entry_order = MagicMock()
    core._emit_breach_diag = MagicMock()
    core._alert_degraded = MagicMock()
    return core, watched, osm


def _install_hydrated_submit_test_stubs(monkeypatch):
    import ap.execution as execution_mod

    monkeypatch.setattr(execution_mod, "_refresh_ask_at_submit", lambda broker, contract: (
        2.50,
        0,
        True,
        "ok",
        {
            "submit_bid": 2.40,
            "submit_mid": 2.45,
            "submit_ask": 2.50,
            "submit_last": 2.48,
            "spread_pct": 0.04,
        },
    ))
    fake_confirm_module = types.SimpleNamespace(
        check_entry_confirmation=lambda **kwargs: types.SimpleNamespace(
            passed=True,
            fail_reason="",
            metadata={},
            to_meta=lambda **meta_kwargs: {"confirmation_passed": True},
        )
    )
    monkeypatch.setitem(sys.modules, "ap_entry_confirmation", fake_confirm_module)
    monkeypatch.setattr("ap_execution_core.funnel.inc", lambda *args, **kwargs: None)

def test_hydration_applied_without_master_control_terminalizes(monkeypatch):
    core, watched, osm = _build_hydrated_breach_harness()
    _install_hydrated_submit_test_stubs(monkeypatch)
    core.master_control = None

    core._on_entry_trigger(watched)

    osm.submit_existing_entry.assert_not_called()
    core._cleanup_pending_entry_order.assert_called_once()
    assert core._cleanup_pending_entry_order.call_args.kwargs["reason"] == "hydrated_prebreach_revalidation_unavailable"
    core.store.update_signal_fields.assert_called_with(
        "sig-bridge-2",
        {
            "decision_status": "blocked_at_breach",
            "context_notes": "hydrated_prebreach_revalidation_unavailable",
        },
    )


def test_hydration_applied_with_revalidation_ok_may_proceed(monkeypatch):
    core, watched, osm = _build_hydrated_breach_harness()
    _install_hydrated_submit_test_stubs(monkeypatch)
    core.master_control = SimpleNamespace(
        revalidate_exposure=MagicMock(return_value=SimpleNamespace(ok=True))
    )

    core._on_entry_trigger(watched)

    core.master_control.revalidate_exposure.assert_called_once()
    osm.submit_existing_entry.assert_called_once()


def test_hydration_applied_with_revalidation_blocked_terminalizes(monkeypatch):
    core, watched, osm = _build_hydrated_breach_harness()
    _install_hydrated_submit_test_stubs(monkeypatch)
    core.master_control = SimpleNamespace(
        revalidate_exposure=MagicMock(
            return_value=SimpleNamespace(ok=False, reason="cap_exceeded")
        )
    )

    core._on_entry_trigger(watched)

    osm.submit_existing_entry.assert_not_called()
    core._cleanup_pending_entry_order.assert_called_once()
    assert (
        core._cleanup_pending_entry_order.call_args.kwargs["reason"]
        == "hydrated_prebreach_revalidation_blocked:cap_exceeded"
    )
def test_hydration_success_cas_miss_when_status_changed_before_persist(monkeypatch, caplog):
    osm = _make_osm()
    cur = _HydrationCursor(
        row={
            "status": "PENDING_TRIGGER",
            "contract": "DEFERRED:AAPL",
            "broker_order_id": None,
            "submitted_ts": None,
        },
        update_rowcount=0,
    )
    _install_fake_osm_db(monkeypatch, cur)

    with caplog.at_level("INFO", logger="ap.order_state_machine"):
        ok = osm.record_deferred_hydration_result(
            "local-123",
            success=True,
            status="PENDING_TRIGGER",
            contract="AAPL260717C00200000",
            limit_price=1.23,
            qty=2,
            reserved_cost=246.0,
            contract_selection_status="HYDRATED_PRE_BREACH",
        )

    assert ok is False
    assert "DEFERRED_HYDRATION_STALE_SKIP local=local-123 reason=cas_miss" in caplog.text
    update_sql, _ = cur.executed[1]
    assert "UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'" in update_sql
    assert "UPPER(COALESCE(contract,'')) LIKE 'DEFERRED:%%'" in update_sql
    assert "(broker_order_id IS NULL OR broker_order_id = '')" in update_sql
    assert "submitted_ts IS NULL" in update_sql
    assert "(limit_price IS NULL OR limit_price <= 0.01)" in update_sql


def test_hydration_success_cas_miss_when_broker_order_id_appears_before_persist(monkeypatch, caplog):
    osm = _make_osm()
    cur = _HydrationCursor(
        row={
            "status": "PENDING_TRIGGER",
            "contract": "DEFERRED:AAPL",
            "broker_order_id": None,
            "submitted_ts": None,
        },
        update_rowcount=0,
    )
    _install_fake_osm_db(monkeypatch, cur)

    with caplog.at_level("INFO", logger="ap.order_state_machine"):
        ok = osm.record_deferred_hydration_result(
            "local-123",
            success=True,
            status="PENDING_TRIGGER",
            contract="AAPL260717C00200000",
            limit_price=1.23,
            qty=2,
            reserved_cost=246.0,
        )

    assert ok is False
    assert "reason=cas_miss" in caplog.text


def test_hydration_success_cas_miss_when_submitted_ts_appears_before_persist(monkeypatch, caplog):
    osm = _make_osm()
    cur = _HydrationCursor(
        row={
            "status": "PENDING_TRIGGER",
            "contract": "DEFERRED:AAPL",
            "broker_order_id": None,
            "submitted_ts": None,
        },
        update_rowcount=0,
    )
    _install_fake_osm_db(monkeypatch, cur)

    with caplog.at_level("INFO", logger="ap.order_state_machine"):
        ok = osm.record_deferred_hydration_result(
            "local-123",
            success=True,
            status="PENDING_TRIGGER",
            contract="AAPL260717C00200000",
            limit_price=1.23,
            qty=2,
            reserved_cost=246.0,
        )

    assert ok is False
    assert "reason=cas_miss" in caplog.text


def test_hydration_success_cas_miss_when_contract_no_longer_deferred_before_persist(monkeypatch, caplog):
    osm = _make_osm()
    cur = _HydrationCursor(
        row={
            "status": "PENDING_TRIGGER",
            "contract": "DEFERRED:AAPL",
            "broker_order_id": None,
            "submitted_ts": None,
        },
        update_rowcount=0,
    )
    _install_fake_osm_db(monkeypatch, cur)

    with caplog.at_level("INFO", logger="ap.order_state_machine"):
        ok = osm.record_deferred_hydration_result(
            "local-123",
            success=True,
            status="PENDING_TRIGGER",
            contract="AAPL260717C00200000",
            limit_price=1.23,
            qty=2,
            reserved_cost=246.0,
        )

    assert ok is False
    assert "reason=cas_miss" in caplog.text


def test_successful_hydration_updates_exactly_one_row_and_cas_keeps_pending_trigger(monkeypatch):
    osm = _make_osm()
    cur = _HydrationCursor(
        row={
            "status": "PENDING_TRIGGER",
            "contract": "DEFERRED:AAPL",
            "broker_order_id": None,
            "submitted_ts": None,
        },
        update_rowcount=1,
    )
    _install_fake_osm_db(monkeypatch, cur)

    ok = osm.record_deferred_hydration_result(
        "local-123",
        success=True,
        status="PENDING_TRIGGER",
        contract="AAPL260717C00200000",
        limit_price=1.23,
        qty=2,
        reserved_cost=246.0,
    )

    assert ok is True
    assert len(cur.executed) == 2
    update_sql, update_params = cur.executed[1]
    assert "UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'" in update_sql
    assert update_params[-2:] == ("local-123", "client@example.com")


def test_failure_hydration_meta_write_cas_protects_submitted_or_broker_rows(monkeypatch, caplog):
    osm = _make_osm()
    cur = _HydrationCursor(
        row={
            "status": "PENDING_TRIGGER",
            "contract": "DEFERRED:AAPL",
            "broker_order_id": None,
            "submitted_ts": None,
        },
        update_rowcount=0,
    )
    _install_fake_osm_db(monkeypatch, cur)

    with caplog.at_level("INFO", logger="ap.order_state_machine"):
        ok = osm.record_deferred_hydration_result(
            "local-123",
            success=False,
            status="PENDING_TRIGGER",
            reason="NO_CHAIN_DATA",
            contract_selection_status="HYDRATION_DATA_PENDING",
        )

    assert ok is False
    assert "DEFERRED_HYDRATION_STALE_SKIP local=local-123 reason=cas_miss" in caplog.text
    update_sql, _ = cur.executed[1]
    assert "UPPER(COALESCE(status,'')) = 'PENDING_TRIGGER'" in update_sql
    assert "UPPER(COALESCE(contract,'')) LIKE 'DEFERRED:%%'" in update_sql
    assert "(broker_order_id IS NULL OR broker_order_id = '')" in update_sql
    assert "submitted_ts IS NULL" in update_sql
    assert "limit_price <= 0.01" not in update_sql
