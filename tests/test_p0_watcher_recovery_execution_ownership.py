from __future__ import annotations

import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import ap_execution_core as core_mod
import ap_entry_watcher
from ap_entry_watcher import APEntryWatcher, WatchedSignal, WatchState


def _watched_signal() -> WatchedSignal:
    watched = WatchedSignal(
        {
            "ticker": "SPY",
            "side": "CALL",
            "entry_price": 600.0,
            "stop_price": 595.0,
            "target_price": 610.0,
            "signal_id": "sig-1",
            "canonical_signal_id": "sig-1",
            "local_order_id": "oid-1",
            "client_id": "client@example.com",
            "execution_mode": "live",
            "contract_symbol": "DEFERRED:SPY",
            "contract_deferred": True,
            "timeframe": "1d",
            "score": 88,
            "_approved_plan": SimpleNamespace(
                contract_symbol="SPY260717C00600000",
                limit_price=1.01,
                contracts=1,
                max_position_usd=500.0,
                side="CALL",
                execution_mode="live",
                client_id="client@example.com",
                signal_id="sig-1",
                trigger_price=600.0,
                metadata={
                    "contract_deferred": True,
                    "broker_ready": True,
                    "materialization_status": "SELECTED",
                },
                ticker="SPY",
            ),
        },
        overnight=False,
    )
    watched.MOMENTUM_POLLS_REQUIRED = 2
    return watched


def _watcher_with_row(row: dict) -> APEntryWatcher:
    osm = MagicMock()
    osm.update_order_meta.return_value = True
    osm.get_order.return_value = row
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="LIVE")
    watcher.execution_mode = "live"
    watcher.paper = False
    watcher._persist_watcher_audit = MagicMock()
    return watcher


def test_package_watcher_wraps_base_poll_loop_without_reimplementing_processors():
    assert "_poll_active_signals" in APEntryWatcher.__dict__
    assert APEntryWatcher._poll_active_signals is not ap_entry_watcher._BaseAPEntryWatcher._poll_active_signals
    assert "_process_triggered_poll_result" not in APEntryWatcher.__dict__
    assert "_process_terminal_poll_result" not in APEntryWatcher.__dict__


def test_reconcile_broker_intent_keeps_watcher_and_dedup_until_retry_time():
    watcher = _watcher_with_row({
        "status": "SUBMITTED",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {"submit_intent_at": "2026-07-14T13:30:00+00:00"},
    })
    watched = _watched_signal()
    watched._watcher_ref = watcher
    watcher._pending = [watched]
    watcher._dedup_set.add("sig-1")
    watcher.on_trigger = MagicMock(return_value={
        "disposition": "RECONCILE_BROKER_INTENT",
        "reason_code": "BROKER_AMBIGUOUS_READ_TIMEOUT_RECONCILIATION_REQUIRED",
    })
    watcher._fetch_quotes = MagicMock(return_value={
        "SPY": {"bid": 600.20, "ask": 600.22, "quote_age_ms": 3}
    })

    watcher._poll_active_signals(open_protect_active=False)
    watcher._poll_active_signals(open_protect_active=False)

    assert watched in watcher._pending
    assert "sig-1" in watcher._dedup_set
    assert watched.state == WatchState.PENDING
    assert watched.deferred_retry_not_before is not None
    assert watcher.on_trigger.call_count == 1

    watcher._poll_active_signals(open_protect_active=False)

    assert watcher.on_trigger.call_count == 1


def test_trigger_timestamps_are_persisted_before_execution_callback():
    row = {
        "status": "SUBMITTED",
        "broker_order_id": "BRK-1",
        "submitted_ts": "2026-07-14T13:30:01+00:00",
        "meta": {"submit_intent_at": "2026-07-14T13:30:00+00:00"},
    }
    watcher = _watcher_with_row(row)
    watched = _watched_signal()
    watched._watcher_ref = watcher
    watcher._pending = [watched]
    watcher._dedup_set.add("sig-1")
    events: list[str] = []

    def _update_order_meta(_oid, patch):
        if "trigger_confirmed_at" in patch:
            events.append("timestamps")
        return True

    def _on_trigger(_watched):
        events.append("callback")
        return {"disposition": "SUBMITTED"}

    watcher.order_state_machine.update_order_meta.side_effect = _update_order_meta
    watcher.on_trigger = MagicMock(side_effect=_on_trigger)
    watcher._fetch_quotes = MagicMock(return_value={
        "SPY": {"bid": 600.20, "ask": 600.22, "quote_age_ms": 3}
    })

    watcher._poll_active_signals(open_protect_active=False)
    watcher._poll_active_signals(open_protect_active=False)

    assert events[:2] == ["timestamps", "callback"]
    assert watched not in watcher._pending
    assert "sig-1" not in watcher._dedup_set


def test_live_deferred_timestamp_write_failure_retains_watcher_without_callback():
    watcher = _watcher_with_row({
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {},
    })
    watched = _watched_signal()
    watched._watcher_ref = watcher
    watcher._pending = [watched]
    watcher._dedup_set.add("sig-1")
    watcher.order_state_machine.update_order_meta.return_value = False
    watcher.on_trigger = MagicMock(return_value={"disposition": "SUBMITTED"})
    watcher._fetch_quotes = MagicMock(return_value={
        "SPY": {"bid": 600.20, "ask": 600.22, "quote_age_ms": 3}
    })

    watcher._poll_active_signals(open_protect_active=False)
    watcher._poll_active_signals(open_protect_active=False)

    watcher.on_trigger.assert_not_called()
    assert watched in watcher._pending
    assert "sig-1" in watcher._dedup_set
    assert watched.deferred_retry_not_before is not None


def test_callback_none_with_pending_row_does_not_remove_watcher():
    watcher = _watcher_with_row({
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {},
    })
    watched = _watched_signal()
    watched._watcher_ref = watcher
    watcher._pending = [watched]
    watcher._dedup_set.add("sig-1")
    watcher.on_trigger = MagicMock(return_value=None)
    watcher._fetch_quotes = MagicMock(return_value={
        "SPY": {"bid": 600.20, "ask": 600.22, "quote_age_ms": 3}
    })

    watcher._poll_active_signals(open_protect_active=False)
    watcher._poll_active_signals(open_protect_active=False)

    assert watched in watcher._pending
    assert "sig-1" in watcher._dedup_set


def _install_fake_execution(monkeypatch):
    fake_execution_mod = types.ModuleType("ap.execution")
    fake_execution_mod._refresh_ask_at_submit = lambda broker, contract: (
        1.02,
        5,
        True,
        "ok",
        {
            "submit_bid": 1.00,
            "submit_ask": 1.02,
            "submit_last": 1.01,
            "submit_mid": 1.01,
            "spread_pct": 0.0198,
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution_mod)


def _execution_core(monkeypatch, submit_result: dict):
    _install_fake_execution(monkeypatch)
    plan = SimpleNamespace(
        contract_symbol="SPY260717C00600000",
        limit_price=1.01,
        contracts=1,
        max_position_usd=500.0,
        side="CALL",
        execution_mode="paper",
        client_id="client@example.com",
        signal_id="sig-1",
        trigger_price=600.0,
        metadata={"broker_ready": True, "materialization_status": "SELECTED"},
        ticker="SPY",
    )
    osm = MagicMock()
    osm.client_id = "client@example.com"
    osm.execution_mode = "paper"
    order_row = {
        "local_order_id": "oid-1",
        "client_id": "client@example.com",
        "execution_mode": "paper",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "contract": "SPY260717C00600000",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "broker_ready": True,
            "materialization_status": "SELECTED",
            "trigger_crossed_at": "2026-07-14T13:29:55+00:00",
            "trigger_confirmed_at": "2026-07-14T13:30:00+00:00",
        },
    }
    osm.get_order.return_value = order_row
    def _update_order_meta(_oid, patch):
        order_row["meta"].update(patch)
        return True
    osm.update_order_meta.side_effect = _update_order_meta
    osm.submit_existing_entry.return_value = submit_result
    osm.cancel_pending_entry.return_value = True
    osm.expire_pending_entry.return_value = True
    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = True
    core.mode = "PAPER"
    core.execution_mode = "paper"
    core.email = "client@example.com"
    core.client_id = "client@example.com"
    core.broker = SimpleNamespace(
        cfg=SimpleNamespace(base_url="https://sandbox.tradier.com"),
        sandbox=True,
        get_quote=MagicMock(return_value={
            "bid": 600.20,
            "ask": 600.22,
            "quote_age_ms": 10,
            "source": "test",
        }),
    )
    core.store = MagicMock()
    core.order_state_machine = osm
    core.contract_selector = None
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._alert_degraded = MagicMock()
    core._cleanup_pending_entry_order = core_mod.APExecutionCore._cleanup_pending_entry_order.__get__(core, type(core))
    core._classify_recovered_ownership_loss = core_mod.APExecutionCore._classify_recovered_ownership_loss.__get__(core, type(core))
    return core, osm


def _submit_ready_watched_signal() -> WatchedSignal:
    watched = _watched_signal()
    watched.signal["contract_symbol"] = "SPY260717C00600000"
    watched.signal["contract_deferred"] = False
    watched.signal["_approved_plan"] = SimpleNamespace(
        contract_symbol="SPY260717C00600000",
        limit_price=1.01,
        contracts=1,
        max_position_usd=500.0,
        side="CALL",
        execution_mode="paper",
        client_id="client@example.com",
        signal_id="sig-1",
        trigger_price=600.0,
        metadata={"broker_ready": True, "materialization_status": "SELECTED"},
        ticker="SPY",
    )
    return watched


def test_execution_core_reconciliation_result_never_cancels_pending_entry(monkeypatch):
    core, osm = _execution_core(monkeypatch, {
        "ok": False,
        "local_order_id": "oid-1",
        "reconciliation_required": True,
        "error": "BROKER_AMBIGUOUS_READ_TIMEOUT_RECONCILIATION_REQUIRED",
    })
    watched = _submit_ready_watched_signal()
    result = core_mod.APExecutionCore._on_entry_trigger(core, watched)

    assert result == {
        "disposition": "RECONCILE_BROKER_INTENT",
        "reason_code": "BROKER_AMBIGUOUS_READ_TIMEOUT_RECONCILIATION_REQUIRED",
        "broker_order_id": None,
    }
    osm.cancel_pending_entry.assert_not_called()
    osm.expire_pending_entry.assert_not_called()
    core.store.update_signal_fields.assert_any_call(
        "sig-1",
        {
            "decision_status": "reconcile_pending",
            "context_notes": "osm_reconcile_broker_intent=BROKER_AMBIGUOUS_READ_TIMEOUT_RECONCILIATION_REQUIRED",
        },
    )


def test_execution_core_split_brain_with_broker_id_preserves_broker_identity(monkeypatch):
    core, osm = _execution_core(monkeypatch, {
        "ok": False,
        "local_order_id": "oid-1",
        "split_brain": True,
        "error": "ENTRY_SPLIT_BRAIN_QUARANTINED",
        "broker_order_id": "TR-123",
    })
    result = core_mod.APExecutionCore._on_entry_trigger(core, _submit_ready_watched_signal())

    assert result["disposition"] == "RECONCILE_BROKER_INTENT"
    assert result["reason_code"] == "ENTRY_SPLIT_BRAIN_QUARANTINED"
    assert result["broker_order_id"] == "TR-123"
    osm.cancel_pending_entry.assert_not_called()
    osm.expire_pending_entry.assert_not_called()


def test_execution_core_freezes_breach_before_existing_submit_path(monkeypatch):
    core, osm = _execution_core(monkeypatch, {
        "ok": False,
        "local_order_id": "oid-1",
        "reconciliation_required": True,
        "error": "BROKER_AMBIGUOUS_READ_TIMEOUT_RECONCILIATION_REQUIRED",
    })
    captured = {}

    def capture(signal, **kwargs):
        captured["signal"] = signal
        captured["kwargs"] = kwargs
        return {"ok": True, "accepted": True}

    monkeypatch.setattr(
        "ap.intelligence_context_handoff.enqueue_breach_context_best_effort", capture
    )
    watched = _submit_ready_watched_signal()
    watched.signal["execution_mode"] = "paper"
    watched.trigger_crossed_at = datetime(2026, 7, 14, 13, 30, tzinfo=timezone.utc)
    watched.triggered_at = datetime(2026, 7, 14, 13, 30, 2, tzinfo=timezone.utc)
    watched.breach_price = 600.25
    watched.first_breach_bid = 600.20
    watched.first_breach_ask = 600.25
    watched.last_quote_bid = 600.20
    watched.last_quote_ask = 600.25

    result = core_mod.APExecutionCore._on_entry_trigger(core, watched)

    assert result["disposition"] == "RECONCILE_BROKER_INTENT"
    assert captured["kwargs"]["local_order_id"] == "oid-1"
    assert captured["kwargs"]["execution_mode"] == "PAPER"
    frozen = captured["signal"]
    assert frozen["trigger_source"] == "watcher_confirmed_breach"
    assert frozen["trigger_crossed_at"] == "2026-07-14T13:30:00+00:00"
    assert frozen["underlying_price"] == 600.25
    assert "_approved_plan" not in frozen
    assert osm.submit_existing_entry.call_count == 1
