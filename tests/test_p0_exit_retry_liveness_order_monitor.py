# tests/test_p0_exit_retry_liveness_order_monitor.py
# =============================================================================
# P0 regression: PR #423 — restore stale-EXIT cancel/retry liveness under
# watchdog mode, require independent fresh post-cancel broker proof (not the
# cancel response), and prevent duplicate cancel on overlapping poll cycles.
#
# Motivating incident: 2026-08-07 AVGO PAPER — qty=7, scale-out
# SELL_TO_CLOSE qty=2 stuck WORKING under ORDER_MONITOR_MODE=watchdog; broker
# continued reserving the 2 contracts; a later full-flatten attempt failed
# because only 5 of 7 were available.
# =============================================================================

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap.order_monitor import (  # noqa: E402
    APOrderMonitor,
    NewerExitLookup,
    NewerExitLookupState,
)


def _monitor(**kwargs):
    return APOrderMonitor(
        client_id="mon@test.local",
        broker=kwargs.get("broker", MagicMock()),
        order_state_machine=kwargs.get("osm", MagicMock()),
        position_manager=kwargs.get("pm", MagicMock()),
        exit_engine=kwargs.get("exit_engine", MagicMock()),
        entry_watcher=kwargs.get("entry_watcher", MagicMock()),
        client_mode="PAPER",
        data_broker=kwargs.get("data_broker"),
    )


def _watchdog_mode(monkeypatch, *, stale_exit_recovery=True):
    import ap.order_monitor as om_mod
    monkeypatch.setattr(om_mod, "ORDER_MONITOR_MODE", "watchdog")
    monkeypatch.setattr(om_mod, "ORDER_MONITOR_CAN_ACT", False)
    monkeypatch.setattr(
        om_mod, "ALLOW_STALE_EXIT_RECOVERY_IN_WATCHDOG", stale_exit_recovery
    )


def _actor_mode(monkeypatch):
    import ap.order_monitor as om_mod
    monkeypatch.setattr(om_mod, "ORDER_MONITOR_MODE", "actor")
    monkeypatch.setattr(om_mod, "ORDER_MONITOR_CAN_ACT", True)


class _DurableCancelOSM:
    """Durable row double used to model a monitor/process restart."""

    def __init__(self):
        self.row = {
            "local_order_id": "loc-restart",
            "kind": "EXIT",
            "position_id": "pos-restart",
            "broker_order_id": "bro-restart",
            "status": "EXIT_ACKNOWLEDGED",
            "meta": {},
        }
        self.transitions = []

    def get_order(self, local_order_id):
        if local_order_id != self.row["local_order_id"]:
            return None
        row = dict(self.row)
        row["meta"] = dict(self.row["meta"])
        return row

    def update_order_meta(self, local_order_id, meta_patch):
        if local_order_id != self.row["local_order_id"]:
            return False
        self.row["meta"].update(meta_patch)
        return True

    def persist_stale_exit_cancel_attempt(self, local_order_id, broker_order_id, attempt):
        if (
            local_order_id != self.row["local_order_id"]
            or broker_order_id != self.row["broker_order_id"]
        ):
            return False
        marker_key = "stale_exit_cancel_liveness"
        marker = self.row["meta"].get(marker_key)
        if marker is not None:
            if not isinstance(marker, dict) or marker.get("broker_order_id") != broker_order_id:
                return False
            try:
                existing_attempt = int(marker["attempt"])
            except (KeyError, TypeError, ValueError, OverflowError):
                return False
            if existing_attempt < 0 or existing_attempt >= int(attempt):
                return False
        self.row["meta"][marker_key] = {
            "broker_order_id": broker_order_id,
            "attempt": int(attempt),
            "updated_at": "test",
        }
        return True

    def transition(self, local_order_id, status, **kwargs):
        self.transitions.append((local_order_id, status, kwargs))
        if local_order_id != self.row["local_order_id"]:
            return False
        self.row["status"] = status
        return True


def _install_replacement_lookup_db(monkeypatch, outcomes):
    """Run the production SQL seam against deterministic cursor outcomes."""
    import ap.order_monitor as om_mod

    pending = list(outcomes)
    current_rows = []
    captured = []

    class _Cursor:
        def execute(self, sql, params):
            captured.append((sql, params))

        def fetchall(self):
            return list(current_rows)

    class _Connection:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def execute(self, sql, params):
            cursor = _Cursor()
            cursor.execute(sql, params)
            return cursor

        def fetchall(self):
            return list(current_rows)

    def _conn():
        return _Connection()

    def _retry(fn):
        if not pending:
            raise AssertionError("replacement lookup outcome queue exhausted")
        outcome = pending.pop(0)
        if isinstance(outcome, BaseException):
            raise outcome
        current_rows[:] = outcome
        return fn()

    monkeypatch.setattr(om_mod, "conn", _conn)
    monkeypatch.setattr(om_mod, "run_with_retry", _retry)
    return captured


def _reopen_gate_monitor(monkeypatch, *, pm):
    import ap.order_monitor as om_mod

    _actor_mode(monkeypatch)
    monkeypatch.setattr(om_mod, "ALLOW_ORDER_MONITOR_POSITION_REOPEN", True)
    mon = _monitor(pm=pm)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    return mon


def _reopen_gate_kwargs(mon):
    return {
        "position_id": "pos-reopen",
        "canceled_exit_order_id": "loc-old",
        "contract": "AAPL260814C00200000",
        "reason": "broker-confirmed cancel",
    }


def _replacement_row(local_id="loc-new", broker_id="bro-new", status="EXIT_SUBMITTED"):
    return {
        "local_order_id": local_id,
        "broker_order_id": broker_id,
        "status": status,
        "created_ts": "2026-08-11T12:00:01+00:00",
        "submitted_ts": "2026-08-11T12:00:02+00:00",
    }


def test_position_reopen_db_failure_is_unavailable_and_holds(monkeypatch):
    _install_replacement_lookup_db(monkeypatch, [RuntimeError("postgres unavailable")])
    pm = MagicMock(spec=[
        "revert_position_to_open",
        "revertpositiontoopen",
        "revert_to_open_after_exit_cancel",
        "update_position",
    ])
    mon = _reopen_gate_monitor(monkeypatch, pm=pm)

    lookup = mon._get_newer_active_exit_order(
        position_id="pos-reopen", canceled_exit_order_id="loc-old"
    )
    assert lookup.state is NewerExitLookupState.UNAVAILABLE
    mon._get_newer_active_exit_order = MagicMock(return_value=lookup)

    assert mon._guarded_revert_position_open_after_exit_cancel(**_reopen_gate_kwargs(mon)) is False
    pm.revert_position_to_open.assert_not_called()
    pm.revertpositiontoopen.assert_not_called()
    pm.revert_to_open_after_exit_cancel.assert_not_called()
    pm.update_position.assert_not_called()
    hold = mon._emit_order_event.call_args
    assert hold.kwargs["decision"] == "HOLD"
    assert hold.kwargs["reason_code"] == "POSITION_REOPEN_HOLD_REPLACEMENT_LOOKUP_UNAVAILABLE"
    assert hold.kwargs["inputs"]["lookup_state"] == "UNAVAILABLE"


def test_position_reopen_authoritative_zero_rows_is_permitted(monkeypatch):
    captured = _install_replacement_lookup_db(monkeypatch, [[]])
    pm = MagicMock(spec=[
        "revert_position_to_open",
        "revertpositiontoopen",
        "revert_to_open_after_exit_cancel",
        "update_position",
    ])
    pm.revert_position_to_open.return_value = True
    mon = _reopen_gate_monitor(monkeypatch, pm=pm)

    lookup = mon._get_newer_active_exit_order(
        position_id="pos-reopen", canceled_exit_order_id="loc-old"
    )
    assert lookup.state is NewerExitLookupState.AUTHORITATIVE_NONE
    assert lookup.count == 0
    mon._get_newer_active_exit_order = MagicMock(return_value=lookup)

    assert mon._guarded_revert_position_open_after_exit_cancel(**_reopen_gate_kwargs(mon)) is True
    pm.revert_position_to_open.assert_called_once()
    assert captured
    assert "LIMIT 1" not in captured[0][0].upper()


def test_position_reopen_single_newer_exit_is_found_and_blocked(monkeypatch):
    row = _replacement_row()
    _install_replacement_lookup_db(monkeypatch, [[row]])
    pm = MagicMock(spec=[
        "revert_position_to_open",
        "revertpositiontoopen",
        "revert_to_open_after_exit_cancel",
        "update_position",
    ])
    mon = _reopen_gate_monitor(monkeypatch, pm=pm)

    lookup = mon._get_newer_active_exit_order(
        position_id="pos-reopen", canceled_exit_order_id="loc-old"
    )
    assert lookup.state is NewerExitLookupState.FOUND
    assert lookup.row["local_order_id"] == "loc-new"
    mon._get_newer_active_exit_order = MagicMock(return_value=lookup)

    assert mon._guarded_revert_position_open_after_exit_cancel(**_reopen_gate_kwargs(mon)) is False
    pm.revert_position_to_open.assert_not_called()
    pm.update_position.assert_not_called()


def test_position_reopen_multiple_newer_exits_is_ambiguous_and_holds(monkeypatch):
    rows = [_replacement_row("loc-new-a", "bro-new-a"), _replacement_row("loc-new-b", "bro-new-b")]
    _install_replacement_lookup_db(monkeypatch, [rows])
    pm = MagicMock(spec=[
        "revert_position_to_open",
        "revertpositiontoopen",
        "revert_to_open_after_exit_cancel",
        "update_position",
    ])
    mon = _reopen_gate_monitor(monkeypatch, pm=pm)

    lookup = mon._get_newer_active_exit_order(
        position_id="pos-reopen", canceled_exit_order_id="loc-old"
    )
    assert lookup.state is NewerExitLookupState.AMBIGUOUS
    assert lookup.count == 2
    assert [item["local_order_id"] for item in lookup.diagnostic()["replacement_identities"]] == [
        "loc-new-a", "loc-new-b"
    ]
    mon._get_newer_active_exit_order = MagicMock(return_value=lookup)

    assert mon._guarded_revert_position_open_after_exit_cancel(**_reopen_gate_kwargs(mon)) is False
    pm.revert_position_to_open.assert_not_called()
    pm.revertpositiontoopen.assert_not_called()
    pm.revert_to_open_after_exit_cancel.assert_not_called()
    pm.update_position.assert_not_called()
    hold = mon._emit_order_event.call_args
    assert hold.kwargs["decision"] == "HOLD"
    assert hold.kwargs["reason_code"] == "POSITION_REOPEN_HOLD_REPLACEMENT_LOOKUP_AMBIGUOUS"
    assert hold.kwargs["inputs"]["replacement_count"] == 2
    assert hold.kwargs["inputs"]["replacement_identities"][1]["broker_order_id"] == "bro-new-b"


def test_position_reopen_db_failure_then_authoritative_none_recovers(monkeypatch):
    _install_replacement_lookup_db(monkeypatch, [RuntimeError("temporary postgres failure"), []])
    pm = MagicMock(spec=[
        "revert_position_to_open",
        "revertpositiontoopen",
        "revert_to_open_after_exit_cancel",
        "update_position",
    ])
    pm.revert_position_to_open.return_value = True
    mon = _reopen_gate_monitor(monkeypatch, pm=pm)

    assert mon._guarded_revert_position_open_after_exit_cancel(**_reopen_gate_kwargs(mon)) is False
    pm.revert_position_to_open.assert_not_called()
    assert mon._guarded_revert_position_open_after_exit_cancel(**_reopen_gate_kwargs(mon)) is True
    pm.revert_position_to_open.assert_called_once()
    assert any(call.kwargs["decision"] == "HOLD" for call in mon._emit_order_event.call_args_list)


def test_position_reopen_db_failure_then_replacement_discovery_stays_blocked(monkeypatch):
    _install_replacement_lookup_db(
        monkeypatch,
        [RuntimeError("temporary postgres failure"), [_replacement_row()]],
    )
    pm = MagicMock(spec=[
        "revert_position_to_open",
        "revertpositiontoopen",
        "revert_to_open_after_exit_cancel",
        "update_position",
    ])
    mon = _reopen_gate_monitor(monkeypatch, pm=pm)

    assert mon._guarded_revert_position_open_after_exit_cancel(**_reopen_gate_kwargs(mon)) is False
    assert mon._guarded_revert_position_open_after_exit_cancel(**_reopen_gate_kwargs(mon)) is False
    pm.revert_position_to_open.assert_not_called()
    pm.revertpositiontoopen.assert_not_called()
    pm.revert_to_open_after_exit_cancel.assert_not_called()
    pm.update_position.assert_not_called()


# ── 1. Watchdog stale-EXIT recovery allowed by default ──────────────────────

def test_watchdog_stale_exit_recovery_enabled_by_default_cancels_and_confirms(monkeypatch):
    """The AVGO shape: watchdog mode, working stale exit, exact broker id
    resolved. With the flag on (default), the monitor must now: query
    broker, cancel, independently re-query for terminal proof, transition
    to CANCELED, clear_exit_in_flight, and hand off replacement authority.
    """
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)

    broker = MagicMock()
    call_log = []

    def _get_order(oid):
        call_log.append(("get", oid))
        # First GET (pre-cancel) says working; second GET (post-cancel,
        # independent proof) says canceled.
        if sum(1 for c in call_log if c[0] == "get") == 1:
            return {"status": "working"}
        return {"status": "canceled"}

    broker.get_order.side_effect = _get_order
    broker.cancel_order.return_value = {"status": "pending"}  # deliberately NOT terminal

    osm = MagicMock()
    osm.get_order.return_value = {
        "kind": "EXIT", "position_id": "pos-avgo", "broker_order_id": "bro-avgo",
    }
    exit_engine = MagicMock()
    pm = MagicMock()

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine, pm=pm)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._get_newer_active_exit_order = MagicMock(return_value=None)

    mon._handle_stale_exit(
        local_order_id="loc-avgo", status="WORKING", contract="AVGO260814C00350000",
        age_secs=120.0, position_id="pos-avgo", reason="stale exit AVGO scale-out qty=2",
    )

    assert broker.cancel_order.call_count == 1
    get_calls = [c for c in call_log if c[0] == "get"]
    assert len(get_calls) == 2, "must perform pre-cancel GET AND independent post-cancel GET"
    osm.transition.assert_called_once_with(
        "loc-avgo",
        "CANCELED",
        broker_order_id="bro-avgo",
        position_id="pos-avgo",
        last_error="stale exit AVGO scale-out qty=2",
    )
    exit_engine.clear_exit_in_flight.assert_called_once_with(
        "pos-avgo",
        reason="stale exit AVGO scale-out qty=2",
        local_order_id="loc-avgo",
        broker_order_id="bro-avgo",
    )
    exit_engine.mark_exit_replacement_safe.assert_called_once()
    _, kwargs = exit_engine.mark_exit_replacement_safe.call_args
    assert kwargs["local_order_id"] == "loc-avgo"
    assert kwargs["broker_order_id"] == "bro-avgo"
    pm.update_position.assert_not_called()


@pytest.mark.parametrize("broker_status", ["open", "working", "accepted", "queued"])
def test_check_exit_orders_routes_aged_ack_live_status_to_stale_owner(
    monkeypatch, broker_status
):
    """The production poller must reach stale recovery for every live ACK alias."""
    import ap.order_monitor as om_mod

    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    monkeypatch.setattr(om_mod, "TIMEOUT_EXIT_ACK", 0)
    old_ts = datetime.now(timezone.utc) - timedelta(seconds=30)
    order = {
        "local_order_id": "loc-ack-live",
        "broker_order_id": "bro-ack-live",
        "position_id": "pos-ack-live",
        "contract": "AVGO260814C00350000",
        "status": "EXIT_ACKNOWLEDGED",
        "created_ts": old_ts,
        "submitted_ts": old_ts,
    }
    mon = _monitor(osm=MagicMock())
    mon._get_active_exit_orders = MagicMock(return_value=[order])
    mon._query_broker_order = MagicMock(return_value=broker_status)
    mon._handle_stale_exit = MagicMock()

    mon._check_exit_orders()

    mon._query_broker_order.assert_called_once_with("bro-ack-live")
    mon._handle_stale_exit.assert_called_once()
    assert mon._handle_stale_exit.call_args.args[0:2] == (
        "loc-ack-live", "EXIT_ACKNOWLEDGED",
    )


def test_check_exit_orders_routes_aged_partial_fill_to_remainder_owner(monkeypatch):
    """A durable partial row cannot strand its broker-working remainder."""
    import ap.order_monitor as om_mod

    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    monkeypatch.setattr(om_mod, "TIMEOUT_PARTIAL_FILL", 0)
    old_ts = datetime.now(timezone.utc) - timedelta(seconds=30)
    order = {
        "local_order_id": "loc-partial-live",
        "broker_order_id": "bro-partial-live",
        "position_id": "pos-partial-live",
        "contract": "AVGO260814C00350000",
        "status": "EXIT_PARTIAL_FILL",
        "created_ts": old_ts,
        "submitted_ts": old_ts,
    }
    mon = _monitor(osm=MagicMock())
    mon._get_active_exit_orders = MagicMock(return_value=[order])
    mon._query_broker_order = MagicMock(return_value="working")
    mon._handle_stale_exit = MagicMock()

    mon._check_exit_orders()

    mon._handle_stale_exit.assert_called_once()
    assert mon._handle_stale_exit.call_args.args[0:2] == (
        "loc-partial-live", "EXIT_PARTIAL_FILL",
    )


def test_partial_to_filled_reread_applies_canonical_full_fill_truth(monkeypatch):
    """A newer raw FILLED payload must not be converted into a zero remainder."""
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    osm = MagicMock()
    osm.get_order.return_value = {
        "kind": "EXIT",
        "position_id": "pos-race",
        "broker_order_id": "bro-race",
        "status": "EXIT_PARTIAL_FILL",
        "qty": 2,
        "filled_qty": 1,
    }
    osm.transition.return_value = True
    mon = _monitor(osm=osm)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._query_broker_order = MagicMock(return_value="partially_filled")
    mon._advance_from_broker_status = MagicMock()
    mon._query_broker_order_payload = MagicMock(
        return_value={
            "status": "filled",
            "exec_quantity": 2,
            "avg_fill_price": 2.55,
        }
    )

    mon._handle_stale_exit(
        local_order_id="loc-race",
        status="EXIT_PARTIAL_FILL",
        contract="AVGO260814C00350000",
        age_secs=120.0,
        position_id="pos-race",
        reason="partial to filled race",
    )

    osm.transition.assert_called_once_with(
        "loc-race",
        "EXIT_FILLED",
        filled_qty=2,
        fill_price=2.55,
        broker_order_id="bro-race",
    )
    mon._advance_from_broker_status.assert_not_called()
    assert mon.broker.cancel_order.called is False


def test_watchdog_flag_explicitly_disabled_still_suppresses(monkeypatch):
    _watchdog_mode(monkeypatch, stale_exit_recovery=False)
    broker = MagicMock()
    osm = MagicMock()
    osm.get_order.return_value = {"kind": "EXIT", "position_id": "pos-2", "broker_order_id": "bro-2"}

    mon = _monitor(broker=broker, osm=osm)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()

    mon._handle_stale_exit(
        local_order_id="loc-2", status="WORKING", contract="AAPL260814C00200000",
        age_secs=120.0, position_id="pos-2", reason="test",
    )

    broker.get_order.assert_not_called()
    broker.cancel_order.assert_not_called()


def test_actor_mode_terminal_pre_cancel_proof_does_not_issue_delete(monkeypatch):
    import ap.order_monitor as om_mod
    monkeypatch.setattr(om_mod, "ORDER_MONITOR_MODE", "actor")
    monkeypatch.setattr(om_mod, "ORDER_MONITOR_CAN_ACT", True)
    monkeypatch.setattr(om_mod, "ALLOW_STALE_EXIT_RECOVERY_IN_WATCHDOG", False)

    broker = MagicMock()
    broker.get_order.return_value = {"status": "canceled"}
    osm = MagicMock()
    osm.get_order.return_value = {"kind": "EXIT", "position_id": "pos-3", "broker_order_id": "bro-3"}
    exit_engine = MagicMock()

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._get_newer_active_exit_order = MagicMock(return_value=None)

    mon._handle_stale_exit(
        local_order_id="loc-3", status="WORKING", contract="AAPL260814C00200000",
        age_secs=120.0, position_id="pos-3", reason="test",
    )
    broker.cancel_order.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    assert mon._emit_order_event.call_args.kwargs["reason_code"] == "BROKER_STATUS_UNKNOWN"


@pytest.mark.parametrize("snapshot", ["error", "unknown"], ids=["error", "unknown"])
def test_pre_cancel_broker_truth_failure_blocks_delete_and_replacement(monkeypatch, snapshot):
    """A known broker ID is not enough to authorize DELETE without live proof."""
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    broker = MagicMock()
    if snapshot == "error":
        broker.get_order.side_effect = ConnectionError("pre-cancel read timeout")
    else:
        broker.get_order.return_value = {"status": "provider_unknown"}
    osm = MagicMock()
    osm.get_order.return_value = {
        "kind": "EXIT",
        "position_id": "pos-pre-cancel",
        "broker_order_id": "bro-pre-cancel",
    }
    exit_engine = MagicMock()

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()

    mon._handle_stale_exit(
        local_order_id="loc-pre-cancel",
        status="WORKING",
        contract="AAPL260814C00200000",
        age_secs=120.0,
        position_id="pos-pre-cancel",
        reason="pre-cancel truth unavailable",
    )

    broker.cancel_order.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    exit_engine.finalize_exit_replacement_safe.assert_not_called()
    exit_engine.clear_exit_in_flight.assert_not_called()
    osm.transition.assert_not_called()
    assert mon._emit_order_event.call_args.kwargs["reason_code"] == "BROKER_STATUS_UNKNOWN"


def test_missing_broker_id_blocks_with_named_diagnostic(monkeypatch):
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    broker = MagicMock()
    osm = MagicMock()
    osm.get_order.return_value = {"kind": "EXIT", "position_id": "pos-4"}  # no broker_order_id

    mon = _monitor(broker=broker, osm=osm)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()

    mon._handle_stale_exit(
        local_order_id="loc-4", status="WORKING", contract="AAPL260814C00200000",
        age_secs=120.0, position_id="pos-4", reason="test",
    )

    broker.get_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    kwargs = mon._emit_order_event.call_args.kwargs
    assert kwargs["reason_code"] == "STALE_EXIT_BROKER_ID_UNPROVEN"


@pytest.mark.parametrize(
    "payload",
    [
        {"status": "filled", "order_status": "working"},
        {"status": "working", "order_status": "filled"},
    ],
    ids=["status_first", "order_status_first"],
)
def test_conflicting_duplicate_broker_status_authorities_hold(payload):
    broker = MagicMock()
    broker.get_order.return_value = payload
    mon = _monitor(broker=broker)

    assert mon._query_broker_order(f"bro-conflict-{payload['status']}") is None


def test_broker_already_filled_applies_fill_no_cancel_no_replacement(monkeypatch):
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    broker = MagicMock()
    broker.get_order.return_value = {"status": "filled"}
    osm = MagicMock()
    osm.get_order.return_value = {"kind": "EXIT", "position_id": "pos-5", "broker_order_id": "bro-5"}
    exit_engine = MagicMock()

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._advance_from_broker_status = MagicMock()

    mon._handle_stale_exit(
        local_order_id="loc-5", status="WORKING", contract="AAPL260814C00200000",
        age_secs=120.0, position_id="pos-5", reason="test",
    )

    broker.cancel_order.assert_not_called()
    mon._advance_from_broker_status.assert_called_once()
    exit_engine.mark_exit_replacement_safe.assert_not_called()


def test_cancel_still_working_blocks_replacement(monkeypatch):
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    broker = MagicMock()
    broker.get_order.return_value = {"status": "working"}
    broker.cancel_order.return_value = {"status": "working"}
    osm = MagicMock()
    osm.get_order.return_value = {"kind": "EXIT", "position_id": "pos-6", "broker_order_id": "bro-6"}
    exit_engine = MagicMock()

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()

    mon._handle_stale_exit(
        local_order_id="loc-6", status="WORKING", contract="AAPL260814C00200000",
        age_secs=120.0, position_id="pos-6", reason="test",
    )

    osm.transition.assert_not_called()
    exit_engine.clear_exit_in_flight.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    kwargs = mon._emit_order_event.call_args.kwargs
    assert kwargs["reason_code"] == "EXIT_CANCEL_NOT_CONFIRMED_REPLACEMENT_BLOCKED"


def test_cancel_post_cancel_get_failure_blocks_replacement(monkeypatch):
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    broker = MagicMock()
    call_log = {"n": 0}

    def _get_order(oid):
        call_log["n"] += 1
        if call_log["n"] == 1:
            return {"status": "working"}
        raise ConnectionError("read timeout")

    broker.get_order.side_effect = _get_order
    broker.cancel_order.return_value = {"status": "canceled"}  # response says canceled...

    osm = MagicMock()
    osm.get_order.return_value = {"kind": "EXIT", "position_id": "pos-7", "broker_order_id": "bro-7"}
    exit_engine = MagicMock()

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()

    mon._handle_stale_exit(
        local_order_id="loc-7", status="WORKING", contract="AAPL260814C00200000",
        age_secs=120.0, position_id="pos-7", reason="test",
    )

    # ...but the fresh post-cancel GET failed, so replacement must still be blocked.
    osm.transition.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()


def test_cancel_to_canceled_with_cumulative_partial_fill_caps_replacement_to_remainder(monkeypatch):
    """A fill earned during DELETE must be durable before CANCELED handoff."""
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)

    class _PartialFillOSM:
        def __init__(self):
            self.row = {
                "local_order_id": "loc-canceled-partial",
                "kind": "EXIT",
                "position_id": "pos-canceled-partial",
                "broker_order_id": "bro-canceled-partial",
                "status": "EXIT_ACKNOWLEDGED",
                "qty": 2,
                "filled_qty": 0,
                "meta": {},
            }
            self.transitions = []

        def get_order(self, local_order_id):
            if local_order_id != self.row["local_order_id"]:
                return None
            row = dict(self.row)
            row["meta"] = dict(self.row["meta"])
            return row

        def persist_stale_exit_cancel_attempt(self, local_order_id, broker_order_id, attempt):
            if (
                local_order_id != self.row["local_order_id"]
                or broker_order_id != self.row["broker_order_id"]
            ):
                return False
            self.row["meta"]["stale_exit_cancel_liveness"] = {
                "broker_order_id": broker_order_id,
                "attempt": int(attempt),
                "updated_at": "test",
            }
            return True

        def transition(self, local_order_id, status, **kwargs):
            if local_order_id != self.row["local_order_id"]:
                return False
            self.transitions.append((local_order_id, status, kwargs))
            self.row["status"] = status
            if status in {"EXIT_PARTIAL_FILL", "EXIT_FILLED"}:
                self.row["filled_qty"] = int(kwargs.get("filled_qty") or 0)
            return True

    broker = MagicMock()
    broker.get_order.side_effect = [
        {"status": "working"},
        {"status": "canceled", "exec_quantity": 1, "avg_fill_price": 1.25},
    ]
    broker.cancel_order.return_value = {"status": "canceled"}
    osm = _PartialFillOSM()
    exit_engine = MagicMock()
    exit_engine.mark_exit_replacement_safe.return_value = True
    exit_engine.finalize_exit_replacement_safe.return_value = True

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._get_newer_active_exit_order = MagicMock(return_value=None)

    mon._handle_stale_exit(
        local_order_id="loc-canceled-partial",
        status="EXIT_ACKNOWLEDGED",
        contract="AAPL260814C00200000",
        age_secs=120.0,
        position_id="pos-canceled-partial",
        reason="late partial fill during cancel",
    )

    assert broker.cancel_order.call_count == 1
    assert osm.row["filled_qty"] == 1
    assert osm.transitions[0][1] == "EXIT_PARTIAL_FILL"
    assert osm.transitions[0][2]["broker_order_id"] == "bro-canceled-partial"
    assert osm.transitions[-1][1] == "CANCELED"
    assert osm.transitions[-1][2]["broker_order_id"] == "bro-canceled-partial"
    exit_engine.mark_exit_replacement_safe.assert_called_once()
    _, mark_kwargs = exit_engine.mark_exit_replacement_safe.call_args
    assert mark_kwargs["replacement_qty"] == 1
    assert mark_kwargs["replacement_qty"] != 2


def test_canceled_after_already_durable_partial_fill_preserves_remainder(monkeypatch):
    """A terminal cancel repeating durable partial truth still hands off its remainder."""
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)

    events = []

    class _AlreadyPartialFillOSM:
        def __init__(self):
            self.row = {
                "local_order_id": "loc-durable-partial",
                "kind": "EXIT",
                "position_id": "pos-durable-partial",
                "broker_order_id": "bro-durable-partial",
                "status": "EXIT_PARTIAL_FILL",
                "qty": 2,
                "filled_qty": 1,
                "meta": {},
            }
            self.transitions = []

        def get_order(self, local_order_id):
            if local_order_id != self.row["local_order_id"]:
                return None
            row = dict(self.row)
            row["meta"] = dict(self.row["meta"])
            return row

        def persist_stale_exit_cancel_attempt(self, local_order_id, broker_order_id, attempt):
            if (
                local_order_id != self.row["local_order_id"]
                or broker_order_id != self.row["broker_order_id"]
            ):
                return False
            self.row["meta"]["stale_exit_cancel_liveness"] = {
                "broker_order_id": broker_order_id,
                "attempt": int(attempt),
                "updated_at": "test",
            }
            return True

        def transition(self, local_order_id, status, **kwargs):
            if local_order_id != self.row["local_order_id"]:
                return False
            self.transitions.append((local_order_id, status, kwargs))
            self.row["status"] = status
            if status == "CANCELED":
                events.append("osm_terminal_success")
            return True

    broker = MagicMock()
    pre_cancel = {
        "status": "partially_filled",
        "exec_quantity": 1,
        "avg_fill_price": 1.25,
    }
    broker.get_order.side_effect = [
        pre_cancel,
        pre_cancel,
        {"status": "canceled", "exec_quantity": 1, "avg_fill_price": 1.25},
    ]
    broker.cancel_order.return_value = {"status": "canceled"}
    osm = _AlreadyPartialFillOSM()
    exit_engine = MagicMock()
    exit_engine.mark_exit_replacement_safe.side_effect = (
        lambda *args, **kwargs: events.append("mark_replacement_safe") or True
    )
    exit_engine.finalize_exit_replacement_safe.side_effect = (
        lambda *args, **kwargs: events.append("finalize_replacement_safe") or True
    )
    exit_engine.clear_exit_in_flight.side_effect = (
        lambda *args, **kwargs: events.append("clear_exit_in_flight")
    )

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._advance_from_broker_status = MagicMock()
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._get_newer_active_exit_order = MagicMock(return_value=None)

    mon._handle_stale_exit(
        local_order_id="loc-durable-partial",
        status="EXIT_PARTIAL_FILL",
        contract="AAPL260814C00200000",
        age_secs=120.0,
        position_id="pos-durable-partial",
        reason="durable partial truth repeated at cancel",
    )

    assert broker.get_order.call_count == 3
    assert broker.cancel_order.call_count == 1
    assert osm.row["filled_qty"] == 1
    assert [status for _, status, _ in osm.transitions] == ["CANCELED"]
    assert osm.transitions[-1][2]["broker_order_id"] == "bro-durable-partial"
    mon._advance_from_broker_status.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_called_once()
    _, mark_kwargs = exit_engine.mark_exit_replacement_safe.call_args
    assert mark_kwargs["replacement_qty"] == 1
    assert mark_kwargs["replacement_qty"] != 0
    assert mark_kwargs["replacement_qty"] != 2
    exit_engine.finalize_exit_replacement_safe.assert_called_once()
    assert events == [
        "mark_replacement_safe",
        "osm_terminal_success",
        "finalize_replacement_safe",
        "clear_exit_in_flight",
    ]
    exit_engine.clear_exit_in_flight.assert_called_once_with(
        "pos-durable-partial",
        reason="durable partial truth repeated at cancel",
        local_order_id="loc-durable-partial",
        broker_order_id="bro-durable-partial",
    )


@pytest.mark.parametrize(
    ("previous_filled", "cumulative", "expected_remainder", "expected_transition"),
    [
        (1, 0, None, None),
        (1, -1, None, None),
        (1, "bad", None, None),
        (1, 3, None, None),
        (1, 1, 1, None),
        (0, 1, 1, "EXIT_PARTIAL_FILL"),
        (1, 2, 0, "EXIT_FILLED"),
    ],
)
def test_terminal_cumulative_fill_matrix_is_strict_and_cumulative(
    previous_filled, cumulative, expected_remainder, expected_transition,
):
    """CANCELED/EXPIRED/REJECTED share one strict cumulative-fill path."""

    class _TerminalOSM:
        def __init__(self):
            self.row = {
                "local_order_id": "loc-terminal-matrix",
                "kind": "EXIT",
                "position_id": "pos-terminal-matrix",
                "broker_order_id": "bro-terminal-matrix",
                "status": "EXIT_PARTIAL_FILL" if previous_filled else "EXIT_ACKNOWLEDGED",
                "qty": 2,
                "filled_qty": previous_filled,
            }
            self.transitions = []

        def get_order(self, local_order_id):
            return dict(self.row) if local_order_id == self.row["local_order_id"] else None

        def transition(self, local_order_id, status, **kwargs):
            self.transitions.append((status, dict(kwargs)))
            self.row["status"] = status
            if "filled_qty" in kwargs:
                self.row["filled_qty"] = kwargs["filled_qty"]
            return True

    for terminal_status in ("canceled", "expired", "rejected"):
        osm = _TerminalOSM()
        mon = _monitor(osm=osm)
        mon._emit_order_event = MagicMock()
        remainder = mon._apply_broker_partial_exit_fill(
            "loc-terminal-matrix",
            "bro-terminal-matrix",
            "AAPL260814C00200000",
            raw_payload={
                "status": terminal_status,
                "exec_quantity": cumulative,
                "avg_fill_price": 1.25,
            },
        )
        assert remainder == expected_remainder
        if expected_transition is None:
            assert osm.transitions == []
        else:
            assert osm.transitions[-1][0] == expected_transition


def test_terminal_explicit_zero_against_durable_partial_holds_and_grants_zero(monkeypatch):
    """A terminal exec_quantity=0 contradicting durable fill=1 must hold."""
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)

    class _OSM:
        def __init__(self):
            self.row = {
                "local_order_id": "loc-zero-contradiction",
                "kind": "EXIT",
                "position_id": "pos-zero-contradiction",
                "broker_order_id": "bro-zero-contradiction",
                "status": "EXIT_PARTIAL_FILL",
                "qty": 2,
                "filled_qty": 1,
                "meta": {},
            }
            self.transitions = []

        def get_order(self, local_order_id):
            return dict(self.row) if local_order_id == self.row["local_order_id"] else None

        def persist_stale_exit_cancel_attempt(self, local_order_id, broker_order_id, attempt):
            return True

        def transition(self, local_order_id, status, **kwargs):
            self.transitions.append((status, dict(kwargs)))
            return True

    broker = MagicMock()
    broker.get_order.side_effect = [
        {"status": "working"},
        {"status": "canceled", "exec_quantity": 0},
    ]
    broker.cancel_order.return_value = {"status": "canceled"}
    osm = _OSM()
    exit_engine = MagicMock()
    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._get_newer_active_exit_order = MagicMock(return_value=None)

    mon._handle_stale_exit(
        local_order_id="loc-zero-contradiction",
        status="EXIT_PARTIAL_FILL",
        contract="AAPL260814C00200000",
        age_secs=120.0,
        position_id="pos-zero-contradiction",
        reason="terminal zero contradicts durable partial",
    )

    assert osm.transitions == []
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    exit_engine.finalize_exit_replacement_safe.assert_not_called()


@pytest.mark.parametrize("cumulative", [True, 1.5, -1, "bad", 3])
def test_filled_race_rejects_malformed_or_out_of_bounds_cumulative_quantity(cumulative):
    """The FILLED race branch must share strict cumulative-fill semantics."""

    class _OSM:
        def __init__(self):
            self.row = {
                "local_order_id": "loc-filled-strict",
                "kind": "EXIT",
                "position_id": "pos-filled-strict",
                "broker_order_id": "bro-filled-strict",
                "status": "EXIT_ACKNOWLEDGED",
                "qty": 2,
                "filled_qty": 0,
            }
            self.transitions = []

        def get_order(self, local_order_id):
            return dict(self.row) if local_order_id == self.row["local_order_id"] else None

        def transition(self, local_order_id, status, **kwargs):
            self.transitions.append((status, dict(kwargs)))
            return True

    osm = _OSM()
    mon = _monitor(osm=osm)
    mon._emit_order_event = MagicMock()

    result = mon._apply_broker_partial_exit_fill(
        "loc-filled-strict",
        "bro-filled-strict",
        "AAPL260814C00200000",
        raw_payload={
            "status": "filled",
            "exec_quantity": cumulative,
            "avg_fill_price": 1.25,
        },
    )

    assert result is None
    assert osm.transitions == []
    assert mon._emit_order_event.call_args.kwargs["reason_code"] == "BROKER_FILLED_QTY_INVALID"


def test_rejected_is_terminal_stale_exit_cancel_proof():
    mon = _monitor()

    assert mon._is_terminal_cancel_status("rejected") is True
    assert mon._is_terminal_cancel_status({"status": "rejected"}) is True

    assert mon._is_terminal_cancel_status("working") is False
    assert mon._is_terminal_cancel_status("partially_filled") is False
    assert mon._is_terminal_cancel_status("filled") is False


def test_cancel_races_with_full_fill_fill_wins(monkeypatch):
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    broker = MagicMock()
    call_log = {"n": 0}

    def _get_order(oid):
        call_log["n"] += 1
        if call_log["n"] == 1:
            return {"status": "working"}
        return {"status": "filled"}  # post-cancel GET reveals a late fill

    broker.get_order.side_effect = _get_order
    broker.cancel_order.return_value = {"status": "rejected"}

    osm = MagicMock()
    osm.get_order.return_value = {"kind": "EXIT", "position_id": "pos-8", "broker_order_id": "bro-8"}
    exit_engine = MagicMock()

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._advance_from_broker_status = MagicMock()

    mon._handle_stale_exit(
        local_order_id="loc-8", status="WORKING", contract="AAPL260814C00200000",
        age_secs=120.0, position_id="pos-8", reason="test",
    )

    mon._advance_from_broker_status.assert_called_once()
    osm.transition.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()


def test_duplicate_monitor_invocation_no_double_cancel(monkeypatch):
    """Two overlapping poll cycles must not issue two broker cancels for the
    same broker_order_id."""
    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    broker = MagicMock()
    call_log = {"n": 0}

    def _get_order(oid):
        call_log["n"] += 1
        # First call (pre-cancel, invocation 1): working.
        # Second call (post-cancel proof, invocation 1): still working — cancel not yet confirmed.
        # Third call (invocation 2's re-check via inflight branch): now canceled.
        if call_log["n"] <= 2:
            return {"status": "working"}
        return {"status": "canceled"}

    broker.get_order.side_effect = _get_order
    broker.cancel_order.return_value = {"status": "pending"}

    osm = MagicMock()
    osm.get_order.return_value = {"kind": "EXIT", "position_id": "pos-9", "broker_order_id": "bro-9"}
    exit_engine = MagicMock()
    pm = MagicMock()

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine, pm=pm)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._get_newer_active_exit_order = MagicMock(return_value=None)

    for _ in range(2):
        mon._handle_stale_exit(
            local_order_id="loc-9", status="WORKING", contract="AAPL260814C00200000",
            age_secs=120.0, position_id="pos-9", reason="test",
        )

    assert broker.cancel_order.call_count == 1, "second invocation must not re-issue cancel"
    osm.transition.assert_called_once()
    exit_engine.mark_exit_replacement_safe.assert_called_once()


def test_lost_cancel_gets_one_bounded_exact_retry_then_one_replacement(monkeypatch):
    """A lost DELETE must not strand the exact stale exit forever."""
    import ap.order_monitor as om_mod

    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    monkeypatch.setattr(om_mod, "STALE_EXIT_CANCEL_RETRY_AFTER_SECONDS", 0)
    monkeypatch.setattr(om_mod, "STALE_EXIT_CANCEL_MAX_ATTEMPTS", 2)

    broker = MagicMock()
    broker.get_order.side_effect = [
        {"status": "working"},  # first pre-cancel proof
        {"status": "working"},  # first post-cancel proof: DELETE was lost
        {"status": "working"},  # fresh proof before bounded re-cancel
        {"status": "canceled"},  # second post-cancel terminal proof
    ]
    broker.cancel_order.return_value = {"ok": False, "status": "unknown"}
    osm = MagicMock()
    osm.get_order.return_value = {
        "kind": "EXIT",
        "position_id": "pos-retry",
        "broker_order_id": "bro-retry",
    }
    exit_engine = MagicMock()
    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._get_newer_active_exit_order = MagicMock(return_value=None)

    for _ in range(2):
        mon._handle_stale_exit(
            local_order_id="loc-retry",
            status="WORKING",
            contract="AVGO260814C00350000",
            age_secs=120.0,
            position_id="pos-retry",
            reason="lost cancel retry",
        )

    assert [call.args for call in broker.cancel_order.call_args_list] == [
        ("bro-retry",),
        ("bro-retry",),
    ], "the retry must target the exact original broker order"
    assert osm.transition.call_count == 1
    assert exit_engine.mark_exit_replacement_safe.call_count == 1
    assert exit_engine.finalize_exit_replacement_safe.call_count == 1
    assert exit_engine.clear_exit_in_flight.call_count == 1
    assert mon._stale_exit_cancel_inflight == {}
    assert mon._stale_exit_cancel_attempts == {}


def test_bounded_cancel_attempt_survives_monitor_restart(monkeypatch):
    """A new monitor process must resume the durable bound, not reset to 1."""
    import ap.order_monitor as om_mod

    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    monkeypatch.setattr(om_mod, "STALE_EXIT_CANCEL_RETRY_AFTER_SECONDS", 0)
    monkeypatch.setattr(om_mod, "STALE_EXIT_CANCEL_MAX_ATTEMPTS", 2)

    broker = MagicMock()
    broker.get_order.side_effect = [
        {"status": "working"},  # monitor 1: initial proof
        {"status": "working"},  # monitor 1: post-cancel proof lost
        {"status": "working"},  # monitor 2: fresh retry proof after restart
        {"status": "canceled"},  # monitor 2: independent post-cancel proof
        {"status": "working"},  # monitor 3: fresh proof, no third DELETE allowed
    ]
    broker.cancel_order.return_value = {"status": "pending"}
    osm = _DurableCancelOSM()
    exit_engine = MagicMock()

    def _run_new_monitor():
        mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
        mon._emit_order_event = MagicMock()
        mon._alert = MagicMock()
        with om_mod._BROKER_STATUS_CACHE_LOCK:
            om_mod._BROKER_STATUS_CACHE.pop("bro-restart", None)
        mon._handle_stale_exit(
            local_order_id="loc-restart",
            status="WORKING",
            contract="AVGO260814C00350000",
            age_secs=120.0,
            position_id="pos-restart",
            reason="restart-boundary retry",
        )
        return mon

    first = _run_new_monitor()
    assert broker.cancel_order.call_count == 1
    assert first._stale_exit_cancel_attempts["bro-restart"] == 1
    assert osm.row["meta"]["stale_exit_cancel_liveness"]["attempt"] == 1

    second = _run_new_monitor()
    assert broker.cancel_order.call_count == 2
    assert osm.row["meta"]["stale_exit_cancel_liveness"]["attempt"] == 2
    assert len(osm.transitions) == 1
    assert second._stale_exit_cancel_inflight == {}

    _run_new_monitor()
    assert broker.cancel_order.call_count == 2
    assert osm.row["meta"]["stale_exit_cancel_liveness"]["attempt"] == 2


def test_late_fill_wins_over_bounded_recancel(monkeypatch):
    """A fill discovered during the retry window beats cancellation/replacement."""
    import ap.order_monitor as om_mod

    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    monkeypatch.setattr(om_mod, "STALE_EXIT_CANCEL_RETRY_AFTER_SECONDS", 0)
    monkeypatch.setattr(om_mod, "STALE_EXIT_CANCEL_MAX_ATTEMPTS", 2)

    broker = MagicMock()
    broker.get_order.side_effect = [
        {"status": "working"},
        {"status": "working"},
        {"status": "working"},
        {"status": "filled"},
    ]
    broker.cancel_order.return_value = {"ok": False, "status": "unknown"}
    osm = MagicMock()
    osm.get_order.return_value = {
        "kind": "EXIT",
        "position_id": "pos-late-fill",
        "broker_order_id": "bro-late-fill",
    }
    exit_engine = MagicMock()
    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._advance_from_broker_status = MagicMock()

    for _ in range(2):
        mon._handle_stale_exit(
            local_order_id="loc-late-fill",
            status="WORKING",
            contract="AVGO260814C00350000",
            age_secs=120.0,
            position_id="pos-late-fill",
            reason="late fill during cancel retry",
        )

    assert broker.cancel_order.call_count == 2
    mon._advance_from_broker_status.assert_called_once()
    osm.transition.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    exit_engine.clear_exit_in_flight.assert_not_called()
    assert mon._stale_exit_cancel_inflight == {}
    assert mon._stale_exit_cancel_attempts == {}


def test_failed_osm_cancel_transition_retracts_staged_grant_and_keeps_owner(monkeypatch):
    """Broker cancel proof alone must not clear or authorize replacement."""
    from ap_exit_engine import APExitEngine, ManagedPosition

    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    broker = MagicMock()
    broker.get_order.side_effect = [
        {"status": "working"},
        {"status": "canceled"},
    ]
    broker.cancel_order.return_value = {"status": "pending"}
    osm = MagicMock()
    osm.get_order.side_effect = [
        {
            "kind": "EXIT",
            "position_id": "pos-osm-fail",
            "broker_order_id": "bro-osm-fail",
            "status": "EXIT_ACKNOWLEDGED",
        },
        {
            "kind": "EXIT",
            "position_id": "pos-osm-fail",
            "broker_order_id": "bro-osm-fail",
            "status": "EXIT_ACKNOWLEDGED",
        },
        {
            "kind": "EXIT",
            "position_id": "pos-osm-fail",
            "broker_order_id": "bro-osm-fail",
            "status": "EXIT_ACKNOWLEDGED",
        },
    ]
    osm.transition.return_value = False
    engine = APExitEngine(broker=broker)
    engine._emit_exit_event = MagicMock()
    engine._persist_exit_replace_attempt_to_db = MagicMock(return_value=True)
    position = ManagedPosition(
        ticker="AVGO", option_symbol="AVGO260814C00350000", side="CALL", quantity=7,
        entry_price=2.49, underlying_entry=350.0, underlying_target=360.0,
        underlying_stop=340.0, position_id="pos-osm-fail", client_id="client-1",
        execution_mode="paper", exit_in_flight=True,
        pending_exit_local_order_id="loc-osm-fail",
        pending_exit_broker_order_id="bro-osm-fail", pending_exit_qty=2,
    )
    engine._positions.append(position)
    engine._positions_by_id[position.position_id] = position

    mon = _monitor(broker=broker, osm=osm, exit_engine=engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()

    mon._handle_stale_exit(
        local_order_id="loc-osm-fail", status="WORKING",
        contract="AVGO260814C00350000", age_secs=120.0,
        position_id="pos-osm-fail", reason="OSM CAS lost",
    )

    assert position.exit_in_flight is True
    assert position.pending_exit_replace_allowed is False
    assert position.pending_exit_replace_durable_pending is False
    assert position.exit_replace_attempt == 0
    assert "bro-osm-fail" in mon._stale_exit_cancel_inflight
    # STAGED is durably written before the OSM CAS, then safely retracted when
    # that CAS is proven unsuccessful; neither write grants a replacement.
    assert engine._persist_exit_replace_attempt_to_db.call_count == 2
    assert position.exit_retry_liveness["state"] == "NONE"


def test_osm_transition_miss_accepts_exact_terminal_reread(monkeypatch):
    """A concurrent exact CANCELED CAS winner is durable-success evidence."""
    from ap_exit_engine import APExitEngine, ManagedPosition

    _watchdog_mode(monkeypatch, stale_exit_recovery=True)
    broker = MagicMock()
    broker.get_order.side_effect = [
        {"status": "working"},
        {"status": "canceled"},
    ]
    broker.cancel_order.return_value = {"status": "pending"}
    osm = MagicMock()
    osm.get_order.side_effect = [
        {
            "local_order_id": "loc-osm-reread",
            "kind": "EXIT",
            "position_id": "pos-osm-reread",
            "broker_order_id": "bro-osm-reread",
            "status": "EXIT_ACKNOWLEDGED",
        },
        {
            "local_order_id": "loc-osm-reread",
            "kind": "EXIT",
            "position_id": "pos-osm-reread",
            "broker_order_id": "bro-osm-reread",
            "status": "EXIT_ACKNOWLEDGED",
        },
        {
            "local_order_id": "loc-osm-reread",
            "kind": "EXIT",
            "position_id": "pos-osm-reread",
            "broker_order_id": "bro-osm-reread",
            "status": "CANCELED",
        },
    ]
    osm.transition.return_value = False
    engine = APExitEngine(broker=broker)
    engine._emit_exit_event = MagicMock()
    engine._persist_exit_replace_attempt_to_db = MagicMock(return_value=True)
    position = ManagedPosition(
        ticker="AVGO", option_symbol="AVGO260814C00350000", side="CALL", quantity=7,
        entry_price=2.49, underlying_entry=350.0, underlying_target=360.0,
        underlying_stop=340.0, position_id="pos-osm-reread", client_id="client-1",
        execution_mode="paper", exit_in_flight=True,
        pending_exit_local_order_id="loc-osm-reread",
        pending_exit_broker_order_id="bro-osm-reread", pending_exit_qty=2,
    )
    engine._positions.append(position)
    engine._positions_by_id[position.position_id] = position
    mon = _monitor(broker=broker, osm=osm, exit_engine=engine)
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()

    mon._handle_stale_exit(
        local_order_id="loc-osm-reread", status="WORKING",
        contract="AVGO260814C00350000", age_secs=120.0,
        position_id="pos-osm-reread", reason="concurrent OSM winner",
    )

    assert osm.transition.call_count == 1
    assert osm.get_order.call_count == 3
    assert position.exit_replace_attempt == 1
    assert position.exit_in_flight is False
    assert position.pending_exit_replace_allowed is True
    assert position.pending_exit_replace_durable_pending is False
    assert mon._stale_exit_cancel_inflight == {}


def test_paper_and_live_identity_preserved_through_handoff(monkeypatch):
    _actor_mode(monkeypatch)
    broker = MagicMock()
    broker.get_order.side_effect = [
        {"status": "working"},
        {"status": "canceled", "exec_quantity": 0},
    ]
    broker.cancel_order.return_value = {"status": "pending"}
    osm = MagicMock()
    osm.get_order.return_value = {
        "local_order_id": "loc-live-1",
        "kind": "EXIT",
        "position_id": "pos-live-1",
        "broker_order_id": "bro-live-1",
        "status": "EXIT_ACKNOWLEDGED",
        "qty": 2,
        "filled_qty": 0,
    }
    exit_engine = MagicMock()

    mon = _monitor(broker=broker, osm=osm, exit_engine=exit_engine)
    mon.client_id = "client-A"
    mon._emit_order_event = MagicMock()
    mon._alert = MagicMock()
    mon._get_newer_active_exit_order = MagicMock(return_value=None)

    mon._handle_stale_exit(
        local_order_id="loc-live-1", status="WORKING", contract="SPY260814C00500000",
        age_secs=120.0, position_id="pos-live-1", reason="test",
    )

    _, kwargs = exit_engine.mark_exit_replacement_safe.call_args
    assert kwargs["local_order_id"] == "loc-live-1"
    assert kwargs["broker_order_id"] == "bro-live-1"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
