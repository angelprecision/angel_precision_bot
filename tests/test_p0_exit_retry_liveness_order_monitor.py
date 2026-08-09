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
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap.order_monitor import APOrderMonitor  # noqa: E402


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
    osm.transition.assert_called_once_with("loc-avgo", "CANCELED", last_error="stale exit AVGO scale-out qty=2")
    exit_engine.clear_exit_in_flight.assert_called_once_with("pos-avgo")
    exit_engine.mark_exit_replacement_safe.assert_called_once()
    _, kwargs = exit_engine.mark_exit_replacement_safe.call_args
    assert kwargs["local_order_id"] == "loc-avgo"
    assert kwargs["broker_order_id"] == "bro-avgo"


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


def test_global_actor_mode_still_works_without_the_new_flag(monkeypatch):
    import ap.order_monitor as om_mod
    monkeypatch.setattr(om_mod, "ORDER_MONITOR_MODE", "actor")
    monkeypatch.setattr(om_mod, "ORDER_MONITOR_CAN_ACT", True)
    monkeypatch.setattr(om_mod, "ALLOW_STALE_EXIT_RECOVERY_IN_WATCHDOG", False)

    broker = MagicMock()
    broker.get_order.return_value = {"status": "canceled"}
    broker.cancel_order.return_value = {"status": "pending"}
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
    broker.cancel_order.assert_called_once()


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


def test_paper_and_live_identity_preserved_through_handoff(monkeypatch):
    _actor_mode(monkeypatch)
    broker = MagicMock()
    broker.get_order.return_value = {"status": "canceled"}
    broker.cancel_order.return_value = {"status": "pending"}
    osm = MagicMock()
    osm.get_order.return_value = {"kind": "EXIT", "position_id": "pos-live-1", "broker_order_id": "bro-live-1"}
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
