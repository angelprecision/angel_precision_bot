# tests/test_p0_exit_retry_liveness_autonomous_recovery.py
# =============================================================================
# P0 regression: PR #423 Patch 3 — single-cancellation-owner guarantee.
# When APOrderMonitor is alive, ap.exit_autonomous_recovery must never
# independently cancel a broker exit order it might already be working;
# it must defer and report recovery_owner=order_monitor_stale_exit. When
# the monitor is unavailable/dead/unregistered, autonomous recovery keeps
# its existing exact-identity-fenced independent-cancel fallback.
# =============================================================================

from __future__ import annotations

import os
import sys
import threading
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path("/home/claude/angel_precision_bot")
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

from ap.exit_autonomous_recovery import (  # noqa: E402
    recover_exit_position,
    _order_monitor_alive,
)
from ap.self_healing import APSelfHealingSystem, ComponentHealth  # noqa: E402
from ap_exit_engine import APExitEngine, ManagedPosition  # noqa: E402


def _alive_monitor():
    mon = MagicMock()
    t = threading.Thread(target=lambda: None)
    t.start()
    t.join()
    # A real Thread object whose is_alive() we control via a stand-in.
    fake_thread = MagicMock()
    fake_thread.is_alive.return_value = True
    mon._thread = fake_thread
    return mon


def _dead_monitor():
    mon = MagicMock()
    fake_thread = MagicMock()
    fake_thread.is_alive.return_value = False
    mon._thread = fake_thread
    return mon


def test_order_monitor_alive_helper_true_case():
    assert _order_monitor_alive(_alive_monitor()) is True


def test_order_monitor_alive_helper_false_cases():
    assert _order_monitor_alive(_dead_monitor()) is False
    assert _order_monitor_alive(None) is False
    assert _order_monitor_alive(object()) is False  # no _thread attr at all


def _pos(**overrides):
    defaults = dict(
        position_id="pos-1",
        option_symbol="AVGO260814C00350000",
        pending_exit_local_order_id="loc-1",
        pending_exit_broker_order_id="",  # forces the ambiguous-scan path
        pending_exit_qty=2,
        last_exit_signal_ts=None,
        last_callback_identity_missing_ts=None,
    )
    defaults.update(overrides)
    return MagicMock(**defaults)


class _DurableOSM:
    """Small exact-identity OSM double for autonomous handoff tests."""

    def __init__(
        self,
        *,
        local_id="loc-1",
        broker_id="bro-known",
        position_id="pos-1",
        exit_engine=None,
    ):
        self.row = {
            "local_order_id": local_id,
            "position_id": position_id,
            "broker_order_id": broker_id,
            "status": "EXIT_ACKNOWLEDGED",
            "meta": {},
        }
        self.transitions = []
        self.exit_engine = exit_engine

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
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        return True

    def transition(self, local_order_id, status, **kwargs):
        self.transitions.append((local_order_id, status, kwargs))
        if local_order_id != self.row["local_order_id"]:
            return False
        self.row["status"] = status
        self.row["broker_order_id"] = kwargs.get("broker_order_id") or self.row["broker_order_id"]
        self.row["position_id"] = kwargs.get("position_id") or self.row["position_id"]
        if self.exit_engine is not None and status in {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}:
            self.exit_engine.clear_exit_in_flight(
                self.row["position_id"],
                reason=kwargs.get("last_error", ""),
                local_order_id=local_order_id,
                broker_order_id=self.row["broker_order_id"],
            )
        return True


def test_stale_exit_cancel_attempt_persistence_is_monotonic_and_fail_closed():
    from ap.exit_autonomous_recovery import _persist_stale_exit_cancel_attempt

    osm = _DurableOSM()
    assert _persist_stale_exit_cancel_attempt(osm, "loc-1", "bro-known", 1) is True
    assert _persist_stale_exit_cancel_attempt(osm, "loc-1", "bro-known", 1) is False
    assert _persist_stale_exit_cancel_attempt(osm, "loc-1", "bro-known", 2) is True
    assert _persist_stale_exit_cancel_attempt(osm, "loc-1", "bro-known", 1) is False
    assert osm.row["meta"]["stale_exit_cancel_liveness"]["attempt"] == 2

    osm.row["meta"]["stale_exit_cancel_liveness"]["attempt"] = "corrupt"
    assert _persist_stale_exit_cancel_attempt(osm, "loc-1", "bro-known", 3) is False


def _production_callsite_position():
    position = ManagedPosition(
        ticker="AVGO",
        option_symbol="AVGO260814C00350000",
        side="CALL",
        quantity=2,
        entry_price=2.49,
        underlying_entry=350.0,
        underlying_target=360.0,
        underlying_stop=340.0,
        position_id="pos-self-healing-callsite",
        client_id="client-self-healing",
        quantity_remaining=2,
    )
    position.exit_in_flight = True
    position.pending_exit_local_order_id = "loc-self-healing"
    position.pending_exit_broker_order_id = "bro-self-healing"
    position.pending_exit_qty = 2
    position.last_exit_signal_ts = datetime.now(timezone.utc) - timedelta(seconds=120)
    return position


def _production_callsite_runner(*, broker, position, osm, order_monitor):
    engine = APExitEngine(broker=broker, email="client-self-healing")
    engine._emit_exit_event = MagicMock()
    engine._persist_exit_replace_attempt_to_db = MagicMock(return_value=True)
    engine._positions = [position]
    engine._positions_by_id[position.position_id] = position
    osm.exit_engine = engine
    return (
        engine,
        SimpleNamespace(
            email="client-self-healing",
            core=SimpleNamespace(exit_eng=engine, broker=broker),
            order_state_machine=osm,
            order_monitor=order_monitor,
        ),
    )


def test_self_healing_callsite_live_monitor_keeps_cancel_owner(monkeypatch):
    """The real self-healing caller must pass the live monitor and issue zero cancels."""
    broker = MagicMock()
    broker.get_order.return_value = {"status": "working"}
    position = _production_callsite_position()
    osm = _DurableOSM(
        local_id="loc-self-healing",
        broker_id="bro-self-healing",
        position_id=position.position_id,
    )
    engine, runner = _production_callsite_runner(
        broker=broker,
        position=position,
        osm=osm,
        order_monitor=_alive_monitor(),
    )
    healer = APSelfHealingSystem()
    healer._persist_recovery_actions = MagicMock()
    health = ComponentHealth("client-self-healing", "exit_quarantine")

    actions = healer._run_autonomous_exit_recovery("client-self-healing", runner, health)

    assert [action.action for action in actions] == ["CONFIRMED_OPEN"]
    assert actions[0].details["recovery_owner"] == "order_monitor_stale_exit"
    broker.cancel_order.assert_not_called()
    assert osm.row["meta"] == {}
    assert position.exit_in_flight is True
    assert engine.active_positions() == [position]


def test_client_runner_callsite_passes_osm_and_order_monitor(monkeypatch):
    """The runner health-loop caller must provide both recovery authorities."""
    import ap.exit_autonomous_recovery as rec_mod
    from client_runner import ClientRunner

    captured = {}

    def _capture(*args, **kwargs):
        captured["args"] = args
        captured["kwargs"] = kwargs
        return []

    monkeypatch.setattr(rec_mod, "recover_exit_engine", _capture)
    runner = ClientRunner.__new__(ClientRunner)
    runner.email = "client-callsite"
    runner._last_exit_recovery_ts = 0.0
    runner.core = SimpleNamespace(exit_eng=object(), broker=object())
    runner.broker = runner.core.broker
    runner.order_state_machine = object()
    runner.order_monitor = _alive_monitor()

    runner._run_exit_autonomous_recovery()

    assert captured["args"] == (runner.core.exit_eng,)
    assert captured["kwargs"]["broker"] is runner.core.broker
    assert captured["kwargs"]["osm"] is runner.order_state_machine
    assert captured["kwargs"]["order_monitor"] is runner.order_monitor


def test_self_healing_callsite_dead_monitor_completes_durable_exact_replacement_handoff(monkeypatch):
    """The real self-healing caller must execute the dead-monitor takeover end to end."""
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(rec_mod, "STALE_EXIT_RECOVERY_AGE_SECONDS", 45)
    broker = MagicMock()
    broker.get_order.side_effect = [
        {"status": "working"},
        {"status": "canceled"},
    ]
    broker.cancel_order.return_value = {"ok": False, "status": "unknown"}
    position = _production_callsite_position()
    osm = _DurableOSM(
        local_id="loc-self-healing",
        broker_id="bro-self-healing",
        position_id=position.position_id,
    )
    engine, runner = _production_callsite_runner(
        broker=broker,
        position=position,
        osm=osm,
        order_monitor=_dead_monitor(),
    )
    mark_spy = MagicMock(wraps=engine.mark_exit_replacement_safe)
    finalize_spy = MagicMock(wraps=engine.finalize_exit_replacement_safe)
    engine.mark_exit_replacement_safe = mark_spy
    engine.finalize_exit_replacement_safe = finalize_spy
    healer = APSelfHealingSystem()
    healer._persist_recovery_actions = MagicMock()
    health = ComponentHealth("client-self-healing", "exit_quarantine")

    actions = healer._run_autonomous_exit_recovery("client-self-healing", runner, health)

    assert [action.action for action in actions] == ["REPLACEMENT_SAFE"]
    assert broker.cancel_order.call_args.args == ("bro-self-healing",)
    assert broker.get_order.call_args_list[0].args == ("bro-self-healing",)
    assert broker.get_order.call_args_list[1].args == ("bro-self-healing",)
    assert osm.row["meta"]["stale_exit_cancel_liveness"]["attempt"] == 1
    assert osm.row["status"] == "CANCELED"
    assert position.exit_in_flight is False
    assert position.pending_exit_replace_allowed is True
    assert position.pending_exit_replace_durable_pending is False
    assert position.exit_replace_attempt == 1
    mark_spy.assert_called_once()
    finalize_spy.assert_called_once()
    assert engine._persist_exit_replace_attempt_to_db.call_count == 1


def test_ambiguous_multi_match_defers_when_order_monitor_alive(monkeypatch):
    """
    Two ambiguous open exit orders found for the contract, but the order
    monitor is alive: autonomous recovery must NOT independently cancel
    either one, and must report ownership deferred to the order monitor.
    """
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(
        rec_mod, "_matching_open_exit_orders",
        lambda broker, contract, exclude_broker_id=None: [
            ("bro-a", {"status": "working"}), ("bro-b", {"status": "working"}),
        ],
    )
    cancel_spy = MagicMock(return_value=(True, {"status": "canceled"}))
    monkeypatch.setattr(rec_mod, "_cancel_order_with_proof", cancel_spy)

    broker = MagicMock()
    pos = _pos()
    action = recover_exit_position(
        pos, broker=broker, exit_engine=MagicMock(),
        order_monitor=_alive_monitor(),
    )

    cancel_spy.assert_not_called()
    assert action.action == "CONFIRMED_OPEN"
    assert action.details.get("recovery_owner") == "order_monitor_stale_exit"


def test_ambiguous_multi_match_cancels_independently_when_order_monitor_dead(monkeypatch):
    """
    Same ambiguous-multi-match shape, but the order monitor is dead/absent:
    autonomous recovery retains its existing independent-cancel fallback
    (unchanged behavior from before #423).
    """
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(
        rec_mod, "_matching_open_exit_orders",
        lambda broker, contract, exclude_broker_id=None: [
            ("bro-a", {"status": "working"}), ("bro-b", {"status": "working"}),
        ],
    )
    cancel_spy = MagicMock(return_value=(True, {"status": "canceled"}))
    monkeypatch.setattr(rec_mod, "_cancel_order_with_proof", cancel_spy)

    exit_engine = MagicMock()
    broker = MagicMock()
    pos = _pos()

    for om in (_dead_monitor(), None):
        cancel_spy.reset_mock()
        action = recover_exit_position(
            pos, broker=broker, exit_engine=exit_engine, order_monitor=om,
        )
        assert cancel_spy.call_count == 2, f"expected independent cancel fallback for order_monitor={om}"
        assert action.action in ("REPLACEMENT_SAFE", "CLEARED_IN_FLIGHT", "NOOP")


def test_single_open_order_path_defers_to_live_monitor(monkeypatch):
    """
    A live order monitor remains the sole cancel owner for the exact open
    order, so autonomous recovery only reconfirms ownership.
    """
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(
        rec_mod, "_get_order", lambda broker, bid: {"status": "working"},
    )
    pos = _pos(pending_exit_broker_order_id="bro-known")

    action_alive = recover_exit_position(
        pos, broker=MagicMock(), exit_engine=MagicMock(), order_monitor=_alive_monitor(),
    )
    assert action_alive.action == "CONFIRMED_OPEN"
    assert action_alive.details.get("recovery_owner") == "order_monitor_stale_exit"


def test_single_open_order_dead_monitor_uses_bounded_exact_cancel_and_durable_handoff(monkeypatch):
    """A dead monitor must not strand a stale, known broker-owned exit."""
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(rec_mod, "STALE_EXIT_RECOVERY_AGE_SECONDS", 45)
    monkeypatch.setattr(rec_mod, "STALE_EXIT_CANCEL_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(rec_mod, "_get_order", lambda broker, bid: {"status": "working"})
    cancel_spy = MagicMock(return_value=(True, {"status": "canceled"}))
    monkeypatch.setattr(rec_mod, "_cancel_order_with_proof", cancel_spy)

    pos = _pos(
        pending_exit_broker_order_id="bro-known",
        last_exit_signal_ts=datetime.now(timezone.utc) - timedelta(seconds=120),
    )
    osm = _DurableOSM()
    exit_engine = MagicMock()

    action = recover_exit_position(
        pos,
        broker=MagicMock(),
        exit_engine=exit_engine,
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "REPLACEMENT_SAFE"
    assert action.details["durable_osm_transition"] == "CANCELED"
    assert cancel_spy.call_args.args[1] == "bro-known"
    assert osm.row["meta"][rec_mod.STALE_EXIT_CANCEL_LIVENESS_META_KEY]["attempt"] == 1
    assert len(osm.transitions) == 1
    assert osm.transitions[0][0:2] == ("loc-1", "CANCELED")
    exit_engine.mark_exit_replacement_safe.assert_called_once()
    _, mark_kwargs = exit_engine.mark_exit_replacement_safe.call_args
    assert mark_kwargs["defer_attempt"] is True
    exit_engine.finalize_exit_replacement_safe.assert_called_once()
    exit_engine.clear_exit_in_flight.assert_called_once_with(
        "pos-1",
        reason="autonomous_recovery_stale_known_exit_canceled",
        local_order_id="loc-1",
        broker_order_id="bro-known",
        reconciled=False,
    )


def test_terminal_and_multi_match_autonomous_paths_use_durable_osm_handoff(monkeypatch):
    """Every autonomous replacement release path must cross OSM CANCELED."""
    import ap.exit_autonomous_recovery as rec_mod

    exit_engine = MagicMock()
    broker = MagicMock()
    pos = _pos(pending_exit_broker_order_id="bro-terminal")

    monkeypatch.setattr(rec_mod, "_get_order", lambda broker, bid: {"status": "canceled"})
    monkeypatch.setattr(rec_mod, "_matching_open_exit_orders", lambda *args, **kwargs: [])
    terminal_osm = _DurableOSM(broker_id="bro-terminal")
    terminal_action = recover_exit_position(
        pos, broker=broker, exit_engine=exit_engine, osm=terminal_osm,
        order_monitor=_dead_monitor(),
    )
    assert terminal_action.action == "REPLACEMENT_SAFE"
    assert terminal_osm.transitions[0][1] == "CANCELED"

    monkeypatch.setattr(
        rec_mod,
        "_matching_open_exit_orders",
        lambda *args, **kwargs: [
            ("bro-a", {"status": "working"}),
            ("bro-b", {"status": "working"}),
        ],
    )
    monkeypatch.setattr(
        rec_mod, "_cancel_order_with_proof",
        lambda broker, broker_id: (True, {"status": "canceled"}),
    )
    multi_osm = _DurableOSM(broker_id="")
    multi_action = recover_exit_position(
        _pos(pending_exit_broker_order_id=""),
        broker=broker,
        exit_engine=exit_engine,
        osm=multi_osm,
        order_monitor=_dead_monitor(),
    )
    assert multi_action.action == "REPLACEMENT_SAFE"
    assert multi_osm.transitions[0][1] == "CANCELED"


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
