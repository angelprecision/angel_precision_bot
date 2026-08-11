# tests/test_p0_exit_retry_liveness_autonomous_recovery.py
# =============================================================================
# P0 regression: PR #423 — ambiguous broker exit identity is diagnostics-only.
# Contract/side matching can discover candidate orders, but it cannot prove
# which broker order belongs to the local OSM generation. Autonomous recovery
# must therefore perform zero broker mutation and grant zero replacement
# authority whenever more than one live candidate exists, regardless of
# order-monitor health. Broker query failure is also unavailable truth, not an
# authoritative empty snapshot.
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
    RecoveryAction,
    recover_exit_position,
    recover_exit_engine,
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
        supplied_broker_id = kwargs.get("broker_order_id")
        if (
            supplied_broker_id
            and self.row["broker_order_id"]
            and supplied_broker_id != self.row["broker_order_id"]
        ):
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


def test_stale_exit_cancel_attempt_writer_requires_exact_execution_mode():
    from ap.exit_autonomous_recovery import _persist_stale_exit_cancel_attempt

    class _ModeOSM:
        def __init__(self):
            self.row = {
                "local_order_id": "loc-mode",
                "broker_order_id": "bro-mode",
                "execution_mode": "paper",
                "meta": {},
            }

        def persist_stale_exit_cancel_attempt(self, local_id, broker_id, attempt, execution_mode):
            if (
                local_id != self.row["local_order_id"]
                or broker_id != self.row["broker_order_id"]
                or execution_mode != self.row["execution_mode"]
            ):
                return False
            self.row["meta"]["attempt"] = attempt
            return True

    osm = _ModeOSM()
    assert _persist_stale_exit_cancel_attempt(
        osm, "loc-mode", "bro-mode", 1, execution_mode="paper"
    ) is True
    assert _persist_stale_exit_cancel_attempt(
        osm, "loc-mode", "bro-mode", 2, execution_mode="live"
    ) is False
    assert _persist_stale_exit_cancel_attempt(
        osm, "loc-mode", "bro-mode", 2, execution_mode=""
    ) is False


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
        execution_mode="paper",
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
    assert engine._persist_exit_replace_attempt_to_db.call_count == 2


def test_ambiguous_multi_match_never_cancels_without_exact_identity(monkeypatch):
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(
        rec_mod,
        "_matching_open_exit_orders",
        lambda broker, contract, exclude_broker_id=None: (True, [
            ("bro-a", {"status": "working", "quantity": 2}),
            ("bro-b", {"status": "working", "quantity": 2}),
        ]),
    )

    cancel_spy = MagicMock(return_value=(True, {"status": "canceled"}))
    monkeypatch.setattr(rec_mod, "_cancel_order_with_proof", cancel_spy)

    for order_monitor in (_alive_monitor(), _dead_monitor(), None):
        cancel_spy.reset_mock()

        exit_engine = MagicMock()
        osm = _DurableOSM(broker_id="")

        action = recover_exit_position(
            _pos(pending_exit_broker_order_id=""),
            broker=MagicMock(),
            exit_engine=exit_engine,
            osm=osm,
            order_monitor=order_monitor,
        )

        assert action.action == "NOOP"
        assert action.reason == "multiple_live_exit_orders_identity_ambiguous"
        assert action.details["match_count"] == 2
        assert action.details["broker_mutation_blocked"] is True
        assert action.details["replacement_blocked"] is True
        assert {
            item["broker_order_id"]
            for item in action.details["matches"]
        } == {"bro-a", "bro-b"}

        cancel_spy.assert_not_called()
        exit_engine.mark_exit_replacement_safe.assert_not_called()
        exit_engine.finalize_exit_replacement_safe.assert_not_called()
        exit_engine.clear_exit_in_flight.assert_not_called()
        assert osm.transitions == []


def test_ambiguous_multi_match_with_partial_fill_blocks_all_broker_mutation(monkeypatch):
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(
        rec_mod,
        "_matching_open_exit_orders",
        lambda broker, contract, exclude_broker_id=None: (True, [
            (
                "bro-working",
                {
                    "status": "working",
                    "quantity": 2,
                },
            ),
            (
                "bro-partial",
                {
                    "status": "partially_filled",
                    "quantity": 2,
                    "exec_quantity": 1,
                },
            ),
        ]),
    )

    cancel_spy = MagicMock(return_value=(True, {"status": "canceled"}))
    monkeypatch.setattr(rec_mod, "_cancel_order_with_proof", cancel_spy)

    exit_engine = MagicMock()
    osm = _DurableOSM(broker_id="")

    action = recover_exit_position(
        _pos(pending_exit_broker_order_id=""),
        broker=MagicMock(),
        exit_engine=exit_engine,
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "multiple_live_exit_orders_identity_ambiguous"
    assert action.details["broker_mutation_blocked"] is True

    cancel_spy.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    exit_engine.finalize_exit_replacement_safe.assert_not_called()
    exit_engine.clear_exit_in_flight.assert_not_called()
    assert osm.transitions == []


class _BrokerSnapshots:
    def __init__(self, *, orders=None, positions=None):
        self.orders = orders
        self.positions = positions
        self.list_orders_calls = 0
        self.list_positions_calls = 0
        self.cancel_calls = 0

    def list_orders(self):
        self.list_orders_calls += 1
        if isinstance(self.orders, BaseException):
            raise self.orders
        return self.orders

    def list_positions(self):
        self.list_positions_calls += 1
        if isinstance(self.positions, BaseException):
            raise self.positions
        return self.positions

    def cancel_order(self, broker_order_id):
        self.cancel_calls += 1
        return {"status": "canceled", "broker_order_id": broker_order_id}


def _recovery_position(**overrides):
    defaults = dict(
        position_id="pos-snapshot",
        option_symbol="AVGO260814C00350000",
        pending_exit_local_order_id="loc-snapshot",
        pending_exit_broker_order_id="",
        pending_exit_qty=2,
        contracts=2,
        quantity_remaining=2,
        exit_in_flight=True,
        closed=False,
        last_exit_signal_ts=None,
        last_callback_identity_missing_ts=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def test_open_order_query_failure_is_noop_and_does_not_mutate_or_check_positions():
    broker = _BrokerSnapshots(
        orders=ConnectionError("Tradier orders unavailable"),
        positions=[{"symbol": "AVGO260814C00350000", "quantity": 2}],
    )
    exit_engine = MagicMock()
    osm = _DurableOSM(broker_id="")

    action = recover_exit_position(
        _recovery_position(),
        broker=broker,
        exit_engine=exit_engine,
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_recovery_open_order_query_unavailable"
    assert action.details["broker_truth_unavailable"] is True
    assert action.details["replacement_blocked"] is True
    assert broker.list_positions_calls == 0
    assert broker.cancel_calls == 0
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    exit_engine.finalize_exit_replacement_safe.assert_not_called()
    exit_engine.clear_exit_in_flight.assert_not_called()
    assert osm.transitions == []


def test_exact_order_query_failure_is_noop_even_when_fallback_snapshots_look_safe():
    class _ExactLookupFailureBroker:
        def __init__(self):
            self.list_orders_calls = 0
            self.list_positions_calls = 0

        def get_order(self, broker_order_id):
            raise ConnectionError("Tradier exact order unavailable")

        def list_orders(self):
            self.list_orders_calls += 1
            return []

        def list_positions(self):
            self.list_positions_calls += 1
            return [{"symbol": "AVGO260814C00350000", "quantity": 2}]

    broker = _ExactLookupFailureBroker()
    position = _recovery_position(pending_exit_broker_order_id="bro-old")
    exit_engine = MagicMock()
    osm = _DurableOSM(broker_id="bro-old")

    action = recover_exit_position(
        position,
        broker=broker,
        exit_engine=exit_engine,
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_recovery_exact_order_query_unavailable"
    assert action.details["broker_truth_unavailable"] is True
    assert action.details["replacement_blocked"] is True
    assert broker.list_orders_calls == 0
    assert broker.list_positions_calls == 0
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    exit_engine.finalize_exit_replacement_safe.assert_not_called()
    exit_engine.clear_exit_in_flight.assert_not_called()
    assert osm.transitions == []
    assert position.exit_in_flight is True
    assert position.closed is False


def test_authoritative_zero_orders_and_held_position_allows_recovery():
    broker = _BrokerSnapshots(
        orders=[],
        positions=[{"symbol": "AVGO260814C00350000", "quantity": 2}],
    )
    exit_engine = MagicMock()
    osm = _DurableOSM(
        local_id="loc-snapshot",
        broker_id="",
        position_id="pos-snapshot",
    )

    action = recover_exit_position(
        _recovery_position(),
        broker=broker,
        exit_engine=exit_engine,
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "REPLACEMENT_SAFE"
    assert action.details["open_order_query_available"] is True
    assert action.details["position_query_available"] is True
    assert broker.list_orders_calls == 1
    assert broker.list_positions_calls == 1
    exit_engine.mark_exit_replacement_safe.assert_called_once()
    assert len(osm.transitions) == 1


def test_position_query_failure_after_authoritative_zero_orders_is_noop():
    broker = _BrokerSnapshots(
        orders=[],
        positions=TimeoutError("Tradier positions unavailable"),
    )
    exit_engine = MagicMock()
    osm = _DurableOSM(broker_id="")

    action = recover_exit_position(
        _recovery_position(),
        broker=broker,
        exit_engine=exit_engine,
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_recovery_position_query_unavailable"
    assert action.details["open_order_query_available"] is True
    assert action.details["position_query_available"] is False
    exit_engine.mark_position_closed.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    assert osm.transitions == []


def test_terminal_exact_order_requires_position_proof_and_closes_authoritative_flat_state():
    class _TerminalFlatBroker:
        def __init__(self):
            self.list_positions_calls = 0

        def get_order(self, broker_order_id):
            return {"id": broker_order_id, "status": "canceled"}

        def list_orders(self):
            return []

        def list_positions(self):
            self.list_positions_calls += 1
            return []

    broker = _TerminalFlatBroker()
    position = _recovery_position(pending_exit_broker_order_id="bro-old")
    exit_engine = MagicMock()

    def _mark_closed(position_id, **kwargs):
        assert position_id == position.position_id
        position.closed = True
        position.quantity_remaining = 0
        position.exit_in_flight = False

    exit_engine.mark_position_closed.side_effect = _mark_closed

    action = recover_exit_position(
        position,
        broker=broker,
        exit_engine=exit_engine,
        osm=_DurableOSM(broker_id="bro-old"),
        order_monitor=_dead_monitor(),
    )

    assert action.action == "MARKED_CLOSED"
    assert action.reason == "autonomous_recovery_contract_flat_at_broker"
    assert broker.list_positions_calls == 1
    exit_engine.mark_position_closed.assert_called_once()
    exit_engine.mark_exit_replacement_safe.assert_not_called()


def test_exact_filled_order_is_not_claimed_closed_without_engine_confirmation():
    broker = MagicMock()
    broker.get_order.return_value = {
        "id": "bro-filled",
        "status": "filled",
        "quantity": 2,
    }
    position = _recovery_position(pending_exit_broker_order_id="bro-filled")
    exit_engine = MagicMock()

    action = recover_exit_position(
        position,
        broker=broker,
        exit_engine=exit_engine,
        osm=_DurableOSM(broker_id="bro-filled"),
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_recovery_broker_filled_close_unconfirmed"
    exit_engine.mark_position_closed.assert_called_once()
    assert exit_engine.mark_position_closed.call_args.kwargs["economic_pending"] is True
    assert position.closed is False
    assert position.quantity_remaining == 2
    assert position.exit_in_flight is True


def test_exact_filled_scale_out_uses_real_engine_partial_fill_accounting():
    """A filled tranche must reduce the remainder, not finalize the position."""
    broker = MagicMock()
    broker.get_order.return_value = {
        "id": "bro-filled",
        "status": "filled",
        "contract": "AVGO260814C00350000",
        "quantity": 2,
        "filled_qty": 2,
    }
    position = _production_callsite_position()
    position.quantity = 7
    position.quantity_remaining = 7
    position.pending_exit_broker_order_id = "bro-filled"
    position.pending_exit_action = "SCALE_OUT"
    position.pending_exit_qty = 2
    position.pending_exit_filled_qty = 0

    exit_engine = APExitEngine(broker=broker, email="client-self-healing")
    exit_engine._emit_exit_event = MagicMock()
    exit_engine._persist_exit_replace_attempt_to_db = MagicMock(return_value=True)
    partial_fill_spy = MagicMock(wraps=exit_engine.note_partial_exit_fill)
    close_spy = MagicMock(wraps=exit_engine.mark_position_closed)
    exit_engine.note_partial_exit_fill = partial_fill_spy
    exit_engine.mark_position_closed = close_spy
    exit_engine.add_position(position)

    action = recover_exit_position(
        position,
        broker=broker,
        exit_engine=exit_engine,
        osm=_DurableOSM(
            local_id=position.pending_exit_local_order_id,
            broker_id="bro-filled",
            position_id=position.position_id,
        ),
        order_monitor=_dead_monitor(),
    )

    assert action.action == "CONFIRMED_OPEN"
    assert action.reason == "broker_order_filled_partial_position"
    partial_fill_spy.assert_called_once()
    assert partial_fill_spy.call_args.kwargs["cumulative_filled"] == 2
    close_spy.assert_not_called()
    assert position.quantity_remaining == 5
    assert position.closed is False
    assert position.scale_outs_done == 1
    assert position.pending_exit_qty == 0
    assert position.exit_in_flight is False
    assert exit_engine.active_positions() == [position]
    assert exit_engine._can_submit_exit(position, datetime.now(timezone.utc)) is True


@pytest.mark.parametrize(
    "payload",
    [
        {"id": "bro-filled", "status": "filled", "quantity": False},
        {"id": "bro-filled", "status": "filled", "quantity": True},
        {"id": "bro-filled", "status": "filled", "quantity": 0},
        {"id": "bro-filled", "status": "filled", "quantity": -1},
        {"id": "bro-filled", "status": "filled", "quantity": 1.5},
        {"id": "bro-filled", "status": "filled", "quantity": " 2 "},
    ],
    ids=["boolean_false", "boolean_true", "zero", "negative", "fraction", "whitespace"],
)
def test_malformed_filled_quantity_is_zero_mutation(payload):
    broker = MagicMock()
    broker.get_order.return_value = payload
    position = _recovery_position(pending_exit_broker_order_id="bro-filled")
    exit_engine = MagicMock()

    action = recover_exit_position(
        position,
        broker=broker,
        exit_engine=exit_engine,
        osm=_DurableOSM(broker_id="bro-filled"),
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_recovery_exact_order_query_unavailable"
    assert action.details["broker_mutation_blocked"] is True
    exit_engine.mark_position_closed.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()


def test_conflicting_broker_order_authorities_are_zero_mutation():
    broker = MagicMock()
    broker.get_order.return_value = {
        "id": "bro-filled",
        "status": "filled",
        "state": "working",
        "quantity": 2,
    }
    position = _recovery_position(pending_exit_broker_order_id="bro-filled")
    exit_engine = MagicMock()

    action = recover_exit_position(
        position,
        broker=broker,
        exit_engine=exit_engine,
        osm=_DurableOSM(broker_id="bro-filled"),
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_recovery_exact_order_query_unavailable"
    assert action.details["broker_mutation_blocked"] is True
    exit_engine.mark_position_closed.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()


def test_recovery_processes_all_positions_by_default_and_reports_explicit_capacity():
    import ap.exit_autonomous_recovery as recovery_module

    positions = [
        SimpleNamespace(position_id=f"pos-{index}", exit_in_flight=True)
        for index in range(11)
    ]
    engine = MagicMock()
    engine.active_positions.return_value = positions
    calls = []

    def _recover(position, **_kwargs):
        calls.append(position.position_id)
        return RecoveryAction("NOOP", "test", position.position_id)

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(recovery_module, "recover_exit_position", _recover)
    try:
        actions = recover_exit_engine(engine, broker=object())
        assert len(actions) == 11
        assert calls == [f"pos-{index}" for index in range(11)]

        calls.clear()
        capped = recover_exit_engine(engine, broker=object(), max_positions=10)
    finally:
        monkeypatch.undo()

    assert calls == [f"pos-{index}" for index in range(10)]
    assert capped[-1].action == "DEFERRED"
    assert capped[-1].reason == "autonomous_recovery_capacity_deferred"
    assert capped[-1].position_id == "pos-10"
    assert capped[-1].details["broker_mutation_blocked"] is True


def test_authoritative_zero_orders_and_flat_position_uses_real_close_contract_and_verifies_state():
    broker = _BrokerSnapshots(orders=[], positions=[])
    position = _recovery_position()
    exit_engine = MagicMock()

    def _mark_closed(position_id, **kwargs):
        assert position_id == position.position_id
        assert kwargs["reason"] == "AUTONOMOUS_RECOVERY_BROKER_FLAT"
        assert kwargs["qty_filled"] == 2
        assert kwargs["fill_price"] is None
        assert kwargs["local_order_id"] == "loc-snapshot"
        assert kwargs["broker_order_id"] == ""
        assert kwargs["reconciled"] is True
        assert kwargs["economic_pending"] is True
        position.closed = True
        position.quantity_remaining = 0
        position.exit_in_flight = False

    exit_engine.mark_position_closed.side_effect = _mark_closed

    action = recover_exit_position(
        position,
        broker=broker,
        exit_engine=exit_engine,
        osm=_DurableOSM(broker_id=""),
        order_monitor=_dead_monitor(),
    )

    assert action.action == "MARKED_CLOSED"
    assert action.reason == "autonomous_recovery_contract_flat_at_broker"
    exit_engine.mark_position_closed.assert_called_once()
    exit_engine.mark_exit_replacement_safe.assert_not_called()


def test_authoritative_flat_position_closes_through_real_exit_engine():
    broker = _BrokerSnapshots(orders=[], positions=[])
    position = _production_callsite_position()
    # Exercise the negative-proof path directly.  A pending broker ID now
    # requires an authoritative exact get_order() snapshot first.
    position.pending_exit_broker_order_id = ""
    exit_engine = APExitEngine(broker=broker, email="client-self-healing")
    exit_engine._emit_exit_event = MagicMock()
    exit_engine._persist_exit_replace_attempt_to_db = MagicMock(return_value=True)
    exit_engine._positions = [position]
    exit_engine._positions_by_id[position.position_id] = position

    action = recover_exit_position(
        position,
        broker=broker,
        exit_engine=exit_engine,
        osm=None,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "MARKED_CLOSED"
    assert position.closed is True
    assert position.quantity_remaining == 0
    assert position.exit_in_flight is False
    assert position.position_id not in exit_engine._positions_by_id


def test_broker_flat_close_not_claimed_when_engine_does_not_confirm_closure():
    broker = _BrokerSnapshots(orders=[], positions=[])
    position = _recovery_position()
    exit_engine = MagicMock()

    action = recover_exit_position(
        position,
        broker=broker,
        exit_engine=exit_engine,
        osm=_DurableOSM(broker_id=""),
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_recovery_broker_flat_close_unconfirmed"
    exit_engine.mark_position_closed.assert_called_once()
    exit_engine.mark_exit_replacement_safe.assert_not_called()


def test_malformed_position_snapshot_is_unavailable_not_flat():
    broker = _BrokerSnapshots(
        orders=[],
        positions=[{"symbol": "AVGO260814C00350000", "quantity": "not-a-number"}],
    )
    exit_engine = MagicMock()
    osm = _DurableOSM(broker_id="")

    action = recover_exit_position(
        _recovery_position(),
        broker=broker,
        exit_engine=exit_engine,
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_recovery_position_query_unavailable"
    exit_engine.mark_position_closed.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    assert osm.transitions == []


def test_tradier_position_transport_failure_is_not_coerced_to_empty_snapshot():
    from ap.brokers.tradier import TradierBroker, TradierConfig

    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="acct",
        )
    )
    broker._get = MagicMock(side_effect=ConnectionError("positions unavailable"))

    with pytest.raises(ConnectionError):
        broker.list_positions()


@pytest.mark.parametrize("payload", [{}, [], {"orders": {}}, {"orders": []}])
def test_tradier_list_orders_malformed_top_level_payload_raises(payload):
    from ap.brokers.tradier import TradierBroker, TradierConfig

    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="acct",
        )
    )
    broker._get = MagicMock(return_value=payload)

    with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
        broker.list_orders()


@pytest.mark.parametrize("payload", [{}, [], {"positions": {}}, {"positions": []}])
def test_tradier_list_positions_malformed_top_level_payload_raises(payload):
    from ap.brokers.tradier import TradierBroker, TradierConfig

    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="acct",
        )
    )
    broker._get = MagicMock(return_value=payload)

    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        broker.list_positions()


@pytest.mark.parametrize(
    "row",
    [
        {"symbol": "AVGO260814C00350000", "cost_basis": 1.0},
        {"quantity": 2, "cost_basis": 1.0},
        {"symbol": "AVGO260814C00350000", "quantity": False, "cost_basis": 1.0},
    ],
    ids=["missing_quantity", "missing_symbol", "boolean_quantity"],
)
def test_tradier_list_positions_malformed_row_field_raises(row):
    from ap.brokers.tradier import TradierBroker, TradierConfig

    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="acct",
        )
    )
    broker._get = MagicMock(return_value={"positions": {"position": [row]}})

    with pytest.raises(ValueError, match="TRADIER_POSITIONS_PAYLOAD_MALFORMED"):
        broker.list_positions()


@pytest.mark.parametrize(
    "payload",
    [{"orders": None}, {"orders": {"order": None}}, {"orders": {"order": []}}],
)
def test_tradier_list_orders_explicit_empty_shapes_are_authoritative(payload):
    from ap.brokers.tradier import TradierBroker, TradierConfig

    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="acct",
        )
    )
    broker._get = MagicMock(return_value=payload)

    assert broker.list_orders() == []


@pytest.mark.parametrize(
    "payload",
    [{"positions": None}, {"positions": {"position": None}}, {"positions": {"position": []}}],
)
def test_tradier_list_positions_explicit_empty_shapes_are_authoritative(payload):
    from ap.brokers.tradier import TradierBroker, TradierConfig

    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="acct",
        )
    )
    broker._get = MagicMock(return_value=payload)

    assert broker.list_positions() == []


def test_recovery_with_malformed_real_tradier_order_snapshot_is_noop():
    from ap.brokers.tradier import TradierBroker, TradierConfig

    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="acct",
        )
    )
    broker._get = MagicMock(return_value={})
    position = _recovery_position()
    exit_engine = MagicMock()
    osm = _DurableOSM(broker_id="")

    action = recover_exit_position(
        position,
        broker=broker,
        exit_engine=exit_engine,
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_recovery_open_order_query_unavailable"
    assert action.details["broker_truth_unavailable"] is True
    assert action.details["replacement_blocked"] is True
    assert broker._get.call_count == 1
    exit_engine.mark_position_closed.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    exit_engine.finalize_exit_replacement_safe.assert_not_called()
    exit_engine.clear_exit_in_flight.assert_not_called()
    assert osm.transitions == []
    assert position.closed is False
    assert position.exit_in_flight is True


@pytest.mark.parametrize(
    "row",
    [
        {"symbol": "AVGO260814C00350000", "cost_basis": 1.0},
        {"quantity": 2, "cost_basis": 1.0},
        {"symbol": "AVGO260814C00350000", "quantity": False, "cost_basis": 1.0},
    ],
    ids=["missing_quantity", "missing_symbol", "boolean_quantity"],
)
def test_recovery_with_malformed_tradier_position_field_blocks_all_mutation(row):
    from ap.brokers.tradier import TradierBroker, TradierConfig

    broker = TradierBroker(
        TradierConfig(
            base_url="https://api.tradier.com",
            access_token="test-token",
            account_id="acct",
        )
    )

    def _get(endpoint):
        if endpoint.endswith("/orders"):
            return {"orders": {"order": []}}
        return {"positions": {"position": [row]}}

    broker._get = MagicMock(side_effect=_get)
    exit_engine = MagicMock()
    osm = _DurableOSM(broker_id="")

    action = recover_exit_position(
        _recovery_position(),
        broker=broker,
        exit_engine=exit_engine,
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_recovery_position_query_unavailable"
    assert action.details["broker_truth_unavailable"] is True
    assert action.details["replacement_blocked"] is True
    exit_engine.mark_position_closed.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()
    exit_engine.finalize_exit_replacement_safe.assert_not_called()
    exit_engine.clear_exit_in_flight.assert_not_called()
    assert osm.transitions == []


def test_cancel_ack_without_fresh_exact_order_proof_does_not_unlock_replacement():
    from ap.exit_autonomous_recovery import _cancel_order_with_proof

    class _CancelAckOnlyBroker:
        def cancel_order(self, broker_order_id):
            return {"status": "canceled", "broker_order_id": broker_order_id}

        def get_order(self, broker_order_id):
            raise ConnectionError("exact cancel proof unavailable")

    confirmed, proof = _cancel_order_with_proof(
        _CancelAckOnlyBroker(),
        "bro-old",
        max_retries=1,
        retry_delay=0,
    )

    assert confirmed is False
    assert proof["cancel_response_status"] == "canceled"
    assert proof["confirmed_status"] == ""


def test_exact_broker_cancel_blocked_when_osm_broker_identity_mismatches(monkeypatch):
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(rec_mod, "STALE_EXIT_RECOVERY_AGE_SECONDS", 45)

    pos = _pos(
        pending_exit_broker_order_id="bro-position",
        last_exit_signal_ts=datetime.now(timezone.utc) - timedelta(seconds=120),
    )

    # Durable OSM says this local order belongs to a DIFFERENT broker order.
    osm = _DurableOSM(broker_id="bro-osm")

    broker = MagicMock()
    broker.get_order.return_value = {"status": "working"}

    cancel_spy = MagicMock(return_value=(True, {"status": "canceled"}))
    monkeypatch.setattr(rec_mod, "_cancel_order_with_proof", cancel_spy)

    exit_engine = MagicMock()

    action = recover_exit_position(
        pos,
        broker=broker,
        exit_engine=exit_engine,
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "autonomous_cancel_attempt_durability_unconfirmed"

    cancel_spy.assert_not_called()
    exit_engine.mark_exit_replacement_safe.assert_not_called()

    assert osm.row["meta"] == {}
    assert osm.transitions == []


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


def test_autonomous_retry_waits_for_fresh_marker_interval_and_working_proof(monkeypatch):
    """A fresh attempt-1 marker blocks autonomous attempt 2 until due and re-proven."""
    import ap.exit_autonomous_recovery as rec_mod
    import ap.order_monitor as om_mod

    monkeypatch.setattr(rec_mod, "STALE_EXIT_RECOVERY_AGE_SECONDS", 45)
    monkeypatch.setattr(rec_mod, "STALE_EXIT_CANCEL_MAX_ATTEMPTS", 2)
    monkeypatch.setattr(om_mod, "STALE_EXIT_CANCEL_RETRY_AFTER_SECONDS", 15)

    base = datetime(2026, 8, 10, 12, 0, tzinfo=timezone.utc)
    now = {"value": base + timedelta(seconds=10)}
    monkeypatch.setattr(rec_mod, "_now", lambda: now["value"])

    pos = _pos(
        pending_exit_broker_order_id="bro-known",
        last_exit_signal_ts=base - timedelta(seconds=120),
    )
    osm = _DurableOSM()
    assert rec_mod._persist_stale_exit_cancel_attempt(osm, "loc-1", "bro-known", 1)
    osm.row["meta"][rec_mod.STALE_EXIT_CANCEL_LIVENESS_META_KEY]["updated_at"] = base.isoformat()

    broker = MagicMock()
    broker.get_order.return_value = {"status": "working"}
    cancel_spy = MagicMock(return_value=(True, {"status": "canceled"}))
    monkeypatch.setattr(rec_mod, "_cancel_order_with_proof", cancel_spy)

    action_before_due = recover_exit_position(
        pos,
        broker=broker,
        exit_engine=MagicMock(),
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action_before_due.reason == "autonomous_exit_cancel_retry_not_due"
    cancel_spy.assert_not_called()
    assert osm.row["meta"][rec_mod.STALE_EXIT_CANCEL_LIVENESS_META_KEY]["attempt"] == 1

    now["value"] = base + timedelta(seconds=15)
    action_after_due = recover_exit_position(
        pos,
        broker=broker,
        exit_engine=MagicMock(),
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action_after_due.action == "REPLACEMENT_SAFE"
    assert broker.get_order.call_count == 3  # initial proof, blocked proof, retry proof
    assert cancel_spy.call_count == 1
    assert osm.row["meta"][rec_mod.STALE_EXIT_CANCEL_LIVENESS_META_KEY]["attempt"] == 2


def test_autonomous_retry_missing_marker_timestamp_fails_closed(monkeypatch):
    """An existing durable attempt without a valid timestamp cannot consume another attempt."""
    import ap.exit_autonomous_recovery as rec_mod

    monkeypatch.setattr(rec_mod, "STALE_EXIT_RECOVERY_AGE_SECONDS", 45)
    pos = _pos(
        pending_exit_broker_order_id="bro-known",
        last_exit_signal_ts=datetime.now(timezone.utc) - timedelta(seconds=120),
    )
    osm = _DurableOSM()
    assert rec_mod._persist_stale_exit_cancel_attempt(osm, "loc-1", "bro-known", 1)
    osm.row["meta"][rec_mod.STALE_EXIT_CANCEL_LIVENESS_META_KEY].pop("updated_at")

    cancel_spy = MagicMock(return_value=(True, {"status": "canceled"}))
    monkeypatch.setattr(rec_mod, "_cancel_order_with_proof", cancel_spy)
    action = recover_exit_position(
        pos,
        broker=MagicMock(get_order=MagicMock(return_value={"status": "working"})),
        exit_engine=MagicMock(),
        osm=osm,
        order_monitor=_dead_monitor(),
    )

    assert action.reason == "autonomous_exit_cancel_retry_marker_timestamp_invalid"
    cancel_spy.assert_not_called()
    assert osm.row["meta"][rec_mod.STALE_EXIT_CANCEL_LIVENESS_META_KEY]["attempt"] == 1


def test_terminal_autonomous_path_uses_durable_osm_handoff(monkeypatch):
    """The exact terminal generation stays attached to every durable handoff."""
    import ap.exit_autonomous_recovery as rec_mod

    exit_engine = MagicMock()
    broker = MagicMock()
    broker.list_positions.return_value = [
        {"symbol": "AVGO260814C00350000", "quantity": 2},
    ]
    pos = _pos(pending_exit_broker_order_id="bro-terminal")

    monkeypatch.setattr(rec_mod, "_get_order", lambda broker, bid: {"status": "canceled"})
    monkeypatch.setattr(rec_mod, "_matching_open_exit_orders", lambda *args, **kwargs: (True, []))
    terminal_osm = _DurableOSM(broker_id="bro-terminal")
    terminal_action = recover_exit_position(
        pos, broker=broker, exit_engine=exit_engine, osm=terminal_osm,
        order_monitor=_dead_monitor(),
    )
    assert terminal_action.action == "REPLACEMENT_SAFE"
    assert terminal_action.broker_order_id == "bro-terminal"
    assert terminal_osm.transitions[0][1] == "CANCELED"
    assert terminal_osm.transitions[0][2]["broker_order_id"] == "bro-terminal"
    for hook in (
        exit_engine.mark_exit_replacement_safe,
        exit_engine.finalize_exit_replacement_safe,
        exit_engine.clear_exit_in_flight,
    ):
        _, hook_kwargs = hook.call_args
        assert hook_kwargs["broker_order_id"] == "bro-terminal"


def test_terminal_autonomous_path_rejects_changed_osm_broker_generation(monkeypatch):
    """A stale terminal proof cannot cancel or clear a newer durable generation."""
    import ap.exit_autonomous_recovery as rec_mod

    exit_engine = MagicMock()
    broker = MagicMock()
    broker.list_positions.return_value = [
        {"symbol": "AVGO260814C00350000", "quantity": 2},
    ]
    pos = _pos(pending_exit_broker_order_id="bro-old")

    monkeypatch.setattr(rec_mod, "_get_order", lambda broker, bid: {"status": "canceled"})
    monkeypatch.setattr(rec_mod, "_matching_open_exit_orders", lambda *args, **kwargs: (True, []))
    terminal_osm = _DurableOSM(broker_id="bro-old")

    def _advance_durable_generation(*args, **kwargs):
        terminal_osm.row["broker_order_id"] = "bro-new"
        return True

    exit_engine.mark_exit_replacement_safe.side_effect = _advance_durable_generation

    action = recover_exit_position(
        pos, broker=broker, exit_engine=exit_engine, osm=terminal_osm,
        order_monitor=_dead_monitor(),
    )

    assert action.action == "NOOP"
    assert action.reason == "durable_osm_cancel_unproven"
    assert action.broker_order_id == "bro-old"
    assert terminal_osm.row["status"] == "EXIT_ACKNOWLEDGED"
    assert terminal_osm.row["broker_order_id"] == "bro-new"
    assert terminal_osm.transitions[0][2]["broker_order_id"] == "bro-old"
    exit_engine.mark_exit_replacement_safe.assert_called_once()
    exit_engine.finalize_exit_replacement_safe.assert_not_called()
    exit_engine.clear_exit_in_flight.assert_not_called()
    exit_engine.revoke_exit_replacement_safe.assert_called_once()


if __name__ == "__main__":
    sys.exit(pytest.main([__file__, "-v"]))
