# tests/test_p0_exit_retry_liveness_avgo_replay.py
# =============================================================================
# PR #423 component replay: a real APOrderMonitor and a real APExitEngine
# share the stale-exit cancel/replacement handoff for the motivating AVGO
# qty=7 / scale-out qty=2 shape.  This file deliberately uses a compact OSM
# and a direct broker-capture callback.  The real idempotency -> execution
# core -> OSM submit -> broker POST path is proved separately by
# test_p0_exit_retry_liveness_production_shape.py.
# =============================================================================

from __future__ import annotations

import os
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("ENCRYPTION_KEY", "ap-pr423-avgo-replay-test")

from ap.order_monitor import APOrderMonitor  # noqa: E402
from ap_exit_engine import APExitEngine, ExitDecision, ManagedPosition  # noqa: E402


class _ReplayBroker:
    def __init__(self, payloads):
        self.payloads = [dict(payload) for payload in payloads]
        self.get_calls = []
        self.cancel_calls = []
        self.submit_calls = []
        self.orders = {
            "bro-avgo": {
                "status": "working",
                "position_id": "pos-avgo",
                "kind": "EXIT",
            },
        }

    @property
    def active_exit_broker_ids(self):
        terminal = {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED", "FILLED"}
        return {
            broker_order_id
            for broker_order_id, order in self.orders.items()
            if str(order.get("status") or "").upper() not in terminal
        }

    def get_order(self, broker_order_id):
        self.get_calls.append(broker_order_id)
        if not self.payloads:
            return {"status": "unknown"}
        payload = dict(self.payloads.pop(0))
        if broker_order_id in self.orders:
            self.orders[broker_order_id].update(payload)
        return payload

    def cancel_order(self, broker_order_id):
        self.cancel_calls.append(broker_order_id)
        return {"status": "pending"}

    def submit_order(self, **kwargs):
        """Capture the simulated broker POST and enforce no old/new overlap."""
        old_order = self.orders["bro-avgo"]
        old_status = str(old_order.get("status") or "").upper()
        assert old_status in {"CANCELED", "CANCELLED", "EXPIRED", "REJECTED"}
        active_before = sorted(self.active_exit_broker_ids)
        assert "bro-avgo" not in active_before

        post = dict(kwargs)
        post["old_status"] = old_status
        post["active_broker_ids"] = active_before
        self.submit_calls.append(post)

        broker_order_id = f"bro-avgo-replacement-{len(self.submit_calls)}"
        self.orders[broker_order_id] = {
            "status": "working",
            "position_id": "pos-avgo",
            "kind": "EXIT",
            "qty": int(kwargs["quantity"]),
        }
        return {"status": "accepted", "broker_order_id": broker_order_id}


class _ReplayOSM:
    """Test-only OSM stand-in that invokes the real exit-engine hooks.

    This is component evidence for the monitor/engine handoff, not a claim
    that the production OSM submit boundary is exercised.
    """

    def __init__(self, exit_engine, *, partial=False):
        self.exit_engine = exit_engine
        self.partial = partial
        self.order = {
            "local_order_id": "loc-avgo",
            "broker_order_id": "bro-avgo",
            "kind": "EXIT",
            "position_id": "pos-avgo",
            "status": "EXIT_ACKNOWLEDGED",
            "qty": 2,
            "filled_qty": 0,
            "meta": {},
        }
        self.orders = {self.order["local_order_id"]: self.order}
        self.transitions = []

    def get_order(self, local_order_id):
        order = self.orders.get(local_order_id)
        return dict(order) if order is not None else None

    def update_order_meta(self, local_order_id, meta_patch):
        order = self.orders.get(local_order_id)
        if order is None:
            return False
        order.setdefault("meta", {}).update(dict(meta_patch or {}))
        return True

    def persist_stale_exit_cancel_attempt(self, local_order_id, broker_order_id, attempt):
        order = self.orders.get(local_order_id)
        if order is None or order.get("broker_order_id") != broker_order_id:
            return False
        marker_key = "stale_exit_cancel_liveness"
        marker = order.setdefault("meta", {}).get(marker_key)
        if marker is not None:
            if not isinstance(marker, dict) or marker.get("broker_order_id") != broker_order_id:
                return False
            try:
                existing_attempt = int(marker["attempt"])
            except (KeyError, TypeError, ValueError, OverflowError):
                return False
            if existing_attempt < 0 or existing_attempt >= int(attempt):
                return False
        order["meta"][marker_key] = {
            "broker_order_id": broker_order_id,
            "attempt": int(attempt),
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        return True

    def _get_active_exit_order(self, position_id):
        active_statuses = {
            "EXIT_REQUESTED",
            "EXIT_SUBMITTED",
            "EXIT_ACKNOWLEDGED",
            "EXIT_PARTIAL_FILL",
        }
        for order in self.orders.values():
            if (
                order.get("position_id") == position_id
                and str(order.get("status") or "").upper() in active_statuses
            ):
                return dict(order)
        return None

    get_active_exit_order = _get_active_exit_order

    def record_replacement(self, *, local_order_id, broker_order_id, qty):
        self.orders[local_order_id] = {
            "local_order_id": local_order_id,
            "broker_order_id": broker_order_id,
            "kind": "EXIT",
            "position_id": "pos-avgo",
            "client_id": "client-avgo",
            "execution_mode": "paper",
            "status": "EXIT_SUBMITTED",
            "qty": int(qty),
            "filled_qty": 0,
            "meta": {},
        }

    def transition(self, local_order_id, new_status, **kwargs):
        self.transitions.append((local_order_id, new_status, dict(kwargs)))
        if new_status == "EXIT_PARTIAL_FILL":
            self.order["status"] = new_status
            self.order["filled_qty"] = int(kwargs["filled_qty"])
            self.exit_engine.note_partial_exit_fill(
                self.order["position_id"],
                local_order_id=local_order_id,
                broker_order_id=self.order["broker_order_id"],
                cumulative_filled=int(kwargs["filled_qty"]),
                fill_price=kwargs.get("fill_price"),
            )
            return True
        if new_status == "CANCELED":
            self.order["status"] = new_status
            self.exit_engine.on_exit_failure(
                self.order["position_id"],
                reason=kwargs.get("last_error", ""),
                local_order_id=local_order_id,
                broker_order_id=self.order["broker_order_id"],
            )
            return True
        return True


def _engine(*, partial=False):
    engine = APExitEngine(broker=MagicMock())
    engine._emit_exit_event = MagicMock()
    engine._persist_exit_replace_attempt_to_db = MagicMock(return_value=True)
    applied_by_identity = {}

    def _persist_fill(
        pos, *, local_order_id, broker_order_id, cumulative_filled_qty,
        prior_cumulative_filled=None,
    ):
        order_identity = (local_order_id, broker_order_id)
        applied = applied_by_identity.get(
            order_identity,
            int(prior_cumulative_filled or 0),
        )
        cumulative = int(cumulative_filled_qty)
        delta = max(0, cumulative - applied)
        pos.quantity_remaining = max(0, int(pos.quantity_remaining or 0) - delta)
        applied_by_identity[order_identity] = max(applied, cumulative)
        return {
            "ok": True,
            "applied_delta": delta,
            "applied_cumulative_qty": cumulative,
            "quantity_remaining": pos.quantity_remaining,
            "identity": {
                "local_order_id": local_order_id,
                "broker_order_id": broker_order_id,
            },
        }
    engine._persist_exit_fill_consumption_to_db = MagicMock(side_effect=_persist_fill)
    position = ManagedPosition(
        ticker="AVGO",
        option_symbol="AVGO260814C00350000",
        side="CALL",
        quantity=7,
        entry_price=2.49,
        underlying_entry=350.0,
        underlying_target=360.0,
        underlying_stop=340.0,
        position_id="pos-avgo",
        client_id="client-avgo",
        execution_mode="paper",
        exit_in_flight=True,
        pending_exit_local_order_id="loc-avgo",
        pending_exit_broker_order_id="bro-avgo",
        pending_exit_qty=2,
    )
    engine._positions.append(position)
    engine._positions_by_id[position.position_id] = position
    return engine, position


def _monitor(monkeypatch, broker, osm, engine, pm):
    import ap.order_monitor as monitor_module

    # The production status cache is intentionally process-global.  Clear it
    # here so this replay is independent from another test's same broker id.
    with monitor_module._BROKER_STATUS_CACHE_LOCK:
        monitor_module._BROKER_STATUS_CACHE.clear()

    monkeypatch.setattr(monitor_module, "ORDER_MONITOR_MODE", "watchdog")
    monkeypatch.setattr(monitor_module, "ORDER_MONITOR_CAN_ACT", False)
    monkeypatch.setattr(
        monitor_module, "ALLOW_STALE_EXIT_RECOVERY_IN_WATCHDOG", True,
    )
    monitor = APOrderMonitor(
        client_id="client-avgo",
        broker=broker,
        order_state_machine=osm,
        position_manager=pm,
        exit_engine=engine,
        entry_watcher=MagicMock(),
        client_mode="PAPER",
    )
    monitor._emit_order_event = MagicMock()
    monitor._alert = MagicMock()
    return monitor


def _set_fresh_replacement_quote(position):
    quote_ts = datetime.now(timezone.utc)
    position.current_bid = 2.00
    position.current_ask = 2.40
    position.current_option_price = 2.20
    position.current_underlying = 350.0
    position.option_bid_valid = True
    position.last_quote_update_ts = quote_ts
    position.last_option_quote_update_ts = quote_ts
    position.last_underlying_quote_update_ts = quote_ts


def _submit_component_replacement(monkeypatch, broker, osm, engine, position, *, expected_qty):
    """Exercise the engine pricing/quantity seam with a test callback.

    The callback intentionally captures a simulated POST directly.  Full
    production-shaped submission evidence lives in the companion test.
    """
    import ap.exit_safety as exit_safety_module

    # The broker-truth and submission-safety gates are independently covered;
    # this component replay keeps its external dependency to a captured,
    # simulated broker POST.
    monkeypatch.setattr(
        exit_safety_module,
        "resolve_exit_broker_truth",
        lambda **_: {
            "is_fresh_exact": False,
            "broker_truth_open_qty": None,
            "audit": {"source": "avgo_replay"},
        },
    )
    monkeypatch.setattr(
        exit_safety_module,
        "evaluate_exit_submission_safety",
        lambda **_: {"blocked": False},
    )

    engine.broker = broker
    engine._quote_broker = broker
    engine.order_state_machine = osm
    engine.osm = osm

    def _on_exit(pos, decision):
        broker_result = broker.submit_order(
            position_id=pos.position_id,
            quantity=decision.quantity,
            limit_price=decision.suggested_limit,
        )
        local_order_id = f"loc-avgo-replacement-{len(broker.submit_calls)}"
        osm.record_replacement(
            local_order_id=local_order_id,
            broker_order_id=broker_result["broker_order_id"],
            qty=decision.quantity,
        )
        return {
            "status": "EXIT_SUBMITTED",
            "accepted": True,
            "local_order_id": local_order_id,
            "broker_order_id": broker_result["broker_order_id"],
        }

    engine.on_exit = _on_exit
    _set_fresh_replacement_quote(position)
    decision = ExitDecision(
        action="STOP",
        quantity=position.quantity_remaining,
        reason="RUNNER TRAIL",
        urgency="HIGH",
        pnl_pct=0.10,
    )

    # Run the underlying engine seam directly so this component replay proves
    # pricing, quantity capping, and post-cancel identity without presenting
    # the test callback as the production OSM/broker submission path.
    import ap.exit_decision_idempotency_guard as idempotency_guard

    submit_core = getattr(
        APExitEngine,
        idempotency_guard._ORIGINAL_SUBMIT_ATTR,
        APExitEngine._submit_exit_decision,
    )
    assert submit_core(engine, position, decision) is True

    assert decision.reason_code == "RUNNER_TRAIL"
    assert decision._pricing_meta["attempt"] == 1
    assert decision._pricing_meta["tier"] == "TRAIL_BETWEEN"
    assert decision.suggested_limit == 2.07
    assert len(broker.submit_calls) == 1
    post = broker.submit_calls[0]
    assert post["quantity"] == expected_qty
    assert post["limit_price"] == 2.07
    assert post["old_status"] == "CANCELED"
    assert post["active_broker_ids"] == []
    assert broker.orders["bro-avgo"]["status"].upper() == "CANCELED"
    assert broker.active_exit_broker_ids == {
        "bro-avgo-replacement-1",
    }
    assert position.exit_in_flight is True
    assert position.pending_exit_replace_allowed is False
    assert position.pending_exit_replace_qty == expected_qty
    assert position.exit_retry_liveness["state"] == "REPLACEMENT_OWNED_BY_NEW_GENERATION"
    assert position.pending_exit_broker_order_id == "bro-avgo-replacement-1"


def test_avgo_real_monitor_and_exit_engine_cancel_replay_preserves_identity(monkeypatch):
    engine, position = _engine()
    broker = _ReplayBroker([
        {"status": "working"},
        {"status": "canceled"},
    ])
    osm = _ReplayOSM(engine)
    pm = MagicMock()
    monitor = _monitor(monkeypatch, broker, osm, engine, pm)

    monitor._handle_stale_exit(
        local_order_id="loc-avgo",
        status="WORKING",
        contract="AVGO260814C00350000",
        age_secs=120.0,
        position_id="pos-avgo",
        reason="stale AVGO scale-out",
    )

    assert broker.cancel_calls == ["bro-avgo"]
    assert [status for _, status, _ in osm.transitions] == ["CANCELED"]
    assert position.exit_in_flight is False
    assert position.pending_exit_local_order_id == ""
    assert position.pending_exit_broker_order_id == ""
    assert position.pending_exit_replace_allowed is True
    assert position.pending_exit_replace_qty == 2
    assert position.exit_replace_attempt == 1
    assert position.quantity_remaining == 7
    pm.update_position.assert_not_called()
    _submit_component_replacement(
        monkeypatch, broker, osm, engine, position, expected_qty=2,
    )


def test_avgo_partial_fill_replay_cancels_only_unfilled_remainder(monkeypatch):
    engine, position = _engine(partial=True)
    broker = _ReplayBroker([
        {"status": "partially_filled", "exec_quantity": 1, "avg_fill_price": 2.40},
        {"status": "partially_filled", "exec_quantity": 1, "avg_fill_price": 2.40},
        {"status": "canceled"},
    ])
    osm = _ReplayOSM(engine, partial=True)
    pm = MagicMock()
    monitor = _monitor(monkeypatch, broker, osm, engine, pm)

    monitor._handle_stale_exit(
        local_order_id="loc-avgo",
        status="WORKING",
        contract="AVGO260814C00350000",
        age_secs=120.0,
        position_id="pos-avgo",
        reason="stale AVGO scale-out after partial fill",
    )

    assert broker.cancel_calls == ["bro-avgo"]
    assert [status for _, status, _ in osm.transitions] == [
        "EXIT_PARTIAL_FILL", "CANCELED",
    ]
    assert osm.order["filled_qty"] == 1
    assert position.quantity_remaining == 6
    assert position.exit_in_flight is False
    assert position.pending_exit_replace_allowed is True
    assert position.pending_exit_replace_qty == 1
    assert position.exit_replace_attempt == 1
    pm.update_position.assert_not_called()
    _submit_component_replacement(
        monkeypatch, broker, osm, engine, position, expected_qty=1,
    )
