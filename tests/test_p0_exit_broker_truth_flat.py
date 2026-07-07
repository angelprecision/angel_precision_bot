from __future__ import annotations

from types import SimpleNamespace

from ap.exit_broker_truth_flat import (
    FLAT_REASON,
    classify_broker_truth_flat,
    clear_flat_repair_position_memory,
    is_broker_repair_position_id,
)


def test_broker_repair_qty_zero_classifies_flat():
    decision = classify_broker_truth_flat(
        position_id="broker-repair-client@example.com-VZ260717C00040000",
        broker_truth_open_qty=0,
        allow_missing_position_with_broker_truth=False,
    )
    assert decision.flat is True
    assert decision.reason == FLAT_REASON
    assert decision.broker_truth_open_qty == 0
    assert decision.repair_context is True


def test_explicit_broker_truth_flag_qty_zero_classifies_flat():
    decision = classify_broker_truth_flat(
        position_id="pos-normal-shaped-but-broker-truth",
        broker_truth_open_qty=0,
        allow_missing_position_with_broker_truth=True,
    )
    assert decision.flat is True
    assert decision.reason == FLAT_REASON


def test_normal_position_qty_zero_does_not_classify_flat_repair():
    decision = classify_broker_truth_flat(
        position_id="pos-normal",
        broker_truth_open_qty=0,
        allow_missing_position_with_broker_truth=False,
    )
    assert decision.flat is False
    assert decision.repair_context is False


def test_broker_repair_qty_positive_does_not_classify_flat():
    decision = classify_broker_truth_flat(
        position_id="broker-repair-client@example.com-VZ260717C00040000",
        broker_truth_open_qty=1,
        allow_missing_position_with_broker_truth=True,
    )
    assert decision.flat is False
    assert decision.broker_truth_open_qty == 1
    assert decision.repair_context is True


def test_clear_flat_repair_position_memory():
    pos = SimpleNamespace(
        closed=False,
        close_reason="",
        quantity_remaining=1,
        exit_in_flight=True,
        pending_exit_reason="old",
        pending_exit_action="CLOSE_ALL",
        pending_exit_qty=1,
        pending_exit_local_order_id="L-EXIT",
        pending_exit_broker_order_id="B-EXIT",
    )
    clear_flat_repair_position_memory(pos)
    assert pos.closed is True
    assert pos.close_reason == FLAT_REASON
    assert pos.quantity_remaining == 0
    assert pos.exit_in_flight is False
    assert pos.pending_exit_reason == ""
    assert pos.pending_exit_action == ""
    assert pos.pending_exit_qty == 0
    assert pos.pending_exit_local_order_id == ""
    assert pos.pending_exit_broker_order_id == ""


def test_broker_repair_position_id_classifier():
    assert is_broker_repair_position_id("broker-repair-client@example.com-XYZ") is True
    assert is_broker_repair_position_id("pos-normal") is False
