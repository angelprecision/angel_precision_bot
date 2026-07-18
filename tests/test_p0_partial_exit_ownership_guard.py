from __future__ import annotations

from types import SimpleNamespace

import ap.partial_exit_ownership_guard as guard


def test_partial_status_variants_are_recognized() -> None:
    for status in (
        "PARTIAL_FILL",
        "partially_filled",
        "partial",
        "EXIT_PARTIAL_FILL",
    ):
        assert guard.is_partial_exit_result({"status": status}) is True

    assert guard.is_partial_exit_result({"status": "FILLED"}) is False
    assert guard.is_partial_exit_result({"status": "CANCELED"}) is False


def test_partial_ownership_uses_remaining_quantity_on_same_broker_order() -> None:
    fields = guard.partial_exit_ownership_fields(
        {
            "qty": 5,
            "filled_qty": 0,
            "local_order_id": "exit-local-1",
            "broker_order_id": "exit-broker-old",
        },
        {
            "status": "PARTIAL_FILL",
            "filled_qty": 2,
            "broker_order_id": "exit-broker-1",
        },
    )

    assert fields == {
        "exit_in_flight": True,
        "pending_exit_local_order_id": "exit-local-1",
        "pending_exit_broker_order_id": "exit-broker-1",
        "pending_exit_qty": 3,
    }


def test_completed_scale_out_does_not_remain_inflight() -> None:
    projection = SimpleNamespace(closed=False)
    wrapped = guard.wrap_exit_fill_reconcile(
        lambda order, result: {"position_id": "position-1", "projection": projection}
    )

    result = wrapped(
        {
            "client_id": "client@example.com",
            "local_order_id": "scale-local-1",
            "broker_order_id": "scale-broker-1",
            "qty": 3,
        },
        {"status": "FILLED", "filled_qty": 3},
    )

    assert result["position_id"] == "position-1"
    assert "partial_exit_ownership_verified" not in result


def test_partial_fill_verifies_atomic_durable_identity() -> None:
    projection = SimpleNamespace(closed=False)
    wrapped = guard.wrap_exit_fill_reconcile(
        lambda order, result: {
            "position_id": "position-1",
            "projection": projection,
            "exit_ownership": guard.partial_exit_ownership_fields(order, result),
        }
    )
    order = {
        "client_id": "client@example.com",
        "local_order_id": "exit-local-1",
        "broker_order_id": "exit-broker-1",
        "qty": 5,
    }
    broker_result = {
        "status": "PARTIAL_FILL",
        "filled_qty": 2,
        "broker_order_id": "exit-broker-1",
    }

    result = wrapped(order, broker_result)

    assert result["partial_exit_ownership_verified"] is True


def test_fully_closed_projection_never_reopens_exit_ownership() -> None:
    projection = SimpleNamespace(closed=True)
    wrapped = guard.wrap_exit_fill_reconcile(
        lambda order, result: {"position_id": "position-1", "projection": projection}
    )

    result = wrapped(
        {
            "client_id": "client@example.com",
            "local_order_id": "exit-local-1",
            "qty": 1,
        },
        {"status": "PARTIAL_FILL", "filled_qty": 1},
    )

    assert result["position_id"] == "position-1"
    assert "partial_exit_ownership_verified" not in result


def test_missing_atomic_ownership_is_reported_without_second_write() -> None:
    projection = SimpleNamespace(closed=False)
    wrapped = guard.wrap_exit_fill_reconcile(
        lambda order, result: {"position_id": "position-1", "projection": projection}
    )

    result = wrapped(
        {
            "client_id": "client@example.com",
            "local_order_id": "exit-local-1",
            "qty": 5,
        },
        {"status": "PARTIAL_FILL", "filled_qty": 2},
    )

    assert result["position_id"] == "position-1"
    assert result["partial_exit_ownership_verified"] is False
