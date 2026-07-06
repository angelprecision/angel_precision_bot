"""
PR #300 amendment — selected materialization persistence must be mandatory.

If stamp_selected() receives a valid real-OCC payload but cannot persist
orders.meta materialization_status=SELECTED / broker_ready=True, execution must
not be able to mirror broker_ready=True in memory and continue toward broker
submit.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest

import ap  # noqa: F401 - package import installs safety guards
import ap.deferred_materializer as dm
from ap.deferred_materializer_persist_guard import MaterializationSelectedPersistError


_REAL_OCC = "GS  260717C00465000"
_DEFERRED = "DEFERRED:GS"
_CLIENT_ID = "jasoncosby1@gmail.com"
_EXEC_MODE = "live"


def _osm(update_ok: bool):
    osm = MagicMock()
    osm.update_order_meta.return_value = update_ok
    osm.transition.return_value = True
    return osm


def test_selected_materialization_persist_failure_raises_and_terminalizes():
    """Valid real OCC selection + failed meta persist must fail closed."""
    osm = _osm(update_ok=False)

    with pytest.raises(MaterializationSelectedPersistError) as exc:
        dm.stamp_selected(
            osm,
            "LOID-GS-1",
            client_id=_CLIENT_ID,
            execution_mode=_EXEC_MODE,
            symbol="GS",
            direction="CALL",
            contract=_REAL_OCC,
            bid=1.80,
            ask=1.86,
            mid=1.83,
            limit_price=1.87,
            qty=1,
            reserved_cost=187.0,
        )

    assert "materialization_selected_meta_persist_failed" in str(exc.value)
    osm.update_order_meta.assert_called_once()
    osm.transition.assert_called_once_with(
        "LOID-GS-1",
        "EXPIRED",
        last_error="materialization_selected_meta_persist_failed",
    )


def test_selected_materialization_persist_success_still_returns_true():
    """Successful durable SELECTED/broker_ready=True stamp remains unchanged."""
    osm = _osm(update_ok=True)

    ok = dm.stamp_selected(
        osm,
        "LOID-GS-1",
        client_id=_CLIENT_ID,
        execution_mode=_EXEC_MODE,
        symbol="GS",
        direction="CALL",
        contract=_REAL_OCC,
        bid=1.80,
        ask=1.86,
        mid=1.83,
        limit_price=1.87,
        qty=1,
        reserved_cost=187.0,
    )

    assert ok is True
    osm.transition.assert_not_called()
    patch = osm.update_order_meta.call_args.args[1]
    assert patch["materialization_status"] == dm.SELECTED
    assert patch["broker_ready"] is True
    assert patch["selected_contract"] == _REAL_OCC


def test_invalid_deferred_payload_preserves_original_false_behavior():
    """Invalid placeholder payloads should refuse broker_ready=True without raising."""
    osm = _osm(update_ok=True)

    ok = dm.stamp_selected(
        osm,
        "LOID-GS-1",
        client_id=_CLIENT_ID,
        execution_mode=_EXEC_MODE,
        symbol="GS",
        direction="CALL",
        contract=_DEFERRED,
        bid=1.80,
        ask=1.86,
        mid=1.83,
        limit_price=1.87,
        qty=1,
        reserved_cost=187.0,
    )

    assert ok is False
    osm.transition.assert_not_called()
    osm.update_order_meta.assert_not_called()
