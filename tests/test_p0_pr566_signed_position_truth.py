"""PR #566: signed unrelated broker quantities must preserve target truth."""
from __future__ import annotations

import os
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

_CLIENT_ID = "jasoncosby1@gmail.com"
_CONTRACT = "GS  260717C00465000"


def _run_broker_truth(list_positions_return):
    from ap.exit_safety import resolve_exit_broker_truth

    broker = MagicMock()
    broker.list_positions = MagicMock(return_value=list_positions_return)
    broker.account_id = "VA_TEST"

    with patch("ap.exit_safety._extract_broker_account_id", return_value="VA_TEST"):
        return resolve_exit_broker_truth(
            broker=broker,
            client_id=_CLIENT_ID,
            contract=_CONTRACT,
        )


def test_unrelated_negative_stock_quantity_does_not_invalidate_flat_snapshot():
    result = _run_broker_truth([{"symbol": "AAPL", "quantity": -100}])

    assert result["broker_truth_open_qty"] == 0
    assert result["is_fresh_exact"] is True
    assert result["audit"]["snapshot_status"] == "contract_absent_open_qty_zero"


def test_unrelated_negative_short_option_does_not_invalidate_flat_snapshot():
    result = _run_broker_truth(
        [
            {
                "option_symbol": "SPY260717C00600000",
                "quantity": -3,
                "side": "short",
                "account_number": "VA_TEST",
            }
        ]
    )

    assert result["broker_truth_open_qty"] == 0
    assert result["is_fresh_exact"] is True
    assert result["audit"]["snapshot_status"] == "contract_absent_open_qty_zero"


@pytest.mark.parametrize(
    "row",
    [
        {"symbol": _CONTRACT, "quantity": -1, "side": "short"},
        {"symbol": _CONTRACT, "quantity": "not-a-number"},
        {"symbol": _CONTRACT, "quantity": 2, "qty": 1},
    ],
)
def test_target_contradictory_or_malformed_quantity_is_unknown_not_flat(row):
    result = _run_broker_truth([row])

    assert result["broker_truth_open_qty"] is None
    assert result["is_fresh_exact"] is False
    assert result["audit"]["snapshot_status"] == "broker_positions_malformed"


def test_malformed_unrelated_identity_keeps_snapshot_unknown():
    result = _run_broker_truth(
        [
            {
                "symbol": "SPY260717C00600000",
                "option_symbol": "SPY260717P00600000",
                "quantity": 1,
            }
        ]
    )

    assert result["broker_truth_open_qty"] is None
    assert result["is_fresh_exact"] is False
    assert result["audit"]["snapshot_status"] == "broker_positions_malformed"
