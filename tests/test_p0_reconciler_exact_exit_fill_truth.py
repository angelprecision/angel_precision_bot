"""P0 #428 regressions for exact broker EXIT truth in the reconciler."""

from __future__ import annotations

import copy
import os
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test",
)

import ap.db as db_mod
from ap import manual_close_reconciliation as manual_close
from ap_reconciler import APBrokerReconciler, _empty_summary


CLIENT = "jason@example.com"
MODE_LIVE = "live"
MODE_PAPER = "paper"
POSITION_ID = "position-c-135"
TARGET_CONTRACT = "C260814C00135000"
OLD_CONTRACT = "C260807P00097000"


class _ExitCursor:
    """Small cursor double that applies the exact SQL identity predicates."""

    def __init__(self, rows: list[dict]):
        self.rows = list(rows)
        self.sql: list[str] = []
        self.params: list[tuple] = []
        self._selected: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        compact = " ".join(str(sql).split())
        self.sql.append(compact)
        self.params.append(tuple(params))
        client_id, mode, contract, position_id = params
        statuses = {"FILLED", "EXIT_FILLED", "EXIT_PARTIAL_FILL"}
        self._selected = [
            row
            for row in self.rows
            if row.get("client_id") == client_id
            and str(row.get("execution_mode") or "").strip().lower() == mode
            and str(row.get("kind") or "").upper() == "EXIT"
            and str(row.get("status") or "").upper() in statuses
            and str(row.get("contract") or "").strip().upper() == contract
            and str(row.get("position_id") or "").strip() == position_id
        ][:2]

    def fetchall(self):
        return list(self._selected)


def _install_exit_rows(monkeypatch, rows: list[dict]) -> _ExitCursor:
    cursor = _ExitCursor(rows)

    class _Connection:
        def __enter__(self):
            return cursor

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(db_mod, "conn", lambda: _Connection())
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())
    return cursor


def _reconciler(*, mode: str = MODE_LIVE) -> APBrokerReconciler:
    return APBrokerReconciler(
        broker=MagicMock(),
        client_id=CLIENT,
        osm=MagicMock(),
        pm=MagicMock(),
        execution_mode=mode,
    )


def _exit_row(
    *,
    contract: str = TARGET_CONTRACT,
    position_id: str = POSITION_ID,
    mode: str = MODE_LIVE,
    broker_order_id: str = "TR-195",
    fill_price: float = 1.95,
    filled_qty: int = 9,
    status: str = "EXIT_FILLED",
) -> dict:
    return {
        "client_id": CLIENT,
        "broker_order_id": broker_order_id,
        "local_order_id": "exit-local-1",
        "position_id": position_id,
        "contract": contract,
        "execution_mode": mode,
        "kind": "EXIT",
        "status": status,
        "fill_price": fill_price,
        "filled_qty": filled_qty,
        "filled_ts": "2026-08-10T19:00:00+00:00",
        "updated_ts": "2026-08-10T19:00:01+00:00",
    }


def _position(*, mode: str = MODE_LIVE) -> dict:
    return {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "contract": TARGET_CONTRACT,
        "underlying": "C",
        "qty": 9,
        "quantity_remaining": 9,
        "avg_fill": 2.33,
        "entry_price": 2.33,
        "status": "OPEN",
        "side": "CALL",
        "execution_mode": mode,
    }


def test_exact_c_incident_old_same_ticker_fill_is_not_evidence(monkeypatch):
    cursor = _install_exit_rows(
        monkeypatch,
        [_exit_row(contract=OLD_CONTRACT, position_id="old-put-position", fill_price=0.71)],
    )
    rec = _reconciler()

    result = rec._get_recent_exit_fill(
        TARGET_CONTRACT,
        position_id=POSITION_ID,
        execution_mode=MODE_LIVE,
    )

    assert result is None
    assert "symbol" not in cursor.sql[0].lower()
    assert "position_id::text = %s" in cursor.sql[0]


def test_exact_target_stc_fill_is_accepted_with_full_identity(monkeypatch):
    cursor = _install_exit_rows(monkeypatch, [_exit_row()])
    rec = _reconciler()

    result = rec._get_recent_exit_fill(
        TARGET_CONTRACT,
        position_id=POSITION_ID,
        execution_mode=MODE_LIVE,
    )

    assert result is not None
    assert result["broker_order_id"] == "TR-195"
    assert result["fill_price"] == 1.95
    assert cursor.params[0] == (CLIENT, MODE_LIVE, TARGET_CONTRACT, POSITION_ID)
    assert "LIMIT 2" in cursor.sql[0]


def test_exact_contract_with_wrong_position_id_is_rejected(monkeypatch):
    _install_exit_rows(monkeypatch, [_exit_row(position_id="different-position")])
    rec = _reconciler()

    assert rec._get_recent_exit_fill(
        TARGET_CONTRACT,
        position_id=POSITION_ID,
        execution_mode=MODE_LIVE,
    ) is None


def test_wrong_client_or_unusable_economics_are_rejected(monkeypatch):
    rec = _reconciler()

    _install_exit_rows(
        monkeypatch,
        [{**_exit_row(), "client_id": "other@example.com"}],
    )
    assert rec._get_recent_exit_fill(
        TARGET_CONTRACT,
        position_id=POSITION_ID,
        execution_mode=MODE_LIVE,
    ) is None

    for invalid_row in (
        _exit_row(broker_order_id=" ", fill_price=1.95),
        _exit_row(broker_order_id="TR-ZERO-QTY", filled_qty=0),
        _exit_row(broker_order_id="TR-ZERO-PRICE", fill_price=0),
    ):
        _install_exit_rows(monkeypatch, [invalid_row])
        assert rec._get_recent_exit_fill(
            TARGET_CONTRACT,
            position_id=POSITION_ID,
            execution_mode=MODE_LIVE,
        ) is None


@pytest.mark.parametrize(
    ("stored_mode", "query_mode"),
    [(MODE_LIVE, MODE_PAPER), (MODE_PAPER, MODE_LIVE)],
)
def test_cross_mode_exit_fill_is_rejected_both_directions(
    monkeypatch,
    stored_mode: str,
    query_mode: str,
):
    _install_exit_rows(monkeypatch, [_exit_row(mode=stored_mode)])
    rec = _reconciler(mode=query_mode)

    assert rec._get_recent_exit_fill(
        TARGET_CONTRACT,
        position_id=POSITION_ID,
        execution_mode=query_mode,
    ) is None


def test_multiple_exact_candidates_hold_without_newest_wins(monkeypatch):
    rows = [
        _exit_row(broker_order_id="TR-195-A", fill_price=1.95),
        _exit_row(broker_order_id="TR-195-B", fill_price=0.71),
    ]
    _install_exit_rows(monkeypatch, rows)
    rec = _reconciler()
    rec._alert = MagicMock()

    assert rec._get_recent_exit_fill(
        TARGET_CONTRACT,
        position_id=POSITION_ID,
        execution_mode=MODE_LIVE,
    ) is None
    assert any(
        "RECONCILER_EXIT_FILL_IDENTITY_AMBIGUOUS" in call.args[0]
        for call in rec._alert.call_args_list
    )


def _run_unproven_three_passes(rec: APBrokerReconciler, pos: dict, mark: float):
    summary = _empty_summary(CLIENT)
    rec._alert = MagicMock()
    rec._get_recent_exit_fill = MagicMock(return_value=None)
    rec._active_exit_order_exists = MagicMock(return_value=None)
    rec._broker_open_exit_exists_for_contract = MagicMock(return_value=False)
    rec._get_current_option_price = MagicMock(return_value=mark)
    rec._execute_reconciler_close = MagicMock()
    rec._record_reconciler_rejection = MagicMock()

    before = copy.deepcopy(pos)
    for _ in range(3):
        rec._handle_db_position_missing_at_broker(
            pos=pos,
            contract=TARGET_CONTRACT,
            underlying="C",
            db_qty=9,
            entry_px=2.33,
            summary=summary,
        )
    return summary, before


def test_broker_flat_three_passes_with_current_mark_do_not_close_or_write_proof():
    rec = _reconciler()
    pos = _position()
    summary, before = _run_unproven_three_passes(rec, pos, mark=1.95)

    assert pos == before
    assert rec._execute_reconciler_close.call_count == 0
    assert rec._record_reconciler_rejection.call_count == 0
    rec._get_current_option_price.assert_not_called()
    assert rec._ghost_tracker[TARGET_CONTRACT] == 3
    assert summary["positions_alerted"] == 3
    messages = [call.args[0] for call in rec._alert.call_args_list]
    assert any("ghost_pass=1" in message for message in messages)
    assert any("ghost_pass=2" in message for message in messages)
    assert any("ghost_pass=3" in message for message in messages)
    assert all(
        "BROKER_POSITION_MISSING_EXIT_FILL_UNPROVEN" in message
        for message in messages
    )


def test_broker_flat_three_passes_without_mark_do_not_use_entry_price_or_close():
    rec = _reconciler()
    pos = _position()
    summary, before = _run_unproven_three_passes(rec, pos, mark=0.0)

    assert pos == before
    assert rec._execute_reconciler_close.call_count == 0
    assert rec._record_reconciler_rejection.call_count == 0
    rec._get_current_option_price.assert_not_called()
    assert summary["positions_alerted"] == 3


def test_active_exit_guard_still_blocks_ghost_path():
    rec = _reconciler()
    rec._alert = MagicMock()
    rec._get_recent_exit_fill = MagicMock(return_value=None)
    rec._active_exit_order_exists = MagicMock(return_value={"status": "EXIT_REQUESTED"})
    rec._broker_open_exit_exists_for_contract = MagicMock(return_value=False)
    rec._get_current_option_price = MagicMock(return_value=1.95)
    rec._execute_reconciler_close = MagicMock()
    summary = _empty_summary(CLIENT)

    rec._handle_db_position_missing_at_broker(
        pos=_position(),
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=9,
        entry_px=2.33,
        summary=summary,
    )

    rec._execute_reconciler_close.assert_not_called()
    rec._get_current_option_price.assert_not_called()
    assert rec._ghost_tracker == {}
    assert any(
        "GHOST_CLOSE_BLOCKED_ACTIVE_EXIT" in call.args[0]
        for call in rec._alert.call_args_list
    )


def test_existing_exact_bot_exit_fill_still_reaches_existing_close_path(monkeypatch):
    _install_exit_rows(monkeypatch, [_exit_row()])
    rec = _reconciler()
    rec._execute_reconciler_close = MagicMock()
    rec._record_reconciler_rejection = MagicMock()
    summary = _empty_summary(CLIENT)

    rec._handle_db_position_missing_at_broker(
        pos=_position(),
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=9,
        entry_px=2.33,
        summary=summary,
    )

    rec._execute_reconciler_close.assert_called_once()
    close_kwargs = rec._execute_reconciler_close.call_args.kwargs
    assert close_kwargs["exit_px"] == 1.95
    assert close_kwargs["close_confidence"] == "HIGH"
    rec._record_reconciler_rejection.assert_called_once()


def test_strict_manual_close_selector_keeps_exact_target_stc_fill():
    detected_at = datetime(2026, 8, 10, 20, 0, tzinfo=timezone.utc)
    evidence, reason = manual_close.select_external_close_fills(
        orders=[
            {
                "id": "TR-195",
                "symbol": TARGET_CONTRACT,
                "side": "sell_to_close",
                "status": "filled",
                "filled_quantity": 9,
                "avg_fill_price": 1.95,
                "last_fill_date": "2026-08-10T19:00:00+00:00",
            },
            {
                "id": "TR-071",
                "symbol": OLD_CONTRACT,
                "side": "sell_to_close",
                "status": "filled",
                "filled_quantity": 9,
                "avg_fill_price": 0.71,
                "last_fill_date": "2026-08-10T19:01:00+00:00",
            },
        ],
        position={
            "client_id": CLIENT,
            "contract": TARGET_CONTRACT,
            "side": "CALL",
            "quantity_remaining": 9,
            "entry_ts": "2026-08-10T18:00:00+00:00",
        },
        bot_exit_order_ids=set(),
        adopted_fills=[],
        detected_at=detected_at,
    )

    assert reason == "exact_external_broker_fill"
    assert evidence is not None
    assert evidence["fill_price"] == 1.95
    assert evidence["broker_order_id"] == "TR-195"


def test_unproven_path_makes_no_broker_submit_or_cancel_call():
    rec = _reconciler()
    _run_unproven_three_passes(rec, _position(), mark=1.95)

    for method_name in ("post", "submit_order", "place_order", "cancel_order"):
        getattr(rec.broker, method_name).assert_not_called()
