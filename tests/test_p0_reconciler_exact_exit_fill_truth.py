"""P0 #428 regressions for exact broker EXIT truth in the reconciler."""

from __future__ import annotations

import copy
import os
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test",
)

import ap.db as db_mod
from ap import manual_close_reconciliation as manual_close
import ap.position_manager as position_manager_mod
from ap_reconciler import APBrokerReconciler, _empty_summary


CLIENT = "jason@example.com"
MODE_LIVE = "live"
MODE_PAPER = "paper"
POSITION_ID = "position-c-135"
TARGET_CONTRACT = "C260814C00135000"
OLD_CONTRACT = "C260807P00134000"
OLD_BROKER_ORDER_ID = "36637227"


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
        upper_sql = compact.upper()
        if "STATUS = ANY" in upper_sql:
            self._selected = []
            return

        client_id, mode, contract, position_id = params[:4]
        param_index = 4
        expected_local = ""
        expected_broker = ""
        if "O.LOCAL_ORDER_ID = %S" in upper_sql:
            expected_local = str(params[param_index] or "").strip()
            param_index += 1
        if "O.BROKER_ORDER_ID = %S" in upper_sql:
            expected_broker = str(params[param_index] or "").strip()
        statuses = {"FILLED", "EXIT_FILLED"}
        self._selected = [
            row
            for row in self.rows
            if row.get("client_id") == client_id
            and str(row.get("execution_mode") or "").strip().lower() == mode
            and str(row.get("kind") or "").upper() == "EXIT"
            and str(row.get("status") or "").upper() in statuses
            and str(row.get("contract") or "").strip().upper() == contract
            and str(row.get("position_id") or "").strip() == position_id
            and (not expected_local or str(row.get("local_order_id") or "").strip() == expected_local)
            and (not expected_broker or str(row.get("broker_order_id") or "").strip() == expected_broker)
        ][:2]

    def fetchall(self):
        return list(self._selected)

    def fetchone(self):
        return self._selected[0] if self._selected else None


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
    local_order_id: str = "exit-local-1",
    filled_ts: str | None = "2026-08-10T19:00:00+00:00",
    position_entry_ts: str | None = "2026-08-10T18:00:00+00:00",
) -> dict:
    return {
        "client_id": CLIENT,
        "broker_order_id": broker_order_id,
        "local_order_id": local_order_id,
        "position_id": position_id,
        "contract": contract,
        "execution_mode": mode,
        "kind": "EXIT",
        "status": status,
        "fill_price": fill_price,
        "filled_qty": filled_qty,
        "filled_ts": filled_ts,
        "updated_ts": "2026-08-10T19:00:01+00:00",
        "position_entry_ts": position_entry_ts,
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
        "entry_ts": "2026-08-10T18:00:00+00:00",
        "opened_at": "2026-08-10T18:00:00+00:00",
        "local_order_id": "entry-local-1",
        "pending_exit_local_order_id": "exit-local-1",
        "pending_exit_broker_order_id": "TR-195",
    }


class _HealerCursor:
    """Cursor double that exposes the old OR versus the exact-pair SQL fence."""

    def __init__(
        self,
        rows: list[dict],
        *,
        current_local: str = "exit-B",
        current_broker: str = "222",
    ):
        self.rows = list(rows)
        self.current_local = current_local
        self.current_broker = current_broker
        self.sql: list[str] = []
        self._selected: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        compact = " ".join(str(sql).split())
        self.sql.append(compact)
        upper_sql = compact.upper()
        if "FROM ORDERS O" not in upper_sql:
            return

        exact_pair_predicate = (
            "NULLIF(TRIM(COALESCE(P.PENDING_EXIT_LOCAL_ORDER_ID, '')), '') IS NULL"
            in upper_sql
            and "NULLIF(TRIM(COALESCE(P.PENDING_EXIT_BROKER_ORDER_ID, '')), '') IS NULL"
            in upper_sql
        )

        def _matches(row: dict) -> bool:
            local_matches = (
                str(row.get("local_order_id") or "").strip() == self.current_local
            )
            broker_matches = (
                str(row.get("broker_order_id") or "").strip() == self.current_broker
            )
            return (
                local_matches and broker_matches
                if exact_pair_predicate
                else local_matches or broker_matches
            )

        self._selected = [dict(row) for row in self.rows if _matches(row)]

    def fetchall(self):
        return list(self._selected)


def _install_healer_rows(monkeypatch, rows: list[dict]) -> _HealerCursor:
    cursor = _HealerCursor(rows)

    class _Connection:
        def __enter__(self):
            return cursor

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(db_mod, "conn", lambda: _Connection())
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())
    return cursor


def _healer_row(*, local_order_id: str, broker_order_id: str) -> dict:
    return {
        "local_order_id": local_order_id,
        "position_id": POSITION_ID,
        "fill_price": 4.79,
        "filled_qty": 2,
        "filled_ts": "2026-08-10T19:00:00+00:00",
        "broker_order_id": broker_order_id,
    }


@pytest.mark.parametrize(
    ("row_local", "row_broker", "expected_heals"),
    [
        pytest.param("exit-B", "111", 0, id="local-only-match-holds"),
        pytest.param("exit-A", "222", 0, id="broker-only-match-holds"),
        pytest.param("exit-B", "222", 1, id="exact-pair-heals"),
    ],
)
def test_backup_healer_requires_exact_current_exit_generation_pair(
    monkeypatch,
    row_local: str,
    row_broker: str,
    expected_heals: int,
):
    cursor = _install_healer_rows(
        monkeypatch,
        [_healer_row(local_order_id=row_local, broker_order_id=row_broker)],
    )
    fake_pm = MagicMock()
    fake_pm.close_position_from_exit_fill.return_value = True
    monkeypatch.setattr(
        position_manager_mod,
        "APPositionManager",
        lambda _client_id: fake_pm,
    )

    rec = _reconciler()
    summary = _empty_summary(CLIENT)
    rec._heal_exit_filled_positions_from_orders(summary)

    assert cursor.sql
    healer_sql = cursor.sql[0].upper()
    assert "PENDING_EXIT_LOCAL_ORDER_ID" in healer_sql
    assert "PENDING_EXIT_BROKER_ORDER_ID" in healer_sql
    assert fake_pm.close_position_from_exit_fill.call_count == expected_heals
    assert summary["positions_corrected"] == expected_heals


def test_exact_c_incident_old_same_ticker_fill_is_not_evidence(monkeypatch):
    cursor = _install_exit_rows(
        monkeypatch,
        [
            _exit_row(
                contract=OLD_CONTRACT,
                position_id="old-put-position",
                mode=MODE_PAPER,
                broker_order_id=OLD_BROKER_ORDER_ID,
                fill_price=0.71,
                filled_qty=21,
            )
        ],
    )
    rec = _reconciler(mode=MODE_PAPER)

    result = rec._get_recent_exit_fill(
        TARGET_CONTRACT,
        position_id=POSITION_ID,
        execution_mode=MODE_PAPER,
        local_order_id="exit-local-1",
        broker_order_id=OLD_BROKER_ORDER_ID,
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
        local_order_id="exit-local-1",
        broker_order_id="TR-195",
    )

    assert result is not None
    assert result["broker_order_id"] == "TR-195"
    assert result["fill_price"] == 1.95
    assert cursor.params[0] == (
        CLIENT,
        MODE_LIVE,
        TARGET_CONTRACT,
        POSITION_ID,
        "exit-local-1",
        "TR-195",
    )
    assert "LIMIT 2" in cursor.sql[0]
    assert "EXIT_PARTIAL_FILL" not in cursor.sql[0]


def test_exact_contract_with_wrong_position_id_is_rejected(monkeypatch):
    _install_exit_rows(monkeypatch, [_exit_row(position_id="different-position")])
    rec = _reconciler()

    assert rec._get_recent_exit_fill(
        TARGET_CONTRACT,
        position_id=POSITION_ID,
        execution_mode=MODE_LIVE,
        local_order_id="exit-local-1",
        broker_order_id="TR-195",
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
        local_order_id="exit-local-1",
        broker_order_id="TR-195",
    ) is None

    for invalid_row in (
        _exit_row(broker_order_id=" ", fill_price=1.95),
        _exit_row(broker_order_id="TR-ZERO-QTY", filled_qty=0),
        _exit_row(broker_order_id="TR-ZERO-PRICE", fill_price=0),
        _exit_row(broker_order_id="TR-BOOL-QTY", filled_qty=True),
        _exit_row(broker_order_id="TR-BOOL-PRICE", fill_price=True),
    ):
        _install_exit_rows(monkeypatch, [invalid_row])
        assert rec._get_recent_exit_fill(
            TARGET_CONTRACT,
            position_id=POSITION_ID,
            execution_mode=MODE_LIVE,
            local_order_id="exit-local-1",
            broker_order_id=str(invalid_row.get("broker_order_id") or "").strip(),
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
        local_order_id="exit-local-1",
        broker_order_id="TR-195",
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
        local_order_id="exit-local-1",
    ) is None
    assert any(
        "RECONCILER_EXIT_FILL_IDENTITY_AMBIGUOUS" in call.args[0]
        for call in rec._alert.call_args_list
    )


@pytest.mark.parametrize("quantity_remaining", [4, 2])
def test_exit_partial_fill_is_not_terminal_evidence_or_reconciler_authority(
    monkeypatch, quantity_remaining: int
):
    """A partial EXIT row is not terminal evidence, even at full quantity coverage."""
    cursor = _install_exit_rows(
        monkeypatch,
        [_exit_row(status="EXIT_PARTIAL_FILL", filled_qty=2, fill_price=4.79)],
    )
    rec = _reconciler()
    rec._alert = MagicMock()
    rec._active_exit_order_exists = MagicMock(return_value=None)
    rec._broker_open_exit_exists_for_contract = MagicMock(return_value=False)
    rec._get_current_option_price = MagicMock(return_value=1.95)
    rec._execute_reconciler_close = MagicMock()
    rec._record_reconciler_rejection = MagicMock()
    pos = _position()
    pos["quantity_remaining"] = quantity_remaining
    pos["qty"] = quantity_remaining
    summary = _empty_summary(CLIENT)

    rec._handle_db_position_missing_at_broker(
        pos=pos,
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=quantity_remaining,
        entry_px=2.33,
        summary=summary,
    )

    assert cursor.sql and "EXIT_PARTIAL_FILL" not in cursor.sql[0]
    rec._execute_reconciler_close.assert_not_called()
    rec._record_reconciler_rejection.assert_not_called()
    rec._get_current_option_price.assert_not_called()
    assert summary["positions_alerted"] == 1
    assert any(
        "BROKER_POSITION_MISSING_EXIT_FILL_UNPROVEN" in call.args[0]
        for call in rec._alert.call_args_list
    )


@pytest.mark.parametrize("status", ["EXIT_FILLED", "FILLED"])
def test_terminal_exact_exit_status_can_authorize_full_close(monkeypatch, status: str):
    cursor = _install_exit_rows(
        monkeypatch,
        [_exit_row(status=status, filled_qty=2, fill_price=4.79)],
    )
    rec = _reconciler()
    rec._execute_reconciler_close = MagicMock()
    rec._record_reconciler_rejection = MagicMock()
    pos = _position()
    pos["quantity_remaining"] = 2
    pos["qty"] = 2
    summary = _empty_summary(CLIENT)

    rec._handle_db_position_missing_at_broker(
        pos=pos,
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=2,
        entry_px=2.33,
        summary=summary,
    )

    assert cursor.sql and "EXIT_PARTIAL_FILL" not in cursor.sql[0]
    rec._execute_reconciler_close.assert_called_once()
    close_kwargs = rec._execute_reconciler_close.call_args.kwargs
    assert close_kwargs["exact_exit_fill_qty"] == 2
    assert close_kwargs["exit_px"] == 4.79
    assert close_kwargs["exact_exit_evidence"]["broker_order_id"] == "TR-195"
    assert close_kwargs["exact_exit_evidence"]["exit_local_order_id"] == "exit-local-1"
    rec._record_reconciler_rejection.assert_called_once()
    rejection_kwargs = rec._record_reconciler_rejection.call_args.kwargs
    assert rejection_kwargs["reason_code"] == (
        "BROKER_POSITION_MISSING_EXACT_EXIT_FILL_CONFIRMED"
    )
    assert rejection_kwargs["broker_exit_order_id"] == "TR-195"
    assert rejection_kwargs["exit_local_order_id"] == "exit-local-1"
    assert summary["positions_alerted"] == 0


def test_missing_id_exit_with_only_partial_fill_does_not_promote_to_exit_filled(
    monkeypatch,
):
    cursor = _install_exit_rows(
        monkeypatch,
        [_exit_row(status="EXIT_PARTIAL_FILL", filled_qty=2, fill_price=4.79)],
    )
    rec = _reconciler()
    rec._alert = MagicMock()
    rec._recover_missing_broker_id_exit = MagicMock(return_value=False)
    rec.osm.transition = MagicMock(return_value=True)
    summary = _empty_summary(CLIENT)
    order = {
        "local_order_id": "exit-missing-broker-id",
        "kind": "EXIT",
        "status": "EXIT_PARTIAL_FILL",
        "position_id": POSITION_ID,
        "contract": TARGET_CONTRACT,
        "underlying": "C",
        "execution_mode": MODE_LIVE,
        "qty": 2,
        "filled_qty": 2,
        "broker_order_id": None,
    }

    assert rec._resolve_missing_id_exit_truth(
        order,
        summary,
        reason="partial-only evidence",
    ) is True

    assert cursor.sql and "EXIT_PARTIAL_FILL" not in cursor.sql[0]
    assert rec.osm.transition.call_count == 1
    assert rec.osm.transition.call_args.args[1] == "CANCELED"
    assert all(
        call.args[1] != "EXIT_FILLED"
        for call in rec.osm.transition.call_args_list
    )
    assert summary["orders_corrected"] == 1
    assert not any(
        "MISSING_ID_EXIT_RESOLVED_BY_RECENT_FILL" in call.args[0]
        for call in rec._alert.call_args_list
    )


def test_historical_same_position_exit_cannot_authorize_current_remaining_close(
    monkeypatch,
):
    """A prior scale-out is not evidence for the current final EXIT generation."""
    cursor = _install_exit_rows(
        monkeypatch,
        [
            _exit_row(
                local_order_id="exit-old",
                broker_order_id="111",
                filled_qty=2,
                fill_price=1.50,
            )
        ],
    )
    rec = _reconciler()
    rec._alert = MagicMock()
    rec._active_exit_order_exists = MagicMock(return_value=None)
    rec._broker_open_exit_exists_for_contract = MagicMock(return_value=False)
    rec._execute_reconciler_close = MagicMock()
    rec._record_reconciler_rejection = MagicMock()
    pos = _position()
    pos.update(
        {
            "qty": 4,
            "quantity_remaining": 2,
            "pending_exit_local_order_id": "exit-current",
            "pending_exit_broker_order_id": "222",
        }
    )
    before = copy.deepcopy(pos)
    summary = _empty_summary(CLIENT)

    rec._handle_db_position_missing_at_broker(
        pos=pos,
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=4,
        entry_px=2.33,
        summary=summary,
    )

    assert cursor.params[0][-2:] == ("exit-current", "222")
    assert pos == before
    rec._execute_reconciler_close.assert_not_called()
    rec._record_reconciler_rejection.assert_not_called()
    assert summary["positions_alerted"] == 1
    assert any(
        "BROKER_POSITION_MISSING_EXIT_FILL_UNPROVEN" in call.args[0]
        for call in rec._alert.call_args_list
    )


def test_historical_terminal_fill_cannot_bypass_active_current_exit_guard(
    monkeypatch,
):
    _install_exit_rows(
        monkeypatch,
        [
            _exit_row(
                local_order_id="exit-old",
                broker_order_id="111",
                filled_qty=2,
                fill_price=1.50,
            )
        ],
    )
    rec = _reconciler()
    rec._alert = MagicMock()
    rec._active_exit_order_exists = MagicMock(return_value={"status": "EXIT_REQUESTED"})
    rec._broker_open_exit_exists_for_contract = MagicMock(return_value=False)
    rec._execute_reconciler_close = MagicMock()
    pos = _position()
    pos.update(
        {
            "qty": 4,
            "quantity_remaining": 2,
            "pending_exit_local_order_id": "exit-current",
            "pending_exit_broker_order_id": "222",
        }
    )
    summary = _empty_summary(CLIENT)

    rec._handle_db_position_missing_at_broker(
        pos=pos,
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=4,
        entry_px=2.33,
        summary=summary,
    )

    rec._execute_reconciler_close.assert_not_called()
    assert summary["positions_alerted"] == 1
    assert any(
        "GHOST_CLOSE_BLOCKED_ACTIVE_EXIT" in call.args[0]
        for call in rec._alert.call_args_list
    )


def test_broker_flat_before_manual_close_adoption_does_not_mutate_position(
    monkeypatch,
):
    """The reconciler must hold while the slower external-fill scanner catches up."""
    _install_exit_rows(
        monkeypatch,
        [
            _exit_row(
                local_order_id="exit-old",
                broker_order_id="111",
                filled_qty=2,
                fill_price=1.50,
            )
        ],
    )
    rec = _reconciler()
    rec._alert = MagicMock()
    rec._active_exit_order_exists = MagicMock(return_value=None)
    rec._broker_open_exit_exists_for_contract = MagicMock(return_value=False)
    rec._execute_reconciler_close = MagicMock()
    rec._record_reconciler_rejection = MagicMock()
    pos = _position()
    pos.update(
        {
            "qty": 4,
            "quantity_remaining": 2,
            "pending_exit_local_order_id": "",
            "pending_exit_broker_order_id": "",
        }
    )
    before = copy.deepcopy(pos)
    summary = _empty_summary(CLIENT)

    for _ in range(3):
        rec._handle_db_position_missing_at_broker(
            pos=pos,
            contract=TARGET_CONTRACT,
            underlying="C",
            db_qty=4,
            entry_px=2.33,
            summary=summary,
        )

    assert pos == before
    rec._execute_reconciler_close.assert_not_called()
    rec._record_reconciler_rejection.assert_not_called()
    assert summary["positions_alerted"] == 3


def test_missing_id_exit_generation_mismatch_cannot_promote_current_order(
    monkeypatch,
):
    _install_exit_rows(
        monkeypatch,
        [
            _exit_row(
                local_order_id="exit-A",
                broker_order_id="111",
                filled_qty=2,
                fill_price=1.50,
            )
        ],
    )
    rec = _reconciler()
    rec._alert = MagicMock()
    rec._recover_missing_broker_id_exit = MagicMock(return_value=False)
    rec.osm.transition = MagicMock(return_value=True)
    summary = _empty_summary(CLIENT)
    order = {
        "local_order_id": "exit-B",
        "kind": "EXIT",
        "status": "EXIT_PARTIAL_FILL",
        "position_id": POSITION_ID,
        "contract": TARGET_CONTRACT,
        "underlying": "C",
        "execution_mode": MODE_LIVE,
        "qty": 2,
        "filled_qty": 0,
        "broker_order_id": None,
    }

    assert rec._resolve_missing_id_exit_truth(
        order,
        summary,
        reason="historical generation mismatch",
    ) is True

    assert rec.osm.transition.call_args.args[0] == "exit-B"
    assert rec.osm.transition.call_args.args[1] == "CANCELED"
    assert all(
        call.args[1] != "EXIT_FILLED"
        for call in rec.osm.transition.call_args_list
    )


@pytest.mark.parametrize(
    ("filled_ts", "position_entry_ts"),
    [
        (None, "2026-08-10T18:00:00+00:00"),
        ("not-a-timestamp", "2026-08-10T18:00:00+00:00"),
        ("2026-08-10T19:00:00", "2026-08-10T18:00:00+00:00"),
        ("2026-08-10T17:59:59+00:00", "2026-08-10T18:00:00+00:00"),
    ],
)
def test_invalid_or_pre_entry_exit_timestamp_cannot_authorize_close(
    monkeypatch,
    filled_ts: str | None,
    position_entry_ts: str,
):
    _install_exit_rows(
        monkeypatch,
        [
            _exit_row(
                filled_ts=filled_ts,
                position_entry_ts=position_entry_ts,
            )
        ],
    )
    rec = _reconciler()
    rec._alert = MagicMock()
    rec._active_exit_order_exists = MagicMock(return_value=None)
    rec._broker_open_exit_exists_for_contract = MagicMock(return_value=False)
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

    rec._execute_reconciler_close.assert_not_called()
    rec._record_reconciler_rejection.assert_not_called()
    assert summary["positions_alerted"] == 1


def _run_exact_fill_handler(
    *,
    quantity_remaining: int,
    filled_qty: int,
    status: str = "EXIT_FILLED",
    fill_price: float = 1.95,
):
    rec = _reconciler()
    rec._alert = MagicMock()
    rec._get_recent_exit_fill = MagicMock(
        return_value=_exit_row(
            filled_qty=filled_qty,
            status=status,
            fill_price=fill_price,
        )
    )
    rec._execute_reconciler_close = MagicMock()
    rec._record_reconciler_rejection = MagicMock()
    pos = _position()
    pos["quantity_remaining"] = quantity_remaining
    pos["qty"] = quantity_remaining
    summary = _empty_summary(CLIENT)

    rec._handle_db_position_missing_at_broker(
        pos=pos,
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=quantity_remaining,
        entry_px=2.33,
        summary=summary,
    )
    return rec, summary


@pytest.mark.parametrize("status", ["EXIT_FILLED", "EXIT_PARTIAL_FILL"])
def test_partial_exact_exit_fill_cannot_price_remaining_position(status: str):
    rec, summary = _run_exact_fill_handler(
        quantity_remaining=4,
        filled_qty=2,
        status=status,
        fill_price=4.79,
    )

    rec._execute_reconciler_close.assert_not_called()
    rec._record_reconciler_rejection.assert_not_called()
    assert summary["positions_alerted"] == 1
    assert any(
        "RECONCILER_EXIT_FILL_QTY_COVERAGE_UNPROVEN" in call.args[0]
        and "filled_qty=2" in call.args[0]
        and "quantity_remaining=4" in call.args[0]
        for call in rec._alert.call_args_list
    )


def test_exact_exit_fill_equal_to_remaining_quantity_can_authorize_close():
    rec, summary = _run_exact_fill_handler(
        quantity_remaining=2,
        filled_qty=2,
        fill_price=4.79,
    )

    rec._execute_reconciler_close.assert_called_once()
    assert rec._execute_reconciler_close.call_args.kwargs["exact_exit_fill_qty"] == 2
    rec._record_reconciler_rejection.assert_called_once()
    assert summary["positions_alerted"] == 0


def test_close_rechecks_remaining_quantity_before_mutating_positions(monkeypatch):
    rec = _reconciler()
    rec._alert = MagicMock()
    summary = _empty_summary(CLIENT)
    updates: list[tuple] = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=()):
            compact = " ".join(str(sql).split()).upper()
            if "FROM ORDERS" in compact:
                self._row = _exit_row(filled_qty=2, fill_price=4.79)
            elif "FOR UPDATE" in compact:
                self._row = {
                    "quantity_remaining": 4,
                    "qty": 4,
                    "pending_exit_local_order_id": "exit-local-1",
                    "pending_exit_broker_order_id": "TR-195",
                }
            elif compact.startswith("UPDATE POSITIONS"):
                updates.append((sql, params))

        def fetchone(self):
            return getattr(self, "_row", None)

    cursor = _Cursor()

    class _Connection:
        def __enter__(self):
            return cursor

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(db_mod, "conn", lambda: _Connection())
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())

    rec._execute_reconciler_close(
        pos=_position(),
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=4,
        entry_px=2.33,
        exit_px=4.79,
        close_confidence="HIGH",
        summary=summary,
        exact_exit_fill_qty=2,
        exact_exit_evidence=_exit_row(filled_qty=2, fill_price=4.79),
    )

    assert updates == []
    assert summary["positions_alerted"] == 1
    assert any(
        "position_remaining_changed_before_close" in call.args[0]
        for call in rec._alert.call_args_list
    )


def test_exact_exit_evidence_identity_reaches_position_engine_and_proof(monkeypatch):
    evidence = _exit_row(filled_qty=2, fill_price=4.79)
    updates: list[tuple] = []
    proof_calls: list[dict] = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=()):
            compact = " ".join(str(sql).split()).upper()
            if "FROM ORDERS" in compact:
                self._row = dict(evidence)
            elif "FOR UPDATE" in compact:
                self._row = {
                    "quantity_remaining": 2,
                    "qty": 2,
                    "pending_exit_local_order_id": "exit-local-1",
                    "pending_exit_broker_order_id": "TR-195",
                }
            elif compact.startswith("UPDATE POSITIONS"):
                updates.append((sql, params))

        def fetchone(self):
            return getattr(self, "_row", None)

    cursor = _Cursor()

    class _Connection:
        def __enter__(self):
            return cursor

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(db_mod, "conn", lambda: _Connection())
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())

    class _Query:
        def __init__(self, supabase):
            self.supabase = supabase
            self.filters = []

        def select(self, *_args):
            return self

        def eq(self, key, value):
            self.filters.append((key, str(value)))
            return self

        def limit(self, *_args):
            return self

        def execute(self):
            rows = [
                row
                for row in self.supabase.proof_rows
                if all(str(row.get(key, "")) == value for key, value in self.filters)
            ]
            return type("_Result", (), {"data": rows})()

    class _Supabase:
        def __init__(self):
            self.proof_rows = []

        def table(self, *_args):
            return _Query(self)

    rec = _reconciler()
    rec.supabase_client = _Supabase()
    rec.exit_engine = MagicMock()
    summary = _empty_summary(CLIENT)

    monkeypatch.setattr(
        "ap.proof_taxonomy_guard.resolve_originating_entry_identity",
        lambda **_kwargs: SimpleNamespace(
            client_id=CLIENT,
            position_id=POSITION_ID,
            local_order_id="entry-local-1",
            execution_mode=MODE_LIVE,
        ),
    )

    from ap_proof_logger import APProofLogger

    def _capture_log_trade(_self, **kwargs):
        proof_calls.append(kwargs)
        rec.supabase_client.proof_rows.append(
            {
                "id": "proof-1",
                "client_email": CLIENT,
                "position_id": kwargs["position_id"],
                "local_order_id": kwargs["local_order_id"],
                "execution_mode": kwargs["execution_mode"],
                "entry_option_price": kwargs["entry_option_price"],
                "exit_option_price": kwargs["exit_option_price"],
                "contracts": kwargs["contracts"],
                "option_pnl_pct": kwargs["option_pnl_pct"],
                "win": kwargs["win"],
                "exit_local_order_id": kwargs["exit_local_order_id"],
                "broker_exit_order_id": kwargs["broker_exit_order_id"],
                "broker_exit_fill_ts": kwargs["broker_exit_fill_ts"].isoformat(),
                "broker_exit_filled_qty": kwargs["broker_exit_filled_qty"],
            }
        )
        return {"_proof_persisted": True}

    monkeypatch.setattr(APProofLogger, "log_trade", _capture_log_trade)

    rec._execute_reconciler_close(
        pos=_position(),
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=2,
        entry_px=2.33,
        exit_px=4.79,
        close_confidence="HIGH",
        summary=summary,
        exact_exit_fill_qty=2,
        exact_exit_evidence=evidence,
    )

    assert updates
    assert len(proof_calls) == 1
    proof = proof_calls[0]
    assert proof["position_id"] == POSITION_ID
    assert proof["local_order_id"] == "entry-local-1"
    assert proof["exit_local_order_id"] == "exit-local-1"
    assert proof["broker_exit_order_id"] == "TR-195"
    assert proof["broker_exit_fill_ts"].isoformat() == evidence["filled_ts"]
    assert proof["broker_exit_filled_qty"] == 2
    rec.exit_engine.mark_position_closed.assert_called_once()
    mark_kwargs = rec.exit_engine.mark_position_closed.call_args.kwargs
    assert mark_kwargs["local_order_id"] == "exit-local-1"
    assert mark_kwargs["broker_order_id"] == "TR-195"
    assert mark_kwargs["broker_exit_order_id"] == "TR-195"
    assert mark_kwargs["broker_exit_fill_ts"].isoformat() == evidence["filled_ts"]
    assert mark_kwargs["broker_exit_filled_qty"] == 2


def _complete_durable_reconciler_proof() -> dict:
    return {
        "id": "proof-existing",
        "client_email": CLIENT,
        "position_id": POSITION_ID,
        "local_order_id": "entry-local-1",
        "execution_mode": MODE_LIVE,
        "entry_option_price": 2.33,
        "exit_option_price": 4.79,
        "contracts": 2,
        "option_pnl_pct": 105.58,
        "win": True,
        "exit_local_order_id": "exit-local-1",
        "broker_exit_order_id": "TR-195",
        "broker_exit_fill_ts": "2026-08-10T19:00:00+00:00",
        "broker_exit_filled_qty": 2,
    }


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        pytest.param("exit_local_order_id", None, id="missing-exit-local"),
        pytest.param("broker_exit_order_id", "OLD-BROKER", id="wrong-exit-broker"),
        pytest.param("broker_exit_fill_ts", None, id="missing-fill-timestamp"),
        pytest.param("broker_exit_filled_qty", 1, id="wrong-fill-quantity"),
        pytest.param("exit_option_price", 1.95, id="wrong-exit-economics"),
        pytest.param("option_pnl_pct", 0.0, id="wrong-pnl-economics"),
    ],
)
def test_existing_incomplete_or_mismatched_proof_is_hold_not_already_exists(
    monkeypatch, field, replacement
):
    state, rec = _restart_fixture(monkeypatch)
    existing = _complete_durable_reconciler_proof()
    if replacement is None:
        existing.pop(field)
    else:
        existing[field] = replacement
    rec.supabase_client.proof_rows.append(existing)

    result = rec._persist_exact_reconciler_proof(
        pos=dict(state.position),
        exact_exit_evidence=dict(state.exit),
        close_qty=2,
        entry_px=2.33,
        exit_px=4.79,
        close_confidence="HIGH",
    )

    assert result["success"] is False, field
    assert result["disposition"] == "EVIDENCE_UNPROVEN", field
    assert "existing_proof_exit_provenance_unconfirmed" in result["reason"]
    assert rec.supabase_client.insert_payloads == []


def test_existing_complete_proof_is_already_exists_only_after_exact_confirmation(monkeypatch):
    state, rec = _restart_fixture(monkeypatch)
    rec.supabase_client.proof_rows.append(_complete_durable_reconciler_proof())

    result = rec._persist_exact_reconciler_proof(
        pos=dict(state.position),
        exact_exit_evidence=dict(state.exit),
        close_qty=2,
        entry_px=2.33,
        exit_px=4.79,
        close_confidence="HIGH",
    )

    assert result["success"] is True
    assert result["disposition"] == "ALREADY_EXISTS"
    assert rec.supabase_client.insert_payloads == []


def test_reconciler_skips_fallback_proof_after_canonical_callback_persists(monkeypatch):
    evidence = _exit_row(filled_qty=2, fill_price=4.79)
    updates: list[tuple] = []
    fallback_calls: list[tuple] = []

    class _Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=()):
            compact = " ".join(str(sql).split()).upper()
            if "FROM ORDERS" in compact:
                self._row = dict(evidence)
            elif "FOR UPDATE" in compact:
                self._row = {
                    "quantity_remaining": 2,
                    "qty": 2,
                    "pending_exit_local_order_id": "exit-local-1",
                    "pending_exit_broker_order_id": "TR-195",
                }
            elif compact.startswith("UPDATE POSITIONS"):
                updates.append((sql, params))

        def fetchone(self):
            return getattr(self, "_row", None)

    cursor = _Cursor()

    class _Connection:
        def __enter__(self):
            return cursor

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(db_mod, "conn", lambda: _Connection())
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())

    class _Supabase:
        def table(self, *args):
            fallback_calls.append(args)
            raise AssertionError("fallback proof writer must not run")

    rec = _reconciler()
    rec.supabase_client = _Supabase()
    rec.exit_engine = MagicMock()
    rec.exit_engine.mark_position_closed.return_value = True
    summary = _empty_summary(CLIENT)

    rec._execute_reconciler_close(
        pos=_position(),
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=2,
        entry_px=2.33,
        exit_px=4.79,
        close_confidence="HIGH",
        summary=summary,
        exact_exit_fill_qty=2,
        exact_exit_evidence=evidence,
    )

    assert updates
    assert fallback_calls == []
    assert summary.get("proof_write_failures", 0) == 0


def test_real_exit_engine_callback_carries_exit_provenance_without_overwriting_entry_id():
    """Exercise the production ExitEngine -> ExecutionCore callback seam."""
    from ap_execution_core import APExecutionCore
    from ap_exit_engine import APExitEngine

    core = APExecutionCore.__new__(APExecutionCore)
    core.proof = MagicMock()
    core.proof.log_trade.return_value = {"_proof_persisted": True}
    core.feedback = MagicMock()
    core.store = MagicMock()
    core.shadow = MagicMock()

    engine = APExitEngine.__new__(APExitEngine)
    engine._lock = threading.RLock()
    engine._positions = []
    engine._positions_by_id = {}
    engine._email = CLIENT
    engine._emit_exit_event = MagicMock()
    engine.on_exit_fill_confirmed = core._finalize_proof

    pos = SimpleNamespace(
        position_id=POSITION_ID,
        ticker="C",
        current_underlying=100.0,
        closed=False,
        close_reason="",
        quantity_remaining=2,
        _submit_generation=0,
        current_option_price=2.33,
        exit_in_flight=True,
        pending_exit_reason="",
        pending_exit_action="",
        pending_exit_qty=2,
        pending_exit_filled_qty=2,
        pending_scale_counted=False,
        pending_exit_local_order_id="exit-local-1",
        pending_exit_broker_order_id="TR-195",
        last_applied_exit_local_order_id="",
        last_applied_exit_broker_order_id="",
        last_exit_signal_ts=None,
        last_callback_identity_missing=False,
        last_callback_identity_missing_ts=None,
        exit_identity_quarantine=False,
        pending_exit_replace_allowed=False,
        pending_exit_replace_reason="",
        pending_exit_replace_allowed_ts=None,
        _proof_staged={
            "ticker": "C",
            "pattern": "",
            "side": "CALL",
            "timeframe": "1d",
            "score": 0,
            "tier": "A",
            "context_score": 0,
            "setup_status": "reconciler_auto_close",
            "entry_option_price": 2.33,
            "exit_option_price": 4.00,
            "underlying_entry": 100.0,
            "underlying_exit": 100.0,
            "contracts": 2,
            "exit_reason": "RECONCILER_AUTO_CLOSE",
            "opt_pnl": 0.0,
            "spread_pct": 0.0,
            "chain_grade": "",
            "opened_at": "2026-08-10T18:00:00+00:00",
            "synthetic_entry": False,
            "position_id": POSITION_ID,
            "local_order_id": "entry-local-1",
            "signal": {},
            "paper": False,
        },
        _proof_finalized=False,
    )
    engine._positions_by_id[POSITION_ID] = pos

    fill_ts = datetime(2026, 8, 10, 19, 0, tzinfo=timezone.utc)
    result = engine.mark_position_closed(
        POSITION_ID,
        reason="reconciler_auto_close",
        qty_filled=2,
        fill_price=4.79,
        local_order_id="exit-local-1",
        broker_order_id="TR-195",
        broker_exit_order_id="TR-195",
        broker_exit_fill_ts=fill_ts,
        broker_exit_filled_qty=2,
        reconciled=True,
    )

    assert result is True
    proof = core.proof.log_trade.call_args.kwargs
    assert proof["local_order_id"] == "entry-local-1"
    assert proof["exit_local_order_id"] == "exit-local-1"
    assert proof["broker_exit_order_id"] == "TR-195"
    assert proof["broker_exit_fill_ts"] == fill_ts
    assert proof["broker_exit_filled_qty"] == 2


class _ProofSchemaFallbackSupabase:
    def __init__(self, *, missing_field: str | None = None):
        self.attempts: list[dict] = []
        self.missing_field = missing_field

    def table(self, name):
        assert name == "proof_trades"
        return self

    def insert(self, payload):
        self.attempts.append(dict(payload))
        return self

    def execute(self):
        attempt = len(self.attempts)
        if attempt <= 3:
            raise RuntimeError("column schema drift")
        if self.missing_field and self.missing_field in self.attempts[-1]:
            raise RuntimeError(f'column "{self.missing_field}" does not exist')
        return SimpleNamespace(data=[])


def _proof_log_kwargs(*, fill_ts: datetime) -> dict:
    return {
        "ticker": "C",
        "pattern": "test",
        "side": "CALL",
        "timeframe": "1d",
        "score": 80,
        "tier": "A",
        "context_score": 70,
        "setup_status": "broker_exit_fill",
        "entry_trigger": 400.0,
        "entry_option_price": 2.33,
        "exit_option_price": 4.79,
        "underlying_entry": 400.0,
        "underlying_exit": 405.0,
        "contracts": 2,
        "exit_reason": "RECONCILED_CLOSE",
        "option_pnl_pct": 105.58,
        "underlying_pnl_pct": 1.25,
        "win": True,
        "position_id": POSITION_ID,
        "local_order_id": "ENTRY",
        "exit_local_order_id": "EXIT",
        "broker_exit_order_id": "BROKER-EXIT",
        "broker_exit_fill_ts": fill_ts,
        "broker_exit_filled_qty": 2,
    }


def _stub_proof_taxonomy_guard(monkeypatch):
    monkeypatch.setattr(
        "ap.proof_taxonomy_guard.resolve_originating_entry_identity",
        lambda **_kwargs: SimpleNamespace(
            local_order_id="ENTRY",
            position_id=POSITION_ID,
            execution_mode=MODE_LIVE,
            synthetic_entry=False,
        ),
    )
    monkeypatch.setattr("ap.proof_taxonomy_guard._lifecycle_proof_stamp", lambda _identity: {})
    monkeypatch.setattr("ap.proof_taxonomy_guard._persist_stamp", lambda *_args: None)


def test_stage4_fallback_preserves_broker_exit_provenance(monkeypatch):
    from ap_proof_logger import APProofLogger

    fill_ts = datetime(2026, 8, 10, 19, 0, tzinfo=timezone.utc)
    supabase = _ProofSchemaFallbackSupabase()
    _stub_proof_taxonomy_guard(monkeypatch)
    monkeypatch.setattr("ap_proof_logger._resolve_entry_execution_mode", lambda *_args: MODE_LIVE)

    result = APProofLogger(
        supabase_client=supabase,
        client_email=CLIENT,
        mode=MODE_LIVE,
    ).log_trade(**_proof_log_kwargs(fill_ts=fill_ts))

    assert result["_proof_persisted"] is True
    assert len(supabase.attempts) == 4
    payload = supabase.attempts[-1]
    assert payload["local_order_id"] == "ENTRY"
    assert payload["exit_local_order_id"] == "EXIT"
    assert payload["broker_exit_order_id"] == "BROKER-EXIT"
    assert payload["broker_exit_fill_ts"] == fill_ts.isoformat()
    assert payload["broker_exit_filled_qty"] == 2


@pytest.mark.parametrize(
    "missing_field",
    ("broker_exit_order_id", "broker_exit_fill_ts", "broker_exit_filled_qty"),
)
def test_stage4_missing_required_broker_provenance_does_not_claim_persistence(
    monkeypatch,
    missing_field,
):
    from ap_proof_logger import APProofLogger

    supabase = _ProofSchemaFallbackSupabase(missing_field=missing_field)
    _stub_proof_taxonomy_guard(monkeypatch)
    monkeypatch.setattr("ap_proof_logger._resolve_entry_execution_mode", lambda *_args: MODE_LIVE)

    result = APProofLogger(
        supabase_client=supabase,
        client_email=CLIENT,
        mode=MODE_LIVE,
    ).log_trade(
        **_proof_log_kwargs(fill_ts=datetime(2026, 8, 10, 19, 0, tzinfo=timezone.utc))
    )

    assert len(supabase.attempts) == 4
    assert result["_proof_persisted"] is False


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
    assert close_kwargs["exact_exit_fill_qty"] == 9
    rec._record_reconciler_rejection.assert_called_once()


@pytest.mark.parametrize("missing_identity", [
    "pending_exit_local_order_id",
    "pending_exit_broker_order_id",
])
def test_reconciler_close_requires_current_exit_identity_pair(
    monkeypatch, missing_identity: str
):
    _install_exit_rows(monkeypatch, [_exit_row()])
    rec = _reconciler()
    rec._alert = MagicMock()
    rec._execute_reconciler_close = MagicMock()
    pos = _position()
    pos[missing_identity] = ""
    summary = _empty_summary(CLIENT)

    rec._handle_db_position_missing_at_broker(
        pos=pos,
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=9,
        entry_px=2.33,
        summary=summary,
    )

    rec._execute_reconciler_close.assert_not_called()
    assert summary["positions_alerted"] == 1
    assert any(
        "current_exit_local_and_broker_identity_pair_missing" in call.args[0]
        for call in rec._alert.call_args_list
    )


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
                "id": OLD_BROKER_ORDER_ID,
                "symbol": OLD_CONTRACT,
                "side": "sell_to_close",
                "status": "filled",
                "filled_quantity": 21,
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


class _RestartDbState:
    """Production-shaped durable state for the close-then-restart regression."""

    def __init__(self):
        self.position = _position()
        self.position.update(
            {
                "qty": 2,
                "quantity_remaining": 2,
                "status": "OPEN",
                "close_source": "",
                "exit_ts": None,
            }
        )
        self.entry = {
            "client_id": CLIENT,
            "local_order_id": "entry-local-1",
            "broker_order_id": "ENTRY-BROKER-1",
            "position_id": POSITION_ID,
            "contract": TARGET_CONTRACT,
            "kind": "ENTRY",
            "status": "FILLED",
            "execution_mode": MODE_LIVE,
            "filled_qty": 2,
            "fill_price": 2.33,
            "filled_ts": "2026-08-10T18:00:00+00:00",
        }
        self.exit = _exit_row(
            filled_qty=2,
            fill_price=4.79,
            position_entry_ts="2026-08-10T18:00:00+00:00",
        )
        self.position_updates: list[tuple] = []
        self.order_updates: list[tuple] = []


class _RestartCursor:
    def __init__(self, state: _RestartDbState):
        self.state = state
        self._one = None
        self._many: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, sql, params=()):
        compact = " ".join(str(sql).split())
        upper = compact.upper()
        self._one = None
        self._many = []

        if upper.startswith("UPDATE POSITIONS"):
            self.state.position_updates.append((sql, tuple(params)))
            if len(params) >= 10:
                (
                    status,
                    exit_ts,
                    exit_price,
                    realized_pnl,
                    realized_pnl_pct,
                    quantity_remaining,
                    close_source,
                    close_confidence,
                    position_id,
                    client_id,
                ) = params[:10]
                if (
                    str(position_id) == POSITION_ID
                    and str(client_id) == CLIENT
                ):
                    self.state.position.update(
                        {
                            "status": status,
                            "exit_ts": exit_ts,
                            "exit_price": exit_price,
                            "realized_pnl": realized_pnl,
                            "realized_pnl_pct": realized_pnl_pct,
                            "quantity_remaining": quantity_remaining,
                            "close_source": close_source,
                            "close_confidence": close_confidence,
                        }
                    )
            return self

        if upper.startswith("UPDATE ORDERS"):
            self.state.order_updates.append((sql, tuple(params)))
            return self

        if "FROM ORDERS O" in upper and "JOIN POSITIONS P" in upper:
            self._many = [dict(self.state.exit)]
            return self

        if "FROM ORDERS" in upper and "KIND = 'EXIT'" in upper:
            self._one = dict(self.state.exit)
            return self

        if "FROM ORDERS" in upper:
            # Both the taxonomy resolver and APProofLogger's mode resolver use
            # an exact originating ENTRY local-order lookup.
            requested_local = str(params[-1] if params else "").strip()
            if not requested_local or requested_local == self.state.entry["local_order_id"]:
                self._one = dict(self.state.entry)
            return self

        if "FROM POSITIONS" in upper and "FOR UPDATE" in upper:
            self._one = {
                key: self.state.position.get(key)
                for key in (
                    "quantity_remaining",
                    "qty",
                    "pending_exit_local_order_id",
                    "pending_exit_broker_order_id",
                )
            }
            return self

        if "FROM POSITIONS" in upper and "ID::TEXT" in upper and "SELECT P.*" not in upper:
            self._one = dict(self.state.position)
            return self

        if "SELECT P.* FROM POSITIONS P" in upper:
            if (
                str(self.state.position.get("status") or "").upper() == "CLOSED"
                and str(self.state.position.get("close_source") or "").upper()
                == "RECONCILER_AUTO_CLOSE"
            ):
                self._many = [dict(self.state.position)]
            return self

    def fetchone(self):
        return self._one

    def fetchall(self):
        return list(self._many)


class _RestartConnection:
    def __init__(self, state: _RestartDbState):
        self.cursor = _RestartCursor(state)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_args):
        return False


class _RestartSupabaseTable:
    def __init__(self, client):
        self.client = client
        self.filters: list[tuple[str, str]] = []
        self.insert_row: dict | None = None

    def select(self, *_args):
        return self

    def eq(self, key, value):
        self.filters.append((key, str(value)))
        return self

    def limit(self, value):
        self.limit_value = int(value)
        return self

    def insert(self, row):
        self.insert_row = dict(row)
        return self

    def execute(self):
        if self.insert_row is not None:
            row = dict(self.insert_row)
            row.setdefault("id", f"proof-{len(self.client.proof_rows) + 1}")
            self.client.proof_rows.append(row)
            self.client.insert_payloads.append(row)
            return SimpleNamespace(data=[dict(row)])
        rows = [
            dict(row)
            for row in self.client.proof_rows
            if all(str(row.get(key, "")) == value for key, value in self.filters)
        ]
        return SimpleNamespace(data=rows[: getattr(self, "limit_value", len(rows))])


class _RestartSupabase:
    def __init__(self):
        self.proof_rows: list[dict] = []
        self.insert_payloads: list[dict] = []

    def table(self, _name):
        return _RestartSupabaseTable(self)


class _UnavailableSupabase:
    def table(self, _name):
        raise RuntimeError("supabase unavailable before restart")


def _install_restart_db(monkeypatch, state: _RestartDbState):
    monkeypatch.setattr(db_mod, "conn", lambda: _RestartConnection(state))
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())


def _restart_fixture(monkeypatch):
    state = _RestartDbState()
    _install_restart_db(monkeypatch, state)
    rec = _reconciler()
    rec.supabase_client = _RestartSupabase()
    return state, rec


@pytest.mark.parametrize(
    "mutate_evidence",
    [
        pytest.param(lambda evidence: evidence.update(filled_qty=True), id="bool-quantity"),
        pytest.param(lambda evidence: evidence.update(fill_price=True), id="bool-price"),
        pytest.param(
            lambda evidence: evidence.update(filled_ts="2026-08-10T17:00:00+00:00"),
            id="pre-entry-timestamp",
        ),
    ],
)
def test_close_mutation_fence_holds_on_coercion_or_timestamp_truth(
    monkeypatch, mutate_evidence
):
    state, rec = _restart_fixture(monkeypatch)
    rec._alert = MagicMock()
    evidence = dict(state.exit)
    mutate_evidence(evidence)
    summary = _empty_summary(CLIENT)

    rec._execute_reconciler_close(
        pos=dict(state.position),
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=2,
        entry_px=2.33,
        exit_px=4.79,
        close_confidence="HIGH",
        summary=summary,
        exact_exit_fill_qty=2,
        exact_exit_evidence=evidence,
    )

    assert state.position["status"] == "OPEN"
    assert state.order_updates == []
    assert summary["positions_alerted"] == 1


def test_reconciler_close_then_restart_repairs_exact_proof_once(monkeypatch):
    """A process death after CLOSED commit must be repairable and idempotent."""
    state, first = _restart_fixture(monkeypatch)
    first.supabase_client = _UnavailableSupabase()
    first_summary = _empty_summary(CLIENT)
    position_before = dict(state.position)
    entry_before = dict(state.entry)
    exit_before = dict(state.exit)

    # Exact EXIT evidence authorizes the durable position close.  The unavailable
    # proof sink simulates the process dying before proof persistence completes.
    first._execute_reconciler_close(
        pos=dict(state.position),
        contract=TARGET_CONTRACT,
        underlying="C",
        db_qty=2,
        entry_px=2.33,
        exit_px=4.79,
        close_confidence="HIGH",
        summary=first_summary,
        exact_exit_fill_qty=2,
        exact_exit_evidence=dict(state.exit),
    )
    assert state.position["status"] == "CLOSED"
    assert state.position["close_source"] == "RECONCILER_AUTO_CLOSE"
    assert first_summary["proof_write_failures"] == 1

    # Restart with a fresh reconciler instance and a now-available proof sink.
    second = _reconciler()
    proof_sink = _RestartSupabase()
    second.supabase_client = proof_sink
    summary = _empty_summary(CLIENT)
    second._repair_missing_reconciler_proofs(summary)

    assert summary["reconciler_proof_repair_persisted"] == 1
    assert len(proof_sink.proof_rows) == 1
    proof = proof_sink.proof_rows[0]
    assert proof["position_id"] == POSITION_ID
    assert proof["local_order_id"] == "entry-local-1"
    assert proof["exit_local_order_id"] == "exit-local-1"
    assert proof["broker_exit_order_id"] == "TR-195"
    assert proof["broker_exit_fill_ts"] == "2026-08-10T19:00:00+00:00"
    assert proof["broker_exit_filled_qty"] == 2

    # The repair is proof-only: position remains closed, order rows are untouched,
    # and no broker mutation path is reachable.
    assert state.position["status"] == "CLOSED"
    assert state.position["quantity_remaining"] == 0
    assert state.entry == entry_before
    assert state.exit == exit_before
    assert state.order_updates == []
    assert first.broker.method_calls == []
    assert second.broker.method_calls == []

    # A second restart pass confirms the exact existing row and does not insert.
    second_summary = _empty_summary(CLIENT)
    second._repair_missing_reconciler_proofs(second_summary)
    assert second_summary["reconciler_proof_repair_already_exists"] == 1
    assert second_summary["reconciler_proof_repair_persisted"] == 0
    assert len(proof_sink.proof_rows) == 1
    assert state.position["status"] == "CLOSED"
    assert dict(state.position)["close_source"] == "RECONCILER_AUTO_CLOSE"
    assert position_before["status"] == "OPEN"


def test_restart_repair_keyset_paginates_past_first_fifty_rows(monkeypatch):
    """A full page of already-seen/held rows cannot starve older candidates."""
    rows = []
    for index in range(51):
        row = _position()
        row.update(
            {
                "id": f"position-{index:03d}",
                "status": "CLOSED",
                "close_source": "RECONCILER_AUTO_CLOSE",
                "exit_ts": f"2026-08-10T19:{index:02d}:00+00:00",
            }
        )
        rows.append(row)

    class _PagingCursor:
        def __init__(self):
            self.calls: list[tuple] = []
            self._many: list[dict] = []

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params=()):
            compact = " ".join(str(sql).split()).upper()
            if "SELECT P.* FROM POSITIONS P" not in compact:
                self._many = []
                return
            params = tuple(params)
            self.calls.append(params)
            candidates = [dict(row) for row in rows if row["client_id"] == params[0]]
            if len(params) == 4:
                cursor_ts, _same_ts, cursor_id = params[1:]
                candidates = [
                    row
                    for row in candidates
                    if row["exit_ts"] < cursor_ts
                    or (row["exit_ts"] == cursor_ts and row["id"] < cursor_id)
                ]
            elif len(params) == 2:
                cursor_id = params[1]
                candidates = [row for row in candidates if row["exit_ts"] is None and row["id"] < cursor_id]
            candidates.sort(key=lambda row: (row["exit_ts"] is not None, row["exit_ts"] or "", row["id"]), reverse=True)
            self._many = candidates[:50]

        def fetchall(self):
            return list(self._many)

    cursor = _PagingCursor()

    class _PagingConnection:
        def __enter__(self):
            return cursor

        def __exit__(self, *_args):
            return False

    monkeypatch.setattr(db_mod, "conn", lambda: _PagingConnection())
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())
    rec = _reconciler()
    rec._get_recent_exit_fill = MagicMock(return_value=None)
    rec._alert = MagicMock()

    first_summary = _empty_summary(CLIENT)
    rec._repair_missing_reconciler_proofs(first_summary)
    second_summary = _empty_summary(CLIENT)
    rec._repair_missing_reconciler_proofs(second_summary)

    assert len(cursor.calls) == 2
    assert cursor.calls[0] == (CLIENT,)
    assert len(cursor.calls[1]) == 4
    assert cursor.calls[1][3] == "position-001"
    assert first_summary["reconciler_proof_repair_candidates"] == 50
    assert second_summary["reconciler_proof_repair_candidates"] == 1
    assert first_summary["reconciler_proof_repair_failures"] == 50
    assert second_summary["reconciler_proof_repair_failures"] == 1
    assert rec._reconciler_proof_repair_cursor is None


@pytest.mark.parametrize(
    ("case", "mutate_position", "mutate_exit"),
    [
        ("wrong_execution_mode", lambda pos: pos.update(execution_mode=MODE_PAPER), lambda row: None),
        ("wrong_position", lambda pos: None, lambda row: row.update(position_id="other-position")),
        ("wrong_contract", lambda pos: None, lambda row: row.update(contract=OLD_CONTRACT)),
        ("wrong_exit_local", lambda pos: None, lambda row: row.update(local_order_id="old-exit")),
        ("wrong_exit_broker", lambda pos: None, lambda row: row.update(broker_order_id="old-broker")),
        ("partial_status", lambda pos: None, lambda row: row.update(status="EXIT_PARTIAL_FILL")),
        ("malformed_quantity", lambda pos: None, lambda row: row.update(filled_qty=True)),
        ("malformed_price", lambda pos: None, lambda row: row.update(fill_price=float("nan"))),
        ("missing_timestamp", lambda pos: None, lambda row: row.update(filled_ts=None)),
        ("naive_timestamp", lambda pos: None, lambda row: row.update(filled_ts="2026-08-10T19:00:00")),
        ("pre_entry_timestamp", lambda pos: None, lambda row: row.update(filled_ts="2026-08-10T17:00:00+00:00")),
    ],
)
def test_restart_repair_holds_on_malformed_or_mismatched_truth(
    monkeypatch,
    case,
    mutate_position,
    mutate_exit,
):
    state, rec = _restart_fixture(monkeypatch)
    state.position.update(status="CLOSED", close_source="RECONCILER_AUTO_CLOSE", quantity_remaining=0)
    mutate_position(state.position)
    bad_exit = dict(state.exit)
    mutate_exit(bad_exit)
    rec._get_recent_exit_fill = MagicMock(return_value=bad_exit)
    rec._alert = MagicMock()
    summary = _empty_summary(CLIENT)

    rec._repair_missing_reconciler_proofs(summary)

    assert summary["reconciler_proof_repair_failures"] == 1, case
    assert rec.supabase_client.proof_rows == []
    assert state.position["status"] == "CLOSED"
    assert state.order_updates == []
    assert rec.broker.method_calls == []
    assert any("RECONCILER_PROOF_REPAIR_HOLD" in call.args[0] for call in rec._alert.call_args_list)


def test_restart_repair_holds_on_ambiguous_exit_or_missing_entry_identity(monkeypatch):
    state, rec = _restart_fixture(monkeypatch)
    state.position.update(status="CLOSED", close_source="RECONCILER_AUTO_CLOSE", quantity_remaining=0)
    rec._get_recent_exit_fill = MagicMock(return_value=None)
    rec._alert = MagicMock()
    summary = _empty_summary(CLIENT)

    rec._repair_missing_reconciler_proofs(summary)
    assert rec.supabase_client.proof_rows == []
    assert summary["reconciler_proof_repair_failures"] == 1

    # Exact EXIT truth may be present, but missing originating ENTRY identity is
    # still a HOLD and cannot be promoted from the runtime LIVE mode.
    rec._get_recent_exit_fill = MagicMock(return_value=dict(state.exit))
    monkeypatch.setattr(
        "ap.proof_taxonomy_guard.resolve_originating_entry_identity",
        lambda **_kwargs: None,
    )
    summary = _empty_summary(CLIENT)
    rec._repair_missing_reconciler_proofs(summary)
    assert rec.supabase_client.proof_rows == []
    assert summary["reconciler_proof_repair_failures"] == 1
    assert rec.broker.method_calls == []


def test_unknown_origin_mode_stays_quarantined_and_never_becomes_live(monkeypatch):
    state, rec = _restart_fixture(monkeypatch)
    state.position.update(status="CLOSED", close_source="RECONCILER_AUTO_CLOSE", quantity_remaining=0)
    state.entry["execution_mode"] = ""
    rec._get_recent_exit_fill = MagicMock(return_value=dict(state.exit))

    summary = _empty_summary(CLIENT)
    rec._repair_missing_reconciler_proofs(summary)

    assert len(rec.supabase_client.proof_rows) == 1
    assert rec.supabase_client.proof_rows[0]["execution_mode"] == "unknown"
    assert rec.supabase_client.proof_rows[0]["execution_mode"] != MODE_LIVE
    assert summary["reconciler_proof_repair_persisted"] == 1
