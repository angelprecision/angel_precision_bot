"""PR #460: broker-flat reconciler must not fabricate EXIT economics.

The broker-vs-DB reconciler is an exposure/discrepancy observer. Exact external
EXIT selection, durable adoption, weighted-fill aggregation, and canonical
position/proof finalization remain owned by ``ap.manual_close_reconciliation``.
These tests exercise both sides of that seam and trap broker mutation methods.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

# ap package installation imports the DB guard during collection. The tests
# mock all DB boundaries and never connect, but the module still requires the
# repository's normal URL-shaped environment value.
if not os.getenv("DATABASE_URL"):
    os.environ["DATABASE_URL"] = "postgresql://test:test@127.0.0.1:5432/test"

from ap import manual_close_reconciliation as manual_mod
from ap_reconciler import APBrokerReconciler, _empty_summary


CLIENT = "client@example.com"
CONTRACT = "AAPL250102C00100000"
WRONG_CONTRACT = "AAPL250102C00110000"
ENTRY_TS = "2025-01-02T14:00:00+00:00"
DETECTED_AT = datetime(2025, 1, 2, 15, 0, tzinfo=timezone.utc)


def _position(**overrides) -> dict:
    row = {
        "id": "position-460",
        "client_id": CLIENT,
        "contract": CONTRACT,
        "underlying": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "avg_fill": 0.52,
        "entry_price": 0.52,
        "qty": 3,
        "quantity_remaining": 3,
        "entry_ts": ENTRY_TS,
        "opened_at": ENTRY_TS,
        "execution_mode": "live",
        "status": "OPEN",
        "exit_in_flight": False,
        "pending_exit_broker_order_id": None,
        "pending_exit_local_order_id": None,
        "local_order_id": "entry-460",
    }
    row.update(overrides)
    return row


def _broker_order(
    order_id: str = "broker-exit-1",
    *,
    contract: str = CONTRACT,
    side: str = "sell_to_close",
    status: str = "filled",
    qty: int = 3,
    price: float = 0.99,
    filled_at: datetime = datetime(2025, 1, 2, 14, 59, tzinfo=timezone.utc),
) -> dict:
    return {
        "id": order_id,
        "status": status,
        "side": side,
        "option_symbol": contract,
        "exec_quantity": qty,
        "avg_fill_price": price,
        "last_fill_date": filled_at.isoformat() if filled_at is not None else None,
    }


def _select(position: dict, orders: list[dict], *, bot_ids: set[str] | None = None,
            adopted_fills: list[dict] | None = None):
    return manual_mod.select_external_close_fills(
        orders=orders,
        position=position,
        bot_exit_order_ids=bot_ids or set(),
        adopted_fills=adopted_fills,
        detected_at=DETECTED_AT,
    )


def _make_reconciler(*, quote: float, active_exit: bool = False):
    alerts: list[str] = []
    rec = APBrokerReconciler(
        broker=MagicMock(),
        client_id=CLIENT,
        osm=MagicMock(),
        pm=MagicMock(),
        alert_fn=alerts.append,
        execution_mode="live",
    )
    rec._active_exit_order_exists = lambda **_: active_exit
    rec._broker_open_exit_exists_for_contract = lambda *_: False
    rec._mark_ghost_seen = lambda *_: True
    rec._get_current_option_price = MagicMock(return_value=quote)
    return rec, alerts


@pytest.mark.parametrize("quote", [0.55, 0.0])
def test_reconciler_three_pass_flat_is_hold_only(quote):
    """A quote or entry price can never become a reconciler close."""
    rec, alerts = _make_reconciler(quote=quote)
    summary = _empty_summary(CLIENT)

    rec._handle_db_position_missing_at_broker(
        pos=_position(),
        contract=CONTRACT,
        underlying="AAPL",
        db_qty=3,
        entry_px=0.52,
        summary=summary,
    )

    rec._get_current_option_price.assert_not_called()
    assert summary["positions_corrected"] == 0
    assert summary["reconciler_full_close_count"] == 0
    assert summary["reconciler_partial_close_preserved_count"] == 0
    assert "RECONCILER_BROKER_FLAT_EXIT_FILL_UNRESOLVED" in summary["errors"]
    assert any("no realized economics mutated" in message for message in alerts)


def test_canonical_manual_close_uses_broker_fill_not_current_quote(monkeypatch):
    """The existing canonical detector finalizes at the exact broker fill."""
    position = _position()
    broker_order = _broker_order(price=0.99, qty=3)
    finalized: list[dict] = []
    adopted: list[dict] = []
    mutations: list[str] = []

    class _ReadOnlyBroker:
        cfg = SimpleNamespace(account_id="ACCOUNT-460")

        def list_positions_authoritative(self):
            return []

        def _get(self, path):
            if "/orders" in path:
                return {"orders": {"order": [broker_order]}}
            raise AssertionError(f"unexpected broker read: {path}")

        def __getattr__(self, name):
            if name in {
                "place_order", "submit_order", "buy_option", "sell_option", "cancel_order",
            }:
                def _mutating(*args, **kwargs):
                    mutations.append(name)
                    raise AssertionError(f"broker mutation is forbidden: {name}")
                return _mutating
            raise AttributeError(name)

    class _PM:
        def close_position_from_exit_fill(self, **kwargs):
            finalized.append(kwargs)
            return True

    runner = SimpleNamespace(
        email=CLIENT,
        mode="LIVE",
        broker=_ReadOnlyBroker(),
        position_manager=_PM(),
        core=SimpleNamespace(exit_eng=SimpleNamespace(mark_position_closed=lambda *_: None)),
        _last_manual_close_check_ts=0.0,
    )

    monkeypatch.setattr(manual_mod, "MANUAL_CLOSE_INTERVAL_SEC", 0)
    monkeypatch.setattr(
        manual_mod,
        "load_manual_close_state",
        lambda client_id, mode: ([position], set(), {}),
    )
    monkeypatch.setattr(manual_mod, "load_terminal_recovery_candidates", lambda *_: [])

    def _adopt(**kwargs):
        adopted.append(kwargs)
        return True, "external_exit_adoption_complete"

    monkeypatch.setattr(manual_mod, "adopt_external_exit_fills", _adopt)
    monkeypatch.setattr(manual_mod, "_evict_exit_engine", lambda *args, **kwargs: None)

    manual_mod.detect_manual_closes(runner)

    assert len(finalized) == 1
    assert finalized[0]["exit_price"] == pytest.approx(0.99)
    assert finalized[0]["filled_qty"] == 3
    assert finalized[0]["broker_order_id"] == "broker-exit-1"
    assert len(adopted) == 1
    assert mutations == []


@pytest.mark.parametrize(
    ("order", "reason"),
    [
        (_broker_order(contract=WRONG_CONTRACT), "no_exact_external_filled_exit_order"),
        (_broker_order(side="buy_to_open"), "no_exact_external_filled_exit_order"),
        (_broker_order(qty=2), "external_fill_qty_ambiguous:2/3"),
        (_broker_order(price=float("nan")), "no_exact_external_filled_exit_order"),
        (_broker_order(filled_at=None), "no_exact_external_filled_exit_order"),
    ],
)
def test_non_exact_external_evidence_holds(order, reason):
    evidence, actual_reason = _select(_position(), [order])
    assert evidence is None
    assert actual_reason == reason


def test_weighted_multi_fill_preserves_all_broker_ids():
    orders = [
        _broker_order(
            "broker-exit-a",
            qty=1,
            price=0.90,
            filled_at=datetime(2025, 1, 2, 14, 58, tzinfo=timezone.utc),
        ),
        _broker_order(
            "broker-exit-b",
            qty=2,
            price=1.05,
            filled_at=datetime(2025, 1, 2, 14, 59, tzinfo=timezone.utc),
        ),
    ]
    evidence, reason = _select(_position(), orders)

    assert reason == "exact_external_broker_fill"
    assert evidence is not None
    assert evidence["fill_price"] == pytest.approx(1.00)
    assert evidence["filled_qty"] == 3
    assert evidence["broker_order_ids"] == ["broker-exit-a", "broker-exit-b"]


def test_bot_owned_exit_blocks_external_takeover():
    evidence, reason = _select(
        _position(),
        [_broker_order("bot-exit-1")],
        bot_ids={"bot-exit-1"},
    )
    assert evidence is None
    assert reason == "bot_owned_exit_order_present"


def test_previously_adopted_fill_is_not_re_adopted_and_remains_exact():
    fill = {
        "broker_order_id": "broker-exit-1",
        "filled_qty": 3,
        "fill_price": 0.99,
        "filled_at": datetime(2025, 1, 2, 14, 59, tzinfo=timezone.utc),
        "raw_status": "EXIT_FILLED",
        "raw_side": "sell_to_close",
        "db_contract": CONTRACT,
        "db_direction": "CALL",
    }
    evidence, reason = _select(
        _position(),
        [_broker_order("broker-exit-1", price=0.55)],
        adopted_fills=[fill],
    )

    assert reason == "exact_external_broker_fill"
    assert evidence is not None
    assert evidence["fills"] == []
    assert evidence["broker_order_ids"] == ["broker-exit-1"]
    assert evidence["fill_price"] == pytest.approx(0.99)


@pytest.mark.parametrize("runner_mode,position_mode", [("LIVE", "paper"), ("PAPER", "live")])
def test_manual_close_runner_mode_fence_blocks_cross_mode_evidence(monkeypatch, runner_mode, position_mode):
    position = _position(execution_mode=position_mode)
    finalizer = MagicMock(return_value=True)
    broker = MagicMock()
    runner = SimpleNamespace(
        email=CLIENT,
        mode=runner_mode,
        broker=broker,
        position_manager=SimpleNamespace(close_position_from_exit_fill=finalizer),
        core=SimpleNamespace(exit_eng=SimpleNamespace()),
        _last_manual_close_check_ts=0.0,
    )

    monkeypatch.setattr(manual_mod, "MANUAL_CLOSE_INTERVAL_SEC", 0)
    monkeypatch.setattr(manual_mod, "load_manual_close_state", lambda *_: ([position], set(), {}))
    monkeypatch.setattr(manual_mod, "load_terminal_recovery_candidates", lambda *_: [])

    manual_mod.detect_manual_closes(runner)

    finalizer.assert_not_called()
    broker.list_positions_authoritative.assert_not_called()


def test_active_exit_still_blocks_ghost_takeover():
    rec, alerts = _make_reconciler(quote=0.55, active_exit=True)
    summary = _empty_summary(CLIENT)

    rec._handle_db_position_missing_at_broker(
        pos=_position(),
        contract=CONTRACT,
        underlying="AAPL",
        db_qty=3,
        entry_px=0.52,
        summary=summary,
    )

    rec._get_current_option_price.assert_not_called()
    assert summary["errors"] == []
    assert any("GHOST_CLOSE_BLOCKED_ACTIVE_EXIT" in message for message in alerts)
