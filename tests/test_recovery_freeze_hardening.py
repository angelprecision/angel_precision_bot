from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from unittest.mock import MagicMock, patch


def _fake_reconciler_module():
    mod = types.ModuleType("ap_reconciler")
    mod.BROKER_FILLED = frozenset({"filled", "partially_filled"})
    mod.BROKER_TERMINAL = frozenset({"canceled", "cancelled", "rejected", "expired"})
    mod.BROKER_TO_OSM = {
        "filled": "FILLED",
        "partially_filled": "PARTIAL_FILL",
        "canceled": "CANCELED",
        "cancelled": "CANCELED",
        "rejected": "REJECTED",
        "expired": "EXPIRED",
    }
    return mod


def _fake_db_for_orders(orders):
    mod = types.ModuleType("ap.db")
    mod.get_open_orders_for_reconcile = lambda client_id=None: orders
    mod.run_with_retry = lambda fn, *args, **kwargs: fn()
    return mod


def _make_recovery():
    sys.modules.pop("ap_recovery", None)
    import ap_recovery

    rec = ap_recovery.APStartupRecovery(
        client_id=" Trader@Example.COM ",
        broker=MagicMock(),
        osm=MagicMock(),
        pm=MagicMock(),
        master_control=MagicMock(),
    )
    return rec, ap_recovery


def test_recovery_normalizes_client_id():
    rec, _ = _make_recovery()
    assert rec.client_id == "trader@example.com"


def test_recovery_does_not_transition_entry_filled_with_zero_qty():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {
        "status": "filled",
        "avg_fill_price": "1.25",
    }
    result = {"errors": []}
    order = {
        "kind": "ENTRY",
        "local_order_id": "entry-1",
        "broker_order_id": "broker-1",
        "status": "ACKNOWLEDGED",
        "contract": "SPY260626C00500000",
    }

    with patch.dict(
        sys.modules,
        {
            "ap.db": _fake_db_for_orders([order]),
            "ap_reconciler": _fake_reconciler_module(),
        },
    ):
        rec._verify_pending_entries(result)

    rec.osm.transition.assert_not_called()
    assert any("RECOVERY_FILL_TRUTH_MISSING" in err for err in result["errors"])


def test_recovery_does_not_transition_entry_filled_with_zero_price():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {
        "status": "filled",
        "exec_quantity": "1",
    }
    result = {"errors": []}
    order = {
        "kind": "ENTRY",
        "local_order_id": "entry-2",
        "broker_order_id": "broker-2",
        "status": "ACKNOWLEDGED",
        "contract": "SPY260626C00500000",
    }

    with patch.dict(
        sys.modules,
        {
            "ap.db": _fake_db_for_orders([order]),
            "ap_reconciler": _fake_reconciler_module(),
        },
    ):
        rec._verify_pending_entries(result)

    rec.osm.transition.assert_not_called()
    assert any("RECOVERY_FILL_TRUTH_MISSING" in err for err in result["errors"])


def test_recovery_terminal_rejected_can_transition_without_fill_price():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {"status": "rejected"}
    rec.osm.transition.return_value = True
    result = {"errors": [], "entries_corrected": 0}
    order = {
        "kind": "ENTRY",
        "local_order_id": "entry-3",
        "broker_order_id": "broker-3",
        "status": "ACKNOWLEDGED",
        "contract": "SPY260626C00500000",
    }

    with patch.dict(
        sys.modules,
        {
            "ap.db": _fake_db_for_orders([order]),
            "ap_reconciler": _fake_reconciler_module(),
        },
    ):
        rec._verify_pending_entries(result)

    rec.osm.transition.assert_called_once()
    args, kwargs = rec.osm.transition.call_args
    assert args[:2] == ("entry-3", "REJECTED")
    assert "filled_qty" not in kwargs
    assert "fill_price" not in kwargs
    assert result["entries_corrected"] == 1


def test_recovery_filled_with_qty_and_price_transitions():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {
        "status": "filled",
        "exec_quantity": "2",
        "avg_fill_price": "1.50",
    }
    rec.osm.transition.return_value = True
    result = {"errors": [], "entries_corrected": 0}
    order = {
        "kind": "ENTRY",
        "local_order_id": "entry-4",
        "broker_order_id": "broker-4",
        "status": "ACKNOWLEDGED",
        "contract": "SPY260626C00500000",
    }

    with patch.dict(
        sys.modules,
        {
            "ap.db": _fake_db_for_orders([order]),
            "ap_reconciler": _fake_reconciler_module(),
        },
    ):
        rec._verify_pending_entries(result)

    rec.osm.transition.assert_called_once_with(
        "entry-4",
        "FILLED",
        filled_qty=2,
        fill_price=1.50,
    )
    assert result["entries_corrected"] == 1


def test_recovery_does_not_transition_exit_filled_with_missing_fill_truth():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {"status": "filled", "exec_quantity": "1"}
    result = {"errors": [], "exits_reattached": 0}

    class FakeConn:
        rowcount = 0

        def execute(self, *args, **kwargs):
            return self

        def fetchone(self):
            return {
                "local_order_id": "exit-1",
                "broker_order_id": "broker-exit-1",
                "status": "EXIT_ACKNOWLEDGED",
            }

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [
        {"id": "pos-1", "underlying": "SPY", "status": "CLOSING"}
    ]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
        },
    ):
        rec._reattach_exit_protections(result)

    rec.osm.transition.assert_not_called()
    assert any("RECOVERY_EXIT_FILL_TRUTH_MISSING" in err for err in result["errors"])
    assert result["exits_reattached"] == 0


def test_recovery_reseed_dedup_does_not_default_call():
    rec, _ = _make_recovery()
    rec.mc._seen_signals = {}
    result = {"dedup_seeded": 0}

    class FakeConn:
        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return [{
                "signal_id": "sig-1",
                "ticker": "SPY",
                "direction": None,
                "side": None,
                "contract": None,
                "timeframe": "1d",
            }]

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    with patch.dict(sys.modules, {"ap.db": fake_db}):
        rec._reseed_dedup(result)

    assert f"sig:sig-1:{rec.client_id}" in rec.mc._seen_signals
    assert f"{rec.client_id}:SPY:CALL:1d" not in rec.mc._seen_signals
    assert f"{rec.client_id}:SPY:PUT:1d" not in rec.mc._seen_signals


def test_recovery_plan_rebuild_derives_put_from_occ_contract():
    rec, _ = _make_recovery()
    plan = rec._build_recovery_plan_from_order({
        "local_order_id": "entry-put",
        "signal_id": "sig-put",
        "symbol": "SPY",
        "contract": "SPY260626P00500000",
        "trigger_price": 500.0,
    })

    assert plan is not None
    assert plan.side == "PUT"
    assert plan.direction == "PUT"
    assert plan.metadata["recovery_side_source"] == "occ_contract"


def test_recovery_plan_rebuild_blocks_unresolved_side():
    rec, _ = _make_recovery()
    plan = rec._build_recovery_plan_from_order({
        "local_order_id": "entry-unknown",
        "signal_id": "sig-unknown",
        "symbol": "SPY",
        "trigger_price": 500.0,
    })

    assert plan is None


def test_recovery_restores_partial_active_and_remaining_qty_positions():
    rec, _ = _make_recovery()
    rec.pm.register_recovered_position = MagicMock()
    captured_sql = []

    rows = [
        {"id": "partial-1", "status": "PARTIAL", "quantity_remaining": 1, "contract": "SPY260626C00500000"},
        {"id": "active-1", "status": "ACTIVE", "qty": 1, "contract": "SPY260626P00500000"},
        {"id": "closed-with-qty", "status": "CLOSED", "quantity_remaining": 1, "contract": "QQQ260626C00400000"},
    ]

    class FakeConn:
        def execute(self, sql, params=()):
            captured_sql.append(sql)
            return self

        def fetchall(self):
            return rows

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    result = {"errors": [], "positions_recovered": 0}
    with patch.dict(sys.modules, {"ap.db": fake_db}):
        rec._recover_positions(result)

    assert rec.pm.register_recovered_position.call_count == 3
    assert result["positions_recovered"] == 3
    sql = "\n".join(captured_sql)
    assert "'PARTIAL'" in sql
    assert "'ACTIVE'" in sql
    assert "quantity_remaining" in sql


def test_recovery_buying_power_uses_avg_fill_qty_fallback():
    rec, _ = _make_recovery()
    rec._load_active_positions = MagicMock(return_value=[
        {"id": "pos-1", "avg_fill": 1.25, "quantity_remaining": 2},
        {"id": "pos-2", "entry_price": 0.50, "qty": 1},
    ])
    rec.mc._reserved_capital = 0.0
    rec.mc.reserved_capital = 0.0

    result = {}
    rec._recompute_buying_power(result)

    assert result["buying_power_reserved"] == 300.0
    assert rec.mc._reserved_capital == 300.0
    assert rec.mc.reserved_capital == 300.0
