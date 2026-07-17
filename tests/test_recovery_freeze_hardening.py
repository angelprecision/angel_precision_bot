from __future__ import annotations

import sys
import types
from contextlib import contextmanager
from types import SimpleNamespace
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
    mod.get_open_orders_for_reconcile = (
        lambda client_id=None, execution_mode=None: orders
    )
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
        master_control=SimpleNamespace(mode="paper"),
    )
    return rec, ap_recovery


def test_recovery_normalizes_client_id():
    rec, _ = _make_recovery()
    assert rec.client_id == "trader@example.com"


def test_recovery_reconciles_stale_exit_claims_in_startup_pass():
    rec, ap_recovery = _make_recovery()
    reconciled = MagicMock(return_value={"claim_state": "RELEASED_NO_SUBMIT"})

    class _ClaimCursor:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchall(self):
            return [{"generation_key": "client|position|3|1"}]

    @contextmanager
    def _conn():
        yield _ClaimCursor()

    fake_db = types.ModuleType("ap.db")
    fake_db.conn = _conn
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()

    fake_guard = types.ModuleType("ap.exit_decision_idempotency_guard")
    fake_guard.reconcile_stale_exit_generation_claim = reconciled

    result = {"stale_exit_claims_reconciled": 0}
    with patch.dict(sys.modules, {"ap.db": fake_db, "ap.exit_decision_idempotency_guard": fake_guard}):
        rec._reconcile_stale_exit_generation_claims(result)

    reconciled.assert_called_once_with(
        "client|position|3|1",
        execution_core=rec.execution_core,
        osm=rec.osm,
    )
    assert result["stale_exit_claims_reconciled"] == 1


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

        def fetchall(self):
            return [{
                "local_order_id": "exit-1",
                "broker_order_id": "broker-exit-1",
                "status": "EXIT_ACKNOWLEDGED",
            }]

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
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)

    rec.osm.transition.assert_not_called()
    assert any("RECOVERY_EXIT_FILL_TRUTH_MISSING" in err for err in result["errors"])
    assert result["exits_reattached"] == 0
    assert live_exit_orders == []


def test_recovery_routes_downtime_exit_fill_through_canonical_reducer():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {
        "status": "filled",
        "exec_quantity": "2",
        "avg_fill_price": "1.75",
    }
    rec.osm.transition.return_value = True
    reconcile = MagicMock(return_value={"position_id": "pos-1"})
    result = {"errors": [], "exits_reattached": 0}

    rows = iter([
        {
            "local_order_id": "exit-2",
            "broker_order_id": "broker-exit-2",
            "status": "EXIT_ACKNOWLEDGED",
            "position_id": "pos-1",
        },
        {
            "client_id": rec.client_id,
            "local_order_id": "exit-2",
            "broker_order_id": "broker-exit-2",
            "status": "EXIT_FILLED",
            "filled_qty": 2,
            "fill_price": 1.75,
            "filled_ts": "2026-07-17T12:00:00+00:00",
            "kind": "EXIT",
        },
    ])

    class FakeConn:
        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return [next(rows)]

        def fetchone(self):
            return next(rows)

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [
        {"id": "pos-1", "underlying": "SPY", "status": "CLOSING"}
    ]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    fake_guard = types.ModuleType("ap.exit_fill_truth_guard")
    fake_guard.reconcile_confirmed_exit_fill = reconcile

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
            "ap.exit_fill_truth_guard": fake_guard,
        },
    ):
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)

    rec.osm.transition.assert_called_once_with(
        "exit-2",
        "EXIT_FILLED",
        filled_qty=2,
        fill_price=1.75,
    )
    reconcile.assert_called_once_with(
        {
            "client_id": rec.client_id,
            "local_order_id": "exit-2",
            "broker_order_id": "broker-exit-2",
            "status": "EXIT_FILLED",
            "filled_qty": 2,
            "fill_price": 1.75,
            "filled_ts": "2026-07-17T12:00:00+00:00",
            "kind": "EXIT",
        },
        {
            "status": "EXIT_FILLED",
            "broker_order_id": "broker-exit-2",
            "filled_qty": 2,
            "fill_price": 1.75,
            "filled_ts": "2026-07-17T12:00:00+00:00",
        },
    )
    assert result["errors"] == []
    assert result["exits_reattached"] == 0
    assert live_exit_orders == []


def test_recovery_pending_local_identity_wins_over_newer_unrelated_exit():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {"status": "open"}
    rec.exit_engine = MagicMock()
    reconcile = MagicMock()
    result = {"errors": [], "exits_reattached": 0}

    position = {
        "id": "pos-owned",
        "underlying": "SPY",
        "status": "CLOSING",
        "pending_exit_local_order_id": "exit-owned",
        "pending_exit_broker_order_id": "broker-owned",
    }
    owned_order = {
        "client_id": rec.client_id,
        "local_order_id": "exit-owned",
        "broker_order_id": "broker-owned",
        "status": "EXIT_ACKNOWLEDGED",
        "position_id": "pos-owned",
        "contract": "SPY260626C00500000",
    }

    class FakeConn:
        def __init__(self):
            self.params = None

        def execute(self, _sql, params=()):
            self.params = params
            return self

        def fetchall(self):
            if self.params == (rec.client_id, "exit-owned"):
                return [owned_order]
            raise AssertionError(f"unexpected params {self.params}")

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [position]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    fake_guard = types.ModuleType("ap.exit_fill_truth_guard")
    fake_guard.reconcile_confirmed_exit_fill = reconcile

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
            "ap.exit_fill_truth_guard": fake_guard,
        },
    ):
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)
        rec._reattach_live_exit_protections(result, live_exit_orders)

    rec.broker.get_order.assert_called_once_with("broker-owned")
    reconcile.assert_not_called()
    rec.exit_engine.seed_from_db.assert_called_once()
    assert live_exit_orders == [owned_order]
    assert result["errors"] == []


def test_recovery_multiple_fallback_active_exits_fail_closed():
    rec, _ = _make_recovery()
    rec.exit_engine = MagicMock()
    rec.osm.transition.return_value = True
    result = {"errors": [], "exits_reattached": 0}

    rows = [
        {
            "local_order_id": "exit-a",
            "broker_order_id": "broker-a",
            "status": "EXIT_ACKNOWLEDGED",
            "position_id": "pos-ambiguous",
            "contract": "SPY260626C00500000",
        },
        {
            "local_order_id": "exit-b",
            "broker_order_id": "broker-b",
            "status": "EXIT_SUBMITTED",
            "position_id": "pos-ambiguous",
            "contract": "SPY260626C00500000",
        },
    ]

    class FakeConn:
        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return rows

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [
        {"id": "pos-ambiguous", "underlying": "SPY", "status": "CLOSING"}
    ]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    reconcile = MagicMock()
    fake_guard = types.ModuleType("ap.exit_fill_truth_guard")
    fake_guard.reconcile_confirmed_exit_fill = reconcile

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
            "ap.exit_fill_truth_guard": fake_guard,
        },
    ):
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)
        rec._reattach_live_exit_protections(result, live_exit_orders)

    assert live_exit_orders == []
    rec.broker.get_order.assert_not_called()
    rec.osm.transition.assert_not_called()
    reconcile.assert_not_called()
    rec.exit_engine.seed_from_db.assert_not_called()
    assert any("RECOVERY_ACTIVE_EXIT_IDENTITY_AMBIGUOUS" in err for err in result["errors"])


def test_recovery_pending_broker_id_mismatch_fails_closed():
    rec, _ = _make_recovery()
    rec.exit_engine = MagicMock()
    result = {"errors": [], "exits_reattached": 0}

    order = {
        "client_id": rec.client_id,
        "local_order_id": "exit-owned",
        "broker_order_id": "broker-actual",
        "status": "EXIT_ACKNOWLEDGED",
        "position_id": "pos-mismatch",
        "contract": "SPY260626C00500000",
    }

    class FakeConn:
        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return [order]

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [
        {
            "id": "pos-mismatch",
            "underlying": "SPY",
            "status": "CLOSING",
            "pending_exit_local_order_id": "exit-owned",
            "pending_exit_broker_order_id": "broker-expected",
        }
    ]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    reconcile = MagicMock()
    fake_guard = types.ModuleType("ap.exit_fill_truth_guard")
    fake_guard.reconcile_confirmed_exit_fill = reconcile

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
            "ap.exit_fill_truth_guard": fake_guard,
        },
    ):
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)
        rec._reattach_live_exit_protections(result, live_exit_orders)

    assert live_exit_orders == []
    rec.broker.get_order.assert_not_called()
    rec.osm.transition.assert_not_called()
    reconcile.assert_not_called()
    rec.exit_engine.seed_from_db.assert_not_called()
    assert any("RECOVERY_PENDING_EXIT_BROKER_ID_MISMATCH" in err for err in result["errors"])


def test_recovery_missing_authoritative_pending_local_id_fails_closed():
    rec, _ = _make_recovery()
    rec.exit_engine = MagicMock()
    result = {"errors": [], "exits_reattached": 0}

    class FakeConn:
        def __init__(self):
            self.params = None

        def execute(self, _sql, params=()):
            self.params = params
            return self

        def fetchall(self):
            if self.params == (rec.client_id, "exit-missing"):
                return []
            raise AssertionError(f"unexpected params {self.params}")

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [
        {
            "id": "pos-missing-owned",
            "underlying": "SPY",
            "status": "CLOSING",
            "pending_exit_local_order_id": "exit-missing",
            "pending_exit_broker_order_id": "broker-missing",
        }
    ]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    reconcile = MagicMock()
    fake_guard = types.ModuleType("ap.exit_fill_truth_guard")
    fake_guard.reconcile_confirmed_exit_fill = reconcile

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
            "ap.exit_fill_truth_guard": fake_guard,
        },
    ):
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)
        rec._reattach_live_exit_protections(result, live_exit_orders)

    assert live_exit_orders == []
    rec.broker.get_order.assert_not_called()
    rec.osm.transition.assert_not_called()
    reconcile.assert_not_called()
    rec.exit_engine.seed_from_db.assert_not_called()
    assert any("RECOVERY_PENDING_EXIT_LOCAL_ID_UNRESOLVED" in err for err in result["errors"])


def test_recovery_reports_closing_position_without_active_exit():
    rec, _ = _make_recovery()
    rec.exit_engine = MagicMock()
    result = {"errors": [], "exits_reattached": 0}

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [
        {"id": "pos-missing", "underlying": "SPY", "status": "CLOSING"}
    ]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()

    class FakeConn:
        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return []

        def fetchone(self):
            return None

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db.conn = fake_conn

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
        },
    ):
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)

    assert live_exit_orders == []
    rec.broker.get_order.assert_not_called()
    rec.exit_engine.seed_from_db.assert_not_called()
    assert any(
        "RECOVERY_CLOSING_POSITION_WITHOUT_ACTIVE_EXIT pos=pos-missing underlying=SPY"
        in err
        for err in result["errors"]
    )


def test_recovery_reattaches_partial_downtime_exit_after_canonical_reconcile():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {
        "status": "partially_filled",
        "exec_quantity": "1",
        "avg_fill_price": "1.20",
    }
    rec.osm.transition.return_value = True
    rec.exit_engine = MagicMock()
    reconcile = MagicMock(return_value={"position_id": "pos-3"})
    result = {"errors": [], "exits_reattached": 0}

    rows = iter([
        {
            "local_order_id": "exit-3",
            "broker_order_id": "broker-exit-3",
            "status": "EXIT_ACKNOWLEDGED",
            "position_id": "pos-3",
            "contract": "SPY260626C00500000",
        },
        {
            "client_id": rec.client_id,
            "local_order_id": "exit-3",
            "broker_order_id": "broker-exit-3",
            "status": "EXIT_PARTIAL_FILL",
            "filled_qty": 1,
            "fill_price": 1.20,
            "filled_ts": "2026-07-17T12:00:00+00:00",
            "kind": "EXIT",
            "position_id": "pos-3",
            "contract": "SPY260626C00500000",
        },
    ])

    class FakeConn:
        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return [next(rows)]

        def fetchone(self):
            return next(rows)

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [
        {"id": "pos-3", "underlying": "SPY", "status": "CLOSING"}
    ]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    fake_guard = types.ModuleType("ap.exit_fill_truth_guard")
    fake_guard.reconcile_confirmed_exit_fill = reconcile

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
            "ap.exit_fill_truth_guard": fake_guard,
        },
    ):
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)
        rec._reattach_live_exit_protections(result, live_exit_orders)

    rec.osm.transition.assert_called_once_with(
        "exit-3",
        "EXIT_PARTIAL_FILL",
        filled_qty=1,
        fill_price=1.20,
    )
    reconcile.assert_called_once()
    rec.broker.get_order.assert_called_once_with("broker-exit-3")
    rec.exit_engine.seed_from_db.assert_called_once()
    assert result["exits_reattached"] == 1


def test_recovery_reattaches_active_exit_without_canonical_reconcile():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {"status": "open"}
    rec.exit_engine = MagicMock()
    result = {"errors": [], "exits_reattached": 0}

    rows = iter([
        {
            "local_order_id": "exit-4",
            "broker_order_id": "broker-exit-4",
            "status": "EXIT_ACKNOWLEDGED",
            "position_id": "pos-4",
            "contract": "QQQ260626P00400000",
        },
    ])

    class FakeConn:
        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return [next(rows)]

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [
        {"id": "pos-4", "underlying": "QQQ", "status": "CLOSING"}
    ]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    reconcile = MagicMock()
    fake_guard = types.ModuleType("ap.exit_fill_truth_guard")
    fake_guard.reconcile_confirmed_exit_fill = reconcile

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
            "ap.exit_fill_truth_guard": fake_guard,
        },
    ):
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)
        rec._reattach_live_exit_protections(result, live_exit_orders)

    reconcile.assert_not_called()
    rec.broker.get_order.assert_called_once_with("broker-exit-4")
    rec.exit_engine.seed_from_db.assert_called_once()
    assert result["exits_reattached"] == 1


def test_recovery_single_fallback_active_exit_remains_supported():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {"status": "open"}
    rec.exit_engine = MagicMock()
    result = {"errors": [], "exits_reattached": 0}

    order = {
        "local_order_id": "exit-fallback",
        "broker_order_id": "broker-fallback",
        "status": "EXIT_ACKNOWLEDGED",
        "position_id": "pos-fallback",
        "contract": "QQQ260626P00400000",
    }

    class FakeConn:
        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return [order]

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [
        {"id": "pos-fallback", "underlying": "QQQ", "status": "CLOSING"}
    ]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    reconcile = MagicMock()
    fake_guard = types.ModuleType("ap.exit_fill_truth_guard")
    fake_guard.reconcile_confirmed_exit_fill = reconcile

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
            "ap.exit_fill_truth_guard": fake_guard,
        },
    ):
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)
        rec._reattach_live_exit_protections(result, live_exit_orders)

    assert live_exit_orders == [order]
    rec.broker.get_order.assert_called_once_with("broker-fallback")
    reconcile.assert_not_called()
    rec.exit_engine.seed_from_db.assert_called_once()
    assert result["errors"] == []
    assert result["exits_reattached"] == 1


def test_recovery_does_not_reattach_full_downtime_exit_after_canonical_reconcile():
    rec, _ = _make_recovery()
    rec.broker.get_order.return_value = {
        "status": "filled",
        "exec_quantity": "2",
        "avg_fill_price": "1.75",
    }
    rec.osm.transition.return_value = True
    rec.exit_engine = MagicMock()
    reconcile = MagicMock(return_value={"position_id": "pos-5"})
    result = {"errors": [], "exits_reattached": 0}

    rows = iter([
        {
            "local_order_id": "exit-5",
            "broker_order_id": "broker-exit-5",
            "status": "EXIT_ACKNOWLEDGED",
            "position_id": "pos-5",
            "contract": "SPY260626C00510000",
        },
        {
            "client_id": rec.client_id,
            "local_order_id": "exit-5",
            "broker_order_id": "broker-exit-5",
            "status": "EXIT_FILLED",
            "filled_qty": 2,
            "fill_price": 1.75,
            "filled_ts": "2026-07-17T12:00:00+00:00",
            "kind": "EXIT",
            "position_id": "pos-5",
            "contract": "SPY260626C00510000",
        },
    ])

    class FakeConn:
        def execute(self, *args, **kwargs):
            return self

        def fetchall(self):
            return [next(rows)]

        def fetchone(self):
            return next(rows)

    @contextmanager
    def fake_conn():
        yield FakeConn()

    fake_db = types.ModuleType("ap.db")
    fake_db.list_positions = lambda client_id=None, status=None: [
        {"id": "pos-5", "underlying": "SPY", "status": "CLOSING"}
    ]
    fake_db.run_with_retry = lambda fn, *args, **kwargs: fn()
    fake_db.conn = fake_conn

    fake_guard = types.ModuleType("ap.exit_fill_truth_guard")
    fake_guard.reconcile_confirmed_exit_fill = reconcile

    with patch.dict(
        sys.modules,
        {
            "ap.db": fake_db,
            "ap_reconciler": _fake_reconciler_module(),
            "ap.exit_fill_truth_guard": fake_guard,
        },
    ):
        live_exit_orders = rec._recover_exit_fills_that_occurred_during_downtime(result)
        rec._reattach_live_exit_protections(result, live_exit_orders)

    reconcile.assert_called_once()
    rec.broker.get_order.assert_called_once_with("broker-exit-5")
    rec.exit_engine.seed_from_db.assert_not_called()
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
