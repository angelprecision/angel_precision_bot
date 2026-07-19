"""P0 regression coverage for durable position execution-mode identity."""
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path

import pytest

from ap.position_manager import APPositionManager
from ap_reconciler import APBrokerReconciler


REPO_ROOT = Path(__file__).resolve().parents[1]
MIGRATION = REPO_ROOT / "migrations" / "20260719032517_add_positions_execution_mode.sql"


def _position_kwargs(**overrides):
    values = {
        "plan_id": "plan-1",
        "signal_id": "signal-1",
        "ticker": "SPY",
        "contract": "SPY260821C00650000",
        "side": "CALL",
        "qty": 1,
        "entry_price": 1.25,
        "execution_mode": "live",
    }
    values.update(overrides)
    return values


def test_schema_attestation_requires_positions_execution_mode():
    from ap.schema_attestation import REQUIRED_SCHEMA

    assert "execution_mode" in REQUIRED_SCHEMA["positions"]


def test_migration_backfills_only_unique_originating_entry_identity():
    sql = MIGRATION.read_text(encoding="utf-8")

    assert "ADD COLUMN IF NOT EXISTS execution_mode TEXT" in sql
    assert "o.client_id = p.client_id" in sql
    assert "o.local_order_id = p.local_order_id" in sql
    assert "o.broker_order_id = p.broker_order_id" in sql
    assert "HAVING COUNT(DISTINCT o.id) = 1" in sql
    assert "COUNT(DISTINCT LOWER(TRIM(o.execution_mode))) = 1" in sql
    assert "execution_mode IS NULL" in sql  # historical unknown is permitted
    assert "DEFAULT 'paper'" not in sql and 'DEFAULT "paper"' not in sql
    assert "DEFAULT 'live'" not in sql and 'DEFAULT "live"' not in sql


@pytest.mark.parametrize("execution_mode", [None, "", "unknown", "staging"])
def test_open_position_rejects_missing_or_invalid_execution_mode(execution_mode):
    manager = APPositionManager("client@example.com")

    with pytest.raises(ValueError, match="invalid_or_missing_position_execution_mode"):
        manager.open_position(**_position_kwargs(execution_mode=execution_mode))


def test_open_position_rejects_database_without_execution_mode_column():
    manager = APPositionManager("client@example.com")
    manager._position_columns_cache = set()

    with pytest.raises(RuntimeError, match="required_position_column_missing:execution_mode"):
        manager.open_position(**_position_kwargs())


def test_open_position_persists_normalized_execution_mode(monkeypatch):
    import ap.position_manager as position_manager

    executed: list[tuple[str, tuple]] = []

    class Cursor:
        def __init__(self):
            self.last_sql = ""

        def execute(self, sql, params=()):
            self.last_sql = str(sql)
            executed.append((self.last_sql, tuple(params)))
            return self

        def fetchone(self):
            if "INSERT INTO positions" in self.last_sql:
                return {"id": "position-1"}
            return None

    @contextmanager
    def fake_conn():
        yield Cursor()

    monkeypatch.setattr(position_manager, "conn", fake_conn)
    monkeypatch.setattr(position_manager, "run_with_retry", lambda fn, **_: fn())

    manager = APPositionManager("client@example.com")
    manager._position_columns_cache = {"execution_mode"}
    position_id = manager.open_position(**_position_kwargs(execution_mode=" LIVE "))

    assert position_id == "position-1"
    insert_sql, insert_params = next(
        (sql, params) for sql, params in executed if "INSERT INTO positions" in sql
    )
    assert "execution_mode" in insert_sql
    assert insert_params[-1] == "live"


def test_fill_monitor_preserves_order_execution_mode(monkeypatch):
    from ap import fill_monitor

    class PositionManager:
        kwargs = None

        def open_position(self, **kwargs):
            self.kwargs = kwargs
            return "position-1"

    manager = PositionManager()
    order = {
        "client_id": "client@example.com",
        "local_order_id": "local-1",
        "broker_order_id": "broker-1",
        "symbol": "SPY",
        "contract": "SPY260821C00650000",
        "direction": "CALL",
        "qty": 1,
        "execution_mode": "live",
    }
    monkeypatch.setattr(fill_monitor, "_record_position_create_failure", lambda *_: None)

    result = fill_monitor._open_position_safe(
        manager,
        order=order,
        result={"filled_qty": 1, "avg_fill": 1.25},
        plan_id="plan-1",
        signal_id="signal-1",
        local_id="local-1",
        broker=object(),
        quote_broker=None,
    )

    assert result == "position-1"
    assert manager.kwargs["execution_mode"] == "live"


def _reconciler(mode="live"):
    reconciler = APBrokerReconciler.__new__(APBrokerReconciler)
    reconciler.client_id = "client@example.com"
    reconciler.execution_mode = mode
    return reconciler


def test_reconciled_fill_refuses_cross_mode_position_materialization():
    reconciler = _reconciler("live")

    class PositionManager:
        calls = []

        def open_position(self, **kwargs):
            self.calls.append(kwargs)
            return "position-1"

    reconciler.pm = PositionManager()
    summary = {"positions_alerted": 0, "positions_imported": 0}
    result = reconciler._ensure_position_for_filled_entry(
        {
            "local_order_id": "local-1",
            "contract": "SPY260821C00650000",
            "symbol": "SPY",
            "direction": "CALL",
            "execution_mode": "paper",
        },
        filled_qty=1,
        avg_fill=1.25,
        summary=summary,
    )

    assert result is None
    assert reconciler.pm.calls == []
    assert "reconciler_filled_entry_execution_mode_unproven" in summary["errors"]


def test_reconciled_fill_persists_exact_mode_and_order_identity():
    reconciler = _reconciler("live")

    class PositionManager:
        kwargs = None

        def open_position(self, **kwargs):
            self.kwargs = kwargs
            return "position-1"

    reconciler.pm = PositionManager()
    reconciler._find_db_position_by_contract = lambda *_: None
    reconciler._find_db_position_by_id = lambda *_: None
    reconciler._seed_exit_engine_from_position = lambda *_: None
    reconciler._alert = lambda *_: None
    summary = {"positions_alerted": 0, "positions_imported": 0}

    result = reconciler._ensure_position_for_filled_entry(
        {
            "local_order_id": "local-1",
            "broker_order_id": "broker-1",
            "plan_id": "plan-1",
            "signal_id": "signal-1",
            "contract": "SPY260821C00650000",
            "symbol": "SPY",
            "direction": "CALL",
            "execution_mode": "live",
        },
        filled_qty=1,
        avg_fill=1.25,
        summary=summary,
    )

    assert result == "position-1"
    assert reconciler.pm.kwargs["execution_mode"] == "live"
    assert reconciler.pm.kwargs["local_order_id"] == "local-1"
    assert reconciler.pm.kwargs["broker_order_id"] == "broker-1"


def test_position_contract_lookup_is_exact_mode_scoped(monkeypatch):
    import sys
    from types import SimpleNamespace

    executed: list[tuple[str, tuple]] = []

    class Cursor:
        def execute(self, sql, params=()):
            executed.append((str(sql), tuple(params)))
            return self

        def fetchone(self):
            return None

    @contextmanager
    def fake_conn():
        yield Cursor()

    fake_db = SimpleNamespace(conn=fake_conn, run_with_retry=lambda fn, **_: fn())
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)

    reconciler = _reconciler("live")
    assert reconciler._find_db_position_by_contract("SPY260821C00650000") is None
    sql, params = executed[-1]
    assert "LOWER(TRIM(COALESCE(execution_mode,'')))=%s" in sql
    assert params[0:2] == ("client@example.com", "live")
