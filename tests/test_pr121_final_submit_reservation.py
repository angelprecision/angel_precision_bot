from __future__ import annotations

import os
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXEC_SRC = (REPO_ROOT / "ap" / "execution.py").read_text()

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_pr121",
)


@pytest.fixture
def reconcile_fn():
    from ap.execution import _reconcile_final_order_reservation

    return _reconcile_final_order_reservation


class TestFinalSubmitReservationHelper:
    def test_keeps_existing_reservation_when_submit_cost_fits(self, reconcile_fn):
        ok, reserved_cost, final_order_cost = reconcile_fn(
            "client-A",
            qty=2,
            submit_limit=2.40,
            reserved_cost=500.0,
            equity=10_000.0,
        )
        assert ok is True
        assert reserved_cost == pytest.approx(500.0)
        assert final_order_cost == pytest.approx(480.0)

    def test_reserves_delta_when_submit_cost_grows(self, reconcile_fn, monkeypatch):
        calls: list[tuple[str, float, float]] = []

        def fake_reserve(client_id, amount, equity):
            calls.append((client_id, amount, equity))
            return True

        monkeypatch.setattr("ap.execution.reserve_equity_if_available", fake_reserve)

        ok, reserved_cost, final_order_cost = reconcile_fn(
            "client-A",
            qty=2,
            submit_limit=2.60,
            reserved_cost=500.0,
            equity=10_000.0,
        )
        assert ok is True
        assert reserved_cost == pytest.approx(520.0)
        assert final_order_cost == pytest.approx(520.0)
        assert calls == [("client-A", pytest.approx(20.0), 10_000.0)]

    def test_rejects_when_delta_reservation_fails(self, reconcile_fn, monkeypatch):
        monkeypatch.setattr(
            "ap.execution.reserve_equity_if_available",
            lambda client_id, amount, equity: False,
        )

        ok, reserved_cost, final_order_cost = reconcile_fn(
            "client-A",
            qty=3,
            submit_limit=2.10,
            reserved_cost=600.0,
            equity=10_000.0,
        )
        assert ok is False
        assert reserved_cost == pytest.approx(600.0)
        assert final_order_cost == pytest.approx(630.0)


class TestProcessSignalReservationWiring:
    def test_reconcile_happens_before_insert_and_submit(self):
        idx_reconcile = EXEC_SRC.find("_reconcile_final_order_reservation(")
        idx_insert = EXEC_SRC.find("insert_order(")
        idx_submit = EXEC_SRC.rfind("_submit_order_with_retry(")
        assert idx_reconcile > 0
        assert idx_insert > 0
        assert idx_submit > 0
        assert idx_reconcile < idx_insert
        assert idx_reconcile < idx_submit

    def test_insert_and_trade_executed_use_reconciled_reserved_cost(self):
        idx_insert = EXEC_SRC.find("insert_order(")
        insert_window = EXEC_SRC[idx_insert:idx_insert + 500]
        assert "reserved_cost=float(reserved_cost)" in insert_window

        idx_audit = EXEC_SRC.find('"TRADE_EXECUTED"')
        audit_window = EXEC_SRC[idx_audit:idx_audit + 600]
        assert '"reserved_cost": float(reserved_cost)' in audit_window

    def test_reject_path_releases_and_returns_reason_code(self):
        idx = EXEC_SRC.find("if not reservation_ok:")
        assert idx > 0
        window = EXEC_SRC[idx:idx + 1600]
        assert "release_equity(client_id, reserved_cost)" in window
        assert "release_symbol_lock(client_id, symbol)" in window
        assert 'reason_code": "FINAL_CONTRACT_UNAFFORDABLE"' in window
        assert 'error": "insufficient_available_equity"' in window
