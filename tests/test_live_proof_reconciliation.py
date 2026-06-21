"""
tests/test_live_proof_reconciliation.py — PR5

Verifies the live proof reconciliation repair planner and its safety guards.
The repair script reuses the existing classify_official() eligibility rules —
these tests confirm the *linking* logic and that it never fabricates fills.
"""
from __future__ import annotations

import sys
import importlib.util
from pathlib import Path
from datetime import datetime
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]
_SCRIPT = _REPO / "scripts" / "repair_live_proof_reconciliation.py"
_MIG = _REPO / "migrations" / "2026_06_20_execution_mode_normalization.sql"


def _load_script():
    stubs = {
        "ap.db": MagicMock(),
        "ap.operator.live_execution_journal": MagicMock(),
    }
    name = "repair_live_proof_shim"
    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location(name, _SCRIPT)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        try:
            spec.loader.exec_module(mod)
        finally:
            sys.modules.pop(name, None)
    return mod


class TestRepairPlanner:
    def test_links_broker_order_id_when_missing(self):
        mod = _load_script()
        order = {"broker_order_id": "133931493", "local_order_id": "loc-1"}
        proof = {"broker_entry_order_id": None, "local_order_id": None,
                 "execution_mode": "unknown", "entry_option_price": 1.20,
                 "broker_reconciled": False, "entry_price_source": None}
        updates = mod._plan_repair(order, proof)
        assert updates["broker_entry_order_id"] == "133931493"
        assert updates["local_order_id"] == "loc-1"
        assert updates["execution_mode"] == "live"

    def test_marks_reconciled_only_with_real_entry_price(self):
        """broker_reconciled is set ONLY when a real entry fill price exists."""
        mod = _load_script()
        order = {"broker_order_id": "X", "local_order_id": "L"}
        # no entry price → must NOT mark reconciled
        proof_no_price = {"entry_option_price": 0, "broker_reconciled": False,
                          "execution_mode": "live", "broker_entry_order_id": "X",
                          "local_order_id": "L", "entry_price_source": None}
        updates = mod._plan_repair(order, proof_no_price)
        assert "broker_reconciled" not in updates

        # real entry price → may mark reconciled
        proof_priced = {"entry_option_price": 1.50, "broker_reconciled": False,
                        "execution_mode": "live", "broker_entry_order_id": "X",
                        "local_order_id": "L", "entry_price_source": None}
        updates = mod._plan_repair(order, proof_priced)
        assert updates.get("broker_reconciled") is True
        assert updates.get("entry_price_source") == mod.PRICE_SOURCE_TRADIER_ENTRY

    def test_noop_when_already_linked(self):
        mod = _load_script()
        order = {"broker_order_id": "X", "local_order_id": "L"}
        proof = {"broker_entry_order_id": "X", "local_order_id": "L",
                 "execution_mode": "live", "entry_option_price": 2.0,
                 "broker_reconciled": True, "entry_price_source": "TRADIER_ENTRY_FILL"}
        updates = mod._plan_repair(order, proof)
        assert updates == {}

    def test_never_fabricates_price(self):
        """The planner must never write an entry/exit PRICE — only identifiers
        and flags backed by the real order."""
        mod = _load_script()
        order = {"broker_order_id": "X", "local_order_id": "L"}
        proof = {"broker_entry_order_id": None, "local_order_id": None,
                 "execution_mode": "unknown", "entry_option_price": 0,
                 "broker_reconciled": False, "entry_price_source": None}
        updates = mod._plan_repair(order, proof)
        assert "entry_option_price" not in updates
        assert "exit_fill_price" not in updates
        assert "realized_pnl_pct" not in updates


class TestSafetyGuards:
    def test_apply_requires_double_confirmation(self):
        """--apply alone must not write; needs the second flag."""
        src = _SCRIPT.read_text()
        assert "--i-understand-this-writes-live" in src
        assert "REFUSING TO WRITE" in src

    def test_dry_run_is_default(self):
        src = _SCRIPT.read_text()
        assert 'writing = args.apply and getattr(args, "i_understand_this_writes_live"' in src

    def test_reuses_classify_official(self):
        """Must reuse the existing eligibility authority, not reimplement it."""
        src = _SCRIPT.read_text()
        assert "from ap.operator.live_execution_journal import (" in src
        assert "classify_official" in src

    def test_not_wired_into_runtime(self):
        """Script must be standalone (only runs under __main__)."""
        src = _SCRIPT.read_text()
        assert 'if __name__ == "__main__":' in src


class TestMigration:
    def test_migration_is_idempotent_and_scoped(self):
        sql = _MIG.read_text()
        # lowercases casing, resolves unknown from joined order, touches only
        # execution_mode
        assert "lower(execution_mode)" in sql
        assert "execution_mode = 'unknown'" in sql
        # must not touch P&L / eligibility columns
        assert "realized_pnl" not in sql
        assert "official_live_performance_eligible" not in sql

    def test_migration_wrapped_in_transaction(self):
        sql = _MIG.read_text()
        assert "BEGIN;" in sql
        assert "COMMIT;" in sql
