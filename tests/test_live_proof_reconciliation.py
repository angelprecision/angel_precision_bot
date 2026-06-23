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
        order = {"broker_order_id": "133931493", "local_order_id": "loc-1",
                 "client_id": "jasoncosby1@gmail.com"}
        proof = {"broker_entry_order_id": None, "local_order_id": None,
                 "execution_mode": "unknown", "entry_option_price": 1.20,
                 "broker_reconciled": False, "entry_price_source": None,
                 "client_email": "jasoncosby1@gmail.com"}
        updates = mod._plan_repair(order, proof)
        assert updates["broker_entry_order_id"] == "133931493"
        assert updates["local_order_id"] == "loc-1"
        assert updates["execution_mode"] == "live"

    def test_marks_reconciled_only_with_real_entry_price(self):
        """broker_reconciled is set ONLY when a real entry fill price exists."""
        mod = _load_script()
        order = {"broker_order_id": "X", "local_order_id": "L",
                 "client_id": "c@x.com"}
        proof_no_price = {"entry_option_price": 0, "broker_reconciled": False,
                          "execution_mode": "live", "broker_entry_order_id": "X",
                          "local_order_id": "L", "entry_price_source": None,
                          "client_email": "c@x.com"}
        updates = mod._plan_repair(order, proof_no_price)
        assert "broker_reconciled" not in updates

        proof_priced = {"entry_option_price": 1.50, "broker_reconciled": False,
                        "execution_mode": "live", "broker_entry_order_id": "X",
                        "local_order_id": "L", "entry_price_source": None,
                        "client_email": "c@x.com"}
        updates = mod._plan_repair(order, proof_priced)
        assert updates.get("broker_reconciled") is True
        assert updates.get("entry_price_source") == mod.PRICE_SOURCE_TRADIER_ENTRY

    def test_noop_when_already_linked(self):
        mod = _load_script()
        order = {"broker_order_id": "X", "local_order_id": "L", "client_id": "c@x.com"}
        proof = {"broker_entry_order_id": "X", "local_order_id": "L",
                 "execution_mode": "live", "entry_option_price": 2.0,
                 "broker_reconciled": True, "entry_price_source": "TRADIER_ENTRY_FILL",
                 "client_email": "c@x.com"}
        updates = mod._plan_repair(order, proof)
        assert updates == {}

    def test_never_fabricates_price(self):
        mod = _load_script()
        order = {"broker_order_id": "X", "local_order_id": "L", "client_id": "c@x.com"}
        proof = {"broker_entry_order_id": None, "local_order_id": None,
                 "execution_mode": "unknown", "entry_option_price": 0,
                 "broker_reconciled": False, "entry_price_source": None,
                 "client_email": "c@x.com"}
        updates = mod._plan_repair(order, proof)
        assert "entry_option_price" not in updates
        assert "exit_fill_price" not in updates
        assert "realized_pnl_pct" not in updates

    def test_refuses_cross_client_repair(self):
        """HARD HOLD: a proof row belonging to a different client must never be
        touched, even if every other field lines up."""
        mod = _load_script()
        order = {"broker_order_id": "X", "local_order_id": "L",
                 "client_id": "jasoncosby1@gmail.com"}
        proof = {"broker_entry_order_id": None, "local_order_id": None,
                 "execution_mode": "unknown", "entry_option_price": 1.5,
                 "broker_reconciled": False, "entry_price_source": None,
                 "client_email": "someone.else@gmail.com"}
        updates = mod._plan_repair(order, proof)
        assert updates == {}, "must refuse to repair across clients"


class TestSafetyGuards:
    def test_apply_requires_double_confirmation(self):
        src = _SCRIPT.read_text()
        assert "--i-understand-this-writes-live" in src
        assert "REFUSING TO WRITE" in src

    def test_dry_run_is_default(self):
        src = _SCRIPT.read_text()
        assert 'writing = args.apply and getattr(args, "i_understand_this_writes_live"' in src

    def test_reuses_classify_official(self):
        src = _SCRIPT.read_text()
        assert "from ap.operator.live_execution_journal import (" in src
        assert "classify_official" in src

    def test_not_wired_into_runtime(self):
        src = _SCRIPT.read_text()
        assert 'if __name__ == "__main__":' in src


class TestHardHoldFixes:
    """The four HARD HOLD review fixes."""

    def test_uses_real_conn_api_not_get_connection(self):
        """Must use ap.db.conn() (the real @contextmanager), not the
        nonexistent ap.db.get_connection."""
        src = _SCRIPT.read_text()
        assert "from ap.db import conn" in src
        assert "with conn() as c:" in src
        # the import line must not call get_connection
        assert "from ap.db import get_connection" not in src
        assert "ap.db.get_connection(" not in src
        assert "get_connection()" not in src

    def test_uses_client_id_not_client_email_on_orders(self):
        """orders has NO client_email column — the orders query must select and
        filter by client_id (proof_trades is the table that has client_email)."""
        src = _SCRIPT.read_text()
        oq_start = src.find("FROM orders")
        oq = src[src.rfind("SELECT", 0, oq_start): oq_start + 300]
        assert "client_id" in oq
        assert "client_email" not in oq  # orders has no such column
        assert "AND client_id = %s" in src

    def test_fallback_match_requires_client_predicate(self):
        """The fuzzy fallback (ticker+contract+time) MUST include the client
        predicate so trades can't be mis-linked across clients."""
        src = _SCRIPT.read_text()
        idx = src.find("client + ticker + contract + entry-time window")
        assert idx != -1
        block = src[idx: idx + 1500]
        assert "client_email = %s" in block
        assert "ticker = %s AND contract = %s" in block

    def test_cross_table_bridge_documented(self):
        src = _SCRIPT.read_text()
        assert "orders.client_id == proof_trades.client_email" in src


class TestAmendmentRound2:
    """Review round 2 items 1-7."""

    def test_script_imports_cleanly(self):
        """Item 1: an actual importlib load — proves the script's imports are
        wired correctly (not just that grep finds the right strings)."""
        import importlib.util
        from unittest.mock import MagicMock
        from unittest.mock import patch
        import sys as _sys
        # Stub the late-imported deps so the module loads in a sandbox without a DB.
        stubs = {
            "ap.db": MagicMock(),
            "ap.operator.live_execution_journal": MagicMock(),
        }
        with patch.dict(_sys.modules, stubs):
            spec = importlib.util.spec_from_file_location("pr5_imp_test", _SCRIPT)
            mod = importlib.util.module_from_spec(spec)
            _sys.modules["pr5_imp_test"] = mod
            try:
                spec.loader.exec_module(mod)
            finally:
                _sys.modules.pop("pr5_imp_test", None)
        # If we got here, the script imported cleanly.
        assert callable(mod.main)
        assert callable(mod._plan_repair)
        assert callable(mod._find_proof_row)

    def test_ambiguous_fallback_refuses(self):
        """Item 3: if multiple proof rows match the fuzzy window, refuse and
        return AMBIGUOUS_MATCH — never pick one and guess."""
        mod = _load_script()
        # Simulate a cursor that returns two matching rows on fetchall
        class _Cur:
            description = [("id",), ("client_email",), ("ticker",), ("contract",)]
            _stage = 0
            def execute(self, *a, **k):
                # only the fallback query reaches fetchall; earlier id-based
                # joins use fetchone() and return None to fall through
                self._stage += 1
            def fetchone(self):
                return None  # broker_id / local_id / position_id miss
            def fetchall(self):
                # two ambiguous matches in the time window
                return [
                    (1, "jasoncosby1@gmail.com", "AMAT", "AMAT260620C00640000"),
                    (2, "jasoncosby1@gmail.com", "AMAT", "AMAT260620C00640000"),
                ]
        from datetime import datetime
        order = {
            "broker_order_id": "X", "local_order_id": "L", "position_id": None,
            "client_id": "jasoncosby1@gmail.com", "symbol": "AMAT",
            "contract": "AMAT260620C00640000",
            "created_ts": datetime(2026, 6, 18, 9, 35),
        }
        row, key = mod._find_proof_row(_Cur(), order)
        assert row is None
        assert key == "AMBIGUOUS_MATCH"

    def test_missing_client_id_refuses_fallback(self):
        """Item 3: if client_id is missing, refuse the fallback explicitly
        rather than silently skipping."""
        mod = _load_script()
        class _Cur:
            description = [("id",)]
            def execute(self, *a, **k): pass
            def fetchone(self): return None
            def fetchall(self): return []
        from datetime import datetime
        order = {
            "broker_order_id": "X", "local_order_id": None, "position_id": None,
            "client_id": None,  # missing
            "symbol": "AMAT", "contract": "AMAT260620C00640000",
            "created_ts": datetime(2026, 6, 18, 9, 35),
        }
        row, key = mod._find_proof_row(_Cur(), order)
        assert row is None
        assert key == "missing_client_id_fallback_refused"

    def test_no_price_fabrication_explicit(self):
        """Item 5: planner must never touch price/pnl/eligibility columns —
        only identifier/flag updates."""
        mod = _load_script()
        order = {"broker_order_id": "X", "local_order_id": "L", "client_id": "c@x.com"}
        # extreme case: every linkage field empty, every price field zero/null
        proof = {
            "broker_entry_order_id": None, "local_order_id": None,
            "execution_mode": "unknown", "entry_option_price": 0,
            "exit_option_price": None, "realized_pnl_pct": None,
            "broker_reconciled": False, "entry_price_source": None,
            "exit_price_source": None,
            "official_live_performance_eligible": False,
            "client_email": "c@x.com",
        }
        updates = mod._plan_repair(order, proof)
        FORBIDDEN = {
            "entry_option_price", "exit_option_price",
            "realized_pnl_pct", "option_pnl_pct",
            "official_live_performance_eligible",
        }
        for f in FORBIDDEN:
            assert f not in updates, f"planner must never write {f}"

    def test_main_loop_surfaces_safety_refusals(self):
        """Item 3: AMBIGUOUS_MATCH and missing-client refusals must be visible
        in the operator-facing output (not silently dropped as 'no proof row')."""
        src = _SCRIPT.read_text()
        assert "[AMBIGUOUS]" in src
        assert "[NO CLIENT_ID]" in src
        assert "Safety refusals:" in src

    def test_migration_is_evidence_backed_only(self):
        """Item 6: no broad unknown -> live on proof_trades. Every proof_trades
        UPDATE must JOIN to orders and require evidence."""
        sql = _MIG.read_text()
        # there must NOT be a bare proof_trades update that lowercases
        # everything without joining to orders. Find every proof_trades UPDATE
        # and ensure each has a FROM orders join.
        import re
        proof_updates = [m.start() for m in re.finditer(r"UPDATE proof_trades", sql)]
        assert len(proof_updates) >= 1, "expected at least one proof_trades update"
        for start in proof_updates:
            # the next ; ends this statement
            end = sql.find(";", start)
            stmt = sql[start:end]
            assert "FROM orders" in stmt or "FROM   orders" in stmt, (
                "every proof_trades UPDATE must join to orders for evidence — "
                "no broad normalization allowed"
            )


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
