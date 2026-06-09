"""
tests/test_exit_engine_broker_repair.py
========================================
P0 hotfix tests: exit engine repairs broker-visible positions that are
missing from local engine state before evaluating exits.

Covers:
  - Missing position detected and loaded into engine
  - DB row found → engine uses it (db_seen_before=True)
  - DB row missing → repair upserted (db_repaired=True)
  - Live quote fetched, current_option_price + pnl_pct set
  - Exit rule evaluated in same cycle (via _check_all_positions)
  - exit_order_submitted=False in repair log (submission is next loop step)
  - Broker fetch failure is non-blocking
  - Already-in-engine positions are not re-added
  - No scanner/entry/scoring/sizing code touched
"""
from __future__ import annotations

import os, sys, re
from pathlib import Path
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch, call
from dataclasses import dataclass, field
from typing import Optional

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))
os.environ.setdefault("DATABASE_URL",
                       "postgresql://test:test@127.0.0.1:5432/test_exit_repair")

from ap_exit_engine import (
    _extract_ticker_from_occ,
    _infer_direction_from_occ,
)


# ── OCC helpers ───────────────────────────────────────────────────────────────

def test_extract_ticker_rivn():
    assert _extract_ticker_from_occ("RIVN260612P00016500") == "RIVN"

def test_extract_ticker_spy():
    assert _extract_ticker_from_occ("SPY260612C00580000") == "SPY"

def test_extract_ticker_single_letter():
    assert _extract_ticker_from_occ("C260612C00136000") == "C"

def test_infer_direction_put():
    assert _infer_direction_from_occ("RIVN260612P00016500") == "PUT"

def test_infer_direction_call():
    assert _infer_direction_from_occ("C260612C00136000") == "CALL"


# ── Engine fixture ────────────────────────────────────────────────────────────

def _make_engine(broker_positions=None, quote_map=None, db_row=None, position_id="POS-001"):
    """Build a minimal APExitEngine with mocked broker and position_manager."""
    from ap_exit_engine import APExitEngine

    broker = MagicMock()
    broker.list_positions.return_value = broker_positions or []
    broker.cfg = MagicMock()
    broker.cfg.account_id = "VA23856550"

    def _mock_get_quote(sym):
        return (quote_map or {}).get(sym, {})

    broker.get_quote.side_effect = _mock_get_quote

    engine = APExitEngine(broker=broker, email="jasoncosby1@gmail.com")
    engine._email = "jasoncosby1@gmail.com"

    # Wire position_manager mock
    pm = MagicMock()
    pm.get_position_by_contract.return_value = db_row
    pm.get_position.return_value = db_row
    pm.open_position.return_value = position_id
    engine._position_manager = pm

    return engine, broker, pm


def _broker_pos(symbol="RIVN260612P00016500", qty=1, cost_basis=120.0, side="PUT"):
    return {"symbol": symbol, "quantity": qty, "cost_basis": cost_basis, "side": side}


def _db_row(contract="RIVN260612P00016500", qty=1, entry=1.20, pid="POS-001"):
    return {
        "id": pid, "client_id": "jasoncosby1@gmail.com",
        "contract": contract, "option_symbol": contract,
        "ticker": "RIVN", "direction": "PUT",
        "qty": qty, "quantity_remaining": qty,
        "avg_fill": entry, "entry_price": entry,
        "underlying_entry": 0.0, "target_underlying": 0.0, "stop_underlying": 0.0,
        "scale_outs_done": 0, "signal_id": "SIG-RIVN-001", "status": "OPEN",
    }


# ── Test 1: RIVN missing from engine — repair detects it ─────────────────────

def test_broker_only_position_added_to_engine():
    """RIVN is in broker but not engine → gets loaded into self._positions."""
    engine, broker, pm = _make_engine(
        broker_positions=[_broker_pos("RIVN260612P00016500", qty=1, cost_basis=120.0)],
        quote_map={"RIVN260612P00016500": {"bid": 1.73, "ask": 1.78, "last": 1.73}},
        db_row=_db_row(),
    )
    # Engine starts empty
    assert len(engine._positions) == 0

    engine._broker_position_precheck()

    # Position now in engine
    assert len(engine._positions) == 1
    assert engine._positions[0].option_symbol == "RIVN260612P00016500"


def test_broker_repair_sets_live_prices():
    """After repair, current_option_price reflects broker bid quote."""
    engine, broker, pm = _make_engine(
        broker_positions=[_broker_pos("RIVN260612P00016500", cost_basis=120.0)],
        quote_map={"RIVN260612P00016500": {"bid": 1.73, "ask": 1.78, "last": 1.73}},
        db_row=_db_row(),
    )
    engine._broker_position_precheck()

    pos = engine._positions[0]
    assert pos.current_option_price == pytest.approx(1.73, abs=0.01)
    assert pos.current_bid  == pytest.approx(1.73, abs=0.01)


def test_broker_repair_sets_pnl_pct():
    """Peak P&L pct is set from broker mark vs avg_entry."""
    # cost_basis=120, qty=1 → avg_entry = 120/(1*100) = 1.20
    # broker mark=1.73 → pnl_pct = (1.73-1.20)/1.20 ≈ +44.2%
    engine, broker, pm = _make_engine(
        broker_positions=[_broker_pos("RIVN260612P00016500", cost_basis=120.0)],
        quote_map={"RIVN260612P00016500": {"bid": 1.73, "ask": 1.78, "last": 1.73}},
        db_row=_db_row(entry=1.20),
    )
    engine._broker_position_precheck()

    pos = engine._positions[0]
    assert pos.peak_pnl_pct == pytest.approx(0.4417, abs=0.005)
    assert pos.touched_profit is True


def test_broker_repair_uses_db_entry_price_when_available():
    """entry_price comes from DB row, not from broker cost_basis calculation."""
    engine, broker, pm = _make_engine(
        broker_positions=[_broker_pos("RIVN260612P00016500", cost_basis=120.0)],
        quote_map={"RIVN260612P00016500": {"bid": 1.73, "ask": 1.78, "last": 1.73}},
        db_row=_db_row(entry=1.25),   # DB says 1.25
    )
    engine._broker_position_precheck()

    pos = engine._positions[0]
    assert pos.entry_price == pytest.approx(1.25, abs=0.01)


# ── Test 2: DB row found → db_seen_before=True, no upsert ────────────────────

def test_db_found_no_upsert(caplog):
    """When DB has the row, open_position must NOT be called."""
    import logging
    engine, broker, pm = _make_engine(
        broker_positions=[_broker_pos("RIVN260612P00016500")],
        quote_map={"RIVN260612P00016500": {"bid": 1.73, "ask": 1.78, "last": 1.73}},
        db_row=_db_row(),
    )
    with caplog.at_level(logging.WARNING, logger="ap_exit_engine"):
        engine._broker_position_precheck()

    pm.open_position.assert_not_called()
    assert "db_seen_before=True" in caplog.text


# ── Test 3: DB missing → repair upserted ─────────────────────────────────────

def test_db_missing_triggers_repair(caplog):
    """When DB row is missing, open_position is called and db_repaired=True logged."""
    import logging
    engine, broker, pm = _make_engine(
        broker_positions=[_broker_pos("RIVN260612P00016500")],
        quote_map={"RIVN260612P00016500": {"bid": 1.73, "ask": 1.78, "last": 1.73}},
        db_row=None,   # not in DB
    )
    with caplog.at_level(logging.WARNING, logger="ap_exit_engine"):
        engine._broker_position_precheck()

    pm.open_position.assert_called_once()
    call_kwargs = pm.open_position.call_args.kwargs
    assert call_kwargs["contract"] == "RIVN260612P00016500"
    assert call_kwargs["side"] == "PUT"
    assert call_kwargs["qty"] == 1
    assert "db_repaired=True" in caplog.text


# ── Test 4: Required audit log fields present ────────────────────────────────

def test_audit_log_all_required_fields(caplog):
    """EXIT_ENGINE_REPAIRED_BROKER_POSITION_AND_EVALUATED_EXIT must log all fields."""
    import logging
    engine, broker, pm = _make_engine(
        broker_positions=[_broker_pos("RIVN260612P00016500", cost_basis=120.0)],
        quote_map={"RIVN260612P00016500": {"bid": 1.73, "ask": 1.78, "last": 1.73}},
        db_row=_db_row(),
    )
    with caplog.at_level(logging.WARNING, logger="ap_exit_engine"):
        engine._broker_position_precheck()

    log_text = caplog.text
    assert "EXIT_ENGINE_REPAIRED_BROKER_POSITION_AND_EVALUATED_EXIT" in log_text
    assert "client_email=" in log_text
    assert "account_id=" in log_text
    assert "contract_symbol=" in log_text
    assert "broker_qty=" in log_text
    assert "broker_cost_basis=" in log_text
    assert "broker_mark_or_bid=" in log_text
    assert "broker_pnl_pct=" in log_text
    assert "engine_seen_before=False" in log_text
    assert "db_repaired=" in log_text
    assert "exit_rule_triggered=" in log_text
    assert "exit_order_submitted=False" in log_text
    assert "reason_no_exit=" in log_text


# ── Test 5: Broker fetch failure is non-blocking ──────────────────────────────

def test_broker_fetch_failure_non_blocking(caplog):
    """list_positions() raises → precheck returns False, engine continues."""
    import logging
    from ap_exit_engine import APExitEngine

    broker = MagicMock()
    broker.list_positions.side_effect = Exception("tradier_timeout")
    engine = APExitEngine(broker=broker, email="jasoncosby1@gmail.com")

    with caplog.at_level(logging.ERROR, logger="ap_exit_engine"):
        result = engine._broker_position_precheck()

    assert result is False
    assert "EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE" in caplog.text
    # Engine positions untouched
    assert len(engine._positions) == 0


# ── Test 6: Already-in-engine positions are not re-added ─────────────────────

def test_engine_position_not_duplicated():
    """If RIVN is already in engine, repair must not add it again."""
    from ap_exit_engine import APExitEngine, ManagedPosition

    broker = MagicMock()
    broker.list_positions.return_value = [
        _broker_pos("RIVN260612P00016500")
    ]
    broker.get_quote.return_value = {"bid": 1.73, "ask": 1.78, "last": 1.73}
    broker.cfg = MagicMock()
    broker.cfg.account_id = "VA23856550"

    engine = APExitEngine(broker=broker, email="jasoncosby1@gmail.com")
    engine._position_manager = MagicMock()

    # Pre-load the position into engine
    existing = ManagedPosition(
        ticker="RIVN", option_symbol="RIVN260612P00016500",
        side="PUT", quantity=1, entry_price=1.20,
        underlying_entry=0.0, underlying_target=0.0, underlying_stop=0.0,
    )
    existing.quantity_remaining = 1
    engine.add_position(existing)
    assert len(engine._positions) == 1

    engine._broker_position_precheck()

    # Still only 1 — not duplicated
    assert len(engine._positions) == 1


# ── Test 7: No mutation of scoring/sizing/scanner ────────────────────────────

def test_no_entry_or_scoring_calls():
    """Repair path must not call any entry, scoring, or sizing functions."""
    engine, broker, pm = _make_engine(
        broker_positions=[_broker_pos("RIVN260612P00016500")],
        quote_map={"RIVN260612P00016500": {"bid": 1.73, "ask": 1.78, "last": 1.73}},
        db_row=_db_row(),
    )
    engine._broker_position_precheck()

    # Broker calls: list_positions + get_quote only
    broker_methods_called = {c[0] for c in broker.method_calls}
    forbidden = {"submit_order", "place_order", "create_order",
                 "score", "size_position", "select_contract"}
    assert not (broker_methods_called & forbidden), \
        f"Forbidden broker methods called: {broker_methods_called & forbidden}"


# ── Test 8: Two broker positions, both loaded ─────────────────────────────────

def test_two_broker_positions_both_loaded():
    """Both of Jason's live positions (C + RIVN) must be loaded into engine."""
    engine, broker, pm = _make_engine(
        broker_positions=[
            _broker_pos("C260612C00136000",    qty=1, cost_basis=180.0, side="CALL"),
            _broker_pos("RIVN260612P00016500", qty=1, cost_basis=120.0, side="PUT"),
        ],
        quote_map={
            "C260612C00136000":    {"bid": 1.55, "ask": 1.60, "last": 1.55},
            "RIVN260612P00016500": {"bid": 1.73, "ask": 1.78, "last": 1.73},
        },
        db_row=None,   # both missing from DB
    )
    # open_position is called twice — return unique IDs so add_position doesn't dedup
    pm.open_position.side_effect = ["POS-C-001", "POS-RIVN-001"]
    pm.get_position.side_effect = [
        _db_row("C260612C00136000",    pid="POS-C-001"),
        _db_row("RIVN260612P00016500", pid="POS-RIVN-001"),
    ]

    engine._broker_position_precheck()

    assert len(engine._positions) == 2
    contracts = {p.option_symbol for p in engine._positions}
    assert "C260612C00136000"    in contracts
    assert "RIVN260612P00016500" in contracts
    assert pm.open_position.call_count == 2


# ── Test 9: seed_from_db stores position_manager reference ───────────────────

def test_seed_from_db_stores_position_manager():
    """seed_from_db must store position_manager as self._position_manager."""
    from ap_exit_engine import APExitEngine

    engine = APExitEngine(broker=MagicMock(), email="jason@ap.com")
    assert engine._position_manager is None

    pm = MagicMock()
    pm.get_active_positions.return_value = []
    engine.seed_from_db(pm)

    assert engine._position_manager is pm
