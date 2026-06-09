"""
tests/test_pr100_broker_precheck.py
PR#100: exit engine broker position precheck — load, quote, pnl-seed, safety.
Source-level behavioral tests (same pattern as PR#96 tests).
"""
from pathlib import Path
import re, pytest

SRC = (Path(__file__).resolve().parents[1] / "ap_exit_engine.py").read_text()

# Isolate the precheck body for targeted assertions
_idx_pre  = SRC.find("def _broker_position_precheck(")
_idx_next = SRC.find("\n    def ", _idx_pre + 1)
PRECHECK_BODY = SRC[_idx_pre:_idx_next]


# ── Test 1: broker-only position gets loaded into self._positions ─────────────
def test_01_add_position_called_for_missing():
    """add_position() must be called inside the precheck for each repaired sym."""
    assert "self.add_position(pos)" in PRECHECK_BODY

def test_01b_fetch_broker_quote_helper_exists():
    assert "def _fetch_broker_quote" in SRC

def test_01c_quote_helper_calls_tradier_markets_quotes():
    idx = SRC.find("def _fetch_broker_quote")
    end = SRC.find("\n    def ", idx + 1)
    body = SRC[idx:end]
    assert "/v1/markets/quotes" in body


# ── Test 2: DB row exists → hydrated from DB (not upserted) ──────────────────
def test_02_db_load_tried_first():
    """_load_db_position_row must be called before _upsert_broker_position_to_db."""
    idx_load   = PRECHECK_BODY.find("_load_db_position_row")
    idx_upsert = PRECHECK_BODY.find("_upsert_broker_position_to_db")
    assert idx_load > 0 and idx_upsert > 0
    assert idx_load < idx_upsert, "DB load must precede DB upsert"

def test_02b_db_row_path_does_not_upsert():
    """When db_row is found and load succeeds, pos is set so upsert is skipped.
    Upsert is inside `if pos is None:` — only runs when db load failed.
    """
    # The upsert is guarded by `if pos is None:`
    assert "if pos is None:" in PRECHECK_BODY
    # The upsert block is under that guard, not at top level
    idx_guard  = PRECHECK_BODY.find("if pos is None:")
    idx_upsert = PRECHECK_BODY.find("_upsert_broker_position_to_db", idx_guard)
    assert idx_upsert > idx_guard, "upsert must be inside `if pos is None:` guard"


# ── Test 3: DB row missing → upsert path used ────────────────────────────────
def test_03_upsert_path_when_pos_is_none():
    """When pos is None after DB load, upsert is called."""
    assert "if pos is None:" in PRECHECK_BODY
    idx = PRECHECK_BODY.find("if pos is None:")
    block = PRECHECK_BODY[idx:idx+300]
    assert "_upsert_broker_position_to_db" in block


# ── Test 4: Quote sets current_option_price / bid / ask ──────────────────────
def test_04_quote_sets_current_option_price():
    assert "pos.current_option_price = broker_mark" in PRECHECK_BODY

def test_04b_bid_ask_set():
    assert "pos.current_bid = broker_bid" in PRECHECK_BODY
    assert "pos.current_ask = broker_ask" in PRECHECK_BODY

def test_04c_fetch_broker_quote_called():
    assert "_fetch_broker_quote(sym)" in PRECHECK_BODY


# ── Test 5: Positive P&L seeds touched_profit and peak_pnl_pct ───────────────
def test_05_touched_profit_set_when_pnl_positive():
    assert "pos.touched_profit = True" in PRECHECK_BODY

def test_05b_peak_pnl_pct_updated():
    assert "pos.peak_pnl_pct = broker_pnl_pct" in PRECHECK_BODY

def test_05c_pnl_only_seeded_when_positive():
    idx = PRECHECK_BODY.find("broker_pnl_pct > 0")
    assert idx > 0
    block = PRECHECK_BODY[idx:idx+200]
    assert "pos.touched_profit = True" in block


# ── Test 6: Broker failure logs unsafe, does not assume flat ─────────────────
def test_06_broker_failure_logs_unsafe():
    assert "EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE" in PRECHECK_BODY

def test_06b_broker_failure_returns_false_not_flat():
    """On broker failure, return False — do NOT delete or zero-out positions."""
    # Find the except block that catches broker.list_positions() failure
    idx_except = PRECHECK_BODY.find("broker.list_positions() failed")
    region = PRECHECK_BODY[idx_except:idx_except+300]
    assert "return False" in region
    assert "self._positions = []" not in PRECHECK_BODY
    assert ".clear()" not in PRECHECK_BODY


# ── Test 7: Already-tracked positions not duplicated ─────────────────────────
def test_07_missing_set_excludes_existing_engine_positions():
    """engine_syms is built from self._positions and subtracted from broker_syms."""
    assert "missing_from_engine = broker_syms - engine_syms" in PRECHECK_BODY

def test_07b_engine_syms_built_from_self_positions():
    assert "for p in self._positions" in PRECHECK_BODY
    assert "engine_syms" in PRECHECK_BODY


# ── Test 8: No direct exit submission inside precheck ─────────────────────────
def test_08_no_submit_inside_precheck():
    for forbidden in ["_submit_exit", "submit_exit", "place_order", "_place_order",
                      "_submit_order", "broker.submit"]:
        assert forbidden not in PRECHECK_BODY, (
            f"precheck must not call {forbidden}"
        )

def test_08b_exit_order_submitted_false_in_log():
    """The structured log must state exit_order_submitted=False explicitly."""
    assert "exit_order_submitted=%s" in PRECHECK_BODY
    # The value passed for exit_order_submitted must be False
    idx = PRECHECK_BODY.find("exit_order_submitted=%s")
    region = PRECHECK_BODY[idx:idx+400]
    assert "False," in region or "False" in region


# ── Test 9: Precheck called before _check_all_positions ──────────────────────
def test_09_precheck_before_check_all():
    # _check_all_positions calls the precheck at its start
    idx_method = SRC.find("def _check_all_positions(")
    method_end = SRC.find("\n    def ", idx_method + 1)
    method_body = SRC[idx_method:method_end]
    # Precheck is called inside _check_all_positions
    assert "_broker_position_precheck()" in method_body


# ── Test 10: Scope guard — no scanner/entry/scoring files referenced ──────────
def test_10_scope_guard_no_scanner_refs_in_precheck():
    forbidden = [
        "ap_master_control", "ap_scanner", "ap_quality_mode",
        "intelligence_bridge", "ap_entry_watcher", "ap_contract_selector",
        "ap_sizer", "client_runner",
    ]
    for pat in forbidden:
        assert pat not in PRECHECK_BODY, f"precheck must not reference {pat}"

def test_10b_required_log_present():
    assert "EXIT_ENGINE_REPAIRED_BROKER_POSITION_AND_EVALUATED_EXIT" in PRECHECK_BODY

def test_10c_required_log_fields_present():
    for field in ["contract_symbol", "broker_qty", "broker_cost_basis",
                  "broker_mark_or_bid", "broker_pnl_pct",
                  "engine_seen_before", "db_seen_before", "db_repaired",
                  "exit_rule_triggered", "exit_order_submitted", "reason_no_exit"]:
        assert field in PRECHECK_BODY, f"required log field missing: {field}"
