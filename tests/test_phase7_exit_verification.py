"""
Phase 7 VERIFICATION tests: multi-contract partial exits + runner + exit pricing.

This is a VERIFICATION suite. It proves the existing exit code does the
right thing on multi-contract positions. It does NOT modify any source.
Every test is a code-shape proof or a behavioral assertion that runs
against the existing (unmodified) source.

The four properties we verify (from the audit prompt):

  1. Exit-side pricing uses the OPTION CONTRACT symbol, not the
     underlying stock symbol. (If it used the underlying, on QCOM $185
     stock we would 'sell at $185' instead of the option ask at $3.08
     which would crater P&L.)

  2. Exit-side pricing uses SIDE='SELL' (which returns the BID), so we
     model the price we will actually get \u2014 not the ask.

  3. Multi-contract partial exits: OSM EXIT_PARTIAL_FILL path calls
     note_partial_exit_fill with the correct delta (newly-filled qty)
     and never treats a partial as a full close.

  4. Runner logic: EXIT_FILLED with delta < remaining_before is routed
     to note_partial_exit_fill (NOT mark_position_closed), so the
     runner survives a partial-then-final fill sequence.

Run:
    pytest tests/test_phase7_exit_verification.py -xvs
"""
from __future__ import annotations

import os
import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


# ============================================================
# 1. Exit-side pricing reads the OPTION CONTRACT, not the underlying
# ============================================================

class TestExitPricesOptionContractNotUnderlying:
    """Verify exit_manager.get_exit_price_safe passes the option contract
    symbol (e.g. 'QCOM260523C00185000') to get_contract_price, NOT the
    underlying ticker ('QCOM').

    This is the most expensive bug we could ship: pricing a 'sell' against
    the underlying stock price would price the option at $185 (the stock
    price) instead of $3.08 (the actual option ask). The exit engine
    would think it's at +5800% profit and trigger a stop_loss exit at
    a price the broker will reject.
    """

    def test_get_exit_price_safe_passes_contract_not_underlying(self):
        src = (REPO_ROOT / "ap" / "exit_manager.py").read_text()
        # The function signature: get_exit_price_safe(broker, contract: str)
        assert "def get_exit_price_safe(broker: BrokerAdapter, contract: str)" in src

        # The call must use the contract param (it's already named 'contract')
        assert "get_contract_price(broker, contract, side=\"SELL\")" in src

    def test_loop_call_site_uses_pos_contract_not_underlying(self):
        src = (REPO_ROOT / "ap" / "exit_manager.py").read_text()
        # The loop must pass pos['contract'] (option symbol) not pos['underlying']
        # (stock symbol). Find the call site and assert it.
        assert "get_exit_price_safe(broker, pos[\"contract\"])" in src
        # Negative assertion: NEVER call with pos['underlying']
        assert "get_exit_price_safe(broker, pos[\"underlying\"])" not in src
        assert "get_exit_price_safe(broker, pos['underlying'])" not in src

    def test_submit_exit_order_uses_contract_not_underlying(self):
        src = (REPO_ROOT / "ap" / "exit_manager.py").read_text()
        # In submit_exit_order, the broker.place_order call:
        #   - symbol=position["underlying"]   (the parent ticker for routing)
        #   - contract=contract                (the OCC option symbol \u2014 what gets sold)
        # That's correct.
        m = re.search(
            r"def submit_exit_order.*?broker\.place_order\((.*?)\)",
            src, re.DOTALL,
        )
        assert m, "submit_exit_order should call broker.place_order"
        call_body = m.group(1)
        # 'contract=contract' is what tells the broker which option to sell
        assert "contract=contract" in call_body
        # side must be sell_to_close (close existing long)
        assert 'side="sell_to_close"' in call_body


# ============================================================
# 2. Exit-side pricing uses SIDE='SELL' (returns BID, not ask)
# ============================================================

class TestExitPricesUseSellSide:
    """get_contract_price(side='SELL') returns the BID (what we get filled at).
    get_contract_price(side='BUY') returns the ASK (what we pay).

    For an EXIT we are SELLING the long option, so we get the BID. If we
    accidentally priced an exit with 'BUY', the exit-condition check
    would compare bid-target against ask, which is always wider and would
    delay TPs / trigger stops late.
    """

    def test_exit_price_safe_passes_side_sell(self):
        src = (REPO_ROOT / "ap" / "exit_manager.py").read_text()
        # Match the canonical call signature
        assert 'side="SELL"' in src

    def test_contract_pricing_sell_uses_bid(self):
        src = (REPO_ROOT / "ap" / "contract_pricing.py").read_text()
        # The SELL branch returns the bid first, then last, then mid.
        assert "side == \"SELL\"" in src
        # Quick textual proof: in the SELL branch the first non-comment
        # return is bid.
        sell_branch_start = src.find("if side == \"SELL\":")
        assert sell_branch_start > 0
        sell_block = src[sell_branch_start:sell_branch_start + 600]
        # First return after the branch start must reference bid
        m = re.search(r"return _round_tick\((\w+)", sell_block)
        assert m, "SELL branch must return some price"
        assert m.group(1) == "bid", \
            "SELL branch's first return must be bid (not last/mid)"

    def test_contract_pricing_spread_check_skipped_on_sell(self):
        """A wide spread must NEVER block an EXIT \u2014 we have to get out.
        The spread check in get_contract_price must only run when
        side=='BUY'."""
        src = (REPO_ROOT / "ap" / "contract_pricing.py").read_text()
        # Look for the SPREAD CHECK comment + gate
        assert "SPREAD CHECK: BUY side only" in src
        assert "if side == \"BUY\":" in src
        # The spread check block must NOT also gate on SELL.
        # Find the spread block and check no SELL gating inside.
        spread_idx = src.find("SPREAD CHECK")
        block = src[spread_idx:spread_idx + 1200]
        # The first 'if side ==' inside the spread block must be 'BUY'
        m = re.search(r'if side == "(BUY|SELL)"', block)
        assert m and m.group(1) == "BUY", \
            "Spread check must gate on BUY only, not SELL"


# ============================================================
# 3. Multi-contract qty handling at exit submit
# ============================================================

class TestMultiContractExitQty:
    """When a position has qty=N contracts and we send a full exit, the
    broker.place_order qty must be N (not 1 and not the option_multiplier
    multiplied)."""

    def test_submit_exit_order_uses_position_qty(self):
        src = (REPO_ROOT / "ap" / "exit_manager.py").read_text()
        # int(position['qty']) is passed as qty=
        assert 'qty      = int(position["qty"])' in src or \
               'qty = int(position["qty"])' in src

        # Inside broker.place_order:
        m = re.search(r"resp = broker\.place_order\((.*?)\)", src, re.DOTALL)
        assert m
        body = m.group(1)
        assert "qty=qty" in body, "broker.place_order must receive qty=qty"

    def test_no_hardcoded_qty_one_in_exit_manager(self):
        """A 'qty=1' force inside the exit submit path would cap multi-contract
        positions to selling 1 lot per attempt. There must be no such hard-code."""
        src = (REPO_ROOT / "ap" / "exit_manager.py").read_text()
        # The submit_exit_order function must not contain a literal qty=1.
        m = re.search(
            r"def submit_exit_order.*?(?=^def |\Z)",
            src, re.DOTALL | re.MULTILINE,
        )
        assert m, "submit_exit_order should exist"
        body = m.group(0)
        # Tolerant search: anywhere qty=1 or qty = 1 (with broker context)
        assert not re.search(r"\bqty\s*=\s*1\b", body), \
            "submit_exit_order must not hard-code qty=1"


# ============================================================
# 4. Runner / partial-exit OSM handoff (note_partial_exit_fill)
# ============================================================

class TestPartialExitOSMHandoff:
    """The Order State Machine handles EXIT_PARTIAL_FILL by computing the
    *delta* (newly-filled qty) and routing to exit_engine.note_partial_exit_fill.
    If we ever forget to gate this and call mark_position_closed on a
    partial, the runner state disappears mid-trade. Verify the source.
    """

    OSM_SRC = (REPO_ROOT / "ap" / "order_state_machine.py").read_text()

    def test_exit_partial_fill_routes_to_note_partial(self):
        # The handler block has a distinctive comment marker; find it
        # (not the OrderStatus enum entry).
        idx = self.OSM_SRC.find("── EXIT_PARTIAL_FILL")
        assert idx > 0, "OSM must have an EXIT_PARTIAL_FILL handler block"
        # Look in a window of 800 chars after the marker.
        window = self.OSM_SRC[idx:idx + 800]
        assert "_delta = max(0, _cum_filled - _prev_filled)" in window, \
            "EXIT_PARTIAL_FILL must compute _delta = cum - prev"
        assert "note_partial_exit_fill" in window, \
            "EXIT_PARTIAL_FILL must route to note_partial_exit_fill"

    def test_exit_filled_branches_by_delta_vs_remaining(self):
        """In the EXIT_FILLED branch, if delta < remaining_before, the OSM
        must treat the fill as a completed SCALE-OUT (partial) by calling
        note_partial_exit_fill \u2014 NOT mark_position_closed. This is the
        runner-preservation gate."""
        idx = self.OSM_SRC.find("EXIT_FILLED treated as completed scale-out")
        assert idx > 0, "Runner-preservation comment must be present"
        window = self.OSM_SRC[idx:idx + 600]
        assert "note_partial_exit_fill" in window, \
            "Completed-scale-out path must call note_partial_exit_fill"

    def test_exit_filled_full_close_branch_exists(self):
        """The full-close branch (delta >= remaining) does NOT use
        note_partial_exit_fill; it proceeds to the final-close path. We
        just prove the branch comment is present so the structure stays
        clear."""
        assert "EXIT_FILLED treated as full close" in self.OSM_SRC

    def test_zero_qty_fill_quarantines_not_closes(self):
        """EXIT_FILLED with qty=0 must quarantine the order, not silently
        close the position. This is the bug the FIX-H comment guards."""
        idx = self.OSM_SRC.find("EXIT_FILLED with zero/unknown quantity")
        assert idx > 0
        window = self.OSM_SRC[idx:idx + 600]
        assert "identity_quarantine=True" in window
        assert "EXIT_FILLED_ZERO_QTY_QUARANTINE" in window

    def test_duplicate_callback_clears_only_when_safe(self):
        """A duplicate EXIT_FILLED callback (cum hasn't advanced) must call
        clear_exit_in_flight \u2014 only safe because prev > 0 means we already
        accepted at least one fill earlier."""
        idx = self.OSM_SRC.find("EXIT_FILLED duplicate callback")
        assert idx > 0
        window = self.OSM_SRC[idx:idx + 400]
        assert "clear_exit_in_flight" in window


# ============================================================
# 5. Position row carries the option contract symbol used at exit time
# ============================================================

class TestPositionContractIntegrity:
    """The positions table row stores .contract (option symbol) separately
    from .underlying (stock). The exit manager pulls .contract for the
    price quote and broker submit. If the row's .contract column ever
    got the stock symbol by mistake, exits would target the stock instead
    of the option. Verify the SELECT pulls the right column."""

    def test_get_open_positions_selects_contract(self):
        src = (REPO_ROOT / "ap" / "exit_manager.py").read_text()
        m = re.search(
            r"def get_open_positions.*?ORDER BY entry_ts ASC",
            src, re.DOTALL,
        )
        assert m, "get_open_positions must select positions"
        select_block = m.group(0)
        # Both columns must appear in the SELECT
        assert "underlying," in select_block
        assert "contract," in select_block
        # Verify the order is logical (underlying first then contract \u2014
        # mirroring how exit_manager uses them)
        assert select_block.find("underlying,") < select_block.find("contract,")


# ============================================================
# 6. End-to-end shape: exit_manager.exit_manager_loop uses contract correctly
# ============================================================

class TestExitManagerLoopShape:
    def test_loop_fetches_price_then_decides(self):
        src = (REPO_ROOT / "ap" / "exit_manager.py").read_text()
        # The loop body must fetch current_price before checking conditions.
        idx_price = src.find("current_price = get_exit_price_safe(broker, pos[\"contract\"])")
        idx_check = src.find("should_exit, reason = check_exit_conditions(pos, current_price)")
        assert idx_price > 0
        assert idx_check > 0
        assert idx_price < idx_check, \
            "current_price must be fetched BEFORE check_exit_conditions"

    def test_loop_skips_when_no_valid_price(self):
        """If get_exit_price_safe returns <= 0, the loop MUST skip rather
        than passing a zero price to check_exit_conditions (which would
        evaluate pnl_pct against fill and trigger a stop_loss immediately
        \u2014 a catastrophic false exit)."""
        src = (REPO_ROOT / "ap" / "exit_manager.py").read_text()
        assert "if current_price <= 0:" in src
        # In the body of that branch we must have a continue or skip.
        idx = src.find("if current_price <= 0:")
        block = src[idx:idx + 200]
        assert "continue" in block


# ============================================================
# 7. Behavioral test: check_exit_conditions math is correct
# ============================================================

class TestCheckExitConditionsMath:
    """A behavioral test that drives check_exit_conditions directly. This
    is a functional test, not just a source-shape test \u2014 it proves the
    math actually fires correctly."""

    @pytest.fixture(autouse=True)
    def _set_dummy_db_url(self):
        os.environ.setdefault(
            "DATABASE_URL",
            "postgresql://test:test@127.0.0.1:5432/test_phase7",
        )

    def test_take_profit_fires_above_threshold(self):
        from ap.exit_manager import check_exit_conditions
        pos = {"avg_fill": 3.00, "tp_pct": 0.20, "sl_pct": 0.25,
               "contract": "QCOM260523C00185000"}
        # TP triggers at 3.00 * 1.20 = 3.60. At 3.65 we should exit.
        should, reason = check_exit_conditions(pos, current_price=3.65)
        assert should is True
        assert reason == "TAKE_PROFIT"

    def test_stop_loss_fires_below_threshold(self):
        from ap.exit_manager import check_exit_conditions
        pos = {"avg_fill": 3.00, "tp_pct": 0.20, "sl_pct": 0.25,
               "contract": "QCOM260523C00185000"}
        # SL triggers at 3.00 * 0.75 = 2.25. At 2.20 we should exit.
        should, reason = check_exit_conditions(pos, current_price=2.20)
        assert should is True
        assert reason == "STOP_LOSS"

    def test_hold_between_thresholds(self):
        from ap.exit_manager import check_exit_conditions
        pos = {"avg_fill": 3.00, "tp_pct": 0.20, "sl_pct": 0.25,
               "contract": "QCOM260523C00185000"}
        # 3.00 is the entry, no movement \u2014 between TP and SL.
        # EOD check is environment-dependent, so we just assert that the
        # ([0] should_exit) result is False when prices are mid-range.
        # We allow True if EOD has fired (running during market close), but
        # in that case the reason must be EOD-related, not TP/SL.
        should, reason = check_exit_conditions(pos, current_price=3.05)
        if should:
            # Acceptable only if EOD fired
            assert reason in ("EOD_FLATTEN",), \
                f"unexpected exit reason on mid-range price: {reason}"
        else:
            assert should is False

    def test_uses_contract_not_underlying_in_pnl(self):
        """avg_fill is the option entry premium ($3.00). current_price is
        the option mark. The math must use avg_fill = option price."""
        from ap.exit_manager import check_exit_conditions
        # If the function ever started using underlying price by mistake,
        # an avg_fill of $185 with current_price of $3.00 would compute
        # PnL = (3 - 185) / 185 = -98.4% and trigger stop_loss.
        # We assert that with avg_fill at option scale, TP fires at option scale.
        pos = {"avg_fill": 3.00, "tp_pct": 0.20, "sl_pct": 0.25,
               "contract": "QCOM260523C00185000", "underlying": "QCOM"}
        # current_price = 3.65 (option), should TP.
        should, reason = check_exit_conditions(pos, current_price=3.65)
        assert should is True
        assert reason == "TAKE_PROFIT"
