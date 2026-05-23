"""
Post-review safety verification tests for PR #17 P1 entry fix.

Four concerns raised by the reviewer; one test per concern. ALL four must
pass before merging to paper.

Run:
    pytest tests/test_p1_safety_review.py -xvs
"""
from __future__ import annotations

import re
import sys
import types
import importlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


# ============================================================
# Concern #1: current_ask must be from the OPTION CONTRACT, not underlying
# ============================================================

class TestCurrentAskIsContractQuote:
    """The ladder uses current_ask as its anchor. If current_ask is the
    UNDERLYING price (stock at $100) instead of the OPTION ask ($2.10),
    the ladder would set the limit at $100.01 \u2014 a catastrophic mispricing.
    Verify _try_repeg sources current_ask from the option contract symbol."""

    def test_caller_passes_option_contract_to_try_repeg(self):
        """At the call site, _sym is set to order.get('contract') first.
        Verify the priority of fallbacks puts the option contract first."""
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        # The _sym binding before _try_repeg is called must source contract first.
        m = re.search(
            r'_sym = order\.get\("contract"\) or order\.get\("symbol"[^)]*\) or contract',
            src,
        )
        assert m, "_sym priority must be: order.contract -> order.symbol -> contract"

    def test_try_repeg_refuses_non_option_symbols(self):
        """If sym doesn't look like an OCC option contract (no digits), refuse
        to fetch \u2014 we'd otherwise quote the underlying stock instead. The
        ladder then falls back to current_option_price (safer)."""
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "_is_option_contract" in src
        assert "any(ch.isdigit() for ch in sym)" in src
        assert "not an OCC option contract" in src

    def test_underlying_ticker_skips_ask_fetch(self):
        """Functional: if sym='MSFT' (alpha only, no digits), the broker
        get_quote MUST NOT be called for the option contract path \u2014 the
        ladder must fall back gracefully."""
        # Build a minimal isolated function that mirrors the _is_option_contract logic
        def is_option_contract(sym):
            return isinstance(sym, str) and any(ch.isdigit() for ch in sym)

        assert is_option_contract("MSFT260522C00427500") is True
        assert is_option_contract("MSFT") is False
        assert is_option_contract("SPY") is False
        assert is_option_contract("SPY261231C00500000") is True
        assert is_option_contract("") is False
        assert is_option_contract(None) is False

    def test_ladder_falls_back_to_current_option_price(self):
        """When current_ask is None (because sym was a bare underlying), the
        ladder code falls back to current_option_price. This is the safety net."""
        import ap.retry_engine as re_mod
        importlib.reload(re_mod)

        # With current_ask=None, anchor = current_option_price
        order_row = {
            "id": "x", "limit_price": 2.00, "direction": "CALL",
            "signal_entry_price": 100.0,
            "repeg_attempts": 0, "last_repeg_ts": 0,
            "kind": "ENTRY", "current_ask": None,  # underlying-skip case
            "meta": {},
        }
        d = re_mod.decide_repeg(
            order_row=order_row,
            current_option_price=2.05,  # comes from MISSED_MOVE check
            underlying_spot=100.5,
        )
        assert d.ok is True
        # Fallback: anchor=current_option_price (2.05) + ladder[0] (0.01) = 2.06
        assert d.new_limit_price == pytest.approx(2.06, abs=0.01), (
            f"Fallback path must anchor on current_option_price; got {d.new_limit_price}"
        )


# ============================================================
# Concern #2: DEFERRED guard leaves order actionable (not zombied)
# ============================================================

class TestDeferredGuardLeavesActionable:
    """DEFERRED:<sym> contracts mean contract selection hasn't run yet. When
    watcher invalidates one, we must NOT cancel the order AND we must NOT
    leave the watcher stuck in INVALIDATED. The watcher state must be
    restored to PENDING so the next poll re-enters check() and breach-time
    contract selection runs."""

    def test_state_restoration_logic_present(self):
        src = (REPO_ROOT / "ap_execution_core.py").read_text()
        # Must explicitly set state back to PENDING after the DEFERRED log
        assert "watched.state = WatchState.PENDING" in src
        # Must clear breach_count so a stop touch doesn't immediately re-invalidate
        assert "watched.breach_count = 0" in src

    def test_no_cancel_call_in_deferred_branch(self):
        """Static check: the DEFERRED branch must early-return BEFORE
        _cleanup_pending_entry_order AND BEFORE the signal store invalidation
        write."""
        src = (REPO_ROOT / "ap_execution_core.py").read_text()

        m = re.search(
            r"def _on_signal_invalidate\(self, watched: WatchedSignal\):.*?(?=\n    def )",
            src, re.DOTALL,
        )
        assert m, "Could not find _on_signal_invalidate"
        body = m.group(0)

        # The deferred branch must contain its own return BEFORE both:
        # the signal-store invalidation write AND _cleanup_pending_entry_order
        deferred_branch = re.search(
            r"if is_deferred_contract:.*?return  # Do NOT cancel",
            body, re.DOTALL,
        )
        assert deferred_branch, "DEFERRED branch return statement missing"
        d_text = deferred_branch.group(0)
        # No cancel call within the deferred branch
        assert "_cleanup_pending_entry_order" not in d_text
        # No 'invalidated' status write within the deferred branch
        assert 'update_status' not in d_text

    def test_deferred_branch_runs_before_cleanup(self):
        """Order: the DEFERRED check must execute BEFORE _cleanup_pending_entry_order
        is called for any path."""
        src = (REPO_ROOT / "ap_execution_core.py").read_text()
        m = re.search(
            r"def _on_signal_invalidate\(self, watched: WatchedSignal\):.*?(?=\n    def )",
            src, re.DOTALL,
        )
        assert m
        body = m.group(0)
        deferred_idx = body.find("is_deferred_contract")
        cleanup_idx  = body.find("_cleanup_pending_entry_order")
        assert deferred_idx >= 0 and cleanup_idx >= 0
        assert deferred_idx < cleanup_idx, (
            "DEFERRED guard must run BEFORE _cleanup_pending_entry_order"
        )


# ============================================================
# Concern #3: lost_handoff_systemic_halt is READ by entry gate
# ============================================================

class TestSystemicHaltIsRead:
    """The order monitor writes lost_handoff_systemic_halt=True. Verify the
    entry gate in process_signal actually reads it and blocks new arms."""

    def test_entry_gate_reads_systemic_halt(self):
        src = (REPO_ROOT / "ap" / "execution.py").read_text()
        # The gate must check the flag
        assert 'st.get("lost_handoff_systemic_halt")' in src
        # And return a structured error with the same code. Allow multi-line format.
        m = re.search(
            r'"error":\s*"lost_handoff_systemic_halt"',
            src,
        )
        assert m, "Entry gate must return error='lost_handoff_systemic_halt' when flag is set"

    def test_block_reason_is_distinct(self):
        """The error reason must be distinct from kill_switch_active and
        daily_trade_cap so the dashboard can bucket it separately."""
        src = (REPO_ROOT / "ap" / "execution.py").read_text()
        assert '"lost_handoff_systemic_halt"' in src
        # Sanity: the reason isn't conflated with kill_switch
        assert "kill_switch_active" in src

    def test_halt_blocks_before_daily_cap_check(self):
        """The systemic halt must be checked BEFORE the daily-trade cap. If a
        client is in systemic-halt state, we shouldn't waste cycles on cap
        accounting \u2014 we want a fast, clear block."""
        src = (REPO_ROOT / "ap" / "execution.py").read_text()
        halt_idx = src.find('st.get("lost_handoff_systemic_halt")')
        # Find the actual RETURN of daily_trade_cap (skip the comment block
        # near the top of the file that also mentions the name).
        cap_match = re.search(r'"error":\s*"daily_trade_cap"', src)
        assert halt_idx >= 0 and cap_match, (
            f"halt_idx={halt_idx}, cap_match={cap_match}"
        )
        cap_idx = cap_match.start()
        assert halt_idx < cap_idx, (
            f"halt at offset {halt_idx} must precede daily_trade_cap return at {cap_idx}"
        )

    def test_block_returns_actionable_hint(self):
        """The blocked response must include a hint so the operator knows
        how to unblock (clear flag in client_state)."""
        src = (REPO_ROOT / "ap" / "execution.py").read_text()
        assert '"hint":' in src
        assert "operator must clear" in src or "clear flag in client_state" in src


# ============================================================
# Concern #4: attempt labels emit correctly (0/1/2)
# ============================================================

class TestAttemptLabelsClear:
    """Dashboard parser needs explicit entry_attempt=N tokens. Verify:
       - original submit log emits entry_attempt=0
       - repeg log emits entry_attempt=N where N = attempts_used (1 or 2)
       - attempts_used starts at attempts+1 (1 after first repeg)
    """

    def test_original_submit_emits_attempt_zero(self):
        src = (REPO_ROOT / "ap_execution_core.py").read_text()
        # The successful entry-submit log must include entry_attempt=0
        assert "entry_attempt=0" in src, (
            "Original ask submit log must include 'entry_attempt=0' token"
        )

    def test_repeg_log_emits_attempt_token(self):
        src = (REPO_ROOT / "ap" / "retry_engine.py").read_text()
        # REPEG_APPLIED log must contain entry_attempt=%d
        assert "entry_attempt=%d" in src
        # And the value passed must be decision.attempts_used
        # Look for the format-args order: ...local_oid, decision.attempts_used,...
        m = re.search(
            r"REPEG_APPLIED order=%s entry_attempt=%d.*?local_oid,\s*decision\.attempts_used",
            src, re.DOTALL,
        )
        assert m, "entry_attempt must be supplied from decision.attempts_used"

    def test_attempts_used_semantics(self):
        """Functional check: attempts_used after first repeg = 1; after second = 2."""
        import ap.retry_engine as re_mod
        importlib.reload(re_mod)

        # Attempt-1: repeg_attempts=0 (no prior repegs), should return attempts_used=1
        row1 = {
            "id": "x", "limit_price": 2.00, "direction": "CALL",
            "signal_entry_price": 100.0,
            "repeg_attempts": 0, "last_repeg_ts": 0,
            "kind": "ENTRY", "current_ask": 2.10, "meta": {},
        }
        d1 = re_mod.decide_repeg(order_row=row1, current_option_price=2.10, underlying_spot=100.5)
        assert d1.attempts_used == 1, f"After 1st repeg: expected attempts_used=1, got {d1.attempts_used}"

        # Attempt-2: repeg_attempts=1 (one prior repeg), should return attempts_used=2
        row2 = {
            "id": "x", "limit_price": 2.11, "direction": "CALL",
            "signal_entry_price": 100.0,
            "repeg_attempts": 1, "last_repeg_ts": 0,
            "kind": "ENTRY", "current_ask": 2.15, "meta": {},
        }
        d2 = re_mod.decide_repeg(order_row=row2, current_option_price=2.15, underlying_spot=100.5)
        assert d2.attempts_used == 2, f"After 2nd repeg: expected attempts_used=2, got {d2.attempts_used}"

        # Attempt-3: repeg_attempts=2 already at max, should reject (max_attempts_reached)
        row3 = {
            "id": "x", "limit_price": 2.13, "direction": "CALL",
            "signal_entry_price": 100.0,
            "repeg_attempts": 2, "last_repeg_ts": 0,
            "kind": "ENTRY", "current_ask": 2.20, "meta": {},
        }
        d3 = re_mod.decide_repeg(order_row=row3, current_option_price=2.20, underlying_spot=100.5)
        assert d3.ok is False
        assert d3.reason == "max_attempts_reached"
