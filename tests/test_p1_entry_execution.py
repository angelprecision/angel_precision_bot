"""
Regression tests for the 2026-05-21 P1 entry execution fix.

Background: prod funnel was 348 canceled / 44 expired / 2 filled on 392 entries.
Root causes: Attempt-0 used mid-blend pricing, repeg ladder waited 30s, no
LOST_HANDOFF detection until 120s, watcher_invalidated had no forensic context,
DEFERRED contract failures were treated as true thesis breaks.

Tests prove each fix in code shape and behavior. Run:
    pytest tests/test_p1_entry_execution.py -xvs
"""
from __future__ import annotations

import os
import re
import sys
import importlib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


# ============================================================
# FIX A — Attempt-0 pricing uses ask in LIVE mode
# ============================================================

class TestAttemptZeroPricing:
    def test_live_ask_path_present(self):
        src = (REPO_ROOT / "ap" / "contract_selector.py").read_text()
        # The new ask-execution branch must exist
        assert "is_live and _attempt0_mode == \"ASK\" and ask > 0" in src
        assert "ASK_EXECUTION" in src

    def test_env_var_documented(self):
        src = (REPO_ROOT / "ap" / "contract_selector.py").read_text()
        assert "ENTRY_ATTEMPT0_PRICING" in src

    def test_scoring_price_still_mid(self):
        """Scoring/ranking MUST stay on mid so cross-name comparisons remain
        comparable. Only execution_price_per_share is allowed to use ask."""
        src = (REPO_ROOT / "ap" / "contract_selector.py").read_text()
        assert "scoring_price_per_share = mid" in src

    def test_paper_mode_keeps_mid_simulation(self):
        """Paper mode must still use the blend so backtests stay comparable."""
        # The is_live gate ensures this — paper mode falls through to the
        # legacy spread-tiered branches.
        src = (REPO_ROOT / "ap" / "contract_selector.py").read_text()
        # The ASK_EXECUTION branch is guarded by is_live
        assert re.search(r"if is_live and _attempt0_mode == \"ASK\"", src)


# ============================================================
# FIX B — Repeg ladder ask + 0.01, ask + 0.02
# ============================================================

class TestRepegLadder:
    def test_default_interval_is_6s(self):
        src = (REPO_ROOT / "ap" / "retry_engine.py").read_text()
        # REPEG_INTERVAL_SECS default must be 6
        assert re.search(r'REPEG_INTERVAL_SECS\s*=\s*int\(os\.getenv\(\s*"REPEG_INTERVAL_SECS"\s*,\s*"6"\s*\)\)', src)

    def test_ladder_constant_exists(self):
        src = (REPO_ROOT / "ap" / "retry_engine.py").read_text()
        assert "ENTRY_REPEG_LADDER" in src
        assert "ENTRY_REPEG_LADDER_PENNIES" in src

    def test_default_ladder_is_penny_two_penny(self):
        src = (REPO_ROOT / "ap" / "retry_engine.py").read_text()
        m = re.search(r'ENTRY_REPEG_LADDER_PENNIES\s*=\s*os\.getenv\(\s*"ENTRY_REPEG_LADDER_PENNIES"\s*,\s*"([^"]+)"\s*\)', src)
        assert m
        assert m.group(1) == "0.01,0.02"

    def test_ladder_branch_uses_kind_entry(self):
        src = (REPO_ROOT / "ap" / "retry_engine.py").read_text()
        # The new branch must check kind == "ENTRY"
        assert "kind == \"ENTRY\" and ENTRY_REPEG_LADDER" in src

    def test_caller_passes_kind_and_current_ask(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        # The _try_repeg builder must include kind + current_ask in order_row
        assert "\"kind\":" in src and "or \"ENTRY\")" in src
        assert "\"current_ask\":" in src

    def test_decide_repeg_ladder_math(self):
        """Functional test of the actual ladder computation."""
        # Reset module state in case prior tests mutated env
        import ap.retry_engine as re_mod
        importlib.reload(re_mod)

        order_row = {
            "id": "x", "limit_price": 2.00, "direction": "CALL",
            "signal_entry_price": 100.0,
            "repeg_attempts": 0, "last_repeg_ts": 0,
            "kind": "ENTRY", "current_ask": 2.10,
            "meta": {},
        }
        d = re_mod.decide_repeg(
            order_row=order_row,
            current_option_price=2.10,
            underlying_spot=100.5,
        )
        assert d.ok is True
        # Attempt-1: ask (2.10) + ladder[0] (0.01) = 2.11
        assert d.new_limit_price == pytest.approx(2.11, abs=0.01), (
            f"Expected ask+0.01 ladder => 2.11; got {d.new_limit_price}"
        )
        assert d.attempts_used == 1
        assert "entry_ladder" in d.reason

    def test_decide_repeg_ladder_second_attempt(self):
        import ap.retry_engine as re_mod
        importlib.reload(re_mod)

        order_row = {
            "id": "x", "limit_price": 2.11, "direction": "CALL",
            "signal_entry_price": 100.0,
            "repeg_attempts": 1, "last_repeg_ts": 0,
            "kind": "ENTRY", "current_ask": 2.15,
            "meta": {},
        }
        d = re_mod.decide_repeg(
            order_row=order_row,
            current_option_price=2.15,
            underlying_spot=100.5,
        )
        assert d.ok is True
        # Attempt-2: ask (2.15) + ladder[1] (0.02) = 2.17
        assert d.new_limit_price == pytest.approx(2.17, abs=0.01), (
            f"Expected ask+0.02 ladder => 2.17; got {d.new_limit_price}"
        )
        assert d.attempts_used == 2

    def test_exit_orders_use_legacy_gap_close(self):
        """EXIT orders MUST NOT use the entry ladder — they're closing positions
        and the gap-close formula is correct for them."""
        import ap.retry_engine as re_mod
        importlib.reload(re_mod)

        order_row = {
            "id": "x", "limit_price": 2.00, "direction": "CALL",
            "signal_entry_price": 100.0,
            "repeg_attempts": 0, "last_repeg_ts": 0,
            "kind": "EXIT", "current_ask": 2.10,
            "meta": {},
        }
        d = re_mod.decide_repeg(
            order_row=order_row,
            current_option_price=2.10,
            underlying_spot=100.5,
        )
        assert d.ok is True
        # Legacy formula: limit + 0.5 * (current_option - limit) = 2.00 + 0.05 = 2.05
        assert d.new_limit_price == pytest.approx(2.05, abs=0.01), (
            f"EXIT orders must use gap-close, not ladder; got {d.new_limit_price}"
        )
        assert d.reason == "aligned_repeg"


# ============================================================
# FIX C — LOST_HANDOFF_30S + systemic guard
# ============================================================

class TestLostHandoffSystemic:
    def test_timeout_created_default_is_30s(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert re.search(
            r'ORDER_TIMEOUT_CREATED"\s*,\s*"30"\s*\)',
            src,
        ), "TIMEOUT_CREATED default must be 30 (was 120)"

    def test_entry_limit_max_age_default_is_25s(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert re.search(
            r'ENTRY_LIMIT_MAX_AGE_SECONDS"\s*,\s*"25"\s*\)',
            src,
        ), "ENTRY_LIMIT_MAX_AGE_SECONDS default must be 25 (was 150)"

    def test_missed_move_default_is_6s(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        # Look for both occurrences
        sixes = re.findall(r'MISSED_MOVE_MIN_SECS"[^)]*"6"', src)
        assert len(sixes) >= 2, "MISSED_MOVE_MIN_SECS must default to 6 in all reads"

    def test_lost_handoff_30s_reason_code(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        # Reason must include LOST_HANDOFF_30S
        assert "LOST_HANDOFF_30S" in src

    def test_systemic_helper_exists(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "def _record_lost_handoff_and_check_systemic" in src
        assert "LOST_HANDOFF_SYSTEMIC" in src
        assert "_LOST_HANDOFF_WINDOW_SEC" in src
        assert "_LOST_HANDOFF_SYSTEMIC_THRESH" in src

    def test_systemic_threshold_is_three_in_five_min(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "_LOST_HANDOFF_WINDOW_SEC      = 300" in src
        assert "_LOST_HANDOFF_SYSTEMIC_THRESH = 3" in src

    def test_one_shot_per_window(self):
        """Systemic event must NOT loop infinitely. Single _lost_handoff_systemic_active
        guard ensures only ONE self-heal attempt per window."""
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "_lost_handoff_systemic_active" in src


# ============================================================
# FIX D — watcher_invalidated forensic context + DEFERRED guard
# ============================================================

class TestWatcherInvalidatedContext:
    def test_deferred_contract_guard_present(self):
        src = (REPO_ROOT / "ap_execution_core.py").read_text()
        assert "DEFERRED_CONTRACT_INVALIDATED" in src
        assert "is_deferred_contract" in src
        # The guard must return early without canceling
        assert "Do NOT cancel" in src or "not treating as true thesis break" in src

    def test_full_forensic_log_present(self):
        src = (REPO_ROOT / "ap_execution_core.py").read_text()
        # All required forensic fields must appear in the log format string
        required = [
            "WATCHER_INVALIDATED",
            "signal_id=",
            "plan_id=",
            "local_order_id=",
            "client_id=",
            "symbol=",
            "contract=",
            "side=",
            "trigger=",
            "underlying=",
            "opt_bid=",
            "opt_ask=",
            "opt_mid=",
            "stop=",
            "target=",
            "age=",
        ]
        for tok in required:
            assert tok in src, f"Required forensic token '{tok}' missing from watcher_invalidated log"

    def test_deferred_path_returns_before_cleanup(self):
        """The DEFERRED branch must return BEFORE _cleanup_pending_entry_order
        so the order isn't permanently canceled."""
        src = (REPO_ROOT / "ap_execution_core.py").read_text()
        # Extract the _on_signal_invalidate function body
        m = re.search(
            r"def _on_signal_invalidate\(self, watched: WatchedSignal\):.*?(?=\n    def |\n\nclass )",
            src, re.DOTALL,
        )
        assert m, "Could not find _on_signal_invalidate"
        body = m.group(0)
        # The DEFERRED return must come before _cleanup_pending_entry_order
        deferred_idx = body.find("DEFERRED_CONTRACT_INVALIDATED")
        cleanup_idx  = body.find("_cleanup_pending_entry_order")
        assert deferred_idx >= 0
        assert cleanup_idx > deferred_idx, (
            "DEFERRED branch must early-return BEFORE _cleanup_pending_entry_order. "
            "Otherwise DEFERRED contracts get permanently canceled."
        )


# ============================================================
# Acceptance: env vars match the spec
# ============================================================

class TestEnvVarTargets:
    """Verify the spec values are the in-code defaults so Render-default deploys
    behave correctly even without operator env overrides."""

    @pytest.mark.parametrize("var,expected,file_path", [
        ("ORDER_TIMEOUT_CREATED",          "30",  "ap/order_monitor.py"),
        ("ORDER_TIMEOUT_CREATED_NO_BROKER_WARN", "10", "ap/order_monitor.py"),
        ("ENTRY_LIMIT_MAX_AGE_SECONDS",    "25",  "ap/order_monitor.py"),
        ("REPEG_INTERVAL_SECS",            "6",   "ap/retry_engine.py"),
        ("REPEG_MAX_ATTEMPTS",             "2",   "ap/retry_engine.py"),
    ])
    def test_default(self, var, expected, file_path):
        src = (REPO_ROOT / file_path).read_text()
        pat = rf'getenv\(\s*"{var}"\s*,\s*"{expected}"\s*\)'
        assert re.search(pat, src), f"{var} default must be {expected!r} in {file_path}"
