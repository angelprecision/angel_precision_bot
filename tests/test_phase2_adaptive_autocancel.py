"""
Phase 2 regression tests: adaptive autocancel.

Spec:
  - 25s = re-evaluate, not hard kill
  - 90s normal max if still aligned
  - 120s A/A+ max if still aligned (score >= ENTRY_APLUS_SCORE_THRESHOLD)
  - Immediate cancel only for thesis_invalid, spread_wide, runaway_quote,
    positions_full, lost_handoff, risk_gate_blocked
  - Every cancel writes exact reason_code

Run:
    pytest tests/test_phase2_adaptive_autocancel.py -xvs
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


class TestAdaptiveAutocancelConstants:
    def test_three_env_thresholds_declared(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        # Phase 2 introduces three distinct age thresholds.
        assert "ENTRY_REEVAL_AGE_SECONDS" in src
        assert "ENTRY_MAX_AGE_NORMAL" in src
        assert "ENTRY_MAX_AGE_APLUS" in src

    def test_defaults_match_spec(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        # 25s re-eval, 90s normal, 120s A+
        assert re.search(r'ENTRY_REEVAL_AGE_SECONDS\s*=\s*int\(os\.getenv\(\s*"ENTRY_REEVAL_AGE_SECONDS"\s*,\s*"25"', src)
        assert re.search(r'ENTRY_MAX_AGE_NORMAL\s*=\s*int\(os\.getenv\(\s*"ENTRY_MAX_AGE_NORMAL"\s*,\s*"90"', src)
        assert re.search(r'ENTRY_MAX_AGE_APLUS\s*=\s*int\(os\.getenv\(\s*"ENTRY_MAX_AGE_APLUS"\s*,\s*"120"', src)

    def test_aplus_score_threshold_declared(self):
        """A/A+ tier needs a score threshold so order_monitor can tell which
        ceiling to apply."""
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "ENTRY_APLUS_SCORE_THRESHOLD" in src


class TestReevalNotHardCancelAt25s:
    """The 25s threshold must trigger re-evaluation, NOT a hard cancel.
    Prove by code shape: the 25s-age branch contains 'return False' (continue)
    in the re-eval path, NOT _handle_stale_entry."""

    def test_reeval_path_returns_false(self):
        """The Step 2 (between 25s and ceiling) branch must return False
        (continue), not return True (cancel)."""
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        # Find the Step 2 block — between '# Step 2:' marker and the next major comment.
        m = re.search(
            r"# Step 2: between 25s and the ceiling.*?return False",
            src, re.DOTALL,
        )
        assert m, "Step 2 re-eval block missing"
        block = m.group(0)
        # Must end in 'return False' (continue), not 'return True'
        assert block.rstrip().endswith("return False"), (
            "Re-eval path must return False (continue), not cancel"
        )
        # Must NOT call _handle_stale_entry in the re-eval branch
        assert "_handle_stale_entry" not in block, (
            "Re-eval branch must NOT call _handle_stale_entry"
        )

    def test_reeval_log_present(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "ENTRY_REEVAL" in src
        assert "order continues if aligned" in src

    def test_reeval_throttled_to_avoid_log_spam(self):
        """If we log re-eval on every poll between 25s and 90s, that's noise.
        Throttle: only emit once per 15s."""
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "last_reeval_ts" in src
        assert ">= 15:" in src or ">= 15.0:" in src or "_t.time() - _last_reeval >= 15" in src


class TestCeilingHardCancel:
    def test_90s_ceiling_for_normal_tier(self):
        """At 90s for a non-A+ setup, hard cancel with reason_code=ENTRY_MAX_AGE_NORMAL_REACHED."""
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "ENTRY_MAX_AGE_NORMAL_REACHED" in src

    def test_120s_ceiling_for_aplus_tier(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "ENTRY_MAX_AGE_APLUS_REACHED" in src

    def test_ceiling_block_calls_handle_stale_entry(self):
        """Step 1 (past ceiling) MUST call _handle_stale_entry to cancel."""
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        m = re.search(
            r"# Step 1: if past absolute ceiling.*?return True",
            src, re.DOTALL,
        )
        assert m, "Step 1 ceiling block missing"
        block = m.group(0)
        assert "_handle_stale_entry" in block, (
            "Ceiling block must call _handle_stale_entry"
        )
        assert "return True" in block, "Ceiling block must return True (canceled)"

    def test_tier_selection_uses_score(self):
        """The _max_age must come from ENTRY_MAX_AGE_APLUS when score >= threshold,
        else ENTRY_MAX_AGE_NORMAL."""
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "_max_age = ENTRY_MAX_AGE_APLUS if _is_aplus else ENTRY_MAX_AGE_NORMAL" in src
        assert "_is_aplus = _score >= ENTRY_APLUS_SCORE_THRESHOLD" in src


class TestReasonCodeOnEveryCancel:
    """Every cancel path in adaptive autocancel must emit a structured
    reason_code (so dashboard can bucket)."""

    def test_normal_ceiling_emits_event_with_reason_code(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        m = re.search(r"# Step 1: if past absolute ceiling.*?return True", src, re.DOTALL)
        assert m
        block = m.group(0)
        assert "reason_code=_ceiling_reason" in block, (
            "Ceiling cancel must emit decision_event with explicit reason_code"
        )
        # The _ceiling_reason variable must be one of the two ceiling codes.
        assert "ENTRY_MAX_AGE_APLUS_REACHED" in block or "ENTRY_MAX_AGE_NORMAL_REACHED" in block

    def test_reeval_continue_emits_event(self):
        """Re-eval branch must also emit a decision_event so dashboard can see
        the orders that lived past 25s."""
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        m = re.search(r"# Step 2: between 25s and the ceiling.*?return False", src, re.DOTALL)
        assert m
        block = m.group(0)
        assert 'reason_code="ENTRY_REEVAL"' in block
        assert 'decision="CONTINUE"' in block
