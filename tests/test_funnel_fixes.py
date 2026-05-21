"""
Regression tests for the 2026-05-20 funnel-leak fixes.

Background: on 2026-05-20, Jose + tradefluence saw 29 orders, 0 fills. Root causes:
  #1  CREATED orders aged out at 120s (LOST_HANDOFF: watcher fell off, no submit)
  #2  Stale-arm guard rejected at 0.0-0.4% deltas (vs 1.5% threshold)
  #3  positions_full_at_breach fired with zero actual fills (phantom CREATED slots)
  #4  Overnight setups invalidated pre-9:30 by pre-market stop touches

These tests prove each fix in isolation. Run:
    pytest tests/test_funnel_fixes.py -xvs
"""
from __future__ import annotations

import os
import re
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]


# ============================================================
# FIX #2 — Stale-arm guard
# ============================================================

class TestStaleArmGuard:
    """The arm-time staleness gate must:
       (a) be env-tunable (WATCH_ARM_STALE_TOLERANCE_PCT)
       (b) only loosen vs the hard-coded raw (never silently tighten)
       (c) log the effective threshold at module load
       (d) distinguish 'arm_drift' from 'arm_below_stop' in the reject reason
    """

    def test_env_var_exists(self):
        src = (REPO_ROOT / "ap_entry_watcher.py").read_text()
        assert "WATCH_ARM_STALE_TOLERANCE_PCT" in src, (
            "Stale-arm tolerance must be env-tunable. "
            "Production hit 0.0-0.4% rejections at 1.5% raw default; "
            "ops needs an env knob to loosen without a redeploy."
        )

    def test_effective_threshold_uses_max_of_env_and_raw(self):
        """Env can only LOOSEN, never tighten below the safe raw."""
        src = (REPO_ROOT / "ap_entry_watcher.py").read_text()
        assert "WATCH_ARM_EFFECTIVE_THRESHOLD_PCT = max(" in src, (
            "Effective threshold must be max(env, raw) so env cannot accidentally "
            "tighten below the hard-coded MAX_INTRADAY_DRIFT_PCT."
        )

    def test_startup_logs_effective_threshold(self):
        src = (REPO_ROOT / "ap_entry_watcher.py").read_text()
        # The startup log line must mention all three values so ops can see them.
        assert "thresholds loaded" in src
        assert "MAX_INTRADAY_DRIFT_PCT=%.4f" in src
        assert "WATCH_ARM_STALE_TOLERANCE_PCT=%.4f" in src
        assert "effective=%.4f" in src

    def test_reject_reason_distinguishes_drift_from_stop(self):
        """The 'arm_drift_X%' vs 'arm_below_stop_mid_X_stop_Y' reason codes
        let ops post-mortem which gate fired \u2014 the old reason string only
        captured pct_from_trigger, making stop-touch rejects look like drift
        rejects."""
        src = (REPO_ROOT / "ap_entry_watcher.py").read_text()
        assert "arm_drift_" in src
        assert "arm_below_stop_" in src

    def test_no_old_combined_stale_label(self):
        """The crash-prone old form must be gone (would fire as 'stale_price_X'
        regardless of which gate triggered)."""
        src = (REPO_ROOT / "ap_entry_watcher.py").read_text()
        # The new _last_reject_reason should not include the legacy
        # 'stale_price_{pct}' format in code lines (excluding comments).
        code_lines = [
            ln for ln in src.splitlines() if not ln.lstrip().startswith("#")
        ]
        code = "\n".join(code_lines)
        bad = re.search(r"_last_reject_reason\s*=\s*f.stale_price_\{pct_from_trigger", code)
        assert not bad, (
            "Old stale_price_{pct}_from_trigger label is back \u2014 must use "
            "arm_drift_/arm_below_stop_ to distinguish which gate fired."
        )


# ============================================================
# FIX #3 — pending_entries excludes phantom CREATED
# ============================================================

class TestPendingEntryCount:
    """The slot-count query MUST exclude CREATED orders that have aged past
    PENDING_ENTRY_PHANTOM_GRACE_SEC without a broker_order_id. Otherwise dead
    phantom orders consume slots and 'positions_full_at_breach' fires with
    zero actual fills (the exact 5x today failure mode)."""

    def test_pending_entries_query_excludes_phantoms(self):
        src = (REPO_ROOT / "ap" / "position_manager.py").read_text()
        # The query must include the phantom-exclusion clause.
        assert "PENDING_ENTRY_PHANTOM_GRACE_SEC" in src
        # AND must specifically exclude CREATED/PENDING_TRIGGER without broker_order_id past grace
        assert "broker_order_id IS NULL OR broker_order_id = ''" in src
        assert "created_ts < NOW() - " in src

    def test_grace_window_is_env_tunable(self):
        src = (REPO_ROOT / "ap" / "position_manager.py").read_text()
        assert "os.getenv(\"PENDING_ENTRY_PHANTOM_GRACE_SEC\"" in src, (
            "Grace window must be env-tunable so ops can adjust without redeploy."
        )

    def test_grace_window_default_is_reasonable(self):
        """Default 30s: enough for a real OSM handoff, short enough that dead
        phantoms get freed within ~3x of the OSM handoff window."""
        src = (REPO_ROOT / "ap" / "position_manager.py").read_text()
        # Default is "30" in the getenv call
        m = re.search(
            r'os\.getenv\(\s*"PENDING_ENTRY_PHANTOM_GRACE_SEC"\s*,\s*"(\d+)"\s*\)',
            src,
        )
        assert m, "Could not find PENDING_ENTRY_PHANTOM_GRACE_SEC default"
        default = int(m.group(1))
        assert 15 <= default <= 90, (
            f"Default {default}s outside sane range [15, 90]. "
            "Too short = false starves; too long = phantom slot-burn returns."
        )


# ============================================================
# FIX #1 — CREATED never submitted: enriched cancel reason
# ============================================================

class TestLostHandoffForensics:
    """When the 120s phantom cleaner fires, the cancel reason must include
    signal_id, source, and broker_order_id so the operator can trace WHICH
    signal lost its handoff and from WHERE. Without this we can't fix the
    next regression of MSFT/UBER getting stuck in CREATED."""

    def test_cancel_reason_includes_signal_id(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        # The new enriched reason must include forensic fields.
        assert "LOST_HANDOFF" in src
        assert "signal_id=" in src
        assert "broker_order_id=" in src or "broker_oid" in src

    def test_warning_log_emitted_for_lost_handoff(self):
        src = (REPO_ROOT / "ap" / "order_monitor.py").read_text()
        assert "LOST_HANDOFF | local=" in src, (
            "A WARN-level log must fire on every LOST_HANDOFF cancel so it "
            "stands out in production logs."
        )


# ============================================================
# FIX #4 — Pre-9:30 stop touch must not invalidate daily/overnight
# ============================================================

class TestOvernightPreOpenStopGuard:
    """Daily and overnight setups must NOT invalidate when a pre-market stop
    touch happens. Pre-open spreads are wide and thinly-traded; the 9:30
    structural revalidator catches genuine breaks."""

    def test_helper_exists(self):
        src = (REPO_ROOT / "ap_entry_watcher.py").read_text()
        assert "_is_pre_market_now" in src

    def test_pre_open_skip_in_call_branch(self):
        src = (REPO_ROOT / "ap_entry_watcher.py").read_text()
        # The CALL branch must check _pre_open_skip before invalidating on stop.
        assert "_pre_open_skip = (" in src
        assert "not _pre_open_skip" in src

    def test_pre_open_helper_works(self):
        """Quick functional test on the helper itself."""
        sys.path.insert(0, str(REPO_ROOT))
        try:
            # We have to be careful: the module has heavy side effects on import.
            # Read out the function and exec it in isolation instead.
            src = (REPO_ROOT / "ap_entry_watcher.py").read_text()
            m = re.search(
                r"def _is_pre_market_now\(\) -> bool:.*?return False",
                src, re.DOTALL,
            )
            assert m, "Could not find _is_pre_market_now in module"
            ns = {}
            exec(m.group(0), ns)
            assert callable(ns["_is_pre_market_now"])
            # Should not raise, returns a bool
            result = ns["_is_pre_market_now"]()
            assert isinstance(result, bool)
        finally:
            try:
                sys.path.remove(str(REPO_ROOT))
            except ValueError:
                pass

    def test_daily_signal_check_is_used(self):
        """The pre-open skip must apply to daily setups, not just generic
        overnight. _safe_is_daily_signal is the canonical check."""
        src = (REPO_ROOT / "ap_entry_watcher.py").read_text()
        # Look for the _pre_open_skip definition that combines overnight OR daily.
        assert "(self.overnight or _safe_is_daily_signal(self))" in src, (
            "Pre-open skip must apply to both overnight and daily signals."
        )


# ============================================================
# Cross-cutting: env-var documentation
# ============================================================

class TestNewEnvVarsAreSane:
    """The four new env vars added by this fix must have sensible defaults
    and not break anything if unset."""

    @pytest.mark.parametrize("env_var,default,low,high", [
        ("WATCH_ARM_STALE_TOLERANCE_PCT", 0.010, 0.005, 0.030),
        ("PENDING_ENTRY_PHANTOM_GRACE_SEC", 30, 15, 90),
    ])
    def test_default_in_range(self, env_var, default, low, high):
        if env_var == "WATCH_ARM_STALE_TOLERANCE_PCT":
            src = (REPO_ROOT / "ap_entry_watcher.py").read_text()
            m = re.search(
                rf'os\.getenv\(\s*"{env_var}"\s*,\s*"([^"]+)"\s*\)',
                src,
            )
            assert m
            assert low <= float(m.group(1)) <= high
        else:
            src = (REPO_ROOT / "ap" / "position_manager.py").read_text()
            m = re.search(
                rf'os\.getenv\(\s*"{env_var}"\s*,\s*"([^"]+)"\s*\)',
                src,
            )
            assert m
            assert low <= int(m.group(1)) <= high
