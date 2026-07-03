"""
P0 (2026-07-02): DEFERRED PENDING_TRIGGER ghost sweep.

Forensics: ghost DEFERRED rows recurred twice in production (4 rows
hand-terminalized in a prior incident; 9 rows on 2026-07-02 reserving
~$1,712 of phantom live capital 8h after creation). The rows were exempt
from the same-day EOD expiry BY DESIGN (overnight carry) but nothing ever
closed the loop: they blocked capital AND suppressed the armed-deferred
rescue's NOT-EXISTS dedup for the same signal.

The sweep terminalizes DEFERRED PENDING_TRIGGER rows with no broker order
only after they cross a full session boundary (past EOD cutoff) or a hard
age ceiling — the overnight carry window is preserved.
"""

import pathlib
import re

# Repo pattern (see test_pending_trigger_orphan_guard.py): read source
# directly — importing ap.order_monitor triggers DB init at module load.
_REPO = pathlib.Path(__file__).resolve().parents[1]
SRC = (_REPO / "ap" / "order_monitor.py").read_text()
SWEEP = re.search(
    r"def _sweep_stale_deferred_ghosts\(self\).*?(?=\n    def )", SRC, re.S
).group(0)


# ── Compare-and-swap write guard ─────────────────────────────────────────────

def test_update_reasserts_full_eligibility_at_write_time():
    """
    The terminal UPDATE must re-check status, broker_order_id, DEFERRED
    contract, and kind in its WHERE clause — a row that breaches or
    materializes between read and write must be untouchable. This is the
    guard whose absence caused split-brain risk in the hydration path.
    """
    where = re.search(r"UPDATE orders(.*?)RETURNING", SWEEP, re.S).group(1)
    assert "status = 'PENDING_TRIGGER'" in where
    assert "broker_order_id IS NULL OR broker_order_id = ''" in where
    assert "UPPER(contract) LIKE 'DEFERRED:%%'" in where
    assert "kind = 'ENTRY'" in where
    assert "client_id = %s" in where
    assert "created_ts < %s" in where


def test_terminal_state_is_expired_with_forensic_reason():
    assert "SET status = 'EXPIRED'" in SWEEP
    assert "eod_deferred_never_triggered:" in SWEEP


def test_sweep_writes_audit_meta_and_returns_rows():
    assert "deferred_ghost_sweep" in SWEEP
    assert "RETURNING local_order_id, symbol, reserved_cost, created_ts" in SWEEP


# ── Threshold semantics (overnight carry preserved) ──────────────────────────

def test_session_boundary_rule_only_applies_past_eod_cutoff():
    """
    Before the EOD cutoff, ONLY the hard age ceiling may sweep. The morning
    reeval/handoff/rescue window must always get its chance at yesterday's
    rows before they die.
    """
    m = re.search(
        r"if self\._is_after_pt_eod_cutoff\(\):(.*?)else:(.*?)import json",
        SWEEP,
        re.S,
    )
    assert m, "threshold branch missing"
    past_cutoff_branch, before_cutoff_branch = m.group(1), m.group(2)
    assert "_today_et_start" in past_cutoff_branch
    assert "EOD_DEFERRED_GHOST_MAX_AGE_HOURS" in before_cutoff_branch
    assert "_today_et_start" not in before_cutoff_branch


def test_same_day_rows_never_swept_by_session_rule():
    """
    Threshold when past cutoff is the START of today's ET session — rows
    created today survive tonight (carry into tomorrow's morning window).
    """
    assert "_today_et_start = _now_et.replace(" in SWEEP
    assert "hour=0, minute=0, second=0, microsecond=0" in SWEEP


def test_defaults():
    assert '"EOD_DEFERRED_GHOST_SWEEP_ENABLED", "1"' in SRC
    assert '"EOD_DEFERRED_GHOST_MAX_AGE_HOURS", "48"' in SRC
    assert '"EOD_DEFERRED_GHOST_SWEEP_INTERVAL_SECONDS", "600"' in SRC


# ── Safety fences ────────────────────────────────────────────────────────────

def test_sweep_never_raises_and_fails_conservative():
    assert "except Exception as _sweep_exc" in SWEEP
    assert "return 0" in SWEEP
    # ET resolution failure must sweep NOTHING, not everything.
    assert re.search(r"except ImportError:\s*\n\s*return 0", SWEEP)


def test_kill_switch():
    assert SWEEP.strip().startswith("def _sweep_stale_deferred_ghosts")
    assert "if not EOD_DEFERRED_GHOST_SWEEP_ENABLED:" in SWEEP


def test_loop_hook_is_throttled_and_isolated():
    run_src = re.search(r"def _run\(self\).*?(?=\n    def )", SRC, re.S).group(0)
    assert "_sweep_stale_deferred_ghosts" in run_src
    assert "EOD_DEFERRED_GHOST_SWEEP_INTERVAL_SECONDS" in run_src
    # Failure isolation: sweep is inside its own try/except in the loop.
    assert re.search(
        r"try:\s*\n\s*self\._sweep_stale_deferred_ghosts\(\)\s*\n\s*except Exception",
        run_src,
    )


def test_no_broker_calls_in_sweep():
    """The sweep is a DB-only capital reclaim — it must never touch the broker."""
    assert "self.broker" not in SWEEP
    assert "cancel" not in SWEEP.lower()
