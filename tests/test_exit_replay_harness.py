"""
tests/test_exit_replay_harness.py
==============================================================================
Replay tests — prove floors and trails fire on known price paths
before risking real client money.

Run:  pytest tests/test_exit_replay_harness.py -v
==============================================================================
"""
from __future__ import annotations
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Zero the breach-confirmation window for replay so tests prove that the
# correct exit fires on the right price levels, not how long evaluate_exit
# waits for confirmation. The production 45s window is a timing concern
# verified elsewhere. MUST be set before the harness/exit-engine import
# so evaluate_exit's os.getenv() reads it.
#
# Codex P2 (2026-05-25): use direct assignment, NOT os.environ.setdefault().
# setdefault() makes these tests dependent on external process state — if
# CI/job env or a prior test left STOP_BREACH_CONFIRM_SECONDS at a non-zero
# value, scenarios expecting immediate stop behavior would fail non-
# deterministically. This file's assertions validate price-level logic
# independent of timing, so the env vars must be forced to "0" every run.
os.environ["STOP_BREACH_CONFIRM_SECONDS"] = "0"
os.environ["UNDERLYING_STOP_CONFIRM_SECONDS"] = "0"

import pytest

try:
    from ap.exit_replay_harness import replay_price_path
    HAS_ENGINE = True
except ImportError:
    HAS_ENGINE = False

skip = pytest.mark.skipif(not HAS_ENGINE, reason="exit engine not importable")


from datetime import datetime, timezone

# Pin replay clock to mid-session ET so EOD force-close never triggers.
# evaluate_exit reads now_et.hour directly without a tz conversion, so
# we pass a datetime whose .hour == 11 (well inside the 9:30-15:50
# session, regardless of the wall-clock tz the test runner uses).
_MID_SESSION_ET = datetime(2026, 1, 15, 11, 30, 0, tzinfo=timezone.utc)


def run(entry, path, qty=1):
    return replay_price_path(entry, path, qty=qty, now_et=_MID_SESSION_ET)


# ── Scenario 1: single contract profit floor 15% ────────────────────────────
@skip
def test_floor_15_fires():
    """Peak +22%, drops to +2% — profit floor at +15% trigger should exit."""
    r = run(1.00, [1.02, 1.08, 1.15, 1.22, 1.16, 1.10, 1.04, 1.02])
    assert r.exit_fired, f"floor should fire, peak={r.peak_pnl_pct*100:.1f}%"
    assert r.exit_pnl_pct > -0.05, f"should not exit in big loss: {r.exit_pnl_pct*100:.1f}%"


@skip
def test_floor_25_fires():
    """Peak +30%, drops to +8% — profit floor at +25% trigger exits at +10% min."""
    r = run(1.00, [1.05, 1.12, 1.20, 1.28, 1.30, 1.22, 1.14, 1.08])
    assert r.exit_fired, f"floor should fire, peak={r.peak_pnl_pct*100:.1f}%"
    assert r.exit_pnl_pct >= -0.02, f"floor should protect: {r.exit_pnl_pct*100:.1f}%"


# ── Scenario 2: single contract runner trail ─────────────────────────────────
@skip
def test_single_contract_trail_fires():
    """Peak +20%, drops 9% — 8% trail should fire."""
    r = run(1.00, [1.05, 1.10, 1.15, 1.20, 1.18, 1.14, 1.11])
    assert r.exit_fired, f"trail should fire, peak={r.peak_pnl_pct*100:.1f}%"


@skip
def test_no_exit_below_activation():
    """Peak only +8% — trail not armed, no exit expected."""
    r = run(1.00, [1.02, 1.04, 1.06, 1.08, 1.07, 1.06])
    # Either no exit or exit only via hard stop — not profit trail
    if r.exit_fired:
        assert "TRAIL" not in r.exit_reason.upper() and "FLOOR" not in r.exit_reason.upper(), \
            f"trail/floor should not fire at low peak: {r.exit_reason}"


# ── Scenario 3: multi-contract scale-out ────────────────────────────────────
@skip
def test_multi_contract_scale_out():
    """2 contracts, hits +15% — scale out should fire."""
    r = run(1.00, [1.05, 1.10, 1.16, 1.20, 1.18], qty=2)
    assert r.exit_fired, f"scale-out should fire for multi-contract at +15%"


# ── Scenario 4: hard stop ────────────────────────────────────────────────────
@skip
def test_hard_stop_fires():
    """-35% loss — hard stop must fire."""
    r = run(1.00, [0.98, 0.92, 0.85, 0.75, 0.65])
    assert r.exit_fired, "hard stop must fire on -35%"
    assert "STOP" in r.exit_reason.upper() or "HARD" in r.exit_reason.upper(), \
        f"should be a stop exit: {r.exit_reason}"


# ── Scenario 5: never-green stop ─────────────────────────────────────────────
@skip
def test_never_green_no_premature_exit():
    """Flat/slightly down trade — should not exit immediately."""
    r = run(1.00, [0.99, 0.98, 0.97, 0.96])
    # Should not fire hard stop at only -4%
    if r.exit_fired:
        pnl = r.exit_pnl_pct
        assert pnl < -0.25, f"too early exit at {pnl*100:.1f}%"
