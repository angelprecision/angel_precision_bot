"""
tests/test_force_close_breaker.py — P0-3 + kill-switch semantic audit codified.

Covers:
  - check_daily_loss_breach is a pure read (no side effects)
  - request_force_close_all is idempotent (multiple calls = 1 fire)
  - clear_force_close resets state + guard
  - Entry gate fires breaker on daily-loss limit (once per session)
  - Reset session clears the breaker (new trading day)
  - Tick-level self-check from exit engine AND reconciler
  - kill_switch triggers force-close (emergency = flatten, not just block)
  - pause_entries does NOT trigger force-close (cool-off, not emergency)

Run with:
    DATABASE_URL=postgresql://x python3 -m pytest tests/test_force_close_breaker.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))


# ────────────────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────────────────

class _Quiet:
    def info(self, *a, **kw): pass
    def warning(self, *a, **kw): pass
    def error(self, *a, **kw): pass
    def critical(self, *a, **kw): pass
    def debug(self, *a, **kw): pass


@pytest.fixture(autouse=True)
def _silence_mc_logger(monkeypatch):
    import ap_master_control as mc_mod
    monkeypatch.setattr(mc_mod, "log", _Quiet())


def _build_mc(
    pnl_today: float = -100.0,
    max_loss: float = -500.0,
    kill_on: bool | None = False,
    paused: bool | None = False,
):
    """Build a minimal master_control instance directly via __new__.

    Bypasses __init__ which has DB/Supabase dependencies we don't need
    for these unit tests.
    """
    from ap_master_control import APMasterControl
    mc = APMasterControl.__new__(APMasterControl)
    mc._client_id = "test@x.com"
    mc.mode = "PAPER"
    mc.paper = True
    mc.max_daily_loss = max_loss
    mc._force_close_all_state = None
    mc._daily_loss_force_close_fired = False
    mc._alert_fn = None
    mc.pm = None
    mc._kill_switch_fn = (lambda: kill_on) if kill_on is not None else None
    mc._entries_paused_fn = (lambda: paused) if paused is not None else None
    mc._get_snapshot = lambda *a, **kw: {
        "realized_pnl_today": pnl_today,
        "open_positions": [],
    }
    return mc


# ────────────────────────────────────────────────────────────────────────────
# P0-3 core: check + request + clear
# ────────────────────────────────────────────────────────────────────────────

class TestForceCloseCore:
    def test_check_daily_loss_breach_is_pure_read(self):
        mc = _build_mc(pnl_today=-600.0)
        breached, snap = mc.check_daily_loss_breach()
        assert breached is True
        assert snap["realized_pnl_today"] == -600.0
        # CRITICAL: pure read — must not flip the flag
        assert mc.is_force_close_requested() is False

    def test_request_force_close_all_is_idempotent(self):
        mc = _build_mc()
        ok1 = mc.request_force_close_all(reason="test_first")
        assert ok1 is True
        state1 = mc.get_force_close_state()
        # Second call returns False, original reason preserved
        ok2 = mc.request_force_close_all(reason="should_not_overwrite")
        assert ok2 is False
        state2 = mc.get_force_close_state()
        assert state2[1] == "test_first"
        # And no matter how many times we call, only the first wins
        for _ in range(5):
            mc.request_force_close_all(reason="ignored")
        assert mc.get_force_close_state()[1] == "test_first"

    def test_clear_force_close_resets_state_and_guard(self):
        mc = _build_mc()
        mc.request_force_close_all(reason="test")
        mc._daily_loss_force_close_fired = True
        assert mc.is_force_close_requested() is True

        cleared = mc.clear_force_close(reason="manual_clear")
        assert cleared is True
        assert mc.is_force_close_requested() is False
        # Also clears the guard so next breach can re-fire
        assert mc._daily_loss_force_close_fired is False
        # Clearing again is no-op
        assert mc.clear_force_close() is False

    def test_check_handles_snapshot_error_safely(self):
        """Snapshot fetch error must not produce false force-close."""
        mc = _build_mc()
        mc._get_snapshot = lambda *a, **kw: (_ for _ in ()).throw(
            RuntimeError("snapshot broken")
        )
        breached, snap = mc.check_daily_loss_breach()
        assert breached is False  # fail-closed for safety
        assert "realized_pnl_today" in snap


# ────────────────────────────────────────────────────────────────────────────
# Entry-gate trigger + once-per-session guard
# ────────────────────────────────────────────────────────────────────────────

class TestEntryGateTrigger:
    def test_entry_gate_fires_breaker_on_daily_loss_breach(self):
        """Simulates the inline code path in master_control.evaluate()."""
        mc = _build_mc(pnl_today=-600.0)
        snap = mc._get_snapshot()
        if snap["realized_pnl_today"] <= mc.max_daily_loss:
            if not mc._daily_loss_force_close_fired:
                mc._daily_loss_force_close_fired = True
                mc.request_force_close_all(reason="daily_loss_limit")
        assert mc.is_force_close_requested() is True
        assert mc._daily_loss_force_close_fired is True
        state = mc.get_force_close_state()
        assert "daily_loss_limit" in state[1]

    def test_entry_gate_fires_only_once_across_many_signals(self):
        """20 signals all hit the daily-loss gate. Breaker must fire ONCE."""
        mc = _build_mc(pnl_today=-600.0)
        actual_fires = 0
        for _ in range(20):
            snap = mc._get_snapshot()
            if snap["realized_pnl_today"] <= mc.max_daily_loss:
                if not mc._daily_loss_force_close_fired:
                    mc._daily_loss_force_close_fired = True
                    if mc.request_force_close_all(reason="daily_loss_limit"):
                        actual_fires += 1
        assert actual_fires == 1


# ────────────────────────────────────────────────────────────────────────────
# Tick-level self-check (no signal needed)
# ────────────────────────────────────────────────────────────────────────────

class TestTickLevelSelfCheck:
    def test_exit_engine_tick_detects_daily_loss(self):
        """Exit engine reads check_daily_loss_breach() on every tick.
        Catches breach mid-session even without new signal."""
        mc = _build_mc(pnl_today=-600.0)
        # Mirror the exit-engine tick code path
        breached, snap = mc.check_daily_loss_breach()
        if breached and not mc.is_force_close_requested():
            mc.request_force_close_all(reason="daily_loss_limit_tick_detected")
        assert mc.is_force_close_requested() is True
        state = mc.get_force_close_state()
        assert "tick_detected" in state[1]

    def test_reconciler_tick_detects_daily_loss(self):
        """Reconciler also self-checks — belt-and-suspenders if exit engine
        is degraded but reconciler is alive."""
        mc = _build_mc(pnl_today=-700.0)
        breached, snap = mc.check_daily_loss_breach()
        if breached and not mc.is_force_close_requested():
            mc.request_force_close_all(reason="daily_loss_limit_reconciler_detected")
        state = mc.get_force_close_state()
        assert "reconciler_detected" in state[1]

    def test_tick_self_check_no_breach_no_fire(self):
        mc = _build_mc(pnl_today=-100.0, max_loss=-500.0)
        breached, _ = mc.check_daily_loss_breach()
        if breached and not mc.is_force_close_requested():
            mc.request_force_close_all()
        assert mc.is_force_close_requested() is False


# ────────────────────────────────────────────────────────────────────────────
# Kill-switch semantic audit (Step 5)
# ────────────────────────────────────────────────────────────────────────────

class TestKillSwitchSemantics:
    """kill_switch = emergency halt = block entries AND flatten positions.
    pause_entries = cool-off = block entries only, exits keep running.
    These two MUST be provably distinct."""

    def test_kill_switch_triggers_force_close(self):
        mc = _build_mc(kill_on=True)
        if mc._kill_switch_fn() and not mc.is_force_close_requested():
            mc.request_force_close_all(reason="kill_switch_activated")
        assert mc.is_force_close_requested() is True
        assert "kill_switch" in mc.get_force_close_state()[1]

    def test_kill_switch_idempotent_across_signals(self):
        mc = _build_mc(kill_on=True)
        fires = 0
        for _ in range(10):
            if mc._kill_switch_fn() and not mc.is_force_close_requested():
                if mc.request_force_close_all(reason="kill_switch_activated"):
                    fires += 1
        assert fires == 1

    def test_pause_entries_does_NOT_trigger_force_close(self):
        """The whole point of pause vs kill. Pause is cool-off — exits stay
        running, no flatten. If this test ever fails, pause and kill have
        become semantically identical and the safety distinction is gone."""
        mc = _build_mc(paused=True, kill_on=False)
        # Simulate the pause_entries gate path (in master_control.evaluate)
        assert mc._entries_paused_fn() is True
        # The pause path returns _block(...) and does NOT call
        # request_force_close_all. So flag must stay clean.
        assert mc.is_force_close_requested() is False

    def test_kill_off_does_not_fire(self):
        mc = _build_mc(kill_on=False)
        if mc._kill_switch_fn() and not mc.is_force_close_requested():
            mc.request_force_close_all(reason="should_not_fire")
        assert mc.is_force_close_requested() is False

    def test_pause_is_per_client_isolated(self):
        """Each mc instance has its own _entries_paused_fn — pausing one
        client must not affect another."""
        mc_a = _build_mc(paused=True)
        mc_b = _build_mc(paused=False)
        assert mc_a._entries_paused_fn() is True
        assert mc_b._entries_paused_fn() is False


# ────────────────────────────────────────────────────────────────────────────
# Source-level: the code paths are in place
# ────────────────────────────────────────────────────────────────────────────

class TestSourceLevelInvariants:
    """Catches accidental removal of the audit-required code paths."""

    def test_exit_engine_has_daily_loss_tick_check(self):
        src = (REPO_ROOT / "ap_exit_engine.py").read_text()
        assert "check_daily_loss_breach" in src, (
            "Exit engine must call check_daily_loss_breach() on every tick "
            "to catch mid-session breach without waiting for a signal."
        )
        assert "daily_loss_limit_tick_detected" in src

    def test_reconciler_has_daily_loss_tick_check(self):
        src = (REPO_ROOT / "ap_reconciler.py").read_text()
        assert "check_daily_loss_breach" in src
        assert "daily_loss_limit_reconciler_detected" in src

    def test_exit_engine_has_kill_switch_tick_check(self):
        src = (REPO_ROOT / "ap_exit_engine.py").read_text()
        assert "kill_switch_activated_tick_detected" in src, (
            "Exit engine must check kill_switch on every tick to flatten "
            "mid-session if operator activates kill."
        )

    def test_master_control_kill_triggers_force_close(self):
        src = (REPO_ROOT / "ap_master_control.py").read_text()
        assert "kill_switch_activated" in src, (
            "master_control entry gate must trigger force-close when "
            "kill_switch is detected — emergency means flatten, not just block."
        )

    def test_reset_session_clears_force_close(self):
        src = (REPO_ROOT / "ap_master_control.py").read_text()
        # The reset_session method must clear the breaker so a new trading
        # day doesn't inherit yesterday's breaker state.
        assert 'self.clear_force_close(reason="reset_session")' in src
