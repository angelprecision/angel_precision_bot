"""
tests/test_health_and_reconciler_startup_noise.py
PR fix/health-and-reconciler-startup-noise

Two preexisting cosmetic bugs (not caused by recent PRs):

  Bug A — ap_reconciler.py: spurious FILL_MONITOR_NOT_CONFIRMED on every cold start
    Observed 2026-05-26 16:51:39 UTC. _start_reconciler() is called BEFORE
    _start_fill_monitor() in client_runner.py:1697-1718. The reconciler's
    very first cycle (right after self._thread.start()) calls
    _verify_fill_monitor_or_alert() which reports "not wired" because
    self.fill_monitor is still None. ~100ms later _start_fill_monitor
    assigns reconciler.fill_monitor and the warning never fires again.
    Net effect: one misleading WARNING per (client × cold start).

  Bug B — app.py:2187: reconciler_alive in /execution/health always False
    The health endpoint reads `runner.reconciler_thread` but client_runner
    only sets `self.reconciler` (the APBrokerReconciler instance, which
    exposes its own .is_alive() method via self._thread). The runner
    never has a `reconciler_thread` attribute, so the bool() always
    evaluates False. Net effect: operators see false-negative health.

Both fixes are surgical:
  A) Add a FILL_MONITOR_WIRE_GRACE_SEC grace period (default 5s) so
     the first-cycle alert is suppressed when the reconciler just
     started. After grace expires, the alert behaves as before.
  B) Use getattr(runner, 'reconciler', None) and call .is_alive() on
     the APBrokerReconciler instance.

Out-of-scope (NOT touched):
  - reconciler poll cadence
  - fill_monitor behavior
  - any startup sequence besides the grace check
  - any restricted file from previous PRs

Run:
    DATABASE_URL=postgresql://x python3 -m pytest \
      tests/test_health_and_reconciler_startup_noise.py -v
"""
from __future__ import annotations

import os
import sys
import time
import threading
from pathlib import Path
from unittest.mock import MagicMock
import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))


# ────────────────────────────────────────────────────────────────────────────
# Bug A — reconciler startup grace
# ────────────────────────────────────────────────────────────────────────────

class TestBugA_ReconcilerStartupGrace:
    """
    _verify_fill_monitor_or_alert() must NOT emit the
    FILL_MONITOR_NOT_CONFIRMED alert during the first
    FILL_MONITOR_WIRE_GRACE_SEC seconds after the reconciler thread
    started. After grace expires, behavior matches the previous
    contract (alerts on missing/dead fill_monitor).
    """

    def _make_reconciler(self):
        """Build a minimally-wired APBrokerReconciler instance for unit tests."""
        from ap_reconciler import APBrokerReconciler
        # Mocks for required init args
        broker = MagicMock()
        osm = MagicMock()
        osm.client_id = "test@x.com"
        pm = MagicMock()
        r = APBrokerReconciler(
            broker=broker, client_id="test@x.com", osm=osm, pm=pm,
            execution_mode="paper",
        )
        return r

    def test_grace_attribute_exists(self):
        """Reconciler must expose a _started_ts attribute set at start()."""
        from ap_reconciler import APBrokerReconciler
        src = (REPO_ROOT / "ap_reconciler.py").read_text()
        assert "_started_ts" in src, (
            "Reconciler must track when its thread started (self._started_ts) "
            "so the verify-fill-monitor check can apply a grace window."
        )

    def test_grace_constant_present(self):
        """Source must reference FILL_MONITOR_WIRE_GRACE_SEC env var with a default."""
        src = (REPO_ROOT / "ap_reconciler.py").read_text()
        assert "FILL_MONITOR_WIRE_GRACE_SEC" in src, (
            "Source must reference the FILL_MONITOR_WIRE_GRACE_SEC env var "
            "(or constant) so operators can tune the grace window."
        )

    def test_first_cycle_within_grace_does_not_alert(self):
        """Within grace window, _verify_fill_monitor_or_alert must return True
        (treat as 'not yet wired, but no alert') instead of warning."""
        r = self._make_reconciler()
        alerts = []
        r._alert_fn = lambda msg: alerts.append(msg)
        # Simulate brand-new start — _started_ts very recent
        r._started_ts = time.time()
        r.fill_monitor = None
        # Set a short grace for test
        r._fill_monitor_wire_grace_sec = 5.0
        result = r._verify_fill_monitor_or_alert()
        assert result is True, (
            "Inside grace window, _verify_fill_monitor_or_alert must "
            "return True (no alert) instead of False with warning."
        )
        assert alerts == [], f"No alerts expected within grace; got: {alerts}"

    def test_after_grace_with_no_fill_monitor_alerts(self):
        """Outside grace window, missing fill_monitor must alert (preserves
        the original safety contract)."""
        r = self._make_reconciler()
        alerts = []
        r._alert_fn = lambda msg: alerts.append(msg)
        # Simulate started >> grace ago
        r._started_ts = time.time() - 60.0
        r._fill_monitor_wire_grace_sec = 5.0
        r.fill_monitor = None
        result = r._verify_fill_monitor_or_alert()
        assert result is False, "Outside grace, missing fill_monitor must report False"
        assert len(alerts) == 1, f"Expected 1 alert, got {len(alerts)}: {alerts}"
        assert "FILL_MONITOR_NOT_CONFIRMED" in alerts[0]

    def test_alive_fill_monitor_passes_in_any_window(self):
        """If fill_monitor IS alive, must return True regardless of grace state."""
        r = self._make_reconciler()
        alerts = []
        r._alert_fn = lambda msg: alerts.append(msg)
        fm = MagicMock()
        fm.is_alive = lambda: True
        r.fill_monitor = fm

        # Within grace
        r._started_ts = time.time()
        r._fill_monitor_wire_grace_sec = 5.0
        assert r._verify_fill_monitor_or_alert() is True

        # Outside grace
        r._started_ts = time.time() - 60.0
        assert r._verify_fill_monitor_or_alert() is True

        assert alerts == [], f"No alerts expected when fill_monitor alive; got: {alerts}"

    def test_require_fill_monitor_still_raises_outside_grace(self):
        """If require_fill_monitor=True and outside grace, must raise (back-compat)."""
        r = self._make_reconciler()
        r.require_fill_monitor = True
        r.fill_monitor = None
        r._started_ts = time.time() - 60.0
        r._fill_monitor_wire_grace_sec = 5.0
        with pytest.raises(RuntimeError, match="FILL_MONITOR_NOT_CONFIRMED"):
            r._verify_fill_monitor_or_alert()

    def test_require_fill_monitor_does_not_raise_within_grace(self):
        """Within grace, require_fill_monitor=True must NOT raise — grace
        applies uniformly. This lets cold starts succeed even in strict mode."""
        r = self._make_reconciler()
        r.require_fill_monitor = True
        r.fill_monitor = None
        r._started_ts = time.time()  # just started
        r._fill_monitor_wire_grace_sec = 5.0
        result = r._verify_fill_monitor_or_alert()
        # Grace returns True (caller treats as "not yet wired, no action")
        assert result is True

    def test_no_started_ts_falls_back_to_old_behavior(self):
        """If _started_ts is not set (object never went through start()),
        the grace check must NOT silently swallow alerts."""
        r = self._make_reconciler()
        alerts = []
        r._alert_fn = lambda msg: alerts.append(msg)
        # Don't set _started_ts at all
        if hasattr(r, "_started_ts"):
            del r._started_ts
        r._fill_monitor_wire_grace_sec = 5.0
        r.fill_monitor = None
        result = r._verify_fill_monitor_or_alert()
        # No grace → alert as before
        assert result is False
        assert len(alerts) == 1


# ────────────────────────────────────────────────────────────────────────────
# Bug B — health endpoint reconciler_alive
# ────────────────────────────────────────────────────────────────────────────

class TestBugB_HealthEndpointReconcilerAlive:
    """
    /execution/health must report reconciler_alive=True when the
    APBrokerReconciler instance is up. Previously read a nonexistent
    `runner.reconciler_thread` attribute (always None → False).
    """

    def test_app_py_uses_reconciler_dot_is_alive(self):
        """app.py must read runner.reconciler.is_alive(), not
        runner.reconciler_thread.is_alive()."""
        src = (REPO_ROOT / "app.py").read_text()
        lines = src.splitlines()
        # Find the line containing reconciler_alive
        line = next((ln for ln in lines if '"reconciler_alive"' in ln), None)
        assert line is not None, "reconciler_alive entry not found in app.py"
        # Must NOT reference reconciler_thread (the broken attr name)
        assert "reconciler_thread" not in line, (
            f"app.py still reads runner.reconciler_thread (nonexistent). "
            f"Must read runner.reconciler.is_alive() instead.\nLine: {line}"
        )
        # Must reference is_alive on the reconciler object
        assert "reconciler" in line and "is_alive" in line, (
            f"reconciler_alive must call .is_alive() on the reconciler object.\n"
            f"Line: {line}"
        )

    def test_reconciler_object_exposes_is_alive(self):
        """Sanity: APBrokerReconciler must have is_alive() method."""
        from ap_reconciler import APBrokerReconciler
        assert hasattr(APBrokerReconciler, "is_alive"), (
            "APBrokerReconciler must expose is_alive() so the health "
            "endpoint can poll it."
        )


# ────────────────────────────────────────────────────────────────────────────
# Restricted-scope locks
# ────────────────────────────────────────────────────────────────────────────

class TestRestrictedScopeLocks:
    """Prove this PR ONLY touches ap_reconciler.py + app.py + the new test."""

    EXPECTED_FILES = {
        "ap_reconciler.py",
        "app.py",
        "tests/test_health_and_reconciler_startup_noise.py",
    }

    def test_only_allowed_files_changed(self):
        import subprocess
        try:
            r = subprocess.run(
                ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=5,
            )
            if r.returncode != 0:
                pytest.skip("Not in git repo")
            branch = r.stdout.strip()
            if "health-and-reconciler-startup-noise" not in branch:
                pytest.skip(f"Branch {branch} is not this PR's branch")
        except Exception:
            pytest.skip("git unavailable")

        r = subprocess.run(
            ["git", "diff", "--name-only", "origin/main"],
            cwd=str(REPO_ROOT), capture_output=True, text=True, timeout=10,
        )
        changed = {line.strip() for line in r.stdout.splitlines() if line.strip()}
        unexpected = changed - self.EXPECTED_FILES
        assert not unexpected, (
            f"PR touches files outside scope: {unexpected}\n"
            f"Expected: {self.EXPECTED_FILES}"
        )
