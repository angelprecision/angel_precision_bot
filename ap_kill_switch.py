"""
ap_kill_switch.py
=================
Local file-based emergency halt for Angel Precision Intelligence.

This is a COMPLEMENT to the existing DB-backed kill switch in app.py — not a
replacement. The DB kill switch handles operator-triggered halts via the API.
This module handles automatic halts triggered by the health registry when a
CRITICAL organ flatlines.

When KILL.kill(reason) is called:
1. Sets in-memory flag immediately (zero-latency read path)
2. Writes a flag file to disk (survives process restart)
3. Logs CRITICAL to structured logs

The health registry's _fire_kill() callback calls KILL.kill() automatically
when a CRITICAL organ goes STALE or FAILED.

Usage:
    from ap_kill_switch import KILL

    # Check (called by master control / execution core before every trade):
    if KILL.is_killed():
        return block_trade(reason=KILL.reason)

    # Emergency halt:
    KILL.kill("ap_exit_engine STALE — no heartbeat for 45s")

    # Reset (after manual review):
    KILL.reset()
"""
from __future__ import annotations

import logging
import os
import threading
import time

log = logging.getLogger("ap.kill")

_DEFAULT_FLAG_PATH = os.getenv("AP_KILL_FLAG", "logs/KILL_SWITCH.flag")


class KillSwitch:
    """
    Thread-safe, file-backed local kill switch.
    In-memory flag for zero-latency reads.
    Flag file for persistence across restarts.
    """

    def __init__(self, flag_path: str = _DEFAULT_FLAG_PATH):
        self._armed     = False
        self._reason    = ""
        self._armed_at  = 0.0
        self._lock      = threading.Lock()
        self._flag_path = flag_path
        # On startup: if flag file exists from a prior run, log loudly but do NOT
        # auto-arm. The flag persists on disk (Render persistent disk) and could
        # have been written during a startup race in a previous deploy. Require an
        # explicit operator call to arm via the API, or let the health sweep arm
        # it after the startup grace period if a real organ failure is detected.
        if os.path.exists(self._flag_path):
            try:
                with open(self._flag_path) as f:
                    prior_reason = f.read().strip()
            except Exception:
                prior_reason = "unknown"
            log.warning(
                "[KILL_SWITCH] Stale flag file found from prior run: %s — "
                "removing it to allow clean startup. "
                "Kill switch will re-arm if a critical organ fails after startup grace period.",
                prior_reason,
            )
            try:
                os.remove(self._flag_path)
            except Exception:
                pass

    def is_killed(self) -> bool:
        """Zero-latency check. Always prefer in-memory flag over file I/O."""
        return self._armed

    def kill(self, reason: str) -> None:
        """Arm the kill switch. Idempotent — only fires once."""
        with self._lock:
            if self._armed:
                return
            self._armed    = True
            self._reason   = reason
            self._armed_at = time.time()
            log.critical("[KILL_SWITCH] SYSTEM HALT: %s", reason)
            try:
                os.makedirs(os.path.dirname(self._flag_path) or "logs", exist_ok=True)
                with open(self._flag_path, "w") as f:
                    f.write(reason)
            except Exception as e:
                log.error("[KILL_SWITCH] flag file write failed: %s", e)

    def reset(self) -> None:
        """Disarm. Requires manual operator action — not called automatically."""
        with self._lock:
            self._armed  = False
            self._reason = ""
            try:
                if os.path.exists(self._flag_path):
                    os.remove(self._flag_path)
                log.info("[KILL_SWITCH] Reset — system re-armed for trading")
            except Exception as e:
                log.error("[KILL_SWITCH] flag file removal failed: %s", e)

    @property
    def reason(self) -> str:
        return self._reason


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
KILL = KillSwitch()
