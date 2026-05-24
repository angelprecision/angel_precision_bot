"""
ap/reconciler_heartbeat.py — PR #30 live-safety hardening

Minimal reconciler SLA visibility.

Behavior
--------
- Reconciler calls record_heartbeat(client_id) at the end of every
  successful cycle.
- check_staleness(client_id) returns True + a snapshot when the heartbeat
  for that client is older than RECONCILER_SLA_SECONDS.
- A higher-level watchdog (e.g., a monitor tick or a scheduled task) calls
  check_staleness periodically and emits CRITICAL decision_event
  RECONCILER_STALE when True.
- Alert only. This module NEVER closes positions and NEVER blocks exits.

State
-----
In-process dict, same rationale as ap/safety_circuit.py: hot path,
restart-safe, no new DB column. Multi-worker deployments would need a
Redis-backed variant; that's out of scope for proof week.

Public API
----------
  record_heartbeat(client_id, *, status='ok') -> None
  check_staleness(client_id) -> dict      # always includes 'stale' key
  snapshot(client_id) -> dict
  RECONCILER_SLA_SECONDS                  # int, env-tunable

Env vars
--------
  RECONCILER_SLA_SECONDS=120
"""

from __future__ import annotations

import os
import time
import threading
from typing import Optional


RECONCILER_SLA_SECONDS = int(os.getenv("RECONCILER_SLA_SECONDS", "120"))


# client_id -> {"ts": epoch, "status": "ok"|str, "cycles": int}
_state: dict[str, dict] = {}

# Track whether we've already emitted RECONCILER_STALE for the current
# stale period so the watchdog doesn't fire every tick. Cleared when a
# fresh heartbeat lands.
_alerted: dict[str, bool] = {}

_lock = threading.Lock()


def _now() -> float:
    return time.time()


def record_heartbeat(client_id: str, *, status: str = "ok") -> None:
    """Reconciler calls this at the end of every successful cycle.

    A 'failed cycle' should NOT call this (so staleness will trigger).
    Optionally callers can pass status='partial' or 'degraded' for
    telemetry; the watchdog only looks at the timestamp.
    """
    if not client_id:
        return
    with _lock:
        prev = _state.get(client_id) or {}
        _state[client_id] = {
            "ts":     _now(),
            "status": status or "ok",
            "cycles": int(prev.get("cycles", 0)) + 1,
        }
        _alerted.pop(client_id, None)  # fresh heartbeat clears the alert latch


def check_staleness(client_id: str) -> dict:
    """Return a snapshot dict including 'stale' (bool) and 'should_alert'
    (bool). 'should_alert' is True only on the first detection of staleness
    since the last fresh heartbeat — so the caller can emit ONE
    decision_event per stale episode instead of one per tick.
    """
    if not client_id:
        return {"stale": False, "should_alert": False, "client_id": client_id}

    with _lock:
        hb = _state.get(client_id)
        now = _now()
        if hb is None:
            # No heartbeat ever recorded. We do NOT alert here (a freshly
            # started runtime hasn't had a chance to beat yet). Callers can
            # treat 'no heartbeat' as a separate condition if they want.
            return {
                "stale":          False,
                "should_alert":   False,
                "client_id":      client_id,
                "last_heartbeat": None,
                "age_secs":       None,
                "sla_secs":       RECONCILER_SLA_SECONDS,
                "cycles":         0,
                "status":         "no_heartbeat_yet",
            }
        age = now - float(hb["ts"])
        stale = age >= float(RECONCILER_SLA_SECONDS)
        should_alert = bool(stale and not _alerted.get(client_id))
        if should_alert:
            _alerted[client_id] = True
        return {
            "stale":          stale,
            "should_alert":   should_alert,
            "client_id":      client_id,
            "last_heartbeat": hb["ts"],
            "age_secs":       float(age),
            "sla_secs":       RECONCILER_SLA_SECONDS,
            "cycles":         int(hb.get("cycles", 0)),
            "status":         hb.get("status", "ok"),
        }


def snapshot(client_id: str) -> dict:
    """Diagnostic snapshot for telemetry."""
    return check_staleness(client_id)


# ----------------------------------------------------------------------
# Test-only helpers
# ----------------------------------------------------------------------

def _reset_all_for_tests() -> None:
    with _lock:
        _state.clear()
        _alerted.clear()


def _force_heartbeat_age_for_tests(client_id: str, age_secs: float) -> None:
    """Pretend the last heartbeat was `age_secs` ago. Used by tests."""
    with _lock:
        prev = _state.get(client_id) or {"cycles": 0, "status": "ok"}
        _state[client_id] = {
            "ts":     _now() - float(age_secs),
            "status": prev.get("status", "ok"),
            "cycles": int(prev.get("cycles", 0)),
        }
        _alerted.pop(client_id, None)
