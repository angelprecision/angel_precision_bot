"""
ap/safety_circuit.py — PR #30 live-safety hardening

Lightweight per-client broker-error circuit breaker.

Design contract
---------------
- Tracks broker submit/cancel/status errors per client in a rolling window.
- If broker errors exceed BROKER_ERROR_THRESHOLD within
  BROKER_ERROR_WINDOW_SECS for the same client, the circuit opens.
- Open circuit BLOCKS new entry submits only. It does NOT block:
  - exits (any kind)
  - force exits
  - close-all
  - reconciliation
  - status polls
- Open circuit emits ONE CRITICAL decision_event per open (not per check).
- Circuit auto-clears after BROKER_ERROR_CLEAR_AFTER_SECS of clean
  operation (no fresh errors).
- Operators can hard-clear via clear_breaker(client_id) (e.g. from a
  manual ops command).
- This breaker NEVER closes positions and NEVER cancels existing orders.

State location
--------------
In-process per-monitor (and per-execution-call). The state is intentionally
in-memory because:
  1. It's a hot path: every entry submit checks it.
  2. The natural blast radius IS the running process. A restart resets the
     breaker, which is the correct behavior on rollout/restart.
  3. No new DB table needed.

Cross-process coordination (multi-worker setups) would need a Redis-backed
variant; that's out of scope for proof week per the audit prompt.

Public API
----------
  record_broker_error(client_id, error, *, op_kind=None) -> bool
      Record one broker error. Returns True if the circuit just OPENED on
      this call (caller can emit the decision_event).

  is_open(client_id) -> bool
      Quick check: should new entries be blocked for this client?

  clear_breaker(client_id) -> None
      Manually clear an open circuit (ops command).

  snapshot(client_id) -> dict
      Read-only diagnostic of the breaker state for a client.

Env vars (all optional)
-----------------------
  BROKER_ERROR_THRESHOLD       default 5
  BROKER_ERROR_WINDOW_SECS     default 120
  BROKER_ERROR_CLEAR_AFTER_SECS default 300
"""

from __future__ import annotations

import os
import time
import threading
from collections import deque
from typing import Optional


# ----------------------------------------------------------------------
# Configuration (env-tunable, with safe defaults)
# ----------------------------------------------------------------------

BROKER_ERROR_THRESHOLD        = int(os.getenv("BROKER_ERROR_THRESHOLD",        "5"))
BROKER_ERROR_WINDOW_SECS      = int(os.getenv("BROKER_ERROR_WINDOW_SECS",      "120"))
BROKER_ERROR_CLEAR_AFTER_SECS = int(os.getenv("BROKER_ERROR_CLEAR_AFTER_SECS", "300"))


# ----------------------------------------------------------------------
# State (per-client)
# ----------------------------------------------------------------------

# error_events: client_id -> deque[(ts_epoch, sample_error_str)]
_errors: dict[str, deque] = {}

# breaker_open_ts: client_id -> epoch when the breaker opened, or None if closed
_open_ts: dict[str, float] = {}

# last sample error message per client (for telemetry)
_last_error: dict[str, str] = {}

# Total opens per process lifetime (diagnostics; never reset)
_open_count: dict[str, int] = {}

_lock = threading.Lock()


def _now() -> float:
    return time.time()


def _prune_old(client_id: str, cutoff: float) -> None:
    """Drop events older than the window from this client's deque."""
    q = _errors.get(client_id)
    if not q:
        return
    while q and q[0][0] < cutoff:
        q.popleft()


def record_broker_error(
    client_id: str,
    error: object,
    *,
    op_kind: Optional[str] = None,
) -> bool:
    """Record one broker error. Return True if the circuit just OPENED on
    this call (so the caller can emit ONE CRITICAL decision_event).

    Args:
      client_id: which client the error happened under.
      error:     any object with __str__; we truncate to 240 chars.
      op_kind:   optional 'submit' / 'cancel' / 'status' / 'quote' for
                 telemetry. Has no effect on counting.

    Idempotency: if the circuit is already open, this still appends the
    event (so it counts toward post-clear recovery) but returns False —
    the caller has already emitted the open event.
    """
    if not client_id:
        return False

    err_text = str(error)[:240]
    now = _now()
    cutoff = now - float(BROKER_ERROR_WINDOW_SECS)

    with _lock:
        q = _errors.setdefault(client_id, deque(maxlen=1024))
        _prune_old(client_id, cutoff)
        q.append((now, err_text))
        _last_error[client_id] = err_text

        # Already-open? Don't fire the open event twice.
        if _open_ts.get(client_id):
            return False

        # Threshold check
        if len(q) >= BROKER_ERROR_THRESHOLD:
            _open_ts[client_id] = now
            _open_count[client_id] = _open_count.get(client_id, 0) + 1
            return True

    return False


def is_open(client_id: str) -> bool:
    """Return True if the circuit is currently open for this client (i.e.,
    new entries should be blocked). Side-effect: if the circuit has been
    silent for BROKER_ERROR_CLEAR_AFTER_SECS, auto-clear it here.
    """
    if not client_id:
        return False
    with _lock:
        opened_at = _open_ts.get(client_id)
        if not opened_at:
            return False

        # Auto-clear: if no errors have arrived in the clear-after window,
        # close the breaker. Use deque's last-event timestamp (or the
        # original open time if no events since).
        q = _errors.get(client_id)
        last_event_ts = q[-1][0] if q else opened_at
        if _now() - last_event_ts >= float(BROKER_ERROR_CLEAR_AFTER_SECS):
            _open_ts.pop(client_id, None)
            # Keep the rolling deque around so we have history, but it's
            # naturally pruned by record_broker_error / _prune_old.
            return False
        return True


def clear_breaker(client_id: str) -> None:
    """Operator-initiated manual clear of an open circuit."""
    if not client_id:
        return
    with _lock:
        _open_ts.pop(client_id, None)
        # Also flush the rolling window so the next error starts a fresh
        # count, not one already pre-loaded toward threshold.
        q = _errors.get(client_id)
        if q:
            q.clear()


def snapshot(client_id: str) -> dict:
    """Read-only diagnostic snapshot for telemetry / logging."""
    if not client_id:
        return {}
    with _lock:
        q = _errors.get(client_id)
        cutoff = _now() - float(BROKER_ERROR_WINDOW_SECS)
        active = [e for e in (q or []) if e[0] >= cutoff]
        return {
            "client_id":                 client_id,
            "broker_error_count":        len(active),
            "window_secs":               BROKER_ERROR_WINDOW_SECS,
            "threshold":                 BROKER_ERROR_THRESHOLD,
            "open":                      bool(_open_ts.get(client_id)),
            "opened_at_epoch":           _open_ts.get(client_id),
            "open_count_session":        _open_count.get(client_id, 0),
            "last_error_sample":         _last_error.get(client_id, ""),
            "clear_after_secs":          BROKER_ERROR_CLEAR_AFTER_SECS,
        }


# ----------------------------------------------------------------------
# Test-only helpers (under-scored; not part of the public API)
# ----------------------------------------------------------------------

def _reset_all_for_tests() -> None:
    """Clear ALL state. Tests must call this in setUp/setup_method to
    isolate state between cases."""
    with _lock:
        _errors.clear()
        _open_ts.clear()
        _last_error.clear()
        _open_count.clear()
