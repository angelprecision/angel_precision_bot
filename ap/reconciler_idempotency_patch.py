"""
ap.reconciler_idempotency_patch
===============================
Runtime guard for APBrokerReconciler.run_once().

Why
---
Self-healing, watchdogs, and the reconciler's own loop may all call run_once().
Without a local guard, two calls within a few seconds can repeat broker/DB/OSM
corrections and create duplicate transition pressure.

Policy
------
- Skip duplicate run_once calls inside RECONCILER_RUN_ONCE_MIN_INTERVAL_SEC.
- Return the last summary when skipped.
- Serialize run_once with a per-instance lock.
- Preserve original run_once behavior otherwise.
"""

from __future__ import annotations

import logging
import os
import threading
import time
from functools import wraps

log = logging.getLogger("ap.reconciler_idempotency_patch")

_PATCH_FLAG = "__ap_reconciler_idempotency_patch_installed__"
_ORIGINAL_ATTR = "__ap_reconciler_original_run_once__"
MIN_INTERVAL_SEC = float(os.getenv("RECONCILER_RUN_ONCE_MIN_INTERVAL_SEC", "3.0"))


def install_reconciler_idempotency_patch(reconciler_cls):
    if getattr(reconciler_cls, _PATCH_FLAG, False):
        return reconciler_cls

    original = getattr(reconciler_cls, "run_once", None)
    if not callable(original):
        raise AttributeError("APBrokerReconciler.run_once is missing")

    setattr(reconciler_cls, _ORIGINAL_ATTR, original)

    @wraps(original)
    def guarded_run_once(self, *args, **kwargs):
        lock = getattr(self, "_run_once_lock", None)
        if lock is None:
            lock = threading.RLock()
            try:
                setattr(self, "_run_once_lock", lock)
            except Exception:
                pass

        with lock:
            now = time.time()
            last_ts = float(getattr(self, "_last_run_once_ts", 0.0) or 0.0)
            if last_ts and (now - last_ts) < MIN_INTERVAL_SEC:
                summary = getattr(self, "_last_run_once_summary", None)
                if summary is None:
                    summary = {
                        "client_id": getattr(self, "client_id", "unknown"),
                        "skipped": True,
                        "skip_reason": "run_once_called_too_soon",
                        "min_interval_sec": MIN_INTERVAL_SEC,
                    }
                log.debug(
                    "[%s] Reconciler run_once skipped: called %.2fs after previous run; min=%.2fs",
                    getattr(self, "client_id", "unknown"),
                    now - last_ts,
                    MIN_INTERVAL_SEC,
                )
                return summary

            setattr(self, "_last_run_once_ts", now)
            try:
                summary = original(self, *args, **kwargs)
            except Exception:
                # Let caller see the original exception, but do not leave summary mutated.
                raise
            try:
                setattr(self, "_last_run_once_summary", summary)
            except Exception:
                pass
            return summary

    setattr(reconciler_cls, "run_once", guarded_run_once)
    setattr(reconciler_cls, _PATCH_FLAG, True)
    return reconciler_cls


def install_patch_if_available():
    try:
        from ap_reconciler import APBrokerReconciler
        install_reconciler_idempotency_patch(APBrokerReconciler)
        return True
    except Exception as exc:
        log.warning("reconciler idempotency patch install failed: %s", exc)
        return False


__all__ = ["install_reconciler_idempotency_patch", "install_patch_if_available"]
