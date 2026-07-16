"""Surgical PAPER recovery restart-guard bridge.

This package shadows the legacy :mod:`ap.queue` module without rewriting the
large production file in place.  It re-exports the complete legacy surface and
overrides only the restart-guard bypass classifier.

Production incident (2026-07-16): APStartupRecovery legitimately reset Jose's
current-session PAPER WATCHING rows to NEW and stamped ``recovery_rescue`` plus
``recovery_rescue_ts``.  The queue restart guard did not recognize that marker,
so 122 overnight-evaluated signals were terminally rejected as
``restart_guard:overnight_skip`` after market open.

Scope:
* PAPER only.
* Current Eastern trading date only.
* Requires the exact APStartupRecovery marker shape.
* No broker submit/cancel, selector, OSM, positions, proof trades, or queue
  mutation is added here.
* LIVE behavior remains fail-closed and byte-for-byte delegated to the legacy
  classifier.
"""

from __future__ import annotations

import importlib.util as _importlib_util
import sys as _sys
from datetime import datetime as _datetime, timedelta as _timedelta, timezone as _timezone
from pathlib import Path as _Path
from typing import Any as _Any
from zoneinfo import ZoneInfo as _ZoneInfo

_BASE_PATH = _Path(__file__).resolve().parent.parent / "queue.py"
_BASE_MODULE_NAME = "_ap_queue_base"

_spec = _importlib_util.spec_from_file_location(_BASE_MODULE_NAME, _BASE_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover - import guard
    raise ImportError(f"Unable to load legacy queue module from {_BASE_PATH}")

_base = _importlib_util.module_from_spec(_spec)
_sys.modules[_BASE_MODULE_NAME] = _base
_spec.loader.exec_module(_base)

# Re-export the full production module surface first.  Functions defined in the
# legacy module retain their original globals; the authoritative helper is
# replaced on ``_base`` below so _dispatch() resolves the hardened classifier.
for _name in dir(_base):
    if _name.startswith("__") and _name != "__doc__":
        continue
    globals()[_name] = getattr(_base, _name)

_ET = _ZoneInfo("America/New_York")
_BASE_RESTART_GUARD_BYPASS = _base._manual_restart_guard_bypass_enabled
_MAX_RECOVERY_LOOKBACK_HOURS = 48
_MAX_FUTURE_CLOCK_SKEW = _timedelta(minutes=5)


def _parse_aware_timestamp(value: _Any) -> _datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = _datetime.fromisoformat(raw)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(_timezone.utc)


def _is_current_session_paper_recovery(
    *,
    payload: dict | None,
    execution_mode: str | None,
    now: _datetime | None = None,
) -> bool:
    """Prove that a queue row is a legitimate current-session PAPER rescue.

    APStartupRecovery writes all three marker fields atomically while changing
    WATCHING -> NEW.  A bare boolean is insufficient: the timestamp must be
    timezone-aware and belong to today's Eastern session, and the recorded
    lookback must remain inside the production recovery bound.
    """

    if str(execution_mode or "").strip().upper() != "PAPER":
        return False
    if not isinstance(payload, dict) or payload.get("recovery_rescue") is not True:
        return False

    rescued_at_utc = _parse_aware_timestamp(payload.get("recovery_rescue_ts"))
    if rescued_at_utc is None:
        return False

    try:
        lookback_hours = int(payload.get("recovery_rescue_lookback_hours"))
    except (TypeError, ValueError):
        return False
    if not 1 <= lookback_hours <= _MAX_RECOVERY_LOOKBACK_HOURS:
        return False

    now_utc = now or _datetime.now(_timezone.utc)
    if now_utc.tzinfo is None:
        now_utc = now_utc.replace(tzinfo=_timezone.utc)
    else:
        now_utc = now_utc.astimezone(_timezone.utc)

    if rescued_at_utc > now_utc + _MAX_FUTURE_CLOCK_SKEW:
        return False

    return rescued_at_utc.astimezone(_ET).date() == now_utc.astimezone(_ET).date()


def _manual_restart_guard_bypass_enabled(
    *,
    job_last_error: str | None = None,
    job_result: dict | None = None,
    payload: dict | None = None,
    execution_mode: str | None = None,
) -> bool:
    """Extend the existing bypass contract for proven PAPER recovery rows only."""

    if _is_current_session_paper_recovery(
        payload=payload,
        execution_mode=execution_mode,
    ):
        return True

    return _BASE_RESTART_GUARD_BYPASS(
        job_last_error=job_last_error,
        job_result=job_result,
        payload=payload,
        execution_mode=execution_mode,
    )


# _dispatch() was defined in the legacy module and resolves globals there.
# Replacing the helper on _base is therefore required; exporting only the local
# symbol would make tests pass while production continued using the old helper.
_base._manual_restart_guard_bypass_enabled = _manual_restart_guard_bypass_enabled

globals()["_manual_restart_guard_bypass_enabled"] = _manual_restart_guard_bypass_enabled
globals()["_is_current_session_paper_recovery"] = _is_current_session_paper_recovery

try:
    __all__ = sorted(
        set(getattr(_base, "__all__", []))
        | {
            "enqueue_signal",
            "worker_loop",
            "_manual_restart_guard_bypass_enabled",
            "_is_current_session_paper_recovery",
        }
    )
except Exception:  # pragma: no cover
    __all__ = [
        "enqueue_signal",
        "worker_loop",
        "_manual_restart_guard_bypass_enabled",
        "_is_current_session_paper_recovery",
    ]
