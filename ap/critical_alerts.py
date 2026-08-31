"""Critical money-at-risk fence alerting — page a human, don't just log.

Incident this addresses (2026-07-17/18 audit, v1 finding PR-G):
    The exact events that mean client capital is exposed were log-only.
    ``exit_decision_generation_claims`` was missing from production for a
    full trading day; in LIVE mode that silently suppressed every
    actionable exit decision, and the only witness was a ``log.critical``
    line on Render. Nobody was paged.

Contract:
    * ``alert_critical(event, message, ...)`` ALWAYS emits ``log.critical``
      and, when a fence-alert delivery path is wired, delivers exactly one
      alert per (event, dedup_key) per dedup window — a repeating failure
      pages once per window, not once per exit-engine tick.
    * Delivery path: ``ap_health_registry.HEALTH`` alert function (the same
      Discord path wired via ``ap_bootstrap.bootstrap(alert_fn=...)``).
    * Never raises. Alerting failures degrade to logging; the trade path
      must never be harmed by its own observability.
    * ``CRITICAL_ALERTS_ENABLED=0`` disables delivery (loudly); logging is
      unconditional.

Known event codes (extend here when adding new fence alerts):
    EXIT_DECISION_GENERATION_CLAIM_FAILED  — durable exit fence claim
        errored; in LIVE this means an actionable exit was suppressed.
    EXIT_DECISION_GENERATION_READ_UNAVAILABLE — durable generation could
        not be read; LIVE exit suppressed.
    EXIT_DECISION_STALE_CLAIM_AMBIGUOUS   — a stale exit claim was
        quarantined; a position may hold an ambiguous exit state.
    ENTRY_BROKER_TAG_RECOVERY             — an ambiguous broker response
        was recovered via the Tradier tag; the system was one bug away
        from a double submit. Every firing deserves review.
    SCHEMA_ATTESTATION_FAILED             — deployed code requires schema
        the database does not have (non-strict/PAPER path; strict LIVE
        raises at preflight instead).
"""
from __future__ import annotations

import logging
import os
import threading
import time
from collections import deque
from typing import Any

log = logging.getLogger("ap.critical_alerts")

_DEFAULT_DEDUP_SECONDS = 300.0
_CACHE_MAX = 512
_RECENT_MAX = 200

_lock = threading.Lock()
_last_sent: dict[tuple[str, str], float] = {}
_recent: deque[dict[str, Any]] = deque(maxlen=_RECENT_MAX)


def _enabled() -> bool:
    return os.getenv("CRITICAL_ALERTS_ENABLED", "1").strip().lower() not in (
        "", "0", "false", "no",
    )


def _dedup_window_seconds() -> float:
    raw = os.getenv("CRITICAL_ALERT_DEDUP_SECONDS", "").strip()
    try:
        value = float(raw) if raw else _DEFAULT_DEDUP_SECONDS
    except (TypeError, ValueError):
        value = _DEFAULT_DEDUP_SECONDS
    return min(max(value, 0.0), 86400.0)


def _deliver(text: str) -> bool:
    """Deliver via the wired health-registry alert path. Never raises."""
    try:
        from ap_health_registry import HEALTH

        fn = getattr(HEALTH, "_alert_fn", None)
        if fn is None:
            return False
        fn(text)
        return True
    except Exception as exc:  # noqa: BLE001 — observability must not harm trading
        log.error("critical alert delivery failed: %s", exc)
        return False


def alert_critical(
    event: str,
    message: str,
    *,
    dedup_key: str = "",
    window_seconds: float | None = None,
    severity: str = "P0",
    now_monotonic: float | None = None,
) -> bool:
    """Log CRITICAL unconditionally; deliver one page per key per window.

    Returns True when a delivery attempt was made this call (i.e. not
    suppressed by dedup / kill switch); False otherwise. Never raises.
    """
    try:
        event = str(event or "UNKNOWN_CRITICAL_EVENT").strip() or "UNKNOWN_CRITICAL_EVENT"
        text = f"[{severity}] {event} | {message}"
        log.critical("%s", text)

        now_value = time.monotonic() if now_monotonic is None else float(now_monotonic)
        window = _dedup_window_seconds() if window_seconds is None else max(0.0, float(window_seconds))
        key = (event, str(dedup_key or ""))

        with _lock:
            previous = _last_sent.get(key)
            suppressed = previous is not None and (now_value - previous) < window
            if not suppressed:
                _last_sent[key] = now_value
                if len(_last_sent) > _CACHE_MAX:
                    cutoff = now_value - max(window, _DEFAULT_DEDUP_SECONDS) * 2
                    for stale_key in [k for k, ts in _last_sent.items() if ts < cutoff]:
                        _last_sent.pop(stale_key, None)
                    while len(_last_sent) > _CACHE_MAX:
                        _last_sent.pop(next(iter(_last_sent)), None)
            _recent.append({
                "event": event,
                "dedup_key": key[1],
                "severity": severity,
                "message": message,
                "suppressed": suppressed,
                "monotonic": now_value,
            })

        if suppressed:
            return False
        if not _enabled():
            log.critical(
                "CRITICAL_ALERTS_DISABLED_BY_ENV — %s NOT delivered (logging only)", event
            )
            return False
        _deliver(text)
        return True
    except Exception as exc:  # noqa: BLE001 — absolute non-raising guarantee
        try:
            log.error("alert_critical internal failure: %s", exc)
        except Exception:
            pass
        return False


def get_recent_critical_alerts() -> list[dict[str, Any]]:
    """Snapshot of recent alert records (delivered and suppressed)."""
    with _lock:
        return [dict(item) for item in _recent]


def _reset_for_tests() -> None:
    with _lock:
        _last_sent.clear()
        _recent.clear()
