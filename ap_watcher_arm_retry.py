# =============================================================================
# ap_watcher_arm_retry.py  —  PR #92 Watcher Arm Bounded Quote-Readiness Retry
# =============================================================================
# Bounded retry layer + per-pod circuit breaker for the watcher arm path.
# Lives BESIDE ap/queue.py — never owns the arm decision, only wraps the
# retry loop around a caller-supplied arm_callable. The integration in
# ap/queue.py invokes this module only when:
#
#   1. WATCHER_ARM_QUOTE_RETRY_ENABLED=true, AND
#   2. The classifier emitted bucket=QUOTE_READINESS_UNKNOWN.
#
# Default behavior (env flag false) is unchanged — this module is dormant.
#
# Design rules (PR92 spec):
#   * Bounded loop: WATCHER_ARM_QUOTE_RETRY_SECONDS (csv) up to
#     WATCHER_ARM_MAX_QUOTE_WAIT_SECONDS total.
#   * Revalidate on EVERY attempt using the fresh quote (spec §6). If a
#     fresh-quote check shows true invalidation or fresh drift, retry
#     STOPS immediately and the corresponding final_decision wins.
#   * Per-pod in-memory circuit breaker (spec §9) — opens after N
#     failures within a window, stays open for a cooldown, no Redis /
#     no new infra.
#   * No broker submit. No new order row. No signal regeneration.
#   * Preserve full arm_attempts sequence (spec §4).
#
# Public API:
#   maybe_retry_arm(...)            -> RetryOutcome
#   CircuitBreaker.is_open(pod_id)
#   CircuitBreaker.record_failure(pod_id)
#   parse_retry_schedule(csv_str)
# =============================================================================

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable, Optional

from ap_watcher_arm_audit import (
    BUCKET_FRESH_PRICE_DRIFT,
    BUCKET_QUOTE_READINESS_UNKNOWN,
    BUCKET_TRUE_INVALIDATION,
    BUCKET_DOMAIN_SAFETY_BLOCK,
    Classification,
    DECISION_ARMED_AFTER_RETRY,
    DECISION_BLOCKED_INVALID_GEOMETRY,
    DECISION_BLOCKED_PRICE_DRIFT,
    DECISION_BLOCKED_QUOTE_DOMAIN_MISMATCH,
    DECISION_BLOCKED_TRUE_INVALIDATION,
    DECISION_FAILED_AFTER_RETRY,
    DECISION_RETRY_DISABLED_QUOTE_NOT_READY,
    REASON_ARM_TIMEOUT,
    build_attempt_record,
    classify_arm_failure,
    evaluate_fresh_quote,
    quote_is_missing,
    quote_is_stale,
)

log = logging.getLogger("ap.watcher_arm_retry")


# ---------------------------------------------------------------------------
# Env / config
# ---------------------------------------------------------------------------

ENV_ENABLED            = "WATCHER_ARM_QUOTE_RETRY_ENABLED"
ENV_RETRY_SECONDS      = "WATCHER_ARM_QUOTE_RETRY_SECONDS"
ENV_MAX_WAIT_SECONDS   = "WATCHER_ARM_MAX_QUOTE_WAIT_SECONDS"
ENV_CIRCUIT_MAX        = "WATCHER_ARM_RETRY_CIRCUIT_MAX_FAILURES"
ENV_CIRCUIT_WINDOW     = "WATCHER_ARM_RETRY_CIRCUIT_WINDOW_SECONDS"
ENV_CIRCUIT_COOLDOWN   = "WATCHER_ARM_RETRY_CIRCUIT_COOLDOWN_SECONDS"

DEFAULT_RETRY_SCHEDULE = (3, 6, 10)
DEFAULT_MAX_WAIT_SECS  = 15
DEFAULT_CIRCUIT_MAX    = 8
DEFAULT_CIRCUIT_WINDOW = 60
DEFAULT_CIRCUIT_COOLDOWN = 60


def retry_enabled() -> bool:
    v = (os.environ.get(ENV_ENABLED, "") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def parse_retry_schedule(value: Optional[str]) -> tuple[int, ...]:
    """Parse 'WATCHER_ARM_QUOTE_RETRY_SECONDS=3,6,10' into (3, 6, 10).
    Empty / malformed -> DEFAULT_RETRY_SCHEDULE."""
    if not value:
        return DEFAULT_RETRY_SCHEDULE
    out: list[int] = []
    for part in str(value).split(","):
        part = part.strip()
        if not part:
            continue
        try:
            n = int(part)
        except ValueError:
            try:
                n = int(float(part))
            except ValueError:
                continue
        if n > 0 and n <= 600:  # sanity cap: 10 minutes per step
            out.append(n)
    return tuple(out) if out else DEFAULT_RETRY_SCHEDULE


def _env_int(key: str, default: int) -> int:
    try:
        return int(os.environ.get(key, default))
    except (TypeError, ValueError):
        return default


def current_retry_schedule() -> tuple[int, ...]:
    return parse_retry_schedule(os.environ.get(ENV_RETRY_SECONDS, ""))


def current_max_wait_seconds() -> int:
    return _env_int(ENV_MAX_WAIT_SECONDS, DEFAULT_MAX_WAIT_SECS)


def current_circuit_config() -> tuple[int, int, int]:
    """(max_failures, window_secs, cooldown_secs)."""
    return (
        _env_int(ENV_CIRCUIT_MAX,    DEFAULT_CIRCUIT_MAX),
        _env_int(ENV_CIRCUIT_WINDOW, DEFAULT_CIRCUIT_WINDOW),
        _env_int(ENV_CIRCUIT_COOLDOWN, DEFAULT_CIRCUIT_COOLDOWN),
    )


# ---------------------------------------------------------------------------
# Circuit breaker (in-memory, per-pod)
# ---------------------------------------------------------------------------

class CircuitBreaker:
    """Simple sliding-window failure counter per pod_id.

    Thread-safe (one lock). In-memory only — no Redis, no DB. Bounded
    memory by pruning old failures on each call.
    """

    def __init__(self,
                 *,
                 time_fn: Callable[[], float] = time.monotonic):
        self._lock = threading.Lock()
        self._failures: dict[str, list[float]] = {}
        self._opened_until: dict[str, float] = {}
        self._time = time_fn

    def _now(self) -> float:
        return float(self._time())

    def _key(self, pod_id: Optional[str]) -> str:
        # Group missing/None pod ids under a single sentinel so a misconfigured
        # pod cannot bypass the breaker. We classify these as a single bucket.
        return (pod_id or "__no_pod__").strip()

    def is_open(self, pod_id: Optional[str]) -> bool:
        cfg_max, cfg_window, cfg_cooldown = current_circuit_config()
        if cfg_max <= 0:
            return False
        key = self._key(pod_id)
        now = self._now()
        with self._lock:
            until = self._opened_until.get(key, 0.0)
            if until and now < until:
                return True
            if until and now >= until:
                # Cooldown elapsed — clear state for that pod.
                self._opened_until.pop(key, None)
                self._failures.pop(key, None)
            return False

    def record_failure(self, pod_id: Optional[str]) -> bool:
        """Record one retry-failure for this pod. Returns True if this
        recording caused the breaker to OPEN."""
        cfg_max, cfg_window, cfg_cooldown = current_circuit_config()
        if cfg_max <= 0:
            return False
        key = self._key(pod_id)
        now = self._now()
        with self._lock:
            # If breaker already open, just track time — do not re-open.
            until = self._opened_until.get(key, 0.0)
            if until and now < until:
                return False
            # Prune outside window
            bucket = self._failures.setdefault(key, [])
            cutoff = now - max(1, cfg_window)
            bucket[:] = [t for t in bucket if t >= cutoff]
            bucket.append(now)
            if len(bucket) >= cfg_max:
                self._opened_until[key] = now + max(1, cfg_cooldown)
                # Reset counter so on cooldown elapsed we restart cleanly
                self._failures[key] = []
                log.warning(
                    "WATCHER_ARM_RETRY_CIRCUIT_OPEN pod_id=%s failure_count=%d "
                    "window_seconds=%d cooldown_seconds=%d",
                    key, cfg_max, cfg_window, cfg_cooldown,
                )
                return True
        return False

    def record_success(self, pod_id: Optional[str]) -> None:
        """A successful retry resets the failure counter for this pod
        (does NOT close an already-open breaker — cooldown rules apply)."""
        key = self._key(pod_id)
        with self._lock:
            if key not in self._opened_until:
                self._failures.pop(key, None)

    # Test helpers
    def reset(self) -> None:
        with self._lock:
            self._failures.clear()
            self._opened_until.clear()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "failures":      {k: list(v) for k, v in self._failures.items()},
                "opened_until":  dict(self._opened_until),
            }


# Module-level singleton — one breaker per process.
_DEFAULT_BREAKER = CircuitBreaker()


def get_default_breaker() -> CircuitBreaker:
    return _DEFAULT_BREAKER


# ---------------------------------------------------------------------------
# RetryOutcome
# ---------------------------------------------------------------------------

@dataclass
class RetryOutcome:
    """Result of a retry attempt sequence.

    Fields are exactly what ap/queue.py needs to (a) decide whether to
    finish arming or block, and (b) write the audit blob.
    """
    armed:               bool
    final_decision:      str
    final_reason_code:   str
    final_bucket:        str
    arm_attempts:        list[dict[str, Any]] = field(default_factory=list)
    arm_retry_count:     int = 0
    retry_started_at:    Optional[str] = None
    retry_ended_at:      Optional[str] = None
    circuit_breaker_open: bool = False
    recovered_by_retry:  bool = False


# ---------------------------------------------------------------------------
# Quote-snapshot callable shape
# ---------------------------------------------------------------------------
# The integration layer passes two callables:
#
#   refresh_quote() -> dict {underlying: {...}, option: {...}}
#       Re-fetches a FRESH quote snapshot. Must NEVER submit to broker.
#       Returns {} on failure (we'll treat as quote-refresh-failed).
#
#   arm_callable(snapshot) -> (armed: bool, raw_reason: str|None)
#       Re-runs the watcher arm logic against a specific snapshot.
#       Returns the same tuple shape the queue's `entry_watcher.watch()`
#       call exposes. Must NEVER submit to broker.
#
# Both are caller-owned. The retry layer never touches DB or broker.

QuoteSnapshot   = dict[str, Any]   # {"underlying": {...}, "option": {...}}
RefreshQuoteFn  = Callable[[], QuoteSnapshot]
ArmCallableFn   = Callable[[QuoteSnapshot], tuple[bool, Optional[str]]]


# ---------------------------------------------------------------------------
# Retry executor
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fresh_quote_block(
    *,
    direction: Optional[str],
    trigger:   Optional[float],
    stop:      Optional[float],
    snapshot:  QuoteSnapshot,
) -> Optional[dict[str, Any]]:
    """Given a fresh quote snapshot, decide whether the setup is invalidated
    or drifted. Returns the offending {bucket, reason_code, decision}
    dict, or None if the setup is still valid against the fresh quote."""
    underlying = (snapshot or {}).get("underlying")
    if quote_is_missing(underlying) or quote_is_stale(underlying):
        return None  # quote still not ready — caller decides whether to keep retrying
    result = evaluate_fresh_quote(
        direction=direction,
        trigger=trigger,
        stop=stop,
        underlying=underlying,
    )
    if result.get("ok"):
        return None
    return {
        "bucket":         result["bucket"],
        "reason_code":    result["reason_code"],
        "final_decision": result["final_decision"],
    }


def maybe_retry_arm(
    *,
    classification: Classification,
    refresh_quote: RefreshQuoteFn,
    arm_callable: ArmCallableFn,
    direction: Optional[str],
    trigger:   Optional[float],
    stop:      Optional[float],
    pod_id:    Optional[str],
    breaker:   Optional[CircuitBreaker] = None,
    sleep_fn:  Callable[[float], None]  = time.sleep,
    schedule_override:   Optional[tuple[int, ...]] = None,
    max_wait_override:   Optional[int] = None,
    enabled_override:    Optional[bool] = None,
) -> RetryOutcome:
    """Run the bounded retry loop for a QUOTE_READINESS_UNKNOWN failure.

    Caller responsibility:
      * Classification.bucket MUST be BUCKET_QUOTE_READINESS_UNKNOWN.
        Passing any other bucket is a programming error — we surface an
        immediate non-retry decision rather than enter the loop.
      * Pass real refresh_quote() and arm_callable(snapshot) — these
        do the actual quote fetch + arm logic.

    Behavior:
      * If retry is disabled OR breaker is open: return immediately with
        RETRY_DISABLED_QUOTE_NOT_READY.
      * Else iterate the schedule. Each iteration sleeps, refreshes the
        quote, evaluates fresh-quote invalidation, then re-invokes the
        arm callable.
      * Stops EARLY on:
          - fresh-quote true invalidation (BLOCKED_TRUE_INVALIDATION)
          - fresh-quote drift             (BLOCKED_PRICE_DRIFT)
          - arm_callable returns armed=True (ARMED_AFTER_RETRY)
          - cumulative wait >= max_wait_seconds (FAILED_AFTER_RETRY)

    Never submits to broker. Never creates an order row. Never mutates
    signal/order metadata directly — caller persists the audit via
    merge_into_order_meta.
    """
    if breaker is None:
        breaker = _DEFAULT_BREAKER
    enabled = enabled_override if enabled_override is not None else retry_enabled()
    schedule = schedule_override or current_retry_schedule()
    max_wait = max_wait_override if max_wait_override is not None else current_max_wait_seconds()

    # Pre-flight guards.
    if classification.bucket != BUCKET_QUOTE_READINESS_UNKNOWN:
        # Caller used this for a non-retryable bucket. Surface an honest
        # outcome without entering the loop.
        log.debug(
            "maybe_retry_arm called for non-retryable bucket=%s reason=%s",
            classification.bucket, classification.reason_code,
        )
        return RetryOutcome(
            armed=False,
            final_decision=classification.final_decision
                or DECISION_RETRY_DISABLED_QUOTE_NOT_READY,
            final_reason_code=classification.reason_code,
            final_bucket=classification.bucket,
        )

    if not enabled:
        return RetryOutcome(
            armed=False,
            final_decision=DECISION_RETRY_DISABLED_QUOTE_NOT_READY,
            final_reason_code=classification.reason_code,
            final_bucket=classification.bucket,
        )

    if breaker.is_open(pod_id):
        log.warning(
            "WATCHER_ARM_RETRY_SKIPPED_CIRCUIT_OPEN pod_id=%s reason_code=%s",
            pod_id, classification.reason_code,
        )
        return RetryOutcome(
            armed=False,
            final_decision=DECISION_RETRY_DISABLED_QUOTE_NOT_READY,
            final_reason_code=classification.reason_code,
            final_bucket=classification.bucket,
            circuit_breaker_open=True,
        )

    # Enter the loop.
    started_at = _now_iso()
    started_monotonic = time.monotonic()
    attempts: list[dict[str, Any]] = []
    last_reason_code = classification.reason_code

    for idx, wait_seconds in enumerate(schedule, start=1):
        # Honor cumulative max-wait budget
        already_waited = time.monotonic() - started_monotonic
        if already_waited + wait_seconds > max(0, max_wait):
            # No time left to do another wait.
            break

        log.info(
            "WATCHER_ARM_RETRY attempt=%d wait_seconds=%d reason_code=%s",
            idx, wait_seconds, last_reason_code,
        )
        try:
            sleep_fn(float(wait_seconds))
        except Exception:
            pass

        # Fresh quote
        try:
            snapshot = refresh_quote() or {}
        except Exception as e:
            log.warning("WATCHER_ARM_RETRY refresh_quote failed attempt=%d: %s", idx, e)
            snapshot = {}

        snap_underlying = snapshot.get("underlying") if isinstance(snapshot, dict) else None
        snap_option     = snapshot.get("option")     if isinstance(snapshot, dict) else None

        # Revalidate against fresh quote BEFORE invoking arm callable.
        # This catches the spec §6 case where the quote becomes fresh but
        # the price has invalidated the setup.
        block = _fresh_quote_block(
            direction=direction, trigger=trigger, stop=stop,
            snapshot=snapshot,
        )
        if block is not None:
            attempts.append(build_attempt_record(
                attempt_number=idx,
                reason_code_before_attempt=last_reason_code,
                underlying=snap_underlying,
                option=snap_option,
                conceptual_bucket_after_refresh=block["bucket"],
                reason_code_after_refresh=block["reason_code"],
                decision_after_attempt=block["final_decision"],
            ))
            # Record failure on the breaker since we attempted and gave up.
            opened = breaker.record_failure(pod_id)
            return RetryOutcome(
                armed=False,
                final_decision=block["final_decision"],
                final_reason_code=block["reason_code"],
                final_bucket=block["bucket"],
                arm_attempts=attempts,
                arm_retry_count=idx,
                retry_started_at=started_at,
                retry_ended_at=_now_iso(),
                circuit_breaker_open=opened or breaker.is_open(pod_id),
                recovered_by_retry=False,
            )

        # If the quote is still missing/stale, attempts cannot succeed.
        # Record the attempt and continue the loop.
        if quote_is_missing(snap_underlying) or quote_is_stale(snap_underlying):
            # Classify what we observed for the post-refresh annotation.
            re_class = classify_arm_failure(
                "underlying_quote_missing" if quote_is_missing(snap_underlying)
                else "underlying_quote_stale",
            )
            attempts.append(build_attempt_record(
                attempt_number=idx,
                reason_code_before_attempt=last_reason_code,
                underlying=snap_underlying,
                option=snap_option,
                conceptual_bucket_after_refresh=re_class.bucket,
                reason_code_after_refresh=re_class.reason_code,
                decision_after_attempt=None,
            ))
            last_reason_code = re_class.reason_code
            continue

        # Quote is fresh and setup geometry holds. Try to arm.
        try:
            armed, post_reason = arm_callable(snapshot)
        except Exception as e:
            log.warning("WATCHER_ARM_RETRY arm_callable raised attempt=%d: %s", idx, e)
            armed, post_reason = False, f"arm_callable_exception:{type(e).__name__}"

        if armed:
            attempts.append(build_attempt_record(
                attempt_number=idx,
                reason_code_before_attempt=last_reason_code,
                underlying=snap_underlying,
                option=snap_option,
                conceptual_bucket_after_refresh=None,
                reason_code_after_refresh=None,
                decision_after_attempt=DECISION_ARMED_AFTER_RETRY,
            ))
            breaker.record_success(pod_id)
            return RetryOutcome(
                armed=True,
                final_decision=DECISION_ARMED_AFTER_RETRY,
                final_reason_code=classification.reason_code,  # original reason
                final_bucket=classification.bucket,
                arm_attempts=attempts,
                arm_retry_count=idx,
                retry_started_at=started_at,
                retry_ended_at=_now_iso(),
                recovered_by_retry=True,
            )

        # Arm returned False even with fresh quote. Reclassify the new
        # reason — could be drift, true invalidation, or another quote
        # readiness issue.
        post_class = classify_arm_failure(post_reason, fresh_quote=snap_underlying)
        attempts.append(build_attempt_record(
            attempt_number=idx,
            reason_code_before_attempt=last_reason_code,
            underlying=snap_underlying,
            option=snap_option,
            conceptual_bucket_after_refresh=post_class.bucket,
            reason_code_after_refresh=post_class.reason_code,
            decision_after_attempt=post_class.final_decision,
        ))

        # If the new bucket is non-retryable, stop here.
        if post_class.bucket in (
            BUCKET_TRUE_INVALIDATION,
            BUCKET_FRESH_PRICE_DRIFT,
            BUCKET_DOMAIN_SAFETY_BLOCK,
        ):
            opened = breaker.record_failure(pod_id)
            return RetryOutcome(
                armed=False,
                final_decision=post_class.final_decision
                    or DECISION_BLOCKED_TRUE_INVALIDATION,
                final_reason_code=post_class.reason_code,
                final_bucket=post_class.bucket,
                arm_attempts=attempts,
                arm_retry_count=idx,
                retry_started_at=started_at,
                retry_ended_at=_now_iso(),
                circuit_breaker_open=opened or breaker.is_open(pod_id),
                recovered_by_retry=False,
            )

        last_reason_code = post_class.reason_code or last_reason_code
        # else: continue to next iteration

    # Out of attempts / out of budget.
    opened = breaker.record_failure(pod_id)
    final_reason = last_reason_code or REASON_ARM_TIMEOUT
    return RetryOutcome(
        armed=False,
        final_decision=DECISION_FAILED_AFTER_RETRY,
        final_reason_code=final_reason if final_reason != classification.reason_code
                          else REASON_ARM_TIMEOUT,
        final_bucket=BUCKET_QUOTE_READINESS_UNKNOWN,
        arm_attempts=attempts,
        arm_retry_count=len(attempts),
        retry_started_at=started_at,
        retry_ended_at=_now_iso(),
        circuit_breaker_open=opened or breaker.is_open(pod_id),
        recovered_by_retry=False,
    )


__all__ = [
    "RetryOutcome",
    "CircuitBreaker",
    "get_default_breaker",
    "maybe_retry_arm",
    "parse_retry_schedule",
    "retry_enabled",
    "current_retry_schedule",
    "current_max_wait_seconds",
    "current_circuit_config",
    "ENV_ENABLED",
    "ENV_RETRY_SECONDS",
    "ENV_MAX_WAIT_SECONDS",
    "ENV_CIRCUIT_MAX",
    "ENV_CIRCUIT_WINDOW",
    "ENV_CIRCUIT_COOLDOWN",
]
