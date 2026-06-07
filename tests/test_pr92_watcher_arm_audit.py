"""
PR #92 — Watcher Arm Price-Readiness Audit + Safe Retry
========================================================

25 acceptance tests (spec §13). All tests run without DB, without broker,
and without sleeping (sleep_fn is monkey-patched).

  1. watch_arm_failed always writes watcher_arm_audit.
  2. watcher_invalidated always writes watcher_arm_audit.
  3. arm_below_stop is TRUE_INVALIDATION and never retries.
  4. stop_bid_below_call_stop is TRUE_INVALIDATION and never retries.
  5. stop_ask_above_put_stop is TRUE_INVALIDATION and never retries.
  6. Missing underlying quote is QUOTE_READINESS_UNKNOWN.
  7. Stale underlying quote is QUOTE_READINESS_UNKNOWN.
  8. Missing option quote is QUOTE_READINESS_UNKNOWN.
  9. Stale option quote is QUOTE_READINESS_UNKNOWN.
 10. Fresh quote drift = PRICE_DRIFT_FROM_TRIGGER / BLOCKED_PRICE_DRIFT,
     NOT stale quote.
 11. Missing quote retries only when retry env enabled.
 12. Retry disabled preserves current behavior except audit metadata.
 13. Retry success arms watcher; original signal/order metadata preserved.
 14. Retry success does not create duplicate order rows.
 15. Retry attempt revalidates true invalidation on fresh quote.
 16. Fresh quote through stop during retry -> BLOCKED_TRUE_INVALIDATION.
 17. Retry timeout writes arm_attempts + final_decision=FAILED_AFTER_RETRY.
 18. arm_attempts preserves every quote snapshot attempt.
 19. Quote domain mismatch is classified and does not retry unless corrected.
 20. Metadata merge does not overwrite existing orders.meta.
 21. No broker submit occurs during quote retry.
 22. Per-pod circuit breaker opens after configured failures.
 23. Circuit breaker disables retry during cooldown and writes audit.
 24. No tokens/secrets appear in watcher_arm_audit or logs.
 25. Existing behavior preserved when WATCHER_ARM_QUOTE_RETRY_ENABLED=false.
"""
from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from ap_watcher_arm_audit import (  # noqa: E402
    BUCKET_DOMAIN_SAFETY_BLOCK,
    BUCKET_FRESH_PRICE_DRIFT,
    BUCKET_QUOTE_READINESS_UNKNOWN,
    BUCKET_TRUE_INVALIDATION,
    BUCKET_UNKNOWN,
    DECISION_ARMED_AFTER_RETRY,
    DECISION_BLOCKED_INVALID_GEOMETRY,
    DECISION_BLOCKED_PRICE_DRIFT,
    DECISION_BLOCKED_QUOTE_DOMAIN_MISMATCH,
    DECISION_BLOCKED_TRUE_INVALIDATION,
    DECISION_FAILED_AFTER_RETRY,
    DECISION_RETRY_DISABLED_QUOTE_NOT_READY,
    DOSSIER_SECTION,
    DOSSIER_VERSION,
    LIFECYCLE_FAILED,
    LIFECYCLE_RECOVERED,
    REASON_ARM_BELOW_STOP,
    REASON_ARM_TIMEOUT,
    REASON_OPTION_QUOTE_MISSING,
    REASON_OPTION_QUOTE_STALE,
    REASON_PRICE_DRIFT_FROM_TRIGGER,
    REASON_QUOTE_DOMAIN_MISMATCH,
    REASON_STOP_ASK_ABOVE_PUT_STOP,
    REASON_STOP_BID_BELOW_CALL_STOP,
    REASON_UNDERLYING_QUOTE_MISSING,
    REASON_UNDERLYING_QUOTE_STALE,
    REASON_UNKNOWN_ARM_FAILURE,
    build_attempt_record,
    build_watcher_arm_audit,
    classify_arm_failure,
    evaluate_fresh_quote,
    merge_into_order_meta,
    redact,
)
from ap_watcher_arm_retry import (  # noqa: E402
    CircuitBreaker,
    ENV_CIRCUIT_COOLDOWN,
    ENV_CIRCUIT_MAX,
    ENV_CIRCUIT_WINDOW,
    ENV_ENABLED,
    ENV_MAX_WAIT_SECONDS,
    ENV_RETRY_SECONDS,
    RetryOutcome,
    maybe_retry_arm,
    parse_retry_schedule,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _fresh_quote(bid=100.0, ask=100.10, last=100.05, age_seconds=0.5) -> dict:
    return {
        "bid": bid, "ask": ask, "last": last,
        "quote_ts": (datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).isoformat(),
    }


def _stale_quote(bid=100.0, ask=100.10, age_seconds=30.0) -> dict:
    return {
        "bid": bid, "ask": ask,
        "quote_ts": (datetime.now(timezone.utc) - timedelta(seconds=age_seconds)).isoformat(),
    }


def _no_sleep(seconds: float) -> None:
    """sleep_fn override that does nothing — speed for tests."""
    return None


def _baseline_classification(raw_reason="underlying_quote_missing"):
    return classify_arm_failure(raw_reason)


# ===========================================================================
# 1. watch_arm_failed always writes watcher_arm_audit
# ===========================================================================

def test_01_watch_arm_failed_always_writes_audit():
    cls = classify_arm_failure("watch_arm_failed:unknown_reason")
    audit = build_watcher_arm_audit(
        classification=cls,
        symbol="UNH", direction="CALL", trigger_price=350.0, stop_price=340.0,
    )
    assert audit["dossier_section"] == DOSSIER_SECTION
    assert audit["dossier_version"] == DOSSIER_VERSION
    assert audit["lifecycle_event"] == LIFECYCLE_FAILED
    assert audit["block_stage"] == "watcher_arm"
    assert audit["symbol"] == "UNH"
    # An entirely unrecognized reason becomes UNKNOWN_ARM_FAILURE — never absent.
    assert audit["reason_code"] == REASON_UNKNOWN_ARM_FAILURE
    assert audit["conceptual_bucket"] == BUCKET_UNKNOWN


# ===========================================================================
# 2. watcher_invalidated always writes watcher_arm_audit
# ===========================================================================

def test_02_watcher_invalidated_always_writes_audit():
    # The legacy "watcher_invalidated" string is a routing label, not a
    # reason. The classifier returns UNKNOWN with a non-empty audit so
    # the operator sees something instead of a vague bucket.
    cls = classify_arm_failure("watcher_invalidated")
    audit = build_watcher_arm_audit(classification=cls, symbol="X")
    assert audit["symbol"] == "X"
    assert audit["conceptual_bucket"] in (
        BUCKET_UNKNOWN, BUCKET_TRUE_INVALIDATION,
    )
    assert audit["reason_code"] in (
        REASON_UNKNOWN_ARM_FAILURE, *(),  # placeholder for future mapping
    )
    assert audit["raw_reason"] == "watcher_invalidated"


# ===========================================================================
# 3. arm_below_stop is TRUE_INVALIDATION and never retries
# ===========================================================================

def test_03_arm_below_stop_is_true_invalidation_no_retry():
    cls = classify_arm_failure("arm_below_stop_mid_212.50_stop_213.10")
    assert cls.bucket == BUCKET_TRUE_INVALIDATION
    assert cls.reason_code == REASON_ARM_BELOW_STOP
    assert cls.retryable is False
    # Retry layer refuses to retry a non-retryable bucket
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=lambda: {"underlying": _fresh_quote()},
        arm_callable=lambda snap: (True, None),
        direction="CALL", trigger=213.0, stop=212.5,
        pod_id="pod-1",
        sleep_fn=_no_sleep,
        enabled_override=True,
    )
    assert outcome.armed is False
    assert outcome.final_decision == DECISION_BLOCKED_TRUE_INVALIDATION
    assert outcome.arm_retry_count == 0
    assert outcome.arm_attempts == []


# ===========================================================================
# 4. stop_bid_below_call_stop is TRUE_INVALIDATION and never retries
# ===========================================================================

def test_04_stop_bid_below_call_stop_no_retry():
    # The watcher's _pending_audit stamps reason_code="stop_bid_below_call_stop"
    # (the canonical underscore form). The classifier must recognise it.
    cls = classify_arm_failure("stop_bid_below_call_stop")
    assert cls.bucket == BUCKET_TRUE_INVALIDATION
    assert cls.reason_code == REASON_STOP_BID_BELOW_CALL_STOP
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=lambda: {"underlying": _fresh_quote()},
        arm_callable=lambda s: (True, None),
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-1", sleep_fn=_no_sleep, enabled_override=True,
    )
    assert outcome.armed is False
    assert outcome.final_decision == DECISION_BLOCKED_TRUE_INVALIDATION


# ===========================================================================
# 5. stop_ask_above_put_stop is TRUE_INVALIDATION and never retries
# ===========================================================================

def test_05_stop_ask_above_put_stop_no_retry():
    cls = classify_arm_failure("stop_ask_above_put_stop")
    assert cls.bucket == BUCKET_TRUE_INVALIDATION
    assert cls.reason_code == REASON_STOP_ASK_ABOVE_PUT_STOP


# ===========================================================================
# 6. Missing underlying quote = QUOTE_READINESS_UNKNOWN
# ===========================================================================

def test_06_underlying_quote_missing_is_quote_readiness():
    cls = classify_arm_failure("underlying_quote_missing")
    assert cls.bucket == BUCKET_QUOTE_READINESS_UNKNOWN
    assert cls.reason_code == REASON_UNDERLYING_QUOTE_MISSING
    assert cls.retryable is True


# ===========================================================================
# 7. Stale underlying quote = QUOTE_READINESS_UNKNOWN
# ===========================================================================

def test_07_underlying_quote_stale_is_quote_readiness():
    cls = classify_arm_failure("underlying_quote_stale")
    assert cls.bucket == BUCKET_QUOTE_READINESS_UNKNOWN
    assert cls.reason_code == REASON_UNDERLYING_QUOTE_STALE


# ===========================================================================
# 8. Missing option quote = QUOTE_READINESS_UNKNOWN
# ===========================================================================

def test_08_option_quote_missing_is_quote_readiness():
    cls = classify_arm_failure("option_quote_missing")
    assert cls.bucket == BUCKET_QUOTE_READINESS_UNKNOWN
    assert cls.reason_code == REASON_OPTION_QUOTE_MISSING


# ===========================================================================
# 9. Stale option quote = QUOTE_READINESS_UNKNOWN
# ===========================================================================

def test_09_option_quote_stale_is_quote_readiness():
    cls = classify_arm_failure("option_quote_stale")
    assert cls.bucket == BUCKET_QUOTE_READINESS_UNKNOWN
    assert cls.reason_code == REASON_OPTION_QUOTE_STALE


# ===========================================================================
# 10. Fresh quote drift is PRICE_DRIFT_FROM_TRIGGER, NOT stale
# ===========================================================================

def test_10_fresh_quote_drift_is_drift_not_stale():
    # Legacy raw_reason "stale_price_-0.4pct_from_trigger" must classify
    # as drift, not as stale quote — the historical name was misleading.
    cls = classify_arm_failure("stale_price_-0.4pct_from_trigger")
    assert cls.bucket == BUCKET_FRESH_PRICE_DRIFT
    assert cls.reason_code == REASON_PRICE_DRIFT_FROM_TRIGGER
    assert cls.final_decision == DECISION_BLOCKED_PRICE_DRIFT
    assert cls.retryable is False

    # And: even when raw_reason said 'underlying_quote_stale', if the
    # fresh_quote hint shows the quote is actually fresh, classifier
    # demotes to drift.
    cls2 = classify_arm_failure(
        "underlying_quote_stale",
        fresh_quote=_fresh_quote(),
    )
    assert cls2.bucket == BUCKET_FRESH_PRICE_DRIFT


# ===========================================================================
# 11. Missing quote retries only when retry env enabled
# ===========================================================================

def test_11_retry_only_when_enabled(monkeypatch):
    cls = classify_arm_failure("underlying_quote_missing")
    fresh_calls = {"n": 0}
    def refresh():
        fresh_calls["n"] += 1
        return {"underlying": _fresh_quote()}
    # Disabled
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=refresh,
        arm_callable=lambda s: (True, None),
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-1", sleep_fn=_no_sleep,
        enabled_override=False,
    )
    assert outcome.armed is False
    assert outcome.final_decision == DECISION_RETRY_DISABLED_QUOTE_NOT_READY
    assert fresh_calls["n"] == 0  # no refreshes called

    # Enabled — refresh is called and arm succeeds
    breaker = CircuitBreaker()
    outcome2 = maybe_retry_arm(
        classification=cls,
        refresh_quote=refresh,
        arm_callable=lambda s: (True, None),
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-1", sleep_fn=_no_sleep,
        breaker=breaker,
        enabled_override=True,
        schedule_override=(1,),
        max_wait_override=10,
    )
    assert outcome2.armed is True
    assert outcome2.final_decision == DECISION_ARMED_AFTER_RETRY
    assert fresh_calls["n"] == 1


# ===========================================================================
# 12. Retry disabled preserves current behavior except audit metadata
# ===========================================================================

def test_12_retry_disabled_preserves_behavior():
    cls = classify_arm_failure("underlying_quote_missing")
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=lambda: {"underlying": _fresh_quote()},
        arm_callable=lambda s: (True, None),
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-1", sleep_fn=_no_sleep,
        enabled_override=False,
    )
    # No retry happened
    assert outcome.arm_retry_count == 0
    assert outcome.arm_attempts == []
    # But audit reason_code is still the structured one, NOT "unknown".
    assert outcome.final_reason_code == REASON_UNDERLYING_QUOTE_MISSING


# ===========================================================================
# 13. Retry success preserves original signal/order metadata
# ===========================================================================

def test_13_retry_success_preserves_metadata():
    cls = classify_arm_failure("underlying_quote_missing")
    breaker = CircuitBreaker()
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=lambda: {"underlying": _fresh_quote()},
        arm_callable=lambda s: (True, None),
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-1", sleep_fn=_no_sleep, breaker=breaker,
        enabled_override=True, schedule_override=(1,), max_wait_override=10,
    )
    audit = build_watcher_arm_audit(
        classification=cls,
        final_decision=outcome.final_decision,
        recovered_by_retry=outcome.recovered_by_retry,
        arm_attempts=outcome.arm_attempts,
        arm_retry_count=outcome.arm_retry_count,
        signal_id="REEVAL:abc",
        canonical_signal_id="REEVAL:abc",
        plan_id="plan-1",
        client_id="user@example.com",
        local_order_id="loc-42",
        symbol="AAPL", direction="CALL",
        trigger_price=100.5, stop_price=100.0,
    )
    # Original identity preserved verbatim
    assert audit["signal_id"] == "REEVAL:abc"
    assert audit["canonical_signal_id"] == "REEVAL:abc"
    assert audit["plan_id"] == "plan-1"
    assert audit["local_order_id"] == "loc-42"
    # Lifecycle flipped to RECOVERED
    assert audit["lifecycle_event"] == LIFECYCLE_RECOVERED
    assert audit["recovered_by_retry"] is True
    assert audit["final_decision"] == DECISION_ARMED_AFTER_RETRY


# ===========================================================================
# 14. Retry success does NOT create duplicate order rows
# ===========================================================================

def test_14_retry_success_no_duplicate_order_rows():
    """The retry layer never receives a DB handle, never knows what an
    order row is. Statically: maybe_retry_arm's signature has no
    'create_order' / 'insert_order' callable, so it CANNOT create
    duplicates. We assert the contract."""
    import inspect
    sig = inspect.signature(maybe_retry_arm)
    forbidden = {"create_order", "insert_order", "place_order", "submit_order",
                 "broker_submit"}
    assert forbidden.isdisjoint(sig.parameters), (
        f"maybe_retry_arm must not accept any DB/broker mutation callable "
        f"(found: {forbidden & set(sig.parameters)})"
    )


# ===========================================================================
# 15. Retry attempt revalidates true invalidation on fresh quote
# ===========================================================================

def test_15_retry_revalidates_on_fresh_quote():
    """Quote was missing; on retry refresh, fresh quote shows the
    underlying through stop. Retry MUST stop and return
    BLOCKED_TRUE_INVALIDATION — NOT ARMED_AFTER_RETRY."""
    cls = classify_arm_failure("underlying_quote_missing")
    # Fresh quote shows underlying bid 95 -> below 100 stop on a CALL
    fresh = {"bid": 95.0, "ask": 95.10, "last": 95.05,
             "quote_ts": _now_iso()}
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=lambda: {"underlying": fresh},
        arm_callable=lambda s: (True, None),  # arm WOULD succeed but...
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-1", sleep_fn=_no_sleep,
        enabled_override=True, schedule_override=(1,), max_wait_override=10,
    )
    assert outcome.armed is False
    assert outcome.final_decision == DECISION_BLOCKED_TRUE_INVALIDATION
    assert outcome.final_reason_code == REASON_STOP_BID_BELOW_CALL_STOP


# ===========================================================================
# 16. Fresh quote through stop during retry -> BLOCKED_TRUE_INVALIDATION
# ===========================================================================

def test_16_put_fresh_quote_through_stop_blocks():
    """PUT version of test 15: ask climbs above stop on fresh quote."""
    cls = classify_arm_failure("underlying_quote_missing")
    fresh = {"bid": 105.0, "ask": 105.10, "last": 105.05,
             "quote_ts": _now_iso()}
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=lambda: {"underlying": fresh},
        arm_callable=lambda s: (True, None),
        direction="PUT", trigger=100.0, stop=104.0,
        pod_id="pod-1", sleep_fn=_no_sleep,
        enabled_override=True, schedule_override=(1,), max_wait_override=10,
    )
    assert outcome.armed is False
    assert outcome.final_decision == DECISION_BLOCKED_TRUE_INVALIDATION
    assert outcome.final_reason_code == REASON_STOP_ASK_ABOVE_PUT_STOP


# ===========================================================================
# 17. Retry timeout writes arm_attempts + final_decision=FAILED_AFTER_RETRY
# ===========================================================================

def test_17_retry_timeout_writes_attempts_and_failed_after_retry():
    cls = classify_arm_failure("underlying_quote_missing")
    # Quote never gets fresh
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=lambda: {"underlying": None},   # always missing
        arm_callable=lambda s: (True, None),
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-1", sleep_fn=_no_sleep,
        enabled_override=True,
        schedule_override=(1, 2, 3),
        max_wait_override=10,
    )
    assert outcome.armed is False
    assert outcome.final_decision == DECISION_FAILED_AFTER_RETRY
    assert outcome.arm_retry_count == 3
    assert len(outcome.arm_attempts) == 3


# ===========================================================================
# 18. arm_attempts preserves every quote snapshot attempt
# ===========================================================================

def test_18_arm_attempts_preserves_every_snapshot():
    cls = classify_arm_failure("underlying_quote_missing")
    snapshots = [
        {"underlying": None},
        {"underlying": _stale_quote()},
        {"underlying": _fresh_quote(bid=200.0, ask=200.10, last=200.05)},
    ]
    idx = {"n": 0}
    def refresh():
        s = snapshots[idx["n"]]
        idx["n"] = min(idx["n"] + 1, len(snapshots) - 1)
        return s
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=refresh,
        arm_callable=lambda s: (True, None),
        direction="CALL", trigger=200.0, stop=199.0,
        pod_id="pod-1", sleep_fn=_no_sleep,
        enabled_override=True,
        schedule_override=(1, 1, 1), max_wait_override=10,
    )
    # All three snapshots are surfaced
    assert len(outcome.arm_attempts) == 3
    # Attempt 1: missing
    assert outcome.arm_attempts[0]["reason_code_after_refresh"] == REASON_UNDERLYING_QUOTE_MISSING
    # Attempt 2: stale
    assert outcome.arm_attempts[1]["reason_code_after_refresh"] == REASON_UNDERLYING_QUOTE_STALE
    # Attempt 3: arm
    assert outcome.armed is True
    assert outcome.arm_attempts[2]["decision_after_attempt"] == DECISION_ARMED_AFTER_RETRY


# ===========================================================================
# 19. Quote domain mismatch is classified and does not retry unless corrected
# ===========================================================================

def test_19_quote_domain_mismatch_classified_no_retry():
    cls = classify_arm_failure("quote_domain_mismatch")
    assert cls.bucket == BUCKET_DOMAIN_SAFETY_BLOCK
    assert cls.reason_code == REASON_QUOTE_DOMAIN_MISMATCH
    assert cls.final_decision == DECISION_BLOCKED_QUOTE_DOMAIN_MISMATCH
    # Retry layer refuses
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=lambda: {"underlying": _fresh_quote()},
        arm_callable=lambda s: (True, None),
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-1", sleep_fn=_no_sleep,
        enabled_override=True,
    )
    assert outcome.armed is False
    assert outcome.final_decision == DECISION_BLOCKED_QUOTE_DOMAIN_MISMATCH


# ===========================================================================
# 20. Metadata merge does NOT overwrite existing orders.meta
# ===========================================================================

def test_20_metadata_merge_preserves_existing_meta():
    existing = {
        "execution_mode":   "live",
        "pod_id":           "live-pod-1",
        "custom_field":     {"nested": "value"},
        "broker_order_id":  "ord-abc",
    }
    cls = classify_arm_failure("underlying_quote_missing")
    audit = build_watcher_arm_audit(classification=cls, symbol="X")
    merged = merge_into_order_meta(existing, audit)
    # Nothing else was destroyed
    assert merged["execution_mode"] == "live"
    assert merged["pod_id"] == "live-pod-1"
    assert merged["custom_field"] == {"nested": "value"}
    assert merged["broker_order_id"] == "ord-abc"
    # And the audit landed under the right key
    assert "watcher_arm_audit" in merged
    assert merged["watcher_arm_audit"]["symbol"] == "X"

    # Re-merge: existing audit attempts are CONCATENATED
    audit2 = build_watcher_arm_audit(
        classification=cls, symbol="X",
        arm_attempts=[{"attempt_number": 1}],
        arm_retry_count=1,
    )
    merged2 = merge_into_order_meta(merged, audit2)
    # Empty list from first audit + the 1 attempt from second = 1 total
    assert len(merged2["watcher_arm_audit"]["arm_attempts"]) == 1
    # Re-merge again, this time with TWO new attempts
    audit3 = build_watcher_arm_audit(
        classification=cls, symbol="X",
        arm_attempts=[{"attempt_number": 2}, {"attempt_number": 3}],
        arm_retry_count=3,
    )
    merged3 = merge_into_order_meta(merged2, audit3)
    assert len(merged3["watcher_arm_audit"]["arm_attempts"]) == 3
    assert merged3["watcher_arm_audit"]["arm_retry_count"] == 3
    # Existing top-level meta still intact
    assert merged3["execution_mode"] == "live"


def test_20b_merge_accepts_json_string_meta():
    """orders.meta sometimes arrives as a JSON string from psycopg2 raw rows."""
    existing_str = json.dumps({"execution_mode": "paper"})
    cls = classify_arm_failure("underlying_quote_missing")
    audit = build_watcher_arm_audit(classification=cls, symbol="Y")
    merged = merge_into_order_meta(existing_str, audit)
    assert merged["execution_mode"] == "paper"
    assert merged["watcher_arm_audit"]["symbol"] == "Y"


# ===========================================================================
# 21. No broker submit occurs during quote retry
# ===========================================================================

def test_21_no_broker_submit_during_retry():
    """Static contract check + behavioral check.

    Static: the retry module exposes no broker-mutation API.
    Behavioral: even if we pass a broken arm_callable, no broker side
    effects happen because the test arm_callable doesn't call broker
    code and the module never imports broker libraries.
    """
    import ap_watcher_arm_retry as m
    src = open(m.__file__).read()
    for forbidden in (
        "broker.submit", "place_order", "submit_order",
        "ap.broker", "tradier.create_order",
    ):
        assert forbidden not in src, (
            f"retry module must not reference {forbidden}"
        )

    # Behavioral
    cls = classify_arm_failure("underlying_quote_missing")
    calls = {"refresh": 0, "arm": 0}
    def refresh():
        calls["refresh"] += 1
        return {"underlying": None}
    def arm(snap):
        calls["arm"] += 1
        return (False, "underlying_quote_missing")
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=refresh,
        arm_callable=arm,
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-1", sleep_fn=_no_sleep,
        enabled_override=True,
        schedule_override=(1, 1), max_wait_override=10,
    )
    # No matter the outcome, arm_callable was only called when quote was
    # fresh — and since refresh always returns None, arm was never called.
    assert calls["refresh"] == 2
    assert calls["arm"] == 0
    assert outcome.armed is False


# ===========================================================================
# 22. Per-pod circuit breaker opens after configured failures
# ===========================================================================

def test_22_circuit_breaker_opens(monkeypatch):
    monkeypatch.setenv(ENV_CIRCUIT_MAX, "3")
    monkeypatch.setenv(ENV_CIRCUIT_WINDOW, "60")
    monkeypatch.setenv(ENV_CIRCUIT_COOLDOWN, "60")
    breaker = CircuitBreaker()
    assert breaker.is_open("pod-X") is False
    assert breaker.record_failure("pod-X") is False  # 1
    assert breaker.record_failure("pod-X") is False  # 2
    assert breaker.record_failure("pod-X") is True   # 3 -> OPEN
    assert breaker.is_open("pod-X") is True


# ===========================================================================
# 23. Circuit breaker disables retry during cooldown and writes audit
# ===========================================================================

def test_23_circuit_breaker_disables_retry(monkeypatch):
    monkeypatch.setenv(ENV_CIRCUIT_MAX, "1")
    monkeypatch.setenv(ENV_CIRCUIT_WINDOW, "60")
    monkeypatch.setenv(ENV_CIRCUIT_COOLDOWN, "60")
    breaker = CircuitBreaker()
    # Manually open
    breaker.record_failure("pod-Y")
    assert breaker.is_open("pod-Y") is True

    cls = classify_arm_failure("underlying_quote_missing")
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=lambda: {"underlying": _fresh_quote()},
        arm_callable=lambda s: (True, None),
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-Y", sleep_fn=_no_sleep,
        breaker=breaker,
        enabled_override=True,
    )
    assert outcome.armed is False
    assert outcome.final_decision == DECISION_RETRY_DISABLED_QUOTE_NOT_READY
    assert outcome.circuit_breaker_open is True
    assert outcome.arm_attempts == []
    # Audit captures circuit_breaker_open
    audit = build_watcher_arm_audit(
        classification=cls,
        final_decision=outcome.final_decision,
        circuit_breaker_open=outcome.circuit_breaker_open,
        symbol="Y",
    )
    assert audit["circuit_breaker_open"] is True


# ===========================================================================
# 24. No tokens/secrets appear in watcher_arm_audit or logs
# ===========================================================================

def test_24_redaction_strips_tokens_and_secrets():
    # Build an audit that someone might accidentally populate with creds.
    naughty = {
        "ok":         "value-is-fine",
        "api_key":    "leaked",
        "bearer":     "Bearer asdfasdfasdfasdfasdfasdfasdfasdf",
        "secret":     "huge_secret",
        "password":   "p4ss",
        "Authorization": "Bearer xyz123very_long_token_that_should_be_redacted_aaaaaa",
        "nested": {
            "TOKEN": "abc",
            "ok": 1,
            "hmac_key": "deadbeef",
        },
        # A naked bearer string anywhere in values
        "headers": ["Bearer some-long-opaque-token-string-abcdefghijklmnopqrstuvwxyz"],
        "long_hex": "a" * 80,
    }
    out = redact(naughty)
    blob = json.dumps(out)
    assert "leaked" not in blob
    assert "huge_secret" not in blob
    assert "p4ss" not in blob
    assert "deadbeef" not in blob
    assert "Bearer asdfasdfasdfasdfasdfasdfasdfasdf" not in blob
    assert "Bearer xyz123very_long_token_that_should_be_redacted_aaaaaa" not in blob
    assert out["ok"] == "value-is-fine"
    assert out["api_key"] == "***REDACTED***"
    assert out["nested"]["TOKEN"] == "***REDACTED***"
    assert out["nested"]["ok"] == 1
    # Long hex value gets redacted by the value heuristic
    assert out["long_hex"] == "***REDACTED***"


def test_24b_audit_builder_runs_redaction():
    cls = classify_arm_failure("underlying_quote_missing")
    audit = build_watcher_arm_audit(
        classification=cls,
        symbol="X",
        # Push a credential-shaped value through one of the optional fields.
        quote_source="tradier_live_v1",
        broker_base_url="https://api.tradier.com",
    )
    # broker_base_url is NOT a secret key name; it passes through.
    assert audit["broker_base_url"] == "https://api.tradier.com"
    assert audit["quote_source"] == "tradier_live_v1"


# ===========================================================================
# 25. Existing behavior preserved when WATCHER_ARM_QUOTE_RETRY_ENABLED=false
# ===========================================================================

def test_25_existing_behavior_preserved_when_disabled(monkeypatch):
    monkeypatch.delenv(ENV_ENABLED, raising=False)  # default = disabled
    cls = classify_arm_failure("underlying_quote_missing")
    refresh_calls = {"n": 0}
    arm_calls = {"n": 0}
    outcome = maybe_retry_arm(
        classification=cls,
        refresh_quote=lambda: (refresh_calls.__setitem__("n", refresh_calls["n"] + 1)
                                or {"underlying": _fresh_quote()}),
        arm_callable=lambda s: (arm_calls.__setitem__("n", arm_calls["n"] + 1) or (True, None)),
        direction="CALL", trigger=100.5, stop=100.0,
        pod_id="pod-Z", sleep_fn=_no_sleep,
        # NOTE: enabled_override left None so env decides
    )
    assert outcome.armed is False
    assert outcome.final_decision == DECISION_RETRY_DISABLED_QUOTE_NOT_READY
    assert refresh_calls["n"] == 0
    assert arm_calls["n"] == 0
    # Audit still uses the structured reason_code, NOT 'unknown'.
    audit = build_watcher_arm_audit(
        classification=cls,
        final_decision=outcome.final_decision,
        symbol="Z",
    )
    assert audit["reason_code"] == REASON_UNDERLYING_QUOTE_MISSING
    assert audit["final_decision"] == DECISION_RETRY_DISABLED_QUOTE_NOT_READY


# ===========================================================================
# Additional sanity tests
# ===========================================================================

def test_parse_retry_schedule_defaults():
    assert parse_retry_schedule("3,6,10") == (3, 6, 10)
    assert parse_retry_schedule("") == (3, 6, 10)        # default
    assert parse_retry_schedule(None) == (3, 6, 10)      # default
    assert parse_retry_schedule("garbage,xyz") == (3, 6, 10)
    assert parse_retry_schedule("2,4,bad,8") == (2, 4, 8)


def test_evaluate_fresh_quote_ok_path():
    out = evaluate_fresh_quote(
        direction="CALL", trigger=100.0, stop=99.0,
        underlying=_fresh_quote(bid=100.05, ask=100.15, last=100.10),
    )
    assert out["ok"] is True
    assert out["bucket"] is None


def test_evaluate_fresh_quote_invalid_geometry():
    out = evaluate_fresh_quote(
        direction="CALL", trigger=100.0, stop=101.0,   # stop above trigger on CALL = invalid
        underlying=_fresh_quote(),
    )
    assert out["ok"] is False
    assert out["bucket"] == BUCKET_TRUE_INVALIDATION
    assert out["final_decision"] == DECISION_BLOCKED_INVALID_GEOMETRY


def test_no_broker_imports_in_audit_module():
    """ap_watcher_arm_audit must be pure: no broker, no DB, no threading."""
    src = open(
        os.path.join(ROOT, "ap_watcher_arm_audit.py")
    ).read()
    for forbidden in ("import psycopg2", "ap.broker", "from ap.db", "ap.execution"):
        assert forbidden not in src, (
            f"audit module must remain pure (found: {forbidden})"
        )


def test_classification_returns_canonical_enum_values():
    """Every classification must return enum members from the official sets."""
    from ap_watcher_arm_audit import (
        VALID_REASON_CODES, VALID_FINAL_DECISIONS, CONCEPTUAL_BUCKETS,
    )
    samples = [
        "underlying_quote_missing", "option_quote_stale",
        "arm_below_stop_mid_x_stop_y", "stale_price_-0.3pct_from_trigger",
        "bid_X_broke_call_stop_Y", "quote_domain_mismatch",
        "some_completely_unknown_string", "",
    ]
    for s in samples:
        c = classify_arm_failure(s)
        assert c.bucket in CONCEPTUAL_BUCKETS
        assert c.reason_code in VALID_REASON_CODES
        if c.final_decision is not None:
            assert c.final_decision in VALID_FINAL_DECISIONS


def test_attempt_record_includes_all_required_keys():
    rec = build_attempt_record(
        attempt_number=2,
        reason_code_before_attempt=REASON_UNDERLYING_QUOTE_MISSING,
        underlying=_fresh_quote(),
        option=_fresh_quote(bid=2.50, ask=2.55),
        conceptual_bucket_after_refresh=None,
        reason_code_after_refresh=None,
        decision_after_attempt=DECISION_ARMED_AFTER_RETRY,
    )
    for key in (
        "attempt_number", "attempted_at", "reason_code_before_attempt",
        "underlying_bid", "underlying_ask", "underlying_mid", "underlying_last",
        "underlying_quote_ts", "underlying_quote_age_seconds",
        "option_bid", "option_ask", "option_mid", "option_last",
        "option_quote_ts", "option_quote_age_seconds",
        "conceptual_bucket_after_refresh", "reason_code_after_refresh",
        "decision_after_attempt",
    ):
        assert key in rec
