"""
Phase 5 tests: post-cancel retry decision engine.

Behavior under test (from audit prompt)
---------------------------------------
  Max 2 retries, 15-30s delay, alignment-gated.
  Skip retry if cancel_reason in:
    thesis_invalid, spread_wide, runaway_quote, positions_full, lost_handoff,
    risk_gate_blocked.
  Caller logs: ENTRY_RETRY_ARMED, ENTRY_RETRY_SUBMITTED, ENTRY_RETRY_ABORTED.
  No duplicate positions (orchestration side; tested at decision layer that
  we never ARM twice past max_attempts).

Run:
    pytest tests/test_phase5_post_cancel_retry.py -xvs
"""
from __future__ import annotations

import os
import random
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]


# ============================================================
# 1. Constants / env vars
# ============================================================

class TestPhase5Constants:
    def test_module_imports(self):
        import ap.post_cancel_retry as m
        assert hasattr(m, "evaluate_retry")
        assert hasattr(m, "RetryDecision")

    def test_max_attempts_default_2(self):
        import ap.post_cancel_retry as m
        assert m.ENTRY_RETRY_MAX_ATTEMPTS == 2

    def test_delay_bounds_default_15_30(self):
        import ap.post_cancel_retry as m
        assert m.ENTRY_RETRY_DELAY_MIN_SECS == 15
        assert m.ENTRY_RETRY_DELAY_MAX_SECS == 30

    def test_alignment_drift_default(self):
        import ap.post_cancel_retry as m
        assert m.ENTRY_RETRY_ALIGNMENT_DRIFT_PCT == pytest.approx(0.002)


# ============================================================
# 2. Non-retryable reasons \u2014 the explicit skip list
# ============================================================

class TestNonRetryableReasons:
    """Each of these reasons must ABORT with NON_RETRYABLE_REASON."""

    REASONS = [
        "thesis_invalid",
        "spread_wide",
        "runaway_quote",
        "runaway_quote_at_submit",
        "positions_full",
        "lost_handoff",
        "lost_handoff_systemic",
        "risk_gate_blocked",
        "daily_trade_cap",
        "kill_switch_active",
    ]

    @pytest.mark.parametrize("reason", REASONS)
    def test_each_reason_aborts(self, reason):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason=reason, underlying_spot=185.0)
        assert d.action == "ABORT", f"{reason} should ABORT"
        assert d.reason_code == "NON_RETRYABLE_REASON"

    def test_reason_case_insensitive(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000", "meta": {}}
        d = evaluate_retry(canceled_order=order, cancel_reason="RUNAWAY_QUOTE")
        assert d.action == "ABORT"
        assert d.reason_code == "NON_RETRYABLE_REASON"


# ============================================================
# 3. Retryable reasons \u2014 the explicit allow-list arms
# ============================================================

class TestRetryableReasons:
    REASONS = [
        "entry_max_age_normal_reached",
        "entry_max_age_aplus_reached",
        "stale_entry_timeout",
        "missed_move",
        "broker_transient_error",
    ]

    @pytest.mark.parametrize("reason", REASONS)
    def test_each_reason_arms_when_aligned(self, reason):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason=reason,
                           underlying_spot=185.0,  # exactly at signal entry
                           rng=random.Random(42))
        assert d.action == "ARM", f"{reason} should ARM (aligned, attempts=0)"
        assert d.reason_code == "RETRY_ARMED"
        assert 15.0 <= d.wait_secs <= 30.0

    def test_unknown_reason_fails_closed(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="something_new",
                           underlying_spot=185.0)
        assert d.action == "ABORT"
        assert d.reason_code == "UNKNOWN_REASON_FAIL_CLOSED"


# ============================================================
# 4. Max-attempts gate
# ============================================================

class TestMaxAttempts:
    def test_first_retry_armed(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=185.0)
        assert d.action == "ARM"
        assert d.attempt_number == 1
        assert d.max_attempts == 2

    def test_second_retry_armed(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 1}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=185.0)
        assert d.action == "ARM"
        assert d.attempt_number == 2

    def test_third_retry_aborted(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 2}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=185.0)
        assert d.action == "ABORT"
        assert d.reason_code == "MAX_ATTEMPTS_REACHED"
        assert d.attempt_number == 3

    def test_far_above_max_aborted(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 99}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=185.0)
        assert d.action == "ABORT"
        assert d.reason_code == "MAX_ATTEMPTS_REACHED"


# ============================================================
# 5. Alignment gate
# ============================================================

class TestAlignmentGate:
    def test_call_aligned_at_signal_arms(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=185.0)
        assert d.action == "ARM"
        assert d.alignment_ok is True

    def test_call_drifted_down_aborts(self):
        from ap.post_cancel_retry import evaluate_retry
        # 0.002 drift = 0.37 ; spot at 184.5 is well below floor
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=184.0)
        assert d.action == "ABORT"
        assert d.reason_code == "ALIGNMENT_LOST"
        assert d.alignment_ok is False

    def test_put_aligned_at_signal_arms(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "PUT", "symbol": "QCOM", "contract": "QCOM260523P00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=185.0)
        assert d.action == "ARM"
        assert d.alignment_ok is True

    def test_put_drifted_up_aborts(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "PUT", "symbol": "QCOM", "contract": "QCOM260523P00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=186.0)
        assert d.action == "ABORT"
        assert d.reason_code == "ALIGNMENT_LOST"

    def test_no_signal_entry_allows_retry(self):
        """Legacy orders without signal_entry_price still get the retry.
        Same back-compat behavior as retry_engine.py gate 3."""
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"retry_attempts": 0}}  # no signal_entry_price
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=185.0)
        assert d.action == "ARM"
        assert d.alignment_ok is None  # could not determine, allowed by default

    def test_no_spot_allows_retry(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=None)
        assert d.action == "ARM"
        assert d.alignment_ok is None


# ============================================================
# 6. Wait time \u2014 jittered inside [15, 30]
# ============================================================

class TestWaitSecs:
    def test_wait_in_bounds(self):
        from ap.post_cancel_retry import evaluate_retry
        rng = random.Random(1234)
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        for _ in range(100):
            d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                               underlying_spot=185.0, rng=rng)
            assert d.action == "ARM"
            assert 15.0 <= d.wait_secs <= 30.0

    def test_wait_deterministic_with_seed(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d1 = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                            underlying_spot=185.0, rng=random.Random(42))
        d2 = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                            underlying_spot=185.0, rng=random.Random(42))
        assert d1.wait_secs == d2.wait_secs


# ============================================================
# 7. Retry payload \u2014 the dict caller hands to process_signal
# ============================================================

class TestRetryPayload:
    def test_payload_carries_signal_id(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000", "local_order_id": "loc-1",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0,
                          "signal_id": "sig-abc", "score": 88, "source": "scanner"}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=185.0)
        assert d.action == "ARM"
        p = d.retry_payload
        assert p["signal_id"] == "sig-abc"
        assert p["ticker"] == "QCOM"
        assert p["direction"] == "CALL"
        assert p["score"] == 88
        assert p["retry_attempt"] == 1
        assert p["retry_of_local_oid"] == "loc-1"
        assert p["retry_cancel_reason"] == "stale_entry_timeout"

    def test_payload_carries_signal_entry_price(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "PUT", "symbol": "AAPL", "contract": "AAPL260523P00200500", "local_order_id": "loc-2",
                 "meta": {"signal_entry_price": 200.5, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="missed_move",
                           underlying_spot=200.5)
        assert d.action == "ARM"
        assert d.retry_payload["signal_entry_price"] == 200.5


# ============================================================
# 8. Disabled flag
# ============================================================

class TestDisabledFlag:
    def test_disabled_returns_abort(self, monkeypatch):
        monkeypatch.setattr("ap.post_cancel_retry.ENTRY_RETRY_ENABLED", False)
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=185.0)
        assert d.action == "ABORT"
        assert d.reason_code == "RETRY_DISABLED"


# ============================================================
# 9. Reason normalization
# ============================================================

class TestReasonNormalization:
    def test_strip_prefixes(self):
        from ap.post_cancel_retry import evaluate_retry, _normalize_reason
        assert _normalize_reason("cancel_reason:RUNAWAY_QUOTE") == "runaway_quote"
        assert _normalize_reason("reason: thesis_invalid ") == "thesis_invalid"
        assert _normalize_reason("  STALE_ENTRY_TIMEOUT  ") == "stale_entry_timeout"
        assert _normalize_reason(None) == ""
        assert _normalize_reason("") == ""


# ============================================================
# 10. Decision shape \u2014 every field the caller relies on
# ============================================================

class TestDecisionShape:
    def test_arm_carries_full_context(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000", "local_order_id": "loc-3",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="stale_entry_timeout",
                           underlying_spot=185.0, rng=random.Random(1))
        assert d.action == "ARM"
        assert d.cancel_reason_normalized == "stale_entry_timeout"
        assert d.signal_entry_price == 185.0
        assert d.underlying_spot == 185.0
        assert d.direction == "CALL"
        assert d.attempt_number == 1
        assert d.wait_secs > 0

    def test_abort_carries_full_context(self):
        from ap.post_cancel_retry import evaluate_retry
        order = {"direction": "CALL", "symbol": "QCOM", "contract": "QCOM260523C00185000",
                 "meta": {"signal_entry_price": 185.0, "retry_attempts": 0}}
        d = evaluate_retry(canceled_order=order, cancel_reason="thesis_invalid",
                           underlying_spot=185.0)
        assert d.action == "ABORT"
        assert d.reason_code == "NON_RETRYABLE_REASON"
        assert d.cancel_reason_normalized == "thesis_invalid"
        assert d.direction == "CALL"
