"""P0 tests for PR #323 Seam 2: trigger-age vs recovery-age timing semantics.

MISMATCH BEING FIXED:
  * watcher polling = 15 s
  * two confirmations required
  * selector retries default ≈ 20 s × 3 attempts = 60 s
  * recovery window ≈ 300 s
  * old trigger-age gate default = 120 s → a legitimate transient-quote recovery
    that takes 90 s would be rejected by the gate even though the underlying
    has been freshly reconfirmed.

AMENDMENT:
  check_trigger_age_gate gains last_confirmed_trigger_at.  When a fresh
  underlying reconfirmation is obtained (within the current watcher pass or
  recovery pass), the age limit applies to that timestamp, not the original
  breach timestamp.  The limit itself is unchanged.

Tests A–G mirror the specification precisely.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from ap.live_submit_gates import check_trigger_age_gate, GateOutcome


def _now():
    return datetime.now(timezone.utc)


def _iso(dt):
    return dt.isoformat()


def _ago(seconds):
    return _now() - timedelta(seconds=seconds)


def _from_now(seconds):
    return _now() + timedelta(seconds=seconds)


# ═══════════════════════════════════════════════════════════════════════
# Test A: transient zero quotes; retry within budget; fresh reconfirm; PASS
# ═══════════════════════════════════════════════════════════════════════


def test_A_recovery_within_budget_fresh_reconfirm_passes():
    """First selector attempt returns transient zero quotes; retry succeeds
    within the recovery budget; fresh trigger reconfirmation (30 s ago)
    passes when the old original breach was 90 s ago (which would fail with
    the old 120 s gate applied to the original breach time)."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(90)),          # 90 s ago — old gate would BLOCK at 120 s
        last_confirmed_trigger_at=_iso(_ago(30)),   # fresh: 30 s ago — within 120 s → PASS
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.passed, f"Expected PASS but got: {result.detail}"
    assert result.audit["effective_anchor"] == "last_confirmed_trigger_at"


def test_A_recovery_uses_fresh_anchor_not_original_breach():
    """The audit must record that last_confirmed_trigger_at was the effective anchor."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(150)),
        last_confirmed_trigger_at=_iso(_ago(10)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.passed
    assert result.audit["effective_anchor"] == "last_confirmed_trigger_at"
    # Original breach age is recorded for diagnostics even when not the anchor
    assert "original_breach_age_seconds" in result.audit


# ═══════════════════════════════════════════════════════════════════════
# Test B: succeeds after old 120-s boundary but before recovery deadline
# ═══════════════════════════════════════════════════════════════════════


def test_B_succeeds_after_old_boundary_with_fresh_reconfirm():
    """Same path as A: original breach was 130 s ago (past old 120 s limit),
    fresh confirmation was 45 s ago (within limit) → broker POST occurs."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(130)),
        last_confirmed_trigger_at=_iso(_ago(45)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.passed, result.detail


# ═══════════════════════════════════════════════════════════════════════
# Test C: price reverses during recovery → zero broker POST
# ═══════════════════════════════════════════════════════════════════════


def test_C_without_fresh_reconfirm_stale_original_blocks():
    """When no fresh reconfirmation is provided and the original breach
    was 130 s ago, the gate fails — price may have reversed."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(130)),
        last_confirmed_trigger_at=None,     # no fresh confirmation
        execution_mode="live",
        max_age_seconds=120,
    )
    assert not result.passed
    assert result.reason_code == GateOutcome.STALE_TRIGGER_BREACH


def test_C_fresh_confirm_older_than_limit_also_blocks():
    """A fresh confirmation that is itself stale (> max_age) does not help."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(200)),
        last_confirmed_trigger_at=_iso(_ago(130)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert not result.passed


# ═══════════════════════════════════════════════════════════════════════
# Test D: last_confirmed_trigger_at in the future → use original breach
# ═══════════════════════════════════════════════════════════════════════


def test_D_future_last_confirmed_falls_back_to_original_breach():
    """A last_confirmed_trigger_at in the future is a clock-skew signal.
    The gate falls back to trigger_crossed_at as anchor; if that is also
    stale the gate blocks."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(130)),
        last_confirmed_trigger_at=_iso(_from_now(5)),   # 5 s in future → invalid
        execution_mode="live",
        max_age_seconds=120,
    )
    # Falls back to original breach (130 s) → blocked
    assert not result.passed
    assert result.audit["effective_anchor"] == "trigger_crossed_at"


def test_D_future_last_confirmed_but_original_within_limit_passes():
    """If original breach is within limit but last_confirmed is in the
    future, gate still passes using original breach as fallback anchor."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(50)),              # 50 s → within 120 s
        last_confirmed_trigger_at=_iso(_from_now(10)),  # invalid
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.passed
    assert result.audit["effective_anchor"] == "trigger_crossed_at"


# ═══════════════════════════════════════════════════════════════════════
# Test E: last_confirmed earlier than original breach → regression guard
# ═══════════════════════════════════════════════════════════════════════


def test_E_last_confirmed_earlier_than_breach_uses_breach():
    """A last_confirmed_trigger_at that is BEFORE trigger_crossed_at is
    logically impossible and is ignored (regression guard).  The gate
    uses trigger_crossed_at instead."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(50)),              # 50 s ago
        last_confirmed_trigger_at=_iso(_ago(100)),      # 100 s ago — BEFORE original
        execution_mode="live",
        max_age_seconds=120,
    )
    # Should use trigger_crossed_at (50 s) → PASS
    assert result.passed
    assert result.audit["effective_anchor"] == "trigger_crossed_at"


# ═══════════════════════════════════════════════════════════════════════
# Test F: missing trigger_crossed_at → fail closed
# ═══════════════════════════════════════════════════════════════════════


def test_F_missing_trigger_crossed_at_fails_closed_live():
    result = check_trigger_age_gate(
        trigger_crossed_at=None,
        last_confirmed_trigger_at=_iso(_ago(10)),
        execution_mode="live",
        max_age_seconds=120,
    )
    # last_confirmed_trigger_at cannot substitute for missing original —
    # without the original we cannot compute the anchor correctly.
    # LIVE must fail closed.
    assert not result.passed


def test_F_missing_both_fails_closed_live():
    result = check_trigger_age_gate(
        trigger_crossed_at=None,
        last_confirmed_trigger_at=None,
        execution_mode="live",
    )
    assert not result.passed
    assert result.reason_code == GateOutcome.STALE_TRIGGER_BREACH


def test_F_missing_both_passes_paper():
    """Paper mode passes with a warning when both timestamps are absent."""
    result = check_trigger_age_gate(
        trigger_crossed_at=None,
        last_confirmed_trigger_at=None,
        execution_mode="paper",
    )
    assert result.passed


# ═══════════════════════════════════════════════════════════════════════
# Test G: backward compatibility — gate without last_confirmed
# ═══════════════════════════════════════════════════════════════════════


def test_G_backward_compat_no_last_confirmed_within_limit():
    """Existing callers that don't supply last_confirmed_trigger_at work unchanged."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(30)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.passed
    assert result.audit["effective_anchor"] == "trigger_crossed_at"


def test_G_backward_compat_no_last_confirmed_over_limit():
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(130)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert not result.passed


# ═══════════════════════════════════════════════════════════════════════
# Audit field completeness
# ═══════════════════════════════════════════════════════════════════════


def test_audit_includes_all_timing_fields():
    """The gate audit must expose all Seam 2 timing fields for diagnostics."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(90)),
        last_confirmed_trigger_at=_iso(_ago(20)),
        execution_mode="live",
        max_age_seconds=120,
    )
    audit = result.audit
    assert "trigger_crossed_at" in audit
    assert "last_confirmed_trigger_at" in audit
    assert "effective_anchor" in audit
    assert "age_seconds" in audit
    assert "original_breach_age_seconds" in audit


def test_audit_records_original_breach_age_when_fresh_anchor_used():
    """When last_confirmed is the effective anchor, original_breach_age_seconds
    must still appear in audit for forensic attribution."""
    result = check_trigger_age_gate(
        trigger_crossed_at=_iso(_ago(200)),
        last_confirmed_trigger_at=_iso(_ago(10)),
        execution_mode="live",
        max_age_seconds=120,
    )
    assert result.audit["effective_anchor"] == "last_confirmed_trigger_at"
    assert "original_breach_age_seconds" in result.audit
    assert result.audit["original_breach_age_seconds"] > 120
