"""
tests/test_p0_entry_confirmation.py
P0 follow-up: Entry Confirmation Preflight unit tests.
"""
import os
import pytest

os.environ.update({
    "ENTRY_CONFIRM_SECONDS":                  "45",
    "MAX_PRE_ENTRY_OPTION_FADE_PCT":          "8",
    "MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT":  "0.25",
    "CLIENT_PROOF_MAX_SPREAD_PCT":            "0.10",
    "CLIENT_PROOF_QUOTE_MAX_AGE_SECONDS":     "10",
})

from ap_entry_confirmation import check_entry_confirmation, ConfirmationResult


def _plan_with_gate(confirmation_required=True):
    """Minimal plan-like object with hybrid gate metadata."""
    class P:
        def __init__(self):
            self.side = "CALL"
            self.trigger_price = 180.0
            self.limit_price = 3.50
            self.contracts = 1
            self.tier = "A"
            self.metadata = {
                "hybrid_client_quality_gate": {
                    "confirmation_required": confirmation_required,
                    "confirmation_seconds": 45,
                }
            }
    return P()


def _call(**kw):
    defaults = dict(
        plan=_plan_with_gate(),
        direction="CALL",
        trigger_price=180.0,
        live_bid=3.40,
        live_ask=3.60,
        live_quote_age_ms=2000,       # 2 seconds old
        underlying_last=180.5,        # above trigger — good
        decision_option_price=3.50,   # no fade
        score=78.0,
        tier="A",
        timeframe="1d",
        sandbox_mode=False,
    )
    defaults.update(kw)
    return check_entry_confirmation(**defaults)


def _put(**kw):
    p = _plan_with_gate()
    p.side = "PUT"
    defaults = dict(
        plan=p,
        direction="PUT",
        trigger_price=180.0,
        live_bid=3.40,
        live_ask=3.60,
        live_quote_age_ms=2000,
        underlying_last=179.5,        # below trigger — good for PUT
        decision_option_price=3.50,
        score=78.0,
        tier="A",
        timeframe="1d",
        sandbox_mode=False,
    )
    defaults.update(kw)
    return check_entry_confirmation(**defaults)


# ── Gate disabled (no confirmation_required) ──────────────────────────────────
def test_no_confirmation_required_passes():
    r = _call(plan=_plan_with_gate(confirmation_required=False))
    assert r.passed is True
    assert r.metadata.get("confirmation_required") is False


def test_missing_gate_metadata_passes():
    class P:
        metadata = {}
        side = "CALL"
        trigger_price = 180.0
        limit_price = 3.50
        contracts = 1
        tier = "A"
    r = check_entry_confirmation(
        plan=P(), direction="CALL", trigger_price=180.0,
        live_bid=3.40, live_ask=3.60, live_quote_age_ms=1000,
        underlying_last=180.5, decision_option_price=3.50,
    )
    assert r.passed is True


# ── Clean pass ────────────────────────────────────────────────────────────────
def test_clean_call_passes():
    r = _call()
    assert r.passed is True
    assert r.fail_reason is None

def test_clean_put_passes():
    r = _put()
    assert r.passed is True


# ── Quote age ─────────────────────────────────────────────────────────────────
def test_stale_quote_blocks():
    r = _call(live_quote_age_ms=15_000)  # 15s > 10s max
    assert r.passed is False
    assert r.fail_reason == "entry_confirm_failed_stale_quote"

def test_no_quote_blocks():
    r = _call(live_bid=None, live_ask=None)
    assert r.passed is False
    assert r.fail_reason == "entry_confirm_failed_stale_quote"

def test_fresh_quote_passes():
    r = _call(live_quote_age_ms=3000)  # 3s — well within 10s
    assert r.passed is True


# ── Spread ────────────────────────────────────────────────────────────────────
def test_wide_spread_blocks():
    # mid = 2.5, spread = 1.0 → 40% > 10%
    r = _call(live_bid=2.0, live_ask=3.0)
    assert r.passed is False
    assert r.fail_reason == "entry_confirm_failed_spread"

def test_tight_spread_passes():
    r = _call(live_bid=3.45, live_ask=3.55)
    assert r.passed is True


# ── Option fade ───────────────────────────────────────────────────────────────
def test_option_fade_blocks():
    # decision = 3.50, live_mid = 3.00 → 14.3% fade > 8% max
    r = _call(live_bid=2.95, live_ask=3.05, decision_option_price=3.50)
    assert r.passed is False
    assert r.fail_reason == "entry_confirm_failed_option_fade"

def test_acceptable_fade_passes():
    # 2% fade — within 8% threshold
    r = _call(live_bid=3.40, live_ask=3.50, decision_option_price=3.50)
    assert r.passed is True

def test_no_decision_price_skips_fade():
    r = _call(decision_option_price=None)
    assert r.passed is True


# ── Underlying reversal — CALL ────────────────────────────────────────────────
def test_call_reversal_blocks():
    # underlying pulled back 1% below trigger — > 0.25% threshold
    r = _call(underlying_last=178.2, trigger_price=180.0)
    assert r.passed is False
    assert r.fail_reason == "entry_confirm_failed_underlying_reversal"

def test_call_small_pullback_passes():
    # 0.1% below trigger — within 0.25% threshold
    r = _call(underlying_last=179.82, trigger_price=180.0)
    assert r.passed is True

def test_call_above_trigger_passes():
    r = _call(underlying_last=181.0, trigger_price=180.0)
    assert r.passed is True


# ── Underlying reversal — PUT ─────────────────────────────────────────────────
def test_put_reclaim_blocks():
    # underlying reclaimed 1% above trigger — > 0.25% threshold
    r = _put(underlying_last=181.8, trigger_price=180.0)
    assert r.passed is False
    assert r.fail_reason == "entry_confirm_failed_underlying_reversal"

def test_put_below_trigger_passes():
    r = _put(underlying_last=179.0, trigger_price=180.0)
    assert r.passed is True


# ── Metadata completeness ─────────────────────────────────────────────────────
def test_passed_metadata_fields():
    r = _call()
    m = r.to_meta(started_at="2026-01-01T00:00:00Z", completed_at="2026-01-01T00:00:01Z")
    required = [
        "confirmation_required", "confirmation_seconds",
        "confirmation_started_at", "confirmation_completed_at",
        "confirmation_passed", "confirmation_fail_reason",
        "underlying_start", "underlying_end",
        "option_mid_start", "option_mid_end",
        "quote_age_seconds", "spread_pct",
        "live_entry_bid", "live_entry_ask", "live_entry_mid", "live_entry_ts",
    ]
    for field in required:
        assert field in m, f"Missing metadata field: {field}"
    assert m["confirmation_passed"] is True

def test_failed_metadata_has_reason():
    r = _call(live_quote_age_ms=20_000)
    m = r.to_meta("t1", "t2")
    assert m["confirmation_passed"] is False
    assert m["confirmation_fail_reason"] == "entry_confirm_failed_stale_quote"


# ── Tier-based confirmation seconds ──────────────────────────────────────────
def test_aplus_tier_uses_fast_confirm():
    from ap_entry_confirmation import _tier_confirm_seconds
    s = _tier_confirm_seconds(score=80, tier="A+", timeframe="1d")
    assert s <= 15.0, f"A+ daily should get fast confirm (≤15s), got {s}"

def test_btier_daily_uses_normal_confirm():
    from ap_entry_confirmation import _tier_confirm_seconds
    s = _tier_confirm_seconds(score=72, tier="B", timeframe="1d")
    assert s <= 45.0

def test_paper_quote_lag_warning():
    # Sandbox mode + 5% fade → warning set
    r = _call(sandbox_mode=True, live_bid=3.30, live_ask=3.40,
              decision_option_price=3.50)
    # 3% fade < 8% max → passes but warns
    assert r.passed is True
    assert r.metadata.get("paper_quote_lag_warning") is True
