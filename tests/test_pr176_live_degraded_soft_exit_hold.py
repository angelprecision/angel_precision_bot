"""
tests/test_pr176_live_degraded_soft_exit_hold.py

PR #176 — P0: Live degraded soft exits must HOLD, not sell.

These tests prove the spec'd behavior at the only call-site that matters: the
SUBMIT path in ap_exit_engine.py. The merge-rule (no broker exit submitted in
degraded live cases) is verified by proving _pr176_should_hold returns
should_hold=True, which the call-site uses to `continue` past the SUBMIT event.

The 7 spec tests:
  1. NKE-style live PUT, underlying_entry=0, proposed THESIS_FAIL_SOFT_STOP
     -> DATA_DEGRADED_HOLD, no broker submit.
  2. RIVN-style live PUT, no_underlying_data, proposed NEVER_GREEN_STOP
     -> DATA_DEGRADED_HOLD, no broker submit.
  3. exact reason_code=SOFT_LOSS -> blocked when degraded.
  4. execution_mode blank/unknown + client_id=jasoncosby1@gmail.com
     -> treated as live-risk and blocked.
  5. manual close still proceeds.
  6. EOD flatten still proceeds.
  7. paper behavior unchanged.
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timedelta, timezone

import pytest

# Stub DATABASE_URL so ap.db imports don't fail in CI
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")

# Import the engine module
import ap_exit_engine as eng  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_pos(
    *,
    ticker: str = "NKE",
    side: str = "PUT",
    execution_mode: str = "live",
    client_id: str = "jasoncosby1@gmail.com",
    underlying_entry: float = 88.50,
    current_underlying: float = 86.00,
    current_option_price: float = 1.20,
    current_bid: float = 1.10,
    current_ask: float = 1.30,
    opened_at_minutes_ago: float = 20.0,
    position_id: str = "pos-test-001",
) -> eng.ManagedPosition:
    """Factory for a ManagedPosition test fixture."""
    return eng.ManagedPosition(
        ticker=ticker,
        option_symbol=f"{ticker}250620P00085000",
        side=side,
        quantity=1,
        quantity_remaining=1,
        entry_price=1.50,
        underlying_entry=underlying_entry,
        underlying_target=0.0,
        underlying_stop=90.0 if side == "PUT" else 88.0,
        position_id=position_id,
        client_id=client_id,
        signal_id="sig-test-001",
        execution_mode=execution_mode,
        current_option_price=current_option_price,
        current_bid=current_bid,
        current_ask=current_ask,
        current_underlying=current_underlying,
        opened_at=datetime.now(timezone.utc) - timedelta(minutes=opened_at_minutes_ago),
    )


# ─────────────────────────────────────────────────────────────────────────────
# 1. NKE-style live PUT, underlying_entry=0, THESIS_FAIL_SOFT_STOP -> blocked
# ─────────────────────────────────────────────────────────────────────────────

def test_1_nke_style_thesis_fail_with_zero_underlying_entry_is_held():
    pos = _make_pos(
        ticker="NKE",
        side="PUT",
        execution_mode="live",
        underlying_entry=0.0,            # ← the bug condition
        current_underlying=86.00,
        opened_at_minutes_ago=20.0,
    )
    should_hold, hold_code, extra = eng._pr176_should_hold(
        pos,
        decision_reason_code="THESIS_FAIL_SOFT_STOP",
        decision_reason_text="THESIS_FAIL_SOFT_STOP — 14% loss and underlying not confirming (DATA_DEGRADED_HOLD)",
    )
    assert should_hold is True, "live degraded soft exit MUST be held — broker submit must not run"
    assert hold_code == "DATA_DEGRADED_HOLD"
    assert "underlying_entry_missing_or_zero" in extra["pr176_degraded_signals"]


# ─────────────────────────────────────────────────────────────────────────────
# 2. RIVN-style live PUT, no_underlying_data text, NEVER_GREEN_STOP -> blocked
# ─────────────────────────────────────────────────────────────────────────────

def test_2_rivn_style_never_green_with_no_underlying_data_is_held():
    pos = _make_pos(
        ticker="RIVN",
        side="PUT",
        execution_mode="live",
        underlying_entry=10.50,          # plausibly present
        current_underlying=0.0,          # but current is missing
        opened_at_minutes_ago=30.0,      # well past any min-hold
    )
    should_hold, hold_code, extra = eng._pr176_should_hold(
        pos,
        decision_reason_code="NEVER_GREEN_STOP",
        decision_reason_text="NEVER GREEN STOP [0DTE-eq] — 30% at 30min | threshold=-25% | "
                             "thesis never confirmed | underlying:no_underlying_data",
    )
    assert should_hold is True
    assert hold_code == "DATA_DEGRADED_HOLD"
    signals = extra["pr176_degraded_signals"]
    # Both current-missing and text-confessed-no-data should be present
    assert "current_underlying_missing" in signals
    assert "decision_text_no_underlying_data" in signals


# ─────────────────────────────────────────────────────────────────────────────
# 3. SOFT_LOSS reason_code blocked when degraded
# ─────────────────────────────────────────────────────────────────────────────

def test_3_soft_loss_blocked_when_degraded():
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=0.0,
        opened_at_minutes_ago=20.0,
    )
    should_hold, hold_code, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="SOFT_LOSS",
        decision_reason_text="SOFT_LOSS — 14% loss",
    )
    assert should_hold is True
    assert hold_code == "DATA_DEGRADED_HOLD"


def test_3b_soft_loss_blocked_under_live_min_hold_even_when_data_clean():
    """Live SOFT_LOSS cannot fire before 8 minutes even with good data."""
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=88.5,
        current_underlying=86.0,
        opened_at_minutes_ago=3.0,   # 3 min < 8 min floor
    )
    should_hold, hold_code, extra = eng._pr176_should_hold(
        pos,
        decision_reason_code="SOFT_LOSS",
        decision_reason_text="SOFT_LOSS — 14% loss",
    )
    assert should_hold is True
    assert hold_code == "LIVE_MIN_HOLD_NOT_MET"
    assert extra["pr176_floor_min"] == 8.0


def test_3c_never_green_blocked_under_live_min_hold_even_when_data_clean():
    """Live NEVER_GREEN_STOP cannot fire before 12 minutes even with good data."""
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=88.5,
        current_underlying=86.0,
        opened_at_minutes_ago=10.0,   # 10 min < 12 min floor
    )
    should_hold, hold_code, extra = eng._pr176_should_hold(
        pos,
        decision_reason_code="NEVER_GREEN_STOP",
        decision_reason_text="NEVER GREEN STOP — 30% at 10min",
    )
    assert should_hold is True
    assert hold_code == "LIVE_MIN_HOLD_NOT_MET"
    assert extra["pr176_floor_min"] == 12.0


# ─────────────────────────────────────────────────────────────────────────────
# 4. blank/unknown execution_mode + Jason's client_id → treated as live-risk
# ─────────────────────────────────────────────────────────────────────────────

def test_4_blank_execution_mode_jason_client_id_is_live_risk():
    # blank
    pos = _make_pos(
        execution_mode="",
        client_id="jasoncosby1@gmail.com",
        underlying_entry=0.0,
        opened_at_minutes_ago=20.0,
    )
    should_hold, hold_code, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="THESIS_FAIL_SOFT_STOP",
        decision_reason_text="THESIS_FAIL_SOFT_STOP — degraded",
    )
    assert should_hold is True
    assert hold_code == "DATA_DEGRADED_HOLD"


def test_4b_unknown_execution_mode_jason_client_id_is_live_risk():
    pos = _make_pos(
        execution_mode="unknown",
        client_id="jasoncosby1@gmail.com",
        underlying_entry=0.0,
        opened_at_minutes_ago=20.0,
    )
    should_hold, hold_code, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="THESIS_FAIL_SOFT_STOP",
        decision_reason_text="THESIS_FAIL_SOFT_STOP — degraded",
    )
    assert should_hold is True
    assert hold_code == "DATA_DEGRADED_HOLD"


# ─────────────────────────────────────────────────────────────────────────────
# 5. Manual close still proceeds
# ─────────────────────────────────────────────────────────────────────────────

def test_5_manual_close_proceeds_even_when_live_and_degraded():
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=0.0,   # would normally trigger DATA_DEGRADED_HOLD
        opened_at_minutes_ago=1.0,
    )
    should_hold, hold_code, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="MANUAL_CLOSE",
        decision_reason_text="MANUAL_CLOSE — operator requested",
    )
    assert should_hold is False, "manual close must never be gated"
    assert hold_code == ""


def test_5b_manual_text_in_reason_proceeds():
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=0.0,
        opened_at_minutes_ago=1.0,
    )
    should_hold, _, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="SOFT_LOSS",                                 # gated code...
        decision_reason_text="MANUAL operator close requested via UI",    # ...but manual text wins
    )
    assert should_hold is False


# ─────────────────────────────────────────────────────────────────────────────
# 6. EOD flatten still proceeds
# ─────────────────────────────────────────────────────────────────────────────

def test_6_eod_force_close_proceeds_even_when_degraded():
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=0.0,
        opened_at_minutes_ago=1.0,
    )
    should_hold, _, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="EOD_FORCE_CLOSE",
        decision_reason_text="EOD FORCE CLOSE -- 15:50 ET past 15:50",
    )
    assert should_hold is False


def test_6b_eod_text_in_reason_proceeds():
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=0.0,
        opened_at_minutes_ago=1.0,
    )
    should_hold, _, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="SOFT_LOSS",
        decision_reason_text="EOD pre-close protection",
    )
    assert should_hold is False


def test_6c_hard_disaster_and_emergency_proceed():
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=0.0,
        opened_at_minutes_ago=1.0,
    )
    # HARD STOP text
    should_hold, _, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="HARD_STOP",
        decision_reason_text="HARD STOP -- 45% exceeded -40% max loss",
    )
    assert should_hold is False
    # EMERGENCY_FLATTEN
    should_hold, _, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="EMERGENCY_FLATTEN",
        decision_reason_text="EMERGENCY_FLATTEN operator initiated",
    )
    assert should_hold is False


# ─────────────────────────────────────────────────────────────────────────────
# 7. Paper behavior unchanged
# ─────────────────────────────────────────────────────────────────────────────

def test_7_paper_position_never_gated_by_pr176():
    """Paper positions: guard is a complete no-op regardless of degradation."""
    pos = _make_pos(
        execution_mode="paper",
        client_id="jose.vasquez4011@gmail.com",
        underlying_entry=0.0,       # would gate live
        current_underlying=0.0,     # would gate live
        opened_at_minutes_ago=1.0,  # would gate live
    )
    for rc in [
        "SOFT_LOSS", "THESIS_FAIL_SOFT_STOP", "NEVER_GREEN_STOP",
        "SOFT_LOSS_WATCH", "DEEP_LOSS_STOP", "STOP_BREACH_CONFIRMING",
    ]:
        should_hold, _, _ = eng._pr176_should_hold(
            pos,
            decision_reason_code=rc,
            decision_reason_text=f"{rc} — test",
        )
        assert should_hold is False, f"paper must NEVER be gated by PR#176 (rc={rc})"


# ─────────────────────────────────────────────────────────────────────────────
# Merge rule: explicit proof the guard prevents broker submission
# ─────────────────────────────────────────────────────────────────────────────

class _SubmitTrap:
    """Tracks whether the SUBMIT event was reached after the guard."""
    def __init__(self) -> None:
        self.submit_called = False
        self.hold_called = False
        self.last_hold_reason_code = ""

    def emit(self, pos, decision, reason_code, explanation, stage, extra_inputs):
        if decision == "SUBMIT":
            self.submit_called = True
        elif decision == "HOLD":
            self.hold_called = True
            self.last_hold_reason_code = reason_code


def _simulate_submit_call_site(pos, decision_reason_code: str, decision_reason_text: str) -> _SubmitTrap:
    """
    Mirrors the wired call-site logic exactly:
        if guard says hold -> emit HOLD, continue (no SUBMIT).
        otherwise           -> emit SUBMIT.
    """
    trap = _SubmitTrap()

    should_hold, hold_code, extra = eng._pr176_should_hold(
        pos, decision_reason_code, decision_reason_text,
    )
    if should_hold:
        trap.emit(pos, "HOLD", hold_code, "blocked by PR#176", "exit_decision", extra)
        return trap  # `continue` in the real loop → no SUBMIT
    trap.emit(pos, "SUBMIT", decision_reason_code, decision_reason_text, "exit_decision", {})
    return trap


def test_merge_rule_no_broker_submit_on_nke_style_degraded_live():
    pos = _make_pos(execution_mode="live", underlying_entry=0.0, opened_at_minutes_ago=20.0)
    trap = _simulate_submit_call_site(pos, "THESIS_FAIL_SOFT_STOP", "no_underlying_data")
    assert trap.submit_called is False, "MERGE BLOCKER: broker SUBMIT must not fire on degraded live"
    assert trap.hold_called is True
    assert trap.last_hold_reason_code == "DATA_DEGRADED_HOLD"


def test_merge_rule_no_broker_submit_on_rivn_style_degraded_live():
    pos = _make_pos(
        ticker="RIVN", execution_mode="live",
        underlying_entry=10.5, current_underlying=0.0, opened_at_minutes_ago=30.0,
    )
    trap = _simulate_submit_call_site(
        pos, "NEVER_GREEN_STOP",
        "NEVER GREEN STOP — thesis never confirmed | underlying:no_underlying_data",
    )
    assert trap.submit_called is False, "MERGE BLOCKER: broker SUBMIT must not fire on degraded live"
    assert trap.hold_called is True


def test_merge_rule_eod_proceeds_to_submit_even_on_live():
    pos = _make_pos(execution_mode="live", underlying_entry=0.0)
    trap = _simulate_submit_call_site(pos, "EOD_FORCE_CLOSE", "EOD FORCE CLOSE -- 15:50 ET")
    assert trap.submit_called is True, "EOD must reach broker SUBMIT even on live"
    assert trap.hold_called is False


def test_merge_rule_paper_always_reaches_submit():
    pos = _make_pos(execution_mode="paper", underlying_entry=0.0, opened_at_minutes_ago=1.0)
    trap = _simulate_submit_call_site(pos, "SOFT_LOSS", "SOFT_LOSS — degraded")
    assert trap.submit_called is True, "paper SOFT_LOSS reaches SUBMIT unchanged"
    assert trap.hold_called is False


# ─────────────────────────────────────────────────────────────────────────────
# PR #179 amendment tests
# ─────────────────────────────────────────────────────────────────────────────
#
# Two safety fixes applied before merge:
#   1. Broad "RECONCILER" text fragment removed from bypass list.  Generic
#      reconciler paths (e.g. RECONCILER_AUTO_CLOSE) must NOT skip the
#      degraded live soft-exit guard.  Only explicit broker-gone codes
#      (RECONCILER_BROKER_GONE, BROKER_POSITION_GONE) bypass.
#   2. Gated soft-exit text fragments added so production reasons that
#      carry the soft-exit meaning in text (even when reason_code is
#      normalized differently) still trigger the guard.
# ─────────────────────────────────────────────────────────────────────────────

def test_amendment_1_reconciler_auto_close_with_degraded_live_is_held():
    """
    RECONCILER_AUTO_CLOSE no longer bypasses by virtue of the "RECONCILER"
    text fragment.  When wrapped around soft-exit text on a degraded live
    position, the guard must HOLD.
    """
    pos = _make_pos(
        ticker="NKE",
        side="PUT",
        execution_mode="live",
        underlying_entry=0.0,
        opened_at_minutes_ago=20.0,
    )
    should_hold, hold_code, extra = eng._pr176_should_hold(
        pos,
        decision_reason_code="RECONCILER_AUTO_CLOSE",
        decision_reason_text="RECONCILER_AUTO_CLOSE wrapping NEVER GREEN STOP no_underlying_data",
    )
    assert should_hold is True, (
        "RECONCILER_AUTO_CLOSE with degraded live context MUST NOT bypass — "
        "this is the high-confidence-from-degraded-data hole"
    )
    assert hold_code == "DATA_DEGRADED_HOLD"
    # Gated via the text fragment, not the unknown reason_code
    assert extra.get("pr176_gated_by") == "reason_text"


def test_amendment_1b_reconciler_broker_gone_still_bypasses():
    """
    The legitimate broker-gone case must still bypass: it is the position
    is already closed at the broker so there is nothing to submit.
    """
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=0.0,   # would otherwise trigger DATA_DEGRADED_HOLD
        opened_at_minutes_ago=1.0,
    )
    should_hold, hold_code, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="RECONCILER_BROKER_GONE",
        decision_reason_text="reconciler observed broker position is gone — flatten DB row",
    )
    assert should_hold is False, "RECONCILER_BROKER_GONE must still bypass cleanly"
    assert hold_code == ""


def test_amendment_2_soft_loss_with_manual_text_still_bypasses():
    """
    Order-of-checks invariant: manual-text bypass must run BEFORE the new
    gated-text match.  reason_code=SOFT_LOSS (gated) with reason_text
    containing 'MANUAL' must still proceed as a manual close.
    """
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=0.0,
        opened_at_minutes_ago=1.0,
    )
    should_hold, _, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="SOFT_LOSS",
        decision_reason_text="MANUAL operator close requested via UI",
    )
    assert should_hold is False, (
        "manual-text bypass must beat the gated-code check — operator override"
    )


def test_amendment_3_unknown_reason_code_with_gated_text_blocks_for_jason():
    """
    Production reasons may carry the soft-exit meaning in text even if the
    reason_code is unrecognized (normalized by an upstream layer).  When
    the text contains 'NEVER GREEN STOP' AND 'no_underlying_data', the
    guard must HOLD on a Jason live position even though the reason_code
    is not in _PR176_GATED_SOFT_REASON_CODES.
    """
    pos = _make_pos(
        execution_mode="live",
        client_id="jasoncosby1@gmail.com",
        underlying_entry=0.0,
        current_underlying=0.0,
        opened_at_minutes_ago=20.0,
    )
    should_hold, hold_code, extra = eng._pr176_should_hold(
        pos,
        decision_reason_code="POST_NORMALIZED_EXIT",   # unknown to PR #176
        decision_reason_text=(
            "POST_NORMALIZED_EXIT wrapping NEVER GREEN STOP — thesis never "
            "confirmed | underlying:no_underlying_data"
        ),
    )
    assert should_hold is True, (
        "gated text fragment MUST trigger the guard even with an unrecognized "
        "reason_code — this is the upstream-normalization hole"
    )
    assert hold_code == "DATA_DEGRADED_HOLD"
    assert extra.get("pr176_gated_by") == "reason_text"
    # Both degraded signals should be visible
    signals = extra.get("pr176_degraded_signals", [])
    assert "underlying_entry_missing_or_zero" in signals
    assert "decision_text_no_underlying_data" in signals


# ─────────────────────────────────────────────────────────────────────────────
# Amendment merge-rule proofs — broker SUBMIT must not run in these cases
# ─────────────────────────────────────────────────────────────────────────────

def test_amendment_merge_rule_reconciler_auto_close_no_broker_submit():
    pos = _make_pos(
        execution_mode="live",
        underlying_entry=0.0,
        opened_at_minutes_ago=20.0,
    )
    trap = _simulate_submit_call_site(
        pos,
        "RECONCILER_AUTO_CLOSE",
        "RECONCILER_AUTO_CLOSE wrapping NEVER GREEN STOP no_underlying_data",
    )
    assert trap.submit_called is False, (
        "MERGE BLOCKER: RECONCILER_AUTO_CLOSE on degraded live must NOT reach broker"
    )
    assert trap.hold_called is True
    assert trap.last_hold_reason_code == "DATA_DEGRADED_HOLD"


def test_amendment_merge_rule_reconciler_broker_gone_does_submit():
    """RECONCILER_BROKER_GONE must still proceed — DB cleanup path."""
    pos = _make_pos(execution_mode="live", underlying_entry=0.0)
    trap = _simulate_submit_call_site(
        pos,
        "RECONCILER_BROKER_GONE",
        "broker position is gone",
    )
    assert trap.submit_called is True, "RECONCILER_BROKER_GONE must still proceed"
    assert trap.hold_called is False


def test_amendment_merge_rule_unknown_code_gated_text_no_submit():
    pos = _make_pos(
        execution_mode="live",
        client_id="jasoncosby1@gmail.com",
        underlying_entry=0.0,
        opened_at_minutes_ago=20.0,
    )
    trap = _simulate_submit_call_site(
        pos,
        "POST_NORMALIZED_EXIT",
        "POST_NORMALIZED_EXIT wrapping NEVER GREEN STOP no_underlying_data",
    )
    assert trap.submit_called is False, (
        "MERGE BLOCKER: unknown reason_code + gated text on degraded live "
        "must NOT reach broker"
    )
    assert trap.hold_called is True


def test_amendment_paper_with_reconciler_auto_close_still_unchanged():
    """Paper must remain unaffected by the amendment — guard is a no-op."""
    pos = _make_pos(
        execution_mode="paper",
        underlying_entry=0.0,
        opened_at_minutes_ago=20.0,
    )
    should_hold, _, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="RECONCILER_AUTO_CLOSE",
        decision_reason_text="RECONCILER_AUTO_CLOSE NEVER GREEN STOP no_underlying_data",
    )
    assert should_hold is False, "paper must NEVER be gated"


def test_amendment_non_jason_live_with_gated_text_unaffected():
    """Non-Jason live with blank/unknown execution_mode is not on allowlist."""
    pos = _make_pos(
        execution_mode="",
        client_id="some-other-live-client@example.com",  # not on PR176_LIVE_CLIENT_IDS
        underlying_entry=0.0,
        opened_at_minutes_ago=20.0,
    )
    should_hold, _, _ = eng._pr176_should_hold(
        pos,
        decision_reason_code="POST_NORMALIZED_EXIT",
        decision_reason_text="NEVER GREEN STOP no_underlying_data",
    )
    assert should_hold is False, (
        "non-allowlisted client with blank execution_mode must not be gated"
    )
