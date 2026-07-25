"""
tests/test_p0_hydration_identity_and_target_complete.py
=========================================================
PR #388 P0-5 + P0-7:
  * _hydrate_plan_from_signal carries signal_id, canonical_signal_id,
    client_id, execution_mode, stop_underlying, target_underlying — not
    just the trigger/side/score. And the overnight reeval normalizes
    those fields on every MC-returned plan regardless of source.
  * target_complete is wired from the canonical underlying side at all
    three classifier call sites (arm-time, poll-time, open-revalidation).
"""
from __future__ import annotations

import pathlib

import ap_overnight_reeval as ov


REPO = pathlib.Path(__file__).parent.parent
OV_SRC = (REPO / "ap_overnight_reeval.py").read_text()
EW_SRC = (REPO / "ap_entry_watcher.py").read_text()


# ── P0-5 hydration identity ─────────────────────────────────────────────────

def test_hydrate_plan_from_signal_carries_signal_id_and_canonical():
    plan = ov._hydrate_plan_from_signal(
        {
            "signal_id":         "sig-abc",
            "canonical_signal_id": "canon-abc",
            "ticker":            "AAPL",
            "side":              "CALL",
            "entry_trigger":     101.0,
            "stop_price":        99.0,
            "target_price":      110.0,
        },
        client_id="jose@example.com",
        execution_mode="PAPER",
    )
    assert plan.signal_id == "sig-abc"
    assert plan.canonical_signal_id == "canon-abc"
    assert plan.client_id == "jose@example.com"
    assert plan.execution_mode == "paper"
    assert plan.stop_underlying == 99.0
    assert plan.target_underlying == 110.0
    # Metadata carries them too.
    assert plan.metadata["signal_id"] == "sig-abc"
    assert plan.metadata["canonical_signal_id"] == "canon-abc"
    assert plan.metadata["client_id"] == "jose@example.com"
    assert plan.metadata["execution_mode"] == "paper"


def test_hydrate_plan_from_signal_derives_canonical_when_missing():
    """When the payload lacks canonical_signal_id, the hydrator must derive
    it via _resolve_canonical_signal_id — not leave it blank."""
    plan = ov._hydrate_plan_from_signal(
        {
            "signal_id":     "sig-xyz",
            "ticker":        "SPY",
            "side":          "PUT",
            "entry_trigger": 500.0,
            "stop_price":    502.0,
            "target_price":  490.0,
        },
        client_id="jason@example.com",
        execution_mode="live",
    )
    assert plan.canonical_signal_id  # non-empty
    assert plan.stop_underlying == 502.0
    assert plan.target_underlying == 490.0


def test_hydrate_plan_from_signal_missing_fields_do_not_raise():
    """A partial signal payload must not crash the hydrator; missing
    stop/target simply become None (never a random identity)."""
    plan = ov._hydrate_plan_from_signal(
        {"signal_id": "s1", "ticker": "X", "side": "CALL",
         "entry_trigger": 10.0},
        client_id="jose@example.com",
        execution_mode="paper",
    )
    assert plan.signal_id == "s1"
    assert plan.stop_underlying is None
    assert plan.target_underlying is None


def test_overnight_reeval_normalizes_plan_identity_after_master_control():
    """Structural: run_overnight_reeval must set signal_id, canonical_signal_id,
    client_id, execution_mode, and copy stop/target onto the plan after
    master_control returns, regardless of whether the plan came from MC or
    _hydrate_plan_from_signal."""
    # Verify the normalization block exists inline in run_overnight_reeval.
    assert "P0-5 plan normalization" in OV_SRC
    assert 'setattr(_plan, "signal_id"' in OV_SRC
    assert 'setattr(_plan, "canonical_signal_id"' in OV_SRC
    assert 'setattr(_plan, "client_id"' in OV_SRC
    assert 'setattr(_plan, "execution_mode"' in OV_SRC
    # Stop/target fallback from signal.
    assert '"stop_price"' in OV_SRC and '"target_price"' in OV_SRC


# ── P0-7 target_complete wired at all three sites ──────────────────────────

def test_target_complete_wired_at_arm_time_gate():
    """The arm-time gate must compute target_complete from CALL ask / PUT bid
    (not pass False)."""
    # The hardcoded False was replaced by a computed local _arm_tgt_complete.
    assert "target_complete=_arm_tgt_complete" in EW_SRC
    assert "_arm_tgt_complete = True" in EW_SRC


def test_target_complete_wired_at_poll_time_gate():
    """The poll-time gate (WatchedSignal.check) must compute target_complete
    from the canonical side, not pass False."""
    assert "target_complete=_tgt_complete" in EW_SRC
    assert "_tgt_complete = True" in EW_SRC


def test_target_complete_wired_at_open_revalidation():
    """P0-6 wiring: open revalidation computes target_complete from the
    canonical side and passes it into classify_late_attachment."""
    assert "target_complete=_bug_d_target_complete" in EW_SRC
    assert "_bug_d_target_complete = True" in EW_SRC


def test_no_hardcoded_target_complete_false_remains_in_watcher_source():
    """Regression: the two production call sites that previously passed
    target_complete=False must be updated. The classifier module tests
    (test_p0_late_attachment_continuation.py) may still pass False via
    their _decide helper — that's fine; it's a test wrapper, not
    production. This test scans only ap_entry_watcher.py."""
    assert "target_complete=False" not in EW_SRC


# ── Runtime proof: target-complete in each mode short-circuits terminal ────

def test_classifier_call_target_complete_at_ask_geq_target_is_terminal():
    from ap.pending_trigger_classifier import (
        classify_late_attachment,
        TARGET_ALREADY_COMPLETE_TERMINAL,
    )
    d = classify_late_attachment(
        side="CALL", trigger_price=100.0, bid=110.05, ask=110.10,
        target_complete=True,   # caller computed it (ask >= target)
    )
    assert d.classification == TARGET_ALREADY_COMPLETE_TERMINAL


def test_classifier_put_target_complete_at_bid_leq_target_is_terminal():
    from ap.pending_trigger_classifier import (
        classify_late_attachment,
        TARGET_ALREADY_COMPLETE_TERMINAL,
    )
    d = classify_late_attachment(
        side="PUT", trigger_price=100.0, bid=89.90, ask=89.95,
        target_complete=True,   # caller computed it (bid <= target)
    )
    assert d.classification == TARGET_ALREADY_COMPLETE_TERMINAL
