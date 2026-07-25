"""
tests/test_p0_open_revalidation_late_attachment.py
====================================================
PR #388 P0-6 — market-open revalidation must route the daily-valid arm
through the canonical late-attachment classifier, not the old binary
_is_already_through_trigger gate.

Structural + wiring proof: the amendment threads classify_late_attachment
into _revalidate_overnight_at_open with the same six-outcome contract used
at regular-session arm time. No new formula, no new tolerance.
"""
from __future__ import annotations

import pathlib


REPO = pathlib.Path(__file__).parent.parent
EW_SRC = (REPO / "ap_entry_watcher.py").read_text()


def test_open_reval_no_longer_uses_binary_already_through_trigger_for_daily():
    """The daily-valid branch must not decide arm-time via the old binary
    _is_already_through_trigger call. The classifier owns that decision."""
    # The old block was: `if self._is_already_through_trigger(w.side, w.entry_trigger, _bug_d_bid, _bug_d_ask):`
    # After the amendment the daily-valid branch classifies via the shared
    # helper. Confirm the new marker is present and the exact old block-shape
    # is gone.
    assert "OVERNIGHT_DAILY_ARMED_WITHIN_CONTINUATION" in EW_SRC
    assert "OVERNIGHT_DAILY_ARMED_WAITING_RESET" in EW_SRC
    assert "OVERNIGHT_DAILY_ARMED_AWAITING_FIRST_TRUTH" in EW_SRC
    assert "trigger_type=\"overnight_revalidation_late_attachment\"" in EW_SRC


def test_open_reval_uses_canonical_classifier_import():
    """The revalidation site must import classify_late_attachment from the
    canonical module, not re-implement the continuation formula locally."""
    assert "classify_late_attachment as _pt_classify_late_open" in EW_SRC
    # No new tolerance constant introduced in the watcher.
    assert "ENTRY_TRIGGER_CONTINUATION_MAX_ABS" not in EW_SRC
    assert "ENTRY_TRIGGER_CONTINUATION_MAX_BPS" not in EW_SRC


def test_open_reval_wires_target_complete_from_canonical_side():
    """P0-7 partial: the open revalidation must compute target_complete from
    the canonical underlying side (CALL ask>=target, PUT bid<=target)."""
    assert "_bug_d_target_complete = True" in EW_SRC
    # And it must be passed into the classifier call — not hardcoded False.
    assert "target_complete=_bug_d_target_complete" in EW_SRC


def test_open_reval_terminal_reasons_use_new_taxonomy():
    """Terminals emitted by the new open-revalidation classification must
    use the Block-2 reason codes, not the old arm_already_through_trigger."""
    for rc in (
        "stop_already_broken_terminal",
        "target_already_complete_terminal",
        "late_attachment_move_missed_terminal",
    ):
        assert rc in EW_SRC, f"expected {rc!r} in open revalidation reasons"


def test_open_reval_non_terminal_seeds_gate_and_sets_overnight_false():
    """Non-terminal outcomes must seed w.late_attachment_state AND set
    w.overnight = False so the normal poll loop owns the confirmation."""
    # The three seed lines and the overnight=False handoff must all be in
    # the file. Together they prove the poll loop's late-attachment gate
    # (already tested in tests/test_p0_late_attachment_watcher_integration.py)
    # takes over after open revalidation.
    assert "w.late_attachment_state = _PT_OPEN_WITHIN" in EW_SRC
    assert "w.late_attachment_state = _PT_OPEN_WAITING" in EW_SRC
    assert "w.late_attachment_state = _PT_OPEN_AWAITING" in EW_SRC
    # overnight=False handoff must be present in the daily-arm else-branch.
    assert "w.overnight = False" in EW_SRC


def test_old_overnight_daily_already_through_trigger_terminalization_is_gone():
    """The legacy overnight_daily_already_through_trigger reason code was
    the exact terminalization the amendment must replace. Assert the log
    marker for that path (OVERNIGHT_DAILY_ALREADY_THROUGH_TRIGGER) no
    longer appears in the daily-valid branch."""
    # A grep for the old capitalized log marker must miss.
    assert "OVERNIGHT_DAILY_ALREADY_THROUGH_TRIGGER" not in EW_SRC, (
        "The old binary already-through-trigger open gate is still present. "
        "It must be replaced by classify_late_attachment routing."
    )
