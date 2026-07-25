"""
tests/test_p0_decisive_drift_wired.py
=======================================
PR #388 Blocker #4 — decisive_drift_exceeded MUST be computed and passed
to classify_late_attachment at all three production call sites (arm-time
gate, poll-time gate, open revalidation). Prior amendment hardcoded False
at every site, making LATE_ATTACHMENT_MOVE_MISSED_TERMINAL unreachable
from real production data — a far-through daily open could become
WAITING_RESET and later rebreach, giving a completed move another entry.

Drift threshold source: the existing authoritative MAX_INTRADAY_DRIFT_PCT
(default 1.5%). Not a new formula.

Rules:
  CALL decisive drift exceeded: ask > trigger * (1 + MAX_INTRADAY_DRIFT_PCT)
  PUT  decisive drift exceeded: bid < trigger * (1 - MAX_INTRADAY_DRIFT_PCT)
"""
from __future__ import annotations

import pathlib

import pytest

import ap_entry_watcher as ew


REPO = pathlib.Path(__file__).parent.parent
EW_SRC = (REPO / "ap_entry_watcher.py").read_text()


# ── Structural: every call site must pass decisive_drift_exceeded ─────────

def test_no_hardcoded_decisive_drift_false_in_watcher_source():
    """The three production call sites previously passed
    decisive_drift_exceeded=False. All three must be updated."""
    assert "decisive_drift_exceeded=False" not in EW_SRC, (
        "A production call site is still hardcoding decisive_drift_exceeded=False. "
        "It must be computed from MAX_INTRADAY_DRIFT_PCT and passed truthfully."
    )


def test_all_three_call_sites_pass_computed_decisive_drift():
    """Poll-time (_decisive_drift), arm-time (_arm_decisive_drift), open
    revalidation (_open_decisive_drift) — every site computes and passes it."""
    for var in ("_decisive_drift", "_arm_decisive_drift", "_open_decisive_drift"):
        assert f"decisive_drift_exceeded={var}" in EW_SRC, (
            f"expected decisive_drift_exceeded={var} to be passed to "
            f"classify_late_attachment"
        )
        # And each site must actually assign the local via the threshold.
        assert f"{var} = False" in EW_SRC


def test_all_three_call_sites_reference_max_intraday_drift_pct():
    """The threshold used must be the existing authoritative constant,
    not a new tolerance introduced in this PR."""
    # There should be at least three references (one per call site).
    # Two references exist historically (stale-move check + startup log);
    # the amendment adds three more.
    assert EW_SRC.count("MAX_INTRADAY_DRIFT_PCT") >= 5, (
        f"Expected at least 5 references to MAX_INTRADAY_DRIFT_PCT; got "
        f"{EW_SRC.count('MAX_INTRADAY_DRIFT_PCT')}. The three new call "
        f"sites must all consult the existing threshold."
    )


# ── Classifier semantics: decisive_drift_exceeded → LATE_MOVE_MISSED ──────

def test_classifier_terminal_when_call_ask_far_above_trigger():
    """CALL at trigger=100, ask=105 (5% above, well beyond continuation and
    decisive-drift threshold) with decisive_drift_exceeded=True must
    return LATE_ATTACHMENT_MOVE_MISSED_TERMINAL — NOT WAITING_RESET."""
    from ap.pending_trigger_classifier import (
        classify_late_attachment,
        LATE_ATTACHMENT_MOVE_MISSED_TERMINAL,
    )
    d = classify_late_attachment(
        side="CALL", trigger_price=100.0, bid=104.90, ask=105.00,
        stop=90.0, target_complete=False, decisive_drift_exceeded=True,
    )
    assert d.classification == LATE_ATTACHMENT_MOVE_MISSED_TERMINAL


def test_classifier_terminal_when_put_bid_far_below_trigger():
    from ap.pending_trigger_classifier import (
        classify_late_attachment,
        LATE_ATTACHMENT_MOVE_MISSED_TERMINAL,
    )
    d = classify_late_attachment(
        side="PUT", trigger_price=100.0, bid=95.00, ask=95.10,
        stop=110.0, target_complete=False, decisive_drift_exceeded=True,
    )
    assert d.classification == LATE_ATTACHMENT_MOVE_MISSED_TERMINAL


# ── Runtime: poll-time gate terminalizes on decisive drift ──────────────

def _make_watcher_in_state(state):
    from datetime import datetime, timezone
    from ap.pending_trigger_classifier import (
        LATE_ATTACHMENT_WITHIN_CONTINUATION as _WITHIN,
        MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET as _WAITING,
        LATE_ATTACHMENT_AWAITING_FIRST_TRUTH as _AWAITING,
    )
    signal = {
        "signal_id":   "sig-drift",
        "ticker":      "AAPL",
        "side":        "CALL",
        "entry_price": 200.0,
        "stop_price":  180.0,
        "target_price": 400.0,   # far away; won't spuriously fire
        "score":       80.0,
        "grade":       "A",
        "_late_attachment_seed": {
            "state":                state,
            "seen_at":              datetime.now(timezone.utc).isoformat(),
            "quote":                200.05,
            "quote_source":         "ask",
            "allowed_continuation": 0.15,
            "raw_bid":              199.95,
            "raw_ask":              200.05,
        },
    }
    return ew.WatchedSignal(signal, overnight=False)


def test_poll_time_decisive_drift_terminalizes_from_waiting_reset():
    """WAITING_RESET watcher whose next poll shows ask far beyond the
    decisive drift threshold must transition to INVALIDATED via
    LATE_ATTACHMENT_MOVE_MISSED_TERMINAL — never rebreach later."""
    from ap.pending_trigger_classifier import (
        MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET as _WAITING,
    )
    w = _make_watcher_in_state(_WAITING)
    # ask = 220 → 10% above trigger=200; far beyond MAX_INTRADAY_DRIFT_PCT=1.5%.
    st = w.check(bid=219.90, ask=220.00)
    assert st == ew.WatchState.INVALIDATED, (
        f"Expected INVALIDATED via decisive drift; got {st}"
    )


def test_poll_time_within_threshold_stays_in_waiting_reset():
    """A price just outside the continuation window (0.15) but still WITHIN
    the decisive drift threshold (1.5% * 200 = 3.00) must remain
    WAITING_RESET — not terminalize."""
    from ap.pending_trigger_classifier import (
        MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET as _WAITING,
    )
    w = _make_watcher_in_state(_WAITING)
    # ask = 201.00 → 0.5% above trigger — past continuation (0.15) but
    # inside decisive drift (3.00). Stay WAITING_RESET.
    st = w.check(bid=200.95, ask=201.00)
    assert st == ew.WatchState.PENDING
    assert w.late_attachment_state == _WAITING
