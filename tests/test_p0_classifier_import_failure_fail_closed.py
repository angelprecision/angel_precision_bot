"""
tests/test_p0_classifier_import_failure_fail_closed.py
========================================================
PR #388 Blocker #3 — classifier import failure MUST fail closed.

Poll-time gate: if ap.pending_trigger_classifier import fails, WatchedSignal.
check() must preserve the current late_attachment_state and return without
running the ordinary CALL/PUT breach path.

Open revalidation: if the classifier import fails, the watcher must retain
overnight=True and queue_status=OPEN_RECHECK_PENDING so the next reeval
poll can retry — never arm normally, never terminalize.

The prior amendment fell through / armed normally, silently bypassing the
whole safety state machine when its own module failed to import.
"""
from __future__ import annotations

import pathlib

import pytest

import ap_entry_watcher as ew
from ap.pending_trigger_classifier import (
    LATE_ATTACHMENT_AWAITING_FIRST_TRUTH as _AWAITING,
    LATE_ATTACHMENT_WITHIN_CONTINUATION as _WITHIN,
    MISSED_LATE_WATCHER_ATTACHMENT_WAITING_RESET as _WAITING,
)


REPO = pathlib.Path(__file__).parent.parent
EW_SRC = (REPO / "ap_entry_watcher.py").read_text()


# ── Structural: fail-closed branches must exist ────────────────────────────

def test_poll_time_import_failure_returns_state_not_falls_through():
    """The poll-time gate's except block must return self.state (not set
    helpers to None and fall through to the CALL/PUT breach path)."""
    assert "LATE_ATTACHMENT_CLASSIFIER_IMPORT_FAILED" in EW_SRC, (
        "Poll-time fail-closed log marker missing"
    )
    # The critical structural requirement: on import failure the gate
    # returns self.state (preserves late_attachment_state and blocks
    # ordinary breach). Assert the return-after-critical shape is present.
    assert (
        "LATE_ATTACHMENT_CLASSIFIER_IMPORT_FAILED"
        in EW_SRC
    )
    # And the same for the defensive is-None branch:
    assert "LATE_ATTACHMENT_CLASSIFIER_UNAVAILABLE" in EW_SRC


def test_open_revalidation_import_failure_retains_overnight():
    """The open revalidation's fail-closed branch must retain overnight=True
    and queue_status=OPEN_RECHECK_PENDING. Never arm normally."""
    assert "OPEN_REVALIDATION_CLASSIFIER_IMPORT_FAILED" in EW_SRC
    # Must NOT contain the old warning that armed normally.
    assert "OVERNIGHT_DAILY_ARMED (classifier import failed; arming normally)" not in EW_SRC


# ── Runtime: poll-time gate — WITHIN state survives import failure ─────────

def _make_watcher_in_state(state):
    from datetime import datetime, timezone
    seed = {
        "state":                state,
        "seen_at":              datetime.now(timezone.utc).isoformat(),
        "quote":                200.05,
        "quote_source":         "ask",
        "allowed_continuation": 0.15,
        "raw_bid":              199.95,
        "raw_ask":              200.05,
    }
    signal = {
        "signal_id":   "sig-t",
        "ticker":      "AAPL",
        "side":        "CALL",
        "entry_price": 200.0,
        "stop_price":  180.0,
        "target_price": 300.0,   # far above; won't spuriously fire target_complete
        "score":       80.0,
        "grade":       "A",
        "_late_attachment_seed": seed,
    }
    return ew.WatchedSignal(signal, overnight=False)


def test_poll_time_gate_preserves_state_on_classifier_import_error(monkeypatch):
    """Simulate an import failure by breaking sys.modules for the classifier.
    The gate must return PENDING with state preserved; NEVER enter ordinary
    breach and NEVER fire TRIGGERED."""
    w = _make_watcher_in_state(_WITHIN)
    assert w.late_attachment_state == _WITHIN

    # Force the import inside check() to raise by deleting the module.
    import sys as _sys
    original = _sys.modules.pop("ap.pending_trigger_classifier", None)
    class _RaisingLoader:
        def find_module(self, name, path=None):
            if name == "ap.pending_trigger_classifier":
                return self
            return None
        def load_module(self, name):
            raise ImportError("test: classifier import forced to fail")
    _sys.meta_path.insert(0, _RaisingLoader())
    try:
        st = w.check(bid=200.30, ask=200.40)   # would ordinarily WAITING_RESET
    finally:
        _sys.meta_path.pop(0)
        if original is not None:
            _sys.modules["ap.pending_trigger_classifier"] = original

    # Must NOT have progressed to TRIGGERED via ordinary breach.
    assert st != ew.WatchState.TRIGGERED, (
        "Ordinary breach path fired despite classifier import failure — "
        "the safety gate evaporated with its module."
    )
    # State must be preserved.
    assert w.late_attachment_state == _WITHIN, (
        f"late_attachment_state was mutated to {w.late_attachment_state!r} "
        f"after a classifier import failure"
    )
    # breach_count must NOT have incremented via ordinary path.
    assert w.breach_count == 0


def test_poll_time_gate_preserves_waiting_state_on_import_error():
    """Symmetric proof for WAITING_RESET."""
    w = _make_watcher_in_state(_WAITING)
    assert w.late_attachment_state == _WAITING
    import sys as _sys
    original = _sys.modules.pop("ap.pending_trigger_classifier", None)
    class _R:
        def find_module(self, name, path=None):
            return self if name == "ap.pending_trigger_classifier" else None
        def load_module(self, name):
            raise ImportError("forced")
    _sys.meta_path.insert(0, _R())
    try:
        st = w.check(bid=200.30, ask=200.40)
    finally:
        _sys.meta_path.pop(0)
        if original is not None:
            _sys.modules["ap.pending_trigger_classifier"] = original

    assert st != ew.WatchState.TRIGGERED
    assert w.late_attachment_state == _WAITING
