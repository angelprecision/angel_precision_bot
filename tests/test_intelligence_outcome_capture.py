"""
tests/test_intelligence_outcome_capture.py

Intelligence outcome-capture pipeline
(docs/pr_specs/intelligence_outcome_capture_20260903.md).

Proves the three capture points populate the edge dataset and — critically —
that capture is pure side-effect: an exception in capture NEVER propagates into
the trading path, and capture NEVER writes fabricated/zero values.
"""
from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

from ap.selector_feature_capture import capture_selected_contract_features
from ap.option_path_tracker import OptionPathTracker


# ── shared fakes ──────────────────────────────────────────────────────────────
class _RecordingStore:
    """Captures insert_option_outcome calls as (signal_id, row) pairs."""
    def __init__(self):
        self.calls: list[tuple[str, dict]] = []

    def insert_option_outcome(self, signal_id, outcome):
        self.calls.append((str(signal_id), dict(outcome)))


def _selected(**overrides):
    base = dict(
        contract_symbol="AAPL260918C00250000",
        expiration="2026-09-18",
        strike=250.0,
        option_type="CALL",
        bid=1.10, ask=1.20, mid=1.15,
        spread_pct=0.087,
        delta=0.42,
        open_interest=3400,
        volume=1200,
        dte=5,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


# ── A. feature-at-signal capture ──────────────────────────────────────────────
def test_selector_persists_feature_row_on_win():
    store = _RecordingStore()
    ok = capture_selected_contract_features(store, "sig-1", _selected())
    assert ok is True
    assert len(store.calls) == 1
    sid, row = store.calls[0]
    assert sid == "sig-1"
    assert row["spread_pct_at_signal"] == pytest.approx(0.087)
    assert row["delta_at_signal"] == pytest.approx(0.42)
    assert row["oi_at_signal"] == 3400
    assert row["volume_at_signal"] == 1200
    assert row["mark_at_signal"] == pytest.approx(1.15)
    assert row["contract_symbol"] == "AAPL260918C00250000"


def test_iv_written_only_when_present():
    store = _RecordingStore()
    capture_selected_contract_features(store, "sig-iv", _selected(iv=0.31))
    _, row = store.calls[0]
    assert row["iv_at_signal"] == pytest.approx(0.31)

    store2 = _RecordingStore()
    capture_selected_contract_features(store2, "sig-noiv", _selected())
    _, row2 = store2.calls[0]
    assert "iv_at_signal" not in row2  # never fetched, never guessed


def test_selector_capture_exception_never_breaks_caller():
    """The guarantee: a broken store must not raise out of capture."""
    class _Boom:
        def insert_option_outcome(self, *_a, **_k):
            raise RuntimeError("db down")

    # Must return False, never raise.
    assert capture_selected_contract_features(_Boom(), "sig-x", _selected()) is False


def test_no_write_when_signal_id_missing():
    store = _RecordingStore()
    assert capture_selected_contract_features(store, "", _selected()) is False
    assert capture_selected_contract_features(store, None, _selected()) is False
    assert store.calls == []


def test_no_write_when_contract_identity_missing():
    store = _RecordingStore()
    assert capture_selected_contract_features(store, "sig", _selected(contract_symbol="")) is False
    assert store.calls == []


# ── C. post-entry option-path tracker ─────────────────────────────────────────
def test_option_path_tracker_records_peak_trough_and_thresholds():
    store = _RecordingStore()
    t = OptionPathTracker(signal_store=store)
    t.begin("sig-path", entry_mark=1.00)
    t.observe("sig-path", 1.12)   # +12%
    t.observe("sig-path", 1.34)   # +34% peak
    t.observe("sig-path", 0.95)   # -5% trough
    t.finalize("sig-path", close_mark=1.05)

    assert store.calls, "tracker must upsert outcome rows"
    _, last = store.calls[-1]
    assert last["peak_mark"] == pytest.approx(1.34)
    assert last["trough_mark"] == pytest.approx(0.95)
    assert last["max_option_move_pct"] == pytest.approx(34.0, abs=0.1)
    assert last["hit_10pct"] is True
    assert last["hit_30pct"] is True
    assert last["hit_50pct"] is False
    assert last["hit_100pct"] is False
    assert last["close_mark"] == pytest.approx(1.05)
    assert last["minutes_to_option_peak"] is not None


def test_tracker_writes_nothing_when_marks_unusable():
    store = _RecordingStore()
    t = OptionPathTracker(signal_store=store)
    t.begin("sig-bad", entry_mark=0)      # non-positive entry → no state
    t.observe("sig-bad", 1.20)            # unknown signal → ignored
    t.finalize("sig-bad", close_mark=None)
    assert store.calls == []              # never writes zeros/fabrications


def test_tracker_ignores_none_and_negative_samples():
    store = _RecordingStore()
    t = OptionPathTracker(signal_store=store)
    t.begin("sig-neg", entry_mark=2.00)
    t.observe("sig-neg", None)            # unusable → leave state intact
    t.observe("sig-neg", -1.0)           # unusable
    # no upgrade to peak/trough happened, so no flush from those
    t.finalize("sig-neg", close_mark=2.00)
    # finalize flushes once with entry==peak==trough (a legitimate flat path)
    assert len(store.calls) >= 1
    _, last = store.calls[-1]
    assert last["peak_mark"] == pytest.approx(2.00)
    assert last["max_option_move_pct"] == pytest.approx(0.0)


def test_tracker_never_raises_on_broken_store():
    class _Boom:
        def insert_option_outcome(self, *_a, **_k):
            raise RuntimeError("db down")
    t = OptionPathTracker(signal_store=_Boom())
    # None of these may raise.
    t.begin("s", 1.0)
    t.observe("s", 1.5)
    t.finalize("s", 1.4)


# ── B. scoring carry-through in record_outcome ────────────────────────────────
def test_record_outcome_carries_pattern_and_score_from_signal_meta():
    import ap_feedback_loop as fbmod

    fb = fbmod.APFeedbackLoop(supabase_client=None)  # no sb → local only
    fb._write_to_supabase = MagicMock()               # capture the outcome dict
    fb._update_live_stats = MagicMock()
    fb._notify_discord = MagicMock()

    # Exit-time signal has LOST pattern/score at top level, but carries them
    # in signal_meta (snapshotted at stage time).
    signal = {
        "ticker": "NVDA",
        "signal_meta": {"pattern": "2-1-2", "score": 86.0, "side": "PUT",
                        "timeframe": "1d", "regime": "trend"},
    }
    fb.record_outcome(
        signal=signal,
        entry_option_price=1.00,
        exit_option_price=1.20,
        exit_reason="TARGET_HIT",
        underlying_entry=100.0,
        underlying_exit=98.0,
    )
    assert fb._write_to_supabase.called
    outcome = fb._write_to_supabase.call_args[0][0]
    assert outcome["pattern"] == "2-1-2"
    assert outcome["signal_score"] == pytest.approx(86.0)
    assert outcome["side"] == "PUT"


def test_record_outcome_prefers_top_level_when_present():
    import ap_feedback_loop as fbmod

    fb = fbmod.APFeedbackLoop(supabase_client=None)
    fb._write_to_supabase = MagicMock()
    fb._update_live_stats = MagicMock()
    fb._notify_discord = MagicMock()

    signal = {
        "ticker": "TSLA", "pattern": "3-2U", "score": 91.0, "side": "CALL",
        "signal_meta": {"pattern": "SHOULD_NOT_WIN", "score": 1.0},
    }
    fb.record_outcome(
        signal=signal,
        entry_option_price=1.0, exit_option_price=0.9,
        exit_reason="STOP", underlying_entry=100.0, underlying_exit=101.0,
    )
    outcome = fb._write_to_supabase.call_args[0][0]
    assert outcome["pattern"] == "3-2U"          # top-level wins
    assert outcome["signal_score"] == pytest.approx(91.0)
