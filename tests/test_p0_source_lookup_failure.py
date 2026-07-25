"""
tests/test_p0_source_lookup_failure.py
========================================
PR #388 amendment — a source-lookup failure MUST NOT become a silent
"completed no work" success.

Rules asserted:
  * Both sources FAILED + zero rows → RETRYABLE_SOURCE_LOOKUP_FAILED
    (completed=False, retryable=True, source_lookup_failed=True)
  * One source FAILED + rows present → run classified retryable even when
    all rows processed (partial inventory)
  * Only both SUCCESS + zero rows may become COMPLETED_NO_WORK
  * The runner must not set success_date and must not run post-overnight
    handoff on a failed/partial lookup
"""
from __future__ import annotations

import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ap_overnight_reeval as ov


def _empty_fetch_result(*, tq_ok: bool, sup_ok: bool, rows=None):
    return ov._FetchWatchingSignalsResult(
        rows=list(rows or []),
        trade_queue_status=ov._SOURCE_STATUS_SUCCESS if tq_ok else ov._SOURCE_STATUS_FAILED,
        ap_signals_status =ov._SOURCE_STATUS_SUCCESS if sup_ok else ov._SOURCE_STATUS_FAILED,
        trade_queue_error =None if tq_ok else "postgres_down",
        ap_signals_error  =None if sup_ok else "missing_supabase_credentials",
    )


def _run_reeval(monkeypatch, fetch_result):
    """Minimal harness: stub out _fetch_watching_signals_with_status and drive
    run_overnight_reeval with force=True to skip window/day gating."""
    monkeypatch.setattr(
        ov, "_fetch_watching_signals_with_status",
        lambda _client: fetch_result,
    )
    broker = SimpleNamespace(
        submit_order=MagicMock(), place_order=MagicMock(),
        cancel_order=MagicMock(), replace_order=MagicMock(),
    )
    return ov.run_overnight_reeval(
        client_id="jose@example.com",
        broker=broker,
        data_broker=broker,
        master_control=SimpleNamespace(evaluate=lambda *_a, **_kw: None),
        contract_selector=MagicMock(),
        order_state_machine=SimpleNamespace(),
        entry_watcher=SimpleNamespace(),
        force=True,
    )


def test_both_sources_failed_zero_rows_classifies_retryable_source_lookup_failed(monkeypatch):
    fetch = _empty_fetch_result(tq_ok=False, sup_ok=False, rows=[])
    result = _run_reeval(monkeypatch, fetch)
    assert result["result_class"] == "RETRYABLE_SOURCE_LOOKUP_FAILED"
    assert result["completed"] is False
    assert result["retryable"] is True
    assert result["retry_reason"] == "source_lookup_failed"
    assert result.get("source_lookup_failed") is True
    assert result["trade_queue_status"] == "FAILED"
    assert result["ap_signals_status"]  == "FAILED"


def test_missing_supabase_credentials_is_treated_as_failed_not_zero_rows(monkeypatch):
    """Missing credentials was previously a warning that returned an empty
    list. It must now be a FAILED status so a bad deploy cannot silently
    certify the day as complete."""
    fetch = ov._FetchWatchingSignalsResult(
        rows=[],
        trade_queue_status=ov._SOURCE_STATUS_FAILED,  # postgres also down
        ap_signals_status =ov._SOURCE_STATUS_FAILED,
        trade_queue_error ="postgres_down",
        ap_signals_error  ="missing_supabase_credentials",
    )
    result = _run_reeval(monkeypatch, fetch)
    assert result["result_class"] == "RETRYABLE_SOURCE_LOOKUP_FAILED"
    assert result["completed"] is False


def test_both_sources_success_zero_rows_may_complete_no_work(monkeypatch):
    """Only when both sources succeeded and reported zero rows may the run
    be marked COMPLETED_NO_WORK."""
    fetch = _empty_fetch_result(tq_ok=True, sup_ok=True, rows=[])
    result = _run_reeval(monkeypatch, fetch)
    assert result["result_class"] == "COMPLETED_NO_WORK"
    assert result["completed"] is True
    assert result["retryable"] is False


def test_partial_source_inventory_forces_retryable_even_when_rows_processed(monkeypatch):
    """One source FAILED but the other returned rows. Even if the loop
    processes every visible row (e.g. one duplicate that terminal-rejects),
    the run must not be marked completed because inventory is incomplete."""
    dup_row = {
        "id":         "sup:dup",
        "signal_id":  "dup",
        "payload":    {"signal_id": "dup", "ticker": "DUP", "side": "invalid"},
        "created_ts": None,
        "_source":    "ap_signals",
    }
    fetch = _empty_fetch_result(tq_ok=False, sup_ok=True, rows=[dup_row])
    result = _run_reeval(monkeypatch, fetch)
    # The one bad-side row terminal-rejects, but the partial-inventory
    # override MUST downgrade completed to False.
    assert result.get("source_lookup_partial") is True
    assert result["completed"] is False
    assert result["retryable"] is True
    assert result["result_class"] == "RETRYABLE_PARTIAL_SOURCE_INVENTORY"


def test_runner_does_not_set_success_date_on_source_lookup_failed(monkeypatch):
    """Wire the failing result through run_overnight_reeval_attempt on a
    ClientRunner (with all other deps stubbed) and prove that the runner
    never marks the day successful and never invokes post-overnight
    handoff."""
    import threading
    import client_runner as cr

    runner = object.__new__(cr.ClientRunner)
    runner.email = "jose@example.com"
    runner.mode = "PAPER"
    runner.core = SimpleNamespace(broker=object(), entry_watcher=object())
    runner.data_broker = object()
    runner.master_control = object()
    runner.contract_selector = object()
    runner.order_state_machine = object()
    runner.position_manager = object()
    runner._overnight_reeval_attempt_lock = threading.Lock()
    runner._overnight_reeval_state_date = None
    runner._overnight_reeval_success_date = None
    runner._overnight_reeval_exhausted_date = None
    runner._overnight_reeval_last_attempt_at = None
    runner._overnight_reeval_next_retry_at = None
    runner._overnight_reeval_attempt_count = 0
    runner._overnight_reeval_last_result_class = None
    runner._overnight_reeval_last_retry_reason = None
    runner._persist_overnight_reeval_lock = lambda *_a, **_kw: None
    post_calls = []
    runner._run_post_overnight_morning_handoff = lambda result: post_calls.append(dict(result)) or {
        "handoff_result": None, "readiness_result": None,
    }

    # Stub ov.run_overnight_reeval to return the SOURCE_LOOKUP_FAILED shape.
    monkeypatch.setattr(
        ov, "run_overnight_reeval",
        lambda **kwargs: {
            "processed": 0, "armed": 0, "rejected": 0,
            "terminal_rejected": 0, "skipped": 0, "errors": 0,
            "terminal_errors": 0, "retryable_deferred": 0,
            "already_resolved": 0, "unresolved": 0,
            "stale_skipped": 0, "fresh_processed": 0, "fresh_armed": 0,
            "fetched": 0, "stalled": False,
            "result_class": "RETRYABLE_SOURCE_LOOKUP_FAILED",
            "completed": False, "retryable": True,
            "retry_reason": "source_lookup_failed",
            "source_lookup_failed": True,
            "trade_queue_status": "FAILED", "ap_signals_status": "FAILED",
            "trade_queue_error": "postgres_down",
            "ap_signals_error":  "missing_supabase_credentials",
        },
    )
    from datetime import datetime
    from zoneinfo import ZoneInfo
    # force=True: this test verifies runner behavior on a SOURCE_LOOKUP_FAILED
    # engine result. The window/weekday gate (which produces SKIPPED_NOT_DUE)
    # is unrelated to that verification — and passing a fixed weekday-in-
    # window datetime is not portable across CI Python versions where
    # tz-aware comparison edge cases have surfaced. Force-run bypasses the
    # gate cleanly and exercises the actual code path under test.
    result = runner.run_overnight_reeval_attempt(
        force=True,
        now_et=datetime(2026, 7, 23, 9, 31, tzinfo=ZoneInfo("America/New_York")),
        source="scheduler",
    )
    assert result["result_class"] == "RETRYABLE_SOURCE_LOOKUP_FAILED"
    assert runner._overnight_reeval_success_date is None, (
        "runner must not certify success on source lookup failure"
    )
    assert post_calls == [], (
        "runner must not run post-overnight handoff on source lookup failure"
    )
