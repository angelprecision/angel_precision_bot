from __future__ import annotations

from types import SimpleNamespace

import pytest

import ap.preopen_readiness as readiness
import ap.preopen_safe_partial_guard as guard


def _jason_row(**detail_overrides):
    details = {
        "armed": 54,
        "errors": 0,
        "fetched": 200,
        "skipped": 73,
        "rejected": 73,
        "completed": False,
        "processed": 200,
        "retryable": False,
        "unresolved": 0,
        "fresh_armed": 54,
        "result_class": "RETRY_EXHAUSTED",
        "retry_reason": "retryable_rows_remain",
        "attempt_count": 7,
        "fresh_processed": 200,
        "terminal_errors": 0,
        "terminal_rejected": 73,
        "retryable_deferred": 5,
    }
    details.update(detail_overrides)
    return {
        "client_id": "jasoncosby1@gmail.com",
        "execution_mode": "live",
        "trading_date": "2026-08-31",
        "stage": "overnight_reeval",
        "status": "partial",
        "last_success_at": None,
        "last_error": "OVERNIGHT_REEVAL_RETRY_EXHAUSTED",
        "details": details,
    }


def _client_state(pending_count=6):
    return {
        "stale_processing_ids": [],
        "watching_orphans": [],
        "pending_trigger_rows": [
            {"local_order_id": f"order-{i}", "signal_id": f"signal-{i}"}
            for i in range(pending_count)
        ],
        "watching_count": 0,
    }


def test_guard_is_installed_on_package_import():
    assert getattr(readiness._overnight_status, "_ap_safe_partial_guard", False) is True


def test_jason_exact_retry_exhausted_shape_is_safe_when_every_pending_order_is_owned(monkeypatch):
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row())
    base = SimpleNamespace(
        _pending_trigger_without_watcher=lambda runner, rows: []
    )

    safe, proof = guard.classify_safe_exhausted_partial(
        base,
        runner=object(),
        client_state=_client_state(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is True
    assert proof["source"] == "handoff_run_locks.overnight_reeval_safe_exhausted_partial"
    assert proof["retryable_deferred"] == 5
    assert proof["processed"] == 200
    assert proof["fetched"] == 200


def test_one_unowned_pending_order_keeps_safe_partial_blocked(monkeypatch):
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row())
    unowned = [{"local_order_id": "order-4", "signal_id": "signal-4"}]
    base = SimpleNamespace(
        _pending_trigger_without_watcher=lambda runner, rows: unowned
    )

    safe, proof = guard.classify_safe_exhausted_partial(
        base,
        runner=object(),
        client_state=_client_state(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == "pending_trigger_without_watcher_ownership"
    assert proof["unowned_count"] == 1


@pytest.mark.parametrize(
    "overrides,reason",
    [
        ({"errors": 1}, "overnight_has_unresolved_or_error_truth"),
        ({"terminal_errors": 1}, "overnight_has_unresolved_or_error_truth"),
        ({"unresolved": 1}, "overnight_has_unresolved_or_error_truth"),
        ({"processed": 199}, "overnight_source_not_fully_processed"),
        ({"fresh_processed": 199}, "overnight_fresh_processing_incomplete"),
        ({"retryable_deferred": 0}, "overnight_retryable_partial_shape_missing"),
        ({"attempt_count": 0}, "overnight_retryable_partial_shape_missing"),
        ({"result_class": "WAITING_FOR_RETRY"}, "overnight_result_class_not_retry_exhausted"),
        ({"retry_reason": "db_error"}, "overnight_retry_reason_not_retryable_rows_remain"),
        ({"completed": True}, "overnight_terminal_flags_invalid"),
        ({"retryable": True}, "overnight_terminal_flags_invalid"),
    ],
)
def test_ambiguous_or_incomplete_partial_never_becomes_safe(monkeypatch, overrides, reason):
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row(**overrides))
    base = SimpleNamespace(
        _pending_trigger_without_watcher=lambda runner, rows: []
    )

    safe, proof = guard.classify_safe_exhausted_partial(
        base,
        runner=object(),
        client_state=_client_state(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == reason


def test_retryable_count_must_be_durably_represented_by_pending_rows(monkeypatch):
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row(retryable_deferred=5))
    base = SimpleNamespace(
        _pending_trigger_without_watcher=lambda runner, rows: []
    )

    safe, proof = guard.classify_safe_exhausted_partial(
        base,
        runner=object(),
        client_state=_client_state(pending_count=4),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == "retryable_rows_not_durably_represented"


def test_real_overnight_status_wrapper_accepts_only_safe_partial(monkeypatch):
    monkeypatch.setattr(readiness, "_post_overnight_reeval_success_exists", lambda *a, **k: False)
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row())
    monkeypatch.setattr(readiness, "_pending_trigger_without_watcher", lambda runner, rows: [])

    runner = SimpleNamespace(_overnight_reeval_success_date=None)
    state, proof = readiness._overnight_status(
        runner,
        _client_state(),
        "2026-08-31",
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        stage="manual",
    )

    assert state == "safe_partial"
    assert proof["retryable_deferred"] == 5


def test_real_overnight_status_wrapper_preserves_missing_when_owner_is_lost(monkeypatch):
    monkeypatch.setattr(readiness, "_post_overnight_reeval_success_exists", lambda *a, **k: False)
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row())
    monkeypatch.setattr(
        readiness,
        "_pending_trigger_without_watcher",
        lambda runner, rows: [rows[0]],
    )

    runner = SimpleNamespace(_overnight_reeval_success_date=None)
    state, proof = readiness._overnight_status(
        runner,
        _client_state(),
        "2026-08-31",
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        stage="manual",
    )

    assert state == "missing"
    assert proof["source"] == "watching_or_pending_trigger_present_without_overnight_success"
