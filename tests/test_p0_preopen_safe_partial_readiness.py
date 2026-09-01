from __future__ import annotations

import inspect
import sys
import types
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

import ap.preopen_readiness as readiness
import ap.preopen_safe_partial_guard as guard
import ap_overnight_reeval as overnight
import client_runner


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
        "already_resolved": 68,
        "unresolved": 0,
        "fresh_armed": 54,
        "result_class": "RETRY_EXHAUSTED",
        "retry_reason": "retryable_rows_remain",
        "attempt_count": 7,
        "fresh_processed": 200,
        "terminal_errors": 0,
        "terminal_rejected": 73,
        "retryable_deferred": 5,
        "retryable_rows": [
            {
                "job_id": f"job-{i}",
                "signal_id": f"signal-{i}",
                "source": "trade_queue",
            }
            for i in range(5)
        ],
        "source_lookup_partial": False,
        "trade_queue_status": "SUCCESS",
        "ap_signals_status": "SUCCESS",
        "attempted_at": "2026-08-31T16:00:00+00:00",
        "trade_queue_error": None,
        "ap_signals_error": None,
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
            {
                "local_order_id": f"order-{i}",
                "signal_id": f"signal-{i}",
                "overnight_source_table": "trade_queue",
                "overnight_source_job_id": f"job-{i}",
                "overnight_source_signal_id": f"signal-{i}",
            }
            for i in range(pending_count)
        ],
        "watching_count": 0,
    }


def _live_watcher(*, running=True, alive=True, has_order=True):
    watcher = SimpleNamespace(
        _running=running,
        _thread=SimpleNamespace(is_alive=lambda: alive),
    )
    watcher.has_order = lambda _local_order_id: has_order
    return watcher


def _live_runner(*, watcher_running=True, watcher_alive=True):
    return SimpleNamespace(
        mode="LIVE",
        _overnight_reeval_success_date=None,
        _overnight_reeval_attempt_count=7,
        _overnight_reeval_last_attempt_at=datetime(2026, 8, 31, 16, 0, tzinfo=timezone.utc),
        initialized=SimpleNamespace(is_set=lambda: True),
        worker_thread=SimpleNamespace(is_alive=lambda: True),
        is_alive=lambda: True,
        order_state_machine=object(),
        core=SimpleNamespace(
            entry_watcher=_live_watcher(
                running=watcher_running,
                alive=watcher_alive,
            )
        ),
        master_control=SimpleNamespace(mode="LIVE"),
    )


def test_guard_is_installed_on_package_import():
    assert getattr(readiness._overnight_status, "_ap_safe_partial_guard", False) is True


def test_jason_exact_retry_exhausted_shape_is_safe_when_every_pending_order_is_owned(monkeypatch):
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row())
    base = SimpleNamespace(
        _pending_trigger_without_watcher=lambda runner, rows: []
    )

    safe, proof = guard.classify_safe_exhausted_partial(
        base,
        runner=_live_runner(),
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
        runner=_live_runner(),
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
        ({"already_resolved": 0}, "overnight_outcome_accounting_mismatch"),
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
        runner=_live_runner(),
        client_state=_client_state(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == reason


@pytest.mark.parametrize(
    "status_overrides,reason",
    [
        (
            {
                "source_lookup_partial": True,
                "trade_queue_status": "FAILED",
                "ap_signals_status": "SUCCESS",
            },
            "overnight_source_lookup_partial",
        ),
        (
            {
                "source_lookup_partial": True,
                "trade_queue_status": "SUCCESS",
                "ap_signals_status": "FAILED",
            },
            "overnight_source_lookup_partial",
        ),
    ],
)
def test_incomplete_source_inventory_never_becomes_safe_partial(
    monkeypatch, status_overrides, reason
):
    monkeypatch.setattr(
        guard,
        "_latest_exact_overnight_row",
        lambda **_: _jason_row(**status_overrides),
    )
    base = SimpleNamespace(
        _pending_trigger_without_watcher=lambda runner, rows: []
    )

    safe, proof = guard.classify_safe_exhausted_partial(
        base,
        runner=_live_runner(),
        client_state=_client_state(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == reason


def test_failed_source_status_is_blocked_even_if_partial_flag_is_false(monkeypatch):
    row = _jason_row(
        source_lookup_partial=False,
        trade_queue_status="FAILED",
        ap_signals_status="SUCCESS",
    )
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: row)

    safe, proof = guard.classify_safe_exhausted_partial(
        SimpleNamespace(_pending_trigger_without_watcher=lambda runner, rows: []),
        runner=_live_runner(),
        client_state=_client_state(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == "overnight_trade_queue_source_not_success"


def test_safe_partial_proof_must_match_current_runner_attempt(monkeypatch):
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row())
    runner = _live_runner()
    runner._overnight_reeval_last_attempt_at = datetime(
        2026, 8, 31, 16, 1, tzinfo=timezone.utc
    )

    safe, proof = guard.classify_safe_exhausted_partial(
        SimpleNamespace(_pending_trigger_without_watcher=lambda runner, rows: []),
        runner=runner,
        client_state=_client_state(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == "overnight_lock_not_current_attempt"


def test_missing_source_completeness_fields_fail_closed(monkeypatch):
    row = _jason_row()
    for key in (
        "source_lookup_partial",
        "trade_queue_status",
        "ap_signals_status",
    ):
        row["details"].pop(key)
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: row)

    safe, proof = guard.classify_safe_exhausted_partial(
        SimpleNamespace(_pending_trigger_without_watcher=lambda runner, rows: []),
        runner=_live_runner(),
        client_state=_client_state(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == "overnight_source_lookup_partial"


def test_retryable_count_must_be_durably_represented_by_pending_rows(monkeypatch):
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row(retryable_deferred=5))
    base = SimpleNamespace(
        _pending_trigger_without_watcher=lambda runner, rows: []
    )

    safe, proof = guard.classify_safe_exhausted_partial(
        base,
        runner=_live_runner(),
        client_state=_client_state(pending_count=4),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == "retryable_rows_not_durably_represented"


def test_unrelated_owned_pending_order_does_not_satisfy_retryable_row_identity(monkeypatch):
    row = _jason_row(
        already_resolved=72,
        retryable_deferred=1,
        retryable_rows=[
            {
                "job_id": "job-retry",
                "signal_id": "retry-signal",
                "source": "trade_queue",
            }
        ],
    )
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: row)
    base = SimpleNamespace(
        _pending_trigger_without_watcher=lambda runner, rows: []
    )

    safe, proof = guard.classify_safe_exhausted_partial(
        base,
        runner=_live_runner(),
        client_state=_client_state(pending_count=1),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == "retryable_rows_not_exactly_represented"
    assert proof["signal_id"] == "retry-signal"


def test_same_signal_with_wrong_job_or_source_does_not_satisfy_identity(monkeypatch):
    row = _jason_row(
        already_resolved=72,
        retryable_deferred=1,
        retryable_rows=[
            {
                "job_id": "job-retry",
                "signal_id": "retry-signal",
                "source": "trade_queue",
            }
        ],
    )
    state = _client_state(pending_count=0)
    state["pending_trigger_rows"] = [
        {
            "local_order_id": "order-collision",
            "signal_id": "retry-signal",
            "overnight_source_table": "ap_signals",
            "overnight_source_job_id": "sup:retry-signal",
            "overnight_source_signal_id": "retry-signal",
        }
    ]
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: row)

    safe, proof = guard.classify_safe_exhausted_partial(
        SimpleNamespace(_pending_trigger_without_watcher=lambda runner, rows: []),
        runner=_live_runner(),
        client_state=state,
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == "retryable_rows_not_exactly_represented"
    assert proof["source"] == "trade_queue"
    assert proof["job_id"] == "job-retry"


@pytest.mark.parametrize(
    "runner_kwargs",
    [{"watcher_running": False}, {"watcher_alive": False}],
)
def test_non_live_entry_watcher_never_proves_safe_partial(monkeypatch, runner_kwargs):
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row())
    base = SimpleNamespace(
        _pending_trigger_without_watcher=lambda runner, rows: []
    )

    safe, proof = guard.classify_safe_exhausted_partial(
        base,
        runner=_live_runner(**runner_kwargs),
        client_state=_client_state(),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        trading_date="2026-08-31",
    )

    assert safe is False
    assert proof["reason"] == "entry_watcher_not_live"


def test_retryable_result_records_exact_source_identity():
    result = {"retryable_deferred": 0, "retryable_rows": []}

    overnight._record_retryable_row(
        result,
        job_id="sup:signal-5",
        signal_id="signal-5",
        source="ap_signals",
    )

    assert result["retryable_deferred"] == 1
    assert result["retryable_rows"] == [
        {
            "job_id": "sup:signal-5",
            "signal_id": "signal-5",
            "source": "ap_signals",
        }
    ]


def test_overnight_lock_persists_retryable_row_identities(monkeypatch):
    calls = []
    handoff = types.ModuleType("ap.morning_handoff")
    handoff._upsert_handoff_run_lock = lambda **kwargs: calls.append(kwargs)
    monkeypatch.setitem(sys.modules, "ap.morning_handoff", handoff)

    runner = object.__new__(client_runner.ClientRunner)
    runner.email = "jasoncosby1@gmail.com"
    runner.mode = "LIVE"
    runner._overnight_reeval_attempt_count = 7
    runner._overnight_reeval_last_attempt_at = None
    runner._overnight_reeval_next_retry_at = None

    retryable_rows = [
        {
            "job_id": "sup:signal-5",
            "signal_id": "signal-5",
            "source": "ap_signals",
        }
    ]
    client_runner.ClientRunner._persist_overnight_reeval_lock(
        runner,
        {
            "fetched": 1,
            "processed": 1,
            "armed": 0,
            "rejected": 0,
            "terminal_rejected": 0,
            "skipped": 1,
            "errors": 0,
            "terminal_errors": 0,
            "retryable_deferred": 1,
            "retryable_rows": retryable_rows,
            "already_resolved": 0,
            "source_lookup_partial": False,
            "trade_queue_status": "SUCCESS",
            "ap_signals_status": "SUCCESS",
            "trade_queue_error": None,
            "ap_signals_error": None,
            "unresolved": 0,
            "fresh_processed": 1,
            "fresh_armed": 0,
            "stale_skipped": 0,
            "stalled": True,
            "result_class": "RETRY_EXHAUSTED",
            "completed": False,
            "retryable": False,
            "retry_reason": "retryable_rows_remain",
        },
        today=datetime(2026, 8, 31, tzinfo=timezone.utc).date(),
        source="scheduler",
        now_et=datetime(2026, 8, 31, 16, 0, tzinfo=timezone.utc),
        last_error="OVERNIGHT_REEVAL_RETRY_EXHAUSTED",
    )

    details = calls[0]["details"]
    assert details["retryable_rows"] == retryable_rows
    assert details["already_resolved"] == 0
    assert details["source_lookup_partial"] is False
    assert details["trade_queue_status"] == "SUCCESS"
    assert details["ap_signals_status"] == "SUCCESS"


def test_readiness_query_reads_exact_overnight_source_identity():
    source = inspect.getsource(readiness._query_client_state)
    for field in (
        "overnight_source_table",
        "overnight_source_job_id",
        "overnight_source_signal_id",
    ):
        assert field in source


def test_real_overnight_status_wrapper_accepts_only_safe_partial(monkeypatch):
    monkeypatch.setattr(readiness, "_post_overnight_reeval_success_exists", lambda *a, **k: False)
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: _jason_row())
    monkeypatch.setattr(readiness, "_pending_trigger_without_watcher", lambda runner, rows: [])

    runner = _live_runner()
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

    runner = _live_runner()
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


def _patch_full_live_readiness(monkeypatch, *, state, row):
    monkeypatch.setattr(readiness, "_upsert_preopen_row", lambda **_: None)
    monkeypatch.setattr(readiness, "_query_client_state", lambda _client_id: dict(state))
    monkeypatch.setattr(readiness, "_morning_handoff_success_exists", lambda *args: True)
    monkeypatch.setattr(readiness, "_post_overnight_reeval_success_exists", lambda *args: False)
    monkeypatch.setattr(
        readiness,
        "_broker_credentials_present",
        lambda *args: (True, {"credential_status": "present"}),
    )
    monkeypatch.setattr(
        readiness,
        "_selector_identity",
        lambda _runner: {
            "quote_source": "tradier_live",
            "tradier_base_url": "https://broker.example",
        },
    )
    monkeypatch.setattr(readiness, "_pod_mode", lambda: "live")
    monkeypatch.setattr(readiness, "_after_929_et", lambda *_args: True)
    monkeypatch.setattr(guard, "_latest_exact_overnight_row", lambda **_: row)


def test_full_live_readiness_accepts_only_exact_owned_safe_partial(monkeypatch):
    _patch_full_live_readiness(
        monkeypatch,
        state=_client_state(),
        row=_jason_row(),
    )

    result = readiness.run_preopen_autonomous_readiness(
        "jasoncosby1@gmail.com",
        "LIVE",
        stage="deadline",
        runner=_live_runner(),
        now=datetime(2026, 8, 31, 16, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "OK"
    assert result["ok"] is True
    assert result["errors"] == []
    assert result["details"]["overnight_reeval"]["status"] == "safe_partial"


def test_full_live_readiness_blocks_unrelated_owned_pending_order(monkeypatch):
    row = _jason_row(
        already_resolved=72,
        retryable_deferred=1,
        retryable_rows=[
            {
                "job_id": "job-retry",
                "signal_id": "retry-signal",
                "source": "trade_queue",
            }
        ],
    )
    _patch_full_live_readiness(
        monkeypatch,
        state=_client_state(pending_count=1),
        row=row,
    )

    result = readiness.run_preopen_autonomous_readiness(
        "jasoncosby1@gmail.com",
        "LIVE",
        stage="deadline",
        runner=_live_runner(),
        now=datetime(2026, 8, 31, 16, 0, tzinfo=timezone.utc),
    )

    assert result["status"] == "BLOCKED"
    assert result["ok"] is False
    assert "overnight_reeval_missing" in result["errors"]
