from types import SimpleNamespace

from ap import queue
from ap.failed_dir_admission import (
    failed_dir_block_result,
    is_failed_dir_blocked,
    payload_pattern_id,
)


class _MasterControl:
    def __init__(self, mode="PAPER"):
        self.mode = mode

    def evaluate(self, *args, **kwargs):  # pragma: no cover - must not be reached by blocked cases
        raise AssertionError("master_control.evaluate must not run for FAILED_DIR blocks")


def _call_dispatch(monkeypatch, payload, *, mode="PAPER", job_last_error=None, job_result=None):
    marked = []
    logged = []

    def fake_mark_job(job_id, status, *, result=None, error=None):
        marked.append({
            "job_id": job_id,
            "status": status,
            "result": result,
            "error": error,
        })

    def fake_log_rejection_to_db(**kwargs):
        logged.append(kwargs)

    monkeypatch.setattr(queue._orig, "_mark_job", fake_mark_job)
    monkeypatch.setattr(queue._orig, "_log_rejection_to_db", fake_log_rejection_to_db)

    queue._dispatch(
        123,
        "jose.vasquez4011@gmail.com",
        "sig-failed-dir-001",
        payload,
        job_last_error=job_last_error,
        job_result=job_result,
        master_control=_MasterControl(mode=mode),
        contract_selector=SimpleNamespace(),
        order_state_machine=SimpleNamespace(),
        entry_watcher=SimpleNamespace(),
        position_manager=SimpleNamespace(),
        exit_eng=SimpleNamespace(),
        broker=SimpleNamespace(),
    )
    return marked, logged


def test_failed_dir_2d_15min_rejects_before_master_control(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)

    marked, logged = _call_dispatch(monkeypatch, {
        "ticker": "GS",
        "pattern_id": "FAILED_DIR_2D_15min",
        "source_scanner": "failed_dir_intraday_1tf",
        "backtest_match_source": "FALLBACK_DEFAULT",
        "score": 65,
    })

    assert marked[0]["status"] == "REJECTED"
    assert marked[0]["error"] == "blocked_pattern:FAILED_DIR_DISABLED"
    assert marked[0]["result"]["reason_code"] == "BLOCKED_PATTERN_FAILED_DIR_DISABLED"
    assert logged[0]["reason_code"] == "BLOCKED_PATTERN_FAILED_DIR_DISABLED"


def test_failed_dir_multitimeframe_rejected_by_prefix(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)

    marked, _ = _call_dispatch(monkeypatch, {
        "ticker": "COIN",
        "pattern_id": "FAILED_DIR_2D_30min+60min",
        "source_scanner": "failed_dir_intraday_2tf",
        "score": 78,
    })

    assert marked[0]["status"] == "REJECTED"
    assert marked[0]["result"]["pattern_id"] == "FAILED_DIR_2D_30min+60min"


def test_failed_dir_future_variant_rejected_by_prefix(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)

    marked, _ = _call_dispatch(monkeypatch, {
        "ticker": "TEST",
        "pattern_id": "FAILED_DIR_CUSTOM_NEW",
    })

    assert marked[0]["status"] == "REJECTED"
    assert marked[0]["result"]["pattern_id"] == "FAILED_DIR_CUSTOM_NEW"


def test_pattern_fallback_fields_are_checked():
    assert payload_pattern_id({"pattern": "FAILED_DIR_2D_60min"}) == "FAILED_DIR_2D_60min"
    assert payload_pattern_id({"strat_pattern": "FAILED_DIR_2U_30min"}) == "FAILED_DIR_2U_30min"
    assert is_failed_dir_blocked({"pattern": "FAILED_DIR_2D_60min"}) is True
    assert is_failed_dir_blocked({"strat_pattern": "FAILED_DIR_2U_30min"}) is True


def test_paper_overnight_only_failed_dir_still_rejects_before_rescue_route(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)

    marked, _ = _call_dispatch(monkeypatch, {
        "ticker": "AMZN",
        "pattern_id": "FAILED_DIR_2D_15min",
        "force_overnight_reeval_only": True,
        "do_not_queue_directly": True,
    }, mode="PAPER")

    assert marked[0]["status"] == "REJECTED"
    assert marked[0]["error"] == "blocked_pattern:FAILED_DIR_DISABLED"
    assert marked[0]["result"]["reason_code"] == "BLOCKED_PATTERN_FAILED_DIR_DISABLED"


def test_paper_manual_rescue_failed_dir_still_rejects(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)

    marked, _ = _call_dispatch(
        monkeypatch,
        {
            "ticker": "BA",
            "pattern_id": "FAILED_DIR_2D_30min",
        },
        mode="PAPER",
        job_last_error="manual_rescue_current_session",
        job_result={"manual_rescue": True},
    )

    assert marked[0]["status"] == "REJECTED"
    assert marked[0]["error"] == "blocked_pattern:FAILED_DIR_DISABLED"


def test_result_json_preserves_client_id_and_execution_mode():
    payload = {
        "ticker": "GS",
        "side": "CALL",
        "pattern_id": "FAILED_DIR_2D_15min",
        "source_scanner": "failed_dir_intraday_1tf",
        "backtest_match_source": "FALLBACK_DEFAULT",
        "score": 65,
    }

    result = failed_dir_block_result(
        payload,
        client_id="jose.vasquez4011@gmail.com",
        execution_mode="PAPER",
        signal_id="sig-1",
        ticker="GS",
    )

    assert result["client_id"] == "jose.vasquez4011@gmail.com"
    assert result["execution_mode"] == "PAPER"
    assert result["reason_code"] == "BLOCKED_PATTERN_FAILED_DIR_DISABLED"
    assert result["backtest_match_source"] == "FALLBACK_DEFAULT"


def test_failed_dir_enabled_is_only_pass_through_path(monkeypatch):
    monkeypatch.setenv("FAILED_DIR_ENABLED", "1")
    called = []

    def fake_original_dispatch(*args, **kwargs):
        called.append((args, kwargs))

    monkeypatch.setattr(queue, "_ORIGINAL_DISPATCH", fake_original_dispatch)

    queue._dispatch(
        123,
        "jose.vasquez4011@gmail.com",
        "sig-failed-dir-enabled",
        {"ticker": "GS", "pattern_id": "FAILED_DIR_2D_15min"},
        master_control=_MasterControl(mode="PAPER"),
        contract_selector=SimpleNamespace(),
        order_state_machine=SimpleNamespace(),
        entry_watcher=SimpleNamespace(),
    )

    assert len(called) == 1


def test_non_failed_dir_passes_through(monkeypatch):
    monkeypatch.delenv("FAILED_DIR_ENABLED", raising=False)
    called = []

    def fake_original_dispatch(*args, **kwargs):
        called.append((args, kwargs))

    monkeypatch.setattr(queue, "_ORIGINAL_DISPATCH", fake_original_dispatch)

    queue._dispatch(
        123,
        "jose.vasquez4011@gmail.com",
        "sig-normal",
        {"ticker": "AAPL", "pattern_id": "1-2_2U"},
        master_control=_MasterControl(mode="PAPER"),
        contract_selector=SimpleNamespace(),
        order_state_machine=SimpleNamespace(),
        entry_watcher=SimpleNamespace(),
    )

    assert len(called) == 1
