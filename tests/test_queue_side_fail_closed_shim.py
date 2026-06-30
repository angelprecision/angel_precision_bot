from __future__ import annotations

import json
from types import SimpleNamespace

import ap.queue as queue


class _FakeCursor:
    rowcount = 1

    def __init__(self, captured):
        self.captured = captured

    def execute(self, sql, params=None):
        self.captured["sql"] = sql
        self.captured["params"] = params


class _FakeConn:
    def __init__(self, captured):
        self.cursor = _FakeCursor(captured)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *exc):
        return False


class _FakeTable:
    def __init__(self, captured):
        self.captured = captured

    def upsert(self, row, on_conflict=None):
        self.captured["row"] = row
        self.captured["on_conflict"] = on_conflict
        return self

    def execute(self):
        return None


class _FakeSupabase:
    def __init__(self, captured):
        self.captured = captured

    def table(self, name):
        self.captured["table"] = name
        return _FakeTable(self.captured)


def test_enqueue_does_not_default_missing_side_to_call(monkeypatch):
    captured = {}
    monkeypatch.setattr(queue._base, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue._base, "_run_with_retry", lambda fn, *a, **k: fn())

    inserted = queue.enqueue_signal({"ticker": "SPY", "signal_id": "sig-missing-side"}, client_id="client-a")

    assert inserted is True
    payload = json.loads(captured["params"][2])
    assert "side" not in payload
    assert "direction" not in payload
    assert payload["side_validation_error"].startswith("invalid_or_missing_side")


def test_enqueue_normalizes_explicit_bearish_alias(monkeypatch):
    captured = {}
    monkeypatch.setattr(queue._base, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue._base, "_run_with_retry", lambda fn, *a, **k: fn())

    inserted = queue.enqueue_signal(
        {"ticker": "SPY", "signal_id": "sig-bearish", "direction": "bearish"},
        client_id="client-a",
    )

    assert inserted is True
    payload = json.loads(captured["params"][2])
    assert payload["side"] == "PUT"
    assert payload["direction"] == "PUT"


def test_dispatch_rejects_missing_side_before_master_control(monkeypatch):
    calls = []
    rejection_logs = []
    monkeypatch.setattr(queue, "_ORIG_MARK_JOB", lambda *a, **k: calls.append((a, k)))
    monkeypatch.setattr(queue, "_log_rejection_to_db", lambda **k: rejection_logs.append(k))

    queue._dispatch(
        7,
        "client-a",
        "sig-missing-side",
        {"ticker": "SPY", "score": 90},
        master_control=SimpleNamespace(mode="LIVE"),
        contract_selector=None,
        order_state_machine=object(),
        entry_watcher=object(),
    )

    assert calls[0][0][1] == "REJECTED"
    assert calls[0][1]["error"] == "queue_side_validation:INVALID_OR_MISSING_SIDE"
    assert calls[0][1]["result"]["reason_code"] == "INVALID_OR_MISSING_SIDE"
    assert rejection_logs[0]["payload"]["side_validation_error"].startswith("invalid_or_missing_side")


def test_log_signal_to_db_records_unknown_not_call_for_missing_side(monkeypatch):
    captured = {}
    monkeypatch.setattr(queue._base, "_get_sb_client", lambda: _FakeSupabase(captured))

    ok = queue._log_signal_to_db(
        signal_id="sig-missing-side",
        client_id="client-a",
        ticker="SPY",
        side="",
        score=90,
        stage="queue_side_validation",
        reason_code="INVALID_OR_MISSING_SIDE",
        human_reason="missing side",
        payload={"ticker": "SPY"},
    )

    assert ok is True
    assert captured["table"] == "ap_signals"
    assert captured["row"]["side"] == "UNKNOWN"
    assert captured["row"]["raw_payload"]["side_validation_error"].startswith("invalid_or_missing_side")


def test_selector_failure_metadata_is_merged_into_queue_result(monkeypatch):
    calls = []
    monkeypatch.setattr(queue, "_ORIG_MARK_JOB", lambda *a, **k: calls.append((a, k)))
    queue._selector_failure_by_job[11] = {
        "selector_failure": {
            "queue_reason_code": "NO_CHAIN_DATA",
            "chain_rows": 0,
            "survivor_count": 0,
            "top_reject_buckets": {"no_rows": 1},
        }
    }

    queue._mark_job(11, "REJECTED", result={"stage": "contract_selection", "reason": "NO_CHAIN_DATA"})

    result = calls[0][1]["result"]
    assert result["selector_failure"]["queue_reason_code"] == "NO_CHAIN_DATA"
    assert result["chain_rows"] == 0
    assert result["survivor_count"] == 0
    assert result["top_reject_buckets"] == {"no_rows": 1}


def test_live_immediate_execution_is_queue_fatal(monkeypatch):
    calls = []
    monkeypatch.setattr(queue, "_ORIG_MARK_JOB", lambda *a, **k: calls.append((a, k)))
    monkeypatch.setattr(queue._base, "ALLOW_IMMEDIATE_EXECUTION", True)

    queue._dispatch(
        9,
        "client-live",
        "sig-call",
        {"ticker": "SPY", "side": "CALL", "score": 90},
        master_control=SimpleNamespace(mode="LIVE"),
        contract_selector=None,
        order_state_machine=object(),
        entry_watcher=object(),
    )

    assert calls[0][0][1] == "ERROR"
    assert calls[0][1]["error"] == "LIVE_FATAL_IMMEDIATE_EXECUTION_ENABLED"
