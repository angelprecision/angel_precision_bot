"""PR #233 parity tests.

These tests prove behavior parity for all six hardening pieces that previously
lived in the `ap/queue/__init__.py` package-shadow and now live directly in
`ap/queue.py`:

  1. enqueue_signal fail-closed on missing / invalid side
  2. _dispatch fail-closed on missing / invalid side BEFORE Master Control
  3. _log_signal_to_db writes "UNKNOWN" (not "CALL") on missing / invalid side
     and surfaces side_validation_error inside raw_payload
  4. _mark_job merges captured selector failure metadata into result_json
     when the terminating result has stage="contract_selection"
  5. write_breach_last_error general writer + write_deferred_breach_last_error
     compatibility wrapper preserving the PR #182 DEFERRED_BREACH label
  6. LIVE + ALLOW_IMMEDIATE_EXECUTION=1 is queue-fatal (ERROR, not REJECTED)
"""
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

import ap.queue as queue


# ─────────────────────────────────────────────────────────────────────────────
# Fakes for DB / Supabase boundaries
# ─────────────────────────────────────────────────────────────────────────────

class _FakeCursor:
    rowcount = 1

    def __init__(self, captured):
        self.captured = captured

    def execute(self, sql, params=None):
        self.captured.setdefault("calls", []).append((sql, params))
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


# ─────────────────────────────────────────────────────────────────────────────
# Helper unit tests — _normalize_queue_side
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize(
    "payload,expected_side",
    [
        ({"side": "CALL"},        "CALL"),
        ({"side": "call"},        "CALL"),
        ({"side": "  Call  "},    "CALL"),
        ({"side": "BUY"},         "CALL"),
        ({"side": "long"},        "CALL"),
        ({"side": "CALLS"},       "CALL"),
        ({"side": "bull"},        "CALL"),
        ({"side": "BULLISH"},     "CALL"),
        ({"side": "PUT"},         "PUT"),
        ({"side": "sell"},        "PUT"),
        ({"side": "SHORT"},       "PUT"),
        ({"side": "puts"},        "PUT"),
        ({"side": "bear"},        "PUT"),
        ({"side": "BEARISH"},     "PUT"),
        ({"direction": "CALL"},   "CALL"),  # falls back to direction
        ({"direction": "bearish"}, "PUT"),
    ],
)
def test_normalize_queue_side_accepts_aliases(payload, expected_side):
    side, err = queue._normalize_queue_side(payload)
    assert side == expected_side
    assert err is None


@pytest.mark.parametrize(
    "payload",
    [
        {},                       # no side, no direction
        {"side": ""},             # empty string
        {"side": "   "},          # whitespace only
        {"side": "UNKNOWN"},      # unrecognized
        {"side": "neutral"},
        {"side": None, "direction": None},
    ],
)
def test_normalize_queue_side_rejects_invalid(payload):
    side, err = queue._normalize_queue_side(payload)
    assert side is None
    assert err is not None
    assert err.startswith("invalid_or_missing_side")


def test_normalize_queue_side_rejects_non_dict():
    side, err = queue._normalize_queue_side(None)
    assert side is None
    assert err == "invalid_or_missing_side:payload_not_dict"

    side, err = queue._normalize_queue_side("not a dict")
    assert side is None
    assert err == "invalid_or_missing_side:payload_not_dict"


# ─────────────────────────────────────────────────────────────────────────────
# Piece 1 — enqueue_signal fail-closed on missing/invalid side
# ─────────────────────────────────────────────────────────────────────────────

def test_enqueue_does_not_default_missing_side_to_call(monkeypatch):
    captured = {}
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())

    inserted = queue.enqueue_signal(
        {"ticker": "SPY", "signal_id": "sig-missing-side"},
        client_id="client-a",
    )

    assert inserted is True
    payload = json.loads(captured["params"][2])
    assert "side" not in payload
    assert "direction" not in payload
    assert payload["side_validation_error"].startswith("invalid_or_missing_side")


def test_enqueue_does_not_infer_put_from_signal_id_suffix(monkeypatch):
    """Pre-#233 behavior inferred PUT from signal_id ending in ':PUT'.

    Parity check: that inference path is gone.  signal_id is opaque.
    """
    captured = {}
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())

    queue.enqueue_signal(
        {"ticker": "SPY", "signal_id": "scanner_strat_aapl_2026-06-30:PUT"},
        client_id="client-a",
    )

    payload = json.loads(captured["params"][2])
    assert "side" not in payload
    assert "direction" not in payload
    assert payload["side_validation_error"].startswith("invalid_or_missing_side")


def test_enqueue_normalizes_explicit_bearish_alias(monkeypatch):
    captured = {}
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())

    inserted = queue.enqueue_signal(
        {"ticker": "SPY", "signal_id": "sig-bearish", "direction": "bearish"},
        client_id="client-a",
    )

    assert inserted is True
    payload = json.loads(captured["params"][2])
    assert payload["side"] == "PUT"
    assert payload["direction"] == "PUT"
    assert "side_validation_error" not in payload


def test_enqueue_normalizes_explicit_bullish_alias(monkeypatch):
    captured = {}
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())

    queue.enqueue_signal(
        {"ticker": "SPY", "signal_id": "sig-bullish", "side": "BUY"},
        client_id="client-a",
    )

    payload = json.loads(captured["params"][2])
    assert payload["side"] == "CALL"
    assert payload["direction"] == "CALL"


def test_enqueue_stamps_identity_mode_and_submits_pretrigger_once(monkeypatch):
    captured = {}
    handoffs = []
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setattr(
        "ap.intelligence_context_handoff.enqueue_pretrigger_context_best_effort",
        lambda signal, **kwargs: handoffs.append((signal, kwargs)) or {"ok": True, "accepted": True},
    )

    inserted = queue.enqueue_signal(
        {
            "ticker": "SPY",
            "signal_id": "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72:abc123",
            "side": "CALL",
        },
        client_id="client-a",
        execution_mode="PAPER",
    )

    payload = json.loads(captured["params"][2])
    assert inserted is True
    assert payload["canonical_signal_id"] == "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72"
    assert payload["execution_mode"] == "paper"
    assert len(handoffs) == 1
    assert handoffs[0][1] == {
        "client_id": "client-a",
        "execution_mode": "PAPER",
        "canonical_signal_id": "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72",
    }


def test_duplicate_queue_insert_does_not_submit_intelligence(monkeypatch):
    captured = {}
    fake = _FakeConn(captured)
    fake.cursor.rowcount = 0
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: fake))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setattr(
        "ap.intelligence_context_handoff.enqueue_pretrigger_context_best_effort",
        lambda *_args, **_kwargs: pytest.fail("duplicate submitted intelligence"),
    )

    assert queue.enqueue_signal(
        {"ticker": "SPY", "signal_id": "dup-1", "side": "CALL"},
        client_id="client-a",
        execution_mode="PAPER",
    ) is False


def test_dispatch_no_longer_creates_duplicate_pretrigger_handoff():
    source = (__import__("pathlib").Path(queue.__file__)).read_text()
    dispatch = source[source.index("def _dispatch"):source.index("def worker_loop")]

    assert "enqueue_pretrigger_context_best_effort" not in dispatch


# ─────────────────────────────────────────────────────────────────────────────
# Piece 2 — _dispatch fail-closed BEFORE Master Control
# ─────────────────────────────────────────────────────────────────────────────

def test_dispatch_rejects_missing_side_before_master_control(monkeypatch):
    mark_calls = []
    rejection_logs = []

    # Patch _mark_job and _log_rejection_to_db on the module so _dispatch's
    # by-name lookup picks up the spies.
    monkeypatch.setattr(queue, "_mark_job", lambda *a, **k: mark_calls.append((a, k)))
    monkeypatch.setattr(queue, "_log_rejection_to_db", lambda **k: rejection_logs.append(k))
    # Make sure the LIVE-immediate-execution guard does not fire here.
    monkeypatch.setattr(queue, "ALLOW_IMMEDIATE_EXECUTION", False)

    # If MC.evaluate ever runs, fail the test — we must reject upstream of it.
    mc = SimpleNamespace(
        mode="PAPER",
        evaluate=lambda *a, **k: pytest.fail("Master Control reached with missing side"),
    )

    queue._dispatch(
        7,
        "client-a",
        "sig-missing-side",
        {"ticker": "SPY", "score": 90},
        master_control=mc,
        contract_selector=None,
        order_state_machine=object(),
        entry_watcher=object(),
    )

    assert mark_calls, "_mark_job was not invoked"
    args, kwargs = mark_calls[0]
    assert args[0] == 7
    assert args[1] == "REJECTED"
    assert kwargs["error"] == "queue_side_validation:INVALID_OR_MISSING_SIDE"
    assert kwargs["result"]["reason_code"] == "INVALID_OR_MISSING_SIDE"
    assert kwargs["result"]["stage"] == "queue_side_validation"

    assert rejection_logs, "_log_rejection_to_db was not invoked"
    rl = rejection_logs[0]
    assert rl["stage"] == "queue_side_validation"
    assert rl["reason_code"] == "INVALID_OR_MISSING_SIDE"
    assert rl["payload"]["side_validation_error"].startswith("invalid_or_missing_side")


def test_dispatch_rejects_invalid_side_before_master_control(monkeypatch):
    mark_calls = []
    monkeypatch.setattr(queue, "_mark_job", lambda *a, **k: mark_calls.append((a, k)))
    monkeypatch.setattr(queue, "_log_rejection_to_db", lambda **k: None)
    monkeypatch.setattr(queue, "ALLOW_IMMEDIATE_EXECUTION", False)

    mc = SimpleNamespace(
        mode="PAPER",
        evaluate=lambda *a, **k: pytest.fail("Master Control reached with invalid side"),
    )

    queue._dispatch(
        8,
        "client-a",
        "sig-bad-side",
        {"ticker": "SPY", "score": 90, "side": "NEUTRAL"},
        master_control=mc,
        contract_selector=None,
        order_state_machine=object(),
        entry_watcher=object(),
    )

    assert mark_calls[0][1]["error"] == "queue_side_validation:INVALID_OR_MISSING_SIDE"


# ─────────────────────────────────────────────────────────────────────────────
# Piece 3 — _log_signal_to_db writes "UNKNOWN" (not "CALL") for missing side
# ─────────────────────────────────────────────────────────────────────────────

def test_log_signal_to_db_records_unknown_not_call_for_missing_side(monkeypatch):
    captured = {}
    monkeypatch.setattr(queue, "_get_sb_client", lambda: _FakeSupabase(captured))

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
    assert captured["row"]["raw_payload"]["side_validation_error"].startswith(
        "invalid_or_missing_side"
    )


def test_log_signal_to_db_records_normalized_side_for_alias(monkeypatch):
    captured = {}
    monkeypatch.setattr(queue, "_get_sb_client", lambda: _FakeSupabase(captured))

    queue._log_signal_to_db(
        signal_id="sig-bearish",
        client_id="client-a",
        ticker="SPY",
        side="bearish",
        score=90,
        stage="contract_selection",
        reason_code="NO_CHAIN_DATA",
        human_reason="no chain rows",
        payload={"ticker": "SPY"},
    )

    assert captured["row"]["side"] == "PUT"
    assert "side_validation_error" not in captured["row"]["raw_payload"]


# ─────────────────────────────────────────────────────────────────────────────
# Piece 4 — _mark_job merges selector failure metadata into result_json
# ─────────────────────────────────────────────────────────────────────────────

def test_mark_job_merges_selector_failure_metadata_into_result(monkeypatch):
    """Verify the merge happens by inspecting the SQL params written to DB.

    We do NOT patch _mark_job itself (that would skip the merge code).  Instead
    we patch the DB layer to capture the result_json that gets persisted.
    """
    captured = {}
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())

    # Stash a selector failure for this job_id.  _mark_job should pop and merge.
    queue._selector_failure_by_job[11] = {
        "queue_reason_code": "NO_CHAIN_DATA",
        "chain_rows": 0,
        "survivor_count": 0,
        "top_reject_buckets": {"no_rows": 1},
        "tradier_status_code": 200,
        "retryable": False,
        "data_base_url": "https://sandbox.tradier.com/v1",
    }

    queue._mark_job(
        11,
        "REJECTED",
        result={"stage": "contract_selection", "reason": "NO_CHAIN_DATA"},
    )

    # Result was serialized as the 3rd UPDATE param.
    assert captured["params"] is not None
    result_json_str = captured["params"][2]
    persisted = json.loads(result_json_str)

    assert persisted["stage"] == "contract_selection"
    assert persisted["reason"] == "NO_CHAIN_DATA"
    assert persisted["selector_failure"]["queue_reason_code"] == "NO_CHAIN_DATA"
    assert persisted["chain_rows"] == 0
    assert persisted["survivor_count"] == 0
    assert persisted["top_reject_buckets"] == {"no_rows": 1}
    assert persisted["tradier_status_code"] == 200
    assert persisted["retryable"] is False
    assert persisted["data_base_url"] == "https://sandbox.tradier.com/v1"

    # Stash must be cleared on read so a re-mark with the same job_id does
    # not double-merge.
    assert 11 not in queue._selector_failure_by_job


def test_mark_job_does_not_merge_for_non_contract_selection_stage(monkeypatch):
    captured = {}
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())

    queue._selector_failure_by_job[12] = {
        "queue_reason_code": "NO_CHAIN_DATA",
        "chain_rows": 0,
    }

    queue._mark_job(
        12,
        "REJECTED",
        result={"stage": "master_control", "reason": "BLOCKED_CAPITAL"},
    )

    persisted = json.loads(captured["params"][2])
    assert "selector_failure" not in persisted
    assert "chain_rows" not in persisted
    # Stash for unrelated stage is preserved (no spurious pop).
    assert 12 in queue._selector_failure_by_job
    # Cleanup
    queue._selector_failure_by_job.pop(12, None)


# ─────────────────────────────────────────────────────────────────────────────
# Piece 5 — write_breach_last_error + back-compat wrapper
# ─────────────────────────────────────────────────────────────────────────────

def test_write_breach_last_error_writes_generic_label(monkeypatch):
    captured = {}
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())

    queue.write_breach_last_error(
        99,
        reason_code="QUOTE_FETCH_FAILED",
        explanation="tradier returned 502",
        attempt=2,
        client_id="client-a",
        ticker="SPY",
    )

    last_error_str, queue_id = captured["params"]
    assert queue_id == 99
    assert last_error_str.startswith("BREACH_ENTRY_FAILED:QUOTE_FETCH_FAILED:attempt_2")
    assert "tradier returned 502" in last_error_str


def test_write_breach_last_error_no_op_on_missing_queue_id(monkeypatch):
    called = {"hit": False}
    monkeypatch.setattr(queue, "_run_with_retry", lambda *a, **k: called.update(hit=True))

    queue.write_breach_last_error(None, reason_code="X")
    queue.write_breach_last_error(0, reason_code="X")

    assert called["hit"] is False


def test_write_deferred_breach_last_error_preserves_pr182_label(monkeypatch):
    """The deferred wrapper must still emit DEFERRED_BREACH_CONTRACT_FAILED.

    PR #182 operator dashboards grep on that exact label.  The generalized
    write_breach_last_error must not change the persisted format for the
    deferred path.
    """
    captured = {}
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())

    queue.write_deferred_breach_last_error(
        42,
        reason_code="CHEAP_CONTRACT_NO_UPGRADE",
        explanation="premium below floor",
        attempt=3,
        client_id="jason-live",
        ticker="SMCI",
    )

    last_error_str, queue_id = captured["params"]
    assert queue_id == 42
    assert last_error_str.startswith(
        "DEFERRED_BREACH_CONTRACT_FAILED:CHEAP_CONTRACT_NO_UPGRADE:attempt_3"
    )
    assert "premium below floor" in last_error_str


def test_write_deferred_breach_last_error_blank_reason_uses_legacy_fallback(monkeypatch):
    """Legacy code used 'BREACH_SELECTOR_RETURNED_NONE' as the fallback when
    reason_code was blank.  Preserve that for parity."""
    captured = {}
    monkeypatch.setattr(queue, "_conn", lambda: (lambda: _FakeConn(captured)))
    monkeypatch.setattr(queue, "_run_with_retry", lambda fn, *a, **k: fn())

    queue.write_deferred_breach_last_error(
        43,
        reason_code="",
    )

    last_error_str, _ = captured["params"]
    assert "DEFERRED_BREACH_CONTRACT_FAILED:BREACH_SELECTOR_RETURNED_NONE" in last_error_str


# ─────────────────────────────────────────────────────────────────────────────
# Piece 6 — LIVE + ALLOW_IMMEDIATE_EXECUTION=1 is queue-fatal
# ─────────────────────────────────────────────────────────────────────────────

def test_live_immediate_execution_is_queue_fatal(monkeypatch):
    mark_calls = []
    monkeypatch.setattr(queue, "_mark_job", lambda *a, **k: mark_calls.append((a, k)))
    monkeypatch.setattr(queue, "ALLOW_IMMEDIATE_EXECUTION", True)

    mc = SimpleNamespace(
        mode="LIVE",
        evaluate=lambda *a, **k: pytest.fail("MC reached despite LIVE_FATAL guard"),
    )

    queue._dispatch(
        9,
        "client-live",
        "sig-call",
        {"ticker": "SPY", "side": "CALL", "score": 90},
        master_control=mc,
        contract_selector=None,
        order_state_machine=object(),
        entry_watcher=object(),
    )

    assert mark_calls[0][0][1] == "ERROR"
    assert mark_calls[0][1]["error"] == "LIVE_FATAL_IMMEDIATE_EXECUTION_ENABLED"


# ─────────────────────────────────────────────────────────────────────────────
# Worker-loop binding parity
# ─────────────────────────────────────────────────────────────────────────────

def test_worker_loop_resolves_dispatch_by_name():
    """Worker loop must resolve `_dispatch` by name at call time so monkeypatches
    in tests (and any future in-module rebinding) take effect.  We confirm this
    by inspecting the bytecode-level name references in worker_loop's __code__.

    Pre-#233 the shim relied on `_base._dispatch = _dispatch` rebinding the
    legacy module's attribute; post-#233 there is a single _dispatch in this
    module and worker_loop's reference must point to it.
    """
    assert "_dispatch" in queue.worker_loop.__code__.co_names, (
        "worker_loop must reference _dispatch by name; otherwise the PR #233 "
        "hardened path will not run when worker_loop dispatches jobs."
    )
