"""Load-bearing lifecycle tests for PR #430 post-cancel ENTRY retries."""

from __future__ import annotations

import copy
import json
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock

import pytest
import psycopg2

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_p0_post_cancel_retry",
)
os.environ.setdefault("SCHEMA_ATTESTATION_ENABLED", "0")

import ap.order_monitor as order_monitor_module
import ap.post_cancel_retry as retry_module
from ap.order_monitor import APOrderMonitor
from ap.post_cancel_retry import (
    ENTRY_RETRY_MAX_ATTEMPTS,
    RetryAttemptCounterError,
    evaluate_retry,
    parse_retry_attempt_count,
)


CONTRACT = "QCOM260523C00185000"


@pytest.fixture(autouse=True)
def _retry_enabled(monkeypatch):
    monkeypatch.setattr(retry_module, "ENTRY_RETRY_ENABLED", True)
    monkeypatch.setattr(order_monitor_module, "ENTRY_RETRY_ENABLED", True)


@pytest.fixture
def monitor():
    broker = MagicMock()
    osm = MagicMock()
    pm = MagicMock()
    instance = APOrderMonitor(
        client_id="client-A",
        broker=broker,
        order_state_machine=osm,
        position_manager=pm,
        client_mode="PAPER",
        data_broker=broker,
    )
    instance._emit_order_event = MagicMock()
    instance._transition_claimed_retry = MagicMock(return_value=True)
    return instance


def _retry_order(
    *,
    attempt: int = 1,
    status: str = "ARMED",
    mode: str = "paper",
    meta: dict | None = None,
) -> dict:
    retry_meta = {
        "retry_status": status,
        "retry_attempts": attempt,
        "retry_attempt": attempt,
        "retry_ready_at": time.time() - 1,
        "execution_mode": mode,
        "mode": mode.upper(),
        "retry_claim_token": "claim-token",
        "retry_recovery_token": "recovery-token",
        "retry_payload": {
            "symbol": "QCOM",
            "ticker": "QCOM",
            "direction": "CALL",
            "signal_entry_price": 185.0,
            "trigger": {"strike": 185.0, "underlying_price": 185.0},
            "retry_attempts": attempt,
            "retry_attempt": attempt,
        },
    }
    retry_meta.update(meta or {})
    return {
        "local_order_id": "parent-1",
        "client_id": "client-A",
        "kind": "ENTRY",
        "status": "CANCELED",
        "contract": CONTRACT,
        "symbol": "QCOM",
        "direction": "CALL",
        "execution_mode": mode,
        "meta": retry_meta,
        "updated_ts": None,
    }


def _strict_submit_args(order: dict) -> tuple:
    meta = order["meta"]
    return (
        order["local_order_id"],
        order["contract"],
        meta["retry_payload"],
        meta,
    )


class _ClaimDB:
    def __init__(self, row: dict):
        self.row = copy.deepcopy(row)
        self.claimed = False
        self.queries: list[tuple[str, tuple]] = []
        self.claim_sql = ""
        self.claim_params: tuple = ()


class _ClaimConnection:
    def __init__(self, database: _ClaimDB):
        self.database = database
        self.result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        compact_sql = " ".join(str(sql).split())
        self.database.queries.append((compact_sql, tuple(params)))
        if compact_sql.startswith("SELECT"):
            if (
                "meta ->> 'retry_status' = 'ARMED'" in compact_sql
                and not self.database.claimed
            ):
                self.result = [copy.deepcopy(self.database.row)]
            else:
                self.result = []
            return
        if compact_sql.startswith("UPDATE"):
            self.result = None
            if (
                "meta ->> 'retry_status' = 'ARMED'" in compact_sql
                and not self.database.claimed
            ):
                self.database.claimed = True
                self.database.claim_sql = compact_sql
                self.database.claim_params = tuple(params)
                claimed = copy.deepcopy(self.database.row)
                claimed["meta"] = json.loads(params[0])
                self.database.row = claimed
                self.result = claimed

    def fetchall(self):
        return self.result if isinstance(self.result, list) else []

    def fetchone(self):
        return self.result if isinstance(self.result, dict) else None


class _IdentityFencedConnection(_ClaimConnection):
    def execute(self, sql, params=()):
        compact_sql = " ".join(str(sql).split())
        if compact_sql.startswith("SELECT"):
            requested_client = params[0]
        elif compact_sql.startswith("UPDATE"):
            requested_client = params[2]
        else:
            requested_client = None
        requested_mode = params[-5] if len(params) >= 5 else None
        if (
            requested_client != self.database.row.get("client_id")
            or requested_mode != str(self.database.row.get("execution_mode")).lower()
        ):
            self.result = [] if compact_sql.startswith("SELECT") else None
            return
        return super().execute(sql, params)


class _StaticScanConnection:
    def __init__(self, row: dict):
        self.row = row

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        return None

    def fetchall(self):
        return [copy.deepcopy(self.row)]


def _patch_scan(monkeypatch, row):
    monkeypatch.setattr(
        order_monitor_module,
        "conn",
        lambda: _StaticScanConnection(row),
    )


def _ensure_retry_orders_table(database_url: str) -> None:
    with psycopg2.connect(database_url) as database:
        with database.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT,
                    kind TEXT,
                    status TEXT,
                    contract TEXT,
                    symbol TEXT,
                    direction TEXT,
                    execution_mode TEXT,
                    broker_order_id TEXT,
                    last_error TEXT,
                    meta JSONB DEFAULT '{}'::jsonb,
                    created_ts TIMESTAMPTZ DEFAULT NOW(),
                    updated_ts TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )
            for column, column_type in (
                ("client_id", "TEXT"),
                ("kind", "TEXT"),
                ("status", "TEXT"),
                ("contract", "TEXT"),
                ("symbol", "TEXT"),
                ("direction", "TEXT"),
                ("execution_mode", "TEXT"),
                ("broker_order_id", "TEXT"),
                ("last_error", "TEXT"),
                ("meta", "JSONB DEFAULT '{}'::jsonb"),
                ("created_ts", "TIMESTAMPTZ DEFAULT NOW()"),
                ("updated_ts", "TIMESTAMPTZ DEFAULT NOW()"),
            ):
                cursor.execute(
                    f"ALTER TABLE orders ADD COLUMN IF NOT EXISTS "
                    f"{column} {column_type}"
                )


def test_postgres_two_workers_have_one_durable_claim():
    """Exercise the ARMED claim fence against two real PostgreSQL sessions."""
    test_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "").strip()
    database_url = os.getenv("DATABASE_URL", "").strip()
    if not test_url or test_url != database_url:
        pytest.skip(
            "requires an explicitly matching disposable PostgreSQL test URL"
        )

    _ensure_retry_orders_table(test_url)
    local_order_id = f"pr430-pg-{uuid.uuid4().hex}"
    retry_meta = {
        "retry_status": "ARMED",
        "retry_attempts": 1,
        "retry_attempt": 1,
        "retry_ready_at": time.time() - 1,
        "execution_mode": "paper",
        "mode": "PAPER",
        "retry_payload": {"ticker": "QCOM", "direction": "CALL"},
    }
    try:
        with psycopg2.connect(test_url) as database:
            with database.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM orders WHERE local_order_id = %s",
                    (local_order_id,),
                )
                cursor.execute(
                    """
                    INSERT INTO orders
                        (local_order_id, client_id, kind, status, contract,
                         symbol, direction, execution_mode, meta)
                    VALUES (%s, %s, 'ENTRY', 'CANCELED', %s, %s, %s, %s, %s::jsonb)
                    """,
                    (
                        local_order_id,
                        "client-A",
                        CONTRACT,
                        "QCOM",
                        "CALL",
                        "paper",
                        json.dumps(retry_meta),
                    ),
                )

        cas_owner = APOrderMonitor(
            client_id="client-A",
            broker=MagicMock(),
            order_state_machine=MagicMock(),
            position_manager=MagicMock(),
            client_mode="PAPER",
            data_broker=MagicMock(),
        )
        assert cas_owner._cas_retry_parent_meta(
            local_order_id,
            retry_meta,
            {"retry_cas_marker": "winner"},
            "paper",
        )
        assert not cas_owner._cas_retry_parent_meta(
            local_order_id,
            retry_meta,
            {"retry_cas_marker": "stale-writer"},
            "paper",
        )
        retry_meta["retry_cas_marker"] = "winner"

        def _worker():
            instance = APOrderMonitor(
                client_id="client-A",
                broker=MagicMock(),
                order_state_machine=MagicMock(),
                position_manager=MagicMock(),
                client_mode="PAPER",
                data_broker=MagicMock(),
            )
            return instance._claim_armed_retry(
                {
                    "local_order_id": local_order_id,
                    "client_id": "client-A",
                    "kind": "ENTRY",
                    "status": "CANCELED",
                    "contract": CONTRACT,
                    "symbol": "QCOM",
                    "direction": "CALL",
                    "execution_mode": "paper",
                    "meta": retry_meta,
                },
                expected_attempt=1,
                mode="paper",
            )

        with ThreadPoolExecutor(max_workers=2) as executor:
            results = [executor.submit(_worker) for _ in range(2)]
            claimed = [future.result() for future in results]

        assert sum(result is not None for result in claimed) == 1
        with psycopg2.connect(test_url) as database:
            with database.cursor() as cursor:
                cursor.execute(
                    "SELECT meta ->> 'retry_status', meta ->> 'retry_claim_token' "
                    "FROM orders WHERE local_order_id = %s",
                    (local_order_id,),
                )
                status, claim_token = cursor.fetchone()
        assert status == "IN_FLIGHT"
        assert claim_token and claim_token.strip() == claim_token
    finally:
        with psycopg2.connect(test_url) as database:
            with database.cursor() as cursor:
                cursor.execute(
                    "DELETE FROM orders WHERE local_order_id = %s",
                    (local_order_id,),
                )


def test_legacy_singular_counter_advances_to_attempt_two():
    order = _retry_order(attempt=0, meta={"retry_attempts": None, "retry_attempt": 1})
    decision = evaluate_retry(
        canceled_order=order,
        cancel_reason="entry_max_age_normal_reached",
        underlying_spot=185.05,
    )
    assert decision.action == "ARM"
    assert decision.attempt_number == 2
    assert decision.retry_payload["retry_attempts"] == 2


def test_canonical_counter_advances_to_attempt_two():
    order = _retry_order(attempt=1, meta={"retry_attempt": None})
    decision = evaluate_retry(
        canceled_order=order,
        cancel_reason="entry_max_age_normal_reached",
        underlying_spot=185.05,
    )
    assert decision.action == "ARM"
    assert decision.attempt_number == 2


def test_disagreeing_counter_keys_use_monotonic_max():
    meta = {"retry_attempts": 1, "retry_attempt": 3}
    assert parse_retry_attempt_count(meta) == 3
    decision = evaluate_retry(
        canceled_order=_retry_order(meta=meta),
        cancel_reason="entry_max_age_normal_reached",
        underlying_spot=185.05,
    )
    assert decision.action == "ABORT"
    assert decision.reason_code == "MAX_ATTEMPTS_REACHED"
    assert decision.attempt_number == 4


@pytest.mark.parametrize(
    "value",
    [True, False, -1, 1.0, " 1", "01", "not-an-attempt"],
)
def test_malformed_attempt_values_fail_closed(value):
    with pytest.raises(RetryAttemptCounterError):
        parse_retry_attempt_count({"retry_attempts": value})


def test_arm_persists_canonical_counter_and_mode_evidence(monitor, monkeypatch):
    monitor.osm.get_order.return_value = {
        "local_order_id": "parent-1",
        "symbol": "QCOM",
        "contract": CONTRACT,
        "direction": "CALL",
        "execution_mode": "paper",
        "meta": {
            "signal_entry_price": 185.0,
            "retry_attempts": 0,
            "score": 90.0,
            "ticker": "QCOM",
            "signal_id": "sig-1",
            "source": "scanner",
        },
    }
    monitor.broker.get_quote.return_value = {"last": 185.05}
    captured = {}

    def fake_parent_cas(local_order_id, prior_meta, patch, mode):
        captured["local_order_id"] = local_order_id
        captured["mode"] = mode
        captured["meta"] = dict(prior_meta)
        captured["meta"].update(patch)
        return True

    monkeypatch.setattr(monitor, "_cas_retry_parent_meta", fake_parent_cas)
    monitor._maybe_arm_post_cancel_retry(
        local_order_id="parent-1",
        contract=CONTRACT,
        cancel_reason="entry_max_age_normal_reached",
    )

    meta = captured["meta"]
    assert meta["retry_status"] == "ARMED"
    assert meta["retry_attempts"] == 1
    assert meta["retry_attempt"] == 1
    assert meta["retry_last_transition"] == "ARMED"
    assert meta["retry_payload"]["execution_mode"] == "paper"
    assert meta["retry_payload"]["retry_expected_execution_mode"] == "paper"


def test_arm_without_explicit_mode_fails_closed_without_parent_write(monitor):
    monitor.osm.get_order.return_value = {
        "local_order_id": "parent-no-mode",
        "symbol": "QCOM",
        "contract": CONTRACT,
        "direction": "CALL",
        "meta": {
            "signal_entry_price": 185.0,
            "retry_attempts": 0,
            "score": 90.0,
            "ticker": "QCOM",
            "signal_id": "sig-no-mode",
            "source": "scanner",
        },
    }
    monitor.broker.get_quote.return_value = {"last": 185.05}
    monitor._broker_owned_exit_recovery_mode = ""
    monitor._cas_retry_parent_meta = MagicMock()

    monitor._maybe_arm_post_cancel_retry(
        local_order_id="parent-no-mode",
        contract=CONTRACT,
        cancel_reason="entry_max_age_normal_reached",
    )

    monitor._cas_retry_parent_meta.assert_not_called()
    event = monitor._emit_order_event.call_args.kwargs
    assert event["decision"] == "ABORT"
    assert event["reason_code"] == "RETRY_EXECUTION_MODE_UNPROVEN"


def test_two_workers_have_one_durable_claim_and_one_submit(monitor, monkeypatch):
    row = _retry_order()
    database = _ClaimDB(row)
    monkeypatch.setattr(
        order_monitor_module,
        "conn",
        lambda: _ClaimConnection(database),
    )
    worker_two = APOrderMonitor(
        client_id="client-A",
        broker=monitor.broker,
        order_state_machine=MagicMock(),
        position_manager=MagicMock(),
        client_mode="PAPER",
        data_broker=monitor.broker,
    )
    worker_two._emit_order_event = MagicMock()
    monitor._recover_stale_inflight_retries = MagicMock()
    worker_two._recover_stale_inflight_retries = MagicMock()
    monitor._submit_armed_retry = MagicMock()
    worker_two._submit_armed_retry = MagicMock()

    monitor._check_armed_retries()
    worker_two._check_armed_retries()

    assert monitor._submit_armed_retry.call_count == 1
    assert worker_two._submit_armed_retry.call_count == 0
    assert database.claimed is True
    assert "client_id = %s" in database.claim_sql
    assert "execution_mode = %s" in database.claim_sql
    assert database.claim_params[2] == "client-A"
    assert database.claim_params[-5:] == (
        "paper", "paper", "PAPER", "paper", "PAPER",
    )
    claimed_meta = database.row["meta"]
    assert claimed_meta["retry_status"] == "IN_FLIGHT"
    assert claimed_meta["retry_claim_token"]


@pytest.mark.parametrize(
    "monitor_client,row_mode",
    [("client-B", "paper"), ("client-A", "live")],
)
def test_armed_claim_requires_exact_client_and_execution_mode(
    monitor,
    monkeypatch,
    monitor_client,
    row_mode,
):
    monitor.client_id = monitor_client
    row = _retry_order(mode=row_mode)
    database = _ClaimDB(row)
    monkeypatch.setattr(
        order_monitor_module,
        "conn",
        lambda: _IdentityFencedConnection(database),
    )
    monitor._recover_stale_inflight_retries = MagicMock()
    monitor._submit_armed_retry = MagicMock()

    monitor._check_armed_retries()

    assert database.claimed is False
    monitor._submit_armed_retry.assert_not_called()


def test_mode_fence_holds_before_process_signal(monitor, monkeypatch):
    order = _retry_order()
    monitor._retry_client_state_mode = lambda: "live"
    monitor._stamp_retry_status = MagicMock()
    process_signal = MagicMock()
    monkeypatch.setattr("ap.execution.process_signal", process_signal)

    monitor._submit_armed_retry(
        *_strict_submit_args(order),
        claim_token="claim-token",
        expected_mode="paper",
    )

    process_signal.assert_not_called()
    kwargs = monitor._stamp_retry_status.call_args.kwargs
    assert kwargs["status"] == "HOLD"
    assert "runtime_execution_mode_mismatch" in kwargs["detail"]


def test_submit_fence_loss_blocks_process_signal(monitor, monkeypatch):
    order = _retry_order()
    monitor._retry_client_state_mode = lambda: "paper"
    monitor._transition_claimed_retry = MagicMock(return_value=False)
    process_signal = MagicMock()
    monkeypatch.setattr("ap.execution.process_signal", process_signal)

    monitor._submit_armed_retry(
        *_strict_submit_args(order),
        claim_token="claim-token",
        expected_mode="paper",
    )

    process_signal.assert_not_called()


@pytest.mark.parametrize(
    "error",
    [
        "runaway_quote_at_submit",
        "risk_gate_blocked",
        "positions_full",
        "daily_trade_cap",
        "kill_switch_active",
        "read_only_mode",
        "client_inactive",
        "time_gate",
        "trend_gate",
    ],
)
def test_policy_and_risk_rejects_are_terminal(monitor, monkeypatch, error):
    order = _retry_order()
    monitor._retry_client_state_mode = lambda: "paper"
    monitor._find_retry_replacements = lambda *args: []
    monitor._stamp_retry_status = MagicMock()
    monitor._emit_order_event = MagicMock()
    monkeypatch.setattr(
        "ap.execution.process_signal",
        lambda broker, client_id, payload: {"ok": False, "error": error},
    )

    monitor._submit_armed_retry(
        *_strict_submit_args(order),
        claim_token="claim-token",
        expected_mode="paper",
    )

    kwargs = monitor._stamp_retry_status.call_args.kwargs
    assert kwargs["status"] == "ABORTED"
    assert "terminal_submit_reject" in kwargs["detail"]


def test_attempt_one_quote_refresh_rejection_rearms_attempt_two(monitor, monkeypatch):
    order = _retry_order(attempt=1)
    monitor._retry_client_state_mode = lambda: "paper"
    monitor._find_retry_replacements = lambda *args: []
    transition = MagicMock(return_value=True)
    monitor._transition_claimed_retry = transition
    monkeypatch.setattr(retry_module, "_compute_wait_secs", lambda rng=None: 0.0)

    monkeypatch.setattr(
        "ap.execution.process_signal",
        lambda broker, client_id, payload: {
            "ok": False,
            "error": "quote_refresh_failed",
            "refresh_ok": False,
        },
    )

    monitor._submit_armed_retry(
        *_strict_submit_args(order),
        claim_token="claim-token",
        expected_mode="paper",
    )

    patch = transition.call_args.kwargs["patch"]
    assert patch["retry_status"] == "ARMED"
    assert patch["retry_attempts"] == 2
    assert patch["retry_attempt"] == 2
    assert patch["retry_status_detail"] == "rearmed_after:quote_refresh_failed"
    assert patch["retry_payload"]["retry_attempts"] == 2
    assert patch["retry_payload"]["execution_mode"] == "paper"


def test_attempt_two_quote_refresh_rejection_exhausts(monitor, monkeypatch):
    order = _retry_order(attempt=ENTRY_RETRY_MAX_ATTEMPTS)
    monitor._retry_client_state_mode = lambda: "paper"
    monitor._find_retry_replacements = lambda *args: []
    monitor._stamp_retry_status = MagicMock()
    monkeypatch.setattr(
        "ap.execution.process_signal",
        lambda broker, client_id, payload: {
            "ok": False,
            "error": "quote_refresh_failed",
        },
    )

    monitor._submit_armed_retry(
        *_strict_submit_args(order),
        claim_token="claim-token",
        expected_mode="paper",
    )

    kwargs = monitor._stamp_retry_status.call_args.kwargs
    assert kwargs["status"] == "EXHAUSTED"
    assert "attempt=2/2" in kwargs["detail"]


@pytest.mark.parametrize(
    "owner_state, expected_classification",
    [(False, "TRANSIENT"), (None, "HOLD"), (True, "TERMINAL")],
)
def test_symbol_lock_rearm_requires_durable_owner_proof(
    monitor,
    owner_state,
    expected_classification,
):
    monitor._active_retry_symbol_owner_exists = lambda **kwargs: owner_state
    classification, _detail = monitor._classify_retry_submit_failure(
        result={"ok": False, "error": "symbol_locked", "symbol": "QCOM"},
        local_order_id="parent-1",
        retry_payload={"symbol": "QCOM"},
        mode="paper",
    )
    assert classification == expected_classification


def test_success_requires_and_persists_exact_replacement_ids(monitor, monkeypatch):
    order = _retry_order()
    monitor._retry_client_state_mode = lambda: "paper"
    monitor._stamp_retry_status = MagicMock()
    monitor._emit_order_event = MagicMock()
    captured_payload = {}

    def fake_process_signal(broker, client_id, payload):
        captured_payload.update(payload)
        return {
            "ok": True,
            "local_order_id": "replacement-1",
            "broker_order_id": "broker-1",
            "contract": CONTRACT,
        }

    monkeypatch.setattr("ap.execution.process_signal", fake_process_signal)
    monitor._submit_armed_retry(
        *_strict_submit_args(order),
        claim_token="claim-token",
        expected_mode="paper",
    )

    kwargs = monitor._stamp_retry_status.call_args.kwargs
    assert kwargs["status"] == "SUBMITTED"
    assert kwargs["extra"]["retry_new_local_order_id"] == "replacement-1"
    assert kwargs["extra"]["retry_new_broker_order_id"] == "broker-1"
    assert captured_payload["retry_of_local_oid"] == "parent-1"
    assert captured_payload["retry_claim_token"] == "claim-token"
    assert captured_payload["execution_mode"] == "paper"


def test_success_without_exact_replacement_identity_holds(monitor, monkeypatch):
    order = _retry_order()
    monitor._retry_client_state_mode = lambda: "paper"
    monitor._stamp_retry_status = MagicMock()
    monkeypatch.setattr(
        "ap.execution.process_signal",
        lambda broker, client_id, payload: {
            "ok": True,
            "local_order_id": "",
            "broker_order_id": "broker-1",
        },
    )

    monitor._submit_armed_retry(
        *_strict_submit_args(order),
        claim_token="claim-token",
        expected_mode="paper",
    )

    kwargs = monitor._stamp_retry_status.call_args.kwargs
    assert kwargs["status"] == "HOLD"
    assert "missing_exact_replacement_identity" in kwargs["detail"]


def test_malformed_payload_keeps_consumed_attempt_visible(monitor):
    monitor._stamp_retry_status = MagicMock()
    monitor._submit_armed_retry(
        "parent-1",
        CONTRACT,
        {},
        {"retry_attempts": 1, "retry_attempt": 1},
        claim_token="claim-token",
        expected_mode="paper",
    )
    kwargs = monitor._stamp_retry_status.call_args.kwargs
    assert kwargs["status"] == "FAILED"
    assert kwargs["extra"]["retry_attempts"] == 1
    assert "attempt=1" in kwargs["detail"]


def test_process_signal_exception_holds_with_attempt_detail(monitor, monkeypatch):
    order = _retry_order(attempt=1)
    monitor._retry_client_state_mode = lambda: "paper"
    monitor._find_retry_replacements = lambda *args: []
    monitor._stamp_retry_status = MagicMock()
    monitor._emit_order_event = MagicMock()
    monkeypatch.setattr(
        "ap.execution.process_signal",
        lambda broker, client_id, payload: (_ for _ in ()).throw(
            RuntimeError("transport failed")
        ),
    )

    monitor._submit_armed_retry(
        *_strict_submit_args(order),
        claim_token="claim-token",
        expected_mode="paper",
    )

    kwargs = monitor._stamp_retry_status.call_args.kwargs
    assert kwargs["status"] == "HOLD"
    assert "attempt=1" in kwargs["detail"]
    assert "exception_without_correlated_replacement" in kwargs["detail"]


def test_wrong_claim_replacement_is_not_accepted_as_submitted(monitor, monkeypatch):
    order = _retry_order()
    monitor._retry_client_state_mode = lambda: "paper"
    monitor._find_retry_replacements = lambda *args: [{
        "local_order_id": "replacement-from-other-attempt",
        "broker_order_id": "broker-foreign",
        "retry_lineage_exact": False,
    }]
    monitor._stamp_retry_status = MagicMock()
    monitor._emit_order_event = MagicMock()
    monkeypatch.setattr(
        "ap.execution.process_signal",
        lambda broker, client_id, payload: (_ for _ in ()).throw(
            RuntimeError("ambiguous transport result")
        ),
    )

    monitor._submit_armed_retry(
        *_strict_submit_args(order),
        claim_token="claim-token",
        expected_mode="paper",
    )

    kwargs = monitor._stamp_retry_status.call_args.kwargs
    assert kwargs["status"] == "HOLD"
    assert "replacement_truth_ambiguous" in kwargs["detail"]


@pytest.mark.parametrize(
    "replacements, expected_status, expected_detail",
    [
        (
            [{
                "local_order_id": "replacement-1",
                "broker_order_id": "broker-1",
                "retry_lineage_exact": True,
            }],
            "SUBMITTED",
            "recovered_existing_replacement",
        ),
        (
            [
                {
                    "local_order_id": "replacement-1",
                    "broker_order_id": "broker-1",
                    "retry_lineage_exact": True,
                },
                {
                    "local_order_id": "replacement-2",
                    "broker_order_id": "broker-2",
                    "retry_lineage_exact": True,
                },
            ],
            "HOLD",
            "multiple_correlated_replacements",
        ),
    ],
)
def test_stale_recovery_never_resubmits_known_replacement(
    monitor,
    monkeypatch,
    replacements,
    expected_status,
    expected_detail,
):
    stale = _retry_order(status="IN_FLIGHT")
    claimed = copy.deepcopy(stale)
    claimed["meta"]["retry_recovery_token"] = "recovery-token"
    _patch_scan(monkeypatch, stale)
    monitor._claim_stale_inflight_for_recovery = lambda row, mode: claimed
    monitor._find_retry_replacements = MagicMock(return_value=replacements)
    monitor._stamp_retry_status = MagicMock()

    monitor._recover_stale_inflight_retries("paper")

    monitor._find_retry_replacements.assert_called_once_with(
        "parent-1", "claim-token", "paper",
    )
    kwargs = monitor._stamp_retry_status.call_args.kwargs
    assert kwargs["status"] == expected_status
    assert expected_detail in kwargs["detail"]
    assert kwargs["claim_token"] == "recovery-token"
    assert kwargs["claim_token_field"] == "retry_recovery_token"


def test_stale_recovery_without_replacement_rearms_once_with_recovery_fence(
    monitor,
    monkeypatch,
):
    stale = _retry_order(status="IN_FLIGHT")
    claimed = copy.deepcopy(stale)
    _patch_scan(monkeypatch, stale)
    monitor._claim_stale_inflight_for_recovery = lambda row, mode: claimed
    monitor._find_retry_replacements = MagicMock(return_value=[])
    monitor._rearm_or_exhaust_retry = MagicMock()

    monitor._recover_stale_inflight_retries("paper")

    kwargs = monitor._rearm_or_exhaust_retry.call_args.kwargs
    assert kwargs["claim_token"] == "recovery-token"
    assert kwargs["claim_token_field"] == "retry_recovery_token"
    assert kwargs["failure_reason"] == "stale_inflight_no_replacement_proven"


def test_stale_submitting_without_replacement_holds_without_rearm(
    monitor,
    monkeypatch,
):
    stale = _retry_order(status="SUBMITTING")
    claimed = copy.deepcopy(stale)
    _patch_scan(monkeypatch, stale)
    monitor._claim_stale_inflight_for_recovery = lambda row, mode: claimed
    monitor._find_retry_replacements = MagicMock(return_value=[])
    monitor._rearm_or_exhaust_retry = MagicMock()
    monitor._stamp_retry_status = MagicMock()

    monitor._recover_stale_inflight_retries("paper")

    monitor._rearm_or_exhaust_retry.assert_not_called()
    kwargs = monitor._stamp_retry_status.call_args.kwargs
    assert kwargs["status"] == "HOLD"
    assert kwargs["detail"] == "stale_submitting_without_replacement_proof"
    assert kwargs["claim_token"] == "recovery-token"
    assert kwargs["claim_token_field"] == "retry_recovery_token"


def test_cancel_reason_taxonomy_stays_retryable_and_fail_closed():
    retryable = evaluate_retry(
        canceled_order=_retry_order(meta={"retry_attempts": 0, "retry_attempt": 0}),
        cancel_reason="entry_max_age_normal_reached",
        underlying_spot=185.05,
    )
    unknown = evaluate_retry(
        canceled_order=_retry_order(meta={"retry_attempts": 0, "retry_attempt": 0}),
        cancel_reason="future_unreviewed_cancel_reason",
        underlying_spot=185.05,
    )
    assert retryable.action == "ARM"
    assert unknown.action == "ABORT"
    assert unknown.reason_code == "UNKNOWN_REASON_FAIL_CLOSED"


def test_lineage_is_persisted_and_retry_submit_has_no_direct_broker_post():
    execution_source = (Path(__file__).parents[1] / "ap" / "execution.py").read_text()
    for key in (
        "retry_of_local_oid",
        "retry_claim_token",
        "retry_attempts",
        "retry_expected_execution_mode",
    ):
        assert key in execution_source

    monitor_source = (Path(__file__).parents[1] / "ap" / "order_monitor.py").read_text()
    submit_body = monitor_source[
        monitor_source.index("def _submit_armed_retry"):
        monitor_source.index("def _stamp_retry_status")
    ]
    assert "process_signal(self.broker, self.client_id, retry_payload)" in submit_body
    assert "self.broker.place_order(" not in submit_body
    assert "self.broker.cancel_order(" not in submit_body
