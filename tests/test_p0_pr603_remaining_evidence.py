"""Remaining executed proofs for PR #603.

These tests intentionally freeze the PR's production implementation.  The
PostgreSQL cases use the real APOrderStateMachine CAS methods and the real
APStartupRecovery.recover_deferred_lifecycles() boundary.  Only selector and
broker continuation is stubbed so the test proves ownership/concurrency
without placing an external order.
"""
from __future__ import annotations

import json
import os
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest


def _postgres_url_or_skip():
    pytest.importorskip("psycopg2")
    url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
    if not url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("disposable PostgreSQL URL not configured")
    return url


class _RealDictCursor:
    def __init__(self, cursor):
        self.cursor = cursor

    @property
    def rowcount(self):
        return self.cursor.rowcount

    def execute(self, sql, params=()):
        self.cursor.execute(sql, params)
        return self

    def fetchone(self):
        row = self.cursor.fetchone()
        return dict(row) if row else None

    def fetchall(self):
        return [dict(row) for row in self.cursor.fetchall()]


class _ScopedPostgres:
    def __init__(self, url, schema):
        self.url = url
        self.schema = schema

    @contextmanager
    def conn(self):
        import psycopg2
        import psycopg2.extras

        connection = psycopg2.connect(self.url)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(f'SET search_path TO "{self.schema}"')
            yield _RealDictCursor(cursor)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()


def _create_orders_schema(url, schema):
    import psycopg2

    admin = psycopg2.connect(url)
    try:
        with admin.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(
                f"""
                CREATE TABLE "{schema}".orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    execution_mode TEXT,
                    signal_id TEXT,
                    canonical_signal_id TEXT,
                    plan_id TEXT,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    filled_ts TIMESTAMPTZ,
                    contract TEXT,
                    symbol TEXT,
                    direction TEXT,
                    side TEXT,
                    qty INTEGER,
                    limit_price NUMERIC,
                    reserved_cost NUMERIC,
                    contract_selection_status TEXT,
                    score NUMERIC,
                    tier TEXT,
                    trigger_price NUMERIC,
                    stop_underlying NUMERIC,
                    target_underlying NUMERIC,
                    pattern TEXT,
                    timeframe TEXT,
                    meta JSONB,
                    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
            cursor.execute(f'CREATE TABLE "{schema}".positions (id BIGSERIAL PRIMARY KEY)')
            cursor.execute(f'CREATE TABLE "{schema}".proof_trades (id BIGSERIAL PRIMARY KEY)')
            cursor.execute(f'CREATE TABLE "{schema}".trade_queue (id BIGSERIAL PRIMARY KEY)')
        admin.commit()
    finally:
        admin.close()


def _drop_schema(url, schema):
    import psycopg2

    admin = psycopg2.connect(url)
    try:
        admin.autocommit = True
        with admin.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
    finally:
        admin.close()


def _read_order(url, schema, local_order_id):
    import psycopg2
    import psycopg2.extras

    connection = psycopg2.connect(url)
    try:
        with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                f'SELECT * FROM "{schema}".orders WHERE local_order_id=%s',
                (local_order_id,),
            )
            row = cursor.fetchone()
            return dict(row) if row else None
    finally:
        connection.close()


def _read_side_effect_counts(url, schema):
    import psycopg2

    connection = psycopg2.connect(url)
    try:
        with connection.cursor() as cursor:
            return {
                table: cursor.execute(
                    f'SELECT COUNT(*) FROM "{schema}".{table}'
                ) or cursor.fetchone()[0]
                for table in ("orders", "positions", "proof_trades", "trade_queue")
            }
    finally:
        connection.close()


class _CommitThenResponseLossPostgres(_ScopedPostgres):
    """Commit the first UPDATE, then lose the response before rowcount."""

    def __init__(self, url, schema, *, failure_pattern="trigger_crossed_at"):
        super().__init__(url, schema)
        self.failure_pattern = failure_pattern
        self.response_loss_count = 0
        self.authority_update_count = 0

    @contextmanager
    def conn(self):
        import psycopg2
        import psycopg2.extras

        connection = psycopg2.connect(self.url)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        response_loss_for_this_connection = False
        try:
            cursor.execute(f'SET search_path TO "{self.schema}"')

            class _Cursor(_RealDictCursor):
                def execute(inner_self, sql, params=()):
                    nonlocal response_loss_for_this_connection
                    if (
                        sql.lstrip().upper().startswith("UPDATE ORDERS")
                        and self.failure_pattern in sql
                    ):
                        response_loss_for_this_connection = True
                        self.authority_update_count += 1
                    return super(_Cursor, inner_self).execute(sql, params)

            yield _Cursor(cursor)
            connection.commit()
            if response_loss_for_this_connection and self.response_loss_count == 0:
                self.response_loss_count += 1
                raise ConnectionError("committed response lost before rowcount")
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()


def _patch_postgres_modules(monkeypatch, scoped):
    import ap.db as db_module
    import ap.order_state_machine as osm_module

    monkeypatch.setattr(db_module, "conn", scoped.conn)
    monkeypatch.setattr(db_module, "run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setattr(osm_module, "conn", scoped.conn)
    monkeypatch.setattr(osm_module, "run_with_retry", lambda fn, *a, **k: fn())


def _insert_order(scoped, *, local_order_id, client_id, mode, signal_id, meta,
                  contract="DEFERRED:AAPL"):
    with scoped.conn() as connection:
        connection.execute(
            """
            INSERT INTO orders (
                local_order_id, client_id, kind, status, execution_mode,
                signal_id, canonical_signal_id, plan_id, contract, symbol,
                direction, side, qty, limit_price, reserved_cost, score, tier,
                trigger_price, stop_underlying, target_underlying, pattern,
                timeframe, meta
            ) VALUES (
                %s,%s,'ENTRY','PENDING_TRIGGER',%s,
                %s,%s,%s,%s,%s,
                'CALL','CALL',1,1.25,125,85,'A',
                100,95,105,'test','5m',%s::jsonb
            )
            """,
            (
                local_order_id,
                client_id,
                mode,
                signal_id,
                meta["canonical_signal_id"],
                f"plan-{local_order_id}",
                contract,
                "AAPL",
                json.dumps(meta),
            ),
        )


def _retry_meta(*, local_order_id, client_id, mode, signal_id, canonical_signal_id,
                due_at, trigger_crossed_at):
    return {
        "lifecycle_state": "RETRY_WAIT",
        "materialization_status": "RETRY_PENDING",
        "materialization_in_flight": False,
        "materialization_generation": 7,
        "retry_attempt": 2,
        "breach_attempt_count": 2,
        "materialization_attempts": 2,
        "retry_max_attempts": 5,
        "materialization_next_retry_at": due_at,
        "next_retry_at": due_at,
        "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
        "materialization_detail": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "entry_path": "DEFERRED_BREACH_MATERIALIZATION",
        "materialization_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "retry_owner": "prior-owner",
        "broker_ready": False,
        "trigger_crossed_at": trigger_crossed_at,
        "trigger_crossed_at_provenance": {
            "canonical_signal_id": canonical_signal_id,
            "client_id": client_id,
            "execution_mode": mode,
            "local_order_id": local_order_id,
        },
        "signal_id": signal_id,
        "canonical_signal_id": canonical_signal_id,
        "client_id": client_id,
        "execution_mode": mode,
        "local_order_id": local_order_id,
        "trigger_price": 100,
        "observed_underlying_price": 101,
    }


def _build_recovery_core(osm, *, client_id, mode, callback_calls, callback_lock):
    from ap_execution_core import APExecutionCore

    core = APExecutionCore.__new__(APExecutionCore)
    core.client_id = client_id
    core.email = client_id
    core.execution_mode = mode
    core.mode = mode
    core.order_state_machine = osm
    core.entry_watcher = None
    core.broker = MagicMock()
    core.store = MagicMock()

    def _continuation(_watched):
        with callback_lock:
            callback_calls.append(client_id)
        return None

    # This is the post-ownership continuation only.  It prevents selector and
    # broker side effects while leaving the real durable claim/schedule CAS in
    # APExecutionCore.resume_deferred_materialization_retry() active.
    core._on_entry_trigger = _continuation
    return core


def _recovery(*, osm, core, client_id, mode):
    from ap_recovery import APStartupRecovery

    return APStartupRecovery(
        client_id=client_id,
        broker=core.broker,
        osm=osm,
        pm=None,
        master_control=SimpleNamespace(mode=mode.upper()),
        entry_watcher=None,
        execution_core=core,
    )


def _run_recovery_pair(monkeypatch, *, actor_two):
    """Run two real durable consumers against one due retry row."""
    url = _postgres_url_or_skip()
    schema = f"pr603_recovery_{uuid.uuid4().hex}"
    client_id = "jasoncosby1@gmail.com"
    mode = "live"
    local_order_id = f"pr603-retry-{uuid.uuid4().hex}"
    signal_id = f"sig-pr603-{uuid.uuid4().hex}"
    canonical_signal_id = f"canonical-pr603-{uuid.uuid4().hex}"
    now = datetime.now(timezone.utc)
    due_at = (now - timedelta(seconds=60)).isoformat()
    trigger_crossed_at = (now - timedelta(seconds=120)).isoformat()

    _create_orders_schema(url, schema)
    scoped = _ScopedPostgres(url, schema)
    try:
        _patch_postgres_modules(monkeypatch, scoped)
        monkeypatch.setenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", "5")
        monkeypatch.setenv("DEFERRED_RETRY_OWNER_GRACE_SECONDS", "0")
        meta = _retry_meta(
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            canonical_signal_id=canonical_signal_id,
            due_at=due_at,
            trigger_crossed_at=trigger_crossed_at,
        )
        _insert_order(
            scoped,
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            meta=meta,
        )

        import ap_recovery

        callback_calls = []
        callback_lock = threading.Lock()
        osm_one = __import__(
            "ap.order_state_machine", fromlist=["APOrderStateMachine"]
        ).APOrderStateMachine(client_id)
        osm_two = __import__(
            "ap.order_state_machine", fromlist=["APOrderStateMachine"]
        ).APOrderStateMachine(client_id)
        core_one = _build_recovery_core(
            osm_one,
            client_id=client_id,
            mode=mode,
            callback_calls=callback_calls,
            callback_lock=callback_lock,
        )
        core_two = _build_recovery_core(
            osm_two,
            client_id=client_id,
            mode=mode,
            callback_calls=callback_calls,
            callback_lock=callback_lock,
        )
        recovery_one = _recovery(
            osm=osm_one, core=core_one, client_id=client_id, mode=mode
        )

        if actor_two == "startup":
            recovery_two = _recovery(
                osm=osm_two, core=core_two, client_id=client_id, mode=mode
            )

            def _actor_two():
                return recovery_two.recover_deferred_lifecycles()

        elif actor_two == "runtime":
            import client_runner as client_runner_module

            monkeypatch.setattr(
                client_runner_module,
                "APStartupRecovery",
                ap_recovery.APStartupRecovery,
            )
            runner = object.__new__(client_runner_module.ClientRunner)
            runner.email = client_id
            runner.mode = mode.upper()
            runner.stopping = threading.Event()
            runner.stopped = threading.Event()
            runner.failed = threading.Event()
            runner.broker = core_two.broker
            runner.core = core_two
            runner.position_manager = object()
            runner.order_state_machine = osm_two
            runner.master_control = SimpleNamespace(mode=mode.upper())

            def _actor_two():
                return runner._run_deferred_breach_lifecycle_recovery()

        elif actor_two == "watcher":
            from ap_entry_watcher import APEntryWatcher, WatchedSignal

            watcher = APEntryWatcher(
                MagicMock(), order_state_machine=osm_two, mode=mode.upper()
            )
            watched = WatchedSignal(
                {
                    "signal_id": signal_id,
                    "canonical_signal_id": canonical_signal_id,
                    "local_order_id": local_order_id,
                    "client_id": client_id,
                    "execution_mode": mode,
                    "ticker": "AAPL",
                    "side": "CALL",
                    "entry_price": 100,
                    "stop_price": 95,
                    "target_price": 105,
                    "contract_symbol": "DEFERRED:AAPL",
                    "contract_deferred": True,
                    "trigger_crossed_at": trigger_crossed_at,
                    "materialization_generation": 7,
                    "retry_attempt": 2,
                    "metadata": dict(meta),
                },
                overnight=False,
            )
            watched._watcher_ref = watcher
            watcher.on_trigger = lambda value: core_two.resume_deferred_materialization_retry(
                local_order_id=local_order_id,
                expected_generation=7,
                expected_retry_attempt=3,
                owner=f"watcher-callback:{local_order_id}",
            )

            def _actor_two():
                return watcher.on_trigger(watched)

        else:  # pragma: no cover - helper misuse
            raise AssertionError(actor_two)

        barrier = threading.Barrier(2)
        outcomes = []

        def _run(name, fn):
            barrier.wait(timeout=10)
            outcomes.append((name, fn()))

        thread_one = threading.Thread(
            target=_run,
            args=("startup", recovery_one.recover_deferred_lifecycles),
        )
        thread_two = threading.Thread(
            target=_run,
            args=(actor_two, _actor_two),
        )
        thread_one.start()
        thread_two.start()
        thread_one.join(timeout=20)
        thread_two.join(timeout=20)
        assert not thread_one.is_alive()
        assert not thread_two.is_alive()

        final = _read_order(url, schema, local_order_id)
        assert final is not None
        final_meta = dict(final["meta"] or {})
        assert final["client_id"] == client_id
        assert final["execution_mode"] == mode
        assert final["signal_id"] == signal_id
        assert final["status"] == "PENDING_TRIGGER"
        assert final_meta["lifecycle_state"] == "RETRY_WAIT"
        assert final_meta["materialization_status"] == "RETRY_PENDING"
        assert final_meta["materialization_generation"] == 8
        assert final_meta["retry_attempt"] == 3
        assert final_meta["breach_attempt_count"] == 3
        assert final_meta["materialization_attempts"] == 3
        assert final_meta["retry_owner"]
        assert len(callback_calls) == 1
        assert final["broker_order_id"] is None
        assert final["submitted_ts"] is None
        assert all(not core.broker.submit_order.called for core in (core_one, core_two))
        assert len(outcomes) == 2
    finally:
        _drop_schema(url, schema)


def test_postgres_broker_ready_submit_claim_consumes_exact_trigger_authority(
    monkeypatch,
):
    """Submit ownership cannot create, rewrite, or broaden trigger evidence."""
    from ap.order_state_machine import APOrderStateMachine

    url = _postgres_url_or_skip()
    schema = f"pr603_submit_claim_authority_{uuid.uuid4().hex}"
    client_id = "submit-claim-authority@example.com"
    mode = "live"
    signal_id = f"sig-submit-claim-{uuid.uuid4().hex}"
    canonical_signal_id = f"canonical-submit-claim-{uuid.uuid4().hex}"
    local_order_id = f"pr603-submit-claim-{uuid.uuid4().hex}"
    trigger_crossed_at = datetime.now(timezone.utc).isoformat()
    exact_provenance = {
        "canonical_signal_id": canonical_signal_id,
        "client_id": client_id,
        "execution_mode": mode,
        "local_order_id": local_order_id,
    }

    _create_orders_schema(url, schema)
    scoped = _ScopedPostgres(url, schema)
    try:
        _patch_postgres_modules(monkeypatch, scoped)
        _insert_order(
            scoped,
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            contract="AAPL260117C00100000",
            meta={
                "lifecycle_state": "BROKER_READY",
                "materialization_status": "SELECTED",
                "materialization_generation": 3,
                "broker_ready": True,
                "canonical_signal_id": canonical_signal_id,
                "trigger_crossed_at": trigger_crossed_at,
                "trigger_crossed_at_provenance": exact_provenance,
                "selected_contract": "AAPL260117C00100000",
                "selected_limit": 1.25,
                "selected_qty": 1,
            },
        )

        osm = APOrderStateMachine(client_id)
        assert osm.claim_deferred_broker_ready_submit(
            local_order_id,
            owner="submit-owner",
            generation=3,
        )
        after = dict(_read_order(url, schema, local_order_id)["meta"] or {})
        assert after["trigger_crossed_at"] == trigger_crossed_at
        assert after["trigger_crossed_at_provenance"] == exact_provenance
        assert after["recovery_submit_owner"] == "submit-owner"

        timestamp_only_id = f"pr603-submit-claim-timestamp-only-{uuid.uuid4().hex}"
        _insert_order(
            scoped,
            local_order_id=timestamp_only_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            contract="AAPL260117C00100000",
            meta={
                "lifecycle_state": "BROKER_READY",
                "materialization_status": "SELECTED",
                "materialization_generation": 3,
                "broker_ready": True,
                "canonical_signal_id": canonical_signal_id,
                "trigger_crossed_at": trigger_crossed_at,
                "selected_contract": "AAPL260117C00100000",
                "selected_limit": 1.25,
                "selected_qty": 1,
            },
        )
        assert not osm.claim_deferred_broker_ready_submit(
            timestamp_only_id,
            owner="submit-owner",
            generation=3,
        )
        assert dict(
            _read_order(url, schema, timestamp_only_id)["meta"] or {}
        ) == {
            "lifecycle_state": "BROKER_READY",
            "materialization_status": "SELECTED",
            "materialization_generation": 3,
            "broker_ready": True,
            "trigger_crossed_at": trigger_crossed_at,
            "selected_contract": "AAPL260117C00100000",
            "selected_limit": 1.25,
            "selected_qty": 1,
        }

        extra_id = f"pr603-submit-claim-extra-{uuid.uuid4().hex}"
        extra_provenance = {**exact_provenance, "diagnostic": "not-authority"}
        extra_provenance["local_order_id"] = extra_id
        _insert_order(
            scoped,
            local_order_id=extra_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            contract="AAPL260117C00100000",
            meta={
                "lifecycle_state": "BROKER_READY",
                "materialization_status": "SELECTED",
                "materialization_generation": 3,
                "broker_ready": True,
                "canonical_signal_id": canonical_signal_id,
                "trigger_crossed_at": trigger_crossed_at,
                "trigger_crossed_at_provenance": extra_provenance,
                "selected_contract": "AAPL260117C00100000",
                "selected_limit": 1.25,
                "selected_qty": 1,
            },
        )
        assert not osm.claim_deferred_broker_ready_submit(
            extra_id,
            owner="submit-owner",
            generation=3,
        )
        assert dict(_read_order(url, schema, extra_id)["meta"] or {})[
            "trigger_crossed_at_provenance"
        ] == extra_provenance
    finally:
        _drop_schema(url, schema)


def _build_broker_ready_execution_core(osm, *, client_id, mode):
    """Build the real execution-core recovery boundary without broker POST."""
    from ap_execution_core import APExecutionCore

    core = APExecutionCore.__new__(APExecutionCore)
    core.client_id = client_id
    core.email = client_id
    core.execution_mode = mode
    core.mode = mode
    core.order_state_machine = osm
    core.entry_watcher = None
    core.broker = MagicMock()
    core.store = MagicMock()
    core._on_entry_trigger = MagicMock(return_value=None)
    return core


def _broad_recovery_meta(
    *, local_order_id, client_id, mode, signal_id, canonical_signal_id,
    lifecycle, contract, trigger_crossed_at, due_at,
):
    meta = {
        "lifecycle_state": lifecycle,
        "materialization_status": "SELECTED" if lifecycle == "BROKER_READY" else "PROOF_RETRY",
        "materialization_in_flight": False,
        "materialization_generation": 8,
        "retry_attempt": 3,
        "retry_max_attempts": 5,
        "materialization_attempts": 3,
        "broker_ready": lifecycle == "BROKER_READY",
        "materialization_owner": f"prior-owner:{local_order_id}",
        "current_owner": f"prior-owner:{local_order_id}",
        "selected_contract": contract,
        "selected_limit": 1.25,
        "selected_qty": 1,
        "trigger_crossed_at": trigger_crossed_at,
        "trigger_crossed_at_provenance": {
            "canonical_signal_id": canonical_signal_id,
            "client_id": client_id,
            "execution_mode": mode,
            "local_order_id": local_order_id,
        },
        "signal_id": signal_id,
        "canonical_signal_id": canonical_signal_id,
        "client_id": client_id,
        "execution_mode": mode,
        "local_order_id": local_order_id,
        "trigger_price": 100,
        "observed_underlying_price": 101,
        "proof_retry_attempt": 0,
        "proof_retry_max_attempts": 3,
        "proof_retry_next_at": due_at,
        "proof_retry_deadline": (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
        "selected_at": trigger_crossed_at,
        "selected_quote_at": trigger_crossed_at,
    }
    return meta


def _run_broad_recovery_race(monkeypatch, *, lifecycle):
    """Race startup recovery against the exact recurring runtime boundary."""
    from ap.order_state_machine import APOrderStateMachine
    import client_runner as client_runner_module
    import ap_recovery

    url = _postgres_url_or_skip()
    schema = f"pr603_broad_{lifecycle.lower()}_{uuid.uuid4().hex}"
    client_id = f"broad-{lifecycle.lower()}@example.com"
    mode = "paper"
    local_order_id = f"pr603-broad-{uuid.uuid4().hex}"
    signal_id = f"signal-broad-{uuid.uuid4().hex}"
    canonical_signal_id = f"canonical-broad-{uuid.uuid4().hex}"
    contract = "AAPL240101C00100000"
    now = datetime.now(timezone.utc)
    trigger_crossed_at = (now - timedelta(seconds=5)).isoformat()
    due_at = (now - timedelta(seconds=2)).isoformat()

    _create_orders_schema(url, schema)
    scoped = _ScopedPostgres(url, schema)
    try:
        _patch_postgres_modules(monkeypatch, scoped)
        monkeypatch.setenv("DEFERRED_RECOVERY_RETRY_DELAY_SECONDS", "60")
        monkeypatch.setenv("PRE_SUBMIT_PROOF_RETRY_DELAY_SECONDS", "60")
        meta = _broad_recovery_meta(
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            canonical_signal_id=canonical_signal_id,
            lifecycle=lifecycle,
            contract=contract,
            trigger_crossed_at=trigger_crossed_at,
            due_at=due_at,
        )
        _insert_order(
            scoped,
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            meta=meta,
            contract=contract,
        )
        before = _read_side_effect_counts(url, schema)

        osm_one = APOrderStateMachine(client_id)
        osm_two = APOrderStateMachine(client_id)
        core_one = _build_broker_ready_execution_core(
            osm_one, client_id=client_id, mode=mode
        )
        core_two = _build_broker_ready_execution_core(
            osm_two, client_id=client_id, mode=mode
        )
        recovery_one = _recovery(
            osm=osm_one, core=core_one, client_id=client_id, mode=mode
        )

        monkeypatch.setattr(
            client_runner_module,
            "APStartupRecovery",
            ap_recovery.APStartupRecovery,
        )
        runner = object.__new__(client_runner_module.ClientRunner)
        runner.email = client_id
        runner.mode = mode.upper()
        runner.stopping = threading.Event()
        runner.stopped = threading.Event()
        runner.failed = threading.Event()
        runner.broker = core_two.broker
        runner.core = core_two
        runner.position_manager = object()
        runner.order_state_machine = osm_two
        runner.master_control = SimpleNamespace(mode=mode.upper())

        outcomes = []
        barrier = threading.Barrier(2)

        def _startup_actor():
            barrier.wait(timeout=10)
            outcomes.append(recovery_one.recover_deferred_lifecycles())

        def _runtime_actor():
            barrier.wait(timeout=10)
            outcomes.append(runner._run_deferred_breach_lifecycle_recovery())

        startup_thread = threading.Thread(target=_startup_actor)
        runtime_thread = threading.Thread(target=_runtime_actor)
        startup_thread.start()
        runtime_thread.start()
        startup_thread.join(timeout=20)
        runtime_thread.join(timeout=20)
        assert not startup_thread.is_alive()
        assert not runtime_thread.is_alive()
        assert len(outcomes) == 2

        final = _read_order(url, schema, local_order_id)
        final_meta = dict(final["meta"] or {})
        assert final["client_id"] == client_id
        assert final["execution_mode"] == mode
        assert final["status"] == "PENDING_TRIGGER"
        assert final["broker_order_id"] is None
        assert final["submitted_ts"] is None
        # The canonical recovery scaffold claims one durable recovery-submit
        # owner, but the test callback intentionally stops before submit-intent
        # or broker POST.  A peer must lose this CAS rather than create a
        # second recovery authority.
        assert final_meta["recovery_submit_owner"]
        assert final_meta["current_owner"] == final_meta["recovery_submit_owner"]
        assert not final_meta.get("submit_intent_at")
        assert final_meta["materialization_generation"] == (8 if lifecycle == "BROKER_READY" else 9)
        assert final_meta["lifecycle_state"] == "BROKER_READY"
        assert final_meta["broker_ready"] is True
        assert sum(core._on_entry_trigger.call_count for core in (core_one, core_two)) == 1
        assert not core_one.broker.submit_order.called
        assert not core_one.broker.submit_entry.called
        assert not core_two.broker.submit_order.called
        assert not core_two.broker.submit_entry.called
        assert _read_side_effect_counts(url, schema) == before
    finally:
        _drop_schema(url, schema)


def test_postgres_recovery_race_startup_vs_startup_has_one_owner(monkeypatch):
    """Two independent startup-shaped recovery processes share one CAS winner."""
    _run_recovery_pair(monkeypatch, actor_two="startup")


def test_postgres_recovery_race_startup_vs_runtime_tick_has_one_owner(monkeypatch):
    """Startup recovery and the exact ClientRunner runtime tick cannot double-run."""
    _run_recovery_pair(monkeypatch, actor_two="runtime")


def test_postgres_recovery_race_watcher_callback_vs_runtime_tick_has_one_owner(monkeypatch):
    """A watcher callback and runtime recovery share the real durable CAS."""
    _run_recovery_pair(monkeypatch, actor_two="watcher")


@pytest.mark.parametrize("lifecycle", ("BROKER_READY", "PRE_SUBMIT_PROOF_RETRY"))
def test_postgres_broad_runtime_recovery_race_has_one_owner_without_broker_work(
    monkeypatch, lifecycle
):
    """The recurring broad pass cannot duplicate broker-ready/proof ownership."""
    _run_broad_recovery_race(monkeypatch, lifecycle=lifecycle)


def test_postgres_confirmation_restart_reconstructs_authority_without_duplicate_callback(
    monkeypatch,
):
    """A committed trigger authority survives fresh watcher/recovery objects."""
    from ap_entry_watcher import APEntryWatcher, WatchedSignal
    from ap.order_state_machine import APOrderStateMachine
    from ap_recovery import APStartupRecovery

    url = _postgres_url_or_skip()
    schema = f"pr603_restart_{uuid.uuid4().hex}"
    client_id = "confirm@example.com"
    mode = "paper"
    local_order_id = f"pr603-confirm-{uuid.uuid4().hex}"
    signal_id = f"signal-confirm-{uuid.uuid4().hex}"
    canonical_signal_id = f"canonical-confirm-{uuid.uuid4().hex}"
    now = datetime.now(timezone.utc)
    due_at = (now + timedelta(hours=1)).isoformat()
    trigger_crossed_at = (now - timedelta(seconds=5)).isoformat()
    meta = _retry_meta(
        local_order_id=local_order_id,
        client_id=client_id,
        mode=mode,
        signal_id=signal_id,
        canonical_signal_id=canonical_signal_id,
        due_at=due_at,
        trigger_crossed_at=trigger_crossed_at,
    )
    meta.pop("trigger_crossed_at", None)
    meta.pop("trigger_crossed_at_provenance", None)
    _create_orders_schema(url, schema)
    scoped = _ScopedPostgres(url, schema)
    try:
        _patch_postgres_modules(monkeypatch, scoped)
        _insert_order(
            scoped,
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            meta=meta,
        )

        signal = {
            "signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": local_order_id,
            "client_id": client_id,
            "client_email": client_id,
            "execution_mode": mode,
            "ticker": "AAPL",
            "side": "CALL",
            "entry_price": 100.0,
            "stop_price": 95.0,
            "target_price": 105.0,
            "score": 85.0,
            "grade": "A",
            "contract_symbol": "DEFERRED:AAPL",
            "contract_deferred": True,
            "materialization_generation": 7,
            "metadata": {
                "signal_id": signal_id,
                "canonical_signal_id": canonical_signal_id,
                "local_order_id": local_order_id,
                "client_id": client_id,
                "execution_mode": mode,
                "materialization_generation": 7,
                "contract_deferred": True,
            },
        }
        signal["metadata"]["materialization_next_retry_at"] = due_at
        signal["metadata"]["next_retry_at"] = due_at

        osm = APOrderStateMachine(client_id)
        first_watcher = APEntryWatcher(
            MagicMock(), order_state_machine=osm, mode=mode.upper()
        )
        first_watched = WatchedSignal(signal, overnight=False)
        first_watched._watcher_ref = first_watcher
        first_watcher._pending.append(first_watched)
        first_watcher._dedup_set.add(signal_id)
        first_callback = MagicMock(return_value={"disposition": "RETRY_WAIT"})
        first_watcher.on_trigger = first_callback

        with monkeypatch.context() as quote_patch:
            quote_patch.setattr(
                first_watcher,
                "_fetch_quotes",
                lambda _tickers: {"AAPL": {"bid": 99.0, "ask": 100.5}},
            )
            first_watcher._poll_active_signals(open_protect_active=False)
            quote_patch.setattr(
                first_watcher,
                "_fetch_quotes",
                lambda _tickers: {"AAPL": {"bid": 99.2, "ask": 100.6}},
            )
            first_watcher._poll_active_signals(open_protect_active=False)

        assert first_callback.call_count == 1
        committed = _read_order(url, schema, local_order_id)
        committed_meta = dict(committed["meta"] or {})
        committed_timestamp = committed_meta["trigger_crossed_at"]
        committed_provenance = committed_meta["trigger_crossed_at_provenance"]
        assert committed_provenance == {
            "canonical_signal_id": canonical_signal_id,
            "client_id": client_id,
            "execution_mode": mode,
            "local_order_id": local_order_id,
        }

        # Simulate process death by dropping every in-memory owner, then build
        # fresh process-shaped OSM/watcher/recovery objects from the row.
        del first_watched, first_watcher, osm
        fresh_osm = APOrderStateMachine(client_id)
        fresh_watcher = APEntryWatcher(
            MagicMock(), order_state_machine=fresh_osm, mode=mode.upper()
        )
        fresh_callback = MagicMock(return_value={"disposition": "RETRY_WAIT"})
        fresh_watcher.on_trigger = fresh_callback
        recovery = APStartupRecovery(
            client_id=client_id,
            broker=fresh_watcher.broker,
            osm=fresh_osm,
            pm=None,
            master_control=SimpleNamespace(mode=mode.upper()),
            entry_watcher=fresh_watcher,
            execution_core=None,
        )
        recovery_result = recovery.recover_deferred_lifecycles()

        reread = _read_order(url, schema, local_order_id)
        reread_meta = dict(reread["meta"] or {})
        assert recovery_result["errors"] == [
            "recovery_broker_ready_executor_unavailable"
        ]
        assert recovery_result["infrastructure_errors"] == [
            "recovery_broker_ready_executor_unavailable"
        ]
        assert recovery_result["deferred_lifecycles_recovered"] == 1
        assert reread_meta["trigger_crossed_at"] == committed_timestamp
        assert reread_meta["trigger_crossed_at_provenance"] == committed_provenance
        assert len(fresh_watcher._pending) == 1
        assert fresh_callback.call_count == 0
        assert not fresh_watcher.broker.submit_order.called
        assert reread["status"] == "PENDING_TRIGGER"
        assert reread["broker_order_id"] is None
        assert reread["submitted_ts"] is None

        # The first poll in the fresh process is intentionally on the trigger
        # side, but the durable retry schedule is still future-due.  Recovery
        # owns the watcher without replaying callback/materialization work.
        fresh_watcher._fetch_quotes = lambda _tickers: {
            "AAPL": {"bid": 99.2, "ask": 100.6}
        }
        fresh_watcher._poll_active_signals(open_protect_active=False)
        assert fresh_callback.call_count == 0
        assert _read_order(url, schema, local_order_id)["meta"] == reread_meta
    finally:
        _drop_schema(url, schema)


def test_postgres_post_commit_trigger_authority_response_loss_is_adopted_without_duplicates(
    monkeypatch,
):
    """A committed UPDATE with a lost response is adopted exactly once."""
    from ap_entry_watcher import APEntryWatcher, WatchedSignal
    from ap.order_state_machine import APOrderStateMachine

    url = _postgres_url_or_skip()
    schema = f"pr603_response_loss_{uuid.uuid4().hex}"
    client_id = "response-loss@example.com"
    mode = "paper"
    local_order_id = f"pr603-loss-{uuid.uuid4().hex}"
    signal_id = f"signal-loss-{uuid.uuid4().hex}"
    canonical_signal_id = f"canonical-loss-{uuid.uuid4().hex}"
    _create_orders_schema(url, schema)
    scoped = _CommitThenResponseLossPostgres(url, schema)

    class _NoAuditWatcher(APEntryWatcher):
        def _persist_watcher_audit(self, *args, **kwargs):
            return None

    try:
        _patch_postgres_modules(monkeypatch, scoped)
        meta = {
            "signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "client_id": client_id,
            "execution_mode": mode,
            "local_order_id": local_order_id,
        }
        _insert_order(
            scoped,
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            meta=meta,
            contract="AAPL240101C00100000",
        )
        before = _read_side_effect_counts(url, schema)

        signal = {
            "signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": local_order_id,
            "client_id": client_id,
            "execution_mode": mode,
            "ticker": "AAPL",
            "side": "CALL",
            "entry_price": 100.0,
            "stop_price": 95.0,
            "target_price": 105.0,
            "score": 85.0,
            "grade": "A",
            "contract_symbol": "AAPL240101C00100000",
            "metadata": dict(meta),
        }
        osm = APOrderStateMachine(client_id)
        broker = MagicMock()
        watcher = _NoAuditWatcher(broker, order_state_machine=osm, mode="PAPER")
        watched = WatchedSignal(signal, overnight=False)
        watched._watcher_ref = watcher
        watcher._pending.append(watched)
        watcher._dedup_set.add(signal_id)
        callback_calls = []
        selector = MagicMock()

        def _callback(_watched):
            callback_calls.append(local_order_id)
            return None

        watcher.on_trigger = _callback
        quotes = iter(
            (
                {"AAPL": {"bid": 99.0, "ask": 100.5}},
                {"AAPL": {"bid": 99.2, "ask": 100.6}},
                {"AAPL": {"bid": 99.2, "ask": 100.6}},
            )
        )
        watcher._fetch_quotes = lambda _tickers: next(quotes)

        watcher._poll_active_signals(open_protect_active=False)
        watcher._poll_active_signals(open_protect_active=False)
        watcher._poll_active_signals(open_protect_active=False)

        row = _read_order(url, schema, local_order_id)
        durable_meta = dict(row["meta"] or {})
        assert scoped.response_loss_count == 1
        assert scoped.authority_update_count == 2, (
            "the second UPDATE must be the fenced expected-existing CAS, "
            "not a third new-authority attempt"
        )
        assert isinstance(durable_meta.get("trigger_crossed_at"), str)
        assert datetime.fromisoformat(
            durable_meta["trigger_crossed_at"].replace("Z", "+00:00")
        ).tzinfo is not None
        assert durable_meta["trigger_crossed_at_provenance"] == {
            "canonical_signal_id": canonical_signal_id,
            "client_id": client_id,
            "execution_mode": mode,
            "local_order_id": local_order_id,
        }
        assert callback_calls == [local_order_id]
        assert selector.call_count == 0
        assert not broker.submit_order.called
        assert not broker.submit_entry.called
        assert _read_side_effect_counts(url, schema) == before
        assert watcher._pending == []
        assert watched._trigger_authority_persisted is True
    finally:
        _drop_schema(url, schema)


def test_postgres_restart_after_selector_claim_has_one_materialization_owner(
    monkeypatch,
):
    """A crash after the fenced selector claim cannot create a second owner."""
    from ap.order_state_machine import APOrderStateMachine
    from ap_recovery import APStartupRecovery

    url = _postgres_url_or_skip()
    schema = f"pr603_claim_restart_{uuid.uuid4().hex}"
    client_id = "claim-restart@example.com"
    mode = "paper"
    local_order_id = f"pr603-claim-{uuid.uuid4().hex}"
    signal_id = f"signal-claim-{uuid.uuid4().hex}"
    canonical_signal_id = f"canonical-claim-{uuid.uuid4().hex}"
    _create_orders_schema(url, schema)
    scoped = _ScopedPostgres(url, schema)
    try:
        _patch_postgres_modules(monkeypatch, scoped)
        monkeypatch.setenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", "5")
        monkeypatch.setenv("BREACH_SELECTOR_RETRY_DELAY_SECONDS", "60")
        meta = _retry_meta(
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            canonical_signal_id=canonical_signal_id,
            due_at=(datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat(),
            trigger_crossed_at=(datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat(),
        )
        _insert_order(
            scoped,
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            meta=meta,
        )
        osm = APOrderStateMachine(client_id)
        crashed_core = _build_recovery_core(
            osm,
            client_id=client_id,
            mode=mode,
            callback_calls=[],
            callback_lock=threading.Lock(),
        )

        def _crash_after_claim(_watched):
            raise RuntimeError("simulated process death after selector claim")

        crashed_core._on_entry_trigger = _crash_after_claim
        result = crashed_core.resume_deferred_materialization_retry(
            local_order_id=local_order_id,
            expected_generation=7,
            expected_retry_attempt=3,
            owner=f"crash-owner:{local_order_id}",
        )
        assert result["disposition"] == "RETRY_WAIT"

        claimed = _read_order(url, schema, local_order_id)
        claimed_meta = dict(claimed["meta"] or {})
        assert claimed_meta["materialization_generation"] == 8
        assert claimed_meta["retry_attempt"] == 3
        # Scheduling the retry releases the active materialization lease.  The
        # durable retry owner is the surviving authority for the next attempt.
        assert claimed_meta["materialization_owner"] == ""
        assert claimed_meta["retry_owner"] == f"crash-owner:{local_order_id}"
        assert claimed_meta["lifecycle_state"] == "RETRY_WAIT"
        assert claimed_meta["materialization_status"] == "RETRY_PENDING"

        # A fresh recovery object sees the retained future-due retry and
        # reconstitutes exactly one watcher, without claiming generation 9.
        fresh_osm = APOrderStateMachine(client_id)
        from ap_entry_watcher import APEntryWatcher

        fresh_watcher = APEntryWatcher(
            MagicMock(), order_state_machine=fresh_osm, mode=mode.upper()
        )
        fresh_watcher._persist_watcher_audit = lambda *args, **kwargs: None
        fresh_watcher._get_quote = lambda _ticker: {"bid": 99.0, "ask": 99.5}
        fresh_core = _build_recovery_core(
            fresh_osm,
            client_id=client_id,
            mode=mode,
            callback_calls=[],
            callback_lock=threading.Lock(),
        )
        recovery = APStartupRecovery(
            client_id=client_id,
            broker=fresh_core.broker,
            osm=fresh_osm,
            pm=None,
            master_control=SimpleNamespace(mode=mode.upper()),
            entry_watcher=fresh_watcher,
            execution_core=fresh_core,
        )
        recovered = recovery.recover_deferred_lifecycles()
        final = _read_order(url, schema, local_order_id)
        final_meta = dict(final["meta"] or {})
        assert recovered["errors"] == []
        assert len(fresh_watcher._pending) == 1
        assert final_meta["materialization_generation"] == 8
        assert final_meta["retry_attempt"] == 3
        assert final_meta["materialization_owner"] == ""
        assert final_meta["retry_owner"] == f"crash-owner:{local_order_id}"
        assert not fresh_core.broker.submit_order.called
    finally:
        _drop_schema(url, schema)


def test_postgres_restart_during_copyback_keeps_one_owner_without_broker_replay(
    monkeypatch,
):
    """A committed copyback with a lost response remains broker-free on restart."""
    from ap.order_state_machine import APOrderStateMachine
    from ap_recovery import APStartupRecovery

    url = _postgres_url_or_skip()
    schema = f"pr603_copyback_restart_{uuid.uuid4().hex}"
    client_id = "copyback-restart@example.com"
    mode = "paper"
    local_order_id = f"pr603-copyback-{uuid.uuid4().hex}"
    signal_id = f"signal-copyback-{uuid.uuid4().hex}"
    canonical_signal_id = f"canonical-copyback-{uuid.uuid4().hex}"
    _create_orders_schema(url, schema)
    setup = _ScopedPostgres(url, schema)
    try:
        _patch_postgres_modules(monkeypatch, setup)
        meta = _retry_meta(
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            canonical_signal_id=canonical_signal_id,
            due_at=(datetime.now(timezone.utc) - timedelta(seconds=60)).isoformat(),
            trigger_crossed_at=(datetime.now(timezone.utc) - timedelta(seconds=120)).isoformat(),
        )
        _insert_order(
            setup,
            local_order_id=local_order_id,
            client_id=client_id,
            mode=mode,
            signal_id=signal_id,
            meta=meta,
        )
        owner = f"copyback-owner:{local_order_id}"
        osm = APOrderStateMachine(client_id)
        assert osm.claim_deferred_materialization(
            local_order_id,
            owner=owner,
            new_generation=8,
            lease_until=(datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat(),
            trigger_crossed_at=meta["trigger_crossed_at"],
            trigger_price=100.0,
            observed_underlying_price=101.0,
            signal_id=signal_id,
            execution_mode=mode,
            retry_attempt=3,
        )

        # The copyback UPDATE commits, then its response is lost.  The OSM
        # returns False, but fresh durable state must be BROKER_READY and owned.
        copyback = _CommitThenResponseLossPostgres(
            url, schema, failure_pattern="contract_selection_status"
        )
        _patch_postgres_modules(monkeypatch, copyback)
        copyback_osm = APOrderStateMachine(client_id)
        assert not copyback_osm.persist_deferred_broker_ready(
            local_order_id,
            owner=owner,
            generation=8,
            signal_id=signal_id,
            execution_mode=mode,
            contract="AAPL240101C00100000",
            limit_price=1.25,
            qty=1,
            reserved_cost=125.0,
            selector_meta={"selector": "real-postgres-test"},
        )
        durable = _read_order(url, schema, local_order_id)
        durable_meta = dict(durable["meta"] or {})
        assert copyback.response_loss_count == 1
        assert durable_meta["broker_ready"] is True
        assert durable_meta["lifecycle_state"] == "BROKER_READY"
        assert durable_meta["materialization_generation"] == 8
        assert durable_meta["materialization_owner"] == owner
        assert durable["contract"] == "AAPL240101C00100000"

        # Fresh recovery has no broker-submit executor for this BROKER_READY
        # proof. It must retain the durable owner and perform zero broker work.
        fresh_broker = MagicMock()
        recovery = APStartupRecovery(
            client_id=client_id,
            broker=fresh_broker,
            osm=APOrderStateMachine(client_id),
            pm=None,
            master_control=SimpleNamespace(mode=mode.upper()),
            entry_watcher=None,
            execution_core=None,
        )
        recovery_result = recovery.recover_deferred_lifecycles()
        after = _read_order(url, schema, local_order_id)
        after_meta = dict(after["meta"] or {})
        assert recovery_result["errors"] == []
        assert after_meta["materialization_generation"] == 8
        assert after_meta["materialization_owner"] == owner
        assert not fresh_broker.submit_order.called
        assert not fresh_broker.submit_entry.called
    finally:
        _drop_schema(url, schema)


def test_postgres_claim_deferred_materialization_writes_complete_provenance_and_rejects_ambiguous_identity(
    monkeypatch,
):
    """The deferred claim seam cannot create timestamp-only authority."""
    from ap.order_state_machine import APOrderStateMachine

    url = _postgres_url_or_skip()
    schema = f"pr603_claim_provenance_{uuid.uuid4().hex}"
    client_id = "claim-provenance@example.com"
    mode = "live"
    local_order_id = f"pr603-claim-{uuid.uuid4().hex}"
    signal_id = f"sig-claim-{uuid.uuid4().hex}"
    canonical_signal_id = f"canonical-claim-{uuid.uuid4().hex}"
    lease_until = (datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat()
    trigger_crossed_at = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()

    _create_orders_schema(url, schema)
    scoped = _ScopedPostgres(url, schema)
    try:
        _patch_postgres_modules(monkeypatch, scoped)
        with scoped.conn() as connection:
            connection.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, kind, status, execution_mode,
                    signal_id, canonical_signal_id, meta
                ) VALUES (%s,%s,'ENTRY','PENDING_TRIGGER',%s,%s,%s,%s::jsonb)
                """,
                (
                    local_order_id,
                    client_id,
                    mode,
                    signal_id,
                    canonical_signal_id,
                    json.dumps({}),
                ),
            )

        osm = APOrderStateMachine(client_id)
        assert osm.claim_deferred_materialization(
            local_order_id,
            owner="claim-owner",
            new_generation=1,
            lease_until=lease_until,
            trigger_crossed_at=trigger_crossed_at,
            trigger_price=100.0,
            observed_underlying_price=101.0,
            signal_id=signal_id,
            execution_mode=mode,
        )

        row = _read_order(url, schema, local_order_id)
        meta = dict(row["meta"] or {})
        assert meta["trigger_crossed_at"] == trigger_crossed_at
        assert meta["trigger_crossed_at_provenance"] == {
            "canonical_signal_id": canonical_signal_id,
            "client_id": client_id,
            "execution_mode": mode,
            "local_order_id": local_order_id,
        }

        preserved_id = f"pr603-claim-preserved-{uuid.uuid4().hex}"
        preserved_raw = "2026-09-09T20:00:00Z"
        preserved_provenance = {
            "canonical_signal_id": canonical_signal_id,
            "client_id": client_id,
            "execution_mode": mode,
            "local_order_id": preserved_id,
        }
        with scoped.conn() as connection:
            connection.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, kind, status, execution_mode,
                    signal_id, canonical_signal_id, meta
                ) VALUES (%s,%s,'ENTRY','PENDING_TRIGGER',%s,%s,%s,%s::jsonb)
                """,
                (
                    preserved_id,
                    client_id,
                    mode,
                    signal_id,
                    canonical_signal_id,
                    json.dumps({
                        "lifecycle_state": "RETRY_WAIT",
                        "trigger_crossed_at": preserved_raw,
                        "trigger_crossed_at_provenance": preserved_provenance,
                    }),
                ),
            )
        assert osm.claim_deferred_materialization(
            preserved_id,
            owner="claim-owner",
            new_generation=1,
            lease_until=lease_until,
            trigger_crossed_at="2026-09-09T20:00:00+00:00",
            trigger_price=100.0,
            observed_underlying_price=101.0,
            signal_id=signal_id,
            execution_mode=mode,
        )
        preserved_meta = dict(_read_order(url, schema, preserved_id)["meta"] or {})
        assert preserved_meta["trigger_crossed_at"] == preserved_raw
        assert preserved_meta["trigger_crossed_at_provenance"] == preserved_provenance

        timestamp_only_id = f"pr603-claim-timestamp-only-{uuid.uuid4().hex}"
        with scoped.conn() as connection:
            connection.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, kind, status, execution_mode,
                    signal_id, canonical_signal_id, meta
                ) VALUES (%s,%s,'ENTRY','PENDING_TRIGGER',%s,%s,%s,%s::jsonb)
                """,
                (
                    timestamp_only_id,
                    client_id,
                    mode,
                    signal_id,
                    canonical_signal_id,
                    json.dumps({"trigger_crossed_at": trigger_crossed_at}),
                ),
            )
        assert not osm.claim_deferred_materialization(
            timestamp_only_id,
            owner="claim-owner",
            new_generation=1,
            lease_until=lease_until,
            trigger_crossed_at=trigger_crossed_at,
            trigger_price=100.0,
            observed_underlying_price=101.0,
            signal_id=signal_id,
            execution_mode=mode,
        )
        assert (_read_order(url, schema, timestamp_only_id)["meta"] or {}) == {
            "trigger_crossed_at": trigger_crossed_at
        }

        missing_id = f"pr603-claim-missing-{uuid.uuid4().hex}"
        with scoped.conn() as connection:
            connection.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, kind, status, execution_mode,
                    signal_id, meta
                ) VALUES (%s,%s,'ENTRY','PENDING_TRIGGER',%s,%s,%s::jsonb)
                """,
                (
                    missing_id,
                    client_id,
                    mode,
                    signal_id,
                    json.dumps({}),
                ),
            )
        assert not osm.claim_deferred_materialization(
            missing_id,
            owner="claim-owner",
            new_generation=1,
            lease_until=lease_until,
            trigger_crossed_at=trigger_crossed_at,
            trigger_price=100.0,
            observed_underlying_price=101.0,
            signal_id=signal_id,
            execution_mode=mode,
        )
        assert (_read_order(url, schema, missing_id)["meta"] or {}) == {}

        contradictory_id = f"pr603-claim-conflict-{uuid.uuid4().hex}"
        with scoped.conn() as connection:
            connection.execute(
                """
                INSERT INTO orders (
                    local_order_id, client_id, kind, status, execution_mode,
                    signal_id, canonical_signal_id, meta
                ) VALUES (%s,%s,'ENTRY','PENDING_TRIGGER',%s,%s,%s,%s::jsonb)
                """,
                (
                    contradictory_id,
                    client_id,
                    mode,
                    signal_id,
                    canonical_signal_id,
                    json.dumps({"canonical_signal_id": "different-canonical"}),
                ),
            )
        assert not osm.claim_deferred_materialization(
            contradictory_id,
            owner="claim-owner",
            new_generation=1,
            lease_until=lease_until,
            trigger_crossed_at=trigger_crossed_at,
            trigger_price=100.0,
            observed_underlying_price=101.0,
            signal_id=signal_id,
            execution_mode=mode,
        )
        assert (_read_order(url, schema, contradictory_id)["meta"] or {}) == {
            "canonical_signal_id": "different-canonical"
        }
    finally:
        _drop_schema(url, schema)
