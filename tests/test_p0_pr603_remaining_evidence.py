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


def test_postgres_recovery_race_startup_vs_startup_has_one_owner(monkeypatch):
    """Two independent startup-shaped recovery processes share one CAS winner."""
    _run_recovery_pair(monkeypatch, actor_two="startup")


def test_postgres_recovery_race_startup_vs_runtime_tick_has_one_owner(monkeypatch):
    """Startup recovery and the exact ClientRunner runtime tick cannot double-run."""
    _run_recovery_pair(monkeypatch, actor_two="runtime")


def test_postgres_recovery_race_watcher_callback_vs_runtime_tick_has_one_owner(monkeypatch):
    """A watcher callback and runtime recovery share the real durable CAS."""
    _run_recovery_pair(monkeypatch, actor_two="watcher")


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
        assert recovery_result["errors"] == []
        assert recovery_result["deferred_lifecycles_recovered"] == 1
        assert reread_meta["trigger_crossed_at"] == committed_timestamp
        assert reread_meta["trigger_crossed_at_provenance"] == committed_provenance
        assert len(fresh_watcher._pending) == 1
        assert fresh_callback.call_count == 0
        assert not fresh_watcher.broker.submit_order.called
        assert reread["status"] == "PENDING_TRIGGER"
        assert reread["broker_order_id"] is None
        assert reread["submitted_ts"] is None
    finally:
        _drop_schema(url, schema)
