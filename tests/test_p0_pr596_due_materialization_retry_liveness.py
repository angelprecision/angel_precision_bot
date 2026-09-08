"""Fail-first coverage for PR #596's due canonical retry liveness seam."""

from __future__ import annotations

import os
import json
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")
os.environ.setdefault("SCHEMA_ATTESTATION_ENABLED", "0")
from ap_execution_core import APExecutionCore


CLIENT_ID = "jason@example.com"
LOCAL_ORDER_ID = "oid-pr596-liveness"


@pytest.fixture(autouse=True)
def _open_retry_cutoff(monkeypatch):
    monkeypatch.setenv("BREACH_SELECTOR_RETRY_CUTOFF_ET", "2359")


def _row(*, due: bool = True) -> dict:
    now = datetime.now(timezone.utc)
    retry_at = now - timedelta(minutes=1) if due else now + timedelta(minutes=5)
    return {
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "live",
        "signal_id": "sig-pr596-liveness",
        "plan_id": "plan-pr596-liveness",
        "kind": "ENTRY",
        "contract": "DEFERRED:MO",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "MO",
        "direction": "CALL",
        "trigger_price": 300.0,
        "created_ts": now - timedelta(minutes=10),
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
            "materialization_generation": 1,
            "retry_attempt": 1,
            "breach_attempt_count": 1,
            "materialization_attempts": 1,
            "retry_max_attempts": 3,
            "materialization_next_retry_at": retry_at.isoformat(),
            "materialization_last_failure_at": (
                now - timedelta(minutes=2)
            ).isoformat(),
            "materialization_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            "materialization_selector_failure": {
                "reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            },
            "broker_ready": False,
            "materialization_in_flight": False,
            "trigger_crossed_at": (now - timedelta(minutes=3)).isoformat(),
            "absolute_entry_deadline": (now + timedelta(hours=1)).isoformat(),
            # This is the production shape that was stranded: the watcher
            # recorded trigger_ready, but the legacy provenance stamp is absent.
            "watcher_audit": {"reason_code": "trigger_ready"},
        },
    }


@pytest.mark.parametrize("due", [False, True])
def test_trigger_ready_canonical_retry_is_waiting_retryable(due):
    from ap.pending_trigger_classifier import (
        PendingTriggerClassification,
        classify_pending_trigger_row,
    )

    assert classify_pending_trigger_row(_row(due=due), watcher_owned=False) == (
        PendingTriggerClassification.WAITING_RETRYABLE
    )


@pytest.mark.parametrize(
    "mutation",
    [
        lambda meta: meta.update({"retry_attempt": 2}),
        lambda meta: meta.update({"submit_intent_at": "2026-09-08T20:00:00+00:00"}),
        lambda meta: meta.update({
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": "wrong-signal",
                "client_id": CLIENT_ID,
                "execution_mode": "live",
                "local_order_id": LOCAL_ORDER_ID,
            }
        }),
    ],
    ids=["conflicting-attempt", "broker-handoff", "conflicting-provenance"],
)
def test_contradictory_retry_authority_stays_stuck(mutation):
    from ap.pending_trigger_classifier import (
        PendingTriggerClassification,
        classify_pending_trigger_row,
    )

    row = _row()
    mutation(row["meta"])
    assert classify_pending_trigger_row(row, watcher_owned=False) == (
        PendingTriggerClassification.STUCK_TRIGGER_READY
    )


class _NoopBroker:
    pass


def _require_postgres():
    database_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "").strip()
    if not database_url:
        if os.getenv("GITHUB_ACTIONS", "").strip().lower() == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("disposable PostgreSQL URL not configured")
    psycopg2 = pytest.importorskip("psycopg2")
    return psycopg2, database_url


class _CursorWrapper:
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


@contextmanager
def _isolated_postgres(monkeypatch):
    psycopg2, database_url = _require_postgres()
    import psycopg2.extras
    import ap.db as db_mod
    import ap.order_state_machine as osm_mod
    from ap.order_state_machine import APOrderStateMachine

    schema = f"pr596_{uuid.uuid4().hex}"
    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    with admin.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA "{schema}"')
        cursor.execute(
            f"""
            CREATE TABLE "{schema}".orders (
                local_order_id TEXT PRIMARY KEY,
                client_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                execution_mode TEXT NOT NULL,
                signal_id TEXT,
                canonical_signal_id TEXT,
                plan_id TEXT,
                broker_order_id TEXT,
                submitted_ts TIMESTAMPTZ,
                contract TEXT,
                symbol TEXT,
                direction TEXT,
                side TEXT,
                timeframe TEXT,
                score NUMERIC,
                tier TEXT,
                trigger_price NUMERIC,
                stop_underlying NUMERIC,
                target_underlying NUMERIC,
                pattern TEXT,
                qty INTEGER,
                limit_price NUMERIC,
                reserved_cost NUMERIC,
                meta JSONB,
                last_error TEXT,
                created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )

    @contextmanager
    def pg_conn():
        connection = psycopg2.connect(database_url)
        cursor = connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cursor.execute(f'SET search_path TO "{schema}"')
            yield _CursorWrapper(cursor)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    monkeypatch.setattr(osm_mod, "conn", pg_conn)
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **k: fn())
    monkeypatch.setattr(db_mod, "conn", pg_conn)
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn, *a, **k: fn())
    try:
        yield APOrderStateMachine(CLIENT_ID), pg_conn, schema
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def _real_retry_meta(*, now: datetime, due: bool = True, deadline=None) -> dict:
    retry_at = now - timedelta(minutes=1) if due else now + timedelta(minutes=5)
    return {
        "lifecycle_state": "RETRY_WAIT",
        "materialization_status": "RETRY_PENDING",
        "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
        "materialization_generation": 1,
        "retry_attempt": 1,
        "breach_attempt_count": 1,
        "materialization_attempts": 1,
        "retry_max_attempts": 3,
        "materialization_next_retry_at": retry_at.isoformat(),
        "next_retry_at": retry_at.isoformat(),
        "deferred_retry_next_attempt_at": retry_at.isoformat(),
        "materialization_last_failure_at": (now - timedelta(minutes=2)).isoformat(),
        "materialization_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "materialization_selector_failure": {
            "reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        },
        "broker_ready": False,
        "materialization_in_flight": False,
        "trigger_crossed_at": (now - timedelta(minutes=3)).isoformat(),
        "absolute_entry_deadline": (
            deadline or (now + timedelta(hours=1))
        ).isoformat(),
        "watcher_audit": {"reason_code": "trigger_ready"},
    }


def _seed_real_retry(pg_conn, *, local_order_id: str, symbol: str, meta: dict):
    signal_id = f"sig-{local_order_id}"
    with pg_conn() as connection:
        connection.execute(
            """
            INSERT INTO orders (
                local_order_id, client_id, kind, status, execution_mode,
                signal_id, canonical_signal_id, plan_id, contract, symbol,
                direction, side, timeframe, score, tier, trigger_price,
                stop_underlying, target_underlying, pattern, qty, limit_price,
                reserved_cost, meta
            ) VALUES (
                %s, %s, 'ENTRY', 'PENDING_TRIGGER', 'live', %s, %s, %s,
                %s, %s, 'CALL', 'CALL', '1d', 78, 'B', 300, 298, 304,
                '3-1-2', 1, 0.01, 0, %s::jsonb
            )
            """,
            (
                local_order_id,
                CLIENT_ID,
                signal_id,
                signal_id,
                f"plan-{local_order_id}",
                f"DEFERRED:{symbol}",
                symbol,
                json.dumps({
                    **meta,
                    "client_id": CLIENT_ID,
                    "execution_mode": "live",
                    "signal_id": signal_id,
                    "canonical_signal_id": signal_id,
                }),
            ),
        )
    return signal_id


def _read_real(pg_conn, local_order_id: str) -> dict:
    with pg_conn() as connection:
        return connection.execute(
            "SELECT * FROM orders WHERE local_order_id = %s",
            (local_order_id,),
        ).fetchone()


class _ReplayCore:
    """Real APExecutionCore retry method with a deterministic canonical consumer."""

    class _Core(APExecutionCore):
        def __init__(self, osm):
            self.client_id = CLIENT_ID
            self.email = CLIENT_ID
            self.execution_mode = "live"
            self.mode = "LIVE"
            self.paper = False
            self.order_state_machine = osm
            self.broker = _NoopBroker()
            self.consumer_calls = 0

        def _on_entry_trigger(self, watched):
            self.consumer_calls += 1
            row = self.order_state_machine.get_order(watched.signal["local_order_id"])
            meta = row["meta"]
            next_at = datetime.now(timezone.utc) + timedelta(minutes=5)
            reason = "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
            ok = self.order_state_machine.schedule_deferred_materialization_retry(
                watched.signal["local_order_id"],
                owner=watched.signal["owner"],
                generation=watched.signal["materialization_generation"],
                reason_code=reason,
                attempt=watched.signal["retry_attempt"],
                max_attempts=3,
                next_retry_at=next_at.isoformat(),
                selector_failure={
                    "reason_code": reason,
                    "materialization_outcome": "RETRY_LATER_SELECTOR_BUDGET",
                    "materialization_detail": reason,
                    "entry_path": "DEFERRED_BREACH_MATERIALIZATION",
                },
                signal_id=watched.signal["signal_id"],
                execution_mode="live",
            )
            return {"disposition": "KEEP_WATCHER"} if ok else {
                "disposition": "KEEP_WATCHER",
                "reason_code": "REPLAY_SCHEDULE_FAILED",
            }


@pytest.mark.parametrize("symbol", ["MO", "MMM", "WFC"])
def test_real_postgres_due_retry_replay_mo_mmm_wfc(symbol, monkeypatch):
    """Due MO/MMM fail-first and WFC positive control use real OSM SQL."""
    from ap.pending_trigger_classifier import (
        PendingTriggerClassification,
        classify_pending_trigger_row,
        has_canonical_materialization_retry_authority,
    )
    from ap_recovery import APStartupRecovery

    with _isolated_postgres(monkeypatch) as (osm, pg_conn, _schema):
        local_order_id = f"pr596-replay-{symbol.lower()}-{uuid.uuid4().hex[:8]}"
        row = _real_retry_meta(now=datetime.now(timezone.utc), due=True)
        signal_id = _seed_real_retry(
            pg_conn, local_order_id=local_order_id, symbol=symbol, meta=row
        )
        hydrated = _read_real(pg_conn, local_order_id)
        assert has_canonical_materialization_retry_authority(hydrated)
        assert classify_pending_trigger_row(hydrated, watcher_owned=False) == (
            PendingTriggerClassification.WAITING_RETRYABLE
        )

        core = _ReplayCore._Core(osm)
        recovery = APStartupRecovery(
            client_id=CLIENT_ID,
            broker=core.broker,
            osm=osm,
            pm=SimpleNamespace(),
            master_control=SimpleNamespace(mode="LIVE"),
            entry_watcher=None,
            execution_core=core,
        )
        result = {"deferred_lifecycles_recovered": 0, "errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        after = _read_real(pg_conn, local_order_id)
        after_meta = after["meta"]
        assert core.consumer_calls == 1
        assert after["signal_id"] == signal_id
        assert after["status"] == "PENDING_TRIGGER"
        assert after_meta["lifecycle_state"] == "RETRY_WAIT"
        assert after_meta["materialization_status"] == "RETRY_PENDING"
        assert after_meta["materialization_generation"] == 2
        assert after_meta["retry_attempt"] == 1
        assert after_meta["broker_ready"] is False


class _DeadlineCore(_ReplayCore._Core):
    def _on_entry_trigger(self, watched):
        self.consumer_calls += 1
        assert self.order_state_machine.update_order_meta(
            watched.signal["local_order_id"],
            {"absolute_entry_deadline": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()},
        )
        return {}


def test_real_postgres_postclaim_deadline_terminalizes_once(monkeypatch):
    """The real claim, callback deadline, and terminal CAS leave no active row."""
    from ap.order_state_machine import APOrderStateMachine
    from ap_recovery import APStartupRecovery

    with _isolated_postgres(monkeypatch) as (osm, pg_conn, _schema):
        local_order_id = f"pr596-deadline-{uuid.uuid4().hex[:8]}"
        _seed_real_retry(
            pg_conn,
            local_order_id=local_order_id,
            symbol="MO",
            meta=_real_retry_meta(
                now=datetime.now(timezone.utc),
                due=True,
                deadline=datetime.now(timezone.utc) + timedelta(minutes=5),
            ),
        )

        class _CountingOSM(APOrderStateMachine):
            terminal_calls = 0

            def terminalize_materialization_retry(self, *args, **kwargs):
                type(self).terminal_calls += 1
                return super().terminalize_materialization_retry(*args, **kwargs)

        counted_osm = _CountingOSM(CLIENT_ID)
        core = _DeadlineCore(counted_osm)
        recovery = APStartupRecovery(
            client_id=CLIENT_ID,
            broker=core.broker,
            osm=counted_osm,
            pm=SimpleNamespace(),
            master_control=SimpleNamespace(mode="LIVE"),
            entry_watcher=None,
            execution_core=core,
        )
        result = {"deferred_lifecycles_recovered": 0, "errors": []}
        recovery._recover_deferred_breach_lifecycles(result)

        after = _read_real(pg_conn, local_order_id)
        assert counted_osm.terminal_calls == 1
        assert after["status"] == "EXPIRED"
        assert after["meta"]["lifecycle_state"] == "EXPIRED"
        assert after["meta"]["materialization_status"] == "FAILED_TERMINAL"
        assert after["meta"]["materialization_in_flight"] is False
        assert after["meta"]["materialization_owner"] == ""


def _claim_real_retry(osm, pg_conn, *, local_order_id: str, signal_id: str):
    owner = f"pr596-owner-{uuid.uuid4().hex}"
    assert osm.claim_deferred_materialization(
        local_order_id,
        owner=owner,
        new_generation=2,
        lease_until=(datetime.now(timezone.utc) + timedelta(minutes=2)).isoformat(),
        trigger_crossed_at=(datetime.now(timezone.utc) - timedelta(minutes=3)).isoformat(),
        trigger_price=300.0,
        observed_underlying_price=300.1,
        signal_id=signal_id,
        execution_mode="live",
        retry_attempt=1,
        advance_retry_attempt=False,
    )
    return owner


def test_real_postgres_terminal_cas_rowcount_and_broker_handoff_fences(monkeypatch):
    """CAS rowcount=0 is observable for handoff and concurrent advancement."""
    from ap.order_state_machine import APOrderStateMachine

    with _isolated_postgres(monkeypatch) as (osm, pg_conn, _schema):
        for mutation in (
            "broker_ready",
            "broker_submit_key",
            "broker_submit_payload_hash",
            "recovery_submit_owner",
            "nested_broker_submit_key",
            "signal_identity",
            "advance",
        ):
            local_order_id = f"pr596-cas-{mutation}-{uuid.uuid4().hex[:8]}"
            signal_id = _seed_real_retry(
                pg_conn,
                local_order_id=local_order_id,
                symbol="MO",
                meta=_real_retry_meta(now=datetime.now(timezone.utc), due=True),
            )
            owner = _claim_real_retry(
                osm, pg_conn, local_order_id=local_order_id, signal_id=signal_id
            )
            if mutation != "advance":
                broker_patch = {
                    "broker_ready": {"broker_ready": True},
                    "broker_submit_key": {"broker_submit_key": "submit-won"},
                    "broker_submit_payload_hash": {
                        "broker_submit_payload_hash": "payload-won"
                    },
                    "recovery_submit_owner": {
                        "recovery_submit_owner": "recovery-won"
                    },
                    "nested_broker_submit_key": {
                        "materialization": {"broker_submit_key": "nested-submit-won"}
                    },
                    "signal_identity": {"signal_id": "other-signal"},
                }[mutation]
                with pg_conn() as connection:
                    connection.execute(
                        "UPDATE orders SET meta = meta || %s::jsonb WHERE local_order_id = %s",
                        (json.dumps(broker_patch), local_order_id),
                    )
            else:
                # Hold the row lock in one PostgreSQL session while the
                # terminal CAS runs in another. The advancing writer commits
                # generation=3 first; the waiting terminal CAS then returns
                # rowcount=0 against the real concurrent state.
                row_locked = threading.Event()
                cas_started = threading.Event()
                worker_errors = []
                cas_result = {}

                def _advance_generation():
                    try:
                        with pg_conn() as connection:
                            connection.execute(
                                "SELECT local_order_id FROM orders "
                                "WHERE local_order_id = %s FOR UPDATE",
                                (local_order_id,),
                            )
                            row_locked.set()
                            assert cas_started.wait(timeout=5)
                            connection.execute(
                                "UPDATE orders SET meta = meta || %s::jsonb "
                                "WHERE local_order_id = %s",
                                (json.dumps({"materialization_generation": 3}), local_order_id),
                            )
                    except Exception as exc:  # pragma: no cover - surfaced below
                        worker_errors.append(exc)

                def _terminalize_while_advanced():
                    try:
                        assert row_locked.wait(timeout=5)
                        cas_started.set()
                        cas_result["ok"] = osm.terminalize_materialization_retry(
                            local_order_id,
                            reason="RETRY_DEADLINE_EXHAUSTED",
                            terminal_status="EXPIRED",
                            owner=owner,
                            generation=2,
                            retry_attempt=1,
                            client_id=CLIENT_ID,
                            execution_mode="live",
                            signal_id=signal_id,
                        )
                    except Exception as exc:  # pragma: no cover - surfaced below
                        worker_errors.append(exc)

                advance_thread = threading.Thread(target=_advance_generation)
                terminal_thread = threading.Thread(target=_terminalize_while_advanced)
                advance_thread.start()
                terminal_thread.start()
                advance_thread.join(timeout=10)
                terminal_thread.join(timeout=10)
                assert not worker_errors
                assert cas_result["ok"] is False
            if mutation != "advance":
                assert not osm.terminalize_materialization_retry(
                    local_order_id,
                    reason="RETRY_DEADLINE_EXHAUSTED",
                    terminal_status="EXPIRED",
                    owner=owner,
                    generation=2,
                    retry_attempt=1,
                    client_id=CLIENT_ID,
                    execution_mode="live",
                    signal_id=signal_id,
                )
            after = _read_real(pg_conn, local_order_id)
            assert after["status"] == "PENDING_TRIGGER"
            assert after["meta"]["lifecycle_state"] == "MATERIALIZING"
            assert after["meta"]["materialization_status"] == "RUNNING"

        local_order_id = f"pr596-cas-clean-{uuid.uuid4().hex[:8]}"
        signal_id = _seed_real_retry(
            pg_conn,
            local_order_id=local_order_id,
            symbol="MMM",
            meta=_real_retry_meta(now=datetime.now(timezone.utc), due=True),
        )
        owner = _claim_real_retry(
            osm, pg_conn, local_order_id=local_order_id, signal_id=signal_id
        )
        assert osm.terminalize_materialization_retry(
            local_order_id,
            reason="RETRY_DEADLINE_EXHAUSTED",
            terminal_status="EXPIRED",
            owner=owner,
            generation=2,
            retry_attempt=1,
            client_id=CLIENT_ID,
            execution_mode="live",
            signal_id=signal_id,
        )
        assert not osm.terminalize_materialization_retry(
            local_order_id,
            reason="RETRY_DEADLINE_EXHAUSTED",
            terminal_status="EXPIRED",
            owner=owner,
            generation=2,
            retry_attempt=1,
            client_id=CLIENT_ID,
            execution_mode="live",
            signal_id=signal_id,
        )


def test_restart_recovery_accepts_the_same_canonical_retry_authority():
    from ap.pending_trigger_restart_recovery import (
        PendingTriggerRestartRecovery,
        _RowOutcome,
    )

    row = _row(due=False)

    class _OSM:
        client_id = CLIENT_ID

        def get_order(self, local_order_id):
            return row

        def update_order_meta(self, local_order_id, patch):
            row["meta"].update(patch)
            return True

    recovery = PendingTriggerRestartRecovery(
        client_id=CLIENT_ID,
        execution_mode="live",
        osm=_OSM(),
        broker=_NoopBroker(),
        quote_check_fn=lambda *args, **kwargs: pytest.fail(
            "canonical materialization retry must not request a quote"
        ),
    )

    assert recovery.recover_one_row(row) == _RowOutcome.RETRY_OWNED


@pytest.mark.parametrize(
    ("label", "mutate"),
    [
        ("row_client_vs_meta_client", lambda row: row["meta"].update({"client_id": "other@example.com"})),
        ("row_client_vs_meta_email", lambda row: row["meta"].update({"client_email": "other@example.com"})),
        ("live_vs_paper", lambda row: row["meta"].update({"execution_mode": "paper"})),
        ("paper_vs_live", lambda row: (row.update({"execution_mode": "paper"}), row["meta"].update({"execution_mode": "live"}))),
        ("malformed_meta_mode", lambda row: row["meta"].update({"execution_mode": "sandbox"})),
        ("row_vs_meta_signal", lambda row: row["meta"].update({"signal_id": "other-signal"})),
        ("row_vs_meta_canonical", lambda row: row["meta"].update({"canonical_signal_id": "other-canonical"})),
        ("derived_vs_row_canonical", lambda row: row.update({"canonical_signal_id": "other-canonical"})),
        ("local_order_alias", lambda row: row["meta"].update({"local_order_id": "other-order"})),
        ("retry_timestamp_alias", lambda row: row["meta"].update({"next_retry_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()})),
        ("trigger_timestamp_alias", lambda row: row["meta"].update({"triggered_at": (datetime.now(timezone.utc) + timedelta(hours=1)).isoformat()})),
    ],
)
def test_legacy_missing_provenance_rejects_every_duplicate_identity_conflict(label, mutate):
    """The provenance exception never resolves a conflicting durable alias."""
    from ap.pending_trigger_classifier import (
        PendingTriggerClassification,
        classify_pending_trigger_row,
        has_canonical_materialization_retry_authority,
    )

    row = _row()
    mutate(row)
    assert not has_canonical_materialization_retry_authority(row), label
    assert classify_pending_trigger_row(row, watcher_owned=False) == (
        PendingTriggerClassification.STUCK_TRIGGER_READY
    )
    assert classify_pending_trigger_row(row, watcher_owned=True) == (
        PendingTriggerClassification.WAITING_RETRYABLE
    )


def test_legacy_missing_provenance_exact_identity_remains_allowed():
    from ap.pending_trigger_classifier import has_canonical_materialization_retry_authority

    row = _row()
    assert "trigger_crossed_at_provenance" not in row["meta"]
    assert has_canonical_materialization_retry_authority(row)


def test_conflicting_retry_authority_is_hold_end_to_end_without_any_mutation():
    from ap.pending_trigger_restart_recovery import (
        PendingTriggerRestartRecovery,
        _RowOutcome,
    )

    row = _row()
    row["meta"]["client_email"] = "other@example.com"

    class _OSM:
        client_id = CLIENT_ID

        def __init__(self):
            self.calls = {
                "get": 0,
                "meta": 0,
                "claim": 0,
                "cancel": 0,
                "replace": 0,
                "position": 0,
                "proof": 0,
            }

        def get_order(self, _local_order_id):
            self.calls["get"] += 1
            return row

        def update_order_meta(self, *_args, **_kwargs):
            self.calls["meta"] += 1
            return True

        def claim_deferred_materialization(self, *_args, **_kwargs):
            self.calls["claim"] += 1
            return True

        def cancel_pending_entry(self, *_args, **_kwargs):
            self.calls["cancel"] += 1
            return True

        def replace_pending_entry(self, *_args, **_kwargs):
            self.calls["replace"] += 1
            return True

        def mutate_position(self, *_args, **_kwargs):
            self.calls["position"] += 1
            return True

        def mutate_proof(self, *_args, **_kwargs):
            self.calls["proof"] += 1
            return True

    class _Broker:
        def __init__(self):
            self.calls = {"submit": 0, "cancel": 0, "replace": 0}

        def submit_order(self, *_args, **_kwargs):
            self.calls["submit"] += 1
            return None

        def cancel_order(self, *_args, **_kwargs):
            self.calls["cancel"] += 1
            return None

        def replace_order(self, *_args, **_kwargs):
            self.calls["replace"] += 1
            return None

    osm = _OSM()
    broker = _Broker()
    quote_calls = []
    recovery = PendingTriggerRestartRecovery(
        client_id=CLIENT_ID,
        execution_mode="live",
        osm=osm,
        broker=broker,
        quote_check_fn=lambda *args: quote_calls.append(args) or True,
    )

    assert recovery.recover_one_row(row) == _RowOutcome.UNRESOLVED
    assert osm.calls == {
        "get": 0,
        "meta": 0,
        "claim": 0,
        "cancel": 0,
        "replace": 0,
        "position": 0,
        "proof": 0,
    }
    assert broker.calls == {"submit": 0, "cancel": 0, "replace": 0}
    assert quote_calls == []


def test_direct_materializer_holds_conflicting_retry_authority_before_claim():
    from ap_execution_core import APExecutionCore

    row = _row()
    row["meta"]["canonical_signal_id"] = "other-canonical"

    class _OSM:
        def __init__(self):
            self.claim_calls = 0

        def get_order(self, _local_order_id):
            return row

        def claim_deferred_materialization(self, *_args, **_kwargs):
            self.claim_calls += 1
            return True

    osm = _OSM()
    core = object.__new__(APExecutionCore)
    core.client_id = CLIENT_ID
    core.email = CLIENT_ID
    core.execution_mode = "live"
    core.mode = "LIVE"
    core.paper = False
    core.order_state_machine = osm
    result = core.resume_deferred_materialization_retry(
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=1,
        expected_retry_attempt=2,
        owner="direct-retry-owner",
    )
    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RETRY_CONFLICTING_AUTHORITY"
    assert osm.claim_calls == 0


def test_postclaim_cas_miss_with_same_active_fence_is_write_failed_and_retained():
    """A post-claim CAS miss must retain the exact active recovery owner."""
    from ap import db as db_mod
    from ap_canonical_signal import build_canonical_signal_id
    from ap.pending_trigger_classifier import has_canonical_materialization_retry_authority
    from ap_recovery import APStartupRecovery

    owner = "recovery_retry:jason@example.com:oid-pr596-postclaim:3"
    signal_id = "sig-pr596-postclaim"
    canonical_signal_id = build_canonical_signal_id(signal_id)
    now = datetime.now(timezone.utc)
    retry_at = (now - timedelta(minutes=1)).isoformat()
    trigger_at = (now - timedelta(minutes=5)).isoformat()
    row = {
        "local_order_id": "oid-pr596-postclaim",
        "client_id": CLIENT_ID,
        "execution_mode": "live",
        "signal_id": signal_id,
        "canonical_signal_id": canonical_signal_id,
        "kind": "ENTRY",
        "contract": "DEFERRED:MO",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "created_ts": now,
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
            "materialization_generation": 1,
            "retry_attempt": 1,
            "breach_attempt_count": 1,
            "materialization_attempts": 1,
            "retry_max_attempts": 3,
            "materialization_next_retry_at": retry_at,
            "next_retry_at": retry_at,
            "deferred_retry_next_attempt_at": retry_at,
            "materialization_last_failure_at": (now - timedelta(minutes=2)).isoformat(),
            "materialization_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            "materialization_selector_failure": {
                "reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            },
            "broker_ready": False,
            "materialization_in_flight": False,
            "trigger_crossed_at": trigger_at,
            "absolute_entry_deadline": (now + timedelta(hours=1)).isoformat(),
            "watcher_audit": {"reason_code": "trigger_ready"},
            "client_id": CLIENT_ID,
            "execution_mode": "live",
            "canonical_signal_id": canonical_signal_id,
            "signal_id": signal_id,
        },
    }

    active_row = dict(row)
    active_row["meta"] = {
        **row["meta"],
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_generation": 2,
        "materialization_in_flight": True,
        "materialization_owner": owner,
        "current_owner": owner,
        "watcher_token": owner,
        "trigger_crossed_at_provenance": {
            "canonical_signal_id": canonical_signal_id,
            "client_id": CLIENT_ID,
            "execution_mode": "live",
            "local_order_id": "oid-pr596-postclaim",
        },
    }

    class _Cursor:
        rowcount = 1

        def execute(self, *_args, **_kwargs):
            return self

        def fetchall(self):
            return [row]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class _OSM:
        def __init__(self):
            self.terminal_calls = 0
            self.retention_calls = []

        def terminalize_materialization_retry(self, *_args, **_kwargs):
            self.terminal_calls += 1
            return False

        def get_order(self, _local_order_id):
            return active_row

        def retain_recovery_ownership_if_no_watcher(self, local_order_id, **kwargs):
            self.retention_calls.append((local_order_id, kwargs))
            return True

    class _Core:
        def resume_deferred_materialization_retry(self, **_kwargs):
            return {
                "disposition": "TERMINAL_REQUIRED",
                "reason_code": "RETRY_DEADLINE_EXHAUSTED",
                "terminal_status": "EXPIRED",
                "expected_client_id": CLIENT_ID,
                "expected_execution_mode": "live",
                "expected_signal_id": signal_id,
                "expected_lifecycle_state": "MATERIALIZING",
                "expected_materialization_status": "RUNNING",
                "expected_owner": owner,
                "expected_generation": 2,
                "expected_retry_attempt": 1,
            }

    osm = _OSM()
    recovery = APStartupRecovery(
        client_id=CLIENT_ID,
        broker=_NoopBroker(),
        osm=osm,
        pm=SimpleNamespace(),
        master_control=SimpleNamespace(mode="LIVE"),
        entry_watcher=None,
        execution_core=_Core(),
    )
    result = {"deferred_lifecycles_recovered": 0, "errors": []}

    class _Conn:
        def __enter__(self):
            return _Cursor()

        def __exit__(self, *_args):
            return False

    with pytest.MonkeyPatch.context() as patcher:
        patcher.setattr(db_mod, "conn", lambda: _Conn())
        patcher.setattr(db_mod, "run_with_retry", lambda fn, *args, **kwargs: fn())
        assert has_canonical_materialization_retry_authority(row)
        recovery._recover_deferred_breach_lifecycles(result)

    assert osm.terminal_calls == 1, result
    assert osm.retention_calls
    assert osm.retention_calls[0][0] == row["local_order_id"]
    assert "fenced_postclaim_write_failed" in str(result["errors"])
