"""PR #607: due deferred-materialization retry authority recut."""

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import json
import os
import uuid
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

import pytest

from ap.pending_trigger_classifier import (
    PendingTriggerClassification,
    classify_pending_trigger_row,
    has_canonical_materialization_retry_authority,
    is_active_materialization_in_flight,
)


def _make_retry_row(*, ticker="MO", local_order_id="local-mo-retry-1", signal_id="signal-mo-1"):
    now = datetime.now(timezone.utc)
    return {
        "local_order_id": local_order_id,
        "client_id": "client@example.com",
        "execution_mode": "live",
        "signal_id": signal_id,
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "contract": f"DEFERRED:{ticker}",
        "symbol": ticker,
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "watcher_audit": {"reason_code": "trigger_ready"},
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
            "materialization_reason": "NO_CHAIN_DATA",
            "materialization_next_retry_at": (now - timedelta(minutes=1)).isoformat(),
            "materialization_last_failure_at": (now - timedelta(minutes=2)).isoformat(),
            "trigger_crossed_at": (now - timedelta(minutes=3)).isoformat(),
            "absolute_entry_deadline": (now + timedelta(minutes=30)).isoformat(),
            "materialization_generation": 1,
            "retry_attempt": 1,
            "materialization_attempts": 1,
            "breach_attempt_count": 1,
            "retry_max_attempts": 5,
            "broker_ready": False,
            "materialization_in_flight": False,
            "materialization_selector_failure": {
                "reason_code": "NO_CHAIN_DATA",
            },
        },
    }


@pytest.fixture
def canonical_retry_row():
    return _make_retry_row()


@pytest.mark.parametrize("ticker", ["MO", "MMM"])
def test_due_materialization_retry_is_waiting_retryable_on_production_shape(
    canonical_retry_row, ticker
):
    canonical_retry_row["symbol"] = ticker
    canonical_retry_row["meta"]["contract_deferred"] = True

    assert classify_pending_trigger_row(
        canonical_retry_row,
        watcher_owned=False,
    ) == PendingTriggerClassification.WAITING_RETRYABLE


def _set_retry_time(row, *, minutes_from_now: int) -> dict:
    retry_at = datetime.now(timezone.utc) + timedelta(minutes=minutes_from_now)
    row["meta"]["materialization_next_retry_at"] = retry_at.isoformat()
    return row


def _recovery(row, *, resume_return=None, watcher_proof=None):
    from ap_recovery import APStartupRecovery

    class _Watcher:
        def prove_materialization_retry_owner(self, *args, **kwargs):
            return watcher_proof or {
                "proven": False,
                "reason_code": "NO_WATCHER",
            }

    class _Cursor:
        def execute(self, *_args, **_kwargs):
            return self

        def fetchall(self):
            return [row]

    class _Connection:
        def __enter__(self):
            return _Cursor()

        def __exit__(self, *_args):
            return False

    resume = MagicMock(return_value=resume_return or {"disposition": "BROKER_READY"})
    execution_core = SimpleNamespace(
        resume_deferred_materialization_retry=resume,
    )
    osm = SimpleNamespace(client_id=row["client_id"])
    broker = MagicMock()
    recovery = APStartupRecovery(
        row["client_id"],
        broker=broker,
        osm=osm,
        pm=SimpleNamespace(),
        master_control=SimpleNamespace(mode=row["execution_mode"].upper()),
        entry_watcher=_Watcher(),
        execution_core=execution_core,
    )
    result = {"deferred_lifecycles_recovered": 0, "errors": []}

    import ap.db as db_module

    with patch.object(db_module, "conn", lambda: _Connection()), \
         patch.object(db_module, "run_with_retry", lambda fn, *a, **k: fn()):
        recovery._recover_deferred_breach_lifecycles(result)
    return result, resume, broker


def test_canonical_retry_authority_tolerates_absent_legacy_provenance(
    canonical_retry_row,
):
    canonical_retry_row["meta"]["contract_deferred"] = True
    assert has_canonical_materialization_retry_authority(canonical_retry_row)


def test_equal_duplicated_canonical_signal_identity_preserves_retry_authority(
    canonical_retry_row,
):
    row = deepcopy(canonical_retry_row)
    row["canonical_signal_id"] = "canonical-a"
    row["meta"]["canonical_signal_id"] = "canonical-a"

    assert has_canonical_materialization_retry_authority(row)
    assert classify_pending_trigger_row(
        row,
        watcher_owned=False,
    ) == PendingTriggerClassification.WAITING_RETRYABLE

    _result, resume, broker = _recovery(row)
    resume.assert_called_once()
    broker.assert_not_called()


def test_conflicting_duplicated_canonical_signal_identity_is_held_across_recovery(
    canonical_retry_row,
):
    row = deepcopy(canonical_retry_row)
    row["canonical_signal_id"] = "canonical-a"
    row["meta"]["canonical_signal_id"] = "canonical-b"

    assert not has_canonical_materialization_retry_authority(row)
    assert classify_pending_trigger_row(
        row,
        watcher_owned=False,
    ) == PendingTriggerClassification.STUCK_TRIGGER_READY

    _result, resume, broker = _recovery(row)
    resume.assert_not_called()
    broker.assert_not_called()

    from ap.pending_trigger_restart_recovery import (
        PendingTriggerRestartRecovery,
        _RowOutcome,
    )

    restart_broker = MagicMock()
    retry_dispatch = MagicMock()
    recovery = PendingTriggerRestartRecovery(
        client_id=row["client_id"],
        execution_mode=row["execution_mode"],
        osm=SimpleNamespace(),
        entry_watcher=None,
        broker=restart_broker,
        dry_run=True,
    )
    recovery._handle_retryable = retry_dispatch

    assert recovery.recover_one_row(row) == _RowOutcome.UNRESOLVED
    retry_dispatch.assert_not_called()
    restart_broker.assert_not_called()


def test_present_conflicting_provenance_is_not_retry_authority(canonical_retry_row):
    canonical_retry_row["meta"]["trigger_crossed_at_provenance"] = {
        "canonical_signal_id": "wrong-signal",
        "client_id": canonical_retry_row["client_id"],
        "execution_mode": "live",
        "local_order_id": canonical_retry_row["local_order_id"],
    }

    assert not has_canonical_materialization_retry_authority(canonical_retry_row)
    assert classify_pending_trigger_row(
        canonical_retry_row,
        watcher_owned=False,
    ) != PendingTriggerClassification.WAITING_RETRYABLE


def test_provenance_with_unexpected_key_is_held(canonical_retry_row):
    canonical_retry_row["meta"]["trigger_crossed_at_provenance"] = {
        "canonical_signal_id": canonical_retry_row["signal_id"],
        "client_id": canonical_retry_row["client_id"],
        "execution_mode": canonical_retry_row["execution_mode"],
        "local_order_id": canonical_retry_row["local_order_id"],
        "unexpected_authority": "garbage",
    }

    assert not has_canonical_materialization_retry_authority(canonical_retry_row)
    assert classify_pending_trigger_row(
        canonical_retry_row,
        watcher_owned=False,
    ) == PendingTriggerClassification.STUCK_TRIGGER_READY

    from ap.pending_trigger_restart_recovery import (
        PendingTriggerRestartRecovery,
        _RowOutcome,
    )

    recovery = PendingTriggerRestartRecovery(
        client_id=canonical_retry_row["client_id"],
        execution_mode=canonical_retry_row["execution_mode"],
        osm=SimpleNamespace(),
        entry_watcher=None,
        broker=MagicMock(),
        dry_run=True,
    )
    retry_dispatch = MagicMock()
    recovery._handle_retryable = retry_dispatch

    assert recovery.recover_one_row(canonical_retry_row) == _RowOutcome.UNRESOLVED
    retry_dispatch.assert_not_called()


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("client_id", "other-client@example.com"),
        ("client_email", "other-client@example.com"),
        ("execution_mode", "paper"),
        ("signal_id", "other-signal"),
        ("local_order_id", "other-local-order"),
    ],
)
def test_conflicting_duplicated_identity_is_not_legacy_retry_authority(
    canonical_retry_row, field, value
):
    canonical_retry_row["meta"][field] = value
    assert not has_canonical_materialization_retry_authority(canonical_retry_row)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("retry_attempt", None),
        ("materialization_attempts", None),
        ("breach_attempt_count", None),
        ("materialization_generation", None),
        ("retry_max_attempts", None),
        ("retry_attempt", 2),
        ("materialization_next_retry_at", None),
        ("materialization_last_failure_at", None),
        ("trigger_crossed_at", None),
        ("absolute_entry_deadline", None),
    ],
)
def test_incomplete_or_conflicting_retry_lineage_is_not_authority(
    canonical_retry_row, field, value
):
    canonical_retry_row["meta"][field] = value
    assert not has_canonical_materialization_retry_authority(canonical_retry_row)
    assert classify_pending_trigger_row(
        canonical_retry_row,
        watcher_owned=False,
    ) != PendingTriggerClassification.WAITING_RETRYABLE


def test_missing_selector_failure_reason_is_not_authority(canonical_retry_row):
    canonical_retry_row["meta"]["materialization_selector_failure"]["reason_code"] = None
    assert not has_canonical_materialization_retry_authority(canonical_retry_row)


@pytest.mark.parametrize(
    "reason_code", ["OI_TOO_LOW", "UNKNOWN_SELECTOR_REASON"]
)
def test_terminal_or_unknown_selector_reason_is_not_promoted(
    canonical_retry_row, reason_code
):
    canonical_retry_row["meta"]["materialization_selector_failure"]["reason_code"] = (
        reason_code
    )
    assert not has_canonical_materialization_retry_authority(canonical_retry_row)


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("broker_order_id",), "broker-1"),
        (("submitted_ts",), "2026-09-10T12:00:00+00:00"),
        (("meta", "broker_ready"), True),
        (("meta", "submit_intent_at"), "2026-09-10T12:00:00+00:00"),
        (("meta", "broker_submit_key"), "submit-key"),
    ],
)
def test_broker_handoff_contradictions_are_held_without_retry_execution(
    canonical_retry_row, path, value
):
    target = canonical_retry_row
    for key in path[:-1]:
        target = target[key]
    target[path[-1]] = value

    assert not has_canonical_materialization_retry_authority(canonical_retry_row)
    _result, resume, broker = _recovery(canonical_retry_row)
    resume.assert_not_called()
    broker.assert_not_called()


def test_ordinary_trigger_ready_without_retry_authority_stays_stuck(
    canonical_retry_row,
):
    canonical_retry_row["meta"]["materialization_status"] = None
    canonical_retry_row["meta"]["lifecycle_state"] = None
    assert classify_pending_trigger_row(
        canonical_retry_row,
        watcher_owned=False,
    ) == PendingTriggerClassification.STUCK_TRIGGER_READY


def test_active_materializer_precedes_retry_authority(canonical_retry_row):
    row = deepcopy(canonical_retry_row)
    now = datetime.now(timezone.utc)
    row["meta"].update(
        {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_owner": "materializer-1",
            "materialization_lease_until": (now + timedelta(minutes=5)).isoformat(),
        }
    )

    assert is_active_materialization_in_flight(row)
    assert classify_pending_trigger_row(
        row,
        watcher_owned=False,
    ) == PendingTriggerClassification.MATERIALIZATION_IN_FLIGHT


def test_wfc_retry_positive_control_is_unchanged(canonical_retry_row):
    row = deepcopy(canonical_retry_row)
    row["symbol"] = "WFC"
    row["contract"] = "DEFERRED:WFC"
    row["meta"]["contract_deferred"] = True

    assert classify_pending_trigger_row(
        row,
        watcher_owned=False,
    ) == PendingTriggerClassification.WAITING_RETRYABLE


def test_future_retry_waits_without_selector_or_broker_work(canonical_retry_row):
    row = _set_retry_time(deepcopy(canonical_retry_row), minutes_from_now=5)
    result, resume, broker = _recovery(
        row,
        watcher_proof={"proven": True, "reason_code": "WATCHER_OWNS_RETRY"},
    )

    resume.assert_not_called()
    broker.assert_not_called()
    assert result["deferred_lifecycles_recovered"] == 0
    assert row["status"] == "PENDING_TRIGGER"
    assert row["meta"]["lifecycle_state"] == "RETRY_WAIT"
    assert row["meta"]["materialization_status"] == "RETRY_PENDING"


def test_due_retry_uses_existing_consumer_exactly_once(canonical_retry_row):
    row = _set_retry_time(deepcopy(canonical_retry_row), minutes_from_now=-1)
    result, resume, broker = _recovery(row)

    resume.assert_called_once_with(
        local_order_id=row["local_order_id"],
        expected_generation=1,
        expected_retry_attempt=2,
        owner=f"recovery_retry:{row['client_id']}:{row['local_order_id']}:2",
    )
    broker.assert_not_called()
    assert result["deferred_lifecycles_recovered"] == 1


def test_restart_recovery_accepts_same_legacy_retry_authority(canonical_retry_row):
    from ap.pending_trigger_restart_recovery import (
        PendingTriggerRestartRecovery,
        _RowOutcome,
    )

    row = deepcopy(canonical_retry_row)
    osm = SimpleNamespace(
        get_order=MagicMock(return_value=row),
        update_order_meta=MagicMock(return_value=True),
    )
    recovery = PendingTriggerRestartRecovery(
        client_id=row["client_id"],
        execution_mode=row["execution_mode"],
        osm=osm,
        entry_watcher=None,
        broker=MagicMock(),
        quote_check_fn=MagicMock(),
    )

    assert recovery.recover_one_row(row) == _RowOutcome.RETRY_OWNED
    recovery.osm.update_order_meta.assert_called_once()
    recovery.quote_check_fn.assert_not_called()


def test_postgres_production_shape_routes_mo_and_mmm_once(monkeypatch):
    """Replay the durable JSONB row through startup due-retry routing."""
    database_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
    if not database_url:
        pytest.skip("disposable PostgreSQL URL not configured")

    psycopg2 = pytest.importorskip("psycopg2")
    extras = pytest.importorskip("psycopg2.extras")
    schema = f"pr607_{uuid.uuid4().hex}"

    class _Wrapper:
        def __init__(self, connection, cursor):
            self.connection = connection
            self.cursor = cursor

        @property
        def rowcount(self):
            return self.cursor.rowcount

        def execute(self, sql, params=None):
            self.cursor.execute(sql, params)
            return self

        def fetchone(self):
            return self.cursor.fetchone()

        def fetchall(self):
            return self.cursor.fetchall()

    @contextmanager
    def _pg_conn():
        connection = psycopg2.connect(database_url)
        cursor = connection.cursor(cursor_factory=extras.RealDictCursor)
        try:
            cursor.execute(f'SET search_path TO "{schema}"')
            yield _Wrapper(connection, cursor)
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            cursor.close()
            connection.close()

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA "{schema}"')
            cursor.execute(
                f"""
                CREATE TABLE "{schema}".orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    signal_id TEXT,
                    plan_id TEXT,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    execution_mode TEXT NOT NULL,
                    contract TEXT,
                    symbol TEXT,
                    direction TEXT,
                    score NUMERIC,
                    tier TEXT,
                    trigger_price NUMERIC,
                    stop_underlying NUMERIC,
                    target_underlying NUMERIC,
                    pattern TEXT,
                    timeframe TEXT,
                    qty INTEGER,
                    limit_price NUMERIC,
                    reserved_cost NUMERIC,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    meta JSONB,
                    created_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )

        rows = []
        for ticker in ("MO", "MMM"):
            row = _make_retry_row(
                ticker=ticker,
                local_order_id=f"local-{ticker.lower()}-pg",
                signal_id=f"signal-{ticker.lower()}-pg",
            )
            rows.append(row)

        with _pg_conn() as connection:
            for row in rows:
                connection.execute(
                    """
                    INSERT INTO orders (
                        local_order_id, client_id, signal_id, plan_id, kind,
                        status, execution_mode, contract, symbol, direction,
                        score, tier, trigger_price, stop_underlying,
                        target_underlying, pattern, timeframe, qty, limit_price,
                        reserved_cost, broker_order_id, submitted_ts, meta
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb
                    )
                    """,
                    (
                        row["local_order_id"],
                        row["client_id"],
                        row["signal_id"],
                        "plan-pr607",
                        "ENTRY",
                        "PENDING_TRIGGER",
                        "live",
                        row["contract"],
                        row["symbol"],
                        "CALL",
                        78.0,
                        "B",
                        130.0,
                        128.0,
                        133.0,
                        "3-1-2",
                        "1d",
                        1,
                        0.01,
                        0.0,
                        None,
                        None,
                        json.dumps(row["meta"]),
                    ),
                )

        from ap_recovery import APStartupRecovery

        resume = MagicMock(return_value={"disposition": "BROKER_READY"})
        execution_core = SimpleNamespace(
            resume_deferred_materialization_retry=resume,
        )

        class _Watcher:
            def prove_materialization_retry_owner(self, *args, **kwargs):
                return {"proven": False, "reason_code": "NO_WATCHER"}

        broker = MagicMock()
        recovery = APStartupRecovery(
            "client@example.com",
            broker=broker,
            osm=SimpleNamespace(client_id="client@example.com"),
            pm=SimpleNamespace(),
            master_control=SimpleNamespace(mode="LIVE"),
            entry_watcher=_Watcher(),
            execution_core=execution_core,
        )
        result = {"deferred_lifecycles_recovered": 0, "errors": []}

        import ap.db as db_module

        monkeypatch.setattr(db_module, "conn", _pg_conn)
        monkeypatch.setattr(db_module, "run_with_retry", lambda fn, *a, **k: fn())
        recovery._recover_deferred_breach_lifecycles(result)

        assert resume.call_count == 2
        assert {
            call.kwargs["local_order_id"] for call in resume.call_args_list
        } == {"local-mo-pg", "local-mmm-pg"}
        assert all(
            call.kwargs["expected_generation"] == 1
            and call.kwargs["expected_retry_attempt"] == 2
            for call in resume.call_args_list
        )
        assert broker.method_calls == []
        assert result["deferred_lifecycles_recovered"] == 2

        with _pg_conn() as connection:
            connection.execute(
                "SELECT local_order_id, status, meta FROM orders ORDER BY local_order_id"
            )
            persisted = connection.fetchall()
        assert [row["status"] for row in persisted] == [
            "PENDING_TRIGGER",
            "PENDING_TRIGGER",
        ]
        assert all(row["meta"]["lifecycle_state"] == "RETRY_WAIT" for row in persisted)
    finally:
        with admin.cursor() as cursor:
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()
