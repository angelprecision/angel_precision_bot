from __future__ import annotations

import json
import os
import types
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    os.environ.get("INTELLIGENCE_POSTGRES_TEST_URL", "postgresql://mock/mock"),
)

import ap  # noqa: E402  - installs mandatory guard
import ap_execution_core  # noqa: E402
from ap.broker_submit_reconciliation_guard import (  # noqa: E402
    _exact_live_submit_identity,
    _record_first_no_match,
    _release_after_proven_absence,
)
from ap.brokers.tradier import TradierBroker, TradierConfig  # noqa: E402
from ap.order_state_machine import APOrderStateMachine  # noqa: E402


AAPL_ORDER_ID = "33121850-ce16-43c0-a1e0-964c8bc608f1"
CSCO_ORDER_ID = "386a608d-6282-4a08-a97f-2a6413f93064"
CLIENT = "jasoncosby1@gmail.com"


def _row(*, order_id=AAPL_ORDER_ID, contract="AAPL260902P00312500", owner=None, mode="live"):
    key = order_id
    now = datetime.now(timezone.utc) - timedelta(seconds=30)
    return {
        "local_order_id": order_id,
        "client_id": CLIENT,
        "execution_mode": mode,
        "signal_id": f"sig-{order_id}",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "symbol": "AAPL" if contract.startswith("AAPL") else "CSCO",
        "direction": "PUT",
        "contract": contract,
        "qty": 1,
        "limit_price": 1.31 if contract.startswith("AAPL") else 1.59,
        "reserved_cost": 131.0 if contract.startswith("AAPL") else 159.0,
        "meta": {
            "execution_mode": mode,
            "lifecycle_state": "SUBMITTING",
            "materialization_status": "SELECTED",
            "broker_ready": True,
            "materialization_generation": 1,
            "selected_contract": contract,
            "selected_qty": 1,
            "submit_intent_at": now.isoformat(),
            "broker_submit_key": key,
            "broker_submit_payload_hash": "deadbeef" * 8,
            "current_owner": owner or f"broker_submit:{key}",
        },
    }


def _subject(mode="live"):
    return types.SimpleNamespace(client_id=CLIENT, email=CLIENT, execution_mode=mode, mode=mode.upper())


def test_guard_is_mandatory_and_installed_on_package_import():
    assert getattr(TradierBroker.list_orders, "_ap_exact_tag_query", False) is True
    assert getattr(
        ap_execution_core.APExecutionCore.reconcile_deferred_broker_intent,
        "_ap_live_submit_liveness",
        False,
    ) is True
    assert getattr(
        ap_execution_core.APExecutionCore._on_entry_trigger,
        "_ap_submit_intent_router",
        False,
    ) is True
    assert getattr(
        APOrderStateMachine.retain_recovery_ownership_if_no_watcher,
        "_ap_broker_submit_owner_retained",
        False,
    ) is True


@pytest.mark.parametrize(
    "row",
    [
        _row(order_id=AAPL_ORDER_ID, contract="AAPL260902P00312500"),
        _row(order_id=CSCO_ORDER_ID, contract="CSCO260904P00110000"),
    ],
)
def test_exact_jason_production_shapes_are_recognized(row):
    identity, reason = _exact_live_submit_identity(_subject(), row)
    assert reason == "ok"
    assert identity is not None
    assert identity["broker_submit_key"] == row["local_order_id"]
    assert identity["current_owner"] == f"broker_submit:{row['local_order_id']}"


def test_paper_or_different_owner_cannot_enter_live_absence_release():
    identity, reason = _exact_live_submit_identity(_subject("paper"), _row(mode="paper"))
    assert identity is None
    assert reason == "not_live"

    bad = _row(owner="recovery_scheduler:someone")
    identity, reason = _exact_live_submit_identity(_subject(), bad)
    assert identity is None
    assert reason == "broker_submit_owner_mismatch"


def test_tradier_reconciliation_query_requests_tags_and_full_page():
    broker = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com",
        access_token="token",
        account_id="acct",
    ))
    broker._get = MagicMock(return_value={
        "orders": {
            "order": {
                "id": "TR-1",
                "tag": AAPL_ORDER_ID,
                "option_symbol": "AAPL260902P00312500",
            }
        }
    })
    result = broker.list_orders()
    assert result[0]["tag"] == AAPL_ORDER_ID
    params = broker._get.call_args.kwargs["params"]
    assert params["includeTags"] == "true"
    assert params["limit"] == 1500
    assert params["page"] == 1


def test_tradier_null_sentinel_is_authoritative_empty_but_other_scalar_fails():
    broker = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com",
        access_token="token",
        account_id="acct",
    ))
    broker._get = MagicMock(return_value={"orders": {"order": "null"}})
    assert broker.list_orders() == []

    broker._get = MagicMock(return_value={"orders": {"order": "garbage"}})
    with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
        broker.list_orders()


def test_existing_exact_broker_submit_owner_counts_as_recovery_retained():
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT
    osm.execution_mode = "live"
    osm.get_order = MagicMock(return_value=_row())

    original = getattr(
        APOrderStateMachine.retain_recovery_ownership_if_no_watcher,
        "_ap_original",
    )
    original_mock = MagicMock(return_value=False)
    APOrderStateMachine.retain_recovery_ownership_if_no_watcher._ap_original = original_mock
    try:
        ok = osm.retain_recovery_ownership_if_no_watcher(
            AAPL_ORDER_ID,
            recovery_owner=f"recovery_scheduler:{CLIENT}",
            reason="RECONCILE_BROKER_QUERY_FAILED:ValueError",
            recovery_retention_mode="live",
        )
        assert ok is True
        original_mock.assert_not_called()
    finally:
        APOrderStateMachine.retain_recovery_ownership_if_no_watcher._ap_original = original


class _Wrapper:
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


def _require_postgres():
    database_url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
    if not database_url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("disposable PostgreSQL URL not configured")
    psycopg2 = pytest.importorskip("psycopg2")
    return psycopg2, database_url


@contextmanager
def _isolated_schema(monkeypatch):
    psycopg2, database_url = _require_postgres()
    import psycopg2.extras
    import ap.db as ap_db

    schema = f"broker_submit_reconcile_{uuid.uuid4().hex}"

    @contextmanager
    def _pg_conn():
        db = psycopg2.connect(database_url)
        cur = db.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        try:
            cur.execute(f'SET search_path TO "{schema}"')
            yield _Wrapper(cur)
            db.commit()
        except Exception:
            db.rollback()
            raise
        finally:
            cur.close()
            db.close()

    admin = psycopg2.connect(database_url)
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(f'CREATE SCHEMA "{schema}"')
            cur.execute(
                f"""
                CREATE TABLE "{schema}".orders (
                    local_order_id TEXT PRIMARY KEY,
                    client_id TEXT NOT NULL,
                    kind TEXT NOT NULL,
                    status TEXT NOT NULL,
                    execution_mode TEXT,
                    signal_id TEXT,
                    broker_order_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    contract TEXT,
                    qty INTEGER,
                    limit_price NUMERIC,
                    meta JSONB,
                    updated_ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
                )
                """
            )
        monkeypatch.setattr(ap_db, "conn", _pg_conn)
        monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **k: fn())
        yield _pg_conn
    finally:
        with admin.cursor() as cur:
            cur.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        admin.close()


def _insert(pg_conn, row):
    with pg_conn() as c:
        c.execute(
            """
            INSERT INTO orders (
                local_order_id, client_id, kind, status, execution_mode,
                signal_id, broker_order_id, submitted_ts, contract, qty,
                limit_price, meta
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
            """,
            (
                row["local_order_id"], row["client_id"], row["kind"], row["status"],
                row["execution_mode"], row["signal_id"], row["broker_order_id"],
                row["submitted_ts"], row["contract"], row["qty"], row["limit_price"],
                json.dumps(row["meta"]),
            ),
        )


def _read(pg_conn, order_id):
    with pg_conn() as c:
        return c.execute(
            "SELECT * FROM orders WHERE local_order_id=%s", (order_id,)
        ).fetchone()


def test_real_postgres_two_no_match_proofs_release_only_exact_submit_owner(monkeypatch):
    with _isolated_schema(monkeypatch) as pg_conn:
        row = _row()
        _insert(pg_conn, row)
        identity, reason = _exact_live_submit_identity(_subject(), row)
        assert reason == "ok"
        first = datetime.now(timezone.utc).isoformat()
        assert _record_first_no_match(None, AAPL_ORDER_ID, identity, first) is True

        assert _release_after_proven_absence(
            None,
            AAPL_ORDER_ID,
            identity,
            first_no_match_at=first,
            proven_at=datetime.now(timezone.utc).isoformat(),
        ) is True
        after = _read(pg_conn, AAPL_ORDER_ID)
        meta = after["meta"]
        assert meta["lifecycle_state"] == "BROKER_READY"
        assert meta["broker_ready"] is True
        assert meta["submit_intent_at"] == ""
        assert meta["broker_submit_key"] == ""
        assert meta["broker_submit_payload_hash"] == ""
        assert meta["current_owner"] == ""
        assert meta["broker_reconcile_resume_authorized"] is True


def test_real_postgres_release_cas_loses_if_broker_owner_changes(monkeypatch):
    with _isolated_schema(monkeypatch) as pg_conn:
        row = _row()
        _insert(pg_conn, row)
        identity, _ = _exact_live_submit_identity(_subject(), row)
        first = datetime.now(timezone.utc).isoformat()
        assert _record_first_no_match(None, AAPL_ORDER_ID, identity, first) is True
        with pg_conn() as c:
            c.execute(
                "UPDATE orders SET meta = meta || %s::jsonb WHERE local_order_id=%s",
                (json.dumps({"current_owner": "someone_else"}), AAPL_ORDER_ID),
            )
        assert _release_after_proven_absence(
            None,
            AAPL_ORDER_ID,
            identity,
            first_no_match_at=first,
            proven_at=datetime.now(timezone.utc).isoformat(),
        ) is False
        assert _read(pg_conn, AAPL_ORDER_ID)["meta"]["lifecycle_state"] == "SUBMITTING"
