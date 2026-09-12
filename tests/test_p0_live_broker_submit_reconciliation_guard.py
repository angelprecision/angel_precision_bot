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

import ap  # noqa: E402 - installs mandatory guard
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


def _row(
    *,
    order_id=AAPL_ORDER_ID,
    contract="AAPL260902P00312500",
    owner=None,
    mode="live",
    first_no_match_at="",
):
    key = order_id
    submit_at = datetime.now(timezone.utc) - timedelta(seconds=30)
    materialization_owner = f"watcher:{order_id}"
    meta = {
        "execution_mode": mode,
        "lifecycle_state": "SUBMITTING",
        "materialization_status": "SELECTED",
        "materialization_in_flight": False,
        "broker_ready": True,
        "materialization_generation": 1,
        "materialization_owner": materialization_owner,
        "selected_contract": contract,
        "selected_qty": 1,
        "submit_intent_at": submit_at.isoformat(),
        "broker_submit_key": key,
        "broker_submit_payload_hash": "deadbeef" * 8,
        "current_owner": owner or f"broker_submit:{key}",
    }
    if first_no_match_at:
        meta.update({
            "broker_reconcile_no_match_observed_at": first_no_match_at,
            "broker_reconcile_no_match_submit_key": key,
            "broker_reconcile_no_match_payload_hash": "deadbeef" * 8,
            "broker_reconcile_no_match_generation": 1,
        })
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
        "meta": meta,
    }


def _subject(mode="live"):
    return types.SimpleNamespace(
        client_id=CLIENT,
        execution_mode=mode,
        mode=mode.upper(),
    )


def _osm_with_row(row):
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = CLIENT
    osm.execution_mode = "live"
    osm.get_order = MagicMock(return_value=row)
    osm._get_order = osm.get_order
    osm.transition = MagicMock(return_value=True)
    osm.update_order_meta = MagicMock(return_value=True)
    return osm


def test_guard_is_mandatory_and_installed_on_package_import():
    assert getattr(TradierBroker.list_orders, "_ap_exact_tag_query", False)
    assert getattr(
        APOrderStateMachine.submit_existing_entry,
        "_ap_broker_intent_reconcile",
        False,
    )
    assert getattr(
        APOrderStateMachine.retain_recovery_ownership_if_no_watcher,
        "_ap_broker_submit_owner_retained",
        False,
    )


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


def test_paper_and_wrong_owner_fail_closed():
    identity, reason = _exact_live_submit_identity(_subject("paper"), _row(mode="paper"))
    assert identity is None and reason == "not_live"

    identity, reason = _exact_live_submit_identity(
        _subject(), _row(owner="recovery_scheduler:someone")
    )
    assert identity is None and reason == "broker_submit_owner_mismatch"


def test_stale_no_match_proof_cannot_bind_a_new_payload_hash():
    first = (datetime.now(timezone.utc) - timedelta(seconds=20)).isoformat()
    row = _row(first_no_match_at=first)
    row["meta"]["broker_submit_payload_hash"] = "new-payload-hash"
    identity, reason = _exact_live_submit_identity(_subject(), row)
    assert reason == "ok"
    assert identity["first_no_match_at"] == ""


def test_tradier_reconciliation_query_requests_tags_and_full_page():
    broker = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com",
        access_token="token",
        account_id="acct",
    ))
    broker._get = MagicMock(return_value={
        "orders": {"order": {
            "id": "TR-1",
            "tag": AAPL_ORDER_ID,
            "option_symbol": "AAPL260902P00312500",
        }}
    })
    assert broker.list_orders()[0]["tag"] == AAPL_ORDER_ID
    assert broker._get.call_args.kwargs["params"] == {
        "includeTags": "true", "limit": 1500, "page": 1,
    }


def test_tradier_empty_and_malformed_shapes_are_not_conflated():
    broker = TradierBroker(TradierConfig(
        base_url="https://api.tradier.com",
        access_token="token",
        account_id="acct",
    ))
    broker._get = MagicMock(return_value={"orders": {"order": "null"}})
    assert broker.list_orders() == []

    for payload in ({}, [], {"orders": {"order": "garbage"}}):
        broker._get = MagicMock(return_value=payload)
        with pytest.raises(ValueError, match="TRADIER_ORDERS_PAYLOAD_MALFORMED"):
            broker.list_orders()


def test_found_exact_tag_is_adopted_without_new_broker_post():
    row = _row()
    osm = _osm_with_row(row)
    broker = MagicMock()
    broker.list_orders.return_value = [{
        "id": "TR-991",
        "tag": AAPL_ORDER_ID,
        "option_symbol": "AAPL260902P00312500",
        "side": "buy_to_open",
        "quantity": "1",
        "status": "open",
    }]
    broker.place_order = MagicMock()

    result = osm.submit_existing_entry(
        local_order_id=AAPL_ORDER_ID,
        broker=broker,
        plan=None,
        limit_price=row["limit_price"],
    )

    assert result["ok"] is True
    assert result["broker_order_id"] == "TR-991"
    broker.place_order.assert_not_called()
    osm.transition.assert_called_once()


def test_query_error_keeps_submit_intent_and_never_posts():
    row = _row()
    osm = _osm_with_row(row)
    broker = MagicMock()
    broker.list_orders.side_effect = ValueError("TRADIER_ORDERS_PAYLOAD_MALFORMED:root")
    broker.place_order = MagicMock()

    result = osm.submit_existing_entry(
        local_order_id=AAPL_ORDER_ID,
        broker=broker,
        plan=None,
        limit_price=row["limit_price"],
    )

    assert result["ok"] is False
    assert result["reconciliation_required"] is True
    assert "RECONCILE_BROKER_QUERY_FAILED:ValueError" in result["error"]
    broker.place_order.assert_not_called()
    osm.transition.assert_not_called()


def test_exact_broker_submit_owner_counts_as_recovery_retained():
    osm = _osm_with_row(_row())
    assert osm.retain_recovery_ownership_if_no_watcher(
        AAPL_ORDER_ID,
        recovery_owner=f"recovery_scheduler:{CLIENT}",
        reason="RECONCILE_BROKER_QUERY_FAILED:ValueError",
        recovery_retention_mode="live",
    ) is True


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
            cur.execute(f"""
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
            """)
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


def test_real_postgres_two_no_match_proofs_restore_exact_broker_ready_owner(monkeypatch):
    with _isolated_schema(monkeypatch) as pg_conn:
        row = _row()
        _insert(pg_conn, row)
        identity, reason = _exact_live_submit_identity(_subject(), row)
        assert reason == "ok"

        first = (datetime.now(timezone.utc) - timedelta(seconds=5)).isoformat()
        assert _record_first_no_match(AAPL_ORDER_ID, identity, first) is True

        persisted = _read(pg_conn, AAPL_ORDER_ID)
        identity, reason = _exact_live_submit_identity(_subject(), persisted)
        assert reason == "ok"
        assert identity["first_no_match_at"] == first

        assert _release_after_proven_absence(
            AAPL_ORDER_ID,
            identity,
            first_no_match_at=first,
            proven_at=datetime.now(timezone.utc).isoformat(),
        ) is True

        after = _read(pg_conn, AAPL_ORDER_ID)
        meta = after["meta"]
        assert meta["lifecycle_state"] == "BROKER_READY"
        assert meta["broker_ready"] is True
        assert meta["current_owner"] == row["meta"]["materialization_owner"]
        assert meta["submit_intent_at"] == ""
        assert meta["broker_submit_key"] == ""
        assert meta["broker_submit_payload_hash"] == ""
        assert meta["broker_reconcile_resume_authorized"] is True


def test_real_postgres_release_cas_loses_if_broker_owner_changes(monkeypatch):
    with _isolated_schema(monkeypatch) as pg_conn:
        row = _row()
        _insert(pg_conn, row)
        identity, _ = _exact_live_submit_identity(_subject(), row)
        first = datetime.now(timezone.utc).isoformat()
        assert _record_first_no_match(AAPL_ORDER_ID, identity, first) is True
        with pg_conn() as c:
            c.execute(
                "UPDATE orders SET meta = meta || %s::jsonb WHERE local_order_id=%s",
                (json.dumps({"current_owner": "someone_else"}), AAPL_ORDER_ID),
            )
        persisted = _read(pg_conn, AAPL_ORDER_ID)
        identity["first_no_match_at"] = first
        assert _release_after_proven_absence(
            AAPL_ORDER_ID,
            identity,
            first_no_match_at=first,
            proven_at=datetime.now(timezone.utc).isoformat(),
        ) is False
        assert persisted["meta"]["lifecycle_state"] == "SUBMITTING"
