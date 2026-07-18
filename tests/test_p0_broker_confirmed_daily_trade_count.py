from __future__ import annotations

import os
import inspect
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from ap.position_manager import APPositionManager, _broker_confirmed_entry_trades_today


START = datetime(2026, 7, 17, 4, 0, tzinfo=timezone.utc)
END = datetime(2026, 7, 18, 4, 0, tzinfo=timezone.utc)


@pytest.fixture()
def database():
    psycopg2 = pytest.importorskip("psycopg2")
    extras = pytest.importorskip("psycopg2.extras")
    url = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL")
    if not url:
        if os.getenv("GITHUB_ACTIONS") == "true":
            pytest.fail("INTELLIGENCE_POSTGRES_TEST_URL is required in GitHub Actions")
        pytest.skip("PostgreSQL integration URL unavailable")
    connection = psycopg2.connect(url)
    connection.autocommit = True
    schema = f"pr2_trade_count_{uuid4().hex}"
    with connection.cursor() as cursor:
        cursor.execute(f'CREATE SCHEMA "{schema}"')
        cursor.execute(f'SET search_path TO "{schema}"')
        cursor.execute(
            """
            CREATE TABLE orders (
                id TEXT PRIMARY KEY,
                client_id TEXT,
                execution_mode TEXT,
                kind TEXT,
                status TEXT,
                filled_qty INTEGER,
                fill_price NUMERIC,
                filled_ts TIMESTAMPTZ,
                broker_order_id TEXT,
                local_order_id TEXT,
                contract TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE positions (
                id TEXT PRIMARY KEY,
                client_id TEXT,
                execution_mode TEXT,
                plan_id TEXT,
                contract TEXT,
                status TEXT,
                close_source TEXT,
                entry_ts TIMESTAMPTZ,
                local_order_id TEXT,
                broker_order_id TEXT
            )
            """
        )
    try:
        yield connection, extras.RealDictCursor
    finally:
        with connection.cursor() as cursor:
            cursor.execute("SET search_path TO public")
            cursor.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
        connection.close()


def _count(database, *, client="jose@example.com", mode="paper"):
    connection, cursor_factory = database
    with connection.cursor(cursor_factory=cursor_factory) as cursor:
        return _broker_confirmed_entry_trades_today(
            cursor,
            client_id=client,
            execution_mode=mode,
            start_utc=START,
            end_utc=END,
        )


def _order(
    database,
    identity: str,
    *,
    client="jose@example.com",
    mode="paper",
    kind="ENTRY",
    status="FILLED",
    qty=1,
    filled_ts=datetime(2026, 7, 17, 14, 30, tzinfo=timezone.utc),
    broker_id="broker-1",
    local_id="local-1",
    contract="SPY260717C00600000",
    fill_price=1.25,
):
    connection, _ = database
    with connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO orders "
            "(id,client_id,execution_mode,kind,status,filled_qty,fill_price,filled_ts,"
            "broker_order_id,local_order_id,contract) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                identity, client, mode, kind, status, qty, fill_price, filled_ts,
                broker_id, local_id, contract,
            ),
        )


def test_378_synthetic_expired_positions_and_no_fills_equals_zero(database):
    connection, _ = database
    with connection.cursor() as cursor:
        cursor.executemany(
            "INSERT INTO positions "
            "(id,client_id,execution_mode,plan_id,contract,status,close_source,entry_ts) "
            "VALUES (%s,'jose@example.com','live',%s,'SPY260716P00751000','EXPIRED',"
            "'expired_contract_cleanup',%s)",
            [
                (f"position-{index}", f"reconciled:SPY:{index}", START)
                for index in range(378)
            ],
        )
    result = _count(database)
    assert result["trades_today"] == 0
    assert result["synthetic_position_rows_ignored"] == 378
    assert result["trades_today_source"] == "broker_confirmed_entry_orders"
    assert result["trade_count_query_status"] == "ok"


def test_duplicate_callbacks_entry_exit_and_multicontract_count_one(database):
    _order(database, "entry-1", broker_id="broker-economic-1", local_id="callback-1")
    _order(
        database,
        "entry-duplicate-callback",
        broker_id="broker-economic-1",
        local_id="callback-2",
        contract="QQQ260717C00500000",
    )
    _order(
        database,
        "entry-third-contract",
        broker_id="broker-economic-1",
        local_id="callback-3",
        contract="IWM260717C00200000",
    )
    _order(
        database,
        "exit-1",
        kind="EXIT",
        status="EXIT_FILLED",
        broker_id="broker-exit-1",
        local_id="exit-local-1",
    )
    assert _count(database)["trades_today"] == 1


def test_client_mode_session_and_unfilled_isolation(database):
    _order(database, "live-current", mode="live", broker_id="live-current")
    _order(database, "paper-current", mode="paper", broker_id="paper-current")
    _order(
        database,
        "other-client",
        client="jason@example.com",
        broker_id="other-client",
    )
    _order(
        database,
        "yesterday",
        broker_id="yesterday",
        filled_ts=datetime(2026, 7, 16, 14, 30, tzinfo=timezone.utc),
    )
    _order(
        database,
        "canceled-zero",
        status="CANCELED",
        qty=0,
        broker_id="canceled-zero",
        fill_price=9.99,
    )
    live = _count(database, mode="live")
    paper = _count(database, mode="paper")
    assert live["trades_today"] == 1
    assert live["wrong_mode_fills_ignored"] == 1
    assert paper["trades_today"] == 1


def test_exact_filled_local_entry_fallback_counts_once(database):
    _order(database, "recovered-1", broker_id="", local_id="local-recovered")
    _order(
        database,
        "missing-identity",
        broker_id="",
        local_id="",
    )
    result = _count(database)
    assert result["trades_today"] == 1
    assert result["missing_identity_fills_ignored"] == 1


def test_null_mode_is_visible_and_never_counts(database):
    _order(database, "null-mode", mode=None, broker_id="null-mode")
    result = _count(database)
    assert result["trades_today"] == 0
    assert result["null_mode_fills_ignored"] == 1


def test_invalid_requested_mode_fails_closed(database):
    connection, cursor_factory = database
    with connection.cursor(cursor_factory=cursor_factory) as cursor:
        with pytest.raises(ValueError, match="invalid_trade_count_execution_mode"):
            _broker_confirmed_entry_trades_today(
                cursor,
                client_id="jose@example.com",
                execution_mode="",
                start_utc=START,
                end_utc=END,
            )


def test_real_fill_threshold_changes_only_at_configured_limit(database):
    for index in range(19):
        _order(
            database,
            f"threshold-{index}",
            broker_id=f"threshold-broker-{index}",
            local_id=f"threshold-local-{index}",
        )
    assert _count(database)["trades_today"] == 19
    assert _count(database)["trades_today"] < 20
    _order(
        database,
        "threshold-20",
        broker_id="threshold-broker-20",
        local_id="threshold-local-20",
    )
    assert _count(database)["trades_today"] == 20
    assert _count(database)["trades_today"] >= 20


def test_snapshot_wires_trade_limit_to_broker_confirmed_query():
    source = inspect.getsource(APPositionManager.snapshot)
    assert "_broker_confirmed_entry_trades_today(" in source
    assert '"trades_today":       int(trade_count["trades_today"])' in source
    assert "broker_confirmed_entry_trade_count_unavailable" in source


def test_jose_paper_production_shape_counts_paper_fills_only(database):
    """Explicit production-shaped Jose PAPER test.

    Jose is a PAPER client.  The helper defaults (mode='paper') now reflect
    that shape.  This test also inserts a LIVE order under the same client to
    confirm the PAPER count is never contaminated by LIVE rows — the exact
    failure mode that allowed the runbook's 'live' predicate to survive CI
    when the helper previously defaulted to 'live'.
    """
    # Two PAPER ENTRY fills for Jose — distinct broker identities.
    _order(database, "jose-paper-fill-1", broker_id="jose-paper-broker-1", local_id="jose-paper-local-1")
    _order(database, "jose-paper-fill-2", broker_id="jose-paper-broker-2", local_id="jose-paper-local-2",
           contract="QQQ260717C00500000")

    # One LIVE ENTRY fill under the same client — must never appear in the PAPER count.
    _order(database, "jose-live-fill-1", mode="live", broker_id="jose-live-broker-1",
           local_id="jose-live-local-1")

    paper_result = _count(database, mode="paper")
    live_result  = _count(database, mode="live")

    # PAPER count sees only the two PAPER fills.
    assert paper_result["trades_today"] == 2, (
        f"Jose PAPER count must be 2; got {paper_result}"
    )
    # PAPER count must report the stray LIVE order as wrong-mode.
    assert paper_result["wrong_mode_fills_ignored"] == 1, (
        f"Jose PAPER count must ignore 1 LIVE fill; got {paper_result}"
    )

    # LIVE count sees only the one LIVE fill.
    assert live_result["trades_today"] == 1, (
        f"Jose LIVE count must be 1; got {live_result}"
    )
    # LIVE count must report both PAPER orders as wrong-mode.
    assert live_result["wrong_mode_fills_ignored"] == 2, (
        f"Jose LIVE count must ignore 2 PAPER fills; got {live_result}"
    )
