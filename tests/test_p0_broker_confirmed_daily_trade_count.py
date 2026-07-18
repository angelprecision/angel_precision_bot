from __future__ import annotations

import os
import inspect
import re
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest

from ap.position_manager import APPositionManager, _broker_confirmed_entry_trades_today
from ap_master_control import APMasterControl


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


def _count(database, *, client="jose@example.com", mode="live"):
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
    mode="live",
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
    _order(database, "live-current", broker_id="live-current")
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


def _signal(**overrides):
    signal = {
        "signal_id": "trade-count-signal",
        "canonical_signal_id": "trade-count-signal",
        "client_id": "jose@example.com",
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "pattern": "2-3",
        "score": 95.0,
        "entry_trigger": 200.0,
        "target_price": 205.0,
        "stop_price": 197.5,
    }
    signal.update(overrides)
    return signal


def _snapshot(**overrides):
    snapshot = {
        "_snapshot_ok": True,
        "_snapshot_ts": "2026-07-17T14:00:00Z",
        "_snapshot_age_sec": 0.0,
        "open_count": 0,
        "open_positions": [],
        "closing_positions": [],
        "calls_open": 0,
        "puts_open": 0,
        "filled_unreconciled_calls": 0,
        "filled_unreconciled_puts": 0,
        "pending_entries": 0,
        "pending_entry_capital": 0.0,
        "capital_deployed": 0.0,
        "realized_pnl_today": 0.0,
        "trades_today": 0,
        "total_trades": 0,
        "watcher_count": 0,
        "entry_attempt_lock_count": 0,
        "ticker_open_counts": {},
        "ticker_pending_counts": {},
        "symbol_trades": {},
    }
    snapshot.update(overrides)
    return snapshot


def _master_for_snapshot(snapshot: dict, *, max_trades_today: int = 10):
    with patch.object(APMasterControl, "_seed_dedup_from_db", return_value=None):
        control = APMasterControl(
            mode="paper",
            client_id="jose@example.com",
            score_floor=60.0,
            context_floor=0.0,
            account_equity=100_000.0,
            max_capital_pct=0.40,
            max_position_pct=0.40,
            max_total_capital_pct=0.90,
            max_sector_pct=1.0,
            max_ticker_pct=1.0,
            max_positions=20,
            max_calls=20,
            max_puts=20,
            max_trades_today=max_trades_today,
            require_snapshot_freshness_live=False,
            pending_capital_fail_closed_live=False,
        )
    control._get_snapshot = MagicMock(return_value=snapshot)
    control._has_durable_duplicate_signal = MagicMock(return_value=(False, "", ""))
    control._pending_capital_from_snapshot_or_db = MagicMock(return_value=0.0)
    control._sector_capital_deployed = MagicMock(return_value=0.0)
    control._ticker_capital_deployed = MagicMock(return_value=0.0)
    control._run_intelligence = MagicMock(return_value={
        "approved": True,
        "score": 0.0,
        "contracts": 1,
        "reasoning": "observe-only unavailable",
        "_available": False,
    })
    control._run_final_quality_gates = MagicMock(return_value=None)
    control._persist_dedup = MagicMock(return_value=None)
    control._emit_trade_dossier = MagicMock()
    control._log_capital_utilization = MagicMock()
    control._alert_degraded = MagicMock()
    control._store_update = MagicMock()
    control.feedback = None
    control.sizer = None
    control.pm = None
    return control


def test_legacy_snapshot_trade_count_without_proven_status_blocks():
    control = _master_for_snapshot(_snapshot(trades_today=1))

    decision = control.evaluate(_signal(), client_id="jose@example.com")

    assert decision.ok is False
    assert decision.stage == "blocked_risk"
    assert decision.reason == "broker_confirmed_entry_trade_count_unavailable"


def test_explicit_ok_snapshot_reaches_max_trade_comparison():
    control = _master_for_snapshot(
        _snapshot(
            trades_today=2,
            trades_today_source="broker_confirmed_entry_orders",
            trade_count_query_status="ok",
        ),
        max_trades_today=2,
    )

    decision = control.evaluate(_signal(), client_id="jose@example.com")

    assert decision.ok is False
    assert decision.stage == "blocked_risk"
    assert decision.reason == "max_trades_today (2/2)"


def _jose_runbook() -> str:
    return (Path(__file__).resolve().parents[1] / "docs/p0/jose_daily_trade_count_repair.md").read_text()


def _sql_statements(text: str) -> list[str]:
    return re.findall(r"```sql\n(.*?)```", text, flags=re.IGNORECASE | re.DOTALL)


def test_jose_runbook_recomputes_paper_orders_only():
    text = _jose_runbook()

    assert "AND lower(coalesce(execution_mode, '')) = 'paper'" in text
    assert "AND lower(coalesce(execution_mode, '')) = 'live'" not in text


def test_jose_runbook_client_state_statements_are_client_and_mode_fenced():
    text = _jose_runbook()
    client_state_statements = [
        statement
        for block in _sql_statements(text)
        for statement in block.split(";")
        if re.search(r"\b(?:FROM|UPDATE)\s+client_state\b", statement, flags=re.IGNORECASE)
    ]

    assert client_state_statements
    for statement in client_state_statements:
        normalized = re.sub(r"\s+", " ", statement.lower())
        assert "client_id = 'jose.vasquez4011@gmail.com'" in normalized
        assert "lower(mode) = 'paper'" in normalized


def test_jose_runbook_has_no_client_only_update_and_returns_exact_row():
    text = _jose_runbook()
    updates = [
        statement
        for block in _sql_statements(text)
        for statement in block.split(";")
        if re.search(r"\bUPDATE\s+client_state\b", statement, flags=re.IGNORECASE)
    ]

    assert len(updates) == 1
    update = re.sub(r"\s+", " ", updates[0].lower())
    assert "where client_id = 'jose.vasquez4011@gmail.com' and lower(mode) = 'paper'" in update
    assert "returning client_id, mode, day_key, trades_taken_today, updated_at" in update
    assert "exactly one row" in text
    assert "rollback" in text.lower()


def test_jose_runbook_mutation_predicates_exclude_jason_and_tradefluence():
    text = _jose_runbook()
    mutation_statements = [
        statement.lower()
        for block in _sql_statements(text)
        for statement in block.split(";")
        if re.search(r"\bUPDATE\s+client_state\b", statement, flags=re.IGNORECASE)
    ]

    assert mutation_statements
    assert all("jason" not in statement for statement in mutation_statements)
    assert all("tradefluence" not in statement for statement in mutation_statements)
