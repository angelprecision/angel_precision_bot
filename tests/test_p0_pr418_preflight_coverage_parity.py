"""Focused closure for PR #418 preflight coverage and timestamp parity."""
from __future__ import annotations

import json
import os
from datetime import timezone

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable",
)

import psycopg2
import pytest

import ap.selector_recovery_deploy_preflight as preflight_module
from ap.selector_recovery_deploy_preflight import (
    _classify_row,
    _fetch_candidate_rows,
    _parse_timestamp,
)

_PREFIX = "preflight-pr418-followup-"
_SIGNAL_ID = "sig-pr418-followup"
_CLIENT_ID = "preflight-pr418@example.com"


def _pg_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


@pytest.fixture(scope="module", autouse=True)
def _ensure_orders_table_and_cleanup():
    with _pg_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS orders (
                    local_order_id TEXT PRIMARY KEY,
                    broker_order_id TEXT,
                    client_id TEXT,
                    position_id TEXT,
                    kind TEXT,
                    status TEXT,
                    meta JSONB DEFAULT '{}'::jsonb,
                    created_ts TIMESTAMPTZ DEFAULT NOW(),
                    updated_ts TIMESTAMPTZ DEFAULT NOW(),
                    direction TEXT,
                    contract TEXT,
                    qty INTEGER,
                    filled_qty INTEGER,
                    fill_price NUMERIC,
                    signal_id TEXT,
                    execution_mode TEXT,
                    canonical_signal_id TEXT,
                    submitted_ts TIMESTAMPTZ,
                    filled_ts TIMESTAMPTZ,
                    last_error TEXT
                )
                """
            )
            cursor.execute(
                "DELETE FROM orders WHERE local_order_id LIKE %s",
                (f"{_PREFIX}%",),
            )
        connection.commit()
    yield
    with _pg_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM orders WHERE local_order_id LIKE %s",
                (f"{_PREFIX}%",),
            )
        connection.commit()


def _provenance(local_order_id: str) -> dict:
    return {
        "canonical_signal_id": _SIGNAL_ID,
        "client_id": _CLIENT_ID,
        "execution_mode": "paper",
        "local_order_id": local_order_id,
    }


def _row(local_order_id: str, meta: dict) -> dict:
    return {
        "local_order_id": local_order_id,
        "client_id": _CLIENT_ID,
        "execution_mode": "paper",
        "signal_id": _SIGNAL_ID,
        "canonical_signal_id": _SIGNAL_ID,
        "status": "PENDING_TRIGGER",
        "meta": dict(meta),
        "updated_ts": None,
    }


def _insert(local_order_id: str, meta: dict) -> None:
    with _pg_conn() as connection:
        with connection.cursor() as cursor:
            cursor.execute(
                "DELETE FROM orders WHERE local_order_id = %s",
                (local_order_id,),
            )
            cursor.execute(
                """
                INSERT INTO orders (
                    local_order_id,
                    client_id,
                    kind,
                    status,
                    signal_id,
                    canonical_signal_id,
                    execution_mode,
                    meta
                )
                VALUES (%s, %s, 'ENTRY', 'PENDING_TRIGGER', %s, %s, 'paper', %s::jsonb)
                """,
                (
                    local_order_id,
                    _CLIENT_ID,
                    _SIGNAL_ID,
                    _SIGNAL_ID,
                    json.dumps(meta),
                ),
            )
        connection.commit()


def _fetch_one(local_order_id: str) -> dict:
    rows = _fetch_candidate_rows()
    matching = [row for row in rows if row["local_order_id"] == local_order_id]
    assert len(matching) == 1
    return matching[0]


def test_provenance_only_row_is_fetched_and_rejected():
    local_order_id = f"{_PREFIX}provenance-only"
    _insert(
        local_order_id,
        {"trigger_crossed_at_provenance": _provenance(local_order_id)},
    )

    result = _classify_row(_fetch_one(local_order_id))

    assert result["safe"] is False
    assert "TRIGGER_CROSSED_TIMESTAMP_MISSING" in result["findings"]


def test_timestamp_only_row_is_fetched_and_rejected():
    local_order_id = f"{_PREFIX}timestamp-only"
    _insert(local_order_id, {"trigger_crossed_at": "2026-08-06T20:00:00+00:00"})

    result = _classify_row(_fetch_one(local_order_id))

    assert result["safe"] is False
    assert "TRIGGER_PROVENANCE_MISSING" in result["findings"]


def test_both_trigger_claim_keys_absent_remains_outside_candidate_query():
    local_order_id = f"{_PREFIX}ordinary"
    _insert(local_order_id, {"unrelated_field": "ordinary-pending-trigger"})

    rows = _fetch_candidate_rows()

    assert all(row["local_order_id"] != local_order_id for row in rows)


def test_timezone_naive_trigger_timestamp_is_malformed():
    local_order_id = f"{_PREFIX}naive"
    result = _classify_row(
        _row(
            local_order_id,
            {
                "trigger_crossed_at": "2026-08-06T20:00:00",
                "trigger_crossed_at_provenance": _provenance(local_order_id),
            },
        )
    )

    assert result["safe"] is False
    assert "TRIGGER_CROSSED_TIMESTAMP_MALFORMED" in result["findings"]


def test_timezone_aware_trigger_timestamp_remains_valid():
    local_order_id = f"{_PREFIX}aware"
    result = _classify_row(
        _row(
            local_order_id,
            {
                "trigger_crossed_at": "2026-08-06T20:00:00+00:00",
                "trigger_crossed_at_provenance": _provenance(local_order_id),
            },
        )
    )

    assert result["safe"] is True
    assert result["findings"] == []


@pytest.mark.parametrize(
    ("finding", "raw"),
    [
        ("LEASE_TIMESTAMP_MALFORMED", "2026-08-06T20:00:00"),
        ("RETRY_TIMESTAMP_MALFORMED", "2026-08-06T20:05:00"),
    ],
)
def test_naive_non_trigger_timestamps_preserve_legacy_utc_normalization(
    finding,
    raw,
):
    findings: list[str] = []

    parsed = _parse_timestamp(
        raw,
        finding=finding,
        findings=findings,
    )

    assert parsed is not None
    assert parsed.tzinfo is timezone.utc
    assert findings == []


def test_candidate_fetch_executes_select_only(monkeypatch):
    class _Result:
        @staticmethod
        def fetchall():
            return []

    class _Connection:
        query = ""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, traceback):
            return False

        def execute(self, query):
            self.query = query
            normalized = " ".join(query.upper().split())
            assert normalized.startswith("SELECT ")
            for forbidden in (" INSERT ", " UPDATE ", " DELETE ", " MERGE "):
                assert forbidden not in f" {normalized} "
            assert "TRIGGER_CROSSED_AT" in normalized
            assert "TRIGGER_CROSSED_AT_PROVENANCE" in normalized
            return _Result()

    connection = _Connection()
    monkeypatch.setattr(preflight_module, "conn", lambda: connection)

    assert _fetch_candidate_rows() == []
    assert connection.query
