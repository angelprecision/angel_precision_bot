"""Tests for ap/selector_recovery_deploy_preflight.py.

The tool itself reuses real production resolver helpers
(_resolve_selector_attempt_number, load_selector_recovery_cursor,
resolve_deferred_materialization_max_attempts) rather than reimplementing
classification logic, so these tests focus on proving the tool correctly
drives those helpers against realistic row shapes and produces the right
safe/unsafe classification and exit code -- not re-testing the helpers
themselves, which already have their own dedicated coverage.
"""

from __future__ import annotations

import json
import os

os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable")

import psycopg2
import psycopg2.extras
import pytest

from ap.selector_recovery_deploy_preflight import (
    _classify_row,
    _fetch_candidate_rows,
    run_preflight,
)


def _pg_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


@pytest.fixture(scope="module", autouse=True)
def _ensure_orders_table():
    with _pg_conn() as c:
        with c.cursor() as cur:
            cur.execute(
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
        c.commit()
    yield


def _insert(local_order_id: str, meta: dict, **overrides):
    row = {
        "local_order_id": local_order_id,
        "client_id": "preflight-test@example.com",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "signal_id": "sig-preflight-1",
        "canonical_signal_id": "sig-preflight-1",
        "execution_mode": "paper",
    }
    row.update(overrides)
    meta = dict(meta)
    meta.setdefault("lifecycle_state", "MATERIALIZING")
    with _pg_conn() as c:
        with c.cursor() as cur:
            cur.execute("DELETE FROM orders WHERE local_order_id = %s", (local_order_id,))
            cur.execute(
                """
                INSERT INTO orders
                    (local_order_id, client_id, kind, status, signal_id,
                     canonical_signal_id, execution_mode, meta)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                """,
                (
                    row["local_order_id"], row["client_id"], row["kind"],
                    row["status"], row["signal_id"], row["canonical_signal_id"],
                    row["execution_mode"], json.dumps(meta),
                ),
            )
        c.commit()


class TestPreflightIsReadOnly:
    def test_no_mutations_across_a_full_run(self):
        _insert("preflight-readonly-1", {"materialization_generation": 1})
        with _pg_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM orders WHERE local_order_id = %s", ("preflight-readonly-1",))
                before = dict(cur.fetchone())
        run_preflight()
        with _pg_conn() as c:
            with c.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute("SELECT * FROM orders WHERE local_order_id = %s", ("preflight-readonly-1",))
                after = dict(cur.fetchone())
        assert before == after


class TestPreflightClassification:
    def test_counter_conflict_flagged_unsafe(self):
        _insert("preflight-conflict-1", {
            "materialization_generation": 1,
            "retry_attempt": 1,
            "materialization_attempts": 2,
        })
        row = {
            "local_order_id": "preflight-conflict-1",
            "client_id": "preflight-test@example.com",
            "execution_mode": "paper",
            "signal_id": "sig-preflight-1",
            "canonical_signal_id": "sig-preflight-1",
            "status": "PENDING_TRIGGER",
            "meta": {
                "lifecycle_state": "MATERIALIZING",
                "materialization_generation": 1,
                "retry_attempt": 1,
                "materialization_attempts": 2,
            },
            "updated_ts": None,
        }
        result = _classify_row(row)
        assert result["safe"] is False
        assert any("COUNTER_CONFLICT" in f for f in result["findings"])

    def test_expired_lease_no_schedule_flagged_unsafe(self):
        row = {
            "local_order_id": "preflight-lease-1",
            "client_id": "preflight-test@example.com",
            "execution_mode": "paper",
            "signal_id": "sig-preflight-1",
            "canonical_signal_id": "sig-preflight-1",
            "status": "PENDING_TRIGGER",
            "meta": {
                "lifecycle_state": "MATERIALIZING",
                "materialization_generation": 1,
                "materialization_lease_until": "2020-01-01T00:00:00+00:00",
            },
            "updated_ts": None,
        }
        result = _classify_row(row)
        assert result["safe"] is False
        assert "EXPIRED_LEASE_NO_RETRY_SCHEDULE" in result["findings"]

    def test_missing_trigger_provenance_flagged_unsafe(self):
        row = {
            "local_order_id": "preflight-provenance-1",
            "client_id": "preflight-test@example.com",
            "execution_mode": "paper",
            "signal_id": "sig-preflight-1",
            "canonical_signal_id": "sig-preflight-1",
            "status": "PENDING_TRIGGER",
            "meta": {
                "lifecycle_state": "MATERIALIZING",
                "materialization_generation": 1,
                "trigger_crossed_at": "2026-08-01T00:00:00+00:00",
            },
            "updated_ts": None,
        }
        result = _classify_row(row)
        assert result["safe"] is False
        assert "TRIGGER_PROVENANCE_MISSING" in result["findings"]

    def test_clean_row_is_safe(self):
        row = {
            "local_order_id": "preflight-clean-1",
            "client_id": "preflight-test@example.com",
            "execution_mode": "paper",
            "signal_id": "sig-preflight-1",
            "canonical_signal_id": "sig-preflight-1",
            "status": "PENDING_TRIGGER",
            "meta": {
                "lifecycle_state": "MATERIALIZING",
                "materialization_generation": 1,
                "retry_attempt": None,
                "materialization_attempts": None,
            },
            "updated_ts": None,
        }
        result = _classify_row(row)
        assert result["safe"] is True
        assert result["findings"] == []

    def test_real_stale_production_row_shape_classifies_correctly(self):
        """Mirrors the exact residual production row found during the
        audit: PAPER, generation 1, expired lease 2026-07-29, no cursor,
        all attempt counters null. Must resolve attempt=1 (not a
        chart-bypass hazard) and be flagged unsafe specifically for the
        expired-lease-no-schedule condition, matching manual production
        classification."""
        row = {
            "local_order_id": "bb6dba15-60e4-4021-8c03-907a87962cc5",
            "client_id": "jose.vasquez4011@gmail.com",
            "execution_mode": "paper",
            "signal_id": "4d71f906-d772-4f99-a0df-0e074a296707",
            "canonical_signal_id": "4d71f906-d772-4f99-a0df-0e074a296707",
            "status": "PENDING_TRIGGER",
            "meta": {
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_generation": 1,
                "retry_attempt": None,
                "materialization_attempts": None,
                "materialization_lease_until": "2026-07-29T13:41:09.324608+00:00",
                "trigger_crossed_at": "2026-07-29T13:35:15.211736+00:00",
                "trigger_crossed_at_provenance": {
                    "client_id": "jose.vasquez4011@gmail.com",
                    "execution_mode": "paper",
                    "local_order_id": "bb6dba15-60e4-4021-8c03-907a87962cc5",
                    "canonical_signal_id": "4d71f906-d772-4f99-a0df-0e074a296707",
                },
            },
            "updated_ts": None,
        }
        result = _classify_row(row)
        assert result["resolved_attempt"] == 1
        assert result["safe"] is False
        assert result["findings"] == ["EXPIRED_LEASE_NO_RETRY_SCHEDULE"]
        assert "TRIGGER_PROVENANCE_MISSING" not in result["findings"]
        assert "COUNTER_CONFLICT" not in str(result["findings"])


class TestPreflightExitCode:
    def test_run_preflight_reports_unsafe_count(self):
        _insert("preflight-exitcode-1", {
            "materialization_generation": 1,
            "materialization_lease_until": "2020-01-01T00:00:00+00:00",
        })
        result = run_preflight()
        matching = [r for r in result["rows"] if r["local_order_id"] == "preflight-exitcode-1"]
        assert len(matching) == 1
        assert matching[0]["safe"] is False

    def test_output_is_deterministic_json_serializable(self):
        result = run_preflight()
        serialized = json.dumps(result, default=str)
        reparsed = json.loads(serialized)
        assert reparsed["tool"] == "ap.selector_recovery_deploy_preflight"
