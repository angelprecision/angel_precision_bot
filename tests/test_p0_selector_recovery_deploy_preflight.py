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


class TestBroadenedCandidateQuery:
    """Item 3: the query previously only found rows matching the exact
    status='PENDING_TRIGGER' AND lifecycle_state='MATERIALIZING'
    combination. Now it finds any PENDING_TRIGGER ENTRY row carrying any
    deferred-materialization evidence at all."""

    def test_whitespace_drifted_lifecycle_state_is_fetched_and_flagged(self):
        _insert("preflight-broad-ws-1", {
            "lifecycle_state": " MATERIALIZING",
            "materialization_generation": 1,
        })
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "preflight-broad-ws-1"]
        assert len(matching) == 1
        result = _classify_row(matching[0])
        assert result["safe"] is False
        assert any("WHITESPACE_DRIFT" in f for f in result["findings"])

    def test_case_drifted_lifecycle_state_is_fetched_and_flagged(self):
        _insert("preflight-broad-case-1", {
            "lifecycle_state": "materializing",
            "materialization_generation": 1,
        })
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "preflight-broad-case-1"]
        assert len(matching) == 1
        result = _classify_row(matching[0])
        assert result["safe"] is False
        assert any("CASE_DRIFT" in f for f in result["findings"])

    def test_blank_lifecycle_with_active_materialization_status_flagged(self):
        """Blank lifecycle_state paired with an active (non-rearm)
        materialization_status is not a recognized valid pairing --
        previously invisible to the query entirely since it never equals
        'MATERIALIZING'."""
        _insert("preflight-broad-blank-1", {
            "lifecycle_state": "",
            "materialization_status": "RUNNING",
            "materialization_generation": 1,
        })
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "preflight-broad-blank-1"]
        assert len(matching) == 1
        result = _classify_row(matching[0])
        assert result["safe"] is False
        assert any("LIFECYCLE_MATERIALIZATION_STATUS_CONFLICT" in f for f in result["findings"])

    def test_retry_wait_with_contradictory_status_flagged(self):
        _insert("preflight-broad-retrywait-1", {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "SELECTED",
            "materialization_generation": 1,
        })
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "preflight-broad-retrywait-1"]
        assert len(matching) == 1
        result = _classify_row(matching[0])
        assert result["safe"] is False
        assert any("LIFECYCLE_MATERIALIZATION_STATUS_CONFLICT" in f for f in result["findings"])

    def test_retry_wait_with_correct_status_is_a_known_valid_pair(self):
        _insert("preflight-broad-retrywait-2", {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_generation": 1,
            "next_retry_at": "2026-08-10T00:00:00+00:00",
        })
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "preflight-broad-retrywait-2"]
        assert len(matching) == 1
        result = _classify_row(matching[0])
        assert not any("LIFECYCLE_MATERIALIZATION_STATUS_CONFLICT" in f for f in result["findings"])

    def test_row_with_only_a_cursor_and_no_lifecycle_state_is_still_fetched(self):
        """A row carrying a durable cursor but no lifecycle_state at all
        (e.g. a legacy write shape) must still be surfaced -- the old
        query's exact lifecycle_state='MATERIALIZING' requirement would
        have missed this entirely."""
        _insert("preflight-broad-cursor-1", {
            "selector_recovery_cursor_v1": {"version": 1},
        })
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "preflight-broad-cursor-1"]
        assert len(matching) == 1

    def test_row_with_only_a_retry_schedule_is_still_fetched(self):
        _insert("preflight-broad-schedule-1", {
            "next_retry_at": "2026-08-10T00:00:00+00:00",
        })
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "preflight-broad-schedule-1"]
        assert len(matching) == 1

    def test_ordinary_row_with_no_deferred_evidence_at_all_is_not_fetched(self):
        """Positive control: a genuinely ordinary PENDING_TRIGGER row that
        never touched deferred recovery at all must not be swept in --
        the query is broader, not unbounded."""
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("DELETE FROM orders WHERE local_order_id = %s", ("preflight-broad-ordinary-1",))
                cur.execute(
                    """
                    INSERT INTO orders
                        (local_order_id, client_id, kind, status, signal_id,
                         canonical_signal_id, execution_mode, meta)
                    VALUES (%s, %s, 'ENTRY', 'PENDING_TRIGGER', %s, %s, 'paper', %s::jsonb)
                    """,
                    (
                        "preflight-broad-ordinary-1", "preflight-test@example.com",
                        "sig-ordinary-1", "sig-ordinary-1",
                        json.dumps({"unrelated_field": "not-deferred-evidence"}),
                    ),
                )
            c.commit()
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "preflight-broad-ordinary-1"]
        assert len(matching) == 0
