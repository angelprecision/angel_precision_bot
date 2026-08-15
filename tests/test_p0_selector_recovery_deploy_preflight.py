"""Tests for ap/selector_recovery_deploy_preflight.py.

The tool itself reuses real production resolver helpers
(_resolve_selector_attempt_number, load_selector_recovery_cursor,
_selector_cursor_retry_block_reason,
resolve_deferred_materialization_max_attempts) rather than reimplementing
classification logic, so these tests focus on proving the tool correctly
drives those helpers against realistic row shapes and produces the right
safe/unsafe classification and exit code -- not re-testing the helpers
themselves, which already have their own dedicated coverage.

Round 5 additions (surgical amendment -- four false-negative blockers):
  - TestAttempt2MissingCursorDetection  (Blocker 1)
  - TestConfigConflictPreflightExitCode  (Blocker 2)
  - TestGenerationCompleteness           (Blocker 3)
  - TestQueryNormalizationBtrim          (Blocker 4)

Remaining-blocker addition (candidate_row_count enforcement):
  - TestMainExitCodeEnforcesCandidateZero -- command-level tests calling
    main() itself (not a reimplemented exit-code formula) with
    run_preflight() monkeypatched to controlled results, proving the
    actual executable deployment contract requires candidate_row_count==0,
    not merely unsafe_row_count==0.
"""

from __future__ import annotations

import json
import os

os.environ.setdefault("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable")

import psycopg2
import psycopg2.extras
import pytest

import ap.selector_recovery_deploy_preflight as preflight_module
from ap.selector_recovery_deploy_preflight import (
    _classify_row,
    _fetch_candidate_rows,
    main,
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


# ---------------------------------------------------------------------------
# Preserved existing tests (unchanged)
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Blocker 1: attempt-2+ missing cursor detection
# ---------------------------------------------------------------------------

class TestAttempt2MissingCursorDetection:
    """Blocker 1 fix: preflight must call _selector_cursor_retry_block_reason
    after load_selector_recovery_cursor so that a missing cursor candidate
    (which the loader silently converts to a fresh cursor with no error) is
    still detected as MISSING_CURSOR_ON_RETRY when attempt > 1.

    Required spec:
      - attempt counters all equal 2, valid generation, lifecycle
        RETRY_WAIT/RETRY_PENDING, cursor absent => MISSING_CURSOR_ON_RETRY,
        safe=false
      - malformed cursor positive control
      - identity-mismatched cursor positive control
    """

    _BASE_ROW = {
        "local_order_id": "b1-base",
        "client_id": "preflight-test@example.com",
        "execution_mode": "paper",
        "signal_id": "sig-preflight-1",
        "canonical_signal_id": "sig-preflight-1",
        "status": "PENDING_TRIGGER",
        "updated_ts": None,
    }

    def _row(self, oid: str, meta: dict) -> dict:
        return {**self._BASE_ROW, "local_order_id": oid, "meta": meta}

    def test_attempt2_absent_cursor_is_unsafe_missing_cursor_on_retry(self):
        """An attempt-2 row with selector_recovery_cursor_v1 absent (null)
        must be flagged MISSING_CURSOR_ON_RETRY.  Previously the preflight
        called load_selector_recovery_cursor() directly; that helper returns
        a fresh cursor with no cursor_reason for a missing candidate, so the
        row classified safe -- the false negative this test closes."""
        row = self._row("b1-absent-cursor-1", {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_generation": 2,
            "retry_attempt": 2,
            "breach_attempt_count": 2,
            "materialization_attempts": 2,
            "selector_recovery_cursor_v1": None,  # absent
        })
        result = _classify_row(row)
        assert result["safe"] is False
        assert any(
            "MISSING_CURSOR_ON_RETRY" in f for f in result["findings"]
        ), f"Expected MISSING_CURSOR_ON_RETRY in findings; got {result['findings']}"

    def test_attempt1_absent_cursor_does_not_flag_missing_cursor(self):
        """Negative control: attempt-1 rows do not require a cursor; the
        check must not fire at attempt <= 1."""
        row = self._row("b1-absent-cursor-attempt1", {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_generation": 1,
            # All counters absent/null => attempt resolves to 1
            "selector_recovery_cursor_v1": None,
        })
        result = _classify_row(row)
        assert not any(
            "MISSING_CURSOR_ON_RETRY" in f for f in result["findings"]
        ), f"Unexpected MISSING_CURSOR_ON_RETRY at attempt-1; got {result['findings']}"

    def test_attempt2_malformed_cursor_is_unsafe(self):
        """Positive control: a non-dict cursor at attempt-2 is MALFORMED_CURSOR,
        not a pass.  load_selector_recovery_cursor returns the malformed reason;
        _selector_cursor_retry_block_reason propagates it."""
        row = self._row("b1-malformed-cursor-1", {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_generation": 2,
            "retry_attempt": 2,
            "breach_attempt_count": 2,
            "materialization_attempts": 2,
            "selector_recovery_cursor_v1": "not-a-dict",  # malformed
        })
        result = _classify_row(row)
        assert result["safe"] is False
        assert any(
            "MALFORMED_CURSOR" in f for f in result["findings"]
        ), f"Expected MALFORMED_CURSOR in findings; got {result['findings']}"

    def test_attempt2_identity_mismatched_cursor_is_unsafe(self):
        """Positive control: a cursor whose local_order_id does not match
        the row must be rejected as IDENTITY_MISMATCH."""
        row = self._row("b1-mismatch-cursor-1", {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_generation": 2,
            "retry_attempt": 2,
            "breach_attempt_count": 2,
            "materialization_attempts": 2,
            "selector_recovery_cursor_v1": {
                "version": 1,
                "local_order_id": "completely-different-order-id",  # mismatch
                "client_id": "preflight-test@example.com",
                "execution_mode": "paper",
                "signal_id": "sig-preflight-1",
                "materialization_generation": 2,
            },
        })
        result = _classify_row(row)
        assert result["safe"] is False
        assert any(
            "IDENTITY_MISMATCH" in f for f in result["findings"]
        ), f"Expected IDENTITY_MISMATCH in findings; got {result['findings']}"


# ---------------------------------------------------------------------------
# Blocker 2: configuration conflict must produce exit 2 even with zero rows
# ---------------------------------------------------------------------------

class TestConfigConflictPreflightExitCode:
    """Blocker 2 fix: run_preflight() must not substitute a numeric fallback
    when resolve_deferred_materialization_max_attempts() raises
    DeferredMaterializationConfigConflict.  resolved_max_attempts must be
    null, max_attempts_config_conflict must carry the conflict text, and the
    resulting exit code must be 2 regardless of candidate_row_count.

    Required spec:
      - no rows + conflicting env => exit 2
      - no rows + malformed env => exit 2
      - no rows + valid env => exit 0  (conflict not present)
    """

    def test_conflicting_env_sets_null_resolved_max_attempts(self, monkeypatch):
        """MAX_BREACH_SELECTOR_RETRIES and DEFERRED_MATERIALIZATION_MAX_ATTEMPTS
        set to different positive values => conflict => resolved_max_attempts null,
        max_attempts_config_conflict populated, effective exit code 2."""
        monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")
        monkeypatch.setenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", "3")
        result = run_preflight()
        assert result["resolved_max_attempts"] is None, (
            "resolved_max_attempts must be null on conflict, not a fallback integer"
        )
        assert result["max_attempts_config_conflict"] is not None
        # Exit code rule: 2 if unsafe_row_count > 0 OR config conflict present.
        effective_exit = 2 if (
            result["unsafe_row_count"] > 0
            or result["max_attempts_config_conflict"] is not None
        ) else 0
        assert effective_exit == 2

    def test_malformed_env_sets_null_resolved_max_attempts(self, monkeypatch):
        """A non-integer env value raises DeferredMaterializationConfigConflict;
        preflight must treat it identically to a value conflict -- exit 2."""
        monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "not-a-number")
        monkeypatch.delenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", raising=False)
        result = run_preflight()
        assert result["resolved_max_attempts"] is None, (
            "resolved_max_attempts must be null on malformed env, not a fallback integer"
        )
        assert result["max_attempts_config_conflict"] is not None
        effective_exit = 2 if (
            result["unsafe_row_count"] > 0
            or result["max_attempts_config_conflict"] is not None
        ) else 0
        assert effective_exit == 2

    def test_valid_env_produces_no_conflict(self, monkeypatch):
        """When no conflict is present the config-conflict field must be null,
        resolved_max_attempts must be a positive integer, and the config
        portion alone must not force exit 2."""
        monkeypatch.delenv("MAX_BREACH_SELECTOR_RETRIES", raising=False)
        monkeypatch.delenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", raising=False)
        result = run_preflight()
        assert result["max_attempts_config_conflict"] is None
        assert isinstance(result["resolved_max_attempts"], int)
        assert result["resolved_max_attempts"] > 0
        # Config portion alone must not force exit 2.
        config_conflict_exit = 2 if result["max_attempts_config_conflict"] is not None else 0
        assert config_conflict_exit == 0


class TestDirectQuoteBudgetPreflight:
    _BUDGET_KEYS = (
        "SELECTOR_MAX_DIRECT_QUOTE_CALLS",
        "DIRECT_QUOTE_RECOVERY_TOP_N",
        "CONTRACT_REVALIDATE_TOP_N",
    )

    def _run_without_rows(self, monkeypatch, capsys, values):
        for key in (
            *self._BUDGET_KEYS,
            "MAX_BREACH_SELECTOR_RETRIES",
            "DEFERRED_MATERIALIZATION_MAX_ATTEMPTS",
        ):
            monkeypatch.delenv(key, raising=False)
        for key, value in values.items():
            monkeypatch.setenv(key, value)
        monkeypatch.setattr(preflight_module, "_fetch_candidate_rows", lambda: [])

        exit_code = main()
        output = json.loads(capsys.readouterr().out)
        return exit_code, output["selector_direct_quote_budget"]

    def test_canonical_only_40_is_clean_and_reports_both_envelopes(
        self, monkeypatch, capsys
    ):
        exit_code, budget = self._run_without_rows(
            monkeypatch,
            capsys,
            {"SELECTOR_MAX_DIRECT_QUOTE_CALLS": "40"},
        )

        assert exit_code == 0
        assert set(budget) == {
            "canonical_value",
            "direct_recovery_alias",
            "contract_revalidate_alias",
            "source",
            "ordinary_effective_limit",
            "deferred_effective_limit",
            "conflict",
            "conflict_detail",
            "invalid_explicit_keys",
        }
        assert budget == {
            "canonical_value": 40,
            "direct_recovery_alias": None,
            "contract_revalidate_alias": None,
            "source": "SELECTOR_MAX_DIRECT_QUOTE_CALLS",
            "ordinary_effective_limit": 20,
            "deferred_effective_limit": 40,
            "conflict": False,
            "conflict_detail": None,
            "invalid_explicit_keys": [],
        }

    def test_matching_40_aliases_are_clean(self, monkeypatch, capsys):
        exit_code, budget = self._run_without_rows(
            monkeypatch,
            capsys,
            {
                "SELECTOR_MAX_DIRECT_QUOTE_CALLS": "40",
                "DIRECT_QUOTE_RECOVERY_TOP_N": "40",
                "CONTRACT_REVALIDATE_TOP_N": "40",
            },
        )

        assert exit_code == 0
        assert budget["deferred_effective_limit"] == 40
        assert budget["conflict"] is False
        assert budget["invalid_explicit_keys"] == []

    def test_paper_shaped_conflict_holds_even_without_rows(self, monkeypatch, capsys):
        exit_code, budget = self._run_without_rows(
            monkeypatch,
            capsys,
            {
                "SELECTOR_MAX_DIRECT_QUOTE_CALLS": "20",
                "DIRECT_QUOTE_RECOVERY_TOP_N": "8",
                "CONTRACT_REVALIDATE_TOP_N": "20",
            },
        )

        assert exit_code == 2
        assert budget["ordinary_effective_limit"] == 20
        assert budget["deferred_effective_limit"] == 20
        assert budget["conflict"] is True
        assert budget["invalid_explicit_keys"] == []

    @pytest.mark.parametrize(
        ("key", "value"),
        [
            ("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "abc"),
            ("DIRECT_QUOTE_RECOVERY_TOP_N", "0"),
            ("CONTRACT_REVALIDATE_TOP_N", "-2"),
        ],
    )
    def test_malformed_or_non_positive_budget_env_holds_and_names_key(
        self, monkeypatch, capsys, key, value
    ):
        exit_code, budget = self._run_without_rows(
            monkeypatch,
            capsys,
            {
                "SELECTOR_MAX_DIRECT_QUOTE_CALLS": "40",
                key: value,
            },
        )

        assert exit_code == 2
        assert budget["invalid_explicit_keys"] == [key]
        assert budget["conflict"] is False


# ---------------------------------------------------------------------------
# Blocker 3: generation completeness (independent of attempt number)
# ---------------------------------------------------------------------------

class TestGenerationCompleteness:
    """Blocker 3 fix: materialization_generation must be validated for every
    row carrying active lifecycle or ownership evidence, independently of the
    attempt number.  Previously the check only ran inside the attempt-2+ block,
    so an attempt-1 MATERIALIZING row with null generation classified safe.

    Required spec: missing, zero, negative, bool, float, malformed string =>
    unsafe.  Positive integer or integer-shaped string => accepted.
    """

    _BASE = {
        "local_order_id": "b3-base",
        "client_id": "preflight-test@example.com",
        "execution_mode": "paper",
        "signal_id": "sig-preflight-1",
        "canonical_signal_id": "sig-preflight-1",
        "status": "PENDING_TRIGGER",
        "updated_ts": None,
    }

    def _row(self, oid: str, generation) -> dict:
        return {
            **self._BASE,
            "local_order_id": oid,
            "meta": {
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_generation": generation,
                "retry_attempt": None,
                "materialization_attempts": None,
            },
        }

    def test_missing_generation_is_unsafe(self):
        """Null generation on an active MATERIALIZING row must be GENERATION_MISSING."""
        result = _classify_row(self._row("b3-gen-null", None))
        assert result["safe"] is False
        assert "GENERATION_MISSING" in result["findings"], result["findings"]

    def test_zero_generation_is_unsafe(self):
        result = _classify_row(self._row("b3-gen-zero", 0))
        assert result["safe"] is False
        assert "GENERATION_MALFORMED" in result["findings"], result["findings"]

    def test_negative_generation_is_unsafe(self):
        result = _classify_row(self._row("b3-gen-neg", -1))
        assert result["safe"] is False
        assert "GENERATION_MALFORMED" in result["findings"], result["findings"]

    def test_bool_true_generation_is_unsafe(self):
        """True coerces to 1 under plain int(); must be rejected as a bool."""
        result = _classify_row(self._row("b3-gen-bool-true", True))
        assert result["safe"] is False
        assert "GENERATION_MALFORMED" in result["findings"], result["findings"]

    def test_bool_false_generation_is_unsafe(self):
        result = _classify_row(self._row("b3-gen-bool-false", False))
        assert result["safe"] is False
        assert "GENERATION_MALFORMED" in result["findings"], result["findings"]

    def test_float_generation_is_unsafe(self):
        """1.0 is a whole-valued float; must be rejected, not silently cast."""
        result = _classify_row(self._row("b3-gen-float", 1.0))
        assert result["safe"] is False
        assert "GENERATION_MALFORMED" in result["findings"], result["findings"]

    def test_fractional_string_generation_is_unsafe(self):
        result = _classify_row(self._row("b3-gen-frac-str", "1.5"))
        assert result["safe"] is False
        assert "GENERATION_MALFORMED" in result["findings"], result["findings"]

    def test_nonnumeric_string_generation_is_unsafe(self):
        result = _classify_row(self._row("b3-gen-bad-str", "bad"))
        assert result["safe"] is False
        assert "GENERATION_MALFORMED" in result["findings"], result["findings"]

    def test_positive_integer_generation_is_accepted(self):
        """Valid case: positive integer generation on an attempt-1 row must
        not produce any generation finding -- previously this was only checked
        at attempt > 1, so attempt-1 rows with valid generation were already
        fine; the fix must not break them."""
        result = _classify_row(self._row("b3-gen-valid-int", 1))
        assert "GENERATION_MISSING" not in result["findings"], result["findings"]
        assert "GENERATION_MALFORMED" not in result["findings"], result["findings"]

    def test_integer_shaped_string_generation_is_accepted(self):
        """String "1" must parse as generation=1 and not be flagged."""
        result = _classify_row(self._row("b3-gen-valid-str", "1"))
        assert "GENERATION_MISSING" not in result["findings"], result["findings"]
        assert "GENERATION_MALFORMED" not in result["findings"], result["findings"]

    def test_generation_validated_at_attempt_1_not_only_attempt_2_plus(self):
        """The core Blocker 3 scenario: attempt-1 MATERIALIZING/RUNNING row
        with null generation must be unsafe.  This was previously invisible
        because the generation check only ran inside the attempt-2+ block."""
        row = {
            **self._BASE,
            "local_order_id": "b3-gen-attempt1-null",
            "meta": {
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_generation": None,  # absent
                "retry_attempt": None,
                "materialization_attempts": None,
                # breach_attempt_count also absent => attempt resolves to 1
            },
        }
        result = _classify_row(row)
        assert result["safe"] is False
        assert "GENERATION_MISSING" in result["findings"], (
            f"Attempt-1 row with null generation must be unsafe; got {result['findings']}"
        )

    def test_row_with_only_retry_schedule_requires_generation(self):
        """SUPERSEDED BY ROUND 6: this test originally asserted that a
        retry-schedule-only row (next_retry_at present, nothing else) does
        NOT require generation -- on the theory that a bare retry schedule
        wasn't "ownership evidence." Round 6's audit correctly identified
        this as a gap: next_retry_at is part of the same deferred-evidence
        set the candidate query itself uses to fetch rows, so a row that is
        pulled in for having a retry schedule must not then be exempt from
        generation validation. The assertion below is intentionally the
        opposite of the original -- this is the corrected, currently-true
        behavior. See TestGenerationRequiredForAllEvidenceFields for the
        full field-by-field coverage this generalizes."""
        row = {
            **self._BASE,
            "local_order_id": "b3-gen-retry-schedule-only",
            "meta": {
                "next_retry_at": "2026-08-10T00:00:00+00:00",
                # No lifecycle_state, no materialization_status, no cursor,
                # no lease, no owner -- but next_retry_at alone is now
                # sufficient evidence per Round 6.
            },
        }
        result = _classify_row(row)
        assert "GENERATION_MISSING" in result["findings"], (
            f"next_retry_at alone must require generation per Round 6; "
            f"got {result['findings']}"
        )

    def test_row_with_zero_evidence_fields_does_not_require_generation(self):
        """Genuine negative control (what the superseded test above was
        actually trying to express): a row with NO evidence fields at all --
        not even next_retry_at -- must not be flagged for generation."""
        row = {
            **self._BASE,
            "local_order_id": "b3-gen-truly-no-evidence",
            "meta": {
                "unrelated_field": "nothing-to-do-with-deferred",
                # No lifecycle_state, no materialization_status, no cursor,
                # no lease, no owner, no next_retry_at, no attempt counters.
            },
        }
        result = _classify_row(row)
        assert "GENERATION_MISSING" not in result["findings"], result["findings"]
        assert "GENERATION_MALFORMED" not in result["findings"], result["findings"]


# ---------------------------------------------------------------------------
# Blocker 4: query normalization -- BTRIM on kind and status outer predicates
# ---------------------------------------------------------------------------

class TestQueryNormalizationBtrim:
    """Blocker 4 fix: the outer WHERE predicates on kind and status previously
    used UPPER(COALESCE(...)) without BTRIM, so rows with kind=' ENTRY ' or
    status=' PENDING_TRIGGER ' (leading/trailing whitespace) were invisible to
    the preflight despite carrying deferred-materialization evidence.

    Required spec:
      - ' ENTRY ' kind is fetched
      - ' PENDING_TRIGGER ' status is fetched
      - ordinary unrelated rows (without deferred evidence) remain excluded
    """

    def _insert_raw(self, local_order_id: str, kind: str, status: str, meta: dict):
        """Insert a row bypassing the _insert() helper so we can set
        whitespace-drifted kind/status values directly."""
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    "DELETE FROM orders WHERE local_order_id = %s", (local_order_id,)
                )
                cur.execute(
                    """
                    INSERT INTO orders
                        (local_order_id, client_id, kind, status, signal_id,
                         canonical_signal_id, execution_mode, meta)
                    VALUES (%s, %s, %s, %s, %s, %s, 'paper', %s::jsonb)
                    """,
                    (
                        local_order_id,
                        "preflight-test@example.com",
                        kind,
                        status,
                        "sig-preflight-1",
                        "sig-preflight-1",
                        json.dumps(meta),
                    ),
                )
            c.commit()

    def test_whitespace_drifted_kind_is_fetched(self):
        """kind=' ENTRY ' (with spaces) must be found by the broadened query
        after BTRIM is applied."""
        self._insert_raw(
            "b4-btrim-kind-1",
            " ENTRY ",
            "PENDING_TRIGGER",
            {
                "lifecycle_state": "MATERIALIZING",
                "materialization_generation": 1,
            },
        )
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "b4-btrim-kind-1"]
        assert len(matching) == 1, (
            "Row with kind=' ENTRY ' (whitespace-drifted) must be fetched; "
            "check BTRIM on kind predicate"
        )

    def test_whitespace_drifted_status_is_fetched(self):
        """status=' PENDING_TRIGGER ' (with spaces) must be found."""
        self._insert_raw(
            "b4-btrim-status-1",
            "ENTRY",
            " PENDING_TRIGGER ",
            {
                "lifecycle_state": "MATERIALIZING",
                "materialization_generation": 1,
            },
        )
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "b4-btrim-status-1"]
        assert len(matching) == 1, (
            "Row with status=' PENDING_TRIGGER ' (whitespace-drifted) must be fetched; "
            "check BTRIM on status predicate"
        )

    def test_ordinary_unrelated_entry_still_excluded(self):
        """Positive control: a row with exact kind='ENTRY' / status='PENDING_TRIGGER'
        but no deferred-materialization evidence must remain excluded by the
        inner OR predicate, proving BTRIM did not accidentally widen the net
        beyond intended scope."""
        self._insert_raw(
            "b4-btrim-ordinary-1",
            "ENTRY",
            "PENDING_TRIGGER",
            {"unrelated_field": "nothing-to-do-with-deferred"},
        )
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == "b4-btrim-ordinary-1"]
        assert len(matching) == 0, (
            "Ordinary ENTRY/PENDING_TRIGGER row without deferred evidence "
            "must not be fetched by the preflight query"
        )


# ---------------------------------------------------------------------------
# Round 6: generation-evidence predicate must match the candidate query's
# full deferred-evidence field set, not a narrower subset
# ---------------------------------------------------------------------------

class TestGenerationRequiredForAllEvidenceFields:
    """The generation-mandatory predicate previously covered only
    lifecycle_state, materialization_status, selector_recovery_cursor_v1,
    materialization_lease_until, and materialization_owner -- a narrower set
    than what the candidate query itself treats as deferred-materialization
    evidence. A row carrying ONLY current_owner, watcher_token,
    recovery_owner, next_retry_at, retry_attempt, materialization_attempts,
    or breach_attempt_count (with no lifecycle_state/materialization_status/
    cursor/lease/materialization_owner) was fetched by the query but exempt
    from generation validation -- and could classify safe with no
    materialization_generation at all.

    _has_ownership_evidence now mirrors the query's evidence set exactly, and
    uses key presence (`"field" in meta`) rather than truthiness, so an
    explicitly empty ownership field (e.g. current_owner="") is still
    evidence and still requires generation.

    Each field below is tested in isolation: (a) the row is fetched by
    _fetch_candidate_rows, and (b) _classify_row flags it unsafe for missing
    generation.
    """

    def _insert_bare(self, local_order_id: str, meta: dict):
        """Insert a row with meta exactly as given -- no lifecycle_state
        default -- so each test isolates a single evidence field."""
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute(
                    "DELETE FROM orders WHERE local_order_id = %s", (local_order_id,)
                )
                cur.execute(
                    """
                    INSERT INTO orders
                        (local_order_id, client_id, kind, status, signal_id,
                         canonical_signal_id, execution_mode, meta)
                    VALUES (%s, %s, 'ENTRY', 'PENDING_TRIGGER', %s, %s, 'paper', %s::jsonb)
                    """,
                    (
                        local_order_id,
                        "preflight-test@example.com",
                        "sig-preflight-1",
                        "sig-preflight-1",
                        json.dumps(meta),
                    ),
                )
            c.commit()

    def _assert_fetched_and_generation_missing(self, local_order_id: str, meta: dict):
        self._insert_bare(local_order_id, meta)
        rows = _fetch_candidate_rows()
        matching = [r for r in rows if r["local_order_id"] == local_order_id]
        assert len(matching) == 1, (
            f"Row {local_order_id} carrying evidence {meta!r} must be fetched "
            f"by the broadened candidate query"
        )
        result = _classify_row(matching[0])
        assert result["safe"] is False, (
            f"Row {local_order_id} with evidence {meta!r} and no generation "
            f"must classify unsafe; got safe=True"
        )
        assert any(
            f in ("GENERATION_MISSING", "GENERATION_MALFORMED") or f.startswith("GENERATION_")
            for f in result["findings"]
        ), (
            f"Row {local_order_id} must carry a GENERATION_* finding; "
            f"got {result['findings']}"
        )

    def test_current_owner_only_requires_generation(self):
        self._assert_fetched_and_generation_missing(
            "r6-gen-current-owner", {"current_owner": "worker-1"}
        )

    def test_recovery_owner_only_requires_generation(self):
        self._assert_fetched_and_generation_missing(
            "r6-gen-recovery-owner", {"recovery_owner": "worker-2"}
        )

    def test_watcher_token_only_requires_generation(self):
        self._assert_fetched_and_generation_missing(
            "r6-gen-watcher-token", {"watcher_token": "tok-abc123"}
        )

    def test_next_retry_at_only_requires_generation(self):
        self._assert_fetched_and_generation_missing(
            "r6-gen-next-retry-at",
            {"next_retry_at": "2026-08-10T00:00:00+00:00"},
        )

    def test_retry_attempt_only_requires_generation(self):
        self._assert_fetched_and_generation_missing(
            "r6-gen-retry-attempt", {"retry_attempt": 1}
        )

    def test_materialization_attempts_only_requires_generation(self):
        self._assert_fetched_and_generation_missing(
            "r6-gen-materialization-attempts", {"materialization_attempts": 1}
        )

    def test_breach_attempt_count_only_requires_generation(self):
        self._assert_fetched_and_generation_missing(
            "r6-gen-breach-attempt-count", {"breach_attempt_count": 1}
        )

    def test_empty_string_current_owner_still_requires_generation(self):
        """Key presence, not truthiness: current_owner="" is itself
        malformed/incomplete ownership evidence, not the same as the field
        being absent. It must still trigger the generation requirement,
        not silently exempt the row."""
        self._assert_fetched_and_generation_missing(
            "r6-gen-empty-current-owner", {"current_owner": ""}
        )

    def test_empty_string_recovery_owner_still_requires_generation(self):
        self._assert_fetched_and_generation_missing(
            "r6-gen-empty-recovery-owner", {"recovery_owner": ""}
        )

    def test_original_reported_shape_current_owner_and_retry_attempt(self):
        """The exact row shape from the audit report: current_owner plus
        retry_attempt=1, no materialization_generation. Must resolve
        attempt=1 (not a counter conflict) and still be flagged unsafe for
        missing generation -- previously this combination classified safe."""
        row = {
            "local_order_id": "r6-gen-reported-shape",
            "client_id": "preflight-test@example.com",
            "execution_mode": "paper",
            "signal_id": "sig-preflight-1",
            "canonical_signal_id": "sig-preflight-1",
            "status": "PENDING_TRIGGER",
            "meta": {
                "current_owner": "worker-1",
                "retry_attempt": 1,
            },
            "updated_ts": None,
        }
        result = _classify_row(row)
        assert result["resolved_attempt"] == 1
        assert result["safe"] is False
        assert "GENERATION_MISSING" in result["findings"], (
            f"Expected GENERATION_MISSING; got {result['findings']}"
        )

    def test_evidence_field_with_valid_generation_present_is_not_flagged(self):
        """Negative control: when one of the newly-covered evidence fields
        is present AND a valid positive-integer generation is also present,
        no generation finding should fire."""
        row = {
            "local_order_id": "r6-gen-valid-with-owner",
            "client_id": "preflight-test@example.com",
            "execution_mode": "paper",
            "signal_id": "sig-preflight-1",
            "canonical_signal_id": "sig-preflight-1",
            "status": "PENDING_TRIGGER",
            "meta": {
                "current_owner": "worker-1",
                "materialization_generation": 1,
            },
            "updated_ts": None,
        }
        result = _classify_row(row)
        assert "GENERATION_MISSING" not in result["findings"], result["findings"]
        assert "GENERATION_MALFORMED" not in result["findings"], result["findings"]

    def test_row_with_zero_evidence_fields_still_not_flagged(self):
        """Sanity check unrelated to the fix: a row matching none of the
        evidence fields (would not even be fetched by the query in
        practice) must not be flagged for generation when classified
        directly."""
        row = {
            "local_order_id": "r6-gen-no-evidence",
            "client_id": "preflight-test@example.com",
            "execution_mode": "paper",
            "signal_id": "sig-preflight-1",
            "canonical_signal_id": "sig-preflight-1",
            "status": "PENDING_TRIGGER",
            "meta": {"unrelated_field": "nothing-to-do-with-deferred"},
            "updated_ts": None,
        }
        result = _classify_row(row)
        assert "GENERATION_MISSING" not in result["findings"], result["findings"]
        assert "GENERATION_MALFORMED" not in result["findings"], result["findings"]


# ---------------------------------------------------------------------------
# Remaining blocker: preflight must enforce candidate_row_count == 0, not
# merely unsafe_row_count == 0, at the command level (main()).
# ---------------------------------------------------------------------------

class TestMainExitCodeEnforcesCandidateZero:
    """The documented release gate is:

        candidate_row_count = 0
        unsafe_row_count = 0
        max_attempts_config_conflict = null
        exit code = 0

    main() previously only checked unsafe_row_count and
    max_attempts_config_conflict -- a row could be individually well-formed
    (safe=true) and still leave candidate_row_count > 0, and the process
    would incorrectly exit 0. "All discovered rows are internally
    consistent" is a materially weaker claim than "no deferred-materialization
    candidates remain before deployment," and the executable gate must
    enforce the latter, which is what the PR body actually commits to.

    These tests call main() itself -- the actual command-level entry point
    -- with run_preflight() monkeypatched to return controlled results, so
    they prove the real executable contract rather than re-deriving the
    boolean formula independently (which would pass even if main() itself
    drifted from that formula).
    """

    def _run_main_with_result(self, monkeypatch, capsys, result):
        monkeypatch.setattr(preflight_module, "run_preflight", lambda: result)
        exit_code = main()
        captured = capsys.readouterr()
        return exit_code, captured.out

    def test_candidate_rows_present_zero_unsafe_still_exits_2(self, monkeypatch, capsys):
        """candidate_row_count=1, unsafe_row_count=0 -> exit 2.  This is the
        exact case the audit reported: a safe-but-unresolved candidate must
        not be treated as a clean deployment."""
        result = {
            "tool": "ap.selector_recovery_deploy_preflight",
            "candidate_row_count": 1,
            "unsafe_row_count": 0,
            "max_attempts_config_conflict": None,
            "resolved_max_attempts": 5,
            "rows": [{"local_order_id": "x", "safe": True, "findings": []}],
        }
        exit_code, out = self._run_main_with_result(monkeypatch, capsys, result)
        assert exit_code == 2, f"Expected exit 2 for candidate_row_count=1; got {exit_code}"
        assert json.loads(out)["candidate_row_count"] == 1

    def test_zero_candidates_zero_unsafe_no_conflict_exits_0(self, monkeypatch, capsys):
        """candidate_row_count=0, unsafe_row_count=0, no conflict -> exit 0.
        This is the only condition under which the documented release gate
        permits deployment."""
        result = {
            "tool": "ap.selector_recovery_deploy_preflight",
            "candidate_row_count": 0,
            "unsafe_row_count": 0,
            "max_attempts_config_conflict": None,
            "resolved_max_attempts": 5,
            "rows": [],
        }
        exit_code, out = self._run_main_with_result(monkeypatch, capsys, result)
        assert exit_code == 0, f"Expected exit 0 for the fully clean case; got {exit_code}"
        assert json.loads(out)["candidate_row_count"] == 0

    def test_candidate_and_unsafe_both_present_exits_2(self, monkeypatch, capsys):
        """candidate_row_count=1, unsafe_row_count=1 -> exit 2 (both
        conditions independently sufficient; this proves neither masks
        the other)."""
        result = {
            "tool": "ap.selector_recovery_deploy_preflight",
            "candidate_row_count": 1,
            "unsafe_row_count": 1,
            "max_attempts_config_conflict": None,
            "resolved_max_attempts": 5,
            "rows": [{"local_order_id": "x", "safe": False, "findings": ["GENERATION_MISSING"]}],
        }
        exit_code, out = self._run_main_with_result(monkeypatch, capsys, result)
        assert exit_code == 2

    def test_config_conflict_alone_with_zero_candidates_exits_2(self, monkeypatch, capsys):
        """A configuration conflict must fail the gate even with zero
        candidates and zero unsafe rows -- proves the three exit-2
        conditions are independently sufficient, not just candidate_row_count."""
        result = {
            "tool": "ap.selector_recovery_deploy_preflight",
            "candidate_row_count": 0,
            "unsafe_row_count": 0,
            "max_attempts_config_conflict": "MAX_BREACH_SELECTOR_RETRIES=5 conflicts with DEFERRED_MATERIALIZATION_MAX_ATTEMPTS=3",
            "resolved_max_attempts": None,
            "rows": [],
        }
        exit_code, out = self._run_main_with_result(monkeypatch, capsys, result)
        assert exit_code == 2

    def test_tool_exception_exits_1(self, monkeypatch, capsys):
        """A genuine tool error (DB unreachable, etc.) must exit 1, distinct
        from both the clean-pass (0) and unsafe/pending (2) codes -- callers
        need to be able to distinguish 'the gate ran and found a problem'
        from 'the gate could not run at all.'"""
        def _raise():
            raise RuntimeError("could not connect to database")

        monkeypatch.setattr(preflight_module, "run_preflight", _raise)
        exit_code = main()
        captured = capsys.readouterr()
        assert exit_code == 1, f"Expected exit 1 on tool error; got {exit_code}"
        parsed = json.loads(captured.out)
        assert "tool_error" in parsed
        assert "could not connect to database" in parsed["tool_error"]

    def test_end_to_end_run_preflight_result_respects_candidate_gate(self):
        """Integration check: insert one deliberately safe-but-present
        candidate row via the real DB path, run the real run_preflight()
        (not monkeypatched), and confirm candidate_row_count reflects it.
        This does not call main() (the shared test DB accumulates rows
        across the whole module, so asserting a specific exit code here
        would be flaky) -- it instead confirms the data run_preflight()
        hands to main() is accurate, which combined with the monkeypatched
        main() tests above proves the full chain end to end."""
        row_id = "r7-candidate-gate-e2e"
        with _pg_conn() as c:
            with c.cursor() as cur:
                cur.execute("DELETE FROM orders WHERE local_order_id = %s", (row_id,))
                cur.execute(
                    """
                    INSERT INTO orders
                        (local_order_id, client_id, kind, status, signal_id,
                         canonical_signal_id, execution_mode, meta)
                    VALUES (%s, %s, 'ENTRY', 'PENDING_TRIGGER', %s, %s, 'paper', %s::jsonb)
                    """,
                    (
                        row_id, "preflight-test@example.com",
                        "sig-preflight-1", "sig-preflight-1",
                        json.dumps({
                            "lifecycle_state": "MATERIALIZING",
                            "materialization_status": "RUNNING",
                            "materialization_generation": 1,
                        }),
                    ),
                )
            c.commit()
        result = run_preflight()
        matching = [r for r in result["rows"] if r["local_order_id"] == row_id]
        assert len(matching) == 1
        assert matching[0]["safe"] is True, (
            "This row is individually well-formed (valid generation, no "
            "counter conflict, no cursor issue) -- it must classify safe "
            "at the row level even though the deployment gate must still "
            "reject the overall run for candidate_row_count > 0"
        )
        assert result["candidate_row_count"] > 0, (
            "candidate_row_count must reflect the inserted row so that "
            "main() has the data it needs to correctly reject this deployment"
        )
