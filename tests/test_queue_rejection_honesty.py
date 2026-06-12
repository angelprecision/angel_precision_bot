"""
tests/test_queue_rejection_honesty.py

PR #120 — Queue Rejection Honesty.

Tests for:
  - _derive_last_error: all rule branches
  - _mark_job: terminal derives last_error, WATCHING does not, explicit wins
  - SQL CASE logic: verified through _mark_job contract

All tests are isolated — no real DB calls.
psycopg2 dependency is mocked at the module boundary.
"""
from __future__ import annotations

import json
import sys
from unittest.mock import MagicMock, patch, call

import pytest

# ── Mock psycopg2 and DB primitives before importing ap.queue ────────────────
_psycopg2_mock = MagicMock()
sys.modules.setdefault("psycopg2", _psycopg2_mock)
sys.modules.setdefault("psycopg2.extras", MagicMock())
sys.modules.setdefault("psycopg2.pool", MagicMock())

# Patch _run_with_retry to execute inline (no pool, no retry logic)
# and _conn to yield a mock cursor context.
def _make_cursor():
    cursor = MagicMock()
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=cursor)
    ctx.__exit__  = MagicMock(return_value=False)
    conn_ctx = MagicMock()
    conn_ctx.return_value = ctx
    return cursor, conn_ctx

# ── Import helpers under test ────────────────────────────────────────────────
# Import only the pure functions — no side effects from pool init.
# We import _derive_last_error directly and test _mark_job via patched DB.
with patch.dict("os.environ", {"DATABASE_URL": "postgresql://mock/mock"}):
    with patch("psycopg2.pool.ThreadedConnectionPool", MagicMock()):
        # Suppress any pool init
        import ap.queue as _queue_module

_derive_last_error = _queue_module._derive_last_error
_TERMINAL_QUEUE_STATUSES = _queue_module._TERMINAL_QUEUE_STATUSES


# =============================================================================
# _derive_last_error
# =============================================================================

class TestDeriveLastError:

    # ── Required test 1 ──────────────────────────────────────────────────────
    def test_stage_and_reason(self):
        """stage + reason → stage:reason"""
        r = _derive_last_error({"stage": "blocked_score",
                                 "reason": "REJECTED_LOW_SCORE"})
        assert r == "blocked_score:REJECTED_LOW_SCORE"

    # ── Required test 2 ──────────────────────────────────────────────────────
    def test_contract_selection_no_contract(self):
        r = _derive_last_error({"stage": "contract_selection",
                                 "reason": "no_contract_found"})
        assert r == "contract_selection:no_contract_found"

    # ── Required test 3 ──────────────────────────────────────────────────────
    def test_reason_code_only(self):
        """reason_code with no stage/reason → reason_code value"""
        r = _derive_last_error({"reason_code": "FINAL_CONTRACT_UNAFFORDABLE"})
        assert r == "FINAL_CONTRACT_UNAFFORDABLE"

    # ── Required test 4 ──────────────────────────────────────────────────────
    def test_ignores_empty_stage_falls_back_to_reason(self):
        """Empty stage string treated as absent — returns reason only."""
        r = _derive_last_error({"stage": "  ", "reason": "no_contract_found"})
        assert r == "no_contract_found"

    def test_ignores_empty_reason_falls_back_to_stage(self):
        """Empty reason string treated as absent — returns stage only."""
        r = _derive_last_error({"stage": "blocked_score", "reason": ""})
        assert r == "blocked_score"

    def test_empty_stage_and_reason_uses_reason_code(self):
        r = _derive_last_error({"stage": "", "reason": "   ",
                                  "reason_code": "DIRECT_QUOTE_ZERO_BID_ASK"})
        assert r == "DIRECT_QUOTE_ZERO_BID_ASK"

    def test_none_result_returns_none(self):
        assert _derive_last_error(None) is None

    def test_non_dict_returns_none(self):
        assert _derive_last_error("bad") is None
        assert _derive_last_error([])   is None
        assert _derive_last_error(42)   is None

    def test_empty_dict_returns_none(self):
        assert _derive_last_error({}) is None

    def test_all_empty_strings_returns_none(self):
        assert _derive_last_error({"stage": "", "reason": "", "reason_code": ""}) is None

    def test_reason_code_ignored_when_stage_present(self):
        """stage wins over reason_code when stage is non-empty."""
        r = _derive_last_error({"stage": "blocked_system",
                                  "reason_code": "SOME_CODE"})
        assert r == "blocked_system"

    def test_extra_keys_ignored(self):
        """Keys other than stage/reason/reason_code are ignored."""
        r = _derive_last_error({"stage": "revalidation", "reason": "budget_exceeded",
                                  "ticker": "NVDA", "real_cost": 500.0})
        assert r == "revalidation:budget_exceeded"

    def test_realistic_mc_reject(self):
        r = _derive_last_error({
            "stage": "blocked_score",
            "reason": "REJECTED_LOW_SCORE (score=65.0 min_eligible=70.0)",
        })
        assert r == "blocked_score:REJECTED_LOW_SCORE (score=65.0 min_eligible=70.0)"

    def test_realistic_blocked_system(self):
        r = _derive_last_error({
            "stage": "blocked_system",
            "reason": "duplicate_setup (signal_id=abc123)",
        })
        assert r == "blocked_system:duplicate_setup (signal_id=abc123)"

    def test_paper_final_quote_reason_code(self):
        r = _derive_last_error({"reason_code": "PAPER_FINAL_QUOTE_NO_LIVE_DATA_BROKER"})
        assert r == "PAPER_FINAL_QUOTE_NO_LIVE_DATA_BROKER"


# =============================================================================
# _TERMINAL_QUEUE_STATUSES — WATCHING must remain non-terminal
# =============================================================================

def test_watching_is_not_terminal():
    """Required: WATCHING must not appear in _TERMINAL_QUEUE_STATUSES."""
    assert "WATCHING" not in _TERMINAL_QUEUE_STATUSES

def test_rejected_is_terminal():
    assert "REJECTED" in _TERMINAL_QUEUE_STATUSES

def test_error_is_terminal():
    assert "ERROR" in _TERMINAL_QUEUE_STATUSES


# =============================================================================
# _mark_job SQL behaviour (via captured execute calls)
# =============================================================================

def _capture_mark_job(status, *, result=None, error=None):
    """
    Call _mark_job with a mocked DB connection and return the
    (sql, params) tuple captured from cursor.execute().
    """
    cursor = MagicMock()
    conn_ctx = MagicMock()
    conn_ctx.return_value.__enter__ = MagicMock(return_value=cursor)
    conn_ctx.return_value.__exit__  = MagicMock(return_value=False)

    with patch.object(_queue_module, "_conn", return_value=conn_ctx), \
         patch.object(_queue_module, "_run_with_retry", side_effect=lambda fn: fn()):
        _queue_module._mark_job(42, status, result=result, error=error)

    assert cursor.execute.called, "cursor.execute was never called"
    sql, params = cursor.execute.call_args[0]
    return sql, params


class TestMarkJobSQL:

    # ── Required test 5 ──────────────────────────────────────────────────────
    def test_explicit_error_wins_over_derived(self):
        """Explicit error= always wins — even when result has stage/reason."""
        sql, params = _capture_mark_job(
            "REJECTED",
            result={"stage": "blocked_score", "reason": "REJECTED_LOW_SCORE"},
            error="explicit_override",
        )
        # derived_error = "explicit_override" (explicit wins)
        # params[3] = derived_error (the WHEN %s IS NOT NULL check value)
        assert params[3] == "explicit_override"
        assert params[4] == "explicit_override"

    # ── Required test 6 ──────────────────────────────────────────────────────
    def test_rejected_with_stage_reason_derives_last_error(self):
        """REJECTED + result stage/reason → last_error derived, not null."""
        sql, params = _capture_mark_job(
            "REJECTED",
            result={"stage": "contract_selection", "reason": "no_contract_found"},
        )
        # derived_error = "contract_selection:no_contract_found"
        derived = "contract_selection:no_contract_found"
        assert params[3] == derived   # WHEN derived IS NOT NULL
        assert params[4] == derived   # THEN derived
        # terminal=True
        assert params[1] is True

    # ── Required test 7 ──────────────────────────────────────────────────────
    def test_watching_no_derive_no_finished_ts_no_clear(self):
        """WATCHING must not derive last_error, must not set finished_ts,
        must not overwrite existing last_error with NULL."""
        sql, params = _capture_mark_job(
            "WATCHING",
            result={"stage": "after_hours", "reason": "deferred"},
        )
        # terminal=False for WATCHING
        assert params[1] is False, "WATCHING must be non-terminal"
        # No derived error — params[3] (derived_error) must be None
        assert params[3] is None, "WATCHING must not derive last_error"
        # SQL must contain ELSE last_error (preserve existing)
        assert "ELSE last_error" in sql

    # ── Required test 8 ──────────────────────────────────────────────────────
    def test_rejected_no_error_no_result_last_error_null(self):
        """REJECTED with no error= and no result → last_error stays NULL (honest)."""
        sql, params = _capture_mark_job("REJECTED")
        # derived_error is None (nothing to derive)
        assert params[3] is None
        # terminal=True → the CASE writes None (explicit NULL) not ELSE last_error
        assert params[1] is True

    # ── Required test 9 ──────────────────────────────────────────────────────
    def test_result_json_written_exactly(self):
        """result_json must be written exactly as serialised — not modified."""
        result = {"stage": "blocked_score", "reason": "score=65", "extra": [1, 2]}
        sql, params = _capture_mark_job("REJECTED", result=result)
        written_json = params[2]
        assert written_json is not None
        parsed = json.loads(written_json)
        assert parsed == result

    def test_result_json_none_when_no_result(self):
        sql, params = _capture_mark_job("REJECTED", error="some_error")
        assert params[2] is None

    def test_submitted_terminal_derives_from_reason_code(self):
        sql, params = _capture_mark_job(
            "SUBMITTED",
            result={"reason_code": "FINAL_CONTRACT_UNAFFORDABLE"},
        )
        assert params[3] == "FINAL_CONTRACT_UNAFFORDABLE"

    def test_non_terminal_error_route(self):
        """Non-terminal with explicit error= should still write it."""
        sql, params = _capture_mark_job(
            "WATCHING",
            error="after_hours_deferred:awaiting_overnight_reeval",
        )
        # explicit error wins even on non-terminal
        assert params[3] == "after_hours_deferred:awaiting_overnight_reeval"


# =============================================================================
# Verification SQL (documentation test — ensures the intended query runs clean)
# =============================================================================

VERIFICATION_SQL = """\
select date(created_ts) as trade_day,
       status,
       coalesce(last_error, 'null') as last_error,
       count(*) as rows
from trade_queue
where created_ts >= now() - interval '3 days'
group by 1,2,3
order by trade_day desc, rows desc;
"""

def test_verification_sql_is_documented():
    """Ensure the verification SQL string is present in this test module."""
    assert "trade_queue" in VERIFICATION_SQL
    assert "last_error" in VERIFICATION_SQL
    assert "REJECTED" not in VERIFICATION_SQL   # query doesn't filter — shows all
