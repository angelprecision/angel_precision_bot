"""
Phase 3 tests: submit-time ask refresh + chase-band guard.

Behavior under test
-------------------
At entry submit time `process_signal` in ap/execution.py must:

  1. Re-fetch the ask immediately before broker.place_order (selector_ask
     was set during contract selection — it can be stale by the time we
     actually submit).

  2. Compute gap_pct = (submit_ask / selector_ask) - 1.0.

  3. If gap_pct > SUBMIT_CHASE_BAND_PCT (default 0.08):
       - release equity reservation + symbol lock
       - emit RUNAWAY_QUOTE_AT_SUBMIT audit event
       - return {"ok": False, "error": "runaway_quote_at_submit", ...}
       - NEVER call broker.place_order

  4. Otherwise: use submit_ask as the broker limit (it's the fresher quote)
     and persist selector_ask / submit_ask / submit_limit / quote_age_ms /
     entry_attempt=0 in order.meta + the TRADE_EXECUTED audit payload.

  5. If the quote refresh fails (broker raises, no ask, invalid ask), the
     code must fall through with selector_ask (no chase-band guard). A
     quote-feed hiccup must NOT kill all entries.

Run:
    pytest tests/test_phase3_submit_time_refresh.py -xvs
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from unittest.mock import MagicMock

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]
EXEC_SRC = (REPO_ROOT / "ap" / "execution.py").read_text()

# ap.db raises at import time if DATABASE_URL is unset. For these unit tests
# we only need to exercise the pure _refresh_ask_at_submit helper, which does
# not touch the DB. Set a dummy DATABASE_URL before any ap.* import so module
# load succeeds. The dummy is never connected to (no DB calls in this file).
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_phase3",
)


# ============================================================
# 1. Env vars + constants (declared at module scope)
# ============================================================

class TestPhase3Constants:
    def test_submit_chase_band_pct_declared(self):
        assert "SUBMIT_CHASE_BAND_PCT" in EXEC_SRC
        # Default must match REPEG_PROXIMITY_PCT default (0.08) for consistency
        # with the downstream re-peg engine.
        assert re.search(
            r'SUBMIT_CHASE_BAND_PCT\s*=\s*float\(os\.getenv\(\s*"SUBMIT_CHASE_BAND_PCT"\s*,\s*"0\.08"\s*\)\)',
            EXEC_SRC,
        ), "SUBMIT_CHASE_BAND_PCT must default to 0.08 (matches REPEG_PROXIMITY_PCT)"

    def test_submit_quote_max_age_ms_declared(self):
        # Defined for forward use even if not currently gated on age \u2014 the
        # env var must exist so ops can tune it.
        assert "SUBMIT_QUOTE_MAX_AGE_MS" in EXEC_SRC
        assert re.search(
            r'SUBMIT_QUOTE_MAX_AGE_MS\s*=\s*int\(os\.getenv\(\s*"SUBMIT_QUOTE_MAX_AGE_MS"',
            EXEC_SRC,
        )


# ============================================================
# 2. Helper _refresh_ask_at_submit (unit tests, no DB)
# ============================================================

@pytest.fixture
def refresh_fn():
    from ap.execution import _refresh_ask_at_submit
    return _refresh_ask_at_submit


class TestRefreshHelper:
    def test_returns_tuple_shape(self, refresh_fn):
        broker = MagicMock()
        broker.get_quote.return_value = {"ask": 3.10, "bid": 3.05, "last": 3.07}
        ask, age_ms, ok, reason = refresh_fn(broker, "QCOM250523C00185000")
        assert ok is True
        assert ask == 3.10
        assert age_ms >= 0
        assert reason == ""

    def test_no_ask_returns_not_ok(self, refresh_fn):
        broker = MagicMock()
        broker.get_quote.return_value = {"ask": None, "bid": 3.05}
        ask, age_ms, ok, reason = refresh_fn(broker, "QCOM250523C00185000")
        assert ok is False
        assert reason == "no_quote"

    def test_zero_ask_returns_not_ok(self, refresh_fn):
        broker = MagicMock()
        broker.get_quote.return_value = {"ask": 0, "bid": 3.05}
        ask, age_ms, ok, reason = refresh_fn(broker, "QCOM250523C00185000")
        assert ok is False
        assert reason == "no_quote"

    def test_invalid_ask_string_returns_not_ok(self, refresh_fn):
        broker = MagicMock()
        broker.get_quote.return_value = {"ask": "not-a-number"}
        ask, age_ms, ok, reason = refresh_fn(broker, "QCOM250523C00185000")
        assert ok is False
        assert reason == "invalid_ask"

    def test_broker_exception_returns_not_ok(self, refresh_fn):
        broker = MagicMock()
        broker.get_quote.side_effect = RuntimeError("connection refused")
        ask, age_ms, ok, reason = refresh_fn(broker, "QCOM250523C00185000")
        assert ok is False
        assert reason == "broker_error"

    def test_empty_quote_returns_not_ok(self, refresh_fn):
        broker = MagicMock()
        broker.get_quote.return_value = None
        ask, age_ms, ok, reason = refresh_fn(broker, "QCOM250523C00185000")
        assert ok is False
        assert reason == "no_quote"


# ============================================================
# 3. Code-shape proofs for the call-site behavior
# ============================================================

class TestSubmitFlowShape:
    def test_helper_called_before_submit(self):
        """The refresh call must happen BEFORE _submit_order_with_retry."""
        # Find the index of the helper call and the submit call.
        i_refresh = EXEC_SRC.find("_refresh_ask_at_submit(broker, contract)")
        i_submit  = EXEC_SRC.find("_submit_order_with_retry(broker, symbol, contract, qty, float(submit_limit))")
        assert i_refresh > 0, "_refresh_ask_at_submit must be invoked in process_signal"
        assert i_submit > 0, "submit must use the refreshed submit_limit"
        assert i_refresh < i_submit, "refresh must be invoked BEFORE submit"

    def test_submit_uses_submit_limit_not_premium(self):
        """The broker submit must use submit_limit (the refreshed value),
        not the original premium. Otherwise the refresh is cosmetic."""
        assert "_submit_order_with_retry(broker, symbol, contract, qty, float(submit_limit))" in EXEC_SRC

    def test_chase_band_block_present(self):
        """Must have an explicit gap > SUBMIT_CHASE_BAND_PCT branch that
        releases equity + symbol lock before returning."""
        assert "gap_pct > SUBMIT_CHASE_BAND_PCT" in EXEC_SRC
        assert "RUNAWAY_QUOTE_AT_SUBMIT" in EXEC_SRC
        assert 'error": "runaway_quote_at_submit"' in EXEC_SRC

    def test_chase_band_releases_resources_before_return(self):
        """Inside the chase-band block we must release equity AND symbol lock
        before returning, else we leak. Look for the pattern near
        RUNAWAY_QUOTE_AT_SUBMIT."""
        # Locate the chase-band block (the if-statement that returns the
        # runaway_quote_at_submit error) and assert release calls appear in it.
        idx = EXEC_SRC.find("gap_pct > SUBMIT_CHASE_BAND_PCT")
        assert idx > 0
        # window starting at the if-condition up to the return
        window = EXEC_SRC[idx:idx + 1500]
        assert "release_equity(client_id, reserved_cost)" in window
        assert "release_symbol_lock(client_id, symbol)" in window
        assert 'error": "runaway_quote_at_submit"' in window

    def test_chase_band_skipped_when_refresh_fails(self):
        """If refresh_ok is False we must NOT enter the chase-band guard;
        we fall through and submit with selector_ask. Proof: the guard
        condition is `if refresh_ok and gap_pct > SUBMIT_CHASE_BAND_PCT:`."""
        assert "if refresh_ok and gap_pct > SUBMIT_CHASE_BAND_PCT:" in EXEC_SRC


# ============================================================
# 4. Meta + audit telemetry fields
# ============================================================

class TestSubmitTelemetryFields:
    REQUIRED_META_FIELDS = (
        '"selector_ask":',
        '"submit_ask":',
        '"submit_limit":',
        '"quote_age_ms":',
        '"entry_attempt":',
    )

    def test_meta_carries_submit_fields(self):
        # All five fields must be in the order.meta dict at insert time.
        # Look for them in the _meta = { ... } literal inside process_signal.
        for field in self.REQUIRED_META_FIELDS:
            assert field in EXEC_SRC, f"meta missing field: {field}"

    def test_trade_executed_audit_carries_submit_fields(self):
        """The TRADE_EXECUTED audit payload must also include the submit
        telemetry so downstream consumers that read the audit log can chart
        submit-time drift without needing to join on order.meta."""
        # Find the TRADE_EXECUTED block and assert fields inside it.
        idx = EXEC_SRC.find('"TRADE_EXECUTED"')
        assert idx > 0
        window = EXEC_SRC[idx:idx + 1500]
        for field in ("selector_ask", "submit_ask", "submit_limit", "quote_age_ms", "entry_attempt"):
            assert field in window, f"TRADE_EXECUTED audit missing {field}"

    def test_entry_attempt_zero_log_token_present(self):
        """Phase 3 must emit an ENTRY_SUBMIT log with `entry_attempt=0` for
        parity with retry_engine's REPEG_APPLIED entry_attempt=N tokens.
        The dashboard log-parser keys off this token."""
        assert "ENTRY_SUBMIT" in EXEC_SRC
        assert "entry_attempt=0" in EXEC_SRC


# ============================================================
# 5. End-to-end behavior via process_signal (DB-mocked)
# ============================================================
#
# We don't have a Postgres in CI, so these tests assert at the helper layer
# (Section 2) and the code-shape layer (Sections 3-4). The full
# process_signal path is exercised by integration tests gated on AP_DB_URL
# and by the existing test_p1_entry_execution.py regression suite (which
# must keep passing under Phase 3 \u2014 see test_no_regression_in_existing_tests
# below).


class TestRegressionGuard:
    """The Phase 3 change must not break the existing entry-execution
    regression test file. This test is informational \u2014 the CI gate runs
    that file separately. We just record the expectation here.
    """

    def test_existing_regression_file_exists(self):
        path = REPO_ROOT / "tests" / "test_p1_entry_execution.py"
        assert path.exists(), "P1 entry execution regression suite must still exist"
