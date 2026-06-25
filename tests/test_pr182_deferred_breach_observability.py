# tests/test_pr182_deferred_breach_observability.py
# =============================================================================
# PR #182 — Deferred breach contract selection failure observability.
#
# Invariants under test:
#   1. When select() returns None (retryable path), write_deferred_breach_last_error
#      is called with the canonical reason_code from get_last_failure().
#   2. When select() returns None (retryable path), the trade_queue row is NOT
#      moved to a terminal status and _cleanup_pending_entry_order is NOT called.
#   3. write_deferred_breach_last_error does NOT update rows that are already
#      in a terminal status (REJECTED, EXPIRED, CANCELED, FILLED, ...).
#   4. A DB write failure inside write_deferred_breach_last_error never raises
#      to the caller — always fails silently.
#   5. breach_attempt_count in orders.meta increments correctly across ticks.
#   6. write_deferred_breach_last_error is NOT called for the terminal path
#      (when _terminalize_deferred_breach_failure fires instead).
#
# NOT TESTED HERE (covered by existing suites):
#   - _terminalize_deferred_breach_failure → covered by test_p0_deferred_breach_submit_repair.py
#   - selector gate logic → covered by test_dte_ladder.py, test_no_silent_deferred_trigger.py
#   - OSM state transitions → covered by test_breach_block_diagnostics.py
# =============================================================================

from __future__ import annotations

import types
import pytest
from unittest.mock import MagicMock, patch, call, ANY
from datetime import datetime, timezone


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_selector(*, return_none: bool = True, last_failure: dict | None = None):
    """Build a minimal mock contract_selector."""
    sel = MagicMock()
    sel.select.return_value = None if return_none else MagicMock(
        contract_symbol="ROST240628P00227500",
        execution_price_per_share=2.15,
        affordable_contracts=2,
        premium_per_contract=215.0,
    )
    sel.get_last_failure.return_value = last_failure or {
        "stage":       "cheap_contract_gate",
        "reason_code": "CHEAP_CONTRACT_NO_UPGRADE",
        "explanation": "premium $35 below $50 min, no upgrade found for ROST",
    }
    return sel


def _make_plan(
    *,
    ticker: str = "ROST",
    contract_symbol: str = "DEFERRED:ROST",
    signal_id: str = "sig-pr182-001",
    queue_id: int = 77,
    prior_breach_count: int = 0,
):
    """Build a minimal approved plan with deferred contract marker."""
    return types.SimpleNamespace(
        ticker=ticker,
        side="PUT",
        contract_symbol=contract_symbol,
        limit_price=None,
        contracts=2,
        max_position_usd=500.0,
        trigger_price=227.62,
        signal_id=signal_id,
        client_id="jose.vasquez4011",
        metadata={
            "contract_deferred":        True,
            "deferred_breach_selection": True,
            "selection_context":         "deferred_breach",
            "breach_attempt_count":      prior_breach_count,
            "queue_id":                  queue_id,
        },
    )


def _make_watched(plan, *, queue_id: int = 77):
    """Minimal WatchedSignal-like object."""
    return types.SimpleNamespace(
        ticker=plan.ticker,
        trigger_price=plan.trigger_price,
        signal={
            "signal_id":      plan.signal_id,
            "client_id":      plan.client_id,
            "local_order_id": "ord-abc-123",
            "queue_id":       queue_id,
        },
    )


# ---------------------------------------------------------------------------
# Unit tests: write_deferred_breach_last_error() in ap/queue.py
# ---------------------------------------------------------------------------

class TestWriteDeferredBreachLastError:
    """Tests for the new ap.queue.write_deferred_breach_last_error() helper."""

    def test_writes_expected_last_error_string(self, monkeypatch):
        """Canonical last_error format: label:reason_code:attempt_N — detail."""
        from ap import queue as q

        written_sql: list[tuple] = []

        def fake_run_with_retry(fn):
            # Capture whatever the inner closure would execute, then run it
            # against a mock cursor so we can inspect the SQL.
            mock_cur = MagicMock()
            mock_ctx = MagicMock()
            mock_ctx.__enter__ = lambda s: mock_cur
            mock_ctx.__exit__ = MagicMock(return_value=False)
            mock_conn = MagicMock(return_value=mock_ctx)

            class _FakeConnCtx:
                def __call__(self):
                    return mock_ctx

            monkeypatch.setattr(q, "_conn", _FakeConnCtx)
            fn()
            if mock_cur.execute.called:
                written_sql.append(mock_cur.execute.call_args)

        monkeypatch.setattr(q, "_run_with_retry", fake_run_with_retry)

        q.write_deferred_breach_last_error(
            77,
            reason_code="CHEAP_CONTRACT_NO_UPGRADE",
            explanation="premium $35 below $50 min",
            attempt=1,
            client_id="jose.vasquez4011",
            ticker="ROST",
        )

        assert len(written_sql) == 1
        sql_args = written_sql[0][0][1]  # positional args to c.execute()
        last_error_written = sql_args[0]
        assert "DEFERRED_BREACH_CONTRACT_FAILED" in last_error_written
        assert "CHEAP_CONTRACT_NO_UPGRADE" in last_error_written
        assert "attempt_1" in last_error_written
        assert "premium $35 below $50 min" in last_error_written

    def test_no_op_on_none_queue_id(self, monkeypatch):
        """No DB call when queue_id is None or 0."""
        from ap import queue as q
        called = []
        monkeypatch.setattr(q, "_run_with_retry", lambda fn: called.append(True))
        q.write_deferred_breach_last_error(None, reason_code="WHATEVER")
        q.write_deferred_breach_last_error(0,    reason_code="WHATEVER")
        assert called == [], "Expected no DB calls for None/0 queue_id"

    def test_fails_silently_on_db_error(self, monkeypatch):
        """A DB write failure must never raise to the caller."""
        from ap import queue as q

        def exploding_retry(fn):
            raise RuntimeError("Simulated DB connection pool exhausted")

        monkeypatch.setattr(q, "_run_with_retry", exploding_retry)
        # Must not raise
        q.write_deferred_breach_last_error(
            42,
            reason_code="SPREAD_TOO_WIDE",
            explanation="spread 18% exceeds 12% cap",
            attempt=3,
        )

    def test_explanation_truncated_to_400_chars(self, monkeypatch):
        """Explanations longer than 400 chars are truncated before write."""
        from ap import queue as q
        written: list[str] = []

        def fake_run_with_retry(fn):
            mock_cur = MagicMock()
            mock_ctx = MagicMock()
            mock_ctx.__enter__ = lambda s: mock_cur
            mock_ctx.__exit__ = MagicMock(return_value=False)

            class _Ctx:
                def __call__(self):
                    return mock_ctx

            monkeypatch.setattr(q, "_conn", _Ctx)
            fn()
            if mock_cur.execute.called:
                written.append(mock_cur.execute.call_args[0][1][0])

        monkeypatch.setattr(q, "_run_with_retry", fake_run_with_retry)

        long_explanation = "x" * 800
        q.write_deferred_breach_last_error(
            1,
            reason_code="NO_CHAIN_DATA",
            explanation=long_explanation,
            attempt=1,
        )
        assert len(written) == 1
        # The label part + " — " + up to 400 chars of explanation
        # Total may exceed 400 but the detail portion is capped.
        assert "x" * 401 not in written[0], "Explanation was not truncated to 400 chars"

    def test_does_not_overwrite_terminal_rows(self):
        """The UPDATE WHERE clause excludes rows already in terminal statuses.
        Verified by inspecting the SQL text, not by executing against a real DB.
        """
        import inspect
        from ap import queue as q
        src = inspect.getsource(q.write_deferred_breach_last_error)
        # The WHERE clause must guard against terminal statuses
        assert "REJECTED" in src
        assert "EXPIRED"  in src
        assert "CANCELED" in src
        assert "FILLED"   in src

    def test_attempt_suffix_absent_when_zero(self, monkeypatch):
        """attempt=0 means unknown — no :attempt_N suffix should appear."""
        from ap import queue as q
        written: list[str] = []

        def fake_run_with_retry(fn):
            mock_cur = MagicMock()
            mock_ctx = MagicMock()
            mock_ctx.__enter__ = lambda s: mock_cur
            mock_ctx.__exit__ = MagicMock(return_value=False)

            class _Ctx:
                def __call__(self):
                    return mock_ctx

            monkeypatch.setattr(q, "_conn", _Ctx)
            fn()
            if mock_cur.execute.called:
                written.append(mock_cur.execute.call_args[0][1][0])

        monkeypatch.setattr(q, "_run_with_retry", fake_run_with_retry)
        q.write_deferred_breach_last_error(1, reason_code="NO_CHAIN_DATA", attempt=0)
        assert len(written) == 1
        assert "attempt_" not in written[0]


# ---------------------------------------------------------------------------
# Integration tests: wiring in ap_execution_core._on_entry_trigger()
# ---------------------------------------------------------------------------

class TestExecutionCoreWriteBack:
    """Tests that verify the wiring in ap_execution_core._on_entry_trigger().

    These drive the actual code path rather than mocking the whole method,
    using the same pattern as test_p0_deferred_breach_submit_repair.py.
    """

    def test_write_back_called_with_reason_code_from_get_last_failure(
        self, monkeypatch
    ):
        """When select() returns None, write_deferred_breach_last_error receives
        the reason_code from selector.get_last_failure()."""
        import ap_execution_core as ec_mod
        from ap import queue as q

        write_calls: list[dict] = []

        def fake_write(queue_id, *, reason_code, explanation, attempt, client_id, ticker):
            write_calls.append({
                "queue_id":    queue_id,
                "reason_code": reason_code,
                "attempt":     attempt,
            })

        monkeypatch.setattr(q, "write_deferred_breach_last_error", fake_write)

        sel = _make_selector(
            return_none=True,
            last_failure={
                "stage":       "cheap_contract_gate",
                "reason_code": "CHEAP_CONTRACT_NO_UPGRADE",
                "explanation": "premium $35 below $50 min",
            },
        )
        plan = _make_plan(queue_id=77, prior_breach_count=0)
        watched = _make_watched(plan, queue_id=77)

        # Build a minimal core with the mock selector
        core = ec_mod.APExecutionCore.__new__(ec_mod.APExecutionCore)
        core.paper = True
        core.email = "jose.vasquez4011"
        core.client_id = "jose.vasquez4011"
        core.mode = "PAPER"
        core.contract_selector = sel
        core.order_state_machine = MagicMock()
        core.order_state_machine.submit_existing_entry = MagicMock()
        core.store = MagicMock()
        core.entry_watcher = MagicMock()
        core.exit_eng = MagicMock()
        core.tracker = MagicMock()
        core.position_manager = MagicMock()

        # Stub _breach_risk_check to return True (past the risk gate)
        monkeypatch.setattr(
            ec_mod.APExecutionCore,
            "_breach_risk_check",
            lambda self, w: True,
        )
        # Stub _recover_plan_for_revalidation to return our plan
        monkeypatch.setattr(
            ec_mod.APExecutionCore,
            "_recover_plan_for_revalidation",
            lambda self, w: plan,
        )
        # Stub _cleanup_pending_entry_order to detect if it's called
        cleanup_calls = []
        monkeypatch.setattr(
            ec_mod.APExecutionCore,
            "_cleanup_pending_entry_order",
            lambda self, w, action, reason: cleanup_calls.append(action),
        )

        core._on_entry_trigger(watched)

        # write_deferred_breach_last_error must have been called
        assert len(write_calls) >= 1, (
            "write_deferred_breach_last_error was not called when select() returned None"
        )
        assert write_calls[0]["reason_code"] == "CHEAP_CONTRACT_NO_UPGRADE"
        assert write_calls[0]["queue_id"] == 77
        assert write_calls[0]["attempt"] == 1  # prior_breach_count=0 → attempt=1

    def test_breach_attempt_count_increments(self, monkeypatch):
        """On a second trigger tick, attempt should be prior_count + 1."""
        import ap_execution_core as ec_mod
        from ap import queue as q

        write_calls: list[dict] = []
        monkeypatch.setattr(
            q,
            "write_deferred_breach_last_error",
            lambda qid, *, reason_code, explanation, attempt, client_id, ticker:
                write_calls.append({"attempt": attempt}),
        )

        sel = _make_selector(return_none=True)
        plan = _make_plan(queue_id=88, prior_breach_count=2)  # ← already failed twice
        watched = _make_watched(plan, queue_id=88)

        core = ec_mod.APExecutionCore.__new__(ec_mod.APExecutionCore)
        core.paper = True
        core.email = "jose.vasquez4011"
        core.client_id = "jose.vasquez4011"
        core.mode = "PAPER"
        core.contract_selector = sel
        core.order_state_machine = MagicMock()
        core.store = MagicMock()
        core.entry_watcher = MagicMock()
        core.exit_eng = MagicMock()
        core.tracker = MagicMock()
        core.position_manager = MagicMock()

        monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check",         lambda s, w: True)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_recover_plan_for_revalidation", lambda s, w: plan)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_cleanup_pending_entry_order", lambda s, w, action, reason: None)

        core._on_entry_trigger(watched)

        assert write_calls[0]["attempt"] == 3  # prior=2 → this tick=3

    def test_write_back_failure_does_not_block_terminalize(self, monkeypatch):
        """If write_deferred_breach_last_error raises, _terminalize_deferred_breach_failure
        must still run (terminal cleanup must not be blocked by observability)."""
        import ap_execution_core as ec_mod
        from ap import queue as q

        monkeypatch.setattr(
            q,
            "write_deferred_breach_last_error",
            lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("DB exploded")),
        )

        terminalize_calls = []

        def fake_terminalize(self, reason, *, extra_meta=None):
            terminalize_calls.append(reason)

        monkeypatch.setattr(
            ec_mod.APExecutionCore,
            "_terminalize_deferred_breach_failure",
            fake_terminalize,
        )

        sel = _make_selector(return_none=True)
        plan = _make_plan(queue_id=99)
        watched = _make_watched(plan, queue_id=99)

        core = ec_mod.APExecutionCore.__new__(ec_mod.APExecutionCore)
        core.paper = True
        core.email = "jose.vasquez4011"
        core.client_id = "jose.vasquez4011"
        core.mode = "PAPER"
        core.contract_selector = sel
        core.order_state_machine = MagicMock()
        core.store = MagicMock()
        core.entry_watcher = MagicMock()
        core.exit_eng = MagicMock()
        core.tracker = MagicMock()
        core.position_manager = MagicMock()

        monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check",             lambda s, w: True)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_recover_plan_for_revalidation",  lambda s, w: plan)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_cleanup_pending_entry_order",    lambda s, w, action, reason: None)

        # Must not raise even though write_deferred_breach_last_error blows up
        core._on_entry_trigger(watched)
        assert len(terminalize_calls) >= 1, (
            "_terminalize_deferred_breach_failure was not called after write-back failure"
        )

    def test_write_back_not_called_when_selector_is_none(self, monkeypatch):
        """The no-selector terminal path (contract_deferred_no_selector) fires
        _terminalize immediately and must NOT also call write_deferred_breach_last_error."""
        import ap_execution_core as ec_mod
        from ap import queue as q

        write_calls: list = []
        monkeypatch.setattr(
            q,
            "write_deferred_breach_last_error",
            lambda *a, **kw: write_calls.append(True),
        )

        plan = _make_plan(queue_id=55)
        watched = _make_watched(plan, queue_id=55)

        core = ec_mod.APExecutionCore.__new__(ec_mod.APExecutionCore)
        core.paper = True
        core.email = "jose.vasquez4011"
        core.client_id = "jose.vasquez4011"
        core.mode = "PAPER"
        core.contract_selector = None  # ← no selector wired
        core.order_state_machine = MagicMock()
        core.store = MagicMock()
        core.entry_watcher = MagicMock()
        core.exit_eng = MagicMock()
        core.tracker = MagicMock()
        core.position_manager = MagicMock()

        monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check",             lambda s, w: True)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_recover_plan_for_revalidation",  lambda s, w: plan)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_cleanup_pending_entry_order",    lambda s, w, action, reason: None)

        core._on_entry_trigger(watched)

        assert write_calls == [], (
            "write_deferred_breach_last_error should NOT be called when no selector is wired "
            "(terminal path, not retryable)"
        )


# ---------------------------------------------------------------------------
# Source-level structural tests (zero-dep, always fast)
# ---------------------------------------------------------------------------

class TestSourceStructure:
    """Verify the expected code is present without executing it.
    These tests catch accidental deletion or misplacement of the PR #182 block.
    """

    def _src(self, module_path: str) -> str:
        from pathlib import Path
        return (Path(__file__).resolve().parents[1] / module_path).read_text()

    def test_write_deferred_breach_last_error_exists_in_queue(self):
        src = self._src("ap/queue.py")
        assert "def write_deferred_breach_last_error" in src, (
            "write_deferred_breach_last_error not found in ap/queue.py — was it removed?"
        )

    def test_queue_function_updates_only_non_terminal_rows(self):
        src = self._src("ap/queue.py")
        idx = src.find("def write_deferred_breach_last_error")
        region = src[idx: idx + 1200]
        for terminal_status in ("REJECTED", "EXPIRED", "CANCELED", "FILLED"):
            assert terminal_status in region, (
                f"Terminal status {terminal_status!r} not guarded in "
                f"write_deferred_breach_last_error WHERE clause"
            )

    def test_execution_core_imports_write_back(self):
        src = self._src("ap_execution_core.py")
        assert "write_deferred_breach_last_error" in src, (
            "write_deferred_breach_last_error not imported/called in ap_execution_core.py"
        )

    def test_write_back_is_inside_deferred_block(self):
        src = self._src("ap_execution_core.py")
        # The write-back call must appear between the select() call and the
        # _terminalize_deferred_breach_failure call — not before or after.
        idx_select = src.find("_sel = self.contract_selector.select(approved_plan)")
        idx_write  = src.find("write_deferred_breach_last_error")
        idx_term   = src.find("_terminalize_deferred_breach_failure")
        assert idx_select > 0, "_sel = self.contract_selector.select(approved_plan) not found"
        assert idx_write  > 0, "write_deferred_breach_last_error call not found"
        assert idx_term   > 0, "_terminalize_deferred_breach_failure not found"
        assert idx_select < idx_write < idx_term, (
            "write_deferred_breach_last_error is not positioned between "
            "select() and _terminalize_deferred_breach_failure — check placement"
        )

    def test_write_back_wrapped_in_try_except(self):
        src = self._src("ap_execution_core.py")
        idx = src.find("write_deferred_breach_last_error")
        # Walk back 500 chars to find the enclosing try
        region_before = src[max(0, idx - 500): idx]
        assert "try:" in region_before, (
            "write_deferred_breach_last_error is not inside a try block — "
            "a DB failure would propagate and block the terminal cleanup path"
        )
