# tests/test_pr223_deferred_breach_diagnostics_preserved.py
# =============================================================================
# PR #223 amendment — restore deferred-breach selector diagnostic write-back.
#
# PR #223 ("entry confirmation observe-mode continuation rollout") rewrote
# large sections of ap_execution_core.py and ap_entry_confirmation.py. The
# rewrite accidentally dropped the PR #182 write-back block from both
# deferred-breach failure paths inside _on_entry_trigger():
#
#   Path A: contract_selector.select() returns None
#   Path B: selector returns a value but the plan is left with an unresolved
#           DEFERRED:* contract placeholder after copy-back
#
# This file proves the restoration:
#   1. Path A calls write_deferred_breach_last_error() before terminal cleanup,
#      and records breach_attempt_count + reason_code in order meta.
#   2. Path B calls write_deferred_breach_last_error() before terminal cleanup,
#      and records the unresolved-placeholder reason in order meta.
#   3. Observe-mode daily continuation still reaches broker submit (unchanged
#      by this amendment — regression guard only).
#   4. Enforce-mode daily continuation still blocks submit (unchanged by this
#      amendment — regression guard only).
#
# Source-level guards additionally prove both write-back blocks are physically
# present and wired before the corresponding _terminalize_deferred_breach_failure
# call in the source file, so a future rewrite cannot silently drop them again
# without this test failing on the missing literal text.
# =============================================================================

from __future__ import annotations

import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

_REPO = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Source-level guards — fastest, zero-dependency proof the blocks exist
# ---------------------------------------------------------------------------

class TestSourcePreservesWriteBackBlocks:
    """Static proof that both PR182 write-back blocks are present and
    positioned correctly relative to contract selection and terminal cleanup.
    These tests need no imports and cannot be defeated by a broken
    dependency chain — they read the file as text.
    """

    def _src(self) -> str:
        return (_REPO / "ap_execution_core.py").read_text()

    def test_write_deferred_breach_last_error_imported_twice(self):
        """Both Path A and Path B must import write_deferred_breach_last_error
        from ap.queue. Two occurrences of the import line is the structural
        signature of both paths being wired."""
        src = self._src()
        count = src.count("from ap.queue import write_deferred_breach_last_error")
        assert count == 2, (
            f"Expected 2 occurrences of the write_deferred_breach_last_error "
            f"import (Path A + Path B), found {count}. One or both PR182 "
            f"write-back blocks may have been dropped."
        )

    def test_write_deferred_breach_last_error_called_twice(self):
        src = self._src()
        count = src.count("write_deferred_breach_last_error(")
        # 2 calls + 0 (the import line itself doesn't contain the open-paren form)
        assert count >= 2, (
            f"Expected at least 2 calls to write_deferred_breach_last_error(), "
            f"found {count}."
        )

    def test_path_a_write_back_precedes_terminalize(self):
        """Path A: select() returns None. write_deferred_breach_last_error
        must appear after the BREACH_SELECTOR_RETURNED_NONE emit and before
        the first _terminalize_deferred_breach_failure call that follows it."""
        src = self._src()
        idx_not_sel = src.find("if not _sel or not _live_contract:")
        assert idx_not_sel != -1, "Path A guard 'if not _sel or not _live_contract:' not found"

        idx_write = src.find("write_deferred_breach_last_error(", idx_not_sel)
        idx_terminalize = src.find("_terminalize_deferred_breach_failure(", idx_not_sel)

        assert idx_write != -1, "write_deferred_breach_last_error not found after Path A guard"
        assert idx_terminalize != -1, "_terminalize_deferred_breach_failure not found after Path A guard"
        assert idx_not_sel < idx_write < idx_terminalize, (
            "Path A write-back must run AFTER the selector-failure branch is "
            "entered and BEFORE the terminal cleanup call. "
            f"guard={idx_not_sel} write={idx_write} terminalize={idx_terminalize}"
        )

    def test_path_b_write_back_precedes_terminalize(self):
        """Path B: selector returned a value but contract_symbol is still an
        unresolved DEFERRED:* placeholder. write_deferred_breach_last_error
        must appear after that guard and before its _terminalize call.

        Anchored on the exact `if _live_contract.upper().startswith(...)`
        guard statement, not the earlier `_plan_is_placeholder = ...`
        assignment which shares the same substring."""
        src = self._src()
        idx_deferred_guard = src.find(
            'if _live_contract.upper().startswith("DEFERRED:"):'
        )
        assert idx_deferred_guard != -1, "Path B guard statement not found"

        idx_write = src.find("write_deferred_breach_last_error(", idx_deferred_guard)
        idx_terminalize = src.find("_terminalize_deferred_breach_failure(", idx_deferred_guard)

        assert idx_write != -1, "write_deferred_breach_last_error not found after Path B guard"
        assert idx_terminalize != -1, "_terminalize_deferred_breach_failure not found after Path B guard"
        assert idx_deferred_guard < idx_write < idx_terminalize, (
            "Path B write-back must run AFTER the DEFERRED: placeholder guard "
            "is entered and BEFORE the terminal cleanup call. "
            f"guard={idx_deferred_guard} write={idx_write} terminalize={idx_terminalize}"
        )

    def test_path_a_writeback_wrapped_in_try_except(self):
        """The write-back must be best-effort — wrapped in try/except so a
        DB failure cannot block terminal cleanup. Anchored to the import
        line (which is the first statement inside the try: block) rather
        than the call site, since the call sits several statements below
        the try: opener."""
        src = self._src()
        idx_not_sel = src.find("if not _sel or not _live_contract:")
        idx_import = src.find(
            "from ap.queue import write_deferred_breach_last_error", idx_not_sel
        )
        assert idx_import != -1, "Path A write-back import not found"
        region_before = src[max(0, idx_import - 100):idx_import]
        assert "try:" in region_before, (
            "Path A write-back import is not immediately preceded by a "
            "try: block — best-effort guarantee may be missing"
        )

    def test_path_b_writeback_wrapped_in_try_except(self):
        src = self._src()
        # Anchor on the literal guard statement (the `if` line), not the
        # earlier `_plan_is_placeholder = ... .startswith("DEFERRED:")`
        # assignment which shares the same substring.
        idx_deferred_guard = src.find(
            'if _live_contract.upper().startswith("DEFERRED:"):'
        )
        assert idx_deferred_guard != -1, "Path B guard statement not found"
        idx_import = src.find(
            "from ap.queue import write_deferred_breach_last_error", idx_deferred_guard
        )
        assert idx_import != -1, "Path B write-back import not found"
        region_before = src[max(0, idx_import - 100):idx_import]
        assert "try:" in region_before, (
            "Path B write-back import is not immediately preceded by a "
            "try: block — best-effort guarantee may be missing"
        )

    def test_order_meta_fields_present_for_both_paths(self):
        """Both paths must update order meta with all five required fields."""
        src = self._src()
        required_keys = [
            '"breach_attempt_count"',
            '"last_breach_failure_reason"',
            '"last_breach_failure_reason_code"',
            '"last_breach_failure_at"',
            '"last_breach_selector_audit"',
        ]
        for key in required_keys:
            count = src.count(key)
            assert count >= 2, (
                f"Order meta key {key} must appear at least twice "
                f"(once per path), found {count} occurrence(s)."
            )

    def test_update_order_meta_called_for_both_paths(self):
        src = self._src()
        count = src.count('getattr(self.order_state_machine, "update_order_meta", None)')
        assert count >= 2, (
            f"update_order_meta lookup must appear in both Path A and Path B, "
            f"found {count} occurrence(s)."
        )

    def test_path_a_reason_code_default(self):
        """Path A default reason code, when the selector emitted no specific
        REJECT, must be BREACH_SELECTOR_RETURNED_NONE."""
        src = self._src()
        idx_not_sel = src.find("if not _sel or not _live_contract:")
        idx_write = src.find("write_deferred_breach_last_error(", idx_not_sel)
        region = src[idx_not_sel:idx_write]
        assert '"BREACH_SELECTOR_RETURNED_NONE"' in region

    def test_path_b_reason_code_default(self):
        """Path B default reason code must be DEFERRED_UNRESOLVED_AT_BREACH."""
        src = self._src()
        idx_deferred_guard = src.find(
            'if _live_contract.upper().startswith("DEFERRED:"):'
        )
        assert idx_deferred_guard != -1, "Path B guard statement not found"
        idx_write = src.find("write_deferred_breach_last_error(", idx_deferred_guard)
        region = src[idx_deferred_guard:idx_write]
        assert '"DEFERRED_UNRESOLVED_AT_BREACH"' in region


class TestEntryConfirmationDocstringAccurate:
    """The header docstring in ap_entry_confirmation.py must not claim the
    gate is a no-op when confirmation_required is absent — daily continuation
    still runs in observe/enforce mode regardless of that flag."""

    def _src(self) -> str:
        return (_REPO / "ap_entry_confirmation.py").read_text()

    def test_docstring_mentions_daily_continuation_mode(self):
        src = self._src()
        idx = src.find("WHEN confirmation_required IS NOT SET")
        assert idx != -1, "docstring section header not found"
        section = src[idx:idx + 400]
        assert "ENABLE_DAILY_CONTINUATION_MODE" in section, (
            "Docstring must mention that daily continuation still runs "
            "according to ENABLE_DAILY_CONTINUATION_MODE even when "
            "confirmation_required is absent"
        )

    def test_docstring_no_longer_claims_full_no_op(self):
        src = self._src()
        idx = src.find("WHEN confirmation_required IS NOT SET")
        section = src[idx:idx + 400]
        assert "non-client signals (operator, test, paper-only)\nflow through unchanged." not in section, (
            "Docstring still claims the gate is a full no-op when "
            "confirmation_required is absent — this is inaccurate since "
            "daily continuation runs independently of that flag"
        )

    def test_docstring_mentions_diagnostic_only_observe(self):
        src = self._src()
        idx = src.find("WHEN confirmation_required IS NOT SET")
        section = src[idx:idx + 400]
        assert "diagnostic-only" in section.lower() or "diagnostic only" in section.lower()


# ---------------------------------------------------------------------------
# Functional / runtime proof — drives the real code path
# ---------------------------------------------------------------------------
#
# These tests import ap_execution_core directly and execute _on_entry_trigger
# end-to-end through the real deferred-breach branches. They require the
# production module graph to be importable; if your local environment cannot
# satisfy that (e.g. missing yfinance or DB-backed ap.* submodules), use the
# source-level guards above as the fast/cheap proof and run these in CI where
# the full dependency tree is installed.

def _has_ap_execution_core():
    try:
        import ap_execution_core  # noqa: F401
        return True
    except Exception:
        return False


pytestmark_runtime = pytest.mark.skipif(
    not _has_ap_execution_core(),
    reason="ap_execution_core and its dependency graph are not importable "
           "in this environment — source-level guards above already prove "
           "the restoration; skip the runtime proof here.",
)


def _make_plan(*, queue_id: int = 501, prior_breach_count: int = 0, contract_symbol: str = "DEFERRED:ROST"):
    return SimpleNamespace(
        ticker="ROST",
        side="PUT",
        contract_symbol=contract_symbol,
        limit_price=None,
        contracts=2,
        max_position_usd=500.0,
        trigger_price=227.62,
        signal_id="sig-pr223-001",
        client_id="jose.vasquez4011",
        metadata={
            "contract_deferred": True,
            "deferred_breach_selection": True,
            "selection_context": "deferred_breach",
            "local_order_id": "ord-pr223-001",
            "queue_id": queue_id,
            "trade_queue_id": queue_id,
            "breach_attempt_count": prior_breach_count,
        },
    )


def _make_watched(plan, *, queue_id: int):
    return SimpleNamespace(
        ticker=plan.ticker,
        trigger_price=plan.trigger_price,
        signal={
            "signal_id": plan.signal_id,
            "client_id": plan.client_id,
            "local_order_id": "ord-pr223-001",
            "queue_id": queue_id,
            "trade_queue_id": queue_id,
        },
    )


def _bare_core(contract_selector):
    import ap_execution_core as ec_mod
    core = ec_mod.APExecutionCore.__new__(ec_mod.APExecutionCore)
    core.paper = True
    core.email = "jose.vasquez4011"
    core.client_id = "jose.vasquez4011"
    core.mode = "PAPER"
    core.contract_selector = contract_selector
    core.order_state_machine = MagicMock()
    core.store = MagicMock()
    core.entry_watcher = MagicMock()
    core.exit_eng = MagicMock()
    core.tracker = MagicMock()
    core.position_manager = MagicMock()
    return core


@pytestmark_runtime
class TestPathANoneSelectorWriteBackRestored:
    """Spec 1: deferred selector returns None."""

    def test_write_back_called_before_terminal_cleanup(self, monkeypatch):
        import ap_execution_core as ec_mod
        from ap import queue as q

        write_calls = []
        monkeypatch.setattr(
            q, "write_deferred_breach_last_error",
            lambda qid, *, reason_code, explanation, attempt, client_id, ticker:
                write_calls.append({
                    "queue_id": qid, "reason_code": reason_code, "attempt": attempt,
                }),
        )

        sel = MagicMock()
        sel.select.return_value = None
        sel.get_last_failure.return_value = {
            "stage": "cheap_contract_gate",
            "reason_code": "CHEAP_CONTRACT_NO_UPGRADE",
            "explanation": "premium $35 below $50 min, no upgrade found",
        }

        plan = _make_plan(queue_id=501)
        watched = _make_watched(plan, queue_id=501)
        core = _bare_core(sel)

        monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check", lambda s, w: True)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_recover_plan_for_revalidation", lambda s, w: plan)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_cleanup_pending_entry_order", lambda s, w, action, reason: None)

        core._on_entry_trigger(watched)

        assert len(write_calls) >= 1, "write_deferred_breach_last_error not called for Path A"
        assert write_calls[0]["queue_id"] == 501
        assert write_calls[0]["reason_code"] == "CHEAP_CONTRACT_NO_UPGRADE"
        assert write_calls[0]["attempt"] == 1

    def test_order_meta_records_breach_attempt_and_reason(self, monkeypatch):
        import ap_execution_core as ec_mod
        from ap import queue as q

        monkeypatch.setattr(q, "write_deferred_breach_last_error", lambda *a, **kw: None)

        sel = MagicMock()
        sel.select.return_value = None
        sel.get_last_failure.return_value = {
            "stage": "quality_filter",
            "reason_code": "OI_TOO_LOW",
            "explanation": "chain had 38 rows, 0 survived OI gate",
        }

        plan = _make_plan(queue_id=502, prior_breach_count=2)
        watched = _make_watched(plan, queue_id=502)
        core = _bare_core(sel)

        monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check", lambda s, w: True)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_recover_plan_for_revalidation", lambda s, w: plan)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_cleanup_pending_entry_order", lambda s, w, action, reason: None)

        core._on_entry_trigger(watched)

        meta_calls = core.order_state_machine.update_order_meta.call_args_list
        assert meta_calls, "update_order_meta was not called"
        patch = None
        for call in meta_calls:
            args = call.args
            if len(args) >= 2 and isinstance(args[1], dict) and "breach_attempt_count" in args[1]:
                patch = args[1]
                break
        assert patch is not None, "no update_order_meta call carried breach_attempt_count"
        assert patch["breach_attempt_count"] == 3  # prior=2 -> this tick=3
        assert patch["last_breach_failure_reason_code"] == "OI_TOO_LOW"
        assert "last_breach_failure_at" in patch
        assert "last_breach_selector_audit" in patch


@pytestmark_runtime
class TestPathBUnresolvedDeferredWriteBackRestored:
    """Spec 2: deferred selector returns an unresolved DEFERRED:* contract."""

    def test_write_back_called_before_terminal_cleanup(self, monkeypatch):
        import ap_execution_core as ec_mod
        from ap import queue as q

        write_calls = []
        monkeypatch.setattr(
            q, "write_deferred_breach_last_error",
            lambda qid, *, reason_code, explanation, attempt, client_id, ticker:
                write_calls.append({
                    "queue_id": qid, "reason_code": reason_code, "explanation": explanation,
                }),
        )

        # Selector returns a value (truthy) so _sel is not None, but the
        # plan's contract_symbol copy-back leaves it on a DEFERRED: placeholder.
        sel_result = MagicMock()
        sel_result.contract_symbol = "DEFERRED:ROST"  # never resolved to a real symbol
        sel_result.execution_price_per_share = None
        sel_result.affordable_contracts = 0
        sel = MagicMock()
        sel.select.return_value = sel_result
        sel.get_last_failure.return_value = None  # selector itself emitted no REJECT

        plan = _make_plan(queue_id=503, contract_symbol="DEFERRED:ROST")
        watched = _make_watched(plan, queue_id=503)
        core = _bare_core(sel)

        monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check", lambda s, w: True)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_recover_plan_for_revalidation", lambda s, w: plan)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_cleanup_pending_entry_order", lambda s, w, action, reason: None)

        core._on_entry_trigger(watched)

        assert len(write_calls) >= 1, "write_deferred_breach_last_error not called for Path B"
        assert write_calls[0]["queue_id"] == 503
        assert write_calls[0]["reason_code"] == "DEFERRED_UNRESOLVED_AT_BREACH"
        assert "DEFERRED:ROST" in write_calls[0]["explanation"]

    def test_order_meta_records_unresolved_deferred_reason(self, monkeypatch):
        import ap_execution_core as ec_mod
        from ap import queue as q

        monkeypatch.setattr(q, "write_deferred_breach_last_error", lambda *a, **kw: None)

        sel_result = MagicMock()
        sel_result.contract_symbol = "DEFERRED:ROST"
        sel_result.execution_price_per_share = None
        sel_result.affordable_contracts = 0
        sel = MagicMock()
        sel.select.return_value = sel_result
        sel.get_last_failure.return_value = None

        plan = _make_plan(queue_id=504, contract_symbol="DEFERRED:ROST")
        watched = _make_watched(plan, queue_id=504)
        core = _bare_core(sel)

        monkeypatch.setattr(ec_mod.APExecutionCore, "_breach_risk_check", lambda s, w: True)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_recover_plan_for_revalidation", lambda s, w: plan)
        monkeypatch.setattr(ec_mod.APExecutionCore, "_cleanup_pending_entry_order", lambda s, w, action, reason: None)

        core._on_entry_trigger(watched)

        meta_calls = core.order_state_machine.update_order_meta.call_args_list
        patch = None
        for call in meta_calls:
            args = call.args
            if len(args) >= 2 and isinstance(args[1], dict) and "breach_attempt_count" in args[1]:
                patch = args[1]
                break
        assert patch is not None, "no update_order_meta call carried breach_attempt_count"
        assert patch["last_breach_failure_reason_code"] == "DEFERRED_UNRESOLVED_AT_BREACH"
        assert "DEFERRED:ROST" in patch["last_breach_failure_reason"]


@pytestmark_runtime
class TestDailyContinuationRegressionGuard:
    """Spec 3 & 4 — regression-only: confirm the entry-confirmation rollout's
    observe/enforce behavior is unaffected by restoring the PR182 write-back.
    These mirror tests already present in test_execution_core_entry_confirmation.py;
    duplicated here so this file is a self-contained proof of the full amendment
    without requiring the reviewer to cross-reference another file.
    """

    def _plan(self, *, confirmation_required: bool = False, candles=None):
        metadata = {
            "hybrid_client_quality_gate": {
                "confirmation_required": confirmation_required,
                "confirmation_seconds": 45,
            },
        }
        if candles is not None:
            metadata["intraday_candles"] = candles
        return SimpleNamespace(
            contract_symbol="AAPL260117C00200000",
            limit_price=1.00,
            contracts=1,
            trigger_price=100.0,
            side="CALL",
            tier="A",
            metadata=metadata,
        )

    def _run(self, monkeypatch, *, mode: str, underlying_last: float = 100.90):
        import ap_execution_core as ec_mod
        from ap_entry_watcher import WatchedSignal

        monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
        monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", mode)

        fake_execution_mod = types.ModuleType("ap.execution")
        fake_execution_mod._refresh_ask_at_submit = lambda broker, contract: (
            1.02, 5, True, "ok",
            {"submit_bid": 1.00, "submit_ask": 1.02, "submit_last": 1.01,
             "submit_mid": 1.01, "spread_pct": 0.0198},
        )
        monkeypatch.setitem(sys.modules, "ap.execution", fake_execution_mod)

        fake_ledger_mod = types.ModuleType("ap.opportunity_ledger")
        fake_ledger_mod.STAGE_ENTRY_CONFIRMATION = "ENTRY_CONFIRMATION"
        fake_ledger_mod.update_opportunity = lambda *a, **kw: None
        monkeypatch.setitem(sys.modules, "ap.opportunity_ledger", fake_ledger_mod)

        plan = self._plan(confirmation_required=False, candles=None)
        osm = MagicMock()
        osm.submit_existing_entry.return_value = {
            "ok": True, "local_order_id": "local-1", "broker_order_id": "broker-1",
            "status": "SUBMITTED", "error": None,
        }
        osm.update_order_meta.return_value = True
        osm.expire_pending_entry.return_value = True

        core = ec_mod.APExecutionCore.__new__(ec_mod.APExecutionCore)
        core.paper = True
        core.mode = "PAPER"
        core.email = "client@example.com"
        core.client_id = "client@example.com"
        core.broker = SimpleNamespace(
            cfg=SimpleNamespace(base_url="https://api.tradier.com"), sandbox=False,
        )
        core.store = MagicMock()
        core.order_state_machine = osm
        core.contract_selector = None
        core._breach_risk_check = MagicMock(return_value=True)
        core._recover_plan_for_revalidation = MagicMock(return_value=plan)
        core._alert_degraded = MagicMock()
        core._cleanup_pending_entry_order = types.MethodType(
            ec_mod.APExecutionCore._cleanup_pending_entry_order, core,
        )

        watched = WatchedSignal(
            {
                "ticker": "AAPL", "side": "CALL", "entry_price": 100.0,
                "stop_price": 95.0, "target_price": 110.0, "signal_id": "sig-1",
                "canonical_signal_id": "sig-1", "local_order_id": "local-1",
                "client_id": "client@example.com", "timeframe": "1d", "score": 78,
            },
            overnight=False,
        )
        watched.trigger_price = 100.0
        watched.last_quote_bid = underlying_last
        watched.last_quote_ask = underlying_last

        ec_mod.APExecutionCore._on_entry_trigger(core, watched)
        return osm

    def test_observe_mode_still_submits(self, monkeypatch):
        osm = self._run(monkeypatch, mode="observe")
        osm.submit_existing_entry.assert_called_once()
        osm.expire_pending_entry.assert_not_called()

    def test_enforce_mode_still_blocks(self, monkeypatch):
        osm = self._run(monkeypatch, mode="enforce")
        osm.submit_existing_entry.assert_not_called()
        osm.expire_pending_entry.assert_called_once()
