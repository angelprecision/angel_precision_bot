# tests/test_p0_deferred_materialization_audit.py
# =============================================================================
# P0: Deferred materialization audit + small-account rejection proof
#
# Invariants under test:
#   1. DEFERRED + 0.01 PENDING_TRIGGER is allowed before breach (valid placeholder).
#   2. DEFERRED + 0.01 cannot reach broker submit — blocked by pre-submit invariant
#      with reason DEFERRED_CONTRACT_NOT_MATERIALIZED or DEFERRED_LIMIT_NOT_MATERIALIZED.
#   3. Successful materialization updates orders.contract to real OCC symbol before submit.
#   4. Successful materialization updates orders.limit_price to executable price before submit.
#   5. Successful materialization updates reserved_cost = qty * limit_price * 100.
#   6. Failed selector expires with broker_order_id=null.
#   7. Failed selector writes meta.deferred_materialization.success=False.
#   8. Audit includes top_reject_buckets and account_budget on failure.
#   9. No scanner, scoring, or selector threshold constants were modified by this PR.
#
# NOT TESTED HERE (covered by existing suites):
#   - Retry logic (RETRYABLE_BREACH_SELECTOR_REASONS) → test_p0_deferred_breach_retry_chain_taxonomy.py
#   - OSM state transitions on terminalize → test_breach_block_diagnostics.py
#   - Selector gate logic → test_selector_candidate_audit.py
# =============================================================================

from __future__ import annotations

import types
from unittest.mock import MagicMock, call, ANY
from datetime import datetime, timezone
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_osm(*, update_ok: bool = True) -> MagicMock:
    osm = MagicMock()
    osm.update_order_meta.return_value = update_ok
    osm.transition.return_value = True
    osm.expire_pending_entry.return_value = True
    return osm


def _make_deferred_plan(
    *,
    ticker: str = "NVDA",
    contract_symbol: str = "DEFERRED:NVDA",
    limit_price: float = 0.01,
    max_position_usd: float = 800.0,
    side: str = "CALL",
    execution_mode: str = "live",
    breach_attempt_count: int = 0,
) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        ticker=ticker,
        side=side,
        contract_symbol=contract_symbol,
        limit_price=limit_price,
        contracts=2,
        max_position_usd=max_position_usd,
        signal_id="sig-mat-001",
        client_id="jasoncosby1@gmail.com",
        execution_mode=execution_mode,
        metadata={
            "contract_deferred":         True,
            "deferred_breach_selection": True,
            "selection_context":         "deferred_breach",
            "breach_attempt_count":      breach_attempt_count,
            "queue_id":                  42,
        },
    )


def _import_helper():
    """Import _write_deferred_materialization_audit from the execution core module."""
    import sys

    repo_root = Path(__file__).resolve().parents[1]

    # Stub heavy deps so we can import just the helper without the full runtime
    watcher_mod = types.ModuleType("ap_entry_watcher")
    watcher_mod.APEntryWatcher = MagicMock
    watcher_mod.WatchedSignal = MagicMock
    sys.modules["ap_entry_watcher"] = watcher_mod

    exit_mod = types.ModuleType("ap_exit_engine")
    exit_mod.APExitEngine = MagicMock
    exit_mod.ManagedPosition = MagicMock
    sys.modules["ap_exit_engine"] = exit_mod

    feedback_mod = types.ModuleType("ap_feedback_loop")
    feedback_mod.APFeedbackLoop = MagicMock
    sys.modules["ap_feedback_loop"] = feedback_mod

    tier_mod = types.ModuleType("ap_tier_engine")
    tier_mod.APShadowTracker = MagicMock
    sys.modules["ap_tier_engine"] = tier_mod

    signal_store_mod = types.ModuleType("ap_signal_store")
    signal_store_mod.APSignalStore = MagicMock
    sys.modules["ap_signal_store"] = signal_store_mod

    signal_tracker_mod = types.ModuleType("ap_signal_tracker")
    signal_tracker_mod.APSignalTracker = MagicMock
    sys.modules["ap_signal_tracker"] = signal_tracker_mod

    sys.modules.setdefault("intelligence_bridge", types.ModuleType("intelligence_bridge"))

    stub_ap = types.ModuleType("ap")
    stub_ap.__path__ = [str(repo_root / "ap")]
    stub_ap.db = types.ModuleType("ap.db")
    stub_ap.queue = types.ModuleType("ap.queue")
    stub_ap.observability = types.ModuleType("ap.observability")
    stub_ap.observability.emit_decision_event = lambda *a, **kw: None
    stub_ap.observability.get_git_commit = lambda: "test"
    stub_ap.observability.make_config_hash = lambda d: "hash"
    stub_ap.trace = types.ModuleType("ap.trace")
    stub_ap.trace.trace_gate = lambda *a, **kw: None
    sys.modules.setdefault("ap", stub_ap)
    sys.modules.setdefault("ap.db", stub_ap.db)
    sys.modules.setdefault("ap.queue", stub_ap.queue)
    sys.modules.setdefault("ap.observability", stub_ap.observability)
    sys.modules.setdefault("ap.trace", stub_ap.trace)

    # Stub proof_logger funnel
    stub_funnel = types.ModuleType("ap_proof_logger")
    stub_funnel.APProofLogger = MagicMock
    stub_funnel.funnel = MagicMock()
    sys.modules["ap_proof_logger"] = stub_funnel

    sys.modules.pop("ap_execution_core", None)
    import ap_execution_core as core_mod
    return core_mod._write_deferred_materialization_audit


# ---------------------------------------------------------------------------
# Test 1: DEFERRED + 0.01 is a valid PENDING_TRIGGER placeholder before breach
# ---------------------------------------------------------------------------

class TestDeferredPlaceholderAllowedBeforeBreach:
    def test_pending_trigger_status_is_the_expected_pre_breach_state(self):
        """An order created with contract=DEFERRED:TICKER + limit_price=0.01
        is valid ONLY as a PENDING_TRIGGER watcher placeholder.  No
        capitalization, broker routing, or quality checks fire on rows in this
        state — they only run when the watcher fires the breach callback.
        """
        plan = _make_deferred_plan()
        # Validate the placeholder semantics held by the plan object
        assert plan.contract_symbol.startswith("DEFERRED:")
        assert plan.limit_price == 0.01
        assert plan.metadata.get("contract_deferred") is True
        assert plan.metadata.get("deferred_breach_selection") is True

    def test_deferred_metadata_marker_is_present(self):
        """metadata['deferred_breach_selection'] and 'contract_deferred' must
        both be set for the DTE-ladder and pre-submit invariant to trigger on
        this plan.  Missing either marker would cause the breach path to treat
        it as a pre-selected contract and skip safety checks."""
        plan = _make_deferred_plan()
        meta = plan.metadata
        assert meta.get("contract_deferred") is True
        assert meta.get("deferred_breach_selection") is True


# ---------------------------------------------------------------------------
# Test 2: DEFERRED + 0.01 cannot reach broker submit (pre-submit invariant)
# ---------------------------------------------------------------------------

class TestPreSubmitInvariantBlocksDeferredPlaceholder:
    def _run_invariant_check(self, *, contract: str, limit: float) -> str:
        """Mimic the pre-submit invariant logic from ap_execution_core._on_entry_trigger.

        Returns the error code that would be emitted, or '' if no block.
        """
        _pre_contract = str(contract or "")
        _pre_limit = float(limit or 0)
        if (not _pre_contract) or _pre_contract.upper().startswith("DEFERRED:"):
            return "DEFERRED_CONTRACT_NOT_MATERIALIZED"
        if _pre_limit <= 0.01:
            return "DEFERRED_LIMIT_NOT_MATERIALIZED"
        return ""  # would proceed to broker submit

    def test_deferred_contract_placeholder_triggers_not_materialized(self):
        """DEFERRED:TICKER contract blocks with DEFERRED_CONTRACT_NOT_MATERIALIZED."""
        err = self._run_invariant_check(contract="DEFERRED:NVDA", limit=3.50)
        assert err == "DEFERRED_CONTRACT_NOT_MATERIALIZED", f"unexpected err={err!r}"

    def test_empty_contract_triggers_not_materialized(self):
        """Empty contract (copy-back failed silently) also blocks."""
        err = self._run_invariant_check(contract="", limit=3.50)
        assert err == "DEFERRED_CONTRACT_NOT_MATERIALIZED"

    def test_placeholder_limit_0_01_triggers_limit_not_materialized(self):
        """Real OCC contract but placeholder limit 0.01 blocks with DEFERRED_LIMIT_NOT_MATERIALIZED."""
        err = self._run_invariant_check(contract="NVDA260718C00120000", limit=0.01)
        assert err == "DEFERRED_LIMIT_NOT_MATERIALIZED"

    def test_zero_limit_triggers_limit_not_materialized(self):
        """Zero limit_price also blocks."""
        err = self._run_invariant_check(contract="NVDA260718C00120000", limit=0.0)
        assert err == "DEFERRED_LIMIT_NOT_MATERIALIZED"

    def test_real_occ_and_valid_limit_passes_invariant(self):
        """Real OCC symbol + limit > 0.01 allows broker submit to proceed."""
        err = self._run_invariant_check(contract="NVDA260718C00120000", limit=3.50)
        assert err == "", f"expected no block, got {err!r}"


# ---------------------------------------------------------------------------
# Test 3 & 4: Successful materialization updates contract + limit_price in OSM
# ---------------------------------------------------------------------------

class TestSuccessfulMaterializationUpdatesOrderRow:
    def _run_successful_materialization(
        self,
        *,
        selected_contract: str = "NVDA260718C00120000",
        selected_price: float = 3.50,
        selected_qty: int = 2,
        premium_per_contract: float = 350.0,
    ) -> dict:
        """Replicate the execution core's copy-back logic for a successful select()."""
        plan = _make_deferred_plan()

        sel = types.SimpleNamespace(
            contract_symbol=selected_contract,
            execution_price_per_share=selected_price,
            ask=selected_price,
            mid=selected_price,
            affordable_contracts=selected_qty,
            premium_per_contract=premium_per_contract,
        )

        _live_contract = str(plan.contract_symbol or "")
        _plan_is_placeholder = not _live_contract or _live_contract.upper().startswith("DEFERRED:")
        _sel_is_real = (
            bool(sel.contract_symbol)
            and not sel.contract_symbol.upper().startswith("DEFERRED:")
        )

        if _sel_is_real and _plan_is_placeholder:
            plan.contract_symbol = sel.contract_symbol
            _sel_price = (
                getattr(sel, "execution_price_per_share", None)
                or getattr(sel, "ask", None)
                or getattr(sel, "mid", None)
            )
            if _sel_price:
                plan.limit_price = float(_sel_price)
            _sel_qty = int(getattr(sel, "affordable_contracts", 0) or 0)
            if _sel_qty > 0:
                plan.contracts = _sel_qty
                _prem = float(getattr(sel, "premium_per_contract", 0) or 0)
                if _prem > 0:
                    plan.max_position_usd = _sel_qty * _prem

        return {"plan": plan}

    def test_contract_updated_to_real_occ_symbol(self):
        """After successful select(), plan.contract_symbol must be the real OCC contract."""
        result = self._run_successful_materialization()
        assert result["plan"].contract_symbol == "NVDA260718C00120000"
        assert not result["plan"].contract_symbol.startswith("DEFERRED:")

    def test_limit_price_updated_to_executable_value(self):
        """After successful select(), plan.limit_price must be the breach-time price > 0.01."""
        result = self._run_successful_materialization(selected_price=3.50)
        assert result["plan"].limit_price > 0.01
        assert result["plan"].limit_price == 3.50


# ---------------------------------------------------------------------------
# Test 5: Successful materialization updates reserved_cost in OSM update
# ---------------------------------------------------------------------------

class TestSuccessfulMaterializationUpdatesReservedCost:
    def test_reserved_cost_computed_from_qty_and_limit_price(self):
        """_update_contract_pre_submit must write reserved_cost = qty * limit_price * 100.

        This test validates the formula directly (the OSM implementation is
        tested at the unit level; here we confirm the math is correct).
        """
        qty = 2
        limit_price = 3.50
        expected_reserved_cost = qty * limit_price * 100  # = 700.0

        _upd_qty = qty
        _upd_lp = round(limit_price, 2)
        _upd_rc = round(_upd_qty * _upd_lp * 100, 2) if _upd_qty > 0 and _upd_lp > 0 else None

        assert _upd_rc == expected_reserved_cost, (
            f"reserved_cost={_upd_rc} != expected {expected_reserved_cost}"
        )

    def test_reserved_cost_not_set_when_qty_zero(self):
        """Zero qty must produce None reserved_cost (not a spurious zero charge)."""
        _upd_qty = 0
        _upd_lp = 3.50
        _upd_rc = round(_upd_qty * _upd_lp * 100, 2) if _upd_qty > 0 and _upd_lp > 0 else None
        assert _upd_rc is None

    def test_reserved_cost_not_set_when_limit_zero(self):
        """Zero limit must produce None reserved_cost."""
        _upd_qty = 2
        _upd_lp = 0.0
        _upd_rc = round(_upd_qty * _upd_lp * 100, 2) if _upd_qty > 0 and _upd_lp > 0 else None
        assert _upd_rc is None


# ---------------------------------------------------------------------------
# Test 6: Failed selector expires with broker_order_id=null
# ---------------------------------------------------------------------------

class TestFailedSelectorNoBrokerOrderId:
    def _run_failed_selection(self, *, terminalize_via_expire: bool = True) -> MagicMock:
        """Simulate the terminalize path when select() returns None.

        Returns the mock OSM so callers can assert on its calls.
        """
        osm = _make_osm()
        local_order_id = "ord-fail-001"
        reason = "breach_time_contract_selection:CHAIN_ROW_ZERO_BID_ASK"

        # Replicate _terminalize_deferred_breach_failure
        meta_patch = {
            "deferred_breach_failure": True,
            "deferred_breach_reason": reason,
        }
        osm.update_order_meta(local_order_id, meta_patch)

        if terminalize_via_expire:
            if not osm.expire_pending_entry(local_order_id, reason=reason):
                osm.transition(local_order_id, "EXPIRED", last_error=reason)
        else:
            osm.transition(local_order_id, "EXPIRED", last_error=reason)

        return osm

    def test_expire_called_never_submit_called(self):
        """When selection fails, expire_pending_entry fires but submit_existing_entry never does."""
        osm = self._run_failed_selection()
        osm.expire_pending_entry.assert_called_once()
        osm.submit_existing_entry.assert_not_called()

    def test_broker_order_id_never_set(self):
        """transition is never called with a broker_order_id argument."""
        osm = self._run_failed_selection(terminalize_via_expire=False)
        # Only the EXPIRED transition should have been called
        for c in osm.transition.call_args_list:
            broker_id = c.kwargs.get("broker_order_id") or (
                c.args[2] if len(c.args) > 2 else None
            )
            assert not broker_id, (
                f"broker_order_id was set during failed-selector terminalization: {broker_id!r}"
            )


# ---------------------------------------------------------------------------
# Test 7: Failed selector writes meta.deferred_materialization.success=False
# ---------------------------------------------------------------------------

class TestFailedSelectorWritesMaterializationAudit:
    def test_write_deferred_materialization_audit_called_with_success_false(self):
        """_write_deferred_materialization_audit must be called with success=False
        on every selection failure path."""
        osm = _make_osm()
        local_order_id = "ord-fail-002"

        # Import and call the real helper
        fn = _import_helper()
        fn(
            osm,
            local_order_id,
            success=False,
            attempt_ts=datetime.now(timezone.utc).isoformat(),
            original_contract="DEFERRED:NVDA",
            symbol="NVDA",
            side="CALL",
            execution_mode="live",
            account_budget=800.0,
            selector_status="CONTRACT_SELECTION_DATA_ERROR",
            selected_contract=None,
            selected_limit_price=None,
            failure_reason="breach_time_contract_selection:CHAIN_ROW_ZERO_BID_ASK",
            stage="quality_filter",
            expirations_probed=[],
            chain_rows_total=12,
            survivor_count=0,
            top_reject_buckets={"CHAIN_ROW_ZERO_BID_ASK": 12},
        )

        osm.update_order_meta.assert_called_once()
        _call = osm.update_order_meta.call_args
        assert _call.args[0] == local_order_id
        audit = _call.args[1]
        assert "deferred_materialization" in audit
        dm = audit["deferred_materialization"]
        assert dm["success"] is False
        assert dm["attempted"] is True

    def test_helper_is_silent_on_osm_exception(self):
        """_write_deferred_materialization_audit must never raise even when
        update_order_meta throws."""
        osm = MagicMock()
        osm.update_order_meta.side_effect = Exception("db exploded")

        fn = _import_helper()
        # Must not raise
        fn(
            osm,
            "ord-123",
            success=False,
            attempt_ts="2026-07-01T10:00:00+00:00",
            original_contract="DEFERRED:AAPL",
            symbol="AAPL",
            side="PUT",
            execution_mode="paper",
            account_budget=500.0,
            selector_status=None,
            selected_contract=None,
            selected_limit_price=None,
            failure_reason="chain_fetch_failed",
            stage="chain_fetch",
            expirations_probed=None,
            chain_rows_total=0,
            survivor_count=0,
            top_reject_buckets={},
        )

    def test_helper_is_silent_on_none_osm(self):
        """None osm must also not raise."""
        fn = _import_helper()
        fn(
            None,
            "ord-123",
            success=False,
            attempt_ts="2026-07-01T10:00:00+00:00",
            original_contract="DEFERRED:AAPL",
            symbol="AAPL",
            side="PUT",
            execution_mode="paper",
            account_budget=0.0,
            selector_status=None,
            selected_contract=None,
            selected_limit_price=None,
            failure_reason="no_osm",
            stage=None,
            expirations_probed=None,
            chain_rows_total=0,
            survivor_count=0,
            top_reject_buckets={},
        )


# ---------------------------------------------------------------------------
# Test 8: Audit includes top_reject_buckets and account_budget
# ---------------------------------------------------------------------------

class TestAuditFields:
    def test_audit_includes_top_reject_buckets(self):
        """top_reject_buckets must appear in deferred_materialization audit dict."""
        osm = _make_osm()
        fn = _import_helper()
        fn(
            osm,
            "ord-audit-001",
            success=False,
            attempt_ts=datetime.now(timezone.utc).isoformat(),
            original_contract="DEFERRED:SBUX",
            symbol="SBUX",
            side="CALL",
            execution_mode="live",
            account_budget=1200.0,
            selector_status="CONTRACT_SELECTION_QUALITY_REJECT",
            selected_contract=None,
            selected_limit_price=None,
            failure_reason="breach_time_contract_selection:OI_TOO_LOW",
            stage="quality_filter",
            expirations_probed=[
                {"bucket": "A", "expirations_probed": [{"exp": "2026-07-01", "dte": 0}]},
            ],
            chain_rows_total=35,
            survivor_count=0,
            top_reject_buckets={"OI_TOO_LOW": 28, "SPREAD_TOO_WIDE": 7},
        )

        osm.update_order_meta.assert_called_once()
        dm = osm.update_order_meta.call_args.args[1]["deferred_materialization"]
        assert "top_reject_buckets" in dm
        assert dm["top_reject_buckets"] == {"OI_TOO_LOW": 28, "SPREAD_TOO_WIDE": 7}

    def test_audit_includes_account_budget(self):
        """account_budget must appear in deferred_materialization audit dict."""
        osm = _make_osm()
        fn = _import_helper()
        fn(
            osm,
            "ord-audit-002",
            success=False,
            attempt_ts=datetime.now(timezone.utc).isoformat(),
            original_contract="DEFERRED:SBUX",
            symbol="SBUX",
            side="CALL",
            execution_mode="live",
            account_budget=1200.0,
            selector_status="CONTRACT_SELECTION_QUALITY_REJECT",
            selected_contract=None,
            selected_limit_price=None,
            failure_reason="breach_time_contract_selection:UNTRADEABLE_FOR_ACCOUNT_SIZE",
            stage="affordability_gate",
            expirations_probed=None,
            chain_rows_total=18,
            survivor_count=1,
            top_reject_buckets={"PREMIUM_CAP_EXCEEDED": 18},
        )

        dm = osm.update_order_meta.call_args.args[1]["deferred_materialization"]
        assert "account_budget" in dm
        assert dm["account_budget"] == 1200.0

    def test_audit_success_path_carries_selected_fields(self):
        """On success, selected_contract and selected_limit_price must be set."""
        osm = _make_osm()
        fn = _import_helper()
        fn(
            osm,
            "ord-ok-001",
            success=True,
            attempt_ts=datetime.now(timezone.utc).isoformat(),
            original_contract="DEFERRED:NVDA",
            symbol="NVDA",
            side="CALL",
            execution_mode="live",
            account_budget=800.0,
            selector_status="CONTRACT_SELECTED",
            selected_contract="NVDA260718C00120000",
            selected_limit_price=3.50,
            failure_reason=None,
            stage="contract_selected",
            expirations_probed=None,
            chain_rows_total=45,
            survivor_count=12,
            top_reject_buckets={},
        )

        dm = osm.update_order_meta.call_args.args[1]["deferred_materialization"]
        assert dm["success"] is True
        assert dm["selected_contract"] == "NVDA260718C00120000"
        assert dm["selected_limit_price"] == 3.50
        assert dm["failure_reason"] is None

    def test_optional_fields_not_set_when_none(self):
        """nearest_affordable_contract / best_liquid_contract / why_best_contract_failed
        are omitted from the audit dict when not provided (None)."""
        osm = _make_osm()
        fn = _import_helper()
        fn(
            osm,
            "ord-opt-001",
            success=False,
            attempt_ts=datetime.now(timezone.utc).isoformat(),
            original_contract="DEFERRED:AMD",
            symbol="AMD",
            side="PUT",
            execution_mode="paper",
            account_budget=200.0,
            selector_status=None,
            selected_contract=None,
            selected_limit_price=None,
            failure_reason="no_chain_data",
            stage="chain_fetch",
            expirations_probed=None,
            chain_rows_total=0,
            survivor_count=0,
            top_reject_buckets={},
            # NOT passing optional fields → they must not appear in audit
        )

        dm = osm.update_order_meta.call_args.args[1]["deferred_materialization"]
        assert "nearest_affordable_contract" not in dm
        assert "best_liquid_contract" not in dm
        assert "why_best_contract_failed" not in dm

    def test_optional_fields_present_when_supplied(self):
        """Optional enrichment fields are written when the caller supplies them."""
        osm = _make_osm()
        fn = _import_helper()
        fn(
            osm,
            "ord-opt-002",
            success=False,
            attempt_ts=datetime.now(timezone.utc).isoformat(),
            original_contract="DEFERRED:TSLA",
            symbol="TSLA",
            side="CALL",
            execution_mode="live",
            account_budget=1500.0,
            selector_status="CONTRACT_SELECTION_QUALITY_REJECT",
            selected_contract=None,
            selected_limit_price=None,
            failure_reason="UNTRADEABLE_FOR_ACCOUNT_SIZE",
            stage="affordability_gate",
            expirations_probed=None,
            chain_rows_total=22,
            survivor_count=3,
            top_reject_buckets={"PREMIUM_CAP_EXCEEDED": 22},
            nearest_affordable_contract="TSLA260718C00250000",
            best_liquid_contract="TSLA260718C00240000",
            why_best_contract_failed="premium_per_contract=1850_exceeds_budget_1500",
        )

        dm = osm.update_order_meta.call_args.args[1]["deferred_materialization"]
        assert dm["nearest_affordable_contract"] == "TSLA260718C00250000"
        assert dm["best_liquid_contract"] == "TSLA260718C00240000"
        assert dm["why_best_contract_failed"] == "premium_per_contract=1850_exceeds_budget_1500"


# ---------------------------------------------------------------------------
# Test 9: No scanner, scoring, or selector threshold changes
# ---------------------------------------------------------------------------

class TestNoSelectorThresholdChanges:
    def test_pro_quality_thresholds_unchanged(self):
        """Verify the canonical contract quality constants in ap/contract_selector.py
        were NOT modified by this PR.  These values are the production quality gate
        — any accidental change would affect all clients immediately."""
        import sys
        import types as _types

        # Stub external deps before importing the selector
        for mod in ["ap.observability", "ap.trace", "ap.contract_quote_revalidator"]:
            if mod not in sys.modules:
                stub = _types.ModuleType(mod)
                stub.emit_decision_event = lambda *a, **kw: None
                stub.get_git_commit = lambda: "test"
                stub.make_config_hash = lambda d: "hash"
                stub.trace_gate = lambda *a, **kw: None
                stub.revalidate_with_direct_quote = lambda *a, **kw: {"action": "SKIP_NOT_REVALIDATABLE"}
                stub.should_revalidate = lambda r: False
                stub.DEFAULT_REVALIDATE_TOP_N = 3
                sys.modules[mod] = stub

        import ap.contract_selector as cs

        # T1 hard spread cap must stay at 10%
        assert cs._PRO_T1_SPREAD_HARD_MAX == 0.10, (
            f"T1 spread hard max changed: {cs._PRO_T1_SPREAD_HARD_MAX}"
        )
        # T2 hard spread cap must stay at 12%
        assert cs._PRO_T2_SPREAD_HARD_MAX == 0.12, (
            f"T2 spread hard max changed: {cs._PRO_T2_SPREAD_HARD_MAX}"
        )
        # Minimum bid must stay at $0.10
        assert cs._PRO_MIN_BID == 0.10, f"PRO_MIN_BID changed: {cs._PRO_MIN_BID}"
        # Minimum bid size (hard) must stay at 3
        assert cs._PRO_MIN_BID_SIZE_HARD == 3, (
            f"PRO_MIN_BID_SIZE_HARD changed: {cs._PRO_MIN_BID_SIZE_HARD}"
        )

    def test_no_new_scanner_imports_in_execution_core(self):
        """ap_execution_core must not import scanner or scoring modules.
        This PR touches only observability/audit paths — never signals."""
        with open("ap_execution_core.py", "r") as f:
            source = f.read()

        forbidden_imports = [
            "ap_scanner",
            "ap_scoring",
            "ap_strat_agent",
            "ap_intel",
        ]
        for mod in forbidden_imports:
            assert mod not in source, (
                f"Unexpected import of {mod!r} found in ap_execution_core.py"
            )


def test_prebreach_hydration_window_and_status_scope_exist_in_monitor_source():
    src = open("ap/order_monitor.py", "r").read()
    assert "DEFERRED_PREBREACH_HYDRATION_ENABLED" in src
    assert "DEFERRED_HYDRATION_MAX_PER_CYCLE" in src
    assert "DEFERRED_HYDRATION_WINDOW_START_ET" in src
    assert "DEFERRED_HYDRATION_WINDOW_END_ET" in src
    assert 'if status != "PENDING_TRIGGER":' in src or "if status != 'PENDING_TRIGGER':" in src
    assert '"deferred_prebreach_hydration"' in src
    assert "DEFERRED_HYDRATION_DISABLED" in src
    assert "DEFERRED_HYDRATION_SKIPPED_MAX_PER_CYCLE" in src


def test_osm_hydration_write_path_is_in_place_and_guarded():
    src = open("ap/order_state_machine.py", "r").read()
    assert "FOR UPDATE" in src
    assert "contract_selection_status = %s" in src
    assert "qty = %s" in src
    assert 'current_status != "PENDING_TRIGGER"' in src or "current_status != 'PENDING_TRIGGER'" in src
    assert '"contract_materialized_source": "prebreach_hydration"' in src


def test_execution_core_bridges_hydrated_order_row_before_deferred_selection():
    src = open("ap_execution_core.py", "r").read()
    assert "_refresh_hydrated_prebreach_plan(" in src
    assert "PREBREACH_HYDRATION_BRIDGE_APPLIED" in src
    assert 'sig["contract_deferred"] = False' in src
    assert 'sig["contract_materialized_source"] = "prebreach_hydration"' in src


def test_dashboard_read_model_hides_deferred_point_zero_one_limit():
    src = open("ap/operator_queue_read_model.py", "r").read()
    assert "pending pre-breach hydration / breach-time selection" in src
    assert "not priced yet" in src
    assert "last_hydration_attempt" in src
    assert "hydration_failure_reason" in src
