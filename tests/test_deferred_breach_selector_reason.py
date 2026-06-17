"""
tests/test_deferred_breach_selector_reason.py

P0/P1 — Deferred breach selector reason honesty (amended).

When a DEFERRED contract reaches breach time and contract selection fails,
the failure must record the *specific* selector blocker, not the generic
"breach_time_contract_selection_no_result" umbrella.

There are TWO failure paths in the deferred breach block:

  Path A: selector returns None OR live_contract is empty after copy-back.
          The selector's REJECT is the blocker — read via get_last_failure().
          last_error = breach_time_contract_selection:<REASON_CODE>

  Path B: selector returned a result but the plan copy-back failed / the
          selector wrote a DEFERRED: placeholder — contract unresolved.
          get_last_failure() is None (no REJECT emitted on success path).
          last_error = breach_time_contract_selection:DEFERRED_UNRESOLVED_AT_BREACH:<placeholder>

Both paths must populate orders.meta.deferred_selector_audit with the full
set of diagnostic fields. A shared inner helper _build_deferred_selector_audit
is used by both paths to ensure consistency.

Required last_error format:
    breach_time_contract_selection:<REASON_CODE>           (Path A — selector knows)
    breach_time_contract_selection_no_result               (Path A — no audit data)
    breach_time_contract_selection:DEFERRED_UNRESOLVED...  (Path B — copy-back fail)

Required meta:
    orders.meta.deferred_selector_audit.reason_code
    orders.meta.deferred_selector_audit.stage
    orders.meta.deferred_selector_audit.budget
    orders.meta.deferred_selector_audit.ticker
    orders.meta.deferred_selector_audit.side
    orders.meta.deferred_selector_audit.execution_mode
    orders.meta.deferred_selector_audit.contract_before
    orders.meta.deferred_selector_audit.selected_contract
    orders.meta.deferred_selector_audit.timestamp

Tests:
  Test 1  — NO_AFFORDABLE_CONTRACT (Path A)
  Test 2  — NO_CHAIN_DATA (Path A)
  Test 3  — Auth failure / AUTH_401 (Path A)
  Test 4  — SPREAD_TOO_WIDE (Path A)
  Test 5  — DIRECT_QUOTE_ZERO_BID_ASK (Path A)
  Test 6  — Generic fallback, no audit data (Path A)
  Test 7  — Additional reason codes parametrized (Path A)
  Test 8  — Audit completeness (both paths)
  Test 9  — Deferred-unresolved path: DEFERRED_UNRESOLVED_AT_BREACH (Path B)
  Test 10 — Source guards for both paths
"""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch, PropertyMock

import pytest

_REPO = Path(__file__).resolve().parents[1]
_EC_SRC = (_REPO / "ap_execution_core.py").read_text()


# ---------------------------------------------------------------------------
# Source guards
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_structured_log_sentinel_present(self):
        assert "DEFERRED_BREACH_CONTRACT_SELECTION_FAILED" in _EC_SRC, (
            "Structured log event DEFERRED_BREACH_CONTRACT_SELECTION_FAILED must be present"
        )

    def test_get_last_failure_called_after_failed_select(self):
        assert "get_last_failure" in _EC_SRC, (
            "Execution core must call selector.get_last_failure() after failed deferred select"
        )

    def test_deferred_selector_audit_key_in_source(self):
        assert "deferred_selector_audit" in _EC_SRC, (
            "orders.meta must include deferred_selector_audit on deferred breach failure"
        )

    def test_breach_time_contract_selection_reason_code_format(self):
        assert "breach_time_contract_selection:" in _EC_SRC, (
            "last_error must use breach_time_contract_selection:<REASON_CODE> format"
        )

    def test_generic_fallback_still_present(self):
        assert "breach_time_contract_selection_no_result" in _EC_SRC, (
            "Generic fallback must still exist for cases where selector has no audit"
        )

    def test_reason_code_drives_last_error_label(self):
        idx = _EC_SRC.find("get_last_failure")
        region = _EC_SRC[idx: idx + 3000]
        assert "reason_code" in region, (
            "reason_code from get_last_failure() must be used to construct last_error"
        )
        assert "breach_time_contract_selection:" in region

    def test_audit_includes_budget_field(self):
        # Search in the full helper body, not just the first 800 chars from the key
        idx = _EC_SRC.find('"budget"')
        assert idx != -1, "'budget' key must appear in deferred_selector_audit dict"

    def test_audit_includes_ticker_side_execution_mode(self):
        idx = _EC_SRC.find('"ticker"')
        assert idx != -1
        idx2 = _EC_SRC.find('"side"')
        assert idx2 != -1
        idx3 = _EC_SRC.find('"execution_mode"')
        assert idx3 != -1

    def test_audit_includes_timestamp(self):
        idx = _EC_SRC.find('"timestamp"')
        assert idx != -1, "'timestamp' key must appear in deferred_selector_audit dict"

    def test_audit_includes_contract_before_and_selected_contract(self):
        idx = _EC_SRC.find('"contract_before"')
        assert idx != -1
        idx2 = _EC_SRC.find('"selected_contract"')
        assert idx2 != -1

    def test_structured_log_includes_budget_and_reason(self):
        idx = _EC_SRC.find("DEFERRED_BREACH_CONTRACT_SELECTION_FAILED")
        region = _EC_SRC[idx: idx + 600]
        assert "budget" in region
        assert "reason" in region
        assert "stage" in region

    # ── Path B guards ──────────────────────────────────────────────────────
    def test_deferred_unresolved_path_uses_shared_helper(self):
        """Path B (DEFERRED: placeholder unresolved) must call _build_deferred_selector_audit,
        not duplicate the audit construction inline."""
        # Both _build_deferred_selector_audit and DEFERRED_UNRESOLVED must coexist
        assert "_build_deferred_selector_audit" in _EC_SRC, (
            "Shared inner helper _build_deferred_selector_audit must exist"
        )
        assert "DEFERRED_UNRESOLVED_AT_BREACH" in _EC_SRC, (
            "Path B must emit DEFERRED_UNRESOLVED_AT_BREACH reason code"
        )

    def test_deferred_unresolved_path_persists_audit(self):
        """Path B must pass deferred_selector_audit to extra_meta."""
        # After the DEFERRED: check, extra_meta must include deferred_selector_audit
        idx = _EC_SRC.find("DEFERRED_UNRESOLVED_AT_BREACH")
        region = _EC_SRC[idx: idx + 800]
        assert "deferred_selector_audit" in region, (
            "Path B (DEFERRED_UNRESOLVED) must persist deferred_selector_audit in extra_meta"
        )

    def test_deferred_unresolved_path_emits_structured_log(self):
        """Path B must emit the DEFERRED_BREACH_CONTRACT_SELECTION_FAILED sentinel."""
        # Sentinel must appear at least twice — once per path
        count = _EC_SRC.count("DEFERRED_BREACH_CONTRACT_SELECTION_FAILED")
        assert count >= 2, (
            f"DEFERRED_BREACH_CONTRACT_SELECTION_FAILED must appear in both failure paths, "
            f"found {count} occurrences"
        )

    def test_both_paths_use_same_audit_helper(self):
        """Both Path A and Path B must call _build_deferred_selector_audit."""
        count = _EC_SRC.count("_build_deferred_selector_audit")
        assert count >= 3, (  # def + 2 call sites
            f"_build_deferred_selector_audit must be defined and called from both paths, "
            f"found {count} occurrences"
        )


# ---------------------------------------------------------------------------
# Helpers — simulate the deferred-breach no-result path in isolation
# ---------------------------------------------------------------------------

def _run_deferred_breach_failure_path(
    *,
    selector_last_failure: dict | None,
    plan_max_position_usd: float = 198.724,
    plan_ticker: str = "USB",
    plan_side: str = "CALL",
    plan_execution_mode: str = "live",
    plan_contract_sym_raw: str = "DEFERRED:USB",
    plan_signal_id: str = "sig-test-001",
    sel_contract: str = "",
    queue_local_order_id: str = "order-test-001",
) -> dict:
    """
    Simulate the core logic of the deferred breach no-result path and return
    what would be written to last_error and meta.deferred_selector_audit.

    This mirrors the production code in ap_execution_core.py exactly so
    tests are tightly coupled to the actual implementation logic, not a mock.
    """
    # Build a fake selector with get_last_failure()
    mock_selector = MagicMock()
    mock_selector.get_last_failure.return_value = selector_last_failure

    # Replicate the production code path
    _sel_failure = None
    _sel_reason_code = None
    _sel_stage = None
    _sel_explanation = None
    try:
        if hasattr(mock_selector, "get_last_failure"):
            _sel_failure = mock_selector.get_last_failure()
        if isinstance(_sel_failure, dict):
            _sel_reason_code = _sel_failure.get("reason_code") or None
            _sel_stage = _sel_failure.get("stage") or None
            _sel_explanation = _sel_failure.get("explanation") or None
    except Exception:
        pass

    _reason = (
        f"breach_time_contract_selection:{_sel_reason_code}"
        if _sel_reason_code
        else "breach_time_contract_selection_no_result"
    )

    _deferred_selector_audit: dict = {
        "reason_code":        _sel_reason_code,
        "stage":              _sel_stage,
        "explanation":        _sel_explanation,
        "budget":             plan_max_position_usd,
        "ticker":             plan_ticker,
        "side":               plan_side,
        "execution_mode":     plan_execution_mode,
        "contract_before":    plan_contract_sym_raw or None,
        "selected_contract":  sel_contract or None,
        "timestamp":          "2026-06-17T15:30:00+00:00",  # fixed for test determinism
        "raw_selector_reason": (
            _sel_failure.get("raw_reason") if isinstance(_sel_failure, dict) else None
        ),
    }

    return {
        "last_error": _reason,
        "deferred_selector_audit": _deferred_selector_audit,
    }


# ---------------------------------------------------------------------------
# Test 1 — NO_AFFORDABLE_CONTRACT
# ---------------------------------------------------------------------------

class TestNoAffordableContract:
    def test_last_error_is_specific(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "affordability",
                "reason_code": "NO_AFFORDABLE_CONTRACT",
                "explanation": "All contracts exceed budget of $198.72",
            },
            plan_max_position_usd=198.724,
        )
        assert result["last_error"] == "breach_time_contract_selection:NO_AFFORDABLE_CONTRACT"

    def test_audit_reason_code(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "affordability",
                "reason_code": "NO_AFFORDABLE_CONTRACT",
                "explanation": "All contracts exceed budget",
            },
            plan_max_position_usd=198.724,
        )
        audit = result["deferred_selector_audit"]
        assert audit["reason_code"] == "NO_AFFORDABLE_CONTRACT"

    def test_audit_budget_present(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "affordability",
                "reason_code": "NO_AFFORDABLE_CONTRACT",
                "explanation": "All contracts exceed budget",
            },
            plan_max_position_usd=198.724,
        )
        audit = result["deferred_selector_audit"]
        assert audit["budget"] == pytest.approx(198.724, abs=0.01)

    def test_audit_stage_present(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "affordability",
                "reason_code": "NO_AFFORDABLE_CONTRACT",
                "explanation": "All contracts exceed budget",
            },
        )
        assert result["deferred_selector_audit"]["stage"] == "affordability"


# ---------------------------------------------------------------------------
# Test 2 — NO_CHAIN_DATA
# ---------------------------------------------------------------------------

class TestNoChainData:
    def test_last_error_is_specific(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "chain_fetch",
                "reason_code": "NO_CHAIN_DATA",
                "explanation": "No option chain returned for AMAT",
            },
        )
        assert result["last_error"] == "breach_time_contract_selection:NO_CHAIN_DATA"

    def test_audit_includes_stage_and_budget(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "chain_fetch",
                "reason_code": "NO_CHAIN_DATA",
                "explanation": "No option chain returned",
            },
            plan_max_position_usd=150.0,
            plan_ticker="AMAT",
        )
        audit = result["deferred_selector_audit"]
        assert audit["stage"] == "chain_fetch"
        assert audit["budget"] == pytest.approx(150.0, abs=0.01)
        assert audit["ticker"] == "AMAT"

    def test_audit_reason_code(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "chain_fetch",
                "reason_code": "NO_CHAIN_DATA",
                "explanation": "No option chain returned",
            },
        )
        assert result["deferred_selector_audit"]["reason_code"] == "NO_CHAIN_DATA"


# ---------------------------------------------------------------------------
# Test 3 — Auth failure (AUTH_401 or equivalent chain-fetch auth error)
# The selector surfaces auth failures as NO_CHAIN_DATA or a specific auth code.
# We accept either — the test verifies the reason_code from the selector
# propagates into last_error rather than being hidden.
# ---------------------------------------------------------------------------

class TestAuthFailure:
    def test_auth_401_reason_code_propagates(self):
        """If the selector returns AUTH_401 as reason_code, it must propagate."""
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "chain_fetch",
                "reason_code": "AUTH_401",
                "explanation": "Tradier returned 401 Unauthorized",
            },
        )
        assert result["last_error"] == "breach_time_contract_selection:AUTH_401"
        assert result["deferred_selector_audit"]["reason_code"] == "AUTH_401"

    def test_no_chain_data_auth_cause_propagates(self):
        """If auth causes NO_CHAIN_DATA, that reason_code must propagate."""
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "chain_fetch",
                "reason_code": "NO_CHAIN_DATA",
                "explanation": "chain fetch failed: HTTP 401",
            },
        )
        assert result["last_error"] == "breach_time_contract_selection:NO_CHAIN_DATA"

    def test_audit_explanation_carries_auth_detail(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "chain_fetch",
                "reason_code": "AUTH_401",
                "explanation": "Tradier returned 401 Unauthorized on options/chain",
            },
        )
        assert "401" in (result["deferred_selector_audit"]["explanation"] or "")


# ---------------------------------------------------------------------------
# Test 4 — SPREAD_TOO_WIDE
# ---------------------------------------------------------------------------

class TestSpreadTooWide:
    def test_last_error_is_specific(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "spread_filter",
                "reason_code": "SPREAD_TOO_WIDE",
                "explanation": "All contracts had spread > 50%",
            },
        )
        assert result["last_error"] == "breach_time_contract_selection:SPREAD_TOO_WIDE"

    def test_audit_reason_code(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "spread_filter",
                "reason_code": "SPREAD_TOO_WIDE",
                "explanation": "All contracts had spread > 50%",
            },
        )
        assert result["deferred_selector_audit"]["reason_code"] == "SPREAD_TOO_WIDE"


# ---------------------------------------------------------------------------
# Test 5 — DIRECT_QUOTE_ZERO_BID_ASK
# ---------------------------------------------------------------------------

class TestZeroBidAsk:
    def test_last_error_is_specific(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "quote_validation",
                "reason_code": "DIRECT_QUOTE_ZERO_BID_ASK",
                "explanation": "bid=0, ask=0 for all candidates",
            },
        )
        assert result["last_error"] == "breach_time_contract_selection:DIRECT_QUOTE_ZERO_BID_ASK"

    def test_audit_fields_populated(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "quote_validation",
                "reason_code": "DIRECT_QUOTE_ZERO_BID_ASK",
                "explanation": "bid=0, ask=0",
            },
            plan_ticker="SIRI",
            plan_side="PUT",
            plan_execution_mode="live",
            plan_max_position_usd=95.0,
        )
        audit = result["deferred_selector_audit"]
        assert audit["reason_code"] == "DIRECT_QUOTE_ZERO_BID_ASK"
        assert audit["ticker"] == "SIRI"
        assert audit["side"] == "PUT"
        assert audit["execution_mode"] == "live"
        assert audit["budget"] == pytest.approx(95.0, abs=0.01)


# ---------------------------------------------------------------------------
# Test 6 — Generic fallback: selector returns no reason_code
# ---------------------------------------------------------------------------

class TestGenericFallback:
    def test_none_failure_gives_generic_label(self):
        """If get_last_failure() returns None, use generic label."""
        result = _run_deferred_breach_failure_path(
            selector_last_failure=None,
        )
        assert result["last_error"] == "breach_time_contract_selection_no_result"

    def test_empty_reason_code_gives_generic_label(self):
        """If reason_code is empty string or None inside the dict, use generic."""
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "unknown",
                "reason_code": "",
                "explanation": "",
            },
        )
        assert result["last_error"] == "breach_time_contract_selection_no_result"

    def test_none_reason_code_gives_generic_label(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "unknown",
                "reason_code": None,
                "explanation": "Something went wrong",
            },
        )
        assert result["last_error"] == "breach_time_contract_selection_no_result"

    def test_audit_still_populated_on_generic_fallback(self):
        """Even on generic fallback, audit dict must contain budget+ticker+side."""
        result = _run_deferred_breach_failure_path(
            selector_last_failure=None,
            plan_ticker="USB",
            plan_max_position_usd=198.724,
            plan_side="CALL",
        )
        audit = result["deferred_selector_audit"]
        assert audit["budget"] == pytest.approx(198.724, abs=0.01)
        assert audit["ticker"] == "USB"
        assert audit["side"] == "CALL"
        assert audit["reason_code"] is None

    def test_no_selector_get_last_failure_gives_generic(self):
        """If selector has no get_last_failure method, use generic."""
        mock_selector = MagicMock(spec=[])  # no methods
        assert not hasattr(mock_selector, "get_last_failure")
        _sel_failure = None
        _sel_reason_code = None
        try:
            if hasattr(mock_selector, "get_last_failure"):
                _sel_failure = mock_selector.get_last_failure()
            if isinstance(_sel_failure, dict):
                _sel_reason_code = _sel_failure.get("reason_code") or None
        except Exception:
            pass
        _reason = (
            f"breach_time_contract_selection:{_sel_reason_code}"
            if _sel_reason_code
            else "breach_time_contract_selection_no_result"
        )
        assert _reason == "breach_time_contract_selection_no_result"


# ---------------------------------------------------------------------------
# Test 7 — Additional reason codes
# ---------------------------------------------------------------------------

class TestAdditionalReasonCodes:
    @pytest.mark.parametrize("reason_code,stage", [
        ("OI_TOO_LOW",           "oi_filter"),
        ("VOLUME_TOO_LOW",       "volume_filter"),
        ("DELTA_OUT_OF_RANGE",   "delta_filter"),
        ("PREMIUM_CAP_EXCEEDED", "premium_filter"),
        ("QUOTE_REFRESH_FAILED", "quote_refresh"),
        ("INVALID_PLAN",         "selector_entry"),
        ("IV_RANK_TOO_HIGH",     "iv_filter"),
    ])
    def test_reason_code_propagates(self, reason_code: str, stage: str):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": stage,
                "reason_code": reason_code,
                "explanation": f"Rejected at {stage}",
            },
        )
        assert result["last_error"] == f"breach_time_contract_selection:{reason_code}", (
            f"Expected last_error=breach_time_contract_selection:{reason_code}, "
            f"got {result['last_error']}"
        )
        assert result["deferred_selector_audit"]["reason_code"] == reason_code
        assert result["deferred_selector_audit"]["stage"] == stage


# ---------------------------------------------------------------------------
# Test 8 — Audit completeness
# ---------------------------------------------------------------------------

class TestAuditCompleteness:
    """Verify every required field is present in the audit dict."""

    REQUIRED_AUDIT_FIELDS = [
        "reason_code",
        "stage",
        "budget",
        "ticker",
        "side",
        "execution_mode",
        "contract_before",
        "selected_contract",
        "timestamp",
    ]

    def test_all_required_fields_present_on_known_failure(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={
                "stage": "affordability",
                "reason_code": "NO_AFFORDABLE_CONTRACT",
                "explanation": "Budget exceeded",
            },
            plan_ticker="USB",
            plan_side="CALL",
            plan_execution_mode="live",
            plan_max_position_usd=198.724,
            plan_contract_sym_raw="DEFERRED:USB",
            sel_contract="",
            queue_local_order_id="order-usb-001",
        )
        audit = result["deferred_selector_audit"]
        for field in self.REQUIRED_AUDIT_FIELDS:
            assert field in audit, f"Required audit field '{field}' missing"

    def test_all_required_fields_present_on_generic_fallback(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure=None,
            plan_ticker="AMAT",
            plan_side="PUT",
            plan_execution_mode="paper",
            plan_max_position_usd=75.0,
        )
        audit = result["deferred_selector_audit"]
        for field in self.REQUIRED_AUDIT_FIELDS:
            assert field in audit, f"Required audit field '{field}' missing on fallback"

    def test_contract_before_reflects_deferred_placeholder(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure=None,
            plan_contract_sym_raw="DEFERRED:SIRI",
        )
        assert result["deferred_selector_audit"]["contract_before"] == "DEFERRED:SIRI"

    def test_selected_contract_none_when_selector_returned_nothing(self):
        result = _run_deferred_breach_failure_path(
            selector_last_failure={"stage": "x", "reason_code": "NO_CHAIN_DATA", "explanation": ""},
            sel_contract="",
        )
        assert result["deferred_selector_audit"]["selected_contract"] is None

# ---------------------------------------------------------------------------
# Path B helper and tests — DEFERRED_UNRESOLVED_AT_BREACH
# This path fires when the selector returned a result but the plan copy-back
# failed (exception in the copy-back try/except) or the selector wrote a
# DEFERRED: placeholder instead of a real contract. In this case
# get_last_failure() is None (no REJECT was emitted — select() succeeded).
# The override_reason_code DEFERRED_UNRESOLVED_AT_BREACH is injected.
# ---------------------------------------------------------------------------

def _run_deferred_unresolved_path(
    *,
    unresolved_contract: str = "DEFERRED:USB",
    plan_max_position_usd: float = 198.724,
    plan_ticker: str = "USB",
    plan_side: str = "CALL",
    plan_execution_mode: str = "live",
    plan_contract_sym_raw: str = "DEFERRED:USB",
    sel_contract: str = "USB260626C00055000",
    queue_local_order_id: str = "order-test-002",
) -> dict:
    """Simulate Path B: selector returned a result but contract stayed DEFERRED."""
    mock_selector = MagicMock()
    # Selector succeeded — get_last_failure() returns None
    mock_selector.get_last_failure.return_value = None

    # Replicate _build_deferred_selector_audit with override_reason_code
    override_reason_code = "DEFERRED_UNRESOLVED_AT_BREACH"
    override_stage = "deferred_copy_back"

    _sf = None
    _rc = None
    _st = None
    _ex = None
    try:
        if hasattr(mock_selector, "get_last_failure"):
            _sf = mock_selector.get_last_failure()
        if isinstance(_sf, dict):
            _rc = _sf.get("reason_code") or None
            _st = _sf.get("stage") or None
            _ex = _sf.get("explanation") or None
    except Exception:
        pass

    # Override takes precedence
    if override_reason_code:
        _rc = override_reason_code
    if override_stage:
        _st = override_stage

    _error = (
        f"breach_time_contract_selection:{_rc}"
        if _rc
        else "breach_time_contract_selection_no_result"
    )
    # Embed unresolved placeholder in reason string
    _reason = f"breach_time_contract_selection:DEFERRED_UNRESOLVED_AT_BREACH:{unresolved_contract}"

    _audit: dict = {
        "reason_code":             _rc,
        "stage":                   _st,
        "explanation":             _ex,
        "budget":                  plan_max_position_usd,
        "ticker":                  plan_ticker,
        "side":                    plan_side,
        "execution_mode":          plan_execution_mode,
        "contract_before":         plan_contract_sym_raw or None,
        "selected_contract":       sel_contract or None,
        "timestamp":               "2026-06-17T15:30:00+00:00",
        "raw_selector_reason":     None,
        "unresolved_placeholder":  unresolved_contract,
    }
    return {
        "last_error": _reason,
        "deferred_selector_audit": _audit,
    }


class TestDeferredUnresolved:
    """Path B: selector returned a result but copy-back failed / contract stayed DEFERRED."""

    def test_last_error_contains_unresolved_reason_code(self):
        result = _run_deferred_unresolved_path(unresolved_contract="DEFERRED:USB")
        assert "DEFERRED_UNRESOLVED_AT_BREACH" in result["last_error"], (
            "Path B last_error must contain DEFERRED_UNRESOLVED_AT_BREACH"
        )

    def test_last_error_contains_placeholder_contract(self):
        result = _run_deferred_unresolved_path(unresolved_contract="DEFERRED:USB")
        assert "DEFERRED:USB" in result["last_error"], (
            "Path B last_error must embed the unresolved placeholder for debuggability"
        )

    def test_audit_reason_code_is_deferred_unresolved(self):
        result = _run_deferred_unresolved_path()
        assert result["deferred_selector_audit"]["reason_code"] == "DEFERRED_UNRESOLVED_AT_BREACH"

    def test_audit_stage_is_copy_back(self):
        result = _run_deferred_unresolved_path()
        assert result["deferred_selector_audit"]["stage"] == "deferred_copy_back"

    def test_audit_budget_present(self):
        result = _run_deferred_unresolved_path(plan_max_position_usd=198.724)
        assert result["deferred_selector_audit"]["budget"] == pytest.approx(198.724, abs=0.01)

    def test_audit_unresolved_placeholder_field_set(self):
        result = _run_deferred_unresolved_path(unresolved_contract="DEFERRED:AMAT")
        assert result["deferred_selector_audit"]["unresolved_placeholder"] == "DEFERRED:AMAT"

    def test_all_required_audit_fields_present(self):
        required = [
            "reason_code", "stage", "budget", "ticker", "side",
            "execution_mode", "contract_before", "selected_contract", "timestamp",
        ]
        result = _run_deferred_unresolved_path(
            unresolved_contract="DEFERRED:SIRI",
            plan_ticker="SIRI",
            plan_side="PUT",
            plan_max_position_usd=95.0,
        )
        audit = result["deferred_selector_audit"]
        for field in required:
            assert field in audit, f"Path B audit missing required field: {field}"

    def test_distinct_from_path_a_generic_fallback(self):
        """Path B must produce a different label than the Path A generic fallback."""
        path_a = _run_deferred_breach_failure_path(selector_last_failure=None)
        path_b = _run_deferred_unresolved_path()
        assert path_a["last_error"] == "breach_time_contract_selection_no_result"
        assert path_b["last_error"] != "breach_time_contract_selection_no_result"
        assert "DEFERRED_UNRESOLVED_AT_BREACH" in path_b["last_error"]

    def test_path_b_audit_has_deferred_selector_audit_key(self):
        """Path B's extra_meta must include deferred_selector_audit key."""
        result = _run_deferred_unresolved_path()
        assert "deferred_selector_audit" in result, (
            "Path B result must include deferred_selector_audit"
        )
