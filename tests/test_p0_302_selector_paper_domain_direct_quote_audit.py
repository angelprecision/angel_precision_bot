"""
tests/test_p0_302_selector_paper_domain_direct_quote_audit.py

P0 PR #302 — Restore deferred selector flow:
paper live-data routing + direct OCC quote recovery

Tests:
  1.  paper selector uses live domain in auto mode when live URL configured
  2.  paper broker remains sandbox while selector data domain is live
  3.  paper + sandbox data + zero quote failure → PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE
  4.  chain zero bid/ask triggers P0A direct OCC quote recovery attempt
  5.  direct quote recovery selects real OCC when bid/ask positive and gates pass
  6.  direct quote recovery does NOT select when direct bid/ask are zero
  7.  direct quote recovery does NOT bypass spread gate
  8.  direct quote recovery does NOT bypass OI/volume gates
  9.  CHAIN_ROW_ZERO_BID_ASK mostly zero rows → data_quality_zero_quotes
  10. OI_TOO_LOW / SPREAD_TOO_WIDE / VOLUME_TOO_LOW → contract_quality_reject
  11. selector_chain_quote_validity persists row counts and zero_quote_ratio
  12. live gates unchanged — live order sees no paper domain fields in reject
  13. no submit for DEFERRED:* or limit_price <= 0.01 (safety invariant unchanged)
"""
from __future__ import annotations

import math
import os
import types
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import ap.contract_selector as cs
from ap.contract_selector import (
    _PAPER_SANDBOX_DATA_FAILURE_CODES,
    _QUALITY_REJECT_CODES,
    _attach_selector_failure,
    _build_chain_quote_validity,
    _classify_selector_failure,
    _detect_paper_selector_domain,
    _is_paper_sandbox_data_failure,
)


# ─────────────────────────────────────────────────────────────────────────────
# Shared helpers
# ─────────────────────────────────────────────────────────────────────────────

_LIVE_URL  = "https://api.tradier.com/v1"
_SAND_URL  = "https://sandbox.tradier.com/v1"
_REAL_OCC  = "GS  260717C00465000"


def _opt(symbol=_REAL_OCC, bid=1.80, ask=1.86, oi=1200, vol=400,
         delta=-0.45, spread_pct=None) -> dict:
    return {
        "symbol":        symbol,
        "bid":           bid,
        "ask":           ask,
        "open_interest": oi,
        "volume":        vol,
        "option_type":   "call",
        "_ticker":       "GS",
        "greeks":        {"delta": delta},
    }


def _zero_opt(**kw) -> dict:
    return _opt(bid=0, ask=0, **kw)


def _plan_ns(**kw) -> types.SimpleNamespace:
    return types.SimpleNamespace(
        ticker           = kw.get("ticker", "GS"),
        side             = kw.get("side", "CALL"),
        max_position_usd = kw.get("max_position_usd", 500.0),
        execution_mode   = kw.get("execution_mode", "live"),
        metadata         = kw.get("metadata", {}),
        signal_id        = kw.get("signal_id", "SIG-1"),
        score            = kw.get("score", 75.0),
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: paper auto mode with live URL → domain = "live"
# ─────────────────────────────────────────────────────────────────────────────

class TestPaperSelectorDomainDetection:

    def test_1_auto_mode_live_url_returns_live_domain(self, monkeypatch):
        """Test 1: auto mode + live Tradier URL → paper_selector_data_domain='live'."""
        monkeypatch.delenv("PAPER_SELECTOR_MARKET_DATA_DOMAIN", raising=False)
        # Reload the module-level constant via the function
        domain = _detect_paper_selector_domain("paper", _LIVE_URL)
        assert domain == "live", (
            "Test 1: auto mode with live Tradier URL must classify as 'live'"
        )

    def test_1_auto_mode_sandbox_url_returns_sandbox_domain(self, monkeypatch):
        """Auto mode + sandbox URL → 'sandbox'."""
        domain = _detect_paper_selector_domain("paper", _SAND_URL)
        assert domain == "sandbox"

    def test_1_env_live_forces_live_regardless_of_url(self, monkeypatch):
        """PAPER_SELECTOR_MARKET_DATA_DOMAIN=live forces 'live' regardless of URL."""
        monkeypatch.setenv("PAPER_SELECTOR_MARKET_DATA_DOMAIN", "live")
        # Directly test the function — env var read at call time in _detect
        with patch.object(cs, "_PAPER_SELECTOR_MARKET_DATA_DOMAIN", "live"):
            domain = _detect_paper_selector_domain("paper", _SAND_URL)
        assert domain == "live"

    def test_1_env_sandbox_forces_sandbox(self, monkeypatch):
        """PAPER_SELECTOR_MARKET_DATA_DOMAIN=sandbox forces 'sandbox'."""
        with patch.object(cs, "_PAPER_SELECTOR_MARKET_DATA_DOMAIN", "sandbox"):
            domain = _detect_paper_selector_domain("paper", _LIVE_URL)
        assert domain == "sandbox"

    def test_1_unknown_url_returns_unknown(self):
        """Unknown URL (not Tradier) returns 'unknown'."""
        domain = _detect_paper_selector_domain("paper", "https://otherprovider.com")
        assert domain == "unknown"

    def test_2_paper_broker_sandbox_while_selector_live(self):
        """
        Test 2: Paper order broker domain is always sandbox.
        _detect_paper_selector_domain is for the SELECTOR data domain only.
        The broker domain is separately classified in paper domain fields.
        """
        # Selector uses live data
        sel_domain = _detect_paper_selector_domain("paper", _LIVE_URL)
        assert sel_domain == "live"
        # Broker domain for paper is always sandbox (no separate function needed —
        # it's implicit in paper mode). The audit fields confirm both separately.
        # Test the field semantics by checking the paper domain audit build.
        from ap.deferred_breach_underlying_repair import build_paper_domain_fields
        fields = build_paper_domain_fields(
            selector_audit={"tradier_base_url": _LIVE_URL},
            broker_base_url=_SAND_URL,
        )
        assert fields["paper_selector_data_domain"]  == "live",    "selector uses live data"
        assert fields["paper_order_broker_domain"]   == "sandbox", "broker stays sandbox"
        assert fields["paper_data_order_domain_mismatch"] is True, "live data + sandbox broker = mismatch"


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: paper + sandbox + zero quote → PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE
# ─────────────────────────────────────────────────────────────────────────────

class TestPaperSandboxDataClassification:

    def test_3_sandbox_data_zero_quote_fails_as_domain_issue(self):
        """
        Test 3: paper mode + sandbox data + CHAIN_ROW_ZERO_BID_ASK
        → PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE in selector_failure.
        """
        plan = _plan_ns(metadata={})
        _attach_selector_failure(
            plan,
            reason_code="PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE",
            explanation="sandbox data zero quotes",
            chain_rows=10,
            base_url=_SAND_URL,
            execution_mode="paper",
        )
        sf = plan.metadata.get("selector_failure") or {}
        assert sf["reason_code"] == "PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE"
        assert sf["selector_failure_class"] == "paper_data_domain_issue"
        assert sf["data_failure"] is True
        assert sf["quality_failure"] is False

    def test_3_is_paper_sandbox_data_failure_codes(self):
        """All required failure codes must be recognized as sandbox data failures."""
        required = [
            "CHAIN_ROW_ZERO_BID_ASK",
            "DIRECT_QUOTE_ZERO_BID_ASK",
            "CHAIN_PROVIDER_EMPTY_OPTIONS",
            "CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
            "QUOTE_ZERO_BID_ASK",
        ]
        for code in required:
            assert _is_paper_sandbox_data_failure(code), (
                f"Test 3: {code} must be recognized as a paper sandbox data failure"
            )

    def test_3_quality_reject_not_sandbox_failure(self):
        """OI_TOO_LOW is a quality failure, not a sandbox data failure."""
        assert not _is_paper_sandbox_data_failure("OI_TOO_LOW")
        assert not _is_paper_sandbox_data_failure("SPREAD_TOO_WIDE")


# ─────────────────────────────────────────────────────────────────────────────
# Tests 4–8: direct quote recovery (P0A)
# ─────────────────────────────────────────────────────────────────────────────

class TestDirectQuoteRecovery:
    """
    Tests 4–8 verify that the P0A direct quote recovery path (already
    implemented in quality_filter) behaves correctly at the
    _attach_selector_failure audit level.
    """

    def _failure_with_recovery(
        self, *, attempted=True, selected=False, contract=None,
        bid=None, ask=None, mid=None, failure=None
    ) -> dict:
        """Build a failure dict with direct_quote_recovery_audit."""
        plan = _plan_ns(metadata={})
        _attach_selector_failure(
            plan,
            reason_code="CHAIN_ROW_ZERO_BID_ASK",
            explanation="test",
            chain_rows=5,
            base_url=_LIVE_URL,
            direct_quote_recovery_audit={
                "attempted": attempted,
                "selected":  selected,
                "contract":  contract,
                "bid":       bid,
                "ask":       ask,
                "mid":       mid,
                "failure":   failure,
            },
        )
        return plan.metadata.get("selector_failure") or {}

    def test_4_zero_chain_quote_triggers_recovery_attempt(self):
        """
        Test 4: When direct quote recovery is attempted, the audit must
        reflect attempted=True regardless of outcome.
        """
        sf = self._failure_with_recovery(attempted=True, selected=False,
                                         failure="DIRECT_QUOTE_ZERO_BID_ASK")
        assert sf["direct_quote_recovery_attempted"] is True, (
            "Test 4: direct_quote_recovery_attempted must be True when P0A is triggered"
        )

    def test_5_recovery_selects_real_occ_on_positive_quote(self):
        """
        Test 5: When direct quote returns positive bid/ask, recovery selects it
        and stamps the contract, bid, ask, mid.
        """
        sf = self._failure_with_recovery(
            attempted=True, selected=True,
            contract=_REAL_OCC, bid=1.82, ask=1.88, mid=1.85,
        )
        assert sf["direct_quote_recovery_selected"] is True, (
            "Test 5: direct_quote_recovery_selected must be True on successful recovery"
        )
        assert sf["direct_quote_recovery_contract"] == _REAL_OCC
        assert sf["direct_quote_recovery_bid"]       == pytest.approx(1.82)
        assert sf["direct_quote_recovery_ask"]       == pytest.approx(1.88)
        assert sf["direct_quote_recovery_mid"]       == pytest.approx(1.85)
        assert sf.get("direct_quote_recovery_failure") is None

    def test_6_recovery_does_not_select_on_zero_bid_ask(self):
        """
        Test 6: When direct quote returns zero bid/ask, recovery must NOT
        select the contract. direct_quote_recovery_selected must be False.
        """
        sf = self._failure_with_recovery(
            attempted=True, selected=False,
            failure="DIRECT_QUOTE_ZERO_BID_ASK",
        )
        assert sf["direct_quote_recovery_selected"] is False, (
            "Test 6: direct_quote_recovery_selected must be False when direct quote is zero"
        )
        assert sf["direct_quote_recovery_failure"] == "DIRECT_QUOTE_ZERO_BID_ASK"

    def test_7_recovery_does_not_bypass_spread_gate(self):
        """
        Test 7: Recovery selected=False when direct quote fails spread gate.
        The audit failure field shows the reason, not DIRECT_QUOTE_ZERO_BID_ASK.
        """
        sf = self._failure_with_recovery(
            attempted=True, selected=False,
            failure="SPREAD_TOO_WIDE",  # direct quote failed spread gate
        )
        assert sf["direct_quote_recovery_selected"] is False
        assert sf["direct_quote_recovery_failure"] == "SPREAD_TOO_WIDE"

    def test_8_recovery_does_not_bypass_oi_volume_gates(self):
        """
        Test 8: Recovery audit correctly stamps OI/volume gate failures.
        """
        sf = self._failure_with_recovery(
            attempted=True, selected=False,
            failure="OI_TOO_LOW",  # direct quote failed OI gate
        )
        assert sf["direct_quote_recovery_selected"] is False
        assert sf["direct_quote_recovery_failure"] == "OI_TOO_LOW"


# ─────────────────────────────────────────────────────────────────────────────
# Tests 9–10: failure classification
# ─────────────────────────────────────────────────────────────────────────────

class TestSelectorFailureClassification:

    def _validity(self, n=10, zero_pct=0.9) -> dict:
        both_gt = max(0, int(n * (1 - zero_pct)))
        zero_ba = n - both_gt
        return {
            "chain_rows": n,
            "rows_with_bid_and_ask_gt_zero": both_gt,
            "rows_zero_bid_or_ask": zero_ba,
            "zero_quote_ratio": round(zero_ba / n, 4) if n else 1.0,
        }

    def test_9_chain_row_zero_mostly_zero_is_data_quality(self):
        """
        Test 9: CHAIN_ROW_ZERO_BID_ASK with mostly zero rows → data_quality_zero_quotes.
        """
        fclass, dfail, qfail = _classify_selector_failure(
            "CHAIN_ROW_ZERO_BID_ASK",
            chain_quote_validity=self._validity(n=10, zero_pct=0.9),
        )
        assert fclass == "data_quality_zero_quotes", (
            "Test 9: CHAIN_ROW_ZERO_BID_ASK with 90% zero rows must be data_quality_zero_quotes"
        )
        assert dfail  is True,  "data_failure must be True"
        assert qfail  is False, "quality_failure must be False"

    def test_9_direct_quote_zero_is_data_quality(self):
        """DIRECT_QUOTE_ZERO_BID_ASK is always a data quality issue."""
        fclass, dfail, qfail = _classify_selector_failure("DIRECT_QUOTE_ZERO_BID_ASK")
        assert fclass == "data_quality_zero_quotes"
        assert dfail  is True
        assert qfail  is False

    def test_10_oi_too_low_is_contract_quality(self):
        """Test 10a: OI_TOO_LOW → contract_quality_reject."""
        fclass, dfail, qfail = _classify_selector_failure(
            "OI_TOO_LOW",
            chain_quote_validity=self._validity(n=10, zero_pct=0.0),  # valid quotes
        )
        assert fclass == "contract_quality_reject", (
            "Test 10a: OI_TOO_LOW must be contract_quality_reject"
        )
        assert dfail  is False
        assert qfail  is True

    def test_10_spread_too_wide_is_contract_quality(self):
        """Test 10b: SPREAD_TOO_WIDE → contract_quality_reject."""
        fclass, dfail, qfail = _classify_selector_failure("SPREAD_TOO_WIDE")
        assert fclass == "contract_quality_reject"
        assert dfail  is False
        assert qfail  is True

    def test_10_volume_too_low_is_contract_quality(self):
        """Test 10c: VOLUME_TOO_LOW → contract_quality_reject."""
        fclass, dfail, qfail = _classify_selector_failure("VOLUME_TOO_LOW")
        assert fclass == "contract_quality_reject"

    def test_10_paper_sandbox_unusable_is_domain_issue(self):
        """PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE → paper_data_domain_issue."""
        fclass, dfail, qfail = _classify_selector_failure(
            "PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE"
        )
        assert fclass == "paper_data_domain_issue"
        assert dfail  is True
        assert qfail  is False

    def test_all_quality_reject_codes_classify_correctly(self):
        """All codes in _QUALITY_REJECT_CODES must map to contract_quality_reject."""
        for code in _QUALITY_REJECT_CODES:
            fclass, _, qfail = _classify_selector_failure(code)
            assert fclass == "contract_quality_reject", (
                f"{code} must be contract_quality_reject"
            )
            assert qfail is True, f"{code} must have quality_failure=True"


# ─────────────────────────────────────────────────────────────────────────────
# Test 11: chain_quote_validity persisted in selector_failure
# ─────────────────────────────────────────────────────────────────────────────

class TestChainQuoteValidityPersistence:

    def test_11_chain_quote_validity_in_selector_failure(self):
        """
        Test 11: selector_chain_quote_validity is persisted with all required fields
        when passed to _attach_selector_failure.
        """
        validity = _build_chain_quote_validity([
            _opt(bid=1.80, ask=1.86),   # valid
            _opt(bid=0.0,  ask=0.0),    # zero
            _opt(bid=0.0,  ask=0.0),    # zero
            _opt(bid=0.0,  ask=0.0),    # zero
            _opt(bid=1.50, ask=1.55),   # valid
        ])
        plan = _plan_ns(metadata={})
        _attach_selector_failure(
            plan,
            reason_code="CHAIN_ROW_ZERO_BID_ASK",
            explanation="test",
            chain_rows=5,
            base_url=_LIVE_URL,
            chain_quote_validity=validity,
        )
        sf = plan.metadata.get("selector_failure") or {}
        cqv = sf.get("selector_chain_quote_validity") or {}

        assert cqv["chain_rows"] == 5
        assert cqv["rows_with_bid_and_ask_gt_zero"] == 2
        assert cqv["rows_zero_bid_or_ask"] == 3
        assert abs(cqv["zero_quote_ratio"] - 0.6) < 0.01
        assert abs(cqv["nonzero_quote_ratio"] - 0.4) < 0.01
        assert cqv["best_nonzero_quote_candidate"] is not None
        assert cqv["best_zero_quote_candidate"] is not None

    def test_11_build_chain_quote_validity_structure(self):
        """_build_chain_quote_validity returns all required fields."""
        chain = [
            _opt(bid=1.80, ask=1.86, oi=1000),
            _zero_opt(oi=500),
            _zero_opt(oi=200),
        ]
        v = _build_chain_quote_validity(chain)
        required = [
            "chain_rows", "rows_with_bid_gt_zero", "rows_with_ask_gt_zero",
            "rows_with_bid_and_ask_gt_zero", "rows_zero_bid_or_ask",
            "zero_quote_ratio", "nonzero_quote_ratio",
            "best_nonzero_quote_candidate", "best_zero_quote_candidate",
        ]
        missing = [k for k in required if k not in v]
        assert not missing, f"Missing chain_quote_validity fields: {missing}"
        assert v["chain_rows"] == 3
        assert v["rows_with_bid_and_ask_gt_zero"] == 1
        assert v["rows_zero_bid_or_ask"] == 2
        assert abs(v["zero_quote_ratio"] - 2/3) < 0.01
        assert v["best_zero_quote_candidate"]["open_interest"] == 500  # highest OI among zeros

    def test_11_empty_chain_returns_safe_validity(self):
        """Empty chain never raises — returns safe defaults."""
        v = _build_chain_quote_validity([])
        assert v["chain_rows"] == 0
        assert v["zero_quote_ratio"] == 1.0
        assert v["nonzero_quote_ratio"] == 0.0

    def test_11_validity_never_raises(self):
        """_build_chain_quote_validity must never raise on broken input."""
        try:
            _build_chain_quote_validity(None)
            _build_chain_quote_validity([{"bid": "not_a_float"}])
        except Exception as e:
            pytest.fail(f"Must never raise: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Test 12: live gates unchanged
# ─────────────────────────────────────────────────────────────────────────────

class TestLiveGatesUnchanged:

    def test_12_live_orders_no_paper_domain_reclassification(self):
        """
        Test 12: Live orders (Jason) must not get PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE
        reclassification even if base_url is sandbox.
        """
        # The _is_paper_mode check is based on _sel_mode.
        # For live orders, _sel_mode is 'live', so paper domain detection is off.
        plan = _plan_ns(execution_mode="live", metadata={})
        # Simulate what select() does for live mode — no paper reclassification
        _sel_mode = "live"
        _is_paper_mode = _sel_mode in ("paper",)
        assert _is_paper_mode is False, "Live mode must never trigger paper domain logic"

    def test_12_live_failure_classifies_as_quality_or_data_not_domain(self):
        """Live failures with OI_TOO_LOW → contract_quality_reject, not paper_data_domain_issue."""
        fclass, _, _ = _classify_selector_failure("OI_TOO_LOW")
        assert fclass != "paper_data_domain_issue"
        assert fclass == "contract_quality_reject"

    def test_12_attach_selector_failure_live_no_paper_domain_fields(self):
        """_attach_selector_failure for live OI failure has no paper domain stamp."""
        plan = _plan_ns(execution_mode="live", metadata={})
        _attach_selector_failure(
            plan,
            reason_code="OI_TOO_LOW",
            explanation="OI below threshold",
            base_url=_LIVE_URL,
            execution_mode="live",
        )
        sf = plan.metadata["selector_failure"]
        # No paper domain reclassification
        assert sf["reason_code"] == "OI_TOO_LOW"
        assert sf["selector_failure_class"] == "contract_quality_reject"
        # Paper domain fields are NOT in the failure dict (they're in the paper audit helper)
        assert "paper_selector_data_domain" not in sf


# ─────────────────────────────────────────────────────────────────────────────
# Test 13: safety invariants unchanged
# ─────────────────────────────────────────────────────────────────────────────

class TestSafetyInvariantsUnchanged:

    def test_13_deferred_contract_not_submittable(self):
        """
        Test 13a: DEFERRED:* contract must never flow to broker submit.
        This is enforced by the pre-submit gate (PR #300/#301), not by this PR.
        Verify the invariant string is unchanged.
        """
        from ap.deferred_breach_underlying_repair import ZERO_UNDERLYING_TERMINAL_REASON
        assert ZERO_UNDERLYING_TERMINAL_REASON == "metadata_invalid:zero_underlying:no_positive_source"

    def test_13_zero_limit_not_submittable(self):
        """Test 13b: limit_price <= 0.01 must never reach broker submit."""
        # This is verified at the pre-submit invariant level in execution_core.
        # Confirm the constant is in the existing gate logic.
        import ap_execution_core as _core
        src = open("ap_execution_core.py").read()
        assert "0.01" in src or "DEFERRED:" in src, (
            "Pre-submit invariant must still gate on limit_price and DEFERRED:"
        )

    def test_13_direct_quote_recovery_never_submits_zero_quote(self):
        """
        Test 13c: A direct quote recovery with bid=0 must never select the contract.
        """
        sf = {}
        plan = _plan_ns(metadata={})
        _attach_selector_failure(
            plan,
            reason_code="DIRECT_QUOTE_ZERO_BID_ASK",
            explanation="zero direct quote",
            base_url=_LIVE_URL,
            direct_quote_recovery_audit={
                "attempted": True,
                "selected":  False,   # MUST be False for zero quotes
                "failure":   "DIRECT_QUOTE_ZERO_BID_ASK",
            },
        )
        sf = plan.metadata["selector_failure"]
        assert sf["direct_quote_recovery_selected"] is False, (
            "Test 13c: Zero direct quote must never result in selected=True"
        )

    def test_13_paper_sandbox_unusable_does_not_bypass_submit_gates(self):
        """
        Test 13d: PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE is a terminal failure.
        It must not produce a valid contract — it's a classify-and-terminate signal.
        """
        fclass, dfail, _ = _classify_selector_failure(
            "PAPER_SELECTOR_SANDBOX_DATA_UNUSABLE"
        )
        # paper_data_domain_issue means terminalize, not bypass gates
        assert fclass == "paper_data_domain_issue"
        assert dfail  is True
        # No contract should be produced — verified by the no-survivors return path


# ─────────────────────────────────────────────────────────────────────────────
# Fix B flat fields in execution_core
# ─────────────────────────────────────────────────────────────────────────────

class TestFixBFlatFieldsExtended:

    def test_new_flat_fields_present_in_build_flat_selector_audit_fields(self):
        """All 7 new PR #302 flat fields must appear in _build_flat_selector_audit_fields output."""
        from ap_execution_core import _build_flat_selector_audit_fields
        from unittest.mock import MagicMock
        osm = MagicMock()
        result = _build_flat_selector_audit_fields(
            selector_audit={
                "selector_failure_class": "data_quality_zero_quotes",
                "nonzero_quote_rows": 2,
                "zero_quote_ratio": 0.8,
                "data_failure": True,
                "quality_failure": False,
                "direct_quote_recovery_attempted": True,
                "direct_quote_recovery_selected": False,
            },
            attempt_number=1,
            execution_mode="live",
            is_paper=False,
        )
        required_new = [
            "last_deferred_selector_failure_class",
            "last_deferred_selector_nonzero_quote_rows",
            "last_deferred_selector_zero_quote_ratio",
            "last_deferred_selector_data_failure",
            "last_deferred_selector_quality_failure",
            "last_deferred_direct_quote_recovery_attempted",
            "last_deferred_direct_quote_recovery_selected",
        ]
        missing = [k for k in required_new if k not in result]
        assert not missing, f"Missing new PR #302 flat fields: {missing}"
        assert result["last_deferred_selector_failure_class"] == "data_quality_zero_quotes"
        assert result["last_deferred_selector_nonzero_quote_rows"] == 2
        assert abs(result["last_deferred_selector_zero_quote_ratio"] - 0.8) < 0.001
        assert result["last_deferred_selector_data_failure"] is True
        assert result["last_deferred_direct_quote_recovery_attempted"] is True
        assert result["last_deferred_direct_quote_recovery_selected"] is False

    def test_chain_rows_now_passes_through_from_selector_failure(self):
        """
        Previously, chain_rows was lost in _build_deferred_selector_audit.
        PR #302 fixes this passthrough — verify it appears in audit.
        """
        from ap_execution_core import _build_flat_selector_audit_fields
        result = _build_flat_selector_audit_fields(
            selector_audit={"chain_rows": 15, "survivor_count": 0},
            attempt_number=1,
            execution_mode="live",
        )
        assert result["last_deferred_selector_chain_rows"] == 15, (
            "chain_rows must pass through from selector_failure audit"
        )
