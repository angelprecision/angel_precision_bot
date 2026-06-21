"""
tests/test_account_size_tradeability.py — PR2

Verifies the UNTRADEABLE_FOR_ACCOUNT_SIZE classification:
  - When a QUALITY contract exists but exceeds the per-trade budget (LIVE), the
    selector classifies UNTRADEABLE_FOR_ACCOUNT_SIZE (not generic
    NO_AFFORDABLE_CONTRACT), with full budget diagnostics.
  - No ticker-name blacklist: classification is contract/budget based only.
  - Quality gates are not loosened.
  - The classification is breach-time authoritative (runs after the chain /
    DTE ladder is fully evaluated).
"""
from __future__ import annotations

from pathlib import Path

_REPO = Path(__file__).resolve().parents[1]
_SRC  = (_REPO / "ap" / "contract_selector.py").read_text()


class TestClassificationPresent:
    def test_reason_code_defined(self):
        assert "UNTRADEABLE_FOR_ACCOUNT_SIZE" in _SRC

    def test_emitted_in_live_affordability_branch(self):
        """The classification must be in the LIVE affordability reject branch,
        replacing the generic NO_AFFORDABLE_CONTRACT for the budget-exceeds
        case."""
        idx = _SRC.find("UNTRADEABLE_FOR_ACCOUNT_SIZE")
        # find the emit_selector_event that carries it
        region_start = _SRC.find('reason_code="UNTRADEABLE_FOR_ACCOUNT_SIZE"')
        assert region_start != -1
        region = _SRC[region_start - 400: region_start + 400]
        assert "affordability_gate" in region

    def test_records_last_failure(self):
        """Must set _last_failure so the queue + deferred audit surface it."""
        idx = _SRC.find('"reason_code": "UNTRADEABLE_FOR_ACCOUNT_SIZE"')
        assert idx != -1
        region = _SRC[idx - 200: idx + 300]
        assert "tradeability_diag" in region


class TestDiagnostics:
    def test_diag_carries_required_fields(self):
        """Spec-required diagnostics for each rejected ticker."""
        idx = _SRC.find("_tradeability_diag = {")
        assert idx != -1
        block = _SRC[idx: idx + 700]
        for field in (
            "equity",
            "max_position_pct",
            "max_trade_usd",
            "max_affordable_premium",
            "underlying_price",
            "near_atm_premium_estimate",
            "cheapest_quality_survivor_premium",
        ):
            assert field in block, f"missing diagnostic field {field}"


class TestNoBlacklist:
    def test_no_ticker_name_blacklist(self):
        """Classification must be budget/contract based — no hardcoded
        NVDA/AMD/etc. exclusion list anywhere near the gate."""
        idx = _SRC.find("UNTRADEABLE_FOR_ACCOUNT_SIZE")
        region = _SRC[idx - 600: idx + 1200]
        # The gate keys off premium vs budget, never a name set
        assert "premium_per_contract" in region
        assert "budget" in region
        # explicit guard: it must not reference a ticker exclusion set here
        assert "BLACKLIST" not in region.upper()

    def test_decision_unchanged_still_returns_none(self):
        """PR2 only upgrades the REASON; the live reject still returns None."""
        region_start = _SRC.find('reason_code="UNTRADEABLE_FOR_ACCOUNT_SIZE"')
        region = _SRC[region_start: region_start + 1400]
        assert "return None" in region


class TestQualityGatesUntouched:
    def test_no_gate_thresholds_mutated(self):
        """The PR2 branch must not assign min_oi/min_volume/max_spread_pct."""
        idx = _SRC.find("PR2: account-size tradeability classification")
        # scan the surrounding classification block
        block = _SRC[idx: idx + 2500]
        assert "self.min_oi =" not in block
        assert "self.min_volume =" not in block
        assert "self.max_spread_pct =" not in block


class TestBreachTimeAuthoritative:
    def test_classification_after_chain_eval(self):
        """The classification lives in the affordability gate, which runs after
        survivors are computed and ranked — i.e. after the chain (and PR1 ladder)
        is fully evaluated. Guard: the UNTRADEABLE branch appears after the
        'if not survivors' quality summary in source order."""
        no_survivors = _SRC.find("if not survivors:")
        untradeable  = _SRC.find('reason_code="UNTRADEABLE_FOR_ACCOUNT_SIZE"')
        assert no_survivors != -1 and untradeable != -1
        assert untradeable > no_survivors
