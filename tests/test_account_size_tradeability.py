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
        idx = _SRC.find('reason_code="UNTRADEABLE_FOR_ACCOUNT_SIZE"')
        region = _SRC[idx - 1200: idx + 600]
        # The gate keys off premium vs budget, never a name set
        assert "premium_per_contract" in region
        assert "budget" in region
        # explicit guard: no hardcoded ticker exclusion set near the gate
        # (the explanation text legitimately contains the phrase "no ticker
        # blacklist", so we check for an actual set/list of tickers instead).
        assert "EXCLUDED_TICKERS" not in region
        assert "BLACKLISTED_TICKERS" not in region
        assert "NVDA" not in region and "AMD" not in region

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


class TestSizingContextReading:
    """Amendment: read sizing diagnostics from plan.metadata['sizing_context']
    first; ensure they survive downstream into queue/deferred audit."""

    def test_sizing_ctx_helper_present(self):
        assert "def _plan_sizing_ctx(" in _SRC
        assert "def _sizing_val(" in _SRC
        assert 'meta.get("sizing_context")' in _SRC

    def test_diag_reads_sizing_context_first(self):
        """The diag must source equity/budget via _sizing_val (sizing_context
        first), not bare top-level _safe_plan_attr."""
        idx = _SRC.find("_tradeability_diag = {")
        block = _SRC[idx - 700: idx]
        assert "_sizing_val(plan" in block
        assert '"account_equity", "equity"' in block
        assert '"budget", "max_position_usd"' in block

    def test_diag_flattened_into_explanation(self):
        """The key numbers must be embedded in the explanation string so they
        survive downstream paths that persist explanation but may drop the
        structured tradeability_diag dict."""
        idx = _SRC.find("_diag_summary = (")
        assert idx != -1
        block = _SRC[idx: idx + 400]
        for token in ("equity=$", "budget=$", "premium=$", "underlying=$", "dte="):
            assert token in block, f"missing {token} in flattened diag summary"
        # and the explanation must include the summary
        exp_idx = _SRC.find("_explanation = (")
        exp_block = _SRC[exp_idx: exp_idx + 400]
        assert "_diag_summary" in exp_block

    def test_last_failure_carries_both(self):
        """_last_failure must carry the flattened explanation AND the structured
        diag, so whichever downstream copies, the numbers reach the order row."""
        idx = _SRC.find('"reason_code": "UNTRADEABLE_FOR_ACCOUNT_SIZE"')
        block = _SRC[idx: idx + 300]
        assert '"explanation": _explanation' in block
        assert '"tradeability_diag": _tradeability_diag' in block


# ---------------------------------------------------------------------------
# Runtime proof that _sizing_val reads metadata['sizing_context'] first
# ---------------------------------------------------------------------------

import sys, os, importlib.util
from pathlib import Path as _Path
from unittest.mock import MagicMock, patch


def _load_selector():
    stubs = {"ap.brokers": MagicMock(), "ap.brokers.tradier": MagicMock(),
             "yfinance": MagicMock(), "requests": MagicMock()}
    name = "ap_cs_pr2_shim"
    repo = _Path(__file__).resolve().parents[1]
    with patch.dict(sys.modules, stubs):
        spec = importlib.util.spec_from_file_location(name, repo / "ap" / "contract_selector.py")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[name] = mod
        try:
            spec.loader.exec_module(mod)
        finally:
            sys.modules.pop(name, None)
    return mod


class TestSizingValRuntime:
    def test_reads_from_sizing_context_object_plan(self):
        mod = _load_selector()
        plan = MagicMock()
        plan.metadata = {"sizing_context": {"account_equity": 1987.0, "budget": 198.7}}
        # top-level attrs absent/None on purpose
        plan.account_equity = None
        assert mod._sizing_val(plan, "account_equity", "equity") == 1987.0
        assert mod._sizing_val(plan, "budget", "max_position_usd") == 198.7

    def test_reads_from_sizing_context_dict_plan(self):
        mod = _load_selector()
        plan = {"metadata": {"sizing_context": {"risk_pct": 0.10}}}
        assert mod._sizing_val(plan, "risk_pct", "max_position_pct") == 0.10

    def test_falls_back_to_top_level_when_no_ctx(self):
        mod = _load_selector()
        plan = MagicMock()
        plan.metadata = {}
        plan.account_equity = 5000.0
        assert mod._sizing_val(plan, "account_equity", "equity") == 5000.0

    def test_returns_default_when_nothing(self):
        mod = _load_selector()
        plan = {"metadata": {}}
        assert mod._sizing_val(plan, "account_equity", default=0) == 0
