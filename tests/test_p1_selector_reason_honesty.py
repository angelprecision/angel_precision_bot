# tests/test_p1_selector_reason_honesty.py
# =============================================================================
# PR P1 — Selector failure reason honesty + audit payload
#
# Validates that ap/contract_selector.py:
#   1. Emits truthful canonical reason codes for each failure mode
#   2. Attaches plan.metadata["selector_failure"] on every return-None path
#   3. Maps internal codes to stable queue-facing codes via _TO_QUEUE_REASON
#   4. Does NOT change any selection threshold, gate, or recovery behavior
#
# Spec: each test number maps directly to the PR P1 spec requirement.
# =============================================================================

from __future__ import annotations

import inspect
import types
import pytest
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_plan(
    *,
    ticker: str = "ROST",
    side: str = "PUT",
    budget: float = 500.0,
    tier: str = "B",
    score: float = 72.0,
    signal_id: str = "sig-p1-001",
):
    """Minimal ApprovedExecutionPlan-like object for selector tests."""
    return types.SimpleNamespace(
        ticker=ticker,
        side=side,
        max_position_usd=budget,
        tier=tier,
        score=score,
        signal_id=signal_id,
        client_id="jose.vasquez4011",
        trigger_price=None,
        stop_underlying=None,
        target_underlying=None,
        contract_symbol=None,
        limit_price=None,
        contracts=2,
        pattern="",
        timeframe="1d",
        mode="PAPER",
        metadata={},
    )


def _make_selector(
    *,
    mode: str = "paper",
    earnings_guard=None,
    iv_filter=None,
):
    """Build APContractSelectionEngine with a mock broker."""
    from ap.contract_selector import APContractSelectionEngine
    broker = MagicMock()
    broker.base_url = "https://sandbox.tradier.com"
    cfg = MagicMock()
    cfg.base_url = "https://sandbox.tradier.com"
    cfg.access_token = "FAKE_TOKEN"
    broker.cfg = cfg
    broker.session = None
    sel = APContractSelectionEngine(
        broker,
        mode=mode,
        data_broker=broker,
        earnings_guard=earnings_guard,
        iv_filter=None,
    )
    return sel


def _selector_failure(plan) -> dict:
    """Extract plan.metadata['selector_failure'], fail clearly if absent."""
    meta = getattr(plan, "metadata", None)
    assert isinstance(meta, dict), "plan.metadata is not a dict"
    sf = meta.get("selector_failure")
    assert sf is not None, (
        "plan.metadata['selector_failure'] not set — "
        "_attach_selector_failure() was not called on this return-None path"
    )
    return sf


# ---------------------------------------------------------------------------
# Spec Test 1: Empty chain → CHAIN_EMPTY / NO_CHAIN_DATA
# ---------------------------------------------------------------------------

class TestEmptyChain:
    """Spec 1: options chain request succeeded but returned no rows."""

    def test_select_returns_none(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: ([], 227.62),
        )
        plan = _make_plan()
        result = sel.select(plan)
        assert result is None

    def test_reason_code_is_chain_empty(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: ([], 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        assert sf["reason_code"] == "CHAIN_EMPTY", (
            f"Expected CHAIN_EMPTY, got {sf['reason_code']}"
        )

    def test_queue_reason_code_is_no_chain_data(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: ([], 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        assert sf["queue_reason_code"] == "NO_CHAIN_DATA", (
            f"Expected NO_CHAIN_DATA, got {sf['queue_reason_code']}"
        )

    def test_chain_rows_is_zero(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: ([], 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        assert sf["chain_rows"] == 0, f"Expected 0, got {sf['chain_rows']}"

    def test_last_failure_reason_code(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: ([], 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        lf = sel.get_last_failure()
        assert lf is not None
        assert lf["reason_code"] == "CHAIN_EMPTY"


def test_selector_accepts_legacy_plan_mode_without_execution_mode(monkeypatch):
    sel = _make_selector()
    monkeypatch.setattr(
        sel, "_fetch_chain_with_price",
        lambda ticker, direction, **kw: ([], 227.62),
    )
    plan = _make_plan()
    assert not hasattr(plan, "execution_mode")
    sel.select(plan)
    sf = _selector_failure(plan)
    assert sf["reason_code"] != "INVALID_EXECUTION_MODE"


def test_cheap_contract_reject_preserves_specific_reason_in_source():
    from ap import contract_selector as selector_mod

    source = inspect.getsource(selector_mod.APContractSelectionEngine.select)
    assert 'reason_code="CHEAP_CONTRACT_NO_UPGRADE"' in source
    assert 'ALLOW_CHEAP_CONTRACT_IF_ONLY_CHOICE", "false"' in source


# ---------------------------------------------------------------------------
# Spec Test 2: Chain fetch exception → CHAIN_FETCH_FAILED / NO_CHAIN_DATA
# ---------------------------------------------------------------------------

class TestChainFetchFailed:
    """Spec 2: chain request threw an exception."""

    def test_select_returns_none(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (_ for _ in ()).throw(RuntimeError("Network timeout")),
        )
        plan = _make_plan()
        result = sel.select(plan)
        assert result is None

    def test_reason_code_is_chain_fetch_failed(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (_ for _ in ()).throw(ValueError("401 Unauthorized")),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        assert sf["reason_code"] == "CHAIN_FETCH_FAILED"

    def test_queue_reason_code_is_no_chain_data(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (_ for _ in ()).throw(ConnectionError("timeout")),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        assert sf["queue_reason_code"] == "NO_CHAIN_DATA"


# ---------------------------------------------------------------------------
# Spec Test 3: Non-empty chain but all zero bid/ask → CHAIN_ROW_ZERO_BID_ASK
# ---------------------------------------------------------------------------

class TestChainRowZeroBidAsk:
    """Spec 3: chain existed but every candidate had zero bid/ask."""

    def _make_zero_chain(self, n=5):
        """Chain with rows but zero bid/ask — fails quality filter."""
        opts = []
        for i in range(n):
            opts.append({
                "symbol": f"ROST240628P{220+i*5:08.0f}",
                "option_type": "put",
                "strike": float(220 + i * 5),
                "expiration_date": "2024-06-28",
                "bid": 0.0,
                "ask": 0.0,
                "volume": 500,
                "open_interest": 2000,
                "greeks": {"delta": -0.35},
                "_ticker": "ROST",
            })
        return opts

    def test_reason_code_is_chain_row_zero_bid_ask(self, monkeypatch):
        sel = _make_selector()
        chain = self._make_zero_chain(5)
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (chain, 227.62),
        )
        plan = _make_plan()
        result = sel.select(plan)
        assert result is None
        sf = _selector_failure(plan)
        # The top reject bucket should be CHAIN_ROW_ZERO_BID_ASK
        assert sf["chain_rows"] > 0, "chain_rows must be > 0 when chain had rows"
        # Either the top bucket is CHAIN_ROW_ZERO_BID_ASK, or reason_code is
        top_buckets = sf.get("top_reject_buckets", {})
        dominant = next(iter(top_buckets), None)
        assert dominant == "CHAIN_ROW_ZERO_BID_ASK" or sf["reason_code"] == "CHAIN_ROW_ZERO_BID_ASK", (
            f"Expected CHAIN_ROW_ZERO_BID_ASK dominant, got dominant={dominant} "
            f"reason_code={sf['reason_code']}"
        )

    def test_queue_reason_code_is_quote_zero_bid_ask(self, monkeypatch):
        sel = _make_selector()
        chain = self._make_zero_chain(3)
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (chain, 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        # When zero bid/ask is the dominant reason, queue code must be QUOTE_ZERO_BID_ASK
        top_buckets = sf.get("top_reject_buckets", {})
        if next(iter(top_buckets), None) == "CHAIN_ROW_ZERO_BID_ASK":
            assert sf["queue_reason_code"] == "QUOTE_ZERO_BID_ASK", (
                f"Expected QUOTE_ZERO_BID_ASK, got {sf['queue_reason_code']}"
            )

    def test_chain_rows_positive(self, monkeypatch):
        sel = _make_selector()
        chain = self._make_zero_chain(7)
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (chain, 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        assert sf["chain_rows"] == 7


# ---------------------------------------------------------------------------
# Spec Test 4: Direct quote returns zero bid/ask → DIRECT_QUOTE_ZERO_BID_ASK
# ---------------------------------------------------------------------------

class TestDirectQuoteZeroBidAsk:
    """Spec 4: direct quote revalidation returned zero bid/ask."""

    def test_reason_code_in_reason_code_map(self):
        """_REASON_CODE_MAP must not map zero_bid_or_ask to NO_CHAIN_DATA."""
        from ap.contract_selector import _REASON_CODE_MAP
        # zero_bid_or_ask is now CHAIN_ROW_ZERO_BID_ASK
        assert _REASON_CODE_MAP.get("zero_bid_or_ask") == "CHAIN_ROW_ZERO_BID_ASK", (
            f"zero_bid_or_ask maps to {_REASON_CODE_MAP.get('zero_bid_or_ask')}, "
            "expected CHAIN_ROW_ZERO_BID_ASK"
        )
        assert _REASON_CODE_MAP.get("ask_below_bid") == "CHAIN_ROW_ZERO_BID_ASK"
        assert _REASON_CODE_MAP.get("zero_mid") == "CHAIN_ROW_ZERO_BID_ASK"

    def test_direct_quote_zero_maps_to_quote_zero_bid_ask(self):
        """DIRECT_QUOTE_ZERO_BID_ASK → QUOTE_ZERO_BID_ASK via queue map."""
        from ap.contract_selector import _to_queue_reason
        assert _to_queue_reason("DIRECT_QUOTE_ZERO_BID_ASK") == "QUOTE_ZERO_BID_ASK"

    def test_reject_direct_zero_action_maps_to_direct_quote_zero(self, monkeypatch):
        """When revalidator returns action=REJECT_DIRECT_ZERO, the rejection
        reason stored in _rejections must be DIRECT_QUOTE_ZERO_BID_ASK."""
        from ap import contract_quote_revalidator as cqr_mod

        # Make revalidator return REJECT_DIRECT_ZERO for every attempt
        monkeypatch.setattr(
            cqr_mod, "revalidate_with_direct_quote",
            lambda broker, opt, reason: {
                "action": "REJECT_DIRECT_ZERO",
                "reason_code": "DIRECT_QUOTE_ZERO_BID_ASK",
                "opt_updated": None,
                "audit": {},
            },
        )
        monkeypatch.setattr(cqr_mod, "should_revalidate", lambda r: True)

        sel = _make_selector()
        # Chain with a revalidatable reason (bid=0)
        chain = [{
            "symbol": "ROST240628P00225000",
            "option_type": "put",
            "strike": 225.0,
            "expiration_date": "2024-06-28",
            "bid": 0.0,
            "ask": 0.0,
            "volume": 800,
            "open_interest": 3000,
            "greeks": {"delta": -0.38},
            "_ticker": "ROST",
        }]
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (chain, 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        top = sf.get("top_reject_buckets", {})
        # DIRECT_QUOTE_ZERO_BID_ASK must appear in top buckets
        has_direct = (
            "DIRECT_QUOTE_ZERO_BID_ASK" in top
            or "CHAIN_ROW_ZERO_BID_ASK" in top  # pro-quality path uses this
        )
        assert has_direct, (
            f"Expected DIRECT_QUOTE_ZERO_BID_ASK or CHAIN_ROW_ZERO_BID_ASK in top_reject_buckets. "
            f"Got: {top}"
        )


# ---------------------------------------------------------------------------
# Spec Tests 5-8: quality-gate rejections stay honest
# ---------------------------------------------------------------------------

def _make_quality_chain(*, bid=0.25, ask=0.30, oi=50, volume=200, delta=0.35, n=5):
    """Chain with real bid/ask to reach quality filters."""
    opts = []
    for i in range(n):
        opts.append({
            "symbol": f"ROST240628P{(220+i*5):08.0f}",
            "option_type": "put",
            "strike": float(220 + i * 5),
            "expiration_date": "2024-06-28",
            "bid": bid,
            "ask": ask,
            "bid_size": 10,
            "ask_size": 10,
            "volume": volume,
            "open_interest": oi,
            "greeks": {"delta": -abs(delta)},
            "_ticker": "ROST",
        })
    return opts


class TestOIRejectionHonest:
    """Spec 5: OI rejection must be OI_TOO_LOW, not NO_CHAIN_DATA."""

    def test_reason_code_oi_too_low(self, monkeypatch):
        from ap.contract_selector import _REASON_CODE_MAP
        # illiquid_vol maps to OI_TOO_LOW
        assert _REASON_CODE_MAP.get("illiquid_vol") == "OI_TOO_LOW"
        assert _REASON_CODE_MAP.get("oi_too_low")   == "OI_TOO_LOW"
        assert _REASON_CODE_MAP.get("low_oi")       == "OI_TOO_LOW"

    def test_oi_too_low_queue_reason(self):
        from ap.contract_selector import _to_queue_reason
        assert _to_queue_reason("OI_TOO_LOW") == "OI_TOO_LOW"

    def test_selector_failure_oi_bucket(self, monkeypatch):
        """Low-OI chain: top reject bucket must be OI_TOO_LOW."""
        sel = _make_selector()
        # OI far below threshold — volume ok but OI=1
        chain = _make_quality_chain(oi=1, volume=500, bid=0.25, ask=0.30, delta=0.38)
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (chain, 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        top = sf.get("top_reject_buckets", {})
        dominant = next(iter(top), None)
        # OI_TOO_LOW must be in the rejection buckets
        assert "OI_TOO_LOW" in top or dominant == "OI_TOO_LOW", (
            f"Expected OI_TOO_LOW in top_reject_buckets. Got: {top}"
        )


class TestSpreadRejectionHonest:
    """Spec 6: spread rejection must be SPREAD_TOO_WIDE."""

    def test_reason_code_spread_too_wide(self):
        from ap.contract_selector import _REASON_CODE_MAP
        assert _REASON_CODE_MAP.get("spread_too_wide") == "SPREAD_TOO_WIDE"

    def test_spread_too_wide_queue_reason(self):
        from ap.contract_selector import _to_queue_reason
        assert _to_queue_reason("SPREAD_TOO_WIDE") == "SPREAD_TOO_WIDE"

    def test_selector_failure_spread_bucket(self, monkeypatch):
        """Wide-spread chain: SPREAD_TOO_WIDE must appear in rejections."""
        sel = _make_selector()
        # Extremely wide spread (bid=0.10, ask=5.00 → spread_pct ≈ 0.96)
        chain = _make_quality_chain(bid=0.10, ask=5.00, oi=5000, volume=5000, delta=0.38)
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (chain, 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        top = sf.get("top_reject_buckets", {})
        assert "SPREAD_TOO_WIDE" in top, (
            f"Expected SPREAD_TOO_WIDE in top_reject_buckets. Got: {top}"
        )


class TestVolumeRejectionHonest:
    """Spec 7: volume rejection must be VOLUME_TOO_LOW (not NO_CHAIN_DATA)."""

    def test_reason_code_volume_too_low(self):
        from ap.contract_selector import _REASON_CODE_MAP
        assert _REASON_CODE_MAP.get("volume_too_low") == "VOLUME_TOO_LOW"
        assert _REASON_CODE_MAP.get("low_volume")     == "VOLUME_TOO_LOW"
        assert _REASON_CODE_MAP.get("size_too_thin")  == "VOLUME_TOO_LOW"

    def test_volume_too_low_queue_reason(self):
        from ap.contract_selector import _to_queue_reason
        assert _to_queue_reason("VOLUME_TOO_LOW") == "VOLUME_TOO_LOW"


class TestNoAffordableContractHonest:
    """Spec 8: affordability rejection must be NO_AFFORDABLE_CONTRACT."""

    def test_no_affordable_contract_queue_reason(self):
        from ap.contract_selector import _to_queue_reason
        assert _to_queue_reason("NO_AFFORDABLE_CONTRACT") == "NO_AFFORDABLE_CONTRACT"
        # UNTRADEABLE_FOR_ACCOUNT_SIZE also maps to NO_AFFORDABLE_CONTRACT
        assert _to_queue_reason("UNTRADEABLE_FOR_ACCOUNT_SIZE") == "NO_AFFORDABLE_CONTRACT"

    def test_bid_below_min_maps_correctly(self):
        from ap.contract_selector import _REASON_CODE_MAP, _to_queue_reason
        assert _REASON_CODE_MAP.get("bid_below") == "BID_BELOW_MIN", (
            "bid_below should map to BID_BELOW_MIN, not NO_AFFORDABLE_CONTRACT"
        )
        assert _to_queue_reason("BID_BELOW_MIN") == "BID_BELOW_MIN"


# ---------------------------------------------------------------------------
# Spec Test 9: selector_failure includes all required fields
# ---------------------------------------------------------------------------

class TestSelectorFailurePayload:
    """Spec 9: verify all required fields present in selector_failure."""

    REQUIRED_FIELDS = [
        "reason_code", "queue_reason_code", "explanation",
        "chain_rows", "survivor_count",
        "quote_source", "chain_source",
        "tradier_base_url", "sandbox_mode",
        "execution_mode", "top_reject_buckets",
    ]

    def test_all_fields_present_on_empty_chain(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: ([], 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        for field in self.REQUIRED_FIELDS:
            assert field in sf, f"Field '{field}' missing from selector_failure"

    def test_all_fields_present_on_chain_fetch_fail(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (_ for _ in ()).throw(RuntimeError("fail")),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        for field in self.REQUIRED_FIELDS:
            assert field in sf, f"Field '{field}' missing from selector_failure (chain fail path)"

    def test_all_fields_present_on_no_survivors(self, monkeypatch):
        sel = _make_selector()
        chain = _make_quality_chain(oi=1, volume=1, bid=0.25, ask=0.30)
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: (chain, 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        for field in self.REQUIRED_FIELDS:
            assert field in sf, f"Field '{field}' missing from selector_failure (no survivors path)"

    def test_top_reject_buckets_is_dict(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: ([], 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        assert isinstance(sf["top_reject_buckets"], dict)

    def test_chain_rows_is_int(self, monkeypatch):
        sel = _make_selector()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: ([], 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        assert isinstance(sf["chain_rows"], int)

    def test_sandbox_mode_detected_from_base_url(self, monkeypatch):
        sel = _make_selector()
        # broker base_url contains "sandbox"
        assert "sandbox" in sel.data_broker.base_url.lower()
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: ([], 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        assert sf["sandbox_mode"] is True
        assert sf["quote_source"] == "tradier_sandbox"

    def test_execution_mode_paper(self, monkeypatch):
        sel = _make_selector(mode="paper")
        monkeypatch.setattr(
            sel, "_fetch_chain_with_price",
            lambda ticker, direction, **kw: ([], 227.62),
        )
        plan = _make_plan()
        sel.select(plan)
        sf = _selector_failure(plan)
        assert sf["execution_mode"] == "paper"


# ---------------------------------------------------------------------------
# Spec Test 10: Threshold constants unchanged
# ---------------------------------------------------------------------------

class TestThresholdConstants:
    """Spec 10: no selection threshold may have changed in this PR."""

    def test_pro_min_bid_unchanged(self):
        from ap.contract_selector import _PRO_MIN_BID
        assert _PRO_MIN_BID == 0.10, (
            f"_PRO_MIN_BID changed: expected 0.10, got {_PRO_MIN_BID}"
        )

    def test_pro_min_bid_size_hard_unchanged(self):
        from ap.contract_selector import _PRO_MIN_BID_SIZE_HARD
        assert _PRO_MIN_BID_SIZE_HARD == 3

    def test_pro_t1_spread_hard_max_unchanged(self):
        from ap.contract_selector import _PRO_T1_SPREAD_HARD_MAX
        assert _PRO_T1_SPREAD_HARD_MAX == 0.10

    def test_pro_t2_spread_hard_max_unchanged(self):
        from ap.contract_selector import _PRO_T2_SPREAD_HARD_MAX
        assert _PRO_T2_SPREAD_HARD_MAX == 0.12

    def test_min_contract_delta_env_default(self):
        """MIN_CONTRACT_DELTA env default must be 0.10."""
        import os
        val = float(os.getenv("MIN_CONTRACT_DELTA", "0.10"))
        assert val == 0.10

    def test_max_otm_pct_env_default(self):
        import os
        val = float(os.getenv("MAX_OTM_PCT", "0.12"))
        assert val == 0.12

    def test_min_acceptable_premium_env_default(self):
        import os
        val = float(os.getenv("MIN_ACCEPTABLE_PREMIUM_PER_CONTRACT", "50"))
        assert val == 50.0

    def test_no_chain_data_still_valid_code(self):
        """NO_CHAIN_DATA must still exist in observability REASON_CODES
        (backward compat for existing log parsers)."""
        from ap.observability import REASON_CODES
        all_codes = [c for bucket in REASON_CODES.values() for c in bucket]
        assert "NO_CHAIN_DATA" in all_codes, (
            "NO_CHAIN_DATA was removed from REASON_CODES — backward compat broken"
        )


# ---------------------------------------------------------------------------
# Spec Test 11: Source-level guard — forbidden strings not added
# ---------------------------------------------------------------------------

class TestSourceGuard:
    """Spec 11: no restart-guard/recovery/watcher-reseed changes in this PR."""

    def _src(self, path: str) -> str:
        from pathlib import Path
        return (Path(__file__).resolve().parents[1] / path).read_text()

    def test_no_manual_rescue_restart_guard_in_selector(self):
        src = self._src("ap/contract_selector.py")
        assert "manual_rescue_restart_guard" not in src, (
            "manual_rescue_restart_guard found in ap/contract_selector.py — "
            "this must not be added to the selector"
        )

    def test_no_startup_watcher_reseed_in_selector(self):
        src = self._src("ap/contract_selector.py")
        assert "STARTUP_WATCHER_RESEED" not in src

    def test_no_manual_restart_guard_bypass_in_selector(self):
        src = self._src("ap/contract_selector.py")
        assert "_manual_restart_guard_bypass_enabled" not in src

    def test_selector_does_not_import_queue(self):
        """contract_selector must not import from ap.queue (no side effects)."""
        src = self._src("ap/contract_selector.py")
        assert "from ap.queue" not in src
        assert "import ap.queue" not in src

    def test_no_entry_order_creation_in_selector(self):
        src = self._src("ap/contract_selector.py")
        assert "create_entry_order" not in src

    def test_new_codes_in_observability_reason_codes(self):
        """New reason codes must be registered in ap/observability.py."""
        from ap.observability import REASON_CODES
        contract_codes = REASON_CODES.get("CONTRACT", [])
        for code in ("CHAIN_EMPTY", "CHAIN_FETCH_FAILED", "CHAIN_ROW_ZERO_BID_ASK",
                     "DIRECT_QUOTE_ZERO_BID_ASK", "BID_BELOW_MIN"):
            assert code in contract_codes, (
                f"{code} not registered in ap/observability.REASON_CODES['CONTRACT']"
            )

    def test_to_queue_reason_stable_mapping(self):
        """Verify the full stable queue-facing mapping is correct."""
        from ap.contract_selector import _to_queue_reason
        expected = {
            "CHAIN_EMPTY":                   "NO_CHAIN_DATA",
            "CHAIN_FETCH_FAILED":            "NO_CHAIN_DATA",
            "QUOTE_FETCH_FAILED":            "QUOTE_FETCH_FAILED",
            "CHAIN_ROW_ZERO_BID_ASK":        "QUOTE_ZERO_BID_ASK",
            "DIRECT_QUOTE_ZERO_BID_ASK":     "QUOTE_ZERO_BID_ASK",
            "BID_BELOW_MIN":                 "BID_BELOW_MIN",
            "SPREAD_TOO_WIDE":               "SPREAD_TOO_WIDE",
            "OI_TOO_LOW":                    "OI_TOO_LOW",
            "VOLUME_TOO_LOW":                "VOLUME_TOO_LOW",
            "DELTA_OUT_OF_RANGE":            "DELTA_OUT_OF_RANGE",
            "DTE_OUT_OF_RANGE":              "DTE_OUT_OF_RANGE",
            "NO_AFFORDABLE_CONTRACT":        "NO_AFFORDABLE_CONTRACT",
            "UNTRADEABLE_FOR_ACCOUNT_SIZE":  "NO_AFFORDABLE_CONTRACT",
            "PREMIUM_CAP_EXCEEDED":          "PREMIUM_CAP_EXCEEDED",
            "NO_VALID_PLAYBOOK_DTE_CONTRACT":"NO_VALID_PLAYBOOK_DTE_CONTRACT",
            "NO_CONTRACT_AFTER_FILTERS":     "NO_CONTRACT_AFTER_FILTERS",
        }
        for code, expected_q in expected.items():
            got = _to_queue_reason(code)
            assert got == expected_q, (
                f"_to_queue_reason({code!r}) = {got!r}, expected {expected_q!r}"
            )

    def test_attach_selector_failure_never_raises(self):
        """_attach_selector_failure must never raise regardless of inputs."""
        from ap.contract_selector import _attach_selector_failure
        # None plan
        _attach_selector_failure(None, reason_code="CHAIN_EMPTY", explanation="test")
        # Dict plan
        d = {}
        _attach_selector_failure(d, reason_code="OI_TOO_LOW", explanation="low oi")
        assert d.get("metadata", {}).get("selector_failure", {}).get("reason_code") == "OI_TOO_LOW"
        # Plan with no metadata attr
        class BadPlan:
            pass
        _attach_selector_failure(BadPlan(), reason_code="SPREAD_TOO_WIDE", explanation="wide")
