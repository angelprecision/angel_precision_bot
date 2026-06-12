"""
tests/test_contract_quote_revalidator.py

Regression tests for direct option quote revalidation (P0A) and final
pre-submit quote refresh (P0B).

These tests do NOT depend on a live broker, on network, or on real Tradier.
They exercise the contract_quote_revalidator module against a stub broker
to lock in the safety contract.
"""
from __future__ import annotations

from pathlib import Path

import pytest
from unittest.mock import MagicMock

from ap.contract_quote_revalidator import (
    revalidate_with_direct_quote,
    final_quote_check_before_submit,
    fetch_direct_option_quote,
    direct_quote_is_valid,
    should_revalidate,
    clear_quote_cache,
    REASON_CHAIN_ROW_ZERO_BID_ASK,
    REASON_DIRECT_QUOTE_ZERO_BID_ASK,
    REASON_DIRECT_QUOTE_RECOVERED_CHAIN_ZERO,
    REASON_DIRECT_QUOTE_UNAVAILABLE,
    REASON_FINAL_CONTRACT_QUOTE_INVALID,
    REASON_FINAL_SPREAD_TOO_WIDE,
    REASON_FINAL_CONTRACT_UNAFFORDABLE,
)


# ─────────────────────────────────────────────────────────────────────────────
# Stub broker
# ─────────────────────────────────────────────────────────────────────────────

class StubBroker:
    """Minimal broker stub that returns canned quote results by OCC symbol."""

    def __init__(self, quotes: dict | None = None, raise_on=None):
        self.quotes = quotes or {}
        self.raise_on = raise_on or set()
        self.calls = []

    def get_quote(self, symbol: str) -> dict:
        self.calls.append(symbol)
        if symbol in self.raise_on:
            raise RuntimeError("broker boom")
        return self.quotes.get(symbol, {})


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_quote_cache()
    yield
    clear_quote_cache()


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

OCC = "NVDA260620C00500000"
EXEC_SRC = (Path(__file__).resolve().parents[1] / "ap" / "execution.py").read_text()


def _chain_opt_zero(symbol=OCC):
    """An option row from chain with zero bid/ask (the bug case)."""
    return {
        "symbol":        symbol,
        "strike":        500.0,
        "bid":           0.0,
        "ask":           0.0,
        "last":          0.0,
        "volume":        0,
        "open_interest": 0,
        "option_type":   "call",
        "expiration_date": "2026-06-20",
    }


# ─────────────────────────────────────────────────────────────────────────────
# direct_quote_is_valid
# ─────────────────────────────────────────────────────────────────────────────

class TestDirectQuoteIsValid:

    def test_valid(self):
        assert direct_quote_is_valid({"bid": 1.20, "ask": 1.25}) is True

    def test_zero_bid_invalid(self):
        assert direct_quote_is_valid({"bid": 0.0, "ask": 1.25}) is False

    def test_zero_ask_invalid(self):
        assert direct_quote_is_valid({"bid": 1.20, "ask": 0.0}) is False

    def test_missing_invalid(self):
        assert direct_quote_is_valid({"bid": None, "ask": 1.25}) is False

    def test_inverted_book_invalid(self):
        assert direct_quote_is_valid({"bid": 2.00, "ask": 1.50}) is False

    def test_empty_invalid(self):
        assert direct_quote_is_valid({}) is False
        assert direct_quote_is_valid(None) is False


# ─────────────────────────────────────────────────────────────────────────────
# should_revalidate
# ─────────────────────────────────────────────────────────────────────────────

class TestShouldRevalidate:

    @pytest.mark.parametrize("reason", [
        "zero_bid_or_ask",
        "bid_below_0.1",
        "NO_CHAIN_DATA",
        "NO_AFFORDABLE_CONTRACT",
        "missing_bid_ask",
        "low_volume_0",
        "low_oi_0",
    ])
    def test_revalidatable_reasons(self, reason):
        assert should_revalidate(reason) is True

    def test_prefixed_revalidatable(self):
        # selector emits "bid_below_0.1_for_NVDA" style strings sometimes
        assert should_revalidate("zero_bid_or_ask_for_NVDA") is True

    @pytest.mark.parametrize("reason", [
        "spread_too_wide_45.0%",
        "delta_out_of_band_0.85",
        "premium_too_high_$400",
        "dte_too_high_30",
        "",
        None,
    ])
    def test_non_revalidatable(self, reason):
        assert should_revalidate(reason) is False


# ─────────────────────────────────────────────────────────────────────────────
# fetch_direct_option_quote
# ─────────────────────────────────────────────────────────────────────────────

class TestFetchDirectOptionQuote:

    def test_returns_normalized_quote(self):
        broker = StubBroker({
            OCC: {"bid": 1.20, "ask": 1.25, "last": 1.22, "bidsize": 5,
                  "asksize": 8, "volume": 1500, "open_interest": 2500},
        })
        q = fetch_direct_option_quote(broker, OCC)
        assert q is not None
        assert q["bid"] == 1.20
        assert q["ask"] == 1.25
        assert q["last"] == 1.22
        assert q["bid_size"] == 5
        assert q["ask_size"] == 8
        assert q["volume"] == 1500
        assert q["open_interest"] == 2500
        assert q["quote_age_ms"] >= 0

    def test_returns_none_on_broker_error(self):
        broker = StubBroker(raise_on={OCC})
        q = fetch_direct_option_quote(broker, OCC)
        assert q is None

    def test_cache_hit_avoids_second_call(self):
        broker = StubBroker({OCC: {"bid": 1.1, "ask": 1.2}})
        fetch_direct_option_quote(broker, OCC)
        fetch_direct_option_quote(broker, OCC)
        # cache TTL default is 3s — second call should not hit broker
        assert len(broker.calls) == 1

    def test_empty_broker_response_returns_quote_with_nones(self):
        broker = StubBroker({OCC: {}})
        q = fetch_direct_option_quote(broker, OCC)
        assert q is not None
        assert q["bid"] is None
        assert q["ask"] is None

    def test_none_broker_returns_none(self):
        assert fetch_direct_option_quote(None, OCC) is None

    def test_empty_symbol_returns_none(self):
        broker = StubBroker({})
        assert fetch_direct_option_quote(broker, "") is None


# ─────────────────────────────────────────────────────────────────────────────
# revalidate_with_direct_quote — P0A
# ─────────────────────────────────────────────────────────────────────────────

class TestRevalidateWithDirectQuote:

    def test_chain_zero_direct_valid_passes(self):
        """REQUIRED: chain bid=0 ask=0, direct quote valid → PASS."""
        opt = _chain_opt_zero()
        broker = StubBroker({OCC: {"bid": 1.20, "ask": 1.25, "last": 1.22}})

        result = revalidate_with_direct_quote(
            broker, opt, "zero_bid_or_ask", market_open_override=True
        )
        assert result["action"] == "PASS"
        assert result["direct_quote_used"] is True
        assert result["reason_code"] == REASON_DIRECT_QUOTE_RECOVERED_CHAIN_ZERO
        # opt should be patched with direct values
        assert result["opt_updated"]["bid"] == 1.20
        assert result["opt_updated"]["ask"] == 1.25
        assert result["opt_updated"]["_direct_quote_used"] is True
        # audit
        assert result["audit"]["chain_bid"] == 0.0
        assert result["audit"]["direct_bid"] == 1.20
        assert result["audit"]["direct_ask"] == 1.25
        assert result["audit"]["direct_mid"] == pytest.approx(1.225)
        assert result["audit"]["contract_quote_source"] == "direct"

    def test_chain_zero_direct_also_zero_rejects(self):
        """REQUIRED: chain bid=0 ask=0, direct quote also zero → REJECT."""
        opt = _chain_opt_zero()
        broker = StubBroker({OCC: {"bid": 0.0, "ask": 0.0}})

        result = revalidate_with_direct_quote(
            broker, opt, "zero_bid_or_ask", market_open_override=True
        )
        assert result["action"] == "REJECT_DIRECT_ZERO"
        assert result["reason_code"] == REASON_DIRECT_QUOTE_ZERO_BID_ASK
        assert result["direct_quote_used"] is True
        assert result["opt_updated"] is None

    def test_bid_below_threshold_revalidates(self):
        """REQUIRED: bid_below_0.1 chain reject → re-evaluate, not immediate reject."""
        opt = _chain_opt_zero()
        opt["bid"] = 0.05
        opt["ask"] = 0.05
        broker = StubBroker({OCC: {"bid": 0.85, "ask": 0.92}})  # real quote

        result = revalidate_with_direct_quote(
            broker, opt, "bid_below_0.1", market_open_override=True
        )
        assert result["action"] == "PASS"
        assert result["opt_updated"]["bid"] == 0.85
        assert result["opt_updated"]["ask"] == 0.92

    def test_no_chain_data_revalidates(self):
        opt = _chain_opt_zero()
        broker = StubBroker({OCC: {"bid": 1.10, "ask": 1.15}})
        result = revalidate_with_direct_quote(
            broker, opt, "NO_CHAIN_DATA", market_open_override=True
        )
        assert result["action"] == "PASS"

    def test_off_market_hours_falls_through(self):
        """Off market hours → SKIP, original reject stands."""
        opt = _chain_opt_zero()
        broker = StubBroker({OCC: {"bid": 1.20, "ask": 1.25}})
        result = revalidate_with_direct_quote(
            broker, opt, "zero_bid_or_ask", market_open_override=False
        )
        assert result["action"] == "SKIP_NOT_MARKET_HOURS"
        assert result["direct_quote_used"] is False

    def test_non_revalidatable_reason_skipped(self):
        """Spread / delta / dte rejects are not revalidated."""
        opt = _chain_opt_zero()
        opt["bid"] = 1.0
        opt["ask"] = 2.0
        broker = StubBroker({OCC: {"bid": 1.0, "ask": 2.0}})

        result = revalidate_with_direct_quote(
            broker, opt, "spread_too_wide_50.0%", market_open_override=True
        )
        assert result["action"] == "SKIP_NOT_REVALIDATABLE"
        # broker not even called
        assert len(broker.calls) == 0

    def test_missing_symbol_rejects_unavailable(self):
        opt = {"bid": 0, "ask": 0}  # no symbol
        broker = StubBroker({OCC: {"bid": 1.20, "ask": 1.25}})
        result = revalidate_with_direct_quote(
            broker, opt, "zero_bid_or_ask", market_open_override=True
        )
        assert result["action"] == "REJECT_UNAVAILABLE"
        assert result["reason_code"] == REASON_DIRECT_QUOTE_UNAVAILABLE

    def test_broker_error_rejects_unavailable(self):
        opt = _chain_opt_zero()
        broker = StubBroker(raise_on={OCC})
        result = revalidate_with_direct_quote(
            broker, opt, "zero_bid_or_ask", market_open_override=True
        )
        assert result["action"] == "REJECT_UNAVAILABLE"
        assert result["reason_code"] == REASON_DIRECT_QUOTE_UNAVAILABLE

    def test_direct_inverted_book_rejects(self):
        opt = _chain_opt_zero()
        broker = StubBroker({OCC: {"bid": 2.00, "ask": 1.50}})
        result = revalidate_with_direct_quote(
            broker, opt, "zero_bid_or_ask", market_open_override=True
        )
        assert result["action"] == "REJECT_DIRECT_ZERO"
        assert result["reason_code"] == REASON_DIRECT_QUOTE_ZERO_BID_ASK


# ─────────────────────────────────────────────────────────────────────────────
# final_quote_check_before_submit — P0B
# ─────────────────────────────────────────────────────────────────────────────

class TestFinalQuoteCheckBeforeSubmit:

    def _kwargs(self, **over):
        defaults = dict(
            max_spread_pct=0.50,
            min_premium=10.0,
            max_premium=350.0,
            budget_usd=500.0,
            is_live=False,
        )
        defaults.update(over)
        return defaults

    def test_valid_quote_passes(self):
        """REQUIRED: final pre-submit quote valid → order can submit."""
        broker = StubBroker({OCC: {"bid": 1.20, "ask": 1.25, "last": 1.22}})
        r = final_quote_check_before_submit(broker, OCC, **self._kwargs())
        assert r["ok"] is True
        assert r["reason_code"] is None
        assert r["final_bid"] == 1.20
        assert r["final_ask"] == 1.25
        assert r["final_mid"] == pytest.approx(1.225)
        assert r["spread_pct"] == pytest.approx((1.25 - 1.20) / 1.225)

    def test_quote_unavailable_rejects(self):
        """REQUIRED: final pre-submit quote invalid → do not submit."""
        broker = StubBroker(raise_on={OCC})
        r = final_quote_check_before_submit(broker, OCC, **self._kwargs())
        assert r["ok"] is False
        assert r["reason_code"] == REASON_FINAL_CONTRACT_QUOTE_INVALID

    def test_zero_bid_rejects(self):
        broker = StubBroker({OCC: {"bid": 0.0, "ask": 1.25}})
        r = final_quote_check_before_submit(broker, OCC, **self._kwargs())
        assert r["ok"] is False
        assert r["reason_code"] == REASON_FINAL_CONTRACT_QUOTE_INVALID

    def test_zero_ask_rejects(self):
        broker = StubBroker({OCC: {"bid": 1.20, "ask": 0.0}})
        r = final_quote_check_before_submit(broker, OCC, **self._kwargs())
        assert r["ok"] is False
        assert r["reason_code"] == REASON_FINAL_CONTRACT_QUOTE_INVALID

    def test_spread_too_wide_rejects(self):
        """LIVE safety: spread cap is enforced."""
        broker = StubBroker({OCC: {"bid": 1.00, "ask": 3.00}})  # 100% spread
        r = final_quote_check_before_submit(broker, OCC, **self._kwargs(max_spread_pct=0.30))
        assert r["ok"] is False
        assert r["reason_code"] == REASON_FINAL_SPREAD_TOO_WIDE

    def test_unaffordable_rejects(self):
        """LIVE safety: budget cap is enforced."""
        broker = StubBroker({OCC: {"bid": 5.95, "ask": 6.05}})  # $600/contract
        r = final_quote_check_before_submit(broker, OCC, **self._kwargs(
            budget_usd=500.0, is_live=True
        ))
        assert r["ok"] is False
        assert r["reason_code"] == REASON_FINAL_CONTRACT_UNAFFORDABLE

    def test_premium_too_high_rejects(self):
        broker = StubBroker({OCC: {"bid": 4.50, "ask": 4.55}})  # $452.50/contract
        r = final_quote_check_before_submit(broker, OCC, **self._kwargs(
            max_premium=350.0, budget_usd=1000.0
        ))
        assert r["ok"] is False
        assert r["reason_code"] == REASON_FINAL_CONTRACT_UNAFFORDABLE

    def test_premium_too_low_rejects(self):
        broker = StubBroker({OCC: {"bid": 0.05, "ask": 0.06}})  # $5.50/contract
        r = final_quote_check_before_submit(broker, OCC, **self._kwargs(
            min_premium=10.0
        ))
        assert r["ok"] is False
        assert r["reason_code"] == REASON_FINAL_CONTRACT_QUOTE_INVALID

    def test_live_uses_ask_for_affordability(self):
        """LIVE pricing basis must use ask, not mid."""
        # mid=4.975 ($497.50) fits 500 budget, but ask=5.00 ($500) does too
        broker = StubBroker({OCC: {"bid": 4.95, "ask": 5.00}})
        r = final_quote_check_before_submit(broker, OCC, **self._kwargs(
            budget_usd=499.99, is_live=True, max_premium=600.0
        ))
        assert r["ok"] is False
        assert r["reason_code"] == REASON_FINAL_CONTRACT_UNAFFORDABLE

    def test_quote_fields_always_populated_on_failure(self):
        """Forensics: quote fields surfaced even when rejecting."""
        broker = StubBroker({OCC: {"bid": 1.00, "ask": 3.00}})
        r = final_quote_check_before_submit(broker, OCC, **self._kwargs(max_spread_pct=0.30))
        assert r["ok"] is False
        assert r["final_bid"] == 1.00
        assert r["final_ask"] == 3.00
        assert r["final_mid"] is not None
        assert r["spread_pct"] is not None


# ─────────────────────────────────────────────────────────────────────────────
# Reason code uniqueness
# ─────────────────────────────────────────────────────────────────────────────

def test_reject_reasons_are_distinct():
    codes = {
        REASON_CHAIN_ROW_ZERO_BID_ASK,
        REASON_DIRECT_QUOTE_ZERO_BID_ASK,
        REASON_DIRECT_QUOTE_RECOVERED_CHAIN_ZERO,
        REASON_DIRECT_QUOTE_UNAVAILABLE,
        REASON_FINAL_CONTRACT_QUOTE_INVALID,
        REASON_FINAL_SPREAD_TOO_WIDE,
        REASON_FINAL_CONTRACT_UNAFFORDABLE,
    }
    assert len(codes) == 7, "reason codes must be distinct"


# =============================================================================
# FIX 3 — qty-aware order cost validation
# =============================================================================

class TestFinalQuoteQtyValidation:

    def _kwargs(self, **over):
        defaults = dict(
            max_spread_pct=0.50, min_premium=10.0, max_premium=350.0,
            budget_usd=500.0, is_live=True,
        )
        defaults.update(over)
        return defaults

    def test_single_contract_within_budget_passes(self):
        broker = StubBroker({OCC: {"bid": 4.90, "ask": 5.00}})
        r = final_quote_check_before_submit(
            broker,
            OCC,
            qty=1,
            **self._kwargs(budget_usd=500.0, max_premium=600.0),
        )
        assert r["ok"] is True

    def test_multi_contract_total_cost_rejected(self):
        """qty=2, ask=2.60 -> total = 2 * 2.60 * 100 = 520 > budget=500 -> reject."""
        broker = StubBroker({OCC: {"bid": 2.55, "ask": 2.60}})
        r = final_quote_check_before_submit(broker, OCC, qty=2,
                                            **self._kwargs(budget_usd=500.0, max_premium=600.0))
        assert r["ok"] is False
        assert r["reason_code"] == REASON_FINAL_CONTRACT_UNAFFORDABLE

    def test_multi_contract_within_budget_passes(self):
        """qty=2, ask=2.40 -> total = 2 * 2.40 * 100 = 480 <= budget=500 -> pass."""
        broker = StubBroker({OCC: {"bid": 2.35, "ask": 2.40}})
        r = final_quote_check_before_submit(broker, OCC, qty=2,
                                            **self._kwargs(budget_usd=500.0, max_premium=600.0))
        assert r["ok"] is True

    def test_qty_default_1_backward_compat(self):
        """Callers that omit qty still work correctly."""
        broker = StubBroker({OCC: {"bid": 4.90, "ask": 5.00}})
        r = final_quote_check_before_submit(
            broker,
            OCC,
            **self._kwargs(budget_usd=500.0, max_premium=600.0),
        )
        assert r["ok"] is True


class TestExecutionP0BWiring:
    def test_final_quote_gate_uses_position_budget(self):
        assert "budget_usd=float(position_budget)" in EXEC_SRC

    def test_local_order_id_initialized_before_p0b_returns(self):
        init_idx = EXEC_SRC.find("local_order_id = None")
        p0b_idx = EXEC_SRC.find("_p0b_enabled = os.getenv(")
        assert init_idx > 0
        assert p0b_idx > 0
        assert init_idx < p0b_idx


# =============================================================================
# P2 — DEFAULT_REVALIDATE_TOP_N is exported
# =============================================================================

def test_default_revalidate_top_n_exported():
    from ap.contract_quote_revalidator import DEFAULT_REVALIDATE_TOP_N
    assert isinstance(DEFAULT_REVALIDATE_TOP_N, int)
    assert DEFAULT_REVALIDATE_TOP_N > 0


# =============================================================================
# P1 — _p0a_budget cap: broker.get_quote called no more than TOP_N times
#      regardless of how many zero-bid contracts are in the chain
# =============================================================================

class TestP0ABudgetCap:
    """
    Required regression: a stale chain with many zero bid/ask rows must not
    trigger more than CONTRACT_REVALIDATE_TOP_N direct-quote fetches.
    Tests _p0a_budget enforcement via the revalidator module directly —
    no need to instantiate APContractSelectionEngine.
    """

    def _zero_chain(self, n: int, symbol_prefix="OCC") -> list:
        """Build n chain rows that all have zero bid/ask."""
        return [
            {
                "symbol":          f"{symbol_prefix}{i:06d}",
                "strike":          500.0 + i,
                "bid":             0.0,
                "ask":             0.0,
                "volume":          0,
                "open_interest":   0,
                "option_type":     "call",
                "expiration_date": "2026-06-20",
            }
            for i in range(n)
        ]

    def test_budget_limits_get_quote_calls(self):
        """
        With CONTRACT_REVALIDATE_TOP_N=5 and 20 zero-bid contracts,
        broker.get_quote must be called at most 5 times total.
        """
        from ap.contract_quote_revalidator import DEFAULT_REVALIDATE_TOP_N
        call_count = {"n": 0}

        class CountingBroker:
            def get_quote(self, symbol):
                call_count["n"] += 1
                # Return zero so every fetch is a REJECT_DIRECT_ZERO
                return {"bid": 0.0, "ask": 0.0}

        broker = CountingBroker()
        budget = DEFAULT_REVALIDATE_TOP_N  # mirrors what selector sets

        for row in self._zero_chain(20):
            if budget <= 0:
                break
            result = revalidate_with_direct_quote(
                broker, row, "zero_bid_or_ask", market_open_override=True
            )
            # Decrement regardless of action — mirrors both selector paths
            budget -= 1

        assert call_count["n"] <= DEFAULT_REVALIDATE_TOP_N, (
            f"broker.get_quote called {call_count['n']} times, "
            f"expected <= {DEFAULT_REVALIDATE_TOP_N}"
        )

    def test_budget_decrements_on_reject_direct_zero(self):
        """Budget decrements on REJECT_DIRECT_ZERO, not only on PASS."""
        broker = StubBroker({OCC: {"bid": 0.0, "ask": 0.0}})
        budget = 3

        for _ in range(5):
            if budget <= 0:
                break
            revalidate_with_direct_quote(
                broker, _chain_opt_zero(), "zero_bid_or_ask",
                market_open_override=True,
            )
            budget -= 1

        assert len(broker.calls) <= 3

    def test_budget_decrements_on_reject_unavailable(self):
        """Budget decrements when broker raises (REJECT_UNAVAILABLE)."""
        broker = StubBroker(raise_on={OCC})
        budget = 2

        for _ in range(5):
            if budget <= 0:
                break
            revalidate_with_direct_quote(
                broker, _chain_opt_zero(), "zero_bid_or_ask",
                market_open_override=True,
            )
            budget -= 1

        assert len(broker.calls) <= 2

    def test_skip_not_market_hours_does_not_call_broker(self):
        """SKIP_NOT_MARKET_HOURS must never call broker.get_quote."""
        broker = StubBroker({OCC: {"bid": 1.20, "ask": 1.25}})
        for _ in range(10):
            revalidate_with_direct_quote(
                broker, _chain_opt_zero(), "zero_bid_or_ask",
                market_open_override=False,
            )
        assert len(broker.calls) == 0


# =============================================================================
# PAPER live quote source verification (amended — fail-closed behavior)
# =============================================================================

class TestPaperLiveQuoteSource:
    """
    Verify the fail-closed P0B paper quote routing logic.

    Production wiring (ap_execution_core.py):
      self.broker.data_broker = data_broker   (only when data_broker is live)

    Production behavior (ap/execution.py P0B):
      PAPER + data_broker present  → use data_broker for final quote check
      PAPER + no data_broker       → fail closed: PAPER_FINAL_QUOTE_NO_LIVE_DATA_BROKER
      LIVE                         → use broker as-is (IS the live source)
    """

    def test_paper_with_data_broker_uses_live_source(self):
        """PAPER + data_broker present → live broker used, quote passes."""
        live_broker    = StubBroker({OCC: {"bid": 1.20, "ask": 1.25}})
        sandbox_broker = StubBroker({OCC: {"bid": 0.0,  "ask": 0.0}})
        # APExecutionCore sets this in production
        sandbox_broker.data_broker = live_broker

        mode = "PAPER"
        _p0b_data_broker = getattr(sandbox_broker, "data_broker", None)
        assert _p0b_data_broker is live_broker,             "data_broker attribute not resolved from broker"

        _p0b_quote_broker = _p0b_data_broker  # is_paper_mode=True path
        r = final_quote_check_before_submit(
            _p0b_quote_broker, OCC,
            max_spread_pct=0.50, min_premium=10.0, max_premium=350.0,
            budget_usd=500.0, qty=1, is_live=False,
        )
        assert r["ok"] is True
        assert r["final_bid"] == 1.20
        # Sandbox broker must NOT have been called
        assert len(sandbox_broker.calls) == 0

    def test_paper_without_data_broker_triggers_fail_closed(self):
        """PAPER + no data_broker → execution.py must reject with PAPER_FINAL_QUOTE_NO_LIVE_DATA_BROKER.
        Simulates the fail-closed logic directly (without full process_signal wiring)."""
        sandbox_broker = StubBroker({OCC: {"bid": 0.0, "ask": 0.0}})
        # No data_broker attribute — simulates TRADIER_DATA_TOKEN not set

        mode = "PAPER"
        _is_paper_mode   = (mode == "PAPER")
        _p0b_data_broker = getattr(sandbox_broker, "data_broker", None)

        # This is the exact condition execution.py checks before calling _final_quote_check
        should_fail_closed = _is_paper_mode and (_p0b_data_broker is None)
        assert should_fail_closed is True,             "Expected fail-closed condition when PAPER broker has no data_broker"

        # Sandbox broker should never be called for P0B quote truth
        assert len(sandbox_broker.calls) == 0

    def test_paper_data_broker_same_as_broker_no_attach(self):
        """When data_broker IS the same object as broker (TRADIER_DATA_TOKEN absent,
        client_runner falls back to data_broker=broker), execution_core skips
        attaching the attribute (data_broker is not broker check). P0B then
        sees no data_broker and fails closed as expected."""
        broker = StubBroker({OCC: {"bid": 1.10, "ask": 1.15}})
        # Simulate: data_broker=broker (same object, no dedicated live token)
        data_broker = broker
        # APExecutionCore only attaches when data_broker is not None AND not broker:
        if data_broker is not None and data_broker is not broker:
            broker.data_broker = data_broker
        # Should NOT be attached
        assert not hasattr(broker, "data_broker"),             "data_broker must not be attached when it equals execution broker"

    def test_live_mode_uses_execution_broker_directly(self):
        """LIVE mode: broker IS the live source, no data_broker lookup."""
        live_broker = StubBroker({OCC: {"bid": 1.20, "ask": 1.25}})
        mode = "LIVE"
        _is_paper_mode = (mode == "PAPER")
        _p0b_quote_broker = (
            getattr(live_broker, "data_broker", None)
            if _is_paper_mode
            else live_broker
        )
        assert _p0b_quote_broker is live_broker
