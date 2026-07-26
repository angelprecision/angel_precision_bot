"""
Trigger-anchored strike selection (PR #396, post-review amendment).

Scope after the formal audit:

* Single shared strike-preference authority in ``ap/contract_playbook.py``
  (``resolve_trigger_anchored_preferred_strikes``) — both the direct-quote
  recovery ordering in the selector and the final playbook ranker call this
  one resolver, so quote-spending order and final ranking cannot diverge.
* ``APContractSelectionEngine.select()`` feeds trigger-anchored strike
  preference into the post-#389 direct-quote recovery ordering helper, GATED
  on ``PLAYBOOK_STRIKE_SELECTION_ENABLED`` /
  ``PLAYBOOK_CONTRACT_SELECTION_ENABLED``. When the flag is off, the
  post-#389 structural/liquidity ordering remains the baseline.
* Missing trigger falls back to underlying; missing/invalid both anchors returns
  ``TRIGGER_ANCHOR_SOURCE_NONE`` and callers preserve their prior ordering.
* No hard quality gate, budget rule, LIVE ask pricing, PAPER pricing, retry
  behavior, or broker submit/cancel path is touched.

Removed from the pre-review PR (per the review verdict and PR-owner comment):
    * ``ap/contract_intent.py`` — ceremonial architecture without a real
      end-to-end lifecycle. Reintroduce only alongside a real producer,
      persistence seam, consumer, exact ``client_id``/``execution_mode``
      validation, signal & setup-generation fencing, OCC identity validation,
      and stale-generation rejection.

Required-tests coverage (from the audit):
    A. BAC PUT drift replay ................ TestBacPutDriftReplay
    B. BAC CALL drift replay ............... TestBacCallDriftReplay
    C. Equidistant ties .................... TestEquidistantTies
    D. Missing-trigger fallback ............ TestMissingTriggerFallback
    E. Trigger and underlying invalid ...... TestBothAnchorsInvalid
    F. Index replays (SPY/QQQ) ............. TestIndexReplays
    G. Gate preservation ................... TestGatePreservation
    H. Pricing and side effects ............ TestPricingAndSideEffects
"""

from __future__ import annotations

import os
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import Mock, patch

import pytest

from ap.contract_playbook import (
    ContractPlaybookSpec,
    TRIGGER_ANCHOR_SOURCE_NONE,
    TRIGGER_ANCHOR_SOURCE_TRIGGER,
    TRIGGER_ANCHOR_SOURCE_UNDERLYING_FALLBACK,
    build_playbook_candidate_context,
    resolve_trigger_anchored_preferred_strikes,
)


# ---------------------------------------------------------------------------
# Shared fixtures / helpers
# ---------------------------------------------------------------------------

def _next_weekday(day: date) -> date:
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return day


_NEAR_EXPIRY = _next_weekday(date.today() + timedelta(days=2))
_NEAR_EXPIRY_ISO = _NEAR_EXPIRY.isoformat()
_NEAR_DTE = (_NEAR_EXPIRY - date.today()).days


def _occ_symbol(ticker: str, expiration: date, side: str, strike: float) -> str:
    root = "".join(str(ticker or "").upper().split())
    yymmdd = expiration.strftime("%y%m%d")
    cp = "C" if str(side).upper().startswith("C") else "P"
    strike_int = int(round(float(strike) * 1000))
    return f"{root}{yymmdd}{cp}{strike_int:08d}"

def _spec(
    side: str,
    *,
    trigger: float | None,
    underlying: float,
    target: float | None = None,
    instrument_class: str = "ORDINARY_EQUITY",
) -> ContractPlaybookSpec:
    return ContractPlaybookSpec(
        instrument_class=instrument_class,
        timeframe="DAILY",
        side=side,
        preferred_expirations=[_NEAR_EXPIRY_ISO],
        permitted_expirations=[_NEAR_EXPIRY_ISO],
        preferred_dte_order=[_NEAR_DTE],
        strike_policy="LEXICOGRAPHIC_ATM_OR_ONE_STEP_OTM_TOWARD_TARGET",
        preferred_strikes=[],
        strike_band_low=None,
        strike_band_high=None,
        target_underlying=target,
        trigger_price=trigger,
        underlying_price=float(underlying),
        fallback_policy="NEXT_APPROVED_EXPIRATION",
        policy_reason="TEST",
        diagnostics={},
    )


def _candidate_dicts(strikes, *, ticker: str = "BAC", side: str = "PUT"):
    return [
        {
            "symbol": _occ_symbol(ticker, _NEAR_EXPIRY, side, float(s)),
            "strike": float(s),
        }
        for s in strikes
    ]


def _tradier_row(
    strike: float,
    *,
    side: str,
    ticker: str = "BAC",
    expiration: str = _NEAR_EXPIRY_ISO,
    bid: float = 1.20,
    ask: float = 1.30,
    delta: float = 0.42,
    open_interest: int = 5_000,
    volume: int = 800,
    bid_size: int = 20,
    ask_size: int = 20,
    symbol: str | None = None,
) -> dict:
    """Produce a chain row that passes every existing hard quality gate by
    default — tests then perturb specific fields to prove gates still fire on
    trigger-preferred candidates."""
    side_low = side.lower()
    return {
        "symbol": symbol or _occ_symbol(
            ticker, date.fromisoformat(expiration), side, strike
        ),
        "strike": float(strike),
        "bid": float(bid),
        "ask": float(ask),
        "bid_size": int(bid_size),
        "ask_size": int(ask_size),
        "open_interest": int(open_interest),
        "volume": int(volume),
        "greeks": {"delta": float(delta if side_low == "call" else -delta)},
        "expiration_date": expiration,
        "option_type": side_low,
    }


@pytest.fixture(autouse=True)
def _isolate_env(monkeypatch):
    """Enable the playbook flag by default so the reorder path is exercised,
    but let individual tests turn it off to prove flag-off is unchanged.
    Also silence observability DB writes so DB retries do not slow the suite —
    the selector's emit_decision_event calls the DB, and none of these tests
    care about the decision-events table."""
    monkeypatch.setenv("PLAYBOOK_STRIKE_SELECTION_ENABLED", "1")
    monkeypatch.setenv("PLAYBOOK_CONTRACT_SELECTION_ENABLED", "1")
    monkeypatch.setenv("DATABASE_URL", "postgresql://x:x@127.0.0.1:1/x?connect_timeout=1")

    import ap.contract_selector as _cs
    monkeypatch.setattr(_cs, "emit_decision_event", lambda **kw: None)
    yield


def _make_engine(mode: str = "paper"):
    from ap.contract_selector import APContractSelectionEngine

    broker = SimpleNamespace(base_url="https://sandbox.tradier.com")
    engine = APContractSelectionEngine(broker=broker, mode=mode)
    engine.dte_ladder_enabled = False
    return engine


def _plan(
    *,
    ticker: str,
    side: str,
    trigger: float | None,
    target: float,
    execution_mode: str = "paper",
    max_position_usd: float = 5_000.0,
    account_equity: float = 25_000.0,
):
    return {
        "ticker": ticker,
        "side": side,
        "trigger_price": trigger,
        "target_underlying": target,
        "timeframe": "daily",
        "client_id": "client-test",
        "execution_mode": execution_mode,
        "signal_id": "sig-test",
        "metadata": {
            "sizing_context": {
                "max_position_usd": float(max_position_usd),
                "risk_pct": 0.05,
                "account_equity": float(account_equity),
            }
        },
    }


def _run_and_capture(
    *,
    ticker: str,
    side: str,
    trigger: float | None,
    underlying: float,
    chain: list[dict],
    execution_mode: str = "paper",
    engine=None,
    plan_overrides: dict | None = None,
) -> tuple:
    """Drive engine.select() with a mocked chain fetch and capture the strike
    iteration order observed at the quality-filter seam.

    Returns (observed_iteration_order, result, engine, plan)."""
    engine = engine or _make_engine(mode="live" if execution_mode == "live" else "paper")
    observed: list[float] = []

    def _capture_quality_filter(opt, today, **kwargs):
        observed.append(float(opt.get("strike")))
        return "zero_bid_or_ask"

    plan = _plan(
        ticker=ticker,
        side=side,
        trigger=trigger,
        target=(trigger or underlying) * (0.98 if side == "PUT" else 1.02),
        execution_mode=execution_mode,
    )
    if plan_overrides:
        plan.update(plan_overrides)

    with patch.object(
        engine, "_fetch_chain_with_price", return_value=(list(chain), underlying),
    ), patch.object(
        engine, "_quality_filter", side_effect=_capture_quality_filter,
    ), patch(
        "ap.contract_selector._pro_contract_quality", return_value=("B", "ok_liquid"),
    ):
        result = engine.select(plan)

    return observed, result, engine, plan


def _run_full_select(
    *,
    ticker: str,
    side: str,
    trigger: float | None,
    underlying: float,
    chain: list[dict],
    execution_mode: str = "paper",
    engine=None,
):
    """Same as _run_and_capture but does NOT patch the quality filter — the
    real gates run so tests can prove selection outcome end-to-end."""
    engine = engine or _make_engine(mode="live" if execution_mode == "live" else "paper")
    plan = _plan(
        ticker=ticker,
        side=side,
        trigger=trigger,
        target=(trigger or underlying) * (0.98 if side == "PUT" else 1.02),
        execution_mode=execution_mode,
    )
    with patch.object(
        engine, "_fetch_chain_with_price", return_value=(list(chain), underlying),
    ):
        result = engine.select(plan)
    return result, engine, plan


def _run_one_call_direct_quote_case(
    monkeypatch,
    *,
    side: str,
    trigger: float,
    underlying: float,
) -> tuple:
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "1")
    monkeypatch.delenv("DIRECT_QUOTE_RECOVERY_TOP_N", raising=False)
    monkeypatch.delenv("CONTRACT_REVALIDATE_TOP_N", raising=False)

    import ap.contract_quote_revalidator as quote_revalidator

    quote_revalidator.clear_quote_cache()
    chain = [
        _tradier_row(s, side=side, bid=0.0, ask=0.0)
        for s in (60.0, 61.0, 62.0)
    ]
    trigger_primary_symbol = _occ_symbol("BAC", _NEAR_EXPIRY, side, 61.0)
    expected_budget_skipped_symbol = _occ_symbol(
        "BAC", _NEAR_EXPIRY, side, 60.0 if side == "PUT" else 62.0
    )

    engine = _make_engine()
    data_broker = engine.data_broker

    def _direct_quote(symbol):
        assert symbol == trigger_primary_symbol
        return {
            "bid": 1.20,
            "ask": 1.30,
            "last": 1.25,
            "bidsize": 20,
            "asksize": 20,
            "volume": 800,
            "open_interest": 5_000,
        }

    data_broker.get_quote = Mock(side_effect=_direct_quote)
    plan = _plan(
        ticker="BAC",
        side=side,
        trigger=trigger,
        target=trigger * (0.98 if side == "PUT" else 1.02),
        execution_mode="paper",
    )

    with patch.object(
        engine, "_fetch_chain_with_price", return_value=(chain, underlying)
    ), patch(
        "ap.contract_selector._pro_contract_quality",
        return_value=("B", "ok_liquid"),
    ), patch(
        "ap.contract_quote_revalidator.is_market_open",
        return_value=True,
    ):
        result = engine.select(plan)

    diagnostics = plan["metadata"]["selector_request_diagnostics"]
    return (
        result,
        data_broker,
        trigger_primary_symbol,
        expected_budget_skipped_symbol,
        diagnostics,
    )


# ===========================================================================
# Required test A — BAC PUT drift replay
# ===========================================================================

class TestBacPutDriftReplay:
    """trigger=61.17, underlying drifted above 61.50, strikes 60/61/62,
    direct-quote budget = 1. The 61 PUT must receive the first direct-quote
    attempt AND win final selection when it passes every gate."""

    def test_shared_authority_selects_61_when_drifted(self):
        pref = resolve_trigger_anchored_preferred_strikes(
            side="PUT", trigger_price=61.17, underlying_fallback=61.60,
            candidate_strikes=[60, 61, 62],
        )
        assert pref.anchor_source == TRIGGER_ANCHOR_SOURCE_TRIGGER
        assert pref.primary_strike == 61.0
        assert pref.adjacent_otm_strike == 60.0

    def test_playbook_ranker_selects_61_when_drifted(self):
        spec = _spec("PUT", trigger=61.17, underlying=61.60)
        ctx = build_playbook_candidate_context(
            spec, _candidate_dicts([60, 61, 62], side="PUT")
        )
        assert ctx["atm_strike"] == 61.0
        assert ctx["one_step_otm_strike"] == 60.0
        assert ctx["preferred_strikes"] == [61.0, 60.0]
        assert ctx["anchor_source"] == TRIGGER_ANCHOR_SOURCE_TRIGGER

    def test_selector_iterates_61_first_then_60_then_62(self):
        """Direct-quote budget spending order must match the ranker — 61 first
        (nearest trigger), then 60 (adjacent OTM for PUT), then 62."""
        # Tradier returned ascending order (adversarial for this replay).
        chain = [_tradier_row(s, side="PUT") for s in (60.0, 61.0, 62.0)]
        observed, _, _, _ = _run_and_capture(
            ticker="BAC", side="PUT", trigger=61.17, underlying=61.60,
            chain=chain,
        )
        assert observed == [61.0, 60.0, 62.0], observed

    def test_61_put_wins_final_selection_when_all_gates_pass(self):
        chain = [_tradier_row(s, side="PUT") for s in (60.0, 61.0, 62.0)]
        result, _, plan = _run_full_select(
            ticker="BAC", side="PUT", trigger=61.17, underlying=61.60,
            chain=chain,
        )
        assert result is not None, "selector must return a contract"
        assert result.strike == 61.0
        assert plan["contract_symbol"] == result.contract_symbol

    def test_61_put_receives_only_direct_quote_attempt(self, monkeypatch):
        result, data_broker, quoted_symbol, skipped_symbol, diagnostics = (
            _run_one_call_direct_quote_case(
                monkeypatch,
                side="PUT",
                trigger=61.17,
                underlying=61.60,
            )
        )

        assert data_broker.get_quote.call_count == 1
        assert data_broker.get_quote.call_args.args[0] == quoted_symbol
        assert diagnostics["direct_quote_budget"]["used"] == 1
        assert diagnostics["direct_quote_structural_candidates"] == 3
        assert diagnostics["direct_quote_attempted_symbols"] == [quoted_symbol]
        assert diagnostics["direct_quote_unattempted_count"] >= 1
        assert skipped_symbol in diagnostics["direct_quote_unattempted_symbols"]
        assert quoted_symbol not in diagnostics["direct_quote_unattempted_symbols"]
        assert result is not None
        assert result.strike == 61.0

    def test_crossed_61_put_receives_only_direct_quote_attempt(self, monkeypatch):
        """The trigger-primary remains first after the underlying crosses below it."""
        result, data_broker, quoted_symbol, skipped_symbol, diagnostics = (
            _run_one_call_direct_quote_case(
                monkeypatch,
                side="PUT",
                trigger=61.17,
                underlying=60.60,
            )
        )

        assert data_broker.get_quote.call_count == 1
        assert data_broker.get_quote.call_args.args[0] == quoted_symbol
        assert diagnostics["direct_quote_budget"]["used"] == 1
        assert diagnostics["direct_quote_structural_candidates"] == 3
        assert diagnostics["direct_quote_attempted_symbols"] == [quoted_symbol]
        assert skipped_symbol in diagnostics["direct_quote_unattempted_symbols"]
        assert result is not None
        assert result.strike == 61.0


# ===========================================================================
# Required test B — BAC CALL drift replay (mirror-symmetric)
# ===========================================================================

class TestBacCallDriftReplay:
    """trigger=61.17, underlying drifted DOWN to 60.60. The trigger-preferred
    CALL (strike 61) must receive the first quote AND win final selection."""

    def test_shared_authority_selects_61_when_drifted_down(self):
        pref = resolve_trigger_anchored_preferred_strikes(
            side="CALL", trigger_price=61.17, underlying_fallback=60.60,
            candidate_strikes=[60, 61, 62],
        )
        assert pref.primary_strike == 61.0
        assert pref.adjacent_otm_strike == 62.0

    def test_selector_iterates_61_first_then_62_then_60(self):
        chain = [_tradier_row(s, side="CALL") for s in (60.0, 61.0, 62.0)]
        observed, _, _, _ = _run_and_capture(
            ticker="BAC", side="CALL", trigger=61.17, underlying=60.60,
            chain=chain,
        )
        assert observed == [61.0, 62.0, 60.0], observed

    def test_61_call_wins_final_selection(self):
        chain = [_tradier_row(s, side="CALL") for s in (60.0, 61.0, 62.0)]
        result, _, _ = _run_full_select(
            ticker="BAC", side="CALL", trigger=61.17, underlying=60.60,
            chain=chain,
        )
        assert result is not None
        assert result.strike == 61.0

    def test_61_call_receives_only_direct_quote_attempt(self, monkeypatch):
        result, data_broker, quoted_symbol, skipped_symbol, diagnostics = (
            _run_one_call_direct_quote_case(
                monkeypatch,
                side="CALL",
                trigger=61.17,
                underlying=60.60,
            )
        )

        assert data_broker.get_quote.call_count == 1
        assert data_broker.get_quote.call_args.args[0] == quoted_symbol
        assert diagnostics["direct_quote_budget"]["used"] == 1
        assert diagnostics["direct_quote_structural_candidates"] == 3
        assert diagnostics["direct_quote_attempted_symbols"] == [quoted_symbol]
        assert diagnostics["direct_quote_unattempted_count"] >= 1
        assert skipped_symbol in diagnostics["direct_quote_unattempted_symbols"]
        assert quoted_symbol not in diagnostics["direct_quote_unattempted_symbols"]
        assert result is not None
        assert result.strike == 61.0

    def test_crossed_61_call_receives_only_direct_quote_attempt(self, monkeypatch):
        """The trigger-primary remains first after the underlying crosses above it."""
        result, data_broker, quoted_symbol, skipped_symbol, diagnostics = (
            _run_one_call_direct_quote_case(
                monkeypatch,
                side="CALL",
                trigger=61.17,
                underlying=61.60,
            )
        )

        assert data_broker.get_quote.call_count == 1
        assert data_broker.get_quote.call_args.args[0] == quoted_symbol
        assert diagnostics["direct_quote_budget"]["used"] == 1
        assert diagnostics["direct_quote_structural_candidates"] == 3
        assert diagnostics["direct_quote_attempted_symbols"] == [quoted_symbol]
        assert skipped_symbol in diagnostics["direct_quote_unattempted_symbols"]
        assert result is not None
        assert result.strike == 61.0


# ===========================================================================
# Required test C — Equidistant ties
# ===========================================================================

class TestEquidistantTies:
    def test_put_prefers_lower_strike(self):
        pref = resolve_trigger_anchored_preferred_strikes(
            side="PUT", trigger_price=61.5, underlying_fallback=61.5,
            candidate_strikes=[61, 62],
        )
        assert pref.primary_strike == 61.0

    def test_call_prefers_higher_strike(self):
        pref = resolve_trigger_anchored_preferred_strikes(
            side="CALL", trigger_price=61.5, underlying_fallback=61.5,
            candidate_strikes=[61, 62],
        )
        assert pref.primary_strike == 62.0

    def test_put_selector_iteration_order_on_tie(self):
        chain = [_tradier_row(s, side="PUT") for s in (61.0, 62.0)]
        observed, _, _, _ = _run_and_capture(
            ticker="AAPL", side="PUT", trigger=61.5, underlying=61.5, chain=chain,
        )
        assert observed[0] == 61.0

    def test_call_selector_iteration_order_on_tie(self):
        chain = [_tradier_row(s, side="CALL") for s in (61.0, 62.0)]
        observed, _, _, _ = _run_and_capture(
            ticker="AAPL", side="CALL", trigger=61.5, underlying=61.5, chain=chain,
        )
        assert observed[0] == 62.0


# ===========================================================================
# Required test D — Missing-trigger fallback
# ===========================================================================

class TestMissingTriggerFallback:
    """When the scanner trigger is missing, the selector uses current
    underlying as the anchor and audit reports anchor_source =
    ``underlying_fallback``. No synthetic trigger is created."""

    def test_shared_authority_uses_underlying_fallback(self):
        pref = resolve_trigger_anchored_preferred_strikes(
            side="PUT", trigger_price=None, underlying_fallback=61.60,
            candidate_strikes=[60, 61, 62],
        )
        assert pref.anchor_source == TRIGGER_ANCHOR_SOURCE_UNDERLYING_FALLBACK
        assert pref.anchor_price == 61.60
        assert pref.primary_strike == 62.0  # |62-61.6|=0.4 nearest

    def test_selector_orders_by_underlying_when_trigger_missing(self):
        chain = [_tradier_row(s, side="PUT") for s in (60.0, 61.0, 62.0)]
        observed, _, engine, plan = _run_and_capture(
            ticker="BAC", side="PUT", trigger=None, underlying=61.60, chain=chain,
        )
        # CALL/PUT identity remains structural; current-underlying moneyness is
        # secondary and cannot defeat the preferred fallback anchor.
        assert observed == [62.0, 61.0, 60.0]

    def test_audit_reports_underlying_fallback_source(self):
        chain = [_tradier_row(s, side="PUT") for s in (60.0, 61.0, 62.0)]
        engine = _make_engine()
        plan = _plan(ticker="BAC", side="PUT", trigger=None, target=60.0)

        with patch.object(engine, "_fetch_chain_with_price",
                          return_value=(chain, 61.60)), \
             patch.object(engine, "_quality_filter", return_value="zero_bid_or_ask"), \
             patch("ap.contract_selector._pro_contract_quality",
                   return_value=("B", "ok_liquid")):
            engine.select(plan)

        diag = plan["metadata"]["selector_request_diagnostics"]
        audit = diag.get("preferred_strike_ordering")
        assert audit is not None, "selector must emit preferred_strike_ordering audit"
        assert audit["enabled"] is True
        assert audit["anchor_source"] == TRIGGER_ANCHOR_SOURCE_UNDERLYING_FALLBACK
        assert audit["anchor_price"] == 61.60
        assert audit["primary_strike"] == 62.0
        assert audit["reordered"] is True
        # Truthful naming: the audit records the PLANNED reorder, not attempted
        # work. Rows may still be rejected by earlier structural gates, may
        # never reach direct-quote revalidation, and may not consume a
        # SELECTOR_MAX_DIRECT_QUOTE_CALLS attempt before budget exhaustion.
        # A field named 'attempted_candidates' here would mislead operators;
        # pin the correct name so it cannot silently regress.
        assert "ordered_candidates" in audit
        assert "attempted_candidates" not in audit
        assert len(audit["ordered_candidates"]) == 3
        assert [row["strike"] for row in audit["ordered_candidates"]] == [
            62.0, 61.0, 60.0,
        ]


# ===========================================================================
# Required test E — Trigger and underlying both invalid
# ===========================================================================

class TestBothAnchorsInvalid:
    """When both anchors are missing/non-finite, no exception, no synthetic
    anchor, no broker submission."""

    def test_shared_authority_returns_none_source(self):
        pref = resolve_trigger_anchored_preferred_strikes(
            side="PUT", trigger_price=None, underlying_fallback=0.0,
            candidate_strikes=[60, 61, 62],
        )
        assert pref.anchor_source == TRIGGER_ANCHOR_SOURCE_NONE
        assert pref.primary_strike is None
        assert pref.adjacent_otm_strike is None

    def test_nan_and_inf_are_not_valid_anchors(self):
        pref = resolve_trigger_anchored_preferred_strikes(
            side="PUT", trigger_price=float("nan"),
            underlying_fallback=float("inf"),
            candidate_strikes=[60, 61, 62],
        )
        assert pref.anchor_source == TRIGGER_ANCHOR_SOURCE_NONE

    def test_negative_anchors_rejected(self):
        pref = resolve_trigger_anchored_preferred_strikes(
            side="CALL", trigger_price=-5.0, underlying_fallback=-1.0,
            candidate_strikes=[10, 11, 12],
        )
        assert pref.anchor_source == TRIGGER_ANCHOR_SOURCE_NONE

    def test_selector_does_not_raise_and_does_not_submit(self):
        """No trigger, no underlying — selector must return None deterministically
        and must never call any broker submit/cancel path."""
        chain = [_tradier_row(s, side="PUT") for s in (60.0, 61.0, 62.0)]
        engine = _make_engine()
        plan = _plan(ticker="BAC", side="PUT", trigger=None, target=60.0)

        # Sentinel: any broker submission/cancellation attempt raises.
        def _forbidden(*a, **kw):
            raise AssertionError("selector must not touch broker")

        engine.broker.submit_order = _forbidden
        engine.broker.cancel_order = _forbidden
        engine.broker.replace_order = _forbidden

        with patch.object(engine, "_fetch_chain_with_price",
                          return_value=(chain, None)), \
             patch.object(engine, "_quality_filter", return_value="zero_bid_or_ask"), \
             patch("ap.contract_selector._pro_contract_quality",
                   return_value=("B", "ok_liquid")):
            result = engine.select(plan)

        assert result is None  # no quality survivors — deterministic


# ===========================================================================
# Required test F — Index replays (SPY / QQQ)
# ===========================================================================

class TestIndexReplays:
    """Prove index classification, DTE policy, and the configured liquid-index
    list are unchanged, AND that direct-quote ordering + final ranking use the
    same trigger-preferred strikes for SPY/QQQ CALL and PUT."""

    def test_spy_still_classified_as_liquid_index_etf(self):
        from ap.contract_playbook import classify_instrument_class

        assert classify_instrument_class("SPY") == "LIQUID_INDEX_ETF"
        assert classify_instrument_class("QQQ") == "LIQUID_INDEX_ETF"
        assert classify_instrument_class("IWM") == "LIQUID_INDEX_ETF"
        assert classify_instrument_class("DIA") == "LIQUID_INDEX_ETF"

    def test_aapl_still_ordinary_equity(self):
        from ap.contract_playbook import classify_instrument_class

        assert classify_instrument_class("AAPL") == "ORDINARY_EQUITY"

    def test_configured_index_list_not_broadened(self, monkeypatch):
        """Custom config for LIQUID_INDEX_ETFS still narrows the set — the
        amendment does not silently expand it."""
        from ap.contract_playbook import classify_instrument_class

        monkeypatch.setenv("PLAYBOOK_LIQUID_INDEX_ETFS", "SPY,QQQ")
        assert classify_instrument_class("SPY") == "LIQUID_INDEX_ETF"
        assert classify_instrument_class("IWM") == "OTHER_ETF"

    @pytest.mark.parametrize(
        "ticker,side,trigger,underlying,strikes,expected_primary,expected_adj",
        [
            ("SPY", "PUT",  552.30, 553.10, [550,551,552,553,554], 552.0, 551.0),
            ("SPY", "CALL", 553.10, 552.30, [550,551,552,553,554], 553.0, 554.0),
            ("QQQ", "PUT",  490.20, 491.05, [488,489,490,491,492], 490.0, 489.0),
            ("QQQ", "CALL", 491.05, 490.20, [488,489,490,491,492], 491.0, 492.0),
        ],
    )
    def test_index_shared_authority_matches_playbook_ranker(
        self, ticker, side, trigger, underlying, strikes,
        expected_primary, expected_adj,
    ):
        # Shared authority
        pref = resolve_trigger_anchored_preferred_strikes(
            side=side, trigger_price=trigger, underlying_fallback=underlying,
            candidate_strikes=strikes,
        )
        assert pref.primary_strike == expected_primary
        assert pref.adjacent_otm_strike == expected_adj

        # Playbook ranker must produce the SAME primary/adjacent, proving no
        # divergence between quote-spending and final ranking.
        spec = _spec(side, trigger=trigger, underlying=underlying,
                     instrument_class="LIQUID_INDEX_ETF")
        ctx = build_playbook_candidate_context(
            spec, _candidate_dicts(strikes, ticker=ticker, side=side)
        )
        assert ctx["atm_strike"] == expected_primary
        assert ctx["one_step_otm_strike"] == expected_adj

    @pytest.mark.parametrize(
        "ticker,side,trigger,underlying,expected_first",
        [
            ("SPY", "PUT",  552.30, 553.10, 552.0),
            ("SPY", "CALL", 553.10, 552.30, 553.0),
            ("QQQ", "PUT",  490.20, 491.05, 490.0),
            ("QQQ", "CALL", 491.05, 490.20, 491.0),
        ],
    )
    def test_index_selector_iterates_trigger_preferred_first(
        self, ticker, side, trigger, underlying, expected_first,
    ):
        strikes = [expected_first - 2, expected_first - 1, expected_first,
                   expected_first + 1, expected_first + 2]
        chain = [_tradier_row(s, side=side, ticker=ticker) for s in strikes]
        observed, _, _, _ = _run_and_capture(
            ticker=ticker, side=side, trigger=trigger, underlying=underlying,
            chain=chain,
        )
        assert observed[0] == expected_first


# ===========================================================================
# Required test G — Gate preservation for trigger-preferred candidates
# ===========================================================================

class TestGatePreservation:
    """For a trigger-preferred candidate, independently prove that every
    existing hard quality gate still fires — the amendment must not weaken
    any of them."""

    def _reorder_with_bad_primary(self, *, gate_perturbation: dict):
        """Build a chain where the trigger-preferred strike (61 for BAC PUT)
        violates one gate; the other candidates are healthy. Prove the primary
        gets rejected and the healthy fallback is what actually wins."""
        good_side = "put"
        chain = []
        for strike in (60.0, 61.0, 62.0):
            row = _tradier_row(strike, side=good_side)
            if strike == 61.0:
                row.update(gate_perturbation)
            chain.append(row)
        result, engine, plan = _run_full_select(
            ticker="BAC", side="PUT", trigger=61.17, underlying=61.60,
            chain=chain,
        )
        return result, engine, plan

    def test_wide_spread_still_rejects_trigger_preferred_candidate(self):
        # Spread wider than PRO_T2_SPREAD_HARD_MAX (12%) — bid=1.00, ask=1.30 → 26%
        result, _, _ = self._reorder_with_bad_primary(
            gate_perturbation={"bid": 1.00, "ask": 1.30},
        )
        # Trigger-preferred 61 is rejected; 60 or 62 (which pass) wins.
        assert result is not None
        assert result.strike != 61.0

    def test_low_oi_and_volume_still_rejects_trigger_preferred_candidate(self):
        result, _, _ = self._reorder_with_bad_primary(
            gate_perturbation={"open_interest": 1, "volume": 0},
        )
        assert result is not None
        assert result.strike != 61.0

    def test_invalid_delta_still_rejects_trigger_preferred_candidate(self):
        # Delta far outside band
        result, _, _ = self._reorder_with_bad_primary(
            gate_perturbation={"greeks": {"delta": -0.99}},
        )
        assert result is not None
        assert result.strike != 61.0

    def test_invalid_dte_still_rejects_trigger_preferred_candidate(self):
        # Expiration ~120 days out — well beyond max_dte=21
        result, _, _ = self._reorder_with_bad_primary(
            gate_perturbation={
                "expiration_date": (date.today() + timedelta(days=120)).isoformat()
            },
        )
        assert result is not None
        assert result.strike != 61.0

    def test_premium_above_ticker_cap_still_rejects_trigger_preferred(self):
        # BAC has _DEFAULT cap of $800/contract → set premium above that.
        # ask=9.00 → premium=$900 (via quality filter check on _ticker_max).
        result, _, _ = self._reorder_with_bad_primary(
            gate_perturbation={"bid": 8.90, "ask": 9.10},
        )
        assert result is not None
        assert result.strike != 61.0


class TestSurvivorPreferenceRecomputation:
    """The direct path must rank the nearest valid survivors, not frozen tiers."""

    def test_put_recomputes_primary_after_original_preferred_rows_fail(self):
        chain = []
        for strike in (59.0, 60.0, 61.0, 62.0):
            row = _tradier_row(strike, side="PUT")
            if strike in (60.0, 61.0):
                row.update({"bid": 1.00, "ask": 1.30})
            chain.append(row)

        survivor_preference = resolve_trigger_anchored_preferred_strikes(
            side="PUT",
            trigger_price=61.17,
            underlying_fallback=61.60,
            candidate_strikes=[59.0, 62.0],
        )
        assert survivor_preference.primary_strike == 62.0

        result, _, plan = _run_full_select(
            ticker="BAC",
            side="PUT",
            trigger=61.17,
            underlying=61.60,
            chain=chain,
        )

        assert result is not None
        assert result.strike == survivor_preference.primary_strike

    def test_call_recomputes_primary_after_original_preferred_rows_fail(self):
        chain = []
        for strike in (60.0, 61.0, 62.0, 63.0):
            row = _tradier_row(strike, side="CALL")
            if strike in (61.0, 62.0):
                row.update({"bid": 1.00, "ask": 1.30})
            chain.append(row)

        survivor_preference = resolve_trigger_anchored_preferred_strikes(
            side="CALL",
            trigger_price=61.17,
            underlying_fallback=60.60,
            candidate_strikes=[60.0, 63.0],
        )
        assert survivor_preference.primary_strike == 60.0

        result, _, plan = _run_full_select(
            ticker="BAC",
            side="CALL",
            trigger=61.17,
            underlying=60.60,
            chain=chain,
        )

        assert result is not None
        assert result.strike == survivor_preference.primary_strike


# ===========================================================================
# Required test H — Pricing and side effects
# ===========================================================================

class TestPricingAndSideEffects:
    """Prove LIVE affordability uses ask, PAPER pricing unchanged, and the
    selector never touches the broker submit/cancel/replace paths.
    Identity fields (client_id, execution_mode, signal_id) are preserved."""

    def test_live_affordability_uses_ask(self):
        chain = [_tradier_row(s, side="PUT", bid=1.20, ask=1.30) for s in (60.0, 61.0, 62.0)]
        result, engine, plan = _run_full_select(
            ticker="BAC", side="PUT", trigger=61.17, underlying=61.60,
            chain=chain, execution_mode="live",
        )
        assert result is not None
        # LIVE pricing basis on the selected contract must be ASK_EXECUTION.
        assert result.pricing_basis == "ASK_EXECUTION"
        # execution price per share should be at ask, not mid.
        assert result.execution_price_per_share == pytest.approx(result.ask, rel=1e-6)

    def test_paper_pricing_unchanged(self):
        chain = [_tradier_row(s, side="PUT", bid=1.20, ask=1.30) for s in (60.0, 61.0, 62.0)]
        result, _, _ = _run_full_select(
            ticker="BAC", side="PUT", trigger=61.17, underlying=61.60,
            chain=chain, execution_mode="paper",
        )
        assert result is not None
        assert result.pricing_basis == "MID_SIMULATION"

    def test_selector_does_not_touch_broker(self):
        chain = [_tradier_row(s, side="PUT") for s in (60.0, 61.0, 62.0)]
        engine = _make_engine(mode="live")

        def _forbidden(*a, **kw):
            raise AssertionError("selector must not touch broker")

        engine.broker.submit_order = _forbidden
        engine.broker.cancel_order = _forbidden
        engine.broker.replace_order = _forbidden

        plan = _plan(ticker="BAC", side="PUT", trigger=61.17,
                     target=60.0, execution_mode="live")

        with patch.object(engine, "_fetch_chain_with_price",
                          return_value=(chain, 61.60)):
            result = engine.select(plan)

        assert result is not None  # selection succeeded without any broker call

    def test_identity_fields_preserved_across_selection(self):
        chain = [_tradier_row(s, side="PUT") for s in (60.0, 61.0, 62.0)]
        result, _, plan = _run_full_select(
            ticker="BAC", side="PUT", trigger=61.17, underlying=61.60,
            chain=chain,
        )
        assert result is not None
        assert plan["client_id"] == "client-test"
        assert plan["execution_mode"] == "paper"
        assert plan["signal_id"] == "sig-test"


# ===========================================================================
# Additional safety / blast-radius coverage
# ===========================================================================

class TestFlagOffLeavesChainOrderUntouched:
    """When PLAYBOOK_STRIKE_SELECTION_ENABLED is off, #389's structural
    direct-quote recovery ordering is the baseline. Enabling #396 may change
    only the preferred-strike tier inside that shared helper."""

    def test_flag_off_uses_post_389_baseline_order(self, monkeypatch):
        monkeypatch.delenv("PLAYBOOK_STRIKE_SELECTION_ENABLED", raising=False)
        monkeypatch.delenv("PLAYBOOK_CONTRACT_SELECTION_ENABLED", raising=False)

        chain = [_tradier_row(s, side="PUT") for s in (60.0, 61.0, 62.0)]
        observed, _, engine, plan = _run_and_capture(
            ticker="BAC", side="PUT", trigger=60.20, underlying=61.60, chain=chain,
        )
        expected = [
            float(opt["strike"])
            for opt in __import__(
                "ap.contract_selector", fromlist=["_order_chain_for_direct_quote_recovery"]
            )._order_chain_for_direct_quote_recovery(
                list(chain),
                direction="PUT",
                underlying_price=61.60,
                target_delta=0.35,
                today=date.today(),
                request_context=None,
            )
        ]
        assert observed == expected
        assert observed == [61.0, 60.0, 62.0]

        diag = plan["metadata"]["selector_request_diagnostics"]
        audit = diag.get("preferred_strike_ordering")
        assert audit is not None
        assert audit["enabled"] is False
        assert "ordered_candidates" not in audit
        assert diag["direct_quote_candidate_ranking"]

    def test_flag_on_changes_only_preferred_tier_inside_shared_helper(self):
        chain = [_tradier_row(s, side="PUT") for s in (60.0, 61.0, 62.0)]
        observed, _, _, plan = _run_and_capture(
            ticker="BAC", side="PUT", trigger=60.20, underlying=61.60, chain=chain,
        )
        assert observed == [60.0, 61.0, 62.0]
        diag = plan["metadata"]["selector_request_diagnostics"]
        ranking = diag["direct_quote_candidate_ranking"]
        assert [row["strike"] for row in ranking[:3]] == [60.0, 61.0, 62.0]
        assert ranking[0]["preferred_strike_tier"] == 0
        assert any(row["preferred_strike_tier"] > 0 for row in ranking[1:])


class TestPreferredStrikeReorderAudit:
    def test_reordered_false_when_provider_order_is_unchanged(self):
        chain = [_tradier_row(s, side="PUT") for s in (61.0, 60.0, 62.0)]
        _, _, _, plan = _run_and_capture(
            ticker="BAC",
            side="PUT",
            trigger=61.17,
            underlying=61.60,
            chain=chain,
        )

        audit = plan["metadata"]["selector_request_diagnostics"][
            "preferred_strike_ordering"
        ]
        assert audit["reordered"] is False
        assert [row["strike"] for row in audit["ordered_candidates"]] == [
            61.0, 60.0, 62.0,
        ]

    def test_reordered_true_when_provider_order_changes(self):
        chain = [_tradier_row(s, side="PUT") for s in (60.0, 61.0, 62.0)]
        _, _, _, plan = _run_and_capture(
            ticker="BAC",
            side="PUT",
            trigger=61.17,
            underlying=61.60,
            chain=chain,
        )

        audit = plan["metadata"]["selector_request_diagnostics"][
            "preferred_strike_ordering"
        ]
        assert audit["reordered"] is True
        assert [row["strike"] for row in audit["ordered_candidates"]] == [
            61.0, 60.0, 62.0,
        ]


class TestSharedAuthorityIsSingleSource:
    """Regression guard: build_playbook_candidate_context must delegate to
    resolve_trigger_anchored_preferred_strikes. If a second implementation of
    the anchoring formula is ever added anywhere, this test should catch it by
    asserting the two APIs remain in lockstep across representative inputs."""

    @pytest.mark.parametrize(
        "side,trigger,underlying,strikes",
        [
            ("PUT",  61.17, 61.60, [60, 61, 62]),
            ("CALL", 61.17, 60.60, [60, 61, 62]),
            ("PUT",  552.30, 553.10, [550, 551, 552, 553, 554]),
            ("CALL", 490.20, 489.50, [488, 489, 490, 491, 492]),
            ("PUT",  None,   61.60, [60, 61, 62]),   # fallback path
            ("PUT",  61.5,   61.5,  [61, 62]),       # tie-break
            ("CALL", 61.5,   61.5,  [61, 62]),       # tie-break
        ],
    )
    def test_ranker_and_authority_agree(self, side, trigger, underlying, strikes):
        pref = resolve_trigger_anchored_preferred_strikes(
            side=side, trigger_price=trigger, underlying_fallback=underlying,
            candidate_strikes=strikes,
        )
        spec = _spec(side, trigger=trigger, underlying=underlying)
        ctx = build_playbook_candidate_context(
            spec, _candidate_dicts(strikes, side=side)
        )
        assert ctx["atm_strike"] == pref.primary_strike
        assert ctx["one_step_otm_strike"] == pref.adjacent_otm_strike
        assert ctx["anchor_source"] == pref.anchor_source
        assert ctx["anchor_price"] == pref.anchor_price
