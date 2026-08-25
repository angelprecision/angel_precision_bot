from __future__ import annotations

import os
import time
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://fake")

from unittest.mock import MagicMock

from ap.contract_selector import (
    APContractSelectionEngine,
    SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    SELECTOR_REQUEST_KIND_ORDINARY,
    SelectorRequestContext,
    _new_selector_request_context,
    _order_chain_for_direct_quote_recovery,
    _resolve_direct_quote_budget_config,
    _structural_direct_quote_skip,
)
from ap.contract_quote_revalidator import clear_quote_cache
from ap.selector_retry_policy import resolve_selector_recovery_final_reason
from ap_execution_core import _positive_int_env_config


def _clear_capacity_env(monkeypatch):
    for key in (
        "SELECTOR_MAX_DIRECT_QUOTE_CALLS",
        "DIRECT_QUOTE_RECOVERY_TOP_N",
        "CONTRACT_REVALIDATE_TOP_N",
        "SELECTOR_MAX_TOTAL_ELAPSED_MS",
        "SELECTOR_MAX_EXPIRATION_CALLS",
        "SELECTOR_MAX_CHAIN_CALLS",
        "MAX_BREACH_SELECTOR_RETRIES",
        "BREACH_SELECTOR_RETRY_DELAY_SECONDS",
        "SELECTOR_RECOVERY_AFFORDABILITY_HEADROOM_PCT",
        "SELECTOR_RECOVERY_SYMBOL_REFRESH_SECONDS",
    ):
        monkeypatch.delenv(key, raising=False)


def _expiry(days=2):
    d = date.today() + timedelta(days=days)
    while d.weekday() >= 5:  # skip Sat/Sun
        d += timedelta(days=1)
    return d


def _occ(strike: float, side="CALL", days=2, **extra):
    exp = _expiry(days)
    row = {
        "symbol": f"SPY{exp:%y%m%d}{'C' if side == 'CALL' else 'P'}{int(strike * 1000):08d}",
        "expiration_date": exp.isoformat(),
        "option_type": side.lower(),
        "strike": float(strike),
        "bid": 0.0,
        "ask": 0.0,
        "greeks": {"delta": 0.40 if side == "CALL" else -0.40},
        "open_interest": 100,
        "volume": 20,
    }
    row.update(extra)
    return row


def test_new_bounded_defaults(monkeypatch):
    _clear_capacity_env(monkeypatch)
    ctx = _new_selector_request_context(
        "SPY",
        "live",
        selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )

    assert ctx.effective_direct_quote_limit == 40
    assert ctx.max_total_elapsed_ms == 25_000
    assert ctx.max_expiration_calls == 3
    assert ctx.max_chain_calls == 8
    assert _positive_int_env_config("MAX_BREACH_SELECTOR_RETRIES", 5) == 5
    assert _positive_int_env_config("BREACH_SELECTOR_RETRY_DELAY_SECONDS", 8) == 8


def test_ordinary_request_retains_pre_pr_capacity_even_when_recovery_env_is_40(
    monkeypatch,
):
    _clear_capacity_env(monkeypatch)
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "40")
    monkeypatch.setenv("SELECTOR_MAX_TOTAL_ELAPSED_MS", "25000")
    monkeypatch.setenv("SELECTOR_MAX_EXPIRATION_CALLS", "3")
    monkeypatch.setenv("SELECTOR_MAX_CHAIN_CALLS", "8")
    ctx = _new_selector_request_context("SPY", "live")
    assert ctx.selector_request_kind == SELECTOR_REQUEST_KIND_ORDINARY
    # The invariant this test protects: the ordinary direct-quote CEILING stays
    # at 20 even when a deferred-scale env of 40 is present (never adopts the
    # deferred 40 capacity).
    assert ctx.effective_direct_quote_limit == 20
    # Explicit expiration/chain/elapsed env overrides ARE authoritative for
    # ordinary requests (restored base-SHA behavior); only the pre-PR #401
    # DEFAULTS (2 / 6 / 15000) apply when the env is unset.
    assert ctx.max_total_elapsed_ms == 25_000
    assert ctx.max_expiration_calls == 3
    assert ctx.max_chain_calls == 8


def test_ordinary_request_retains_pre_pr_default_when_capacity_env_is_unset(
    monkeypatch,
):
    # Pre-PR #401 the ordinary default was five direct quotes. PR #401 must not
    # let ordinary requests silently inherit the deferred-recovery capacity when
    # the canonical env is absent (pods, local runners, recovery processes).
    _clear_capacity_env(monkeypatch)
    ctx = _new_selector_request_context("SPY", "live")
    assert ctx.selector_request_kind == SELECTOR_REQUEST_KIND_ORDINARY
    assert ctx.effective_direct_quote_limit == 5
    assert ctx.max_direct_quote_calls == 5
    assert ctx.max_total_elapsed_ms == 15_000
    assert ctx.max_expiration_calls == 2
    assert ctx.max_chain_calls == 6


def test_selector_request_context_generic_defaults_remain_pre_pr():
    ctx = SelectorRequestContext(ticker="SPY")
    assert ctx.max_direct_quote_calls == 5
    assert ctx.effective_direct_quote_limit == 5
    assert ctx.max_total_elapsed_ms == 15_000


def test_ordinary_request_honors_explicit_pre_pr_request_limit_env(monkeypatch):
    # Base-SHA behavior: ordinary selector requests honor explicit expiration,
    # chain, and elapsed environment overrides. PR #401 must not hardcode these
    # for ordinary requests (it previously only read them for deferred recovery).
    _clear_capacity_env(monkeypatch)
    monkeypatch.setenv("SELECTOR_MAX_EXPIRATION_CALLS", "2")
    monkeypatch.setenv("SELECTOR_MAX_CHAIN_CALLS", "4")
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "3")
    monkeypatch.setenv("SELECTOR_MAX_TOTAL_ELAPSED_MS", "9000")
    ctx = _new_selector_request_context("SPY", "live")
    assert ctx.selector_request_kind == SELECTOR_REQUEST_KIND_ORDINARY
    assert ctx.max_expiration_calls == 2
    assert ctx.max_chain_calls == 4
    assert ctx.max_direct_quote_calls == 3
    assert ctx.effective_direct_quote_limit == 3
    assert ctx.max_total_elapsed_ms == 9000
    assert ctx.direct_quote_attempts_remaining == 3


@pytest.mark.parametrize("raw", ["abc", "0", "-7"])
def test_bad_canonical_falls_back_to_40_not_alias(monkeypatch, raw):
    _clear_capacity_env(monkeypatch)
    cfg = _resolve_direct_quote_budget_config({
        "SELECTOR_MAX_DIRECT_QUOTE_CALLS": raw,
        "DIRECT_QUOTE_RECOVERY_TOP_N": "8",
        "CONTRACT_REVALIDATE_TOP_N": "20",
    })
    assert cfg.effective_limit == 40
    assert cfg.source == "default"
    assert cfg.direct_recovery_raw == "8"
    assert cfg.contract_revalidate_raw == "20"


@pytest.mark.parametrize("value", [20, 50])
def test_valid_canonical_value_is_honored(value):
    cfg = _resolve_direct_quote_budget_config({
        "SELECTOR_MAX_DIRECT_QUOTE_CALLS": str(value),
        "DIRECT_QUOTE_RECOVERY_TOP_N": "8",
        "CONTRACT_REVALIDATE_TOP_N": "20",
    })
    assert cfg.effective_limit == value
    assert cfg.source == "SELECTOR_MAX_DIRECT_QUOTE_CALLS"
    assert cfg.conflict is True


def test_aliases_are_diagnostic_not_behavioral():
    cfg = _resolve_direct_quote_budget_config({
        "DIRECT_QUOTE_RECOVERY_TOP_N": "8",
        "CONTRACT_REVALIDATE_TOP_N": "20",
    })
    assert cfg.effective_limit == 40
    assert cfg.source == "default"


@pytest.mark.parametrize(
    ("side", "preferred", "expected"),
    [
        ("CALL", [100.0, 101.0], [100.0, 101.0]),
        ("PUT", [100.0, 99.0], [100.0, 99.0]),
    ],
)
def test_deferred_trigger_tiers_spend_first(side, preferred, expected):
    rows = [_occ(strike, side) for strike in (98.0, 99.0, 100.0, 101.0, 102.0)]
    ctx = SelectorRequestContext(
        ticker="SPY",
        selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
        started_at_monotonic=time.monotonic(),
    )
    ordered = _order_chain_for_direct_quote_recovery(
        rows,
        direction=side,
        underlying_price=100.4,
        target_delta=0.40,
        today=date.today(),
        request_context=ctx,
        preferred_strikes=preferred,
    )
    assert [row["strike"] for row in ordered[:2]] == expected


def test_ordinary_request_keeps_baseline_when_no_preference():
    rows = [_occ(strike) for strike in (103.0, 101.0, 102.0)]
    ctx = SelectorRequestContext(
        ticker="SPY",
        selector_request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
        started_at_monotonic=time.monotonic(),
    )
    ordered = _order_chain_for_direct_quote_recovery(
        rows,
        direction="CALL",
        underlying_price=100.0,
        target_delta=0.40,
        today=date.today(),
        request_context=ctx,
        preferred_strikes=None,
    )
    assert [row["strike"] for row in ordered] == [101.0, 102.0, 103.0]


def _engine(**overrides):
    base = {
        "min_dte": 0,
        "max_dte": 21,
        "target_delta": 0.40,
        "delta_band": 0.30,
    }
    base.update(overrides)
    return SimpleNamespace(**base)


@pytest.mark.parametrize(
    ("row", "direction", "budget", "reason"),
    [
        ({"symbol": "bad"}, "CALL", 200.0, "STRUCTURAL_INVALID_OCC"),
        (_occ(100.0, "PUT"), "CALL", 200.0, "STRUCTURAL_SIDE_MISMATCH"),
        (_occ(100.0, days=40), "CALL", 200.0, "STRUCTURAL_DTE_OUT_OF_RANGE"),
        (_occ(130.0), "CALL", 200.0, "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"),
        (_occ(101.0, greeks={"delta": 0.01}), "CALL", 200.0, "STRUCTURAL_DELTA_OUT_OF_RANGE"),
    ],
)
def test_structural_prefilter_consumes_no_provider_call(
    row, direction, budget, reason
):
    ctx = _new_selector_request_context(
        "SPY",
        "live",
        selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )
    result = _structural_direct_quote_skip(
        _engine(),
        row,
        direction=direction,
        ticker="SPY",
        underlying_price=100.0,
        today=date.today(),
        selector_budget=budget,
        request_context=ctx,
    )
    assert result["skip_reason"] == reason
    assert result["provider_call_consumed"] is False
    assert ctx.provider_call_counts.get("direct_quote_calls", 0) == 0


@pytest.mark.parametrize(
    ("row", "reason", "canonical"),
    [
        (_occ(130.0), "STRUCTURAL_MONEYNESS_OUT_OF_RANGE", "MONEYNESS_OUT_OF_RANGE"),
        (_occ(100.0, days=40), "STRUCTURAL_DTE_OUT_OF_RANGE", "DTE_OUT_OF_RANGE"),
        (
            _occ(101.0, greeks={"delta": 0.01}),
            "STRUCTURAL_DELTA_OUT_OF_RANGE",
            "DELTA_OUT_OF_RANGE",
        ),
    ],
    ids=["moneyness", "dte", "delta"],
)
def test_deferred_structural_prefilter_reaches_request_scope_reducer(
    row, reason, canonical
):
    """Exercise the real prefilter -> context ledger -> reducer handoff."""
    ctx = _new_selector_request_context(
        "SPY",
        "live",
        selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )
    diagnostic = _structural_direct_quote_skip(
        _engine(),
        row,
        direction="CALL",
        ticker="SPY",
        underlying_price=100.0,
        today=date.today(),
        selector_budget=200.0,
        request_context=ctx,
    )

    assert diagnostic["skip_reason"] == reason
    assert ctx.structural_skips == [diagnostic]
    assert resolve_selector_recovery_final_reason({
        "structural_skip_records": ctx.structural_skips,
        "attempted_results": {},
        "eligible_unattempted_symbols": [],
        "direct_quote_known_eligible_symbols": [diagnostic["symbol"]],
        "quality_rejections": {},
        "quality_rejection_records": [],
    }) == canonical


@pytest.mark.parametrize(
    ("chain_ask", "budget"),
    [
        # (a) chain ask far above budget — the classic stale-chain case.
        (38.50, 175.0),
        # (b) chain ask far above ticker premium cap.
        (9.00, 5_000.0),
        # (c) small overage — the old "headroom" path is no longer relevant.
        (1.80, 175.0),
    ],
)
def test_chain_ask_price_never_suppresses_direct_quote(chain_ask, budget):
    # Chain price (ask, estimated cost, ticker premium cap) MUST NOT be used to
    # skip the direct quote at this seam. This prefilter is only entered because
    # the chain row already failed a revalidatable quote-quality rule; the fresh
    # direct quote is the authoritative price.
    ctx = _new_selector_request_context(
        "SPY",
        "live",
        selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )
    row = _occ(101.0, ask=chain_ask, bid=0.0)
    assert _structural_direct_quote_skip(
        _engine(),
        row,
        direction="CALL",
        ticker="SPY",
        underlying_price=100.0,
        today=date.today(),
        selector_budget=budget,
        request_context=ctx,
    ) is None
    assert ctx.structural_skips == []


def test_ordinary_request_never_uses_recovery_structural_prefilter():
    ctx = _new_selector_request_context("SPY", "live")
    row = _occ(101.0, ask=38.50)
    assert _structural_direct_quote_skip(
        _engine(),
        row,
        direction="CALL",
        ticker="SPY",
        underlying_price=100.0,
        today=date.today(),
        selector_budget=175.0,
        request_context=ctx,
    ) is None
    assert ctx.structural_skips == []


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 1 regressions: chain price MUST NOT suppress the fresh direct quote.
# Blocker 2 regressions: recovery final-reason resolver is deferred-only.
# ─────────────────────────────────────────────────────────────────────────────

def _live_broker(chain, valid_symbol, valid_quote, underlying_price=100.0):
    """A minimal production-shaped broker for selector integration tests."""

    class _Broker:
        base_url = "https://api.tradier.com"
        cfg = SimpleNamespace(base_url="https://api.tradier.com", access_token="tok")

        def __init__(self):
            self.get_quote = MagicMock(side_effect=self._quote)
            self.submit_order = MagicMock()
            self.cancel_order = MagicMock()
            self.session = MagicMock()
            self.session.get.side_effect = self._session_get

        def _session_get(self, url, *, params=None, headers=None, timeout=None):
            resp = MagicMock()
            resp.status_code = 200
            if "options/expirations" in url:
                resp.json.return_value = {"expirations": {"date": [_expiry().isoformat()]}}
            elif "options/chains" in url:
                resp.json.return_value = {"options": {"option": chain}}
            else:
                resp.json.return_value = {"quotes": {"quote": {"last": underlying_price}}}
            return resp

        def _quote(self, symbol):
            if "".join(str(symbol).upper().split()) == valid_symbol:
                return dict(valid_quote)
            return {"bid": 0.0, "ask": 0.0}

    return _Broker()


def _integration_plan(*, ticker, direction, budget, trigger, underlying):
    return {
        "signal_id": f"sig-{ticker}",
        "client_id": f"client-{ticker}",
        "execution_mode": "LIVE",
        "ticker": ticker,
        "side": direction,
        "target_underlying": float(underlying),
        "wick_targets": [{"distance_pct": 0.5, "confidence": 0.75}],
        "trigger_price": float(trigger),
        "tier": "A",
        "score": 85.0,
        "pattern": "3-1-2",
        "timeframe": "5m",
        "metadata": {
            "sizing_context": {
                "budget": float(budget),
                "account_equity": float(budget) * 10.0,
                "risk_pct": 0.10,
                "max_affordable_premium": float(budget),
            }
        },
        "max_position_usd": float(budget),
    }


def _run_selector(monkeypatch, *, plan, chain, valid_symbol, valid_quote, underlying,
                  request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH):
    _clear_capacity_env(monkeypatch)
    monkeypatch.setenv("PRO_CONTRACT_QUALITY", "true")
    monkeypatch.setenv("TRADIER_MD_THROTTLE_ENABLED", "0")
    monkeypatch.setattr(
        "ap.contract_quote_revalidator.is_market_open",
        lambda *a, **k: True,
    )
    monkeypatch.setattr(
        APContractSelectionEngine, "_emit_selector_event", lambda *a, **k: None,
    )
    broker = _live_broker(chain, valid_symbol, valid_quote, underlying_price=underlying)
    selector = APContractSelectionEngine(
        broker, mode="LIVE", data_broker=broker,
        min_premium=1.0, max_premium=1000.0, min_oi=1, min_volume=0,
    )
    ctx = _new_selector_request_context(
        plan["ticker"], "live", selector_request_kind=request_kind,
    )
    selected = selector.select(plan, request_context=ctx)
    clear_quote_cache()
    return selected, broker, ctx


def test_A_rejected_chain_ask_does_not_suppress_affordable_direct_quote(monkeypatch):
    # A near-trigger CALL with a stale chain ask of $3.15 (a rejected quote:
    # bid=0) whose FRESH direct quote is bid=1.60/ask=1.70 must not be skipped
    # as STRUCTURAL_CLEARLY_UNAFFORDABLE under a $174.71 budget. The provider
    # is called once and the direct quote succeeds.
    exp = _expiry(2)
    occ = f"SPY{exp:%y%m%d}C00101000"
    chain_row = _occ(101.0, "CALL", bid=0.0, ask=3.15,
                     open_interest=2000, volume=500)
    chain = [chain_row]
    plan = _integration_plan(
        ticker="SPY", direction="CALL", budget=174.71,
        trigger=100.0, underlying=100.0,
    )
    selected, broker, ctx = _run_selector(
        monkeypatch, plan=plan, chain=chain, valid_symbol=occ,
        valid_quote={"bid": 1.60, "ask": 1.70, "volume": 500, "open_interest": 2000},
        underlying=100.0,
    )
    # The provider is called exactly once for the near-trigger contract.
    quoted = [call.args[0] for call in broker.get_quote.call_args_list]
    assert quoted == [occ]
    # No structural skip fired on this chain row.
    assert ctx.structural_skips == []
    reasons = {item.get("skip_reason") for item in ctx.structural_skips}
    assert "STRUCTURAL_CLEARLY_UNAFFORDABLE" not in reasons
    assert "STRUCTURAL_PREMIUM_CAP_EXCEEDED" not in reasons
    # The fresh direct quote passed quality; the selector may return the row.
    assert selected is not None
    assert selected.contract_symbol == occ
    # No broker submit path is touched by selector.select().
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0


def test_B_ibm_ordinary_risk_replay_terminates_affordable_when_fresh_ask_exceeds_budget(monkeypatch):
    # Real IBM P0 shape: $174.71 selector budget on a $1747.09 account, near
    # trigger $228.98 CALL, fresh direct quote bid=$2.95/ask=$3.15 → one
    # contract costs ~$315 (unaffordable under $174.71). The selector must:
    #   * call the provider (Blocker 1 no longer suppresses via chain ask);
    #   * pass contract quality (delta/OI/volume/spread);
    #   * terminalize on the truthful account-size affordability reason;
    #   * not schedule any broker submit/cancel;
    #   * not run recovery resolver for an ORDINARY request (Blocker 2).
    exp = _expiry(2)
    occ = "IBM260731C00230000"
    # Override the OCC to the canonical review-supplied symbol.
    ibm_row = _occ(230.0, "CALL", bid=0.0, ask=3.15, greeks={"delta": 0.4082},
                   open_interest=2045, volume=1249)
    ibm_row["symbol"] = occ
    ibm_row["expiration_date"] = exp.isoformat()
    plan = _integration_plan(
        ticker="IBM", direction="CALL", budget=174.71,
        trigger=228.98, underlying=228.98,
    )
    # Exercise ORDINARY selection here — that is the base-SHA path this
    # affordability scenario would have hit in production.
    selected, broker, ctx = _run_selector(
        monkeypatch, plan=plan, chain=[ibm_row], valid_symbol=occ,
        valid_quote={"bid": 2.95, "ask": 3.15, "volume": 1249, "open_interest": 2045},
        underlying=228.98,
        request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
    )
    quoted = [call.args[0] for call in broker.get_quote.call_args_list]
    assert quoted[0] == occ, f"IBM contract must be first direct-quote attempt, got {quoted}"
    assert selected is None
    failure = plan["metadata"].get("selector_failure") or {}
    reason = failure.get("reason_code", "")
    # Truthful terminal affordability reason. Blocker 2 keeps ordinary requests
    # on the base-SHA reason (UNTRADEABLE_FOR_ACCOUNT_SIZE / NO_AFFORDABLE_CONTRACT /
    # PREMIUM_CAP_EXCEEDED), never the deferred-recovery SELECTOR_REQUEST_BUDGET_EXHAUSTED.
    assert reason in {
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
        "NO_AFFORDABLE_CONTRACT",
        "PREMIUM_CAP_EXCEEDED",
    }, f"unexpected reason: {reason!r}"
    assert reason != "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    assert broker.submit_order.call_count == 0
    assert broker.cancel_order.call_count == 0


def test_ordinary_selector_does_not_invoke_deferred_request_scope_reducer(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "ap.contract_selector._resolve_deferred_recovery_final_reason_fail_closed",
        lambda evidence: calls.append(evidence) or "MONEYNESS_OUT_OF_RANGE",
    )
    exp = _expiry(2)
    occ = f"SPY{exp:%y%m%d}C00101000"
    chain_row = _occ(101.0, "CALL", bid=0.0, ask=0.0)
    chain_row["symbol"] = occ
    chain_row["expiration_date"] = exp.isoformat()
    plan = _integration_plan(
        ticker="SPY", direction="CALL", budget=200.0,
        trigger=100.0, underlying=100.0,
    )

    selected, _broker, context = _run_selector(
        monkeypatch,
        plan=plan,
        chain=[chain_row],
        valid_symbol=occ,
        valid_quote={"bid": 0.0, "ask": 0.0, "volume": 0, "open_interest": 0},
        underlying=100.0,
        request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
    )

    assert selected is None
    assert context.selector_request_kind == SELECTOR_REQUEST_KIND_ORDINARY
    assert calls == []


def test_C_genuinely_expensive_direct_quote_remains_terminal(monkeypatch):
    # Chain ask is a stale rejected quote; fresh direct ask is above the ticker
    # premium cap. The provider IS called (chain price never suppresses), and
    # the current direct quote authorizes the terminal affordability reason.
    exp = _expiry(2)
    occ = f"SPY{exp:%y%m%d}C00101000"
    chain_row = _occ(101.0, "CALL", bid=0.0, ask=99999.0,  # obviously stale/rejected
                     open_interest=2000, volume=500)
    plan = _integration_plan(
        ticker="SPY", direction="CALL", budget=200.0,
        trigger=100.0, underlying=100.0,
    )
    # Fresh direct quote is legitimately expensive: $80/contract = $8000.
    selected, broker, ctx = _run_selector(
        monkeypatch, plan=plan, chain=[chain_row], valid_symbol=occ,
        valid_quote={"bid": 79.90, "ask": 80.00, "volume": 500, "open_interest": 2000},
        underlying=100.0,
        request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
    )
    quoted = [call.args[0] for call in broker.get_quote.call_args_list]
    assert quoted == [occ]
    assert selected is None
    failure = plan["metadata"].get("selector_failure") or {}
    reason = failure.get("reason_code", "")
    assert reason in {
        "PREMIUM_CAP_EXCEEDED",
        "UNTRADEABLE_FOR_ACCOUNT_SIZE",
        "NO_AFFORDABLE_CONTRACT",
    }, f"unexpected reason: {reason!r}"
    assert broker.submit_order.call_count == 0


def test_D_capacity_invariants_unchanged(monkeypatch):
    # Deferred unset default = 40.
    _clear_capacity_env(monkeypatch)
    ctx = _new_selector_request_context(
        "SPY", "live", selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )
    assert ctx.effective_direct_quote_limit == 40
    assert ctx.max_direct_quote_calls == 40

    # Ordinary unset default = 5.
    _clear_capacity_env(monkeypatch)
    ctx = _new_selector_request_context("SPY", "live")
    assert ctx.effective_direct_quote_limit == 5
    assert ctx.max_direct_quote_calls == 5

    # Ordinary env-configured value is authoritative up to the ceiling of 20.
    _clear_capacity_env(monkeypatch)
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "1")
    assert _new_selector_request_context("SPY", "live").effective_direct_quote_limit == 1
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "40")
    assert _new_selector_request_context("SPY", "live").effective_direct_quote_limit == 20

    # Deferred honors env-configured value fully.
    _clear_capacity_env(monkeypatch)
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "40")
    ctx = _new_selector_request_context(
        "SPY", "live", selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )
    assert ctx.effective_direct_quote_limit == 40


def test_D_deferred_41st_quote_rejected(monkeypatch):
    # 41st direct quote is rejected once the deferred 40-call budget is
    # exhausted and no extra provider call occurs.
    from ap.contract_quote_revalidator import revalidate_with_direct_quote

    _clear_capacity_env(monkeypatch)
    monkeypatch.setattr(
        "ap.contract_quote_revalidator.is_market_open", lambda *a, **k: True,
    )
    broker = MagicMock()
    broker.get_quote.return_value = {"bid": 1.10, "ask": 1.20}
    ctx = _new_selector_request_context(
        "SPY", "live", selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )
    exp = _expiry(2)

    def _opt(i):
        return {
            "symbol": f"SPY{exp:%y%m%d}C{440000 + i * 1000:08d}",
            "expiration_date": exp.isoformat(),
            "option_type": "call",
            "strike": 440.0 + i,
            "bid": 0.0, "ask": 0.0,
            "greeks": {"delta": 0.40},
            "open_interest": 1200,
            "volume": 300,
        }

    results = [
        revalidate_with_direct_quote(
            broker, _opt(i), "zero_bid_or_ask",
            market_open_override=True, request_context=ctx,
        )
        for i in range(41)
    ]
    assert broker.get_quote.call_count == 40
    assert results[39]["action"] == "PASS"
    assert results[40]["action"] == "SKIP_BUDGET_EXHAUSTED"
    assert results[40]["reason_code"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"


# ─────────────────────────────────────────────────────────────────────────────
# Ordinary selector: recovery final-reason resolver must NOT run (Blocker 2).
# ─────────────────────────────────────────────────────────────────────────────

def test_ordinary_no_survivor_selection_preserves_base_reason(monkeypatch):
    # Ordinary request with a chain that yields no survivors must NOT be
    # rewritten by resolve_selector_recovery_final_reason. The truthful reason
    # is the top-quality rejection, not UNKNOWN_SELECTOR_RECOVERY_FAILURE.
    exp = _expiry(2)
    chain = [
        _occ(101.0 + i, "CALL", bid=0.0, ask=0.0)  # every row is zero bid/ask
        for i in range(3)
    ]
    plan = _integration_plan(
        ticker="SPY", direction="CALL", budget=2000.0,
        trigger=100.0, underlying=100.0,
    )
    selected, broker, ctx = _run_selector(
        monkeypatch, plan=plan, chain=chain,
        valid_symbol="__NONE__",
        valid_quote={"bid": 0.0, "ask": 0.0},
        underlying=100.0,
        request_kind=SELECTOR_REQUEST_KIND_ORDINARY,
    )
    assert selected is None
    failure = plan["metadata"].get("selector_failure") or {}
    reason = failure.get("reason_code", "")
    assert reason != "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
    assert reason != "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
    # It should be a truthful quality/data rejection.
    assert reason, "expected a non-empty base-SHA reason"


def test_deferred_no_survivor_selection_still_uses_recovery_resolver(monkeypatch):
    # The deferred-recovery resolver is still authoritative for deferred kind.
    exp = _expiry(2)
    chain = [
        _occ(101.0 + i, "CALL", bid=0.0, ask=0.0)
        for i in range(3)
    ]
    plan = _integration_plan(
        ticker="SPY", direction="CALL", budget=2000.0,
        trigger=100.0, underlying=100.0,
    )
    selected, broker, ctx = _run_selector(
        monkeypatch, plan=plan, chain=chain,
        valid_symbol="__NONE__",
        valid_quote={"bid": 0.0, "ask": 0.0},
        underlying=100.0,
        request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )
    assert selected is None
    failure = plan["metadata"].get("selector_failure") or {}
    reason = failure.get("reason_code", "")
    # Deferred keeps the recovery-resolver taxonomy — either the retryable
    # data reason or one of the recovery terminals.
    assert reason in {
        "DIRECT_QUOTE_ZERO_BID_ASK",
        "CHAIN_ROW_ZERO_BID_ASK",
        "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "UNKNOWN_SELECTOR_RECOVERY_FAILURE",
    }, f"unexpected deferred reason: {reason!r}"
