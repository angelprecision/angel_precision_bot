from __future__ import annotations

import os
import time
from datetime import date, timedelta
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://fake")

from ap.contract_selector import (
    SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    SELECTOR_REQUEST_KIND_ORDINARY,
    SelectorRequestContext,
    _new_selector_request_context,
    _order_chain_for_direct_quote_recovery,
    _resolve_direct_quote_budget_config,
    _structural_direct_quote_skip,
)
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
    return date.today() + timedelta(days=days)


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
        (_occ(101.0, ask=38.50), "CALL", 175.0, "STRUCTURAL_CLEARLY_UNAFFORDABLE"),
        (_occ(101.0, ask=9.00), "CALL", 5_000.0, "STRUCTURAL_PREMIUM_CAP_EXCEEDED"),
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


def test_affordability_headroom_allows_small_chain_ask_overage():
    ctx = _new_selector_request_context(
        "SPY",
        "live",
        selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
    )
    row = _occ(101.0, ask=1.80)
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
