from __future__ import annotations

import copy
import logging
import os
import time
from datetime import date
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap.contract_quote_revalidator import (
    clear_quote_cache,
    fetch_direct_option_quote_with_meta,
    revalidate_with_direct_quote,
)
from ap.contract_selector import (
    APContractSelectionEngine,
    SelectorRequestContext,
    _new_selector_request_context,
    _order_chain_for_direct_quote_recovery,
    _resolve_direct_quote_budget_config,
    _selector_request_diagnostics,
)


@pytest.fixture(autouse=True)
def _clear_cache_and_throttle(monkeypatch):
    clear_quote_cache()
    monkeypatch.setenv("TRADIER_MD_THROTTLE_ENABLED", "0")
    yield
    clear_quote_cache()


def _ctx(limit: int) -> SelectorRequestContext:
    ctx = SelectorRequestContext(
        ticker="SPY",
        started_at_monotonic=time.monotonic(),
        max_direct_quote_calls=limit,
        effective_direct_quote_limit=limit,
    )
    ctx.diagnostics_sink = {}
    return ctx


def _option(
    idx: int,
    *,
    direction: str = "CALL",
    strike: float = 450.0,
    expiration: str = "2026-07-24",
    delta: float | None = 0.40,
    oi: int | None = 100,
    volume: int | None = 10,
) -> dict:
    cp = "C" if direction == "CALL" else "P"
    occ = f"SPY260724{cp}{int(strike * 1000):08d}"
    opt = {
        "symbol": occ,
        "expiration_date": expiration,
        "strike": strike,
        "option_type": direction.lower(),
        "bid": 0.0,
        "ask": 0.0,
        "open_interest": oi,
        "volume": volume,
        "_provider_index": idx,
    }
    if delta is not None:
        opt["greeks"] = {"delta": delta if direction == "CALL" else -abs(delta)}
    return opt


class TestConfigurationAuthority:
    def test_canonical_conflict_does_not_reduce_limit(self, caplog):
        caplog.set_level(logging.CRITICAL, logger="ap.contract_selector")
        cfg = _resolve_direct_quote_budget_config({
            "SELECTOR_MAX_DIRECT_QUOTE_CALLS": "20",
            "DIRECT_QUOTE_RECOVERY_TOP_N": "8",
            "CONTRACT_REVALIDATE_TOP_N": "20",
        })

        assert cfg.effective_limit == 20
        assert cfg.source == "SELECTOR_MAX_DIRECT_QUOTE_CALLS"
        assert cfg.conflict is True
        assert "direct_recovery=8" in (cfg.conflict_detail or "")
        assert "SELECTOR_DIRECT_QUOTE_BUDGET_CONFLICT" in caplog.text

    @pytest.mark.parametrize(
        ("env", "limit", "source", "conflict"),
        [
            ({"SELECTOR_MAX_DIRECT_QUOTE_CALLS": "20"}, 20, "SELECTOR_MAX_DIRECT_QUOTE_CALLS", False),
            ({"DIRECT_QUOTE_RECOVERY_TOP_N": "8"}, 8, "DIRECT_QUOTE_RECOVERY_TOP_N", False),
            ({"CONTRACT_REVALIDATE_TOP_N": "20"}, 20, "CONTRACT_REVALIDATE_TOP_N", False),
            ({}, 5, "default", False),
        ],
    )
    def test_budget_precedence(self, env, limit, source, conflict):
        cfg = _resolve_direct_quote_budget_config(env)

        assert cfg.effective_limit == limit
        assert cfg.source == source
        assert cfg.conflict is conflict

    @pytest.mark.parametrize("bad", ["abc", "0", "-2"])
    def test_malformed_canonical_falls_back_without_exception(self, bad, caplog):
        caplog.set_level(logging.WARNING, logger="ap.contract_selector")

        cfg = _resolve_direct_quote_budget_config({
            "SELECTOR_MAX_DIRECT_QUOTE_CALLS": bad,
            "DIRECT_QUOTE_RECOVERY_TOP_N": "20",
        })

        assert cfg.effective_limit == 20
        assert cfg.source == "DIRECT_QUOTE_RECOVERY_TOP_N"
        assert "SELECTOR_ENV_PARSE_ERROR key=SELECTOR_MAX_DIRECT_QUOTE_CALLS" in caplog.text

    def test_new_context_diagnostics_start_at_effective_limit(self, monkeypatch):
        monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "20")
        monkeypatch.setenv("DIRECT_QUOTE_RECOVERY_TOP_N", "8")
        ctx = _new_selector_request_context("SPY", "live")
        diag = _selector_request_diagnostics(ctx)

        assert ctx.max_direct_quote_calls == 20
        assert ctx.effective_direct_quote_limit == 20
        assert diag["limits"]["max_direct_quote_calls"] == 20
        assert diag["direct_quote_attempts_remaining"] == 20
        assert diag["direct_quote_budget"]["effective_limit"] == 20
        assert diag["direct_quote_budget"]["direct_recovery_alias"] == 8


class TestOneAuthority:
    def test_twentieth_call_allowed_twenty_first_skipped(self):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 1.10, "ask": 1.20}
        ctx = _ctx(20)

        results = [
            revalidate_with_direct_quote(
                broker,
                _option(idx, strike=440 + idx),
                "zero_bid_or_ask",
                market_open_override=True,
                request_context=ctx,
            )
            for idx in range(21)
        ]

        assert broker.get_quote.call_count == 20
        assert results[19]["action"] == "PASS"
        assert results[20]["action"] == "SKIP_BUDGET_EXHAUSTED"
        assert results[20]["reason_code"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        diag = _selector_request_diagnostics(ctx)
        assert diag["direct_quote_budget"]["used"] == 20
        assert diag["direct_quote_budget"]["remaining"] == 0

    def test_canonical_twenty_legacy_eight_allows_ninth_call(self, monkeypatch):
        monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "20")
        monkeypatch.setenv("DIRECT_QUOTE_RECOVERY_TOP_N", "8")
        ctx = _new_selector_request_context("SPY", "live")
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 1.10, "ask": 1.20}

        for idx in range(9):
            result = revalidate_with_direct_quote(
                broker,
                _option(idx, strike=450 + idx),
                "zero_bid_or_ask",
                market_open_override=True,
                request_context=ctx,
            )

        assert result["action"] == "PASS"
        assert broker.get_quote.call_count == 9
        diag = _selector_request_diagnostics(ctx)
        assert diag["direct_quote_budget"]["used"] == 9
        assert diag["direct_quote_budget"]["remaining"] == 11

    def test_mutable_remaining_counter_cannot_stop_before_effective_limit(self):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 1.10, "ask": 1.20}
        ctx = _ctx(3)
        ctx.direct_quote_attempts_remaining = 0

        result = revalidate_with_direct_quote(
            broker,
            _option(1),
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )

        assert result["action"] == "PASS"
        assert broker.get_quote.call_count == 1

    def test_elapsed_budget_still_blocks_numeric_budget(self):
        broker = MagicMock()
        ctx = _ctx(20)
        ctx.started_at_monotonic = time.monotonic() - 10
        ctx.max_total_elapsed_ms = 1

        result = revalidate_with_direct_quote(
            broker,
            _option(1),
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )

        assert result["action"] == "SKIP_BUDGET_EXHAUSTED"
        assert broker.get_quote.call_count == 0
        assert ctx.budget_exhausted_stage == "direct_quote"

    def test_duplicate_symbols_consume_one_call(self):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 1.10, "ask": 1.20}
        ctx = _ctx(20)

        first = revalidate_with_direct_quote(
            broker,
            {"symbol": "SPY260724C00450000", "bid": 0, "ask": 0},
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )
        second = revalidate_with_direct_quote(
            broker,
            {"symbol": "SPY   260724C00450000", "bid": 0, "ask": 0},
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )

        assert first["action"] == "PASS"
        assert second["action"] == "SKIP_ALREADY_REVALIDATED"
        assert broker.get_quote.call_count == 1
        assert ctx.provider_call_counts["direct_quote_calls"] == 1
        assert ctx.direct_quote_attempted_symbols == ["SPY260724C00450000"]


class TestCandidateOrderingAndReasonHonesty:
    def _chain(self, direction: str) -> list[dict]:
        chain = []
        for idx in range(130):
            strike = 560 + idx if direction == "CALL" else 340 - idx
            chain.append(_option(idx, direction=direction, strike=float(strike), delta=None, oi=0, volume=0))
        chain[100] = _option(100, direction=direction, strike=451.0 if direction == "CALL" else 449.0, delta=0.41, oi=800, volume=90)
        return chain

    @pytest.mark.parametrize("direction", ["CALL", "PUT"])
    def test_130_row_replay_ranks_recoverable_candidate_inside_top_twenty(self, direction):
        chain = self._chain(direction)
        original = copy.deepcopy(chain)
        ctx = _ctx(20)

        ordered = _order_chain_for_direct_quote_recovery(
            chain,
            direction=direction,
            underlying_price=450.0,
            target_delta=0.40,
            today=date(2026, 7, 21),
            request_context=ctx,
        )

        symbols = [row["symbol"] for row in ordered[:20]]
        assert chain == original
        assert chain[100]["symbol"] in symbols
        assert ctx.direct_quote_candidate_ranking[0]["original_index"] == 100
        assert ctx.direct_quote_candidate_ranking[0]["directional_strike_fit"] is True

    def test_original_index_breaks_exact_ties(self):
        ctx = _ctx(20)
        chain = [_option(0), _option(1)]

        ordered = _order_chain_for_direct_quote_recovery(
            chain,
            direction="CALL",
            underlying_price=450.0,
            target_delta=0.40,
            today=date(2026, 7, 21),
            request_context=ctx,
        )

        assert [row["_provider_index"] for row in ordered] == [0, 1]

    def test_budget_skip_keeps_original_reason_and_counts_unattempted(self):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 0.0, "ask": 0.0}
        ctx = _ctx(1)

        first = revalidate_with_direct_quote(
            broker,
            _option(1),
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )
        second = revalidate_with_direct_quote(
            broker,
            _option(2, strike=451.0),
            "low_oi_0",
            market_open_override=True,
            request_context=ctx,
        )

        assert first["action"] == "REJECT_DIRECT_ZERO"
        assert first["reason_code"] == "DIRECT_QUOTE_ZERO_BID_ASK"
        assert second["action"] == "SKIP_BUDGET_EXHAUSTED"
        assert second["audit"]["original_chain_reject_reason"] == "low_oi_0"
        assert second["audit"]["budget_exhausted"] is True
        assert ctx.direct_quote_unattempted_count == 1
        assert ctx.direct_quote_unattempted_symbols == ["SPY260724C00451000"]

    def test_direct_quote_safety_recheck_rejects_wide_spread_and_inverted_quotes(self):
        selector = APContractSelectionEngine(MagicMock(), max_spread_pct=0.20, min_oi=1, min_volume=0)
        today = date(2026, 7, 21)
        wide = _option(1)
        wide.update({"bid": 1.0, "ask": 2.0})
        inverted = _option(2)
        inverted.update({"bid": 2.0, "ask": 1.0})

        assert selector._quality_filter(wide, today) == "spread_too_wide_66.7%"
        assert selector._quality_filter(inverted, today) == "ask_below_bid"

    def test_direct_fetch_records_attempt_once_before_provider_call(self):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 1.1, "ask": 1.2}
        ctx = _ctx(2)

        with patch("ap.tradier_market_data_throttle.before_market_data_call", return_value={"wait_ms": 0.0}), \
             patch("ap.tradier_market_data_throttle.after_market_data_call"):
            result = fetch_direct_option_quote_with_meta(
                broker,
                "SPY260724C00450000",
                request_context=ctx,
            )

        assert result["ok"] is True
        assert ctx.direct_quote_attempted_symbols == ["SPY260724C00450000"]
        assert _selector_request_diagnostics(ctx)["direct_quote_budget"]["remaining"] == 1
