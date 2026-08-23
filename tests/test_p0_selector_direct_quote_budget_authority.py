from __future__ import annotations

import copy
import logging
import os
import runpy
import time
from datetime import date, timedelta
from pathlib import Path
from types import SimpleNamespace
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


def _next_weekday(target: date) -> date:
    while target.weekday() >= 5:  # Sat/Sun
        target += timedelta(days=1)
    return target


# Near-future expiry, recomputed each test session, so DTE-window checks in
# ap.contract_selector.select() (which compare against date.today()) stay
# valid regardless of when CI runs.
_NEAR_EXPIRY_DATE = _next_weekday(date.today() + timedelta(days=2))
_NEAR_EXPIRY = _NEAR_EXPIRY_DATE.isoformat()
_NEAR_EXPIRY_OCC = _NEAR_EXPIRY_DATE.strftime("%y%m%d")


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
    expiration: str = _NEAR_EXPIRY,
    delta: float | None = 0.40,
    oi: int | None = 100,
    volume: int | None = 10,
    bid_size: int | None = 20,
    ask_size: int | None = 20,
) -> dict:
    cp = "C" if direction == "CALL" else "P"
    occ = f"SPY{_NEAR_EXPIRY_OCC}{cp}{int(strike * 1000):08d}"
    opt = {
        "symbol": occ,
        "expiration_date": expiration,
        "strike": strike,
        "option_type": direction.lower(),
        "bid": 0.0,
        "ask": 0.0,
        "open_interest": oi,
        "volume": volume,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "_provider_index": idx,
    }
    if delta is not None:
        opt["greeks"] = {"delta": delta if direction == "CALL" else -abs(delta)}
    return opt


def _response(payload: dict, status_code: int = 200):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload
    return resp


def _plan(direction: str = "CALL", *, execution_mode: str = "LIVE", budget: float = 2000.0) -> dict:
    return {
        "signal_id": "sig-budget-389",
        "client_id": "client-budget-389",
        "execution_mode": execution_mode,
        "ticker": "SPY",
        "side": direction,
        "target_underlying": 450.0,
        "wick_targets": [{"distance_pct": 0.5, "confidence": 0.75}],
        "trigger_price": 450.0,
        "tier": "A",
        "score": 85.0,
        "pattern": "3-1-2",
        "timeframe": "5m",
        "metadata": {
            "sizing_context": {
                "budget": budget,
                "account_equity": 10000.0,
                "risk_pct": 0.05,
                "max_affordable_premium": budget,
            }
        },
        "max_position_usd": budget,
    }


class _DirectQuoteBroker:
    base_url = "https://api.tradier.com"
    cfg = SimpleNamespace(base_url="https://api.tradier.com", access_token="token")

    def __init__(
        self,
        chain: list[dict],
        valid_symbol: str,
        valid_quote: dict | None = None,
        *,
        underlying_price: float = 450.0,
    ):
        self.chain = chain
        self.valid_symbol = valid_symbol
        self.underlying_price = float(underlying_price)
        self.valid_quote = valid_quote or {
            "bid": 1.10,
            "ask": 1.14,
            "volume": 300,
            "open_interest": 1200,
        }
        self.get_quote = MagicMock(side_effect=self._get_quote)
        self.submit_order = MagicMock()
        self.cancel_order = MagicMock()
        self.session = MagicMock()
        self.session.get.side_effect = self._session_get

    def _session_get(self, url, *, params=None, headers=None, timeout=None):
        params = params or {}
        if "options/expirations" in url:
            return _response({"expirations": {"date": [_NEAR_EXPIRY]}})
        if "options/chains" in url:
            return _response({"options": {"option": self.chain}})
        return _response({"quotes": {"quote": {"last": self.underlying_price}}})

    def _get_quote(self, symbol: str):
        if "".join(str(symbol).upper().split()) == self.valid_symbol:
            return dict(self.valid_quote)
        return {"bid": 0.0, "ask": 0.0}


def _production_shaped_chain(direction: str) -> tuple[list[dict], str]:
    chain = []
    expiry = _NEAR_EXPIRY
    for idx in range(130):
        strike = 600 + idx if direction == "CALL" else 300 - idx
        chain.append(
            _option(
                idx,
                direction=direction,
                strike=float(strike),
                expiration=expiry,
                delta=None,
                oi=0,
                volume=0,
            )
        )
    priority_strikes = (
        [451.0, 452.0, 453.0, 454.0, 455.0, 456.0, 457.0, 458.0, 459.0]
        if direction == "CALL"
        else [449.0, 448.0, 447.0, 446.0, 445.0, 444.0, 443.0, 442.0, 441.0]
    )
    for rank_idx, strike in enumerate(priority_strikes[:8]):
        chain[rank_idx] = _option(
            rank_idx,
            direction=direction,
            strike=strike,
            expiration=expiry,
            delta=0.40,
            oi=1200,
            volume=300,
        )
    chain[100] = _option(
        100,
        direction=direction,
        strike=priority_strikes[8],
        expiration=expiry,
        delta=0.41,
        oi=1200,
        volume=300,
    )
    return chain, chain[100]["symbol"]


def _select_with_chain(monkeypatch, chain: list[dict], valid_symbol: str, direction: str, *, quote=None, plan=None):
    monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "20")
    monkeypatch.setenv("DIRECT_QUOTE_RECOVERY_TOP_N", "8")
    monkeypatch.setenv("CONTRACT_REVALIDATE_TOP_N", "20")
    monkeypatch.setenv("PRO_CONTRACT_QUALITY", "true")
    monkeypatch.setattr("ap.contract_quote_revalidator.is_market_open", lambda *args, **kwargs: True)
    monkeypatch.setattr(APContractSelectionEngine, "_emit_selector_event", lambda *args, **kwargs: None)
    broker = _DirectQuoteBroker(chain, valid_symbol, valid_quote=quote)
    selector = APContractSelectionEngine(
        broker,
        mode=(plan or {}).get("execution_mode", "LIVE"),
        data_broker=broker,
        min_premium=1.0,
        max_premium=1000.0,
        min_oi=1,
        min_volume=0,
    )
    plan = plan or _plan(direction)
    return selector.select(plan), selector, broker, plan


class TestConfigurationAuthority:
    def test_malformed_contract_revalidate_alias_does_not_crash_module_load(
        self,
        monkeypatch,
        caplog,
    ):
        monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "20")
        monkeypatch.setenv("CONTRACT_REVALIDATE_TOP_N", "abc")
        caplog.set_level(logging.WARNING)

        module_globals = runpy.run_path(
            str(Path(__file__).resolve().parents[1] / "ap" / "contract_quote_revalidator.py")
        )

        assert module_globals["DEFAULT_REVALIDATE_TOP_N"] == 5
        assert "DIRECT_QUOTE_ENV_PARSE_ERROR key=CONTRACT_REVALIDATE_TOP_N" in caplog.text

    def test_canonical_conflict_does_not_reduce_limit(self, caplog):
        import ap.contract_selector as selector_module

        selector_module._DIRECT_QUOTE_BUDGET_CONFLICTS_LOGGED.clear()
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
            ({"DIRECT_QUOTE_RECOVERY_TOP_N": "8"}, 40, "default", False),
            ({"CONTRACT_REVALIDATE_TOP_N": "20"}, 40, "default", False),
            ({}, 40, "default", False),
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

        assert cfg.effective_limit == 40
        assert cfg.source == "default"
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
            {"symbol": f"SPY{_NEAR_EXPIRY_OCC}C00450000", "bid": 0, "ask": 0},
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )
        second = revalidate_with_direct_quote(
            broker,
            {"symbol": f"SPY   {_NEAR_EXPIRY_OCC}C00450000", "bid": 0, "ask": 0},
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )

        assert first["action"] == "PASS"
        assert second["action"] == "SKIP_ALREADY_REVALIDATED"
        assert broker.get_quote.call_count == 1
        assert ctx.provider_call_counts["direct_quote_calls"] == 1
        assert ctx.direct_quote_attempted_symbols == [f"SPY{_NEAR_EXPIRY_OCC}C00450000"]


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

    def test_duplicate_occ_rows_stay_visible_to_quality_resolution(self):
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

        # Duplicate rows are not discarded before the quality loop. The
        # canonical OCC revalidation set, not this ranking helper, owns the
        # one-call direct-quote cap.
        assert [row["_provider_index"] for row in ordered] == [0, 1]
        assert ctx.direct_quote_duplicate_symbols == [chain[0]["symbol"]]

    def test_interleaved_duplicate_occ_rows_are_grouped_by_financial_quality(self):
        """Duplicate resolution must not depend on rows being adjacent."""
        stale = _option(0, oi=1000, volume=1000)
        usable = _option(1, oi=100, volume=100)
        usable["symbol"] = f" {stale['symbol'].lower()} "
        usable["bid"] = 1.10
        usable["ask"] = 1.11
        interleaver = _option(2, oi=500, volume=500)
        interleaver["symbol"] = interleaver["symbol"].replace("SPY", "QQQ", 1)
        expected_duplicate = stale["symbol"]
        observed = []

        for chain in ([stale, interleaver, usable], [usable, interleaver, stale]):
            ctx = _ctx(20)
            ordered = _order_chain_for_direct_quote_recovery(
                list(chain),
                direction="CALL",
                underlying_price=450.0,
                target_delta=0.40,
                today=date(2026, 7, 21),
                request_context=ctx,
            )
            observed.append([row["symbol"] for row in ordered])
            assert ctx.direct_quote_duplicate_symbols == [expected_duplicate]

        assert observed[0] == observed[1]
        assert observed[0] == [usable["symbol"], stale["symbol"], interleaver["symbol"]]

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
        assert ctx.direct_quote_unattempted_symbols == [f"SPY{_NEAR_EXPIRY_OCC}C00451000"]

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
                f"SPY{_NEAR_EXPIRY_OCC}C00450000",
                request_context=ctx,
            )

        assert result["ok"] is True
        assert ctx.direct_quote_attempted_symbols == [f"SPY{_NEAR_EXPIRY_OCC}C00450000"]
        assert _selector_request_diagnostics(ctx)["direct_quote_budget"]["remaining"] == 1

    def test_duplicate_unattempted_symbols_count_once(self):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 0.0, "ask": 0.0}
        ctx = _ctx(1)

        revalidate_with_direct_quote(
            broker,
            {"symbol": f"SPY{_NEAR_EXPIRY_OCC}C00450000", "bid": 0.0, "ask": 0.0},
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )
        first_skip = revalidate_with_direct_quote(
            broker,
            {"symbol": f"SPY{_NEAR_EXPIRY_OCC}C00451000", "bid": 0.0, "ask": 0.0},
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )
        second_skip = revalidate_with_direct_quote(
            broker,
            {"symbol": f"SPY   {_NEAR_EXPIRY_OCC}C00451000", "bid": 0.0, "ask": 0.0},
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )

        assert first_skip["action"] == "SKIP_BUDGET_EXHAUSTED"
        assert second_skip["action"] == "SKIP_BUDGET_EXHAUSTED"
        assert ctx.direct_quote_unattempted_count == 1
        assert ctx.direct_quote_unattempted_symbols == [f"SPY{_NEAR_EXPIRY_OCC}C00451000"]


class TestSelectorIntegration:
    @pytest.mark.parametrize("direction", ["CALL", "PUT"])
    def test_130_row_selector_replay_selects_recovered_ninth_call_candidate(self, monkeypatch, direction):
        chain, expected_symbol = _production_shaped_chain(direction)
        original_client = "client-budget-389"
        original_mode = "LIVE"
        original_signal = "sig-budget-389"

        selected, selector, broker, plan = _select_with_chain(
            monkeypatch,
            chain,
            expected_symbol,
            direction,
        )

        assert selected is not None
        assert selected.contract_symbol == expected_symbol
        assert plan["client_id"] == original_client
        assert plan["execution_mode"] == original_mode
        assert plan["signal_id"] == original_signal
        assert broker.get_quote.call_count <= 20
        assert broker.get_quote.call_count > 8
        assert broker.submit_order.call_count == 0
        assert broker.cancel_order.call_count == 0
        diagnostics = plan["metadata"]["selector_request_diagnostics"]
        assert diagnostics["direct_quote_budget"]["effective_limit"] == 20
        assert diagnostics["direct_quote_budget"]["source"] == "ordinary_pre_pr_envelope"
        assert diagnostics["direct_quote_budget"]["conflict"] is True
        assert diagnostics["direct_quote_budget"]["direct_recovery_alias"] == 8
        assert diagnostics["direct_quote_budget"]["used"] == broker.get_quote.call_count
        assert diagnostics["direct_quote_budget"]["remaining"] == 20 - broker.get_quote.call_count
        assert diagnostics["direct_quote_candidate_ranking"][8]["symbol"] == expected_symbol

    def test_no_survivor_selector_replay_terminates_budget_exhausted_but_keeps_row_reasons(self, monkeypatch):
        # ORDINARY selector requests MUST NOT adopt recovery-only final reasons
        # (Blocker 2 scopes resolve_selector_recovery_final_reason strictly to
        # deferred-breach contexts). The truthful ordinary terminal reason for
        # a no-survivor replay is the aggregated top-quality rejection
        # (CHAIN_ROW_ZERO_BID_ASK), not the deferred-recovery
        # SELECTOR_REQUEST_BUDGET_EXHAUSTED that previously leaked from the
        # recovery resolver.
        chain, _ = _production_shaped_chain("CALL")

        selected, selector, broker, plan = _select_with_chain(
            monkeypatch,
            chain,
            f"SPY{_NEAR_EXPIRY_OCC}C99999999",
            "CALL",
            quote={"bid": 0.0, "ask": 0.0},
        )

        assert selected is None
        assert broker.get_quote.call_count == 20
        failure = plan["metadata"]["selector_failure"]
        diagnostics = failure["selection_diagnostics"]
        assert failure["reason_code"] == "CHAIN_ROW_ZERO_BID_ASK"
        # The budget diagnostics remain preserved even though the final reason
        # is the truthful ordinary quality rejection.
        assert diagnostics["direct_quote_unattempted_count"] > 0
        assert diagnostics["direct_quote_budget"]["used"] == 20
        assert diagnostics["direct_quote_budget"]["remaining"] == 0
        assert "CHAIN_ROW_ZERO_BID_ASK" in failure["top_reject_buckets"]
        assert "SELECTOR_REQUEST_BUDGET_EXHAUSTED" not in failure["top_reject_buckets"]

    @pytest.mark.parametrize(
        ("name", "chain_overrides", "quote", "plan_overrides", "expected_reason"),
        [
            (
                "wide_spread",
                {},
                {"bid": 1.00, "ask": 1.20, "volume": 300, "open_interest": 1200},
                {},
                "SPREAD_TOO_WIDE",
            ),
            (
                "inverted_bid_ask",
                {},
                {"bid": 1.20, "ask": 1.00, "volume": 300, "open_interest": 1200},
                {},
                "DIRECT_QUOTE_ZERO_BID_ASK",
            ),
            (
                "premium_above_live_budget",
                {},
                {"bid": 1.10, "ask": 1.14, "volume": 300, "open_interest": 1200},
                {"budget": 50.0},
                "UNTRADEABLE_FOR_ACCOUNT_SIZE",
            ),
            (
                "live_ask_affordability",
                {},
                {"bid": 2.00, "ask": 2.05, "volume": 300, "open_interest": 1200},
                {"budget": 200.0},
                "UNTRADEABLE_FOR_ACCOUNT_SIZE",
            ),
            (
                "delta_out_of_range",
                {"delta": 0.01},
                {"bid": 1.10, "ask": 1.14, "volume": 300, "open_interest": 1200},
                {},
                "DELTA_OUT_OF_RANGE",
            ),
            (
                "moneyness_out_of_range",
                {"strike": 560.0, "delta": 0.40},
                {"bid": 1.10, "ask": 1.14, "volume": 300, "open_interest": 1200},
                {},
                "MONEYNESS_OUT_OF_RANGE",
            ),
            (
                "invalid_dte",
                {"expiration": "not-a-date"},
                {"bid": 1.10, "ask": 1.14, "volume": 300, "open_interest": 1200},
                {},
                "DTE_OUT_OF_RANGE",
            ),
            (
                "insufficient_liquidity_after_patch",
                {"oi": 0, "volume": 0},
                {"bid": 1.10, "ask": 1.14},
                {},
                "OI_TOO_LOW",
            ),
        ],
    )
    def test_recovered_direct_quote_rechecks_full_selector_gates(
        self,
        monkeypatch,
        name,
        chain_overrides,
        quote,
        plan_overrides,
        expected_reason,
    ):
        expiration = chain_overrides.pop("expiration", _NEAR_EXPIRY)
        opt = _option(
            0,
            direction="CALL",
            expiration=expiration,
            strike=chain_overrides.pop("strike", 451.0),
            delta=chain_overrides.pop("delta", 0.40),
            oi=chain_overrides.pop("oi", 1200),
            volume=chain_overrides.pop("volume", 300),
        )
        opt.update(chain_overrides)
        plan = _plan("CALL", budget=plan_overrides.get("budget", 2000.0))

        selected, selector, broker, plan = _select_with_chain(
            monkeypatch,
            [opt],
            opt["symbol"],
            "CALL",
            quote=quote,
            plan=plan,
        )

        assert selected is None, name
        # This helper intentionally exercises the ordinary selector path.
        # Recovery-only structural skips must not suppress its direct quote.
        assert broker.get_quote.call_count == 1
        failure = plan["metadata"]["selector_failure"]
        assert failure["reason_code"] == expected_reason
        assert broker.submit_order.call_count == 0
        assert broker.cancel_order.call_count == 0

    def test_valid_recovered_quote_passes_live_and_paper_pricing_paths(self, monkeypatch):
        for execution_mode, expected_basis in (("LIVE", "ASK_EXECUTION"), ("PAPER", "MID_SIMULATION")):
            chain, expected_symbol = _production_shaped_chain("CALL")
            selected, selector, broker, plan = _select_with_chain(
                monkeypatch,
                chain,
                expected_symbol,
                "CALL",
                plan=_plan("CALL", execution_mode=execution_mode),
            )

            assert selected is not None
            assert selected.contract_symbol == expected_symbol
            assert selected.pricing_basis == expected_basis
            assert plan["execution_mode"] == execution_mode


class TestAggregateAuditTruthfulness:
    """Real integration regression for the request-level audit aggregate.

    ``_direct_quote_recovery_audit`` is a REQUEST-level aggregate. A later
    budget-skipped candidate must never overwrite ``attempted``,
    ``selected``, or the recorded contract stamped by an earlier
    successful direct quote. The truthful summary flags are:

      * ``attempted`` — True when any provider call happened in the request
      * ``selected``  — True when any direct quote produced a survivor
      * ``budget_skipped`` — True when any later candidate was unattempted
    """

    def _make_chain(self, direction: str, first_symbol: str, second_symbol: str):
        """Two candidates. First fails a revalidatable chain reason and
        is patched by a successful direct quote. Second also needs
        direct-quote recovery but hits the budget guard first."""
        return [
            _option(
                0,
                direction=direction,
                strike=451.0,
                delta=0.40,
                oi=1200,
                volume=300,
            ) | {"symbol": first_symbol, "bid": 0.0, "ask": 0.0},
            _option(
                1,
                direction=direction,
                strike=452.0,
                delta=0.40,
                oi=1200,
                volume=300,
            ) | {"symbol": second_symbol, "bid": 0.0, "ask": 0.0},
        ]

    def test_budget_skip_after_survivor_preserves_attempted_and_selected(
        self, monkeypatch
    ):
        # Budget of one allows exactly one direct quote. First candidate
        # gets it and survives; second candidate must be reported as
        # budget-skipped without erasing the first's survivor status.
        first_symbol = f"SPY{_NEAR_EXPIRY_OCC}C00451000"
        second_symbol = f"SPY{_NEAR_EXPIRY_OCC}C00452000"
        chain = self._make_chain("CALL", first_symbol, second_symbol)

        monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "1")
        monkeypatch.setenv("DIRECT_QUOTE_RECOVERY_TOP_N", "8")
        monkeypatch.setenv("CONTRACT_REVALIDATE_TOP_N", "20")
        monkeypatch.setenv("PRO_CONTRACT_QUALITY", "true")
        monkeypatch.setattr(
            "ap.contract_quote_revalidator.is_market_open",
            lambda *a, **kw: True,
        )
        monkeypatch.setattr(
            APContractSelectionEngine,
            "_emit_selector_event",
            lambda *a, **kw: None,
        )
        broker = _DirectQuoteBroker(chain, first_symbol)
        selector = APContractSelectionEngine(
            broker,
            mode="LIVE",
            data_broker=broker,
            min_premium=1.0,
            max_premium=1000.0,
            min_oi=1,
            min_volume=0,
        )
        plan = _plan("CALL")
        from ap.contract_selector import SELECTOR_REQUEST_KIND_DEFERRED_BREACH
        selected = selector.select(
            plan,
            request_context=_new_selector_request_context(
                "SPY",
                "live",
                selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
            ),
        )

        assert selected is not None
        assert selected.contract_symbol == first_symbol

        ctx = plan["metadata"].get("selector_request_context") or plan["metadata"].get(
            "selector_diagnostics"
        )
        # The plan diagnostics carry the final aggregate audit.
        audit = plan["metadata"].get("selector_diagnostics", {}).get(
            "direct_quote_recovery_audit"
        )
        # Truthful semantics: attempted stayed True from the survivor,
        # selected stayed True, budget_skipped is now True because a
        # later candidate hit the budget guard.
        assert audit is not None
        assert audit["attempted"] is True
        assert audit["selected"] is True
        assert audit["contract"] == first_symbol
        assert audit.get("budget_skipped") is True
        assert audit.get("budget_skip_reason") == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"

        diagnostics = plan["metadata"]["selector_diagnostics"]
        assert diagnostics["direct_quote_budget"]["used"] == 1
        # Second candidate was never quoted — record it as unattempted.
        assert second_symbol in diagnostics["direct_quote_unattempted_symbols"]


# ─────────────────────────────────────────────────────────────────────────────
# July 23 fleet acceptance replay — 8 tickers × 3 identities = 24 canonical
# selector requests. Every request must:
#
#   * receive an independent fresh selector budget;
#   * preserve incident-shaped candidate rank and attempt/skip order;
#   * recover the declared survivor or exhaust on zero/stale candidates;
#   * report SELECTOR_REQUEST_BUDGET_EXHAUSTED only for all-failing fixtures;
#   * NOT submit / cancel / replace any broker order;
#   * persist the dedicated durable outcome RETRY_LATER_SELECTOR_BUDGET on
#     the deferred-retry row with exact identity fields intact.
#
# Identity fixtures mirror the three that exist in repository configuration
# and runtime. They are never invented on the fly and never collapsed into
# a generic client.
# ─────────────────────────────────────────────────────────────────────────────


FLEET_INCIDENT_FIXTURES: dict[str, dict] = {
    "ABT": {
        "underlying": 124.80, "direction": "CALL", "spacing": 0.5,
        "recovery_rank": 2, "delta": 0.42, "oi": 1850, "volume": 420,
    },
    "COF": {
        "underlying": 214.35, "direction": "PUT", "spacing": 2.5,
        "recovery_rank": None, "delta": 0.39, "oi": 640, "volume": 85,
    },
    "GM": {
        "underlying": 57.60, "direction": "CALL", "spacing": 0.25,
        "recovery_rank": 6, "delta": 0.44, "oi": 5200, "volume": 1600,
    },
    "CAT": {
        "underlying": 418.20, "direction": "PUT", "spacing": 2.5,
        "recovery_rank": 4, "delta": 0.38, "oi": 910, "volume": 210,
    },
    "KHC": {
        "underlying": 29.15, "direction": "CALL", "spacing": 0.5,
        "recovery_rank": None, "delta": 0.41, "oi": 7300, "volume": 2500,
    },
    "ROST": {
        "underlying": 154.70, "direction": "CALL", "spacing": 0.5,
        "recovery_rank": 7, "delta": 0.43, "oi": 780, "volume": 145,
    },
    "UPS": {
        "underlying": 111.85, "direction": "PUT", "spacing": 1.0,
        "recovery_rank": None, "delta": 0.37, "oi": 2400, "volume": 610,
    },
    "BAC": {
        "underlying": 60.90, "direction": "PUT", "spacing": 0.25,
        "recovery_rank": 8, "delta": 0.40, "oi": 9800, "volume": 4100,
    },
}
FLEET_TICKERS: list[str] = list(FLEET_INCIDENT_FIXTURES)

# Exact production client identities as configured in ap/morning_jobs.py
# (DEFAULT_LIVE_CLIENT / DEFAULT_PAPER_CLIENTS). Do not substitute aliases.
FLEET_IDENTITIES: list[tuple[str, str, str]] = [
    ("jasoncosby1@gmail.com",       "live",  "Jason LIVE"),
    ("jose.vasquez4011@gmail.com",  "paper", "Jose PAPER"),
    ("tradefluencehq@gmail.com",    "paper", "Tradefluence PAPER"),
]


def _ticker_option(idx: int, ticker: str, direction: str, strike: float,
                   *, delta: float | None, oi: int | None, volume: int | None) -> dict:
    """Produce a chain option row keyed to ``ticker``. Same shape as
    _option() but the OCC symbol embeds the ticker rather than SPY."""
    cp = "C" if direction == "CALL" else "P"
    occ = f"{ticker}{_NEAR_EXPIRY_OCC}{cp}{int(strike * 1000):08d}"
    opt = {
        "symbol": occ,
        "expiration_date": _NEAR_EXPIRY,
        "strike": float(strike),
        "option_type": direction.lower(),
        "bid": 0.0,
        "ask": 0.0,
        "open_interest": oi,
        "volume": volume,
        "bid_size": 20,
        "ask_size": 20,
        "_provider_index": idx,
    }
    if delta is not None:
        opt["greeks"] = {"delta": delta if direction == "CALL" else -abs(delta)}
    return opt


def _fleet_chain(ticker: str, fixture: dict) -> tuple[list[dict], list[str]]:
    """Build one incident-shaped chain using that ticker's own price scale,
    strike spacing, direction, delta, OI, volume, and provider ordering."""
    direction = fixture["direction"]
    underlying = float(fixture["underlying"])
    spacing = float(fixture["spacing"])
    chain: list[dict] = []
    # Far rows model the large stale/zero provider tail seen in production.
    for idx in range(48):
        distance = (30 + idx) * spacing
        strike = underlying + distance if direction == "CALL" else underlying - distance
        chain.append(_ticker_option(
            idx, ticker, direction, float(strike),
            delta=None, oi=0, volume=0,
        ))
    expected_ranked_symbols: list[str] = []
    # Put priority candidates at deliberately non-ranked provider indexes.
    provider_indexes = [
        31, 4, 27, 9, 42, 1, 35, 13, 46, 18,
        23, 7, 39, 15, 44, 21, 33, 11, 47, 25,
    ]
    for rank, provider_idx in enumerate(provider_indexes, start=1):
        strike = (
            underlying + rank * spacing
            if direction == "CALL"
            else underlying - rank * spacing
        )
        opt = _ticker_option(
            provider_idx,
            ticker,
            direction,
            strike,
            delta=float(fixture["delta"]) + ((rank % 3) - 1) * 0.01,
            oi=int(fixture["oi"]) + rank * 17,
            volume=int(fixture["volume"]) + rank * 7,
        )
        chain[provider_idx] = opt
        expected_ranked_symbols.append(opt["symbol"])
    return chain, expected_ranked_symbols


def _fleet_plan(*, ticker: str, direction: str, client_id: str,
                execution_mode: str, signal_id: str, local_order_id: str,
                underlying: float,
                budget: float = 2000.0) -> dict:
    return {
        "signal_id": signal_id,
        "client_id": client_id,
        "execution_mode": execution_mode,
        "local_order_id": local_order_id,
        "ticker": ticker,
        "side": direction,
        "target_underlying": underlying,
        "wick_targets": [{"distance_pct": 0.5, "confidence": 0.75}],
        "trigger_price": underlying,
        "tier": "A",
        "score": 85.0,
        "pattern": "3-1-2",
        "timeframe": "5m",
        "metadata": {
            "sizing_context": {
                "budget": budget,
                "account_equity": 10000.0,
                "risk_pct": 0.05,
                "max_affordable_premium": budget,
            }
        },
        "max_position_usd": budget,
    }


class TestJuly23FleetAcceptanceReplay:
    """PR #389 amendment 2 — 24-request fleet replay covering the
    July 23 acceptance surface. See module-level comment for contract."""

    def _run_one(self, monkeypatch, ticker: str, client_id: str,
                 execution_mode: str, generation: int,
                 attempt: int) -> tuple:
        # Each production identity owns a canonical selector request. Avoid
        # process-global quote-cache carryover between replay identities.
        clear_quote_cache()
        fixture = FLEET_INCIDENT_FIXTURES[ticker]
        direction = fixture["direction"]
        chain, ranked_symbols = _fleet_chain(ticker, fixture)
        recovery_rank = fixture["recovery_rank"]
        valid_symbol = (
            ranked_symbols[int(recovery_rank) - 1]
            if recovery_rank is not None
            else "NEVERMATCH"
        )
        signal_id = f"sig-{ticker}-{client_id}-g{generation}"
        local_order_id = f"oid-{ticker}-{client_id}-g{generation}-a{attempt}"
        plan = _fleet_plan(
            ticker=ticker,
            direction=direction,
            client_id=client_id,
            execution_mode=execution_mode,
            signal_id=signal_id,
            local_order_id=local_order_id,
            underlying=float(fixture["underlying"]),
        )
        monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "8")
        monkeypatch.setenv("DIRECT_QUOTE_RECOVERY_TOP_N", "8")
        monkeypatch.setenv("CONTRACT_REVALIDATE_TOP_N", "8")
        monkeypatch.setenv("PRO_CONTRACT_QUALITY", "true")
        monkeypatch.setattr(
            "ap.contract_quote_revalidator.is_market_open",
            lambda *a, **kw: True,
        )
        monkeypatch.setattr(
            APContractSelectionEngine,
            "_emit_selector_event",
            lambda *a, **kw: None,
        )
        broker = _DirectQuoteBroker(
            chain,
            valid_symbol,
            valid_quote={
                "bid": 1.10,
                "ask": 1.14,
                "volume": int(fixture["volume"]),
                "open_interest": int(fixture["oi"]),
            },
            underlying_price=float(fixture["underlying"]),
        )
        selector = APContractSelectionEngine(
            broker,
            mode=execution_mode.upper(),
            data_broker=broker,
            min_premium=1.0,
            max_premium=1000.0,
            min_oi=1,
            min_volume=0,
        )
        from ap.contract_selector import SELECTOR_REQUEST_KIND_DEFERRED_BREACH
        selected = selector.select(
            plan,
            request_context=_new_selector_request_context(
                ticker,
                execution_mode,
                selector_request_kind=SELECTOR_REQUEST_KIND_DEFERRED_BREACH,
                recovery_attempt_number=attempt,
            ),
        )
        return selected, broker, plan, fixture, ranked_symbols

    def test_24_requests_each_exhaust_independent_selector_budget(
        self, monkeypatch,
    ):
        """8 tickers × 3 real production identities = 24 canonical
        selector requests. Each request receives its own fresh 8-call
        budget and follows an explicit incident fixture: recoverable cases
        select the declared rank; all-failing cases exhaust with
        SELECTOR_REQUEST_BUDGET_EXHAUSTED. Original quality reasons remain
        attached and the selection pass never touches broker orders.

        Durable persistence of the resulting RETRY_LATER_SELECTOR_BUDGET
        row is exercised by
        test_p0_deferred_due_retry_ownership.py's OSM-seam tests —
        keeping this matrix strictly about selector-budget behavior."""
        replay_receipts: list[dict] = []
        for ticker in FLEET_TICKERS:
            for client_id, execution_mode, label in FLEET_IDENTITIES:
                selected, broker, plan, fixture, ranked_symbols = self._run_one(
                    monkeypatch,
                    ticker=ticker,
                    client_id=client_id,
                    execution_mode=execution_mode,
                    generation=1,
                    attempt=1,
                )
                recovery_rank = fixture["recovery_rank"]
                if recovery_rank is None:
                    assert selected is None, f"{ticker} {label} unexpected selection"
                else:
                    assert selected is not None, f"{ticker} {label} missed recovery"
                    assert selected.contract_symbol == ranked_symbols[recovery_rank - 1]
                assert broker.submit_order.call_count == 0, (
                    f"{ticker} {label} unexpected submit"
                )
                assert broker.cancel_order.call_count == 0, (
                    f"{ticker} {label} unexpected cancel"
                )
                if selected is None:
                    failure = plan["metadata"]["selector_failure"]
                    diagnostics = failure["selection_diagnostics"]
                    assert failure["reason_code"] in {
                        "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                        "CHAIN_ROW_ZERO_BID_ASK",
                        "DIRECT_QUOTE_ZERO_BID_ASK",
                        "MONEYNESS_OUT_OF_RANGE",
                        "DELTA_OUT_OF_RANGE",
                        "DTE_OUT_OF_RANGE",
                        "NO_AFFORDABLE_CONTRACT",
                        "PREMIUM_CAP_EXCEEDED",
                    }
                    assert "CHAIN_ROW_ZERO_BID_ASK" in failure["top_reject_buckets"]
                    assert (
                        "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
                        not in failure["top_reject_buckets"]
                    )
                else:
                    diagnostics = plan["metadata"]["selector_request_diagnostics"]
                # Independent fresh budget: this request can use at most eight
                # calls; structural skips consume none and nothing is inherited
                # from a sibling identity or earlier ticker.
                used = diagnostics["direct_quote_budget"]["used"]
                assert 0 <= used <= 8
                assert diagnostics["direct_quote_budget"]["remaining"] == 8 - used
                ranking = diagnostics["direct_quote_candidate_ranking"]
                assert [r["symbol"] for r in ranking[:20]] == ranked_symbols
                assert [r["rank"] for r in ranking[:20]] == list(range(1, 21))
                expected_skipped = ranked_symbols[8:12]
                if selected is not None:
                    assert any(
                        call.args[0] == ranked_symbols[recovery_rank - 1]
                        for call in broker.get_quote.call_args_list
                    )
                assert len(diagnostics["direct_quote_attempted_symbols"]) == used
                assert len(set(diagnostics["direct_quote_attempted_symbols"])) == used
                if used == 8:
                    assert set(diagnostics["direct_quote_unattempted_symbols"][:4]).issubset(
                        set(expected_skipped)
                    )
                replay_receipts.append({
                    "ticker": ticker,
                    "client_id": plan["client_id"],
                    "execution_mode": plan["execution_mode"],
                    "budget_used": diagnostics["direct_quote_budget"]["used"],
                    "recovery_rank": recovery_rank,
                    "signal_id": plan["signal_id"],
                    "local_order_id": plan["local_order_id"],
                })

        # 8 tickers × 3 identities → exactly 24 canonical requests.
        assert len(replay_receipts) == 24
        # Each (ticker, client_id, execution_mode) triple is unique — no
        # collapse across identities.
        triples = {(r["ticker"], r["client_id"], r["execution_mode"])
                   for r in replay_receipts}
        assert len(triples) == 24
        # LIVE and PAPER identities remain distinct.
        modes = {r["execution_mode"] for r in replay_receipts}
        assert modes == {"live", "paper"}
        # The three real production emails are all represented.
        assert {r["client_id"] for r in replay_receipts} == {
            "jasoncosby1@gmail.com",
            "jose.vasquez4011@gmail.com",
            "tradefluencehq@gmail.com",
        }

    def test_live_and_paper_identities_never_share_budget(self, monkeypatch):
        """Consecutive LIVE and PAPER requests on the same ticker must not
        share a selector budget. The second request's ``used`` is the
        exhaustion count for its own scope, not a residual from the
        first."""
        _live_sel, live_broker, live_plan, _fixture, _ranked = self._run_one(
            monkeypatch,
            ticker="COF",
            client_id="jasoncosby1@gmail.com",
            execution_mode="live",
            generation=1,
            attempt=1,
        )
        _paper_sel, paper_broker, paper_plan, _fixture, _ranked = self._run_one(
            monkeypatch,
            ticker="COF",
            client_id="jose.vasquez4011@gmail.com",
            execution_mode="paper",
            generation=1,
            attempt=1,
        )
        live_diag = live_plan["metadata"]["selector_failure"][
            "selection_diagnostics"]
        paper_diag = paper_plan["metadata"]["selector_failure"][
            "selection_diagnostics"]
        # Both requests exhaust their own budgets independently.
        assert live_diag["direct_quote_budget"]["used"] == 8
        assert paper_diag["direct_quote_budget"]["used"] == 8
        # No cross-mode broker interaction leaked.
        assert live_broker.submit_order.call_count == 0
        assert paper_broker.submit_order.call_count == 0
        assert live_broker.cancel_order.call_count == 0
        assert paper_broker.cancel_order.call_count == 0
