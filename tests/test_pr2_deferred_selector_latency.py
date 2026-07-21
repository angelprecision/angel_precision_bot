from __future__ import annotations

import os
import threading
import time
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap.contract_quote_revalidator import clear_quote_cache, fetch_direct_option_quote_with_meta, revalidate_with_direct_quote
import ap.contract_selector as selector_module
from ap.contract_selector import (
    APContractSelectionEngine,
    ChainProviderError,
    SelectorRequestBudgetExhausted,
    SelectorRequestContext,
    _new_selector_request_context,
)


def _response(*, status_code=200, payload=None):
    resp = MagicMock()
    resp.status_code = status_code
    resp.json.return_value = payload or {}
    return resp


def _option(symbol: str, expiration: str) -> dict:
    return {
        "symbol": symbol,
        "strike": 500.0,
        "bid": 1.2,
        "ask": 1.3,
        "last": 1.25,
        "volume": 100,
        "open_interest": 500,
        "option_type": "call",
        "expiration_date": expiration,
    }


def _make_selector(session=None):
    broker = MagicMock()
    broker.base_url = "https://sandbox.tradier.com"
    broker.access_token = "TOKEN"
    broker.session = session or MagicMock()
    sel = APContractSelectionEngine(broker, mode="paper", data_broker=broker)
    return sel, broker.session


@pytest.fixture(autouse=True)
def _clear_direct_quote_cache():
    clear_quote_cache()
    yield
    clear_quote_cache()


class TestDeferredSelectorLatency:
    def test_six_expiration_probes_reuse_underlying_and_expirations(self):
        sel, session = _make_selector()
        ctx = _new_selector_request_context("SPY")
        expirations = [f"2026-07-{day:02d}" for day in range(10, 16)]
        session.get.side_effect = [
            _response(payload={"quotes": {"quote": {"last": 450.0}}}),
            _response(payload={"expirations": {"date": expirations}}),
            *[
                _response(payload={"options": {"option": [_option(f"SPY{idx}", exp)]}})
                for idx, exp in enumerate(expirations, start=1)
            ],
        ]

        outputs = []
        for exp in expirations:
            chain, underlying = sel._fetch_tradier_chain(
                "SPY",
                "call",
                expiration_override=exp,
                request_context=ctx,
            )
            outputs.append((chain[0]["symbol"], underlying))

        assert ctx.provider_call_counts["underlying_quote_calls"] == 1
        assert ctx.provider_call_counts["expiration_calls"] == 1
        assert ctx.provider_call_counts["chain_calls"] == 6
        assert len(outputs) == 6
        assert all(price == 450.0 for _, price in outputs)

    def test_direct_quote_budget_is_transaction_scoped(self):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 0.0, "ask": 0.0}
        ctx = SelectorRequestContext(
            ticker="SPY",
            direct_quote_attempts_remaining=5,
            started_at_monotonic=time.monotonic(),
        )

        for idx in range(20):
            revalidate_with_direct_quote(
                broker,
                {"symbol": f"SPY260710C00{idx:05d}", "bid": 0.0, "ask": 0.0},
                "zero_bid_or_ask",
                market_open_override=True,
                request_context=ctx,
            )

        assert broker.get_quote.call_count == 5
        assert ctx.provider_call_counts["direct_quote_calls"] == 5
        assert ctx.direct_quote_attempts_remaining == 5

    def test_same_occ_is_not_revalidated_twice_in_one_request(self):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 1.1, "ask": 1.2}
        ctx = _new_selector_request_context("SPY")
        opt = {"symbol": "SPY260710C00450000", "bid": 0.0, "ask": 0.0}

        first = revalidate_with_direct_quote(
            broker,
            opt,
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )
        second = revalidate_with_direct_quote(
            broker,
            {**opt, "symbol": "SPY   260710C00450000"},
            "zero_bid_or_ask",
            market_open_override=True,
            request_context=ctx,
        )

        assert first["action"] == "PASS"
        assert second["action"] == "SKIP_ALREADY_REVALIDATED"
        assert broker.get_quote.call_count == 1

    def test_three_expirations_share_one_direct_quote_budget(self):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 0.0, "ask": 0.0}
        ctx = _new_selector_request_context("SPY", "paper")
        ctx.max_direct_quote_calls = 2
        ctx.effective_direct_quote_limit = 2

        results = []
        for expiration_index in range(3):
            results.append(revalidate_with_direct_quote(
                broker,
                {
                    "symbol": f"SPY26071{expiration_index}C0045000{expiration_index}",
                    "bid": 0.0,
                    "ask": 0.0,
                },
                "zero_bid_or_ask",
                market_open_override=True,
                request_context=ctx,
            ))

        assert broker.get_quote.call_count == 2
        assert ctx.provider_call_counts["direct_quote_calls"] == 2
        assert results[-1]["reason_code"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"

    def test_chain_call_ceiling_stops_provider_fan_out(self):
        sel, session = _make_selector()
        ctx = _new_selector_request_context("SPY", "paper")
        ctx.max_chain_calls = 2
        session.get.return_value = _response(payload={
            "options": {"option": [_option("SPY260710C00450000", "2026-07-10")]},
        })

        for _ in range(2):
            sel._fetch_chain_for_expiration(
                session,
                "https://sandbox.tradier.com",
                {},
                "SPY",
                "call",
                "2026-07-10",
                underlying_price=450.0,
                request_context=ctx,
            )
        with pytest.raises(SelectorRequestBudgetExhausted) as exc_info:
            sel._fetch_chain_for_expiration(
                session,
                "https://sandbox.tradier.com",
                {},
                "SPY",
                "call",
                "2026-07-10",
                underlying_price=450.0,
                request_context=ctx,
            )

        assert "chain_calls=2 limit=2" in str(exc_info.value)
        assert session.get.call_count == 2

    def test_total_elapsed_ceiling_stops_direct_quote_before_get(self):
        broker = MagicMock()
        ctx = _new_selector_request_context("SPY", "live")
        ctx.max_total_elapsed_ms = 1
        ctx.started_at_monotonic = time.monotonic() - 1.0

        result = fetch_direct_option_quote_with_meta(
            broker,
            "SPY260710C00450000",
            request_context=ctx,
        )

        assert result["reason_code"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        broker.get_quote.assert_not_called()
        assert ctx.budget_exhausted_stage == "direct_quote"

    def test_live_throttle_acquire_failure_causes_no_provider_get(self):
        broker = MagicMock()
        ctx = _new_selector_request_context("SPY", "live")

        with patch(
            "ap.tradier_market_data_throttle.before_market_data_call",
            side_effect=RuntimeError("throttle unavailable"),
        ):
            result = fetch_direct_option_quote_with_meta(
                broker,
                "SPY260710C00450000",
                request_context=ctx,
            )

        assert result["reason_code"] == "MARKET_DATA_THROTTLE_UNAVAILABLE"
        broker.get_quote.assert_not_called()
        assert ctx.throttle_diagnostics[-1]["phase"] == "acquire"

    def test_paper_unthrottled_direct_quote_requires_explicit_env(self, monkeypatch):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 1.1, "ask": 1.2}
        ctx = _new_selector_request_context("SPY", "paper")
        monkeypatch.setenv("SELECTOR_PAPER_ALLOW_UNTHROTTLED_DIRECT_QUOTES", "1")

        with patch(
            "ap.tradier_market_data_throttle.before_market_data_call",
            side_effect=RuntimeError("throttle unavailable"),
        ):
            result = fetch_direct_option_quote_with_meta(
                broker,
                "SPY260710C00450000",
                request_context=ctx,
            )

        assert result["ok"] is True
        broker.get_quote.assert_called_once()
        assert ctx.throttle_diagnostics[-1]["phase"] == "acquire"

    def test_chain_429_preserves_provider_taxonomy(self):
        sel, session = _make_selector()
        ctx = _new_selector_request_context("SPY", "live")
        session.get.return_value = _response(status_code=429)

        with pytest.raises(ChainProviderError) as exc_info:
            sel._fetch_chain_for_expiration(
                session,
                "https://api.tradier.com",
                {},
                "SPY",
                "call",
                "2026-07-10",
                underlying_price=450.0,
                request_context=ctx,
            )

        assert exc_info.value.status_code == 429
        assert ctx.provider_call_counts["chain_calls"] == 1

    def test_pr314_live_expiration_failure_remains_fail_closed(self):
        session = MagicMock()
        response = _response(status_code=503)
        response.headers = {"Retry-After": "2"}
        session.get.return_value = response
        broker = SimpleNamespace(
            session=session,
            base_url="https://api.tradier.com",
            access_token="TOKEN",
        )
        sel = APContractSelectionEngine(broker, mode="live", data_broker=broker)
        sel.dte_ladder_enabled = True
        sel.deferred_dte_legacy_fallback = False
        plan = SimpleNamespace(
            ticker="SPY",
            side="CALL",
            timeframe="1d",
            execution_mode="live",
            max_position_usd=500.0,
            signal_id="sig-316-exp-failure",
            metadata={"deferred_breach_selection": True},
        )

        assert sel.select(plan) is None
        assert plan.metadata["selector_failure"]["reason_code"] == "CHAIN_PROVIDER_ERROR"
        audit = plan.metadata["dte_ladder_audit"]
        assert audit["expiration_http_status"] == 503
        assert audit["retry_after_ms"] == 2000
        assert audit["fallback_used"] is False
        assert audit["final_reason"] == "CHAIN_PROVIDER_ERROR"
        assert plan.metadata["selector_request_diagnostics"]["expiration_calls"] == 1

    def test_ladder_probes_reuse_exact_same_request_context(self):
        sel, _ = _make_selector()
        today = date.today()
        expirations = []
        for offset in (1, 4, 9):
            candidate = today + timedelta(days=offset)
            while candidate.weekday() >= 5:
                candidate += timedelta(days=1)
            expirations.append(candidate.isoformat())
        ctx = _new_selector_request_context("SPY", "paper")
        sel._fetch_expirations_list = MagicMock(return_value=expirations)
        seen_context_ids = []

        def _probe(plan, *, expiration_override=None, request_context=None, **kwargs):
            seen_context_ids.append(id(request_context))
            plan.metadata["selector_failure"] = {
                "reason_code": "CHAIN_PROVIDER_EMPTY_OPTIONS",
                "explanation": "empty",
            }
            return None

        sel.select = _probe
        plan = SimpleNamespace(
            ticker="SPY",
            timeframe="1d",
            execution_mode="paper",
            metadata={"deferred_breach_selection": True},
        )

        sel._select_with_dte_ladder(plan, request_context=ctx)

        assert len(seen_context_ids) == 3
        assert set(seen_context_ids) == {id(ctx)}

    def test_sequential_requests_do_not_share_counters(self):
        ctx_a = _new_selector_request_context("SPY", "paper")
        ctx_b = _new_selector_request_context("SPY", "paper")
        ctx_a.provider_call_counts["chain_calls"] = 3
        ctx_a.elapsed_ms_by_stage["chain"] = 12.5

        assert ctx_b.provider_call_counts == {}
        assert ctx_b.elapsed_ms_by_stage == {}
        assert ctx_a.revalidated_contracts is not ctx_b.revalidated_contracts

    def test_request_limits_are_env_configured(self, monkeypatch):
        monkeypatch.setenv("SELECTOR_MAX_EXPIRATION_CALLS", "2")
        monkeypatch.setenv("SELECTOR_MAX_CHAIN_CALLS", "4")
        monkeypatch.setenv("SELECTOR_MAX_DIRECT_QUOTE_CALLS", "3")
        monkeypatch.setenv("SELECTOR_MAX_TOTAL_ELAPSED_MS", "9000")

        ctx = _new_selector_request_context("SPY", "live")

        assert ctx.max_expiration_calls == 2
        assert ctx.max_chain_calls == 4
        assert ctx.max_direct_quote_calls == 3
        assert ctx.effective_direct_quote_limit == 3
        assert ctx.max_total_elapsed_ms == 9000
        assert ctx.direct_quote_attempts_remaining == 3

    def test_typeerror_request_context_compatibility_retry_is_removed(self):
        assert not hasattr(selector_module, "_call_select_with_optional_request_context")

    def test_cached_underlying_and_expirations_reused_after_first_chain_failure(self):
        sel, session = _make_selector()
        ctx = _new_selector_request_context("SPY")
        expirations = ["2026-07-10", "2026-07-11"]
        session.get.side_effect = [
            _response(payload={"quotes": {"quote": {"last": 450.0}}}),
            _response(payload={"expirations": {"date": expirations}}),
            _response(payload={"options": {"option": []}}),
            _response(payload={"options": {"option": [_option("SPY2", expirations[1])]}}),
        ]

        with pytest.raises(Exception) as excinfo:
            sel._fetch_tradier_chain(
                "SPY",
                "call",
                expiration_override=expirations[0],
                request_context=ctx,
            )
        assert "zero option rows" in str(excinfo.value)

        chain, underlying = sel._fetch_tradier_chain(
            "SPY",
            "call",
            expiration_override=expirations[1],
            request_context=ctx,
        )

        assert underlying == 450.0
        assert chain[0]["symbol"] == "SPY2"
        assert ctx.provider_call_counts["underlying_quote_calls"] == 1
        assert ctx.provider_call_counts["expiration_calls"] == 1
        assert ctx.provider_call_counts["chain_calls"] == 2

    def test_throttle_acquire_failure_records_structured_diagnostic(self):
        sel, session = _make_selector()
        ctx = _new_selector_request_context("SPY")
        expirations = ["2026-07-10"]
        session.get.side_effect = [
            _response(payload={"quotes": {"quote": {"last": 450.0}}}),
            _response(payload={"expirations": {"date": expirations}}),
            _response(payload={"options": {"option": [_option("SPY1", expirations[0])]}}),
        ]

        with patch("ap.tradier_market_data_throttle.before_market_data_call", side_effect=RuntimeError("throttle boom")):
            chain, underlying = sel._fetch_tradier_chain(
                "SPY",
                "call",
                expiration_override=expirations[0],
                request_context=ctx,
            )

        assert chain[0]["symbol"] == "SPY1"
        assert underlying == 450.0
        assert ctx.throttle_diagnostics
        assert all(diag["phase"] == "acquire" for diag in ctx.throttle_diagnostics)

    def test_after_market_data_call_runs_once_when_provider_raises(self):
        broker = MagicMock()
        broker.get_quote.side_effect = RuntimeError("provider boom")
        ctx = _new_selector_request_context("SPY")

        with patch("ap.tradier_market_data_throttle.before_market_data_call", return_value={"acquired": True, "wait_ms": 7.0}), \
             patch("ap.tradier_market_data_throttle.after_market_data_call") as after_call:
            result = fetch_direct_option_quote_with_meta(
                broker,
                "SPY260710C00450000",
                request_context=ctx,
            )

        assert result["ok"] is False
        after_call.assert_called_once()
        assert ctx.provider_call_counts["direct_quote_calls"] == 1

    def test_concurrent_request_contexts_keep_independent_budgets(self):
        broker = MagicMock()
        broker.get_quote.return_value = {"bid": 1.1, "ask": 1.2}
        ctx_a = _new_selector_request_context("SPY")
        ctx_b = _new_selector_request_context("SPY")
        ctx_a.max_direct_quote_calls = ctx_a.effective_direct_quote_limit = 1
        ctx_b.max_direct_quote_calls = ctx_b.effective_direct_quote_limit = 1

        def _run(ctx, symbol):
            revalidate_with_direct_quote(
                broker,
                {"symbol": symbol, "bid": 0.0, "ask": 0.0},
                "zero_bid_or_ask",
                market_open_override=True,
                request_context=ctx,
            )

        t1 = threading.Thread(target=_run, args=(ctx_a, "SPY260710C00450000"))
        t2 = threading.Thread(target=_run, args=(ctx_b, "SPY260710C00460000"))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert ctx_a.direct_quote_attempts_remaining == 5
        assert ctx_b.direct_quote_attempts_remaining == 5
        assert ctx_a.provider_call_counts["direct_quote_calls"] == 1
        assert ctx_b.provider_call_counts["direct_quote_calls"] == 1
        assert broker.get_quote.call_count == 2

    def test_non_deferred_single_selection_has_single_provider_pass(self):
        sel, session = _make_selector()
        ctx = _new_selector_request_context("SPY")
        expirations = ["2026-07-24"]
        session.get.side_effect = [
            _response(payload={"quotes": {"quote": {"last": 450.0}}}),
            _response(payload={"expirations": {"date": expirations}}),
            _response(payload={"options": {"option": [_option("SPY1", expirations[0])]}}),
        ]

        chain, underlying = sel._fetch_tradier_chain("SPY", "call", request_context=ctx)

        assert chain[0]["symbol"] == "SPY1"
        assert underlying == 450.0
        assert ctx.provider_call_counts == {
            "underlying_quote_calls": 1,
            "expiration_calls": 1,
            "chain_calls": 1,
        }

    def test_cached_and_uncached_chain_output_match(self):
        expirations = ["2026-07-24"]

        sel_a, session_a = _make_selector()
        ctx_a = _new_selector_request_context("SPY")
        session_a.get.side_effect = [
            _response(payload={"quotes": {"quote": {"last": 450.0}}}),
            _response(payload={"expirations": {"date": expirations}}),
            _response(payload={"options": {"option": [_option("SPY1", expirations[0])]}}),
        ]
        chain_a, price_a = sel_a._fetch_tradier_chain("SPY", "call", request_context=ctx_a)

        sel_b, session_b = _make_selector()
        ctx_b = _new_selector_request_context("SPY")
        session_b.get.side_effect = [
            _response(payload={"quotes": {"quote": {"last": 450.0}}}),
            _response(payload={"expirations": {"date": expirations}}),
            _response(payload={"options": {"option": [_option("SPY1", expirations[0])]}}),
        ]
        chain_b, price_b = sel_b._fetch_tradier_chain("SPY", "call", request_context=ctx_b)

        assert chain_a == chain_b
        assert price_a == price_b
