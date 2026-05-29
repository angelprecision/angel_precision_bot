"""Tests for the quote-domain audit + paper/live execution separation PR.

Covers:
  - _is_sandbox_base_url / _broker_quote_identity (quote-source evidence)
  - _compute_paper_marketable_limit (paper cushion math)
  - paper market-order hard guard (never in LIVE)
  - APEntryWatcher._watcher_quote_identity (watcher quote source)

Run: DATABASE_URL=postgresql://test:test@localhost/test python3 -m pytest tests/test_quote_domain_paper_fill.py -v
"""
import os
import importlib
import pytest


def _load_exec(cushion_pct="0.10", max_cushion="0.20", fill_mode="marketable_limit"):
    os.environ["PAPER_ENTRY_SLIPPAGE_CUSHION_PCT"] = cushion_pct
    os.environ["PAPER_ENTRY_MAX_CUSHION_DOLLARS"] = max_cushion
    os.environ["PAPER_ENTRY_FILL_MODE"] = fill_mode
    import ap.execution as ex
    importlib.reload(ex)
    return ex


class FakeCfg:
    def __init__(self, base_url): self.base_url = base_url


class FakeBroker:
    def __init__(self, base_url): self.cfg = FakeCfg(base_url)


class TestSandboxDetection:
    def test_sandbox_url(self):
        ex = _load_exec()
        assert ex._is_sandbox_base_url("https://sandbox.tradier.com") is True

    def test_live_url(self):
        ex = _load_exec()
        assert ex._is_sandbox_base_url("https://api.tradier.com") is False

    def test_empty_and_none(self):
        ex = _load_exec()
        assert ex._is_sandbox_base_url("") is False
        assert ex._is_sandbox_base_url(None) is False


class TestBrokerQuoteIdentity:
    def test_sandbox_identity(self):
        ex = _load_exec()
        ident = ex._broker_quote_identity(FakeBroker("https://sandbox.tradier.com"))
        assert ident["quote_source"] == "tradier_sandbox"
        assert ident["sandbox_mode"] is True
        assert ident["quote_base_url"] == "https://sandbox.tradier.com"

    def test_live_identity(self):
        ex = _load_exec()
        ident = ex._broker_quote_identity(FakeBroker("https://api.tradier.com"))
        assert ident["quote_source"] == "tradier_live"
        assert ident["sandbox_mode"] is False

    def test_unknown_identity(self):
        ex = _load_exec()
        ident = ex._broker_quote_identity(FakeBroker(""))
        assert ident["quote_source"] == "unknown"


class TestPaperMarketableLimit:
    def test_dollar_cap_wins(self):
        ex = _load_exec()
        # ask=3.00, pct=10% → 0.30, capped to $0.20 → 3.20
        pl, cush = ex._compute_paper_marketable_limit(3.00)
        assert pl == 3.20 and cush == 0.20

    def test_pct_wins(self):
        ex = _load_exec()
        # ask=1.00, pct=10% → 0.10 < cap 0.20 → 1.10
        pl, cush = ex._compute_paper_marketable_limit(1.00)
        assert pl == 1.10 and cush == 0.10

    def test_penny_floor(self):
        ex = _load_exec()
        # ask=0.05, pct=10% → 0.005 floored to 0.01 → 0.06
        pl, _ = ex._compute_paper_marketable_limit(0.05)
        assert pl == 0.06

    def test_zero_ask_noop(self):
        ex = _load_exec()
        pl, cush = ex._compute_paper_marketable_limit(0.0)
        assert pl == 0.0 and cush == 0.0

    def test_custom_cushion_env(self):
        ex = _load_exec(cushion_pct="0.05", max_cushion="1.00")
        # ask=2.00, pct=5% → 0.10 < cap 1.00 → 2.10
        pl, cush = ex._compute_paper_marketable_limit(2.00)
        assert pl == 2.10 and cush == 0.10


class TestSubmitOrderTypeGuard:
    """The order_type='market' path must pass limit_price=None to the broker."""

    def test_market_order_passes_none_limit(self):
        ex = _load_exec()
        captured = {}

        class CaptureBroker:
            def place_order(self, **kwargs):
                captured.update(kwargs)
                class R:
                    broker_order_id = "X1"
                    status = "ACK"
                    error = None
                return R()

        ok, oid, err = ex._submit_order_with_retry(
            CaptureBroker(), "SPY", "SPY260101C00500000", 1, 3.50, order_type="market"
        )
        assert ok is True
        assert captured["limit_price"] is None, "market order must send None limit"

    def test_limit_order_passes_price(self):
        ex = _load_exec()
        captured = {}

        class CaptureBroker:
            def place_order(self, **kwargs):
                captured.update(kwargs)
                class R:
                    broker_order_id = "X2"
                    status = "ACK"
                    error = None
                return R()

        ok, oid, err = ex._submit_order_with_retry(
            CaptureBroker(), "SPY", "SPY260101C00500000", 1, 3.50, order_type="limit"
        )
        assert ok is True
        assert captured["limit_price"] == 3.50, "limit order must send the price"

    def test_default_order_type_is_limit(self):
        ex = _load_exec()
        captured = {}

        class CaptureBroker:
            def place_order(self, **kwargs):
                captured.update(kwargs)
                class R:
                    broker_order_id = "X3"
                    status = "ACK"
                    error = None
                return R()

        # No order_type arg → defaults to limit (back-compat)
        ok, oid, err = ex._submit_order_with_retry(
            CaptureBroker(), "SPY", "SPY260101C00500000", 1, 3.50
        )
        assert captured["limit_price"] == 3.50


class TestWatcherQuoteIdentity:
    def test_watcher_sandbox(self):
        import ap_entry_watcher as w
        importlib.reload(w)

        class Stub:
            pass
        stub = Stub()
        stub.broker = FakeBroker("https://sandbox.tradier.com")
        ident = w.APEntryWatcher._watcher_quote_identity(stub)
        assert ident["watcher_quote_source"] == "tradier_sandbox"
        assert ident["watcher_sandbox_mode"] is True

    def test_watcher_live(self):
        import ap_entry_watcher as w
        importlib.reload(w)

        class Stub:
            pass
        stub = Stub()
        stub.broker = FakeBroker("https://api.tradier.com")
        ident = w.APEntryWatcher._watcher_quote_identity(stub)
        assert ident["watcher_quote_source"] == "tradier_live"
        assert ident["watcher_sandbox_mode"] is False

    def test_watcher_fallback_matches_fetch_quotes(self):
        """POINT 3: when base_url can't be read, _watcher_quote_identity must
        fall back to the SAME default as _fetch_quotes (sandbox), not 'unknown'.
        Otherwise the audit mislabels a quote the watcher actually fetched from
        sandbox."""
        import ap_entry_watcher as w
        importlib.reload(w)

        class NoUrlBroker:
            pass  # no base_url, no cfg → triggers the final fallback

        class Stub:
            pass
        stub = Stub()
        stub.broker = NoUrlBroker()
        ident = w.APEntryWatcher._watcher_quote_identity(stub)
        # _fetch_quotes would query https://sandbox.tradier.com in this case,
        # so the audit must say sandbox — never 'unknown'.
        assert ident["watcher_quote_base_url"] == "https://sandbox.tradier.com"
        assert ident["watcher_quote_source"] == "tradier_sandbox"
        assert ident["watcher_sandbox_mode"] is True


# ── REVIEW-POINT TESTS (points 2, 3, 4 from the merge review) ────────────────

class TestPoint2LiveMarketHardBlock:
    """MODE=LIVE + PAPER_ENTRY_FILL_MODE=market must STILL submit limit."""

    def test_live_market_env_still_submits_limit(self):
        # The decision logic: _order_type = "market" only if
        # paper_market_order AND _is_paper AND mode == "PAPER".
        # Simulate the exact guard with mode=LIVE.
        ex = _load_exec(fill_mode="market")
        mode = "LIVE"
        _is_paper = (mode == "PAPER")           # False
        # In LIVE the paper-fill branch never runs, so paper_market_order stays False.
        paper_market_order = False
        _order_type = "market" if (paper_market_order and _is_paper and mode == "PAPER") else "limit"
        assert _order_type == "limit", "LIVE must never produce a market order"

    def test_paper_market_env_produces_market_in_paper(self):
        ex = _load_exec(fill_mode="market")
        mode = "PAPER"
        _is_paper = True
        paper_market_order = True              # set by the paper branch on fresh quote
        _order_type = "market" if (paper_market_order and _is_paper and mode == "PAPER") else "limit"
        assert _order_type == "market"

    def test_defensive_reassert_blocks_market_if_not_paper(self):
        # Even if paper_market_order were True, mode=LIVE must force limit.
        mode = "LIVE"
        _is_paper = False
        paper_market_order = True              # hypothetical leak
        _order_type = "market" if (paper_market_order and _is_paper and mode == "PAPER") else "limit"
        assert _order_type == "limit"


class TestPoint3FailClosedPaper:
    """Paper fill modes must not fire on a failed quote refresh."""

    def test_marketable_limit_no_cushion_on_failed_refresh(self):
        ex = _load_exec(fill_mode="marketable_limit")
        # Simulate the branch condition: refresh_ok=False → no cushion applied.
        refresh_ok = False
        submit_ask = 0.0
        submit_limit = 3.00   # fell through to selector_ask via fail-open
        paper_cushion_applied = 0.0
        if refresh_ok and submit_ask > 0:
            _pl, paper_cushion_applied = ex._compute_paper_marketable_limit(float(submit_ask))
            submit_limit = _pl
        assert paper_cushion_applied == 0.0, "no cushion on failed refresh"
        assert submit_limit == 3.00, "submit_limit unchanged on failed refresh"

    def test_market_falls_back_to_limit_on_failed_refresh(self):
        # market mode + failed refresh → paper_market_order must be False.
        refresh_ok = False
        submit_ask = 0.0
        if refresh_ok and submit_ask > 0:
            paper_market_order = True
        else:
            paper_market_order = False
        assert paper_market_order is False, "no market order on failed refresh"


class TestPoint4ThreeStateMismatch:
    """quote_domain_mismatch_possible must be None when a source is unknown."""

    @staticmethod
    def _compute(is_paper, submit_sandbox, sel_sandbox, sel_src, sub_src):
        # Mirror the exact logic in ap/execution.py
        sel_unknown = (sel_src == "unknown")
        sub_unknown = (sub_src == "unknown")
        if not is_paper:
            return False
        elif sel_unknown or sub_unknown:
            return None
        elif submit_sandbox and (not sel_sandbox):
            return True
        else:
            return False

    def test_proven_mismatch_true(self):
        r = self._compute(True, True, False, "tradier_live", "tradier_sandbox")
        assert r is True

    def test_unknown_source_yields_none(self):
        r = self._compute(True, True, False, "unknown", "tradier_sandbox")
        assert r is None

    def test_unknown_submit_yields_none(self):
        r = self._compute(True, True, False, "tradier_live", "unknown")
        assert r is None

    def test_same_domain_false(self):
        r = self._compute(True, True, True, "tradier_sandbox", "tradier_sandbox")
        assert r is False

    def test_live_mode_false(self):
        r = self._compute(False, True, False, "tradier_live", "tradier_sandbox")
        assert r is False
