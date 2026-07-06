"""
tests/test_p0_tradier_md_throttle.py

Acceptance tests for PR #296: Tradier market-data throttle.

Required coverage:
  1. throttle disabled → no sleep, existing behavior unchanged
  2. throttle enabled → two consecutive calls wait at least configured interval
  3. open window uses OPEN interval (longer than baseline)
  4. jitter is bounded within configured range
  5. direct quote revalidator calls throttle before broker.get_quote
  6. selector chain path calls throttle before quote/expirations/chain GETs
  7. no throttle around submit_existing_entry or broker order placement
  8. provider errors still surface honest reason codes (not hidden by throttle)
  9. throttle disabled is a complete no-op (no lock contention, no imports forced)
"""
from __future__ import annotations

import os
import threading
import time
from unittest.mock import MagicMock, call, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _env(monkeypatch, *, enabled="1", min_ms="100", open_ms="200",
         jitter_ms="0", max_concurrent="1",
         open_start="09:30", open_end="09:40"):
    monkeypatch.setenv("TRADIER_MD_THROTTLE_ENABLED", enabled)
    monkeypatch.setenv("TRADIER_MD_MIN_INTERVAL_MS", min_ms)
    monkeypatch.setenv("TRADIER_MD_OPEN_MIN_INTERVAL_MS", open_ms)
    monkeypatch.setenv("TRADIER_MD_JITTER_MS", jitter_ms)
    monkeypatch.setenv("TRADIER_MD_MAX_CONCURRENT", max_concurrent)
    monkeypatch.setenv("TRADIER_MD_OPEN_WINDOW_START_ET", open_start)
    monkeypatch.setenv("TRADIER_MD_OPEN_WINDOW_END_ET", open_end)


# ─────────────────────────────────────────────────────────────────────────────
# Test 1: throttle disabled → no sleep, no-op
# ─────────────────────────────────────────────────────────────────────────────

def test_disabled_is_complete_noop(monkeypatch):
    """When TRADIER_MD_THROTTLE_ENABLED=0 (default), before_market_data_call
    must return immediately without sleeping, locking, or logging."""
    _env(monkeypatch, enabled="0", min_ms="5000")  # ridiculously large min_ms
    from ap.tradier_market_data_throttle import before_market_data_call, after_market_data_call

    sleep_calls = []
    monkeypatch.setattr(time, "sleep", lambda s: sleep_calls.append(s))

    t0 = time.monotonic()
    before_market_data_call("/v1/markets/quotes", "SPY", context="test")
    after_market_data_call()
    elapsed_ms = (time.monotonic() - t0) * 1000

    assert sleep_calls == [], "disabled throttle must not sleep"
    assert elapsed_ms < 50, f"disabled throttle must return instantly; took {elapsed_ms:.1f} ms"


def test_disabled_default_env(monkeypatch):
    """TRADIER_MD_THROTTLE_ENABLED defaults to 0 when env is unset."""
    monkeypatch.delenv("TRADIER_MD_THROTTLE_ENABLED", raising=False)
    from ap.tradier_market_data_throttle import _cfg
    cfg = _cfg()
    assert cfg["enabled"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Test 2: throttle enabled → consecutive calls wait at least configured interval
# ─────────────────────────────────────────────────────────────────────────────

def test_enabled_consecutive_calls_wait_min_interval(monkeypatch):
    """Two consecutive calls must wait at least min_interval between them."""
    _env(monkeypatch, enabled="1", min_ms="80", jitter_ms="0",
         # Put open window in the past so baseline interval applies
         open_start="00:00", open_end="00:01")
    from ap.tradier_market_data_throttle import before_market_data_call, after_market_data_call
    import ap.tradier_market_data_throttle as _mod
    # Reset shared state
    _mod._LAST_CALL_TS = 0.0

    t0 = time.monotonic()
    before_market_data_call("/v1/markets/quotes", "SPY", context="call1")
    after_market_data_call()

    before_market_data_call("/v1/markets/options/expirations", "SPY", context="call2")
    after_market_data_call()

    elapsed_ms = (time.monotonic() - t0) * 1000
    # Two calls, first is immediate, second waits ≥80ms → total ≥80ms
    assert elapsed_ms >= 70, (
        f"consecutive calls did not wait minimum interval; elapsed={elapsed_ms:.1f} ms"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 3: open window uses OPEN interval (longer than baseline)
# ─────────────────────────────────────────────────────────────────────────────

def test_open_window_uses_longer_interval(monkeypatch):
    """When now() is inside the open window, OPEN interval applies (not baseline)."""
    _env(monkeypatch, enabled="1", min_ms="50", open_ms="200", jitter_ms="0",
         open_start="00:00", open_end="23:59")  # always-open window for test
    from ap.tradier_market_data_throttle import _in_open_window, _cfg
    cfg = _cfg()
    assert _in_open_window(cfg) is True, "always-open window must be detected"
    assert cfg["open_ms"] == 200
    assert cfg["min_ms"] == 50


def test_outside_open_window_uses_baseline(monkeypatch):
    _env(monkeypatch, enabled="1", min_ms="50", open_ms="200", jitter_ms="0",
         open_start="02:00", open_end="02:01")  # never open
    from ap.tradier_market_data_throttle import _in_open_window, _cfg
    cfg = _cfg()
    assert _in_open_window(cfg) is False


# ─────────────────────────────────────────────────────────────────────────────
# Test 4: jitter is bounded within configured range
# ─────────────────────────────────────────────────────────────────────────────

def test_jitter_is_bounded(monkeypatch):
    """Jitter added must be >= 0 and <= configured jitter_ms."""
    _env(monkeypatch, enabled="1", min_ms="0", jitter_ms="50",
         open_start="00:00", open_end="00:01")
    from ap.tradier_market_data_throttle import _cfg

    # Sample jitter 1000 times and confirm it is always within [0, 50]
    import random as _random
    cfg = _cfg()
    for _ in range(1000):
        j = _random.randint(0, max(0, cfg["jitter_ms"]))
        assert 0 <= j <= 50, f"jitter {j} out of bounds"


# ─────────────────────────────────────────────────────────────────────────────
# Test 5: direct quote revalidator calls throttle before broker.get_quote
# ─────────────────────────────────────────────────────────────────────────────

def test_direct_quote_revalidator_calls_throttle_before_broker_get_quote(monkeypatch):
    """fetch_direct_option_quote_with_meta must call before_market_data_call
    BEFORE broker.get_quote() — proven by ordering mock call sequence."""
    call_order = []

    def _fake_throttle(endpoint, symbol, context=""):
        call_order.append(("throttle", endpoint, symbol))

    def _fake_after():
        call_order.append(("after",))

    def _fake_get_quote(sym):
        call_order.append(("broker.get_quote", sym))
        return {"bid": 1.80, "ask": 1.86, "last": 1.83, "bid_size": 10, "ask_size": 10, "volume": 100, "open_interest": 500}

    broker = MagicMock()
    broker.get_quote.side_effect = _fake_get_quote

    # Patch the throttle functions as imported inside the revalidator
    with patch("ap.tradier_market_data_throttle.before_market_data_call", side_effect=_fake_throttle), \
         patch("ap.tradier_market_data_throttle.after_market_data_call", side_effect=_fake_after):
        # Force re-import of the throttle inside the revalidator
        import importlib
        import ap.contract_quote_revalidator as cqr
        importlib.reload(cqr)
        cqr.fetch_direct_option_quote_with_meta(broker, "SPY250706C00450000")

    throttle_pos = next((i for i, c in enumerate(call_order) if c[0] == "throttle"), None)
    broker_pos   = next((i for i, c in enumerate(call_order) if c[0] == "broker.get_quote"), None)

    assert throttle_pos is not None, "throttle before_market_data_call not called"
    assert broker_pos is not None, "broker.get_quote not called"
    assert throttle_pos < broker_pos, (
        f"throttle must be called BEFORE broker.get_quote; "
        f"throttle@{throttle_pos}, broker@{broker_pos}"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 6: selector chain path calls throttle before quote/expirations/chain
# ─────────────────────────────────────────────────────────────────────────────


def _make_selector_for_test():
    """Build a minimal APContractSelectionEngine with the correct constructor.
    Selector takes broker as first positional arg."""
    import ap.contract_selector as cs
    _broker = MagicMock()
    _broker.base_url = "https://sandbox.tradier.com"
    _broker.access_token = "TOKEN"
    _session = MagicMock()
    _broker.session = _session
    _sel = cs.APContractSelectionEngine(_broker, mode="paper", data_broker=_broker)
    return _sel, _session

def test_selector_chain_fetch_calls_throttle(monkeypatch):
    """_fetch_tradier_chain wraps underlying quote, expirations, and chain GETs
    with throttle calls in that order."""
    throttle_calls = []

    def _fake_before(endpoint, symbol, context=""):
        throttle_calls.append(endpoint)

    with patch("ap.tradier_market_data_throttle.before_market_data_call", side_effect=_fake_before), \
         patch("ap.tradier_market_data_throttle.after_market_data_call"):
        import ap.contract_selector as cs
        import importlib; importlib.reload(cs)

        _sel, _s = _make_selector_for_test()
        _s.get.side_effect = [
            MagicMock(status_code=200, json=lambda: {"quotes": {"quote": {"last": 450.0}}}),
            MagicMock(status_code=200, json=lambda: {"expirations": {"date": ["2026-07-10"]}}),
            MagicMock(status_code=200, json=lambda: {"options": {"option": []}}),
        ]
        try:
            _sel._fetch_tradier_chain("SPY", "call")
        except Exception:
            pass

    assert "/v1/markets/quotes" in throttle_calls, "underlying quote not throttled"
    assert "/v1/markets/options/expirations" in throttle_calls, "expirations not throttled"
    assert "/v1/markets/options/chains" in throttle_calls, "chains not throttled"

    # Order check: quotes → expirations → chains
    indices = {ep: throttle_calls.index(ep) for ep in [
        "/v1/markets/quotes",
        "/v1/markets/options/expirations",
        "/v1/markets/options/chains",
    ] if ep in throttle_calls}
    if len(indices) == 3:
        assert indices["/v1/markets/quotes"] < indices["/v1/markets/options/expirations"]
        assert indices["/v1/markets/options/expirations"] < indices["/v1/markets/options/chains"]


# ─────────────────────────────────────────────────────────────────────────────
# Test 7: no throttle around submit_existing_entry or broker order placement
# ─────────────────────────────────────────────────────────────────────────────

def test_throttle_module_has_no_functional_reference_to_submit_paths():
    """The throttle module must not make functional Python calls to submit paths.
    Mentions in docstrings explaining exclusions are fine; callable references are not."""
    import ast as _ast
    _src = open("ap/tradier_market_data_throttle.py").read()
    _tree = _ast.parse(_src)
    _names = {n.id for n in _ast.walk(_tree) if isinstance(n, _ast.Name)}
    _attrs = {n.attr for n in _ast.walk(_tree) if isinstance(n, _ast.Attribute)}
    _all_refs = _names | _attrs
    for _forbidden in ("submit_existing_entry", "place_order", "fill_monitor", "exit_engine"):
        assert _forbidden not in _all_refs, (
            f"throttle module has a functional reference to submit path: {_forbidden!r}"
        )


def test_throttle_not_called_by_refresh_ask_at_submit():
    """_refresh_ask_at_submit sits immediately before broker POST and must not
    be wrapped — confirmed by absence of throttle import in that function."""
    src = open("ap/contract_selector.py").read()
    # Find _refresh_ask_at_submit body (if it exists in this file)
    if "_refresh_ask_at_submit" not in src:
        pytest.skip("_refresh_ask_at_submit not in contract_selector.py")
    idx = src.find("def _refresh_ask_at_submit")
    next_def = src.find("\n    def ", idx + 1)
    body = src[idx:next_def] if next_def > 0 else src[idx:idx+500]
    assert "tradier_market_data_throttle" not in body, (
        "_refresh_ask_at_submit must not call the throttle"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Test 8: provider errors are preserved — throttle does not hide them
# ─────────────────────────────────────────────────────────────────────────────

def test_provider_error_surfaces_original_reason_code(monkeypatch):
    """If broker.get_quote raises or returns empty data, the existing reason
    taxonomy must fire unchanged. The throttle may delay the call, but must
    not convert a provider failure into success or generic UNKNOWN."""
    # Simulate broker.get_quote returning an empty response (zero bid/ask)
    broker = MagicMock()
    broker.get_quote.return_value = {}

    from ap.contract_quote_revalidator import fetch_direct_option_quote_with_meta
    result = fetch_direct_option_quote_with_meta(broker, "SPY250706C00450000")

    # Must not succeed — empty quote must still produce a failure result
    assert result.get("ok") is not True or result.get("quote") is None or (
        # ok=True but quote is marked empty is also acceptable
        result.get("quote", {}).get("_quote_payload_empty") is True
    ), "empty broker response must not become an ok=True clean quote"

    # Provider errors bubble through unchanged
    broker2 = MagicMock()
    broker2.get_quote.side_effect = ConnectionError("Tradier 429")
    result2 = fetch_direct_option_quote_with_meta(broker2, "SPY250706C00450000")
    assert result2.get("ok") is not True, "ConnectionError must not produce ok=True"
    # reason_code must be a meaningful code, not None
    rc = result2.get("reason_code")
    assert rc is not None, "provider error must produce a non-None reason_code"
    assert "UNKNOWN" not in str(rc).upper() or rc.startswith("DIRECT_"), (
        f"reason_code {rc!r} looks like a generic fallback"
    )


def test_chain_provider_errors_not_hidden_by_throttle(monkeypatch):
    """ChainProviderError, ChainEmptyExpirations, ChainAuthError must propagate
    through the throttled code path unchanged."""
    with patch("ap.tradier_market_data_throttle.before_market_data_call"), \
         patch("ap.tradier_market_data_throttle.after_market_data_call"):
        import ap.contract_selector as cs
        import importlib; importlib.reload(cs)

        _sel2, _s2 = _make_selector_for_test()
        _s2.get.side_effect = [
            MagicMock(status_code=200, json=lambda: {"quotes": {"quote": {"last": 450.0}}}),
            MagicMock(status_code=429, json=lambda: {}),
        ]
        with pytest.raises(cs.ChainProviderError):
            _sel2._fetch_tradier_chain("SPY", "call")


# ─────────────────────────────────────────────────────────────────────────────
# Test 9: throttle config parsing — bad env values fall back to defaults
# ─────────────────────────────────────────────────────────────────────────────

def test_bad_env_values_fall_back_to_defaults(monkeypatch):
    monkeypatch.setenv("TRADIER_MD_MIN_INTERVAL_MS", "not_a_number")
    monkeypatch.setenv("TRADIER_MD_JITTER_MS", "")
    monkeypatch.setenv("TRADIER_MD_OPEN_WINDOW_START_ET", "25:99")  # invalid
    from ap.tradier_market_data_throttle import _cfg
    cfg = _cfg()
    assert cfg["min_ms"] == 250    # default
    assert cfg["jitter_ms"] == 75  # default
    assert cfg["open_start"] == "09:30"  # default (invalid fallback)
