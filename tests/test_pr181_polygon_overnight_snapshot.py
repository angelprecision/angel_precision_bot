# tests/test_pr181_polygon_overnight_snapshot.py
# =============================================================================
# PR #181 — fetch_market_snapshot uses Polygon daily snapshot, not Tradier
# timesales.
#
# Covers all 8 spec requirements:
#   1. Good Polygon snapshot returns correct high/low/last
#   2. PUT invalidation when session_high_so_far > prior_day_high
#   3. CALL invalidation when session_low_so_far < prior_day_low
#   4. Missing POLYGON_API_KEY returns None (RETRY_LATER)
#   5. Polygon HTTP error returns None (RETRY_LATER)
#   6. Zero/missing day.h or day.l returns None (RETRY_LATER)
#   7. broker argument is optional and not used
#   8. No Tradier timesales call remains in fetch_market_snapshot
# =============================================================================

from __future__ import annotations

import importlib.util
import os
import sys
import types
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]


# ---------------------------------------------------------------------------
# Module loader
# ---------------------------------------------------------------------------

def _load_validator():
    spec = importlib.util.spec_from_file_location(
        "ap.overnight_daily_validator",
        _REPO / "ap" / "overnight_daily_validator.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ap.overnight_daily_validator"] = mod
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------
# Polygon response builder
# ---------------------------------------------------------------------------

def _polygon_response(
    *,
    h: float = 230.0,
    l: float = 222.0,
    c: float = 227.0,
    vw: float = 226.5,
    last_trade_p: float = 227.50,
    status_code: int = 200,
    status_str: str = "OK",
):
    """Build a minimal Polygon /v2/snapshot ticker response dict."""
    ticker_dict = {
        "ticker": "ROST",
        "day": {"h": h, "l": l, "c": c, "vw": vw},
        "lastTrade": {"p": last_trade_p, "s": 100},
    }
    return {"status": status_str, "ticker": ticker_dict}


def _mock_requests_get(response_data=None, status_code=200, raise_exc=None):
    """Return a mock for requests.get that returns the given response."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.text = str(response_data or {})[:120]
    mock_resp.json.return_value = response_data or {}

    mock_get = MagicMock()
    if raise_exc is not None:
        mock_get.side_effect = raise_exc
    else:
        mock_get.return_value = mock_resp
    return mock_get


# ---------------------------------------------------------------------------
# Spec Req 1: Good Polygon snapshot returns correct high / low / last
# ---------------------------------------------------------------------------

class TestGoodPolygonSnapshot:
    """Spec 1: valid Polygon response produces correct MarketSnapshot."""

    def test_returns_market_snapshot(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response(h=230.0, l=222.0, c=227.0, last_trade_p=227.50)
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr(
            "requests.get", _mock_requests_get(response_data=resp)
        )
        snap = mod.fetch_market_snapshot("ROST")
        assert snap is not None

    def test_session_high_from_day_h(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response(h=231.50, l=220.0)
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        snap = mod.fetch_market_snapshot("ROST")
        assert snap.session_high_so_far == 231.50

    def test_session_low_from_day_l(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response(h=231.50, l=219.75)
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        snap = mod.fetch_market_snapshot("ROST")
        assert snap.session_low_so_far == 219.75

    def test_last_price_from_last_trade_p(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response(h=230.0, l=222.0, last_trade_p=228.33)
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        snap = mod.fetch_market_snapshot("ROST")
        assert snap.last_price == 228.33

    def test_last_price_fallback_to_day_c(self, monkeypatch):
        """When lastTrade.p is missing, last_price falls back to day.c."""
        mod = _load_validator()
        resp = _polygon_response(h=230.0, l=222.0, c=226.80, last_trade_p=0.0)
        resp["ticker"]["lastTrade"]["p"] = None
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        snap = mod.fetch_market_snapshot("ROST")
        assert snap.last_price == 226.80

    def test_last_price_fallback_to_day_vw(self, monkeypatch):
        """When lastTrade.p and day.c missing, falls back to day.vw."""
        mod = _load_validator()
        resp = _polygon_response(h=230.0, l=222.0, c=0.0, vw=225.50, last_trade_p=0.0)
        resp["ticker"]["lastTrade"]["p"] = None
        resp["ticker"]["day"]["c"] = None
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        snap = mod.fetch_market_snapshot("ROST")
        assert snap.last_price == 225.50

    def test_ticker_is_set(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response()
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        snap = mod.fetch_market_snapshot("ROST")
        assert snap.ticker == "ROST"

    def test_fetched_at_is_iso_string(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response()
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        snap = mod.fetch_market_snapshot("ROST")
        assert isinstance(snap.fetched_at, str)
        assert "T" in snap.fetched_at


# ---------------------------------------------------------------------------
# Spec Req 2: PUT invalidation when session_high_so_far > prior_day_high
# ---------------------------------------------------------------------------

class TestPUTInvalidation:
    """Spec 2: PUT signal invalidated when session high breaches prior day high."""

    def test_put_invalidated_when_high_breached(self, monkeypatch):
        mod = _load_validator()
        # session_high=230 > prior_day_high=228 → invalidated
        snap = mod.MarketSnapshot(
            ticker="ROST",
            session_high_so_far=230.0,
            session_low_so_far=222.0,
            last_price=227.0,
            fetched_at="2026-06-25T10:00:00+00:00",
        )
        result = mod.validate_overnight_daily_signal(
            ticker="ROST",
            side="PUT",
            prior_day_high=228.0,
            prior_day_low=218.0,
            snapshot=snap,
        )
        assert result.valid is False
        assert result.reason_code == mod.InvalidationReason.PRIOR_HIGH_BREACHED

    def test_put_valid_when_high_not_breached(self, monkeypatch):
        mod = _load_validator()
        snap = mod.MarketSnapshot(
            ticker="ROST",
            session_high_so_far=226.0,
            session_low_so_far=222.0,
            last_price=225.0,
            fetched_at="2026-06-25T10:00:00+00:00",
        )
        result = mod.validate_overnight_daily_signal(
            ticker="ROST",
            side="PUT",
            prior_day_high=228.0,
            prior_day_low=218.0,
            snapshot=snap,
        )
        assert result.valid is True

    def test_put_invalidated_reason_code(self, monkeypatch):
        mod = _load_validator()
        snap = mod.MarketSnapshot(
            ticker="DE", session_high_so_far=583.0,
            session_low_so_far=570.0, last_price=580.0,
            fetched_at="2026-06-25T10:00:00+00:00",
        )
        result = mod.validate_overnight_daily_signal(
            ticker="DE", side="PUT",
            prior_day_high=580.0, prior_day_low=560.0, snapshot=snap,
        )
        assert result.valid is False
        assert "PRIOR_HIGH_BREACHED" in result.reason_code


# ---------------------------------------------------------------------------
# Spec Req 3: CALL invalidation when session_low_so_far < prior_day_low
# ---------------------------------------------------------------------------

class TestCALLInvalidation:
    """Spec 3: CALL signal invalidated when session low breaches prior day low."""

    def test_call_invalidated_when_low_breached(self, monkeypatch):
        mod = _load_validator()
        snap = mod.MarketSnapshot(
            ticker="ROST",
            session_high_so_far=226.0,
            session_low_so_far=217.0,   # < prior_day_low=218
            last_price=225.0,
            fetched_at="2026-06-25T10:00:00+00:00",
        )
        result = mod.validate_overnight_daily_signal(
            ticker="ROST", side="CALL",
            prior_day_high=228.0, prior_day_low=218.0, snapshot=snap,
        )
        assert result.valid is False
        assert result.reason_code == mod.InvalidationReason.PRIOR_LOW_BREACHED

    def test_call_valid_when_low_not_breached(self, monkeypatch):
        mod = _load_validator()
        snap = mod.MarketSnapshot(
            ticker="ROST",
            session_high_so_far=226.0,
            session_low_so_far=219.5,
            last_price=225.0,
            fetched_at="2026-06-25T10:00:00+00:00",
        )
        result = mod.validate_overnight_daily_signal(
            ticker="ROST", side="CALL",
            prior_day_high=228.0, prior_day_low=218.0, snapshot=snap,
        )
        assert result.valid is True


# ---------------------------------------------------------------------------
# Spec Req 4: Missing POLYGON_API_KEY → None (RETRY_LATER)
# ---------------------------------------------------------------------------

class TestMissingAPIKey:
    """Spec 4: missing POLYGON_API_KEY returns None, does not hard-reject."""

    def test_missing_key_returns_none(self, monkeypatch):
        mod = _load_validator()
        monkeypatch.delenv("POLYGON_API_KEY", raising=False)
        result = mod.fetch_market_snapshot("ROST")
        assert result is None

    def test_empty_key_returns_none(self, monkeypatch):
        mod = _load_validator()
        monkeypatch.setenv("POLYGON_API_KEY", "")
        result = mod.fetch_market_snapshot("ROST")
        assert result is None

    def test_whitespace_only_key_returns_none(self, monkeypatch):
        mod = _load_validator()
        monkeypatch.setenv("POLYGON_API_KEY", "   ")
        result = mod.fetch_market_snapshot("ROST")
        assert result is None

    def test_no_requests_call_when_key_missing(self, monkeypatch):
        """No outbound HTTP request when API key is missing."""
        mod = _load_validator()
        monkeypatch.delenv("POLYGON_API_KEY", raising=False)
        mock_get = MagicMock()
        monkeypatch.setattr("requests.get", mock_get)
        mod.fetch_market_snapshot("ROST")
        mock_get.assert_not_called()


# ---------------------------------------------------------------------------
# Spec Req 5: Polygon HTTP error → None (RETRY_LATER)
# ---------------------------------------------------------------------------

class TestPolygonHTTPError:
    """Spec 5: non-200 HTTP response → None, not hard rejection."""

    @pytest.mark.parametrize("status_code", [400, 401, 403, 429, 500, 503])
    def test_http_error_returns_none(self, monkeypatch, status_code):
        mod = _load_validator()
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        mock_resp = MagicMock()
        mock_resp.status_code = status_code
        mock_resp.text = "error"
        mock_get = MagicMock(return_value=mock_resp)
        monkeypatch.setattr("requests.get", mock_get)
        result = mod.fetch_market_snapshot("ROST")
        assert result is None

    def test_network_exception_returns_none(self, monkeypatch):
        """Network-level exception (ConnectionError, timeout) → None."""
        mod = _load_validator()
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr(
            "requests.get",
            _mock_requests_get(raise_exc=ConnectionError("timeout")),
        )
        result = mod.fetch_market_snapshot("ROST")
        assert result is None

    def test_json_parse_error_returns_none(self, monkeypatch):
        """Malformed JSON response → None."""
        mod = _load_validator()
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.text = "not json"
        mock_resp.json.side_effect = ValueError("JSON decode error")
        monkeypatch.setattr("requests.get", MagicMock(return_value=mock_resp))
        result = mod.fetch_market_snapshot("ROST")
        assert result is None


# ---------------------------------------------------------------------------
# Spec Req 6: Zero / missing day.h or day.l → None (RETRY_LATER)
# ---------------------------------------------------------------------------

class TestMissingDayHL:
    """Spec 6: zero or missing day.h / day.l → None, not hard rejection."""

    def test_zero_day_h_returns_none(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response(h=0.0, l=222.0)
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        assert mod.fetch_market_snapshot("ROST") is None

    def test_zero_day_l_returns_none(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response(h=230.0, l=0.0)
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        assert mod.fetch_market_snapshot("ROST") is None

    def test_none_day_h_returns_none(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response(h=230.0, l=222.0)
        resp["ticker"]["day"]["h"] = None
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        assert mod.fetch_market_snapshot("ROST") is None

    def test_missing_day_node_returns_none(self, monkeypatch):
        """ticker dict present but no 'day' key → None."""
        mod = _load_validator()
        resp = {"status": "OK", "ticker": {"ticker": "ROST", "lastTrade": {"p": 227.0}}}
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        assert mod.fetch_market_snapshot("ROST") is None

    def test_missing_ticker_node_returns_none(self, monkeypatch):
        """Polygon response has no 'ticker' key → None."""
        mod = _load_validator()
        resp = {"status": "OK"}
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        assert mod.fetch_market_snapshot("ROST") is None


# ---------------------------------------------------------------------------
# Spec Req 7: broker is optional and not used
# ---------------------------------------------------------------------------

class TestBrokerOptional:
    """Spec 7: broker argument is optional and not used for the snapshot."""

    def test_call_without_broker(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response()
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        # No broker argument at all
        snap = mod.fetch_market_snapshot("ROST")
        assert snap is not None

    def test_call_with_none_broker(self, monkeypatch):
        mod = _load_validator()
        resp = _polygon_response()
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        snap = mod.fetch_market_snapshot("ROST", broker=None)
        assert snap is not None

    def test_call_with_broker_not_used(self, monkeypatch):
        """Passing a mock broker should not affect the result."""
        mod = _load_validator()
        resp = _polygon_response()
        monkeypatch.setenv("POLYGON_API_KEY", "test-key")
        monkeypatch.setattr("requests.get", _mock_requests_get(response_data=resp))
        fake_broker = MagicMock()
        snap = mod.fetch_market_snapshot("ROST", broker=fake_broker)
        assert snap is not None
        # Broker session must not have been called
        fake_broker.session.get.assert_not_called()

    def test_signature_has_broker_default_none(self):
        """Function signature must accept broker=None."""
        import inspect
        mod = _load_validator()
        sig = inspect.signature(mod.fetch_market_snapshot)
        params = sig.parameters
        assert "broker" in params, "fetch_market_snapshot must have a broker parameter"
        assert params["broker"].default is None, (
            "broker parameter default must be None"
        )


# ---------------------------------------------------------------------------
# Spec Req 8: No Tradier timesales call in fetch_market_snapshot
# ---------------------------------------------------------------------------

class TestNoTimesalesCall:
    """Spec 8: fetch_market_snapshot must not make a Tradier timesales call."""

    def test_source_no_timesales_api_call_in_fetch_market_snapshot(self):
        """Source-level: /v1/markets/timesales must not appear in
        fetch_market_snapshot's function body."""
        src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
        fn_start = src.find("def fetch_market_snapshot(ticker: str, broker=None)")
        assert fn_start != -1, "fetch_market_snapshot(ticker, broker=None) not found"
        # End of function: next top-level def
        fn_end = src.find("\n\n\ndef _missing_data_result(", fn_start)
        fn_body = src[fn_start:fn_end] if fn_end != -1 else src[fn_start:]
        assert "/v1/markets/timesales" not in fn_body, (
            "Tradier timesales API path found inside fetch_market_snapshot — "
            "this function must use Polygon, not Tradier timesales"
        )

    def test_source_no_broker_session_in_fetch_market_snapshot(self):
        """broker.session.get must not appear in the function body."""
        src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
        fn_start = src.find("def fetch_market_snapshot(ticker: str, broker=None)")
        fn_end   = src.find("\n\n\ndef _missing_data_result(", fn_start)
        fn_body  = src[fn_start:fn_end] if fn_end != -1 else src[fn_start:]
        assert "broker.session" not in fn_body, (
            "broker.session used inside fetch_market_snapshot — "
            "Polygon does not require a broker session"
        )

    def test_source_polygon_api_key_in_fetch_market_snapshot(self):
        """Polygon API key env var must be read inside the function."""
        src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
        fn_start = src.find("def fetch_market_snapshot(ticker: str, broker=None)")
        fn_end   = src.find("\n\n\ndef _missing_data_result(", fn_start)
        fn_body  = src[fn_start:fn_end] if fn_end != -1 else src[fn_start:]
        assert "POLYGON_API_KEY" in fn_body

    def test_no_tradier_quote_call_in_fetch_market_snapshot(self):
        """Tradier /v1/markets/quotes call must not appear in the function body."""
        src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
        fn_start = src.find("def fetch_market_snapshot(ticker: str, broker=None)")
        fn_end   = src.find("\n\n\ndef _missing_data_result(", fn_start)
        fn_body  = src[fn_start:fn_end] if fn_end != -1 else src[fn_start:]
        assert "/v1/markets/quotes" not in fn_body, (
            "Tradier quotes API path found inside fetch_market_snapshot"
        )

    def test_polygon_endpoint_is_v2_snapshot(self):
        """Source must use the Polygon v2 snapshot endpoint."""
        src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
        assert "api.polygon.io/v2/snapshot" in src, (
            "Polygon v2 snapshot endpoint not found in source"
        )
