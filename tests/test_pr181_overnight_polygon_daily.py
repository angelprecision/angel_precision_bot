"""
tests/test_pr181_overnight_polygon_daily.py
PR #181 — Overnight validator: replace Tradier timesales with Polygon daily snapshot.

Root cause of the 401 errors:
  Paper accounts carry a sandbox bearer token. The PR #111 guard correctly
  routes fetch_market_snapshot to api.tradier.com (not sandbox), but the
  sandbox token is rejected by the live Tradier endpoint → HTTP 401.
  Result: every overnight signal for paper accounts gets
  OVERNIGHT_DAILY_INVALIDATED / DATA_UNAVAILABLE before the session starts.

Fix:
  _fetch_polygon_daily_snapshot uses requests + POLYGON_API_KEY.
  No broker dependency. Works identically for paper and live accounts.
  Uses `day.h` / `day.l` — the same daily-bar H/L the scanner writes
  to ap_signals.prior_day_high / prior_day_low.

Spec tests:
  1. Good Polygon snapshot → MarketSnapshot with day_high/day_low.
  2. PUT invalidated: day.h > prior_day_high.
  3. CALL invalidated: day.l < prior_day_low.
  4. Polygon HTTP error → None → RETRY_LATER (no hard invalidation).
  5. day.h / day.l both zero → None → RETRY_LATER (session not started).
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]


def _import_validator():
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "ap.overnight_daily_validator",
        _REPO / "ap" / "overnight_daily_validator.py",
    )
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ap.overnight_daily_validator"] = mod
    spec.loader.exec_module(mod)
    return mod


def _polygon_resp(day_h: float, day_l: float, last: float = 0.0, status: int = 200):
    """Build a mock requests.Response for the Polygon snapshot endpoint."""
    r = MagicMock()
    r.status_code = status
    r.json.return_value = {
        "ticker": {
            "day": {"h": day_h, "l": day_l, "c": last or day_l},
            "lastTrade": {"p": last},
        }
    }
    return r


# ─────────────────────────────────────────────────────────────────────────────
# 1. Good Polygon snapshot → MarketSnapshot with correct day H/L
# ─────────────────────────────────────────────────────────────────────────────

def test_1_good_polygon_snapshot_builds_market_snapshot():
    """
    When Polygon returns a valid snapshot with day.h / day.l, fetch_market_snapshot
    must return a MarketSnapshot with session_high_so_far=day.h, session_low_so_far=day.l.
    These are the daily-bar H/L consistent with the scanner's prior_day reference.
    """
    mod = _import_validator()
    with patch("ap.overnight_daily_validator.POLYGON_API_KEY", "test-key"), \
         patch("ap.overnight_daily_validator.requests.get") as mock_get:
        mock_get.return_value = _polygon_resp(day_h=72.50, day_l=69.80, last=71.20)
        snap = mod.fetch_market_snapshot("DXCM", broker=None)

    assert snap is not None, "valid Polygon snapshot must produce a MarketSnapshot"
    assert snap.session_high_so_far == 72.50
    assert snap.session_low_so_far  == 69.80
    assert snap.last_price          == 71.20
    assert snap.ticker              == "DXCM"

    # Confirm Polygon was called (not Tradier)
    url_called = mock_get.call_args[0][0]
    assert "polygon.io" in url_called, f"must call Polygon, not Tradier. Got: {url_called}"
    assert "DXCM" in url_called


# ─────────────────────────────────────────────────────────────────────────────
# 2. PUT invalidated: today's high already above prior_day_high
# ─────────────────────────────────────────────────────────────────────────────

def test_2_put_invalidated_when_session_high_above_prior_day_high():
    """
    PUT thesis: invalidated when session_high_so_far > prior_day_high.
    Strat rule: if price already broke prior-day high before our entry,
    the bearish structure is compromised.
    """
    mod = _import_validator()
    prior_high = 70.27
    prior_low  = 68.72

    with patch("ap.overnight_daily_validator.POLYGON_API_KEY", "test-key"), \
         patch("ap.overnight_daily_validator.requests.get") as mock_get:
        # Today's session high has already pierced prior_day_high
        mock_get.return_value = _polygon_resp(day_h=70.90, day_l=69.00, last=70.50)
        result = mod.validate_overnight_daily_signal(
            ticker="DXCM",
            side="PUT",
            prior_day_high=prior_high,
            prior_day_low=prior_low,
            snapshot=mod.fetch_market_snapshot("DXCM"),
        )

    assert result.valid is False, (
        "PUT must be INVALIDATED when day.h > prior_day_high"
    )
    assert result.reason_code is not None
    assert "INVALIDATED" in str(result.reason_code).upper() or \
           result.valid is False, f"Got: {result}"


# ─────────────────────────────────────────────────────────────────────────────
# 3. CALL invalidated: today's low already below prior_day_low
# ─────────────────────────────────────────────────────────────────────────────

def test_3_call_invalidated_when_session_low_below_prior_day_low():
    """
    CALL thesis: invalidated when session_low_so_far < prior_day_low.
    Strat rule: if the market already broke the prior-day low before our
    entry, the bullish structure is compromised.
    """
    mod = _import_validator()
    prior_high = 70.27
    prior_low  = 68.72

    with patch("ap.overnight_daily_validator.POLYGON_API_KEY", "test-key"), \
         patch("ap.overnight_daily_validator.requests.get") as mock_get:
        # Today's low has already breached prior_day_low
        mock_get.return_value = _polygon_resp(day_h=70.00, day_l=68.00, last=68.50)
        snap = mod.fetch_market_snapshot("DXCM")
        result = mod.validate_overnight_daily_signal(
            ticker="DXCM",
            side="CALL",
            prior_day_high=prior_high,
            prior_day_low=prior_low,
            snapshot=snap,
        )

    assert result.valid is False, (
        "CALL must be INVALIDATED when day.l < prior_day_low"
    )


def test_3b_put_still_valid_when_high_below_prior_day_high():
    """PUT thesis intact: today's high is still below prior_day_high → VALID."""
    mod = _import_validator()

    with patch("ap.overnight_daily_validator.POLYGON_API_KEY", "test-key"), \
         patch("ap.overnight_daily_validator.requests.get") as mock_get:
        # day.h = 69.50 < prior_day_high = 70.27 → bearish structure intact
        mock_get.return_value = _polygon_resp(day_h=69.50, day_l=68.80, last=69.10)
        snap = mod.fetch_market_snapshot("DXCM")
        result = mod.validate_overnight_daily_signal(
            ticker="DXCM",
            side="PUT",
            prior_day_high=70.27,
            prior_day_low=68.72,
            snapshot=snap,
        )

    assert result.valid is True, (
        "PUT must remain VALID when day.h is still below prior_day_high"
    )


def test_3c_call_still_valid_when_low_above_prior_day_low():
    """CALL thesis intact: today's low is still above prior_day_low → VALID."""
    mod = _import_validator()

    with patch("ap.overnight_daily_validator.POLYGON_API_KEY", "test-key"), \
         patch("ap.overnight_daily_validator.requests.get") as mock_get:
        # day.l = 69.10 > prior_day_low = 68.72 → bullish structure intact
        mock_get.return_value = _polygon_resp(day_h=70.00, day_l=69.10, last=69.80)
        snap = mod.fetch_market_snapshot("DXCM")
        result = mod.validate_overnight_daily_signal(
            ticker="DXCM",
            side="CALL",
            prior_day_high=70.27,
            prior_day_low=68.72,
            snapshot=snap,
        )

    assert result.valid is True, (
        "CALL must remain VALID when day.l is still above prior_day_low"
    )


# ─────────────────────────────────────────────────────────────────────────────
# 4. Polygon HTTP error → None → RETRY_LATER (not hard invalidation)
# ─────────────────────────────────────────────────────────────────────────────

def test_4_polygon_http_error_returns_none_retry_later():
    """
    When Polygon returns a non-200 status (or raises), fetch_market_snapshot
    must return None — which the reeval interprets as RETRY_LATER, not as
    a hard overnight invalidation. This prevents a Polygon outage from
    killing all overnight signals.
    """
    mod = _import_validator()

    for status in (401, 429, 500, 503):
        with patch("ap.overnight_daily_validator.POLYGON_API_KEY", "test-key"), \
             patch("ap.overnight_daily_validator.requests.get") as mock_get:
            mock_get.return_value = _polygon_resp(day_h=0, day_l=0, status=status)
            snap = mod.fetch_market_snapshot("DXCM")

        assert snap is None, (
            f"HTTP {status} from Polygon must produce None (→ RETRY_LATER), not a snapshot"
        )


def test_4b_polygon_connection_error_returns_none():
    """Network error (timeout, connection refused) → None → RETRY_LATER."""
    mod = _import_validator()

    with patch("ap.overnight_daily_validator.POLYGON_API_KEY", "test-key"), \
         patch("ap.overnight_daily_validator.requests.get", side_effect=ConnectionError("timeout")):
        snap = mod.fetch_market_snapshot("DXCM")

    assert snap is None


# ─────────────────────────────────────────────────────────────────────────────
# 5. day.h / day.l both zero → None → RETRY_LATER (session not started)
# ─────────────────────────────────────────────────────────────────────────────

def test_5_zero_day_hilo_returns_none():
    """
    Before market open, Polygon's snapshot will have day.h=0 / day.l=0
    (no trades yet). fetch_market_snapshot must return None → RETRY_LATER.
    This is the pre-9:30 AM window — equivalent to the old 'no bars yet' path.
    """
    mod = _import_validator()

    with patch("ap.overnight_daily_validator.POLYGON_API_KEY", "test-key"), \
         patch("ap.overnight_daily_validator.requests.get") as mock_get:
        # Pre-market: day object present but H/L are zero
        mock_get.return_value = _polygon_resp(day_h=0.0, day_l=0.0, last=69.50)
        snap = mod.fetch_market_snapshot("DXCM")

    assert snap is None, (
        "day.h=0 / day.l=0 must return None (→ RETRY_LATER), not a snapshot with zeros"
    )


def test_5b_missing_polygon_api_key_returns_none():
    """
    If POLYGON_API_KEY is not set, fetch_market_snapshot must return None
    (→ RETRY_LATER). Never crashes and never hard-invalidates.
    """
    mod = _import_validator()

    with patch("ap.overnight_daily_validator.POLYGON_API_KEY", ""):
        snap = mod.fetch_market_snapshot("DXCM")

    assert snap is None, "missing POLYGON_API_KEY must return None (→ RETRY_LATER)"


# ─────────────────────────────────────────────────────────────────────────────
# Source-level: confirm timesales is gone from fetch_market_snapshot body
# ─────────────────────────────────────────────────────────────────────────────

def test_source_timesales_not_in_fetch_market_snapshot():
    """
    fetch_market_snapshot must no longer CALL timesales.
    The word may appear in docstrings (explaining what changed),
    but no actual /v1/markets/timesales endpoint call should be in the body.
    """
    import ast
    src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
    tree = ast.parse(src)

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "fetch_market_snapshot":
            func_src = ast.get_source_segment(src, node) or ""
            # The actual Tradier call pattern — must not appear
            assert "/v1/markets/timesales" not in func_src, (
                "fetch_market_snapshot must not call /v1/markets/timesales after PR #181"
            )
            assert "_fetch_intraday_bars" not in func_src, (
                "fetch_market_snapshot must not call _fetch_intraday_bars after PR #181"
            )
            return

    pytest.fail("fetch_market_snapshot not found in source")


def test_source_polygon_snapshot_in_fetch_market_snapshot():
    """_fetch_polygon_daily_snapshot must be called from fetch_market_snapshot."""
    src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
    assert "_fetch_polygon_daily_snapshot" in src, (
        "_fetch_polygon_daily_snapshot must exist in overnight_daily_validator.py"
    )
    assert "POLYGON_API_KEY" in src, (
        "POLYGON_API_KEY constant must exist in overnight_daily_validator.py"
    )


def test_source_broker_no_longer_required_for_snapshot():
    """broker parameter must now be optional (default=None) in fetch_market_snapshot."""
    import ast
    src = (_REPO / "ap" / "overnight_daily_validator.py").read_text()
    tree = ast.parse(src)
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == "fetch_market_snapshot":
            for arg in node.args.args:
                if arg.arg == "broker":
                    # Must have a default value (None) — args with defaults are
                    # the last len(defaults) args
                    n_args     = len(node.args.args)
                    n_defaults = len(node.args.defaults)
                    # defaults align to the last n_defaults args
                    args_with_defaults = node.args.args[n_args - n_defaults:]
                    arg_names_with_defaults = [a.arg for a in args_with_defaults]
                    assert "broker" in arg_names_with_defaults, (
                        "broker must have a default value (=None) after PR #181 "
                        "so callers that don't have a live broker don't crash"
                    )
                    return
    pytest.fail("fetch_market_snapshot or broker arg not found")
