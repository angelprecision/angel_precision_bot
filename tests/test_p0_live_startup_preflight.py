"""
P0 (PR #261): LIVE startup preflight fail-closed.

Existing LIVE startup already fail-closed on missing token/account_id/
DATABASE_URL, unsafe flags, default encryption key, and incomplete risk
profile. What it did NOT verify is that credentials actually WORK and that
market data is reachable: a LIVE runner with a revoked token started anyway
and calibrated capital gates to a fake $25K default equity — silent
degraded startup, false confidence, dead trade flow.

These tests drive _run_live_preflight() directly with stub brokers, and
source-fence the fatal LIVE equity fallback + manifest exposure.
"""

import os
import pathlib
import re
import sys
from unittest.mock import MagicMock

import pytest

_REPO = pathlib.Path(__file__).resolve().parents[1]
SRC = (_REPO / "client_runner.py").read_text()

# ─── Mock heavy imports BEFORE importing client_runner (repo pattern,
#     copied from test_paper_selector_market_data_transport.py) ─────────────
os.environ.setdefault("DATABASE_URL", "postgresql://stub:stub@localhost/stub")
for _m in (
    "psycopg2", "psycopg2.extras", "psycopg2.pool", "supabase",
    "cryptography", "cryptography.fernet", "ap.db", "ap.queue",
    "ap.order_monitor", "ap.position_sizer", "ap.market_intelligence",
):
    sys.modules.setdefault(_m, MagicMock())


# ── Load the preflight without full runner init (repo pattern) ──────────────

def _make_runner(monkeypatch, mode="LIVE", email="jason@x.com", account="ACC1"):
    import types
    import client_runner as cr

    r = cr.ClientRunner.__new__(cr.ClientRunner)
    r.email = email
    r.mode = mode
    r.account_id = account
    r.base_url = "https://api.tradier.com"
    return r


class _Broker:
    def __init__(self, equity=12500.0, quote=None, equity_exc=None, quote_exc=None):
        self._equity = equity
        self._quote = quote if quote is not None else {"last": 552.31}
        self._equity_exc = equity_exc
        self._quote_exc = quote_exc

    def get_account_equity(self):
        if self._equity_exc:
            raise self._equity_exc
        return self._equity

    def get_quote(self, symbol):
        if self._quote_exc:
            raise self._quote_exc
        return self._quote


class _BrokerNoEquity:
    def get_quote(self, symbol):
        return {"last": 552.31}


# ── Spec: fail-closed matrix ─────────────────────────────────────────────────

def test_missing_token_path_precedes_preflight():
    """no_token fail-close is upstream of preflight and must remain."""
    assert '_mark_failed("no_token")' in SRC


def test_missing_account_id_fails(monkeypatch):
    monkeypatch.delenv("LIVE_PREFLIGHT_ENABLED", raising=False)
    r = _make_runner(monkeypatch, account="")
    ok, reason = r._run_live_preflight(_Broker())
    assert not ok and reason == "missing_account_id"


def test_auth_profile_error_fails(monkeypatch):
    monkeypatch.delenv("LIVE_PREFLIGHT_ENABLED", raising=False)
    r = _make_runner(monkeypatch)
    ok, reason = r._run_live_preflight(_Broker(equity_exc=RuntimeError("401")))
    assert not ok and reason.startswith("broker_auth_profile_unreadable")
    assert r.live_preflight_status.startswith("failed:")


def test_zero_equity_fails(monkeypatch):
    monkeypatch.delenv("LIVE_PREFLIGHT_ENABLED", raising=False)
    r = _make_runner(monkeypatch)
    ok, reason = r._run_live_preflight(_Broker(equity=0.0))
    assert not ok and reason == "account_equity_not_positive"


def test_broker_without_equity_method_fails(monkeypatch):
    monkeypatch.delenv("LIVE_PREFLIGHT_ENABLED", raising=False)
    r = _make_runner(monkeypatch)
    ok, reason = r._run_live_preflight(_BrokerNoEquity())
    assert not ok and reason == "broker_missing_get_account_equity"


def test_quote_error_fails(monkeypatch):
    monkeypatch.delenv("LIVE_PREFLIGHT_ENABLED", raising=False)
    r = _make_runner(monkeypatch)
    ok, reason = r._run_live_preflight(_Broker(quote_exc=RuntimeError("timeout")))
    assert not ok and reason.startswith("market_data_unusable")


def test_zero_quote_fails(monkeypatch):
    monkeypatch.delenv("LIVE_PREFLIGHT_ENABLED", raising=False)
    r = _make_runner(monkeypatch)
    ok, reason = r._run_live_preflight(_Broker(quote={"last": 0, "bid": 0}))
    assert not ok and reason == "market_data_quote_not_positive"


def test_mode_not_live_fails_the_preflight_itself(monkeypatch):
    monkeypatch.delenv("LIVE_PREFLIGHT_ENABLED", raising=False)
    r = _make_runner(monkeypatch, mode="PAPER")
    ok, reason = r._run_live_preflight(_Broker())
    assert not ok and reason.startswith("execution_mode_not_live")


# ── Spec: successful live config starts ──────────────────────────────────────

def test_valid_live_config_passes(monkeypatch):
    monkeypatch.delenv("LIVE_PREFLIGHT_ENABLED", raising=False)
    r = _make_runner(monkeypatch)
    ok, reason = r._run_live_preflight(_Broker(equity=8421.55, quote={"last": 552.31}))
    assert ok and reason == "ok"
    assert r.live_preflight_status == "ok"


def test_quote_price_falls_through_fields(monkeypatch):
    monkeypatch.delenv("LIVE_PREFLIGHT_ENABLED", raising=False)
    r = _make_runner(monkeypatch)
    ok, _ = r._run_live_preflight(_Broker(quote={"last": None, "bid": 0, "ask": 552.10}))
    assert ok


# ── Kill switch is loud, never silent ────────────────────────────────────────

def test_kill_switch_logs_critical_and_passes(monkeypatch):
    monkeypatch.setenv("LIVE_PREFLIGHT_ENABLED", "0")
    r = _make_runner(monkeypatch)
    ok, reason = r._run_live_preflight(_Broker(equity_exc=RuntimeError("401")))
    assert ok and reason == "disabled_by_env"
    assert r.live_preflight_status == "disabled_by_env"
    assert "LIVE_PREFLIGHT_DISABLED_BY_ENV" in SRC


# ── Source fences: wiring, fatal LIVE equity fallback, paper untouched ──────

def test_preflight_called_after_broker_construction_live_only():
    m = re.search(
        r"self\.broker = broker(.*?)_run_live_preflight\(broker\)", SRC, re.S
    )
    assert m and 'strip().upper() == "LIVE"' in m.group(1)
    assert '_mark_failed(f"LIVE_PREFLIGHT_FAILED:{_pf_reason}")' in SRC


def test_live_equity_fallback_is_fatal_all_three_branches():
    assert 'LIVE_PREFLIGHT_FAILED:equity_zero_at_calibration' in SRC
    assert 'LIVE_PREFLIGHT_FAILED:broker_missing_get_account_equity' in SRC
    assert 'LIVE_PREFLIGHT_FAILED:equity_fetch_failed' in SRC


def test_paper_equity_fallback_preserved():
    """Paper may still degrade to the default equity — LIVE cannot."""
    assert "Tradier returned zero equity — using default" in SRC
    assert "Equity fetch at startup failed" in SRC


def test_manifest_exposes_preflight_status():
    assert '"live_preflight_status": getattr(self, "live_preflight_status", "not_applicable")' in SRC


def test_markers_present():
    assert "LIVE_PREFLIGHT_OK" in SRC
    assert "LIVE_PREFLIGHT_FAILED reason=" in SRC
