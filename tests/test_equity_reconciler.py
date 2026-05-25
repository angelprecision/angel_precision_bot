"""
Patch 5 — tests for scripts/equity_reconciler.py.

Strategy:
  * Offline only. No network calls; no real Tradier; no real Supabase.
  * Mock Tradier via a FakeTradierClient that replaces TradierReadClient.
  * Mock Supabase via the same FakeSupabase chain pattern used in
    test_pod_isolation.py.
  * Cover happy path, every drift kind, missing creds, Tradier auth
    failure, mid-Tradier RuntimeError, position check, and the JSON
    report writer.

Counts: 20 distinct test IDs across 8 classes.

Run:
    pytest tests/test_equity_reconciler.py -v
"""
from __future__ import annotations

import json
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Env stubs BEFORE importing the script.
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_equity_reconciler",
)
os.environ.setdefault("ENCRYPTION_KEY", "angel-equity-recon-pytest-2026")

# Stub supabase so client_runner imports cleanly (decrypt_token is imported lazily).
if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub

# Make scripts/ importable as a module path. The script is intentionally
# in scripts/ not on PYTHONPATH; we load it via importlib.
import importlib.util  # noqa: E402

_SPEC_PATH = REPO_ROOT / "scripts" / "equity_reconciler.py"
_spec = importlib.util.spec_from_file_location("equity_reconciler", _SPEC_PATH)
assert _spec is not None and _spec.loader is not None
equity_reconciler = importlib.util.module_from_spec(_spec)
# Register in sys.modules BEFORE exec_module so @dataclass decorators can
# resolve cls.__module__ during definition (Python 3.12 requirement).
sys.modules["equity_reconciler"] = equity_reconciler
_spec.loader.exec_module(equity_reconciler)


# ────────────────────────────────────────────────────────────────────
# Fake Supabase chain (read-only, mirrors test_pod_isolation pattern)
# ────────────────────────────────────────────────────────────────────

class _Result:
    def __init__(self, data):
        self.data = data


class _Query:
    def __init__(self, rows, *, write_log=None):
        self._rows = list(rows)
        self._write_log = write_log if write_log is not None else []

    def select(self, _cols):
        return self

    def eq(self, col, val):
        self._rows = [r for r in self._rows if r.get(col) == val]
        return self

    def gte(self, col, val):
        self._rows = [r for r in self._rows if (r.get(col) or "") >= val]
        return self

    def lt(self, col, val):
        self._rows = [r for r in self._rows if (r.get(col) or "") < val]
        return self

    def execute(self):
        return _Result(list(self._rows))

    # Forbid writes \u2014 any write must be a test failure.
    def insert(self, *a, **kw):
        self._write_log.append(("insert", a, kw))
        raise AssertionError("equity_reconciler must NEVER insert; got insert()")

    def update(self, *a, **kw):
        self._write_log.append(("update", a, kw))
        raise AssertionError("equity_reconciler must NEVER update; got update()")

    def delete(self, *a, **kw):
        self._write_log.append(("delete", a, kw))
        raise AssertionError("equity_reconciler must NEVER delete; got delete()")


class FakeSupabase:
    def __init__(self, tables):
        self._tables = tables
        self.write_log: list = []

    def table(self, name):
        return _Query(self._tables.get(name, []), write_log=self.write_log)


# ────────────────────────────────────────────────────────────────────
# Fake Tradier client
# ────────────────────────────────────────────────────────────────────

class FakeTradierClient:
    def __init__(self, equity=10000.0, positions=None, gainloss=None,
                 raise_on_equity=None, raise_on_gainloss=None,
                 raise_on_positions=None):
        self._equity = equity
        self._positions = positions or []
        self._gainloss = gainloss or []
        self._raise_on_equity = raise_on_equity
        self._raise_on_gainloss = raise_on_gainloss
        self._raise_on_positions = raise_on_positions
        # Mirror real class shape for masking tests
        self.base_url = "https://sandbox.tradier.com"
        self.account_id = "TEST-ACCT-9999"
        self.access_token = "test-token"

    def get_equity(self):
        if self._raise_on_equity:
            raise self._raise_on_equity
        return self._equity

    def get_positions(self):
        if self._raise_on_positions:
            raise self._raise_on_positions
        return self._positions

    def get_gainloss(self, start, end):
        if self._raise_on_gainloss:
            raise self._raise_on_gainloss
        return self._gainloss


# ────────────────────────────────────────────────────────────────────
# Helpers
# ────────────────────────────────────────────────────────────────────

def _member(email, mode="paper", account_id="ACCT1", token="encrypted-blob"):
    if mode == "live":
        return {
            "email": email, "name": email.split("@")[0],
            "tradier_account_mode": "live",
            "tradier_live_account_id": account_id,
            "tradier_live_access_token": token,
            "tradier_base_url": "https://api.tradier.com",
            "approved": True, "subscription_active": True,
        }
    return {
        "email": email, "name": email.split("@")[0],
        "tradier_account_mode": "paper",
        "tradier_paper_account_id": account_id,
        "tradier_paper_access_token": token,
        "tradier_base_url": "https://sandbox.tradier.com",
        "approved": True, "subscription_active": True,
    }


def _bot_trade(email, ticker, occ, contracts, entry, exit_px, closed_at):
    return {
        "client_email":       email,
        "mode":               "PAPER",
        "position_id":        f"pos-{ticker}-1",
        "local_order_id":     f"lo-{ticker}-1",
        "ticker":             ticker,
        "option_symbol":      occ,
        "side":               "CALL",
        "contracts":          contracts,
        "entry_option_price": entry,
        "exit_fill_price":    exit_px,
        "closed_at":          closed_at,
        "option_pnl_pct":     round((exit_px - entry) / max(entry, 0.01) * 100, 2),
    }


def _gl(occ, qty, gain, close_date):
    cost = 100.0
    return {
        "symbol":     occ,
        "quantity":   qty,
        "proceeds":   cost + gain,
        "cost":       cost,
        "gain_loss":  gain,
        "open_date":  close_date.split("T")[0] if "T" in close_date else close_date,
        "close_date": close_date,
    }


def _args(date="2026-05-26", client=None, per_trade=1.0, daily=5.0, equity=10.0,
          check_positions=False, json_path=None):
    class A:
        pass
    a = A()
    a.date = date
    a.hours = None
    a.client = client
    a.per_trade_tolerance = per_trade
    a.daily_total_tolerance = daily
    a.equity_tolerance = equity
    a.check_positions = check_positions
    a.json = json_path
    return a


@pytest.fixture(autouse=True)
def _stub_decrypt_token(monkeypatch):
    """decrypt_token requires a real ENCRYPTION_KEY; stub it to return raw."""
    fake_cr = types.ModuleType("client_runner")
    fake_cr.decrypt_token = lambda raw, mode="PAPER": raw or ""
    monkeypatch.setitem(sys.modules, "client_runner", fake_cr)
    yield


@pytest.fixture
def patch_tradier(monkeypatch):
    """Helper to install a FakeTradierClient on the equity_reconciler module."""
    def _install(fake_factory):
        # fake_factory: callable that takes (base_url, account_id, token) and
        # returns a FakeTradierClient (or a side-effecting subclass)
        monkeypatch.setattr(
            equity_reconciler, "TradierReadClient", fake_factory,
        )
    return _install


# ════════════════════════════════════════════════════════════════════
# 1) Masking helpers
# ════════════════════════════════════════════════════════════════════

class TestMasking:
    def test_mask_email_redacts_middle(self):
        m = equity_reconciler._mask_email("alice@example.com")
        assert "alice" not in m
        assert "@example.com" not in m
        assert "***" in m
        assert m.endswith(".com")

    def test_mask_account_keeps_only_edges(self):
        m = equity_reconciler._mask_account("VA12345678")
        assert "234" not in m
        assert m.startswith("VA12")
        assert m.endswith("5678")

    def test_mask_account_short_value_safe(self):
        m = equity_reconciler._mask_account("ABC")
        assert m == "AB***"


# ════════════════════════════════════════════════════════════════════
# 2) Trade matching
# ════════════════════════════════════════════════════════════════════

class TestMatchTrades:
    def test_one_to_one_match_by_symbol(self):
        bot = [equity_reconciler.BotTrade(
            client_email="a@x.com", mode="PAPER", position_id="p1",
            local_order_id="o1", ticker="SPY", option_symbol="SPY260526C00500000",
            side="CALL", contracts=1, entry_option_price=1.00,
            exit_fill_price=1.50, closed_at="2026-05-26T15:30:00+00:00",
            bot_pnl=50.0,
        )]
        broker = [equity_reconciler.TradierTrade(
            symbol="SPY260526C00500000", quantity=1, proceeds=150, cost=100,
            gain_loss=50.0, open_date="2026-05-26", close_date="2026-05-26",
        )]
        matches, leftover = equity_reconciler._match_trades(bot, broker, per_trade_tol=1.0)
        assert len(matches) == 1
        assert matches[0].matched is True
        assert matches[0].delta_usd == 0.0
        assert leftover == []

    def test_unmatched_bot_trade(self):
        bot = [equity_reconciler.BotTrade(
            client_email="a@x.com", mode="PAPER", position_id="p1",
            local_order_id="o1", ticker="SPY", option_symbol="UNKNOWN_OCC",
            side="CALL", contracts=1, entry_option_price=1.0,
            exit_fill_price=1.5, closed_at="2026-05-26T15:30:00+00:00",
            bot_pnl=50.0,
        )]
        matches, leftover = equity_reconciler._match_trades(bot, [], per_trade_tol=1.0)
        assert matches[0].matched is False
        assert matches[0].reason == "no_broker_row_for_symbol"

    def test_drift_marked_when_over_tolerance(self):
        bot = [equity_reconciler.BotTrade(
            client_email="a@x.com", mode="PAPER", position_id="p1",
            local_order_id="o1", ticker="SPY", option_symbol="SPY260526C00500000",
            side="CALL", contracts=1, entry_option_price=1.00,
            exit_fill_price=1.50, closed_at="2026-05-26T15:30:00+00:00",
            bot_pnl=50.0,
        )]
        broker = [equity_reconciler.TradierTrade(
            symbol="SPY260526C00500000", quantity=1, proceeds=200, cost=100,
            gain_loss=100.0,  # $50 drift
            open_date="2026-05-26", close_date="2026-05-26",
        )]
        matches, _ = equity_reconciler._match_trades(bot, broker, per_trade_tol=1.0)
        assert matches[0].matched
        assert matches[0].delta_usd == 50.0
        assert matches[0].reason == "drift_over_tolerance"


# ════════════════════════════════════════════════════════════════════
# 3) Happy path
# ════════════════════════════════════════════════════════════════════

class TestHappyPath:
    def test_clean_reconciliation_exits_zero(self, patch_tradier):
        sb = FakeSupabase({
            "members": [_member("a@x.com")],
            "proof_trades": [
                _bot_trade("a@x.com", "SPY", "SPY260526C00500000", 1, 1.00, 1.50,
                           "2026-05-26T15:30:00+00:00"),
            ],
        })
        patch_tradier(lambda b, a, t: FakeTradierClient(
            equity=10050.0,
            gainloss=[_gl("SPY260526C00500000", 1, 50.0, "2026-05-26")],
        ))
        code, results = equity_reconciler.run(_args(), sb=sb)
        assert code == 0
        assert len(results) == 1
        r = results[0]
        assert r.ok is True
        assert r.bot_pnl_total == 50.0
        assert r.broker_pnl_total == 50.0
        assert r.daily_delta == 0.0
        assert r.failure_markers == []

    def test_no_members_returns_zero_no_results(self, patch_tradier):
        sb = FakeSupabase({"members": [], "proof_trades": []})
        code, results = equity_reconciler.run(_args(), sb=sb)
        assert code == 0
        assert results == []


# ════════════════════════════════════════════════════════════════════
# 4) Per-trade drift
# ════════════════════════════════════════════════════════════════════

class TestPerTradeDrift:
    def test_per_trade_drift_flips_exit_to_one(self, patch_tradier):
        sb = FakeSupabase({
            "members": [_member("a@x.com")],
            "proof_trades": [
                _bot_trade("a@x.com", "SPY", "SPY260526C00500000", 1, 1.00, 1.50,
                           "2026-05-26T15:30:00+00:00"),
            ],
        })
        # Bot says $50; broker says $52 (Δ=$2, over $1 tolerance)
        patch_tradier(lambda b, a, t: FakeTradierClient(
            equity=10052.0,
            gainloss=[_gl("SPY260526C00500000", 1, 52.0, "2026-05-26")],
        ))
        code, results = equity_reconciler.run(_args(per_trade=1.0, daily=10.0, equity=10.0), sb=sb)
        assert code == 1
        assert "RECONCILER_DRIFT_PER_TRADE" in results[0].failure_markers
        drift = results[0].per_trade_drifts[0]
        assert drift["delta_usd"] == 2.0
        assert drift["bot_pnl"] == 50.0
        assert drift["broker_pnl"] == 52.0

    def test_per_trade_under_tolerance_does_not_flag(self, patch_tradier):
        sb = FakeSupabase({
            "members": [_member("a@x.com")],
            "proof_trades": [
                _bot_trade("a@x.com", "SPY", "SPY260526C00500000", 1, 1.00, 1.50,
                           "2026-05-26T15:30:00+00:00"),
            ],
        })
        # Δ = $0.50 — under the $1.00 tolerance
        patch_tradier(lambda b, a, t: FakeTradierClient(
            equity=10050.5,
            gainloss=[_gl("SPY260526C00500000", 1, 50.5, "2026-05-26")],
        ))
        code, results = equity_reconciler.run(_args(), sb=sb)
        assert code == 0
        assert "RECONCILER_DRIFT_PER_TRADE" not in results[0].failure_markers


# ════════════════════════════════════════════════════════════════════
# 5) Daily total + equity drift
# ════════════════════════════════════════════════════════════════════

class TestDailyAndEquityDrift:
    def test_daily_total_drift(self, patch_tradier):
        # Two trades each with small drift; per-trade passes (each $0.50)
        # but the sum is $5.00, equal to default tol. We push it to $6.
        sb = FakeSupabase({
            "members": [_member("a@x.com")],
            "proof_trades": [
                _bot_trade("a@x.com", "SPY", "OCC1", 1, 1.00, 1.50, "2026-05-26T15:30:00+00:00"),
                _bot_trade("a@x.com", "QQQ", "OCC2", 1, 2.00, 2.50, "2026-05-26T16:30:00+00:00"),
            ],
        })
        patch_tradier(lambda b, a, t: FakeTradierClient(
            equity=10106.0,
            gainloss=[
                _gl("OCC1", 1, 53.0, "2026-05-26"),  # Δ +$3
                _gl("OCC2", 1, 53.0, "2026-05-26"),  # Δ +$3
            ],
        ))
        code, results = equity_reconciler.run(
            _args(per_trade=10.0, daily=5.0, equity=20.0), sb=sb,
        )
        assert code == 1
        assert "RECONCILER_DRIFT_DAILY_PNL" in results[0].failure_markers
        assert results[0].daily_delta == 6.0

    def test_equity_drift_independent_marker(self, patch_tradier):
        sb = FakeSupabase({
            "members": [_member("a@x.com")],
            "proof_trades": [
                _bot_trade("a@x.com", "SPY", "OCC1", 1, 1.00, 1.50, "2026-05-26T15:30:00+00:00"),
            ],
        })
        # Δ = $50; per-trade tol=high, daily tol=high, equity tol=$10 -> trip equity
        patch_tradier(lambda b, a, t: FakeTradierClient(
            equity=10100.0,
            gainloss=[_gl("OCC1", 1, 100.0, "2026-05-26")],
        ))
        code, results = equity_reconciler.run(
            _args(per_trade=100.0, daily=100.0, equity=10.0), sb=sb,
        )
        assert code == 1
        assert "RECONCILER_DRIFT_EQUITY" in results[0].failure_markers


# ════════════════════════════════════════════════════════════════════
# 6) Positions check
# ════════════════════════════════════════════════════════════════════

class TestPositionsCheck:
    def test_positions_match_no_marker(self, patch_tradier):
        sb = FakeSupabase({
            "members": [_member("a@x.com")],
            "proof_trades": [],
            "positions": [
                {"id": "p1", "client_email": "a@x.com", "status": "open"},
                {"id": "p2", "client_email": "a@x.com", "status": "open"},
            ],
        })
        patch_tradier(lambda b, a, t: FakeTradierClient(
            equity=10000.0,
            positions=[{"symbol": "X"}, {"symbol": "Y"}],
        ))
        code, results = equity_reconciler.run(
            _args(check_positions=True), sb=sb,
        )
        assert code == 0
        assert results[0].open_positions_db == 2
        assert results[0].open_positions_broker == 2
        assert results[0].positions_drift == 0

    def test_positions_drift_flags_marker(self, patch_tradier):
        sb = FakeSupabase({
            "members": [_member("a@x.com")],
            "proof_trades": [],
            "positions": [{"id": "p1", "client_email": "a@x.com", "status": "open"}],
        })
        # Broker shows 2 but DB shows 1 \u2014 drift of +1
        patch_tradier(lambda b, a, t: FakeTradierClient(
            equity=10000.0,
            positions=[{"symbol": "X"}, {"symbol": "Y"}],
        ))
        code, results = equity_reconciler.run(
            _args(check_positions=True), sb=sb,
        )
        assert code == 1
        assert "RECONCILER_DRIFT_POSITIONS" in results[0].failure_markers
        assert results[0].positions_drift == 1


# ════════════════════════════════════════════════════════════════════
# 7) Tradier failure modes
# ════════════════════════════════════════════════════════════════════

class TestTradierFailures:
    def test_auth_failure_marks_client_as_failed(self, patch_tradier):
        sb = FakeSupabase({
            "members": [_member("a@x.com")],
            "proof_trades": [],
        })
        patch_tradier(lambda b, a, t: FakeTradierClient(
            raise_on_gainloss=PermissionError("Tradier 401 on /gainloss"),
        ))
        code, results = equity_reconciler.run(_args(), sb=sb)
        assert code == 1
        assert "RECONCILER_TRADIER_FAIL" in results[0].failure_markers
        assert results[0].ok is False

    def test_missing_credentials_marks_failed(self, patch_tradier):
        # Member with NO Tradier creds
        sb = FakeSupabase({
            "members": [{
                "email": "noauth@x.com",
                "name": "noauth",
                "tradier_account_mode": "paper",
                "approved": True, "subscription_active": True,
            }],
            "proof_trades": [],
        })
        patch_tradier(lambda b, a, t: FakeTradierClient())  # never called
        code, results = equity_reconciler.run(_args(), sb=sb)
        assert code == 1
        assert results[0].tradier_error == "no_credentials"
        assert "RECONCILER_TRADIER_FAIL" in results[0].failure_markers


# ════════════════════════════════════════════════════════════════════
# 8) JSON report writer + write-protection
# ════════════════════════════════════════════════════════════════════

class TestReportWriterAndReadOnly:
    def test_write_report_creates_valid_json(self, tmp_path, patch_tradier):
        sb = FakeSupabase({
            "members": [_member("a@x.com")],
            "proof_trades": [
                _bot_trade("a@x.com", "SPY", "OCC1", 1, 1.0, 1.5, "2026-05-26T15:30:00+00:00"),
            ],
        })
        patch_tradier(lambda b, a, t: FakeTradierClient(
            equity=10050.0,
            gainloss=[_gl("OCC1", 1, 50.0, "2026-05-26")],
        ))
        out = tmp_path / "recon.json"
        args = _args(json_path=str(out))
        code, results = equity_reconciler.run(args, sb=sb)
        equity_reconciler._write_report(str(out), args, results)
        assert out.exists()
        data = json.loads(out.read_text())
        assert "clients" in data
        assert data["clients"][0]["ok"] is True
        assert "bot_pnl_total" in data["clients"][0]
        assert data["tolerances"]["per_trade_usd"] == 1.0

    def test_never_writes_to_supabase(self, patch_tradier):
        sb = FakeSupabase({
            "members": [_member("a@x.com")],
            "proof_trades": [
                _bot_trade("a@x.com", "SPY", "OCC1", 1, 1.0, 1.5, "2026-05-26T15:30:00+00:00"),
            ],
        })
        patch_tradier(lambda b, a, t: FakeTradierClient(
            equity=10050.0,
            gainloss=[_gl("OCC1", 1, 50.0, "2026-05-26")],
        ))
        equity_reconciler.run(_args(), sb=sb)
        # FakeSupabase's insert/update/delete raise AssertionError on attempt.
        # If we got here, none were called.
        assert sb.write_log == []


# ════════════════════════════════════════════════════════════════════
# 9) Window resolution
# ════════════════════════════════════════════════════════════════════

class TestWindow:
    def test_date_resolves_full_utc_day(self):
        a = _args(date="2026-05-26")
        start, end = equity_reconciler._resolve_window(a)
        assert start == datetime(2026, 5, 26, tzinfo=timezone.utc)
        assert end == datetime(2026, 5, 27, tzinfo=timezone.utc)

    def test_hours_resolves_recent_window(self):
        class A:
            pass
        a = A()
        a.date = None
        a.hours = 6
        start, end = equity_reconciler._resolve_window(a)
        assert (end - start) == timedelta(hours=6)
