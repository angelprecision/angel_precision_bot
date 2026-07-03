from __future__ import annotations

import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _stub(name: str):
    sys.modules.setdefault(name, MagicMock())


_stub("psycopg2")
_stub("psycopg2.extras")
_stub("psycopg2.pool")
_stub("supabase")
_stub("cryptography")
_stub("cryptography.fernet")
_stub("ap.db")
_stub("ap.queue")
_stub("ap.order_monitor")
_stub("ap.position_sizer")
_stub("ap.market_intelligence")
_stub("ap.worker_health")
_stub("ap_reconciler")
_stub("ap_recovery")
_stub("ap.self_healing")

sys.modules["supabase"].create_client = MagicMock()
sys.modules["supabase"].Client = MagicMock()
sys.modules["cryptography.fernet"].Fernet = MagicMock()
sys.modules["ap.db"].run_with_retry = MagicMock()
sys.modules["ap.queue"].enqueue_signal = MagicMock()
sys.modules["ap.queue"].worker_loop = MagicMock()
sys.modules["ap.order_monitor"].APOrderMonitor = MagicMock()
sys.modules["ap.position_sizer"].APPositionSizer = MagicMock()
sys.modules["ap.market_intelligence"].APEarningsGuard = MagicMock()
sys.modules["ap.market_intelligence"].APIVRankFilter = MagicMock()
sys.modules["ap.worker_health"].get_monitor = MagicMock()
sys.modules["ap.worker_health"].init_monitor = MagicMock()
sys.modules["ap_reconciler"].APBrokerReconciler = MagicMock()
sys.modules["ap_recovery"].APStartupRecovery = MagicMock()
sys.modules["ap.self_healing"].get_healer = MagicMock()
sys.modules["ap.self_healing"].init_self_healing = MagicMock()

import client_runner as _cr  # noqa: E402


class _Cfg:
    def __init__(self, *, base_url: str):
        self.base_url = base_url


class _ExecutionBroker:
    def __init__(self, *, equity=50000.0, quote=None, equity_exc: Exception | None = None, quote_exc: Exception | None = None):
        self._equity = equity
        self._quote = quote if quote is not None else {"last": 600.0}
        self._equity_exc = equity_exc
        self._quote_exc = quote_exc
        self.cfg = _Cfg(base_url="https://api.tradier.com")

    def get_account_equity(self):
        if self._equity_exc:
            raise self._equity_exc
        return self._equity

    def get_quote(self, symbol: str):
        if self._quote_exc:
            raise self._quote_exc
        return self._quote


class _DataBroker:
    def __init__(self, *, quote=None, quote_exc: Exception | None = None, base_url: str = "https://api.tradier.com"):
        self._quote = quote if quote is not None else {"last": 601.0}
        self._quote_exc = quote_exc
        self.cfg = _Cfg(base_url=base_url)

    def get_quote(self, symbol: str):
        if self._quote_exc:
            raise self._quote_exc
        return self._quote


def test_live_preflight_uses_dedicated_data_broker_for_quotes():
    execution_broker = _ExecutionBroker(quote_exc=RuntimeError("execution quote 401"))
    data_broker = _DataBroker(quote={"bid": 600.0, "ask": 602.0})
    out = _cr._run_live_startup_preflight(
        client_id="live@test",
        execution_broker=execution_broker,
        data_broker=data_broker,
        quote_broker_source="TRADIER_MARKET_DATA_TOKEN",
    )
    assert out["status"] == "passed"
    assert out["equity"] == 50000.0
    assert out["quote_source"] == "TRADIER_MARKET_DATA_TOKEN"
    assert out["quote_broker_class"] == "_DataBroker"
    assert out["quote_price"] == 601.0


def test_live_preflight_fails_when_both_execution_and_data_quotes_fail():
    execution_broker = _ExecutionBroker()
    data_broker = _DataBroker(quote_exc=RuntimeError("data quote 401"))
    with pytest.raises(RuntimeError, match="market-data quote preflight failed"):
        _cr._run_live_startup_preflight(
            client_id="live@test",
            execution_broker=execution_broker,
            data_broker=data_broker,
            quote_broker_source="TRADIER_MARKET_DATA_TOKEN",
        )


def test_live_preflight_falls_back_to_execution_broker_when_no_data_broker():
    execution_broker = _ExecutionBroker(quote={"last": 598.25})
    out = _cr._run_live_startup_preflight(
        client_id="live@test",
        execution_broker=execution_broker,
        data_broker=execution_broker,
        quote_broker_source="execution_broker_fallback_live",
    )
    assert out["status"] == "passed"
    assert out["quote_source"] == "execution_broker_fallback_live"
    assert out["quote_broker_class"] == "_ExecutionBroker"
    assert out["quote_price"] == 598.25


def test_live_preflight_fails_when_equity_fetch_fails():
    execution_broker = _ExecutionBroker(equity_exc=RuntimeError("balances 401"))
    with pytest.raises(RuntimeError, match="equity preflight failed"):
        _cr._run_live_startup_preflight(
            client_id="live@test",
            execution_broker=execution_broker,
            data_broker=_DataBroker(),
            quote_broker_source="TRADIER_MARKET_DATA_TOKEN",
        )


def test_manifest_records_live_preflight_status_and_market_data_source():
    runner = object.__new__(_cr.ClientRunner)
    runner.email = "live@test"
    runner.account_id = "LIVE123"
    runner.mode = "LIVE"
    runner.base_url = "https://api.tradier.com"
    runner.core = None
    runner.position_manager = None
    runner.order_state_machine = None
    runner.contract_selector = None
    runner.order_monitor = None
    runner.quotemonitor = None
    runner.quote_monitor = None
    runner.fill_monitor_thread = None
    runner.worker_thread = None
    runner.equity_thread = None
    runner.reconciler = None
    runner.startup_manifest = {}

    runner._build_startup_manifest(
        equity=50000.0,
        max_trades=5,
        max_pos=2,
        max_loss=-2500.0,
        throttle_threshold=-1000.0,
        stop_threshold=-2500.0,
        data_broker_is_dedicated=True,
        exit_eng=None,
        live_preflight_status="passed",
        quote_preflight_source="TRADIER_MARKET_DATA_TOKEN",
        quote_preflight_broker_class="TradierBroker",
        quote_preflight_base_url="https://api.tradier.com",
        quote_preflight_symbol="SPY",
        quote_preflight_price=601.0,
    )

    assert runner.startup_manifest["live_preflight_status"] == "passed"
    assert runner.startup_manifest["quote_preflight_source"] == "TRADIER_MARKET_DATA_TOKEN"
    assert runner.startup_manifest["quote_preflight_broker_class"] == "TradierBroker"


def test_paper_path_not_forced_through_live_preflight():
    src = (Path(__file__).resolve().parents[1] / "client_runner.py").read_text()
    live_block_match = re.search(
        r'if self\.mode == "LIVE":(.*?)(?=\n        self\._clear_old_phantom_orders\(\))',
        src,
        re.DOTALL,
    )
    assert live_block_match, "Could not isolate LIVE startup block"
    live_block = live_block_match.group(1)
    assert "resolve_market_data_transport(" in live_block
    assert "_run_live_startup_preflight(" in live_block
