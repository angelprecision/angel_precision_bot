from __future__ import annotations

import os
import threading
import time
from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

os.environ.setdefault(
    "DATABASE_URL", "postgresql://test:test@localhost/test"
)

import ap.position_quote_monitor as qpm_module
import ap_health_registry
import client_runner
from ap.position_quote_monitor import APPositionQuoteMonitor


class _Broker:
    def __init__(self, base_url: str, account_id: str = "account-1"):
        self.cfg = SimpleNamespace(
            base_url=base_url, account_id=account_id
        )
        self.calls = 0

    def get_quotes(self, symbols):
        self.calls += 1
        return {
            symbol: {"symbol": symbol, "last": 100.0}
            for symbol in symbols
        }


class _ExitEngine:
    def __init__(self, positions=None, *, apply_error=None):
        self._positions = list(positions or [])
        self._lock = threading.Lock()
        self._apply_error = apply_error
        self.applied = []

    def active_positions(self):
        return self._positions

    def apply_quote_snapshots(self, snapshots):
        if self._apply_error:
            raise self._apply_error
        self.applied.append(snapshots)


def _monitor(*, client="jason@example.com", mode="live", broker=None, engine=None):
    return APPositionQuoteMonitor(
        broker=broker or _Broker("https://api.tradier.com"),
        client_id=client,
        exit_engine=engine or _ExitEngine(),
        execution_mode=mode,
        poll_interval_sec=0.001,
    )


def test_binding_requires_exact_client_mode_broker_and_exit_engine():
    broker = _Broker("https://api.tradier.com")
    engine = _ExitEngine()
    monitor = _monitor(broker=broker, engine=engine)

    assert monitor.binding_matches(
        client_id="jason@example.com",
        execution_mode="LIVE",
        broker=broker,
        exit_engine=engine,
    )
    assert not monitor.binding_matches(
        client_id="jose@example.com",
        execution_mode="live",
        broker=broker,
        exit_engine=engine,
    )
    assert not monitor.binding_matches(
        client_id="jason@example.com",
        execution_mode="paper",
        broker=broker,
        exit_engine=engine,
    )
    assert not monitor.binding_matches(
        client_id="jason@example.com",
        execution_mode="live",
        broker=_Broker("https://api.tradier.com"),
        exit_engine=engine,
    )
    assert not monitor.binding_matches(
        client_id="jason@example.com",
        execution_mode="live",
        broker=broker,
        exit_engine=_ExitEngine(),
    )


def test_binding_snapshot_is_non_secret_and_instance_specific():
    broker = _Broker("https://token:secret@api.tradier.com")
    monitor = _monitor(broker=broker)
    snapshot = monitor.binding_snapshot()

    assert snapshot["monitor_instance_id"]
    assert snapshot["client_id"] == "jason@example.com"
    assert snapshot["execution_mode"] == "live"
    assert snapshot["account_id"] == "account-1"
    assert "secret" not in str(snapshot)
    assert "token" not in str(snapshot)


def test_binding_rejects_mutated_broker_account():
    broker = _Broker("https://api.tradier.com", "account-1")
    engine = _ExitEngine()
    monitor = _monitor(broker=broker, engine=engine)
    broker.cfg.account_id = "account-2"

    assert not monitor.binding_matches(
        client_id="jason@example.com",
        execution_mode="live",
        broker=broker,
        exit_engine=engine,
    )


def test_failed_cycle_advances_attempt_and_completion_not_success(monkeypatch):
    monitor = _monitor()
    monitor._refresh_once = MagicMock(side_effect=RuntimeError("boom"))

    def stop_after_wait(timeout):
        monitor._stop.set()
        return False

    monkeypatch.setattr(qpm_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(monitor._kick, "wait", stop_after_wait)
    monitor._loop()

    assert monitor._last_cycle_attempt_ts > 0
    assert monitor._last_cycle_completed_ts >= monitor._last_cycle_attempt_ts
    assert monitor._last_cycle_success_ts == 0
    assert monitor._last_cycle_ts == 0
    assert monitor._consecutive_failures == 1
    assert "RuntimeError: boom" == monitor._last_cycle_error


def test_successful_cycle_advances_all_timestamps(monkeypatch):
    monitor = _monitor()
    monitor._refresh_once = MagicMock(
        return_value={
            "active_positions": 0,
            "positions_fully_fresh": 0,
            "positions_stale_or_blind": 0,
            "position_state_propagation_ok": True,
            "complete_coverage": True,
        }
    )

    def stop_after_wait(timeout):
        monitor._stop.set()
        return False

    monkeypatch.setattr(qpm_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(monitor._kick, "wait", stop_after_wait)
    monitor._loop()

    assert monitor._last_cycle_attempt_ts > 0
    assert monitor._last_cycle_success_ts >= monitor._last_cycle_attempt_ts
    assert monitor._last_cycle_completed_ts >= monitor._last_cycle_success_ts
    assert monitor._last_cycle_ts == monitor._last_cycle_success_ts
    assert monitor._last_cycle_error == ""


def test_alive_monitor_with_stale_success_or_failure_is_unhealthy():
    monitor = _monitor()
    monitor._thread = MagicMock()
    monitor._thread.is_alive.return_value = True
    monitor._last_cycle_success_ts = (
        time.time() - qpm_module.HEARTBEAT_DEGRADED_SEC - 1
    )
    monitor._last_cycle_complete_coverage = True
    assert not monitor.is_healthy()

    monitor._last_cycle_success_ts = time.time()
    monitor._consecutive_failures = 1
    assert not monitor.is_healthy()

    monitor._consecutive_failures = 0
    assert monitor.is_healthy()


def test_health_keys_are_isolated_by_client_and_mode():
    broker = _Broker("https://api.tradier.com")
    engine = _ExitEngine()
    jason_live = _monitor(broker=broker, engine=engine)
    jose_live = _monitor(
        client="jose@example.com", broker=broker, engine=engine
    )
    jason_paper = _monitor(
        mode="paper", broker=broker, engine=engine
    )

    assert len({
        jason_live._health_key,
        jose_live._health_key,
        jason_paper._health_key,
    }) == 3


def test_registry_registration_and_heartbeat_use_same_scoped_key(monkeypatch):
    monitor = _monitor()
    monitor._refresh_once = MagicMock(
        return_value={
            "active_positions": 0,
            "positions_fully_fresh": 0,
            "positions_stale_or_blind": 0,
            "position_state_propagation_ok": True,
            "complete_coverage": True,
        }
    )

    def stop_after_wait(timeout):
        monitor._stop.set()
        return False

    monkeypatch.setattr(qpm_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(monitor._kick, "wait", stop_after_wait)
    with (
        patch.object(
            ap_health_registry.HEALTH, "ensure_registered"
        ) as registered,
        patch.object(ap_health_registry.HEALTH, "heartbeat") as heartbeat,
    ):
        monitor._loop()

    assert registered.call_args.args[0] == monitor._health_key
    assert (
        registered.call_args.args[1]
        == ap_health_registry.Criticality.HIGH
    )
    assert heartbeat.call_args.args[0] == monitor._health_key


def _position():
    old = qpm_module._utc_now() - timedelta(seconds=120)
    return SimpleNamespace(
        position_id="position-1",
        positionid="position-1",
        ticker="SPY",
        underlying="SPY",
        option_symbol="SPY260731C00550000",
        optionsymbol="SPY260731C00550000",
        execution_mode="live",
        executionmode="live",
        entry_price=0.0,
        entryprice=0.0,
        current_option_price=0.0,
        currentoptionprice=0.0,
        current_underlying=0.0,
        currentunderlying=0.0,
        last_option_quote_update_ts=old,
        lastoptionquoteupdatets=old,
        last_underlying_quote_update_ts=old,
        lastunderlyingquoteupdatets=old,
        touched_profit=False,
        touchedprofit=False,
    )


class _CoverageBroker(_Broker):
    def __init__(self, *, option=True, underlying=True):
        super().__init__("https://api.tradier.com")
        self.option = option
        self.underlying = underlying

    def get_quotes(self, symbols):
        out = {}
        for symbol in symbols:
            if symbol == "SPY" and self.underlying:
                out[symbol] = {
                    "symbol": symbol,
                    "last": 550.0,
                }
            if symbol == "SPY260731C00550000" and self.option:
                out[symbol] = {
                    "symbol": symbol,
                    "bid": 1.0,
                    "ask": 1.1,
                    "last": 1.05,
                }
        return out


class _DirectWriteExitEngine:
    """Production-shaped direct-write seam with no snapshot applier."""

    def __init__(self, positions):
        self._positions = list(positions)
        self._lock = threading.Lock()

    def active_positions(self):
        return self._positions


def _coverage_monitor(*, option=True, underlying=True, apply_error=None):
    qpm_module._SHARED_CACHE.clear()
    position = _position()
    engine = _ExitEngine([position], apply_error=apply_error)
    monitor = _monitor(
        broker=_CoverageBroker(
            option=option, underlying=underlying
        ),
        engine=engine,
    )
    monitor._persist_quote_to_db = MagicMock(return_value=False)
    monitor._persist_mfe_mae_to_orders = MagicMock(return_value=False)
    monitor._mark_mfe_mae_unavailable = MagicMock(return_value=False)
    return monitor, position


def test_missing_option_quote_returns_incomplete_coverage():
    monitor, _ = _coverage_monitor(option=False)
    result = monitor._refresh_once()

    assert result == {
        "active_positions": 1,
        "positions_fully_fresh": 0,
        "positions_stale_or_blind": 1,
        "position_state_propagation_ok": True,
        "complete_coverage": False,
    }


def test_stale_underlying_truth_returns_incomplete_coverage():
    monitor, _ = _coverage_monitor(underlying=False)
    result = monitor._refresh_once()

    assert result["positions_fully_fresh"] == 0
    assert result["positions_stale_or_blind"] == 1
    assert result["complete_coverage"] is False


def test_snapshot_application_error_returns_incomplete_coverage():
    monitor, _ = _coverage_monitor(
        apply_error=RuntimeError("snapshot failed")
    )
    result = monitor._refresh_once()

    assert result["positions_fully_fresh"] == 1
    assert result["position_state_propagation_ok"] is False
    assert result["complete_coverage"] is False


def test_fresh_direct_write_cycle_without_snapshot_applier_is_healthy(
    monkeypatch,
):
    qpm_module._SHARED_CACHE.clear()
    position = _position()
    engine = _DirectWriteExitEngine([position])
    monitor = _monitor(
        broker=_CoverageBroker(),
        engine=engine,
    )
    monitor._persist_quote_to_db = MagicMock(return_value=False)
    monitor._persist_mfe_mae_to_orders = MagicMock(return_value=False)
    monitor._mark_mfe_mae_unavailable = MagicMock(return_value=False)
    monkeypatch.setattr(qpm_module, "DIRECT_POSITION_WRITES", True)

    def stop_after_wait(timeout):
        monitor._stop.set()
        return False

    monkeypatch.setattr(qpm_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(monitor._kick, "wait", stop_after_wait)
    monitor._loop()

    monitor._thread = MagicMock()
    monitor._thread.is_alive.return_value = True
    assert monitor._last_cycle_coverage == {
        "active_positions": 1,
        "positions_fully_fresh": 1,
        "positions_stale_or_blind": 0,
        "position_state_propagation_ok": True,
        "complete_coverage": True,
    }
    assert monitor.is_healthy()


def test_fresh_cycle_without_any_propagation_path_is_incomplete(
    monkeypatch,
):
    qpm_module._SHARED_CACHE.clear()
    position = _position()
    engine = _DirectWriteExitEngine([position])
    monitor = _monitor(
        broker=_CoverageBroker(),
        engine=engine,
    )
    monitor._persist_quote_to_db = MagicMock(return_value=False)
    monitor._persist_mfe_mae_to_orders = MagicMock(return_value=False)
    monitor._mark_mfe_mae_unavailable = MagicMock(return_value=False)
    monkeypatch.setattr(qpm_module, "DIRECT_POSITION_WRITES", False)

    result = monitor._refresh_once()

    assert result == {
        "active_positions": 1,
        "positions_fully_fresh": 1,
        "positions_stale_or_blind": 0,
        "position_state_propagation_ok": False,
        "complete_coverage": False,
    }


def test_incomplete_coverage_degrades_without_healthy_heartbeat(
    monkeypatch,
):
    monitor = _monitor()
    monitor._refresh_once = MagicMock(
        return_value={
            "active_positions": 1,
            "positions_fully_fresh": 0,
            "positions_stale_or_blind": 1,
            "position_state_propagation_ok": True,
            "complete_coverage": False,
        }
    )

    def stop_after_wait(timeout):
        monitor._stop.set()
        return False

    monkeypatch.setattr(qpm_module.time, "sleep", lambda _: None)
    monkeypatch.setattr(monitor._kick, "wait", stop_after_wait)
    with (
        patch.object(ap_health_registry.HEALTH, "heartbeat") as heartbeat,
        patch.object(ap_health_registry.HEALTH, "set_status") as status,
    ):
        monitor._loop()

    monitor._thread = MagicMock()
    monitor._thread.is_alive.return_value = True
    assert not monitor.is_healthy()
    heartbeat.assert_not_called()
    assert (
        status.call_args.args[1]
        == ap_health_registry.HealthStatus.DEGRADED
    )
    assert monitor._last_cycle_success_ts == 0
    assert monitor._consecutive_failures == 0
    assert monitor._last_cycle_error == "incomplete_quote_coverage"


def test_unknown_mode_blocks_monitor_and_runner_startup():
    monitor = _monitor(mode="UNKNOWN")
    monitor.start()
    assert not monitor.is_alive()

    _FakeQPM.created.clear()
    runner = _runner()
    runner.mode = "UNKNOWN"
    with patch.object(client_runner, "APPositionQuoteMonitor", _FakeQPM):
        runner._start_position_quote_monitor(
            _Broker("https://api.tradier.com"),
            MagicMock(),
        )
    assert _FakeQPM.created == []


def test_shared_cache_is_namespaced_and_immediate_refresh_is_scoped():
    qpm_module._SHARED_CACHE.clear()
    live_broker = _Broker("https://api.tradier.com")
    paper_broker = _Broker("https://sandbox.tradier.com")
    engine = _ExitEngine()
    live = _monitor(mode="live", broker=live_broker, engine=engine)
    paper = _monitor(mode="paper", broker=paper_broker, engine=engine)

    assert live._cache_key("SPY") != paper._cache_key("SPY")
    live._fetch_batch_cached(["SPY"])
    paper._fetch_batch_cached(["SPY"])
    assert len(qpm_module._SHARED_CACHE) == 2

    assert live.request_immediate_refresh("SPY")
    assert live._cache_key("SPY") not in qpm_module._SHARED_CACHE
    assert paper._cache_key("SPY") in qpm_module._SHARED_CACHE


class _FakeQPM:
    created = []

    def __init__(
        self,
        *,
        broker,
        client_id,
        exit_engine,
        execution_mode,
        alert_fn,
    ):
        self.broker = broker
        self.client_id = client_id
        self.exit_engine = exit_engine
        self.execution_mode = execution_mode
        self.alive = False
        self.start_calls = 0
        self.stop_calls = 0
        type(self).created.append(self)

    def is_alive(self):
        return self.alive

    def start(self):
        self.start_calls += 1
        self.alive = True

    def stop(self):
        self.stop_calls += 1
        self.alive = False

    def binding_matches(
        self, *, client_id, execution_mode, broker, exit_engine
    ):
        return (
            self.client_id == client_id
            and self.execution_mode == execution_mode
            and self.broker is broker
            and self.exit_engine is exit_engine
        )

    def binding_snapshot(self):
        return {
            "client_id": self.client_id,
            "execution_mode": self.execution_mode,
        }


def _runner():
    runner = client_runner.ClientRunner.__new__(client_runner.ClientRunner)
    runner.email = "jason@example.com"
    runner.mode = "LIVE"
    runner.core = None
    runner.data_broker = None
    runner.databroker = None
    runner.broker = None
    runner.exit_eng = None
    runner.exiteng = None
    runner.exit_engine = None
    runner.quote_monitor = None
    runner.quotemonitor = None
    runner._position_quote_monitor_lock = threading.Lock()
    runner.exit_reliability_monitor = MagicMock()
    runner.exit_reliability_monitor.is_alive.return_value = True
    return runner


def test_two_concurrent_starts_create_only_one_monitor():
    _FakeQPM.created.clear()
    runner = _runner()
    broker = _Broker("https://api.tradier.com")
    engine = MagicMock()

    with patch.object(client_runner, "APPositionQuoteMonitor", _FakeQPM):
        threads = [
            threading.Thread(
                target=runner._start_position_quote_monitor,
                args=(broker, engine),
            )
            for _ in range(2)
        ]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()

    assert len(_FakeQPM.created) == 1
    assert _FakeQPM.created[0].start_calls == 1


def test_wrong_alive_monitor_is_stopped_and_replaced():
    _FakeQPM.created.clear()
    runner = _runner()
    broker = _Broker("https://api.tradier.com")
    engine = MagicMock()
    wrong = _FakeQPM(
        broker=broker,
        client_id="jose@example.com",
        exit_engine=engine,
        execution_mode="live",
        alert_fn=lambda _: None,
    )
    wrong.start()
    runner.quote_monitor = wrong
    runner.quotemonitor = wrong

    with patch.object(client_runner, "APPositionQuoteMonitor", _FakeQPM):
        runner._start_position_quote_monitor(broker, engine)

    assert wrong.stop_calls == 1
    assert runner.quote_monitor is not wrong
    assert runner.quote_monitor.binding_matches(
        client_id="jason@example.com",
        execution_mode="live",
        broker=broker,
        exit_engine=engine,
    )


def test_replacement_fails_closed_when_wrong_monitor_will_not_stop():
    class _StuckQPM(_FakeQPM):
        def stop(self):
            self.stop_calls += 1

    _FakeQPM.created.clear()
    runner = _runner()
    broker = _Broker("https://api.tradier.com")
    engine = MagicMock()
    wrong = _StuckQPM(
        broker=broker,
        client_id="jose@example.com",
        exit_engine=engine,
        execution_mode="live",
        alert_fn=lambda _: None,
    )
    wrong.start()
    runner.quote_monitor = wrong
    runner.quotemonitor = wrong

    with patch.object(client_runner, "APPositionQuoteMonitor", _FakeQPM):
        runner._start_position_quote_monitor(broker, engine)

    assert wrong.stop_calls == 1
    assert runner.quote_monitor is wrong
    assert len(_FakeQPM.created) == 1
