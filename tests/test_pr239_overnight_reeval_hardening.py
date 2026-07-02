from __future__ import annotations

import importlib
import os
import sys
import types
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo


REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

FIXED_ET = datetime(2026, 6, 30, 9, 15, tzinfo=ZoneInfo("America/New_York"))


def _signal(**overrides) -> dict:
    payload = {
        "signal_id": "sig-overnight-1",
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "direction": "CALL",
        "timeframe": "1d",
        "score": 78.0,
        "pattern": "2-1-2",
        "tier": "A",
        "created_at": "2026-06-29T20:00:00+00:00",
    }
    payload.update(overrides)
    return payload


def _job(signal: dict | None = None) -> dict:
    return {
        "id": 101,
        "signal_id": "sig-overnight-1",
        "payload": signal or _signal(),
        "_source": "trade_queue",
    }


def _plan(**overrides) -> types.SimpleNamespace:
    plan = types.SimpleNamespace(
        plan_id="plan-overnight-1",
        signal_id="sig-overnight-1",
        ticker="AAPL",
        side="CALL",
        direction="CALL",
        score=78.0,
        timeframe="1d",
        entry_trigger=101.0,
        trigger_price=101.0,
        trigger_type="breach",
        prior_day_high=101.0,
        prior_day_low=95.0,
        pattern="2-1-2",
        tier="A",
        contract_symbol="AAPL260703C00101000",
        contracts=1,
        limit_price=1.25,
        metadata={},
    )
    for key, value in overrides.items():
        setattr(plan, key, value)
    return plan


class _HappyOSM:
    def __init__(self):
        self.create_calls: list[dict] = []
        self.mark_calls: list[str] = []
        self.transition_calls: list[tuple[str, str, dict]] = []

    def create_entry_order(self, plan, initial_status="CREATED", execution_mode=None, meta=None):
        self.create_calls.append(
            {
                "plan": plan,
                "initial_status": initial_status,
                "execution_mode": execution_mode,
                "meta": meta,
            }
        )
        return "local-ord-1"

    def mark_entry_pending_trigger(self, local_order_id: str) -> bool:
        self.mark_calls.append(local_order_id)
        return True

    def transition(self, local_order_id: str, new_status: str, **kwargs) -> bool:
        self.transition_calls.append((local_order_id, new_status, kwargs))
        return True


class _AlreadyPendingOSM(_HappyOSM):
    def get_order(self, local_order_id: str) -> dict:
        return {"status": "PENDING_TRIGGER"}

    def mark_entry_pending_trigger(self, local_order_id: str) -> bool:
        raise AssertionError("mark_entry_pending_trigger should not run for already pending rows")


class _NoSubmitBroker:
    def __init__(self):
        self.get_prior_day_levels_calls: list[str] = []

    def get_prior_day_levels(self, ticker: str) -> dict:
        self.get_prior_day_levels_calls.append(ticker)
        return {"prior_day_high": 101.0, "prior_day_low": 95.0}

    def submit_order(self, *_args, **_kwargs):
        raise AssertionError("submit_order must not run before breach")

    def place_order(self, *_args, **_kwargs):
        raise AssertionError("place_order must not run before breach")


def _install_common_stubs(monkeypatch, *, validation=None):
    fake_validator = types.ModuleType("ap.overnight_daily_validator")
    fake_validator.fetch_market_snapshot = lambda ticker, broker: {"last": 100.0}
    fake_validator.validate_overnight_daily_signal = (
        validation
        or (lambda **kwargs: types.SimpleNamespace(valid=True, reason_code="", reason_text=""))
    )
    fake_validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", fake_validator)

    fake_auth = types.ModuleType("ap.authorization")
    fake_auth.is_live_broker = lambda broker: False
    fake_auth.broker_live_mode_known = lambda broker: True
    fake_auth.check_live_authorization = lambda client_id: None
    fake_auth.authorization_gate_enforced = lambda: False
    fake_auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    fake_auth.execution_mode_for_broker = lambda broker: "PAPER"
    monkeypatch.setitem(sys.modules, "ap.authorization", fake_auth)


def _run(
    monkeypatch,
    *,
    signal: dict | None = None,
    broker=None,
    osm=None,
    plan=None,
    contract_selector=None,
    validation=None,
):
    _install_common_stubs(monkeypatch, validation=validation)
    ov = importlib.import_module("ap_overnight_reeval")
    ov = importlib.reload(ov)
    monkeypatch.setattr(ov, "_et_now", lambda: FIXED_ET)
    monkeypatch.setattr(ov, "_lifecycle_ok", False, raising=False)
    monkeypatch.setattr(ov, "_fetch_watching_signals", lambda client_id: [_job(signal)])
    monkeypatch.setattr(ov, "_shared_watch_arm_failure_already_recorded", lambda *a, **k: False)

    rejected: list[tuple[object, str, str]] = []
    errors: list[tuple[object, str, str]] = []
    waiting: list[tuple[object, str, str]] = []
    monkeypatch.setattr(ov, "_mark_job_rejected", lambda job_id, client_id, reason: rejected.append((job_id, client_id, reason)))
    monkeypatch.setattr(ov, "_mark_job_error", lambda job_id, client_id, reason: errors.append((job_id, client_id, reason)))
    monkeypatch.setattr(ov, "_mark_job_watching_reason", lambda job_id, client_id, reason: waiting.append((job_id, client_id, reason)))

    broker = broker or _NoSubmitBroker()
    osm = osm or _HappyOSM()

    master_control = MagicMock()
    master_control.evaluate.return_value = types.SimpleNamespace(
        ok=True,
        plan=plan or _plan(),
        reason="approved",
        score=78.0,
    )

    if contract_selector is None:
        contract_selector = MagicMock()
        contract_selector.select.return_value = "AAPL260703C00101000"

    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result = ov.run_overnight_reeval(
        client_id="tradefluencehq@gmail.com",
        broker=broker,
        master_control=master_control,
        contract_selector=contract_selector,
        order_state_machine=osm,
        entry_watcher=entry_watcher,
        force=True,
    )
    return types.SimpleNamespace(
        result=result,
        rejected=rejected,
        errors=errors,
        waiting=waiting,
        broker=broker,
        osm=osm,
        master_control=master_control,
        contract_selector=contract_selector,
        entry_watcher=entry_watcher,
    )


def test_missing_side_rejected_before_trigger_derivation(monkeypatch):
    state = _run(
        monkeypatch,
        signal=_signal(side="", direction=None, entry_trigger=None),
    )
    assert state.result["rejected"] == 1
    assert state.rejected == [(101, "tradefluencehq@gmail.com", "invalid_or_missing_side")]
    state.master_control.evaluate.assert_not_called()
    state.entry_watcher.watch.assert_not_called()
    assert state.osm.create_calls == []


def test_junk_side_not_treated_as_put(monkeypatch):
    state = _run(
        monkeypatch,
        signal=_signal(side="nonsense", direction=None, entry_trigger=None),
    )
    assert state.result["rejected"] == 1
    assert state.rejected == [(101, "tradefluencehq@gmail.com", "invalid_or_missing_side")]
    state.master_control.evaluate.assert_not_called()
    state.entry_watcher.watch.assert_not_called()
    assert state.osm.create_calls == []


def test_already_pending_trigger_row_still_arms(monkeypatch):
    osm = _AlreadyPendingOSM()
    state = _run(monkeypatch, osm=osm)
    assert state.result["armed"] == 1
    assert state.result["errors"] == 0
    assert osm.mark_calls == []
    assert osm.transition_calls == []
    state.entry_watcher.watch.assert_called_once()


def test_deferred_contract_preserves_selector_failure_metadata(monkeypatch):
    plan = _plan(contract_symbol=None, metadata=None)
    contract_selector = MagicMock()
    contract_selector.select.side_effect = RuntimeError("zero bids")
    contract_selector.get_last_failure.return_value = {
        "reason_code": "PRE_MARKET_ZERO_BID",
        "detail": "all quotes empty",
    }

    state = _run(
        monkeypatch,
        plan=plan,
        contract_selector=contract_selector,
    )

    assert state.result["armed"] == 1
    watched_plan = state.entry_watcher.watch.call_args.args[0]
    assert watched_plan.metadata["contract_deferred"] is True
    assert watched_plan.metadata["pre_market_contract_selection_failed"] is True
    assert watched_plan.metadata["pre_market_selector_failure"] == {
        "reason_code": "PRE_MARKET_ZERO_BID",
        "detail": "all quotes empty",
    }
    assert watched_plan.metadata["pre_market_selector_reason_code"] == "PRE_MARKET_ZERO_BID"
    assert watched_plan.metadata["contract_selection_deferred_to"] == "breach_time"
    assert state.osm.create_calls[0]["meta"]["pre_market_selector_reason_code"] == "PRE_MARKET_ZERO_BID"


def test_no_broker_submit_before_breach(monkeypatch):
    broker = _NoSubmitBroker()
    plan = _plan(contract_symbol=None, metadata={})
    contract_selector = MagicMock()
    contract_selector.select.return_value = None
    contract_selector.get_last_failure.return_value = {"reason_code": "PRE_MARKET_NO_CHAIN"}

    state = _run(
        monkeypatch,
        broker=broker,
        plan=plan,
        contract_selector=contract_selector,
    )

    assert state.result["armed"] == 1
    assert broker.get_prior_day_levels_calls == ["AAPL"]
    state.entry_watcher.watch.assert_called_once()


def test_fresh_rows_still_process_and_arm_normally(monkeypatch):
    state = _run(monkeypatch)

    assert state.result["processed"] == 1
    assert state.result["armed"] == 1
    assert state.result["fresh_processed"] == 1
    assert state.result["fresh_armed"] == 1
    assert state.result["stale_skipped"] == 0
    assert state.result["stale_inventory_only"] is False
    state.entry_watcher.watch.assert_called_once()
    state.master_control.evaluate.assert_called_once()


def test_stale_rows_are_skipped_without_arming_and_reported_in_summary(monkeypatch):
    state = _run(
        monkeypatch,
        signal=_signal(created_at="2026-06-20T20:00:00+00:00"),
    )

    assert state.result["armed"] == 0
    assert state.result["rejected"] == 1
    assert state.result["stale_skipped"] == 1
    assert state.result["fresh_processed"] == 0
    assert state.result["fresh_armed"] == 0
    assert state.result["stale_inventory_only"] is True
    assert state.rejected == [(101, "tradefluencehq@gmail.com", "stale_signal:age=10d")]
    state.entry_watcher.watch.assert_not_called()
    state.master_control.evaluate.assert_not_called()


def test_stale_only_run_reports_stale_inventory_only(monkeypatch):
    state = _run(
        monkeypatch,
        signal=_signal(created_at="2026-06-22T20:00:00+00:00"),
    )

    assert state.result["processed"] == 1
    assert state.result["stale_skipped"] == 1
    assert state.result["fresh_processed"] == 0
    assert state.result["fresh_armed"] == 0
    assert state.result["stale_inventory_only"] is True
