from __future__ import annotations

import sys
import types
from types import SimpleNamespace
from unittest.mock import MagicMock

import ap_execution_core as core_mod
import ap_entry_confirmation as entry_confirmation_mod
from ap_entry_watcher import WatchedSignal


def _plan(*, confirmation_required: bool = False, candles=None, metadata_override=None):
    metadata = {
        "hybrid_client_quality_gate": {
            "confirmation_required": confirmation_required,
            "confirmation_seconds": 45,
        },
    }
    if candles is not None:
        metadata["intraday_candles"] = candles
    if metadata_override is not None:
        metadata = metadata_override
    return SimpleNamespace(
        contract_symbol="AAPL260117C00200000",
        limit_price=1.00,
        contracts=1,
        trigger_price=100.0,
        side="CALL",
        tier="A",
        metadata=metadata,
    )


def _failing_candles():
    return [
        {"open": 99.80, "high": 100.20, "low": 99.70, "close": 99.90},
        {"open": 99.90, "high": 99.95, "low": 99.40, "close": 99.60},
    ]


def _capture_entry_confirmation_patch(osm: MagicMock) -> dict:
    for call in osm.update_order_meta.call_args_list:
        patch = call.args[1]
        if isinstance(patch, dict) and "entry_confirmation" in patch:
            return patch["entry_confirmation"]
    raise AssertionError("entry_confirmation meta patch not found")


def _blocked_calls(store: MagicMock):
    blocked = []
    for call in store.update_signal_fields.call_args_list:
        payload = call.args[1]
        if payload.get("decision_status") == "blocked_at_breach":
            blocked.append(call)
    return blocked


def _run_entry_trigger(
    monkeypatch,
    *,
    mode: str,
    confirmation_required: bool = False,
    candles=None,
    underlying_last: float = 100.80,
    plan_override=None,
):
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", mode)
    monkeypatch.setenv("ENTRY_CONFIRMATION_REQUIRED_DEFAULT", "1")

    fake_execution_mod = types.ModuleType("ap.execution")
    fake_execution_mod._refresh_ask_at_submit = lambda broker, contract: (
        1.02,
        5,
        True,
        "ok",
        {
            "submit_bid": 1.00,
            "submit_ask": 1.02,
            "submit_last": 1.01,
            "submit_mid": 1.01,
            "spread_pct": 0.0198,
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution_mod)

    ledger_events = []
    fake_ledger_mod = types.ModuleType("ap.opportunity_ledger")
    fake_ledger_mod.STAGE_ENTRY_CONFIRMATION = "ENTRY_CONFIRMATION"
    fake_ledger_mod.update_opportunity = lambda *args, **kwargs: ledger_events.append((args, kwargs))
    monkeypatch.setitem(sys.modules, "ap.opportunity_ledger", fake_ledger_mod)

    plan = plan_override or _plan(confirmation_required=confirmation_required, candles=candles)
    osm = MagicMock()
    osm.submit_existing_entry.return_value = {
        "ok": True,
        "local_order_id": "local-1",
        "broker_order_id": "broker-1",
        "status": "SUBMITTED",
        "error": None,
    }
    osm.update_order_meta.return_value = True
    osm.expire_pending_entry.return_value = True

    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = True
    core.mode = "PAPER"
    core.email = "client@example.com"
    core.client_id = "client@example.com"
    core.broker = SimpleNamespace(
        cfg=SimpleNamespace(base_url="https://api.tradier.com"),
        sandbox=False,
    )
    core.store = MagicMock()
    core.order_state_machine = osm
    core.contract_selector = None
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._alert_degraded = MagicMock()
    core._cleanup_pending_entry_order = types.MethodType(
        core_mod.APExecutionCore._cleanup_pending_entry_order,
        core,
    )

    watched = WatchedSignal(
        {
            "ticker": "AAPL",
            "side": "CALL",
            "entry_price": 100.0,
            "stop_price": 95.0,
            "target_price": 110.0,
            "signal_id": "sig-1",
            "canonical_signal_id": "sig-1",
            "local_order_id": "local-1",
            "client_id": "client@example.com",
            "timeframe": "1d",
            "score": 78,
        },
        overnight=False,
    )
    watched.trigger_price = 100.0
    watched.last_quote_bid = underlying_last
    watched.last_quote_ask = underlying_last

    core_mod.APExecutionCore._on_entry_trigger(core, watched)
    return {
        "core": core,
        "osm": osm,
        "store": core.store,
        "ledger_events": ledger_events,
    }


def test_observe_missing_continuation_submits_and_records_would_block(monkeypatch):
    result = _run_entry_trigger(
        monkeypatch,
        mode="observe",
        confirmation_required=False,
        candles=None,
        underlying_last=100.90,
    )
    result["osm"].submit_existing_entry.assert_called_once()
    result["osm"].expire_pending_entry.assert_not_called()
    assert _blocked_calls(result["store"]) == []
    meta = _capture_entry_confirmation_patch(result["osm"])
    assert meta["daily_continuation_mode"] == "observe"
    assert meta["daily_continuation_would_block"] is True


def test_enforce_missing_continuation_blocks_and_expires_pending_entry(monkeypatch):
    result = _run_entry_trigger(
        monkeypatch,
        mode="enforce",
        confirmation_required=False,
        candles=None,
        underlying_last=100.90,
    )
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-1",
        reason="DAILY_CONTINUATION_MISSING_INTRADAY_CANDLES",
    )
    assert len(_blocked_calls(result["store"])) == 1
    assert result["ledger_events"], "ENTRY_CONFIRMATION_FAILED should be recorded"
    args, kwargs = result["ledger_events"][0]
    assert args[2] == "ENTRY_CONFIRMATION_FAILED"
    assert kwargs["miss_reason"] == "DAILY_CONTINUATION_MISSING_INTRADAY_CANDLES"


def test_observe_failed_continuation_with_data_submits_and_records_would_block(monkeypatch):
    result = _run_entry_trigger(
        monkeypatch,
        mode="observe",
        confirmation_required=False,
        candles=_failing_candles(),
        underlying_last=99.60,
    )
    result["osm"].submit_existing_entry.assert_called_once()
    result["osm"].expire_pending_entry.assert_not_called()
    meta = _capture_entry_confirmation_patch(result["osm"])
    assert meta["daily_continuation_mode"] == "observe"
    assert meta["daily_continuation_passed"] is False
    assert meta["daily_continuation_would_block"] is True
    assert meta["daily_continuation_fail_reason"] == "DAILY_CONTINUATION_TRIGGER_TOUCH_ONLY"


def test_enforce_failed_continuation_with_data_blocks_submit(monkeypatch):
    result = _run_entry_trigger(
        monkeypatch,
        mode="enforce",
        confirmation_required=False,
        candles=_failing_candles(),
        underlying_last=99.60,
    )
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-1",
        reason="DAILY_CONTINUATION_TRIGGER_TOUCH_ONLY",
    )


def test_missing_confirmation_required_defaults_on_and_blocks_reversal_before_submit(monkeypatch):
    plan = _plan(metadata_override={})
    result = _run_entry_trigger(
        monkeypatch,
        mode="off",
        underlying_last=99.60,
        plan_override=plan,
    )
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-1",
        reason="entry_confirm_failed_underlying_reversal",
    )
    meta = _capture_entry_confirmation_patch(result["osm"])
    assert meta["confirmation_required"] is True
    assert meta["confirmation_required_source"] == "global_default"


def test_confirmation_exception_fails_closed_even_when_confirmation_not_required(monkeypatch):
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", "observe")
    monkeypatch.setattr(
        entry_confirmation_mod,
        "check_entry_confirmation",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    result = _run_entry_trigger(
        monkeypatch,
        mode="observe",
        confirmation_required=False,
        candles=_failing_candles(),
        underlying_last=99.60,
    )
    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-1",
        reason="entry_confirm_error:boom",
    )
    assert len(_blocked_calls(result["store"])) == 1
    assert result["ledger_events"], "ENTRY_CONFIRMATION_FAILED should be recorded"
