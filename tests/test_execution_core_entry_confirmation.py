from __future__ import annotations

import math
import sys
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import ap_execution_core as core_mod
import ap_entry_confirmation as entry_confirmation_mod
from ap_entry_watcher import APEntryWatcher, WatchedSignal


def _plan(*, confirmation_required: bool = False, candles=None, side: str = "CALL"):
    metadata = {
        "hybrid_client_quality_gate": {
            "confirmation_required": confirmation_required,
            "confirmation_seconds": 45,
        },
    }
    if candles is not None:
        metadata["intraday_candles"] = candles
    return SimpleNamespace(
        contract_symbol="AAPL260117C00200000",
        limit_price=1.00,
        contracts=1,
        trigger_price=100.0,
        stop_underlying=95.0 if side == "CALL" else 105.0,
        target_underlying=110.0 if side == "CALL" else 90.0,
        side=side,
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
    underlying_quote_age_ms: float | None = 0,
    execution_mode: str = "paper",
    quote_age_ms: float | None = 5,
    submit_bid: float | None = 1.00,
    submit_ask: float | None = 1.02,
    plan_metadata=None,
    plan_side: str = "CALL",
    invoke_direct_trigger: bool = True,
):
    monkeypatch.delenv("ENABLE_DAILY_CONTINUATION_VALIDATION", raising=False)
    monkeypatch.setenv("ENABLE_DAILY_CONTINUATION_MODE", mode)

    fake_execution_mod = types.ModuleType("ap.execution")
    fake_execution_mod._refresh_ask_at_submit = lambda broker, contract: (
        submit_ask,
        quote_age_ms,
        True,
        "ok",
        {
            "submit_bid": submit_bid,
            "submit_ask": submit_ask,
            "submit_last": (
                (submit_bid + submit_ask) / 2
                if submit_bid is not None and submit_ask is not None else None
            ),
            "submit_mid": (
                (submit_bid + submit_ask) / 2
                if submit_bid is not None and submit_ask is not None else None
            ),
            "spread_pct": (
                (submit_ask - submit_bid) / ((submit_bid + submit_ask) / 2)
                if submit_bid is not None and submit_ask is not None else None
            ),
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution_mod)

    ledger_events = []
    fake_ledger_mod = types.ModuleType("ap.opportunity_ledger")
    fake_ledger_mod.STAGE_ENTRY_CONFIRMATION = "ENTRY_CONFIRMATION"
    fake_ledger_mod.update_opportunity = lambda *args, **kwargs: ledger_events.append((args, kwargs))
    monkeypatch.setitem(sys.modules, "ap.opportunity_ledger", fake_ledger_mod)

    plan = _plan(
        confirmation_required=confirmation_required,
        candles=candles,
        side=plan_side,
    )
    if plan_metadata is not None:
        plan.metadata = plan_metadata
    osm = MagicMock()
    osm.client_id = "client@example.com"
    osm.execution_mode = execution_mode
    now = datetime.now(timezone.utc)
    order_row = {
        "id": "local-1",
        "local_order_id": "local-1",
        "client_id": "client@example.com",
        "execution_mode": execution_mode,
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "kind": "ENTRY",
        "contract": plan.contract_symbol,
        "limit_price": plan.limit_price,
        "qty": plan.contracts,
        "meta": {
            "trigger_crossed_at": now.isoformat(),
            "trigger_confirmed_at": now.isoformat(),
            "absolute_entry_deadline": (now + timedelta(minutes=5)).isoformat(),
        },
    }
    osm.get_order.side_effect = lambda local_order_id: order_row
    osm.submit_existing_entry.return_value = {
        "ok": True,
        "local_order_id": "local-1",
        "broker_order_id": "broker-1",
        "status": "SUBMITTED",
        "error": None,
    }
    def _update_order_meta(local_order_id, patch):
        order_row.setdefault("meta", {}).update(patch)
        return True

    osm.update_order_meta.side_effect = _update_order_meta
    osm.expire_pending_entry.return_value = True

    core = core_mod.APExecutionCore.__new__(core_mod.APExecutionCore)
    core.paper = execution_mode == "paper"
    core.mode = execution_mode.upper()
    core.execution_mode = execution_mode
    core.email = "client@example.com"
    core.client_id = "client@example.com"
    core.broker = SimpleNamespace(
        cfg=SimpleNamespace(base_url="https://api.tradier.com"),
        sandbox=False,
        get_quote=lambda ticker: {
            "bid": underlying_last - 0.01,
            "ask": underlying_last + 0.01,
            "quote_age_ms": 0,
            "source": "test_synchronous_quote",
        },
    )
    core.store = MagicMock()
    core.order_state_machine = osm
    core.contract_selector = None
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._refresh_hydrated_prebreach_plan = MagicMock(return_value=False)
    core._alert_degraded = MagicMock()
    core._cleanup_pending_entry_order = types.MethodType(
        core_mod.APExecutionCore._cleanup_pending_entry_order,
        core,
    )

    watched = WatchedSignal(
        {
            "ticker": "AAPL",
            "side": plan_side,
            "entry_price": 100.0,
            "stop_price": 95.0 if plan_side == "CALL" else 105.0,
            "target_price": 110.0 if plan_side == "CALL" else 90.0,
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
    watched.last_quote_age_ms = underlying_quote_age_ms

    if invoke_direct_trigger:
        core_mod.APExecutionCore._on_entry_trigger(core, watched)
    return {
        "core": core,
        "osm": osm,
        "store": core.store,
        "ledger_events": ledger_events,
        "watched": watched,
    }


def _run_watcher_poll_to_submit(
    monkeypatch,
    *,
    execution_mode: str = "live",
    plan_metadata=None,
    quote_age_ms: float | None = 5,
    submit_bid: float | None = 1.00,
    submit_ask: float | None = 1.02,
    underlying_last: float = 100.80,
    underlying_quote_age_ms: float | None = 0,
    plan_side: str = "CALL",
    watcher_quote=None,
):
    result = _run_entry_trigger(
        monkeypatch,
        mode="off",
        execution_mode=execution_mode,
        plan_metadata=plan_metadata,
        quote_age_ms=quote_age_ms,
        submit_bid=submit_bid,
        submit_ask=submit_ask,
        underlying_last=underlying_last,
        underlying_quote_age_ms=underlying_quote_age_ms,
        plan_side=plan_side,
        invoke_direct_trigger=False,
    )
    core = result["core"]

    class _DummyBroker:
        session = None

    class _Watcher(APEntryWatcher):
        def __init__(self, *args, quotes=None, **kwargs):
            super().__init__(*args, **kwargs)
            self._quotes = quotes or {}

        def _fetch_quotes(self, tickers):
            return {str(k).upper(): dict(v) for k, v in self._quotes.items()}

        def _persist_watcher_audit(self, local_order_id, payload):
            return None

    quote = {
        "bid": 100.0,
        "ask": 101.0,
        "last": 101.0,
        "quote_age_ms": underlying_quote_age_ms,
    }
    if watcher_quote is not None:
        quote.update(watcher_quote)

    watcher = _Watcher(_DummyBroker(), quotes={"AAPL": quote})
    watcher.on_trigger = core._on_entry_trigger
    watched = WatchedSignal(
        {
            "ticker": "AAPL",
            "side": plan_side,
            "entry_price": 100.0,
            "stop_price": 95.0 if plan_side == "CALL" else 105.0,
            "target_price": 110.0 if plan_side == "CALL" else 90.0,
            "signal_id": "sig-1",
            "canonical_signal_id": "sig-1",
            "local_order_id": "local-1",
            "client_id": "client@example.com",
            "timeframe": "1d",
            "score": 78,
        },
        overnight=False,
    )
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)

    fake_execution_mod = types.ModuleType("ap.execution")
    fake_execution_mod._refresh_ask_at_submit = lambda broker, contract: (
        submit_ask,
        quote_age_ms,
        True,
        "ok",
        {
            "submit_bid": submit_bid,
            "submit_ask": submit_ask,
            "submit_last": (
                (submit_bid + submit_ask) / 2
                if submit_bid is not None and submit_ask is not None else None
            ),
            "submit_mid": (
                (submit_bid + submit_ask) / 2
                if submit_bid is not None and submit_ask is not None else None
            ),
            "spread_pct": (
                (submit_ask - submit_bid) / ((submit_bid + submit_ask) / 2)
                if submit_bid is not None and submit_ask is not None else None
            ),
        },
    )
    monkeypatch.setitem(sys.modules, "ap.execution", fake_execution_mod)

    # ── Lifecycle singleton seed ──────────────────────────────────────────
    # ap_lifecycle.LEDGER is a module-level singleton that persists across
    # tests in the same pytest process.  _poll_active_signals → _ew_record
    # attempts a TRIGGER_READY transition.  LEGAL_TRANSITIONS requires the
    # signal to be in WATCHING state first; NONE→TRIGGER_READY is illegal
    # and causes an ILLEGAL_TRANSITION error that prevents submit.
    #
    # This helper bypasses add_signal() (which is where WATCHING is normally
    # registered), so we seed the lifecycle directly:
    #   NONE → ADOPTED (legal from None)
    #   ADOPTED → WATCHING (legal)
    #
    # The seed is reset for each call so prior test runs don't leave the
    # signal in a terminal state that blocks the next test's transitions.
    try:
        from ap_lifecycle import (
            LEDGER as _TEST_LEDGER,
            SignalState as _TEST_SS,
            LifecycleOwner as _TEST_LO,
        )
        _lc_sig_id = str(watched.signal.get("signal_id", ""))
        _lc_ticker = str(watched.ticker or "")
        if _lc_sig_id and _lc_ticker:
            # Reset any prior state so this call is always self-contained.
            with _TEST_LEDGER._entry_lock:
                _TEST_LEDGER._current_state.pop(_lc_sig_id, None)
            # NONE → ADOPTED → WATCHING (both legal transitions)
            _TEST_LEDGER.transition(
                _lc_sig_id, _lc_ticker,
                _TEST_SS.ADOPTED, _TEST_LO.WATCHER, "test_helper_seed",
            )
            _TEST_LEDGER.transition(
                _lc_sig_id, _lc_ticker,
                _TEST_SS.WATCHING, _TEST_LO.WATCHER, "test_helper_seed_watching",
            )
    except Exception:
        pass  # defensive — test proceeds if lifecycle is unavailable

    # ── Disable time-of-day gates so test is not market-hours-dependent ──
    # live_submit_gates checks ENTRY_CUTOFF_ET_HHMM (default 1530 = 3:30 PM ET).
    # Running outside market hours causes ENTRY_CUTOFF_EXCEEDED and blocks the
    # submit without mocking, making this test non-deterministic in CI depending
    # on when the push triggers the run.  Set to 2359 (end of day) to disable.
    monkeypatch.setenv("ENTRY_CUTOFF_ET_HHMM", "2359")

    watcher._poll_active_signals(False)
    watcher._poll_active_signals(False)
    result["watcher"] = watcher
    return result


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
        reason="daily_continuation_failed:missing_intraday_context",
    )
    assert len(_blocked_calls(result["store"])) == 1
    assert result["ledger_events"], "ENTRY_CONFIRMATION_FAILED should be recorded"
    args, kwargs = result["ledger_events"][0]
    assert args[2] == "ENTRY_CONFIRMATION_FAILED"
    assert kwargs["miss_reason"] == "daily_continuation_failed:missing_intraday_context"


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
        reason="daily_continuation_failed:trigger_touch_only",
    )


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


def test_fresh_option_and_fresh_underlying_reaches_submit(monkeypatch):
    result = _run_entry_trigger(
        monkeypatch,
        mode="off",
        execution_mode="live",
        plan_metadata={"confirmation_required": True},
        quote_age_ms=1000,
        underlying_quote_age_ms=1000,
    )

    result["osm"].submit_existing_entry.assert_called_once()
    meta = _capture_entry_confirmation_patch(result["osm"])
    assert meta["quote_age_seconds"] == 1.0
    assert meta["underlying_quote_age_seconds"] == 1.0


def test_missing_underlying_age_blocks_before_submit(monkeypatch):
    result = _run_entry_trigger(
        monkeypatch,
        mode="off",
        execution_mode="live",
        plan_metadata={"confirmation_required": True},
        underlying_quote_age_ms=None,
    )

    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-1",
        reason="entry_confirm_failed_missing_underlying_quote_age",
    )


def test_stale_underlying_age_blocks_before_submit(monkeypatch):
    result = _run_entry_trigger(
        monkeypatch,
        mode="off",
        execution_mode="live",
        plan_metadata={"confirmation_required": True},
        quote_age_ms=1000,
        underlying_quote_age_ms=11_000,
    )

    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-1",
        reason="entry_confirm_failed_stale_underlying_quote",
    )
    meta = _capture_entry_confirmation_patch(result["osm"])
    assert meta["quote_age_seconds"] == 1.0
    assert meta["underlying_quote_age_seconds"] == 11.0


def test_invalid_underlying_age_blocks_before_submit(monkeypatch):
    result = _run_entry_trigger(
        monkeypatch,
        mode="off",
        execution_mode="live",
        plan_metadata={"confirmation_required": True},
        underlying_quote_age_ms=math.inf,
    )

    result["osm"].submit_existing_entry.assert_not_called()
    result["osm"].expire_pending_entry.assert_called_once_with(
        "local-1",
        reason="entry_confirm_failed_invalid_underlying_quote_age",
    )


def test_stale_underlying_cannot_borrow_option_quote_age(monkeypatch):
    result = _run_entry_trigger(
        monkeypatch,
        mode="off",
        execution_mode="live",
        plan_metadata={"confirmation_required": True},
        quote_age_ms=1000,
        underlying_quote_age_ms=20_000,
    )

    result["osm"].submit_existing_entry.assert_not_called()
    meta = _capture_entry_confirmation_patch(result["osm"])
    assert meta["quote_age_seconds"] == 1.0
    assert meta["underlying_quote_age_seconds"] == 20.0
    assert meta["underlying_quote_age_status"] == "stale"


def test_production_shaped_watcher_quote_age_reaches_submit_without_manual_seed(monkeypatch):
    result = _run_watcher_poll_to_submit(
        monkeypatch,
        execution_mode="live",
        plan_metadata={"confirmation_required": True},
        quote_age_ms=1000,
        watcher_quote={"bid": 100.0, "ask": 101.0, "quote_age_ms": 1000},
    )

    result["osm"].submit_existing_entry.assert_called_once()
    meta = _capture_entry_confirmation_patch(result["osm"])
    assert meta["quote_age_seconds"] == 1.0
    assert meta["underlying_quote_age_seconds"] == 1.0
    assert result["watcher"]._pending == []
