from __future__ import annotations

import math
import sys
import threading
import types
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
import ap_execution_core as core_mod
import ap_entry_confirmation as entry_confirmation_mod
import ap.intelligence_breach_runtime_bridge as breach_bridge_mod
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
    bridge_signal_fields=None,
    contract_selector=None,
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
        "canonical_signal_id": "sig-1",
        "signal_id": "sig-1",
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
    def _read_trigger_confirmation_authority(
        local_order_id,
        *,
        client_id,
        execution_mode,
        signal_id,
        canonical_signal_id,
        expected_materialization_generation=None,
    ):
        if local_order_id != order_row["local_order_id"]:
            return None
        meta = order_row.get("meta") or {}
        provenance = meta.get("trigger_crossed_at_provenance")
        if (
            order_row.get("status") != "PENDING_TRIGGER"
            or str(order_row.get("client_id") or "").strip().lower()
            != str(client_id or "").strip().lower()
            or str(order_row.get("execution_mode") or "").strip().lower()
            != str(execution_mode or "").strip().lower()
            or str(order_row.get("signal_id") or "").strip()
            != str(signal_id or "").strip()
            or str(order_row.get("canonical_signal_id") or "").strip()
            != str(canonical_signal_id or "").strip()
            or not isinstance(meta.get("trigger_crossed_at"), str)
            or not isinstance(provenance, dict)
            or provenance != {
                "canonical_signal_id": str(canonical_signal_id or "").strip(),
                "client_id": str(client_id or "").strip().lower(),
                "execution_mode": str(execution_mode or "").strip().lower(),
                "local_order_id": str(local_order_id or "").strip(),
            }
        ):
            return None
        generation = meta.get("materialization_generation")
        if (
            expected_materialization_generation is not None
            and generation != expected_materialization_generation
        ):
            return None
        return {
            "proven": True,
            "trigger_crossed_at": meta["trigger_crossed_at"],
            "trigger_crossed_at_provenance": dict(provenance),
            "materialization_generation": generation,
        }

    def _update_order_meta(local_order_id, patch, **_expected):
        order_row.setdefault("meta", {}).update(patch)
        return True

    osm.update_order_meta.side_effect = _update_order_meta
    osm.read_trigger_confirmation_authority.side_effect = (
        _read_trigger_confirmation_authority
    )
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
    core.contract_selector = contract_selector
    core._breach_risk_check = MagicMock(return_value=True)
    core._recover_plan_for_revalidation = MagicMock(return_value=plan)
    core._refresh_hydrated_prebreach_plan = MagicMock(return_value=False)
    core._alert_degraded = MagicMock()
    core._cleanup_pending_entry_order = types.MethodType(
        core_mod.APExecutionCore._cleanup_pending_entry_order,
        core,
    )

    watched_signal = {
        "ticker": "AAPL",
        "side": plan_side,
        "entry_price": 100.0,
        "stop_price": 95.0 if plan_side == "CALL" else 105.0,
        "target_price": 110.0 if plan_side == "CALL" else 90.0,
        "signal_id": "sig-1",
        "canonical_signal_id": "sig-1",
        "local_order_id": "local-1",
        "client_id": "client@example.com",
        "execution_mode": execution_mode,
        "timeframe": "1d",
        "score": 78,
    }
    if bridge_signal_fields:
        watched_signal.update(bridge_signal_fields)
    watched = WatchedSignal(watched_signal, overnight=False)
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

    watcher = _Watcher(
        _DummyBroker(),
        order_state_machine=result["osm"],
        mode=execution_mode,
        quotes={"AAPL": quote},
    )
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
            "execution_mode": execution_mode,
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


def test_real_entry_trigger_bridge_exception_preserves_existing_submission(monkeypatch):
    def explode(*_args, **_kwargs):
        raise RuntimeError("bridge unavailable")

    monkeypatch.setattr(
        breach_bridge_mod,
        "submit_breach_intelligence_nonblocking",
        explode,
    )
    result = _run_entry_trigger(monkeypatch, mode="off")

    result["osm"].submit_existing_entry.assert_called_once()
    result["osm"].expire_pending_entry.assert_not_called()


@pytest.mark.parametrize(
    "bridge_result",
    [
        {
            "ok": True,
            "accepted": False,
            "handoff_status": "SATURATED_OR_REJECTED",
            "fallback_reason": "handoff_capacity_exhausted",
        },
        {
            "ok": True,
            "accepted": False,
            "handoff_status": "INVALID_ARTIFACT",
            "fallback_reason": "BREACH_INTEL_EVIDENCE_MISSING",
        },
    ],
    ids=["saturated-or-rejected", "invalid-artifact"],
)
def test_real_entry_trigger_bridge_nonacceptance_preserves_existing_submission(
    monkeypatch, bridge_result
):
    monkeypatch.setattr(
        breach_bridge_mod,
        "submit_breach_intelligence_nonblocking",
        lambda *_args, **_kwargs: dict(bridge_result),
    )
    result = _run_entry_trigger(monkeypatch, mode="off")

    result["osm"].submit_existing_entry.assert_called_once()
    result["osm"].expire_pending_entry.assert_not_called()


def test_real_entry_trigger_missing_intelligence_is_diagnostic_only(monkeypatch):
    bridge_result = {}
    real_bridge_submit = breach_bridge_mod.submit_breach_intelligence_nonblocking

    def capture_bridge_result(*args, **kwargs):
        result = real_bridge_submit(*args, **kwargs)
        bridge_result.update(result)
        return result

    monkeypatch.setattr(
        breach_bridge_mod,
        "submit_breach_intelligence_nonblocking",
        capture_bridge_result,
    )
    result = _run_entry_trigger(monkeypatch, mode="off")

    assert bridge_result["handoff_status"] == "INVALID_ARTIFACT"
    assert "materialization_generation_missing" in bridge_result["fallback_reason"]
    result["osm"].submit_existing_entry.assert_called_once()
    result["osm"].expire_pending_entry.assert_not_called()


def test_real_entry_trigger_blocked_background_does_not_delay_existing_continuation(
    monkeypatch,
):
    background_started = threading.Event()
    release_background = threading.Event()
    background_finished = threading.Event()
    workers: list[threading.Thread] = []

    def fake_handoff(_fn, _artifact, **_kwargs):
        def worker():
            background_started.set()
            release_background.wait(5.0)
            background_finished.set()

        thread = threading.Thread(target=worker, daemon=True)
        workers.append(thread)
        thread.start()
        assert background_started.wait(1.0)
        return {"ok": True, "accepted": True}

    monkeypatch.setattr(
        breach_bridge_mod,
        "submit_intelligence_enqueue",
        fake_handoff,
    )
    bridge_signal_fields = {
        "materialization_generation": 1,
        "trigger_crossed_at": "2026-09-12T16:00:00+00:00",
        "point_in_time": {
            "phase": "BREACH",
            "as_of": "2026-09-12T16:00:00+00:00",
            "data_sources": {"candles": {}, "coverage": {}},
            "underlying_observation": None,
            "provenance": {"source": "test.614"},
        },
    }
    try:
        result = _run_entry_trigger(
            monkeypatch,
            mode="off",
            bridge_signal_fields=bridge_signal_fields,
        )

        assert background_started.is_set()
        assert not background_finished.is_set()
        result["osm"].submit_existing_entry.assert_called_once()
        result["osm"].expire_pending_entry.assert_not_called()
    finally:
        release_background.set()
        assert background_finished.wait(1.0)
        for thread in workers:
            thread.join(timeout=1.0)
        breach_bridge_mod.reset_breach_bridge_state_for_tests()


def test_real_entry_trigger_accepted_bridge_has_no_duplicate_selector_surface(
    monkeypatch,
):
    selector = MagicMock()
    monkeypatch.setattr(
        breach_bridge_mod,
        "submit_breach_intelligence_nonblocking",
        lambda *_args, **_kwargs: {
            "ok": True,
            "accepted": True,
            "handoff_status": "ACCEPTED",
        },
    )
    result = _run_entry_trigger(
        monkeypatch,
        mode="off",
        contract_selector=selector,
    )

    result["osm"].submit_existing_entry.assert_called_once()
    result["osm"].expire_pending_entry.assert_not_called()
    selector.select.assert_not_called()


@pytest.mark.parametrize(
    "bridge_behavior",
    [
        (
            "accepted",
            lambda: {
                "ok": True,
                "accepted": True,
                "handoff_status": "ACCEPTED",
            },
        ),
        (
            "bridge-exception",
            lambda: (_ for _ in ()).throw(RuntimeError("bridge unavailable")),
        ),
        (
            "saturated-or-rejected",
            lambda: {
                "ok": True,
                "accepted": False,
                "handoff_status": "SATURATED_OR_REJECTED",
            },
        ),
        (
            "missing-or-invalid-intelligence",
            lambda: {
                "ok": False,
                "accepted": False,
                "handoff_status": "INVALID_ARTIFACT",
                "fallback_reason": "BREACH_INTEL_EVIDENCE_MISSING",
            },
        ),
    ],
    ids=lambda item: item[0] if isinstance(item, tuple) else str(item),
)
def test_real_deferred_entry_trigger_calls_selector_once_for_each_bridge_outcome(
    monkeypatch, bridge_behavior
):
    """The observe-only bridge never suppresses the deferred selector path."""

    _label, behavior = bridge_behavior

    class _RecordingSelector:
        def __init__(self):
            self.calls = 0
            self.last_plan = None
            self.last_request_context = None

        def select(self, plan, *, request_context=None):
            self.calls += 1
            self.last_plan = plan
            self.last_request_context = request_context
            return None

    selector = _RecordingSelector()

    def bridge_result(*_args, **_kwargs):
        result = behavior()
        if isinstance(result, dict):
            return dict(result)
        return result

    monkeypatch.setattr(
        breach_bridge_mod,
        "submit_breach_intelligence_nonblocking",
        bridge_result,
    )
    monkeypatch.setenv("SELECTOR_DURABLE_RECOVERY_CURSOR_ENABLED", "0")

    result = _run_entry_trigger(
        monkeypatch,
        mode="off",
        execution_mode="paper",
        invoke_direct_trigger=False,
        contract_selector=selector,
    )
    core = result["core"]
    plan = result["core"]._recover_plan_for_revalidation.return_value
    plan.contract_symbol = "DEFERRED:AAPL"
    plan.metadata["contract_deferred"] = True
    result["watched"].signal["contract_deferred"] = True
    result["watched"].signal["contract_symbol"] = "DEFERRED:AAPL"
    durable_row = result["osm"].get_order("local-1")
    durable_row["contract"] = "DEFERRED:AAPL"

    # This is the real callback entrypoint and the real selector attribute used
    # by its deferred branch; no synthetic selector marker is consulted.
    core._on_entry_trigger(result["watched"])

    assert selector.calls == 1
    assert selector.last_plan is plan
    assert selector.last_request_context is not None


@pytest.mark.parametrize("accepted", [True, False], ids=["accepted", "rejected"])
def test_real_entry_trigger_bridge_result_cannot_change_disposition(
    monkeypatch, accepted
):
    monkeypatch.setattr(
        breach_bridge_mod,
        "submit_breach_intelligence_nonblocking",
        lambda *_args, **_kwargs: {
            "ok": True,
            "accepted": accepted,
            "handoff_status": "ACCEPTED" if accepted else "SATURATED_OR_REJECTED",
        },
    )
    result = _run_entry_trigger(monkeypatch, mode="off")

    result["osm"].submit_existing_entry.assert_called_once()
    result["osm"].expire_pending_entry.assert_not_called()
