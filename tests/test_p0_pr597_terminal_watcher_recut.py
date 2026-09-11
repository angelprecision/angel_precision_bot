"""Focused P0 coverage for exact terminal deferred-watcher convergence."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from ap_entry_watcher import APEntryWatcher, WatchedSignal


TMO_LOCAL_ORDER_ID = "b9e29854-1c97-43d6-986b-eef0506bdf5a"
CLIENT_ID = "tmo-live@example.com"
SIGNAL_ID = "tmo-signal-1"
CANONICAL_SIGNAL_ID = SIGNAL_ID
TERMINAL_REASON = "restart_stuck_trigger_ready_no_broker_proof"


def _signal(**overrides):
    signal = {
        "ticker": "TMO",
        "side": "CALL",
        "entry_price": 100.0,
        "stop_price": 90.0,
        "target_price": 120.0,
        "contract_symbol": "DEFERRED:TMO",
        "local_order_id": TMO_LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "LIVE",
        "signal_id": SIGNAL_ID,
        "canonical_signal_id": CANONICAL_SIGNAL_ID,
    }
    signal.update(overrides)
    return signal


def _row(**overrides):
    row = {
        "kind": "ENTRY",
        "local_order_id": TMO_LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "LIVE",
        "signal_id": SIGNAL_ID,
        "canonical_signal_id": CANONICAL_SIGNAL_ID,
        "symbol": "TMO",
        "direction": "CALL",
        "status": "CANCELED",
        "broker_order_id": None,
        "submitted_ts": None,
        "last_error": TERMINAL_REASON,
        "meta": {},
    }
    row.update(overrides)
    return row


def _setup(row, *, callback=None, update_result=True, signal=None):
    osm = MagicMock()
    osm.get_order.return_value = row
    osm.update_order_meta.return_value = update_result
    broker = MagicMock()
    watcher = APEntryWatcher(broker, order_state_machine=osm, mode="LIVE")
    watcher._persist_watcher_audit = MagicMock()
    watched = WatchedSignal(signal or _signal(), overnight=False)
    watched._watcher_ref = watcher
    watcher._pending.append(watched)
    watcher._dedup_set.add(watched.signal_id)
    watcher._fetch_quotes = MagicMock(
        return_value={watched.ticker: {"bid": 101.0, "ask": 101.0}}
    )
    watcher.on_trigger = callback or MagicMock(return_value={"disposition": "SUBMITTED"})
    return watcher, watched, osm, broker


def _poll_to_completed(watcher):
    watcher._poll_active_signals()
    watcher._poll_active_signals()


def test_terminal_tmo_row_is_removed_before_dispatch_and_stays_removed():
    watcher, watched, osm, broker = _setup(_row(), update_result=False)

    _poll_to_completed(watcher)
    watcher._poll_active_signals()

    assert watched not in watcher._pending
    assert watched.signal_id not in watcher._dedup_set
    watcher.on_trigger.assert_not_called()
    osm.cancel_pending_entry.assert_not_called()
    broker.assert_not_called()
    osm.update_order_meta.assert_not_called()


@pytest.mark.parametrize(
    "field, value",
    [
        ("last_error", TERMINAL_REASON),
        ("meta", {"restart_recovery_terminal_reason": TERMINAL_REASON}),
        ("meta", {"terminal_reason": TERMINAL_REASON}),
        ("meta", {"reason_code": TERMINAL_REASON}),
        ("meta", {"final_reason": TERMINAL_REASON}),
        ("meta", {"watcher_invalidation_reason": TERMINAL_REASON}),
    ],
)
def test_all_canonical_terminal_reason_aliases_converge(field, value):
    row = _row(last_error=None, meta={})
    if field == "last_error":
        row[field] = value
    else:
        row[field] = value
    watcher, watched, _, _ = _setup(row)

    assert watcher._resolve_trigger_callback_disposition(
        watched, {"disposition": "SUBMITTED"}
    ) == ("TERMINAL_DURABLE", None)


def test_legacy_materialization_reason_is_accepted_but_cannot_conflict():
    row = _row(
        last_error=TERMINAL_REASON,
        meta={
            "terminal_reason": TERMINAL_REASON,
            "materialization_reason": "historical_selector_diagnostic",
        },
    )
    watcher, watched, _, _ = _setup(row)

    assert watcher._resolve_trigger_callback_disposition(
        watched, {"disposition": "SUBMITTED"}
    ) == ("TERMINAL_DURABLE", None)


def test_materialization_reason_alone_is_legacy_terminal_authority():
    row = _row(last_error=None, meta={"materialization_reason": TERMINAL_REASON})
    watcher, watched, _, _ = _setup(row)

    assert watcher._resolve_trigger_callback_disposition(
        watched, {"disposition": "SUBMITTED"}
    ) == ("TERMINAL_DURABLE", None)


@pytest.mark.parametrize(
    "meta",
    [{}, {"watcher_audit": {"reason_code": "trigger_ready"}}],
)
def test_terminal_without_canonical_reason_is_hold(meta):
    watcher, watched, osm, broker = _setup(_row(last_error=None, meta=meta))

    _poll_to_completed(watcher)

    assert watched in watcher._pending
    assert watched.signal_id in watcher._dedup_set
    watcher.on_trigger.assert_not_called()
    osm.cancel_pending_entry.assert_not_called()
    broker.assert_not_called()


def test_conflicting_canonical_terminal_reasons_are_hold():
    row = _row(
        last_error="terminal_reason_a",
        meta={"terminal_reason": "terminal_reason_b"},
    )
    watcher, watched, _, _ = _setup(row)

    _poll_to_completed(watcher)

    assert watched in watcher._pending
    assert watched.signal_id in watcher._dedup_set
    watcher.on_trigger.assert_not_called()


@pytest.mark.parametrize("missing", ["local_order_id", "client_id", "execution_mode", "signal_id"])
def test_missing_exact_identity_is_hold(missing):
    watcher, watched, _, _ = _setup(_row())
    if missing == "signal_id":
        watched.signal_id = ""
        watched.signal.pop("signal_id", None)
    else:
        watched.signal[missing] = ""

    classification, _, _ = watcher._read_deferred_terminal_truth(watched)

    assert classification == "HOLD"


@pytest.mark.parametrize(
    "row_patch",
    [
        {"local_order_id": "different-order"},
        {"client_id": "other@example.com"},
        {"execution_mode": "PAPER"},
        {"signal_id": "different-signal"},
        {"canonical_signal_id": "different-canonical"},
    ],
)
def test_identity_mismatch_keeps_exact_watcher_and_suppresses_callback(row_patch):
    watcher, watched, osm, broker = _setup(_row(**row_patch))

    _poll_to_completed(watcher)

    assert watched in watcher._pending
    assert watched.signal_id in watcher._dedup_set
    watcher.on_trigger.assert_not_called()
    osm.cancel_pending_entry.assert_not_called()
    broker.assert_not_called()


@pytest.mark.parametrize(
    "row_patch",
    [
        {"broker_order_id": "TRADIER-1"},
        {"submitted_ts": "2026-09-11T16:00:00+00:00"},
        {"meta": {"submit_intent_at": "2026-09-11T16:00:00+00:00"}},
        {"meta": {"broker_submit_key": TMO_LOCAL_ORDER_ID}},
        {"meta": {"broker_submit_payload_hash": "hash"}},
        {"meta": {"broker_ready": True}},
        {"meta": {"materialization": {"broker_ready": True}}},
        {"meta": {"submit_intent_owner": "watcher:terminal-convergence"}},
        {"meta": {"recovery_submit_owner": "recovery:terminal-convergence"}},
    ],
)
def test_terminal_broker_handoff_evidence_is_reconciliation_hold(row_patch):
    watcher, watched, osm, broker = _setup(_row(**row_patch))

    assert watcher._resolve_trigger_callback_disposition(
        watched, {"disposition": "SUBMITTED"}
    ) == ("RECONCILE_BROKER_INTENT", None)
    _poll_to_completed(watcher)

    assert watched in watcher._pending
    watcher.on_trigger.assert_not_called()
    osm.cancel_pending_entry.assert_not_called()
    broker.assert_not_called()


def test_unrelated_same_ticker_watcher_survives_terminal_convergence():
    first_row = _row()
    second_signal = _signal(
        local_order_id="other-order",
        client_id="other@example.com",
        signal_id="other-signal",
        canonical_signal_id="other-signal",
    )
    second_row = _row(
        local_order_id="other-order",
        client_id="other@example.com",
        signal_id="other-signal",
        canonical_signal_id="other-signal",
        status="PENDING_TRIGGER",
        last_error=None,
    )
    osm = MagicMock()
    rows = {TMO_LOCAL_ORDER_ID: first_row, "other-order": second_row}
    osm.get_order.side_effect = lambda local_order_id: rows[local_order_id]
    osm.update_order_meta.return_value = False
    watcher = APEntryWatcher(MagicMock(), order_state_machine=osm, mode="LIVE")
    watcher._persist_watcher_audit = MagicMock()
    first = WatchedSignal(_signal(), overnight=False)
    second = WatchedSignal(second_signal, overnight=False)
    for item in (first, second):
        item._watcher_ref = watcher
        watcher._pending.append(item)
        watcher._dedup_set.add(item.signal_id)
    watcher._fetch_quotes = MagicMock(
        return_value={"TMO": {"bid": 101.0, "ask": 101.0}}
    )
    watcher.on_trigger = MagicMock(return_value={"disposition": "SUBMITTED"})

    _poll_to_completed(watcher)

    assert first not in watcher._pending
    assert second in watcher._pending
    assert second.signal_id in watcher._dedup_set
    watcher.on_trigger.assert_not_called()


def test_ordinary_pending_trigger_still_reaches_existing_trigger_authority_cas():
    row = _row(status="PENDING_TRIGGER", last_error=None, meta={})
    watcher, watched, osm, _ = _setup(row, update_result=True)

    _poll_to_completed(watcher)

    watcher.on_trigger.assert_called_once_with(watched)
    assert watched in watcher._pending
    assert watched.signal_id in watcher._dedup_set
    assert any(
        call.kwargs.get("expected_status") == "PENDING_TRIGGER"
        and call.kwargs.get("expected_execution_mode") == "live"
        and call.kwargs.get("expected_signal_id") == SIGNAL_ID
        for call in osm.update_order_meta.call_args_list
    )


def test_mid_callback_terminalization_beats_stale_submitted_claim():
    row = _row(status="PENDING_TRIGGER", last_error=None, meta={})

    def _callback(_watched):
        row.update(
            status="CANCELED",
            last_error=TERMINAL_REASON,
            meta={"restart_recovery_terminal_reason": TERMINAL_REASON},
        )
        return {"disposition": "SUBMITTED"}

    watcher, watched, osm, broker = _setup(
        row, callback=MagicMock(side_effect=_callback), update_result=True
    )

    _poll_to_completed(watcher)

    watcher.on_trigger.assert_called_once_with(watched)
    assert watched not in watcher._pending
    assert watched.signal_id not in watcher._dedup_set
    osm.cancel_pending_entry.assert_not_called()
    broker.assert_not_called()


def test_two_terminal_observations_are_idempotent():
    watcher, watched, _, _ = _setup(_row())
    completed = [("trigger", watched)]

    assert watcher._converge_terminal_deferred_watchers(completed) == []
    assert watcher._converge_terminal_deferred_watchers(completed) == []
    assert watcher._pending == []
    assert watched.signal_id not in watcher._dedup_set
