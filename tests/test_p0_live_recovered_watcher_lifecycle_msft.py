from __future__ import annotations

from unittest.mock import MagicMock

import ap_lifecycle
import ap_entry_watcher as ew


MSFT_SIGNAL_ID = "c493c9dc-ceaa-4c86-8b62-08c3cc8bad5c"
MSFT_LOCAL_ORDER_ID = "d6dd2c80-d0a1-45ca-bda9-4813ba7e64ba"
JASON_CLIENT_ID = "jasoncosby1@gmail.com"


def _clear_lifecycle(signal_id: str = MSFT_SIGNAL_ID) -> None:
    # Test-only reset of the process-local singleton. Existing watcher tests use
    # the same guarded reset because lifecycle memory intentionally survives
    # within one Python process.
    with ap_lifecycle.LEDGER._entry_lock:
        ap_lifecycle.LEDGER._current_state.pop(signal_id, None)


def _exact_live_row() -> dict:
    return {
        "local_order_id": MSFT_LOCAL_ORDER_ID,
        "client_id": JASON_CLIENT_ID,
        "execution_mode": "live",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "signal_id": MSFT_SIGNAL_ID,
        "canonical_signal_id": MSFT_SIGNAL_ID,
        "symbol": "MSFT",
        "contract": "DEFERRED:MSFT",
        "direction": "CALL",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "client_id": JASON_CLIENT_ID,
            "execution_mode": "live",
            "signal_id": MSFT_SIGNAL_ID,
            "canonical_signal_id": MSFT_SIGNAL_ID,
            "side": "CALL",
            "direction": "CALL",
        },
    }


def _recovered_signal(*, materialization_resume: bool = False) -> dict:
    signal = {
        "signal_id": MSFT_SIGNAL_ID,
        "canonical_signal_id": MSFT_SIGNAL_ID,
        "ticker": "MSFT",
        "side": "CALL",
        "score": 70,
        "grade": "B",
        "entry_price": 494.52,
        "entry_trigger": 494.52,
        "stop_price": 489.44,
        "target_price": 505.00,
        "local_order_id": MSFT_LOCAL_ORDER_ID,
        "client_id": JASON_CLIENT_ID,
        "execution_mode": "live",
        "contract_symbol": "DEFERRED:MSFT",
        "contract_deferred": True,
        "timeframe": "1d",
        "pattern": "2-3",
        "__recovery_rearm": True,
        "metadata": {
            "client_id": JASON_CLIENT_ID,
            "execution_mode": "live",
            "signal_id": MSFT_SIGNAL_ID,
            "canonical_signal_id": MSFT_SIGNAL_ID,
            "side": "CALL",
            "direction": "CALL",
        },
    }
    if materialization_resume:
        signal["__materialization_resume"] = True
    return signal


def _watcher() -> ew.APEntryWatcher:
    osm = MagicMock()
    osm.client_id = JASON_CLIENT_ID
    osm.get_order.return_value = _exact_live_row()

    watcher = ew.APEntryWatcher(
        broker=MagicMock(),
        order_state_machine=osm,
        require_on_trigger=False,
        mode="LIVE",
    )
    # add_signal() itself performs no market-data fetch. Keep callbacks inert so
    # this test isolates the committed registry/lifecycle admission boundary.
    watcher.on_trigger = None
    watcher.on_expire = None
    watcher.on_invalidate = None
    return watcher


def test_failfirst_recovered_live_msft_registration_restores_watching_lifecycle():
    """2026-09-11 production incident: recovered ownership must be executable.

    Base before this PR registers the recovered watcher in `_pending` while the
    process-local lifecycle remains None. A later legitimate breach therefore
    attempts NONE -> TRIGGER_READY. The fix must restore the already persisted
    signal through the existing legal recovery path: NONE -> ADOPTED -> WATCHING.
    """
    _clear_lifecycle()
    watcher = _watcher()
    provenance: dict = {}

    accepted = watcher.add_signal(
        _recovered_signal(),
        registration_provenance_out=provenance,
    )

    assert accepted is True
    assert provenance.get("created_by_this_call") is True
    assert len(watcher._pending) == 1
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) == ap_lifecycle.SignalState.WATCHING


def test_materialization_resume_does_not_create_parallel_lifecycle_owner():
    """#607 owns deferred retry adoption; this PR must stay out of that seam."""
    _clear_lifecycle()
    watcher = _watcher()

    accepted = watcher.add_signal(_recovered_signal(materialization_resume=True))

    assert accepted is True
    assert len(watcher._pending) == 1
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) is None


def test_ordinary_add_signal_behavior_is_unchanged_by_recovery_bridge():
    _clear_lifecycle()
    watcher = _watcher()
    signal = _recovered_signal()
    signal.pop("__recovery_rearm", None)

    accepted = watcher.add_signal(signal)

    assert accepted is True
    assert len(watcher._pending) == 1
    # Ordinary callers retain their existing lifecycle owner; this narrow PR
    # must not manufacture an ADOPTED state for a normal arm.
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) is None
