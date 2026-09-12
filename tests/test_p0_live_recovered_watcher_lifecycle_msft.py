from __future__ import annotations

from copy import deepcopy
import os
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

import ap_lifecycle
import ap_entry_watcher as ew
from ap.pending_trigger_restart_recovery import (
    PendingTriggerRestartRecovery,
    _RowOutcome,
)


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
        "ticker": "MSFT",
        "contract": "DEFERRED:MSFT",
        "direction": "CALL",
        "entry_price": 494.52,
        "stop_price": 489.44,
        "target_price": 505.00,
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "client_id": JASON_CLIENT_ID,
            "execution_mode": "live",
            "signal_id": MSFT_SIGNAL_ID,
            "canonical_signal_id": MSFT_SIGNAL_ID,
            "side": "CALL",
            "direction": "CALL",
            "trigger_price": 494.52,
            "stop_price": 489.44,
            "target_price": 505.00,
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


class _RecoveryOSM:
    """Exact-row test double that records forbidden recovery mutations."""

    def __init__(self, row: dict):
        self.client_id = JASON_CLIENT_ID
        self._row = deepcopy(row)
        self.meta_writes: list[tuple] = []
        self.cancel_calls: list[tuple] = []
        self.submit_calls: list[tuple] = []
        self.position_calls: list[tuple] = []
        self.proof_calls: list[tuple] = []

    def get_order(self, local_order_id: str):
        if local_order_id != MSFT_LOCAL_ORDER_ID:
            return None
        return deepcopy(self._row)

    def update_order_meta(self, local_order_id: str, patch: dict, **kwargs):
        self.meta_writes.append((local_order_id, deepcopy(patch), deepcopy(kwargs)))
        self._row["meta"] = {
            **(self._row.get("meta") or {}),
            **dict(patch),
        }
        return True

    def cancel_pending_entry(self, local_order_id: str, **kwargs):
        self.cancel_calls.append((local_order_id, deepcopy(kwargs)))
        return True

    def submit_existing_entry(self, local_order_id: str, **kwargs):
        self.submit_calls.append((local_order_id, deepcopy(kwargs)))
        return True

    def update_position(self, *args, **kwargs):
        self.position_calls.append((args, deepcopy(kwargs)))
        return True

    def write_proof_trade(self, *args, **kwargs):
        self.proof_calls.append((args, deepcopy(kwargs)))
        return True


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


def _production_recovery_watcher() -> tuple[ew.APEntryWatcher, _RecoveryOSM]:
    row = _exact_live_row()
    osm = _RecoveryOSM(row)
    watcher = ew.APEntryWatcher(
        broker=MagicMock(),
        order_state_machine=osm,
        require_on_trigger=False,
        mode="LIVE",
    )
    # Recovery audit is observability only; suppress it here so the test can
    # prove that recovery admission itself does not write the durable order.
    watcher._persist_watcher_audit = MagicMock()
    watcher._is_regular_session_now = MagicMock(return_value=False)
    watcher._is_past_entry_cutoff_now = MagicMock(return_value=False)
    # The real watch() arm-time path still runs.  Keep its regular-session
    # quote gate deterministic and safely below the MSFT trigger.
    watcher._get_quote = MagicMock(
        return_value={"bid": 494.50, "ask": 494.51}
    )
    return watcher, osm


def test_failfirst_recovered_live_msft_production_path_restores_and_dispatches():
    """2026-09-11 production incident: recovered ownership must be executable.

    Exercise the actual restart caller rather than direct registry insertion:
    PendingTriggerRestartRecovery -> watch(recovery_rearm=True) -> registration
    -> two valid CALL observations -> durable trigger persistence -> one callback.
    """
    _clear_lifecycle()
    watcher, osm = _production_recovery_watcher()
    row = _exact_live_row()
    recovery = PendingTriggerRestartRecovery(
        client_id=JASON_CLIENT_ID,
        execution_mode="live",
        osm=osm,
        entry_watcher=watcher,
        broker=MagicMock(),
        quote_check_fn=lambda *_args: False,
        caller_source="p0_msft_recovery_test",
    )

    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.WATCHER_OWNED
    assert recovery.last_watcher_registered_by_this_attempt is True
    assert len(watcher._pending) == 1
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) == ap_lifecycle.SignalState.WATCHING
    # No recovery-time order, broker, position, or proof mutation is allowed.
    assert osm.meta_writes == []
    assert osm.cancel_calls == []
    assert osm.submit_calls == []
    assert osm.position_calls == []
    assert osm.proof_calls == []

    # This replay covers two regular-session breach polls.  Pin that intent
    # explicitly so the proof does not become runner-wall-clock dependent after
    # 16:00 ET, when the production watcher correctly queues new arms overnight.
    watcher._pending[0].overnight = False

    callback = MagicMock(return_value={"disposition": "KEEP_WATCHER"})
    watcher.on_trigger = callback
    watcher._fetch_quotes = MagicMock(
        return_value={"MSFT": {"bid": 494.50, "ask": 494.53}}
    )

    watcher._poll_active_signals(False)
    assert callback.call_count == 0
    watcher._poll_active_signals(False)

    assert callback.call_count == 1
    assert len(osm.meta_writes) == 1
    trigger_patch = osm.meta_writes[0][1]
    assert trigger_patch["trigger_crossed_at"]
    assert trigger_patch["trigger_crossed_at_provenance"] == {
        "canonical_signal_id": MSFT_SIGNAL_ID,
        "client_id": JASON_CLIENT_ID,
        "execution_mode": "live",
        "local_order_id": MSFT_LOCAL_ORDER_ID,
    }
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) == ap_lifecycle.SignalState.TRIGGER_READY
    # The trigger transition is now legal and reaches the existing lifecycle
    # owner; no broker submit is invented by this watcher test.
    assert any(
        entry.to_state == ap_lifecycle.SignalState.TRIGGER_READY
        for entry in ap_lifecycle.LEDGER.history(MSFT_SIGNAL_ID)
    )
    assert osm.cancel_calls == []
    assert osm.submit_calls == []
    assert osm.position_calls == []
    assert osm.proof_calls == []


def test_recovery_admission_duplicate_observation_does_not_create_second_owner():
    _clear_lifecycle()
    watcher, osm = _production_recovery_watcher()
    row = _exact_live_row()
    recovery = PendingTriggerRestartRecovery(
        client_id=JASON_CLIENT_ID,
        execution_mode="live",
        osm=osm,
        entry_watcher=watcher,
        broker=MagicMock(),
        quote_check_fn=lambda *_args: False,
    )

    assert recovery.recover_one_row(row) == _RowOutcome.WATCHER_OWNED
    assert recovery.recover_one_row(row) == _RowOutcome.WATCHER_OWNED

    assert len(watcher._pending) == 1
    assert recovery.last_watcher_registered_by_this_attempt is False
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) == ap_lifecycle.SignalState.WATCHING
    assert osm.meta_writes == []
    assert osm.cancel_calls == []
    assert osm.submit_calls == []


def test_recovery_watch_missing_signal_id_fails_before_uuid_synthesis():
    _clear_lifecycle()
    watcher = _watcher()
    watcher.add_signal = MagicMock(return_value=True)
    plan = SimpleNamespace(
        signal_id="",
        canonical_signal_id="",
        ticker="MSFT",
        side="CALL",
        metadata={},
    )

    accepted = watcher.watch(
        plan,
        MSFT_LOCAL_ORDER_ID,
        recovery_rearm=True,
    )

    assert accepted is False
    watcher.add_signal.assert_not_called()
    assert watcher._last_reject_reason == ew.RECOVERY_TRIGGER_EVIDENCE_IDENTITY_UNPROVEN
    assert watcher._pending == []
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) is None


@pytest.mark.parametrize(
    ("prior_state", "accepted", "expected_state"),
    [
        (None, True, ap_lifecycle.SignalState.WATCHING),
        (ap_lifecycle.SignalState.ADOPTED, True, ap_lifecycle.SignalState.WATCHING),
        (ap_lifecycle.SignalState.WATCHING, True, ap_lifecycle.SignalState.WATCHING),
        (ap_lifecycle.SignalState.TRIGGER_READY, False, ap_lifecycle.SignalState.TRIGGER_READY),
        (ap_lifecycle.SignalState.ENTRY_SUBMITTED, False, ap_lifecycle.SignalState.ENTRY_SUBMITTED),
        (ap_lifecycle.SignalState.ERROR, False, ap_lifecycle.SignalState.ERROR),
    ],
)
def test_recovery_registration_accepts_only_existing_legal_lifecycle_states(
    prior_state, accepted, expected_state
):
    _clear_lifecycle()
    if prior_state is not None:
        with ap_lifecycle.LEDGER._entry_lock:
            ap_lifecycle.LEDGER._current_state[MSFT_SIGNAL_ID] = prior_state
    watcher = _watcher()

    result = watcher.add_signal(_recovered_signal())

    assert result is accepted
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) == expected_state
    assert (len(watcher._pending) == 1) is accepted
    if not accepted:
        assert watcher._dedup_set == set()


def test_recovered_missing_signal_id_rolls_back_exact_registration_without_lifecycle():
    _clear_lifecycle()
    watcher = _watcher()
    signal = _recovered_signal()
    signal["signal_id"] = ""
    provenance: dict = {}

    accepted = watcher.add_signal(
        signal,
        registration_provenance_out=provenance,
    )

    assert accepted is False
    assert watcher._pending == []
    assert watcher._dedup_set == set()
    assert provenance == {
        "created_by_this_call": False,
        "registration_token": None,
    }
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) is None


def test_recovery_missing_ticker_fails_closed_without_lifecycle():
    _clear_lifecycle()
    watcher = _watcher()
    signal = _recovered_signal()
    signal.pop("ticker")

    with pytest.raises(ValueError, match="requires ticker"):
        watcher.add_signal(signal)

    assert watcher._pending == []
    assert watcher._dedup_set == set()
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) is None


def test_recovery_restoration_failure_rolls_back_the_exact_object_and_dedup_key():
    _clear_lifecycle()
    watcher = _watcher()
    provenance: dict = {}
    created: list = []

    def _fail_after_registration(watched):
        created.append(watched)
        return False

    watcher._restore_recovered_watcher_lifecycle = _fail_after_registration
    accepted = watcher.add_signal(
        _recovered_signal(),
        registration_provenance_out=provenance,
    )

    assert accepted is False
    assert created
    assert all(item is not created[0] for item in watcher._pending)
    assert MSFT_SIGNAL_ID not in watcher._dedup_set
    assert provenance == {
        "created_by_this_call": False,
        "registration_token": None,
    }
    assert ap_lifecycle.LEDGER.current_state(MSFT_SIGNAL_ID) is None


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
