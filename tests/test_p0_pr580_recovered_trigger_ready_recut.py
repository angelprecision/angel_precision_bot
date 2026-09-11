"""Fail-first P0 proof for the clean #580 recovered-trigger-ready recut."""

from __future__ import annotations

import copy
import inspect
import uuid
import os
from datetime import datetime as _RealDateTime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

import ap_entry_watcher as ew
from ap.pending_trigger_restart_recovery import (
    PendingTriggerRestartRecovery,
    _RowOutcome,
)


CLIENT_ID = "recovered-trigger-ready@example.com"
EXECUTION_MODE = "paper"


def _row() -> dict:
    local_order_id = f"local-{uuid.uuid4()}"
    signal_id = f"signal-{uuid.uuid4()}"
    canonical_signal_id = f"canonical-{uuid.uuid4()}"
    provenance = {
        "canonical_signal_id": canonical_signal_id,
        "client_id": CLIENT_ID,
        "execution_mode": EXECUTION_MODE,
        "local_order_id": local_order_id,
    }
    return {
        "kind": "ENTRY",
        "local_order_id": local_order_id,
        "signal_id": signal_id,
        "canonical_signal_id": canonical_signal_id,
        "client_id": CLIENT_ID,
        "execution_mode": EXECUTION_MODE,
        "status": "PENDING_TRIGGER",
        "direction": "CALL",
        "ticker": "SPY",
        "entry_price": 450.0,
        "stop_price": 440.0,
        "target_price": 455.0,
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "trigger_price": 450.0,
            "trigger_crossed_at": "2026-09-11T16:00:00.123456Z",
            "trigger_crossed_at_provenance": provenance,
            "watcher_audit": {"reason_code": "trigger_ready"},
        },
    }


class _RecordingOSM:
    def __init__(self, row: dict):
        self.row = row
        self.cancel_calls: list[tuple[str, str]] = []
        self.meta_writes: list[tuple[str, dict]] = []
        self.meta_write_calls: list[tuple[str, dict, dict]] = []
        self.authority_reads: list[tuple[str, dict]] = []

    def get_order(self, local_order_id: str):
        if local_order_id != self.row["local_order_id"]:
            return None
        return self.row

    def update_order_meta(self, local_order_id: str, patch: dict, **kwargs) -> bool:
        self.meta_writes.append((local_order_id, dict(patch)))
        self.meta_write_calls.append((local_order_id, dict(patch), dict(kwargs)))
        self.row["meta"].update(dict(patch))
        return True

    def read_trigger_confirmation_authority(
        self,
        local_order_id: str,
        *,
        client_id: str,
        execution_mode: str,
        signal_id: str,
        canonical_signal_id: str,
        expected_materialization_generation=None,
    ) -> dict:
        self.authority_reads.append(
            (
                local_order_id,
                {
                    "client_id": client_id,
                    "execution_mode": execution_mode,
                    "signal_id": signal_id,
                    "canonical_signal_id": canonical_signal_id,
                    "expected_materialization_generation": expected_materialization_generation,
                },
            )
        )
        provenance = self.row["meta"].get("trigger_crossed_at_provenance")
        return {
            "proven": local_order_id == self.row["local_order_id"]
            and client_id == self.row["client_id"]
            and execution_mode == self.row["execution_mode"]
            and signal_id == self.row["signal_id"]
            and canonical_signal_id == self.row["canonical_signal_id"],
            "trigger_crossed_at": self.row["meta"].get("trigger_crossed_at"),
            "trigger_crossed_at_provenance": copy.deepcopy(provenance),
        }

    def cancel_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.cancel_calls.append((local_order_id, reason))
        self.row["status"] = "CANCELED"
        self.row["last_error"] = reason
        return True


class _TestWatcher(ew.APEntryWatcher):
    def __init__(self, broker, *, order_state_machine, mode="PAPER"):
        super().__init__(
            broker,
            order_state_machine=order_state_machine,
            mode=mode,
        )
        self.audit_calls: list[tuple[str, dict]] = []

    def _persist_watcher_audit(self, local_order_id, payload):
        self.audit_calls.append((local_order_id, dict(payload)))


_FIXED_NOW = _RealDateTime(2026, 9, 11, 15, 0, tzinfo=timezone.utc)


class _FixedDateTime(_RealDateTime):
    @classmethod
    def now(cls, tz=None):
        if tz is None:
            return _FIXED_NOW.replace(tzinfo=None)
        return _FIXED_NOW.astimezone(tz)


def _fixed_watcher_time():
    return patch.object(ew._base, "datetime", _FixedDateTime)


def _recovered_watcher(row: dict):
    osm = _RecordingOSM(row)
    broker = MagicMock()
    watcher = _TestWatcher(
        broker,
        order_state_machine=osm,
        mode=EXECUTION_MODE.upper(),
    )
    watcher._get_quote = MagicMock(return_value={"bid": 0, "ask": 0})
    watcher._fetch_quotes = MagicMock(return_value={row["ticker"]: {}})
    callback = MagicMock(return_value={"disposition": "SUBMITTED"})
    watcher.on_trigger = callback
    quote_calls: list[tuple] = []

    def _quote_check(*args):
        quote_calls.append(args)
        return True

    recovery = PendingTriggerRestartRecovery(
        client_id=CLIENT_ID,
        execution_mode=EXECUTION_MODE,
        osm=osm,
        entry_watcher=watcher,
        broker=broker,
        quote_check_fn=_quote_check,
    )
    return recovery, watcher, osm, broker, callback, quote_calls


def _active_materialization_meta(row: dict) -> dict:
    now = _RealDateTime.now(timezone.utc)
    return {
        "watcher_audit": {"reason_code": "trigger_ready"},
        "lifecycle_state": "MATERIALIZING",
        "materialization_status": "RUNNING",
        "materialization_in_flight": True,
        "materialization_owner": "materializer:recovered-trigger-ready",
        "materialization_generation": 1,
        "materialization_lease_until": (now + timedelta(minutes=5)).isoformat(),
        "trigger_crossed_at": row["meta"]["trigger_crossed_at"],
        "broker_ready": False,
    }


def _canonical_retry_meta(row: dict) -> dict:
    now = _RealDateTime.now(timezone.utc)
    return {
        "watcher_audit": {"reason_code": "trigger_ready"},
        "lifecycle_state": "RETRY_WAIT",
        "materialization_status": "RETRY_PENDING",
        "materialization_in_flight": False,
        "materialization_attempts": 1,
        "retry_attempt": 1,
        "breach_attempt_count": 1,
        "materialization_generation": 1,
        "retry_max_attempts": 20,
        "materialization_next_retry_at": (now + timedelta(minutes=5)).isoformat(),
        "materialization_last_failure_at": now.isoformat(),
        "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
        "materialization_reason": "NO_CHAIN_DATA",
        "materialization_selector_failure": {
            "reason_code": "NO_CHAIN_DATA"
        },
        "absolute_entry_deadline": (now + timedelta(hours=1)).isoformat(),
        "trigger_crossed_at": row["meta"]["trigger_crossed_at"],
        "trigger_crossed_at_provenance": copy.deepcopy(
            row["meta"]["trigger_crossed_at_provenance"]
        ),
        "broker_ready": False,
        "contract_deferred": True,
    }


def _watcher_signal(row: dict, **overrides) -> dict:
    signal = {
        "signal_id": row["signal_id"],
        "canonical_signal_id": row["canonical_signal_id"],
        "local_order_id": row["local_order_id"],
        "client_id": row["client_id"],
        "execution_mode": row["execution_mode"],
        "ticker": row["ticker"],
        "side": row["direction"],
        "entry_price": row["entry_price"],
        "stop_price": row["stop_price"],
        "target_price": row["target_price"],
        "score": 80.0,
        "grade": "A",
    }
    signal.update(overrides)
    return signal


class _RecordingWatcher:
    def __init__(self):
        self._pending: list = []
        self._dedup_set: set[str] = set()
        self.watch_calls: list[tuple[object, str, bool]] = []

    def watch(
        self,
        plan,
        local_order_id: str,
        *,
        recovery_rearm: bool = False,
        registration_provenance_out: dict | None = None,
    ) -> bool:
        self.watch_calls.append((plan, local_order_id, recovery_rearm))
        watcher = type(
            "RegisteredWatcher",
            (),
            {
                "signal": {
                    "local_order_id": local_order_id,
                    "signal_id": plan.get("signal_id"),
                    "client_id": plan.get("client_id"),
                    "execution_mode": plan.get("execution_mode"),
                },
                "state": "PENDING",
                "_ownership_quarantine": False,
            },
        )()
        self._pending.append(watcher)
        self._dedup_set.add(watcher.signal["signal_id"])
        if registration_provenance_out is not None:
            registration_provenance_out["created_by_this_call"] = True
            registration_provenance_out["registration_token"] = "recut-token"
        return True


def test_fail_first_recovered_trigger_ready_rearms_before_quote_or_terminalization():
    row = _row()
    osm = _RecordingOSM(row)
    watcher = _RecordingWatcher()
    quote_calls: list[tuple] = []

    def _quote_check(*args):
        quote_calls.append(args)
        return True

    recovery = PendingTriggerRestartRecovery(
        client_id=CLIENT_ID,
        execution_mode=EXECUTION_MODE,
        osm=osm,
        entry_watcher=watcher,
        broker=object(),
        quote_check_fn=_quote_check,
    )

    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.WATCHER_OWNED
    assert quote_calls == []
    assert osm.cancel_calls == []
    assert row["status"] == "PENDING_TRIGGER"
    assert len(watcher.watch_calls) == 1
    assert watcher.watch_calls[0][2] is True


def test_real_watcher_reconstructs_exact_authority_and_dispatches_once():
    row = _row()
    expected_timestamp = row["meta"]["trigger_crossed_at"]
    expected_provenance = copy.deepcopy(
        row["meta"]["trigger_crossed_at_provenance"]
    )
    recovery, watcher, osm, broker, callback, quote_calls = _recovered_watcher(row)

    with _fixed_watcher_time():
        outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.WATCHER_OWNED
    assert quote_calls == [], "durable trigger-ready recovery must not quote-gate"
    assert len(osm.authority_reads) == 1
    assert osm.meta_writes == []
    assert len(watcher._pending) == 1
    watched = watcher._pending[0]
    assert watched.signal.get("__recovered_trigger_ready") is True
    assert watched.signal.get("__durable_trigger_authority_proven") is None
    assert watched._trigger_authority_persisted is True
    assert watched._durable_trigger_crossed_at_raw == expected_timestamp
    assert watched._durable_trigger_crossed_at_provenance == expected_provenance
    assert ew._base._EW_LEDGER.current_state(row["signal_id"]) == ew._base._EW_SS.WATCHING

    with _fixed_watcher_time():
        watcher._poll_active_signals(open_protect_active=False)
        callback.assert_called_once_with(watched)
        watcher._poll_active_signals(open_protect_active=False)

    callback.assert_called_once_with(watched)
    assert watcher._pending == []
    assert watched.signal_id not in watcher._dedup_set
    history = ew._base._EW_LEDGER.history(row["signal_id"])
    assert [(entry.from_state, entry.to_state) for entry in history] == [
        (None, "ADOPTED"),
        ("ADOPTED", "WATCHING"),
        ("WATCHING", "TRIGGER_READY"),
    ]
    assert all(entry.to_state != "ERROR" for entry in history)
    assert osm.cancel_calls == []
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    assert row["status"] == "PENDING_TRIGGER"
    assert row["meta"]["trigger_crossed_at"] == expected_timestamp
    assert row["meta"]["trigger_crossed_at_provenance"] == expected_provenance
    authority_writes = [
        (patch, kwargs)
        for _, patch, kwargs in osm.meta_write_calls
        if kwargs.get("expected_existing_trigger_authority")
    ]
    assert len(authority_writes) == 1
    assert authority_writes[0][0] == {
        "trigger_crossed_at": expected_timestamp,
        "trigger_crossed_at_provenance": expected_provenance,
    }
    assert any(
        patch.get("trigger_crossed_at") == expected_timestamp
        and patch.get("trigger_crossed_at_provenance") == expected_provenance
        for _, patch in osm.meta_writes
    )


def test_recovered_trigger_dispatches_when_quote_transport_raises():
    row = _row()
    expected_timestamp = row["meta"]["trigger_crossed_at"]
    expected_provenance = copy.deepcopy(
        row["meta"]["trigger_crossed_at_provenance"]
    )
    recovery, watcher, osm, broker, callback, quote_calls = _recovered_watcher(row)
    watcher._fetch_quotes = MagicMock(
        side_effect=RuntimeError("quote transport unavailable")
    )

    with _fixed_watcher_time():
        assert recovery.recover_one_row(row) == _RowOutcome.WATCHER_OWNED
        watched = watcher._pending[0]
        watcher._poll_active_signals(open_protect_active=False)
        callback.assert_called_once_with(watched)
        watcher._poll_active_signals(open_protect_active=False)
        callback.assert_called_once_with(watched)

    watcher._fetch_quotes.assert_not_called()
    assert quote_calls == []
    assert osm.cancel_calls == []
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()
    assert row["meta"]["trigger_crossed_at"] == expected_timestamp
    assert row["meta"]["trigger_crossed_at_provenance"] == expected_provenance
    assert row["meta"].get("trigger_confirmed_at") is None
    authority_writes = [
        (patch, kwargs)
        for _, patch, kwargs in osm.meta_write_calls
        if kwargs.get("expected_existing_trigger_authority")
    ]
    assert len(authority_writes) == 1


def test_preexisting_trigger_ready_lifecycle_is_accepted_and_dispatches_once():
    row = _row()
    recovery, watcher, osm, broker, callback, quote_calls = _recovered_watcher(row)
    ledger = ew._base._EW_LEDGER
    for state, reason in (
        (ew._base._EW_SS.ADOPTED, "seed_recovered_trigger_ready_adopted"),
        (ew._base._EW_SS.WATCHING, "seed_recovered_trigger_ready_watching"),
        (ew._base._EW_SS.TRIGGER_READY, "seed_recovered_trigger_ready"),
    ):
        ledger.transition(
            row["signal_id"],
            row["ticker"],
            state,
            ew._base._EW_LO.WATCHER,
            reason,
            strict=True,
        )

    with _fixed_watcher_time():
        outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.WATCHER_OWNED
    assert quote_calls == []
    watcher._fetch_quotes.assert_not_called()
    assert len(watcher._pending) == 1
    watched = watcher._pending[0]
    assert watched.signal.get("__recovered_trigger_ready") is True
    assert ledger.current_state(row["signal_id"]) == ew._base._EW_SS.TRIGGER_READY
    assert osm.cancel_calls == []
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()

    with _fixed_watcher_time():
        watcher._poll_active_signals(open_protect_active=False)
        callback.assert_called_once_with(watched)
        watcher._poll_active_signals(open_protect_active=False)

    callback.assert_called_once_with(watched)
    assert watcher._pending == []
    assert watched.signal_id not in watcher._dedup_set
    assert all(entry.to_state != "ERROR" for entry in ledger.history(row["signal_id"]))
    assert osm.cancel_calls == []
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()


@pytest.mark.parametrize(
    "provenance_field",
    ["client_id", "execution_mode", "signal_id", "canonical_signal_id", "local_order_id"],
)
def test_wrong_trigger_authority_provenance_is_unresolved_without_cleanup(
    provenance_field,
):
    row = _row()
    row["meta"]["trigger_crossed_at_provenance"][provenance_field] = (
        "wrong-value"
    )
    recovery, watcher, osm, _broker, _callback, quote_calls = _recovered_watcher(row)

    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.UNRESOLVED
    assert watcher._pending == []
    assert quote_calls == []
    assert osm.cancel_calls == []
    assert row["status"] == "PENDING_TRIGGER"


@pytest.mark.parametrize(
    "row_field, value",
    [("client_id", "other-client@example.com"), ("execution_mode", "live")],
)
def test_wrong_durable_recovery_scope_is_unresolved_without_cleanup(row_field, value):
    row = _row()
    row[row_field] = value
    recovery, watcher, osm, _broker, _callback, quote_calls = _recovered_watcher(row)

    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.UNRESOLVED
    assert watcher._pending == []
    assert quote_calls == []
    assert osm.cancel_calls == []
    assert row["status"] == "PENDING_TRIGGER"


@pytest.mark.parametrize(
    "row_field, value, expected_outcome",
    [
        ("broker_order_id", "broker-accepted", _RowOutcome.SKIPPED),
        ("submitted_ts", "2026-09-11T16:01:00+00:00", _RowOutcome.SKIPPED),
    ],
)
def test_broker_handoff_evidence_is_a_reconciliation_hold(
    row_field, value, expected_outcome
):
    row = _row()
    row[row_field] = value
    recovery, watcher, osm, broker, callback, quote_calls = _recovered_watcher(row)

    outcome = recovery.recover_one_row(row)

    assert outcome == expected_outcome
    assert watcher._pending == []
    assert callback.call_count == 0
    assert len(quote_calls) == 1
    assert osm.cancel_calls == []
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()


@pytest.mark.parametrize(
    "meta_field, value",
    [
        ("submit_intent_at", "2026-09-11T16:01:00+00:00"),
        ("broker_submit_key", "submit-key"),
        ("broker_submit_payload_hash", "payload-hash"),
        ("broker_ready", True),
    ],
)
def test_broker_handoff_metadata_is_a_reconciliation_hold(meta_field, value):
    row = _row()
    row["meta"][meta_field] = value
    recovery, watcher, osm, broker, callback, quote_calls = _recovered_watcher(row)

    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.UNRESOLVED
    assert watcher._pending == []
    assert callback.call_count == 0
    assert quote_calls == []
    assert osm.cancel_calls == []
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()


@pytest.mark.parametrize(
    "provenance",
    [
        None,
        {"client_id": CLIENT_ID},
        "not-a-provenance-object",
    ],
)
def test_missing_or_malformed_trigger_provenance_is_hold_not_cancel(provenance):
    row = _row()
    row["meta"]["trigger_crossed_at_provenance"] = provenance
    recovery, watcher, osm, broker, callback, quote_calls = _recovered_watcher(row)

    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.UNRESOLVED
    assert watcher._pending == []
    assert callback.call_count == 0
    assert quote_calls == []
    assert osm.cancel_calls == []
    assert row["status"] == "PENDING_TRIGGER"
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()


def test_ambiguous_trigger_ready_without_timestamp_is_held_not_canceled():
    row = _row()
    row["meta"].pop("trigger_crossed_at", None)
    row["meta"].pop("trigger_crossed_at_provenance", None)
    recovery, watcher, osm, broker, callback, _quote_calls = _recovered_watcher(row)

    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.UNRESOLVED
    assert recovery._row_failure_reasons[row["local_order_id"]] == (
        "restart_trigger_ready_authority_unproven"
    )
    assert row["status"] == "PENDING_TRIGGER"
    assert watcher._pending == []
    assert osm.cancel_calls == []
    assert osm.meta_writes == []
    callback.assert_not_called()
    broker.submit_order.assert_not_called()
    broker.cancel_order.assert_not_called()


def test_active_materializer_remains_the_existing_owner():
    row = _row()
    row["contract"] = "DEFERRED:SPY"
    row["meta"].update(_active_materialization_meta(row))
    recovery, watcher, osm, _broker, callback, quote_calls = _recovered_watcher(row)

    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.MATERIALIZATION_OWNED
    assert watcher._pending == []
    assert callback.call_count == 0
    assert quote_calls == []
    assert osm.cancel_calls == []
    assert osm.meta_writes == []


def test_canonical_retry_owner_remains_the_existing_owner():
    row = _row()
    row["contract"] = "DEFERRED:SPY"
    row["meta"].update(_canonical_retry_meta(row))
    recovery, watcher, osm, _broker, callback, quote_calls = _recovered_watcher(row)

    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.RETRY_OWNED
    assert watcher._pending == []
    assert callback.call_count == 0
    assert quote_calls == []
    assert osm.cancel_calls == []
    assert any(
        patch.get("restart_recovery_retry_subtype") == "MATERIALIZATION_RETRY"
        for _, patch in osm.meta_writes
    )


def test_terminal_durable_row_is_not_recovered_as_trigger_ready():
    row = _row()
    row["status"] = "CANCELED"
    row["last_error"] = "existing_terminal_reason"
    recovery, watcher, osm, _broker, callback, _quote_calls = _recovered_watcher(row)

    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.SKIPPED
    assert watcher._pending == []
    assert callback.call_count == 0
    assert osm.cancel_calls == []


def test_recovered_marker_requires_restart_context_and_exact_authority():
    row = _row()
    _recovery, watcher, osm, _broker, _callback, _quote_calls = _recovered_watcher(row)
    plan = type(
        "Plan",
        (),
        {
            "signal_id": row["signal_id"],
            "canonical_signal_id": row["canonical_signal_id"],
            "ticker": row["ticker"],
            "side": row["direction"],
            "trigger_price": row["entry_price"],
            "stop_underlying": row["stop_price"],
            "target_underlying": row["target_price"],
            "client_id": row["client_id"],
            "execution_mode": row["execution_mode"],
            "metadata": row["meta"],
            "_recovered_trigger_ready": True,
        },
    )()

    assert watcher.watch(plan, row["local_order_id"]) is False
    assert watcher._pending == []
    assert osm.cancel_calls == []
    assert watcher._last_reject_reason == "invalid_recovered_trigger_ready_context"

    # The marker cannot bypass the existing exact durable readback, even in
    # the otherwise valid recovery_rearm shape.
    row["meta"].pop("trigger_crossed_at", None)
    assert watcher.watch(plan, row["local_order_id"], recovery_rearm=True) is False
    assert watcher._pending == []
    assert osm.cancel_calls == []


def test_package_recovered_conflict_holds_without_touching_incumbent():
    row = _row()
    osm = _RecordingOSM(row)
    watcher = _TestWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
    incumbent = ew.WatchedSignal(_watcher_signal(row), overnight=False)
    incumbent._watcher_ref = watcher
    watcher._pending.append(incumbent)
    watcher._dedup_set.add(incumbent.signal_id)
    incumbent_state = incumbent.state
    incoming = _watcher_signal(
        row,
        local_order_id="different-local-order",
        signal_id="different-signal",
        canonical_signal_id="different-canonical",
        side="PUT",
        __recovered_trigger_ready=True,
        __durable_trigger_authority_proven=True,
    )

    accepted = watcher.add_signal(incoming)

    assert accepted is False
    assert watcher._last_reject_reason == "recovered_trigger_ready_conflict_hold"
    assert watcher._pending == [incumbent]
    assert incumbent.state == incumbent_state
    assert watcher._dedup_set == {incumbent.signal_id}
    assert osm.cancel_calls == []
    assert osm.meta_writes == []


def test_package_recovered_quarantined_conflict_holds_without_touching_incumbent():
    row = _row()
    osm = _RecordingOSM(row)
    watcher = _TestWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
    incumbent = ew.WatchedSignal(
        _watcher_signal(
            row,
            local_order_id="quarantined-local-order",
            signal_id="quarantined-signal",
            canonical_signal_id="quarantined-canonical",
        ),
        overnight=False,
    )
    incumbent._watcher_ref = watcher
    incumbent._ownership_quarantine = True
    assert incumbent.is_active is False
    incumbent_state = incumbent.state
    watcher._pending.append(incumbent)
    watcher._dedup_set.add(incumbent.signal_id)
    incoming = _watcher_signal(
        row,
        local_order_id="different-local-order",
        signal_id="different-signal",
        canonical_signal_id="different-canonical",
        side="PUT",
        __recovered_trigger_ready=True,
        __durable_trigger_authority_proven=True,
    )

    accepted = watcher.add_signal(incoming)

    assert accepted is False
    assert watcher._last_reject_reason == "recovered_trigger_ready_conflict_hold"
    assert watcher._pending == [incumbent]
    assert incumbent.state == incumbent_state
    assert incumbent._ownership_quarantine is True
    assert watcher._dedup_set == {incumbent.signal_id}
    assert osm.cancel_calls == []
    assert osm.meta_writes == []


def test_package_recovered_exact_existing_owner_keeps_idempotent_dedup_behavior():
    row = _row()
    osm = _RecordingOSM(row)
    watcher = _TestWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
    incumbent = ew.WatchedSignal(_watcher_signal(row), overnight=False)
    incumbent._watcher_ref = watcher
    watcher._pending.append(incumbent)
    watcher._dedup_set.add(incumbent.signal_id)
    incoming = _watcher_signal(
        row,
        __recovered_trigger_ready=True,
        __durable_trigger_authority_proven=True,
    )

    accepted = watcher.add_signal(incoming)

    assert accepted is False
    assert watcher._last_reject_reason == "dedup_block"
    assert watcher._pending == [incumbent]
    assert watcher._dedup_set == {incumbent.signal_id}
    assert osm.cancel_calls == []


def test_lifecycle_restore_failure_rolls_back_only_new_registration():
    row = _row()
    osm = _RecordingOSM(row)
    watcher = _TestWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
    signal = _watcher_signal(
        row,
        trigger_crossed_at=row["meta"]["trigger_crossed_at"],
        metadata={
            "trigger_crossed_at": row["meta"]["trigger_crossed_at"],
            "trigger_crossed_at_provenance": copy.deepcopy(
                row["meta"]["trigger_crossed_at_provenance"]
            ),
        },
        __recovered_trigger_ready=True,
        __durable_trigger_authority_proven=True,
    )

    lifecycle_globals = (
        watcher._restore_recovered_trigger_ready_lifecycle.__func__.__globals__
    )
    with patch.dict(lifecycle_globals, {"_EW_LIFECYCLE_OK": False}):
        accepted = watcher.add_signal(signal)

    assert accepted is False
    assert watcher._last_reject_reason == (
        "recovered_trigger_ready_lifecycle_restore_failed"
    )
    assert watcher._pending == []
    assert watcher._dedup_set == set()
    assert osm.cancel_calls == []
    assert osm.meta_writes == []


def test_ordinary_prebreach_restart_rearm_remains_on_existing_path():
    row = _row()
    row["meta"] = {"trigger_price": row["entry_price"]}
    osm = _RecordingOSM(row)
    watcher = _RecordingWatcher()
    quote_calls: list[tuple] = []

    def _quote_check(*args):
        quote_calls.append(args)
        return False

    recovery = PendingTriggerRestartRecovery(
        client_id=CLIENT_ID,
        execution_mode=EXECUTION_MODE,
        osm=osm,
        entry_watcher=watcher,
        broker=object(),
        quote_check_fn=_quote_check,
    )
    outcome = recovery.recover_one_row(row)

    assert outcome == _RowOutcome.WATCHER_OWNED
    assert len(quote_calls) == 1
    assert len(watcher.watch_calls) == 1
    assert watcher.watch_calls[0][2] is True
    assert watcher.watch_calls[0][0].get("_recovered_trigger_ready") is None
    assert osm.cancel_calls == []


def test_ordinary_watcher_admission_does_not_use_recovered_lifecycle_path():
    row = _row()
    row["meta"] = {"trigger_price": row["entry_price"]}
    osm = _RecordingOSM(row)
    watcher = _TestWatcher(MagicMock(), order_state_machine=osm, mode="PAPER")
    watcher._get_quote = MagicMock(return_value={"bid": 0, "ask": 0})
    plan = SimpleNamespace(
        signal_id=row["signal_id"],
        canonical_signal_id=row["canonical_signal_id"],
        ticker=row["ticker"],
        side=row["direction"],
        trigger_price=row["entry_price"],
        stop_underlying=row["stop_price"],
        target_underlying=row["target_price"],
        client_id=row["client_id"],
        execution_mode=row["execution_mode"],
        metadata=row["meta"],
    )

    with _fixed_watcher_time():
        accepted = watcher.watch(plan, row["local_order_id"])

    assert accepted is True
    assert len(watcher._pending) == 1
    assert watcher._pending[0].signal.get("__recovered_trigger_ready") is None
    assert watcher._last_reject_reason == ""


def test_recovered_marker_does_not_widen_public_watcher_api():
    assert "recovered_trigger_ready" not in inspect.signature(
        ew.APEntryWatcher.watch
    ).parameters
    assert "recovered_trigger_ready" not in inspect.signature(
        ew.APEntryWatcher._watch_impl
    ).parameters
    assert "recovered_trigger_ready" not in inspect.signature(
        ew.APEntryWatcher.watch_with_result
    ).parameters


def test_recovered_callback_retry_wait_honors_deferred_schedule():
    row = _row()
    recovery, watcher, osm, _broker, callback, quote_calls = _recovered_watcher(row)
    callback.return_value = {
        "disposition": "RETRY_WAIT",
        "retry_after_seconds": 60,
    }

    with _fixed_watcher_time():
        assert recovery.recover_one_row(row) == _RowOutcome.WATCHER_OWNED
        watched = watcher._pending[0]
        watcher._poll_active_signals(open_protect_active=False)
        watcher._poll_active_signals(open_protect_active=False)

    callback.assert_called_once_with(watched)
    assert quote_calls == []
    assert watcher._pending == [watched]
    assert watcher._dedup_set == {watched.signal_id}
    assert watched.state == ew.WatchState.PENDING
    assert watched.deferred_retry_not_before is not None
    assert osm.cancel_calls == []
