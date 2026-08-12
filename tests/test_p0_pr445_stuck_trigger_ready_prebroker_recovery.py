from __future__ import annotations

import os
from unittest.mock import MagicMock

os.environ.setdefault("DATABASE_URL", "postgresql://user:pass@localhost/db")

from ap.broker_submit_identity import canonical_broker_submit_key
from ap.pending_trigger_classifier import (
    PendingTriggerClassification,
    classify_pending_trigger_row,
)
from ap.pending_trigger_restart_recovery import (
    PendingTriggerRestartRecovery,
    _RowOutcome,
)


LOCAL_ORDER_ID = "lo-pr445"
SIGNAL_ID = "sig-pr445"
CLIENT_ID = "client@test.com"
MODE = "paper"
TRIGGERED_AT = "2026-08-12T16:00:00+00:00"
CANONICAL_SIGNAL_ID = "canonical-pr445"


def _row(**meta_overrides):
    meta = {
        "watcher_audit": {"reason_code": "trigger_ready"},
        "trigger_crossed_at": TRIGGERED_AT,
        "trigger_crossed_at_provenance": {
            "canonical_signal_id": CANONICAL_SIGNAL_ID,
            "client_id": CLIENT_ID,
            "execution_mode": MODE,
            "local_order_id": LOCAL_ORDER_ID,
        },
        "canonical_signal_id": CANONICAL_SIGNAL_ID,
        "trigger_price": 100.0,
        "observed_underlying_price": 101.0,
        "contract_deferred": True,
        "score": 85,
        "tier": "A",
        "timeframe": "5m",
        **meta_overrides,
    }
    return {
        "local_order_id": LOCAL_ORDER_ID,
        "signal_id": SIGNAL_ID,
        "canonical_signal_id": CANONICAL_SIGNAL_ID,
        "client_id": CLIENT_ID,
        "execution_mode": MODE,
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "direction": "CALL",
        "ticker": "SPY",
        "contract": "DEFERRED:SPY",
        "plan_id": "plan-pr445",
        "qty": 1,
        "entry_price": 100.0,
        "stop_price": 95.0,
        "target_price": 110.0,
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": meta,
    }


class _OSM:
    def __init__(self, row):
        self.rows = {row["local_order_id"]: row}
        self.claim_calls = []
        self.cancel_calls = []
        self.retry_calls = []

    def get_order(self, local_order_id):
        row = self.rows.get(local_order_id)
        if row is None:
            return None
        return {**row, "meta": dict(row.get("meta") or {})}

    def claim_deferred_materialization(self, local_order_id, **kwargs):
        self.claim_calls.append((local_order_id, kwargs))
        row = self.rows[local_order_id]
        if row.get("status") != "PENDING_TRIGGER":
            return False
        meta = row.setdefault("meta", {})
        if meta.get("lifecycle_state") not in (None, ""):
            return False
        meta.update(
            {
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_in_flight": True,
                "materialization_owner": kwargs["owner"],
                "materialization_generation": kwargs["new_generation"],
                "retry_attempt": kwargs["retry_attempt"],
                "broker_ready": False,
            }
        )
        return True

    def schedule_deferred_materialization_retry(self, local_order_id, **kwargs):
        self.retry_calls.append((local_order_id, kwargs))
        row = self.rows[local_order_id]
        row["meta"].update(
            {
                "lifecycle_state": "RETRY_WAIT",
                "materialization_status": "RETRY_PENDING",
                "materialization_next_retry_at": kwargs["next_retry_at"],
            }
        )
        return True

    def update_order_meta(self, local_order_id, patch):
        self.rows[local_order_id].setdefault("meta", {}).update(patch)
        return True

    def transition(self, local_order_id, status, **kwargs):
        row = self.rows[local_order_id]
        row["status"] = status
        if kwargs.get("broker_order_id"):
            row["broker_order_id"] = kwargs["broker_order_id"]
        if kwargs.get("submitted_ts"):
            row["submitted_ts"] = kwargs["submitted_ts"]
        if kwargs.get("filled_qty") is not None:
            row["filled_qty"] = kwargs["filled_qty"]
        if kwargs.get("fill_price") is not None:
            row["fill_price"] = kwargs["fill_price"]
        return True

    def cancel_pending_entry(self, local_order_id, *, reason=""):
        self.cancel_calls.append((local_order_id, reason))
        self.rows[local_order_id]["status"] = "CANCELED"
        return True


class _Watcher:
    def __init__(self, callback):
        self._pending = []
        self.on_trigger = callback


def _recovery(row, osm, watcher, broker, *, positions=None):
    return PendingTriggerRestartRecovery(
        client_id=CLIENT_ID,
        execution_mode=MODE,
        osm=osm,
        entry_watcher=watcher,
        broker=broker,
        quote_check_fn=lambda *args: False,
        position_check_fn=lambda _row: positions or [],
    )


def test_classifier_keeps_legacy_default_and_exposes_proof_gated_label():
    row = _row()
    assert (
        classify_pending_trigger_row(row)
        == PendingTriggerClassification.STUCK_TRIGGER_READY
    )
    assert (
        classify_pending_trigger_row(row, prebroker_recovery_proven=True)
        == PendingTriggerClassification.STUCK_TRIGGER_READY_PREBROKER_RECOVERABLE
    )


def test_exact_no_broker_claims_once_and_continues_through_callback():
    row = _row()
    osm = _OSM(row)
    callback_calls = []

    def _callback(watched):
        callback_calls.append(watched)
        osm.rows[LOCAL_ORDER_ID]["status"] = "SUBMITTED"
        osm.rows[LOCAL_ORDER_ID]["broker_order_id"] = "broker-1"
        return {"disposition": "SUBMITTED"}

    broker = MagicMock()
    broker.list_orders.return_value = []
    watcher = _Watcher(_callback)
    recovery = _recovery(row, osm, watcher, broker)

    assert recovery.recover_one_row(row) == _RowOutcome.SKIPPED
    assert len(osm.claim_calls) == 1
    assert len(callback_calls) == 1
    assert osm.cancel_calls == []

    # A second pass sees the fenced/accepted row and cannot invoke the
    # continuation a second time.
    assert recovery.recover_one_row(row) == _RowOutcome.SKIPPED
    assert len(osm.claim_calls) == 1
    assert len(callback_calls) == 1


def test_broker_truth_unavailable_holds_without_claim_or_cancel():
    row = _row()
    osm = _OSM(row)
    callback = MagicMock()
    watcher = _Watcher(callback)
    broker = MagicMock()
    broker.list_orders.side_effect = RuntimeError("broker unavailable")
    recovery = _recovery(row, osm, watcher, broker)

    assert recovery.recover_one_row(row) == _RowOutcome.UNRESOLVED
    assert osm.claim_calls == []
    assert osm.cancel_calls == []
    callback.assert_not_called()


def test_exact_broker_match_is_adopted_without_claim_or_callback():
    row = _row()
    osm = _OSM(row)
    callback = MagicMock()
    broker = MagicMock()
    broker.list_orders.return_value = [
        {
            "tag": canonical_broker_submit_key(LOCAL_ORDER_ID),
            "id": "broker-existing",
            "side": "buy_to_open",
            "quantity": "1",
            "status": "working",
        }
    ]
    recovery = _recovery(row, osm, _Watcher(callback), broker)

    assert recovery.recover_one_row(row) == _RowOutcome.SKIPPED
    assert osm.claim_calls == []
    assert osm.cancel_calls == []
    callback.assert_not_called()
    broker.place_order.assert_not_called()
    assert osm.rows[LOCAL_ORDER_ID]["status"] == "SUBMITTED"
    assert osm.rows[LOCAL_ORDER_ID]["broker_order_id"] == "broker-existing"
    assert osm.rows[LOCAL_ORDER_ID]["meta"]["recovery_classification"] == "BROKER_ORDER_ADOPTED"


def test_matching_position_holds_without_broker_lookup():
    row = _row()
    osm = _OSM(row)
    callback = MagicMock()
    watcher = _Watcher(callback)
    broker = MagicMock()
    broker.list_orders.return_value = []
    recovery = _recovery(
        row,
        osm,
        watcher,
        broker,
        positions=[
            {
                "client_id": CLIENT_ID,
                "execution_mode": MODE,
                "local_order_id": LOCAL_ORDER_ID,
                "status": "OPEN",
            }
        ],
    )

    assert recovery.recover_one_row(row) == _RowOutcome.UNRESOLVED
    assert osm.claim_calls == []
    assert osm.cancel_calls == []
    broker.list_orders.assert_not_called()
    callback.assert_not_called()


def test_missing_or_malformed_trigger_proof_never_terminalizes():
    row = _row()
    row["meta"].pop("trigger_crossed_at_provenance")
    osm = _OSM(row)
    callback = MagicMock()
    recovery = _recovery(row, osm, _Watcher(callback), MagicMock())

    assert recovery.recover_one_row(row) == _RowOutcome.UNRESOLVED
    assert osm.claim_calls == []
    assert osm.cancel_calls == []
    callback.assert_not_called()
