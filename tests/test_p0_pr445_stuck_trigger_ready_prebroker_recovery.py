from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

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
from ap_execution_core import APExecutionCore


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
        self.transition_calls = []
        self.meta_calls = []
        self.rearm_calls = []
        self.adopt_calls = []

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
        if meta.get("lifecycle_state") not in (None, "", "RETRY_WAIT"):
            return False
        meta.update(
            {
                "lifecycle_state": "MATERIALIZING",
                "materialization_status": "RUNNING",
                "materialization_in_flight": True,
                "materialization_owner": kwargs["owner"],
                "materialization_generation": kwargs["new_generation"],
                "materialization_lease_until": kwargs["lease_until"],
                "retry_attempt": kwargs["retry_attempt"],
                "breach_attempt_count": kwargs["retry_attempt"],
                "materialization_attempts": kwargs["retry_attempt"],
                "signal_id": row.get("signal_id"),
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
                "materialization_in_flight": False,
                "materialization_owner": "",
                "materialization_lease_until": "",
                "materialization_generation": kwargs["generation"],
                "retry_attempt": kwargs["attempt"],
                "breach_attempt_count": kwargs["attempt"],
                "materialization_attempts": kwargs["attempt"],
                "materialization_next_retry_at": kwargs["next_retry_at"],
                "materialization_reason": kwargs["reason_code"],
                "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
                "materialization_detail": kwargs["reason_code"],
                "entry_path": "DEFERRED_BREACH_MATERIALIZATION",
                "broker_ready": False,
            }
        )
        return True

    def update_order_meta(self, local_order_id, patch):
        self.meta_calls.append((local_order_id, dict(patch)))
        self.rows[local_order_id].setdefault("meta", {}).update(patch)
        return True

    def rearm_deferred_materialization_direction_reversal(self, local_order_id, **kwargs):
        self.rearm_calls.append((local_order_id, kwargs))
        watcher_token = str(kwargs.get("watcher_token") or "").strip()
        recovery_owner = str(kwargs.get("owner") or "").strip()
        generation = kwargs.get("generation")
        self.rows[local_order_id]["meta"].update(
            {
                "lifecycle_state": "",
                "materialization_status": "",
                "materialization_in_flight": False,
                "materialization_generation": generation,
                "current_owner": watcher_token,
                "watcher_token": watcher_token,
                "watcher_generation": generation if watcher_token else 0,
                "recovery_ownership": "" if watcher_token else "recovery_scheduler",
                "recovery_owner": "" if watcher_token else recovery_owner,
                "direction_reversal_rearm_requires_watcher": not bool(
                    watcher_token
                ),
                "broker_ready": False,
            }
        )
        for key in (
            "trigger_crossed_at",
            "trigger_crossed_at_provenance",
            "trigger_confirmed_at",
        ):
            self.rows[local_order_id]["meta"].pop(key, None)
        return True

    def adopt_direction_reversal_watcher_ownership(self, local_order_id, **kwargs):
        self.adopt_calls.append((local_order_id, kwargs))
        row = self.rows[local_order_id]
        meta = row.setdefault("meta", {})
        token = str(kwargs.get("watcher_token") or "").strip()
        generation = kwargs.get("generation")
        if (
            row.get("status") != "PENDING_TRIGGER"
            or not token
            or meta.get("direction_reversal_rearm_requires_watcher") is not True
            or meta.get("materialization_generation") != generation
        ):
            return False
        meta.update(
            {
                "lifecycle_state": "",
                "materialization_status": "WAITING_FOR_TRIGGER",
                "current_owner": token,
                "watcher_token": token,
                "watcher_generation": generation,
                "recovery_ownership": "",
                "recovery_owner": "",
                "direction_reversal_rearm_requires_watcher": False,
            }
        )
        return True

    def transition(self, local_order_id, status, **kwargs):
        self.transition_calls.append((local_order_id, status, dict(kwargs)))
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
        if kwargs.get("filled_ts") is not None:
            row["filled_ts"] = kwargs["filled_ts"]
        if kwargs.get("contract") is not None:
            row["contract"] = kwargs["contract"]
        if kwargs.get("meta_patch") is not None:
            row.setdefault("meta", {}).update(kwargs["meta_patch"])
        return True

    def cancel_pending_entry(self, local_order_id, *, reason=""):
        self.cancel_calls.append((local_order_id, reason))
        self.rows[local_order_id]["status"] = "CANCELED"
        return True


class _Watcher:
    def __init__(self, callback):
        self._pending = []
        self.on_trigger = callback


class _RecoveryWatcher(_Watcher):
    """Minimal watcher that proves a real registration to PTR."""

    owner_token = "watcher-owner-pr445"

    def __init__(self, callback):
        super().__init__(callback)
        self._dedup_set = set()
        self.watch_calls = []

    def watch(
        self,
        plan,
        local_order_id,
        *,
        recovery_rearm=False,
        registration_provenance_out=None,
        **_kwargs,
    ):
        self.watch_calls.append((plan, local_order_id, recovery_rearm))
        signal_id = str(getattr(plan, "signal_id", "") or SIGNAL_ID)
        self._pending.append(
            SimpleNamespace(
                signal={
                    "local_order_id": local_order_id,
                    "signal_id": signal_id,
                    "client_id": CLIENT_ID,
                    "execution_mode": MODE,
                },
                state="PENDING",
                _ownership_quarantine=False,
            )
        )
        self._dedup_set.add(signal_id)
        if registration_provenance_out is not None:
            registration_provenance_out.update(
                {
                    "created_by_this_call": True,
                    "registration_token": "registration-pr445",
                }
            )
        return True


def _recovery(row, osm, watcher, broker, *, positions=None, execution_core=None):
    return PendingTriggerRestartRecovery(
        client_id=CLIENT_ID,
        execution_mode=MODE,
        osm=osm,
        entry_watcher=watcher,
        broker=broker,
        quote_check_fn=lambda *args: False,
        position_check_fn=lambda _row: positions or [],
        execution_core=execution_core,
    )


def _broker_order(**overrides):
    order = {
        "tag": canonical_broker_submit_key(LOCAL_ORDER_ID),
        "id": "broker-existing",
        "side": "buy_to_open",
        "quantity": "1",
        "status": "working",
        "option_symbol": "SPY260821C00500000",
    }
    order.update(overrides)
    return order


def _real_recovery_callback(monkeypatch, *, side="CALL", bid=101.0, ask=102.0):
    """Bind the production callback to a minimal, durable recovery harness."""
    monkeypatch.setenv("SELECTOR_DURABLE_RECOVERY_CURSOR_ENABLED", "0")
    row = _row()
    row["execution_mode"] = "live"
    row["meta"]["trigger_crossed_at_provenance"]["execution_mode"] = "live"
    owner = "prebroker_recovery:client@test.com:live:lo-pr445"
    row["direction"] = side
    row["meta"].update(
        {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_owner": owner,
            "materialization_generation": 1,
            "retry_attempt": 1,
        }
    )
    osm = _OSM(row)
    osm.submit_existing_entry = MagicMock()
    broker = MagicMock()
    store = SimpleNamespace(
        update_status=MagicMock(),
        update_signal_fields=MagicMock(),
    )
    selector = MagicMock()
    selector.select.return_value = None
    selector.data_broker = SimpleNamespace(
        base_url="https://api.tradier.com",
        get_quote=MagicMock(
            return_value={
                "bid": bid,
                "ask": ask,
                "provider_timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ),
    )
    plan = SimpleNamespace(
        plan_id="plan-pr445",
        signal_id=SIGNAL_ID,
        client_id=CLIENT_ID,
        execution_mode="live",
        ticker="SPY",
        side=side,
        direction=side,
        score=85,
        tier="A",
        timeframe="5m",
        trigger_price=100.0,
        stop_underlying=95.0,
        target_underlying=110.0,
        contract_symbol="DEFERRED:SPY",
        limit_price=0.01,
        contracts=1,
        max_position_usd=1.0,
        metadata={"contract_deferred": True},
    )
    signal = {
        "signal_id": SIGNAL_ID,
        "local_order_id": LOCAL_ORDER_ID,
        "client_id": CLIENT_ID,
        "execution_mode": "live",
        "ticker": "SPY",
        "side": side,
        "entry_price": 100.0,
        "stop_price": 95.0,
        "target_price": 110.0,
        "contract_symbol": "DEFERRED:SPY",
        "contract_deferred": True,
        "_approved_plan": plan,
        "ownership_kind": "materialization_retry",
        "owner": owner,
        "materialization_generation": 1,
        "retry_attempt": 1,
        "fenced": True,
        "recovery_submit_fenced": True,
        "recovery_submit_owner": owner,
        "recovery_submit_generation": 1,
        "_recovery_pre_claimed": True,
        "_recovery_pre_claimed_owner": owner,
        "_recovery_pre_claimed_generation": 1,
        "_recovery_pre_claimed_attempt": 1,
        "_recovery_pre_claimed_client_id": CLIENT_ID,
        "_recovery_pre_claimed_mode": "live",
    }
    watched = SimpleNamespace(
        signal=signal,
        ticker="SPY",
        side=side,
        trigger_price=100.0,
        entry_trigger=100.0,
        stop_level=95.0,
        target_price=110.0,
        trigger_crossed_at=datetime.now(timezone.utc),
        triggered_at=datetime.now(timezone.utc),
        breach_price=101.0,
    )
    cleanup_calls = []

    def _cleanup(_watched, *, action, reason):
        cleanup_calls.append((action, reason))
        return True

    core = SimpleNamespace(
        client_id=CLIENT_ID,
        email=CLIENT_ID,
        execution_mode="live",
        mode="LIVE",
        paper=False,
        _max_positions=7,
        master_control=None,
        order_state_machine=osm,
        contract_selector=selector,
        broker=broker,
        store=store,
        _breach_risk_check=lambda _watched: True,
        _recover_plan_for_revalidation=lambda _watched: plan,
        _refresh_hydrated_prebreach_plan=lambda **_kwargs: False,
        _cleanup_pending_entry_order=_cleanup,
        _emit_breach_diag=MagicMock(),
        _is_real_occ_contract=APExecutionCore._is_real_occ_contract,
    )
    result = APExecutionCore._on_entry_trigger(core, watched)
    return result, selector, broker, osm, cleanup_calls


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

    row["meta"].update(
        {
            "lifecycle_state": "MATERIALIZING",
            "materialization_status": "RUNNING",
            "materialization_in_flight": True,
            "materialization_owner": (
                f"prebroker_recovery:{CLIENT_ID}:{MODE}:{LOCAL_ORDER_ID}"
            ),
        }
    )
    assert (
        classify_pending_trigger_row(row)
        == PendingTriggerClassification.WAITING_RETRYABLE
    )


def _claim_row(osm, *, lease_until: str):
    owner = f"prebroker_recovery:{CLIENT_ID}:{MODE}:{LOCAL_ORDER_ID}"
    assert osm.claim_deferred_materialization(
        LOCAL_ORDER_ID,
        owner=owner,
        new_generation=1,
        lease_until=lease_until,
        trigger_crossed_at=TRIGGERED_AT,
        trigger_price=100.0,
        observed_underlying_price=101.0,
        signal_id=SIGNAL_ID,
        execution_mode=MODE,
        retry_attempt=1,
    )
    return owner


def test_restart_after_claim_with_active_lease_leaves_worker_alone():
    row = _row()
    osm = _OSM(row)
    owner = _claim_row(osm, lease_until="2099-01-01T00:00:00+00:00")
    callback = MagicMock()
    broker = MagicMock()
    recovery = _recovery(osm.get_order(LOCAL_ORDER_ID), osm, _Watcher(callback), broker)

    assert recovery.recover_one_row(osm.get_order(LOCAL_ORDER_ID)) == _RowOutcome.RETRY_OWNED
    assert len(osm.claim_calls) == 1
    assert osm.retry_calls == []
    assert osm.cancel_calls == []
    assert osm.transition_calls == []
    callback.assert_not_called()
    broker.list_orders.assert_not_called()
    assert osm.rows[LOCAL_ORDER_ID]["meta"]["materialization_owner"] == owner
    assert osm.rows[LOCAL_ORDER_ID]["meta"]["materialization_generation"] == 1
    assert osm.rows[LOCAL_ORDER_ID]["meta"]["retry_attempt"] == 1


def test_restart_after_claim_expiry_transfers_same_lineage_to_canonical_retry():
    row = _row()
    osm = _OSM(row)
    owner = _claim_row(osm, lease_until="2020-01-01T00:00:00+00:00")
    callback = MagicMock()
    broker = MagicMock()
    broker.list_orders.return_value = []
    recovery = _recovery(osm.get_order(LOCAL_ORDER_ID), osm, _Watcher(callback), broker)

    assert recovery.recover_one_row(osm.get_order(LOCAL_ORDER_ID)) == _RowOutcome.RETRY_OWNED
    assert len(osm.claim_calls) == 1
    assert len(osm.retry_calls) == 1
    retry_kwargs = osm.retry_calls[0][1]
    assert retry_kwargs["owner"] == owner
    assert retry_kwargs["generation"] == 1
    assert retry_kwargs["attempt"] == 1
    assert osm.cancel_calls == []
    assert osm.transition_calls == []
    callback.assert_not_called()
    broker.list_orders.assert_called_once_with()
    meta = osm.rows[LOCAL_ORDER_ID]["meta"]
    assert meta["lifecycle_state"] == "RETRY_WAIT"
    assert meta["materialization_status"] == "RETRY_PENDING"
    assert meta["materialization_generation"] == 1
    assert meta["retry_attempt"] == 1
    assert meta["materialization_in_flight"] is False

    # The pending-trigger classifier keeps this canonical retry visible to
    # the existing due-retry owner instead of reverting to STUCK terminal
    # cleanup on the next restart pass.
    assert (
        classify_pending_trigger_row(osm.get_order(LOCAL_ORDER_ID))
        == PendingTriggerClassification.WAITING_RETRYABLE
    )


def test_restart_after_claim_expiry_adopts_exact_broker_order_without_retry():
    row = _row()
    osm = _OSM(row)
    _claim_row(osm, lease_until="2020-01-01T00:00:00+00:00")
    callback = MagicMock()
    broker = MagicMock()
    broker.list_orders.return_value = [_broker_order()]
    recovery = _recovery(osm.get_order(LOCAL_ORDER_ID), osm, _Watcher(callback), broker)

    assert recovery.recover_one_row(osm.get_order(LOCAL_ORDER_ID)) == _RowOutcome.SKIPPED
    assert len(osm.claim_calls) == 1
    assert osm.retry_calls == []
    assert osm.cancel_calls == []
    callback.assert_not_called()
    broker.list_orders.assert_called_once_with()
    assert osm.rows[LOCAL_ORDER_ID]["status"] == "SUBMITTED"
    assert osm.rows[LOCAL_ORDER_ID]["broker_order_id"] == "broker-existing"
    assert osm.rows[LOCAL_ORDER_ID]["contract"] == "SPY260821C00500000"


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


def test_rearm_watcher_required_is_handed_to_exact_generation_watcher_adoption():
    row = _row()
    osm = _OSM(row)
    callback_calls = []

    def _callback(watched):
        callback_calls.append(watched)
        owner = watched.signal["owner"]
        generation = watched.signal["materialization_generation"]
        assert osm.rearm_deferred_materialization_direction_reversal(
            LOCAL_ORDER_ID,
            owner=owner,
            watcher_token="",
            generation=generation,
            signal_id=SIGNAL_ID,
            execution_mode=MODE,
            market_truth_audit={"reason": "CALL_NO_LONGER_ABOVE_TRIGGER"},
        )
        return {
            "disposition": "REARM_WATCHER_REQUIRED",
            "reason_code": "REARM_DIRECTION_REVERSAL",
            "local_order_id": LOCAL_ORDER_ID,
            "expected_client_id": CLIENT_ID,
            "expected_execution_mode": MODE,
            "expected_signal_id": SIGNAL_ID,
            "expected_canonical_signal_id": CANONICAL_SIGNAL_ID,
            "expected_generation": generation,
        }

    broker = MagicMock()
    broker.list_orders.return_value = []
    watcher = _RecoveryWatcher(_callback)
    recovery = _recovery(row, osm, watcher, broker)

    assert recovery.recover_one_row(row) == _RowOutcome.WATCHER_OWNED
    assert len(callback_calls) == 1
    assert len(watcher.watch_calls) == 1
    assert len(osm.adopt_calls) == 1
    adopted = osm.rows[LOCAL_ORDER_ID]
    assert adopted["status"] == "PENDING_TRIGGER"
    assert adopted["meta"]["materialization_status"] == "WAITING_FOR_TRIGGER"
    assert adopted["meta"]["current_owner"] == watcher.owner_token
    assert adopted["meta"]["watcher_token"] == watcher.owner_token
    assert adopted["meta"]["recovery_owner"] == ""
    assert adopted["meta"]["direction_reversal_rearm_requires_watcher"] is False
    assert adopted["contract"] == "DEFERRED:SPY"
    assert adopted["broker_order_id"] is None

    # A restart/follow-up pass observes the exact watcher ownership and must
    # not invoke the synthetic callback, register a second watcher, or adopt
    # the durable row a second time.
    assert recovery.recover_one_row(osm.get_order(LOCAL_ORDER_ID)) == _RowOutcome.WATCHER_OWNED
    assert len(callback_calls) == 1
    assert len(watcher.watch_calls) == 1
    assert len(osm.adopt_calls) == 1
    broker.list_orders.assert_called_once_with()


def test_reconcile_broker_intent_is_consumed_without_manufacturing_fill_ownership():
    row = _row()
    osm = _OSM(row)
    callback_calls = []
    actual_contract = "SPY260821C00500000"

    def _callback(_watched):
        callback_calls.append(True)
        osm.rows[LOCAL_ORDER_ID]["contract"] = actual_contract
        osm.rows[LOCAL_ORDER_ID]["meta"].update(
            {
                "submit_intent_at": "2026-08-12T16:01:00+00:00",
                "broker_submit_key": canonical_broker_submit_key(LOCAL_ORDER_ID),
                "lifecycle_state": "SUBMITTING",
            }
        )
        return {
            "disposition": "RECONCILE_BROKER_INTENT",
            "reason_code": "ENTRY_BROKER_IDENTITY_UNPROVEN",
        }

    broker = MagicMock()
    broker.list_orders.side_effect = [[], [_broker_order(status="working")]]
    core = SimpleNamespace(
        client_id=CLIENT_ID,
        email=CLIENT_ID,
        execution_mode=MODE,
        mode=MODE.upper(),
        order_state_machine=osm,
        broker=broker,
    )
    core.reconcile_deferred_broker_intent = (
        APExecutionCore.reconcile_deferred_broker_intent.__get__(core, type(core))
    )
    watcher = _Watcher(_callback)
    recovery = _recovery(row, osm, watcher, broker, execution_core=core)

    assert recovery.recover_one_row(row) == _RowOutcome.SKIPPED
    assert callback_calls == [True]
    adopted = osm.rows[LOCAL_ORDER_ID]
    assert adopted["status"] == "SUBMITTED"
    assert adopted["contract"] == actual_contract
    assert adopted["broker_order_id"] == "broker-existing"
    assert adopted["meta"]["current_owner"] == "ORDER_MONITOR"
    assert adopted["meta"]["lifecycle_state"] == "SUBMITTED"
    assert len(osm.transition_calls) == 1
    assert broker.list_orders.call_count == 2
    broker.place_order.assert_not_called()

    # The durable broker id/status fence makes a restart read-only: no second
    # synthetic callback and no second broker reconciliation query.
    assert recovery.recover_one_row(osm.get_order(LOCAL_ORDER_ID)) == _RowOutcome.SKIPPED
    assert callback_calls == [True]
    assert len(osm.transition_calls) == 1
    assert broker.list_orders.call_count == 2


def test_due_retry_reconcile_disposition_reaches_existing_broker_reconciler():
    row = _row()
    row["symbol"] = "SPY"
    row["meta"].update(
        {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "materialization_generation": 3,
            "materialization_next_retry_at": "2020-01-01T00:00:00+00:00",
            "retry_attempt": 0,
            "retry_max_attempts": 5,
            "breach_attempt_count": 0,
            "materialization_attempts": 0,
        }
    )
    osm = _OSM(row)
    callback_calls = []
    actual_contract = "SPY260821C00500000"

    def _callback(_watched):
        callback_calls.append(True)
        osm.rows[LOCAL_ORDER_ID]["contract"] = actual_contract
        osm.rows[LOCAL_ORDER_ID]["meta"].update(
            {
                "submit_intent_at": "2026-08-12T16:02:00+00:00",
                "broker_submit_key": canonical_broker_submit_key(LOCAL_ORDER_ID),
                "lifecycle_state": "SUBMITTING",
            }
        )
        return {
            "disposition": "RECONCILE_BROKER_INTENT",
            "reason_code": "ENTRY_BROKER_IDENTITY_UNPROVEN",
        }

    broker = MagicMock()
    broker.list_orders.return_value = [_broker_order(status="working")]
    core = SimpleNamespace(
        client_id=CLIENT_ID,
        email=CLIENT_ID,
        execution_mode=MODE,
        mode=MODE.upper(),
        order_state_machine=osm,
        broker=broker,
    )
    core._on_entry_trigger = _callback
    core.reconcile_deferred_broker_intent = (
        APExecutionCore.reconcile_deferred_broker_intent.__get__(core, type(core))
    )

    outcome = APExecutionCore.resume_deferred_materialization_retry(
        core,
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=3,
        expected_retry_attempt=1,
        owner="recovery-retry-pr445",
    )

    assert outcome["disposition"] == "SUBMITTED"
    assert callback_calls == [True]
    assert osm.rows[LOCAL_ORDER_ID]["status"] == "SUBMITTED"
    assert osm.rows[LOCAL_ORDER_ID]["contract"] == actual_contract
    assert osm.rows[LOCAL_ORDER_ID]["broker_order_id"] == "broker-existing"
    assert len(osm.transition_calls) == 1
    broker.list_orders.assert_called_once_with()
    broker.place_order.assert_not_called()

    # The accepted durable boundary is terminal for this synthetic retry
    # invocation; a repeated due-retry call cannot call the callback again.
    second = APExecutionCore.resume_deferred_materialization_retry(
        core,
        local_order_id=LOCAL_ORDER_ID,
        expected_generation=3,
        expected_retry_attempt=1,
        owner="recovery-retry-pr445",
    )
    assert second["disposition"] == "KEEP_WATCHER"
    assert callback_calls == [True]
    assert broker.list_orders.call_count == 1


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
    broker.list_orders.return_value = [_broker_order()]
    recovery = _recovery(row, osm, _Watcher(callback), broker)

    assert recovery.recover_one_row(row) == _RowOutcome.SKIPPED
    assert osm.claim_calls == []
    assert osm.cancel_calls == []
    callback.assert_not_called()
    broker.place_order.assert_not_called()
    assert osm.rows[LOCAL_ORDER_ID]["status"] == "SUBMITTED"
    assert osm.rows[LOCAL_ORDER_ID]["broker_order_id"] == "broker-existing"
    assert osm.rows[LOCAL_ORDER_ID]["contract"] == "SPY260821C00500000"
    assert osm.rows[LOCAL_ORDER_ID]["submitted_ts"] is None
    assert osm.rows[LOCAL_ORDER_ID]["meta"]["recovery_classification"] == "BROKER_ORDER_ADOPTED"
    assert osm.rows[LOCAL_ORDER_ID]["meta"]["broker_reconcile_response"] == _broker_order()
    assert osm.meta_calls == []
    transition_kwargs = osm.transition_calls[0][2]
    assert transition_kwargs["contract"] == "SPY260821C00500000"
    assert transition_kwargs["expected_contract"] == "DEFERRED:SPY"
    assert transition_kwargs["meta_patch"]["broker_reconcile_response"] == _broker_order()


@pytest.mark.parametrize(
    ("option_symbol", "reason"),
    [
        (None, "broker_contract_missing_or_invalid"),
        ("QQQ260821C00500000", "broker_contract_underlying_mismatch"),
        ("SPY260821P00500000", "broker_contract_direction_mismatch"),
    ],
)
def test_exact_tag_without_matching_occ_contract_holds_without_mutation(option_symbol, reason):
    row = _row()
    osm = _OSM(row)
    broker = MagicMock()
    broker.list_orders.return_value = [_broker_order(option_symbol=option_symbol)]
    recovery = _recovery(row, osm, _Watcher(MagicMock()), broker)

    proof = recovery._prove_stuck_trigger_ready_prebroker(row, LOCAL_ORDER_ID)
    assert proof == {"disposition": "HOLD", "reason_code": reason}
    assert recovery.recover_one_row(row) == _RowOutcome.UNRESOLVED
    assert osm.transition_calls == []
    assert osm.meta_calls == []
    assert osm.claim_calls == []
    assert osm.cancel_calls == []
    assert osm.rows[LOCAL_ORDER_ID]["status"] == "PENDING_TRIGGER"
    assert osm.rows[LOCAL_ORDER_ID]["contract"] == "DEFERRED:SPY"
    assert osm.rows[LOCAL_ORDER_ID]["broker_order_id"] is None


def test_broker_adoption_preserves_actual_creation_time_and_repeated_pass_is_idempotent():
    row = _row()
    osm = _OSM(row)
    broker = MagicMock()
    broker.list_orders.return_value = [
        _broker_order(create_date="2026-08-12T15:50:00Z")
    ]
    recovery = _recovery(row, osm, _Watcher(MagicMock()), broker)

    assert recovery.recover_one_row(row) == _RowOutcome.SKIPPED
    assert osm.rows[LOCAL_ORDER_ID]["submitted_ts"] == "2026-08-12T15:50:00+00:00"
    assert osm.rows[LOCAL_ORDER_ID]["contract"] == "SPY260821C00500000"
    first_transition_count = len(osm.transition_calls)

    # Callers supply a fresh durable snapshot on every pass. The second pass
    # must see the broker-owned status and perform no adoption transition.
    assert recovery.recover_one_row(osm.get_order(LOCAL_ORDER_ID)) == _RowOutcome.SKIPPED
    assert len(osm.transition_calls) == first_transition_count
    assert broker.list_orders.call_count == 1
    assert osm.rows[LOCAL_ORDER_ID]["submitted_ts"] == "2026-08-12T15:50:00+00:00"


def test_filled_broker_order_without_positive_fill_price_adopts_ownership_only():
    row = _row()
    osm = _OSM(row)
    broker = MagicMock()
    broker.list_orders.return_value = [
        _broker_order(
            status="filled",
            exec_quantity="1",
            transaction_date="2026-08-12T15:51:00Z",
        )
    ]
    recovery = _recovery(row, osm, _Watcher(MagicMock()), broker)

    assert recovery.recover_one_row(row) == _RowOutcome.SKIPPED
    assert len(osm.transition_calls) == 1
    adopted = osm.rows[LOCAL_ORDER_ID]
    assert adopted["status"] == "SUBMITTED"
    assert adopted["contract"] == "SPY260821C00500000"
    assert adopted["broker_order_id"] == "broker-existing"
    assert "filled_qty" not in adopted
    assert "fill_price" not in adopted
    assert "filled_ts" not in adopted


def test_filled_broker_order_without_exact_fill_timestamp_adopts_ownership_only():
    row = _row()
    osm = _OSM(row)
    broker = MagicMock()
    broker.list_orders.return_value = [
        _broker_order(
            status="filled",
            exec_quantity="1",
            avg_fill_price="2.50",
        )
    ]
    recovery = _recovery(row, osm, _Watcher(MagicMock()), broker)

    assert recovery.recover_one_row(row) == _RowOutcome.SKIPPED
    assert len(osm.transition_calls) == 1
    adopted = osm.rows[LOCAL_ORDER_ID]
    assert adopted["status"] == "SUBMITTED"
    assert adopted["contract"] == "SPY260821C00500000"
    assert adopted["broker_order_id"] == "broker-existing"
    assert "filled_qty" not in adopted
    assert "fill_price" not in adopted
    assert "filled_ts" not in adopted


def test_filled_broker_order_stays_submitted_for_canonical_fill_monitor():
    row = _row()
    osm = _OSM(row)
    broker = MagicMock()
    broker.list_orders.return_value = [
        _broker_order(
            status="filled",
            create_date="2026-08-12T15:50:00Z",
            exec_quantity="1",
            avg_fill_price="2.50",
            transaction_date="2026-08-12T15:51:00Z",
        )
    ]
    recovery = _recovery(row, osm, _Watcher(MagicMock()), broker)

    assert recovery.recover_one_row(row) == _RowOutcome.SKIPPED
    adopted = osm.rows[LOCAL_ORDER_ID]
    assert adopted["status"] == "SUBMITTED"
    assert adopted["contract"] == "SPY260821C00500000"
    assert adopted["broker_order_id"] == "broker-existing"
    assert adopted["submitted_ts"] == "2026-08-12T15:50:00+00:00"
    assert "filled_ts" not in adopted
    assert "filled_qty" not in adopted
    assert "fill_price" not in adopted
    assert len(osm.transition_calls) == 1
    assert osm.transition_calls[0][1] == "SUBMITTED"


@pytest.mark.parametrize(
    ("side", "bid", "ask"),
    [
        ("CALL", 98.0, 99.0),    # no longer above the trigger
        ("PUT", 101.0, 102.0),   # no longer below the trigger
        ("CALL", 111.0, 112.0),  # target already completed
        ("CALL", 94.0, 95.0),    # stop invalidated
    ],
)
def test_real_entry_trigger_recovery_revalidates_market_before_selector_or_post(
    side, bid, ask, monkeypatch
):
    result, selector, broker, osm, cleanup_calls = _real_recovery_callback(
        monkeypatch, side=side, bid=bid, ask=ask
    )

    assert selector.select.call_count == 0
    assert osm.submit_existing_entry.call_count == 0
    assert broker.method_calls == []
    assert result["disposition"] in {"REARM_WATCHER_REQUIRED", "TERMINAL_DURABLE"}, result


def test_real_entry_trigger_recovery_allows_at_most_one_selector_continuation_when_valid(
    monkeypatch,
):
    result, selector, broker, osm, cleanup_calls = _real_recovery_callback(
        monkeypatch, side="CALL", bid=101.0, ask=102.0
    )

    # The selector is reached only after the fresh market gate passes, and the
    # invalid placeholder result is terminalized without any broker POST.
    assert selector.select.call_count == 1, result
    assert selector.select.call_count <= 1
    assert osm.submit_existing_entry.call_count == 0
    assert broker.method_calls == []
    assert cleanup_calls


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
