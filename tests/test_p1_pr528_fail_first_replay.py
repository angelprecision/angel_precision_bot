"""Exact implementation-base replay for PR #528.

CI copies this test unchanged into the PR base worktree. The base invocation
proves the historical STUCK_TRIGGER_READY -> terminal/cancel path through the
real PendingTriggerRestartRecovery action table. The head invocation proves the
same durable row is observed as RETRY_OWNED without authority drift or broker
activity.
"""
from __future__ import annotations

import copy
import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from ap.pending_trigger_classifier import (
    PendingTriggerClassification as PTC,
    classify_pending_trigger_row,
)
from ap.pending_trigger_restart_recovery import (
    PendingTriggerRestartRecovery,
    _RowOutcome,
)
from ap_canonical_signal import build_canonical_signal_id


_CLIENT_ID = "jasoncosby1@gmail.com"
_MODE = "live"
_LOCAL_ORDER_ID = "pr528-fail-first-live-order"
_SIGNAL_ID = "pr528-fail-first-live-signal"
_AUTHORITY_FIELDS = (
    "lifecycle_state",
    "materialization_status",
    "materialization_in_flight",
    "materialization_generation",
    "materialization_attempts",
    "retry_attempt",
    "breach_attempt_count",
    "retry_max_attempts",
    "materialization_next_retry_at",
    "next_retry_at",
    "materialization_reason",
    "materialization_last_failure_at",
    "materialization_outcome",
    "retry_owner",
    "broker_ready",
)


class _DurableOSM:
    def __init__(self, row: dict):
        self.row = copy.deepcopy(row)
        self.cancel_calls: list[tuple[str, str]] = []
        self.meta_writes: list[dict] = []

    def get_order(self, local_order_id: str):
        if local_order_id != self.row["local_order_id"]:
            return None
        return copy.deepcopy(self.row)

    def update_order_meta(self, local_order_id: str, patch: dict) -> bool:
        if local_order_id != self.row["local_order_id"]:
            return False
        self.meta_writes.append(dict(patch))
        self.row["meta"].update(dict(patch))
        return True

    def cancel_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        if local_order_id != self.row["local_order_id"]:
            return False
        self.cancel_calls.append((local_order_id, reason))
        self.row["status"] = "CANCELED"
        self.row["last_error"] = reason
        return True


def _production_row() -> dict:
    now = datetime.now(timezone.utc)
    next_retry_at = (now + timedelta(minutes=2)).isoformat()
    return {
        "local_order_id": _LOCAL_ORDER_ID,
        "signal_id": _SIGNAL_ID,
        "plan_id": "pr528-live-plan",
        "client_id": _CLIENT_ID,
        "client_email": _CLIENT_ID,
        "execution_mode": _MODE,
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "direction": "CALL",
        "ticker": "SPY",
        "symbol": "SPY",
        "contract": "DEFERRED:SPY",
        "entry_price": 450.0,
        "trigger_price": 450.0,
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "watcher_audit": {"reason_code": "trigger_ready"},
            "canonical_signal_id": build_canonical_signal_id(_SIGNAL_ID),
            "trigger_crossed_at": (now - timedelta(seconds=5)).isoformat(),
            "trigger_crossed_at_provenance": {
                "canonical_signal_id": _SIGNAL_ID,
                "client_id": _CLIENT_ID,
                "execution_mode": _MODE,
                "local_order_id": _LOCAL_ORDER_ID,
            },
            "trigger_price": 450.0,
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "materialization_in_flight": False,
            "materialization_generation": 1,
            "materialization_attempts": 1,
            "retry_attempt": 1,
            "breach_attempt_count": 1,
            "retry_max_attempts": 5,
            "materialization_next_retry_at": next_retry_at,
            "next_retry_at": next_retry_at,
            "materialization_reason": "PROVIDER_TIMEOUT",
            "materialization_last_failure_at": now.isoformat(),
            "materialization_outcome": "RETRY_LATER_DATA_UNAVAILABLE",
            "retry_owner": "materializer:pr528-live",
            "broker_ready": False,
        },
    }


def _authority(meta: dict) -> dict:
    return {key: meta.get(key) for key in _AUTHORITY_FIELDS}


def test_pr528_exact_base_terminalizes_and_head_preserves_retry():
    expect_base = os.getenv("PR528_EXPECT_BASE", "").strip() == "1"
    row = _production_row()
    before_authority = _authority(row["meta"])
    before_watcher_audit = dict(row["meta"]["watcher_audit"])

    classification = classify_pending_trigger_row(row, watcher_owned=False)
    if expect_base:
        assert classification == PTC.STUCK_TRIGGER_READY
    else:
        assert classification == PTC.WAITING_RETRYABLE

    osm = _DurableOSM(row)
    broker = MagicMock()
    recovery = PendingTriggerRestartRecovery(
        client_id=_CLIENT_ID,
        execution_mode=_MODE,
        osm=osm,
        entry_watcher=None,
        broker=broker,
        quote_check_fn=lambda *_args, **_kwargs: False,
        caller_source="pr528_exact_base_replay",
    )
    outcome = recovery.recover_one_row(row)

    if expect_base:
        assert outcome == _RowOutcome.TERMINALIZED
        assert osm.cancel_calls == [
            (
                _LOCAL_ORDER_ID,
                "restart_stuck_trigger_ready_no_broker_proof",
            )
        ]
        assert osm.row["status"] == "CANCELED"
    else:
        assert outcome == _RowOutcome.RETRY_OWNED
        assert osm.cancel_calls == []
        assert osm.row["status"] == "PENDING_TRIGGER"
        assert _authority(osm.row["meta"]) == before_authority
        assert osm.row["meta"]["watcher_audit"] == before_watcher_audit
        assert osm.row["meta"]["restart_recovery_cls"] == PTC.WAITING_RETRYABLE
        assert (
            osm.row["meta"]["restart_recovery_retry_subtype"]
            == "materialization"
        )
        assert osm.row["meta"]["restart_recovery_at"]

    assert broker.method_calls == []
