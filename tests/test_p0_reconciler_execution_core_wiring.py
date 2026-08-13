"""Regression tests for the canonical PENDING_TRIGGER broker-intent consumer."""

from __future__ import annotations

import os
from types import SimpleNamespace

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import ap.db as db
import ap.pending_trigger_restart_recovery as ptr_module
from ap_reconciler import APBrokerReconciler, _empty_summary


def test_reconciler_passes_execution_core_to_pending_trigger_recovery(monkeypatch):
    captured = {}
    execution_core = object()

    class _Recovery:
        def __init__(self, **kwargs):
            captured.update(kwargs)

        def recover_one_row(self, _row):
            return ptr_module._RowOutcome.UNRESOLVED

    monkeypatch.setattr(ptr_module, "PendingTriggerRestartRecovery", _Recovery)
    monkeypatch.setattr(db, "run_with_retry", lambda callback: callback())
    monkeypatch.setattr(
        db,
        "get_open_orders_with_invalid_execution_mode",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(
        db,
        "get_open_orders_for_reconcile",
        lambda **_kwargs: [
            {
                "local_order_id": "local-submit-intent",
                "client_id": "client@example.com",
                "execution_mode": "paper",
                "status": "PENDING_TRIGGER",
                "contract": "DEFERRED:SPY",
                "broker_order_id": None,
                "submitted_ts": None,
                "meta": {"submit_intent_at": "2026-08-13T16:00:00+00:00"},
            }
        ],
    )

    reconciler = APBrokerReconciler(
        broker=SimpleNamespace(),
        client_id="client@example.com",
        osm=SimpleNamespace(),
        pm=SimpleNamespace(),
        execution_mode="paper",
        execution_core=execution_core,
    )
    alerts = []
    reconciler._alert = alerts.append

    summary = _empty_summary(reconciler.client_id)
    reconciler._reconcile_orders(summary)

    assert captured["execution_core"] is execution_core
    assert captured["caller_source"] == "ap_reconciler._reconcile_orders"
    assert alerts
