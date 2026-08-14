"""P0 regression tests for evidence-bearing PENDING_TRIGGER reconciliation."""

from __future__ import annotations

import os
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

import ap.db as db
from ap_reconciler import APBrokerReconciler, _empty_summary


@pytest.mark.parametrize("execution_mode", ["paper", "live"])
def test_reconciler_routes_submit_intent_to_canonical_consumer(
    monkeypatch, execution_mode
):
    consumer_calls = []

    class _ExecutionCore:
        def reconcile_deferred_broker_intent(self, *, local_order_id):
            consumer_calls.append(local_order_id)
            return {
                "disposition": "RECONCILE_PENDING",
                "reason_code": "BROKER_LOOKUP_FAILED",
            }

    class _OSM:
        def __init__(self):
            self.retention_calls = []

        def retain_recovery_ownership_if_no_watcher(self, local_order_id, **kwargs):
            self.retention_calls.append((local_order_id, kwargs))
            return True

    class _Broker:
        def __init__(self):
            self.calls = []

        def __getattr__(self, name):
            if name not in {"place_order", "buy_option", "submit_order", "cancel_order"}:
                raise AttributeError(name)

            def _unexpected_call(*args, **kwargs):
                self.calls.append((name, args, kwargs))
                raise AssertionError(f"broker mutation called: {name}")

            return _unexpected_call

    execution_core = _ExecutionCore()
    osm = _OSM()
    broker = _Broker()
    query_context = []

    monkeypatch.setattr(db, "run_with_retry", lambda callback: callback())
    monkeypatch.setattr(
        db,
        "get_open_orders_with_invalid_execution_mode",
        lambda **_kwargs: [],
    )
    monkeypatch.setattr(
        db,
        "get_open_orders_for_reconcile",
        lambda **kwargs: (
            query_context.append(kwargs)
            or [
                {
                    "local_order_id": "local-submit-intent",
                    "client_id": "client@example.com",
                    "execution_mode": execution_mode,
                    "status": "PENDING_TRIGGER",
                    "kind": "ENTRY",
                    "contract": "DEFERRED:SPY",
                    "broker_order_id": None,
                    "submitted_ts": None,
                    "meta": {"submit_intent_at": "2026-08-13T16:00:00+00:00"},
                }
            ]
        ),
    )

    reconciler = APBrokerReconciler(
        broker=broker,
        client_id="client@example.com",
        osm=osm,
        pm=SimpleNamespace(),
        execution_mode=execution_mode,
        execution_core=execution_core,
    )
    monkeypatch.setattr(reconciler, "_check_ghost_fills", lambda _summary: None)

    summary = _empty_summary(reconciler.client_id)
    reconciler._reconcile_orders(summary)

    assert query_context == [
        {"client_id": "client@example.com", "execution_mode": execution_mode}
    ]
    assert consumer_calls == ["local-submit-intent"]
    assert len(osm.retention_calls) == 1
    retained_local_id, retention_kwargs = osm.retention_calls[0]
    assert retained_local_id == "local-submit-intent"
    assert retention_kwargs["recovery_owner"] == (
        f"prebroker_recovery:client@example.com:{execution_mode}:local-submit-intent"
    )
    assert retention_kwargs["recovery_retention_mode"] == execution_mode.upper()
    assert broker.calls == []


def test_reconciler_does_not_classify_plain_pending_trigger_as_broker_intent():
    row = {
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {},
    }
    assert APBrokerReconciler._has_submit_intent_evidence(row) is False
    row["meta"] = {"submit_intent_at": "2026-08-13T16:00:00+00:00"}
    assert APBrokerReconciler._has_submit_intent_evidence(row) is True
