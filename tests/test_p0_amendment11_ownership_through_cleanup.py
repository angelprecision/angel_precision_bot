from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ap_execution_core
from ap.order_state_machine import APOrderStateMachine


def _pending_row(meta=None, **overrides):
    row = {
        "local_order_id": "oid-1", "client_id": "jason@example.com",
        "execution_mode": "live", "kind": "ENTRY", "status": "PENDING_TRIGGER",
        "broker_order_id": None, "submitted_ts": None, "meta": meta or {},
    }
    row.update(overrides)
    return row


@pytest.mark.parametrize("guarded", [
    {"lifecycle_state": "SUBMITTING"},
    {"submit_intent_at": datetime.now(timezone.utc).isoformat()},
    {"broker_submit_key": "oid-1"},
    {"current_owner": "broker_submit:oid-1"},
    {
        "recovery_submit_owner": "worker-B",
        "recovery_submit_lease_until": (datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
    },
])
@pytest.mark.parametrize("method", ["cancel_pending_entry", "expire_pending_entry"])
def test_generic_pending_cleanup_refuses_ambiguous_meta(guarded, method):
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = "jason@example.com"
    osm._get_order = MagicMock(return_value=_pending_row(guarded))
    osm.transition = MagicMock()
    assert getattr(osm, method)("oid-1", reason="test") is False
    osm.transition.assert_not_called()


@pytest.mark.parametrize("column", ["broker_order_id", "submitted_ts"])
@pytest.mark.parametrize("method", ["cancel_pending_entry", "expire_pending_entry"])
def test_generic_pending_cleanup_refuses_broker_columns(column, method):
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = "jason@example.com"
    osm._get_order = MagicMock(return_value=_pending_row(**{column: "present"}))
    osm.transition = MagicMock()
    assert getattr(osm, method)("oid-1", reason="test") is False
    osm.transition.assert_not_called()


def _recovered_watched():
    plan = SimpleNamespace(metadata={
        "recovery_submit_fenced": True,
        "recovery_submit_owner": "worker-A",
        "recovery_submit_generation": 3,
    }, execution_mode="live")
    signal = {
        "signal_id": "sig-1", "local_order_id": "oid-1",
        "execution_mode": "live", "recovery_submit_owner": "worker-A",
        "recovery_submit_generation": 3, "recovery_submit_fenced": True,
        "_approved_plan": plan,
    }
    return SimpleNamespace(signal=signal, ticker="SPY", trigger_price=600.0)


def test_stale_worker_risk_failure_uses_terminal_cas_and_never_expires_new_owner():
    core = SimpleNamespace(
        client_id="jason@example.com", email="jason@example.com", execution_mode="live",
        store=MagicMock(), order_state_machine=MagicMock(), _max_positions=4,
    )
    core._breach_risk_check = MagicMock(return_value=False)
    core._emit_breach_diag = MagicMock()
    core._classify_recovered_ownership_loss = (
        ap_execution_core.APExecutionCore._classify_recovered_ownership_loss.__get__(core, type(core))
    )
    core._cleanup_pending_entry_order = (
        ap_execution_core.APExecutionCore._cleanup_pending_entry_order.__get__(core, type(core))
    )
    core._on_entry_trigger = ap_execution_core.APExecutionCore._on_entry_trigger.__get__(core, type(core))
    core.order_state_machine.terminalize_recovered_entry.return_value = False
    core.order_state_machine.get_order.return_value = _pending_row({
        "lifecycle_state": "BROKER_READY", "recovery_submit_owner": "worker-B",
    })

    result = core._on_entry_trigger(_recovered_watched())

    assert result["disposition"] == "OWNERSHIP_TRANSFERRED"
    core.order_state_machine.terminalize_recovered_entry.assert_called_once()
    core.order_state_machine.expire_pending_entry.assert_not_called()
    core.order_state_machine.cancel_pending_entry.assert_not_called()


def test_recovered_cleanup_cas_success_is_terminal_without_generic_helper():
    core = SimpleNamespace(order_state_machine=MagicMock())
    core._classify_recovered_ownership_loss = MagicMock()
    core._cleanup_pending_entry_order = (
        ap_execution_core.APExecutionCore._cleanup_pending_entry_order.__get__(core, type(core))
    )
    watched = _recovered_watched()
    watched.signal["_callback_ownership_context"] = {
        "is_recovered": True, "owner": "worker-A", "generation": 3,
        "execution_mode": "live", "local_order_id": "oid-1",
    }
    core.order_state_machine.terminalize_recovered_entry.return_value = True
    assert core._cleanup_pending_entry_order(watched, action="cancel", reason="hard_reject") is True
    core.order_state_machine.cancel_pending_entry.assert_not_called()


def test_fence_lost_branch_precedes_generic_cancel_cleanup():
    import inspect
    source = inspect.getsource(ap_execution_core.APExecutionCore._on_entry_trigger)
    fence = source.index('submit_res.get("error") == "RECOVERY_SUBMIT_INTENT_FENCE_LOST"')
    generic_cancel = source.index('if hasattr(self.order_state_machine, "cancel_pending_entry")')
    assert fence < generic_cancel
    fenced_block = source[fence:generic_cancel]
    assert "_classify_recovered_ownership_loss" in fenced_block
    assert "return" in fenced_block


def test_real_claim_and_intent_cas_reject_stale_owner(monkeypatch):
    """Execute the real OSM CAS methods against one transactional fake row."""
    import json
    state = _pending_row({
        "lifecycle_state": "BROKER_READY", "broker_ready": True,
        "materialization_generation": 3, "submit_intent_at": None,
        "recovery_submit_owner": "", "recovery_submit_lease_until": "",
    })

    class Cursor:
        rowcount = 0

        def execute(self, sql, params):
            patch = json.loads(params[0])
            meta = state["meta"]
            now = datetime.now(timezone.utc)
            if "recovery_submit_claimed_at" in patch:
                lease_raw = str(meta.get("recovery_submit_lease_until") or "")
                try:
                    lease = datetime.fromisoformat(lease_raw)
                    if lease.tzinfo is None:
                        lease = lease.replace(tzinfo=timezone.utc)
                except Exception:
                    lease = datetime.min.replace(tzinfo=timezone.utc)
                eligible = (
                    state["status"] == "PENDING_TRIGGER"
                    and not state.get("broker_order_id") and not state.get("submitted_ts")
                    and meta.get("broker_ready") is True
                    and meta.get("lifecycle_state") == "BROKER_READY"
                    and int(meta.get("materialization_generation") or 0) == int(params[3])
                    and not meta.get("submit_intent_at")
                    and (not meta.get("recovery_submit_owner") or lease < now)
                )
            else:
                lease = datetime.fromisoformat(str(meta.get("recovery_submit_lease_until")))
                if lease.tzinfo is None:
                    lease = lease.replace(tzinfo=timezone.utc)
                eligible = (
                    state["status"] == "PENDING_TRIGGER"
                    and not state.get("broker_order_id") and not state.get("submitted_ts")
                    and state["execution_mode"] == params[3]
                    and meta.get("lifecycle_state") == "BROKER_READY"
                    and meta.get("broker_ready") is True
                    and int(meta.get("materialization_generation") or 0) == int(params[4])
                    and meta.get("recovery_submit_owner") == params[5]
                    and lease >= now and not meta.get("submit_intent_at")
                )
            self.rowcount = 1 if eligible else 0
            if eligible:
                meta.update(patch)
            return self

    cursor = Cursor()

    class Conn:
        rowcount = 0
        def __enter__(self): return cursor
        def __exit__(self, *_args): return False
        def execute(self, sql, params): return cursor.execute(sql, params)

    class ConnContext:
        def __enter__(self): return Conn()
        def __exit__(self, *_args): return False

    # The full P0 suite reloads ap.order_state_machine in import tests. Patch
    # the globals used by the already-collected class methods, not whichever
    # module object happens to be current in sys.modules.
    method_globals = APOrderStateMachine.claim_deferred_broker_ready_submit.__globals__
    monkeypatch.setitem(method_globals, "conn", lambda: ConnContext())
    monkeypatch.setitem(method_globals, "run_with_retry", lambda fn: fn())
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = "jason@example.com"

    assert osm.claim_deferred_broker_ready_submit("oid-1", owner="worker-A", generation=3)
    state["meta"]["recovery_submit_lease_until"] = (
        datetime.now(timezone.utc) - timedelta(seconds=1)
    ).isoformat()
    assert osm.claim_deferred_broker_ready_submit("oid-1", owner="worker-B", generation=3)
    assert not osm.persist_deferred_submit_intent(
        "oid-1", owner="worker-A", generation=3, execution_mode="live",
        payload_hash="hash-A", broker_submit_key="oid-1",
    )
    assert osm.persist_deferred_submit_intent(
        "oid-1", owner="worker-B", generation=3, execution_mode="live",
        payload_hash="hash-B", broker_submit_key="oid-1",
    )
    assert state["meta"]["lifecycle_state"] == "SUBMITTING"
    assert state["meta"]["broker_submit_payload_hash"] == "hash-B"
