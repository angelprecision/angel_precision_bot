"""P0 invariant tests for deferred-breach ownership and durable handoff."""

from __future__ import annotations

import json
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import ap.order_state_machine as osm_mod
from ap.order_state_machine import APOrderStateMachine
from ap_recovery import APStartupRecovery
from ap_execution_core import (
    RETRYABLE_BREACH_SELECTOR_REASONS,
    _classify_materialization_handoff,
    _validate_deferred_selector_result,
)
from ap_entry_watcher import APEntryWatcher


class _Cursor:
    def __init__(self, sink, rowcount=1):
        self.sink = sink
        self.rowcount = rowcount

    def execute(self, sql, params=()):
        self.sink.append((" ".join(str(sql).split()), tuple(params)))
        return self


class _Conn:
    def __init__(self, sink, rowcount=1):
        self.cursor = _Cursor(sink, rowcount=rowcount)
        self.rowcount = rowcount

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_):
        return False


@pytest.fixture
def db_spy(monkeypatch):
    sink = []
    state = {"rowcount": 1}

    monkeypatch.setattr(
        osm_mod,
        "conn",
        lambda: _Conn(sink, rowcount=state["rowcount"]),
    )
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **k: fn())
    return sink, state


def _claim_kwargs():
    return {
        "owner": "watcher:abc",
        "generation": 2,
        "lease_until": (datetime.now(timezone.utc) + timedelta(seconds=120)).isoformat(),
        "trigger_crossed_at": datetime.now(timezone.utc).isoformat(),
        "trigger_price": 100.0,
        "observed_underlying_price": 100.2,
        "signal_id": "sig-1",
        "execution_mode": "LIVE",
        "canonical_signal_id": "sig-1",
    }


def test_claim_is_atomic_fenced_and_identity_scoped(db_spy):
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    assert osm.claim_deferred_materialization("oid-1", **_claim_kwargs()) is True
    sql, params = sink[-1]
    patch = json.loads(params[0])
    assert patch["lifecycle_state"] == "MATERIALIZING"
    assert patch["materialization_in_flight"] is True
    assert patch["materialization_owner"] == "watcher:abc"
    assert patch["materialization_generation"] == 2
    assert patch["client_id"] == "client@example.com"
    assert "signal_id = %s" in sql
    assert "materialization_lease_until" in sql
    assert "broker_order_id IS NULL" in sql


def test_claim_cas_miss_blocks_worker(db_spy):
    _, state = db_spy
    state["rowcount"] = 0
    osm = APOrderStateMachine("client@example.com")
    assert osm.claim_deferred_materialization("oid-1", **_claim_kwargs()) is False


@pytest.mark.parametrize(
    "contract,limit_price,qty",
    [("DEFERRED:SPY", 1.0, 1), ("SPY260717C00600000", 0, 1), ("SPY260717C00600000", 1.0, 0)],
)
def test_copyback_rejects_invalid_selector_result_without_db_write(
    db_spy, contract, limit_price, qty,
):
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    assert osm.persist_deferred_broker_ready(
        "oid-1",
        owner="watcher:abc",
        generation=1,
        signal_id="sig-1",
        execution_mode="live",
        contract=contract,
        limit_price=limit_price,
        qty=qty,
        reserved_cost=max(0, qty * limit_price * 100),
        selector_meta={},
    ) is False
    assert sink == []


@pytest.mark.parametrize(
    "selection",
    [
        types.SimpleNamespace(
            contract_symbol="DEFERRED:SPY", execution_price_per_share=1.0,
            affordable_contracts=1,
        ),
        types.SimpleNamespace(
            contract_symbol="SPY260717C00600000", execution_price_per_share=0,
            ask=0, mid=0, affordable_contracts=1,
        ),
        types.SimpleNamespace(
            contract_symbol="SPY260717C00600000", execution_price_per_share=1.0,
            affordable_contracts=0,
        ),
    ],
)
def test_selector_object_is_not_success_without_occ_price_and_quantity(selection):
    valid, _, _, _ = _validate_deferred_selector_result(selection, "SPY")
    assert valid is False


def test_copyback_is_one_complete_cas_write(db_spy):
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    assert osm.persist_deferred_broker_ready(
        "oid-1",
        owner="watcher:abc",
        generation=3,
        signal_id="sig-1",
        execution_mode="paper",
        contract="SPY260717C00600000",
        limit_price=1.25,
        qty=2,
        reserved_cost=250,
        selector_meta={"selector_pricing_basis": "ask", "selector_diagnostics": {"seen": 4}},
    ) is True
    assert len(sink) == 1
    sql, params = sink[0]
    patch = json.loads(params[4])
    assert (params[0], params[1], params[2], params[3]) == (
        "SPY260717C00600000", 1.25, 2, 250.0,
    )
    assert patch["broker_ready"] is True
    assert patch["lifecycle_state"] == "BROKER_READY"
    assert patch["selector_pricing_basis"] == "ask"
    assert patch["selector_failure"] is None
    assert "materialization_owner" in sql
    assert "materialization_generation" in sql


def test_retry_wait_is_durable_and_releases_materializer_claim(db_spy):
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    due = (datetime.now(timezone.utc) + timedelta(seconds=20)).isoformat()
    assert osm.schedule_deferred_materialization_retry(
        "oid-1",
        owner="watcher:abc",
        generation=1,
        reason_code="CHAIN_PROVIDER_ERROR",
        attempt=1,
        max_attempts=3,
        next_retry_at=due,
        selector_failure={"provider_status": 503},
    ) is True
    patch = json.loads(sink[-1][1][0])
    assert patch["lifecycle_state"] == "RETRY_WAIT"
    assert patch["materialization_in_flight"] is False
    assert patch["retry_owner"] == "watcher:abc"
    assert patch["retry_attempt"] == 1
    assert patch["retry_max_attempts"] == 3
    assert patch["next_retry_at"] == due
    assert patch["materialization_selector_failure"]["provider_status"] == 503


@pytest.mark.parametrize(
    "reason",
    [
        "CHAIN_PROVIDER_ERROR",
        "CHAIN_PROVIDER_EMPTY_OPTIONS",
        "CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
        "QUOTE_FETCH_FAILED",
        "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
        "MARKET_DATA_THROTTLE_UNAVAILABLE",
        "PROVIDER_RATE_LIMITED",
        "PROVIDER_TIMEOUT",
    ],
)
def test_transient_provider_reasons_remain_retryable(reason):
    assert reason in RETRYABLE_BREACH_SELECTOR_REASONS


def test_terminal_reason_and_owner_release_are_atomic(db_spy):
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    assert osm.terminalize_deferred_breach(
        "oid-1",
        reason_code="BREACH_RETRY_EXHAUSTED:CHAIN_PROVIDER_ERROR",
        terminal_status="ERROR",
        owner="watcher:abc",
        generation=1,
        diagnostics={"last_selector_reason": "CHAIN_PROVIDER_ERROR"},
    ) is True
    sql, params = sink[-1]
    patch = json.loads(params[2])
    assert params[0] == "ERROR"
    assert params[1] == "BREACH_RETRY_EXHAUSTED:CHAIN_PROVIDER_ERROR"
    assert patch["reason_code"] == params[1]
    assert patch["materialization_in_flight"] is False
    assert patch["materialization_owner"] == ""
    assert "last_error=%s" in sql


def test_handoff_blocks_durable_price_mismatch():
    ok, reason = _classify_materialization_handoff(
        handoff_snapshot={"captured": True, "selector_contract": "SPY260717C00600000"},
        pre_submit_contract="SPY260717C00600000",
        pre_submit_limit=1.25,
        pre_submit_qty=1,
        order_row_contract="SPY260717C00600000",
        order_row_limit=1.20,
        order_row_qty=1,
    )
    assert ok is False
    assert reason.startswith("order_row_limit_diverges")


def test_handoff_blocks_durable_quantity_mismatch():
    ok, reason = _classify_materialization_handoff(
        handoff_snapshot={"captured": True, "selector_contract": "SPY260717C00600000"},
        pre_submit_contract="SPY260717C00600000",
        pre_submit_limit=1.25,
        pre_submit_qty=2,
        order_row_contract="SPY260717C00600000",
        order_row_limit=1.25,
        order_row_qty=1,
    )
    assert ok is False
    assert reason.startswith("order_row_qty_diverges")


def test_watcher_retains_retry_ownership_from_callback_result():
    """Amendment §4: a RETRY_WAIT claim must be *verified* against the
    durable order row, not trusted verbatim.  With a matching row the
    watcher returns the same RETRY_WAIT disposition and next_retry_at
    it did before §4."""
    due = (datetime.now(timezone.utc) + timedelta(seconds=20)).isoformat()
    osm = MagicMock()
    osm.get_order.return_value = {
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "meta": {
            "lifecycle_state": "RETRY_WAIT",
            "materialization_status": "RETRY_PENDING",
            "next_retry_at": due,
            "retry_attempt": 1,
            "retry_max_attempts": 3,
            "broker_ready": False,
        },
    }
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="LIVE")
    watched = types.SimpleNamespace(signal={
        "local_order_id": "oid-retry",
        "contract_symbol": "DEFERRED:SPY",
    })
    assert watcher._resolve_trigger_callback_disposition(
        watched, {"disposition": "RETRY_WAIT", "next_retry_at": due}
    ) == ("RETRY_WAIT", due)


def test_deferred_callback_without_durable_outcome_is_unknown():
    osm = MagicMock()
    osm.get_order.return_value = {
        "status": "PENDING_TRIGGER",
        "meta": {"lifecycle_state": "BROKER_READY", "broker_ready": True},
    }
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="LIVE")
    watched = types.SimpleNamespace(
        signal={"local_order_id": "oid-1", "contract_symbol": "DEFERRED:SPY"}
    )
    assert watcher._resolve_trigger_callback_disposition(watched, None) == ("UNKNOWN", None)


def test_durable_retry_row_is_a_real_watcher_owned_schedule():
    due = (datetime.now(timezone.utc) + timedelta(seconds=20)).isoformat()
    osm = MagicMock()
    osm.get_order.return_value = {
        "status": "PENDING_TRIGGER",
        "meta": {"lifecycle_state": "RETRY_WAIT", "next_retry_at": due},
    }
    watcher = APEntryWatcher(None, order_state_machine=osm, mode="LIVE")
    watched = types.SimpleNamespace(
        signal={"local_order_id": "oid-1", "contract_symbol": "DEFERRED:SPY"}
    )
    assert watcher._resolve_trigger_callback_disposition(watched, None) == ("RETRY_WAIT", due)


def test_source_has_no_thread_only_deferred_retry_scheduler():
    source = open("ap_execution_core.py", encoding="utf-8").read()
    assert "name=f\"deferred_retry_" not in source
    assert "schedule_deferred_materialization_retry" in source
    assert '"disposition": "RETRY_WAIT"' in source


def test_submit_path_requires_durable_intent_and_stable_tag():
    source = open("ap/order_state_machine.py", encoding="utf-8").read()
    assert '"submit_intent_at"' in source
    assert '"broker_submit_payload_hash"' in source
    assert "canonical_broker_submit_key(local_order_id)" in source
    assert "persist_deferred_broker_ready" in source


def test_restart_recovery_handles_retry_materializing_and_broker_ready():
    source = open("ap_recovery.py", encoding="utf-8").read()
    assert 'lifecycle in {"BROKER_READY", "SUBMITTING"}' in source
    assert 'lifecycle == "RETRY_WAIT"' in source
    assert 'lifecycle == "MATERIALIZING"' in source
    assert "materialization_resume=materialization_resume" in source
    runner_source = open("client_runner.py", encoding="utf-8").read()
    assert "_run_deferred_breach_lifecycle_recovery" in runner_source
    assert "recover_deferred_lifecycles()" in runner_source


class _RecoveryCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, *_args, **_kwargs):
        return self

    def fetchall(self):
        return self.rows


class _RecoveryConn:
    def __init__(self, rows):
        self.cursor = _RecoveryCursor(rows)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_):
        return False


def _recovery_order(lifecycle, *, lease_until=None, next_retry_at=None, contract="DEFERRED:SPY"):
    meta = {
        "lifecycle_state": lifecycle,
        "materialization_status": (
            "RETRY_PENDING" if lifecycle == "RETRY_WAIT"
            else "RUNNING" if lifecycle == "MATERIALIZING"
            else "SELECTED" if lifecycle == "BROKER_READY"
            else "WAITING_FOR_TRIGGER"
        ),
        "materialization_generation": 2,
        "broker_ready": lifecycle == "BROKER_READY",
        "next_retry_at": next_retry_at,
        "materialization_lease_until": lease_until,
        "signal_entry_price": 500.0,
        "execution_mode": "paper",
    }
    return {
        "local_order_id": f"oid-{lifecycle or 'waiting'}",
        "client_id": "client@example.com",
        "signal_id": f"sig-{lifecycle or 'waiting'}",
        "plan_id": "plan-1",
        "symbol": "SPY",
        "contract": contract,
        "direction": "CALL",
        "score": 80,
        "tier": "A",
        "trigger_price": 500.0,
        "stop_underlying": 495.0,
        "target_underlying": 510.0,
        "pattern": "2-1-2",
        "timeframe": "1d",
        "execution_mode": "paper",
        "qty": 1,
        "limit_price": 1.25,
        "reserved_cost": 125.0,
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "created_ts": datetime.now(timezone.utc),
        "meta": meta,
    }


def _run_recovery_rows(monkeypatch, rows, *, execution_core=None):
    import ap.db as ap_db

    monkeypatch.setattr(ap_db, "conn", lambda: _RecoveryConn(rows))
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **k: fn())
    watcher = types.SimpleNamespace(
        has_order=lambda _oid: False,
        watch=MagicMock(return_value=True),
    )
    osm = types.SimpleNamespace(
        submit_existing_entry=MagicMock(return_value={"ok": True}),
        terminalize_deferred_breach=MagicMock(return_value=True),
        update_order_meta=MagicMock(return_value=True),
        client_id="client@example.com",
    )
    recovery = APStartupRecovery(
        client_id="client@example.com",
        broker=object(),
        osm=osm,
        pm=None,
        master_control=types.SimpleNamespace(mode="PAPER"),
        entry_watcher=watcher,
        execution_core=execution_core,
    )
    result = {"deferred_lifecycles_recovered": 0}
    recovery._recover_deferred_breach_lifecycles(result)
    return watcher, osm, result


def test_restart_broker_ready_routes_through_scaffold_and_never_submits(monkeypatch):
    """Amendment §2: recovery of a BROKER_READY row MUST route through
    execution_core.resume_deferred_broker_ready_order.  It MUST NOT call
    osm.submit_existing_entry directly.  While the scaffold's canonical
    gates are unwired the row remains BROKER_READY under the recovery
    scheduler (RETRY_WAIT ownership recorded via meta patch)."""
    row = _recovery_order("BROKER_READY", contract="SPY260717C00600000")
    ec = types.SimpleNamespace(
        resume_deferred_broker_ready_order=MagicMock(return_value={
            "disposition": "RETRY_WAIT",
            "reason_code": "RECOVERY_SUBMIT_GATES_NOT_YET_WIRED",
            "next_retry_at": "2026-07-10T20:00:00+00:00",
            "attempt": 1,
            "max_attempts": 20,
            "owner": "recovery_scheduler:client@example.com",
            "generation": 1,
            "local_order_id": "oid-BROKER_READY",
        }),
    )
    watcher, osm, _ = _run_recovery_rows(monkeypatch, [row], execution_core=ec)

    # Hard invariant: direct submit path never touched
    osm.submit_existing_entry.assert_not_called()
    # Row not destroyed by scaffold RETRY_WAIT
    osm.terminalize_deferred_breach.assert_not_called()
    # Scaffold was consulted
    ec.resume_deferred_broker_ready_order.assert_called_once()
    call_kwargs = ec.resume_deferred_broker_ready_order.call_args.kwargs
    assert call_kwargs["local_order_id"] == "oid-BROKER_READY"
    # Ownership persisted via non-destructive meta patch
    osm.update_order_meta.assert_called_once()
    patch = osm.update_order_meta.call_args.args[1]
    assert patch["recovery_reason_code"] == "RECOVERY_SUBMIT_GATES_NOT_YET_WIRED"
    assert patch["recovery_attempt_count"] == 1
    assert patch["recovery_owner"] == "recovery_scheduler:client@example.com"
    # Preservation: no key in the patch mutates broker_ready / contract / qty / limit
    for forbidden in {"broker_ready", "contract", "qty", "limit_price",
                      "selected_contract", "selected_limit", "selected_qty",
                      "client_id", "execution_mode"}:
        assert forbidden not in patch, (
            f"§2 preservation invariant broken: meta patch mutated {forbidden!r}"
        )


def test_restart_broker_ready_without_execution_core_retains_row(monkeypatch):
    """Without execution_core wired, the recovery must NOT fall through
    to submit_existing_entry (that's the exact path §2 forbids).  Row
    must be retained without terminalization; a future recovery pass
    with execution_core wired will handle it."""
    row = _recovery_order("BROKER_READY", contract="SPY260717C00600000")
    watcher, osm, _ = _run_recovery_rows(monkeypatch, [row], execution_core=None)
    osm.submit_existing_entry.assert_not_called()
    osm.terminalize_deferred_breach.assert_not_called()
    osm.update_order_meta.assert_not_called()


def test_restart_due_retry_row_rearms_same_generation(monkeypatch):
    due = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    watcher, osm, result = _run_recovery_rows(
        monkeypatch, [_recovery_order("RETRY_WAIT", next_retry_at=due)]
    )
    watcher.watch.assert_called_once()
    assert watcher.watch.call_args.kwargs["materialization_resume"] is True
    assert watcher.watch.call_args.args[0].metadata["materialization_generation"] == 2
    osm.submit_existing_entry.assert_not_called()
    assert result["deferred_lifecycles_recovered"] == 1


def test_restart_stale_materializing_row_rearms(monkeypatch):
    stale_lease = (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat()
    watcher, _, result = _run_recovery_rows(
        monkeypatch, [_recovery_order("MATERIALIZING", lease_until=stale_lease)]
    )
    watcher.watch.assert_called_once()
    assert watcher.watch.call_args.kwargs["materialization_resume"] is True
    assert result["deferred_lifecycles_recovered"] == 1


# NOTE: test_restart_broker_ready_resumes_submit_once was removed under
# amendment §2 — it asserted the exact direct-submit behaviour that §2
# forbids.  Replaced by test_restart_broker_ready_routes_through_scaffold_and_never_submits
# and test_restart_broker_ready_without_execution_core_retains_row above.
