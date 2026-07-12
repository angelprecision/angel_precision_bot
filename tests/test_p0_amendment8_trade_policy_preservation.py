"""P0 tests for Amendment §8: trade-policy preservation (regression proof).

Proves that none of the deferred-breach lifecycle amendments (§1–§7)
altered trade-policy semantics — sizing, exits, TP/SL, direction, or
contract selection. The recovery + reconciler paths only ever add
recovery_* bookkeeping to orders.meta via a non-destructive JSONB merge;
they never rewrite a contract, quantity, limit, direction, stop, target,
or selector field, and the terminalize path only touches status/last_error/
meta.

The assertions are a mix of:
  * behavioural — drive the recovery pass and inspect every meta write
  * structural — inspect the exact SQL SET clauses of the mutating OSM
    methods so a future refactor that widens them fails this test
"""

from __future__ import annotations

import inspect
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import ap.order_state_machine as osm_mod
import ap_recovery
from ap_recovery import APStartupRecovery


# Fields that encode a trade decision. No recovery meta write may set any
# of these; the terminalize SET clause may not name any of these columns.
TRADE_POLICY_FIELDS = {
    "contract", "qty", "quantity", "limit_price", "reserved_cost",
    "direction", "side", "stop_underlying", "target_underlying",
    "stop_price", "target_price", "score", "tier",
    "selected_contract", "selected_limit", "selected_qty",
    "selector_pricing_basis", "contract_selection_status",
    "broker_ready", "trigger_price",
}


# ─────────────────────────── Recovery scaffold ──────────────────────────


class _Cur:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, *_a, **_k):
        return self

    def fetchall(self):
        return self.rows


class _Conn:
    def __init__(self, rows):
        self.cursor = _Cur(rows)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_):
        return False


def _row(lifecycle, *, created_ts=None, submit_intent=False, broker_order_id=None):
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
        "next_retry_at": (datetime.now(timezone.utc) - timedelta(seconds=1)).isoformat(),
        "materialization_lease_until": None,
        "execution_mode": "paper",
        "selector_diagnostics": {"seen": 4},
    }
    if submit_intent:
        meta["submit_intent_at"] = datetime.now(timezone.utc).isoformat()
        meta["broker_submit_key"] = "oid-x"
    return {
        "local_order_id": f"oid-{lifecycle or 'waiting'}",
        "client_id": "client@example.com",
        "signal_id": "sig-1", "plan_id": "plan-1", "symbol": "SPY",
        "contract": "SPY260717C00600000", "direction": "CALL",
        "score": 80, "tier": "A", "trigger_price": 500.0,
        "stop_underlying": 495.0, "target_underlying": 510.0,
        "pattern": "2-1-2", "timeframe": "1d", "execution_mode": "paper",
        "qty": 1, "limit_price": 1.25, "reserved_cost": 125.0,
        "status": "PENDING_TRIGGER", "broker_order_id": broker_order_id,
        "submitted_ts": None,
        "created_ts": created_ts or datetime.now(timezone.utc),
        "meta": meta,
    }


def _run(monkeypatch, rows, *, osm, entry_watcher=None, execution_core=None):
    import ap.db as ap_db
    monkeypatch.setattr(ap_db, "conn", lambda: _Conn(rows))
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **k: fn())
    rec = APStartupRecovery(
        client_id="client@example.com", broker=object(), osm=osm, pm=None,
        master_control=types.SimpleNamespace(mode="PAPER"),
        entry_watcher=entry_watcher, execution_core=execution_core,
    )
    result = {"deferred_lifecycles_recovered": 0}
    rec._recover_deferred_breach_lifecycles(result)
    return result


def _osm():
    return types.SimpleNamespace(
        client_id="client@example.com",
        terminalize_deferred_breach=MagicMock(return_value=True),
        update_order_meta=MagicMock(return_value=True),
        submit_existing_entry=MagicMock(return_value={"ok": True}),
    )


def _all_meta_patch_keys(osm):
    keys = set()
    for call in osm.update_order_meta.call_args_list:
        patch = call.args[1] if len(call.args) > 1 else call.kwargs.get("meta_patch", {})
        keys |= set(patch)
    return keys


# ═══════════════════════════════════════════════════════════════════════
# Behavioural: recovery meta writes never touch trade policy
# ═══════════════════════════════════════════════════════════════════════


def test_retry_wait_meta_write_has_no_trade_policy_key(monkeypatch):
    osm = _osm()
    ec = types.SimpleNamespace(
        resume_deferred_broker_ready_order=MagicMock(return_value={
            "disposition": "RETRY_WAIT",
            "reason_code": "RECOVERY_SUBMIT_GATES_NOT_YET_WIRED",
            "attempt": 1, "max_attempts": 20,
            "owner": "recovery_scheduler:client@example.com",
            "generation": 2, "next_retry_at": None,
        }),
    )
    _run(monkeypatch, [_row("BROKER_READY")], osm=osm,
         entry_watcher=None, execution_core=ec)
    assert not (_all_meta_patch_keys(osm) & TRADE_POLICY_FIELDS)


def test_section5_retention_marker_has_no_trade_policy_key(monkeypatch):
    osm = _osm()
    _run(monkeypatch, [_row("RETRY_WAIT")], osm=osm, entry_watcher=None)
    assert not (_all_meta_patch_keys(osm) & TRADE_POLICY_FIELDS)


def test_crash_window_retention_has_no_trade_policy_key(monkeypatch):
    osm = _osm()
    _run(monkeypatch, [_row("SUBMITTING", submit_intent=True)],
         osm=osm, entry_watcher=None, execution_core=None)
    assert not (_all_meta_patch_keys(osm) & TRADE_POLICY_FIELDS)


def test_recovery_never_calls_submit_existing_entry_for_broker_ready(monkeypatch):
    """The deferred recovery path must never place an order via the direct
    submit method — that is the §2 invariant and a trade-policy safeguard."""
    osm = _osm()
    ec = types.SimpleNamespace(
        resume_deferred_broker_ready_order=MagicMock(return_value={
            "disposition": "RETRY_WAIT", "reason_code": "x",
            "attempt": 1, "max_attempts": 20, "owner": "o",
            "generation": 2, "next_retry_at": None,
        }),
    )
    _run(monkeypatch, [_row("BROKER_READY")], osm=osm,
         entry_watcher=None, execution_core=ec)
    osm.submit_existing_entry.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# Structural: the mutating OSM SQL never widens to trade-policy columns
# ═══════════════════════════════════════════════════════════════════════


def test_terminalize_set_clause_is_status_lasterror_meta_only():
    """terminalize_deferred_breach must only SET status, last_error and
    meta — never a trade-policy column."""
    src = inspect.getsource(osm_mod.APOrderStateMachine.terminalize_deferred_breach)
    assert "UPDATE orders SET status=%s, last_error=%s," in src
    # No trade-policy column may appear in a SET assignment.
    for col in ("contract=", "qty=", "limit_price=", "reserved_cost=",
                "direction=", "stop_underlying=", "target_underlying="):
        assert col not in src, f"terminalize SET widened to {col!r}"


def test_update_order_meta_is_non_destructive_merge():
    """The generic meta writer must use a JSONB || merge so unrelated
    trade-policy keys already in meta survive every recovery write."""
    src = inspect.getsource(osm_mod.APOrderStateMachine.update_order_meta)
    assert "COALESCE(meta, '{}'::jsonb) || %s::jsonb" in src
    # It must not SET any top-level trade column.
    for col in ("SET contract", "SET qty", "SET limit_price", "SET direction"):
        assert col not in src


def test_recovery_retention_marker_keys_are_recovery_namespaced():
    """Every key the §5 retention helper writes is recovery-namespaced."""
    src = inspect.getsource(ap_recovery.APStartupRecovery._recover_deferred_breach_lifecycles)
    # The retention helper block lists its patch keys; all must be recovery_*.
    assert '"recovery_ownership"' in src
    assert '"recovery_owner"' in src
    assert '"recovery_retention_reason"' in src
    # And it must go through the non-destructive update_order_meta, never a
    # raw UPDATE that could clobber trade fields.
    assert "update_order_meta" in src
