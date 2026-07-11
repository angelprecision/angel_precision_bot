"""P0 tests for Amendment §9: diagnostics preservation (regression proof).

Proves that the deferred-breach lifecycle amendments preserve — and, where
relevant, enrich — diagnostic and telemetry state rather than stripping it:

  * every recovery meta write is a non-destructive JSONB merge, so
    pre-existing diagnostics (selector_diagnostics, trigger telemetry,
    materialization history) survive
  * terminalize carries a recovery_classification diagnostic and merges,
    never overwrites, meta
  * the §2 RETRY_WAIT write carries the full recovery telemetry set
    (reason_code, attempt, max_attempts, owner, generation, next_retry_at)
  * the §6 reconciler surfaces submit_intent_at + broker_submit_key for
    crash-window diagnostics
"""

from __future__ import annotations

import inspect
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import ap.order_state_machine as osm_mod
import ap_execution_core
from ap_recovery import APStartupRecovery


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


def _row(lifecycle, *, created_ts=None, submit_intent=False):
    meta = {
        "lifecycle_state": lifecycle,
        "materialization_status": (
            "RETRY_PENDING" if lifecycle == "RETRY_WAIT" else "SELECTED"
            if lifecycle == "BROKER_READY" else "WAITING_FOR_TRIGGER"
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
        "status": "PENDING_TRIGGER", "broker_order_id": None,
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


# ═══════════════════════════════════════════════════════════════════════
# Structural: non-destructive merge preserves pre-existing diagnostics
# ═══════════════════════════════════════════════════════════════════════


def test_update_order_meta_preserves_existing_keys_via_merge():
    src = inspect.getsource(osm_mod.APOrderStateMachine.update_order_meta)
    assert "COALESCE(meta, '{}'::jsonb) || %s::jsonb" in src


def test_terminalize_merges_diagnostics_not_overwrites():
    src = inspect.getsource(osm_mod.APOrderStateMachine.terminalize_deferred_breach)
    # meta is merged with ||, and the diagnostics dict is folded into the patch.
    assert "COALESCE(meta, '{}'::jsonb) || %s::jsonb" in src
    assert "dict(diagnostics or {})" in src


# ═══════════════════════════════════════════════════════════════════════
# Behavioural: recovery enriches diagnostics/telemetry
# ═══════════════════════════════════════════════════════════════════════


def test_stale_terminalize_carries_recovery_classification(monkeypatch):
    osm = _osm()
    old = datetime.now(timezone.utc) - timedelta(hours=100)
    _run(monkeypatch, [_row("", created_ts=old)], osm=osm, entry_watcher=None)
    osm.terminalize_deferred_breach.assert_called_once()
    diag = osm.terminalize_deferred_breach.call_args.kwargs["diagnostics"]
    assert diag["recovery_classification"] == "stale_over_72h"


def test_broker_ready_terminal_carries_recovery_telemetry(monkeypatch):
    osm = _osm()
    ec = types.SimpleNamespace(
        resume_deferred_broker_ready_order=MagicMock(return_value={
            "disposition": "TERMINAL_DURABLE",
            "reason_code": "RECOVERY_TRIGGER_TOO_OLD",
            "terminal_status": "EXPIRED",
            "attempt": 3, "max_attempts": 20,
            "owner": "recovery_scheduler:client@example.com",
        }),
    )
    _run(monkeypatch, [_row("BROKER_READY")], osm=osm,
         entry_watcher=None, execution_core=ec)
    diag = osm.terminalize_deferred_breach.call_args.kwargs["diagnostics"]
    assert diag["recovery_classification"] == "broker_ready_boundary"
    assert diag["recovery_attempt"] == 3
    assert diag["recovery_max_attempts"] == 20
    assert diag["recovery_owner"] == "recovery_scheduler:client@example.com"


def test_retry_wait_write_carries_full_telemetry(monkeypatch):
    osm = _osm()
    ec = types.SimpleNamespace(
        resume_deferred_broker_ready_order=MagicMock(return_value={
            "disposition": "RETRY_WAIT",
            "reason_code": "RECOVERY_SUBMIT_GATES_NOT_YET_WIRED",
            "attempt": 2, "max_attempts": 20,
            "owner": "recovery_scheduler:client@example.com",
            "generation": 5, "next_retry_at": "2026-07-10T20:00:00+00:00",
        }),
    )
    _run(monkeypatch, [_row("BROKER_READY")], osm=osm,
         entry_watcher=None, execution_core=ec)
    osm.update_order_meta.assert_called_once()
    patch = osm.update_order_meta.call_args.args[1]
    assert patch["recovery_reason_code"] == "RECOVERY_SUBMIT_GATES_NOT_YET_WIRED"
    assert patch["recovery_attempt_count"] == 2
    assert patch["recovery_max_attempts"] == 20
    assert patch["recovery_owner"] == "recovery_scheduler:client@example.com"
    assert patch["recovery_generation"] == 5
    assert patch["recovery_next_retry_at"] == "2026-07-10T20:00:00+00:00"


def test_section5_retention_marker_carries_diagnostics(monkeypatch):
    osm = _osm()
    _run(monkeypatch, [_row("RETRY_WAIT")], osm=osm, entry_watcher=None)
    patch = osm.update_order_meta.call_args.args[1]
    assert patch["recovery_retained_at"]           # timestamp present
    assert patch["recovery_retention_reason"]      # reason present
    assert patch["recovery_retention_mode"] == "PAPER"


def test_reconciler_surfaces_crash_window_diagnostics():
    """The §6 reconciler passes through submit_intent_at + broker_submit_key
    so a crash-window row is diagnosable."""
    core = types.SimpleNamespace()
    core.client_id = "jason@example.com"
    core.email = "jason@example.com"
    core.order_state_machine = MagicMock()
    core.order_state_machine.get_order.return_value = {
        "local_order_id": "oid-1",
        "client_id": "jason@example.com",
        "broker_order_id": None,
        "meta": {
            "submit_intent_at": "2026-07-10T19:59:00+00:00",
            "broker_submit_key": "oid-1",
        },
    }
    core.reconcile_deferred_broker_intent = (
        ap_execution_core.APExecutionCore.reconcile_deferred_broker_intent.__get__(
            core, type(core)
        )
    )
    result = core.reconcile_deferred_broker_intent(local_order_id="oid-1")
    assert result["submit_intent_at"] == "2026-07-10T19:59:00+00:00"
    assert result["broker_submit_key"] == "oid-1"
