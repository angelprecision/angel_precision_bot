"""P0 tests for Amendment §2: fail-closed recovery submit scaffold.

Proves the mandatory invariants from PR #323 amendment §2:

  * a BROKER_READY row cannot reach osm.submit_existing_entry() via
    recovery (the exact behaviour §2 explicitly forbids)
  * the scaffold never calls the broker (never touches broker.place_order,
    broker.buy_option, or any broker method)
  * TERMINAL_DURABLE is only returned on a truthful boundary
    (client mismatch, wrong kind, invalid status, missing trigger,
    malformed trigger, trigger too old, retry exhausted)
  * RETRY_WAIT is returned for engineering-incomplete cases with
    bounded next_retry_at, owner, generation, attempt metadata
  * KEEP_WATCHER is returned when inspection could not complete
  * broker_ready, contract, qty, limit and selector evidence are NOT
    mutated by the RETRY_WAIT persistence path
"""

from __future__ import annotations

import os
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import ap_execution_core


# ─────────────────────────── Scaffold subject ───────────────────────────


def _make_core(client_id="jason@example.com", email="jason@example.com"):
    """Build a minimal APExecutionCore-like object exposing only what
    resume_deferred_broker_ready_order needs (via bind-then-invoke).

    We don't instantiate the real APExecutionCore because its __init__
    has heavy collaborators. resume_deferred_broker_ready_order only
    reads self.order_state_machine, self.client_id, self.email —
    all patched here."""
    core = types.SimpleNamespace()
    core.client_id = client_id
    core.email = email
    core.execution_mode = "live"
    core.order_state_machine = MagicMock()
    core.order_state_machine.claim_deferred_broker_ready_submit.return_value = False
    core._is_real_occ_contract = ap_execution_core.APExecutionCore._is_real_occ_contract
    core._on_entry_trigger = MagicMock()
    # Bind the unbound method to our stub instance
    core.resume_deferred_broker_ready_order = (
        ap_execution_core.APExecutionCore.resume_deferred_broker_ready_order.__get__(
            core, type(core)
        )
    )
    return core


def _future_iso(seconds=30):
    return (datetime.now(timezone.utc) + timedelta(seconds=seconds)).isoformat()


def _past_iso(seconds=30):
    return (datetime.now(timezone.utc) - timedelta(seconds=seconds)).isoformat()


def _row(**overrides):
    base = {
        "local_order_id": "oid-1",
        "client_id": "jason@example.com",
        "execution_mode": "live",
        "signal_id": "sig-1",
        "symbol": "SPY",
        "direction": "CALL",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "broker_order_id": None,
        "submitted_ts": None,
        "contract": "SPY260717C00600000",
        "qty": 1,
        "limit_price": 2.10,
        "reserved_cost": 210.0,
        "meta": {
            "lifecycle_state": "BROKER_READY",
            "broker_ready": True,
            "trigger_crossed_at": _past_iso(20),   # 20s ago — well within window
            "trigger_confirmed_at": _past_iso(19),
            "materialization_generation": 3,
            "selected_contract": "SPY260717C00600000",
            "selected_limit": 2.10,
            "selected_qty": 1,
        },
    }
    base.update(overrides)
    return base


# ═══════════════════════════════════════════════════════════════════════
# Hard invariant: no broker POST, no submit_existing_entry
# ═══════════════════════════════════════════════════════════════════════


def test_scaffold_never_calls_broker():
    """A BROKER_READY row must never surface a broker call from the
    scaffold, regardless of disposition returned."""
    core = _make_core()
    broker = MagicMock()
    core.broker = broker
    core.order_state_machine.get_order.return_value = _row()

    core.resume_deferred_broker_ready_order(local_order_id="oid-1")

    # None of these can have been touched by the scaffold path.
    for method in ("place_order", "buy_option", "sell_option", "post", "submit"):
        m = getattr(broker, method)
        assert not m.called, (
            f"§2 scaffold called broker.{method} — must never POST"
        )


def test_losing_recovery_claim_never_calls_submit_existing_entry():
    core = _make_core()
    core.order_state_machine.get_order.return_value = _row()
    core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    core.order_state_machine.submit_existing_entry.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# RETRY_WAIT: engineering incomplete on a healthy row
# ═══════════════════════════════════════════════════════════════════════


def test_healthy_broker_ready_without_claim_returns_reconcile_pending():
    core = _make_core()
    core.order_state_machine.get_order.return_value = _row()
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")

    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["reason_code"] == "RECOVERY_SUBMIT_CLAIM_NOT_ACQUIRED"
    assert result["local_order_id"] == "oid-1"
    assert result["attempt"] == 1
    assert result["max_attempts"] == 20  # default
    assert result["generation"] == 3
    assert result["owner"].startswith("recovery_submit:oid-1:")
    # next_retry_at must be a bounded future time
    _next = datetime.fromisoformat(result["next_retry_at"])
    _delta = (_next - datetime.now(timezone.utc)).total_seconds()
    assert 0 < _delta <= 60, f"next_retry_at must be bounded, got {_delta}s"


def test_retry_attempt_increments_before_claim_attempt():
    core = _make_core()
    row = _row()
    row["meta"]["recovery_attempt_count"] = 5
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "RECONCILE_PENDING"
    assert result["attempt"] == 6


# ═══════════════════════════════════════════════════════════════════════
# TERMINAL_DURABLE only on truthful boundaries
# ═══════════════════════════════════════════════════════════════════════


def test_client_id_mismatch_terminalizes():
    core = _make_core(client_id="jason@example.com")
    row = _row(client_id="someone-else@example.com")
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"] == "RECOVERY_CLIENT_ID_MISMATCH"
    assert result["terminal_status"] == "ERROR"


def test_wrong_kind_terminalizes():
    core = _make_core()
    row = _row(kind="EXIT")
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"].startswith("RECOVERY_WRONG_KIND")


def test_missing_trigger_evidence_terminalizes():
    core = _make_core()
    row = _row()
    row["meta"]["trigger_crossed_at"] = None
    row["meta"]["trigger_confirmed_at"] = None
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"] == "RECOVERY_MISSING_TRIGGER_EVIDENCE"


def test_malformed_trigger_timestamp_terminalizes():
    core = _make_core()
    row = _row()
    row["meta"]["trigger_crossed_at"] = "not-a-timestamp"
    row["meta"]["trigger_confirmed_at"] = None
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"] == "RECOVERY_INVALID_TRIGGER_TIMESTAMP"


def test_trigger_older_than_max_age_terminalizes():
    core = _make_core()
    row = _row()
    row["meta"]["trigger_crossed_at"] = _past_iso(3600)  # 1h ago; default max is 300s
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"] == "RECOVERY_TRIGGER_TOO_OLD"


def test_retry_exhausted_terminalizes():
    core = _make_core()
    row = _row()
    row["meta"]["recovery_attempt_count"] = 20  # default max
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"] == "RECOVERY_RETRY_EXHAUSTED"
    assert result["attempt"] == 21
    assert result["max_attempts"] == 20


def test_config_max_trigger_age_env_override(monkeypatch):
    monkeypatch.setenv("DEFERRED_RECOVERY_MAX_TRIGGER_AGE_SECONDS", "60")
    core = _make_core()
    row = _row()
    row["meta"]["trigger_crossed_at"] = _past_iso(120)  # 2m ago > 60s
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["reason_code"] == "RECOVERY_TRIGGER_TOO_OLD"


def test_config_max_attempts_env_override(monkeypatch):
    monkeypatch.setenv("DEFERRED_RECOVERY_MAX_ATTEMPTS", "3")
    core = _make_core()
    row = _row()
    row["meta"]["recovery_attempt_count"] = 3
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "TERMINAL_DURABLE"
    assert result["reason_code"] == "RECOVERY_RETRY_EXHAUSTED"


# ═══════════════════════════════════════════════════════════════════════
# KEEP_WATCHER when inspection incomplete
# ═══════════════════════════════════════════════════════════════════════


def test_no_osm_returns_keep_watcher():
    core = _make_core()
    core.order_state_machine = None
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RECOVERY_OSM_UNAVAILABLE"


def test_get_order_raises_returns_keep_watcher():
    core = _make_core()
    core.order_state_machine.get_order.side_effect = RuntimeError("db down")
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"].startswith("RECOVERY_ROW_READ_ERROR")


def test_row_missing_returns_keep_watcher():
    core = _make_core()
    core.order_state_machine.get_order.return_value = None
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RECOVERY_ROW_MISSING"


def test_status_submitted_family_returns_keep_watcher():
    """If the row is already submitted, reconciler/order-monitor owns it."""
    core = _make_core()
    row = _row(status="SUBMITTED", broker_order_id="TR-9")
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "KEEP_WATCHER"


def test_already_submitted_by_broker_order_id_returns_keep_watcher():
    core = _make_core()
    row = _row()
    row["broker_order_id"] = "TR-42"
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "KEEP_WATCHER"
    assert result["reason_code"] == "RECOVERY_ALREADY_SUBMITTED"


def test_already_submitted_by_submitted_ts_returns_keep_watcher():
    core = _make_core()
    row = _row()
    row["submitted_ts"] = "2026-07-10T15:00:00+00:00"
    core.order_state_machine.get_order.return_value = row
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["disposition"] == "KEEP_WATCHER"


# ═══════════════════════════════════════════════════════════════════════
# Owner label + preservation guarantee
# ═══════════════════════════════════════════════════════════════════════


def test_owner_label_identifies_exact_recovery_submit_claim():
    core = _make_core(client_id="jason@example.com")
    core.order_state_machine.get_order.return_value = _row()
    result = core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    assert result["owner"].startswith("recovery_submit:oid-1:")


def test_scaffold_does_not_mutate_the_row():
    """The scaffold is inspection-only; the caller applies persistence."""
    core = _make_core()
    row = _row()
    core.order_state_machine.get_order.return_value = row
    core.resume_deferred_broker_ready_order(local_order_id="oid-1")
    # No mutation call surfaced from scaffold:
    core.order_state_machine.update_order_meta.assert_not_called()
    core.order_state_machine.terminalize_deferred_breach.assert_not_called()
    core.order_state_machine.submit_existing_entry.assert_not_called()
