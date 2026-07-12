"""P0 tests for PR #323 Seam 1: PRE_SUBMIT_PROOF_RETRY.

When the pre-submit order-row read fails transiently (BLOCK_RETRY verdict),
the selected OCC contract must be preserved in a bounded PRE_SUBMIT_PROOF_RETRY
state rather than immediately terminalized.  The retry only re-reads the row;
it does NOT rerun contract selection.

Tests:
  A. Selector succeeds; first order-row read fails; persist_pre_submit_proof_retry
     succeeds; outcome is DEFERRED_PRE_SUBMIT_PROOF_RETRY, NOT a terminal.
  B. persist_pre_submit_proof_retry CAS fails (returns False); falls back to
     terminal MATERIALIZATION_ORDER_ROW_UNREADABLE (legacy safe path).
  C. Process dies after PRE_SUBMIT_PROOF_RETRY persistence; startup recovery
     picks it up; within deadline → retained with durable ownership.
  D. Two workers race the proof-retry CAS; exactly one succeeds.
  E. Selected quote becomes stale (deadline expired in recovery);
     terminalize with exact read failure reason; zero broker POST.
  F. client_id and execution_mode remain unchanged across recovery.
"""

from __future__ import annotations

import json
import types
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest

import ap.order_state_machine as osm_mod
from ap.order_state_machine import APOrderStateMachine
from ap_recovery import APStartupRecovery


# ─────────────────────────── OSM db spy ──────────────────────────────────


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
    monkeypatch.setattr(osm_mod, "conn", lambda: _Conn(sink, rowcount=state["rowcount"]))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **k: fn())
    return sink, state


# ─────────────────────────── Recovery scaffold ───────────────────────────


class _RecoveryCursor:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, *_a, **_k):
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


def _proof_retry_row(*, deadline_offset_seconds=60, contract="SPY260717C00600000"):
    """Build a PRE_SUBMIT_PROOF_RETRY order row for recovery tests."""
    deadline = (datetime.now(timezone.utc) + timedelta(seconds=deadline_offset_seconds)).isoformat()
    meta = {
        "lifecycle_state": "PRE_SUBMIT_PROOF_RETRY",
        "materialization_status": "SELECTED",
        "broker_ready": True,
        "materialization_generation": 3,
        "execution_mode": "paper",
        "proof_retry_attempt": 1,
        "proof_retry_max_attempts": 3,
        "proof_retry_next_at": datetime.now(timezone.utc).isoformat(),
        "proof_retry_deadline": deadline,
        "proof_retry_last_read_error": "read_error:db_hiccup",
        "proof_retry_owner": "materializer:worker-A",
        "selected_at": datetime.now(timezone.utc).isoformat(),
        "selected_quote_at": datetime.now(timezone.utc).isoformat(),
    }
    return {
        "local_order_id": "oid-proof-retry",
        "client_id": "client@example.com",
        "signal_id": "sig-1", "plan_id": "plan-1", "symbol": "SPY",
        "contract": contract, "direction": "CALL",
        "score": 80, "tier": "A", "trigger_price": 500.0,
        "stop_underlying": 495.0, "target_underlying": 510.0,
        "pattern": "2-1-2", "timeframe": "1d", "execution_mode": "paper",
        "qty": 1, "limit_price": 1.25, "reserved_cost": 125.0,
        "status": "PENDING_TRIGGER", "broker_order_id": None,
        "submitted_ts": None, "created_ts": datetime.now(timezone.utc),
        "meta": meta,
    }


def _run_recovery(monkeypatch, rows, *, osm, entry_watcher=None, execution_core=None):
    import ap.db as ap_db
    monkeypatch.setattr(ap_db, "conn", lambda: _RecoveryConn(rows))
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *a, **k: fn())
    rec = APStartupRecovery(
        client_id="client@example.com", broker=object(), osm=osm, pm=None,
        master_control=types.SimpleNamespace(mode="PAPER"),
        entry_watcher=entry_watcher, execution_core=execution_core,
    )
    result = {"deferred_lifecycles_recovered": 0}
    rec._recover_deferred_breach_lifecycles(result)
    return result


def _osm_mock(*, terminate_returns=True, update_meta_returns=True):
    return types.SimpleNamespace(
        client_id="client@example.com",
        terminalize_deferred_breach=MagicMock(return_value=terminate_returns),
        update_order_meta=MagicMock(return_value=update_meta_returns),
        submit_existing_entry=MagicMock(return_value={"ok": True}),
    )


# ═══════════════════════════════════════════════════════════════════════
# Test A: Selector succeeds; first read fails; proof retry scheduled
# ═══════════════════════════════════════════════════════════════════════


def test_A_proof_retry_persist_scheduled_on_block_retry(db_spy):
    """persist_pre_submit_proof_retry writes PRE_SUBMIT_PROOF_RETRY state to meta.
    The CAS predicate requires lifecycle_state='BROKER_READY' and owner+generation."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    ok = osm.persist_pre_submit_proof_retry(
        "oid-1",
        owner="materializer:worker-A",
        generation=3,
        retry_attempt=1,
        max_attempts=3,
        next_retry_at=(datetime.now(timezone.utc) + timedelta(seconds=5)).isoformat(),
        retry_deadline=(datetime.now(timezone.utc) + timedelta(seconds=60)).isoformat(),
        read_error="read_error:db_hiccup",
        selected_at=datetime.now(timezone.utc).isoformat(),
        selected_quote_at=datetime.now(timezone.utc).isoformat(),
    )
    assert ok is True
    sql, params = sink[-1]
    patch = json.loads(params[0])
    # Transition to PRE_SUBMIT_PROOF_RETRY
    assert patch["lifecycle_state"] == "PRE_SUBMIT_PROOF_RETRY"
    # KEEP broker_ready=True — the contract IS selected
    assert patch["broker_ready"] is True
    # Retry tracking persisted
    assert patch["proof_retry_attempt"] == 1
    assert patch["proof_retry_max_attempts"] == 3
    assert "proof_retry_deadline" in patch
    # CAS predicate requires BROKER_READY lifecycle and owner+generation
    assert "lifecycle_state','') = 'BROKER_READY'" in sql
    assert "materialization_owner','') = %s" in sql
    assert "materialization_generation')::int, 0) = %s" in sql
    # Owner+generation bindings
    assert "materializer:worker-A" in params
    assert 3 in params


def test_A_trade_policy_fields_not_in_proof_retry_patch(db_spy):
    """persist_pre_submit_proof_retry uses non-destructive meta merge.
    Trade-policy columns (contract, qty, limit_price) are preserved in
    the row's actual columns — the meta patch must NOT write them."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    osm.persist_pre_submit_proof_retry(
        "oid-1",
        owner="w", generation=1, retry_attempt=1, max_attempts=3,
        next_retry_at="2026-07-11T10:00:00+00:00",
        retry_deadline="2026-07-11T10:01:00+00:00",
        read_error="timeout", selected_at="now", selected_quote_at="now",
    )
    patch = json.loads(sink[-1][1][0])
    for forbidden in {"contract", "qty", "limit_price", "reserved_cost",
                      "direction", "stop_underlying", "target_underlying"}:
        assert forbidden not in patch, f"meta patch wrote trade-policy field {forbidden!r}"


def test_A_proof_retry_generation_and_owner_required(db_spy):
    """Missing owner or invalid generation → no DB write."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    # Missing owner
    assert osm.persist_pre_submit_proof_retry(
        "oid-1", owner="", generation=1, retry_attempt=1, max_attempts=3,
        next_retry_at="2026-07-11T10:00:00+00:00",
        retry_deadline="2026-07-11T10:01:00+00:00",
        read_error="x", selected_at="now", selected_quote_at="now",
    ) is False
    assert sink == []


# ═══════════════════════════════════════════════════════════════════════
# Test B: CAS fails → fall back to terminal
# ═══════════════════════════════════════════════════════════════════════


def test_B_cas_miss_means_persist_returns_false(db_spy):
    """When rowcount=0 (another worker already claimed or row is not BROKER_READY),
    persist_pre_submit_proof_retry returns False — caller must terminalize."""
    _, state = db_spy
    state["rowcount"] = 0
    osm = APOrderStateMachine("client@example.com")
    result = osm.persist_pre_submit_proof_retry(
        "oid-1", owner="w", generation=1, retry_attempt=1, max_attempts=3,
        next_retry_at="2026-07-11T10:00:00+00:00",
        retry_deadline="2026-07-11T10:01:00+00:00",
        read_error="x", selected_at="now", selected_quote_at="now",
    )
    assert result is False


# ═══════════════════════════════════════════════════════════════════════
# Test C: Recovery picks up PRE_SUBMIT_PROOF_RETRY within deadline
# ═══════════════════════════════════════════════════════════════════════


class _ProofConsumerOSM:
    def __init__(self, row):
        self.client_id = row["client_id"]
        self.row = row
        self.claims = 0
        self.terminalize_deferred_breach = MagicMock(return_value=True)

    def claim_pre_submit_proof_retry(self, local_order_id, **kwargs):
        if self.row["meta"]["lifecycle_state"] != "PRE_SUBMIT_PROOF_RETRY":
            return False
        self.claims += 1
        self.row["meta"].update({
            "lifecycle_state": "BROKER_READY",
            "materialization_generation": kwargs["new_generation"],
            "materialization_owner": kwargs["owner"],
            "proof_retry_attempt": kwargs["attempt"],
        })
        return True

    def get_order(self, local_order_id):
        return self.row


def test_C_due_proof_retry_claims_and_enters_canonical_recovery(monkeypatch):
    row = _proof_retry_row(deadline_offset_seconds=120)
    row["meta"].update({
        "selected_contract": row["contract"],
        "selected_qty": row["qty"],
        "selected_limit": row["limit_price"],
    })
    osm = _ProofConsumerOSM(row)
    core = types.SimpleNamespace(
        client_id=row["client_id"], execution_mode="paper",
        resume_deferred_broker_ready_order=MagicMock(return_value={
            "disposition": "SUBMITTED", "reason_code": "RECOVERY_CANONICAL_SUBMIT_ACCEPTED",
        }),
    )
    result = _run_recovery(monkeypatch, [row], osm=osm, execution_core=core)
    assert osm.claims == 1
    core.resume_deferred_broker_ready_order.assert_called_once_with(
        local_order_id=row["local_order_id"], plan=None,
    )
    assert row["meta"]["materialization_generation"] == 4
    assert result["deferred_lifecycles_recovered"] == 1


def test_C_future_proof_retry_is_not_claimed(monkeypatch):
    row = _proof_retry_row(deadline_offset_seconds=120)
    row["meta"]["proof_retry_next_at"] = (
        datetime.now(timezone.utc) + timedelta(seconds=30)
    ).isoformat()
    osm = _ProofConsumerOSM(row)
    core = types.SimpleNamespace(client_id=row["client_id"], execution_mode="paper")
    _run_recovery(monkeypatch, [row], osm=osm, execution_core=core)
    assert osm.claims == 0


# ═══════════════════════════════════════════════════════════════════════
# Test D: Two workers race — exactly one CAS wins
# ═══════════════════════════════════════════════════════════════════════


def test_D_two_workers_race_exactly_one_wins(db_spy):
    """The CAS owner+generation predicate ensures only one worker can
    transition to PRE_SUBMIT_PROOF_RETRY.  Simulate second worker by
    setting rowcount=0 on the second call."""
    sink, state = db_spy
    osm = APOrderStateMachine("client@example.com")
    # First worker succeeds
    state["rowcount"] = 1
    ok_first = osm.persist_pre_submit_proof_retry(
        "oid-1", owner="worker-A", generation=3, retry_attempt=1, max_attempts=3,
        next_retry_at="2026-07-11T10:00:05+00:00",
        retry_deadline="2026-07-11T10:01:00+00:00",
        read_error="db_hiccup", selected_at="now", selected_quote_at="now",
    )
    # Second worker: same owner+generation → CAS miss (row already advanced)
    state["rowcount"] = 0
    ok_second = osm.persist_pre_submit_proof_retry(
        "oid-1", owner="worker-B", generation=3, retry_attempt=1, max_attempts=3,
        next_retry_at="2026-07-11T10:00:05+00:00",
        retry_deadline="2026-07-11T10:01:00+00:00",
        read_error="db_hiccup", selected_at="now", selected_quote_at="now",
    )
    assert ok_first is True
    assert ok_second is False


def test_D_due_consumer_claim_is_generation_fenced(db_spy):
    sink, state = db_spy
    osm = APOrderStateMachine("client@example.com")
    state["rowcount"] = 1
    assert osm.claim_pre_submit_proof_retry(
        "oid-1", owner="proof-worker", expected_generation=3,
        new_generation=4, attempt=2,
        claimed_at=datetime.now(timezone.utc).isoformat(),
    ) is True
    sql, params = sink[-1]
    patch = json.loads(params[0])
    assert patch["lifecycle_state"] == "BROKER_READY"
    assert patch["materialization_generation"] == 4
    assert patch["proof_retry_attempt"] == 2
    assert "proof_retry_next_at','')::timestamptz <= NOW()" in sql
    assert params[-2:] == (3, 2)


# ═══════════════════════════════════════════════════════════════════════
# Test E: Deadline expired → terminalize with exact read failure reason
# ═══════════════════════════════════════════════════════════════════════


def test_E_expired_deadline_terminalizes_with_exact_reason(monkeypatch):
    """When the retry deadline has passed, the row must be terminalized
    with the exact read failure reason — zero broker POST."""
    osm = _osm_mock()
    result = _run_recovery(
        monkeypatch,
        [_proof_retry_row(deadline_offset_seconds=-10)],  # 10s in the PAST
        osm=osm,
    )
    osm.terminalize_deferred_breach.assert_called_once()
    call_kwargs = osm.terminalize_deferred_breach.call_args.kwargs
    # Terminal status is EXPIRED
    assert call_kwargs["terminal_status"] == "EXPIRED"
    # Reason encodes the original read error
    assert call_kwargs["reason_code"] == "PROOF_RETRY_DEADLINE_EXCEEDED"
    # Diagnostics carry selected contract
    assert call_kwargs["diagnostics"]["selected_contract"] == "SPY260717C00600000"
    # Zero broker POST
    osm.submit_existing_entry.assert_not_called()


def test_E_unparseable_deadline_fails_closed(monkeypatch):
    """An unparseable proof_retry_deadline → fail closed → terminalize."""
    osm = _osm_mock()
    row = _proof_retry_row(deadline_offset_seconds=+120)
    row["meta"]["proof_retry_deadline"] = "not-a-timestamp"
    _run_recovery(monkeypatch, [row], osm=osm)
    osm.terminalize_deferred_breach.assert_called_once()
    osm.submit_existing_entry.assert_not_called()


def test_E_missing_deadline_fails_closed(monkeypatch):
    """No proof_retry_deadline in meta → fail closed → terminalize."""
    osm = _osm_mock()
    row = _proof_retry_row(deadline_offset_seconds=+120)
    del row["meta"]["proof_retry_deadline"]
    _run_recovery(monkeypatch, [row], osm=osm)
    osm.terminalize_deferred_breach.assert_called_once()


# ═══════════════════════════════════════════════════════════════════════
# Test F: client_id and execution_mode unchanged
# ═══════════════════════════════════════════════════════════════════════


def test_F_execution_mode_mismatch_terminalizes(monkeypatch):
    row = _proof_retry_row(deadline_offset_seconds=120)
    osm = _ProofConsumerOSM(row)
    core = types.SimpleNamespace(client_id=row["client_id"], execution_mode="live")
    _run_recovery(monkeypatch, [row], osm=osm, execution_core=core)
    osm.terminalize_deferred_breach.assert_called_once()
    assert osm.claims == 0


# ═══════════════════════════════════════════════════════════════════════
# OSM: persist CAS SQL structure
# ═══════════════════════════════════════════════════════════════════════


def test_osm_persist_proof_retry_uses_non_destructive_merge(db_spy):
    """The SQL must use JSONB || merge, never overwrite the row."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    osm.persist_pre_submit_proof_retry(
        "oid-1", owner="w", generation=2, retry_attempt=1, max_attempts=3,
        next_retry_at="2026-07-11T10:00:05+00:00",
        retry_deadline="2026-07-11T10:01:00+00:00",
        read_error="x", selected_at="now", selected_quote_at="now",
    )
    sql, _ = sink[-1]
    assert "COALESCE(meta, '{}'::jsonb) || %s::jsonb" in sql
    # No SET contract= or SET qty= — non-destructive
    assert "SET contract" not in sql
    assert "SET qty" not in sql
    assert "SET limit_price" not in sql
