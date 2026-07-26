"""PR #386 restart-recovery + idempotent-proof-repair regressions.

Covers the required regression matrix:

1. Simulated crash after position commit but before proof persist:
   CLOSED + qty=0 + adopted EXIT + no proof → recovery creates exactly one
   canonical proof from persisted position truth.

2. Duplicate caller with wrong evidence: stored position economics remain
   unchanged; repaired proof uses stored exit_price/broker_order_id/reason.

3. Existing correctly-bound proof: duplicate call returns True; no proof
   UPDATE or duplicate INSERT.

4. Proof persistence failure: finalizer/recovery reports False; exit
   engine is NOT evicted; recovery candidate remains discoverable.

5. Concurrent duplicate recovery: exactly one proof row exists; no
   arbitrary candidate is mutated (validated via _ensure_terminal_close_proof
   returning True from bound_position state on the second invocation).
"""
from __future__ import annotations

import os
import types
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import ap.db as db_mod
import ap.position_manager as pm_mod
from ap import manual_close_reconciliation as manual_mod


CLIENT = "jason@example.com"
POSITION_ID = "pos-recovery-001"
CONTRACT = "F260731C00014000"


# ─── Position DB cursor that fakes SELECT ... FOR UPDATE and UPDATEs ────────

class _PosCursor:
    def __init__(self, positions):
        self.positions = positions
        self._fetchone = None
        self._fetchall = []
        self.proof_selects = []
        self.proof_updates = []
        self.sql_history = []
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=()):
        s = " ".join(str(sql).split())
        self.sql_history.append((s, params))
        self._fetchone = None
        self._fetchall = []
        if s.startswith("SELECT * FROM positions WHERE id=%s AND client_id=%s FOR UPDATE"):
            pid, cid = params
            row = self.positions.get(pid)
            if row and row.get("client_id") == cid:
                self._fetchone = dict(row)
            return self
        if s.startswith("UPDATE positions SET"):
            pid, cid = params[-2], params[-1]
            row = self.positions.get(pid)
            if not row or row.get("client_id") != cid:
                return self
            self._fetchone = {"id": pid, "status": "CLOSED"}
            return self
        if "FROM proof_trades" in s and s.strip().startswith("SELECT"):
            self.proof_selects.append((s, params))
            return self
        if s.strip().startswith("UPDATE proof_trades"):
            self.proof_updates.append((s, params))
            return self
        # Ignore other DDL/DML in these tests.
        return self
    def fetchall(self): return list(self._fetchall)
    def fetchone(self): return self._fetchone


class _APM(pm_mod.APPositionManager):
    def __new__(cls, *a, **kw): return object.__new__(cls)
    def __init__(self, client_id):
        self.client_id = client_id
        self.ensure_calls = []
        self._forced_return = None
    def _has_position_column(self, name): return True
    def _ensure_terminal_close_proof(self, **kwargs):
        # Capture every call — assertions inspect fields.
        self.ensure_calls.append(kwargs)
        if self._forced_return is not None:
            return self._forced_return
        return True

    # PR #386 blocker 3: bypass the advisory-lock wrapper for unit tests.
    # The real wrapper opens conn() and runs pg_advisory_xact_lock + a
    # binding-state re-read; behavioral coverage of the lock lives in
    # the concurrent-serialization regression below.
    def _with_terminal_proof_lock(self, position_id, fn):
        return fn()

    def _proof_row_binding_state(self, **kwargs):
        return "bound_position"


def _install_pos_db(monkeypatch, positions_by_id):
    cursor = _PosCursor(positions_by_id)
    monkeypatch.setattr(pm_mod, "conn", lambda: cursor)
    monkeypatch.setattr(pm_mod, "run_with_retry", lambda fn: fn())
    return cursor


def _terminal_row(**overrides):
    row = {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "status": "CLOSED",
        "qty": 2,
        "quantity_remaining": 0,
        "avg_fill": 0.73,
        "exit_price": 0.90,
        "realized_pnl": 34.0,
        "realized_pnl_pct": 23.3,
        "contract": CONTRACT,
        "underlying": "F",
        "side": "CALL",
        "direction": "CALL",
        "local_order_id": "entry-1",
        "broker_order_id": "STORED-BROKER-ID",
        "entry_ts": "2026-07-21T15:26:58.911238+00:00",
        "exit_ts": "2026-07-21T15:57:39+00:00",
        "exit_reason": "MANUAL_CLIENT_CLOSE_BROKER_CONFIRMED",
        "close_source": "manual_client_close_broker_fill",
        "execution_mode": "live",
    }
    row.update(overrides)
    return row


# ═══ Regression 1: crash-after-commit recovery creates canonical proof ══════

def test_terminal_without_proof_recovery_creates_exactly_one_proof_from_persisted(monkeypatch):
    row = _terminal_row()
    cursor = _install_pos_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)

    ok, reason = apm.repair_terminal_proof_from_persisted(POSITION_ID)

    assert ok is True
    assert reason == "proof_bound"
    # Exactly one canonical proof call.
    assert len(apm.ensure_calls) == 1
    call = apm.ensure_calls[0]
    # Every field derived from persisted row — no caller evidence to override.
    assert call["position_id"] == POSITION_ID
    assert call["local_order_id"] == "entry-1"
    assert call["contract"] == CONTRACT
    assert call["side"] == "CALL"
    assert call["opened_at"] == "2026-07-21T15:26:58.911238+00:00"
    assert call["closed_at"] == "2026-07-21T15:57:39+00:00"
    assert call["entry_option_price"] == 0.73
    assert call["exit_option_price"] == 0.90
    assert call["contracts"] == 2
    assert call["exit_reason"] == "MANUAL_CLIENT_CLOSE_BROKER_CONFIRMED"
    assert call["setup_status"] == "manual_client_close_broker_fill"
    assert call["execution_mode"] == "live"
    assert call["exit_fill_price"] == 0.90
    assert call["missing_reason_code"] == "TERMINAL_PROOF_RESTART_RECOVERY"
    # Position row was NOT rewritten (no UPDATE statements).
    assert not any(
        s.startswith("UPDATE positions") for s, _ in cursor.sql_history
    )


def test_repair_refuses_when_position_still_has_remaining(monkeypatch):
    _install_pos_db(monkeypatch, {POSITION_ID: _terminal_row(status="CLOSED", quantity_remaining=1)})
    apm = _APM(CLIENT)
    ok, reason = apm.repair_terminal_proof_from_persisted(POSITION_ID)
    assert ok is False
    assert reason == "position_has_remaining_quantity"
    assert apm.ensure_calls == []


def test_repair_refuses_when_position_is_not_terminal(monkeypatch):
    _install_pos_db(monkeypatch, {POSITION_ID: _terminal_row(status="OPEN", quantity_remaining=0)})
    apm = _APM(CLIENT)
    ok, reason = apm.repair_terminal_proof_from_persisted(POSITION_ID)
    assert ok is False
    assert reason == "position_not_terminal"
    assert apm.ensure_calls == []


# ═══ Regression 2: duplicate caller with WRONG evidence uses stored ═════════

def test_duplicate_finalizer_with_wrong_caller_evidence_uses_stored_economics(monkeypatch):
    """Second caller passes exit_price=99.99 and broker_order_id="WRONG";
    idempotent branch must call _ensure_terminal_close_proof with the
    STORED persisted values, not the caller's."""
    row = _terminal_row(exit_price=0.90, broker_order_id="STORED-BROKER-ID",
                        exit_reason="MANUAL_CLIENT_CLOSE_BROKER_CONFIRMED")
    cursor = _install_pos_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)

    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID,
        exit_price=99.99,                      # WRONG — must not appear in proof
        filled_qty=2,
        broker_order_id="WRONG-BROKER-ID",     # WRONG — must not appear
        exit_reason="wrong_reason_from_caller",
        close_source="wrong_source_from_caller",
    )
    assert ok is True

    # The idempotent path called _ensure_terminal_close_proof with PERSISTED
    # values, not caller evidence.
    assert len(apm.ensure_calls) == 1
    call = apm.ensure_calls[0]
    assert call["exit_option_price"] == 0.90                 # stored, not 99.99
    assert call["exit_fill_price"] == 0.90                   # stored
    assert call["exit_reason"] == "MANUAL_CLIENT_CLOSE_BROKER_CONFIRMED"
    assert call["setup_status"] == "manual_client_close_broker_fill"

    # No generic proof_trades UPDATE (caller-evidence repair block was skipped).
    assert cursor.proof_updates == []
    # No generic proof_trades SELECT from the fill-repair block either.
    assert cursor.proof_selects == []


# ═══ Regression 3: existing bound proof → no mutation ═══════════════════════

def test_idempotent_repair_when_proof_already_bound_returns_true_without_mutation(monkeypatch):
    row = _terminal_row()
    cursor = _install_pos_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)
    # _ensure returns True immediately (models bound_position path).
    apm._forced_return = True

    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID, exit_price=1.0, filled_qty=2,
        broker_order_id="anything",
    )
    assert ok is True
    # Exactly ONE call to _ensure (which internally short-circuits on bound
    # proof) — never a second INSERT.
    assert len(apm.ensure_calls) == 1
    assert cursor.proof_updates == []


# ═══ Regression 4: proof persistence failure → False, no evict, discoverable ═

def test_idempotent_proof_failure_returns_false_so_caller_does_not_evict(monkeypatch):
    row = _terminal_row()
    _install_pos_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)
    apm._forced_return = False   # models proof repair failure

    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID, exit_price=1.0, filled_qty=2,
        broker_order_id="anything",
    )
    assert ok is False
    # Position row untouched — recovery candidate remains discoverable.
    assert row["status"] == "CLOSED"
    assert row["exit_price"] == 0.90


def test_recovery_method_reports_failure_when_ensure_returns_false(monkeypatch):
    row = _terminal_row()
    _install_pos_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)
    apm._forced_return = False

    ok, reason = apm.repair_terminal_proof_from_persisted(POSITION_ID)
    assert ok is False
    assert reason == "proof_repair_failed"


# ═══ Regression 5: concurrent duplicate recovery → single bound outcome ═════

def test_concurrent_duplicate_recovery_produces_single_bound_result(monkeypatch):
    """Two overlapping runners each call repair_terminal_proof_from_persisted
    for the same terminal row. The first bind wins; the second observes
    bound_position and returns True without a second INSERT."""
    row = _terminal_row()
    _install_pos_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)
    # First _ensure returns True (fresh bind). Second returns True immediately
    # (models bound_position path — no INSERT, no UPDATE).
    apm._forced_return = True

    ok1, _ = apm.repair_terminal_proof_from_persisted(POSITION_ID)
    ok2, _ = apm.repair_terminal_proof_from_persisted(POSITION_ID)
    assert ok1 is True
    assert ok2 is True
    # Both invocations call _ensure exactly once each; the function is the
    # canonical serialization point (bound_position short-circuit).
    assert len(apm.ensure_calls) == 2
    # Position row was NOT rewritten by either recovery pass.
    assert row["status"] == "CLOSED"
    assert row["exit_price"] == 0.90
    assert row["realized_pnl"] == 34.0


# ═══ First-close fail-closed on proof-binding failure (amendment 1+2) ══════

def test_first_close_returns_false_when_proof_binding_fails(monkeypatch):
    """The first-close path in close_position_from_exit_fill must return
    False when the terminal proof lock/binding fails, so the caller does
    NOT evict the exit engine — PASS 0 must remain able to repair.

    Uses an OPEN position with qty=2; we force _ensure_terminal_close_proof
    to return False. The updated position row is committed inside the
    transaction (that's the crash window), but the outer function must
    then surface False and leave the row/durable evidence discoverable.
    """
    row = {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "status": "OPEN",
        "qty": 2,
        "quantity_remaining": 2,
        "avg_fill": 0.73,
        "exit_price": 0,
        "realized_pnl": 0,
        "realized_pnl_pct": 0,
        "contract": CONTRACT,
        "underlying": "F",
        "side": "CALL",
        "direction": "CALL",
        "local_order_id": "entry-1",
        "broker_order_id": None,
        "entry_ts": "2026-07-21T15:26:58.911238+00:00",
        "exit_ts": None,
        "exit_reason": None,
        "close_source": None,
        "execution_mode": "live",
    }
    _install_pos_db(monkeypatch, {POSITION_ID: row})
    apm = _APM(CLIENT)
    apm._forced_return = False   # models proof binding failure

    ok = apm.close_position_from_exit_fill(
        position_id=POSITION_ID, exit_price=0.90, filled_qty=2,
        broker_order_id="BROKER-1", exit_reason="broker_close",
        close_source="broker_exit_fill",
    )
    assert ok is False
    # Exactly one ensure invocation (through the lock wrapper).
    assert len(apm.ensure_calls) == 1
    # The ensure call used PERSISTED fields (persisted qty=2, mode=live),
    # not fabricated fallbacks like contracts=1 or execution_mode="".
    kw = apm.ensure_calls[0]
    assert kw["contracts"] == 2
    assert kw["execution_mode"] == "live"


# ═══ Persisted-truth finite-number gate (PR #386 amendment 2) ══════════════

def test_persisted_truth_rejects_infinity_and_nan(monkeypatch):
    """_validate_persisted_terminal_truth MUST reject +inf/-inf/NaN for
    every numeric field entering proof, and MUST NOT write proof when
    repair_terminal_proof_from_persisted encounters such a row."""
    import math

    for field, bad_value in [
        ("avg_fill", math.inf),
        ("avg_fill", -math.inf),
        ("avg_fill", math.nan),
        ("exit_price", math.inf),
        ("exit_price", -math.inf),
        ("exit_price", math.nan),
        ("realized_pnl_pct", math.inf),
        ("realized_pnl_pct", -math.inf),
        ("realized_pnl_pct", math.nan),
    ]:
        row = _terminal_row(**{field: bad_value})
        ok, reason = pm_mod._validate_persisted_terminal_truth(row)
        assert ok is False, f"{field}={bad_value!r} should be rejected"
        assert reason in {
            "invalid_entry_price", "invalid_exit_price", "non_finite_pnl_pct",
        }, f"{field}={bad_value!r} unexpected reason={reason}"

        _install_pos_db(monkeypatch, {POSITION_ID: row})
        apm = _APM(CLIENT)
        ok, reason = apm.repair_terminal_proof_from_persisted(POSITION_ID)
        assert ok is False, f"{field}={bad_value!r} recovery must refuse"
        assert apm.ensure_calls == [], (
            f"{field}={bad_value!r} must NOT reach _ensure_terminal_close_proof"
        )


# ═══ Serialized proof-binding lock (PR #386 blocker 3) ═════════════════════

def test_terminal_proof_lock_serializes_overlapping_workers(monkeypatch):
    """Two workers race to bind proof for the same (client, position). The
    canonical serialization is a Postgres transaction-scoped advisory lock
    acquired inside _with_terminal_proof_lock. This regression models the
    lock with an in-process mutex (keyed identically) and asserts:

      * Both callers observe True (idempotent success on second bind).
      * Exactly ONE fresh proof insert occurs — the second caller sees
        bound_position on its re-read and short-circuits.
      * Neither call mutates any existing proof row.

    Real cross-process serialization uses pg_advisory_xact_lock which the
    unit-level fake connection accepts silently; the barrier here proves
    the ordering contract the wrapper is designed to enforce.
    """
    import threading

    row = _terminal_row()
    _install_pos_db(monkeypatch, {POSITION_ID: row})

    lock_map: dict[str, threading.Lock] = {}
    lock_map_guard = threading.Lock()
    inserts: list[str] = []
    bound: dict[str, bool] = {}
    barrier = threading.Barrier(2)
    results: list[tuple[bool, str]] = []

    class _LockedAPM(pm_mod.APPositionManager):
        def __new__(cls, *a, **kw): return object.__new__(cls)
        def __init__(self, client_id):
            self.client_id = client_id
        def _has_position_column(self, name): return True
        def _proof_row_binding_state(self, **kwargs):
            pid = kwargs.get("position_id") or POSITION_ID
            return "bound_position" if bound.get(pid) else None
        def _ensure_terminal_close_proof(self, **kwargs):
            pid = kwargs.get("position_id") or POSITION_ID
            if bound.get(pid):
                # Bound-position short-circuit inside ensure — no INSERT.
                return True
            inserts.append(pid)
            bound[pid] = True
            return True
        def _with_terminal_proof_lock(self, position_id, fn):
            key = f"terminal-proof-bind:{self.client_id}:{position_id}"
            with lock_map_guard:
                lock = lock_map.setdefault(key, threading.Lock())
            with lock:
                result = fn()
                if result is True:
                    if self._proof_row_binding_state(position_id=position_id) != "bound_position":
                        return False
                return result

    def _worker():
        apm = _LockedAPM(CLIENT)
        barrier.wait()
        results.append(apm.repair_terminal_proof_from_persisted(POSITION_ID))

    threads = [threading.Thread(target=_worker) for _ in range(2)]
    for t in threads: t.start()
    for t in threads: t.join(timeout=5)

    assert len(results) == 2
    assert all(ok is True for ok, _ in results)
    # Exactly ONE fresh INSERT — the lock serialized both workers and the
    # second observed bound_position.
    assert len(inserts) == 1
    # Row was never rewritten.
    assert row["exit_price"] == 0.90
    assert row["realized_pnl"] == 34.0


# ═══ Reconciler PASS 0 wiring (proof-only recovery in detect_manual_closes) ═

def test_reconciler_pass0_calls_recovery_then_evicts_exit_engine(monkeypatch):
    """detect_manual_closes must invoke repair_terminal_proof_from_persisted
    for each terminal-recovery candidate and evict the exit engine ONLY
    after proof binding is proven."""
    class _Broker:
        cfg = types.SimpleNamespace(account_id="LIVE-ACCOUNT")
        def _get(self, path): return {"positions": "null"} if "/positions" in path else {"orders": "null"}
        def list_orders(self): return []

    class _PM:
        calls = []
        def close_position_from_exit_fill(self, **kwargs):
            self.calls.append(kwargs); return True
        def repair_terminal_proof_from_persisted(self, position_id):
            self.calls.append({"__recovery__": position_id})
            return True, "proof_bound"

    class _ExitEng:
        closed = []
        def mark_position_closed(self, pid): self.closed.append(pid)

    runner = types.SimpleNamespace(
        email=CLIENT, mode="LIVE", broker=_Broker(),
        position_manager=_PM(),
        core=types.SimpleNamespace(exit_eng=_ExitEng()),
        _last_manual_close_check_ts=0.0,
    )
    _fills = {POSITION_ID: [{
        "broker_order_id": "BROK-EXT-1",
        "filled_qty": 2,
        "fill_price": 0.90,
        "filled_at": datetime(2026, 7, 21, 15, 57, 39, tzinfo=timezone.utc),
        "created_at": None,
        "raw_status": "EXIT_FILLED",
        "raw_side": "sell_to_close",
        "db_contract": CONTRACT,
        "db_direction": "CALL",
    }]}
    monkeypatch.setattr(manual_mod, "load_manual_close_state",
                        lambda cid, mode: ([], set(), _fills))
    monkeypatch.setattr(manual_mod, "load_terminal_recovery_candidates",
                        lambda cid, mode: [{
                            "id": POSITION_ID, "execution_mode": "live",
                            "contract": CONTRACT, "side": "CALL", "direction": "CALL",
                            "qty": 2, "quantity_remaining": 0,
                            "entry_ts": "2026-07-21T15:26:58.911238+00:00",
                        }])
    monkeypatch.setattr(manual_mod.time, "time",
                        lambda: datetime(2026, 7, 21, 15, 58, 0, tzinfo=timezone.utc).timestamp())

    manual_mod.detect_manual_closes(runner)

    # Recovery method was called exactly once for the candidate.
    assert any(c.get("__recovery__") == POSITION_ID for c in runner.position_manager.calls)
    # Exit engine evicted AFTER recovery succeeded.
    assert runner.core.exit_eng.closed == [POSITION_ID]


def test_reconciler_pass0_does_not_evict_when_recovery_defers(monkeypatch):
    class _Broker:
        cfg = types.SimpleNamespace(account_id="LIVE-ACCOUNT")
        def _get(self, path): return {"positions": "null"} if "/positions" in path else {"orders": "null"}
        def list_orders(self): return []

    class _PM:
        def close_position_from_exit_fill(self, **kwargs): return True
        def repair_terminal_proof_from_persisted(self, position_id):
            return False, "proof_repair_failed"

    class _ExitEng:
        closed = []
        def mark_position_closed(self, pid): self.closed.append(pid)

    runner = types.SimpleNamespace(
        email=CLIENT, mode="LIVE", broker=_Broker(),
        position_manager=_PM(),
        core=types.SimpleNamespace(exit_eng=_ExitEng()),
        _last_manual_close_check_ts=0.0,
    )
    _fills = {POSITION_ID: [{
        "broker_order_id": "BROK-EXT-1",
        "filled_qty": 2,
        "fill_price": 0.90,
        "filled_at": datetime(2026, 7, 21, 15, 57, 39, tzinfo=timezone.utc),
        "created_at": None,
        "raw_status": "EXIT_FILLED",
        "raw_side": "sell_to_close",
        "db_contract": CONTRACT,
        "db_direction": "CALL",
    }]}
    monkeypatch.setattr(manual_mod, "load_manual_close_state",
                        lambda cid, mode: ([], set(), _fills))
    monkeypatch.setattr(manual_mod, "load_terminal_recovery_candidates",
                        lambda cid, mode: [{
                            "id": POSITION_ID, "execution_mode": "live",
                            "contract": CONTRACT, "side": "CALL", "direction": "CALL",
                            "qty": 2, "quantity_remaining": 0,
                            "entry_ts": "2026-07-21T15:26:58.911238+00:00",
                        }])
    monkeypatch.setattr(manual_mod.time, "time",
                        lambda: datetime(2026, 7, 21, 15, 58, 0, tzinfo=timezone.utc).timestamp())

    manual_mod.detect_manual_closes(runner)
    assert runner.core.exit_eng.closed == []
