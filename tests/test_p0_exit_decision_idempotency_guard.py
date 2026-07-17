from __future__ import annotations

import importlib
import threading
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

import ap.exit_decision_idempotency_guard as guard
import ap.trade_lifecycle_guards as lifecycle_guards
from ap.db import conn

_FAKE_CLAIMS_STORE: dict[str, dict] | None = None


def _reset_guard_caches() -> None:
    with guard._LEDGER_LOCK:
        guard._LEDGER_LAST_WRITTEN.clear()
    with guard._PRECHECK_LOCK:
        guard._PRECHECK_LAST_RUN.clear()


def _pos(**overrides):
    values = {
        "position_id": "position-1",
        "client_id": "client@example.com",
        "ticker": "SPY",
        "option_symbol": "SPY260716P00751000",
        "quantity_remaining": 3,
        "closed": False,
        "exit_in_flight": False,
        "pending_exit_action": "",
        "pending_exit_reason": "",
        "pending_exit_qty": 0,
        "pending_exit_local_order_id": "",
        "pending_exit_broker_order_id": "",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _decision(**overrides):
    values = {
        "action": "SCALE_OUT",
        "quantity": 1,
        "reason_code": "TP_SCALE_OUT",
        "reason": "SCALE_1 (+15%)",
        "should_act": True,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


class _FakeEngine:
    def __init__(self, positions, *, osm=None):
        self._lock = threading.RLock()
        self._positions = list(positions)
        self._positions_by_id = {
            str(position.position_id): position
            for position in positions
            if getattr(position, "position_id", "")
        }
        self.order_state_machine = osm
        self.osm = None

    def active_positions(self):
        with self._lock:
            return [
                position
                for position in self._positions
                if not position.closed and int(position.quantity_remaining or 0) > 0
            ]


class _FakeOSM:
    def __init__(self, active_order=None):
        self.active_order = active_order

    def get_active_exit_order(self, position_id):
        return self.active_order


class _FakeClaimConnection:
    def __init__(self, store: dict[str, dict]):
        self.store = store
        self._rows = []
        self._row = None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def execute(self, sql, params=()):
        sql = " ".join(str(sql).split())
        if sql.startswith("INSERT INTO exit_decision_generation_claims"):
            (
                generation_key,
                client_id,
                position_id,
                remaining_qty,
                exit_generation,
                decision_action,
                decision_reason_code,
                inserted_state,
                update_state,
                released_state,
            ) = params
            existing = self.store.get(generation_key)
            if existing is None or existing.get("claim_state") == released_state:
                row = {
                    "generation_key": generation_key,
                    "client_id": client_id,
                    "position_id": position_id,
                    "remaining_qty": remaining_qty,
                    "exit_generation": exit_generation,
                    "decision_action": decision_action,
                    "decision_reason_code": decision_reason_code,
                    "claim_state": update_state if existing else inserted_state,
                    "local_order_id": None,
                    "broker_order_id": None,
                    "last_error": None,
                    "claimed_at": None,
                    "released_at": None,
                }
                self.store[generation_key] = row
                self._row = dict(row)
            else:
                self._row = None
        elif sql.startswith("SELECT generation_key, client_id, position_id, remaining_qty"):
            generation_key = params[0]
            row = self.store.get(generation_key)
            self._row = dict(row) if row else None
        elif sql.startswith("UPDATE exit_decision_generation_claims SET claim_state="):
            (
                claim_state,
                released_state_marker,
                release_state_target,
                local_order_id_check,
                local_order_id,
                broker_order_id_check,
                broker_order_id,
                error_check,
                error_text,
                generation_key,
            ) = params
            row = self.store[generation_key]
            row["claim_state"] = claim_state
            if released_state_marker == release_state_target == guard._CLAIM_STATE_RELEASED_NO_SUBMIT:
                row["released_at"] = "released"
            if local_order_id_check:
                row["local_order_id"] = local_order_id
            if broker_order_id_check:
                row["broker_order_id"] = broker_order_id
            row["last_error"] = error_text if error_check else None
            self._row = None
        elif sql.startswith("SELECT generation_key, claim_state, local_order_id, broker_order_id, last_error"):
            self._rows = [
                {
                    "generation_key": key,
                    "claim_state": value.get("claim_state"),
                    "local_order_id": value.get("local_order_id"),
                    "broker_order_id": value.get("broker_order_id"),
                    "last_error": value.get("last_error"),
                }
                for key, value in sorted(self.store.items())
            ]
        elif sql.startswith("TRUNCATE TABLE exit_decision_generation_claims"):
            self.store.clear()
        return self

    def fetchone(self):
        return self._row

    def fetchall(self):
        return list(self._rows)


@pytest.fixture
def generation_claims_table(monkeypatch):
    global _FAKE_CLAIMS_STORE
    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "20260717_exit_decision_generation_claims.sql"
    ).read_text()
    try:
        with conn() as c:
            c.execute(migration)
            c.execute("TRUNCATE TABLE exit_decision_generation_claims")
        _FAKE_CLAIMS_STORE = None
        yield
        with conn() as c:
            c.execute("TRUNCATE TABLE exit_decision_generation_claims")
    except Exception:
        store: dict[str, dict] = {}
        _FAKE_CLAIMS_STORE = store
        monkeypatch.setattr(guard, "conn", lambda: _FakeClaimConnection(store))
        monkeypatch.setattr(guard, "run_with_retry", lambda fn, **kwargs: fn())
        yield
        store.clear()
        _FAKE_CLAIMS_STORE = None


def _claim_rows():
    if _FAKE_CLAIMS_STORE is not None:
        return [
            {
                "generation_key": key,
                "claim_state": value.get("claim_state"),
                "local_order_id": value.get("local_order_id"),
                "broker_order_id": value.get("broker_order_id"),
                "last_error": value.get("last_error"),
            }
            for key, value in sorted(_FAKE_CLAIMS_STORE.items())
        ]
    with conn() as c:
        rows = c.execute(
            "SELECT generation_key, claim_state, local_order_id, broker_order_id, last_error "
            "FROM exit_decision_generation_claims ORDER BY generation_key"
        ).fetchall()
    return [dict(row) for row in rows]


def _identity_from_result(result):
    if not isinstance(result, dict):
        return {"accepted": True, "local_order_id": "", "broker_order_id": "", "raw_status": ""}
    accepted = result.get("accepted")
    if accepted is None:
        accepted = result.get("ok", True)
    return {
        "accepted": bool(accepted),
        "local_order_id": str(result.get("local_order_id") or result.get("exit_local_order_id") or ""),
        "broker_order_id": str(result.get("broker_order_id") or result.get("order_id") or result.get("id") or ""),
        "raw_status": str(result.get("status") or result.get("raw_status") or ""),
    }


def _make_submit_engine(pos, *, callback, active_order=None):
    engine = SimpleNamespace(
        _lock=threading.RLock(),
        client_id=pos.client_id,
        order_state_machine=_FakeOSM(active_order=active_order),
        osm=None,
        on_exit=callback,
        on_scale=callback,
    )
    engine._extract_exit_order_identity = _identity_from_result
    return engine


def _invoke_submit_callback(engine, pos, decision):
    action = str(getattr(decision, "action", "") or "").upper()
    if action == "SCALE_OUT":
        return bool(engine.on_scale(pos, decision))
    return bool(engine.on_exit(pos, decision))


def test_active_exit_statuses_block_early_re_evaluation() -> None:
    for status in ("EXIT_REQUESTED", "EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL"):
        assert guard.active_exit_order_blocks({"status": status}) is True
    assert guard.active_exit_order_blocks({"status": "EXIT_FILLED"}) is False
    assert guard.active_exit_order_blocks(None) is False


def test_ledger_records_one_decision_per_fingerprint_window() -> None:
    _reset_guard_caches()
    pos = _pos()
    decision = _decision()
    assert guard.should_write_ledger(pos, decision, now_monotonic=100.0) is True
    assert guard.should_write_ledger(pos, decision, now_monotonic=101.0) is False
    assert guard.should_write_ledger(pos, decision, now_monotonic=100.0 + guard._ACTION_LEDGER_TTL + 0.01) is True


def test_durable_generation_key_uses_terminal_orders_and_remaining_qty(monkeypatch) -> None:
    class Cursor:
        def execute(self, sql, params):
            assert "COUNT(DISTINCT local_order_id)" in sql
            assert params[0:2] == ("client@example.com", "position-1")
            return self

        def fetchone(self):
            return {"terminal_exit_count": 2}

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(guard, "conn", lambda: Cursor())
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())

    assert guard._durable_exit_generation(_pos(quantity_remaining=3), "client@example.com") == (
        "client@example.com|position-1|3|3",
        3,
    )


def test_durable_generation_claim_is_atomic(monkeypatch) -> None:
    captured = {}

    class Cursor:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return self

        def fetchone(self):
            return {
                "generation_key": "client|position|3|1",
                "client_id": "client",
                "position_id": "position",
                "remaining_qty": 3,
                "exit_generation": 1,
                "decision_action": "SCALE_OUT",
                "decision_reason_code": "TP_SCALE_OUT",
                "claim_state": guard._CLAIM_STATE_CLAIMED,
                "local_order_id": None,
                "broker_order_id": None,
                "last_error": None,
                "claimed_at": None,
                "released_at": None,
            }

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return False

    monkeypatch.setattr(guard, "conn", lambda: Cursor())
    monkeypatch.setattr(guard, "run_with_retry", lambda fn: fn())

    claimed = guard._claim_durable_decision_generation(
        generation_key="client|position|3|1",
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
    )
    assert claimed["claimed"] is True
    assert "claim_state" in captured["sql"]
    assert "WHERE exit_decision_generation_claims.claim_state=%s" in captured["sql"]


def test_ledger_wrapper_is_independent_of_durable_submit_claim(monkeypatch) -> None:
    _reset_guard_caches()
    calls = []
    wrapped = guard.wrap_ledger(lambda pos, decision, client_id="": calls.append((client_id, decision.action)) or "written")
    assert wrapped(_pos(), _decision(), client_id="client@example.com") == "written"
    assert calls == [("client@example.com", "SCALE_OUT")]


def test_durable_claim_migration_is_locked_to_internal_roles() -> None:
    migration = (
        Path(__file__).resolve().parents[1]
        / "migrations"
        / "20260717_exit_decision_generation_claims.sql"
    ).read_text()
    assert "generation_key       TEXT PRIMARY KEY" in migration
    assert "claim_state          TEXT NOT NULL DEFAULT 'CLAIMED'" in migration
    assert "ENABLE ROW LEVEL SECURITY" in migration
    assert "REVOKE ALL ON TABLE exit_decision_generation_claims FROM anon, authenticated" in migration


def test_ledger_suppresses_closed_zero_and_inflight_positions() -> None:
    _reset_guard_caches()
    decision = _decision()
    assert guard.should_write_ledger(_pos(closed=True), decision, now_monotonic=1.0) is False
    assert guard.should_write_ledger(_pos(quantity_remaining=0), decision, now_monotonic=1.0) is False
    assert guard.should_write_ledger(_pos(exit_in_flight=True), decision, now_monotonic=1.0) is False


def test_external_precheck_cache_bounds_db_and_osm_reads() -> None:
    _reset_guard_caches()
    pos = _pos(position_id="position-cache")
    assert guard.should_run_external_precheck(pos, now_monotonic=100.0) is True
    assert guard.should_run_external_precheck(pos, now_monotonic=101.0) is False
    assert guard.should_run_external_precheck(pos, now_monotonic=100.0 + guard._EXTERNAL_PRECHECK_TTL + 0.01) is True
    assert guard.should_run_external_precheck(_pos(exit_in_flight=True), now_monotonic=500.0) is False


def test_submit_wrapper_same_process_concurrency_allows_one_callback(generation_claims_table, monkeypatch) -> None:
    callback_entered = threading.Event()
    callback_release = threading.Event()
    callback_count = 0
    callback_count_lock = threading.Lock()

    def callback(pos, decision):
        nonlocal callback_count
        with callback_count_lock:
            callback_count += 1
        callback_entered.set()
        assert callback_release.wait(timeout=2.0)
        pos.exit_in_flight = True
        pos.pending_exit_local_order_id = "exit-local-1"
        pos.pending_exit_broker_order_id = "exit-broker-1"
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": "exit-local-1",
            "broker_order_id": "exit-broker-1",
        }

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    engine = _make_submit_engine(pos, callback=callback)
    first_result = []

    thread = threading.Thread(target=lambda: first_result.append(wrapped(engine, pos, _decision())))
    thread.start()
    assert callback_entered.wait(timeout=2.0)

    second_result = wrapped(engine, pos, _decision())
    callback_release.set()
    thread.join(timeout=2.0)

    assert thread.is_alive() is False
    assert first_result == [True]
    assert second_result is False
    assert callback_count == 1
    assert _claim_rows()[0]["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED
    assert engine._ap_exit_submit_claims == set()


def test_submit_wrapper_separate_engine_durability_blocks_second_engine(generation_claims_table, monkeypatch) -> None:
    callback_count = 0

    def callback(pos, decision):
        nonlocal callback_count
        callback_count += 1
        pos.exit_in_flight = True
        pos.pending_exit_local_order_id = "exit-local-1"
        pos.pending_exit_broker_order_id = "exit-broker-1"
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": "exit-local-1",
            "broker_order_id": "exit-broker-1",
        }

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)

    assert wrapped(_make_submit_engine(_pos(), callback=callback), _pos(), _decision()) is True
    assert wrapped(_make_submit_engine(_pos(), callback=callback), _pos(), _decision()) is False
    assert callback_count == 1


def test_submit_wrapper_restart_durability_blocks_fresh_engine(generation_claims_table, monkeypatch) -> None:
    callback_count = 0

    def callback(pos, decision):
        nonlocal callback_count
        callback_count += 1
        pos.exit_in_flight = True
        pos.pending_exit_local_order_id = "exit-local-1"
        pos.pending_exit_broker_order_id = "exit-broker-1"
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": "exit-local-1",
            "broker_order_id": "exit-broker-1",
        }

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)

    first_pos = _pos()
    assert wrapped(_make_submit_engine(first_pos, callback=callback), first_pos, _decision()) is True
    restarted_pos = _pos()
    assert wrapped(_make_submit_engine(restarted_pos, callback=callback), restarted_pos, _decision()) is False
    assert callback_count == 1


def test_submit_wrapper_releases_claim_on_conclusive_pre_submit_failure(generation_claims_table, monkeypatch) -> None:
    responses = iter([
        {"ok": False, "accepted": False, "status": "ERROR", "error": "broker_http_400:bad request"},
        {"ok": True, "accepted": True, "status": "EXIT_SUBMITTED", "local_order_id": "exit-local-2", "broker_order_id": "exit-broker-2"},
    ])
    callback_count = 0

    def callback(pos, decision):
        nonlocal callback_count
        callback_count += 1
        result = next(responses)
        if result["status"] == "EXIT_SUBMITTED":
            pos.exit_in_flight = True
            pos.pending_exit_local_order_id = result["local_order_id"]
            pos.pending_exit_broker_order_id = result["broker_order_id"]
        return result

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(lambda engine, pos, decision: bool((engine.on_scale if str(decision.action).upper() == "SCALE_OUT" else engine.on_exit)(pos, decision).get("ok")))
    pos = _pos()

    assert wrapped(_make_submit_engine(pos, callback=callback), pos, _decision()) is False
    assert _claim_rows()[0]["claim_state"] == guard._CLAIM_STATE_RELEASED_NO_SUBMIT

    retry_pos = _pos()
    assert wrapped(_make_submit_engine(retry_pos, callback=callback), retry_pos, _decision()) is True
    assert callback_count == 2
    assert _claim_rows()[0]["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED


def test_submit_wrapper_keeps_claim_on_ambiguous_submit_failure(generation_claims_table, monkeypatch) -> None:
    callback_count = 0

    def callback(pos, decision):
        nonlocal callback_count
        callback_count += 1
        return {
            "ok": False,
            "accepted": True,
            "status": "EXIT_REQUESTED",
            "error": "BROKER_AMBIGUOUS_READ_TIMEOUT_RECONCILIATION_REQUIRED:timeout",
            "reconciliation_required": True,
            "identity_quarantine": True,
        }

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(lambda engine, pos, decision: bool((engine.on_scale if str(decision.action).upper() == "SCALE_OUT" else engine.on_exit)(pos, decision).get("ok")))
    first_pos = _pos()

    assert wrapped(_make_submit_engine(first_pos, callback=callback), first_pos, _decision()) is False
    replay_pos = _pos()
    assert wrapped(_make_submit_engine(replay_pos, callback=callback), replay_pos, _decision()) is False
    assert _claim_rows()[0]["claim_state"] == guard._CLAIM_STATE_AMBIGUOUS
    assert callback_count == 1


def test_submit_wrapper_active_exit_fence_hydrates_and_blocks_submit(generation_claims_table, monkeypatch) -> None:
    callback = MagicMock()
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    engine = _make_submit_engine(
        pos,
        callback=callback,
        active_order={
            "status": "EXIT_PARTIAL_FILL",
            "local_order_id": "exit-local-live",
            "broker_order_id": "exit-broker-live",
        },
    )

    assert wrapped(engine, pos, _decision()) is False
    assert callback.call_count == 0
    assert pos.exit_in_flight is True
    assert pos.pending_exit_local_order_id == "exit-local-live"
    assert pos.pending_exit_broker_order_id == "exit-broker-live"
    assert _claim_rows() == []


def test_submit_wrapper_generation_advances_after_remaining_qty_changes(generation_claims_table, monkeypatch) -> None:
    callback_count = 0

    def callback(pos, decision):
        nonlocal callback_count
        callback_count += 1
        local_id = f"exit-local-{callback_count}"
        broker_id = f"exit-broker-{callback_count}"
        pos.exit_in_flight = True
        pos.pending_exit_local_order_id = local_id
        pos.pending_exit_broker_order_id = broker_id
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": local_id,
            "broker_order_id": broker_id,
        }

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda pos, *_: (f"client|position|{int(pos.quantity_remaining)}|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)

    first_pos = _pos(quantity_remaining=3)
    assert wrapped(_make_submit_engine(first_pos, callback=callback), first_pos, _decision()) is True
    same_generation_pos = _pos(quantity_remaining=3)
    assert wrapped(_make_submit_engine(same_generation_pos, callback=callback), same_generation_pos, _decision()) is False

    next_generation_pos = _pos(quantity_remaining=2)
    assert wrapped(_make_submit_engine(next_generation_pos, callback=callback), next_generation_pos, _decision(quantity=1)) is True
    assert callback_count == 2


def test_submit_wrapper_partial_fill_blocks_duplicate_then_allows_next_generation(generation_claims_table, monkeypatch) -> None:
    callback_count = 0

    def callback(pos, decision):
        nonlocal callback_count
        callback_count += 1
        pos.exit_in_flight = True
        pos.pending_exit_local_order_id = "exit-local-next"
        pos.pending_exit_broker_order_id = "exit-broker-next"
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": "exit-local-next",
            "broker_order_id": "exit-broker-next",
        }

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda pos, *_: (f"client|position|{int(pos.quantity_remaining)}|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)

    live_partial_pos = _pos(quantity_remaining=3)
    live_partial_engine = _make_submit_engine(
        live_partial_pos,
        callback=callback,
        active_order={
            "status": "EXIT_PARTIAL_FILL",
            "local_order_id": "exit-local-live",
            "broker_order_id": "exit-broker-live",
        },
    )
    assert wrapped(live_partial_engine, live_partial_pos, _decision()) is False

    next_generation_pos = _pos(quantity_remaining=2)
    assert wrapped(_make_submit_engine(next_generation_pos, callback=callback), next_generation_pos, _decision()) is True
    assert callback_count == 1


def test_submit_wrapper_does_not_depend_on_ledger_for_duplicate_suppression(generation_claims_table, monkeypatch) -> None:
    callback_count = 0

    def callback(pos, decision):
        nonlocal callback_count
        callback_count += 1
        pos.exit_in_flight = True
        pos.pending_exit_local_order_id = "exit-local-ledgerless"
        pos.pending_exit_broker_order_id = "exit-broker-ledgerless"
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": "exit-local-ledgerless",
            "broker_order_id": "exit-broker-ledgerless",
        }

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)

    first_pos = _pos()
    assert wrapped(_make_submit_engine(first_pos, callback=callback), first_pos, _decision()) is True
    second_pos = _pos()
    assert wrapped(_make_submit_engine(second_pos, callback=callback), second_pos, _decision()) is False
    assert callback_count == 1


def test_submit_wrapper_blocks_when_claim_acquisition_fails(monkeypatch) -> None:
    callback = MagicMock()
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    monkeypatch.setattr(
        guard,
        "_claim_durable_decision_generation",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("db unavailable")),
    )
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    assert wrapped(_make_submit_engine(pos, callback=callback), pos, _decision()) is False
    assert callback.call_count == 0


def test_precheck_hydrates_existing_durable_exit_order(monkeypatch) -> None:
    _reset_guard_caches()

    class FakeOSM:
        def get_active_exit_order(self, position_id):
            assert position_id == "position-active-exit"
            return {
                "status": "EXIT_ACKNOWLEDGED",
                "local_order_id": "exit-local-1",
                "broker_order_id": "exit-broker-1",
            }

    pos = _pos(position_id="position-active-exit")
    engine = _FakeEngine([pos], osm=FakeOSM())
    wrapped = guard.wrap_precheck(lambda self: True)
    monkeypatch.setattr(guard, "_terminal_position_snapshot", lambda pos, engine: None)

    assert wrapped(engine) is True
    assert pos.exit_in_flight is True
    assert pos.pending_exit_local_order_id == "exit-local-1"
    assert pos.pending_exit_broker_order_id == "exit-broker-1"
    assert engine.active_positions() == [pos]


def test_precheck_removes_terminal_position_only_with_fresh_exact_broker_flat(monkeypatch) -> None:
    _reset_guard_caches()
    pos = _pos(position_id="position-terminal")
    engine = _FakeEngine([pos])
    wrapped = guard.wrap_precheck(lambda self: True)

    monkeypatch.setattr(guard, "_active_exit_order", lambda engine, position_id: None)
    monkeypatch.setattr(
        guard,
        "_terminal_position_snapshot",
        lambda pos, engine: {
            "id": "position-terminal",
            "status": "CLOSED",
            "contract": "SPY260716P00751000",
        },
    )
    monkeypatch.setattr(guard, "_fresh_exact_broker_flat", lambda pos, engine: True)

    assert wrapped(engine) is True
    assert pos.closed is True
    assert pos.quantity_remaining == 0
    assert engine.active_positions() == []
    assert "position-terminal" not in engine._positions_by_id


def test_precheck_preserves_exit_when_broker_truth_is_unavailable(monkeypatch) -> None:
    _reset_guard_caches()
    pos = _pos(position_id="position-unknown-broker")
    engine = _FakeEngine([pos])
    wrapped = guard.wrap_precheck(lambda self: False)

    monkeypatch.setattr(guard, "_active_exit_order", lambda engine, position_id: None)
    monkeypatch.setattr(
        guard,
        "_terminal_position_snapshot",
        lambda pos, engine: {
            "id": "position-unknown-broker",
            "status": "CLOSED",
            "contract": "SPY260716P00751000",
        },
    )
    monkeypatch.setattr(guard, "_fresh_exact_broker_flat", lambda pos, engine: False)

    assert wrapped(engine) is False
    assert pos.closed is False
    assert pos.quantity_remaining == 3
    assert engine.active_positions() == [pos]


@pytest.mark.parametrize(
    ("name", "bad_value", "attribute", "expected"),
    [
        ("EXIT_DECISION_LEDGER_ACTION_DEDUPE_SECONDS", "bad", "_ACTION_LEDGER_TTL", 60.0),
        ("EXIT_DECISION_LEDGER_HOLD_DEDUPE_SECONDS", "nan", "_HOLD_LEDGER_TTL", 300.0),
        ("EXIT_DECISION_LEDGER_DEDUPE_CACHE_MAX", "-1", "_LEDGER_CACHE_MAX", 4096),
        ("EXIT_DECISION_EXTERNAL_PRECHECK_SECONDS", "inf", "_EXTERNAL_PRECHECK_TTL", 30.0),
        ("EXIT_DECISION_PRECHECK_CACHE_MAX", "1.2", "_PRECHECK_CACHE_MAX", 4096),
    ],
)
def test_malformed_environment_imports_safely_with_bounded_fallback(monkeypatch, name, bad_value, attribute, expected) -> None:
    monkeypatch.setenv(name, bad_value)
    reloaded = importlib.reload(guard)
    assert getattr(reloaded, attribute) == expected
    monkeypatch.delenv(name, raising=False)
    importlib.reload(guard)


def test_valid_environment_values_are_clamped(monkeypatch) -> None:
    monkeypatch.setenv("EXIT_DECISION_LEDGER_ACTION_DEDUPE_SECONDS", "999999")
    monkeypatch.setenv("EXIT_DECISION_PRECHECK_CACHE_MAX", "999999")
    reloaded = importlib.reload(guard)
    assert reloaded._ACTION_LEDGER_TTL == 86400.0
    assert reloaded._PRECHECK_CACHE_MAX == 100000
    monkeypatch.delenv("EXIT_DECISION_LEDGER_ACTION_DEDUPE_SECONDS")
    monkeypatch.delenv("EXIT_DECISION_PRECHECK_CACHE_MAX")
    importlib.reload(guard)


def test_installed_guard_manifest_reports_exact_names(monkeypatch) -> None:
    installed = []
    module = SimpleNamespace(install=lambda: installed.append("canonical"))
    monkeypatch.setattr(
        lifecycle_guards,
        "_GUARDS",
        (("canonical_exit_fill_truth", "ap.fake_guard", "install", True),),
    )
    monkeypatch.setattr(lifecycle_guards.importlib, "import_module", lambda _name: module)
    manifest = lifecycle_guards.install_trade_lifecycle_guards()
    assert list(manifest) == ["canonical_exit_fill_truth"]
    assert manifest["canonical_exit_fill_truth"]["status"] == "installed"
    assert installed == ["canonical"]


def test_live_preflight_fails_when_generation_claim_migration_absent(monkeypatch) -> None:
    monkeypatch.setattr(
        lifecycle_guards,
        "install_trade_lifecycle_guards",
        lambda: {"canonical_exit_fill_truth": {"status": "installed", "required_when_present": True}},
    )
    monkeypatch.setattr(lifecycle_guards, "_generation_claims_table_exists", lambda: False)
    ok, diagnostic = lifecycle_guards.lifecycle_guard_preflight("live")
    assert ok is False
    assert diagnostic["generation_claims_table_exists"] is False


def test_client_runner_live_preflight_blocks_entries_before_broker_auth(monkeypatch) -> None:
    from client_runner import ClientRunner

    monkeypatch.setattr(
        lifecycle_guards,
        "lifecycle_guard_preflight",
        lambda _mode: (False, {"missing_required_guards": ["proof_taxonomy"], "generation_claims_table_exists": False}),
    )
    runner = SimpleNamespace(email="client@example.com", mode="LIVE", account_id="account-1")
    broker = SimpleNamespace(get_account_equity=MagicMock(return_value=1000.0))
    ok, reason = ClientRunner._run_live_preflight(runner, broker)
    assert ok is False
    assert reason.startswith("lifecycle_guard_preflight_failed:")
    assert broker.get_account_equity.call_count == 0


def test_live_preflight_fails_when_required_guard_installation_failed(monkeypatch) -> None:
    monkeypatch.setattr(
        lifecycle_guards,
        "install_trade_lifecycle_guards",
        lambda: {"canonical_exit_fill_truth": {"status": "installation_failed", "required_when_present": True}},
    )
    monkeypatch.setattr(lifecycle_guards, "_generation_claims_table_exists", lambda: True)
    ok, diagnostic = lifecycle_guards.lifecycle_guard_preflight("live")
    assert ok is False
    assert diagnostic["missing_required_guards"] == ["canonical_exit_fill_truth"]


def test_paper_remains_available_with_guard_diagnostics(monkeypatch) -> None:
    monkeypatch.setattr(
        lifecycle_guards,
        "install_trade_lifecycle_guards",
        lambda: {"canonical_exit_fill_truth": {"status": "installation_failed", "required_when_present": True}},
    )
    monkeypatch.setattr(lifecycle_guards, "_generation_claims_table_exists", lambda: False)
    ok, diagnostic = lifecycle_guards.lifecycle_guard_preflight("paper")
    assert ok is True
    assert diagnostic["missing_required_guards"] == ["canonical_exit_fill_truth"]


def test_durable_claim_infrastructure_failure_does_not_suppress_protective_exit(monkeypatch) -> None:
    _reset_guard_caches()
    calls = []
    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda *_: (_ for _ in ()).throw(RuntimeError("database unavailable")),
    )
    wrapped = guard.wrap_ledger(lambda pos, decision, client_id="": calls.append((pos, decision)) or "written")
    assert wrapped(_pos(), _decision(), client_id="client@example.com") == "written"
    assert len(calls) == 1
