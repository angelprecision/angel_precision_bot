from __future__ import annotations

import importlib
import sys
import threading
import time
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
        "side": "PUT",
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
    def __init__(self, active_order=None, *, client_id=""):
        self.client_id = client_id
        self.active_order = dict(active_order) if isinstance(active_order, dict) else active_order
        self.active_orders_by_position = {}
        if isinstance(self.active_order, dict):
            self.active_orders_by_position[str(self.active_order.get("position_id") or "")] = self.active_order

    def get_active_exit_order(self, position_id):
        position_key = str(position_id or "")
        return self.active_orders_by_position.get(position_key)

    def create_exit_order(self, **kwargs):
        local_order_id = str(kwargs.get("local_order_id") or "exit-local-created")
        self.active_order = {
            "client_id": self.client_id,
            "local_order_id": local_order_id,
            "broker_order_id": "",
            "kind": "EXIT",
            "status": "EXIT_REQUESTED",
            "position_id": kwargs.get("position_id"),
            "qty": kwargs.get("qty"),
            "contract": kwargs.get("contract"),
            "symbol": kwargs.get("symbol"),
            "execution_mode": kwargs.get("execution_mode"),
            "meta": {},
        }
        self.active_orders_by_position[str(kwargs.get("position_id") or "")] = self.active_order
        return local_order_id

    def get_order(self, local_order_id):
        if isinstance(self.active_order, dict) and self.active_order.get("local_order_id") == local_order_id:
            return dict(self.active_order)
        for order in self.active_orders_by_position.values():
            if isinstance(order, dict) and order.get("local_order_id") == local_order_id:
                return dict(order)
        return None

    def update_order_meta(self, local_order_id, patch):
        if isinstance(self.active_order, dict) and self.active_order.get("local_order_id") == local_order_id:
            meta = dict(self.active_order.get("meta") or {})
            meta.update(dict(patch or {}))
            self.active_order["meta"] = meta
        for order in self.active_orders_by_position.values():
            if isinstance(order, dict) and order.get("local_order_id") == local_order_id:
                meta = dict(order.get("meta") or {})
                meta.update(dict(patch or {}))
                order["meta"] = meta
        return True

    def transition(self, local_order_id, new_status, **kwargs):
        order = None
        if isinstance(self.active_order, dict) and self.active_order.get("local_order_id") == local_order_id:
            order = self.active_order
        if order is None:
            for candidate in self.active_orders_by_position.values():
                if isinstance(candidate, dict) and candidate.get("local_order_id") == local_order_id:
                    order = candidate
                    break
        if order is None:
            return False
        order["status"] = new_status
        if "last_error" in kwargs:
            order["last_error"] = kwargs.get("last_error")
        return True

    def retire_unsubmitted_exit_intent(self, local_order_id, *, last_error):
        order = None
        if isinstance(self.active_order, dict) and self.active_order.get("local_order_id") == local_order_id:
            order = self.active_order
        if order is None:
            for candidate in self.active_orders_by_position.values():
                if isinstance(candidate, dict) and candidate.get("local_order_id") == local_order_id:
                    order = candidate
                    break
        if order is None:
            return False
        meta = dict(order.get("meta") or {})
        if (
            str(order.get("status") or "").upper() != "EXIT_REQUESTED"
            or str(order.get("broker_order_id") or "").strip()
            or order.get("submitted_ts")
            or str(meta.get("submit_intent_at") or "").strip()
            or bool(meta.get("split_brain_quarantine"))
            or bool(meta.get("reconciliation_required"))
        ):
            return False
        order["status"] = "ERROR"
        order["last_error"] = last_error
        return True


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
        if sql.startswith("UPDATE exit_decision_generation_claims SET client_id="):
            if "claimed_at <= NOW() - (%s * INTERVAL '1 second')" in sql:
                (
                    client_id,
                    position_id,
                    remaining_qty,
                    exit_generation,
                    decision_action,
                    decision_reason_code,
                    update_state,
                    last_error,
                    generation_key,
                    existing_state,
                    lease_seconds,
                ) = params
                existing = self.store.get(generation_key)
                claimed_at = (existing or {}).get("claimed_at")
                if (
                    existing is not None
                    and existing.get("claim_state") == existing_state
                    and claimed_at is not None
                    and float(claimed_at) <= (time.time() - float(lease_seconds))
                ):
                    row = {
                        "generation_key": generation_key,
                        "client_id": client_id,
                        "position_id": position_id,
                        "remaining_qty": remaining_qty,
                        "exit_generation": exit_generation,
                        "decision_action": decision_action,
                        "decision_reason_code": decision_reason_code,
                        "claim_state": update_state,
                        "local_order_id": existing.get("local_order_id"),
                        "broker_order_id": existing.get("broker_order_id"),
                        "last_error": last_error,
                        "claimed_at": existing.get("claimed_at"),
                        "released_at": existing.get("released_at"),
                    }
                    self.store[generation_key] = row
                    self._row = dict(row)
                else:
                    self._row = None
                return self
            (
                client_id,
                position_id,
                remaining_qty,
                exit_generation,
                decision_action,
                decision_reason_code,
                update_state,
                local_order_id,
                generation_key,
                released_state,
            ) = params
            existing = self.store.get(generation_key)
            if existing is not None and existing.get("claim_state") == released_state:
                row = {
                    "generation_key": generation_key,
                    "client_id": client_id,
                    "position_id": position_id,
                    "remaining_qty": remaining_qty,
                    "exit_generation": exit_generation,
                    "decision_action": decision_action,
                    "decision_reason_code": decision_reason_code,
                    "claim_state": update_state,
                    "local_order_id": local_order_id,
                    "broker_order_id": None,
                    "last_error": None,
                    "claimed_at": None,
                    "released_at": None,
                }
                self.store[generation_key] = row
                self._row = dict(row)
            else:
                self._row = None
        elif sql.startswith("INSERT INTO exit_decision_generation_claims"):
            (
                generation_key,
                client_id,
                position_id,
                remaining_qty,
                exit_generation,
                decision_action,
                decision_reason_code,
                inserted_state,
                local_order_id,
            ) = params
            existing = self.store.get(generation_key)
            if existing is None:
                row = {
                    "generation_key": generation_key,
                    "client_id": client_id,
                    "position_id": position_id,
                    "remaining_qty": remaining_qty,
                    "exit_generation": exit_generation,
                    "decision_action": decision_action,
                    "decision_reason_code": decision_reason_code,
                    "claim_state": inserted_state,
                    "local_order_id": local_order_id,
                    "broker_order_id": None,
                    "last_error": None,
                    "claimed_at": time.time(),
                    "released_at": None,
                }
                self.store[generation_key] = row
                self._row = dict(row)
            else:
                self._row = None
        elif sql.startswith("UPDATE exit_decision_generation_claims SET last_error="):
            token, generation_key, claim_state, required_pattern, reconciling_pattern, lease_seconds = params
            row = self.store.get(generation_key)
            last_error = str((row or {}).get("last_error") or "")
            required_prefix = str(required_pattern).rstrip("%")
            reconciling_prefix = str(reconciling_pattern).rstrip("%")
            claimed_at = (row or {}).get("claimed_at")
            if (
                row is not None
                and row.get("claim_state") == claim_state
                and (
                    not last_error
                    or last_error.startswith(required_prefix)
                    or (
                        last_error.startswith(reconciling_prefix)
                        and claimed_at is not None
                        and float(claimed_at) <= (time.time() - float(lease_seconds))
                    )
                )
            ):
                row["last_error"] = token
                row["claimed_at"] = time.time()
                self._row = dict(row)
            else:
                self._row = None
        elif sql.startswith("SELECT generation_key, client_id, position_id, remaining_qty"):
            generation_key = params[0]
            row = self.store.get(generation_key)
            self._row = dict(row) if row else None
        elif sql.startswith("UPDATE exit_decision_generation_claims SET claim_state="):
            if "COALESCE(last_error,'')=%s RETURNING" in sql:
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
                    expected_state,
                    reconciliation_token,
                ) = params
                row = self.store[generation_key]
                if row.get("claim_state") == expected_state and str(row.get("last_error") or "") == reconciliation_token:
                    row["claim_state"] = claim_state
                    if released_state_marker == release_state_target == guard._CLAIM_STATE_RELEASED_NO_SUBMIT:
                        row["released_at"] = "released"
                    if local_order_id_check:
                        row["local_order_id"] = local_order_id
                    if broker_order_id_check:
                        row["broker_order_id"] = broker_order_id
                    row["last_error"] = error_text if error_check else None
                    self._row = dict(row)
                else:
                    self._row = None
            else:
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


def _make_submit_engine(pos, *, callback, active_order=None, mode="LIVE"):
    if isinstance(active_order, dict) and not active_order.get("position_id"):
        active_order = dict(active_order)
        active_order["position_id"] = pos.position_id
    osm = _FakeOSM(active_order=active_order, client_id=pos.client_id)
    engine = SimpleNamespace(
        _lock=threading.RLock(),
        client_id=pos.client_id,
        order_state_machine=osm,
        osm=None,
        on_exit=callback,
        on_scale=callback,
        master_control=SimpleNamespace(mode=mode),
    )
    engine._extract_exit_order_identity = _identity_from_result
    return engine


def _invoke_submit_callback(engine, pos, decision):
    action = str(getattr(decision, "action", "") or "").upper()
    if action == "SCALE_OUT":
        return bool(engine.on_scale(pos, decision))
    return bool(engine.on_exit(pos, decision))


def _mark_claim_stale_ambiguous(generation_key: str, *, local_order_id: str, broker_order_id: str = "", last_error: str | None = None):
    error_text = last_error or guard._STALE_CLAIM_RECONCILIATION_REQUIRED
    if _FAKE_CLAIMS_STORE is not None:
        row = _FAKE_CLAIMS_STORE[generation_key]
        row["claim_state"] = guard._CLAIM_STATE_AMBIGUOUS
        row["local_order_id"] = local_order_id
        row["broker_order_id"] = broker_order_id
        row["last_error"] = error_text
        row["claimed_at"] = time.time() - (guard._CLAIM_LEASE_SECONDS + 5.0)
        return
    with conn() as c:
        c.execute(
            "UPDATE exit_decision_generation_claims "
            "SET claim_state=%s, local_order_id=%s, broker_order_id=%s, last_error=%s, "
            "claimed_at = NOW() - (%s * INTERVAL '1 second') "
            "WHERE generation_key=%s",
            (
                guard._CLAIM_STATE_AMBIGUOUS,
                local_order_id,
                broker_order_id,
                error_text,
                guard._CLAIM_LEASE_SECONDS + 5.0,
                generation_key,
            ),
        )


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
        def __init__(self):
            self._row = None

        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
            if "claimed_at <= NOW() - (%s * INTERVAL '1 second')" in sql:
                self._row = None
            else:
                self._row = {
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
            return self

        def fetchone(self):
            return self._row

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
    assert "ON CONFLICT (generation_key) DO NOTHING" in captured["sql"] or "UPDATE exit_decision_generation_claims SET" in captured["sql"]


def test_durable_generation_claim_does_not_reclaim_existing_claimed_row(
    generation_claims_table,
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    first = guard._claim_durable_decision_generation(
        generation_key="client|position|3|1",
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
    )
    second = guard._claim_durable_decision_generation(
        generation_key="client|position|3|1",
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
    )
    assert first["claimed"] is True
    assert second["claimed"] is False
    assert second["claim_state"] == guard._CLAIM_STATE_CLAIMED


def test_durable_generation_claim_marks_stale_existing_claimed_row_ambiguous(
    generation_claims_table,
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    first = guard._claim_durable_decision_generation(
        generation_key="client|position|3|1",
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
    )
    assert first["claimed"] is True

    if _FAKE_CLAIMS_STORE is not None:
        _FAKE_CLAIMS_STORE["client|position|3|1"]["claimed_at"] = time.time() - (guard._CLAIM_LEASE_SECONDS + 5.0)
    else:
        with conn() as c:
            c.execute(
                "UPDATE exit_decision_generation_claims "
                "SET claimed_at = NOW() - (%s * INTERVAL '1 second') "
                "WHERE generation_key=%s",
                (guard._CLAIM_LEASE_SECONDS + 5.0, "client|position|3|1"),
            )

    reclaimed = guard._claim_durable_decision_generation(
        generation_key="client|position|3|1",
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
    )
    assert reclaimed["claimed"] is False
    assert reclaimed["claim_state"] == guard._CLAIM_STATE_AMBIGUOUS
    assert reclaimed["last_error"] == guard._STALE_CLAIM_RECONCILIATION_REQUIRED


def test_submit_wrapper_blocks_stale_claimed_generation_without_resubmit(
    generation_claims_table,
    monkeypatch,
) -> None:
    callback = MagicMock()
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    first = guard._claim_durable_decision_generation(
        generation_key="client|position|3|1",
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
    )
    assert first["claimed"] is True

    if _FAKE_CLAIMS_STORE is not None:
        _FAKE_CLAIMS_STORE["client|position|3|1"]["claimed_at"] = time.time() - (guard._CLAIM_LEASE_SECONDS + 5.0)
    else:
        with conn() as c:
            c.execute(
                "UPDATE exit_decision_generation_claims "
                "SET claimed_at = NOW() - (%s * INTERVAL '1 second') "
                "WHERE generation_key=%s",
                (guard._CLAIM_LEASE_SECONDS + 5.0, "client|position|3|1"),
            )

    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    assert wrapped(_make_submit_engine(pos, callback=callback), pos, _decision()) is False
    assert callback.call_count == 0
    rows = _claim_rows()
    assert rows[0]["claim_state"] == guard._CLAIM_STATE_AMBIGUOUS
    assert rows[0]["last_error"] == guard._STALE_CLAIM_RECONCILIATION_REQUIRED


def test_stale_claim_reconciliation_releases_no_submit_and_allows_retry(
    generation_claims_table,
    monkeypatch,
) -> None:
    key = "client|position|3|1"
    guard._claim_durable_decision_generation(
        generation_key=key,
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
        local_order_id="exit-local-1",
    )
    _mark_claim_stale_ambiguous(key, local_order_id="exit-local-1")
    osm = SimpleNamespace(get_order=MagicMock(return_value={
        "local_order_id": "exit-local-1",
        "client_id": "client",
        "position_id": "position",
        "kind": "EXIT",
        "status": "EXIT_REQUESTED",
        "qty": 3,
        "broker_order_id": "",
        "submitted_ts": None,
        "meta": {"exit_generation_claim": 1},
    }))

    reconciled = guard.reconcile_stale_exit_generation_claim(key, osm=osm)
    assert reconciled["claim_state"] == guard._CLAIM_STATE_RELEASED_NO_SUBMIT

    retried = guard._claim_durable_decision_generation(
        generation_key=key,
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
        local_order_id="exit-local-1",
    )
    assert retried["claimed"] is True


def test_stale_claim_reconciliation_promotes_broker_owned_and_blocks_duplicate_callback(
    generation_claims_table,
    monkeypatch,
) -> None:
    key = "client|position|3|1"
    guard._claim_durable_decision_generation(
        generation_key=key,
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
        local_order_id="exit-local-1",
    )
    _mark_claim_stale_ambiguous(key, local_order_id="exit-local-1")
    core = SimpleNamespace(
        order_state_machine=SimpleNamespace(
            get_order=MagicMock(side_effect=[
                {
                    "local_order_id": "exit-local-1",
                    "client_id": "client",
                    "position_id": "position",
                    "kind": "EXIT",
                    "status": "EXIT_REQUESTED",
                    "qty": 3,
                    "broker_order_id": "",
                    "submitted_ts": None,
                    "meta": {
                        "submit_intent_at": "2026-07-17T12:00:00+00:00",
                        "broker_submit_key": "exit-local-1",
                        "exit_generation_claim": 1,
                    },
                },
                {
                    "local_order_id": "exit-local-1",
                    "client_id": "client",
                    "position_id": "position",
                    "kind": "EXIT",
                    "status": "EXIT_SUBMITTED",
                    "qty": 3,
                    "broker_order_id": "broker-exit-1",
                    "submitted_ts": "2026-07-17T12:00:01+00:00",
                    "meta": {
                        "submit_intent_at": "2026-07-17T12:00:00+00:00",
                        "broker_submit_key": "exit-local-1",
                        "exit_generation_claim": 1,
                    },
                },
            ]),
        ),
        reconcile_exit_broker_intent=MagicMock(return_value={
            "disposition": "ALREADY_RECONCILED",
            "reason_code": "BROKER_ORDER_ADOPTED",
            "broker_order_id": "broker-exit-1",
            "status": "EXIT_SUBMITTED",
        }),
    )

    reconciled = guard.reconcile_stale_exit_generation_claim(key, execution_core=core)
    assert reconciled["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED
    assert core.reconcile_exit_broker_intent.call_count == 1

    callback = MagicMock(return_value=True)
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: (key, 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    assert wrapped(_make_submit_engine(pos, callback=callback), pos, _decision()) is False
    assert callback.call_count == 0


def test_stale_claim_reconciliation_routes_confirmed_fill_once(
    generation_claims_table,
    monkeypatch,
) -> None:
    key = "client|position|3|1"
    guard._claim_durable_decision_generation(
        generation_key=key,
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
        local_order_id="exit-local-fill",
    )
    _mark_claim_stale_ambiguous(key, local_order_id="exit-local-fill")
    reconcile = MagicMock(return_value={})
    fake_guard = SimpleNamespace(reconcile_confirmed_exit_fill=reconcile)
    monkeypatch.setitem(sys.modules, "ap.exit_fill_truth_guard", fake_guard)
    core = SimpleNamespace(
        order_state_machine=SimpleNamespace(
            get_order=MagicMock(side_effect=[
                {
                    "local_order_id": "exit-local-fill",
                    "client_id": "client",
                    "position_id": "position",
                    "kind": "EXIT",
                    "status": "EXIT_REQUESTED",
                    "qty": 3,
                    "broker_order_id": "",
                    "submitted_ts": None,
                    "meta": {
                        "submit_intent_at": "2026-07-17T12:00:00+00:00",
                        "broker_submit_key": "exit-local-fill",
                        "exit_generation_claim": 1,
                    },
                },
                {
                    "local_order_id": "exit-local-fill",
                    "client_id": "client",
                    "position_id": "position",
                    "kind": "EXIT",
                    "status": "EXIT_FILLED",
                    "qty": 3,
                    "broker_order_id": "broker-exit-fill",
                    "filled_qty": 3,
                    "fill_price": 2.15,
                    "filled_ts": "2026-07-17T12:01:00+00:00",
                    "submitted_ts": "2026-07-17T12:00:01+00:00",
                    "meta": {
                        "submit_intent_at": "2026-07-17T12:00:00+00:00",
                        "broker_submit_key": "exit-local-fill",
                        "exit_generation_claim": 1,
                    },
                },
            ]),
        ),
        reconcile_exit_broker_intent=MagicMock(return_value={
            "disposition": "ALREADY_RECONCILED",
            "reason_code": "BROKER_ORDER_ADOPTED",
            "broker_order_id": "broker-exit-fill",
            "status": "EXIT_FILLED",
        }),
    )

    reconciled = guard.reconcile_stale_exit_generation_claim(key, execution_core=core)
    assert reconciled["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED
    reconcile.assert_called_once()


def test_stale_claim_reconciliation_keeps_ambiguous_when_truth_unavailable(
    generation_claims_table,
    monkeypatch,
) -> None:
    key = "client|position|3|1"
    guard._claim_durable_decision_generation(
        generation_key=key,
        client_id="client",
        position_id="position",
        remaining_qty=3,
        exit_generation=1,
        decision=_decision(),
        local_order_id="exit-local-1",
    )
    _mark_claim_stale_ambiguous(key, local_order_id="exit-local-1")
    core = SimpleNamespace(
        order_state_machine=SimpleNamespace(
            get_order=MagicMock(return_value={
                "local_order_id": "exit-local-1",
                "client_id": "client",
                "position_id": "position",
                "kind": "EXIT",
                "status": "EXIT_REQUESTED",
                "qty": 3,
                "broker_order_id": "",
                "submitted_ts": None,
                "meta": {
                    "submit_intent_at": "2026-07-17T12:00:00+00:00",
                    "broker_submit_key": "exit-local-1",
                    "exit_generation_claim": 1,
                },
            }),
        ),
        reconcile_exit_broker_intent=MagicMock(return_value={
            "disposition": "RECONCILE_PENDING",
            "reason_code": "RECONCILE_BROKER_QUERY_FAILED:TimeoutError",
        }),
    )

    reconciled = guard.reconcile_stale_exit_generation_claim(key, execution_core=core)
    assert reconciled["claim_state"] == guard._CLAIM_STATE_AMBIGUOUS
    assert reconciled["last_error"].startswith(
        "STALE_CLAIM_RECONCILIATION_REQUIRED:RECONCILE_BROKER_QUERY_FAILED"
    )


def test_stale_claim_reconciliation_race_allows_one_terminal_transition(monkeypatch) -> None:
    key = "client|position|3|1"
    claim = {
        "generation_key": key,
        "client_id": "client",
        "position_id": "position",
        "remaining_qty": 3,
        "exit_generation": 1,
        "claim_state": guard._CLAIM_STATE_AMBIGUOUS,
        "local_order_id": "exit-local-1",
        "broker_order_id": "",
        "last_error": guard._STALE_CLAIM_RECONCILIATION_REQUIRED,
    }
    acquire_results = iter([
        (dict(claim), "token-1"),
        ({}, "token-2"),
    ])
    finish_calls = []
    monkeypatch.setattr(guard, "_acquire_stale_claim_reconciliation", lambda *_: next(acquire_results))
    monkeypatch.setattr(guard, "_load_durable_decision_generation", lambda *_: {
        "generation_key": key,
        "claim_state": guard._CLAIM_STATE_RELEASED_NO_SUBMIT,
        "local_order_id": "exit-local-1",
    })

    def _finish(*args, **kwargs):
        finish_calls.append(kwargs)
        return {
            "generation_key": key,
            "claim_state": kwargs["claim_state"],
            "local_order_id": "exit-local-1",
        }

    monkeypatch.setattr(guard, "_finish_stale_claim_reconciliation", _finish)
    osm = SimpleNamespace(get_order=MagicMock(return_value={
        "local_order_id": "exit-local-1",
        "client_id": "client",
        "position_id": "position",
        "kind": "EXIT",
        "status": "EXIT_REQUESTED",
        "qty": 3,
        "broker_order_id": "",
        "submitted_ts": None,
        "meta": {"exit_generation_claim": 1},
    }))

    first = guard.reconcile_stale_exit_generation_claim(key, osm=osm)
    second = guard.reconcile_stale_exit_generation_claim(key, osm=osm)
    assert first["claim_state"] == guard._CLAIM_STATE_RELEASED_NO_SUBMIT
    assert second["claim_state"] == guard._CLAIM_STATE_RELEASED_NO_SUBMIT
    assert len(finish_calls) == 1


def test_submit_wrapper_precreates_exact_local_exit_identity(
    generation_claims_table,
    monkeypatch,
) -> None:
    captured = {}
    original_claim = guard._claim_durable_decision_generation

    def _claim_proxy(**kwargs):
        captured.update(kwargs)
        return original_claim(**kwargs)

    monkeypatch.setattr(guard, "_claim_durable_decision_generation", _claim_proxy)
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    callback = MagicMock(return_value=False)
    pos = _pos()
    engine = _make_submit_engine(pos, callback=callback)
    wrapped = guard.wrap_submit(_invoke_submit_callback)

    wrapped(engine, pos, _decision())

    assert captured["local_order_id"]
    assert pos.pending_exit_local_order_id == captured["local_order_id"]
    assert engine.order_state_machine.active_order["local_order_id"] == captured["local_order_id"]
    assert engine.order_state_machine.active_order["meta"]["exit_generation_claim"] == 1


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
    assert "IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'anon')" in migration
    assert "IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'authenticated')" in migration
    assert "ENABLE ROW LEVEL SECURITY" in migration
    assert "REVOKE ALL ON TABLE exit_decision_generation_claims FROM anon;" in migration
    assert "REVOKE ALL ON TABLE exit_decision_generation_claims FROM authenticated;" in migration


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


@pytest.mark.parametrize("malformed_qty", ["1", 1.0, True, None, 0, -1])
def test_submit_wrapper_rejects_non_exact_integer_decision_quantity_before_reservation(
    malformed_qty,
    monkeypatch,
) -> None:
    callback = MagicMock(side_effect=AssertionError("callback must not run"))
    generation_read = MagicMock(
        side_effect=AssertionError("generation must not be read")
    )
    monkeypatch.setattr(guard, "_durable_exit_generation", generation_read)
    pos = _pos()
    engine = _make_submit_engine(pos, callback=callback)

    result = guard.wrap_submit(_invoke_submit_callback)(
        engine,
        pos,
        _decision(quantity=malformed_qty),
    )

    assert result is False
    callback.assert_not_called()
    generation_read.assert_not_called()
    assert engine.order_state_machine.active_order is None
    assert pos.pending_exit_local_order_id == ""
    assert pos.exit_in_flight is False


def test_malformed_quantity_does_not_make_open_position_inert_then_valid_retry_submits(
    generation_claims_table,
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    callback = MagicMock(return_value={
        "ok": True,
        "accepted": True,
        "status": "EXIT_SUBMITTED",
        "local_order_id": "exit-local-created",
        "broker_order_id": "exit-broker-valid-retry",
    })
    pos = _pos()
    engine = _make_submit_engine(pos, callback=callback)
    wrapped = guard.wrap_submit(_invoke_submit_callback)

    assert wrapped(engine, pos, _decision(quantity="1")) is False
    assert pos.exit_in_flight is False
    assert wrapped(engine, pos, _decision(quantity=1)) is True
    assert callback.call_count == 1


def test_submit_wrapper_releases_claim_on_conclusive_pre_submit_failure(generation_claims_table, monkeypatch) -> None:
    responses = iter([
        {"ok": False, "accepted": False, "status": "ERROR", "error": "NO_POST_ATTEMPTED:validation_failed"},
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
    engine = _make_submit_engine(pos, callback=callback)

    assert wrapped(engine, pos, _decision()) is False
    assert _claim_rows()[0]["claim_state"] == guard._CLAIM_STATE_RELEASED_NO_SUBMIT
    assert pos.pending_exit_local_order_id
    assert engine.order_state_machine.active_order["local_order_id"] == pos.pending_exit_local_order_id

    retry_pos = _pos()
    assert wrapped(_make_submit_engine(retry_pos, callback=callback), retry_pos, _decision()) is True
    assert callback_count == 2
    assert _claim_rows()[0]["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED


def test_submit_wrapper_retires_reserved_exit_row_after_conclusive_pre_submit_failure(
    generation_claims_table,
    monkeypatch,
) -> None:
    def callback(pos, decision):
        return {
            "ok": False,
            "accepted": False,
            "status": "ERROR",
            "error": "NO_POST_ATTEMPTED:validation_failed",
        }

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(
        lambda engine, pos, decision: bool(
            (engine.on_scale if str(decision.action).upper() == "SCALE_OUT" else engine.on_exit)(pos, decision).get("ok")
        )
    )
    pos = _pos()
    engine = _make_submit_engine(pos, callback=callback)

    assert wrapped(engine, pos, _decision()) is False
    assert engine.order_state_machine.active_order["status"] == "ERROR"
    assert engine.order_state_machine.active_order["last_error"] == "NO_POST_ATTEMPTED:validation_failed"


def test_submit_wrapper_retires_reserved_exit_row_when_claim_acquisition_fails_in_live(
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    monkeypatch.setattr(guard, "_claim_durable_decision_generation", lambda **kwargs: (_ for _ in ()).throw(RuntimeError("db down")))
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    engine = _make_submit_engine(pos, callback=MagicMock(), mode="LIVE")

    assert wrapped(engine, pos, _decision()) is False
    assert engine.order_state_machine.active_order["status"] == "ERROR"
    assert engine.order_state_machine.active_order["last_error"].startswith("EXIT_DECISION_GENERATION_CLAIM_FAILED:")
    assert pos.pending_exit_local_order_id == engine.order_state_machine.active_order["local_order_id"]


def test_submit_wrapper_retires_reserved_exit_row_when_claim_meta_persist_fails_in_live(
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    engine = _make_submit_engine(pos, callback=MagicMock(), mode="LIVE")

    def broken_update(local_order_id, patch):
        raise RuntimeError("meta write failed")

    engine.order_state_machine.update_order_meta = broken_update

    assert wrapped(engine, pos, _decision()) is False
    assert engine.order_state_machine.active_order["status"] == "ERROR"
    assert engine.order_state_machine.active_order["last_error"] == "EXIT_DECISION_LOCAL_EXIT_META_PERSIST_FAILED"
    assert pos.pending_exit_local_order_id == ""


def test_submit_wrapper_retires_reserved_exit_row_when_claim_meta_persist_returns_false_in_live(
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    engine = _make_submit_engine(pos, callback=MagicMock(), mode="LIVE")

    engine.order_state_machine.update_order_meta = lambda local_order_id, patch: False

    assert wrapped(engine, pos, _decision()) is False
    assert engine.order_state_machine.active_order["status"] == "ERROR"
    assert engine.order_state_machine.active_order["last_error"] == "EXIT_DECISION_LOCAL_EXIT_META_PERSIST_FAILED"
    assert pos.pending_exit_local_order_id == ""


def test_submit_wrapper_allows_exact_reserved_exit_request_through_fence(
    generation_claims_table,
    monkeypatch,
) -> None:
    callback_count = 0

    def callback(pos, decision):
        nonlocal callback_count
        callback_count += 1
        pos.exit_in_flight = True
        pos.pending_exit_local_order_id = "exit-local-reserved"
        pos.pending_exit_broker_order_id = "exit-broker-1"
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": "exit-local-reserved",
            "broker_order_id": "exit-broker-1",
        }

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos(pending_exit_local_order_id="exit-local-reserved")
    engine = _make_submit_engine(
        pos,
        callback=callback,
        active_order={
            "client_id": pos.client_id,
            "kind": "EXIT",
            "status": "EXIT_REQUESTED",
            "local_order_id": "exit-local-reserved",
            "broker_order_id": "",
            "position_id": pos.position_id,
            "qty": _decision().quantity,
            "execution_mode": "live",
        },
    )

    assert wrapped(engine, pos, _decision()) is True
    assert callback_count == 1
    assert _claim_rows()[0]["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED


def test_submit_wrapper_claim_loser_retires_distinct_reserved_exit_intent(
    generation_claims_table,
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    monkeypatch.setattr(
        guard,
        "_claim_durable_decision_generation",
        lambda **kwargs: {
            "claimed": False,
            "claim_state": guard._CLAIM_STATE_CLAIMED,
            "local_order_id": "exit-local-winning",
        },
    )
    callback = MagicMock()
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    engine = _make_submit_engine(pos, callback=callback, mode="LIVE")

    assert wrapped(engine, pos, _decision()) is False
    assert callback.call_count == 0
    assert engine.order_state_machine.active_order["status"] == "ERROR"
    assert engine.order_state_machine.active_order["last_error"] == "EXIT_DECISION_GENERATION_DUPLICATE_SUPPRESSED"


def test_submit_wrapper_claim_loser_keeps_shared_reserved_exit_intent(
    generation_claims_table,
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    callback = MagicMock()
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    reserved_local_id = "exit-local-shared"
    pos = _pos(pending_exit_local_order_id=reserved_local_id)
    engine = _make_submit_engine(pos, callback=callback, mode="LIVE")
    engine.order_state_machine.active_order = {
        "client_id": pos.client_id,
        "local_order_id": reserved_local_id,
        "broker_order_id": "",
        "kind": "EXIT",
        "status": "EXIT_REQUESTED",
        "position_id": pos.position_id,
        "contract": pos.option_symbol,
        "qty": 1,
        "execution_mode": "live",
        "meta": {},
    }
    engine.order_state_machine.active_orders_by_position[pos.position_id] = engine.order_state_machine.active_order

    def same_claim(**kwargs):
        engine.order_state_machine.active_order["local_order_id"] = reserved_local_id
        pos.pending_exit_local_order_id = reserved_local_id
        return {
            "claimed": False,
            "claim_state": guard._CLAIM_STATE_CLAIMED,
            "local_order_id": reserved_local_id,
        }

    monkeypatch.setattr(guard, "_ensure_local_exit_intent_row", lambda *args, **kwargs: reserved_local_id)
    monkeypatch.setattr(guard, "_claim_durable_decision_generation", same_claim)

    assert wrapped(engine, pos, _decision()) is False
    assert callback.call_count == 0
    assert engine.order_state_machine.active_order["status"] == "EXIT_REQUESTED"


def test_submit_wrapper_claim_loser_cleanup_refuses_after_submit_evidence(
    generation_claims_table,
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    callback = MagicMock()
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    engine = _make_submit_engine(pos, callback=callback, mode="LIVE")

    def losing_claim(**kwargs):
        engine.order_state_machine.active_order["submitted_ts"] = "2026-07-17T22:00:00+00:00"
        meta = dict(engine.order_state_machine.active_order.get("meta") or {})
        meta["submit_intent_at"] = "2026-07-17T21:59:59+00:00"
        engine.order_state_machine.active_order["meta"] = meta
        return {
            "claimed": False,
            "claim_state": guard._CLAIM_STATE_CLAIMED,
            "local_order_id": "exit-local-winning",
        }

    monkeypatch.setattr(guard, "_claim_durable_decision_generation", losing_claim)

    assert wrapped(engine, pos, _decision()) is False
    assert callback.call_count == 0
    assert engine.order_state_machine.active_order["status"] == "EXIT_REQUESTED"
    assert engine.order_state_machine.active_order["submitted_ts"] == "2026-07-17T22:00:00+00:00"


def test_retire_local_exit_intent_after_no_submit_uses_atomic_osm_retire() -> None:
    pos = _pos()
    engine = _make_submit_engine(pos, callback=MagicMock(), mode="LIVE")
    local_order_id = engine.order_state_machine.create_exit_order(
        position_id=pos.position_id,
        contract=pos.option_symbol,
        symbol=pos.ticker,
        direction=pos.side,
        qty=pos.quantity_remaining,
        local_order_id="exit-local-atomic",
    )
    engine.order_state_machine.active_order["submitted_ts"] = "2026-07-17T22:00:00+00:00"
    meta = dict(engine.order_state_machine.active_order.get("meta") or {})
    meta["submit_intent_at"] = "2026-07-17T21:59:59+00:00"
    engine.order_state_machine.active_order["meta"] = meta

    guard._retire_local_exit_intent_after_no_submit(
        engine,
        local_order_id,
        error_text="EXIT_DECISION_GENERATION_DUPLICATE_SUPPRESSED",
    )

    assert engine.order_state_machine.active_order["status"] == "EXIT_REQUESTED"
    assert engine.order_state_machine.active_order["submitted_ts"] == "2026-07-17T22:00:00+00:00"


def test_engine_reserved_exit_request_predicate_allows_exact_reserved_row() -> None:
    from ap_exit_engine import _is_reserved_local_exit_submit_intent

    pos = _pos(pending_exit_local_order_id="exit-local-reserved", exit_in_flight=False)
    assert _is_reserved_local_exit_submit_intent(
        pos,
        {
            "status": "EXIT_REQUESTED",
            "local_order_id": "exit-local-reserved",
            "broker_order_id": "",
        },
    ) is True
    assert _is_reserved_local_exit_submit_intent(
        pos,
        {
            "status": "EXIT_SUBMITTED",
            "local_order_id": "exit-local-reserved",
            "broker_order_id": "",
        },
    ) is False


def test_submit_wrapper_treats_broker_conn_error_as_ambiguous(generation_claims_table, monkeypatch) -> None:
    callback_count = 0

    def callback(pos, decision):
        nonlocal callback_count
        callback_count += 1
        return {
            "ok": False,
            "accepted": False,
            "status": "ERROR",
            "error": "broker_conn_error:socket reset",
        }

    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(
        lambda engine, pos, decision: bool(
            (engine.on_scale if str(decision.action).upper() == "SCALE_OUT" else engine.on_exit)(pos, decision).get("ok")
        )
    )
    first_pos = _pos()

    assert wrapped(_make_submit_engine(first_pos, callback=callback), first_pos, _decision()) is False
    replay_pos = _pos()
    assert wrapped(_make_submit_engine(replay_pos, callback=callback), replay_pos, _decision()) is False
    assert _claim_rows()[0]["claim_state"] == guard._CLAIM_STATE_AMBIGUOUS
    assert callback_count == 1


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


def test_unsubmitted_identity_mismatch_is_retired_and_next_tick_submits(
    generation_claims_table,
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    pos = _pos(pending_exit_local_order_id="exit-local-bad", exit_in_flight=True)
    callback = MagicMock(return_value={
        "ok": True,
        "accepted": True,
        "status": "EXIT_SUBMITTED",
        "local_order_id": "exit-local-created",
        "broker_order_id": "exit-broker-repaired",
    })
    engine = _make_submit_engine(
        pos,
        callback=callback,
        active_order={
            "client_id": pos.client_id,
            "position_id": pos.position_id,
            "kind": "EXIT",
            "status": "EXIT_REQUESTED",
            "local_order_id": "exit-local-bad",
            "broker_order_id": "",
            "qty": 3,
            "contract": pos.option_symbol,
            "execution_mode": "live",
            "meta": {},
        },
    )
    wrapped = guard.wrap_submit(_invoke_submit_callback)

    assert wrapped(engine, pos, _decision(quantity=1)) is False
    assert engine.order_state_machine.active_order["status"] == "ERROR"
    assert pos.exit_in_flight is False
    assert pos.pending_exit_local_order_id == ""
    assert callback.call_count == 0

    assert wrapped(engine, pos, _decision(quantity=1)) is True
    assert callback.call_count == 1


def test_identity_mismatch_with_submit_evidence_stays_owned_for_reconciliation(
    generation_claims_table,
    monkeypatch,
) -> None:
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    pos = _pos()
    callback = MagicMock()
    engine = _make_submit_engine(
        pos,
        callback=callback,
        active_order={
            "client_id": pos.client_id,
            "position_id": pos.position_id,
            "kind": "EXIT",
            "status": "EXIT_REQUESTED",
            "local_order_id": "exit-local-ambiguous",
            "broker_order_id": "",
            "qty": 3,
            "contract": pos.option_symbol,
            "execution_mode": "live",
            "submitted_ts": None,
            "meta": {"submit_intent_at": "2026-08-08T12:00:00+00:00"},
        },
    )
    wrapped = guard.wrap_submit(_invoke_submit_callback)

    assert wrapped(engine, pos, _decision(quantity=1)) is False
    assert engine.order_state_machine.active_order["status"] == "EXIT_REQUESTED"
    assert pos.exit_in_flight is True
    assert pos.pending_exit_local_order_id == "exit-local-ambiguous"
    callback.assert_not_called()


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


def test_submit_wrapper_generation_read_failure_blocks_live_submit(monkeypatch) -> None:
    callback = MagicMock(return_value={
        "ok": True,
        "accepted": True,
        "status": "EXIT_SUBMITTED",
        "local_order_id": "exit-local-open",
        "broker_order_id": "exit-broker-open",
    })
    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda *_: (_ for _ in ()).throw(RuntimeError("db unavailable")),
    )
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    assert wrapped(_make_submit_engine(pos, callback=callback, mode="LIVE"), pos, _decision()) is False
    assert callback.call_count == 0


def test_submit_wrapper_generation_read_failure_may_fail_open_in_paper(monkeypatch) -> None:
    callback = MagicMock(return_value={
        "ok": True,
        "accepted": True,
        "status": "EXIT_SUBMITTED",
        "local_order_id": "exit-local-open",
        "broker_order_id": "exit-broker-open",
    })
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda *_: (_ for _ in ()).throw(RuntimeError("db unavailable")),
    )
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    assert wrapped(_make_submit_engine(pos, callback=callback, mode="PAPER"), pos, _decision()) is True
    assert callback.call_count == 1


def test_submit_wrapper_different_positions_do_not_crosswire_callback_trace(
    generation_claims_table, monkeypatch
) -> None:
    real_thread_lock = threading.Lock

    class RecordingLock:
        def __init__(self):
            self._lock = real_thread_lock()
            self.entered = 0
            self.max_active = 0
            self.active = 0
            self.enter_positions = []
            self._guard = real_thread_lock()

        def acquire(self, *args, **kwargs):
            return self._lock.acquire(*args, **kwargs)

        def release(self):
            return self._lock.release()

        def __enter__(self):
            self._lock.acquire()
            with self._guard:
                self.active += 1
                self.entered += 1
                self.max_active = max(self.max_active, self.active)
            return self

        def __exit__(self, exc_type, exc, tb):
            with self._guard:
                self.active -= 1
            self._lock.release()
            return False

    lock_created = threading.Event()
    callback_release = threading.Event()
    first_callback_entered = threading.Event()
    callback_count = 0
    callback_count_lock = threading.Lock()
    lock_ids = []

    def callback(pos, decision):
        nonlocal callback_count
        with callback_count_lock:
            callback_count += 1
            index = callback_count
        if index == 1:
            first_callback_entered.set()
            assert callback_release.wait(timeout=2.0)
        local_order_id = str(pos.pending_exit_local_order_id or "").strip()
        assert local_order_id
        pos.exit_in_flight = True
        pos.pending_exit_broker_order_id = f"exit-broker-{pos.position_id}"
        return {
            "ok": True,
            "accepted": True,
            "status": "EXIT_SUBMITTED",
            "local_order_id": local_order_id,
            "broker_order_id": f"exit-broker-{pos.position_id}",
        }

    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda pos, *_: (f"client|{pos.position_id}|3|1", 1),
    )
    created_locks = []

    def fake_thread_lock():
        lock = RecordingLock()
        created_locks.append(lock)
        lock_created.set()
        return lock

    monkeypatch.setattr(guard.threading, "Lock", fake_thread_lock)
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    engine = _make_submit_engine(_pos(position_id="position-anchor"), callback=callback)
    engine.order_state_machine.adopt_broker_owned_exit_request = lambda *args, **kwargs: {
        "disposition": "ADOPTED",
        "adopted": True,
        "status": "EXIT_SUBMITTED",
    }
    pos_a = _pos(position_id="position-a")
    pos_b = _pos(position_id="position-b")
    results = []

    def run_submit(label, pos):
        results.append((label, wrapped(engine, pos, _decision())))
        lock_ids.append(id(engine._ap_exit_submit_callback_lock))

    thread_a = threading.Thread(target=lambda: run_submit("a", pos_a))
    thread_b = threading.Thread(target=lambda: run_submit("b", pos_b))
    thread_a.start()
    assert lock_created.wait(timeout=2.0)
    thread_b.start()
    assert first_callback_entered.wait(timeout=2.0)
    callback_release.set()
    thread_a.join(timeout=2.0)
    thread_b.join(timeout=2.0)

    assert thread_a.is_alive() is False
    assert thread_b.is_alive() is False
    assert len(set(lock_ids)) == 1
    callback_lock = engine._ap_exit_submit_callback_lock
    assert callback_lock in created_locks
    assert callback_lock.entered == 2
    assert callback_lock.max_active == 1
    assert sorted(results) == [("a", True), ("b", True)]
    assert callback_count == 2
    rows = _claim_rows()
    assert [row["claim_state"] for row in rows] == [
        guard._CLAIM_STATE_BROKER_OWNED,
        guard._CLAIM_STATE_BROKER_OWNED,
    ]
    assert [row["generation_key"] for row in rows] == [
        "client|position-a|3|1",
        "client|position-b|3|1",
    ]
    assert [row["local_order_id"] for row in rows] == [
        pos_a.pending_exit_local_order_id,
        pos_b.pending_exit_local_order_id,
    ]
    assert [row["broker_order_id"] for row in rows] == [
        "exit-broker-position-a",
        "exit-broker-position-b",
    ]
    assert [row["last_error"] for row in rows] == [None, None]


def test_submit_wrapper_post_submit_claim_update_failure_does_not_raise(
    generation_claims_table,
    monkeypatch,
) -> None:
    callback = MagicMock(return_value={
        "ok": True,
        "accepted": True,
        "status": "EXIT_SUBMITTED",
        "local_order_id": "exit-local-safe",
        "broker_order_id": "exit-broker-safe",
    })
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    monkeypatch.setattr(
        guard,
        "_update_durable_decision_generation",
        lambda *args, **kwargs: (_ for _ in ()).throw(RuntimeError("post-submit write failed")),
    )
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    assert wrapped(_make_submit_engine(pos, callback=callback), pos, _decision()) is True
    assert callback.call_count == 1
    replay_pos = _pos()
    assert wrapped(_make_submit_engine(replay_pos, callback=callback), replay_pos, _decision()) is False
    assert callback.call_count == 1


def test_client_runner_source_wires_order_state_machine_into_exit_engine() -> None:
    source = (Path(__file__).resolve().parents[1] / "client_runner.py").read_text()
    assert "exit_eng.order_state_machine = self.order_state_machine" in source
    assert "exit_eng.osm = self.order_state_machine" in source


def test_submit_wrapper_claim_acquisition_failure_blocks_live_cross_process_submit(monkeypatch) -> None:
    callback = MagicMock(return_value={
        "ok": True,
        "accepted": True,
        "status": "EXIT_SUBMITTED",
        "local_order_id": "exit-local-live",
        "broker_order_id": "exit-broker-live",
    })
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    monkeypatch.setattr(
        guard,
        "_claim_durable_decision_generation",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("claim unavailable")),
    )
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    first_pos = _pos()
    second_pos = _pos()
    assert wrapped(_make_submit_engine(first_pos, callback=callback, mode="LIVE"), first_pos, _decision()) is False
    assert wrapped(_make_submit_engine(second_pos, callback=callback, mode="LIVE"), second_pos, _decision()) is False
    assert callback.call_count == 0


def test_submit_wrapper_claim_acquisition_failure_may_fail_open_in_paper(monkeypatch) -> None:
    callback = MagicMock(return_value={
        "ok": True,
        "accepted": True,
        "status": "EXIT_SUBMITTED",
        "local_order_id": "exit-local-paper",
        "broker_order_id": "exit-broker-paper",
    })
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    monkeypatch.setattr(
        guard,
        "_claim_durable_decision_generation",
        lambda **kwargs: (_ for _ in ()).throw(RuntimeError("claim unavailable")),
    )
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos(execution_mode="PAPER")
    assert wrapped(_make_submit_engine(pos, callback=callback, mode="PAPER"), pos, _decision()) is True
    assert callback.call_count == 1


def test_submit_wrapper_blocks_when_claim_already_exists(generation_claims_table, monkeypatch) -> None:
    callback = MagicMock()
    monkeypatch.setattr(guard, "_durable_exit_generation", lambda *_: ("client|position|3|1", 1))
    wrapped = guard.wrap_submit(_invoke_submit_callback)
    pos = _pos()
    first_engine = _make_submit_engine(_pos(), callback=MagicMock(return_value={
        "ok": True,
        "accepted": True,
        "status": "EXIT_SUBMITTED",
        "local_order_id": "exit-local-claimed",
        "broker_order_id": "exit-broker-claimed",
    }))
    assert wrapped(first_engine, _pos(), _decision()) is True
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
    schema_module = SimpleNamespace(
        attest_schema=lambda strict=False: {
            "ok": True,
            "missing_tables": [],
            "missing_columns": {},
            "skipped": False,
        }
    )
    monkeypatch.setitem(sys.modules, "ap.schema_attestation", schema_module)
    monkeypatch.setattr(
        lifecycle_guards,
        "_GUARDS",
        (("canonical_exit_fill_truth", "ap.fake_guard", "install", True),),
    )
    monkeypatch.setattr(lifecycle_guards.importlib, "import_module", lambda _name: module)
    manifest = lifecycle_guards.install_trade_lifecycle_guards()
    assert list(manifest) == ["_schema_attestation", "canonical_exit_fill_truth"]
    assert manifest["_schema_attestation"]["status"] == "installed"
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
