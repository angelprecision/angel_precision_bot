from __future__ import annotations

import json
import os
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("AP_ENV", "test")


def _order(**overrides):
    value = {
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "broker_order_id": "entry-broker-1",
        "kind": "ENTRY",
        "symbol": "INTC",
        "contract": "INTC260810P00098000",
        "direction": "PUT",
        "qty": 1,
        "execution_mode": "live",
        "plan_id": "plan-1",
        "signal_id": "signal-1",
        "tier": "A",
        "score": 80,
        "pattern": "daily",
        "filled_qty": 0,
        "status": "ACKNOWLEDGED",
    }
    value.update(overrides)
    return value


class _Result:
    def __init__(self, *, rowcount=0, row=None):
        self.rowcount = rowcount
        self._row = row

    def fetchone(self):
        return self._row


class _Connection:
    def __init__(
        self,
        row=None,
        *,
        update_rowcount=1,
        update_error=None,
        position_row=None,
    ):
        self.row = dict(row) if row is not None else None
        if self.row is not None:
            self.row.setdefault("broker_order_id", "entry-broker-1")
            self.row.setdefault("contract", "INTC260810P00098000")
        self.update_rowcount = update_rowcount
        self.update_error = update_error
        self.position_row = (
            dict(position_row)
            if position_row is not None
            else {
                "id": "canonical-position-1",
                "client_id": "jason@example.com",
                "contract": "INTC260810P00098000",
                "execution_mode": "live",
            }
        )
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        normalized = " ".join(sql.split())
        if normalized.startswith("SELECT id, client_id"):
            position_id, client_id = params
            if (
                self.position_row is None
                or self.position_row.get("id") != position_id
                or self.position_row.get("client_id") != client_id
            ):
                return _Result(row=None)
            return _Result(row=dict(self.position_row))
        if normalized.startswith("UPDATE orders") and "SET position_id=%s" in normalized:
            if self.update_error:
                raise self.update_error
            (
                requested_position,
                client_id,
                local_id,
                broker_id,
                contract,
                mode,
                same_position,
            ) = params
            eligible = bool(
                self.row
                and self.row.get("client_id") == client_id
                and self.row.get("local_order_id") == local_id
                and self.row.get("broker_order_id") == broker_id
                and self.row.get("contract") == contract
                and self.row.get("kind") == "ENTRY"
                and str(self.row.get("execution_mode") or "").strip().lower() == mode
                and (
                    self.row.get("position_id") in (None, "")
                    or self.row.get("position_id") == same_position
                )
            )
            if eligible and self.update_rowcount:
                self.row["position_id"] = requested_position
            return _Result(rowcount=self.update_rowcount if eligible else 0)
        if normalized.startswith("UPDATE orders"):
            if self.update_error:
                raise self.update_error
            if "last_error=CASE" in normalized:
                meta_payload, client_id, local_id = params
                if self.row and self.row.get("client_id") == client_id and self.row.get("local_order_id") == local_id:
                    self.row["meta"] = {
                        **(self.row.get("meta") or {}),
                        **json.loads(meta_payload),
                    }
                return _Result(rowcount=1 if self.row else 0)
            if "last_error=%s" in normalized:
                last_error, meta_payload, client_id, local_id = params
                if self.row and self.row.get("client_id") == client_id and self.row.get("local_order_id") == local_id:
                    self.row["last_error"] = last_error
                    self.row["meta"] = {
                        **(self.row.get("meta") or {}),
                        **json.loads(meta_payload),
                    }
                return _Result(rowcount=1 if self.row else 0)
            meta_payload, client_id, local_id = params
            if self.row and self.row.get("client_id") == client_id and self.row.get("local_order_id") == local_id:
                self.row["meta"] = {
                    **(self.row.get("meta") or {}),
                    **json.loads(meta_payload),
                }
            return _Result(rowcount=1 if self.row else 0)
        if normalized.startswith("SELECT client_id"):
            if self.row is None:
                return _Result(row=None)
            client_id, local_id, broker_id, contract = params
            if (
                self.row.get("client_id") != client_id
                or self.row.get("local_order_id") != local_id
                or self.row.get("broker_order_id") != broker_id
                or self.row.get("contract") != contract
            ):
                return _Result(row=None)
            return _Result(row=dict(self.row))
        raise AssertionError(f"unexpected SQL: {sql}")


def _install_db(monkeypatch, db):
    from ap import fill_monitor as fm

    @contextmanager
    def _conn():
        yield db

    monkeypatch.setattr(fm, "conn", _conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda fn: fn())


def _owner(position_id, *, client_id="jason@example.com", mode="live", contract=None, closed=False):
    return SimpleNamespace(
        position_id=position_id,
        client_id=client_id,
        execution_mode=mode,
        option_symbol=contract or "INTC260810P00098000",
        closed=closed,
    )


def test_filled_entry_binds_null_position_id_and_reads_back(monkeypatch):
    from ap import fill_monitor as fm

    db = _Connection({
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "kind": "ENTRY",
        "execution_mode": "live",
        "position_id": None,
    })
    _install_db(monkeypatch, db)

    result = fm._bind_filled_entry_position_id(_order(), "canonical-position-1")

    assert result["ok"] is True
    assert result["disposition"] == "BOUND"
    assert db.row["position_id"] == "canonical-position-1"
    update_sql, update_params = next(
        (sql, params)
        for sql, params in db.calls
        if "SET position_id=%s" in sql
    )
    assert "kind='ENTRY'" in update_sql
    assert "LOWER(BTRIM(COALESCE(execution_mode,'')))=%s" in update_sql
    assert update_params == (
        "canonical-position-1",
        "jason@example.com",
        "entry-local-1",
        "entry-broker-1",
        "INTC260810P00098000",
        "live",
        "canonical-position-1",
    )


def test_zero_row_already_same_position_is_idempotent(monkeypatch):
    from ap import fill_monitor as fm

    db = _Connection({
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "kind": "ENTRY",
        "execution_mode": "live",
        "position_id": "canonical-position-1",
    }, update_rowcount=0)
    _install_db(monkeypatch, db)

    result = fm._bind_filled_entry_position_id(_order(), "canonical-position-1")

    assert result["ok"] is True
    assert result["disposition"] == "ALREADY_BOUND"


def test_different_existing_position_id_is_conflict_without_overwrite(monkeypatch):
    from ap import fill_monitor as fm

    db = _Connection({
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "kind": "ENTRY",
        "execution_mode": "live",
        "position_id": "other-position",
    }, update_rowcount=0)
    _install_db(monkeypatch, db)

    result = fm._bind_filled_entry_position_id(_order(), "canonical-position-1")

    assert result["ok"] is False
    assert result["reason_code"] == "FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED"
    assert result["detail_reason"] == "POSITION_IDENTITY_CONFLICT"
    assert result["existing_position_id"] == "other-position"
    assert db.row["position_id"] == "other-position"


def test_bind_fences_exact_broker_and_contract_identity(monkeypatch):
    from ap import fill_monitor as fm

    db = _Connection({
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "broker_order_id": "a-different-broker-order",
        "kind": "ENTRY",
        "execution_mode": "live",
        "position_id": None,
    })
    _install_db(monkeypatch, db)

    result = fm._bind_filled_entry_position_id(_order(), "canonical-position-1")

    assert result["ok"] is False
    assert result["detail_reason"] == "order_row_missing_after_bind"
    assert db.row["position_id"] is None


def test_bind_fails_closed_on_position_identity_mismatch(monkeypatch):
    from ap import fill_monitor as fm

    db = _Connection(
        {
            "client_id": "jason@example.com",
            "local_order_id": "entry-local-1",
            "kind": "ENTRY",
            "execution_mode": "live",
            "position_id": None,
        },
        position_row={
            "id": "canonical-position-1",
            "client_id": "jason@example.com",
            "contract": "INTC260810C00098000",
            "execution_mode": "live",
        },
    )
    _install_db(monkeypatch, db)

    result = fm._bind_filled_entry_position_id(_order(), "canonical-position-1")

    assert result["ok"] is False
    assert result["detail_reason"] == "canonical_position_identity_mismatch"
    assert db.row["position_id"] is None


@pytest.mark.parametrize(
    ("overrides", "detail"),
    [
        ({"client_id": ""}, "client_id_missing"),
        ({"local_order_id": ""}, "local_order_id_missing"),
        ({"broker_order_id": ""}, "broker_order_id_missing_or_invalid"),
        ({"contract": "", "symbol": ""}, "contract_missing"),
        ({"kind": "EXIT"}, "kind_not_ENTRY"),
        ({"execution_mode": "sandbox"}, "execution_mode_missing_or_invalid"),
    ],
)
def test_bind_rejects_incomplete_identity_before_database_write(monkeypatch, overrides, detail):
    from ap import fill_monitor as fm

    db = _Connection(None)
    _install_db(monkeypatch, db)

    result = fm._bind_filled_entry_position_id(
        _order(**overrides), "canonical-position-1"
    )

    assert result["ok"] is False
    assert result["detail_reason"] == detail
    assert db.calls == []


def test_bind_rejects_missing_canonical_position_id_without_database_write(monkeypatch):
    from ap import fill_monitor as fm

    db = _Connection(None)
    _install_db(monkeypatch, db)

    result = fm._bind_filled_entry_position_id(_order(), "")

    assert result["ok"] is False
    assert result["detail_reason"] == "canonical_position_id_missing"
    assert db.calls == []


def test_bind_fails_closed_when_row_disappears_before_readback(monkeypatch):
    from ap import fill_monitor as fm

    db = _Connection(None, update_rowcount=0)
    _install_db(monkeypatch, db)

    result = fm._bind_filled_entry_position_id(_order(), "canonical-position-1")

    assert result["ok"] is False
    assert result["detail_reason"] == "order_row_missing_after_bind"


def test_bind_fails_closed_on_database_error(monkeypatch):
    from ap import fill_monitor as fm

    db = _Connection(
        {
            "client_id": "jason@example.com",
            "local_order_id": "entry-local-1",
            "kind": "ENTRY",
            "execution_mode": "live",
            "position_id": None,
        },
        update_error=RuntimeError("schema unavailable"),
    )
    _install_db(monkeypatch, db)

    result = fm._bind_filled_entry_position_id(_order(), "canonical-position-1")

    assert result["ok"] is False
    assert result["detail_reason"] == "database_error"
    assert result["exception_type"] == "RuntimeError"


def test_owner_verification_requires_one_exact_canonical_owner(monkeypatch):
    from ap import fill_monitor as fm

    engine = SimpleNamespace(
        active_positions=lambda: [_owner("canonical-position-1")]
    )

    result = fm._verify_canonical_entry_owner(
        engine, _order(), "canonical-position-1"
    )

    assert result["ok"] is True
    assert result["disposition"] == "CANONICAL_OWNER_PROVEN"


@pytest.mark.parametrize(
    "owners",
    [
        [_owner("canonical-position-1"), _owner("duplicate-position")],
        [_owner("canonical-position-1"), _owner("broker-repair-jason")],
        [_owner("wrong-position")],
        [_owner("canonical-position-1", mode="paper")],
        [_owner("canonical-position-1", client_id="other@example.com")],
    ],
)
def test_owner_verification_fails_on_duplicate_or_wrong_identity(owners):
    from ap import fill_monitor as fm

    engine = SimpleNamespace(active_positions=lambda: owners)

    result = fm._verify_canonical_entry_owner(
        engine, _order(), "canonical-position-1"
    )

    assert result["ok"] is False
    assert result["reason_code"] == "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN"


def test_seed_exit_engine_returns_structured_success_result(monkeypatch):
    from ap import fill_monitor as fm
    from ap_exit_engine import CanonicalAdoptionResult

    class _Engine:
        def __init__(self):
            self.seeded = []

        def _resolved_execution_mode_detail(self):
            return {"mode": "live", "status": "PROVEN", "sources": {}}

        def adopt_canonical_position_identity(self, **_kwargs):
            return CanonicalAdoptionResult(
                disposition="NO_REPAIR_FOUND",
                adopted=False,
                safe_to_seed=True,
                retryable=False,
            )

        def get_position(self, _position_id):
            return None

        def seed_position(self, position_id, order, result):
            self.seeded.append((position_id, order, result))

    engine = _Engine()
    result = fm._seed_exit_engine(
        engine,
        "canonical-position-1",
        _order(),
        {"filled_qty": 1, "avg_fill": 1.46},
        "signal-1",
    )

    assert result["ok"] is True
    assert result["disposition"] == "SEEDED"
    assert len(engine.seeded) == 1


def test_seed_exit_engine_returns_structured_failure_for_retry(monkeypatch):
    from ap import fill_monitor as fm
    from ap_exit_engine import CanonicalAdoptionResult

    class _Engine:
        def _resolved_execution_mode_detail(self):
            return {"mode": "live", "status": "PROVEN", "sources": {}}

        def adopt_canonical_position_identity(self, **_kwargs):
            return CanonicalAdoptionResult(
                disposition="RETRY_REPAIR_IDENTITY_UNPROVEN",
                adopted=False,
                safe_to_seed=False,
                retryable=True,
                reason="active_count=2",
            )

        def active_positions(self):
            return []

    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)

    result = fm._seed_exit_engine(
        _Engine(),
        "canonical-position-1",
        _order(),
        {"filled_qty": 1, "avg_fill": 1.46},
        "signal-1",
    )

    assert result["ok"] is False
    assert result["reason_code"] == "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN"
    assert result["adoption_disposition"] == "RETRY_REPAIR_IDENTITY_UNPROVEN"


def _patch_process_side_effects(monkeypatch, events):
    from ap import fill_monitor as fm

    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda _broker, _order: {
            "status": "FILLED",
            "filled_qty": 1,
            "avg_fill": 1.46,
        },
    )
    monkeypatch.setattr(fm, "trace_gate", lambda *a, **k: events.append("trace"))
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *a, **k: events.append("pair_cancel"),
    )
    monkeypatch.setattr(fm, "_release_entry_guards", lambda _order: events.append("release"))
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)


def test_process_binds_before_stop_seed_and_owner_verification(monkeypatch):
    from ap import fill_monitor as fm

    events = []
    _patch_process_side_effects(monkeypatch, events)
    db = _Connection({
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "kind": "ENTRY",
        "execution_mode": "live",
        "position_id": None,
    })
    _install_db(monkeypatch, db)

    engine = SimpleNamespace(active_positions=lambda: [_owner("canonical-position-1")])
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *a, **k: events.append("open") or "canonical-position-1",
    )

    original_bind = fm._bind_filled_entry_position_id

    def _bind(*args, **kwargs):
        events.append("bind")
        return original_bind(*args, **kwargs)

    monkeypatch.setattr(fm, "_bind_filled_entry_position_id", _bind)
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda *a, **k: events.append("standing_stop"),
    )

    def _seed(*args, **kwargs):
        events.append("seed")
        return {"ok": True, "disposition": "SEEDED"}

    monkeypatch.setattr(fm, "_seed_exit_engine", _seed)
    monkeypatch.setattr(
        fm,
        "_verify_canonical_entry_owner",
        lambda *a, **k: events.append("verify") or {"ok": True},
    )

    class _OSM:
        def transition(self, *args, **kwargs):
            events.append("transition")
            return True

    fm.process_pending_order(
        object(),
        _order(),
        osm=_OSM(),
        pm=object(),
        exit_engine=engine,
    )

    assert db.row["position_id"] == "canonical-position-1"
    assert events.index("open") < events.index("bind")
    assert events.index("bind") < events.index("standing_stop")
    assert events.index("standing_stop") < events.index("seed")
    assert events.index("seed") < events.index("verify")


def test_process_bind_failure_emits_and_skips_stop_and_seed(monkeypatch):
    from ap import fill_monitor as fm

    events = []
    failures = []
    _patch_process_side_effects(monkeypatch, events)
    db = _Connection({
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "kind": "ENTRY",
        "execution_mode": "live",
        "position_id": "different-position",
    }, update_rowcount=0)
    _install_db(monkeypatch, db)
    monkeypatch.setattr(
        fm,
        "_emit_canonical_owner_handoff_failure",
        lambda _order, _position_id, result: failures.append(result),
    )
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *a, **k: "canonical-position-1",
    )
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda *a, **k: events.append("standing_stop"),
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *a, **k: events.append("seed") or {"ok": True},
    )

    class _OSM:
        def transition(self, *args, **kwargs):
            return True

    fm.process_pending_order(
        object(),
        _order(),
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(),
    )

    assert failures
    assert failures[0]["reason_code"] == "FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED"
    assert "standing_stop" not in events
    assert "seed" not in events
    assert db.row["position_id"] == "different-position"


def test_process_owner_ambiguity_is_visible_after_seed(monkeypatch):
    from ap import fill_monitor as fm

    events = []
    failures = []
    quarantines = []
    _patch_process_side_effects(monkeypatch, events)
    db = _Connection({
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "kind": "ENTRY",
        "execution_mode": "live",
        "position_id": None,
    })
    _install_db(monkeypatch, db)
    monkeypatch.setattr(
        fm,
        "_emit_canonical_owner_handoff_failure",
        lambda _order, _position_id, result: failures.append(result),
    )
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *a, **k: "canonical-position-1",
    )
    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", lambda *a, **k: None)
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *a, **k: {"ok": True, "disposition": "SEEDED"},
    )
    monkeypatch.setattr(
        fm,
        "_verify_canonical_entry_owner",
        lambda *a, **k: {
            "ok": False,
            "reason_code": "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
            "detail_reason": "owner_cardinality_or_identity_unproven",
        },
    )

    engine = SimpleNamespace(
        quarantine_canonical_owner_handoff=lambda **kwargs: quarantines.append(kwargs),
    )

    class _OSM:
        def transition(self, *args, **kwargs):
            return True

    fm.process_pending_order(
        object(),
        _order(),
        osm=_OSM(),
        pm=object(),
        exit_engine=engine,
    )

    assert failures
    assert failures[0]["reason_code"] == "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN"
    assert quarantines


def test_filled_handoff_retry_replays_without_terminal_osm_transition(monkeypatch):
    from ap import fill_monitor as fm

    events = []
    _patch_process_side_effects(monkeypatch, events)
    order = _order(
        status="FILLED",
        position_id=None,
        meta={
            "canonical_owner_handoff_retry_required": True,
            "canonical_owner_handoff_standing_stop_attempted": False,
        },
    )
    db = _Connection({
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "kind": "ENTRY",
        "execution_mode": "live",
        "position_id": None,
        "meta": dict(order["meta"]),
    })
    _install_db(monkeypatch, db)
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *a, **k: events.append("open") or "canonical-position-1",
    )
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda *a, **k: events.append("standing_stop") or True,
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *a, **k: events.append("seed") or {"ok": True, "disposition": "SEEDED"},
    )
    monkeypatch.setattr(
        fm,
        "_verify_canonical_entry_owner",
        lambda *a, **k: events.append("verify") or {"ok": True},
    )

    class _OSM:
        def transition(self, *args, **kwargs):
            raise AssertionError("terminal FILLED row must not transition again")

    fm.process_pending_order(
        object(),
        order,
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(),
    )

    assert db.row["position_id"] == "canonical-position-1"
    assert db.row["meta"]["canonical_owner_handoff_retry_required"] is False
    assert events.index("open") < events.index("standing_stop") < events.index("seed") < events.index("verify")


def test_exit_engine_quarantines_and_releases_same_contract_on_retry():
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine(None, email="jason@example.com")
    position = ManagedPosition(
        ticker="INTC",
        option_symbol="INTC260810P00098000",
        side="PUT",
        quantity=1,
        entry_price=1.46,
        underlying_entry=98.0,
        underlying_target=100.0,
        underlying_stop=96.0,
    )
    position.position_id = "canonical-position-1"
    position.client_id = "jason@example.com"
    position.execution_mode = "live"
    position.quantity_remaining = 1
    engine.add_position(position)

    quarantined = engine.quarantine_canonical_owner_handoff(
        canonical_position_id="canonical-position-1",
        contract="INTC260810P00098000",
        client_id="jason@example.com",
        execution_mode="live",
        reason="owner_cardinality_or_identity_unproven",
    )
    assert quarantined["quarantined_ids"] == ["canonical-position-1"]
    assert engine.active_positions() == []

    cleared = engine.clear_canonical_owner_handoff_quarantine(
        canonical_position_id="canonical-position-1",
        contract="INTC260810P00098000",
        client_id="jason@example.com",
        execution_mode="live",
    )
    assert cleared["cleared_ids"] == ["canonical-position-1"]
    assert engine.active_positions() == [position]
