from __future__ import annotations

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
    def __init__(self, row=None, *, update_rowcount=1, update_error=None):
        self.row = dict(row) if row is not None else None
        self.update_rowcount = update_rowcount
        self.update_error = update_error
        self.calls = []

    def execute(self, sql, params=()):
        self.calls.append((sql, params))
        normalized = " ".join(sql.split())
        if normalized.startswith("UPDATE orders"):
            if self.update_error:
                raise self.update_error
            requested_position, client_id, local_id, mode, same_position = params
            eligible = bool(
                self.row
                and self.row.get("client_id") == client_id
                and self.row.get("local_order_id") == local_id
                and self.row.get("kind") == "ENTRY"
                and self.row.get("execution_mode") == mode
                and (
                    self.row.get("position_id") in (None, "")
                    or self.row.get("position_id") == same_position
                )
            )
            if eligible and self.update_rowcount:
                self.row["position_id"] = requested_position
            return _Result(rowcount=self.update_rowcount if eligible else 0)
        if normalized.startswith("SELECT client_id"):
            return _Result(row=dict(self.row) if self.row is not None else None)
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
    update_sql, update_params = db.calls[0]
    assert "kind='ENTRY'" in update_sql
    assert "execution_mode=%s" in update_sql
    assert update_params == (
        "canonical-position-1",
        "jason@example.com",
        "entry-local-1",
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


@pytest.mark.parametrize(
    ("overrides", "detail"),
    [
        ({"client_id": ""}, "client_id_missing"),
        ({"local_order_id": ""}, "local_order_id_missing"),
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
    assert failures[0]["reason_code"] == "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN"
