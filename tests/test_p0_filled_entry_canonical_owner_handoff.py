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
        claim_rowcount=None,
        metadata_update_rowcount=1,
        update_error=None,
        position_row=None,
    ):
        self.row = dict(row) if row is not None else None
        if self.row is not None:
            self.row.setdefault("broker_order_id", "entry-broker-1")
            self.row.setdefault("contract", "INTC260810P00098000")
            self.row.setdefault("last_error", None)
            self.row.setdefault("meta", {})
        self.update_rowcount = update_rowcount
        self.claim_rowcount = update_rowcount if claim_rowcount is None else claim_rowcount
        self.metadata_update_rowcount = metadata_update_rowcount
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
                meta_payload,
                client_id,
                local_id,
                broker_id,
                contract,
                mode,
            ) = params
            eligible = bool(
                self.row
                and self.row.get("client_id") == client_id
                and self.row.get("local_order_id") == local_id
                and self.row.get("broker_order_id") == broker_id
                and self.row.get("contract") == contract
                and self.row.get("kind") == "ENTRY"
                and str(self.row.get("execution_mode") or "").strip().lower() == mode
                and self.row.get("position_id") in (None, "")
            )
            if eligible and self.update_rowcount:
                self.row["position_id"] = requested_position
                self.row["meta"] = {
                    **(self.row.get("meta") or {}),
                    **json.loads(meta_payload),
                }
            return _Result(rowcount=self.update_rowcount if eligible else 0)
        if normalized.startswith("UPDATE orders") and "canonical_owner_handoff_standing_stop_state" in normalized:
            if self.update_error:
                raise self.update_error
            meta_payload = params[0]
            client_id, local_id = params[1:3]
            meta_patch = json.loads(meta_payload)
            eligible = bool(
                self.row
                and self.row.get("client_id") == client_id
                and self.row.get("local_order_id") == local_id
                and not str(
                    (self.row.get("meta") or {}).get(
                        "canonical_owner_handoff_standing_stop_state", ""
                    )
                    or ""
                ).strip()
            )
            if eligible and self.claim_rowcount:
                self.row["meta"] = {**(self.row.get("meta") or {}), **meta_patch}
            return _Result(rowcount=self.claim_rowcount if eligible else 0)
        if normalized.startswith("UPDATE orders"):
            if self.update_error:
                raise self.update_error
            if "last_error=CASE" in normalized:
                meta_payload, client_id, local_id = params[:3]
                if self.row and self.row.get("client_id") == client_id and self.row.get("local_order_id") == local_id:
                    if self.metadata_update_rowcount:
                        self.row["meta"] = {
                            **(self.row.get("meta") or {}),
                            **json.loads(meta_payload),
                        }
                        if str(self.row.get("last_error") or "").startswith(
                            (
                                "FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED",
                                "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
                            )
                        ):
                            self.row["last_error"] = None
                return _Result(rowcount=self.metadata_update_rowcount if self.row else 0)
            if "last_error=%s" in normalized:
                last_error = params[0]
                if "meta=" in normalized:
                    meta_payload, client_id, local_id = params[1:4]
                else:
                    meta_payload = None
                    client_id, local_id = params[1:3]
                if self.row and self.row.get("client_id") == client_id and self.row.get("local_order_id") == local_id:
                    if self.metadata_update_rowcount:
                        self.row["last_error"] = last_error
                        if meta_payload is not None:
                            self.row["meta"] = {
                                **(self.row.get("meta") or {}),
                                **json.loads(meta_payload),
                            }
                return _Result(rowcount=self.metadata_update_rowcount if self.row else 0)
            meta_payload, client_id, local_id = params[:3]
            if self.row and self.row.get("client_id") == client_id and self.row.get("local_order_id") == local_id:
                if self.metadata_update_rowcount:
                    self.row["meta"] = {
                        **(self.row.get("meta") or {}),
                        **json.loads(meta_payload),
                    }
            return _Result(rowcount=self.metadata_update_rowcount if self.row else 0)
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


def _adoption_position(
    position_id,
    *,
    client_id="jason@example.com",
    mode="live",
    contract="INTC260810P00098000",
):
    from ap_exit_engine import ManagedPosition

    position = ManagedPosition(
        ticker="INTC",
        option_symbol=contract,
        side="PUT",
        quantity=1,
        entry_price=1.46,
        underlying_entry=98.0,
        underlying_target=100.0,
        underlying_stop=96.0,
    )
    position.position_id = position_id
    position.client_id = client_id
    position.execution_mode = mode
    position.quantity_remaining = 1
    return position


def _install_adoption_positions(engine, positions):
    with engine._lock:
        engine._positions.extend(positions)
        engine._positions_by_id.update({p.position_id: p for p in positions})


def _adopt_exact(
    engine,
    *,
    canonical_position_id="jason-live-canonical",
    client_id="jason@example.com",
    mode="live",
):
    return engine.adopt_canonical_position_identity(
        contract="INTC260810P00098000",
        canonical_position_id=canonical_position_id,
        local_order_id="entry-local-1",
        broker_order_id="entry-broker-1",
        signal_id="signal-1",
        canonical_signal_id="signal-1",
        entry_fill=1.46,
        entry_ts=None,
        execution_mode=mode,
        client_id=client_id,
    )


def _handoff_invariant_counts(
    engine,
    *,
    canonical_position_id="jason-live-canonical",
    client_id="jason@example.com",
    mode="live",
    contract="INTC260810P00098000",
):
    from ap_exit_engine import _classify_canonical_repair_owner_domain

    active = list(engine.active_positions())
    canonical_count = sum(
        str(getattr(position, "position_id", "") or "").strip()
        == canonical_position_id
        and str(getattr(position, "client_id", "") or "").strip().lower()
        == client_id.strip().lower()
        and str(getattr(position, "execution_mode", "") or "").strip().lower()
        == mode.strip().lower()
        and str(getattr(position, "option_symbol", "") or "").strip().upper()
        == contract.strip().upper()
        for position in active
    )
    ambiguous_count = sum(
        _classify_canonical_repair_owner_domain(
            position,
            client_id=client_id,
            execution_mode=mode,
            contract=contract,
        )
        == "IDENTITY_UNPROVEN"
        for position in active
    )
    return canonical_count, ambiguous_count


def test_handoff_marker_update_fences_broker_contract_and_mode(monkeypatch):
    from ap import fill_monitor as fm

    db = _Connection({
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "broker_order_id": "entry-broker-1",
        "contract": "INTC260810P00098000",
        "kind": "ENTRY",
        "execution_mode": "live",
        "position_id": None,
    })
    _install_db(monkeypatch, db)

    assert fm._update_canonical_handoff_order(
        _order(),
        meta_patch={"canonical_owner_handoff_retry_required": True},
        last_error="FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
        operation="test",
    ) is True
    sql, _params = db.calls[-1]
    assert "broker_order_id=%s" in sql
    assert "contract=%s" in sql
    assert "LOWER(BTRIM(COALESCE(execution_mode,'')))=%s" in sql


@pytest.mark.parametrize(
    "overrides",
    [
        {"broker_order_id": "NONE"},
        {"broker_order_id": "0"},
        {"contract": ""},
        {"execution_mode": " LIVE "},
        {"execution_mode": None},
    ],
)
def test_handoff_metadata_writes_reject_incomplete_identity(monkeypatch, overrides):
    from ap import fill_monitor as fm

    db = _Connection(_order())
    _install_db(monkeypatch, db)

    assert fm._update_canonical_handoff_order(
        _order(**overrides),
        meta_patch={"canonical_owner_handoff_retry_required": True},
        last_error="FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN",
        operation="test",
    ) is False
    assert db.calls == []


def test_handoff_last_error_fallback_zero_row_is_failure(monkeypatch):
    from ap import fill_monitor as fm

    db = _Connection(_order(), metadata_update_rowcount=0)
    _install_db(monkeypatch, db)

    assert fm._persist_canonical_handoff_last_error_fallback(
        _order(), "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN"
    ) is False
    assert db.row["last_error"] is None


@pytest.mark.parametrize(
    "last_error",
    [
        "FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED:database_error",
        "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN:owner_missing",
    ],
)
def test_canonical_owner_handoff_recovery_predicate_matches_each_retry_source(last_error):
    from ap import fill_monitor as fm

    order = _order(
        status="FILLED",
        position_id="canonical-position-1",
        last_error=last_error,
        meta={},
    )

    assert fm._is_canonical_owner_handoff_recovery(order) is True
    assert fm._is_canonical_owner_handoff_recovery(
        {**order, "broker_order_id": "N/A"}
    ) is False


def test_canonical_owner_handoff_recovery_includes_durable_pending_marker():
    from ap import fill_monitor as fm

    order = _order(
        status="FILLED",
        position_id="canonical-position-1",
        meta={"canonical_owner_handoff_entry_handoff_proven": False},
    )

    assert fm._is_canonical_owner_handoff_recovery(order) is True
    assert fm._is_canonical_owner_handoff_recovery(
        {
            **order,
            "meta": {"canonical_owner_handoff_entry_handoff_proven": True},
        }
    ) is False


def test_pending_sql_attests_the_same_retry_prefix_constants(monkeypatch):
    from ap import fill_monitor as fm

    captured = {}

    class _PendingResult:
        def fetchall(self):
            return []

    class _PendingConnection:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return _PendingResult()

    @contextmanager
    def _conn():
        yield _PendingConnection()

    monkeypatch.setattr(fm, "conn", _conn)
    monkeypatch.setattr(fm, "run_with_retry", lambda fn: fn())

    assert fm.get_pending_orders("jason@example.com") == []
    assert "last_error" in captured["sql"]
    assert "canonical_owner_handoff_entry_handoff_proven" in captured["sql"]
    assert "status IN ('CANCELED', 'REJECTED', 'EXPIRED', 'ERROR')" in captured["sql"]
    assert "updated_ts >= NOW() - INTERVAL '1 hour'" in captured["sql"]
    assert "UPPER(BTRIM(broker_order_id)) NOT IN" in captured["sql"]
    assert tuple(
        f"{prefix}%" for prefix in fm._CANONICAL_OWNER_HANDOFF_RETRY_ERROR_PREFIXES
    ) == tuple(captured["params"][1:])


def test_terminal_fill_reconciliation_updates_economics_without_status_transition(
    monkeypatch,
):
    from ap import order_state_machine as osm_module
    from ap.order_state_machine import APOrderStateMachine

    row = {
        "client_id": "jason@example.com",
        "local_order_id": "entry-local-1",
        "broker_order_id": "entry-broker-1",
        "status": "CANCELED",
        "filled_qty": 0,
        "fill_price": None,
    }
    captured = {}

    class _Cursor:
        rowcount = 1

    class _Connection:
        def execute(self, sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return _Cursor()

    @contextmanager
    def _conn():
        yield _Connection()

    monkeypatch.setattr(osm_module, "conn", _conn)
    monkeypatch.setattr(osm_module, "run_with_retry", lambda fn: fn())
    osm = object.__new__(APOrderStateMachine)
    osm.client_id = "jason@example.com"
    osm._get_order = lambda _local_id: dict(row)

    assert osm.reconcile_terminal_fill(
        "entry-local-1",
        cumulative_filled=1,
        fill_price=1.46,
        broker_order_id="entry-broker-1",
    ) is True
    assert "status=" not in captured["sql"].split("WHERE", 1)[0]
    assert "status=%s" in captured["sql"]
    assert row["status"] == "CANCELED"


def test_open_position_safe_does_not_replay_terminal_position():
    from ap import fill_monitor as fm

    class _PM:
        def get_position_by_local_order(self, _local_id):
            return {"id": "closed-position", "status": "CLOSED"}

        def open_position(self, **_kwargs):
            raise AssertionError("terminal replay must not open or reseed")

    assert fm._open_position_safe(
        _PM(),
        order=_order(),
        result={"filled_qty": 1, "avg_fill": 1.46},
        plan_id="plan-1",
        signal_id="signal-1",
        local_id="entry-local-1",
    ) is None


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
    assert "meta=COALESCE(meta, '{}'::jsonb)" in update_sql
    assert update_params == (
        "canonical-position-1",
        '{"canonical_owner_handoff_entry_handoff_proven":false}',
        "jason@example.com",
        "entry-local-1",
        "entry-broker-1",
        "INTC260810P00098000",
        "live",
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
        ({"broker_order_id": "NONE"}, "broker_order_id_missing_or_invalid"),
        ({"contract": "", "symbol": ""}, "contract_missing"),
        ({"kind": "EXIT"}, "kind_not_ENTRY"),
        ({"execution_mode": "sandbox"}, "execution_mode_missing_or_invalid"),
        ({"execution_mode": " LIVE "}, "execution_mode_missing_or_invalid"),
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
        [_owner("canonical-position-1"), _owner("broker-repair-ambiguous", mode="")],
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


def test_standing_stop_claim_is_durable_before_broker_call(monkeypatch):
    from ap import fill_monitor as fm

    order = _order()
    db = _Connection(
        {
            "client_id": "jason@example.com",
            "local_order_id": "entry-local-1",
            "broker_order_id": "entry-broker-1",
            "contract": "INTC260810P00098000",
            "kind": "ENTRY",
            "execution_mode": "live",
            "meta": {},
        }
    )
    _install_db(monkeypatch, db)
    observed_states = []

    def _place(**kwargs):
        observed_states.append(kwargs["order"]["meta"][(
            "canonical_owner_handoff_standing_stop_state"
        )])
        return {"outcome": "SUBMITTED", "broker_stop_id": "stop-1"}

    monkeypatch.setattr(fm, "_place_standing_stop_best_effort", _place)

    result = fm._establish_canonical_handoff_standing_stop(
        broker=object(), order=order, qty=1, entry_price=1.46
    )

    assert observed_states == ["SUBMITTING"]
    assert result["state"] == "SUBMITTED"
    assert result["protection_proven"] is True
    assert db.row["meta"]["canonical_owner_handoff_standing_stop_state"] == "SUBMITTED"
    assert db.row["meta"]["canonical_owner_handoff_standing_stop_id"] == "stop-1"


def test_rest_2xx_without_standing_stop_order_id_is_unproven():
    from ap import fill_monitor as fm

    class _Response:
        status_code = 202
        text = "accepted"

        @staticmethod
        def json():
            return {"order": {"status": "accepted"}}

    broker = SimpleNamespace(
        base_url="https://broker.example",
        account_id="acct-1",
        session=SimpleNamespace(post=lambda *args, **kwargs: _Response()),
    )

    result = fm._place_standing_stop_best_effort(
        broker=broker, order=_order(), qty=1, entry_price=1.46
    )

    assert result["outcome"] == "OUTCOME_UNPROVEN"
    assert result["ok"] is False
    assert not result.get("broker_stop_id")


@pytest.mark.parametrize(
    "stop_response",
    [{"success": True}, True, {"id": True, "status": "accepted"}],
)
def test_helper_success_without_standing_stop_order_id_is_unproven(stop_response):
    from ap import fill_monitor as fm

    broker = SimpleNamespace(
        place_stop_order=lambda **kwargs: stop_response,
    )

    result = fm._place_standing_stop_best_effort(
        broker=broker, order=_order(), qty=1, entry_price=1.46
    )

    assert result["outcome"] == "OUTCOME_UNPROVEN"
    assert result["ok"] is False
    assert not result.get("broker_stop_id")


def test_submitted_standing_stop_without_order_id_cannot_prove_protection(monkeypatch):
    from ap import fill_monitor as fm

    order = _order()
    db = _Connection(
        {
            "client_id": "jason@example.com",
            "local_order_id": "entry-local-1",
            "broker_order_id": "entry-broker-1",
            "contract": "INTC260810P00098000",
            "kind": "ENTRY",
            "execution_mode": "live",
            "meta": {},
        }
    )
    _install_db(monkeypatch, db)
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda **_kwargs: {"outcome": "SUBMITTED"},
    )

    result = fm._establish_canonical_handoff_standing_stop(
        broker=object(), order=order, qty=1, entry_price=1.46
    )

    assert result["state"] == "OUTCOME_UNPROVEN"
    assert result["protection_proven"] is False
    assert db.row["meta"]["canonical_owner_handoff_standing_stop_state"] == "OUTCOME_UNPROVEN"


def test_submitted_standing_stop_is_not_called_again(monkeypatch):
    from ap import fill_monitor as fm

    order = _order(
        meta={
            "canonical_owner_handoff_standing_stop_state": "SUBMITTED",
            "canonical_owner_handoff_standing_stop_id": "stop-1",
        }
    )
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda **_kwargs: pytest.fail("SUBMITTED state must not resubmit"),
    )

    result = fm._establish_canonical_handoff_standing_stop(
        broker=object(), order=order, qty=1, entry_price=1.46
    )

    assert result["state"] == "SUBMITTED"
    assert result["protection_proven"] is True


def test_prior_submitting_standing_stop_is_marked_unproven_without_resubmit(monkeypatch):
    from ap import fill_monitor as fm

    order = _order(
        meta={"canonical_owner_handoff_standing_stop_state": "SUBMITTING"}
    )
    db = _Connection(
        {
            "client_id": "jason@example.com",
            "local_order_id": "entry-local-1",
            "broker_order_id": "entry-broker-1",
            "contract": "INTC260810P00098000",
            "kind": "ENTRY",
            "execution_mode": "live",
            "meta": dict(order["meta"]),
        }
    )
    _install_db(monkeypatch, db)
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda **_kwargs: pytest.fail("prior SUBMITTING state must not resubmit"),
    )

    result = fm._establish_canonical_handoff_standing_stop(
        broker=object(), order=order, qty=1, entry_price=1.46
    )

    assert result["state"] == "OUTCOME_UNPROVEN"
    assert result["protection_proven"] is False
    assert db.row["meta"]["canonical_owner_handoff_standing_stop_state"] == "OUTCOME_UNPROVEN"


@pytest.mark.parametrize(
    ("call_result", "expected_state"),
    [
        ({"outcome": "FAILED", "detail_reason": "broker_rejected"}, "FAILED"),
        ({"outcome": "OUTCOME_UNPROVEN", "detail_reason": "timeout"}, "OUTCOME_UNPROVEN"),
    ],
)
def test_standing_stop_failure_outcomes_are_durable_and_not_retried(
    monkeypatch, call_result, expected_state
):
    from ap import fill_monitor as fm

    order = _order()
    db = _Connection(
        {
            "client_id": "jason@example.com",
            "local_order_id": "entry-local-1",
            "broker_order_id": "entry-broker-1",
            "contract": "INTC260810P00098000",
            "kind": "ENTRY",
            "execution_mode": "live",
            "meta": {},
        }
    )
    _install_db(monkeypatch, db)
    calls = []
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda **_kwargs: calls.append(1) or call_result,
    )

    first = fm._establish_canonical_handoff_standing_stop(
        broker=object(), order=order, qty=1, entry_price=1.46
    )
    second = fm._establish_canonical_handoff_standing_stop(
        broker=object(), order=order, qty=1, entry_price=1.46
    )

    assert first["state"] == expected_state
    assert second["state"] == expected_state
    assert first["protection_proven"] is False
    assert second["protection_proven"] is False
    assert calls == [1]


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
    def _trace_gate(
        signal_id,
        ticker,
        gate,
        status,
        reason="",
        score=None,
        iv_rank=None,
        spread_pct=None,
        trigger_price=None,
        contracts=None,
        pnl_pct=None,
    ):
        events.append("trace")

    monkeypatch.setattr(fm, "trace_gate", _trace_gate)
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *a, **k: events.append("pair_cancel"),
    )
    monkeypatch.setattr(fm, "_release_entry_guards", lambda _order: events.append("release"))
    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)


def test_overfill_is_held_before_osm_or_position_mutation(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args: {"status": "FILLED", "filled_qty": 2, "avg_fill": 1.46},
    )
    transition_calls = []
    position_calls = []

    class _OSM:
        def transition(self, *args, **kwargs):
            transition_calls.append((args, kwargs))
            return True

        def increment_retry(self, *_args, **_kwargs):
            return None

    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *args, **kwargs: position_calls.append((args, kwargs)),
    )

    fm.process_pending_order(
        object(),
        _order(qty=1, status="ACKNOWLEDGED"),
        osm=_OSM(),
        pm=SimpleNamespace(),
        exit_engine=SimpleNamespace(),
        runtime_execution_mode="live",
    )

    assert transition_calls == []
    assert position_calls == []


def test_partial_osm_false_blocks_exit_proof_sync(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args: {
            "status": "EXIT_PARTIAL_FILL",
            "filled_qty": 1,
            "avg_fill": 1.25,
        },
    )
    sync_calls = []

    class _OSM:
        def apply_fill_update(self, **_kwargs):
            return False

        def transition(self, *args, **kwargs):
            pytest.fail("same-state partial update should use apply_fill_update")

    monkeypatch.setattr(fm, "_sync_exit_price", lambda *args, **kwargs: sync_calls.append(1))

    fm.process_pending_order(
        object(),
        _order(
            kind="EXIT",
            local_order_id="exit-local-1",
            broker_order_id="exit-broker-1",
            position_id="position-1",
            status="EXIT_PARTIAL_FILL",
            qty=2,
            filled_qty=0,
        ),
        osm=_OSM(),
        runtime_execution_mode="live",
    )

    assert sync_calls == []


def test_terminal_osm_false_does_not_release_entry_guards(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args: {
            "status": "CANCELED",
            "filled_qty": 0,
            "avg_fill": 0.0,
            "reason": "canceled",
        },
    )
    released = []
    monkeypatch.setattr(fm, "_release_entry_guards", lambda _order: released.append(1))

    class _OSM:
        def transition(self, *args, **kwargs):
            return False

    fm.process_pending_order(
        object(),
        _order(status="ACKNOWLEDGED"),
        osm=_OSM(),
        runtime_execution_mode="live",
    )

    assert released == []


@pytest.mark.parametrize("terminal_status", ["CANCELED", "ERROR"])
def test_late_broker_fill_after_local_terminal_status_reaches_reconciliation(
    monkeypatch, terminal_status,
):
    from ap import fill_monitor as fm

    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args: {"status": "FILLED", "filled_qty": 1, "avg_fill": 1.46},
    )
    transition_calls = []
    late_calls = []

    class _OSM:
        def transition(self, *args, **kwargs):
            transition_calls.append((args, kwargs))
            pytest.fail("late fill must not transition a local terminal order")

        def increment_retry(self, *_args, **_kwargs):
            return None

        def update_order_meta(self, *args, **kwargs):
            return True

    monkeypatch.setattr(
        fm,
        "_reconcile_late_broker_fill",
        lambda *args, **kwargs: late_calls.append((args, kwargs)) or True,
    )

    fm.process_pending_order(
        object(),
        _order(status=terminal_status),
        osm=_OSM(),
        pm=SimpleNamespace(),
        exit_engine=SimpleNamespace(),
        runtime_execution_mode="live",
    )

    assert transition_calls == []
    assert late_calls
    assert late_calls[0][1]["mapped"] == "FILLED"


def test_late_entry_fill_materializes_position_without_terminal_transition(monkeypatch):
    from ap import fill_monitor as fm

    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args: {"status": "FILLED", "filled_qty": 1, "avg_fill": 1.46},
    )
    monkeypatch.setattr(fm, "trace_gate", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    open_calls = []
    marker_calls = []
    terminal_fill_calls = []
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *args, **kwargs: open_calls.append((args, kwargs)) or "position-1",
    )
    monkeypatch.setattr(
        fm,
        "_establish_canonical_handoff_standing_stop",
        lambda **kwargs: {"standing_stop_attempted": True, "state": "SUBMITTED", "protection_proven": True},
    )
    monkeypatch.setattr(fm, "_bind_filled_entry_position_id", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(fm, "_verify_canonical_entry_owner", lambda *args, **kwargs: {"ok": True})
    monkeypatch.setattr(fm, "_clear_canonical_owner_handoff_retry", lambda *args, **kwargs: True)

    class _OSM:
        def transition(self, *_args, **_kwargs):
            pytest.fail("late fill must preserve the local terminal status")

        def reconcile_terminal_fill(self, *args, **kwargs):
            terminal_fill_calls.append((args, kwargs))
            return True

        def update_order_meta(self, local_order_id, patch):
            marker_calls.append((local_order_id, patch))
            return True

    fm.process_pending_order(
        object(),
        _order(status="CANCELED"),
        osm=_OSM(),
        pm=SimpleNamespace(),
        exit_engine=SimpleNamespace(),
        runtime_execution_mode="live",
    )

    assert len(open_calls) == 1
    assert terminal_fill_calls[0][1]["cumulative_filled"] == 1
    assert marker_calls[0][1]["late_fill_reconciliation"]["status"] == "RECONCILED"


def test_late_exit_fill_uses_canonical_reconciler_without_terminal_transition(monkeypatch):
    from ap import exit_fill_truth_guard
    from ap import fill_monitor as fm

    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args: {
            "status": "EXIT_PARTIAL_FILL",
            "filled_qty": 1,
            "avg_fill": 1.25,
        },
    )
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    reconciler_calls = []
    monkeypatch.setattr(
        exit_fill_truth_guard,
        "reconcile_confirmed_exit_fill",
        lambda order, result: reconciler_calls.append((order, result))
        or {
            "position_id": "position-1",
            "projection": SimpleNamespace(closed=False),
        },
    )
    partial_calls = []

    class _ExitEngine:
        def note_partial_exit_fill(self, *args, **kwargs):
            partial_calls.append((args, kwargs))

    class _OSM:
        def transition(self, *_args, **_kwargs):
            pytest.fail("late EXIT fill must preserve the local terminal status")

        def update_order_meta(self, *_args, **_kwargs):
            return True

    fm.process_pending_order(
        object(),
        _order(
            kind="EXIT",
            local_order_id="exit-local-1",
            broker_order_id="exit-broker-1",
            position_id="position-1",
            status="CANCELED",
            qty=2,
        ),
        osm=_OSM(),
        exit_engine=_ExitEngine(),
        runtime_execution_mode="live",
    )

    assert reconciler_calls[0][1]["status"] == "EXIT_PARTIAL_FILL"
    assert partial_calls


@pytest.mark.parametrize("kind", ["ENTRY", "EXIT"])
def test_terminal_cumulative_fill_updates_delta_then_terminalizes_remainder(
    monkeypatch, kind
):
    from ap import fill_monitor as fm

    mapped = "PARTIAL_FILL" if kind == "ENTRY" else "EXIT_PARTIAL_FILL"
    transitions = []
    sync_calls = []
    released = []
    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args: {
            "status": mapped,
            "filled_qty": 1,
            "avg_fill": 1.46,
            "terminal_remainder_status": "CANCELED",
            "terminal_remainder_qty": 1,
            "terminal_remainder_reason": "canceled_after_partial_execution",
        },
    )
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_release_entry_guards", lambda order: released.append(order))
    monkeypatch.setattr(fm, "_sync_exit_price", lambda *args, **kwargs: sync_calls.append(1))
    if kind == "ENTRY":
        monkeypatch.setattr(
            fm,
            "_reconcile_entry_fill_economics",
            lambda *args, **kwargs: (True, "position-1"),
        )

    class _OSM:
        def transition(self, *args, **kwargs):
            transitions.append((args, kwargs))
            return True

        def update_order_meta(self, *_args, **_kwargs):
            return True

    fm.process_pending_order(
        object(),
        _order(
            kind=kind,
            status="ACKNOWLEDGED" if kind == "ENTRY" else "EXIT_ACKNOWLEDGED",
            local_order_id=f"{kind.lower()}-local-1",
            broker_order_id=f"{kind.lower()}-broker-1",
            position_id="position-1" if kind == "EXIT" else None,
            qty=2,
        ),
        osm=_OSM(),
        pm=SimpleNamespace() if kind == "ENTRY" else None,
        exit_engine=SimpleNamespace(),
        runtime_execution_mode="live",
    )

    assert [call[0][1] for call in transitions] == [mapped, "CANCELED"]
    assert transitions[0][1]["filled_qty"] == 1
    assert transitions[1][1]["filled_qty"] == 1
    if kind == "ENTRY":
        assert len(released) == 1
    else:
        assert sync_calls == [1]


def test_pair_cancel_false_broker_response_does_not_transition(monkeypatch):
    from ap import fill_monitor as fm
    from ap import signal_pair_manager

    class _PairManager:
        def on_fill(self, **_kwargs):
            return "opposite-local-1"

    transition_calls = []

    class _OSM:
        def get_order(self, _local_id):
            return {"broker_order_id": "opposite-broker-1"}

        def transition(self, *args, **kwargs):
            transition_calls.append((args, kwargs))
            return True

    class _Broker:
        def cancel_order(self, _broker_order_id):
            return {
                "ok": False,
                "status": "unknown",
                "broker_order_id": "opposite-broker-1",
            }

    monkeypatch.setattr(signal_pair_manager, "get_pair_manager", lambda: _PairManager())
    fm._cancel_pair_opposite(
        _order(direction="CALL"),
        _Broker(),
        _OSM(),
    )

    assert transition_calls == []


def test_pair_cancel_unconfirmed_status_does_not_transition(monkeypatch):
    from ap import fill_monitor as fm
    from ap import signal_pair_manager

    class _PairManager:
        def on_fill(self, **_kwargs):
            return "opposite-local-1"

    transition_calls = []

    class _OSM:
        def get_order(self, _local_id):
            return {"broker_order_id": "opposite-broker-1"}

        def transition(self, *args, **kwargs):
            transition_calls.append((args, kwargs))
            return True

    class _Broker:
        def cancel_order(self, _broker_order_id):
            return {
                "ok": True,
                "status": "FILLED",
                "broker_order_id": "opposite-broker-1",
            }

    monkeypatch.setattr(signal_pair_manager, "get_pair_manager", lambda: _PairManager())
    fm._cancel_pair_opposite(
        _order(direction="CALL"),
        _Broker(),
        _OSM(),
    )

    assert transition_calls == []


@pytest.mark.parametrize(
    ("row_mode", "runtime_mode"),
    [
        ("paper", "live"),
        ("live", "paper"),
        (None, "live"),
        (" LIVE ", "live"),
        ("sandbox", "live"),
        ("live", None),
    ],
    ids=[
        "paper-row-live-runtime",
        "live-row-paper-runtime",
        "missing-row-mode",
        "noncanonical-row-mode",
        "malformed-row-mode",
        "missing-runtime-mode",
    ],
)
def test_broker_filled_mode_admission_holds_before_any_side_effect(
    monkeypatch, row_mode, runtime_mode
):
    from ap import fill_monitor as fm

    events = []
    _patch_process_side_effects(monkeypatch, events)
    order = _order(execution_mode=row_mode)

    def _unexpected(name):
        def _call(*_args, **_kwargs):
            pytest.fail(f"{name} must not run before broker fill mode admission")

        return _call

    for name in (
        "_open_position_safe",
        "_establish_canonical_handoff_standing_stop",
        "_seed_exit_engine",
        "_verify_canonical_entry_owner",
    ):
        monkeypatch.setattr(fm, name, _unexpected(name))

    class _OSM:
        def transition(self, *_args, **_kwargs):
            pytest.fail("OSM transition must not run for an unproven fill mode")

        def increment_retry(self, *_args, **_kwargs):
            pytest.fail("mode admission hold must return before retry handling")

    fm.process_pending_order(
        object(),
        order,
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(),
        runtime_execution_mode=runtime_mode,
    )

    assert events == []


@pytest.mark.parametrize("fill_price", [float("nan"), float("inf"), float("-inf")])
def test_broker_nonfinite_fill_price_holds_before_position_engine_or_broker_mutation(
    monkeypatch, fill_price
):
    from ap import fill_monitor as fm

    events = []
    broker_mutations = []
    real_check_order_with_broker = fm.check_order_with_broker
    _patch_process_side_effects(monkeypatch, events)
    monkeypatch.setattr(
        fm, "check_order_with_broker", real_check_order_with_broker
    )

    class _Broker:
        def get_order(self, _broker_order_id):
            return {
                "id": _broker_order_id,
                "status": "FILLED",
                "exec_quantity": 1,
                "avg_fill_price": fill_price,
            }

        def cancel_order(self, *args, **kwargs):
            broker_mutations.append(("cancel", args, kwargs))

        def place_stop_order(self, *args, **kwargs):
            broker_mutations.append(("stop", args, kwargs))

    order = _order()

    def _unexpected(name):
        def _call(*_args, **_kwargs):
            pytest.fail(f"{name} must not run for a non-finite broker fill price")

        return _call

    for name in (
        "_open_position_safe",
        "_establish_canonical_handoff_standing_stop",
        "_cancel_pair_opposite",
        "_seed_exit_engine",
        "_verify_canonical_entry_owner",
    ):
        monkeypatch.setattr(fm, name, _unexpected(name))

    class _OSM:
        def transition(self, *_args, **_kwargs):
            pytest.fail("OSM transition must not run for a non-finite fill price")

        def increment_retry(self, *_args, **_kwargs):
            return None

    fm.process_pending_order(
        _Broker(),
        order,
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(),
        runtime_execution_mode="live",
    )

    assert events == []
    assert broker_mutations == []


@pytest.mark.parametrize("fill_price", [True, False])
def test_broker_boolean_fill_price_is_rejected_before_side_effects(
    monkeypatch, fill_price
):
    from ap import fill_monitor as fm

    events = []
    broker_mutations = []
    real_check_order_with_broker = fm.check_order_with_broker
    _patch_process_side_effects(monkeypatch, events)
    monkeypatch.setattr(
        fm, "check_order_with_broker", real_check_order_with_broker
    )

    class _Broker:
        def get_order(self, _broker_order_id):
            return {
                "id": _broker_order_id,
                "status": "FILLED",
                "exec_quantity": 1,
                "avg_fill_price": fill_price,
            }

        def cancel_order(self, *args, **kwargs):
            broker_mutations.append(("cancel", args, kwargs))

        def place_stop_order(self, *args, **kwargs):
            broker_mutations.append(("stop", args, kwargs))

    def _unexpected(name):
        def _call(*_args, **_kwargs):
            pytest.fail(f"{name} must not run for a boolean broker fill price")

        return _call

    for name in (
        "_open_position_safe",
        "_establish_canonical_handoff_standing_stop",
        "_cancel_pair_opposite",
        "_seed_exit_engine",
        "_verify_canonical_entry_owner",
    ):
        monkeypatch.setattr(fm, name, _unexpected(name))

    class _OSM:
        def transition(self, *_args, **_kwargs):
            pytest.fail("OSM transition must not run for a boolean fill price")

        def increment_retry(self, *_args, **_kwargs):
            return None

    fm.process_pending_order(
        _Broker(),
        _order(),
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(),
        runtime_execution_mode="live",
    )

    assert events == []
    assert broker_mutations == []


@pytest.mark.parametrize(
    "raw_response",
    [None, {}, {"filled_quantity": 1}],
    ids=["none", "empty-dict", "missing-status"],
)
def test_malformed_broker_response_cannot_authorize_db_only_recovery(
    monkeypatch, raw_response
):
    from ap import fill_monitor as fm

    events = []
    broker_mutations = []
    real_check_order_with_broker = fm.check_order_with_broker
    _patch_process_side_effects(monkeypatch, events)
    monkeypatch.setattr(
        fm, "check_order_with_broker", real_check_order_with_broker
    )

    class _Broker:
        def get_order(self, _broker_order_id):
            return raw_response

        def cancel_order(self, *args, **kwargs):
            broker_mutations.append(("cancel", args, kwargs))

        def place_stop_order(self, *args, **kwargs):
            broker_mutations.append(("stop", args, kwargs))

    order = _order(
        status="FILLED",
        position_id=None,
        filled_qty=1,
        fill_price=1.46,
        meta={"canonical_owner_handoff_entry_handoff_proven": False},
    )
    result = fm.check_order_with_broker(_Broker(), order)

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_RESPONSE_MALFORMED"
    assert result["raw"]["_malformed_broker_response"] is True
    assert not fm._broker_poll_unavailable_for_durable_filled_recovery(result)

    def _unexpected(name):
        def _call(*_args, **_kwargs):
            pytest.fail(f"{name} must not run for a malformed broker response")

        return _call

    for name in (
        "_open_position_safe",
        "_establish_canonical_handoff_standing_stop",
        "_cancel_pair_opposite",
        "_seed_exit_engine",
        "_verify_canonical_entry_owner",
    ):
        monkeypatch.setattr(fm, name, _unexpected(name))

    class _OSM:
        def transition(self, *_args, **_kwargs):
            pytest.fail("malformed broker truth must not transition OSM")

        def increment_retry(self, *_args, **_kwargs):
            events.append("retry")

    fm.process_pending_order(
        _Broker(),
        order,
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(),
        runtime_execution_mode="live",
    )

    assert events in ([], ["retry"])
    assert broker_mutations == []


@pytest.mark.parametrize(
    "malformed_quantity",
    [True, "bad", "1.5", float("inf"), float("-inf"), -1, 2**63],
    ids=[
        "bool",
        "malformed-string",
        "fractional-string",
        "inf",
        "-inf",
        "negative",
        "overflow",
    ],
)
def test_malformed_broker_quantity_cannot_authorize_db_only_recovery(
    monkeypatch, malformed_quantity
):
    from ap import fill_monitor as fm

    events = []
    broker_mutations = []
    real_check_order_with_broker = fm.check_order_with_broker
    _patch_process_side_effects(monkeypatch, events)
    monkeypatch.setattr(
        fm, "check_order_with_broker", real_check_order_with_broker
    )

    class _Broker:
        def get_order(self, _broker_order_id):
            return {
                "id": _broker_order_id,
                "status": "FILLED",
                "exec_quantity": malformed_quantity,
                "avg_fill_price": 1.46,
            }

        def cancel_order(self, *args, **kwargs):
            broker_mutations.append(("cancel", args, kwargs))

        def place_stop_order(self, *args, **kwargs):
            broker_mutations.append(("stop", args, kwargs))

    order = _order(
        status="FILLED",
        position_id=None,
        filled_qty=1,
        fill_price=1.46,
        meta={"canonical_owner_handoff_entry_handoff_proven": False},
    )
    result = fm.check_order_with_broker(_Broker(), order)

    assert result["status"] == "ERROR"
    assert result["reason"] == "BROKER_QUANTITY_INVALID"
    assert result["raw"]["exec_quantity"] is malformed_quantity
    assert result["raw"]["_malformed_broker_quantity"] is True
    assert not fm._broker_poll_unavailable_for_durable_filled_recovery(result)

    open_calls = []
    standing_stop_calls = []
    pair_cancel_calls = []
    seed_calls = []
    verify_calls = []

    monkeypatch.setattr(fm, "_open_position_safe", lambda *a, **k: open_calls.append(1))
    monkeypatch.setattr(
        fm,
        "_establish_canonical_handoff_standing_stop",
        lambda **k: standing_stop_calls.append(1),
    )
    monkeypatch.setattr(
        fm, "_cancel_pair_opposite", lambda *a, **k: pair_cancel_calls.append(1)
    )
    monkeypatch.setattr(fm, "_seed_exit_engine", lambda *a, **k: seed_calls.append(1))
    monkeypatch.setattr(
        fm, "_verify_canonical_entry_owner", lambda *a, **k: verify_calls.append(1)
    )

    transition_calls = []

    class _OSM:
        def transition(self, *args, **kwargs):
            transition_calls.append((args, kwargs))

        def increment_retry(self, *_args, **_kwargs):
            return None

    fm.process_pending_order(
        _Broker(),
        order,
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(),
        runtime_execution_mode="live",
    )

    assert transition_calls == []
    assert open_calls == []
    assert seed_calls == []
    assert verify_calls == []
    assert standing_stop_calls == []
    assert pair_cancel_calls == []
    assert broker_mutations == []
    assert events == []


@pytest.mark.parametrize("valid_quantity", [1, "1", 1.0, "1.0"])
def test_valid_integral_broker_quantity_remains_fill_truth(monkeypatch, valid_quantity):
    from ap import fill_monitor as fm

    monkeypatch.setattr(fm, "audit", lambda *a, **k: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *a, **k: None)
    result = fm.check_order_with_broker(
        SimpleNamespace(
            get_order=lambda _broker_order_id: {
                "id": _broker_order_id,
                "status": "FILLED",
                "exec_quantity": valid_quantity,
                "avg_fill_price": 1.46,
            }
        ),
        _order(),
    )

    assert result["status"] == "FILLED"
    assert result["filled_qty"] == 1


def test_explicit_unproven_runtime_mode_cannot_fall_back_to_engine_mode():
    from ap import fill_monitor as fm

    engine = SimpleNamespace(execution_mode="live")
    assert fm._resolve_runtime_execution_mode(
        runtime_execution_mode="", exit_engine=engine
    ) == ""


@pytest.mark.parametrize("entry_price", [float("nan"), float("inf"), float("-inf")])
def test_standing_stop_rejects_nonfinite_entry_price_before_broker_call(entry_price):
    from ap import fill_monitor as fm

    broker_calls = []
    broker = SimpleNamespace(
        place_stop_order=lambda **kwargs: broker_calls.append(kwargs),
    )

    result = fm._place_standing_stop_best_effort(
        broker=broker,
        order=_order(),
        qty=1,
        entry_price=entry_price,
    )

    assert result["outcome"] == "FAILED"
    assert broker_calls == []


def test_process_establishes_stop_before_bind_seed_and_owner_verification(monkeypatch):
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
        runtime_execution_mode="live",
    )

    assert db.row["position_id"] == "canonical-position-1"
    assert events.index("open") < events.index("standing_stop")
    assert events.index("standing_stop") < events.index("bind")
    assert events.index("standing_stop") < events.index("seed")
    assert events.index("seed") < events.index("verify")


def test_process_bind_failure_keeps_stop_attempt_before_bind_and_skips_seed(monkeypatch):
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
    }, update_rowcount=0, claim_rowcount=1)
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
        runtime_execution_mode="live",
    )

    assert failures
    assert failures[0]["reason_code"] == "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN_NO_PROTECTION"
    assert "standing_stop" in events
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
        runtime_execution_mode="live",
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

    class _HandoffLock:
        active = False

        def __enter__(self):
            self.active = True
            return self

        def __exit__(self, *_args):
            self.active = False

    handoff_lock = _HandoffLock()

    def _seed(*args, **kwargs):
        assert handoff_lock.active
        events.append("seed")
        return {"ok": True, "disposition": "SEEDED"}

    def _verify(*args, **kwargs):
        assert handoff_lock.active
        events.append("verify")
        return {"ok": True}

    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        _seed,
    )
    monkeypatch.setattr(
        fm,
        "_verify_canonical_entry_owner",
        _verify,
    )

    class _OSM:
        def transition(self, *args, **kwargs):
            raise AssertionError("terminal FILLED row must not transition again")

    fm.process_pending_order(
        object(),
        order,
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(_lock=handoff_lock),
        runtime_execution_mode="live",
    )

    assert db.row["position_id"] == "canonical-position-1"
    assert db.row["meta"]["canonical_owner_handoff_retry_required"] is False
    assert events.index("open") < events.index("standing_stop") < events.index("seed") < events.index("verify")


@pytest.mark.parametrize(
    ("failure_stage", "last_error"),
    [
        (
            "bind",
            "FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED:database_error",
        ),
        (
            "seed",
            "FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN:owner_lookup_failed",
        ),
    ],
)
def test_restart_reloads_last_error_fallback_and_repairs_without_terminal_transition(
    monkeypatch, failure_stage, last_error
):
    from ap import fill_monitor as fm

    events = []
    _patch_process_side_effects(monkeypatch, events)
    db = _Connection(
        {
            "client_id": "jason@example.com",
            "local_order_id": "entry-local-1",
            "kind": "ENTRY",
            "execution_mode": "live",
            "position_id": "different-position" if failure_stage == "bind" else None,
            "meta": {},
        },
        update_rowcount=0 if failure_stage == "bind" else 1,
        claim_rowcount=1,
    )
    _install_db(monkeypatch, db)

    stop_calls = []
    seed_calls = []
    verify_calls = []
    retry_persistence_failed = {"value": True}

    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *a, **k: events.append("open") or "canonical-position-1",
    )
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda **_kwargs: stop_calls.append(1)
        or {"outcome": "SUBMITTED", "broker_stop_id": "stop-1"},
    )

    def _seed(*_args, **_kwargs):
        seed_calls.append(1)
        if failure_stage == "seed" and len(seed_calls) == 1:
            return {
                "ok": False,
                "reason_code": last_error,
                "detail_reason": "owner_lookup_failed",
            }
        return {"ok": True, "disposition": "SEEDED"}

    monkeypatch.setattr(fm, "_seed_exit_engine", _seed)
    monkeypatch.setattr(
        fm,
        "_verify_canonical_entry_owner",
        lambda *_args, **_kwargs: verify_calls.append(1) or {"ok": True},
    )

    original_update = fm._update_canonical_handoff_order

    def _update(order, **kwargs):
        if (
            kwargs.get("operation") == "retry marker"
            and retry_persistence_failed["value"]
        ):
            return False
        return original_update(order, **kwargs)

    monkeypatch.setattr(fm, "_update_canonical_handoff_order", _update)
    monkeypatch.setattr(
        fm,
        "_persist_canonical_handoff_last_error_fallback",
        lambda _order, reason: db.row.__setitem__("last_error", reason),
    )

    engine = SimpleNamespace(
        clear_canonical_owner_handoff_quarantine=lambda **_kwargs: {"ok": True},
        quarantine_canonical_owner_handoff=lambda **_kwargs: {"ok": True},
    )

    class _OSM:
        transition_calls = 0

        def transition(self, *args, **kwargs):
            self.transition_calls += 1
            if self.transition_calls > 1:
                raise AssertionError("restart recovery must not terminalize FILLED again")
            return True

    osm = _OSM()
    fm.process_pending_order(
        object(),
        _order(),
        osm=osm,
        pm=object(),
        exit_engine=engine,
        runtime_execution_mode="live",
    )
    assert db.row["last_error"].startswith(last_error.split(":", 1)[0])

    retry_persistence_failed["value"] = False
    restarted_order = _order(
        status="FILLED",
        position_id="canonical-position-1",
        last_error=db.row["last_error"],
        meta=dict(db.row.get("meta") or {}),
    )
    if failure_stage == "bind":
        db.row["position_id"] = "canonical-position-1"

    fm.process_pending_order(
        object(),
        restarted_order,
        osm=osm,
        pm=object(),
        exit_engine=engine,
        runtime_execution_mode="live",
    )

    assert osm.transition_calls == 1
    assert len(stop_calls) == 1
    assert db.row["last_error"] is None
    assert db.row["meta"]["canonical_owner_handoff_retry_required"] is False
    assert verify_calls == [1]


@pytest.mark.parametrize("execution_mode", ["live", "paper"])
def test_filled_handoff_uses_durable_db_proof_when_broker_poll_unavailable(
    monkeypatch, execution_mode
):
    from ap import fill_monitor as fm

    events = []
    _patch_process_side_effects(monkeypatch, events)
    order = _order(
        status="FILLED",
        position_id=None,
        filled_qty=1,
        fill_price=1.46,
        meta={"canonical_owner_handoff_entry_handoff_proven": False},
        execution_mode=execution_mode,
    )
    db = _Connection(
        {
            "client_id": "jason@example.com",
            "local_order_id": "entry-local-1",
            "broker_order_id": "entry-broker-1",
            "contract": "INTC260810P00098000",
            "kind": "ENTRY",
            "execution_mode": "live",
            "status": "FILLED",
            "position_id": None,
            "filled_qty": 1,
            "fill_price": 1.46,
            "meta": {"canonical_owner_handoff_entry_handoff_proven": False},
            "execution_mode": execution_mode,
        },
        position_row={
            "id": "canonical-position-1",
            "client_id": "jason@example.com",
            "contract": "INTC260810P00098000",
            "execution_mode": execution_mode,
        },
    )
    _install_db(monkeypatch, db)
    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args: {
            "status": "ERROR",
            "filled_qty": 0,
            "avg_fill": 0.0,
            "reason": "timeout",
            "raw": {
                "status": "ERROR",
                "reason": "timeout",
                "_broker_response_unavailable": True,
            },
        },
    )
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *args, **kwargs: events.append("open") or "canonical-position-1",
    )
    monkeypatch.setattr(
        fm,
        "_establish_canonical_handoff_standing_stop",
        lambda **_kwargs: pytest.fail(
            "DB-only recovery must not submit a standing stop without broker truth"
        ),
    )
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *args, **kwargs: pytest.fail(
            "DB-only recovery must not create a broker cancel mutation"
        ),
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: events.append("seed")
        or {"ok": True, "disposition": "SEEDED"},
    )
    monkeypatch.setattr(
        fm,
        "_verify_canonical_entry_owner",
        lambda *args, **kwargs: events.append("verify") or {"ok": True},
    )

    class _OSM:
        def transition(self, *args, **kwargs):
            raise AssertionError("durable FILLED recovery must not terminalize again")

    class _Lock:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    fm.process_pending_order(
        object(),
        order,
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(_lock=_Lock()),
        runtime_execution_mode=execution_mode,
    )

    assert db.row["position_id"] == "canonical-position-1"
    assert db.row["meta"]["canonical_owner_handoff_entry_handoff_proven"] is True
    assert events.index("open") < events.index("seed") < events.index("verify")


@pytest.mark.parametrize(
    ("overrides", "runtime_execution_mode"),
    [
        ({"execution_mode": "paper"}, "live"),
        ({"execution_mode": "live"}, "paper"),
        ({"execution_mode": None}, "live"),
        ({"execution_mode": " LIVE "}, "live"),
        ({"execution_mode": "sandbox"}, "live"),
        ({"client_id": ""}, "live"),
        ({"local_order_id": ""}, "live"),
        ({"contract": ""}, "live"),
    ],
    ids=[
        "paper-row-in-live-runtime",
        "live-row-in-paper-runtime",
        "null-mode",
        "whitespace-mode",
        "malformed-mode",
        "missing-client",
        "missing-local-order",
        "missing-contract",
    ],
)
def test_durable_db_recovery_holds_without_exact_identity_or_mode(
    monkeypatch, overrides, runtime_execution_mode
):
    from ap import fill_monitor as fm

    events = []
    _patch_process_side_effects(monkeypatch, events)
    order = _order(
        status="FILLED",
        position_id=None,
        filled_qty=1,
        fill_price=1.46,
        meta={"canonical_owner_handoff_entry_handoff_proven": False},
        **overrides,
    )
    db = _Connection(dict(order))
    _install_db(monkeypatch, db)
    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args: {
            "status": "UNKNOWN",
            "filled_qty": 0,
            "avg_fill": 0.0,
            "reason": "broker_timeout",
            "raw": {},
        },
    )

    def _unexpected(name):
        def _call(*_args, **_kwargs):
            pytest.fail(f"{name} must not run for an unproven DB-only replay")

        return _call

    for name in (
        "_open_position_safe",
        "_establish_canonical_handoff_standing_stop",
        "_cancel_pair_opposite",
        "_seed_exit_engine",
        "_verify_canonical_entry_owner",
    ):
        monkeypatch.setattr(fm, name, _unexpected(name))

    class _OSM:
        def increment_retry(self, *_args, **_kwargs):
            return None

    class _Lock:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    fm.process_pending_order(
        object(),
        order,
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(_lock=_Lock()),
        runtime_execution_mode=runtime_execution_mode,
    )

    assert db.row["position_id"] is None
    assert events == []


@pytest.mark.parametrize("fill_price", [float("nan"), float("inf"), float("-inf")])
def test_durable_db_recovery_rejects_nonfinite_fill_price_without_side_effects(
    monkeypatch, fill_price
):
    from ap import fill_monitor as fm

    events = []
    _patch_process_side_effects(monkeypatch, events)
    order = _order(
        status="FILLED",
        position_id=None,
        filled_qty=1,
        fill_price=fill_price,
        meta={"canonical_owner_handoff_entry_handoff_proven": False},
    )
    db = _Connection(dict(order))
    _install_db(monkeypatch, db)
    monkeypatch.setattr(
        fm,
        "check_order_with_broker",
        lambda *_args: {
            "status": "UNKNOWN",
            "filled_qty": 0,
            "avg_fill": 0.0,
            "reason": "broker_timeout",
            "raw": {},
        },
    )

    def _unexpected(*_args, **_kwargs):
        pytest.fail("non-finite DB fill must not reach PM, exit engine, or broker mutation")

    monkeypatch.setattr(fm, "_open_position_safe", _unexpected)
    monkeypatch.setattr(fm, "_establish_canonical_handoff_standing_stop", _unexpected)
    monkeypatch.setattr(fm, "_cancel_pair_opposite", _unexpected)
    monkeypatch.setattr(fm, "_seed_exit_engine", _unexpected)
    monkeypatch.setattr(fm, "_verify_canonical_entry_owner", _unexpected)

    class _OSM:
        def increment_retry(self, *_args, **_kwargs):
            return None

    class _Lock:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    fm.process_pending_order(
        object(),
        order,
        osm=_OSM(),
        pm=object(),
        exit_engine=SimpleNamespace(_lock=_Lock()),
        runtime_execution_mode="live",
    )

    assert db.row["position_id"] is None
    assert events == []


@pytest.mark.parametrize("entry_price", [float("nan"), float("inf"), float("-inf")])
def test_position_manager_rejects_nonfinite_entry_price(entry_price):
    from ap.position_manager import APPositionManager

    manager = APPositionManager("jason@example.com")
    with pytest.raises(ValueError, match="entry_price"):
        manager.open_position(
            plan_id="plan-1",
            signal_id="signal-1",
            ticker="INTC",
            contract="INTC260810P00098000",
            side="PUT",
            qty=1,
            entry_price=entry_price,
            execution_mode="live",
        )


def test_exit_engine_quarantine_is_exactly_fenced_by_client_mode_and_contract():
    from ap_exit_engine import APExitEngine, ManagedPosition

    engine = APExitEngine(None, email="jason@example.com")
    positions = []
    for position_id, client_id, mode, contract in [
        ("jason-live", "jason@example.com", "live", "INTC260810P00098000"),
        ("jason-paper", "jason@example.com", "paper", "INTC260810P00098000"),
        ("jose-live", "jose@example.com", "live", "INTC260810P00098000"),
        ("tradefluence-paper", "tradefluence@example.com", "paper", "INTC260810P00098000"),
        ("jason-live-other-contract", "jason@example.com", "live", "INTC260810C00098000"),
    ]:
        position = ManagedPosition(
            ticker="INTC",
            option_symbol=contract,
            side="PUT",
            quantity=1,
            entry_price=1.46,
            underlying_entry=98.0,
            underlying_target=100.0,
            underlying_stop=96.0,
        )
        position.position_id = position_id
        position.client_id = client_id
        position.execution_mode = mode
        position.quantity_remaining = 1
        positions.append(position)
    with engine._lock:
        engine._positions.extend(positions)
        engine._positions_by_id.update({p.position_id: p for p in positions})

    quarantined = engine.quarantine_canonical_owner_handoff(
        canonical_position_id="jason-live",
        contract="INTC260810P00098000",
        client_id=" JASON@EXAMPLE.COM ",
        execution_mode=" LIVE ",
        reason="owner_cardinality_or_identity_unproven",
    )
    assert quarantined["ok"] is True
    assert quarantined["quarantined_ids"] == ["jason-live"]
    assert {p.position_id for p in engine.active_positions()} == {
        "jason-paper",
        "jose-live",
        "tradefluence-paper",
        "jason-live-other-contract",
    }

    cleared = engine.clear_canonical_owner_handoff_quarantine(
        canonical_position_id="jason-live",
        contract="INTC260810P00098000",
        client_id="jason@example.com",
        execution_mode="live",
    )
    assert cleared["ok"] is True
    assert cleared["cleared_ids"] == ["jason-live"]
    assert {p.position_id for p in engine.active_positions()} == {
        "jason-live",
        "jason-paper",
        "jose-live",
        "tradefluence-paper",
        "jason-live-other-contract",
    }


def test_exit_engine_malformed_quarantine_mode_performs_zero_mutations():
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
    position.position_id = "jason-live"
    position.client_id = "jason@example.com"
    position.execution_mode = "live"
    position.quantity_remaining = 1
    engine.add_position(position)

    result = engine.quarantine_canonical_owner_handoff(
        canonical_position_id="jason-live",
        contract="INTC260810P00098000",
        client_id="jason@example.com",
        execution_mode="sandbox",
    )

    assert result["ok"] is False
    assert result["quarantined_ids"] == []
    assert engine.active_positions() == [position]


@pytest.mark.parametrize(
    ("foreign_client", "foreign_mode", "foreign_contract"),
    [
        ("jose@example.com", "live", "INTC260810P00098000"),
        ("jose@example.com", "", "INTC260810P00098000"),
        ("jose@example.com", "sandbox", "INTC260810P00098000"),
        ("jason@example.com", "paper", "INTC260810P00098000"),
        ("tradefluence", "paper", "INTC260810P00098000"),
        ("jason@example.com", "live", "INTC260810C00098000"),
    ],
)
def test_existing_canonical_adoption_ignores_foreign_same_contract_owner_domain(
    foreign_client, foreign_mode, foreign_contract
):
    from ap_exit_engine import APExitEngine

    engine = APExitEngine(None, email="jason@example.com")
    canonical = _adoption_position("jason-live-canonical")
    foreign = _adoption_position(
        "broker-repair-foreign",
        client_id=foreign_client,
        mode=foreign_mode,
        contract=foreign_contract,
    )
    _install_adoption_positions(engine, [canonical, foreign])

    result = _adopt_exact(engine)

    assert result.disposition == "ALREADY_CANONICAL_REPAIR_REMOVED"
    assert result.adopted is True
    assert foreign in engine._positions
    assert engine._positions_by_id[foreign.position_id] is foreign
    assert getattr(foreign, "adoption_identity_quarantined", False) is False
    assert engine.active_positions() == [canonical, foreign]


@pytest.mark.parametrize(
    "repair_overrides",
    [
        {"mode": ""},
        {"mode": "live-ish"},
    ],
)
def test_ambiguous_same_contract_repair_is_quarantined_before_canonical_seed(
    repair_overrides,
):
    from ap import fill_monitor as fm
    from ap_exit_engine import APExitEngine

    engine = APExitEngine(None, email="jason@example.com")
    repair = _adoption_position(
        "broker-repair-jason-ambiguous-mode",
        **repair_overrides,
    )
    _install_adoption_positions(engine, [repair])

    result = _adopt_exact(engine)

    assert result.disposition == "NO_REPAIR_FOUND"
    assert result.adopted is False
    assert repair not in engine.active_positions()
    assert getattr(repair, "adoption_identity_quarantined", False) is True

    canonical = _adoption_position("jason-live-canonical")
    engine.add_position(canonical)
    verified = fm._verify_canonical_entry_owner(
        engine,
        _order(),
        "jason-live-canonical",
    )
    assert verified["ok"] is True
    assert _handoff_invariant_counts(engine) == (1, 0)


def test_blank_client_same_contract_repair_is_quarantined_without_ownership_guess():
    from ap import fill_monitor as fm
    from ap_exit_engine import APExitEngine

    engine = APExitEngine(None, email="jason@example.com")
    repair = _adoption_position(
        "broker-repair-blank-client",
        client_id="",
        mode="live",
    )
    _install_adoption_positions(engine, [repair])

    result = _adopt_exact(engine)

    assert result.disposition == "NO_REPAIR_FOUND"
    assert repair not in engine.active_positions()
    assert getattr(repair, "client_id", "") == ""
    assert getattr(repair, "execution_mode", "") == "live"
    assert getattr(repair, "adoption_identity_quarantined", False) is True

    canonical = _adoption_position("jason-live-canonical")
    engine.add_position(canonical)
    verified = fm._verify_canonical_entry_owner(
        engine,
        _order(),
        "jason-live-canonical",
    )
    assert verified["ok"] is True
    assert _handoff_invariant_counts(engine) == (1, 0)


def test_existing_canonical_adoption_quarantines_ambiguous_repair_before_success():
    from ap_exit_engine import APExitEngine

    engine = APExitEngine(None, email="jason@example.com")
    canonical = _adoption_position("jason-live-canonical")
    repair = _adoption_position(
        "broker-repair-jason-blank-mode",
        mode="",
    )
    _install_adoption_positions(engine, [canonical, repair])

    assert _handoff_invariant_counts(engine) == (1, 1)
    result = _adopt_exact(engine)

    assert result.disposition == "ALREADY_CANONICAL_REPAIR_REMOVED"
    assert result.adopted is True
    assert repair in engine._positions
    assert repair not in engine.active_positions()
    assert getattr(repair, "adoption_identity_quarantined", False) is True
    assert _handoff_invariant_counts(engine) == (1, 0)


def test_restart_replay_cannot_prove_existing_canonical_with_ambiguous_repairs():
    from ap import fill_monitor as fm
    from ap_exit_engine import APExitEngine

    engine = APExitEngine(None, email="jason@example.com")
    canonical = _adoption_position("jason-live-canonical")
    blank_mode = _adoption_position(
        "broker-repair-jason-replay-blank-mode",
        mode="",
    )
    blank_client = _adoption_position(
        "broker-repair-jason-replay-blank-client",
        client_id="",
        mode="live",
    )
    _install_adoption_positions(engine, [canonical, blank_mode, blank_client])

    before_recovery = fm._verify_canonical_entry_owner(
        engine,
        _order(status="FILLED", position_id="jason-live-canonical"),
        "jason-live-canonical",
    )
    assert before_recovery["ok"] is False
    assert before_recovery["behavior_active_canonical_owner_count"] == 1
    assert before_recovery[
        "behavior_active_ambiguous_same_contract_repair_count"
    ] == 2

    result = _adopt_exact(engine)
    assert result.disposition == "ALREADY_CANONICAL_REPAIR_REMOVED"
    assert result.adopted is True

    after_recovery = fm._verify_canonical_entry_owner(
        engine,
        _order(status="FILLED", position_id="jason-live-canonical"),
        "jason-live-canonical",
    )
    assert after_recovery["ok"] is True
    assert after_recovery["behavior_active_canonical_owner_count"] == 1
    assert after_recovery[
        "behavior_active_ambiguous_same_contract_repair_count"
    ] == 0
    assert _handoff_invariant_counts(engine) == (1, 0)


def test_exact_repair_is_adopted_while_foreign_same_contract_repair_stays_untouched():
    from ap_exit_engine import APExitEngine

    engine = APExitEngine(None, email="jason@example.com")
    canonical = _adoption_position("jason-live-canonical")
    exact_repair = _adoption_position(
        "broker-repair-jason-live",
        client_id="jason@example.com",
        mode="live",
    )
    foreign_repair = _adoption_position(
        "broker-repair-jose-paper",
        client_id="jose@example.com",
        mode="paper",
    )
    _install_adoption_positions(engine, [canonical, exact_repair, foreign_repair])

    result = _adopt_exact(engine)

    assert result.disposition == "ALREADY_CANONICAL_REPAIR_REMOVED"
    assert result.adopted is True
    assert exact_repair.position_id not in engine._positions_by_id
    assert exact_repair not in engine._positions
    assert foreign_repair in engine._positions
    assert getattr(foreign_repair, "adoption_identity_quarantined", False) is False


def test_foreign_repair_before_exact_repair_does_not_veto_adoption():
    from ap_exit_engine import APExitEngine

    engine = APExitEngine(None, email="jason@example.com")
    foreign_repair = _adoption_position(
        "broker-repair-jose-live",
        client_id="jose@example.com",
        mode="live",
    )
    exact_repair = _adoption_position(
        "broker-repair-jason-live",
        client_id="jason@example.com",
        mode="live",
    )
    _install_adoption_positions(engine, [foreign_repair, exact_repair])

    result = _adopt_exact(engine)

    assert result.disposition == "ADOPTED"
    assert result.adopted is True
    assert engine._positions_by_id["jason-live-canonical"] is exact_repair
    assert foreign_repair in engine._positions
    assert getattr(foreign_repair, "adoption_identity_quarantined", False) is False


def test_canonical_seed_can_coexist_with_foreign_same_contract_repair():
    from ap_exit_engine import APExitEngine

    engine = APExitEngine(None, email="jason@example.com")
    foreign_repair = _adoption_position(
        "broker-repair-jose-live",
        client_id="jose@example.com",
        mode="live",
    )
    canonical = _adoption_position(
        "jason-live-canonical",
        client_id="jason@example.com",
        mode="live",
    )
    _install_adoption_positions(engine, [foreign_repair])

    engine.add_position(canonical)

    assert engine._positions == [foreign_repair, canonical]
    assert engine._positions_by_id[canonical.position_id] is canonical
    assert getattr(foreign_repair, "adoption_identity_quarantined", False) is False
