from __future__ import annotations

import importlib
import logging
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

REPO_ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
sys.path.insert(0, str(REPO_ROOT))

FIXED_ET = datetime(2026, 6, 12, 9, 15, tzinfo=ZoneInfo("America/New_York"))


class _FakeOrderStateMachine:
    def __init__(
        self,
        *,
        cleanup_succeeds: bool = True,
        client_id: str = "client-1",
        canonical_signal_id: str = "CANON-001",
    ):
        self.orders: dict[str, dict] = {}
        self.create_calls = 0
        self.cleanup_succeeds = cleanup_succeeds
        self.client_id = client_id
        self.canonical_signal_id = canonical_signal_id
        self._created_seq = 0
        self.expire_calls: list[tuple[str, str]] = []
        self.cancel_calls: list[tuple[str, str]] = []
        self.transition_calls: list[tuple[str, str, dict]] = []
        self.get_order_calls: list[str] = []

    def create_entry_order(self, plan, initial_status="CREATED", execution_mode=None, **kwargs):
        self.create_calls += 1
        self._created_seq += 1
        local_order_id = f"local-ord-{self.create_calls}"
        # Production-shaped ENTRY order row so the DB fake can answer the
        # resolver's orders-table fences (active + latest-by-created_ts).
        self.orders[local_order_id] = {
            "local_order_id": local_order_id,
            "client_id": self.client_id,
            "canonical_signal_id": self.canonical_signal_id,
            "kind": "ENTRY",
            "status": initial_status,
            "execution_mode": execution_mode,
            "created_ts": self._created_seq,
            "contract": getattr(plan, "contract_symbol", None),
            "meta": kwargs.get("meta") or {},
        }
        return local_order_id

    def mark_entry_pending_trigger(self, local_order_id: str) -> bool:
        self.orders.setdefault(local_order_id, {})["status"] = "PENDING_TRIGGER"
        return True

    def get_order(self, local_order_id: str) -> dict:
        self.get_order_calls.append(local_order_id)
        return dict(self.orders.get(local_order_id) or {})

    def expire_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.expire_calls.append((local_order_id, reason))
        if not self.cleanup_succeeds:
            return False
        self.orders.setdefault(local_order_id, {})["status"] = "EXPIRED"
        self.orders[local_order_id]["last_error"] = reason
        return True

    def cancel_pending_entry(self, local_order_id: str, *, reason: str = "") -> bool:
        self.cancel_calls.append((local_order_id, reason))
        if not self.cleanup_succeeds:
            return False
        self.orders.setdefault(local_order_id, {})["status"] = "CANCELED"
        self.orders[local_order_id]["last_error"] = reason
        return True

    def transition(self, local_order_id: str, new_status: str, **kwargs) -> bool:
        self.transition_calls.append((local_order_id, new_status, kwargs))
        if not self.cleanup_succeeds:
            return False
        self.orders.setdefault(local_order_id, {})["status"] = new_status
        self.orders[local_order_id].update(kwargs)
        return True


class _FakeDBConn:
    """In-memory shim for ap.db.conn.

    Backs client_signal_opportunities against a shared row store (the fake
    ledger's `rows`, keyed by (canonical_signal_id, client_id)) so the PR #404
    atomic claim/bind/complete path — which runs SELECT ... FOR UPDATE and
    UPDATE ... RETURNING metadata against ap.db.conn — sees durable metadata
    that persists across repeated reeval runs. Any SQL not targeting
    client_signal_opportunities (e.g. the orders table) returns no rows, exactly
    as the prior trivial stub did.
    """

    # Active-ownership statuses the resolver's active fence filters on.
    _ACTIVE_ORDER_STATUSES = {
        "CREATED", "PENDING_TRIGGER", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL",
    }

    def __init__(self, opp_rows: dict | None = None, orders: dict | None = None):
        self._rows = opp_rows if opp_rows is not None else {}
        self._orders = orders if orders is not None else {}
        self._result: list[dict] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def _row_for_key(self, canonical, client_id):
        row = self._rows.get((canonical, client_id))
        if row is not None and not row.get("id"):
            row["id"] = f"{canonical}::{client_id}"
        return row

    def _row_for_id(self, row_id):
        for row in self._rows.values():
            if row.get("id") == row_id:
                return row
        return None

    def _orders_query(self, s, params):
        # Production-shaped ENTRY fences:
        #   active: WHERE client_id AND kind='ENTRY' AND mode AND canonical
        #           AND status IN (...) ORDER BY created_ts DESC LIMIT 1
        #   latest: WHERE client_id AND mode AND canonical AND kind='ENTRY'
        #           ORDER BY created_ts DESC LIMIT 1
        client_id = params[0] if len(params) > 0 else None
        mode = str(params[1] or "").strip().lower() if len(params) > 1 else ""
        canonical = params[2] if len(params) > 2 else None
        status_filter = None
        if "status in (" in s:
            status_filter = {str(p).upper() for p in params[3:]}

        matches = []
        for row in self._orders.values():
            if str(row.get("client_id") or "") != str(client_id or ""):
                continue
            if str(row.get("kind") or "") != "ENTRY":
                continue
            if str(row.get("execution_mode") or "").strip().lower() != mode:
                continue
            if str(row.get("canonical_signal_id") or "") != str(canonical or ""):
                continue
            if status_filter is not None and str(row.get("status") or "").upper() not in status_filter:
                continue
            matches.append(row)

        matches.sort(key=lambda r: r.get("created_ts") or 0, reverse=True)
        self._result = [dict(matches[0])] if matches else []

    def execute(self, sql, params=()):
        s = " ".join(str(sql or "").split()).lower()
        params = tuple(params or ())
        self._result = []
        if "from orders" in s:
            self._orders_query(s, params)
            return self
        if "client_signal_opportunities" not in s:
            return self  # other tables — no rows, as before.

        if s.startswith("update client_signal_opportunities"):
            import json
            raw_meta, row_id = params[0], params[-1]
            new_meta = json.loads(raw_meta) if isinstance(raw_meta, (str, bytes)) else raw_meta
            row = self._row_for_id(row_id)
            if row is not None:
                row["metadata"] = new_meta
                self._result = [{"metadata": new_meta}]
            return self

        # SELECT paths keyed by (canonical_signal_id, client_id).
        canonical = params[0] if len(params) > 0 else None
        client_id = params[1] if len(params) > 1 else None
        row = self._row_for_key(canonical, client_id)
        if row is not None:
            if s.startswith("select metadata from client_signal_opportunities"):
                self._result = [{"metadata": row.get("metadata")}]
            else:
                self._result = [dict(row)]
        return self

    def fetchone(self):
        return dict(self._result[0]) if self._result else None

    def fetchall(self):
        return [dict(r) for r in self._result]


class _RaisingDBConn:
    """ap.db.conn shim that fails on use — simulates a Postgres outage so the
    atomic claim CAS (the durable ownership authority) fails closed."""

    def __enter__(self):
        raise RuntimeError("simulated_postgres_outage")

    def __exit__(self, *_args):
        return False


class _FakeOpportunityLedger(types.ModuleType):
    CREATED = "CREATED"
    WATCHER_ARMED = "WATCHER_ARMED"
    BROKER_SUBMITTED = "BROKER_SUBMITTED"
    BROKER_ACKED = "BROKER_ACKED"
    FILLED = "FILLED"
    TERMINAL_STATUSES = {
        "MISSED",
        "REJECTED",
        "CANCELED",
        "CANCELLED",
        "EXPIRED",
        "FAILED",
        "INTERNAL_ERROR",
        "FILLED",
    }

    class _Query:
        def __init__(self, storage: dict[tuple[str, str], dict]):
            self._storage = storage
            self._filters: dict[str, str] = {}

        def select(self, *_args, **_kwargs):
            return self

        def eq(self, key: str, value: str):
            self._filters[key] = value
            return self

        def limit(self, _n: int):
            return self

        def execute(self):
            key = (
                self._filters.get("canonical_signal_id"),
                self._filters.get("client_id"),
            )
            row = self._storage.get(key)
            return types.SimpleNamespace(data=[dict(row)] if row else [])

    class _SB:
        def __init__(self, storage: dict[tuple[str, str], dict]):
            self._storage = storage

        def table(self, name: str):
            assert name == "client_signal_opportunities"
            return _FakeOpportunityLedger._Query(self._storage)

    def __init__(self):
        super().__init__("ap.opportunity_ledger")
        self.rows: dict[tuple[str, str], dict] = {}
        self.invalidated_calls: list[dict] = []
        self.internal_error_calls: list[dict] = []

    def _get_sb(self):
        return self._SB(self.rows)

    def create_opportunities(
        self,
        signal_id: str,
        client_ids: list[str],
        payload: dict,
        canonical_signal_id: str | None = None,
        sb=None,
    ) -> int:
        canonical = canonical_signal_id or payload.get("canonical_signal_id") or signal_id
        for client_id in client_ids:
            self.rows.setdefault(
                (canonical, client_id),
                {
                    "signal_id": signal_id,
                    "canonical_signal_id": canonical,
                    "client_id": client_id,
                    "opportunity_status": "CREATED",
                    "miss_stage": None,
                    "miss_reason": None,
                    "metadata": {},
                },
                )
        return len(client_ids)

    def update_opportunity(
        self,
        signal_id: str,
        client_id: str,
        status: str,
        *,
        canonical_signal_id: str | None = None,
        order_local_id: str | None = None,
        miss_stage: str | None = None,
        miss_reason: str | None = None,
        extra_meta: dict | None = None,
        **_kwargs,
    ) -> bool:
        canonical = canonical_signal_id or signal_id
        row = self.rows.setdefault(
            (canonical, client_id),
            {
                "signal_id": signal_id,
                "canonical_signal_id": canonical,
                "client_id": client_id,
                "metadata": {},
            },
        )
        row["opportunity_status"] = status
        if order_local_id is not None:
            row["order_local_id"] = order_local_id
        if miss_stage is not None:
            row["miss_stage"] = miss_stage
        if miss_reason is not None:
            row["miss_reason"] = miss_reason
        row["metadata"] = {**(row.get("metadata") or {}), **(extra_meta or {})}
        return True

    def mark_watcher_armed(
        self,
        signal_id: str,
        client_id: str,
        *,
        canonical_signal_id: str | None = None,
        order_local_id: str | None = None,
        extra_meta: dict | None = None,
        **_kwargs,
    ) -> bool:
        canonical = canonical_signal_id or signal_id
        row = self.rows.setdefault(
            (canonical, client_id),
            {
                "signal_id": signal_id,
                "canonical_signal_id": canonical,
                "client_id": client_id,
                "metadata": {},
            },
        )
        row.update(
            {
                "opportunity_status": self.WATCHER_ARMED,
                "order_local_id": order_local_id,
                "metadata": {**(row.get("metadata") or {}), **(extra_meta or {})},
            }
        )
        return True

    def mark_watcher_invalidated(
        self,
        signal_id: str,
        client_id: str,
        reason: str,
        *,
        canonical_signal_id: str | None = None,
        order_local_id: str | None = None,
        extra_meta: dict | None = None,
        **_kwargs,
    ) -> bool:
        canonical = canonical_signal_id or signal_id
        row = self.rows.setdefault(
            (canonical, client_id),
            {
                "signal_id": signal_id,
                "canonical_signal_id": canonical,
                "client_id": client_id,
                "metadata": {},
            },
        )
        row.update(
            {
                "opportunity_status": "MISSED",
                "miss_stage": "WATCHER_ARM",
                "miss_reason": reason,
                "order_local_id": order_local_id,
                "metadata": {**(row.get("metadata") or {}), **(extra_meta or {})},
            }
        )
        self.invalidated_calls.append(dict(row))
        return True

    def mark_internal_error(
        self,
        signal_id: str,
        client_id: str,
        miss_reason: str,
        *,
        canonical_signal_id: str | None = None,
        order_local_id: str | None = None,
        extra_meta: dict | None = None,
        **_kwargs,
    ) -> bool:
        canonical = canonical_signal_id or signal_id
        row = self.rows.setdefault(
            (canonical, client_id),
            {
                "signal_id": signal_id,
                "canonical_signal_id": canonical,
                "client_id": client_id,
                "metadata": {},
            },
        )
        row.update(
            {
                "opportunity_status": "INTERNAL_ERROR",
                "miss_stage": "INTERNAL_ERROR",
                "miss_reason": miss_reason,
                "order_local_id": order_local_id,
                "metadata": {**(row.get("metadata") or {}), **(extra_meta or {})},
            }
        )
        self.internal_error_calls.append(dict(row))
        return True


def _install_reeval_stubs(
    monkeypatch,
    ledger: _FakeOpportunityLedger | None = None,
    execution_mode: str = "PAPER",
    db_raises: bool = False,
    osm: "_FakeOrderStateMachine | None" = None,
):
    fake_validator = types.ModuleType("ap.overnight_daily_validator")
    fake_validator.fetch_market_snapshot = lambda ticker, broker: {"last": 100.0}
    fake_validator.validate_overnight_daily_signal = (
        lambda **kwargs: types.SimpleNamespace(
            valid=True,
            reason_code="",
            reason_text="",
        )
    )
    fake_validator.InvalidationReason = object
    monkeypatch.setitem(sys.modules, "ap.overnight_daily_validator", fake_validator)

    fake_auth = types.ModuleType("ap.authorization")
    fake_auth.is_live_broker = lambda broker: False
    fake_auth.broker_live_mode_known = lambda broker: True
    fake_auth.check_live_authorization = lambda client_id: None
    fake_auth.authorization_gate_enforced = lambda: False
    fake_auth.LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"
    fake_auth.execution_mode_for_broker = lambda broker: execution_mode
    monkeypatch.setitem(sys.modules, "ap.authorization", fake_auth)

    fake_db = types.ModuleType("ap.db")
    # Share the ledger's row store so the atomic claim/bind/complete CAS
    # (SELECT ... FOR UPDATE / UPDATE ... RETURNING) sees the same durable
    # client_signal_opportunities rows create_opportunities() seeds.
    _opp_rows = ledger.rows if ledger is not None else {}
    _order_rows = osm.orders if osm is not None else {}
    if db_raises:
        fake_db.conn = lambda *a, **kw: _RaisingDBConn()
    else:
        fake_db.conn = lambda *a, **kw: _FakeDBConn(_opp_rows, _order_rows)
    fake_db.run_with_retry = lambda fn, *a, **kw: fn()
    monkeypatch.setitem(sys.modules, "ap.db", fake_db)

    if ledger is not None:
        monkeypatch.setitem(sys.modules, "ap.opportunity_ledger", ledger)


def _make_plan():
    return types.SimpleNamespace(
        plan_id="plan-001",
        signal_id="sig-001",
        ticker="AAPL",
        side="CALL",
        direction="CALL",
        score=75.0,
        timeframe="1d",
        entry_trigger=101.0,
        trigger_price=101.0,
        trigger_type="breach",
        prior_day_high=100.0,
        prior_day_low=95.0,
        pattern="2-3",
        tier="A",
        contract_symbol="AAPL260619C00100000",
        contracts=1,
        limit_price=1.25,
        metadata={},
    )


def _make_signal():
    return {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "CALL",
        "timeframe": "1d",
        "score": 75.0,
        "pattern": "2-3",
        "tier": "A",
        "entry_trigger": 101.0,
        "created_at": "2026-06-11T20:00:00+00:00",
    }


def _make_job(source: str) -> dict:
    signal = _make_signal()
    if source == "ap_signals":
        return {
            "id": "sup:sig-001",
            "signal_id": signal["signal_id"],
            "payload": signal,
            "_source": "ap_signals",
        }
    return {
        "id": "job-001",
        "signal_id": signal["signal_id"],
        "payload": signal,
        "_source": "trade_queue",
    }


def _run_reeval(
    monkeypatch,
    entry_watcher,
    *,
    source: str = "trade_queue",
    ledger: _FakeOpportunityLedger | None = None,
    cleanup_succeeds: bool = True,
    master_decision=None,
    master_reevaluate_decision=None,
    classifier_result=None,
    return_controls: bool = False,
    osm: _FakeOrderStateMachine | None = None,
    execution_mode: str = "PAPER",
    client_id: str = "client-1",
    db_raises: bool = False,
):
    import ap_overnight_reeval as ov

    if ledger is None:
        ledger = _FakeOpportunityLedger()
    # Create the OSM before stubbing so the DB fake and OSM share order storage
    # (the resolver's orders-table fences read what create_entry_order writes).
    osm = osm or _FakeOrderStateMachine(
        cleanup_succeeds=cleanup_succeeds, client_id=client_id
    )
    _install_reeval_stubs(
        monkeypatch, ledger=ledger, execution_mode=execution_mode,
        db_raises=db_raises, osm=osm,
    )
    monkeypatch.setattr(ov, "_et_now", lambda: FIXED_ET)
    monkeypatch.setattr(
        ov,
        "_fetch_watching_signals",
        lambda client_id: [_make_job(source)],
    )

    rejected_calls: list[tuple[str, str, str]] = []
    error_calls: list[tuple[str, str, str]] = []
    watching_calls: list[tuple[str, str, str]] = []
    monkeypatch.setattr(
        ov,
        "_mark_job_rejected",
        lambda job_id, client_id, reason: rejected_calls.append((str(job_id), client_id, reason)),
    )
    monkeypatch.setattr(
        ov,
        "_mark_job_error",
        lambda job_id, client_id, reason: error_calls.append((str(job_id), client_id, reason)),
    )
    monkeypatch.setattr(
        ov,
        "_mark_job_watching_reason",
        lambda job_id, client_id, reason: watching_calls.append((str(job_id), client_id, reason)),
    )
    if classifier_result is not None:
        monkeypatch.setattr(
            ov,
            "_classify_pending_entry_for_overnight",
            lambda *a, **kw: classifier_result,
        )

    broker = MagicMock()
    broker.get_prior_day_levels.return_value = {
        "prior_day_high": 100.0,
        "prior_day_low": 95.0,
    }

    master_control = MagicMock()
    _default_decision = types.SimpleNamespace(
        ok=True,
        plan=_make_plan(),
        reason="approved",
        score=75.0,
    )
    if master_reevaluate_decision is not None:
        # PR #404 P0-1: the caller re-invokes master_control.evaluate() after
        # cleaning up a stale pending entry. First call gets master_decision,
        # every subsequent call gets master_reevaluate_decision.
        _first = master_decision or _default_decision
        master_control.evaluate.side_effect = (
            [_first] + [master_reevaluate_decision] * 32
        )
    else:
        master_control.evaluate.return_value = master_decision or _default_decision

    contract_selector = MagicMock()
    contract_selector.select.return_value = "AAPL260619C00100000"

    result = ov.run_overnight_reeval(
        client_id=client_id,
        broker=broker,
        master_control=master_control,
        contract_selector=contract_selector,
        order_state_machine=osm,
        entry_watcher=entry_watcher,
        force=True,
    )
    if return_controls:
        return result, osm, rejected_calls, error_calls, {
            "broker": broker,
            "contract_selector": contract_selector,
            "master_control": master_control,
            "watching_calls": watching_calls,
            "ledger": ledger,
        }
    return result, osm, rejected_calls, error_calls


def _make_pending_trigger_order(*, age_seconds: int) -> dict:
    created = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
    return {
        "local_order_id": "pending-1",
        "broker_order_id": None,
        "status": "PENDING_TRIGGER",
        "symbol": "AAPL",
        "contract": "DEFERRED:AAPL",
        "position_id": None,
        "signal_id": "sig-001",
        "plan_id": "plan-001",
        "created_ts": created.isoformat(),
        "submitted_ts": None,
        "limit_price": 1.25,
        "price": 1.25,
        "fill_price": None,
        "score": 75.0,
        "tier": "A",
        "trigger_price": 101.0,
        "meta": {},
    }


def test_pending_trigger_watchdog_preserves_active_watcher_owned_order(monkeypatch, caplog):
    from ap import order_monitor as om_mod

    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 60)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

    watcher = MagicMock()
    watcher.has_order.return_value = True
    osm = _FakeOrderStateMachine()
    monitor = om_mod.APOrderMonitor(
        client_id="client-1",
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
        entry_watcher=watcher,
    )
    monitor._emit_order_event = MagicMock()
    monitor._get_active_entry_orders = MagicMock(
        return_value=[_make_pending_trigger_order(age_seconds=120)]
    )

    caplog.set_level(logging.INFO, logger="ap.order_monitor")
    monitor._check_entry_orders()

    watcher.has_order.assert_called_once_with("pending-1")
    assert osm.expire_calls == []
    assert osm.transition_calls == []
    assert "PENDING_TRIGGER_WATCHDOG_SEEN" in caplog.text
    assert "watcher_owner_state=True" in caplog.text
    assert "ownership_check_available=True" in caplog.text
    assert "ownership_check_error=None" in caplog.text
    assert "cleanup_action=preserve_watcher_owned" in caplog.text
    assert "PENDING_TRIGGER_ORPHAN_EXPIRED" not in caplog.text


def test_pending_trigger_watchdog_expires_orphan_without_watcher_ownership(monkeypatch, caplog):
    from ap import order_monitor as om_mod

    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 60)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

    watcher = MagicMock()
    watcher.has_order.return_value = False
    osm = _FakeOrderStateMachine()
    monitor = om_mod.APOrderMonitor(
        client_id="client-1",
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
        entry_watcher=watcher,
    )
    monitor._emit_order_event = MagicMock()
    orphan = _make_pending_trigger_order(age_seconds=120)
    orphan.update({
        "signal_id": None,
        "contract": None,
        "trigger_price": None,
        "entry_trigger": None,
    })
    monitor._get_active_entry_orders = MagicMock(return_value=[orphan])

    caplog.set_level(logging.INFO, logger="ap.order_monitor")
    monitor._check_entry_orders()

    watcher.has_order.assert_called_once_with("pending-1")
    assert len(osm.expire_calls) == 1
    local_order_id, reason = osm.expire_calls[0]
    assert local_order_id == "pending-1"
    assert reason.startswith(
        "PENDING_TRIGGER_ORPHAN_EXPIRED: no_broker_order_id no_submitted_ts age="
    )
    assert osm.orders["pending-1"]["status"] == "EXPIRED"
    assert "watcher_owner_state=False" in caplog.text
    assert "ownership_check_available=True" in caplog.text
    assert "ownership_check_error=None" in caplog.text
    assert "PENDING_TRIGGER_ORPHAN_EXPIRED" in caplog.text


def test_pending_trigger_watchdog_preserves_order_when_entry_watcher_missing(monkeypatch, caplog):
    from ap import order_monitor as om_mod

    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 60)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

    osm = _FakeOrderStateMachine()
    monitor = om_mod.APOrderMonitor(
        client_id="client-1",
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
        entry_watcher=None,
    )
    monitor._emit_order_event = MagicMock()
    monitor._get_active_entry_orders = MagicMock(
        return_value=[_make_pending_trigger_order(age_seconds=120)]
    )

    caplog.set_level(logging.INFO, logger="ap.order_monitor")
    monitor._check_entry_orders()

    assert osm.expire_calls == []
    assert osm.transition_calls == []
    assert "PENDING_TRIGGER_ORPHAN_OWNERSHIP_UNKNOWN" in caplog.text
    assert "watcher_owner_state=unknown" in caplog.text
    assert "ownership_check_available=False" in caplog.text
    assert "ownership_check_error=entry_watcher_missing" in caplog.text
    assert "cleanup_action=preserve_ownership_unknown" in caplog.text
    assert "PENDING_TRIGGER_ORPHAN_EXPIRED" not in caplog.text


def test_pending_trigger_watchdog_preserves_order_when_ownership_check_raises(monkeypatch, caplog):
    from ap import order_monitor as om_mod

    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_ENABLED", True)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_MAX_AGE_SECONDS", 60)
    monkeypatch.setattr(om_mod, "PENDING_TRIGGER_CLEANUP_DRY_RUN", False)

    watcher = MagicMock()
    watcher.has_order.side_effect = RuntimeError("watcher registry unavailable")
    osm = _FakeOrderStateMachine()
    monitor = om_mod.APOrderMonitor(
        client_id="client-1",
        broker=MagicMock(),
        order_state_machine=osm,
        position_manager=MagicMock(),
        entry_watcher=watcher,
    )
    monitor._emit_order_event = MagicMock()
    monitor._get_active_entry_orders = MagicMock(
        return_value=[_make_pending_trigger_order(age_seconds=120)]
    )

    caplog.set_level(logging.INFO, logger="ap.order_monitor")
    monitor._check_entry_orders()

    watcher.has_order.assert_called_once_with("pending-1")
    assert osm.expire_calls == []
    assert osm.transition_calls == []
    assert "PENDING_TRIGGER_ORPHAN_OWNERSHIP_UNKNOWN" in caplog.text
    assert "watcher_owner_state=unknown" in caplog.text
    assert "ownership_check_available=True" in caplog.text
    assert "ownership_check_error=RuntimeError: watcher registry unavailable" in caplog.text
    assert "cleanup_action=preserve_ownership_unknown" in caplog.text
    assert "PENDING_TRIGGER_ORPHAN_EXPIRED" not in caplog.text


def test_overnight_watch_false_cleans_order_and_records_source_and_proof(monkeypatch, caplog):
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = False
    entry_watcher._last_reject_reason = "armed_false"

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
    )

    assert result["rejected"] == 0
    assert result["skipped"] == 1
    assert result["retryable_deferred"] == 1
    assert result["errors"] == 0
    assert error_calls == []
    assert rejected_calls == []
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:armed_false")
    ]
    assert "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_DONE" in caplog.text
    assert "overnight_watch_arm_unknown" in caplog.text


def test_dedup_block_stale_order_owner_is_retryable_not_already_armed(monkeypatch, caplog):
    existing = types.SimpleNamespace(
        signal_id="sig-001",
        ticker="AAPL",
        side="CALL",
        signal={
            "signal_id": "sig-001",
            "client_id": "client-1",
            "execution_mode": "paper",
            "ticker": "AAPL",
            "side": "CALL",
            "local_order_id": "local-stale-owner",
        },
    )
    entry_watcher = types.SimpleNamespace(
        _pending=[existing],
        _dedup_set={"sig-001"},
        _lock=None,
        _last_reject_reason="dedup_block",
    )
    entry_watcher.watch = MagicMock(return_value=False)

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
    )

    assert result["armed"] == 0
    assert result["skipped"] == 1
    assert result["retryable_deferred"] == 1
    assert rejected_calls == []
    assert error_calls == []
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_ownership_conflict")
    ]
    assert "overnight_watch_ownership_conflict" in caplog.text
    assert "overnight_watch_already_watching" not in caplog.text


def _pending_entry_decision(reason="pending_entry_exists"):
    return types.SimpleNamespace(
        ok=False,
        plan=None,
        reason=reason,
        reason_code="",
        score=75.0,
    )


def _assert_pending_owner_fail_closed(
    monkeypatch,
    *,
    classifier_result=None,
):
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True
    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        master_decision=_pending_entry_decision(),
        classifier_result=classifier_result,
        return_controls=True,
    )

    assert result["armed"] == 0
    assert result["skipped"] == 1
    assert result["retryable_deferred"] == 1
    assert result["rejected"] == 0
    assert result["terminal_rejected"] == 0
    assert rejected_calls == []
    assert error_calls == []
    # PR #404 P1: DB_ERROR / CONFLICT and unexpected classifier values MUST
    # durably stamp the exact ownership-unknown class on the queue row so
    # operators can distinguish DB error from conflict; the prior "silent
    # defer" behavior (watching_calls == []) was itself the bug this fixes.
    assert len(controls["watching_calls"]) == 1
    _wc = controls["watching_calls"][0]
    assert _wc[2].startswith("pending_entry_owner_unknown:"), _wc
    controls["contract_selector"].select.assert_not_called()
    entry_watcher.watch.assert_not_called()
    assert osm.create_calls == 0
    controls["broker"].submit_order.assert_not_called()


def test_pending_owner_missing_client_conflict_does_not_release_runtime(monkeypatch):
    _assert_pending_owner_fail_closed(
        monkeypatch,
        classifier_result="PENDING_OWNER_CONFLICT",
    )


def test_pending_owner_missing_mode_conflict_does_not_release_runtime(monkeypatch):
    _assert_pending_owner_fail_closed(
        monkeypatch,
        classifier_result="PENDING_OWNER_CONFLICT",
    )


def test_pending_owner_unknown_status_conflict_does_not_release_runtime(monkeypatch):
    _assert_pending_owner_fail_closed(
        monkeypatch,
        classifier_result="PENDING_OWNER_CONFLICT",
    )


def test_pending_owner_mixed_stale_plus_conflict_does_not_release_runtime(monkeypatch):
    _assert_pending_owner_fail_closed(
        monkeypatch,
        classifier_result="PENDING_OWNER_CONFLICT",
    )


def test_pending_owner_mixed_cross_client_plus_conflict_does_not_release_runtime(monkeypatch):
    _assert_pending_owner_fail_closed(
        monkeypatch,
        classifier_result="PENDING_OWNER_CONFLICT",
    )


def test_pending_owner_db_error_does_not_release_runtime(monkeypatch):
    _assert_pending_owner_fail_closed(
        monkeypatch,
        classifier_result="PENDING_OWNER_DB_ERROR",
    )


def test_pending_owner_unexpected_classifier_value_does_not_release_runtime(monkeypatch):
    _assert_pending_owner_fail_closed(
        monkeypatch,
        classifier_result="PENDING_OWNER_NEW_FUTURE_VALUE",
    )


def test_pending_owner_explicit_stale_release_continues_to_normal_arm_runtime(monkeypatch):
    # PR #404 P0-1: after a PENDING_OWNER_STALE release the caller must
    # re-invoke master_control.evaluate() so score / 0DTE / priority / context /
    # tier / intelligence gates all run on the released candidate. The prior
    # fall-through skipped those gates. This test proves the second MC call
    # happens and, when it approves, the signal continues to arm.
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    _reeval_approved = types.SimpleNamespace(
        ok=True, plan=_make_plan(), reason="approved_after_stale_release", score=75.0,
    )

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        master_decision=_pending_entry_decision(),
        master_reevaluate_decision=_reeval_approved,
        classifier_result="PENDING_OWNER_STALE",
        return_controls=True,
    )

    assert result["armed"] == 1
    assert result["retryable_deferred"] == 0
    assert rejected_calls == []
    assert error_calls == []
    assert osm.create_calls == 1
    entry_watcher.watch.assert_called_once()
    # Master Control was invoked TWICE — once for the initial pending-entry
    # rejection, once for the post-cleanup re-evaluation (P0-1).
    assert controls["master_control"].evaluate.call_count >= 2
    assert controls["watching_calls"] == []


# ─────────────────────────────────────────────────────────────────────────────
# PR #404 P0-1: stale-release must re-invoke Master Control end-to-end.
# ─────────────────────────────────────────────────────────────────────────────

def test_stale_release_reevaluate_downstream_gate_rejection_is_honored(monkeypatch):
    # A stale pending row is released, but on re-evaluation Master Control now
    # rejects on a DOWNSTREAM gate (e.g. score/0DTE/priority/context/tier/
    # intelligence) that the original pending-entry short-circuit had skipped.
    # The candidate MUST NOT arm — the prior fall-through skipped these gates
    # entirely and could arm signals that failed them.
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    _downstream_reject = types.SimpleNamespace(
        ok=False,
        plan=None,
        reason="blocked_score:priority_floor",
        reason_code="",
        score=42.0,
    )

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        master_decision=_pending_entry_decision(),
        master_reevaluate_decision=_downstream_reject,
        classifier_result="PENDING_OWNER_STALE",
        return_controls=True,
    )

    assert controls["master_control"].evaluate.call_count >= 2
    assert result["armed"] == 0
    assert result["rejected"] == 1
    assert result["terminal_rejected"] == 1
    assert result["retryable_deferred"] == 0
    assert osm.create_calls == 0
    entry_watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    assert any(
        "mc_blocked_after_stale_release" in c[2] for c in rejected_calls
    ), rejected_calls


def test_stale_release_reevaluate_still_pending_fails_closed(monkeypatch):
    # If the re-evaluation somehow still reports pending_entry_exists (another
    # actor raced in, cleanup didn't actually clear it, etc.), the caller must
    # fail closed as retryable and NEVER bypass the pending-entry gate a
    # second time.
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        master_decision=_pending_entry_decision(),
        master_reevaluate_decision=_pending_entry_decision(),
        classifier_result="PENDING_OWNER_STALE",
        return_controls=True,
    )

    assert controls["master_control"].evaluate.call_count >= 2
    assert result["armed"] == 0
    assert result["retryable_deferred"] == 1
    assert osm.create_calls == 0
    entry_watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    assert any(
        "pending_entry_still_present_after_cleanup" in c[2]
        for c in controls["watching_calls"]
    ), controls["watching_calls"]


# ─────────────────────────────────────────────────────────────────────────────
# PR #404 P0-2: stale rows must be terminalized (with readback) before release.
# ─────────────────────────────────────────────────────────────────────────────

def test_stale_release_terminalization_failure_blocks_replacement(monkeypatch):
    # OSM cleanup returns False for the stale row (or the readback is still
    # non-terminal). No replacement is authorized: no re-evaluation, no new
    # order, no watcher call, no broker submit. Deferred as retryable with a
    # diagnostic reason.
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    # Stub classifier to synthesize a stale outcome carrying a specific row.
    stale_outcome = ov._PendingEntryOutcome(
        "PENDING_OWNER_STALE",
        ({"local_order_id": "ghost-1", "client_id": "client-1",
          "execution_mode": "paper"},),
        "stale_release",
    )

    class _FailingOSM(_FakeOrderStateMachine):
        def expire_pending_entry(self, local_order_id, *, reason=""):
            self.expire_calls.append((local_order_id, reason))
            return False  # cleanup refuses

        def cancel_pending_entry(self, local_order_id, *, reason=""):
            self.cancel_calls.append((local_order_id, reason))
            return False

        def transition(self, local_order_id, new_status, **kwargs):
            self.transition_calls.append((local_order_id, new_status, kwargs))
            return False

    osm = _FailingOSM()
    _reeval_approved = types.SimpleNamespace(
        ok=True, plan=_make_plan(), reason="approved", score=75.0,
    )

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        master_decision=_pending_entry_decision(),
        master_reevaluate_decision=_reeval_approved,
        classifier_result=stale_outcome,
        return_controls=True,
    )

    # Master Control was called EXACTLY ONCE — the re-evaluate must not run
    # because cleanup failed. No replacement is authorized.
    assert controls["master_control"].evaluate.call_count == 1
    assert result["armed"] == 0
    assert result["retryable_deferred"] == 1
    assert osm.create_calls == 0
    entry_watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    assert any(
        "pending_entry_stale_cleanup_failed" in c[2]
        for c in controls["watching_calls"]
    ), controls["watching_calls"]


# ─────────────────────────────────────────────────────────────────────────────
# PR #404 P0-4: lease-freshness ownership check.
# ─────────────────────────────────────────────────────────────────────────────

def test_pending_owner_lease_active_recognizes_production_ownership_fields():
    import ap_overnight_reeval as ov
    from datetime import datetime, timedelta, timezone

    _now = datetime.now(timezone.utc)
    _future = (_now + timedelta(minutes=15)).isoformat()
    _past = (_now - timedelta(minutes=15)).isoformat()

    # in-flight + fresh lease → owned
    owned, _ = ov._pending_owner_lease_active({
        "materialization_in_flight": True,
        "materialization_lease_until": _future,
    })
    assert owned is True

    # in-flight + expired lease → NOT owned (ghost row)
    owned, reason = ov._pending_owner_lease_active({
        "materialization_in_flight": True,
        "materialization_lease_until": _past,
    })
    assert owned is False
    assert "lease_expired" in reason

    # materialization_status=RUNNING with no lease → owned
    owned, _ = ov._pending_owner_lease_active({"materialization_status": "RUNNING"})
    assert owned is True

    # materialization_status=RUNNING with expired lease → NOT owned
    owned, _ = ov._pending_owner_lease_active({
        "materialization_status": "RUNNING",
        "materialization_lease_until": _past,
    })
    assert owned is False

    # recovery_owner identity present → owned (regardless of timestamps)
    for _field in (
        "materialization_owner", "recovery_owner", "recovery_ownership",
        "current_owner", "watcher_token", "watcher_retry_owner",
    ):
        owned, _ = ov._pending_owner_lease_active({_field: "owner-abc"})
        assert owned is True, _field

    # fresh retry timestamp → owned
    owned, _ = ov._pending_owner_lease_active(
        {"materialization_next_retry_at": _future}
    )
    assert owned is True

    # stale retry timestamp → NOT owned (this is the "false active" case the
    # prior implementation could not distinguish).
    owned, _ = ov._pending_owner_lease_active(
        {"materialization_next_retry_at": _past}
    )
    assert owned is False

    # empty / non-dict → NOT owned
    assert ov._pending_owner_lease_active({})[0] is False
    assert ov._pending_owner_lease_active(None)[0] is False


def _attempt_scope(ledger: _FakeOpportunityLedger, *, client_id="client-1", mode="paper"):
    row = ledger.rows[("CANON-001", client_id)]
    scopes = row["metadata"]["overnight_watch_arm_attempt_scopes"]
    return scopes[f"{mode}:2026-06-12"]


def _run_watch_false(monkeypatch, *, ledger, osm, cleanup_succeeds=True):
    watcher = MagicMock()
    watcher.watch.return_value = False
    watcher._last_reject_reason = "armed_false"
    return _run_reeval(
        monkeypatch,
        watcher,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        cleanup_succeeds=cleanup_succeeds,
        return_controls=True,
    )


def test_attempt_first_generic_false_is_retryable_with_durable_count(monkeypatch):
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()

    result, osm, rejected_calls, error_calls, controls = _run_watch_false(
        monkeypatch, ledger=ledger, osm=osm
    )

    scope = _attempt_scope(ledger)
    assert osm.create_calls == 1
    assert scope["count"] == 1
    assert scope["state"] == "RETRYABLE"
    assert scope["local_order_id"] == "local-ord-1"
    assert osm.orders["local-ord-1"]["status"] == "EXPIRED"
    assert result["retryable_deferred"] == 1
    assert result["terminal_rejected"] == 0
    assert rejected_calls == []
    assert error_calls == []
    controls["broker"].submit_order.assert_not_called()
    controls["broker"].cancel_order.assert_not_called()


def test_attempt_second_requires_prior_terminal_truth_before_replacement(monkeypatch):
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()

    _run_watch_false(monkeypatch, ledger=ledger, osm=osm)
    result, osm, *_ = _run_watch_false(monkeypatch, ledger=ledger, osm=osm)

    scope = _attempt_scope(ledger)
    assert "local-ord-1" in osm.get_order_calls
    assert osm.orders["local-ord-1"]["status"] == "EXPIRED"
    assert osm.create_calls == 2
    assert scope["count"] == 2
    assert scope["state"] == "RETRYABLE"
    assert scope["local_order_id"] == "local-ord-2"
    active = [o for o in osm.orders.values() if o["status"] == "PENDING_TRIGGER"]
    assert active == []
    assert result["retryable_deferred"] == 1


def test_attempt_active_prior_order_blocks_replacement(monkeypatch):
    ledger = _FakeOpportunityLedger()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "metadata": {
            "overnight_watch_arm_attempt_scopes": {
                "paper:2026-06-12": {
                    "count": 1,
                    "state": "IN_PROGRESS",
                    "token": "tok-1",
                    "local_order_id": "active-1",
                }
            }
        },
    }
    osm = _FakeOrderStateMachine()
    osm.orders["active-1"] = {"status": "PENDING_TRIGGER"}
    watcher = MagicMock()

    result, osm, _, _, controls = _run_reeval(
        monkeypatch, watcher, ledger=ledger, osm=osm, return_controls=True
    )

    assert osm.create_calls == 0
    watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    assert result["retryable_deferred"] == 1


def test_attempt_unknown_prior_order_status_blocks_replacement(monkeypatch):
    ledger = _FakeOpportunityLedger()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "metadata": {
            "overnight_watch_arm_attempt_scopes": {
                "paper:2026-06-12": {
                    "count": 1,
                    "state": "RETRYABLE",
                    "token": "tok-1",
                    "local_order_id": "weird-1",
                }
            }
        },
    }
    osm = _FakeOrderStateMachine()
    osm.orders["weird-1"] = {"status": "WAT"}
    watcher = MagicMock()

    result, osm, _, _, controls = _run_reeval(
        monkeypatch, watcher, ledger=ledger, osm=osm, return_controls=True
    )

    assert osm.create_calls == 0
    watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    assert result["retryable_deferred"] == 1
    assert "weird-1" in osm.get_order_calls


def test_attempt_database_failure_blocks_replacement(monkeypatch):
    # The durable ownership authority is the PostgreSQL row lock/CAS. When that
    # DB is unavailable the atomic claim must fail closed (WATCH_ATTEMPT_DB_ERROR)
    # — no order created, no watcher armed, no broker submit (spec Test L).
    ledger = _FakeOpportunityLedger()
    watcher = MagicMock()

    result, osm, _, _, controls = _run_reeval(
        monkeypatch, watcher, ledger=ledger, return_controls=True, db_raises=True
    )

    assert osm.create_calls == 0
    watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    assert result["retryable_deferred"] == 1


def test_attempts_are_bounded_at_three_total(monkeypatch):
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()

    for _ in range(3):
        _run_watch_false(monkeypatch, ledger=ledger, osm=osm)
    result, osm, rejected_calls, error_calls, controls = _run_watch_false(
        monkeypatch, ledger=ledger, osm=osm
    )

    scope = _attempt_scope(ledger)
    assert osm.create_calls == 3
    assert scope["count"] == 3
    assert scope["state"] == "EXHAUSTED"
    assert result["errors"] == 1
    assert result["terminal_errors"] == 1
    assert result["rejected"] == 0
    assert result["terminal_rejected"] == 0
    assert error_calls == [("job-001", "client-1", "overnight_watch_arm_retry_exhausted")]
    assert rejected_calls == []
    controls["broker"].submit_order.assert_not_called()


def test_attempt_success_stops_future_creation(monkeypatch):
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    _run_watch_false(monkeypatch, ledger=ledger, osm=osm)

    success_watcher = MagicMock()
    success_watcher.watch.return_value = True
    result, osm, *_ = _run_reeval(
        monkeypatch, success_watcher, ledger=ledger, osm=osm, return_controls=True
    )
    assert result["armed"] == 1
    assert osm.create_calls == 2
    assert _attempt_scope(ledger)["state"] == "ARMED"

    third_watcher = MagicMock()
    third, osm, *_ = _run_reeval(
        monkeypatch, third_watcher, ledger=ledger, osm=osm, return_controls=True
    )
    assert osm.create_calls == 2
    third_watcher.watch.assert_not_called()
    assert third["armed"] == 1
    assert third["fresh_armed"] == 0


def test_attempt_bind_failure_cleans_new_order_and_marks_error(monkeypatch):
    # A failed local-order bind is a TERMINAL ownership failure — the durable
    # attempt is written as ERROR, which the resolver treats as non-replaceable
    # — so this run must not report retryable_deferred. Prior behavior returned
    # retryable while writing ERROR, which contradicted the next run's refusal.
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    watcher = MagicMock()
    monkeypatch.setattr(ov, "_bind_watch_arm_attempt_order", lambda **kwargs: False)

    result, osm, _, _, controls = _run_reeval(
        monkeypatch, watcher, ledger=ledger, osm=osm, return_controls=True
    )

    assert osm.create_calls == 1
    assert osm.orders["local-ord-1"]["status"] == "EXPIRED"
    assert _attempt_scope(ledger)["state"] == "ERROR"
    watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()

    assert result["retryable_deferred"] == 0
    assert result["errors"] == 1
    assert result["terminal_errors"] == 1

    # Second run against the same durable ledger + OSM: the resolver / claim
    # must honor the ERROR terminal, create no new order, never call the
    # watcher, never submit to the broker, and never claim retryable success.
    second_watcher = MagicMock()
    second_result, osm, _, _, controls2 = _run_reeval(
        monkeypatch, second_watcher, ledger=ledger, osm=osm, return_controls=True
    )
    assert osm.create_calls == 1
    second_watcher.watch.assert_not_called()
    controls2["broker"].submit_order.assert_not_called()
    assert _attempt_scope(ledger)["state"] == "ERROR"
    assert second_result["retryable_deferred"] == 0


def test_attempt_cleanup_failure_cannot_retry(monkeypatch):
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine(cleanup_succeeds=False)
    watcher = MagicMock()
    watcher.watch.return_value = False
    watcher._last_reject_reason = "armed_false"
    _run_reeval(monkeypatch, watcher, ledger=ledger, osm=osm)

    second_watcher = MagicMock()
    result, osm, *_ = _run_reeval(
        monkeypatch, second_watcher, ledger=ledger, osm=osm, return_controls=True
    )
    assert _attempt_scope(ledger)["state"] == "ERROR"
    assert osm.create_calls == 1
    second_watcher.watch.assert_not_called()
    # Durable ERROR is a terminal ownership failure — the second run is
    # already-resolved, never retryable (a fake retryable would contradict
    # the non-replaceable ERROR state and would hide the underlying failure).
    assert result["retryable_deferred"] == 0
    assert result["already_resolved"] >= 1


def test_attempt_exact_owner_dedup_does_not_consume_extra_retry(monkeypatch):
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    existing = types.SimpleNamespace(
        signal_id="sig-001",
        ticker="AAPL",
        side="CALL",
        signal={
            "signal_id": "sig-001",
            "client_id": "client-1",
            "execution_mode": "paper",
            "ticker": "AAPL",
            "side": "CALL",
            "local_order_id": "local-ord-1",
        },
    )
    watcher = types.SimpleNamespace(
        _pending=[existing],
        _dedup_set={"sig-001"},
        _lock=None,
        _last_reject_reason="dedup_block",
    )
    watcher.watch = MagicMock(return_value=False)

    result, osm, *_ = _run_reeval(monkeypatch, watcher, ledger=ledger, osm=osm)

    scope = _attempt_scope(ledger)
    assert scope["count"] == 1
    assert scope["state"] == "ARMED"
    assert result["armed"] == 1
    assert result["fresh_armed"] == 0
    assert ledger.invalidated_calls == []
    assert osm.orders["local-ord-1"]["status"] == "EXPIRED"


def test_attempt_client_and_mode_isolation(monkeypatch):
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    for client_id, mode in (
        ("client-1", "LIVE"),
        ("client-1", "PAPER"),
        ("client-2", "LIVE"),
    ):
        watcher = MagicMock()
        watcher.watch.return_value = False
        watcher._last_reject_reason = "armed_false"
        _run_reeval(
            monkeypatch,
            watcher,
            ledger=ledger,
            osm=osm,
            execution_mode=mode,
            client_id=client_id,
        )

    assert _attempt_scope(ledger, client_id="client-1", mode="live")["count"] == 1
    assert _attempt_scope(ledger, client_id="client-1", mode="paper")["count"] == 1
    assert _attempt_scope(ledger, client_id="client-2", mode="live")["count"] == 1


def test_overnight_watch_false_cleanup_failure_marks_cleanup_failed_error_and_proof(monkeypatch, caplog):
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = False
    entry_watcher._last_reject_reason = "armed_false"

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        cleanup_succeeds=False,
    )

    assert result["errors"] == 1
    assert result["rejected"] == 0
    assert rejected_calls == []
    assert error_calls == [
        (
            "job-001",
            "client-1",
            "overnight_watch_arm_failed_cleanup_failed:overnight_watch_arm_failed:armed_false",
        )
    ]
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:armed_false")
    ]
    assert osm.cancel_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:armed_false")
    ]
    assert osm.transition_calls == [
        (
            "local-ord-1",
            "EXPIRED",
            {"last_error": "overnight_watch_arm_failed:armed_false"},
        )
    ]
    proof = ledger.rows[("CANON-001", "client-1")]
    assert proof["opportunity_status"] == "INTERNAL_ERROR"
    assert proof["miss_reason"] == (
        "overnight_watch_arm_failed_cleanup_failed:overnight_watch_arm_failed:armed_false"
    )
    assert proof["metadata"]["overnight_watch_arm_cleanup_failed"] is True
    assert proof["metadata"]["cleanup_method"] == "transition:EXPIRED"
    assert proof["metadata"]["cleanup_success"] is False
    assert proof["metadata"]["original_reason"] == "overnight_watch_arm_failed:armed_false"
    assert "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_FAILED" in caplog.text
    assert "cleanup_success=False" in caplog.text
    assert "overnight_watch_arm_failed_cleanup_failed" in caplog.text


_ACTIVE_ENTRY_STATUSES = {
    "CREATED", "PENDING_TRIGGER", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL",
}


def _active_entry_count(osm):
    return sum(
        1 for row in osm.orders.values()
        if str(row.get("status") or "").upper() in _ACTIVE_ENTRY_STATUSES
    )


def test_shared_setup_does_not_create_repeated_local_orders_after_watch_arm_failure(monkeypatch):
    # PR #404 Blocker 1 — full production-shaped ap_signals lifecycle across a
    # durable OSM + durable client_signal_opportunities. Each retryable watch-arm
    # failure creates exactly ONE new local ENTRY, and only after the prior order
    # is proven terminal (via the orders-table fence). Bounded at three total
    # attempts; the fourth reeval creates nothing and calls no watcher. Never
    # more than one active ENTRY at a time, and zero broker submissions.
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = False
    entry_watcher._last_reject_reason = "armed_false"

    def _run():
        return _run_reeval(
            monkeypatch,
            entry_watcher,
            source="ap_signals",
            ledger=ledger,
            osm=osm,
            return_controls=True,
        )

    # ── Run 1: claim count 1, create order 1, watch False, order 1 terminal ──
    r1, osm, _, _, controls1 = _run()
    assert osm.create_calls == 1
    assert osm.orders["local-ord-1"]["status"] in ("EXPIRED", "CANCELED")
    scope1 = _attempt_scope(ledger)
    assert scope1["state"] == "RETRYABLE"
    assert scope1["count"] == 1
    assert scope1["local_order_id"] == "local-ord-1"
    assert _active_entry_count(osm) <= 1
    assert controls1["broker"].submit_order.call_count == 0

    # ── Run 2: resolver proves order 1 terminal → claim count 2, order 2 ──
    r2, osm, _, _, controls2 = _run()
    assert osm.create_calls == 2
    assert osm.orders["local-ord-2"]["status"] in ("EXPIRED", "CANCELED")
    scope2 = _attempt_scope(ledger)
    assert scope2["state"] == "RETRYABLE"
    assert scope2["count"] == 2
    assert scope2["local_order_id"] == "local-ord-2"
    assert _active_entry_count(osm) <= 1
    assert controls2["broker"].submit_order.call_count == 0

    # ── Run 3: claim count 3, order 3, watch False → EXHAUSTED ──
    r3, osm, _, _, controls3 = _run()
    assert osm.create_calls == 3
    assert osm.orders["local-ord-3"]["status"] in ("EXPIRED", "CANCELED")
    scope3 = _attempt_scope(ledger)
    assert scope3["state"] == "EXHAUSTED"
    assert scope3["count"] == 3
    assert _active_entry_count(osm) <= 1
    assert controls3["broker"].submit_order.call_count == 0

    # ── Run 4: no order 4, no watcher call, still EXHAUSTED/count 3 ──
    _watch_calls_before = entry_watcher.watch.call_count
    r4, osm, _, _, controls4 = _run()
    assert osm.create_calls == 3
    assert entry_watcher.watch.call_count == _watch_calls_before
    scope4 = _attempt_scope(ledger)
    assert scope4["state"] == "EXHAUSTED"
    assert scope4["count"] == 3
    assert _active_entry_count(osm) <= 1
    assert controls4["broker"].submit_order.call_count == 0


def test_watcher_true_but_armed_completion_failure_is_not_reported_armed(monkeypatch):
    # watch() succeeds but the durable ARMED completion CAS fails. The signal
    # must NOT be reported armed; it is retryable_deferred and the live
    # PENDING_TRIGGER order + in-memory watcher are preserved (never cleaned up,
    # never a second order, never a broker submit).
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    monkeypatch.setattr(ov, "_complete_watch_arm_attempt_checked", lambda **kw: False)

    result, osm, _, _, controls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        return_controls=True,
    )

    assert result["armed"] == 0
    assert result["retryable_deferred"] == 1
    assert result["skipped"] == 1
    assert osm.create_calls == 1
    assert osm.orders["local-ord-1"]["status"] == "PENDING_TRIGGER"
    entry_watcher.watch.assert_called_once()
    assert controls["broker"].submit_order.call_count == 0


def test_bind_failure_completion_cas_failure_is_terminal_error_not_retryable(monkeypatch):
    # After the ENTRY is created, _bind_watch_arm_attempt_order fails (durable
    # scope was stolen). Cleanup of the created ENTRY succeeds. But then the
    # ERROR completion CAS ALSO fails (rare but possible: another actor rewrote
    # the durable owner between bind-check and completion). If we report this as
    # retryable_deferred, the durable attempt is stranded IN_PROGRESS and every
    # future reevaluation returns WATCH_ATTEMPT_ALREADY_IN_PROGRESS forever.
    # Must be reported as a terminal error so the operator sees the stuck
    # ownership rather than a fake "will retry" signal.
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    monkeypatch.setattr(ov, "_bind_watch_arm_attempt_order", lambda **kw: False)
    monkeypatch.setattr(ov, "_complete_watch_arm_attempt_checked", lambda **kw: False)

    result, osm, _, _, controls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        return_controls=True,
    )

    # No second ENTRY, no watcher call, no broker submit — the bind-failure path
    # must terminate this reevaluation before Step 7.
    assert osm.create_calls == 1
    entry_watcher.watch.assert_not_called()
    assert controls["broker"].submit_order.call_count == 0

    # And the outcome is a terminal error, not a "will retry" lie.
    assert result["retryable_deferred"] == 0
    assert result["errors"] == 1
    assert result["terminal_errors"] == 1


def test_ap_signals_watcher_armed_completion_fail_then_restart_no_duplicate_entry(monkeypatch):
    # ap_signals path:
    #   1) watcher.watch() → True
    #   2) WATCHER_ARMED durable ledger write succeeds
    #   3) attempt ARMED completion CAS fails → retryable_deferred (PENDING_TRIGGER
    #      order preserved, in-memory watcher preserved)
    #   4) Simulate a process restart: clear the in-memory watcher registry.
    #   5) Next reevaluation MUST reattach (or fail closed) via the active-order
    #      fence and MUST NOT create a second ENTRY or submit to the broker.
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()

    # ── run 1: attempt ARMED completion fails, but everything else succeeds ──
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    _real_complete = ov._complete_watch_arm_attempt_checked

    def _fail_only_armed(**kw):
        if str(kw.get("state") or "").upper() == ov.WATCH_ATTEMPT_STATE_ARMED:
            return False
        return _real_complete(**kw)

    monkeypatch.setattr(ov, "_complete_watch_arm_attempt_checked", _fail_only_armed)

    result1, osm, _, _, controls1 = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="ap_signals",
        ledger=ledger,
        osm=osm,
        return_controls=True,
    )

    assert result1["armed"] == 0
    assert result1["retryable_deferred"] == 1
    assert osm.create_calls == 1
    assert osm.orders["local-ord-1"]["status"] == "PENDING_TRIGGER"
    entry_watcher.watch.assert_called_once()
    assert controls1["broker"].submit_order.call_count == 0

    # ── run 2: simulate restart — fresh entry_watcher (no in-memory arm) ──
    monkeypatch.undo()

    entry_watcher2 = MagicMock()
    entry_watcher2.watch.return_value = True

    _created_before = osm.create_calls

    result2, osm2, _, _, controls2 = _run_reeval(
        monkeypatch,
        entry_watcher2,
        source="ap_signals",
        ledger=ledger,
        osm=osm,
        return_controls=True,
    )

    # The critical invariant: no second ENTRY, no broker submit. Whether the
    # next reeval reattaches or fails closed is up to the recovery path; what it
    # MUST NOT do is trust stale in-memory state and materialize a duplicate.
    assert osm2.create_calls == _created_before
    assert controls2["broker"].submit_order.call_count == 0


def test_retryable_cleanup_completion_failure_is_terminal_error_not_fake_retry(monkeypatch):
    # watch() False, cleanup succeeds, but the durable RETRYABLE completion CAS
    # fails. This is a terminal error — never reported as a retryable success,
    # because a lost durable owner cannot be trusted to bound the next retry.
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = False
    entry_watcher._last_reject_reason = "armed_false"

    monkeypatch.setattr(ov, "_complete_watch_arm_attempt_checked", lambda **kw: False)

    result, osm, _, _, controls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        return_controls=True,
    )

    assert result["retryable_deferred"] == 0
    assert result["errors"] == 1
    assert result["terminal_errors"] == 1
    assert controls["broker"].submit_order.call_count == 0


def test_shared_setup_previous_session_failure_does_not_block_retry(monkeypatch):
    ledger = _FakeOpportunityLedger()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "opportunity_status": "MISSED",
        "miss_stage": "WATCHER_ARM",
        "miss_reason": "overnight_watch_arm_failed:old_failure",
        "metadata": {
            "overnight_watch_arm_failure": True,
            "overnight_reeval_session_key": "2026-06-11",
        },
    }
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, _, _ = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="ap_signals",
        ledger=ledger,
    )

    assert result["armed"] == 1
    assert result["skipped"] == 0
    assert osm.create_calls == 1
    entry_watcher.watch.assert_called_once()


def test_shared_setup_retryable_same_session_failure_does_not_block_retry(monkeypatch):
    ledger = _FakeOpportunityLedger()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "opportunity_status": "INTERNAL_ERROR",
        "miss_stage": "DATA_NOT_READY",
        "miss_reason": "overnight_watch_arm_retryable:data_not_ready",
        "metadata": {
            "overnight_watch_arm_failure": True,
            "overnight_reeval_session_key": "2026-06-12",
        },
    }
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, _, _ = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="ap_signals",
        ledger=ledger,
    )

    assert result["armed"] == 1
    assert result["skipped"] == 0
    assert osm.create_calls == 1
    entry_watcher.watch.assert_called_once()


def test_overnight_watch_exception_cleans_order_marks_error_and_records_proof(monkeypatch, caplog):
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("watcher boom")

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
    )

    assert result["errors"] == 1
    assert rejected_calls == []
    assert error_calls == [
        ("job-001", "client-1", "overnight_watch_arm_failed:exception:watcher boom")
    ]
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:exception:watcher boom")
    ]
    proof = ledger.rows[("CANON-001", "client-1")]
    assert proof["opportunity_status"] == "INTERNAL_ERROR"
    assert proof["miss_reason"] == "overnight_watch_arm_failed:exception:watcher boom"
    assert proof["metadata"]["overnight_source_table"] == "trade_queue"
    assert proof["metadata"]["overnight_reeval_session_key"] == "2026-06-12"
    assert "OVERNIGHT_WATCH_ARM_EXCEPTION_CLEANUP_DONE" in caplog.text


def test_overnight_watch_exception_cleanup_failure_marks_cleanup_failed_error_and_proof(monkeypatch, caplog):
    ledger = _FakeOpportunityLedger()
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("watcher boom")

    caplog.set_level(logging.INFO, logger="ap.overnight_reeval")
    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        cleanup_succeeds=False,
    )

    assert result["errors"] == 1
    assert rejected_calls == []
    assert error_calls == [
        (
            "job-001",
            "client-1",
            "overnight_watch_arm_failed_cleanup_failed:overnight_watch_arm_failed:exception:watcher boom",
        )
    ]
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:exception:watcher boom")
    ]
    proof = ledger.rows[("CANON-001", "client-1")]
    assert proof["opportunity_status"] == "INTERNAL_ERROR"
    assert proof["miss_reason"] == (
        "overnight_watch_arm_failed_cleanup_failed:"
        "overnight_watch_arm_failed:exception:watcher boom"
    )
    assert proof["metadata"]["overnight_watch_arm_cleanup_failed"] is True
    assert proof["metadata"]["cleanup_method"] == "transition:EXPIRED"
    assert proof["metadata"]["cleanup_success"] is False
    assert proof["metadata"]["original_reason"] == "overnight_watch_arm_failed:exception:watcher boom"
    assert "OVERNIGHT_WATCH_ARM_FAILED_CLEANUP_FAILED" in caplog.text
    assert "cleanup_success=False" in caplog.text


def test_hard_70_floor_unchanged(monkeypatch):
    monkeypatch.setenv("HYBRID_CLIENT_QUALITY_MODE", "true")
    monkeypatch.setenv("MIN_CLIENT_SCORE", "70")
    monkeypatch.setenv("ALLOW_CLIENT_TIER_B", "true")
    monkeypatch.setenv("DAILY_CLIENT_PATTERN_WHITELIST", "2-3,3-2-2,1-2_2D")
    monkeypatch.setenv("INTRADAY_CLIENT_PATTERN_WHITELIST", "")
    monkeypatch.setenv("ALLOW_FAILED_DIR_CLIENT", "false")
    monkeypatch.setenv("ENTRY_CONFIRM_SECONDS", "45")
    monkeypatch.setenv("MAX_CLIENT_TRADES_PER_DAY", "5")
    monkeypatch.setenv("MAX_CLIENT_DAILY_TRADES", "3")
    monkeypatch.setenv("MAX_CLIENT_INTRADAY_TRADES", "2")
    monkeypatch.setenv("MAX_CLIENT_SYMBOL_TRADES_PER_DAY", "1")
    monkeypatch.setenv("MAX_PRE_ENTRY_OPTION_FADE_PCT", "8")
    monkeypatch.setenv("MAX_PRE_ENTRY_UNDERLYING_REVERSAL_PCT", "0.25")

    gate_mod = importlib.import_module("ap_hybrid_client_quality_gate")
    gate_mod = importlib.reload(gate_mod)

    signal = {
        "symbol": "AAPL",
        "direction": "CALL",
        "timeframe": "1d",
        "pattern": "2-3",
        "tier": "A",
        "trigger_price": 180.0,
        "stop_underlying": 175.0,
        "target_underlying": 190.0,
    }
    empty_snap = {
        "trades_today": 0,
        "daily_trades": 0,
        "intraday_trades": 0,
        "symbol_trades": {},
    }

    blocked = gate_mod.evaluate_client_quality_gate(
        {**signal, "score": 69},
        "client-1",
        empty_snap,
    )
    allowed = gate_mod.evaluate_client_quality_gate(
        {**signal, "score": 70},
        "client-1",
        empty_snap,
    )

    assert blocked.allowed is False
    assert blocked.block_reason == "client_score_below_70"
    assert allowed.allowed is True
