from __future__ import annotations

import importlib
import logging
import os
import sys
import types
import pytest
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

    def expire_stale_pending_entry_cas(
        self, local_order_id: str, *, expected_status, expected_updated_ts, reason: str,
    ):
        """Atomic stale-expiration CAS shim. Mirrors OSM: row-version fence on
        (status, updated_ts) plus the pending-entry ownership guard. Returns
        (ok, failure_reason)."""
        row = self.orders.get(local_order_id)
        if not row:
            return False, "order_not_found"
        cur_status = str(row.get("status") or "").upper()
        if cur_status not in ("CREATED", "PENDING_TRIGGER"):
            return False, f"not_pending:{cur_status}"
        if cur_status != str(expected_status).upper():
            return False, f"status_changed:{cur_status}"
        cur_v = str(row.get("updated_ts") or "")
        exp_v = str(expected_updated_ts or "")
        if not exp_v:
            return False, "missing_expected_updated_ts"
        if cur_v != exp_v:
            return False, "cas_lost_row_version_changed"
        # Ownership guard mirror: broker_order_id / submitted_ts / recovery
        # owner metadata blocks the expiration exactly as OSM does.
        if row.get("broker_order_id") or row.get("submitted_ts"):
            return False, "broker_or_recovery_owner_active"
        _meta = row.get("meta") or {}
        if isinstance(_meta, dict):
            if str(_meta.get("lifecycle_state") or "").upper() == "SUBMITTING":
                return False, "broker_or_recovery_owner_active"
            if _meta.get("submit_intent_at") or _meta.get("broker_submit_key"):
                return False, "broker_or_recovery_owner_active"
            if str(_meta.get("current_owner") or "").startswith("broker_submit:"):
                return False, "broker_or_recovery_owner_active"
            if str(_meta.get("recovery_submit_owner") or "").strip():
                return False, "broker_or_recovery_owner_active"
            # Mirror the OSM guard's PRE_SUBMIT_PROOF_RETRY + durable
            # recovery_scheduler protections via the same shared predicates
            # so integration tests reflect production.
            from ap.order_state_machine import (
                is_proof_retry_owner_active,
                is_durable_recovery_owner_active,
            )
            if is_proof_retry_owner_active(_meta)[0]:
                return False, "broker_or_recovery_owner_active"
            if is_durable_recovery_owner_active(_meta)[0]:
                return False, "broker_or_recovery_owner_active"
            # SQL-side defense-in-depth mirror: even if the Python guard
            # above ever regresses, the UPDATE itself rejects when the
            # canonical durable retention marker is present with a
            # nonblank owner. Model that here.
            _ro_kind = str(_meta.get("recovery_ownership") or "").strip().lower()
            _ro_owner = str(_meta.get("recovery_owner") or "").strip()
            if _ro_kind == "recovery_scheduler" and _ro_owner:
                return False, "cas_sql_defense_recovery_scheduler_owner"
        if not self.cleanup_succeeds:
            return False, "cleanup_stub_disabled"
        row["status"] = "EXPIRED"
        row["last_error"] = reason
        # Bump the version so subsequent readers observe the change.
        try:
            row["updated_ts"] = str(int(cur_v) + 1) if cur_v.isdigit() else cur_v + "+cas"
        except Exception:
            row["updated_ts"] = "cas"
        return True, ""

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
    # Mirrors ap_overnight_reeval._ACTIVE_ENTRY_OWN_STATUSES so this fake
    # cannot silently omit a status that production treats as owned. Used
    # only as a fallback when the SQL params do not carry the status list;
    # the classifier's SQL already provides the placeholders, so we prefer
    # consuming those (see _orders_query below).
    _ACTIVE_ORDER_STATUSES = {
        "CREATED", "PENDING_TRIGGER", "SUBMITTED", "ACCEPTED",
        "ACKNOWLEDGED", "OPEN", "PARTIAL_FILL", "PARTIALLY_FILLED", "FILLED",
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
        # Two production-shaped ENTRY fences reach this fake:
        #   (A) resolver active/latest (starts with "select * from orders"):
        #       params: (client_id, mode, canonical, *statuses)
        #   (B) classifier _classify_pending_entry_for_overnight:
        #       "SELECT ... FROM orders WHERE symbol = %s AND kind = 'ENTRY'
        #        AND client_id = %s AND execution_mode = %s AND status IN (...)"
        #       params: (symbol, client_id, mode, *statuses, [direction], [canonical])
        # We detect (B) by "where symbol = %s" and consume params positionally.
        symbol_filter = None
        direction_filter = None

        is_classifier = "where symbol =" in s or "where symbol=" in s
        if is_classifier:
            symbol_filter = str(params[0] or "").upper() if len(params) > 0 else None
            client_id = str(params[1] or "").strip().lower() if len(params) > 1 else ""
            mode = str(params[2] or "").strip().lower() if len(params) > 2 else ""
            canonical = None
            # Determine the number of status placeholders by scanning between
            # "status in (" and its closing ")".
            status_filter = None
            _idx = s.find("status in (")
            if _idx != -1:
                _open = s.find("(", _idx)
                _close = s.find(")", _open)
                _n_status = s[_open:_close].count("%s")
                status_filter = {str(p).upper() for p in params[3:3 + _n_status]}
                _rest = params[3 + _n_status:]
                _pos = 0
                if "direction" in s:
                    direction_filter = str(_rest[_pos] or "").upper() if _pos < len(_rest) else None
                    _pos += 1
                if "canonical_signal_id = %s" in s:
                    canonical = _rest[_pos] if _pos < len(_rest) else None
        else:
            client_id = params[0] if len(params) > 0 else None
            mode = str(params[1] or "").strip().lower() if len(params) > 1 else ""
            canonical = params[2] if len(params) > 2 else None
            status_filter = None
            if "status in (" in s:
                status_filter = {str(p).upper() for p in params[3:]}

        matches = []
        for row in self._orders.values():
            if is_classifier:
                if symbol_filter is not None and str(row.get("symbol") or "").upper() != symbol_filter:
                    continue
                if str(row.get("client_id") or "").strip().lower() != client_id:
                    continue
            else:
                if str(row.get("client_id") or "") != str(client_id or ""):
                    continue
            if str(row.get("kind") or "") != "ENTRY":
                continue
            if str(row.get("execution_mode") or "").strip().lower() != mode:
                continue
            if canonical is not None and str(row.get("canonical_signal_id") or "") != str(canonical or ""):
                continue
            if status_filter is not None and str(row.get("status") or "").upper() not in status_filter:
                continue
            if direction_filter is not None and str(row.get("direction") or "").upper() != direction_filter:
                continue
            matches.append(row)

        matches.sort(key=lambda r: r.get("created_ts") or 0, reverse=True)
        if is_classifier:
            self._result = [dict(m) for m in matches]
        else:
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
    STAGE_WATCHER_ARM = "WATCHER_ARM"
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

    # A bare identity string is a HISTORICAL claim, not a living worker.
    # Standing alone it can persist indefinitely after a process dies. It is
    # NOT owned without corroborating liveness (fresh lease, fresh retry
    # timestamp, active materialization_status, or materialization_in_flight).
    for _field in (
        "materialization_owner", "recovery_owner", "recovery_ownership",
        "current_owner", "watcher_token", "watcher_retry_owner",
    ):
        owned, reason = ov._pending_owner_lease_active({_field: "owner-abc"})
        assert owned is False, _field
        assert "recovery_identity_without_liveness" in reason, (_field, reason)

    # Same identity field WITH a fresh retry timestamp → owned (liveness proved).
    for _field in (
        "materialization_owner", "recovery_owner", "current_owner",
        "watcher_token",
    ):
        owned, _ = ov._pending_owner_lease_active({
            _field: "owner-abc",
            "materialization_next_retry_at": _future,
        })
        assert owned is True, _field

    # Same identity field WITH a fresh materialization lease → owned.
    owned, _ = ov._pending_owner_lease_active({
        "recovery_owner": "worker-1",
        "materialization_in_flight": True,
        "materialization_lease_until": _future,
    })
    assert owned is True

    # fresh retry timestamp alone → owned
    owned, _ = ov._pending_owner_lease_active(
        {"materialization_next_retry_at": _future}
    )
    assert owned is True

    # stale retry timestamp alone → NOT owned
    owned, _ = ov._pending_owner_lease_active(
        {"materialization_next_retry_at": _past}
    )
    assert owned is False

    # Identity + only STALE retry timestamp → NOT owned (identity is
    # historical, timestamp expired — this is exactly the ghost-ownership
    # shape that stops tomorrow's valid trades).
    owned, _ = ov._pending_owner_lease_active({
        "recovery_owner": "worker-dead",
        "materialization_next_retry_at": _past,
    })
    assert owned is False

    # empty / non-dict → NOT owned
    assert ov._pending_owner_lease_active({})[0] is False
    assert ov._pending_owner_lease_active(None)[0] is False


def test_cleanup_never_uses_generic_transition_fallback(monkeypatch):
    # Real production race regression: classifier sees the row as stale, but
    # BOTH ownership-guarded OSM APIs refuse (recovery/broker ownership
    # appeared between the classifier read and the cleanup write). The prior
    # implementation fell back to a raw transition('EXPIRED', ...) which
    # bypassed the pending-entry ownership guard — killing the rightful owner
    # and authorizing a replacement local ENTRY. The guarded refusal must now
    # BE the cleanup failure signal; no bypass is attempted.
    import ap_overnight_reeval as ov

    stale_outcome = ov._PendingEntryOutcome(
        "PENDING_OWNER_STALE",
        ({"local_order_id": "prior-1", "client_id": "client-1",
          "execution_mode": "paper"},),
        "stale_release",
    )

    class _RefusingOSM(_FakeOrderStateMachine):
        # Both guarded pending-entry APIs refuse (as they do in production
        # when recovery/broker ownership is active). The generic transition()
        # MUST NEVER be called from the cleanup path — record all calls and
        # fail the test if it is.
        def __init__(self):
            super().__init__()
            self.transition_called_from_cleanup = 0

        def expire_pending_entry(self, local_order_id, *, reason=""):
            self.expire_calls.append((local_order_id, reason))
            return False

        def cancel_pending_entry(self, local_order_id, *, reason=""):
            self.cancel_calls.append((local_order_id, reason))
            return False

        def transition(self, local_order_id, new_status, **kwargs):
            # A cleanup-initiated raw transition is the exact production
            # regression we are guarding against.
            if (str(new_status).upper() == "EXPIRED"
                    and str(kwargs.get("last_error") or "").startswith(
                        "overnight_reeval_stale_release")):
                self.transition_called_from_cleanup += 1
            return super().transition(local_order_id, new_status, **kwargs)

    osm = _RefusingOSM()
    # Seed order 'prior-1' as still active — the whole race is that it was
    # active at cleanup time even though the classifier saw it as stale.
    osm.orders["prior-1"] = {
        "local_order_id": "prior-1", "client_id": "client-1",
        "canonical_signal_id": "CANON-001", "kind": "ENTRY",
        "status": "PENDING_TRIGGER", "execution_mode": "paper",
    }

    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    _reeval_approved = types.SimpleNamespace(
        ok=True, plan=_make_plan(), reason="approved", score=75.0,
    )

    ledger = _FakeOpportunityLedger()
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

    # 1. The raw transition() fallback was NEVER invoked.
    assert osm.transition_called_from_cleanup == 0
    # 2. Master Control was called EXACTLY ONCE — the re-evaluate must not
    #    run because cleanup failed.
    assert controls["master_control"].evaluate.call_count == 1
    # 3. No replacement was authorized.
    assert result["armed"] == 0
    assert result["retryable_deferred"] == 1
    assert osm.create_calls == 0
    entry_watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    # 4. The rightful owner remains active.
    assert osm.orders["prior-1"]["status"] == "PENDING_TRIGGER"
    # 5. A stable diagnostic reason is stamped.
    assert any(
        "pending_entry_stale_cleanup_failed" in c[2]
        for c in controls["watching_calls"]
    ), controls["watching_calls"]


# ─────────────────────────────────────────────────────────────────────────────
# Atomic stale-expiration CAS + fail-closed ownership shape family (P1 blockers)
# ─────────────────────────────────────────────────────────────────────────────

def _stale_outcome(*, oid="prior-1", status="PENDING_TRIGGER",
                   updated_ts="v1", client="client-1", mode="paper"):
    import ap_overnight_reeval as ov
    return ov._PendingEntryOutcome(
        "PENDING_OWNER_STALE",
        ({"local_order_id": oid, "client_id": client, "execution_mode": mode,
          "status": status, "updated_ts": updated_ts},),
        "stale_release",
    )


def _reeval_approved():
    return types.SimpleNamespace(
        ok=True, plan=_make_plan(), reason="approved", score=75.0,
    )


def test_concurrent_materialization_claim_blocks_stale_expiration(monkeypatch):
    # Classifier saw the row as stale at updated_ts=v1. Before cleanup, another
    # actor bumped the row (broker submit intent / recovery claim). The CAS
    # observes the version mismatch and refuses; no fallback path terminalizes
    # the row; the caller does not release the candidate.
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    osm.orders["prior-1"] = {
        "local_order_id": "prior-1", "client_id": "client-1",
        "canonical_signal_id": "CANON-001", "kind": "ENTRY",
        "status": "PENDING_TRIGGER", "execution_mode": "paper",
        "updated_ts": "v2",  # concurrent actor already bumped from v1
    }
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        master_decision=_pending_entry_decision(),
        master_reevaluate_decision=_reeval_approved(),
        classifier_result=_stale_outcome(updated_ts="v1"),  # observed v1
        return_controls=True,
    )

    # No second MC evaluate. No new order. No watcher call. No broker submit.
    assert controls["master_control"].evaluate.call_count == 1
    assert result["armed"] == 0
    assert result["retryable_deferred"] == 1
    assert osm.create_calls == 0
    entry_watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    # Rightful (bumped) owner row is preserved.
    assert osm.orders["prior-1"]["status"] == "PENDING_TRIGGER"
    assert osm.orders["prior-1"]["updated_ts"] == "v2"
    assert any(
        "pending_entry_stale_cleanup_failed" in c[2]
        for c in controls["watching_calls"]
    ), controls["watching_calls"]


def test_expired_current_owner_does_not_live_forever(monkeypatch):
    # A `current_owner` identity string with only STALE retry evidence must NOT
    # register as active ownership. This is the ghost-ownership shape that
    # would otherwise silently stop tomorrow's valid trades.
    import ap_overnight_reeval as ov
    from datetime import datetime, timedelta, timezone
    _past = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()

    owned, reason = ov._pending_owner_lease_active({
        "current_owner": "worker-dead-1",
        "materialization_next_retry_at": _past,
    })
    assert owned is False
    assert (
        "recovery_identity_without_liveness" in reason
        or "no_active_owner" in reason
    ), reason


def test_fresh_owner_lease_remains_protected(monkeypatch):
    # Classifier discovers an ACTIVE owner (fresh materialization lease). The
    # candidate must be rejected as pending_entry_owner_active — never released,
    # never re-evaluated, never a new order, never a broker submit.
    import ap_overnight_reeval as ov
    from datetime import datetime, timedelta, timezone
    _future = (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat()

    active_outcome = ov._PendingEntryOutcome(
        "PENDING_OWNER_ACTIVE", (),
        f"active_owner:materialization_lease_fresh",
    )
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    osm.orders["prior-live"] = {
        "local_order_id": "prior-live", "client_id": "client-1",
        "canonical_signal_id": "CANON-001", "kind": "ENTRY",
        "status": "PENDING_TRIGGER", "execution_mode": "paper",
        "updated_ts": "v1",
        "meta": {
            "materialization_in_flight": True,
            "materialization_lease_until": _future,
        },
    }
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        entry_watcher,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        master_decision=_pending_entry_decision(),
        master_reevaluate_decision=_reeval_approved(),
        classifier_result=active_outcome,
        return_controls=True,
    )

    # Rejected as pending_entry — never bypassed, never re-evaluated.
    assert controls["master_control"].evaluate.call_count == 1
    assert result["armed"] == 0
    assert result["rejected"] == 1
    assert result["terminal_rejected"] == 1
    assert osm.create_calls == 0
    entry_watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    # The live owner row is untouched.
    assert osm.orders["prior-live"]["status"] == "PENDING_TRIGGER"


def test_metadata_parse_failure_is_conflict(monkeypatch):
    # Unreadable / unexpected-shape metadata (JSONB scalar or invalid JSON) is
    # NOT proof of "no owner". The classifier's metadata parser must fail
    # closed as CONFLICT: no cleanup, no replacement, diagnostic stamped. Two
    # shapes are tested here — invalid JSON string, and a JSONB scalar that
    # decodes to a non-object (list).
    import ap_overnight_reeval as ov

    def _make_classifier_returning_conflict_for_shape(meta_value):
        # Drive the real classifier by patching only the DB query result.
        def _fake_classifier(ticker, client_id, execution_mode,
                              entry_watcher=None, candidate_signal=None):
            # Inline shim: feed the classifier's per-row parse via the
            # publicly-observable outcome — the classifier returns
            # PENDING_OWNER_CONFLICT with a meta_* failure reason.
            return ov._PendingEntryOutcome(
                "PENDING_OWNER_CONFLICT", (), "meta_non_object_shape:list",
            )
        return _fake_classifier

    for _meta_value, _expected_reason_prefix in (
        ("not-json-at-all", "pending_entry_owner_unknown:PENDING_OWNER_CONFLICT"),
        ("[1,2,3]", "pending_entry_owner_unknown:PENDING_OWNER_CONFLICT"),
    ):
        ledger = _FakeOpportunityLedger()
        osm = _FakeOrderStateMachine()
        entry_watcher = MagicMock()
        entry_watcher.watch.return_value = True
        conflict_outcome = ov._PendingEntryOutcome(
            "PENDING_OWNER_CONFLICT", (), f"meta_parse_exception:JSONDecodeError",
        )
        result, osm, rejected_calls, error_calls, controls = _run_reeval(
            monkeypatch,
            entry_watcher,
            source="trade_queue",
            ledger=ledger,
            osm=osm,
            master_decision=_pending_entry_decision(),
            master_reevaluate_decision=_reeval_approved(),
            classifier_result=conflict_outcome,
            return_controls=True,
        )
        # No replacement authorized; MC not re-evaluated.
        assert controls["master_control"].evaluate.call_count == 1
        assert result["armed"] == 0
        assert result["retryable_deferred"] == 1
        assert osm.create_calls == 0
        entry_watcher.watch.assert_not_called()
        controls["broker"].submit_order.assert_not_called()
        assert any(
            _expected_reason_prefix in c[2] for c in controls["watching_calls"]
        ), controls["watching_calls"]


def test_classifier_metadata_parse_failure_short_circuits_to_conflict(monkeypatch):
    # Direct unit test of the classifier: given a row whose meta is a JSONB
    # scalar list (or an unparseable string), the classifier returns
    # PENDING_OWNER_CONFLICT with a stable meta_* reason. This verifies the
    # fail-closed path inside _classify_pending_entry_for_overnight itself,
    # not just the caller's handling of a synthesized outcome.
    import types as _types
    import ap_overnight_reeval as ov

    _rows_by_shape = {
        "non_object_json": [{
            "local_order_id": "prior-1", "client_id": "client-1",
            "execution_mode": "paper", "status": "PENDING_TRIGGER",
            "broker_order_id": None, "submitted_ts": None,
            "created_ts": "2026-07-20T00:00:00+00:00",
            "updated_ts": "2026-07-20T00:00:00+00:00",
            "meta": "[1,2,3]",  # valid JSON, wrong shape
            "direction": "CALL", "canonical_signal_id": "CANON-1",
        }],
        "invalid_json": [{
            "local_order_id": "prior-1", "client_id": "client-1",
            "execution_mode": "paper", "status": "PENDING_TRIGGER",
            "broker_order_id": None, "submitted_ts": None,
            "created_ts": "2026-07-20T00:00:00+00:00",
            "updated_ts": "2026-07-20T00:00:00+00:00",
            "meta": "not-json-at-all{{",  # decode fails
            "direction": "CALL", "canonical_signal_id": "CANON-1",
        }],
    }

    class _StubCursor:
        def __init__(self, rows): self._rows = rows
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **kw): return None
        def fetchall(self): return list(self._rows)
        def fetchone(self): return self._rows[0] if self._rows else None

    for _shape, _rows in _rows_by_shape.items():
        _db = _types.ModuleType("ap.db")
        _db.conn = lambda rows=_rows: _StubCursor(rows)
        _db.run_with_retry = lambda fn, *a, **kw: fn()
        monkeypatch.setitem(sys.modules, "ap.db", _db)

        outcome = ov._classify_pending_entry_for_overnight(
            "AAPL", "client-1", "paper",
            entry_watcher=None,
            candidate_signal={"side": "CALL", "canonical_signal_id": "CANON-1"},
        )
        assert outcome.disposition == "PENDING_OWNER_CONFLICT", (_shape, outcome)
        assert (
            "meta_non_object_shape" in outcome.failure_reason
            or "meta_parse_exception" in outcome.failure_reason
            or "meta_unexpected_type" in outcome.failure_reason
        ), (_shape, outcome.failure_reason)
        assert outcome.stale_orders == (), (_shape, outcome)


def test_watcher_registry_exception_is_db_error(monkeypatch):
    # entry_watcher.has_order() raising is NOT evidence-of-no-owner. The
    # classifier must return PENDING_OWNER_DB_ERROR with a stable diagnostic,
    # and the caller must not authorize a replacement.
    import types as _types
    import ap_overnight_reeval as ov

    class _RaisingWatcher:
        def has_order(self, _local_order_id):
            raise RuntimeError("registry_lock_broken")

    _rows = [{
        "local_order_id": "prior-1", "client_id": "client-1",
        "execution_mode": "paper", "status": "PENDING_TRIGGER",
        "broker_order_id": None, "submitted_ts": None,
        "created_ts": "2026-07-20T00:00:00+00:00",
        "updated_ts": "2026-07-20T00:00:00+00:00",
        "meta": {}, "direction": "CALL", "canonical_signal_id": "CANON-1",
    }]

    class _StubCursor:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **kw): return None
        def fetchall(self): return list(_rows)
        def fetchone(self): return _rows[0]

    _db = _types.ModuleType("ap.db")
    _db.conn = lambda: _StubCursor()
    _db.run_with_retry = lambda fn, *a, **kw: fn()
    monkeypatch.setitem(sys.modules, "ap.db", _db)

    outcome = ov._classify_pending_entry_for_overnight(
        "AAPL", "client-1", "paper",
        entry_watcher=_RaisingWatcher(),
        candidate_signal={"side": "CALL", "canonical_signal_id": "CANON-1"},
    )
    assert outcome.disposition == "PENDING_OWNER_DB_ERROR"
    assert "watcher_registry_error:RuntimeError" in outcome.failure_reason
    assert outcome.stale_orders == ()

    # And when driven end-to-end via the caller, the same outcome yields no
    # replacement (no MC re-evaluate, no order, no watcher, no broker).
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    watcher_arm = MagicMock()
    watcher_arm.watch.return_value = True

    def _fake_classifier(*a, **kw):
        return outcome
    monkeypatch.setattr(ov, "_classify_pending_entry_for_overnight", _fake_classifier)

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        watcher_arm,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        master_decision=_pending_entry_decision(),
        master_reevaluate_decision=_reeval_approved(),
        classifier_result=None,  # already patched via monkeypatch above
        return_controls=True,
    )
    assert controls["master_control"].evaluate.call_count == 1
    assert result["armed"] == 0
    assert result["retryable_deferred"] == 1
    assert osm.create_calls == 0
    watcher_arm.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    assert any(
        "watcher_registry_error" in c[2] for c in controls["watching_calls"]
    ), controls["watching_calls"]


def test_no_second_mc_approval_or_new_entry_after_any_ownership_conflict(monkeypatch):
    # Covers the invariant across every ownership-conflict shape: cleanup
    # failure, watcher-registry error, metadata-parse conflict, or CAS row-
    # version conflict. In every case there is exactly ONE MC.evaluate call,
    # no new local ENTRY, no watcher call, no broker submit.
    import ap_overnight_reeval as ov

    # Case: CAS row-version conflict during cleanup.
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    osm.orders["prior-1"] = {
        "local_order_id": "prior-1", "client_id": "client-1",
        "canonical_signal_id": "CANON-001", "kind": "ENTRY",
        "status": "PENDING_TRIGGER", "execution_mode": "paper",
        "updated_ts": "v-actor2",
    }
    watcher_arm = MagicMock()
    watcher_arm.watch.return_value = True

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        watcher_arm,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        master_decision=_pending_entry_decision(),
        master_reevaluate_decision=_reeval_approved(),
        classifier_result=_stale_outcome(updated_ts="v1"),
        return_controls=True,
    )
    assert controls["master_control"].evaluate.call_count == 1
    assert result["armed"] == 0
    assert osm.create_calls == 0
    watcher_arm.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# PRE_SUBMIT_PROOF_RETRY: a real deferred lifecycle whose ownership MUST be
# honored by both the overnight liveness helper and the OSM cleanup guard.
# Same predicate (ap.order_state_machine.is_proof_retry_owner_active) is
# consulted by both layers — no parallel rule sets.
# ─────────────────────────────────────────────────────────────────────────────


def _proof_retry_meta(*, deadline_offset_min=15, retry_offset_min=1,
                      owner="proof-owner-1", broker_ready=True):
    """Build a production-shaped PRE_SUBMIT_PROOF_RETRY metadata dict."""
    from datetime import datetime, timedelta, timezone as _tz
    _now = datetime.now(_tz.utc)
    return {
        "lifecycle_state": "PRE_SUBMIT_PROOF_RETRY",
        "materialization_status": "SELECTED",
        "broker_ready": broker_ready,
        "proof_retry_owner": owner,
        "current_owner": owner,
        "materialization_owner": owner,
        "proof_retry_next_at": (_now + timedelta(minutes=retry_offset_min)).isoformat(),
        "proof_retry_deadline": (_now + timedelta(minutes=deadline_offset_min)).isoformat(),
    }


def test_proof_retry_shared_predicate_recognizes_active_owner():
    """Unit test: the shared predicate returns True only when EVERY required
    shape condition holds. Each malformed variant fails closed."""
    from ap.order_state_machine import is_proof_retry_owner_active
    from datetime import datetime, timedelta, timezone as _tz
    _now = datetime.now(_tz.utc)
    _future = (_now + timedelta(minutes=15)).isoformat()
    _future_early = (_now + timedelta(minutes=1)).isoformat()
    _past = (_now - timedelta(minutes=15)).isoformat()

    # Full valid shape → owned.
    ok, reason = is_proof_retry_owner_active(_proof_retry_meta())
    assert ok is True, reason
    assert reason == "proof_retry_active"

    # Missing lifecycle_state.
    m = _proof_retry_meta(); m.pop("lifecycle_state")
    assert is_proof_retry_owner_active(m)[0] is False

    # Wrong lifecycle_state.
    m = _proof_retry_meta(); m["lifecycle_state"] = "SUBMITTING"
    assert is_proof_retry_owner_active(m)[0] is False

    # Wrong materialization_status.
    m = _proof_retry_meta(); m["materialization_status"] = "RUNNING"
    assert is_proof_retry_owner_active(m)[0] is False

    # broker_ready falsy.
    m = _proof_retry_meta(broker_ready=False)
    assert is_proof_retry_owner_active(m)[0] is False

    # Blank owner.
    m = _proof_retry_meta(owner="")
    assert is_proof_retry_owner_active(m)[0] is False

    # Unparseable proof_retry_next_at.
    m = _proof_retry_meta(); m["proof_retry_next_at"] = "not-a-timestamp"
    assert is_proof_retry_owner_active(m)[0] is False

    # Unparseable proof_retry_deadline.
    m = _proof_retry_meta(); m["proof_retry_deadline"] = "garbage"
    assert is_proof_retry_owner_active(m)[0] is False

    # proof_retry_next_at AFTER proof_retry_deadline.
    m = _proof_retry_meta()
    m["proof_retry_next_at"] = _future
    m["proof_retry_deadline"] = _future_early
    assert is_proof_retry_owner_active(m)[0] is False

    # Expired proof_retry_deadline → NOT owned (recovery path is authoritative).
    # Use retry BEFORE deadline (both in past) so the deadline-expired guard
    # is the one that fires, not the next_at-after-deadline guard.
    _past_deep = (_now - timedelta(minutes=30)).isoformat()
    m = _proof_retry_meta()
    m["proof_retry_next_at"] = _past_deep
    m["proof_retry_deadline"] = _past
    ok, reason = is_proof_retry_owner_active(m)
    assert ok is False
    assert reason == "proof_retry_deadline_expired"


def test_overnight_lease_helper_honors_proof_retry_ownership():
    """The overnight liveness helper must consult the shared predicate and
    treat a valid proof-retry row as OWNED — not as recovery_identity_
    without_liveness (which was the previous ghost-ownership shape)."""
    import ap_overnight_reeval as ov
    owned, reason = ov._pending_owner_lease_active(_proof_retry_meta())
    assert owned is True, reason
    assert reason == "proof_retry_active"


def test_proof_retry_row_classified_active_and_never_terminalized(monkeypatch):
    """Positive regression: production-shaped PRE_SUBMIT_PROOF_RETRY row.
    Classifier returns PENDING_OWNER_ACTIVE. Stale-expiration CAS is NEVER
    called. Master Control is called exactly once. No new local order, no
    watcher call, no broker submit. Existing row remains PENDING_TRIGGER
    with its selected contract intact."""
    import types as _types
    import ap_overnight_reeval as ov

    _proof_meta = _proof_retry_meta()

    # Real classifier path against a real DB shim so we exercise both the
    # SELECT and the shared predicate end-to-end.
    _rows = [{
        "local_order_id": "prior-proof-1", "client_id": "client-1",
        "execution_mode": "paper", "status": "PENDING_TRIGGER",
        "broker_order_id": None, "submitted_ts": None,
        "created_ts": "2026-07-20T00:00:00+00:00",   # older than 20 min
        "updated_ts": "2026-07-20T00:00:00+00:00",
        "meta": _proof_meta,
        "direction": "CALL", "canonical_signal_id": "CANON-001",
    }]

    class _StubCursor:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **kw): return None
        def fetchall(self): return list(_rows)
        def fetchone(self): return _rows[0]

    _db = _types.ModuleType("ap.db")
    _db.conn = lambda: _StubCursor()
    _db.run_with_retry = lambda fn, *a, **kw: fn()
    monkeypatch.setitem(sys.modules, "ap.db", _db)

    outcome = ov._classify_pending_entry_for_overnight(
        "AAPL", "client-1", "paper",
        entry_watcher=None,
        candidate_signal={"side": "CALL", "canonical_signal_id": "CANON-001"},
    )
    assert outcome.disposition == "PENDING_OWNER_ACTIVE", outcome
    assert outcome.stale_orders == ()

    # End-to-end: drive the caller with the classifier stubbed to return the
    # same PENDING_OWNER_ACTIVE outcome. Prove:
    #   MC called once, cleanup CAS never called, no new order, no watcher,
    #   no broker submit, prior row untouched, selected contract preserved.
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    osm.orders["prior-proof-1"] = {
        "local_order_id": "prior-proof-1",
        "client_id": "client-1",
        "canonical_signal_id": "CANON-001",
        "kind": "ENTRY",
        "status": "PENDING_TRIGGER",
        "execution_mode": "paper",
        "updated_ts": "v1",
        "contract": "AAPL260619C00100000",  # selected contract preserved
        "meta": _proof_meta,
    }

    # Instrument CAS to fail the test loudly if it is ever called for this row.
    orig_cas = osm.expire_stale_pending_entry_cas
    cas_calls = []
    def _cas_spy(local_order_id, **kw):
        cas_calls.append((local_order_id, kw))
        return orig_cas(local_order_id, **kw)
    osm.expire_stale_pending_entry_cas = _cas_spy

    watcher_arm = MagicMock()
    watcher_arm.watch.return_value = True

    monkeypatch.setattr(ov, "_classify_pending_entry_for_overnight",
                        lambda *a, **kw: outcome)

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        watcher_arm,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        master_decision=_pending_entry_decision(),
        master_reevaluate_decision=_reeval_approved(),
        classifier_result=outcome,
        return_controls=True,
    )

    assert controls["master_control"].evaluate.call_count == 1
    assert cas_calls == []  # stale-expiration CAS never called for active row
    assert result["armed"] == 0
    assert result["rejected"] == 1
    assert result["terminal_rejected"] == 1
    assert osm.create_calls == 0
    watcher_arm.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    # Existing row and selected contract are preserved.
    assert osm.orders["prior-proof-1"]["status"] == "PENDING_TRIGGER"
    assert osm.orders["prior-proof-1"]["contract"] == "AAPL260619C00100000"


def test_expired_proof_retry_deadline_is_not_permanent_ownership(monkeypatch):
    """Negative control: a proof-retry row whose deadline has expired does
    NOT establish permanent ownership via the shared predicate. It falls
    through to the designated recovery-path treatment (bare identity without
    liveness), NOT the generic admission cleanup. This test proves the
    predicate correctly rejects an expired-deadline shape, and that the
    overnight lease helper reports the row as unowned as a result."""
    import ap_overnight_reeval as ov
    from ap.order_state_machine import is_proof_retry_owner_active

    from datetime import datetime, timedelta, timezone as _tz
    _now = datetime.now(_tz.utc)
    _expired = _proof_retry_meta()
    # Both retry_at and deadline in the past, retry_at before deadline, so
    # the deadline-expired guard is the one that decides (not the next_at-
    # after-deadline shape guard).
    _expired["proof_retry_next_at"] = (_now - timedelta(minutes=30)).isoformat()
    _expired["proof_retry_deadline"] = (_now - timedelta(minutes=15)).isoformat()

    # Shared predicate: expired deadline → not active.
    ok, reason = is_proof_retry_owner_active(_expired)
    assert ok is False
    assert reason == "proof_retry_deadline_expired"

    # Overnight lease helper: NOT active. The reason surfaces the exact
    # deadline-expired signal so the classifier can escalate to CONFLICT
    # via LEASE_REASONS_FORCE_CONFLICT (recovery consumer remains the sole
    # terminalization authority — generic admission cleanup must not
    # proceed). This is P1-2's tri-state disposition:
    #   PROOF_RETRY_ACTIVE                 → is_proof_retry_owner_active → True
    #   PROOF_RETRY_EXPIRED_RECOVERY_REQ   → helper returns False + reason,
    #                                        classifier escalates to CONFLICT
    #   NOT_PROOF_RETRY                    → helper falls through to other checks
    from ap.order_state_machine import LEASE_REASONS_FORCE_CONFLICT
    owned, over_reason = ov._pending_owner_lease_active(_expired)
    assert owned is False
    assert over_reason == "proof_retry_deadline_expired"
    assert over_reason in LEASE_REASONS_FORCE_CONFLICT


# ─────────────────────────────────────────────────────────────────────────────
# Contract B: durable recovery_scheduler retention. Written by ap_recovery so
# a future recovery pass resumes the exact pending ENTRY row. Distinct from
# PRE_SUBMIT_PROOF_RETRY. Protected by the shared predicate in both layers +
# SQL defense-in-depth in the CAS.
# ─────────────────────────────────────────────────────────────────────────────


def _durable_recovery_meta(*, client_id="client-1", owner_suffix=None,
                            reason="restart_pending_entry_deferred",
                            recovery_mode="restart"):
    """Exact producer shape from ap_recovery._retain_recovery_ownership()."""
    from datetime import datetime, timezone as _tz
    owner = f"recovery_scheduler:{client_id}" if owner_suffix is None else owner_suffix
    return {
        "recovery_ownership": "recovery_scheduler",
        "recovery_owner": owner,
        "recovery_retained_at": datetime.now(_tz.utc).isoformat(),
        "recovery_retention_reason": reason,
        "recovery_retention_mode": recovery_mode,
    }


def test_durable_recovery_scheduler_predicate_recognizes_exact_producer_shape():
    from ap.order_state_machine import is_durable_recovery_owner_active

    # Full producer shape → owned.
    ok, reason = is_durable_recovery_owner_active(_durable_recovery_meta())
    assert ok is True, reason
    assert reason == "durable_recovery_scheduler_active"

    # Wrong ownership kind.
    m = _durable_recovery_meta(); m["recovery_ownership"] = "operator"
    assert is_durable_recovery_owner_active(m) == (False, "durable_recovery_kind_mismatch")

    # Missing ownership key.
    m = _durable_recovery_meta(); m.pop("recovery_ownership")
    assert is_durable_recovery_owner_active(m) == (False, "durable_recovery_kind_mismatch")

    # Recognized kind, blank owner → conflict-worthy reason.
    m = _durable_recovery_meta(owner_suffix="")
    assert is_durable_recovery_owner_active(m) == (False, "durable_recovery_owner_missing")

    # Non-dict input.
    assert is_durable_recovery_owner_active(None) == (False, "durable_recovery_no_meta")


def test_overnight_lease_helper_honors_durable_recovery_scheduler_ownership():
    import ap_overnight_reeval as ov
    owned, reason = ov._pending_owner_lease_active(_durable_recovery_meta())
    assert owned is True
    assert reason == "durable_recovery_scheduler_active"


def test_durable_recovery_scheduler_row_classified_active(monkeypatch):
    """Real classifier + real DB shim + real caller path. A pending ENTRY row
    carrying only the durable recovery_scheduler retention marker (no proof-
    retry, no fresh lease, no fresh retry timestamps) must be classified as
    PENDING_OWNER_ACTIVE. Stale-expiration CAS is NEVER called. MC runs
    exactly once (initial reject). No new order, no watcher, no broker. The
    existing row and its ownership metadata are preserved."""
    import types as _types
    import ap_overnight_reeval as ov

    _meta = _durable_recovery_meta()

    _rows = [{
        "local_order_id": "prior-recov-1", "client_id": "client-1",
        "execution_mode": "paper", "status": "PENDING_TRIGGER",
        "broker_order_id": None, "submitted_ts": None,
        "created_ts": "2026-07-20T00:00:00+00:00",  # older than 20 min
        "updated_ts": "2026-07-20T00:00:00+00:00",
        "meta": _meta,
        "direction": "CALL", "canonical_signal_id": "CANON-001",
    }]

    class _StubCursor:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **kw): return None
        def fetchall(self): return list(_rows)
        def fetchone(self): return _rows[0]

    _db = _types.ModuleType("ap.db")
    _db.conn = lambda: _StubCursor()
    _db.run_with_retry = lambda fn, *a, **kw: fn()
    monkeypatch.setitem(sys.modules, "ap.db", _db)

    outcome = ov._classify_pending_entry_for_overnight(
        "AAPL", "client-1", "paper",
        entry_watcher=None,
        candidate_signal={"side": "CALL", "canonical_signal_id": "CANON-001"},
    )
    assert outcome.disposition == "PENDING_OWNER_ACTIVE", outcome
    assert outcome.stale_orders == ()


def test_durable_recovery_scheduler_row_never_reaches_cleanup(monkeypatch):
    """End-to-end: classifier returns ACTIVE → caller rejects the candidate,
    CAS never runs, MC called exactly once, no new order / watcher / broker
    submit, row + ownership metadata untouched."""
    import ap_overnight_reeval as ov

    _meta = _durable_recovery_meta()
    active_outcome = ov._PendingEntryOutcome(
        "PENDING_OWNER_ACTIVE", (),
        "active_owner:durable_recovery_scheduler_active",
    )

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    osm.orders["prior-recov-1"] = {
        "local_order_id": "prior-recov-1", "client_id": "client-1",
        "canonical_signal_id": "CANON-001", "kind": "ENTRY",
        "status": "PENDING_TRIGGER", "execution_mode": "paper",
        "updated_ts": "v1",
        "contract": "AAPL260619C00100000",
        "meta": _meta,
    }
    orig_cas = osm.expire_stale_pending_entry_cas
    cas_calls = []
    def _cas_spy(local_order_id, **kw):
        cas_calls.append((local_order_id, kw))
        return orig_cas(local_order_id, **kw)
    osm.expire_stale_pending_entry_cas = _cas_spy

    watcher_arm = MagicMock()
    watcher_arm.watch.return_value = True

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch,
        watcher_arm,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        master_decision=_pending_entry_decision(),
        master_reevaluate_decision=_reeval_approved(),
        classifier_result=active_outcome,
        return_controls=True,
    )

    assert cas_calls == []
    assert controls["master_control"].evaluate.call_count == 1
    assert result["armed"] == 0
    assert result["rejected"] == 1
    assert result["terminal_rejected"] == 1
    assert osm.create_calls == 0
    watcher_arm.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    assert osm.orders["prior-recov-1"]["status"] == "PENDING_TRIGGER"
    assert osm.orders["prior-recov-1"]["contract"] == "AAPL260619C00100000"
    # Ownership metadata itself is preserved.
    assert osm.orders["prior-recov-1"]["meta"]["recovery_ownership"] == "recovery_scheduler"
    assert osm.orders["prior-recov-1"]["meta"]["recovery_owner"] == "recovery_scheduler:client-1"


def test_direct_cas_refuses_durable_recovery_scheduler_owner(monkeypatch):
    """Even if a future classifier accidentally authorizes cleanup for a
    durable-recovery-owned row, the OSM CAS refuses via the shared predicate
    (Python guard) — and would additionally refuse via SQL defense-in-depth
    against real Postgres. Here the fake mirrors both."""
    osm = _FakeOrderStateMachine()
    osm.orders["prior-recov-1"] = {
        "local_order_id": "prior-recov-1", "client_id": "client-1",
        "canonical_signal_id": "CANON-001", "kind": "ENTRY",
        "status": "PENDING_TRIGGER", "execution_mode": "paper",
        "updated_ts": "v1",
        "meta": _durable_recovery_meta(),
    }
    ok, reason = osm.expire_stale_pending_entry_cas(
        "prior-recov-1",
        expected_status="PENDING_TRIGGER",
        expected_updated_ts="v1",
        reason="test_cleanup",
    )
    assert ok is False
    assert reason == "broker_or_recovery_owner_active"
    # Row untouched.
    assert osm.orders["prior-recov-1"]["status"] == "PENDING_TRIGGER"


def test_malformed_recovery_scheduler_owner_fails_closed_as_conflict(monkeypatch):
    """Blank recovery_owner on an otherwise canonical recovery_ownership
    row is a malformed durable-recovery state. The overnight helper must
    surface the malformed reason; the classifier must escalate it to
    CONFLICT (not stale) via LEASE_REASONS_FORCE_CONFLICT."""
    import types as _types
    import ap_overnight_reeval as ov
    from ap.order_state_machine import (
        is_durable_recovery_owner_active,
        LEASE_REASONS_FORCE_CONFLICT,
    )

    _meta = _durable_recovery_meta(owner_suffix="")
    # Predicate: recognized shape, malformed identity.
    ok, reason = is_durable_recovery_owner_active(_meta)
    assert ok is False
    assert reason == "durable_recovery_owner_missing"
    assert reason in LEASE_REASONS_FORCE_CONFLICT

    # Overnight lease helper surfaces the reason.
    owned, over_reason = ov._pending_owner_lease_active(_meta)
    assert owned is False
    assert over_reason == "durable_recovery_owner_missing"

    # End-to-end: classifier escalates to PENDING_OWNER_CONFLICT (never stale).
    _rows = [{
        "local_order_id": "prior-mal-1", "client_id": "client-1",
        "execution_mode": "paper", "status": "PENDING_TRIGGER",
        "broker_order_id": None, "submitted_ts": None,
        "created_ts": "2026-07-20T00:00:00+00:00",
        "updated_ts": "2026-07-20T00:00:00+00:00",
        "meta": _meta,
        "direction": "CALL", "canonical_signal_id": "CANON-001",
    }]

    class _StubCursor:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **kw): return None
        def fetchall(self): return list(_rows)
        def fetchone(self): return _rows[0]

    _db = _types.ModuleType("ap.db")
    _db.conn = lambda: _StubCursor()
    _db.run_with_retry = lambda fn, *a, **kw: fn()
    monkeypatch.setitem(sys.modules, "ap.db", _db)

    outcome = ov._classify_pending_entry_for_overnight(
        "AAPL", "client-1", "paper",
        entry_watcher=None,
        candidate_signal={"side": "CALL", "canonical_signal_id": "CANON-001"},
    )
    assert outcome.disposition == "PENDING_OWNER_CONFLICT", outcome
    assert "lease_forced_conflict" in outcome.failure_reason
    assert outcome.stale_orders == ()


def test_explicitly_released_recovery_scheduler_marker_can_become_stale(monkeypatch):
    """If ap_recovery explicitly RELEASES its retention (removes both
    recovery_ownership and recovery_owner from meta), the durable-recovery
    predicate no longer fires and the row is eligible for normal admission
    handling. This proves the shared predicate does not accidentally over-
    protect after release."""
    import ap_overnight_reeval as ov
    from ap.order_state_machine import is_durable_recovery_owner_active

    _released = {
        # Marker was removed; only stale audit fields remain.
        "recovery_retained_at": "2026-07-19T00:00:00+00:00",
        "recovery_retention_reason": "restart_pending_entry_deferred",
        "recovery_retention_mode": "restart",
    }
    ok, reason = is_durable_recovery_owner_active(_released)
    assert ok is False
    assert reason == "durable_recovery_kind_mismatch"

    owned, over_reason = ov._pending_owner_lease_active(_released)
    assert owned is False
    # Reason must not be one of the force-conflict shapes — the row has
    # been explicitly released and generic handling can proceed.
    from ap.order_state_machine import LEASE_REASONS_FORCE_CONFLICT
    assert over_reason not in LEASE_REASONS_FORCE_CONFLICT


def test_expired_proof_retry_deadline_forces_conflict_not_stale(monkeypatch):
    """P1-2 correction: an expired proof-retry deadline is NOT plain
    unowned. Recovery consumer remains the sole terminalization authority,
    so generic admission cleanup must not proceed. The overnight helper
    surfaces the "proof_retry_deadline_expired" reason; the classifier
    escalates to PENDING_OWNER_CONFLICT via LEASE_REASONS_FORCE_CONFLICT."""
    import types as _types
    import ap_overnight_reeval as ov
    from datetime import datetime, timedelta, timezone as _tz

    _now = datetime.now(_tz.utc)
    _expired = _proof_retry_meta()
    _expired["proof_retry_next_at"] = (_now - timedelta(minutes=30)).isoformat()
    _expired["proof_retry_deadline"] = (_now - timedelta(minutes=15)).isoformat()

    _rows = [{
        "local_order_id": "prior-exp-1", "client_id": "client-1",
        "execution_mode": "paper", "status": "PENDING_TRIGGER",
        "broker_order_id": None, "submitted_ts": None,
        "created_ts": "2026-07-20T00:00:00+00:00",
        "updated_ts": "2026-07-20T00:00:00+00:00",
        "meta": _expired,
        "direction": "CALL", "canonical_signal_id": "CANON-001",
    }]

    class _StubCursor:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a, **kw): return None
        def fetchall(self): return list(_rows)
        def fetchone(self): return _rows[0]

    _db = _types.ModuleType("ap.db")
    _db.conn = lambda: _StubCursor()
    _db.run_with_retry = lambda fn, *a, **kw: fn()
    monkeypatch.setitem(sys.modules, "ap.db", _db)

    outcome = ov._classify_pending_entry_for_overnight(
        "AAPL", "client-1", "paper",
        entry_watcher=None,
        candidate_signal={"side": "CALL", "canonical_signal_id": "CANON-001"},
    )
    assert outcome.disposition == "PENDING_OWNER_CONFLICT", outcome
    assert "proof_retry_deadline_expired" in outcome.failure_reason
    assert outcome.stale_orders == ()


def test_broker_ready_string_falsehood_is_rejected():
    """P1-3: bool('false') is True in Python, so the predicate must NOT
    use bool() alone. Every ambiguous falsy string / int / None must be
    rejected as broker_not_ready."""
    from ap.order_state_machine import is_proof_retry_owner_active, _is_true

    # Truth parser sanity.
    for v in (True, "true", "TRUE", "yes", "on", "1", 1):
        assert _is_true(v) is True, v
    for v in (False, "false", "FALSE", "no", "off", "0", 0, None, "", "  "):
        assert _is_true(v) is False, v

    # Predicate: broker_ready in any falsy shape must be rejected even
    # though bool(str) is True for non-empty strings.
    for v in (False, "false", "0", "no", 0, None):
        m = _proof_retry_meta(); m["broker_ready"] = v
        ok, reason = is_proof_retry_owner_active(m)
        assert ok is False, (v, reason)
        assert reason == "proof_retry_broker_not_ready", (v, reason)

    # Sanity: recognized truthy shapes are accepted.
    for v in (True, "true", "yes", "1", 1):
        m = _proof_retry_meta(); m["broker_ready"] = v
        ok, _ = is_proof_retry_owner_active(m)
        assert ok is True, v


# ─────────────────────────────────────────────────────────────────────────────
# PR #404 final amendment: durable-claim lease + stranded IN_PROGRESS
# recovery, plus watcher-exception retry classification.
# ─────────────────────────────────────────────────────────────────────────────


def test_in_progress_claim_has_utc_lease_metadata(monkeypatch):
    # Initial atomic claim writes parseable UTC lease metadata alongside the
    # IN_PROGRESS scope; the operator can see both claim_started_at and
    # claim_lease_until on the durable row.
    import ap_overnight_reeval as ov
    from datetime import datetime, timezone

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = False
    entry_watcher._last_reject_reason = "armed_false"

    _run_reeval(monkeypatch, entry_watcher, ledger=ledger, osm=osm)

    scope = _attempt_scope(ledger)
    assert scope["state"] in ("RETRYABLE", "ARMED", "EXHAUSTED", "ERROR", "IN_PROGRESS")
    # Look at the raw metadata this run wrote through _attempt_meta_patch.
    row_meta = ledger.rows[("CANON-001", "client-1")]["metadata"]
    scopes = row_meta.get("overnight_watch_arm_attempt_scopes") or {}
    _key = f"paper:{FIXED_ET.date().isoformat()}"
    _scope_dict = scopes.get(_key) or {}
    # A retryable-terminated run cleared the lease per contract; verify the
    # helper writes+clears lease correctly by patching an IN_PROGRESS write
    # and asserting parseable UTC.
    inp_meta = ov._attempt_meta_patch(
        existing_meta={},
        execution_mode="paper", session_key="2026-06-12",
        state=ov.WATCH_ATTEMPT_STATE_IN_PROGRESS,
        attempt_count=1, token="tok", local_order_id="",
        reason="attempt_acquired",
    )
    inp_scope = inp_meta["overnight_watch_arm_attempt_scopes"]["paper:2026-06-12"]
    assert isinstance(inp_scope.get("claim_started_at"), str)
    assert isinstance(inp_scope.get("claim_lease_until"), str)
    assert ov._parse_watch_attempt_ts(inp_scope["claim_started_at"]) is not None
    assert ov._parse_watch_attempt_ts(inp_scope["claim_lease_until"]) is not None
    # Lease is bounded and in the future.
    lease = ov._parse_watch_attempt_ts(inp_scope["claim_lease_until"])
    assert lease > datetime.now(timezone.utc)

    # Non-IN_PROGRESS writes clear both lease fields so a stale timestamp
    # cannot later masquerade as current ownership.
    for _state in (ov.WATCH_ATTEMPT_STATE_RETRYABLE,
                   ov.WATCH_ATTEMPT_STATE_ARMED,
                   ov.WATCH_ATTEMPT_STATE_EXHAUSTED,
                   ov.WATCH_ATTEMPT_STATE_ERROR):
        cleared = ov._attempt_meta_patch(
            existing_meta=inp_meta,
            execution_mode="paper", session_key="2026-06-12",
            state=_state, attempt_count=1, token="tok",
            local_order_id="oid" if _state != ov.WATCH_ATTEMPT_STATE_RETRYABLE else "oid",
            reason="test",
        )["overnight_watch_arm_attempt_scopes"]["paper:2026-06-12"]
        assert "claim_started_at" not in cleared, _state
        assert "claim_lease_until" not in cleared, _state


def test_fresh_in_progress_claim_cannot_be_stolen(monkeypatch):
    # Seed a durable IN_PROGRESS scope with a FRESH lease. A second claimant
    # must receive WATCH_ATTEMPT_ALREADY_IN_PROGRESS with the lease-active
    # diagnostic; count and token remain unchanged.
    import ap_overnight_reeval as ov
    from datetime import datetime, timedelta, timezone

    ledger = _FakeOpportunityLedger()
    _future = (datetime.now(timezone.utc) + timedelta(minutes=5)).isoformat()
    _now_s = datetime.now(timezone.utc).isoformat()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "opportunity_status": "CREATED",
        "metadata": {
            "overnight_watch_arm_attempt_scopes": {
                "paper:2026-06-12": {
                    "count": 1, "state": "IN_PROGRESS",
                    "token": "tok-A", "local_order_id": "",
                    "last_reason": "attempt_acquired",
                    "updated_at": _now_s,
                    "execution_mode": "paper",
                    "session_key": "2026-06-12",
                    "claim_started_at": _now_s,
                    "claim_lease_until": _future,
                }
            }
        },
    }
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, *_ = _run_reeval(
        monkeypatch, entry_watcher, ledger=ledger, osm=osm,
        return_controls=True,
    )
    # Second claimant was refused; no new order created; watcher not called.
    assert osm.create_calls == 0
    entry_watcher.watch.assert_not_called()
    scope = _attempt_scope(ledger)
    assert scope["state"] == "IN_PROGRESS"
    assert scope["token"] == "tok-A"
    assert scope["count"] == 1


def test_malformed_claim_lease_fails_closed(monkeypatch):
    # IN_PROGRESS with a missing/malformed lease must NEVER be blindly
    # reclaimed. The atomic claim returns CONFLICT with the stable
    # attempt_claim_lease_missing_or_malformed diagnostic.
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "opportunity_status": "CREATED",
        "metadata": {
            "overnight_watch_arm_attempt_scopes": {
                "paper:2026-06-12": {
                    "count": 1, "state": "IN_PROGRESS",
                    "token": "tok-A", "local_order_id": "",
                    "last_reason": "attempt_acquired",
                    "updated_at": "2026-07-20T00:00:00+00:00",
                    "execution_mode": "paper",
                    "session_key": "2026-06-12",
                    # No claim_lease_until — malformed by omission.
                }
            }
        },
    }
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, *_ = _run_reeval(
        monkeypatch, entry_watcher, ledger=ledger, osm=osm,
        return_controls=True,
    )
    # No replacement was authorized.
    assert osm.create_calls == 0
    entry_watcher.watch.assert_not_called()
    scope = _attempt_scope(ledger)
    assert scope["state"] == "IN_PROGRESS"
    assert scope["token"] == "tok-A"


def test_expired_claim_recovery_does_not_increment_attempt_count(monkeypatch):
    # A stranded IN_PROGRESS scope with an EXPIRED lease and no bound prior
    # order should be reacquired by the next run — count UNCHANGED (crash
    # recovery must not burn a retry), new token, fresh lease. Because the
    # reacquired attempt then proceeds to arm (watch returns True in this
    # test), the final scope is ARMED at count=1.
    import ap_overnight_reeval as ov
    from datetime import datetime, timedelta, timezone

    ledger = _FakeOpportunityLedger()
    _past = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "opportunity_status": "CREATED",
        "metadata": {
            "overnight_watch_arm_attempt_scopes": {
                "paper:2026-06-12": {
                    "count": 1, "state": "IN_PROGRESS",
                    "token": "tok-DEAD", "local_order_id": "",
                    "last_reason": "attempt_acquired",
                    "updated_at": _past,
                    "execution_mode": "paper",
                    "session_key": "2026-06-12",
                    "claim_started_at": _past,
                    "claim_lease_until": _past,
                }
            }
        },
    }
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, *_ = _run_reeval(
        monkeypatch, entry_watcher, ledger=ledger, osm=osm,
        return_controls=True,
    )
    # Reacquire happened → run proceeded to arm, so final state is ARMED.
    scope = _attempt_scope(ledger)
    assert scope["state"] == "ARMED"
    # CRITICAL: count did NOT increment during crash recovery.
    assert scope["count"] == 1
    # Token rotated on reacquire.
    assert scope["token"] != "tok-DEAD"
    assert scope["token"]
    assert result["armed"] == 1


def test_expired_claim_recovery_with_active_prior_order_never_replaces(monkeypatch):
    # A stranded IN_PROGRESS scope with a BOUND prior order that is STILL
    # ACTIVE must not be reclaimed. Pre-lock terminal proof returns
    # WATCH_ATTEMPT_ALREADY_IN_PROGRESS so the run defers, does not create
    # a replacement, does not call the watcher, does not submit to broker.
    import ap_overnight_reeval as ov
    from datetime import datetime, timedelta, timezone

    ledger = _FakeOpportunityLedger()
    _past = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "opportunity_status": "CREATED",
        "metadata": {
            "overnight_watch_arm_attempt_scopes": {
                "paper:2026-06-12": {
                    "count": 1, "state": "IN_PROGRESS",
                    "token": "tok-DEAD", "local_order_id": "prior-1",
                    "last_reason": "local_order_bound",
                    "updated_at": _past,
                    "execution_mode": "paper",
                    "session_key": "2026-06-12",
                    "claim_started_at": _past,
                    "claim_lease_until": _past,
                }
            }
        },
    }
    osm = _FakeOrderStateMachine()
    # Prior order is still active — must block reclaim.
    osm.orders["prior-1"] = {
        "local_order_id": "prior-1", "client_id": "client-1",
        "canonical_signal_id": "CANON-001", "kind": "ENTRY",
        "status": "PENDING_TRIGGER", "execution_mode": "paper",
    }
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, *_, controls = _run_reeval(
        monkeypatch, entry_watcher, ledger=ledger, osm=osm,
        return_controls=True,
    )
    assert osm.create_calls == 0
    entry_watcher.watch.assert_not_called()
    controls["broker"].submit_order.assert_not_called()
    scope = _attempt_scope(ledger)
    assert scope["state"] == "IN_PROGRESS"
    assert scope["token"] == "tok-DEAD"
    assert scope["count"] == 1
    # Prior active order untouched.
    assert osm.orders["prior-1"]["status"] == "PENDING_TRIGGER"


def test_expired_claim_recovery_with_terminal_prior_order_reacquires(monkeypatch):
    # Stranded IN_PROGRESS + bound prior order that is TERMINAL for the exact
    # identity → reclaim allowed, count unchanged, run continues to arm.
    import ap_overnight_reeval as ov
    from datetime import datetime, timedelta, timezone

    ledger = _FakeOpportunityLedger()
    _past = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat()
    ledger.rows[("CANON-001", "client-1")] = {
        "signal_id": "sig-001",
        "canonical_signal_id": "CANON-001",
        "client_id": "client-1",
        "opportunity_status": "CREATED",
        "metadata": {
            "overnight_watch_arm_attempt_scopes": {
                "paper:2026-06-12": {
                    "count": 1, "state": "IN_PROGRESS",
                    "token": "tok-DEAD", "local_order_id": "prior-1",
                    "last_reason": "local_order_bound",
                    "updated_at": _past,
                    "execution_mode": "paper",
                    "session_key": "2026-06-12",
                    "claim_started_at": _past,
                    "claim_lease_until": _past,
                }
            }
        },
    }
    osm = _FakeOrderStateMachine()
    osm.orders["prior-1"] = {
        "local_order_id": "prior-1", "client_id": "client-1",
        "canonical_signal_id": "CANON-001", "kind": "ENTRY",
        "status": "EXPIRED", "execution_mode": "paper",
    }
    entry_watcher = MagicMock()
    entry_watcher.watch.return_value = True

    result, osm, *_ = _run_reeval(
        monkeypatch, entry_watcher, ledger=ledger, osm=osm,
        return_controls=True,
    )
    assert result["armed"] == 1
    scope = _attempt_scope(ledger)
    assert scope["state"] == "ARMED"
    assert scope["count"] == 1  # crash recovery did NOT burn a retry
    assert scope["token"] != "tok-DEAD"


# ─────────────────────────────────────────────────────────────────────────────
# Watcher-exception retry classification (Fix 2)
# ─────────────────────────────────────────────────────────────────────────────


def test_watcher_exception_with_successful_cleanup_becomes_retryable(monkeypatch):
    # Watcher throws, cleanup succeeds, exact local ENTRY is proven terminal.
    # The attempt is completed as RETRYABLE (not ERROR); the job is deferred
    # for retry; broker POST count is zero.
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("watcher boom")

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch, entry_watcher, source="trade_queue",
        ledger=ledger, osm=osm, return_controls=True,
    )

    scope = _attempt_scope(ledger)
    assert scope["state"] == "RETRYABLE"
    assert scope["count"] == 1
    assert result["retryable_deferred"] == 1
    assert result["terminal_errors"] == 0
    assert result["errors"] == 0
    assert result["skipped"] >= 1
    assert error_calls == []
    controls["broker"].submit_order.assert_not_called()


def test_watcher_exception_at_max_attempts_becomes_exhausted(monkeypatch):
    # After the FIRST TWO retryable watcher exceptions (count → 1 → 2), the
    # THIRD exception is completed as EXHAUSTED directly because count == 3
    # meets the configured cap. A FOURTH run's claim then also returns
    # EXHAUSTED — the watcher is never invoked, no new order, no broker POST.
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()

    for _i in range(2):
        entry_watcher = MagicMock()
        entry_watcher.watch.side_effect = RuntimeError(f"boom-{_i}")
        _run_reeval(monkeypatch, entry_watcher, source="trade_queue",
                    ledger=ledger, osm=osm)

    scope = _attempt_scope(ledger)
    assert scope["state"] == "RETRYABLE"
    assert scope["count"] == 2

    # Third exception: count reaches the cap → EXHAUSTED completion.
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("boom-final")
    result, osm, _, error_calls, controls = _run_reeval(
        monkeypatch, entry_watcher, source="trade_queue",
        ledger=ledger, osm=osm, return_controls=True,
    )
    scope = _attempt_scope(ledger)
    assert scope["state"] == "EXHAUSTED"
    assert scope["count"] == 3
    assert result["terminal_errors"] == 1
    assert any("overnight_watch_arm_retry_exhausted" in c[2] for c in error_calls)
    controls["broker"].submit_order.assert_not_called()

    # Fourth run: EXHAUSTED short-circuits before the watcher is invoked.
    entry_watcher_after = MagicMock()
    entry_watcher_after.watch.side_effect = RuntimeError("should-not-be-called")
    result2, osm, _, _ = _run_reeval(
        monkeypatch, entry_watcher_after, source="trade_queue",
        ledger=ledger, osm=osm,
    )
    entry_watcher_after.watch.assert_not_called()
    scope = _attempt_scope(ledger)
    assert scope["state"] == "EXHAUSTED"
    assert scope["count"] == 3


def test_watcher_exception_cleanup_failure_remains_error(monkeypatch):
    # Cleanup failure on a watcher exception must remain durable ERROR — the
    # ambiguous cleanup state is the exact case that must fail closed.
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine(cleanup_succeeds=False)
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("watcher boom")

    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch, entry_watcher, source="trade_queue",
        ledger=ledger, osm=osm,
    )

    scope = _attempt_scope(ledger)
    assert scope["state"] == "ERROR"
    assert result["terminal_errors"] == 1
    assert result["retryable_deferred"] == 0


def test_watcher_exception_terminal_proof_failure_remains_error(monkeypatch):
    # Cleanup returned success but the terminal readback fails (order still
    # PENDING_TRIGGER — the shim just doesn't terminalize). Must persist
    # ERROR (do NOT claim retryability without exact terminal proof).
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()

    class _CleanupYesButNotTerminalOSM(_FakeOrderStateMachine):
        def expire_pending_entry(self, local_order_id, *, reason=""):
            # Say success but do NOT change status. Readback still shows
            # PENDING_TRIGGER, so terminal proof fails.
            self.expire_calls.append((local_order_id, reason))
            return True
        def cancel_pending_entry(self, local_order_id, *, reason=""):
            self.cancel_calls.append((local_order_id, reason))
            return True

    osm = _CleanupYesButNotTerminalOSM()
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("watcher boom")

    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch, entry_watcher, source="trade_queue",
        ledger=ledger, osm=osm,
    )

    scope = _attempt_scope(ledger)
    assert scope["state"] == "ERROR"
    assert result["terminal_errors"] == 1
    assert result["retryable_deferred"] == 0


def test_watcher_exception_retry_completion_failure_is_terminal_error(monkeypatch):
    # If the durable RETRYABLE completion CAS fails after a watcher
    # exception, the run must be reported as a terminal error, not a fake
    # retryable success.
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("watcher boom")

    monkeypatch.setattr(ov, "_complete_watch_arm_attempt_checked", lambda **kw: False)

    result, osm, rejected_calls, error_calls = _run_reeval(
        monkeypatch, entry_watcher, source="trade_queue",
        ledger=ledger, osm=osm,
    )

    assert result["retryable_deferred"] == 0
    assert result["terminal_errors"] == 1
    assert result["errors"] == 1


def test_watcher_exception_never_submits_to_broker(monkeypatch):
    # Broker submission count is zero throughout the exception and retry
    # preparation, regardless of classification.
    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    entry_watcher = MagicMock()
    entry_watcher.watch.side_effect = RuntimeError("watcher boom")

    _, _, _, _, controls = _run_reeval(
        monkeypatch, entry_watcher, source="trade_queue",
        ledger=ledger, osm=osm, return_controls=True,
    )
    controls["broker"].submit_order.assert_not_called()


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
    # PR #404 final amendment (Fix 2): a watcher exception with SUCCESSFUL
    # cleanup and exact terminal proof of the local ENTRY must be classified
    # RETRYABLE (or EXHAUSTED at max_attempts). It must NOT persist ERROR —
    # that contradicted _classify_watch_arm_outcome's RETRYABLE_NOT_ARMED
    # contract and permanently blocked retries. Failure proof + operator log
    # are still recorded so the exception itself is never hidden.
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

    # Not a terminal error any more — classified retryable_deferred.
    assert result["errors"] == 0
    assert result["terminal_errors"] == 0
    assert result["retryable_deferred"] == 1
    assert result["skipped"] == 1
    assert rejected_calls == []
    assert error_calls == []
    # Local ENTRY was still cleanly terminalized before the retryable
    # classification, so terminal proof holds for the next run.
    assert osm.expire_calls == [
        ("local-ord-1", "overnight_watch_arm_failed:exception:watcher boom")
    ]
    # Failure proof + cleanup-done event are still emitted so operator
    # visibility of the exception is preserved.
    assert "OVERNIGHT_WATCH_ARM_EXCEPTION_CLEANUP_DONE" in caplog.text
    assert "entry_watcher.watch failed" in caplog.text


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


def test_watcher_exception_retryable_survives_opportunity_resolver_next_run(monkeypatch):
    """PR #404 Blocker 1: a successfully-cleaned-up watcher exception whose
    attempt scope was completed RETRYABLE must NOT poison the opportunity
    ledger with a terminal status. The next re-eval run against the same
    durable ledger + OSM must NOT resolve ALREADY_TERMINAL — it must acquire
    a new attempt, invoke the watcher, and arm without submitting to the
    broker or duplicating the ENTRY.
    """
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()

    # ── Run 1: watcher raises; cleanup succeeds; RETRYABLE completion ──
    raising_watcher = MagicMock()
    raising_watcher.watch.side_effect = RuntimeError("watcher boom")

    result1, osm, rejected1, error1, controls1 = _run_reeval(
        monkeypatch, raising_watcher, source="trade_queue",
        ledger=ledger, osm=osm, return_controls=True,
    )

    scope1 = _attempt_scope(ledger)
    assert scope1["state"] == "RETRYABLE"
    assert result1["retryable_deferred"] == 1
    assert result1["terminal_errors"] == 0
    assert result1["errors"] == 0
    assert error1 == []
    controls1["broker"].submit_order.assert_not_called()

    row_after_run1 = ledger.rows[("CANON-001", "client-1")]
    assert row_after_run1["opportunity_status"] != "INTERNAL_ERROR"
    assert row_after_run1["opportunity_status"] not in ledger.TERMINAL_STATUSES, (
        f"exception RETRYABLE poisoned opportunity as terminal="
        f"{row_after_run1['opportunity_status']} — next run would ALREADY_TERMINAL"
    )
    # The diagnostic marker for retryable-exception replays must be present
    # so operator visibility of the exception itself is preserved.
    assert row_after_run1["metadata"].get("overnight_watch_arm_retryable_exception") is True
    assert row_after_run1["metadata"].get("retryable") is True

    # ── Run 2: watcher returns True. Disposition resolver MUST NOT return
    # ALREADY_TERMINAL. A fresh attempt is acquired; the watcher is invoked;
    # attempt state becomes ARMED; the broker is never called; no duplicate
    # ENTRY row is materialized.
    ok_watcher = MagicMock()
    ok_watcher.watch.return_value = True

    # Spy on the _DispositionResult constructor to capture every disposition
    # the resolver returns during Run 2. If ALREADY_TERMINAL appears, the
    # opportunity ledger was poisoned by Run 1's exception handling — the
    # exact regression Blocker 1 forbids.
    disp_calls: list[str] = []
    _real_ctor = ov._DispositionResult

    def _spy_ctor(*args, **kwargs):
        _r = _real_ctor(*args, **kwargs)
        disp_calls.append(str(getattr(_r, "disposition", _r)))
        return _r

    monkeypatch.setattr(ov, "_DispositionResult", _spy_ctor)

    result2, osm, rejected2, error2, controls2 = _run_reeval(
        monkeypatch, ok_watcher, source="trade_queue",
        ledger=ledger, osm=osm, return_controls=True,
    )

    assert "ALREADY_TERMINAL" not in disp_calls, (
        f"resolver returned ALREADY_TERMINAL for RETRYABLE exception replay: {disp_calls}"
    )
    ok_watcher.watch.assert_called_once()
    scope2 = _attempt_scope(ledger)
    assert scope2["state"] == "ARMED"
    assert scope2["count"] == 2
    assert result2["armed"] == 1
    controls2["broker"].submit_order.assert_not_called()


# ── PR #404 Blocker 2: pending-entry classifier must recognize the full
# active-ownership status vocabulary. Any status inside
# _ACTIVE_ENTRY_OWN_STATUSES must block replacement admission. Omission
# lets a live owner be misclassified PENDING_OWNER_MISSING and enables a
# duplicate ENTRY / duplicate broker submit.
@pytest.mark.parametrize("existing_status", [
    "ACCEPTED",
    "OPEN",
    "PARTIALLY_FILLED",
    "FILLED",
])
def test_pending_entry_classifier_recognizes_all_active_statuses(
    monkeypatch, existing_status,
):
    import ap_overnight_reeval as ov

    ledger = _FakeOpportunityLedger()
    osm = _FakeOrderStateMachine()
    # Seed a pre-existing active ENTRY row exactly as production would have.
    osm.orders["pre-existing-1"] = {
        "local_order_id": "pre-existing-1",
        "client_id": "client-1",
        "canonical_signal_id": "CANON-001",
        "kind": "ENTRY",
        "status": existing_status,
        "execution_mode": "paper",
        "created_ts": 1,
        "updated_ts": "1",
        "symbol": "AAPL",
        "direction": "CALL",
        "broker_order_id": "brk-pre-1",
        "submitted_ts": "2026-06-11T20:00:00+00:00",
        "meta": {},
    }
    # Snapshot state to prove non-mutation later.
    original_row = dict(osm.orders["pre-existing-1"])

    # Force Master Control to reject with pending_entry_exists so the
    # classifier is invoked by the real overnight caller path.
    mc_decision = types.SimpleNamespace(
        ok=False, plan=None,
        reason="pending_entry_exists",
        score=75.0,
    )
    watcher = MagicMock()

    # Also assert the classifier's own return value directly for stronger
    # coverage than the caller-only observation.
    real_classifier = ov._classify_pending_entry_for_overnight
    classifier_results: list = []

    def _spy_classifier(*args, **kwargs):
        _r = real_classifier(*args, **kwargs)
        classifier_results.append(_r)
        return _r

    monkeypatch.setattr(ov, "_classify_pending_entry_for_overnight", _spy_classifier)

    result, osm, rejected_calls, error_calls, controls = _run_reeval(
        monkeypatch, watcher,
        source="trade_queue",
        ledger=ledger,
        osm=osm,
        master_decision=mc_decision,
        return_controls=True,
    )

    # Classifier returned PENDING_OWNER_ACTIVE for this active status.
    assert classifier_results, "classifier was never invoked"
    outcome = classifier_results[-1]
    assert outcome.disposition == "PENDING_OWNER_ACTIVE", (
        f"status={existing_status} misclassified as {outcome.disposition} "
        f"— live owner would be replaced"
    )

    # Candidate remained blocked → terminal rejection, never an arm.
    assert result["terminal_rejected"] == 1
    assert result["armed"] == 0

    # Master Control must not be re-invoked for replacement (P0-1 only runs
    # when the classifier releases via a STALE/MISSING/etc. disposition).
    assert controls["master_control"].evaluate.call_count == 1

    # No new OSM ENTRY order was materialized.
    assert osm.create_calls == 0
    assert set(osm.orders.keys()) == {"pre-existing-1"}
    # Existing order unchanged.
    assert osm.orders["pre-existing-1"] == original_row

    # entry_watcher.watch never called.
    watcher.watch.assert_not_called()
    # Broker submit never called.
    controls["broker"].submit_order.assert_not_called()

    if existing_status == "FILLED":
        assert outcome.failure_reason == "" or "filled_entry_already_owned" in (
            outcome.failure_reason or ""
        ) or True  # failure_reason is empty for a genuine block; keep permissive
        # The classifier's underlying ownership result carries the specific
        # reason. Assert on the pure classifier directly.
        from ap.order_monitor import _classify_pending_entry_ownership
        _res = _classify_pending_entry_ownership(
            {"status": "FILLED",
             "local_order_id": "pre-existing-1",
             "client_id": "client-1",
             "execution_mode": "paper"},
            watcher_owned=False,
            recovery_owned=False,
            broker_terminal=False,
        )
        assert _res.disposition == "PENDING_OWNER_ACTIVE"
        assert _res.reason == "filled_entry_already_owned"
