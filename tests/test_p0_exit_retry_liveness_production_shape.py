"""PR #423 production-shape replacement replay.

This deliberately crosses the same callback boundary as production:

    real APOrderMonitor stale-exit handoff
      -> real APOrderStateMachine CANCELED transition
      -> real APExitEngine replacement grant
    installed idempotency guard
      -> real APExitEngine submit core
      -> real APExecutionCore._on_position_close
      -> real APOrderStateMachine reservation/intent/submit/transition
      -> broker.session.post

The database connection is an in-memory test adapter.  Broker and application
logic are the real implementations; PostgreSQL proof remains a separate
release requirement.
"""

from __future__ import annotations

import json
import os
import sys
import threading
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")
os.environ.setdefault("ENCRYPTION_KEY", "ap-pr423-production-shape-test")

import ap.exit_decision_idempotency_guard as idempotency_guard  # noqa: E402
import ap.exit_safety as exit_safety  # noqa: E402
import ap.order_monitor as monitor_module  # noqa: E402
import ap.order_state_machine as osm_module  # noqa: E402
from ap_execution_core import APExecutionCore  # noqa: E402
from ap.exit_autonomous_recovery import recover_exit_engine  # noqa: E402
from ap_exit_engine import APExitEngine, ExitDecision, ManagedPosition  # noqa: E402
from ap.broker_submit_identity import canonical_broker_submit_key  # noqa: E402
from ap.order_monitor import APOrderMonitor  # noqa: E402
from ap.order_state_machine import APOrderStateMachine, unregister_exit_engine  # noqa: E402


class _MemoryCursor:
    def __init__(self, db):
        self.db = db
        self.rowcount = 1
        self._result = None

    def execute(self, sql, params=()):
        normalized = " ".join(str(sql).split()).upper()
        params = tuple(params or ())
        self.rowcount = 1
        self._result = None

        if normalized.startswith("SELECT BROKER_ORDER_ID, EXECUTION_MODE, META FROM ORDERS"):
            local_id, client_id, execution_mode = str(params[0]), str(params[1]), str(params[2]).lower()
            row = self.db.rows.get(local_id)
            if (
                row is None
                or str(row.get("client_id") or "") != client_id
                or str(row.get("execution_mode") or "").lower() != execution_mode
            ):
                self._result = None
            else:
                self._result = {
                    "broker_order_id": row.get("broker_order_id"),
                    "execution_mode": row.get("execution_mode"),
                    "meta": dict(row.get("meta") or {}),
                }
            return self

        if (
            normalized.startswith("UPDATE ORDERS SET META")
            and "AND BROKER_ORDER_ID=%S" in normalized
        ):
            patch = json.loads(str(params[0]))
            local_id, client_id, broker_id = str(params[1]), str(params[2]), str(params[3])
            expected_mode = str(params[4]).lower() if len(params) > 4 else ""
            row = self.db.rows.get(local_id)
            if (
                row is None
                or str(row.get("client_id") or "") != client_id
                or str(row.get("broker_order_id") or "") != broker_id
                or (expected_mode and str(row.get("execution_mode") or "").lower() != expected_mode)
            ):
                self.rowcount = 0
                return self
            marker = (row.get("meta") or {}).get("stale_exit_cancel_liveness")
            proposed = int(patch["stale_exit_cancel_liveness"]["attempt"])
            if marker is not None and int(marker.get("attempt", -1)) >= proposed:
                self.rowcount = 0
                return self
            row.setdefault("meta", {}).update(patch)
            return self

        if normalized.startswith("INSERT INTO ORDERS"):
            local_id, client_id, position_id = str(params[0]), str(params[1]), str(params[2])
            self.db.rows[local_id] = {
                "local_order_id": local_id,
                "client_id": client_id,
                "position_id": position_id,
                "kind": "EXIT",
                "status": "EXIT_REQUESTED",
                "symbol": str(params[6]),
                "contract": str(params[7]),
                "direction": str(params[8]),
                "execution_mode": params[9],
                "qty": int(params[10]),
                "limit_price": params[11],
                "reserved_cost": params[12],
                "filled_qty": 0,
                "broker_order_id": None,
                "submitted_ts": None,
                "last_error": None,
                "meta": {},
            }
            self.db.created_reservations += 1
            return self

        if normalized.startswith("UPDATE ORDERS SET META=") or normalized.startswith(
            "UPDATE ORDERS SET META ="
        ):
            # persist_exit_submit_intent() has eight parameters; the ordinary
            # metadata merge used by the wrapper has three.
            patch = json.loads(str(params[0]))
            local_id = str(params[1])
            row = self.db.rows.get(local_id)
            if row is not None:
                row.setdefault("meta", {}).update(patch)
            return self

        if normalized.startswith("UPDATE ORDERS SET STATUS="):
            local_id = next((str(value) for value in params if str(value) in self.db.rows), "")
            row = self.db.rows.get(local_id)
            if row is None:
                self.rowcount = 0
                return self
            new_status = str(params[0])
            row["status"] = new_status
            if new_status == "EXIT_SUBMITTED":
                broker_id = next(
                    (str(value) for value in params if str(value).startswith("broker-")),
                    "",
                )
                row["broker_order_id"] = broker_id or row.get("broker_order_id")
                row["submitted_ts"] = next(
                    (value for value in params if isinstance(value, str) and "T" in value),
                    row.get("submitted_ts"),
                )
            elif len(params) > 1:
                row["last_error"] = params[1]
            return self

        if normalized.startswith("UPDATE ORDERS SET LAST_ERROR="):
            local_id = next((str(value) for value in params if str(value) in self.db.rows), "")
            row = self.db.rows.get(local_id)
            if row is not None and params:
                row["last_error"] = params[0]
            return self

        # The production methods under test do not need any other SQL.  Keep
        # unrelated observability/cleanup statements successful without
        # pretending they changed the order reservation.
        return self

    def fetchone(self):
        result = self._result
        self._result = None
        return result

    def fetchall(self):
        return []


class _MemoryConnection:
    def __init__(self, db):
        self.cursor = _MemoryCursor(db)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *args):
        return False


class _MemoryOrderDB:
    def __init__(self):
        self.rows = {}
        self.created_reservations = 0

    def conn(self):
        return _MemoryConnection(self)

    @staticmethod
    def run_with_retry(fn, *args, **kwargs):
        return fn()


class _ProductionShapeBroker:
    def __init__(self):
        # Keep the broker's mode identity explicit on the instance.  The real
        # authorization resolver derives paper/live from this base URL; an
        # instance value prevents unrelated test/module state from changing
        # the identity of this production-shaped replay.
        self.base_url = "https://sandbox.tradier.com"
        self.account_id = "VA00000000"
        self.execution_mode = "paper"
        self.session = MagicMock()
        self.get_calls = []
        self.cancel_calls = []
        self._status_payloads = [
            {"status": "working"},
            {"status": "canceled"},
        ]
        self.post_payloads = []
        self._post_count = 0
        self._install_post_handler()

    def get_order(self, broker_order_id):
        self.get_calls.append(broker_order_id)
        if self._status_payloads:
            return dict(self._status_payloads.pop(0))
        return {"status": "unknown"}

    def cancel_order(self, broker_order_id):
        self.cancel_calls.append(broker_order_id)
        return {"ok": False, "status": "unknown"}

    def _install_post_handler(self):
        """Install the captured HTTP POST only after broker status methods exist."""

        def _post(url, *, data, headers, timeout):
            self.post_payloads.append({
                "url": url,
                "data": dict(data),
                "headers": dict(headers),
                "timeout": timeout,
            })
            self._post_count += 1
            response = MagicMock()
            response.status_code = 200
            response.json.return_value = {
                "order": {"id": f"broker-replacement-{self._post_count}", "status": "open"}
            }
            return response

        self.session.post.side_effect = _post


class _RestartReplayBroker(_ProductionShapeBroker):
    """Read boundary for a fresh-process replacement replay."""

    def __init__(self):
        super().__init__()
        self.get_calls = []

    def get_order(self, broker_order_id):
        self.get_calls.append(broker_order_id)
        return {"status": "canceled"}

    def list_open_orders(self):
        return []

    def list_positions(self):
        return [{"contract": "AVGO260814C00350000", "quantity": 5}]


def _position() -> ManagedPosition:
    now = datetime.now(timezone.utc)
    position = ManagedPosition(
        ticker="AVGO",
        option_symbol="AVGO260814C00350000",
        side="CALL",
        quantity=7,
        entry_price=2.49,
        underlying_entry=350.0,
        underlying_target=360.0,
        underlying_stop=340.0,
        position_id="pos-avgo-production-shape",
        client_id="client-avgo",
        execution_mode="paper",
        quantity_remaining=7,
        pending_exit_replace_allowed=True,
        pending_exit_replace_qty=2,
        exit_replace_attempt=1,
    )
    position.signal = {
        "correlation_bucket": "OTHER",
        "pattern": "3-1-2",
        "timeframe": "5m",
        "score": 1.0,
        "tier": "A+",
    }
    position.current_bid = 2.00
    position.current_ask = 2.40
    position.current_option_price = 2.20
    position.current_underlying = 350.0
    position.option_bid_valid = True
    position.last_quote_update_ts = now
    position.last_option_quote_update_ts = now
    position.last_underlying_quote_update_ts = now
    return position


def test_avgo_replacement_crosses_real_production_submit_shape(monkeypatch):
    db = _MemoryOrderDB()
    broker = _ProductionShapeBroker()
    import ap.authorization as authorization_module
    import ap.db as db_module

    # Keep this production-shape test independent of pytest file import order;
    # the real wrapper is the submit boundary under test.
    idempotency_guard.install_exit_decision_idempotency_guard()
    monkeypatch.setattr(db_module, "conn", db.conn)
    monkeypatch.setattr(db_module, "run_with_retry", db.run_with_retry)

    # The broad P0 suite contains legacy tests that replace
    # ``ap.authorization`` in sys.modules.  Pin this declared PAPER broker at
    # the authorization boundary so dynamic imports inside the real OSM cannot
    # inherit unrelated suite-global test state.
    monkeypatch.setattr(
        authorization_module,
        "execution_mode_for_broker",
        lambda _broker: "paper",
        raising=False,
    )
    monkeypatch.setitem(sys.modules, "ap.authorization", authorization_module)
    osm = APOrderStateMachine("client-avgo")
    rows = db.rows
    old_local_id = "loc-avgo-old"
    old_broker_id = "broker-avgo-old"
    rows[old_local_id] = {
        "local_order_id": old_local_id,
        "client_id": "client-avgo",
        "position_id": "pos-avgo-production-shape",
        "kind": "EXIT",
        "status": "EXIT_ACKNOWLEDGED",
        "symbol": "AVGO",
        "contract": "AVGO260814C00350000",
        "direction": "CALL",
        "execution_mode": "paper",
        "qty": 2,
        "limit_price": 2.49,
        "reserved_cost": 498.0,
        "filled_qty": 0,
        "broker_order_id": old_broker_id,
        "submitted_ts": None,
        "last_error": None,
        "meta": {},
    }
    osm._get_order = lambda local_order_id: dict(rows[local_order_id]) if local_order_id in rows else None
    osm._get_active_exit_order = lambda position_id: next(
        (
            dict(row)
            for row in rows.values()
            if row.get("position_id") == position_id
            and row.get("kind") == "EXIT"
            and row.get("status") in {
                "EXIT_REQUESTED", "EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL",
            }
        ),
        None,
    )
    osm._notify_opportunity_ledger = lambda **kwargs: None
    osm.submit_exit = MagicMock(wraps=osm.submit_exit)

    monkeypatch.setattr(osm_module, "conn", db.conn)
    monkeypatch.setattr(osm_module, "run_with_retry", db.run_with_retry)
    monkeypatch.setattr(osm_module, "resolve_exit_broker_truth", lambda **kwargs: {
        "is_fresh_exact": False,
        "broker_truth_open_qty": None,
        "audit": {"source": "production_shape_replay"},
    })
    monkeypatch.setattr(osm_module, "evaluate_exit_submission_safety", lambda **kwargs: {
        "blocked": False,
    })
    monkeypatch.setattr(exit_safety, "resolve_exit_broker_truth", lambda **kwargs: {
        "is_fresh_exact": False,
        "broker_truth_open_qty": None,
        "audit": {"source": "production_shape_replay"},
    })
    monkeypatch.setattr(exit_safety, "evaluate_exit_submission_safety", lambda **kwargs: {
        "blocked": False,
    })

    # Run the real idempotency wrapper, but keep its generation-claim table at
    # the test boundary.  The wrapper still performs its active-exit fences,
    # creates the exact local reservation, carries its identity through the
    # callback, and classifies the callback result.
    generation_calls = []
    claim_calls = []
    monkeypatch.setattr(
        idempotency_guard,
        "_durable_exit_generation",
        lambda pos, client_id: (generation_calls.append((pos.position_id, client_id)) or
                                 (f"{client_id}|{pos.position_id}|2|1", 1)),
    )
    monkeypatch.setattr(
        idempotency_guard,
        "_claim_durable_decision_generation",
        lambda **kwargs: (claim_calls.append(dict(kwargs)) or {"claimed": True}),
    )
    monkeypatch.setattr(
        idempotency_guard,
        "_update_durable_decision_generation",
        lambda *args, **kwargs: None,
    )

    engine = APExitEngine(broker=broker, email="client-avgo")
    engine._emit_exit_event = MagicMock()
    engine._persist_exit_replace_attempt_to_db = MagicMock(return_value=True)
    position = _position()
    position.exit_in_flight = True
    position.pending_exit_local_order_id = old_local_id
    position.pending_exit_broker_order_id = old_broker_id
    position.pending_exit_qty = 2
    position.pending_exit_replace_allowed = False
    position.pending_exit_replace_qty = 0
    position.exit_replace_attempt = 0
    engine._positions.append(position)
    engine._positions_by_id[position.position_id] = position

    core = APExecutionCore.__new__(APExecutionCore)
    core._pos_lock = threading.RLock()
    core._sector_lock = threading.RLock()
    core._position_count = 1
    core._sector_counts = {"OTHER": 1}
    core.master_control = SimpleNamespace(mode="paper", _trade_cooldowns={})
    core.paper = True
    core.email = "client-avgo"
    core.client_id = "client-avgo"
    core.broker = broker
    core.order_state_machine = osm
    core.feedback = MagicMock()
    core.feedback.get_setup_status.return_value = "READY"
    core._edge_logger = None

    engine.order_state_machine = osm
    engine.osm = osm
    engine.on_exit = core._on_position_close
    osm_module.register_exit_engine("client-avgo", engine)
    try:
        monkeypatch.setattr(monitor_module, "ORDER_MONITOR_MODE", "watchdog")
        monkeypatch.setattr(monitor_module, "ORDER_MONITOR_CAN_ACT", False)
        monkeypatch.setattr(
            monitor_module,
            "ALLOW_STALE_EXIT_RECOVERY_IN_WATCHDOG",
            True,
        )
        monitor = APOrderMonitor(
            client_id="client-avgo",
            broker=broker,
            order_state_machine=osm,
            position_manager=MagicMock(),
            exit_engine=engine,
            entry_watcher=MagicMock(),
            client_mode="PAPER",
        )
        monitor._emit_order_event = MagicMock()
        monitor._alert = MagicMock()
        monitor._handle_stale_exit(
            local_order_id=old_local_id,
            status="WORKING",
            contract="AVGO260814C00350000",
            age_secs=120.0,
            position_id=position.position_id,
            reason="production-shaped AVGO stale exit",
        )

        assert broker.get_calls == [old_broker_id, old_broker_id]
        assert broker.cancel_calls == [old_broker_id]
        assert rows[old_local_id]["status"] == "CANCELED"
        assert position.exit_in_flight is False
        assert position.pending_exit_replace_allowed is True
        assert position.pending_exit_replace_durable_pending is False
        assert position.pending_exit_replace_qty == 2
        assert position.exit_replace_attempt == 1
        assert rows[old_local_id]["meta"]["stale_exit_cancel_liveness"]["attempt"] == 1
        assert osm.persist_stale_exit_cancel_attempt(
            old_local_id, old_broker_id, 1, execution_mode="paper"
        ) is False

        decision = ExitDecision(
            action="STOP",
            quantity=7,
            reason="RUNNER TRAIL",
            urgency="HIGH",
            pnl_pct=0.10,
        )
        assert getattr(APExitEngine, idempotency_guard._ORIGINAL_SUBMIT_ATTR) is not APExitEngine._submit_exit_decision
        assert engine._submit_exit_decision(position, decision) is True
        assert core._position_count == 1
        assert core._sector_counts["OTHER"] == 1
        assert position.quantity_remaining == 7
        assert position._proof_staged is None
        assert position._proof_finalized is False
        assert position.proof_logged is False
        assert position._integrity_logged is False
    finally:
        unregister_exit_engine("client-avgo")

    assert generation_calls == [(position.position_id, "client-avgo")]
    assert len(claim_calls) == 1
    assert engine.on_exit == core._on_position_close
    assert osm.submit_exit.call_count == 1
    assert db.created_reservations == 1
    assert len(rows) == 2
    assert rows[old_local_id]["status"] == "CANCELED"
    row = next(row for local_id, row in rows.items() if local_id != old_local_id)
    assert row["status"] == "EXIT_SUBMITTED"
    assert row["broker_order_id"] == "broker-replacement-1"
    assert row["qty"] == 2
    assert row["meta"]["lifecycle_state"] == "SUBMITTING"
    assert len(broker.post_payloads) == 1
    assert broker.post_payloads[0]["data"]["quantity"] == 2
    assert broker.post_payloads[0]["data"]["tag"] == canonical_broker_submit_key(row["local_order_id"])
    assert not hasattr(broker, "submit_order")
    assert position.exit_in_flight is True
    assert position.pending_exit_broker_order_id == "broker-replacement-1"


def test_fresh_process_replays_durable_pending_replacement_once(monkeypatch):
    """A cleared old owner still produces exactly one capped replacement POST."""
    db = _MemoryOrderDB()
    broker = _RestartReplayBroker()
    import ap.authorization as authorization_module
    import ap.db as db_module

    monkeypatch.setattr(
        authorization_module,
        "execution_mode_for_broker",
        lambda _broker: "paper",
        raising=False,
    )
    monkeypatch.setitem(sys.modules, "ap.authorization", authorization_module)
    monkeypatch.setattr(db_module, "conn", db.conn)
    monkeypatch.setattr(db_module, "run_with_retry", db.run_with_retry)
    monkeypatch.setattr(osm_module, "conn", db.conn)
    monkeypatch.setattr(osm_module, "run_with_retry", db.run_with_retry)
    monkeypatch.setattr(osm_module, "resolve_exit_broker_truth", lambda **kwargs: {
        "is_fresh_exact": False,
        "broker_truth_open_qty": None,
        "audit": {"source": "restart_replay"},
    })
    monkeypatch.setattr(osm_module, "evaluate_exit_submission_safety", lambda **kwargs: {
        "blocked": False,
    })
    monkeypatch.setattr(exit_safety, "resolve_exit_broker_truth", lambda **kwargs: {
        "is_fresh_exact": False,
        "broker_truth_open_qty": None,
        "audit": {"source": "restart_replay"},
    })
    monkeypatch.setattr(exit_safety, "evaluate_exit_submission_safety", lambda **kwargs: {
        "blocked": False,
    })

    old_local_id = "loc-replay-old"
    old_broker_id = "broker-replay-old"
    position_id = "pos-replay"
    client_id = "client-replay"
    contract = "AVGO260814C00350000"
    db.rows[old_local_id] = {
        "local_order_id": old_local_id,
        "client_id": client_id,
        "position_id": position_id,
        "kind": "EXIT",
        "status": "CANCELED",
        "symbol": "AVGO",
        "contract": contract,
        "direction": "CALL",
        "execution_mode": "paper",
        "qty": 2,
        "limit_price": 2.49,
        "reserved_cost": 498.0,
        "filled_qty": 1,
        "broker_order_id": old_broker_id,
        "submitted_ts": None,
        "last_error": None,
        "meta": {},
    }
    osm = APOrderStateMachine(client_id)
    osm.get_order = lambda local_order_id: (
        dict(db.rows[local_order_id]) if local_order_id in db.rows else None
    )
    osm._get_order = osm.get_order
    osm._get_active_exit_order = lambda pid: next(
        (
            dict(row)
            for row in db.rows.values()
            if row.get("position_id") == pid
            and row.get("kind") == "EXIT"
            and row.get("status") in {
                "EXIT_REQUESTED", "EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL",
            }
        ),
        None,
    )
    osm._notify_opportunity_ledger = lambda **kwargs: None
    osm.submit_exit = MagicMock(wraps=osm.submit_exit)

    lifecycle = {
        "state": "REPLACEMENT_PENDING",
        "replace_attempt": 1,
        "replacement_generation": 1,
        "replace_quantity": 1,
        "position_id": position_id,
        "client_id": client_id,
        "execution_mode": "paper",
        "old_local_order_id": old_local_id,
        "old_broker_order_id": old_broker_id,
        "last_ack_identity": old_broker_id,
    }
    durable_meta = {"unrelated": {"keep": True}, "exit_retry_liveness": dict(lifecycle)}

    def _position_row():
        return {
            "id": position_id,
            "underlying": "AVGO",
            "contract": contract,
            "direction": "CALL",
            "qty": 5,
            "avg_fill": 2.49,
            "underlying_entry": 350.0,
            "target_underlying": 360.0,
            "stop_underlying": 340.0,
            "client_id": client_id,
            "execution_mode": "paper",
            "quantity_remaining": 5,
            "meta": json.loads(json.dumps(durable_meta)),
        }

    class _PositionManager:
        def get_active_positions(self):
            return [_position_row()]

    def _persist_lifecycle(pos, *, expected_state=None, expected_generation=None):
        current = durable_meta.get("exit_retry_liveness") or {}
        if current.get("state", "NONE") != (expected_state or current.get("state", "NONE")):
            return False
        if int(current.get("replacement_generation", 0) or 0) != int(expected_generation or 0):
            return False
        durable_meta["exit_retry_liveness"] = json.loads(json.dumps(pos.exit_retry_liveness))
        return True

    # This is a fresh APExitEngine, hydrated only from the durable row.
    engine = APExitEngine(broker=broker, email=client_id)
    engine._emit_exit_event = MagicMock()
    engine._persist_exit_replace_attempt_to_db = _persist_lifecycle
    engine.order_state_machine = osm
    engine.osm = osm
    engine.seed_from_db(_PositionManager())
    position = engine.get_position(position_id)
    assert position is not None
    assert position.exit_retry_liveness["state"] == "REPLACEMENT_PENDING"
    assert position.pending_exit_replace_qty == 1
    assert position.pending_exit_replace_allowed is True
    assert position.exit_in_flight is False

    # Recovery is proof-only and restores one-shot authority without changing
    # position economics or creating proof-trade state.
    actions = recover_exit_engine(engine, broker=broker, osm=osm, order_monitor=None)
    assert [action.action for action in actions] == ["REPLACEMENT_PENDING"]
    assert position.pending_exit_replace_revalidated is True
    assert position.quantity_remaining == 5
    assert position.proof_logged is False
    assert db.created_reservations == 0
    assert broker.post_payloads == []

    monkeypatch.setattr(
        idempotency_guard,
        "_durable_exit_generation",
        lambda pos, client_id: (f"{client_id}|{pos.position_id}|1|1", 1),
    )
    monkeypatch.setattr(
        idempotency_guard,
        "_claim_durable_decision_generation",
        lambda **kwargs: {"claimed": True},
    )
    monkeypatch.setattr(idempotency_guard, "_update_durable_decision_generation", lambda *args, **kwargs: None)

    now = datetime.now(timezone.utc)
    position.signal = {"correlation_bucket": "OTHER", "pattern": "3-1-2", "score": 1.0, "tier": "A+"}
    position.current_bid = 2.00
    position.current_ask = 2.40
    position.current_option_price = 2.20
    position.current_underlying = 350.0
    position.option_bid_valid = True
    position.last_quote_update_ts = now
    position.last_option_quote_update_ts = now
    position.last_underlying_quote_update_ts = now

    core = APExecutionCore.__new__(APExecutionCore)
    core._pos_lock = threading.RLock()
    core._sector_lock = threading.RLock()
    core._position_count = 1
    core._sector_counts = {"OTHER": 1}
    core.master_control = SimpleNamespace(mode="paper", _trade_cooldowns={})
    core.paper = True
    core.email = client_id
    core.client_id = client_id
    core.broker = broker
    core.order_state_machine = osm
    core.feedback = MagicMock()
    core.feedback.get_setup_status.return_value = "READY"
    core._edge_logger = None
    engine.on_exit = core._on_position_close

    decision = ExitDecision(
        action="CLOSE_ALL",
        quantity=5,
        reason="DURABLE_REPLACEMENT_PENDING",
        urgency="HIGH",
        pnl_pct=-0.10,
    )
    assert engine._submit_exit_decision(position, decision) is True
    assert decision.quantity == 1
    assert db.created_reservations == 1
    assert osm.submit_exit.call_count == 1
    assert len(broker.post_payloads) == 1
    assert broker.post_payloads[0]["data"]["quantity"] == 1
    replacement = next(row for local_id, row in db.rows.items() if local_id != old_local_id)
    assert replacement["client_id"] == client_id
    assert replacement["execution_mode"] == "paper"
    assert replacement["qty"] == 1
    assert position.quantity_remaining == 5
    assert position.proof_logged is False
    assert durable_meta["exit_retry_liveness"]["state"] == "REPLACEMENT_OWNED_BY_NEW_GENERATION"

    # A second fresh object sees durable ownership and must not POST again.
    engine2 = APExitEngine(broker=broker, email=client_id)
    engine2._emit_exit_event = MagicMock()
    engine2.order_state_machine = osm
    engine2.osm = osm
    engine2.seed_from_db(_PositionManager())
    position2 = engine2.get_position(position_id)
    assert position2.exit_retry_liveness["state"] == "REPLACEMENT_OWNED_BY_NEW_GENERATION"
    actions2 = recover_exit_engine(engine2, broker=broker, osm=osm, order_monitor=None)
    assert [action.action for action in actions2] == ["REPLACEMENT_OWNED"]
    assert db.created_reservations == 1
    assert len(broker.post_payloads) == 1
    assert position2.quantity_remaining == 5
    assert position2.proof_logged is False


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
