import json
import os
import types
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import ap.db as db_mod
import client_runner as runner_mod
from client_runner import manual_close_reconciliation as manual_mod


CLIENT = "jasoncosby1@gmail.com"
CONTRACT = "F260731C00014000"
POSITION_ID = "ca06eeca-f55b-4778-8756-66c91bae877b"
ENTRY_TS = "2026-07-21T15:26:58.911238+00:00"
DETECTED_EPOCH = datetime(
    2026, 7, 21, 15, 58, 0, tzinfo=timezone.utc
).timestamp()


class _Broker:
    def __init__(
        self,
        *,
        positions_payload=None,
        orders=None,
        positions_error=None,
        orders_error=None,
    ):
        self.cfg = types.SimpleNamespace(account_id="LIVE-ACCOUNT")
        self._positions_payload = (
            {"positions": "null"} if positions_payload is None else positions_payload
        )
        self._orders = list(orders or [])
        self._positions_error = positions_error
        self._orders_error = orders_error
        self.calls = []

    def _get(self, path):
        self.calls.append(("get", path))
        if self._positions_error:
            raise self._positions_error
        return self._positions_payload

    def list_orders(self):
        self.calls.append(("list_orders", None))
        if self._orders_error:
            raise self._orders_error
        return list(self._orders)

    def place_order(self, *args, **kwargs):  # pragma: no cover
        raise AssertionError("manual-close reconciliation must not submit orders")

    def cancel_order(self, *args, **kwargs):  # pragma: no cover
        raise AssertionError("manual-close reconciliation must not cancel orders")


class _PM:
    def __init__(self, result=True):
        self.result = result
        self.calls = []

    def close_position_from_exit_fill(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


class _ExitEngine:
    def __init__(self):
        self.closed = []

    def mark_position_closed(self, position_id):
        self.closed.append(position_id)


def _position(**overrides):
    row = {
        "id": POSITION_ID,
        "client_id": CLIENT,
        "contract": CONTRACT,
        "underlying": "F",
        "avg_fill": 0.73,
        "qty": 2,
        "quantity_remaining": None,
        "side": "CALL",
        "local_order_id": "5a427bfb-9bd5-4e0d-81ac-8b6a40ba8795",
        "entry_ts": ENTRY_TS,
        "opened_at": ENTRY_TS,
        "execution_mode": "live",
        "exit_in_flight": False,
        "pending_exit_broker_order_id": None,
        "pending_exit_local_order_id": None,
    }
    row.update(overrides)
    return row


def _filled_exit(**overrides):
    row = {
        "id": "137780001",
        "status": "filled",
        "side": "sell_to_close",
        "symbol": "F",
        "option_symbol": CONTRACT,
        "quantity": 2,
        "exec_quantity": 2,
        "avg_fill_price": 0.75,
        "transaction_date": "2026-07-21T15:57:39.880419Z",
    }
    row.update(overrides)
    return row


def _runner(*, broker, pm):
    runner = runner_mod.ClientRunner.__new__(runner_mod.ClientRunner)
    runner.email = CLIENT
    runner.mode = "LIVE"
    runner.broker = broker
    runner.position_manager = pm
    runner.core = types.SimpleNamespace(exit_eng=_ExitEngine())
    runner._last_manual_close_check_ts = 0.0
    return runner


def _install_scan_boundaries(monkeypatch, position=None, known_ids=None, adopt=True):
    monkeypatch.setattr(manual_mod.time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        manual_mod,
        "load_manual_close_state",
        lambda client_id: ([position or _position()], set(known_ids or set())),
    )
    adopted = []

    def _adopt(**kwargs):
        adopted.append(kwargs)
        return (adopt, "test_adoption")

    monkeypatch.setattr(manual_mod, "adopt_external_exit_fills", _adopt)
    return adopted


def test_shadow_package_patches_supervisor_class_binding():
    assert runner_mod._base.ClientRunner is runner_mod.ClientRunner
    assert runner_mod.ClientRunner._detect_manual_closes is manual_mod.detect_manual_closes


def test_manual_close_adopts_exact_fill_then_calls_canonical_finalizer(monkeypatch):
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch)

    runner._detect_manual_closes()

    assert len(adopted) == 1
    assert adopted[0]["client_id"] == CLIENT
    assert adopted[0]["execution_mode"] == "live"
    assert adopted[0]["position"]["id"] == POSITION_ID
    assert adopted[0]["evidence"]["broker_order_ids"] == ["137780001"]

    assert len(pm.calls) == 1
    call = pm.calls[0]
    assert call["position_id"] == POSITION_ID
    assert call["exit_price"] == 0.75
    assert call["filled_qty"] == 2
    assert call["filled_ts"] == "2026-07-21T15:57:39.880419+00:00"
    assert call["broker_order_id"] == "137780001"
    assert call["close_source"] == "manual_client_close_broker_fill"
    assert call["close_confidence"] == "HIGH"
    assert "MANUAL_CLIENT_CLOSE_BROKER_CONFIRMED" in call["exit_reason"]
    assert runner.core.exit_eng.closed == [POSITION_ID]
    assert broker.calls == [
        ("get", "/v1/accounts/LIVE-ACCOUNT/positions"),
        ("list_orders", None),
    ]


def test_positions_query_failure_never_reads_orders_or_mutates(monkeypatch):
    broker = _Broker(
        positions_error=RuntimeError("tradier unavailable"),
        orders=[_filled_exit()],
    )
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch)

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []
    assert runner.core.exit_eng.closed == []
    assert broker.calls == [("get", "/v1/accounts/LIVE-ACCOUNT/positions")]


def test_orders_query_failure_never_adopts_or_finalizes(monkeypatch):
    broker = _Broker(orders_error=RuntimeError("orders unavailable"))
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch)

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []
    assert runner.core.exit_eng.closed == []


def test_bot_owned_exit_order_is_not_reclassified_as_external(monkeypatch):
    broker = _Broker(orders=[_filled_exit(id="KNOWN-EXIT")])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch, known_ids={"KNOWN-EXIT"})

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []
    assert runner.core.exit_eng.closed == []


def test_mode_mismatch_is_fenced_before_order_position_or_proof_mutation(monkeypatch):
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(
        monkeypatch,
        position=_position(execution_mode="paper"),
    )

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []
    assert runner.core.exit_eng.closed == []


def test_existing_exit_owner_is_fenced(monkeypatch):
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(
        monkeypatch,
        position=_position(pending_exit_broker_order_id="137700000"),
    )

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []


def test_partial_external_fill_cannot_terminally_close_full_position(monkeypatch):
    broker = _Broker(orders=[_filled_exit(exec_quantity=1, quantity=1)])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch)

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []
    assert runner.core.exit_eng.closed == []


def test_stale_same_contract_fill_before_entry_is_rejected(monkeypatch):
    broker = _Broker(
        orders=[
            _filled_exit(
                transaction_date="2026-07-21T15:20:00Z",
                avg_fill_price=9.99,
            )
        ]
    )
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch)

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []


def test_multiple_manual_fills_use_quantity_weighted_broker_price(monkeypatch):
    broker = _Broker(
        orders=[
            _filled_exit(
                id="EXIT-1",
                exec_quantity=1,
                quantity=1,
                avg_fill_price=0.74,
                transaction_date="2026-07-21T15:56:00Z",
            ),
            _filled_exit(
                id="EXIT-2",
                exec_quantity=1,
                quantity=1,
                avg_fill_price=0.76,
                transaction_date="2026-07-21T15:57:39Z",
            ),
        ]
    )
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch)

    runner._detect_manual_closes()

    assert len(adopted) == 1
    evidence = adopted[0]["evidence"]
    assert evidence["broker_order_ids"] == ["EXIT-1", "EXIT-2"]
    assert evidence["fill_price"] == 0.75
    assert len(pm.calls) == 1
    assert pm.calls[0]["exit_price"] == 0.75
    assert pm.calls[0]["filled_qty"] == 2
    assert pm.calls[0]["broker_order_id"] == "EXIT-2"


def test_open_broker_contract_is_not_considered_manually_closed(monkeypatch):
    broker = _Broker(
        positions_payload={
            "positions": {
                "position": {
                    "symbol": CONTRACT,
                    "quantity": 2,
                    "cost_basis": 146,
                }
            }
        },
        orders=[_filled_exit()],
    )
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch)

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []
    assert runner.core.exit_eng.closed == []
    assert broker.calls == [("get", "/v1/accounts/LIVE-ACCOUNT/positions")]


def test_failed_order_adoption_blocks_position_and_proof_finalization(monkeypatch):
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch, adopt=False)

    runner._detect_manual_closes()

    assert len(adopted) == 1
    assert pm.calls == []
    assert runner.core.exit_eng.closed == []


def test_finalizer_failure_does_not_evict_exit_engine_after_adoption(monkeypatch):
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM(result=False)
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch)

    runner._detect_manual_closes()

    assert len(adopted) == 1
    assert len(pm.calls) == 1
    assert runner.core.exit_eng.closed == []


class _FakeCursor:
    def __init__(self, rows):
        self.rows = rows
        self._fetchall = []
        self._fetchone = None
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, sql, params=()):
        compact = " ".join(str(sql).split())
        self.executed.append((compact, params))
        self._fetchall = []
        self._fetchone = None

        if "pg_advisory_xact_lock" in compact:
            return self
        if "WHERE client_id=%s AND broker_order_id=%s" in compact:
            client_id, broker_order_id = params
            self._fetchall = [
                row
                for row in self.rows
                if row.get("client_id") == client_id
                and row.get("broker_order_id") == broker_order_id
            ][:2]
            return self
        if compact.startswith("INSERT INTO orders"):
            (
                client_id,
                local_order_id,
                broker_order_id,
                position_id,
                symbol,
                contract,
                direction,
                qty,
                filled_qty,
                fill_price,
                created_ts,
                updated_ts,
                submitted_ts,
                filled_ts,
                meta,
                execution_mode,
            ) = params
            if any(row.get("local_order_id") == local_order_id for row in self.rows):
                return self
            row = {
                "client_id": client_id,
                "local_order_id": local_order_id,
                "broker_order_id": broker_order_id,
                "position_id": position_id,
                "kind": "EXIT",
                "status": "EXIT_FILLED",
                "symbol": symbol,
                "contract": contract,
                "direction": direction,
                "qty": qty,
                "filled_qty": filled_qty,
                "fill_price": fill_price,
                "created_ts": created_ts,
                "updated_ts": updated_ts,
                "submitted_ts": submitted_ts,
                "filled_ts": filled_ts,
                "meta": json.loads(meta),
                "execution_mode": execution_mode,
            }
            self.rows.append(row)
            self._fetchone = row
            return self
        if "WHERE local_order_id=%s" in compact:
            local_order_id = params[0]
            self._fetchall = [
                row for row in self.rows if row.get("local_order_id") == local_order_id
            ][:2]
            return self
        raise AssertionError(f"unexpected SQL: {compact}")

    def fetchall(self):
        return list(self._fetchall)

    def fetchone(self):
        return self._fetchone


def test_external_fill_adoption_writes_real_exit_lifecycle_shape(monkeypatch):
    rows = []
    cursor = _FakeCursor(rows)
    monkeypatch.setattr(db_mod, "conn", lambda: cursor)
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())

    evidence, reason = manual_mod.select_external_close_fills(
        orders=[_filled_exit()],
        position=_position(),
        known_exit_order_ids=set(),
        detected_at=datetime.fromtimestamp(DETECTED_EPOCH, tz=timezone.utc),
    )
    assert reason == "exact_external_broker_fill"

    ok, adopt_reason = manual_mod.adopt_external_exit_fills(
        client_id=CLIENT,
        execution_mode="live",
        position=_position(),
        evidence=evidence,
    )

    assert ok is True
    assert adopt_reason == "external_exit_adoption_complete"
    assert len(rows) == 1
    row = rows[0]
    assert row["client_id"] == CLIENT
    assert row["position_id"] == POSITION_ID
    assert row["local_order_id"] == f"external-exit:{CLIENT}:137780001"
    assert row["broker_order_id"] == "137780001"
    assert row["kind"] == "EXIT"
    assert row["status"] == "EXIT_FILLED"
    assert row["contract"] == CONTRACT
    assert row["direction"] == "CALL"
    assert row["qty"] == 2
    assert row["filled_qty"] == 2
    assert row["fill_price"] == 0.75
    assert row["execution_mode"] == "live"
    assert row["meta"]["external_broker_order"] is True
    assert row["meta"]["adopted_without_submit"] is True


def test_external_fill_adoption_is_idempotent_for_exact_existing_row(monkeypatch):
    filled_at = datetime(2026, 7, 21, 15, 57, 39, tzinfo=timezone.utc)
    rows = [
        {
            "client_id": CLIENT,
            "local_order_id": f"external-exit:{CLIENT}:137780001",
            "broker_order_id": "137780001",
            "position_id": POSITION_ID,
            "kind": "EXIT",
            "status": "EXIT_FILLED",
            "symbol": "F",
            "contract": CONTRACT,
            "direction": "CALL",
            "qty": 2,
            "filled_qty": 2,
            "fill_price": 0.75,
            "filled_ts": filled_at,
            "execution_mode": "live",
        }
    ]
    cursor = _FakeCursor(rows)
    monkeypatch.setattr(db_mod, "conn", lambda: cursor)
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())

    evidence = {
        "fills": [
            {
                "broker_order_id": "137780001",
                "filled_qty": 2,
                "fill_price": 0.75,
                "filled_at": filled_at,
                "raw_status": "filled",
                "raw_side": "sell_to_close",
            }
        ]
    }
    ok, reason = manual_mod.adopt_external_exit_fills(
        client_id=CLIENT,
        execution_mode="live",
        position=_position(),
        evidence=evidence,
    )

    assert ok is True
    assert reason == "external_exit_adoption_complete"
    assert len(rows) == 1


def test_external_fill_adoption_rejects_existing_mode_or_position_mismatch(monkeypatch):
    filled_at = datetime(2026, 7, 21, 15, 57, 39, tzinfo=timezone.utc)
    rows = [
        {
            "client_id": CLIENT,
            "local_order_id": f"external-exit:{CLIENT}:137780001",
            "broker_order_id": "137780001",
            "position_id": "wrong-position",
            "kind": "EXIT",
            "status": "EXIT_FILLED",
            "symbol": "F",
            "contract": CONTRACT,
            "direction": "CALL",
            "qty": 2,
            "filled_qty": 2,
            "fill_price": 0.75,
            "filled_ts": filled_at,
            "execution_mode": "paper",
        }
    ]
    cursor = _FakeCursor(rows)
    monkeypatch.setattr(db_mod, "conn", lambda: cursor)
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())

    evidence = {
        "fills": [
            {
                "broker_order_id": "137780001",
                "filled_qty": 2,
                "fill_price": 0.75,
                "filled_at": filled_at,
                "raw_status": "filled",
                "raw_side": "sell_to_close",
            }
        ]
    }
    ok, reason = manual_mod.adopt_external_exit_fills(
        client_id=CLIENT,
        execution_mode="live",
        position=_position(),
        evidence=evidence,
    )

    assert ok is False
    assert reason == "external_exit_adoption_existing_row"
    assert len(rows) == 1
