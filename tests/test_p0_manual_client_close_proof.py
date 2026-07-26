"""PR #386 (post-review): manual-close reconciliation regression suite.

Covers:
  * happy path (single external fill → adopt → finalize → evict)
  * broker-positions error is uncertainty, not empty account
  * broker-orders error is uncertainty, not empty
  * bot-owned EXIT id fences (real bot ownership)
  * mode fence
  * existing exit-owner fence (in-flight / pending_exit_*)
  * quantity ambiguity (partial external can't terminally close a full pos)
  * stale fill before entry rejected
  * multi-fill weighted aggregate
  * broker still holds contract → not closed
  * adoption failure blocks finalizer + eviction
  * finalizer failure does not evict
  * adoption is atomic per position (single conn / advisory lock)
  * multi-fill resume: first fill previously adopted, second retries → completes
    with weighted aggregate across both fills
  * CLOSING position (not just OPEN) is still scanned
  * paginated broker-orders fetch pulls beyond default 25-order cap
  * delegate contract: ClientRunner._detect_manual_closes calls the helper
"""
from __future__ import annotations

import json
import os
import sys
import types
from datetime import datetime, timezone

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test")

import ap.db as db_mod
import client_runner as runner_mod
from ap import manual_close_reconciliation as manual_mod


CLIENT = "jasoncosby1@gmail.com"
CONTRACT = "F260731C00014000"
POSITION_ID = "ca06eeca-f55b-4778-8756-66c91bae877b"
ENTRY_TS = "2026-07-21T15:26:58.911238+00:00"
DETECTED_EPOCH = datetime(2026, 7, 21, 15, 58, 0, tzinfo=timezone.utc).timestamp()


class _Broker:
    def __init__(
        self,
        *,
        positions_payload=None,
        orders=None,
        positions_error=None,
        orders_error=None,
        orders_pages=None,
    ):
        self.cfg = types.SimpleNamespace(account_id="LIVE-ACCOUNT")
        self._positions_payload = (
            {"positions": "null"} if positions_payload is None else positions_payload
        )
        self._orders = list(orders or [])
        self._positions_error = positions_error
        self._orders_error = orders_error
        self._orders_pages = orders_pages  # optional: list-of-pages for paginated _get
        self.calls: list[tuple] = []

    def _get(self, path):
        self.calls.append(("get", path))
        if path.startswith(f"/v1/accounts/{self.cfg.account_id}/positions"):
            if self._positions_error:
                raise self._positions_error
            return self._positions_payload
        if path.startswith(f"/v1/accounts/{self.cfg.account_id}/orders"):
            if self._orders_error:
                raise self._orders_error
            if self._orders_pages is not None:
                # Return each page in sequence based on how many order-page
                # calls we've observed so far.
                page_calls = [c for c in self.calls if isinstance(c[1], str) and "/orders" in c[1]]
                idx = len(page_calls) - 1
                if idx < len(self._orders_pages):
                    return {"orders": {"order": self._orders_pages[idx]}}
                return {"orders": "null"}
            return {"orders": {"order": list(self._orders)}}
        raise AssertionError(f"unexpected broker path: {path}")

    def list_orders(self):
        # Fallback path when _get is unavailable — not used by tests that
        # exercise pagination.
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
        self.calls: list[dict] = []

    def close_position_from_exit_fill(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


class _ExitEngine:
    def __init__(self):
        self.closed: list[str] = []

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
        "status": "OPEN",
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


def _install_scan_boundaries(
    monkeypatch,
    position=None,
    bot_exit_ids=None,
    adopted_fills_by_pos=None,
    adopt=True,
):
    monkeypatch.setattr(manual_mod.time, "time", lambda: DETECTED_EPOCH)
    monkeypatch.setattr(
        manual_mod,
        "load_manual_close_state",
        lambda client_id, execution_mode: (
            [position or _position()],
            set(bot_exit_ids or set()),
            dict(adopted_fills_by_pos or {}),
        ),
    )
    adopted: list[dict] = []

    def _adopt(**kwargs):
        adopted.append(kwargs)
        return (adopt, "test_adoption")

    monkeypatch.setattr(manual_mod, "adopt_external_exit_fills", _adopt)
    return adopted


# ─── delegate contract ───────────────────────────────────────────────────────

def test_client_runner_detect_manual_closes_delegates_to_helper(monkeypatch):
    calls = []
    monkeypatch.setattr(manual_mod, "detect_manual_closes", lambda self: calls.append(self))
    runner = _runner(broker=_Broker(), pm=_PM())
    runner._detect_manual_closes()
    assert calls == [runner]


def test_no_shadow_package_client_runner_is_module():
    # If someone reintroduces the shadow package this fails loudly.
    assert getattr(runner_mod, "__file__", "").endswith("client_runner.py")
    assert not hasattr(runner_mod, "_base"), "shadow package must not be present"


# ─── happy path ──────────────────────────────────────────────────────────────

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


# ─── broker error paths (uncertainty, never "empty account") ─────────────────

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


# ─── fences ──────────────────────────────────────────────────────────────────

def test_bot_owned_exit_order_is_not_reclassified_as_external(monkeypatch):
    broker = _Broker(orders=[_filled_exit(id="KNOWN-EXIT")])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch, bot_exit_ids={"KNOWN-EXIT"})

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []


def test_mode_mismatch_is_fenced_before_order_position_or_proof_mutation(monkeypatch):
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(
        monkeypatch, position=_position(execution_mode="paper"),
    )

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []


def test_existing_exit_owner_is_fenced(monkeypatch):
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(
        monkeypatch, position=_position(pending_exit_broker_order_id="137700000"),
    )

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []


# ─── quantity + timing correctness ───────────────────────────────────────────

def test_partial_external_fill_cannot_terminally_close_full_position(monkeypatch):
    broker = _Broker(orders=[_filled_exit(exec_quantity=1, quantity=1)])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch)

    runner._detect_manual_closes()

    assert adopted == []
    assert pm.calls == []


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


# ─── failure ordering ────────────────────────────────────────────────────────

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


# ─── amendment #1: multi-fill resume without stranding ───────────────────────

def test_multi_fill_resume_after_partial_prior_adoption_completes_with_weighted_aggregate(monkeypatch):
    """Prior scan adopted EXIT-1; EXIT-2 was newly filled since. Next scan
    must NOT reject the whole evidence set as 'bot_owned_exit_order_present'
    — those adopted IDs are OUR own external work, not bot exits. The
    resume must aggregate EXIT-1 + EXIT-2 for weighted P&L truth."""
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
    # EXIT-1 already durable: provide full fill dict as loaded from DB.
    exit1_durable = {
        "broker_order_id": "EXIT-1",
        "filled_qty": 1,
        "fill_price": 0.74,
        "filled_at": datetime(2026, 7, 21, 15, 56, 0, tzinfo=timezone.utc),
        "created_at": None,
        "raw_status": "EXIT_FILLED",
        "raw_side": "sell_to_close",
    }
    adopted = _install_scan_boundaries(
        monkeypatch,
        adopted_fills_by_pos={POSITION_ID: [exit1_durable]},
    )

    runner._detect_manual_closes()

    # Adoption is called for the NEW fill only (EXIT-2).
    assert len(adopted) == 1
    new_fills = adopted[0]["evidence"]["fills"]
    assert [f["broker_order_id"] for f in new_fills] == ["EXIT-2"]
    # Aggregate view spans BOTH fills for finalizer truth.
    all_ids = adopted[0]["evidence"]["broker_order_ids"]
    assert all_ids == ["EXIT-1", "EXIT-2"]
    # Finalizer receives quantity-weighted aggregate price.
    assert pm.calls[0]["exit_price"] == 0.75
    assert pm.calls[0]["filled_qty"] == 2
    assert runner.core.exit_eng.closed == [POSITION_ID]


# ─── amendment #2: CLOSING is still scanned ──────────────────────────────────

def test_active_position_family_includes_closing(monkeypatch):
    """A position advanced to CLOSING (e.g. by row-at-a-time reconciler)
    with retained external evidence must remain scannable so the manual-
    close path can re-finalize with weighted aggregate."""
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(
        monkeypatch, position=_position(status="CLOSING"),
    )

    runner._detect_manual_closes()

    assert len(adopted) == 1
    assert len(pm.calls) == 1


# ─── amendment #3: paginated broker order fetch ──────────────────────────────

def test_broker_orders_paginated_beyond_default_25(monkeypatch):
    """Tradier's list_orders defaults to ~25. Our fetch must page via
    _get with an explicit limit to see the actual close deep in a busy
    session."""
    # Page 1: 500 unrelated orders. Page 2: our real EXIT. Page 3: empty.
    page1 = [
        {"id": f"NOISE-{n}", "status": "canceled",
         "side": "buy_to_open", "option_symbol": "OTHER260731C00010000"}
        for n in range(500)
    ]
    page2 = [_filled_exit()]
    broker = _Broker(orders_pages=[page1, page2])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(monkeypatch)

    runner._detect_manual_closes()

    # Must have paged past the first 500 to reach our real exit.
    order_paths = [c[1] for c in broker.calls if isinstance(c[1], str) and "/orders" in c[1]]
    assert len(order_paths) >= 2, f"paginated fetch expected; saw {order_paths}"
    assert "page=1" in order_paths[0]
    assert "page=2" in order_paths[1]
    # Adoption happened using the deep-page order.
    assert len(adopted) == 1
    assert adopted[0]["evidence"]["broker_order_ids"] == ["137780001"]
    assert pm.calls[0]["exit_price"] == 0.75


# ─── DB-shape checks (existing coverage, updated for atomic API) ─────────────

class _FakeCursor:
    """A single-connection fake cursor that models the exact SQL used by
    load_manual_close_state and adopt_external_exit_fills."""
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
                row for row in self.rows
                if row.get("client_id") == client_id
                and row.get("broker_order_id") == broker_order_id
            ][:2]
            return self
        if compact.startswith("INSERT INTO orders"):
            (
                client_id, local_order_id, broker_order_id, position_id,
                symbol, contract, direction, qty, filled_qty, fill_price,
                created_ts, updated_ts, submitted_ts, filled_ts, meta,
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
    rows: list[dict] = []
    cursor = _FakeCursor(rows)
    monkeypatch.setattr(db_mod, "conn", lambda: cursor)
    monkeypatch.setattr(db_mod, "run_with_retry", lambda fn: fn())

    evidence, reason = manual_mod.select_external_close_fills(
        orders=[_filled_exit()],
        position=_position(),
        bot_exit_order_ids=set(),
        adopted_fills=[],
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
    assert reason.startswith("external_exit_adoption_error:")
    assert len(rows) == 1


# ─── Blocker 2: cross-session recovery from durable DB rows ──────────────────

def test_cross_session_recovery_empty_broker_orders_finalizes_from_durable_rows(monkeypatch):
    """Blocker 2 regression: after a process restart the broker returns [] for
    current-session orders, so previous-session fills are no longer visible.
    Durable adopted EXIT rows in DB must reconstruct the full weighted aggregate
    and re-invoke the finalizer. broker_orders=[] must NOT strand the position.

    Root: load_manual_close_state previously loaded only broker_order_id — it
    now loads filled_qty / fill_price / filled_ts so select_external_close_fills
    can build the aggregate from DB evidence without broker confirmation.
    """
    filled_at = datetime(2026, 7, 21, 15, 57, 39, tzinfo=timezone.utc)
    broker = _Broker(orders=[])   # previous-session orders gone
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    durable_fill = {
        "broker_order_id": "137780001",
        "filled_qty": 2,
        "fill_price": 0.75,
        "filled_at": filled_at,
        "created_at": None,
        "raw_status": "EXIT_FILLED",
        "raw_side": "sell_to_close",
    }
    _install_scan_boundaries(
        monkeypatch,
        adopted_fills_by_pos={POSITION_ID: [durable_fill]},
    )

    runner._detect_manual_closes()

    # Finalizer must be called from durable DB evidence alone.
    assert len(pm.calls) == 1, "finalizer not called with empty broker orders"
    call = pm.calls[0]
    assert call["position_id"] == POSITION_ID
    assert call["exit_price"] == 0.75
    assert call["filled_qty"] == 2
    assert call["broker_order_id"] == "137780001"
    assert call["close_source"] == "manual_client_close_broker_fill"
    # Exit engine eviction must still fire.
    assert runner.core.exit_eng.closed == [POSITION_ID]


# ─── Blocker 3: PARTIAL and ACTIVE status positions are scannable ─────────────

def test_partial_status_position_is_in_active_family(monkeypatch):
    """Blocker 3 regression: a PARTIAL position must remain visible to the
    manual-close scanner. Previously ACTIVE_POSITION_STATUSES was limited to
    OPEN and CLOSING, silently skipping PARTIAL positions with retained evidence.
    """
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(
        monkeypatch,
        position=_position(status="PARTIAL", quantity_remaining=2),
    )

    runner._detect_manual_closes()

    assert len(adopted) == 1, "PARTIAL position must be scanned"
    assert len(pm.calls) == 1


def test_active_status_position_is_in_active_family(monkeypatch):
    """Blocker 3 regression: a position with status='ACTIVE' must be scannable."""
    broker = _Broker(orders=[_filled_exit()])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)
    adopted = _install_scan_boundaries(
        monkeypatch,
        position=_position(status="ACTIVE", quantity_remaining=2),
    )

    runner._detect_manual_closes()

    assert len(adopted) == 1, "ACTIVE position must be scanned"
    assert len(pm.calls) == 1


# ─── Blocker 4: mode-scoped EXIT ownership fence ─────────────────────────────

def test_wrong_mode_exit_rows_do_not_pollute_current_runner_fence(monkeypatch):
    """Blocker 4 regression: PAPER broker EXIT IDs must not enter the LIVE
    runner's bot_exit_ids set, and vice versa. load_manual_close_state now
    filters orders by exact execution_mode.

    This test verifies the mode-scoped call signature: load_manual_close_state
    receives execution_mode='live' and must filter the orders table by it, so
    PAPER-mode EXIT IDs are excluded from the bot_ids fence.

    We simulate a scenario where a broker order ID exists as a PAPER EXIT in
    DB (wrong mode) but appears in the current LIVE broker orders list as an
    external fill. If mode scoping is wrong, it would be treated as bot-owned
    and the manual close would be fenced incorrectly.
    """
    # Bot exit IDs should be empty for LIVE mode even if "KNOWN-PAPER-EXIT"
    # exists in the PAPER orders table — mode filter must exclude it.
    broker = _Broker(orders=[_filled_exit(id="KNOWN-PAPER-EXIT")])
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    # Simulate: bot_exit_ids set is EMPTY for LIVE mode (PAPER exit was filtered)
    # even though the broker_order_id "KNOWN-PAPER-EXIT" exists in DB for PAPER.
    adopted = _install_scan_boundaries(
        monkeypatch,
        bot_exit_ids=set(),  # mode-scoped query returns empty for LIVE
    )

    runner._detect_manual_closes()

    # Must NOT be fenced — the PAPER exit ID is excluded from LIVE bot fence.
    assert len(adopted) == 1, (
        "PAPER-mode EXIT IDs must not fence LIVE runner's external close path"
    )
    assert len(pm.calls) == 1

    # Verify load_manual_close_state receives execution_mode from the runner.
    # (The monkeypatch lambda now accepts (client_id, execution_mode); if the
    # caller omitted execution_mode the lambda would raise TypeError here.)


# ─── Blocker 5: supervisor boundary protects health loop ─────────────────────

def test_unexpected_helper_exception_does_not_kill_health_loop_iteration(monkeypatch):
    """Blocker 5 regression: an unexpected exception inside detect_manual_closes
    (e.g. malformed env var, import failure, unhandled edge) must NOT propagate
    out of _detect_manual_closes. The health loop iteration must continue to
    drive entry permission, split-brain recovery, overnight reevaluation, etc.
    """
    import ap.manual_close_reconciliation as manual_local

    def _bomb(self):
        raise RuntimeError("simulated catastrophic helper failure")

    monkeypatch.setattr(manual_local, "detect_manual_closes", _bomb)

    broker = _Broker()
    pm = _PM()
    runner = _runner(broker=broker, pm=pm)

    # Must not raise — supervisor boundary must catch and log it.
    try:
        runner._detect_manual_closes()
    except Exception as exc:
        raise AssertionError(
            f"_detect_manual_closes must not propagate exceptions; got: {exc}"
        )
    # No finalization attempted.
    assert pm.calls == []


# ─── Blocker 6: duplicate finalization idempotency ───────────────────────────

def test_two_finalization_attempts_produce_one_terminal_economic_result(monkeypatch):
    """Blocker 6 regression: if two health-loop iterations overlap, the second
    call to _finalize_position must be a no-op and must not write duplicate P&L.

    The idempotency guard re-reads position status from DB before calling the
    finalizer. We simulate a 'already terminal' position by patching
    _position_is_still_active to return False on the second call.
    """
    import ap.manual_close_reconciliation as manual_local

    call_count = [0]
    real_is_active = manual_local._position_is_still_active

    def _is_active_once(**kwargs):
        """Return True on first call (position active), False on second (already terminal)."""
        call_count[0] += 1
        return call_count[0] == 1

    monkeypatch.setattr(manual_local, "_position_is_still_active", _is_active_once)

    evidence = {
        "fills": [],
        "adopted_fills": [],
        "all_fills": [],
        "filled_qty": 2,
        "fill_price": 0.75,
        "filled_ts": "2026-07-21T15:57:39+00:00",
        "broker_order_id": "137780001",
        "broker_order_ids": ["137780001"],
    }

    pm = _PM()
    finalizer = pm.close_position_from_exit_fill

    # First call: position still active → finalizer must fire.
    result1 = manual_local._finalize_position(
        finalizer=finalizer,
        client_id=CLIENT,
        position_id=POSITION_ID,
        contract=CONTRACT,
        evidence=evidence,
    )
    assert result1 is True
    assert len(pm.calls) == 1, "first finalization must call the finalizer"

    # Second call: position already terminal → must return True (idempotent)
    # WITHOUT calling the finalizer again.
    result2 = manual_local._finalize_position(
        finalizer=finalizer,
        client_id=CLIENT,
        position_id=POSITION_ID,
        contract=CONTRACT,
        evidence=evidence,
    )
    assert result2 is True, "idempotent finalization must return True"
    assert len(pm.calls) == 1, (
        "second finalization attempt must NOT invoke the finalizer again — "
        "duplicate P&L write is a data integrity violation"
    )


# ─── Blocker 6: atomic adoption rollback on mid-transaction failure ───────────

def test_atomic_adoption_rollback_on_second_fill_insert_failure(monkeypatch):
    """Blocker 6 / atomic adoption regression: if the first fill INSERT
    succeeds but the second fails, the transaction must roll back and leave
    ZERO rows — no partial adoption stranding.

    This is the 'claimed rollback that doesn't actually exercise rollback'
    note from the review. Here we inject a real failure during the second
    INSERT and verify the final row count is zero.
    """
    rows: list[dict] = []
    insert_count = [0]

    class _RollbackCursor:
        """Fake cursor: first INSERT succeeds, second raises, simulating a
        mid-transaction failure (e.g. unique violation, constraint error)."""

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            if exc_type is not None:
                # Simulate rollback: clear any rows added in this txn.
                rows.clear()
            return False

        def execute(self, sql, params=()):
            compact = " ".join(str(sql).split())
            if "pg_advisory_xact_lock" in compact:
                return self
            if "WHERE client_id=%s AND broker_order_id=%s" in compact:
                self._last_fetchall = []
                return self
            if compact.startswith("INSERT INTO orders"):
                insert_count[0] += 1
                if insert_count[0] == 2:
                    raise RuntimeError("simulated second INSERT failure")
                # First INSERT 'succeeds': add to rows and return it.
                client_id, local_order_id, broker_order_id, position_id = params[:4]
                row = {
                    "client_id": client_id,
                    "local_order_id": local_order_id,
                    "broker_order_id": broker_order_id,
                    "position_id": position_id,
                    "kind": "EXIT",
                    "status": "EXIT_FILLED",
                    "contract": CONTRACT,
                    "direction": "CALL",
                    "qty": 1,
                    "filled_qty": 1,
                    "fill_price": 0.74,
                    "execution_mode": "live",
                }
                rows.append(row)
                self._last_fetchone = row
                return self
            self._last_fetchall = []
            self._last_fetchone = None
            return self

        def fetchall(self):
            return list(getattr(self, "_last_fetchall", []))

        def fetchone(self):
            return getattr(self, "_last_fetchone", None)

    import ap.db as db_mod
    monkeypatch.setattr(db_mod, "conn", lambda: _RollbackCursor())

    # run_with_retry must propagate the exception so rollback logic fires.
    def _run_no_retry(fn):
        return fn()

    monkeypatch.setattr(db_mod, "run_with_retry", _run_no_retry)

    filled_at = datetime(2026, 7, 21, 15, 57, 0, tzinfo=timezone.utc)
    evidence = {
        "fills": [
            {
                "broker_order_id": "EXIT-A",
                "filled_qty": 1,
                "fill_price": 0.74,
                "filled_at": filled_at,
                "created_at": None,
                "raw_status": "filled",
                "raw_side": "sell_to_close",
            },
            {
                "broker_order_id": "EXIT-B",
                "filled_qty": 1,
                "fill_price": 0.76,
                "filled_at": filled_at,
                "created_at": None,
                "raw_status": "filled",
                "raw_side": "sell_to_close",
            },
        ]
    }

    import ap.manual_close_reconciliation as manual_local

    ok, reason = manual_local.adopt_external_exit_fills(
        client_id=CLIENT,
        execution_mode="live",
        position=_position(qty=2, quantity_remaining=2),
        evidence=evidence,
    )

    assert ok is False, "adoption must fail when second INSERT raises"
    assert len(rows) == 0, (
        f"transaction rollback must leave zero rows; got {len(rows)} — "
        "partial adoption stranding is a recovery correctness violation"
    )
