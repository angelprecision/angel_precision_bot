"""P0 regression tests for the diagnostics-only FILLED ENTRY trace seam."""

from __future__ import annotations

import ast
import importlib.util
import os
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
TRACE_PATH = REPO_ROOT / "ap" / "trace.py"
TRACE_SPEC = importlib.util.spec_from_file_location("p0_trace_under_test", TRACE_PATH)
assert TRACE_SPEC is not None and TRACE_SPEC.loader is not None
trace_module = importlib.util.module_from_spec(TRACE_SPEC)
TRACE_SPEC.loader.exec_module(trace_module)


def _exact_filled_entry_trace() -> None:
    trace_module.trace_gate(
        "signal-id",
        "SMCI",
        "ORDER_FILLED",
        "PASS",
        reason="entry_filled",
        trigger_price=1.49,
        contracts=1,
        side="PUT",
        kind="ENTRY",
        tier="B",
        plan_id="plan-id",
        execution_mode="live",
        client_id="client@example.com",
    )


def test_filled_entry_trace_accepts_exact_production_context_kwargs(monkeypatch) -> None:
    """The fresh-fill diagnostic shape must return normally and be rendered."""
    calls = []
    monkeypatch.setattr(trace_module.log, "info", lambda *args, **kwargs: calls.append((args, kwargs)))

    _exact_filled_entry_trace()

    assert len(calls) == 1
    message, values = calls[0]
    rendered = message[0] % message[1:]
    assert values == {}
    assert "side=PUT" in rendered
    assert "kind=ENTRY" in rendered
    assert "tier=B" in rendered
    assert "plan_id=plan-id" in rendered
    assert "execution_mode=live" in rendered
    assert "client_id=client@example.com" in rendered


def test_trace_logger_failure_cannot_escape_into_money_state(monkeypatch) -> None:
    """Even a broken logging backend cannot acquire fill-handoff authority."""

    def _raise(*_args, **_kwargs):
        raise RuntimeError("simulated logging backend failure")

    monkeypatch.setattr(trace_module.log, "info", _raise)
    monkeypatch.setattr(trace_module.log, "debug", _raise)

    _exact_filled_entry_trace()


def test_unrenderable_future_context_is_also_non_authoritative() -> None:
    """A hostile diagnostic object's string conversion cannot break tracing."""

    class BrokenString:
        def __str__(self) -> str:
            raise RuntimeError("cannot render")

    trace_module.trace_gate(
        "signal-2",
        "SMCI",
        "ORDER_FILLED",
        "PASS",
        future_diagnostic=BrokenString(),
    )


def test_legacy_trace_fields_keep_the_existing_normalized_shape(monkeypatch) -> None:
    """Legacy callers retain every normalized field without new context."""
    calls = []
    monkeypatch.setattr(trace_module.log, "info", lambda *args, **kwargs: calls.append((args, kwargs)))

    trace_module.trace_gate(
        "signal-legacy",
        "SMCI",
        "IV_GATE",
        "REJECT",
        reason="iv_extreme",
        score=70.0,
        iv_rank=185.0,
        spread_pct=0.123,
        trigger_price=1.49,
        contracts=1,
        pnl_pct=5.0,
    )

    assert len(calls) == 1
    message, values = calls[0]
    rendered = message[0] % message[1:]
    assert values == {}
    for field in (
        "signal=signal-legacy",
        "ticker=SMCI",
        "gate=IV_GATE",
        "status=REJECT",
        "reason=iv_extreme",
        "score=70.0",
        "iv=185.0",
        "spread=0.123",
        "trigger=1.49",
        "contracts=1",
        "pnl=5.00%",
    ):
        assert field in rendered
    assert "context=" not in rendered


def test_filled_entry_caller_continues_after_real_trace_call() -> None:
    """The next handoff step is reachable; trace_gate is not mocked away."""
    sentinel = []

    def production_shaped_filled_entry_handoff() -> None:
        _exact_filled_entry_trace()
        sentinel.append("canonical_owner_handoff")

    production_shaped_filled_entry_handoff()

    assert sentinel == ["canonical_owner_handoff"]


def test_real_fill_monitor_reaches_step_after_trace(monkeypatch) -> None:
    """The real FILLED-entry path reaches its first downstream handoff step."""
    os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")
    from ap import fill_monitor as fm

    sentinel = []

    class Broker:
        def get_order(self, broker_order_id):
            return {
                "status": "filled",
                "exec_quantity": 1,
                "avg_fill_price": 1.49,
            }

    class OSM:
        def transition(self, *_args, **_kwargs):
            return True

    order = {
        "client_id": "client@example.com",
        "local_order_id": "ord-1",
        "broker_order_id": "brk-1",
        "kind": "ENTRY",
        "symbol": "SMCI",
        "contract": "SMCI260821P00038000",
        "direction": "PUT",
        "qty": 1,
        "limit_price": 1.49,
        "reserved_cost": 149.0,
        "execution_mode": "live",
        "plan_id": "plan-1",
        "signal_id": "signal-1",
        "tier": "B",
        "score": 75,
        "status": "ACKNOWLEDGED",
        "filled_qty": 0,
    }

    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_reset_broker_anomaly_count", lambda *args, **kwargs: None)
    monkeypatch.setattr(
        fm,
        "_cancel_pair_opposite",
        lambda *args, **kwargs: sentinel.append("after_trace"),
    )
    monkeypatch.setattr(
        fm,
        "_open_position_safe",
        lambda *args, **kwargs: sentinel.append("position") or None,
    )
    monkeypatch.setattr(
        fm,
        "_place_standing_stop_best_effort",
        lambda *args, **kwargs: sentinel.append("stop"),
    )
    monkeypatch.setattr(
        fm,
        "_seed_exit_engine",
        lambda *args, **kwargs: sentinel.append("seed"),
    )
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: None)

    fm.process_pending_order(
        Broker(),
        order,
        osm=OSM(),
        pm=object(),
        exit_engine=None,
    )

    assert sentinel[0] == "after_trace"


def test_real_fill_monitor_reaches_canonical_handoff_helpers_after_trace(monkeypatch) -> None:
    """The real tracer and downstream handoff helpers execute in order."""
    os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")
    from ap import fill_monitor as fm
    from ap import trace as production_trace
    import ap.db as ap_db
    import ap.signal_pair_manager as pair_manager_module

    assert fm.trace_gate is production_trace.trace_gate
    assert getattr(fm.trace_gate, "__module__", None) == "ap.trace"
    assert Path(fm.trace_gate.__code__.co_filename).resolve() == TRACE_PATH.resolve()

    events = []

    class Broker:
        def get_order(self, broker_order_id):
            return {
                "status": "filled",
                "exec_quantity": 1,
                "avg_fill_price": 1.49,
            }

        def place_stop_order(self, **kwargs):
            events.append(("stop", kwargs))
            return {"id": "stop-1", "status": "accepted"}

    class OSM:
        def transition(self, *_args, **_kwargs):
            return True

    class PositionManager:
        def get_position_by_local_order(self, _local_order_id):
            return None

        def get_position_by_broker_order(self, _broker_order_id):
            return None

        def open_position(self, **kwargs):
            events.append(("position", kwargs))
            return "position-1"

    class ExitEngine:
        def seed_position(self, position_id, order, result):
            events.append(("seed", position_id, order, result))

    class PairManager:
        def on_fill(self, **kwargs):
            events.append(("pair", kwargs))
            return None

    class Connection:
        def __init__(self):
            self._row = None
            self.order_row = {
                "client_id": "client@example.com",
                "local_order_id": "ord-2",
                "broker_order_id": "brk-2",
                "position_id": None,
                "kind": "ENTRY",
                "status": "FILLED",
                "execution_mode": "live",
                "contract": "SMCI260821P00038000",
                "signal_id": "signal-2",
                "plan_id": "plan-2",
                "filled_qty": 1,
            }
            self.position_row = {
                "id": "position-1",
                "client_id": "client@example.com",
                "execution_mode": "live",
                "contract": "SMCI260821P00038000",
                "status": "OPEN",
                "signal_id": "signal-2",
                "plan_id": "plan-2",
                "local_order_id": None,
                "broker_order_id": None,
            }

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, sql, params):
            normalized = " ".join(str(sql).split())
            params = tuple(params)

            if normalized.startswith("SELECT * FROM orders"):
                client_id, local_order_id = params
                self._row = (
                    dict(self.order_row)
                    if client_id == self.order_row["client_id"]
                    and local_order_id == self.order_row["local_order_id"]
                    else None
                )
                return

            if normalized.startswith("SELECT * FROM positions"):
                position_id, client_id = params
                self._row = (
                    dict(self.position_row)
                    if position_id == self.position_row["id"]
                    and client_id == self.position_row["client_id"]
                    else None
                )
                return

            if normalized.startswith("UPDATE positions SET"):
                local_order_id, broker_order_id, position_id, client_id = params
                assert position_id == self.position_row["id"]
                assert client_id == self.position_row["client_id"]
                self.position_row["local_order_id"] = local_order_id
                self.position_row["broker_order_id"] = broker_order_id
                return

            if normalized.startswith("UPDATE orders SET position_id=%s,"):
                position_id, client_id, local_order_id = params
                assert client_id == self.order_row["client_id"]
                assert local_order_id == self.order_row["local_order_id"]
                self.order_row["position_id"] = position_id
                events.append(("link", params))
                return

            if normalized == "UPDATE orders SET position_id=%s WHERE local_order_id=%s":
                position_id, local_order_id = params
                assert local_order_id == self.order_row["local_order_id"]
                self.order_row["position_id"] = position_id
                events.append(("link", params))
                return

            raise AssertionError(f"unexpected SQL in focused test: {normalized}")

        def fetchone(self):
            return dict(self._row) if self._row else None

    order = {
        "client_id": "client@example.com",
        "local_order_id": "ord-2",
        "broker_order_id": "brk-2",
        "kind": "ENTRY",
        "symbol": "SMCI",
        "contract": "SMCI260821P00038000",
        "direction": "PUT",
        "qty": 1,
        "limit_price": 1.49,
        "underlying_entry": 450.0,
        "reserved_cost": 149.0,
        "execution_mode": "live",
        "plan_id": "plan-2",
        "signal_id": "signal-2",
        "tier": "B",
        "score": 75,
        "status": "ACKNOWLEDGED",
        "filled_qty": 0,
    }

    monkeypatch.setattr(production_trace.log, "info", lambda *args, **kwargs: events.append(("trace", args)))
    monkeypatch.setattr(fm, "audit", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "emit_fill_event", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_reset_broker_anomaly_count", lambda *args, **kwargs: None)
    monkeypatch.setattr(fm, "_release_entry_guards", lambda *args, **kwargs: None)
    monkeypatch.setattr(pair_manager_module, "get_pair_manager", lambda: PairManager())
    db = Connection()
    monkeypatch.setattr(fm, "conn", lambda: db)
    monkeypatch.setattr(fm, "run_with_retry", lambda fn, *args, **kwargs: fn())
    monkeypatch.setattr(ap_db, "conn", lambda: db)
    monkeypatch.setattr(ap_db, "run_with_retry", lambda fn, *args, **kwargs: fn())

    fm.process_pending_order(
        Broker(),
        order,
        osm=OSM(),
        pm=PositionManager(),
        exit_engine=ExitEngine(),
    )

    # AMENDMENT (PR #544): standing broker stop is no longer submitted from
    # fill reconciliation — the canonical exit engine is the sole exit
    # authority. The prior sequence ended with ``"stop"`` at index 3; that
    # step is gone. The remaining ordered prefix is trace → pair → position,
    # followed by an unordered ``link`` and ``seed`` pair.
    assert [event[0] for event in events[:3]] == [
        "trace",
        "pair",
        "position",
    ]
    assert sorted(event[0] for event in events[3:]) == ["link", "seed"]


def test_current_fill_monitor_retains_the_production_trace_shape() -> None:
    """The real caller remains the source of the newer diagnostic kwargs."""
    source = (REPO_ROOT / "ap" / "fill_monitor.py").read_text()
    call_start = source.index("trace_gate(", source.index("if ok and kind == \"ENTRY\""))
    call = source[call_start : call_start + 700]
    for fragment in (
        'side=(order.get("direction") or "CALL").upper()',
        'kind="ENTRY"',
        'tier=str(order.get("tier") or "B")',
        'plan_id=str(plan_id or "")',
    ):
        assert fragment in call


def test_trace_module_adds_no_money_state_authority() -> None:
    """Static authority fence: this module only formats and logs diagnostics."""
    source = TRACE_PATH.read_text().lower()
    tree = ast.parse(source)
    authority_markers = (
        "broker",
        "submit_order",
        "cancel_order",
        "insert into",
        "update ",
        "delete from",
        "orders",
        "positions",
        "proof_trades",
        "enqueue",
        "dequeue",
        "queue.",
    )
    assert not any(marker in source for marker in authority_markers)
    functions = {
        node.name: node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
    }
    assert set(functions) == {
        "_context_text",
        "trace_gate",
    }
    for node in ast.walk(functions["trace_gate"]):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
            assert node.func.attr in {"debug", "info"}
