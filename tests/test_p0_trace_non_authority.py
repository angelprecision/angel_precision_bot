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
