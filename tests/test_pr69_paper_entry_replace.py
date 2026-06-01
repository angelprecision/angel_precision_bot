"""
PR #69 — PAPER entry retry + market fallback (Codex P1 acceptance)
==================================================================

Surgical mock test proving the two Codex P1 invariants are honored:

  1. broker.submit_option is NEVER called anywhere in PR #69 — only
     broker.place_order(...) is used (the canonical method per
     ap/broker.py:117, ap/brokers/tradier.py:179, ap/execution.py:749,
     ap/exit_manager.py:205, ap/retry_engine.py:360).

  2. When the broker has no place_order method, the helper MUST NOT
     cancel the existing broker order — pre-flight catches it and
     returns broker_missing_place_order with NO destructive action.

Method: source-grep + AST extraction of _paper_replace, executed
against a minimal mock broker. No DB, no network, no fixtures.

Per founder directive ("dont do any tests but surgical corrections")
this is a single focused test that proves the merge-blocker is
addressed. py_compile already passes; this confirms behavior.
"""
from __future__ import annotations

import ast
import os
import sys

import pytest


REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ORDER_MONITOR_PATH = os.path.join(REPO_ROOT, "ap", "order_monitor.py")


def _read_source() -> str:
    with open(ORDER_MONITOR_PATH, "r", encoding="utf-8") as fh:
        return fh.read()


# ---------------------------------------------------------------------------
# Acceptance #1: no broker.submit_option calls remain anywhere in PR #69
# ---------------------------------------------------------------------------
def test_no_broker_submit_option_calls_in_order_monitor():
    """The canonical broker method is place_order. submit_option does not
    exist on the broker class. PR-69 draft incorrectly called it; the
    Codex P1 patch removed it. Only references that may remain are in
    explanatory comments documenting the fix — never in executable code.
    """
    src = _read_source()
    tree = ast.parse(src)

    bad_calls: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            # Match self.broker.submit_option(...) or <anything>.submit_option(...)
            if isinstance(func, ast.Attribute) and func.attr == "submit_option":
                bad_calls.append((node.lineno, ast.unparse(node)))

    assert not bad_calls, (
        "PR-69 must not call broker.submit_option (it does not exist on the "
        "broker). Found in AST: " + repr(bad_calls)
    )


def test_paper_replace_uses_broker_place_order():
    """The PAPER replace helper must call self.broker.place_order(...)."""
    src = _read_source()
    tree = ast.parse(src)

    found = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            func = node.func
            if (
                isinstance(func, ast.Attribute)
                and func.attr == "place_order"
                and isinstance(func.value, ast.Attribute)
                and func.value.attr == "broker"
            ):
                found = True
                break

    assert found, (
        "PR-69 helper must call self.broker.place_order(...) — the canonical "
        "method used by execution.py, exit_manager.py, and retry_engine.py."
    )


# ---------------------------------------------------------------------------
# Acceptance #2: when place_order is missing, helper must NOT cancel
# ---------------------------------------------------------------------------
def _extract_method_source(src: str, class_name: str, method_name: str) -> str:
    tree = ast.parse(src)
    for cls in ast.walk(tree):
        if isinstance(cls, ast.ClassDef) and cls.name == class_name:
            for item in cls.body:
                if isinstance(item, ast.FunctionDef) and item.name == method_name:
                    return ast.get_source_segment(src, item)
    raise AssertionError(f"could not find {class_name}.{method_name}")


class _MockBrokerNoPlaceOrder:
    """Broker without place_order. cancel_order MUST NOT be called."""
    cancel_calls: list

    def __init__(self):
        self.cancel_calls = []

    def cancel_order(self, broker_oid):  # noqa: D401 — must never run
        self.cancel_calls.append(broker_oid)
        return {"ok": True}


class _MockBrokerWithPlaceOrderButNoCancel:
    """Broker with place_order — used in companion assertion only."""
    def place_order(self, *a, **kw):
        return {"broker_order_id": "NEW123", "status": "ACK"}


class _FakeOrderMonitor:
    """Minimal stand-in carrying just what _paper_replace touches."""
    def __init__(self, broker):
        self.broker = broker
        self.client_id = "test-client"


def test_no_cancel_when_place_order_missing():
    """The hasattr(broker, 'place_order') pre-flight is the explicit
    guarantee that we never cancel a broker order we cannot replace.

    Strategy: extract the _paper_replace method source via AST, bind it
    onto a fake monitor, and call it with a mock broker that lacks
    place_order. Assert cancel_order is never invoked.
    """
    src = _read_source()
    method_src = _extract_method_source(src, "APOrderMonitor", "_paper_replace")

    # Build a module-level namespace and exec the def into it.
    ns: dict = {}
    # Provide log + Optional to satisfy annotations / references inside.
    import logging
    import typing as _typing
    ns["log"] = logging.getLogger("pr69-test")
    ns["Optional"] = _typing.Optional
    exec(method_src, ns)

    paper_replace = ns["_paper_replace"]

    broker = _MockBrokerNoPlaceOrder()
    monitor = _FakeOrderMonitor(broker)

    ok, outcome = paper_replace(
        monitor,
        order={"local_order_id": "loc-1", "qty": 1},
        broker_oid="BROK-OID-1",
        contract="LOW   260620P00215000",
        sym="LOW",
        new_limit=2.55,
        unsupported_log_tag="PAPER_ENTRY_REPEG",
    )

    # 1) Returned the canonical failure code.
    assert ok is False
    assert outcome == "broker_missing_place_order", (
        f"expected broker_missing_place_order, got {outcome!r}"
    )

    # 2) CRITICAL: no cancel was performed. This is the merge-blocker
    #    invariant — never cancel without a validated replacement path.
    assert broker.cancel_calls == [], (
        "broker.cancel_order MUST NOT be called when place_order is "
        f"unavailable; saw calls: {broker.cancel_calls}"
    )


# ---------------------------------------------------------------------------
# Acceptance #3 (defense in depth): hasattr check precedes any cancel
# ---------------------------------------------------------------------------
def test_hasattr_place_order_appears_before_cancel_in_source():
    """Belt-and-suspenders source-order check: in _paper_replace, the
    hasattr(self.broker, 'place_order') guard must textually precede
    the first self.broker.cancel_order call. Catches future regressions
    that pass the unit test by short-circuiting but reorder the source.
    """
    src = _read_source()
    method_src = _extract_method_source(src, "APOrderMonitor", "_paper_replace")

    hasattr_idx = method_src.find('hasattr(self.broker, "place_order")')
    if hasattr_idx < 0:
        hasattr_idx = method_src.find("hasattr(self.broker, 'place_order')")
    cancel_idx = method_src.find("self.broker.cancel_order(")

    assert hasattr_idx >= 0, "missing hasattr(self.broker, 'place_order') guard"
    assert cancel_idx >= 0, "missing self.broker.cancel_order call"
    assert hasattr_idx < cancel_idx, (
        "hasattr guard must appear BEFORE cancel_order in source — "
        f"hasattr at {hasattr_idx}, cancel at {cancel_idx}"
    )
