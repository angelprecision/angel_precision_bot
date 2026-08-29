"""Regression tests for canonical-exit-only behavior after entry fills."""

import ast
import inspect

from ap import fill_monitor


def test_filled_entry_path_does_not_call_standing_stop_helper():
    source = inspect.getsource(fill_monitor)
    tree = ast.parse(source)
    calls = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call):
            fn = node.func
            name = fn.attr if isinstance(fn, ast.Attribute) else fn.id if isinstance(fn, ast.Name) else ""
            if name == "_place_standing_stop_best_effort":
                calls.append(node)
    assert calls == [], "fill reconciliation must not submit a standing broker stop"


def test_legacy_standing_stop_helper_is_a_noop():
    class BrokerThatMustNotBeCalled:
        def place_stop_order(self, **kwargs):
            raise AssertionError("standing protective broker stop must not be submitted")

    result = fill_monitor._place_standing_stop_best_effort(
        broker=BrokerThatMustNotBeCalled(),
        order={"symbol": "NOW", "local_order_id": "entry-1"},
        qty=1,
        entry_price=1.50,
    )

    assert result is None
