"""Regression guards for the admin force-exit proof boundary.

An accepted EXIT submission is not a fill.  The endpoint may request the
broker order, but proof_trades must be finalized only by the normal
broker-confirmed fill callback.
"""

from __future__ import annotations

import ast
from pathlib import Path


_APP_PATH = Path(__file__).resolve().parents[1] / "app.py"


def _force_exit_function() -> ast.FunctionDef:
    tree = ast.parse(_APP_PATH.read_text())
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == "admin_force_exit_position":
            return node
    raise AssertionError("admin_force_exit_position was not found")


def _called_attribute_names(node: ast.AST) -> set[str]:
    return {
        call.func.attr
        for call in ast.walk(node)
        if isinstance(call, ast.Call)
        and isinstance(call.func, ast.Attribute)
    }


def test_admin_force_exit_defers_proof_until_broker_fill() -> None:
    function = _force_exit_function()
    called_attributes = _called_attribute_names(function)

    assert "_submit_exit_decision" in called_attributes
    assert "log_trade" not in called_attributes
    assert "on_exit_fill_confirmed" in ast.get_source_segment(
        _APP_PATH.read_text(), function
    )


def test_admin_force_exit_keeps_failed_submission_fail_closed() -> None:
    source = _APP_PATH.read_text()
    start = source.index("def admin_force_exit_position")
    end = source.index("\n# =============================================================================", start)
    body = source[start:end]

    assert "if not submitted:" in body
    assert "FORCE_EXIT_PROOF_SKIPPED" in body
    assert "confirmed broker EXIT fill" in body
