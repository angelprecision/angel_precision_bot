"""Regression coverage for the deprecated order-insert compatibility path."""

from __future__ import annotations

import ast
import inspect
import os
import re
from contextlib import contextmanager
from textwrap import dedent

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_legacy_order_execution_mode",
)


class _RecordingCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))


def _inserted_row(sql, params):
    columns_match = re.search(
        r"INSERT INTO orders \((.*?)\)\s+VALUES",
        sql,
        flags=re.DOTALL,
    )
    assert columns_match, "insert_order must issue an orders INSERT"
    columns = [column.strip() for column in columns_match.group(1).split(",")]
    return dict(zip(columns, params))


@pytest.mark.parametrize(
    ("execution_mode", "expected"),
    (("  PaPeR  ", "paper"), ("LIVE", "live"), ("sim", None), (None, None)),
)
def test_insert_order_stamps_only_canonical_execution_mode(
    monkeypatch, execution_mode, expected
):
    import ap.db as db

    cursor = _RecordingCursor()

    @contextmanager
    def fake_conn():
        yield cursor

    monkeypatch.setattr(db, "conn", fake_conn)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())

    with pytest.warns(DeprecationWarning):
        db.insert_order(
            client_id="client-A",
            local_order_id="local-1",
            position_id=None,
            kind="ENTRY",
            status="NEW",
            symbol="AAPL",
            contract="AAPL260101C00100000",
            qty=1,
            limit_price=1.25,
            execution_mode=execution_mode,
            meta={"execution_mode": "paper"},
        )

    assert len(cursor.calls) == 1
    row = _inserted_row(*cursor.calls[0])
    assert row["execution_mode"] == expected


def _insert_order_calls(function):
    tree = ast.parse(dedent(inspect.getsource(function)))
    return [
        node
        for node in ast.walk(tree)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "insert_order"
    ]


@pytest.mark.parametrize(
    "qualified_function",
    (
        "ap.execution.process_signal",
        "ap.exit_manager.exit_manager_loop",
    ),
)
def test_legacy_order_callers_pass_runtime_execution_mode(qualified_function):
    module_name, function_name = qualified_function.rsplit(".", 1)
    module = __import__(module_name, fromlist=[function_name])
    calls = _insert_order_calls(getattr(module, function_name))

    assert calls, f"{qualified_function} must retain an insert_order call"
    assert any(
        keyword.arg == "execution_mode"
        and isinstance(keyword.value, ast.Name)
        and keyword.value.id == "mode"
        for call in calls
        for keyword in call.keywords
    ), f"{qualified_function} must pass its resolved runtime mode"
