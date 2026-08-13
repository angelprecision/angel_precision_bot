"""Regression coverage for the deprecated order-insert compatibility path."""

from __future__ import annotations

import os
import re
from contextlib import contextmanager

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_legacy_order_execution_mode",
)

PAPER_BROKER_URL = "https://sandbox.tradier.com"
LIVE_BROKER_URL = "https://api.tradier.com"


class _RecordingCursor:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params):
        self.calls.append((sql, params))


class _Broker:
    def __init__(self, base_url):
        self.base_url = base_url
        self.submissions = []

    def place_order(self, *args, **kwargs):
        self.submissions.append((args, kwargs))


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


@pytest.mark.parametrize(
    ("state", "broker_url", "payload", "expected_mode", "expected_reason"),
    (
        ({"mode": "PAPER"}, PAPER_BROKER_URL, {}, "paper", None),
        ({"mode": "LIVE"}, LIVE_BROKER_URL, {}, "live", None),
        ({}, PAPER_BROKER_URL, {}, None, "EXECUTION_MODE_UNPROVEN"),
        ({"mode": None}, PAPER_BROKER_URL, {}, None, "EXECUTION_MODE_UNPROVEN"),
        ({"mode": ""}, PAPER_BROKER_URL, {}, None, "EXECUTION_MODE_UNPROVEN"),
        ({"mode": "SIM"}, PAPER_BROKER_URL, {}, None, "EXECUTION_MODE_UNPROVEN"),
        ({"mode": "PAPER"}, LIVE_BROKER_URL, {}, None, "EXECUTION_MODE_CONFLICT"),
        ({"mode": "PAPER"}, PAPER_BROKER_URL,
         {"execution_mode": "LIVE"}, None, "EXECUTION_MODE_CONFLICT"),
        ({"mode": "PAPER"}, PAPER_BROKER_URL,
         {"execution_mode": "unknown"}, None, "EXECUTION_MODE_UNPROVEN"),
        ({"mode": "PAPER"}, "", {}, None, "EXECUTION_MODE_UNPROVEN"),
    ),
)
def test_legacy_entry_mode_requires_explicit_matching_authority(
    state, broker_url, payload, expected_mode, expected_reason
):
    from ap.execution import _resolve_legacy_entry_execution_mode

    mode, reason = _resolve_legacy_entry_execution_mode(
        _Broker(broker_url), state, payload
    )

    assert mode == expected_mode
    assert reason == expected_reason


@pytest.mark.parametrize(
    ("state", "broker_url", "expected_error"),
    (
        ({}, PAPER_BROKER_URL, "execution_mode_unproven"),
        ({"mode": None}, PAPER_BROKER_URL, "execution_mode_unproven"),
        ({"mode": ""}, PAPER_BROKER_URL, "execution_mode_unproven"),
        ({"mode": "SIM"}, PAPER_BROKER_URL, "execution_mode_unproven"),
        ({"mode": "PAPER"}, LIVE_BROKER_URL, "execution_mode_conflict"),
    ),
)
def test_process_signal_holds_before_legacy_entry_side_effects(
    monkeypatch, state, broker_url, expected_error
):
    import ap.execution as execution

    state_reads = []
    monkeypatch.setattr(
        execution,
        "get_client",
        lambda client_id: {"status": "ACTIVE"},
    )

    def fake_get_client_state(client_id):
        state_reads.append(client_id)
        return state

    monkeypatch.setattr(execution, "get_client_state", fake_get_client_state)
    monkeypatch.setattr(
        execution,
        "insert_order",
        lambda **kwargs: pytest.fail("unproven legacy ENTRY must not insert"),
    )
    monkeypatch.setattr(execution, "audit", lambda *args, **kwargs: None)

    broker = _Broker(broker_url)
    result = execution.process_signal(broker, "client-A", {})

    assert result["ok"] is False
    assert result["error"] == expected_error
    assert result["reason_code"] in {
        "EXECUTION_MODE_UNPROVEN",
        "EXECUTION_MODE_CONFLICT",
    }
    assert state_reads == ["client-A"]
    assert broker.submissions == []


def test_missing_client_state_has_no_fabricated_execution_mode(monkeypatch):
    import ap.db as db

    class _StateCursor:
        def __init__(self):
            self.params = None

        def execute(self, sql, params):
            self.params = params

        def fetchone(self):
            return None

    cursor = _StateCursor()

    @contextmanager
    def fake_conn():
        yield cursor

    monkeypatch.setattr(db, "conn", fake_conn)
    monkeypatch.setattr(db, "run_with_retry", lambda fn: fn())

    state = db.get_client_state("client-A")

    assert cursor.params == ("client-A",)
    assert state["mode"] is None


@pytest.mark.parametrize(
    ("position_mode", "state_mode", "expected_mode", "expected_reason"),
    (
        ("LIVE", "LIVE", "live", None),
        ("PAPER", "PAPER", "paper", None),
        ("LIVE", "PAPER", None, "EXECUTION_MODE_CONFLICT"),
        (None, "PAPER", None, "POSITION_EXECUTION_MODE_UNPROVEN"),
        ("", "LIVE", None, "POSITION_EXECUTION_MODE_UNPROVEN"),
        ("LIVE", None, None, "EXECUTION_MODE_UNPROVEN"),
    ),
)
def test_legacy_exit_identity_is_position_authoritative_and_fail_closed(
    position_mode, state_mode, expected_mode, expected_reason
):
    from ap.exit_manager import _resolve_legacy_exit_execution_mode

    mode, reason = _resolve_legacy_exit_execution_mode(
        {"execution_mode": position_mode},
        {"mode": state_mode},
    )

    assert mode == expected_mode
    assert reason == expected_reason
