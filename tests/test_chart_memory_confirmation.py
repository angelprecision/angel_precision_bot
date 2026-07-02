import os

import pytest

from ap_chart_memory_confirmation import (
    SAFE_PASS_STATES,
    build_chart_memory_payload,
    normalize_chart_state,
    should_allow_entry_from_chart_memory,
)


class FakeResult:
    def __init__(self, data):
        self.data = data


class FakeQuery:
    def __init__(self, rows):
        self.rows = rows

    def select(self, *_args, **_kwargs):
        return self

    def eq(self, *_args, **_kwargs):
        return self

    def order(self, *_args, **_kwargs):
        return self

    def limit(self, *_args, **_kwargs):
        return self

    def execute(self):
        return FakeResult(self.rows)


class FakeSupabase:
    def __init__(self, rows):
        self.rows = rows

    def table(self, name):
        assert name == "chart_memory_confirmations"
        return FakeQuery(self.rows)


@pytest.fixture(autouse=True)
def clear_chart_flags(monkeypatch):
    monkeypatch.delenv("CHART_VISION_CONFIRMATION_ENABLED", raising=False)
    monkeypatch.delenv("CHART_VISION_BLOCK_SUBMIT", raising=False)


def test_normalize_chart_state_pass_states():
    assert normalize_chart_state("continuation_confirmed") == "continuation_confirmed"
    assert normalize_chart_state("clean_reclaim") == "clean_reclaim"
    assert normalize_chart_state("fresh_intraday_high") == "fresh_intraday_high"


def test_normalize_chart_state_unknown_becomes_unclear():
    assert normalize_chart_state("random_ai_text") == "unclear"
    assert normalize_chart_state(None) == "unclear"


def test_build_payload_preserves_client_and_mode():
    payload = build_chart_memory_payload(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="sig-123",
        ticker="aapl",
        side="call",
        timeframe="1d",
        trigger_level=201.5,
        current_price=202.1,
        chart_state="continuation_confirmed",
        reason="Price reclaimed trigger and held above level",
    )

    assert payload["client_id"] == "jasoncosby1@gmail.com"
    assert payload["execution_mode"] == "live"
    assert payload["signal_id"] == "sig-123"
    assert payload["ticker"] == "AAPL"
    assert payload["side"] == "CALL"
    assert payload["chart_state"] in SAFE_PASS_STATES


def test_disabled_flag_allows_without_db_read():
    allowed, reason, row = should_allow_entry_from_chart_memory(
        FakeSupabase(rows=[]),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="sig-123",
    )

    assert allowed is True
    assert reason == "chart_memory_disabled"
    assert row is None


def test_observe_only_missing_memory_allows(monkeypatch):
    monkeypatch.setenv("CHART_VISION_CONFIRMATION_ENABLED", "true")

    allowed, reason, row = should_allow_entry_from_chart_memory(
        FakeSupabase(rows=[]),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="sig-123",
    )

    assert allowed is True
    assert reason == "chart_memory_missing_observe_only"
    assert row is None


def test_block_mode_missing_memory_blocks(monkeypatch):
    monkeypatch.setenv("CHART_VISION_CONFIRMATION_ENABLED", "true")
    monkeypatch.setenv("CHART_VISION_BLOCK_SUBMIT", "true")

    allowed, reason, row = should_allow_entry_from_chart_memory(
        FakeSupabase(rows=[]),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="sig-123",
    )

    assert allowed is False
    assert reason == "chart_memory_missing_blocked"
    assert row is None


def test_block_mode_allows_safe_state(monkeypatch):
    monkeypatch.setenv("CHART_VISION_CONFIRMATION_ENABLED", "true")
    monkeypatch.setenv("CHART_VISION_BLOCK_SUBMIT", "true")

    memory = {"chart_state": "clean_reclaim", "signal_id": "sig-123"}
    allowed, reason, row = should_allow_entry_from_chart_memory(
        FakeSupabase(rows=[memory]),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="sig-123",
    )

    assert allowed is True
    assert reason == "chart_memory_pass:clean_reclaim"
    assert row == memory


def test_block_mode_rejects_bad_state(monkeypatch):
    monkeypatch.setenv("CHART_VISION_CONFIRMATION_ENABLED", "true")
    monkeypatch.setenv("CHART_VISION_BLOCK_SUBMIT", "true")

    memory = {"chart_state": "late_chase", "signal_id": "sig-123"}
    allowed, reason, row = should_allow_entry_from_chart_memory(
        FakeSupabase(rows=[memory]),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="sig-123",
    )

    assert allowed is False
    assert reason == "chart_memory_block:late_chase"
    assert row == memory
