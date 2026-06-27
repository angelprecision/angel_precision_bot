from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from ap.underlying_confirmation_entry_guard import (
    NO_UNDERLYING_DATA,
    STALE_UNDERLYING_QUOTE,
    ZERO_UNDERLYING,
    check_underlying_confirmation,
)
from ap.order_state_machine import APOrderStateMachine, OrderStatus


NOW = datetime(2026, 6, 26, 14, 0, tzinfo=timezone.utc)


def _plan(**overrides):
    base = dict(
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="sig-1",
        ticker="AAPL",
        symbol="AAPL",
        side="CALL",
        direction="CALL",
        timeframe="1d",
        pattern="TEST_PATTERN",
        score=82,
        trigger_price=100.0,
        stop_underlying=95.0,
        target_underlying=110.0,
        contracts=1,
        limit_price=1.25,
        max_position_usd=125.0,
        metadata={},
    )
    base.update(overrides)
    return SimpleNamespace(**base)


class QuoteBroker:
    def __init__(self, quote):
        self.quote = quote
        self.broker_submit_called = False

    def get_underlying_quote(self, ticker):
        return self.quote


class GuardProbeOSM(APOrderStateMachine):
    def __init__(self, order=None):
        self.client_id = "jasoncosby1@gmail.com"
        self.create_called = False
        self.broker_called = False
        self.transition_called = False
        self.events = []
        self.order = order

    def create_entry_order(self, *args, **kwargs):  # pragma: no cover - must not run in block test
        self.create_called = True
        raise AssertionError("create_entry_order should not run when quote is missing")

    def _submit_order_with_retry(self, *args, **kwargs):  # pragma: no cover - must not run in block test
        self.broker_called = True
        raise AssertionError("broker submit should not run when quote is missing")

    def _get_order(self, local_order_id):
        return self.order

    def transition(self, *args, **kwargs):  # pragma: no cover - must not run in no-mutation test
        self.transition_called = True
        raise AssertionError("underlying guard must not mutate existing ENTRY order state")

    def _emit_transition_event(self, **kwargs):
        self.events.append(kwargs)


def _existing_order(**overrides):
    base = dict(
        local_order_id="local-1",
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        signal_id="sig-1",
        kind="ENTRY",
        status=OrderStatus.PENDING_TRIGGER,
        symbol="AAPL",
        direction="CALL",
        timeframe="1d",
        pattern="TEST_PATTERN",
        score=82,
        trigger_price=100.0,
        stop_underlying=95.0,
        target_underlying=110.0,
        qty=1,
        limit_price=1.25,
        reserved_cost=125.0,
        meta={
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "live",
            "ticker": "AAPL",
            "side": "CALL",
            "timeframe": "1d",
            "pattern": "TEST_PATTERN",
        },
    )
    base.update(overrides)
    return base


def test_missing_underlying_quote_blocks():
    result = check_underlying_confirmation(
        plan=_plan(),
        broker=QuoteBroker(None),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        now=NOW,
    )

    assert not result.passed
    assert result.reason == NO_UNDERLYING_DATA
    assert result.metadata["stage"] == "underlying_confirmation_missing"
    assert result.metadata["client_id"] == "jasoncosby1@gmail.com"
    assert result.metadata["execution_mode"] == "live"


def test_stale_underlying_quote_blocks():
    stale = NOW - timedelta(seconds=120)
    result = check_underlying_confirmation(
        plan=_plan(),
        broker=QuoteBroker({"last": 101.0, "timestamp": stale.isoformat()}),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        now=NOW,
        max_age_sec=30,
    )

    assert not result.passed
    assert result.reason == STALE_UNDERLYING_QUOTE
    assert result.metadata["quote_age_seconds"] == 120.0


def test_zero_underlying_blocks():
    result = check_underlying_confirmation(
        plan=_plan(),
        broker=QuoteBroker({"last": 0.0, "timestamp": NOW.isoformat()}),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        now=NOW,
    )

    assert not result.passed
    assert result.reason == ZERO_UNDERLYING
    assert result.metadata["current_underlying"] == 0.0


def test_valid_fresh_underlying_quote_passes():
    result = check_underlying_confirmation(
        plan=_plan(),
        broker=QuoteBroker({"last": 101.0, "timestamp": NOW.isoformat()}),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        now=NOW,
    )

    assert result.passed
    assert result.metadata["current_underlying"] == 101.0
    assert result.metadata["directional_confirmation_evaluable"] is True
    assert result.metadata["directional_confirmed"] is True


def test_submit_entry_blocks_before_create_or_broker_submit():
    osm = GuardProbeOSM()
    result = osm.submit_entry(
        broker=QuoteBroker(None),
        plan=_plan(),
        limit_price=1.25,
        reserved_cost=125.0,
    )

    assert result["ok"] is False
    assert result["error"] == NO_UNDERLYING_DATA
    assert result["local_order_id"] is None
    assert result["broker_order_id"] is None
    assert result["order_mutated"] is False
    assert osm.create_called is False
    assert osm.broker_called is False


def test_existing_entry_block_does_not_mutate_order_or_submit_broker():
    osm = GuardProbeOSM(order=_existing_order())
    result = osm.submit_existing_entry(
        local_order_id="local-1",
        broker=QuoteBroker(None),
        plan=_plan(),
        limit_price=1.25,
    )

    assert result["ok"] is False
    assert result["error"] == NO_UNDERLYING_DATA
    assert result["status"] == OrderStatus.PENDING_TRIGGER
    assert result["order_mutated"] is False
    assert result["decision_event_only"] is True
    assert osm.transition_called is False
    assert osm.broker_called is False
    assert len(osm.events) == 1
    assert osm.events[0]["old_status"] == OrderStatus.PENDING_TRIGGER
    assert osm.events[0]["new_status"] == OrderStatus.PENDING_TRIGGER
    assert osm.events[0]["decision"] == "REJECT"


def test_live_and_paper_execution_mode_taxonomy_is_not_cross_polluted():
    live = check_underlying_confirmation(
        plan=_plan(execution_mode="live"),
        broker=QuoteBroker(None),
        client_id="jasoncosby1@gmail.com",
        execution_mode="live",
        now=NOW,
    )
    paper = check_underlying_confirmation(
        plan=_plan(client_id="paper@example.com", execution_mode="paper"),
        broker=QuoteBroker(None),
        client_id="paper@example.com",
        execution_mode="paper",
        now=NOW,
    )

    assert live.reason == NO_UNDERLYING_DATA
    assert paper.reason == NO_UNDERLYING_DATA
    assert live.metadata["execution_mode"] == "live"
    assert paper.metadata["execution_mode"] == "paper"
    assert live.metadata["client_id"] == "jasoncosby1@gmail.com"
    assert paper.metadata["client_id"] == "paper@example.com"
