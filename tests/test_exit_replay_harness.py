from datetime import datetime
from zoneinfo import ZoneInfo

from ap.exit_replay_harness import make_position, replay_exit_path


ET = ZoneInfo("America/New_York")


def test_single_contract_profit_floor_after_twenty_five_percent_peak():
    """Peak +25% then current below +10% floor should close all."""
    pos = make_position(entry_price=1.00, quantity=1, option_symbol="SPY260515C00500000")
    result = replay_exit_path(
        [1.00, 1.12, 1.25, 1.09],
        position=pos,
        now_et=datetime(2026, 5, 15, 10, 45, tzinfo=ET),
    )

    first_action = result.first_action
    assert first_action is not None
    assert first_action.action == "CLOSE_ALL"
    assert first_action.quantity == 1
    assert "PROFIT LOCK" in first_action.reason or "TRAIL" in first_action.reason


def test_single_contract_runner_trail_fires_after_peak_giveback():
    """Single contract should trail after +12%+ peak and 8pt giveback."""
    pos = make_position(entry_price=1.00, quantity=1, option_symbol="SPY260515C00500000")
    result = replay_exit_path(
        [1.00, 1.12, 1.20, 1.11],
        position=pos,
        now_et=datetime(2026, 5, 15, 10, 45, tzinfo=ET),
    )

    first_action = result.first_action
    assert first_action is not None
    assert first_action.action == "CLOSE_ALL"
    assert "TRAIL" in first_action.reason or "LOCK" in first_action.reason


def test_multi_contract_scale_one_at_fifteen_percent():
    """Multi-contract positions should scale at +15% instead of all-or-nothing."""
    pos = make_position(entry_price=1.00, quantity=3, option_symbol="SPY260515C00500000")
    result = replay_exit_path(
        [1.00, 1.15],
        position=pos,
        now_et=datetime(2026, 5, 15, 10, 45, tzinfo=ET),
    )

    first_action = result.first_action
    assert first_action is not None
    assert first_action.action == "SCALE_OUT"
    assert first_action.quantity >= 1


def test_never_green_stop_fires_for_fast_bad_trade():
    """A trade that never goes green and drops through the configured stop should close."""
    pos = make_position(entry_price=1.00, quantity=1, option_symbol="SPY260515C00500000")
    result = replay_exit_path(
        [1.00, 0.86, 0.82],
        position=pos,
        now_et=datetime(2026, 5, 15, 10, 45, tzinfo=ET),
    )

    first_action = result.first_action
    assert first_action is not None
    assert first_action.action in {"CLOSE_ALL", "STOP"}
    assert "STOP" in first_action.reason
