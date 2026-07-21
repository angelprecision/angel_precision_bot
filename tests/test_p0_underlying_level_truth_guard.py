"""Regression tests for underlying target/stop truth gating."""
from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

# Import installs the production class-level target/stop truth guard.
from ap.position_quote_monitor import APPositionQuoteMonitor  # noqa: F401,E402
from ap_exit_engine import ManagedPosition, evaluate_exit  # noqa: E402

ET = ZoneInfo("America/New_York")


def make_position(*, ticker: str, side: str, stop: float = 0.0,
                  target: float = 0.0) -> ManagedPosition:
    right = "C" if side == "CALL" else "P"
    pos = ManagedPosition(
        ticker=ticker,
        option_symbol=f"{ticker}260731{right}00100000",
        side=side,
        quantity=1,
        entry_price=5.0,
        underlying_entry=100.0,
        underlying_target=target,
        underlying_stop=stop,
        position_id=f"pos-{ticker.lower()}",
        client_id="truth-guard@example.com",
        execution_mode="paper",
        quantity_remaining=1,
        opened_at=datetime.now(timezone.utc) - timedelta(minutes=30),
    )
    pos.executable_bid_soft_exit_truth_enabled = True
    pos.current_option_price = 4.90
    pos.current_bid = 4.90
    pos.current_ask = 5.10
    pos.current_underlying = 0.0
    pos.exit_decision_quote_snapshot = {
        "position_id": pos.position_id,
        "client_id": pos.client_id,
        "option_symbol": pos.option_symbol,
        "ticker": pos.ticker,
        "option_bid": 4.90,
        "option_ask": 5.10,
        "option_midpoint": 5.00,
        "option_quote_timestamp": datetime.now(timezone.utc).isoformat(),
        "underlying_bid": 0.0,
        "underlying_ask": 0.0,
        "underlying_midpoint": 0.0,
        "underlying_quote_timestamp": datetime.now(timezone.utc).isoformat(),
        "option_quote_provider": "tradier",
        "underlying_quote_provider": "tradier",
        "option_quote_domain": "tradier_live_market_data",
        "underlying_quote_domain": "tradier_live_market_data",
        "execution_mode": "paper",
        "snapshot_timestamp": datetime.now(timezone.utc).isoformat(),
        "cycle_id": "missing-underlying",
    }
    return pos


def test_missing_call_underlying_cannot_confirm_underlying_stop():
    pos = make_position(ticker="CALLSTOP", side="CALL", stop=99.0)
    pos._underlying_stop_breach_ts = datetime.now(timezone.utc) - timedelta(seconds=60)
    assert pos.is_at_stop is False
    decision = evaluate_exit(pos, datetime.now(ET))
    assert decision.reason_code != "UNDERLYING_STOP_CONFIRMED"
    assert decision.action == "HOLD"
    assert pos._underlying_stop_breach_ts is None


def test_missing_put_underlying_cannot_claim_target_hit():
    pos = make_position(ticker="PUTTARGET", side="PUT", target=95.0)
    assert pos.is_at_target is False
    decision = evaluate_exit(pos, datetime.now(ET))
    assert decision.reason_code != "TARGET_HIT"
    assert decision.action == "HOLD"


def test_fresh_valid_underlying_still_allows_confirmed_stop():
    pos = make_position(ticker="VALIDSTOP", side="CALL", stop=99.0)
    now = datetime.now(timezone.utc)
    pos.current_underlying = 98.0
    pos.exit_decision_quote_snapshot.update({
        "underlying_bid": 97.98,
        "underlying_ask": 98.02,
        "underlying_midpoint": 98.0,
        "option_quote_timestamp": now.isoformat(),
        "underlying_quote_timestamp": now.isoformat(),
        "snapshot_timestamp": now.isoformat(),
        "cycle_id": "valid-underlying-stop",
    })
    pos._underlying_stop_breach_ts = now - timedelta(seconds=31)
    assert pos.is_at_stop is True
    decision = evaluate_exit(pos, now.astimezone(ET))
    assert decision.should_act is True
    assert decision.reason_code == "UNDERLYING_STOP_CONFIRMED"
