from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from ap_exit_engine import ManagedPosition, evaluate_exit


UTC = timezone.utc
ET = ZoneInfo("America/New_York")


def _fresh_ts() -> datetime:
    return datetime.now(UTC) - timedelta(seconds=2)


def _now_call(
    *,
    option_bid: float,
    underlying_price: float,
    underlying_available: bool = True,
    underlying_fresh: bool = True,
    opened_minutes_ago: float = 6.0,
) -> ManagedPosition:
    pos = ManagedPosition(
        ticker="NOW",
        option_symbol="NOW260731C00113000",
        side="CALL",
        quantity=1,
        entry_price=1.82,
        underlying_entry=108.15,
        underlying_target=111.36,
        underlying_stop=104.94,
        execution_mode="live",
        quantity_remaining=1,
        opened_at=datetime.now(UTC) - timedelta(minutes=opened_minutes_ago),
    )

    pos.current_bid = option_bid
    pos.currentbid = option_bid
    pos.current_ask = max(option_bid + 0.05, option_bid)
    pos.currentask = pos.current_ask
    pos.current_option_price = option_bid
    pos.currentoptionprice = option_bid
    pos.option_bid_valid = option_bid > 0
    pos.optionbidvalid = option_bid > 0
    pos.option_quote_fresh = True
    pos.optionquotefresh = True
    pos.last_option_bid_update_ts = _fresh_ts()
    pos.lastoptionbidupdatets = pos.last_option_bid_update_ts
    pos.last_option_quote_update_ts = _fresh_ts()
    pos.lastoptionquoteupdatets = pos.last_option_quote_update_ts

    pos.current_underlying = underlying_price
    pos.currentunderlying = underlying_price
    pos.underlying_available = underlying_available
    pos.underlyingavailable = underlying_available
    pos.underlying_fresh = underlying_fresh
    pos.underlyingfresh = underlying_fresh
    pos.last_underlying_quote_update_ts = _fresh_ts()
    pos.lastunderlyingquoteupdatets = pos.last_underlying_quote_update_ts
    return pos


def _market_time() -> datetime:
    return datetime.now(ET).replace(hour=10, minute=0, second=0, microsecond=0)


@pytest.mark.xfail(
    strict=True,
    reason="P0 implementation pending: option drawdown still has hard-stop authority before underlying-stop confirmation",
)
def test_now_call_option_drawdown_does_not_override_fresh_underlying_above_stop():
    """July 28 NOW geometry: option drawdown must not invalidate an intact CALL thesis."""
    pos = _now_call(
        option_bid=1.28,          # approximately -29.7% from 1.82
        underlying_price=108.60, # safely above stored CALL stop 104.94
    )

    decision = evaluate_exit(pos, _market_time())

    assert decision.action == "HOLD", decision.reason
    assert decision.reason_code not in {
        "HARD_STOP",
        "STOP_BREACH_STARTED",
        "STOP_BREACH_CONFIRMING",
        "UNDERLYING_TECHNICAL_STOP_CONFIRMED",
    }


@pytest.mark.xfail(
    strict=True,
    reason="P0 implementation pending: side-aware underlying-stop confirmation taxonomy does not yet exist",
)
def test_now_call_first_underlying_breach_starts_confirmation_without_exit():
    pos = _now_call(option_bid=1.45, underlying_price=104.80)

    decision = evaluate_exit(pos, _market_time())

    assert decision.action == "HOLD", decision.reason
    assert decision.reason_code == "UNDERLYING_STOP_CONFIRMING"


@pytest.mark.xfail(
    strict=True,
    reason="P0 implementation pending: missing underlying must not be mislabeled as a technical stop",
)
def test_missing_underlying_keeps_technical_and_catastrophic_taxonomy_separate():
    pos = _now_call(
        option_bid=1.28,
        underlying_price=0.0,
        underlying_available=False,
        underlying_fresh=False,
    )

    decision = evaluate_exit(pos, _market_time())

    assert decision.action == "HOLD", decision.reason
    assert decision.reason_code == "UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE"
    assert "underlying" in decision.reason.lower()
