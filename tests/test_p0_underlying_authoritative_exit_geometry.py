from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_p0_underlying_authoritative_exit_geometry",
)
os.environ.setdefault("ENCRYPTION_KEY", "ap-pr403-underlying-geometry-test")

from ap_exit_engine import (  # noqa: E402
    APExitEngine,
    ExitDecision,
    ManagedPosition,
    OPTION_CATASTROPHIC_STOP,
    UNDERLYING_STOP_CONFIRMING,
    UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE,
    UNDERLYING_STOP_IDENTITY_UNPROVEN,
    UNDERLYING_TECHNICAL_STOP_CONFIRMED,
    evaluate_exit,
)


UTC = timezone.utc
ET = ZoneInfo("America/New_York")
INCIDENT_ET = datetime(2026, 7, 28, 10, 0, tzinfo=ET)
INCIDENT_UTC = INCIDENT_ET.astimezone(UTC)
CONFIRM_SECONDS = 30.0


@pytest.fixture(autouse=True)
def _deterministic_confirmation_window(monkeypatch):
    """Keep the replay clock deterministic without changing loss thresholds."""
    monkeypatch.setenv("UNDERLYING_STOP_CONFIRM_SECONDS", str(CONFIRM_SECONDS))


def _timestamp(now_utc: datetime, age_seconds: float = 2.0) -> datetime:
    return now_utc - timedelta(seconds=age_seconds)


def _now_call(
    *,
    option_bid: float,
    underlying_price: float,
    underlying_available: bool = True,
    underlying_fresh: bool = True,
    opened_minutes_ago: float = 6.0,
    now_utc: datetime = INCIDENT_UTC,
    side: str = "CALL",
    underlying_stop: float | None = None,
    underlying_target: float | None = None,
    option_symbol: str | None = None,
    execution_mode: str = "live",
) -> ManagedPosition:
    if side == "CALL":
        stop = 104.94 if underlying_stop is None else underlying_stop
        target = 111.36 if underlying_target is None else underlying_target
        symbol = option_symbol or "NOW260731C00113000"
    else:
        stop = 111.36 if underlying_stop is None else underlying_stop
        target = 104.94 if underlying_target is None else underlying_target
        symbol = option_symbol or "NOW260731P00113000"

    pos = ManagedPosition(
        ticker="NOW",
        option_symbol=symbol,
        side=side,
        quantity=1,
        entry_price=1.82,
        underlying_entry=108.15,
        underlying_target=target,
        underlying_stop=stop,
        position_id="pr403-position-1",
        client_id="jasoncosby1@gmail.com",
        execution_mode=execution_mode,
        quantity_remaining=1,
        opened_at=now_utc - timedelta(minutes=opened_minutes_ago),
    )
    _apply_quote_state(
        pos,
        now_utc=now_utc,
        option_bid=option_bid,
        underlying_price=underlying_price,
        underlying_available=underlying_available,
        underlying_fresh=underlying_fresh,
    )
    return pos


def _apply_quote_state(
    pos: ManagedPosition,
    *,
    now_utc: datetime,
    option_bid: float | None = None,
    underlying_price: float | None = None,
    underlying_available: bool = True,
    underlying_fresh: bool = True,
    option_ts: datetime | None = None,
    underlying_ts: datetime | None = None,
) -> None:
    if option_bid is not None:
        bid = float(option_bid)
        ask = max(bid + 0.05, bid) if bid > 0 else 0.0
        pos.current_bid = bid
        pos.currentbid = bid
        pos.current_ask = ask
        pos.currentask = ask
        pos.current_option_price = bid
        pos.currentoptionprice = bid
        pos.analytics_mark_price = bid
        pos.analyticsmarkprice = bid
        pos.option_bid_valid = bid > 0
        pos.optionbidvalid = bid > 0
        pos.option_quote_fresh = bid > 0
        pos.optionquotefresh = bid > 0
        pos.last_option_bid_update_ts = option_ts or _timestamp(now_utc)
        pos.lastoptionbidupdatets = pos.last_option_bid_update_ts
        pos.last_option_quote_update_ts = pos.last_option_bid_update_ts
        pos.lastoptionquoteupdatets = pos.last_option_quote_update_ts

    if underlying_price is not None:
        pos.current_underlying = float(underlying_price)
        pos.currentunderlying = pos.current_underlying
    pos.underlying_available = bool(underlying_available)
    pos.underlyingavailable = pos.underlying_available
    pos.underlying_fresh = bool(underlying_fresh)
    pos.underlyingfresh = pos.underlying_fresh
    pos.last_underlying_quote_update_ts = underlying_ts or _timestamp(now_utc)
    pos.lastunderlyingquoteupdatets = pos.last_underlying_quote_update_ts
    pos.underlying_quote_source = "tradier_quote"
    pos.underlyingquotesource = "tradier_quote"


def _eval(pos: ManagedPosition, now_et: datetime = INCIDENT_ET):
    return evaluate_exit(pos, now_et)


def _advance_underlying(
    pos: ManagedPosition,
    *,
    now_et: datetime,
    price: float | None,
    option_bid: float | None = None,
    available: bool = True,
    fresh: bool = True,
    timestamp_age_seconds: float = 1.0,
) -> None:
    now_utc = now_et.astimezone(UTC)
    _apply_quote_state(
        pos,
        now_utc=now_utc,
        option_bid=option_bid,
        underlying_price=price,
        underlying_available=available,
        underlying_fresh=fresh,
        underlying_ts=_timestamp(now_utc, timestamp_age_seconds),
    )


def test_now_call_incident_replay_is_hold_and_not_a_technical_stop():
    """July 28 NOW: -29.7% BID is above the 3-DTE -33% airbag threshold."""
    pos = _now_call(option_bid=1.28, underlying_price=108.60)

    decision = _eval(pos)

    assert decision.action == "HOLD", decision.reason
    assert "HARD STOP" not in decision.reason
    assert "STOP HIT" not in decision.reason
    assert decision.reason_code not in {
        "STOP_BREACH_STARTED",
        "STOP_BREACH_CONFIRMING",
        UNDERLYING_STOP_CONFIRMING,
        UNDERLYING_TECHNICAL_STOP_CONFIRMED,
    }
    assert pos._underlying_stop_breach_ts is None


def test_call_first_fresh_breach_starts_underlying_confirmation_without_exit():
    pos = _now_call(option_bid=1.45, underlying_price=104.80)

    decision = _eval(pos)

    assert decision.action == "HOLD", decision.reason
    assert decision.reason_code == UNDERLYING_STOP_CONFIRMING
    assert pos._underlying_stop_breach_ts == INCIDENT_UTC
    assert pos._stop_breach_ts is None


def test_call_later_fresh_breach_confirms_once():
    pos = _now_call(option_bid=1.45, underlying_price=104.80)
    first = _eval(pos)
    assert first.reason_code == UNDERLYING_STOP_CONFIRMING

    second_et = INCIDENT_ET + timedelta(seconds=CONFIRM_SECONDS + 1)
    _advance_underlying(pos, now_et=second_et, price=104.70)
    decision = _eval(pos, second_et)

    assert decision.action == "STOP", decision.reason
    assert decision.reason_code == UNDERLYING_TECHNICAL_STOP_CONFIRMED
    assert "NOW260731C00113000" in decision.reason
    assert "source=tradier_quote" in decision.reason
    assert "quote_age_sec" in decision.reason


def test_call_breach_recovers_before_confirmation_and_resets():
    pos = _now_call(option_bid=1.70, underlying_price=104.80)
    assert _eval(pos).reason_code == UNDERLYING_STOP_CONFIRMING

    recovery_et = INCIDENT_ET + timedelta(seconds=10)
    _advance_underlying(pos, now_et=recovery_et, price=105.10)
    decision = _eval(pos, recovery_et)

    assert decision.action == "HOLD", decision.reason
    assert decision.reason_code not in {
        UNDERLYING_STOP_CONFIRMING,
        UNDERLYING_TECHNICAL_STOP_CONFIRMED,
    }
    assert pos._underlying_stop_breach_ts is None
    assert pos._underlying_stop_breach_quote_ts is None


def test_put_mirror_uses_adverse_upward_geometry_for_confirm_and_recovery():
    pos = _now_call(
        side="PUT",
        option_bid=1.45,
        underlying_price=111.50,
        underlying_stop=111.36,
        underlying_target=104.94,
    )
    assert _eval(pos).reason_code == UNDERLYING_STOP_CONFIRMING

    second_et = INCIDENT_ET + timedelta(seconds=CONFIRM_SECONDS + 1)
    _advance_underlying(pos, now_et=second_et, price=111.60)
    confirmed = _eval(pos, second_et)
    assert confirmed.action == "STOP"
    assert confirmed.reason_code == UNDERLYING_TECHNICAL_STOP_CONFIRMED

    recovering = _now_call(
        side="PUT",
        option_bid=1.70,
        underlying_price=111.50,
        underlying_stop=111.36,
        underlying_target=104.94,
    )
    assert _eval(recovering).reason_code == UNDERLYING_STOP_CONFIRMING
    recovery_et = INCIDENT_ET + timedelta(seconds=10)
    _advance_underlying(recovering, now_et=recovery_et, price=111.10)
    recovery = _eval(recovering, recovery_et)
    assert recovery.action == "HOLD"
    assert recovering._underlying_stop_breach_ts is None


def test_missing_underlying_defers_technical_stop_but_not_catastrophic_path():
    pos = _now_call(
        option_bid=1.28,
        underlying_price=0.0,
        underlying_available=False,
        underlying_fresh=False,
    )

    decision = _eval(pos)

    assert decision.action == "HOLD", decision.reason
    assert decision.reason_code == UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE
    assert "underlying" in decision.reason.lower()
    assert "STOP HIT" not in decision.reason
    assert pos._underlying_stop_breach_ts is None


def test_stale_numeric_underlying_beyond_stop_does_not_confirm_or_age_timer():
    pos = _now_call(
        option_bid=1.45,
        underlying_price=104.80,
        underlying_available=True,
        underlying_fresh=False,
        now_utc=INCIDENT_UTC,
    )
    pos.last_underlying_quote_update_ts = INCIDENT_UTC - timedelta(seconds=120)
    pos.lastunderlyingquoteupdatets = pos.last_underlying_quote_update_ts

    decision = _eval(pos)

    assert decision.action == "HOLD", decision.reason
    assert decision.reason_code == UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE
    assert pos._underlying_stop_breach_ts is None


def test_missing_underlying_still_reaches_independent_catastrophic_option_stop():
    pos = _now_call(
        option_bid=0.90,
        underlying_price=0.0,
        underlying_available=False,
        underlying_fresh=False,
    )

    decision = _eval(pos)

    assert decision.action == "STOP", decision.reason
    assert decision.reason_code == OPTION_CATASTROPHIC_STOP
    assert "OPTION_CATASTROPHIC_STOP" in decision.reason
    assert "contract=NOW260731C00113000" in decision.reason
    assert "entry=1.8200" in decision.reason
    assert "source=fresh_executable_bid" in decision.reason
    assert "underlying crossed" not in decision.reason.lower()
    assert UNDERLYING_TECHNICAL_STOP_CONFIRMED not in decision.reason


def test_entry_grace_does_not_suppress_catastrophic_option_stop():
    pos = _now_call(
        option_bid=0.90,
        underlying_price=0.0,
        underlying_available=False,
        underlying_fresh=False,
        opened_minutes_ago=1.0,
    )

    decision = _eval(pos)

    assert decision.action == "STOP"
    assert decision.reason_code == OPTION_CATASTROPHIC_STOP


def test_entry_grace_non_catastrophic_loss_does_not_fabricate_technical_stop():
    pos = _now_call(
        option_bid=1.55,
        underlying_price=108.60,
        opened_minutes_ago=1.0,
    )

    decision = _eval(pos)

    assert decision.action == "HOLD", decision.reason
    assert decision.reason_code != UNDERLYING_TECHNICAL_STOP_CONFIRMED
    assert decision.reason_code != "STOP_BREACH_STARTED"


def test_confirmation_is_interrupted_by_stale_data_and_must_restart():
    pos = _now_call(option_bid=1.45, underlying_price=104.80)
    assert _eval(pos).reason_code == UNDERLYING_STOP_CONFIRMING

    stale_et = INCIDENT_ET + timedelta(seconds=CONFIRM_SECONDS + 5)
    _advance_underlying(
        pos,
        now_et=stale_et,
        price=104.70,
        option_bid=1.45,
        available=False,
        fresh=False,
        timestamp_age_seconds=120,
    )
    stale = _eval(pos, stale_et)
    assert stale.action == "HOLD"
    assert stale.reason_code == UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE
    assert pos._underlying_stop_breach_ts is None

    fresh_et = stale_et + timedelta(seconds=1)
    _advance_underlying(pos, now_et=fresh_et, price=104.70, option_bid=1.45)
    restarted = _eval(pos, fresh_et)
    assert restarted.action == "HOLD"
    assert restarted.reason_code == UNDERLYING_STOP_CONFIRMING


def test_duplicate_evaluation_is_blocked_by_existing_exit_in_flight_fence():
    pos = _now_call(option_bid=1.28, underlying_price=108.60)
    pos.position_id = "pr403-duplicate-position"
    pos.exit_in_flight = True
    pos.pending_exit_reason = "OPTION_CATASTROPHIC_STOP"
    pos.pending_exit_action = "STOP"
    decision = ExitDecision(
        action="STOP",
        quantity=1,
        reason="HARD STOP — OPTION_CATASTROPHIC_STOP",
        urgency="IMMEDIATE",
        reason_code=OPTION_CATASTROPHIC_STOP,
    )

    broker = MagicMock()
    engine = APExitEngine(broker=broker, email=pos.client_id)
    engine._emit_exit_event = lambda *args, **kwargs: None

    assert engine._submit_exit_decision(pos, decision) is False
    broker.assert_not_called()
    assert pos.exit_in_flight is True


@pytest.mark.parametrize("execution_mode", ["paper", "live"])
def test_unproven_midpoint_or_analytics_mark_cannot_authorize_catastrophic_stop(execution_mode):
    pos = _now_call(
        option_bid=0.0,
        underlying_price=108.60,
        execution_mode=execution_mode,
    )
    pos.current_ask = 0.60
    pos.currentask = 0.60
    pos.current_option_price = 0.55
    pos.currentoptionprice = 0.55
    pos.analytics_mark_price = 0.55
    pos.analyticsmarkprice = 0.55
    pos.option_bid_valid = False
    pos.optionbidvalid = False
    pos.option_quote_fresh = False
    pos.optionquotefresh = False

    decision = _eval(pos)

    assert decision.action == "HOLD", decision.reason
    assert decision.reason_code != OPTION_CATASTROPHIC_STOP
    assert decision.reason_code != UNDERLYING_TECHNICAL_STOP_CONFIRMED


@pytest.mark.parametrize(
    ("side", "stop"),
    [("SIDEWAYS", 104.94), ("CALL", 0.0)],
)
def test_invalid_direction_or_stop_fails_closed_for_technical_authority(side, stop):
    pos = _now_call(
        option_bid=1.55,
        underlying_price=100.0,
        side=side,
        underlying_stop=stop,
    )

    decision = _eval(pos)

    assert decision.action == "HOLD", decision.reason
    assert decision.reason_code == UNDERLYING_STOP_DEFERRED_DATA_UNAVAILABLE
    assert UNDERLYING_TECHNICAL_STOP_CONFIRMED not in decision.reason


def test_live_technical_stop_requires_durable_position_identity():
    pos = _now_call(option_bid=1.45, underlying_price=104.80)
    pos.position_id = ""
    pos.client_id = ""

    first = _eval(pos)

    assert first.action == "HOLD", first.reason
    assert first.reason_code == UNDERLYING_STOP_IDENTITY_UNPROVEN
    assert pos._underlying_stop_breach_ts is None

    second_et = INCIDENT_ET + timedelta(seconds=CONFIRM_SECONDS + 1)
    _advance_underlying(pos, now_et=second_et, price=104.70)
    second = _eval(pos, second_et)

    assert second.action == "HOLD", second.reason
    assert second.reason_code == UNDERLYING_STOP_IDENTITY_UNPROVEN
    assert UNDERLYING_TECHNICAL_STOP_CONFIRMED not in second.reason
