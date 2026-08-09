from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from threading import RLock
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


def test_winner_protection_runs_during_first_technical_breach_confirmation():
    pos = _now_call(option_bid=2.00, underlying_price=104.80)
    pos.touched_profit = True
    pos.max_profit_seen = 0.25

    decision = _eval(pos)

    assert decision.action == "CLOSE_ALL", decision.reason
    assert "TOUCHED PROFIT STOP" in decision.reason
    assert decision.reason_code != UNDERLYING_STOP_CONFIRMING
    assert pos._underlying_stop_breach_ts == INCIDENT_UTC


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


def test_confirmation_requires_new_quote_after_horizon_not_repeated_polling():
    pos = _now_call(option_bid=1.45, underlying_price=104.80)

    first = _eval(pos)
    assert first.reason_code == UNDERLYING_STOP_CONFIRMING
    first_quote_ts = pos._underlying_stop_breach_quote_ts

    quote_b_et = INCIDENT_ET + timedelta(seconds=8)
    _advance_underlying(pos, now_et=quote_b_et, price=104.70)
    quote_b = _eval(pos, quote_b_et)
    assert quote_b.action == "HOLD", quote_b.reason
    assert quote_b.reason_code == UNDERLYING_STOP_CONFIRMING
    quote_b_ts = pos._underlying_stop_breach_quote_ts
    assert quote_b_ts is not None
    assert first_quote_ts is not None
    assert quote_b_ts > first_quote_ts
    assert pos._underlying_stop_breach_ts == INCIDENT_UTC

    for seconds in (16, 24, 32):
        repeated = _eval(pos, INCIDENT_ET + timedelta(seconds=seconds))
        assert repeated.action == "HOLD", repeated.reason
        assert repeated.reason_code == UNDERLYING_STOP_CONFIRMING
        assert pos._underlying_stop_breach_quote_ts == quote_b_ts

    quote_c_et = INCIDENT_ET + timedelta(seconds=40)
    _advance_underlying(pos, now_et=quote_c_et, price=104.60)
    confirmed = _eval(pos, quote_c_et)
    assert confirmed.action == "STOP", confirmed.reason
    assert confirmed.reason_code == UNDERLYING_TECHNICAL_STOP_CONFIRMED


def test_confirmation_restarts_after_process_death_before_technical_stop():
    pos = _now_call(option_bid=1.45, underlying_price=104.80)
    assert _eval(pos).reason_code == UNDERLYING_STOP_CONFIRMING

    quote_b_et = INCIDENT_ET + timedelta(seconds=8)
    _advance_underlying(pos, now_et=quote_b_et, price=104.70)
    assert _eval(pos, quote_b_et).reason_code == UNDERLYING_STOP_CONFIRMING
    assert pos._underlying_stop_breach_ts == INCIDENT_UTC

    # Process death discards the deliberately in-memory confirmation timer;
    # #403 must not add a durable technical-stop lifecycle just to preserve it.
    pos._underlying_stop_breach_ts = None
    pos._underlying_stop_breach_quote_ts = None

    restart_first_et = INCIDENT_ET + timedelta(seconds=20)
    _advance_underlying(pos, now_et=restart_first_et, price=104.60)
    restarted_first = _eval(pos, restart_first_et)
    assert restarted_first.action == "HOLD", restarted_first.reason
    assert restarted_first.reason_code == UNDERLYING_STOP_CONFIRMING
    assert pos._underlying_stop_breach_ts == restart_first_et.astimezone(UTC)

    same_quote_et = INCIDENT_ET + timedelta(seconds=52)
    same_quote = _eval(pos, same_quote_et)
    assert same_quote.action == "HOLD", same_quote.reason
    assert same_quote.reason_code == UNDERLYING_STOP_CONFIRMING

    post_restart_quote_et = INCIDENT_ET + timedelta(seconds=60)
    _advance_underlying(pos, now_et=post_restart_quote_et, price=104.50)
    confirmed = _eval(pos, post_restart_quote_et)
    assert confirmed.action == "STOP", confirmed.reason
    assert confirmed.reason_code == UNDERLYING_TECHNICAL_STOP_CONFIRMED


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


def test_confirmed_technical_stop_overrides_touched_profit_by_thesis_design():
    """A twice-confirmed underlying-stop breach outranks touched-profit /
    profit-floor / scale-out / runner-trail / small-win protection.

    The bot's edge is structural: entries, stops, and targets are set off
    underlying price action (The Strat), not off unrealized option P&L.
    Profit-floor logic protects gains while the underlying thesis is still
    intact; it is not a competing thesis authority. Once the underlying
    itself has confirmed — via two independent fresh observations — that
    the stop level was crossed, the thesis is proven dead, and that signal
    must not be overridden by a downstream option-P&L heuristic. This is
    the same principle the NOW incident enforces in the other direction
    (option BID drawdown alone must not manufacture a technical stop).

    Winner-protection retains priority only during the CONFIRMING window
    (see test_winner_protection_runs_during_first_technical_breach_confirmation);
    once CONFIRMED, the technical stop is authoritative. This is locked in
    by design per docs/pr_specs/p0_underlying_authoritative_exit_geometry.md.
    """
    pos = _now_call(option_bid=2.10, underlying_price=104.80)

    first = _eval(pos)
    assert first.reason_code == UNDERLYING_STOP_CONFIRMING

    # Touched-profit becomes independently eligible at the exact moment the
    # technical stop matures to CONFIRMED on the second fresh observation.
    pos.touched_profit = True
    pos.max_profit_seen = 0.20  # would floor touched-profit protection at +8%

    second_et = INCIDENT_ET + timedelta(seconds=CONFIRM_SECONDS + 1)
    _advance_underlying(pos, now_et=second_et, price=104.70, option_bid=1.50)
    decision = _eval(pos, second_et)

    assert decision.action == "STOP", decision.reason
    assert decision.reason_code == UNDERLYING_TECHNICAL_STOP_CONFIRMED
    assert "TOUCHED PROFIT" not in decision.reason


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


def test_confirmed_technical_stop_uses_one_real_submit_handoff(monkeypatch):
    import ap_exit_engine as exit_engine_mod
    import ap.exit_safety as exit_safety_mod

    pos = _now_call(option_bid=1.45, underlying_price=104.80)
    assert _eval(pos).reason_code == UNDERLYING_STOP_CONFIRMING

    second_et = INCIDENT_ET + timedelta(seconds=CONFIRM_SECONDS + 1)
    _advance_underlying(pos, now_et=second_et, price=104.70)
    decision = _eval(pos, second_et)

    assert decision.action == "STOP", decision.reason
    assert decision.reason_code == UNDERLYING_TECHNICAL_STOP_CONFIRMED

    class _NoSnapshotBroker:
        def __init__(self):
            self.cancel_calls = []

        def list_positions(self):
            raise RuntimeError("test broker snapshot unavailable")

        def cancel_order(self, *args, **kwargs):
            self.cancel_calls.append((args, kwargs))

    broker = _NoSnapshotBroker()
    engine = APExitEngine.__new__(APExitEngine)
    engine.client_id = pos.client_id
    engine._email = pos.client_id
    engine._lock = RLock()
    engine._thread = None
    engine._running = False
    engine.run_id = "pr403-run"
    engine.strategy_version = "pr403-test"
    engine.git_commit = "pr403-test"
    engine.master_control = SimpleNamespace(mode="live")
    engine.on_scale = None
    engine.order_state_machine = None
    engine.osm = None
    engine.broker = broker
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    engine.hydrate_pending_exit_identity_from_db = lambda *_args, **_kwargs: False
    engine._emit_exit_event = lambda *args, **kwargs: None
    engine._clear_degraded_monitoring_state = lambda *args, **kwargs: None

    callback_calls = []

    def _on_exit(callback_pos, callback_decision):
        callback_calls.append((callback_pos, callback_decision))
        return {
            "accepted": True,
            "local_order_id": "local-pr403-technical",
            "broker_order_id": "broker-pr403-technical",
            "status": "accepted",
        }

    engine.on_exit = _on_exit
    monkeypatch.setattr(
        exit_engine_mod,
        "_is_option_quote_stale",
        lambda _pos, _now: (False, 0.0, "fresh"),
    )
    monkeypatch.setattr(
        exit_safety_mod,
        "evaluate_exit_submission_safety",
        lambda **kwargs: {
            "blocked": False,
            "reason": None,
            "position_state": {"entry_ts": None},
            "circuit_breaker": {"blocked": False, "reason": None},
        },
    )

    quantity_before = pos.quantity_remaining
    proof_before = (
        pos._proof_staged,
        pos._proof_finalized,
        pos.proof_logged,
    )
    assert engine._submit_exit_decision(pos, decision) is True

    assert len(callback_calls) == 1
    submitted_pos, submitted_decision = callback_calls[0]
    assert submitted_pos.client_id == "jasoncosby1@gmail.com"
    assert submitted_pos.execution_mode == "live"
    assert submitted_pos.position_id == "pr403-position-1"
    assert submitted_pos.option_symbol == "NOW260731C00113000"
    assert submitted_decision.reason_code == UNDERLYING_TECHNICAL_STOP_CONFIRMED
    assert pos.exit_in_flight is True
    assert pos.pending_exit_local_order_id == "local-pr403-technical"
    assert pos.pending_exit_broker_order_id == "broker-pr403-technical"
    assert pos.quantity_remaining == quantity_before
    assert pos.closed is False
    assert (
        pos._proof_staged,
        pos._proof_finalized,
        pos.proof_logged,
    ) == proof_before
    assert broker.cancel_calls == []

    repeated = _eval(pos, second_et + timedelta(seconds=1))
    if repeated.should_act:
        engine._submit_exit_decision(pos, repeated)
    assert repeated.reason_code == UNDERLYING_STOP_CONFIRMING
    assert len(callback_calls) == 1
    assert broker.cancel_calls == []


def test_confirmed_technical_stop_uses_post_425_broker_owned_handoff(monkeypatch):
    """Join #403 evaluation to the final #425 durable submit/adoption fence."""
    import ap.exit_decision_idempotency_guard as guard
    import ap.exit_safety as exit_safety_mod
    import ap_exit_engine as exit_engine_mod

    pos = _now_call(option_bid=1.45, underlying_price=104.80)
    assert _eval(pos).reason_code == UNDERLYING_STOP_CONFIRMING
    confirmed_et = INCIDENT_ET + timedelta(seconds=CONFIRM_SECONDS + 1)
    _advance_underlying(pos, now_et=confirmed_et, price=104.70)
    decision = _eval(pos, confirmed_et)
    assert decision.reason_code == UNDERLYING_TECHNICAL_STOP_CONFIRMED

    class _BrokerOwnedOSM:
        def __init__(self):
            self.active_order = None
            self.adoption_calls = []

        def _get_active_exit_order(self, position_id):
            if self.active_order and self.active_order.get("position_id") == position_id:
                return dict(self.active_order)
            return None

        get_active_exit_order = _get_active_exit_order

        def get_order(self, local_order_id):
            if self.active_order and self.active_order.get("local_order_id") == local_order_id:
                return dict(self.active_order)
            return None

        def create_exit_order(self, **kwargs):
            local_order_id = str(kwargs["local_order_id"])
            self.active_order = {
                "kind": "EXIT",
                "status": "EXIT_REQUESTED",
                "local_order_id": local_order_id,
                "broker_order_id": "",
                "client_id": pos.client_id,
                "position_id": kwargs["position_id"],
                "execution_mode": kwargs["execution_mode"],
                "contract": kwargs["contract"],
                "symbol": kwargs["symbol"],
                "direction": kwargs["direction"],
                "qty": kwargs["qty"],
                "meta": {},
            }
            return local_order_id

        def update_order_meta(self, local_order_id, patch):
            if not self.active_order or self.active_order.get("local_order_id") != local_order_id:
                return False
            self.active_order["meta"].update(dict(patch or {}))
            return True

        def adopt_broker_owned_exit_request(self, local_order_id, **kwargs):
            self.adoption_calls.append((local_order_id, dict(kwargs)))
            assert self.active_order["status"] == "EXIT_REQUESTED"
            assert self.active_order["execution_mode"] == kwargs["execution_mode"]
            assert self.active_order["client_id"] == kwargs["client_id"]
            assert self.active_order["position_id"] == kwargs["position_id"]
            assert self.active_order["qty"] == kwargs["expected_qty"]
            self.active_order["status"] = "EXIT_SUBMITTED"
            self.active_order["broker_order_id"] = kwargs["broker_order_id"]
            return {
                "disposition": "ADOPTED",
                "adopted": True,
                "status": "EXIT_SUBMITTED",
            }

    class _NoSnapshotBroker:
        def __init__(self):
            self.post_calls = []
            self.cancel_calls = []

        def list_positions(self):
            raise RuntimeError("test broker snapshot unavailable")

        def cancel_order(self, *args, **kwargs):
            self.cancel_calls.append((args, kwargs))

    broker = _NoSnapshotBroker()
    osm = _BrokerOwnedOSM()
    engine = APExitEngine.__new__(APExitEngine)
    engine.client_id = pos.client_id
    engine._email = pos.client_id
    engine._lock = RLock()
    engine._thread = None
    engine._running = False
    engine.run_id = "pr403-post-425-run"
    engine.strategy_version = "pr403-post-425-test"
    engine.git_commit = "pr403-post-425-test"
    engine.master_control = SimpleNamespace(mode="live")
    engine.on_scale = None
    engine.order_state_machine = osm
    engine.osm = None
    engine.broker = broker
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    engine.hydrate_pending_exit_identity_from_db = lambda *_args, **_kwargs: False
    engine._emit_exit_event = lambda *args, **kwargs: None
    engine._clear_degraded_monitoring_state = lambda *args, **kwargs: None

    def _on_exit(callback_pos, callback_decision):
        broker.post_calls.append((callback_pos, callback_decision))
        return {
            "accepted": True,
            "local_order_id": callback_pos.pending_exit_local_order_id,
            "broker_order_id": "broker-pr403-post-425",
            "status": "accepted",
        }

    engine.on_exit = _on_exit
    monkeypatch.setattr(
        exit_engine_mod,
        "_is_option_quote_stale",
        lambda _pos, _now: (False, 0.0, "fresh"),
    )
    monkeypatch.setattr(
        exit_safety_mod,
        "resolve_exit_broker_truth",
        lambda **kwargs: {
            "is_fresh_exact": False,
            "broker_truth_open_qty": None,
            "audit": {"source": "test"},
        },
    )
    monkeypatch.setattr(
        exit_safety_mod,
        "evaluate_exit_submission_safety",
        lambda **kwargs: {
            "blocked": False,
            "reason": None,
            "position_state": {"entry_ts": None},
            "circuit_breaker": {"blocked": False, "reason": None},
        },
    )

    durable_claim = {"claimed": False, "claim_state": guard._CLAIM_STATE_BROKER_OWNED}
    updates = []

    monkeypatch.setattr(
        guard,
        "_durable_exit_generation",
        lambda *_args: ("jasoncosby1@gmail.com|pr403-position-1|1|1", 1),
    )

    def _claim(**kwargs):
        if durable_claim["claimed"]:
            return {
                "claimed": False,
                "claim_state": guard._CLAIM_STATE_BROKER_OWNED,
                "local_order_id": osm.active_order["local_order_id"],
                "broker_order_id": osm.active_order["broker_order_id"],
            }
        durable_claim["claimed"] = True
        return {
            "claimed": True,
            "claim_state": guard._CLAIM_STATE_CLAIMED,
            "local_order_id": "",
            "broker_order_id": "",
        }

    monkeypatch.setattr(guard, "_claim_durable_decision_generation", _claim)
    monkeypatch.setattr(
        guard,
        "_update_durable_decision_generation",
        lambda generation_key, **kwargs: updates.append((generation_key, kwargs)),
    )

    wrapped = guard.wrap_submit(APExitEngine._submit_exit_decision)
    quantity_before = pos.quantity_remaining
    proof_before = (pos._proof_staged, pos._proof_finalized, pos.proof_logged)

    assert wrapped(engine, pos, decision) is True
    assert len(broker.post_calls) == 1
    assert len(osm.adoption_calls) == 1
    assert osm.active_order["status"] == "EXIT_SUBMITTED"
    assert osm.active_order["broker_order_id"] == "broker-pr403-post-425"
    assert updates[-1][1]["claim_state"] == guard._CLAIM_STATE_BROKER_OWNED
    assert broker.post_calls[0][0].client_id == "jasoncosby1@gmail.com"
    assert broker.post_calls[0][0].execution_mode == "live"
    assert broker.post_calls[0][0].position_id == "pr403-position-1"
    assert broker.post_calls[0][0].option_symbol == "NOW260731C00113000"
    assert broker.post_calls[0][1].reason_code == UNDERLYING_TECHNICAL_STOP_CONFIRMED
    assert pos.exit_in_flight is True
    assert pos.quantity_remaining == quantity_before
    assert pos.closed is False
    assert (pos._proof_staged, pos._proof_finalized, pos.proof_logged) == proof_before
    assert broker.cancel_calls == []

    # Simulate process death/restart with a fresh engine and position object.
    restarted_pos = _now_call(option_bid=1.45, underlying_price=104.70)
    restarted_pos.position_id = pos.position_id
    restarted_engine = APExitEngine.__new__(APExitEngine)
    restarted_engine.client_id = restarted_pos.client_id
    restarted_engine._email = restarted_pos.client_id
    restarted_engine._lock = RLock()
    restarted_engine._thread = None
    restarted_engine._running = False
    restarted_engine.master_control = SimpleNamespace(mode="live")
    restarted_engine.order_state_machine = osm
    restarted_engine.osm = None
    restarted_engine.broker = broker
    restarted_engine._positions = [restarted_pos]
    restarted_engine._positions_by_id = {restarted_pos.position_id: restarted_pos}
    restarted_engine._emit_exit_event = lambda *args, **kwargs: None
    restarted_engine._clear_degraded_monitoring_state = lambda *args, **kwargs: None
    restarted_engine.hydrate_pending_exit_identity_from_db = lambda *_args, **_kwargs: False
    restarted_engine.on_scale = None
    restarted_engine.on_exit = _on_exit

    assert wrapped(restarted_engine, restarted_pos, decision) is False
    assert len(broker.post_calls) == 1
    assert len(osm.adoption_calls) == 1
    assert broker.cancel_calls == []


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


@pytest.mark.parametrize(
    ("execution_mode", "expected_first_code", "can_confirm"),
    [
        ("LIVE", UNDERLYING_STOP_IDENTITY_UNPROVEN, False),
        (" live ", UNDERLYING_STOP_IDENTITY_UNPROVEN, False),
        ("PAPER", UNDERLYING_STOP_IDENTITY_UNPROVEN, False),
        ("paper ", UNDERLYING_STOP_IDENTITY_UNPROVEN, False),
        ("", UNDERLYING_STOP_IDENTITY_UNPROVEN, False),
        (None, UNDERLYING_STOP_IDENTITY_UNPROVEN, False),
        ("malformed", UNDERLYING_STOP_IDENTITY_UNPROVEN, False),
        ("live", UNDERLYING_STOP_CONFIRMING, True),
        ("paper", UNDERLYING_STOP_CONFIRMING, True),
    ],
)
def test_technical_stop_requires_exact_canonical_execution_mode(
    execution_mode, expected_first_code, can_confirm,
):
    pos = _now_call(
        option_bid=1.45,
        underlying_price=104.80,
        execution_mode=execution_mode,
    )

    first = _eval(pos)
    assert first.action == "HOLD", first.reason
    assert first.reason_code == expected_first_code

    if not can_confirm:
        assert pos._underlying_stop_breach_ts is None
        assert pos._underlying_stop_breach_quote_ts is None
        return

    second_et = INCIDENT_ET + timedelta(seconds=CONFIRM_SECONDS + 1)
    _advance_underlying(pos, now_et=second_et, price=104.70)
    confirmed = _eval(pos, second_et)
    assert confirmed.action == "STOP", confirmed.reason
    assert confirmed.reason_code == UNDERLYING_TECHNICAL_STOP_CONFIRMED
