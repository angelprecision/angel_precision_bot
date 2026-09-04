"""Regression tests for bounded recovery after a profitable pullback."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

import ap_exit_engine as engine
from ap import touched_profit_confirmation_guard
from ap_exit_engine import ExitDecision, ManagedPosition


UTC = timezone.utc
ET = ZoneInfo("America/New_York")
EVAL_ET = datetime(2026, 8, 28, 10, 0, tzinfo=ET)
EVAL_UTC = EVAL_ET.astimezone(UTC)


def _position(*, peak: float = 0.10, bid: float = 1.02) -> ManagedPosition:
    pos = ManagedPosition(
        ticker="SPY",
        option_symbol="SPY260911P00600000",
        side="PUT",
        quantity=2,
        entry_price=1.00,
        underlying_entry=150.0,
        underlying_target=140.0,
        underlying_stop=152.0,
        execution_mode="live",
        quantity_remaining=2,
        peak_pnl_pct=peak,
        max_profit_seen=peak,
        touched_profit=True,
        opened_at=EVAL_UTC - timedelta(minutes=30),
    )
    pos.current_bid = bid
    pos.current_ask = bid + 0.02
    pos.current_option_price = bid
    pos.current_underlying = 149.0
    pos.option_bid_valid = True
    pos.option_quote_fresh = True
    pos.underlying_available = True
    pos.underlying_fresh = True
    pos.last_option_bid_update_ts = EVAL_UTC - timedelta(seconds=1)
    pos.last_option_quote_update_ts = EVAL_UTC - timedelta(seconds=1)
    pos.last_underlying_quote_update_ts = EVAL_UTC - timedelta(seconds=1)
    return pos


def _refresh_quotes(pos: ManagedPosition, now_et: datetime, bid: float = 1.02) -> None:
    now_utc = now_et.astimezone(UTC)
    pos.current_bid = bid
    pos.current_ask = bid + 0.02
    pos.current_option_price = bid
    pos.last_option_bid_update_ts = now_utc - timedelta(seconds=1)
    pos.last_option_quote_update_ts = now_utc - timedelta(seconds=1)
    pos.last_underlying_quote_update_ts = now_utc - timedelta(seconds=1)


def _evaluate_core(pos: ManagedPosition, now_et: datetime):
    """Exercise the evaluator beneath any process-wide lifecycle wrapper."""
    original = getattr(engine, "_AP_TOUCHED_PROFIT_CONFIRMATION_ORIGINAL", None)
    return (original or engine.evaluate_exit)(pos, now_et)


def test_pullback_timer_starts_at_floor_breach_not_position_entry(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFIT_PULLBACK_RECOVERY_MINUTES", "5")
    pos = _position()

    first = _evaluate_core(pos, EVAL_ET)
    assert first.action == "HOLD"
    assert first.reason_code == "PROFIT_PULLBACK_RECOVERY"
    assert pos.profit_pullback_started_at == EVAL_UTC

    four_minutes_later = EVAL_ET + timedelta(minutes=4)
    _refresh_quotes(pos, four_minutes_later)
    still_recovering = _evaluate_core(pos, four_minutes_later)
    assert still_recovering.action == "HOLD"
    assert still_recovering.reason_code == "PROFIT_PULLBACK_RECOVERY"

    after_window = EVAL_ET + timedelta(minutes=6)
    _refresh_quotes(pos, after_window)
    expired = _evaluate_core(pos, after_window)
    assert expired.action == "CLOSE_ALL"
    assert "TOUCHED PROFIT STOP" in expired.reason


def test_recovered_bid_starts_a_new_pullback_window(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFIT_PULLBACK_RECOVERY_MINUTES", "5")
    pos = _position()
    assert _evaluate_core(pos, EVAL_ET).reason_code == "PROFIT_PULLBACK_RECOVERY"

    recovered_at = EVAL_ET + timedelta(minutes=1)
    _refresh_quotes(pos, recovered_at, bid=1.08)
    recovered = _evaluate_core(pos, recovered_at)
    assert recovered.action == "HOLD"
    assert pos.profit_pullback_started_at is None

    new_breach_at = recovered_at + timedelta(minutes=6)
    _refresh_quotes(pos, new_breach_at, bid=1.02)
    new_breach = _evaluate_core(pos, new_breach_at)
    assert new_breach.reason_code == "PROFIT_PULLBACK_RECOVERY"
    assert pos.profit_pullback_started_at == new_breach_at.astimezone(UTC)


def test_runner_eligible_winner_keeps_profit_floor_authority(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFIT_PULLBACK_RECOVERY_MINUTES", "15")
    pos = _position(peak=0.15, bid=1.02)

    decision = _evaluate_core(pos, EVAL_ET)

    # Standard-DTE canonical runner arm is +12%; a +15% peak is already a
    # runner, so the recovery window must not override its +8% floor.
    assert decision.action == "CLOSE_ALL"
    assert decision.reason_code != "PROFIT_PULLBACK_RECOVERY"
    assert "TOUCHED PROFIT STOP" in decision.reason


def test_hard_stop_precedes_pullback_recovery() -> None:
    pos = _position(peak=0.10, bid=0.60)
    pos.hard_exit_reference_price = 0.60
    pos.hard_exit_reference_validity = "proven"
    pos.hard_exit_reference_ts = EVAL_UTC - timedelta(seconds=1)

    decision = _evaluate_core(pos, EVAL_ET)

    assert decision.action == "STOP"
    assert "HARD STOP" in decision.reason
    assert pos.profit_pullback_started_at is None


def test_live_confirmation_then_recovery_uses_same_timer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("PROFIT_PULLBACK_RECOVERY_MINUTES", "5")
    pos = _position()
    candidate = ExitDecision(
        action="CLOSE_ALL",
        quantity=2,
        reason="TOUCHED PROFIT STOP — peaked +10% now +2% — floor=5%",
        urgency="IMMEDIATE",
        pnl_pct=0.02,
        reason_code="TOUCHED_PROFIT_STOP",
    )
    wrapped = touched_profit_confirmation_guard.wrap_evaluate_exit(
        lambda _pos, now_et=None: candidate,
        exit_decision_cls=ExitDecision,
        classify_decision=engine._classify_exit_decision,
    )

    first = wrapped(pos, EVAL_ET)
    assert first.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"

    pos.last_option_bid_update_ts = EVAL_UTC
    second = wrapped(pos, EVAL_ET)
    assert second.action == "HOLD"
    assert second.reason_code == "PROFIT_PULLBACK_RECOVERY"


def test_restart_hydrates_high_water_pullback_timer_and_entry_clock() -> None:
    from ap_exit_engine import APExitEngine

    row = {
        "id": "position-pullback-1",
        "client_id": "client@example.com",
        "underlying": "SPY",
        "contract": "SPY260911P00600000",
        "direction": "PUT",
        "qty": 2,
        "avg_fill": 1.00,
        "underlying_entry": 150.0,
        "target_underlying": 140.0,
        "stop_underlying": 152.0,
        "entry_ts": EVAL_UTC - timedelta(minutes=30),
        "peak_pnl_pct": 0.10,
        "max_profit_seen": 0.12,
        "touched_profit": True,
        "meta": {
            "profit_pullback": {
                "started_at": EVAL_UTC - timedelta(minutes=1),
                "floor_pct": 0.05,
                "peak_pct": 0.10,
            }
        },
    }

    class _PositionManager:
        def get_active_positions(self):
            return [row]

    engine_instance = APExitEngine(None, email="client@example.com")
    engine_instance.hydrate_pending_exit_identity_from_db = lambda _pos: False
    engine_instance.seed_from_db(_PositionManager())
    hydrated = engine_instance.active_positions()[0]

    assert hydrated.opened_at == EVAL_UTC - timedelta(minutes=30)
    assert hydrated.peak_pnl_pct == pytest.approx(0.12)
    assert hydrated.max_profit_seen == pytest.approx(0.12)
    assert hydrated.touched_profit is True
    assert hydrated.profit_pullback_started_at == EVAL_UTC - timedelta(minutes=1)
    assert hydrated.profit_pullback_floor == pytest.approx(0.05)
