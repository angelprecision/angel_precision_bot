"""P0 regression coverage for executable option and fresh underlying soft exits."""
from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

from ap_exit_engine import (  # noqa: E402
    APExitEngine,
    ExitDecision,
    ManagedPosition,
    SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
    SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS,
    TOUCHED_PROFIT_ARM_PCT,
    _canonical_reason_code,
    _validate_exit_truth_snapshot,
    evaluate_exit,
)

ET = ZoneInfo("America/New_York")
CLIENT = "tradefluence.paper@example.com"


def _engine(pos: ManagedPosition) -> APExitEngine:
    engine = APExitEngine.__new__(APExitEngine)
    engine._email = pos.client_id
    engine._lock = threading.RLock()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    engine._request_immediate_quote_retry = lambda _pos: True
    engine._persist_soft_exit_truth_to_db = lambda _pos, force=False: False
    return engine


def _position(ticker: str, side: str, entry: float, underlying_entry: float, *, mode: str = "paper", age_min: float = 2.0, stop: float = 0.0, target: float = 0.0) -> ManagedPosition:
    right = "C" if side == "CALL" else "P"
    pos = ManagedPosition(
        ticker=ticker,
        option_symbol=f"{ticker}260731{right}00100000",
        side=side,
        quantity=1,
        entry_price=entry,
        underlying_entry=underlying_entry,
        underlying_target=target,
        underlying_stop=stop,
        position_id=f"pos-{ticker.lower()}",
        client_id=CLIENT,
        signal_id=f"sig-{ticker.lower()}",
        execution_mode=mode,
        quantity_remaining=1,
        opened_at=datetime.now(timezone.utc) - timedelta(minutes=age_min),
    )
    pos.executable_bid_soft_exit_truth_enabled = True
    return pos


def _apply_cycle(engine: APExitEngine, pos: ManagedPosition, *, bid: float, ask: float, underlying_bid: float = 0.0, underlying_ask: float = 0.0, underlying_last: float = 0.0, ts: datetime | None = None, option_domain: str = "tradier_live_market_data", underlying_domain: str | None = None, provider: str = "tradier") -> datetime:
    ts = ts or datetime.now(timezone.utc)
    engine._qpm_cycle_context = {
        "option_quotes": {
            pos.option_symbol: {
                "bid": bid,
                "ask": ask,
                "mark": round((bid + ask) / 2.0, 4) if bid and ask else ask,
                "last": round((bid + ask) / 2.0, 4) if bid and ask else ask,
            }
        },
        "underlying_quotes": {
            pos.ticker: {
                "bid": underlying_bid,
                "ask": underlying_ask,
                "last": underlying_last,
            }
        },
        "original_modes": {pos.position_id: pos.execution_mode},
        "original_modes_by_symbol": {pos.option_symbol: pos.execution_mode},
        "quote_provider": provider,
        "quote_domain": option_domain,
        "snapshot_timestamp": ts,
        "cycle_id": f"test-{ts.timestamp()}",
    }
    engine.apply_exit_decision_snapshots([
        {
            "position_id": pos.position_id,
            "option_symbol": pos.option_symbol,
            "ticker": pos.ticker,
            "current_bid": bid,
            "current_ask": ask,
            "current_underlying": underlying_last,
        }
    ])
    if underlying_domain is not None:
        pos.exit_decision_quote_snapshot["underlying_quote_domain"] = underlying_domain
    return ts


def _now_et(ts: datetime) -> datetime:
    return ts.astimezone(ET)


class TestExecutableBidAuthority:
    def test_paper_midpoint_reaches_four_percent_but_bid_does_not_arm(self):
        pos = _position("MID", "CALL", 1.00, 100.0)
        engine = _engine(pos)
        ts = _apply_cycle(engine, pos, bid=1.00, ask=1.10, underlying_bid=100.9, underlying_ask=101.1, underlying_last=101.0)
        assert pos.analytics_mark_price == 1.05
        assert pos.current_option_price == 1.00
        assert pos.touched_profit is False
        assert pos.profit_lock_confirmation_count == 0
        decision = evaluate_exit(pos, _now_et(ts))
        assert decision.action == "HOLD"

    def test_live_and_paper_both_use_bid_while_midpoint_remains_analytics(self):
        for mode in ("paper", "live"):
            pos = _position(mode.upper(), "CALL", 2.00, 100.0, mode=mode)
            engine = _engine(pos)
            _apply_cycle(engine, pos, bid=2.02, ask=2.22, underlying_bid=100.9, underlying_ask=101.1, underlying_last=101.0)
            assert pos.current_option_price == 2.02
            assert pos.executable_exit_price == 2.02
            assert pos.analytics_mark_price == 2.12
            assert abs(pos.option_pnl_pct - 0.01) < 1e-9

    def test_single_four_percent_bid_poll_does_not_durably_arm(self):
        pos = _position("ONE", "CALL", 1.00, 100.0)
        engine = _engine(pos)
        _apply_cycle(engine, pos, bid=1.04, ask=1.08, underlying_bid=100.9, underlying_ask=101.1, underlying_last=101.0)
        assert TOUCHED_PROFIT_ARM_PCT == 0.04
        assert pos.profit_lock_confirmation_count == 1
        assert pos.touched_profit is False
        assert pos.profit_lock_armed_at is None

    def test_two_fresh_four_percent_bid_polls_arm_durably(self):
        pos = _position("TWO", "CALL", 1.00, 100.0)
        engine = _engine(pos)
        first = datetime.now(timezone.utc)
        _apply_cycle(engine, pos, bid=1.04, ask=1.08, underlying_bid=100.9, underlying_ask=101.1, underlying_last=101.0, ts=first)
        _apply_cycle(engine, pos, bid=1.05, ask=1.09, underlying_bid=101.0, underlying_ask=101.2, underlying_last=101.1, ts=first + timedelta(seconds=2))
        assert pos.profit_lock_confirmation_count == 2
        assert pos.touched_profit is True
        assert pos.profit_lock_armed_at == first + timedelta(seconds=2)
        assert pos.profit_lock_arm_bid == 1.05
        assert abs(pos.profit_lock_arm_pnl_pct - 0.05) < 1e-9
        assert pos.peak_executable_bid == 1.05
        assert abs(pos.peak_executable_pnl_pct - 0.05) < 1e-9


class TestProductionReplays:
    def test_pep_put_confirming_underlying_suppresses_touched_profit_exit(self):
        pos = _position("PEP", "PUT", 1.70, 136.19, age_min=2)
        engine = _engine(pos)
        first = datetime.now(timezone.utc)
        _apply_cycle(engine, pos, bid=1.77, ask=1.83, underlying_bid=133.45, underlying_ask=133.49, underlying_last=133.47, ts=first)
        _apply_cycle(engine, pos, bid=1.78, ask=1.84, underlying_bid=133.45, underlying_ask=133.49, underlying_last=133.47, ts=first + timedelta(seconds=2))
        assert pos.touched_profit is True
        third = first + timedelta(seconds=4)
        _apply_cycle(engine, pos, bid=1.52, ask=1.58, underlying_bid=133.45, underlying_ask=133.49, underlying_last=133.47, ts=third)
        decision = evaluate_exit(pos, _now_et(third))
        assert decision.action == "HOLD"
        assert decision.reason_code == SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS
        assert pos.exit_in_flight is False

    def test_lulu_put_single_option_reversal_does_not_close_confirming_thesis(self):
        pos = _position("LULU", "PUT", 2.60, 300.0, age_min=2)
        engine = _engine(pos)
        ts = _apply_cycle(engine, pos, bid=2.28, ask=2.42, underlying_bid=296.8, underlying_ask=297.0, underlying_last=296.9)
        decision = evaluate_exit(pos, _now_et(ts))
        assert decision.action == "HOLD"
        assert decision.reason_code == SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS
        assert pos.quantity_remaining == 1

    def test_abt_missing_underlying_defers_then_confirming_holds_then_break_exits(self):
        pos = _position("ABT", "CALL", 5.00, 101.68, age_min=30)
        engine = _engine(pos)
        refreshes = []
        pos._soft_exit_refresh_callback = lambda: refreshes.append("refresh") or True
        first = _apply_cycle(engine, pos, bid=4.50, ask=4.70)
        deferred = evaluate_exit(pos, _now_et(first))
        assert deferred.action == "HOLD"
        assert deferred.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE
        assert refreshes == ["refresh"]
        assert pos.exit_in_flight is False
        confirming_ts = first + timedelta(seconds=2)
        _apply_cycle(engine, pos, bid=4.50, ask=4.70, underlying_bid=102.68, underlying_ask=102.72, underlying_last=102.70, ts=confirming_ts)
        confirming = evaluate_exit(pos, _now_et(confirming_ts))
        assert confirming.action == "HOLD"
        assert confirming.reason_code == SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS
        broken_ts = first + timedelta(seconds=4)
        _apply_cycle(engine, pos, bid=4.50, ask=4.70, underlying_bid=100.78, underlying_ask=100.82, underlying_last=100.80, ts=broken_ts)
        exit_decision = evaluate_exit(pos, _now_et(broken_ts))
        assert exit_decision.should_act is True
        assert exit_decision.reason_code == "SOFT_OPTION_STOP"


class TestUnderlyingTruthFailClosed:
    def test_missing_stale_future_and_cross_domain_underlying_are_invalid(self):
        cases = []
        base = datetime.now(timezone.utc)
        missing = _position("MIS", "CALL", 1.00, 100.0, age_min=30)
        missing_engine = _engine(missing)
        _apply_cycle(missing_engine, missing, bid=0.90, ask=1.00, ts=base)
        cases.append((missing, "missing_underlying_quote"))
        stale = _position("STL", "CALL", 1.00, 100.0, age_min=30)
        stale_engine = _engine(stale)
        _apply_cycle(stale_engine, stale, bid=0.90, ask=1.00, underlying_bid=99.8, underlying_ask=100.0, underlying_last=99.9, ts=base - timedelta(seconds=30))
        cases.append((stale, "stale"))
        future = _position("FUT", "CALL", 1.00, 100.0, age_min=30)
        future_engine = _engine(future)
        _apply_cycle(future_engine, future, bid=0.90, ask=1.00, underlying_bid=99.8, underlying_ask=100.0, underlying_last=99.9, ts=base + timedelta(seconds=30))
        cases.append((future, "future"))
        cross = _position("DOM", "CALL", 1.00, 100.0, age_min=30)
        cross_engine = _engine(cross)
        _apply_cycle(cross_engine, cross, bid=0.90, ask=1.00, underlying_bid=99.8, underlying_ask=100.0, underlying_last=99.9, ts=base, underlying_domain="tradier_sandbox_market_data")
        cases.append((cross, "cross_domain"))
        for pos, expected_fragment in cases:
            status = _validate_exit_truth_snapshot(pos, now_utc=base)
            assert status.valid is False
            assert expected_fragment in status.reason
            decision = evaluate_exit(pos, base.astimezone(ET))
            assert decision.action == "HOLD"
            assert decision.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE

    def test_repeated_deferred_evaluation_is_idempotent_and_never_marks_exit_inflight(self):
        pos = _position("IDEM", "CALL", 1.00, 100.0, age_min=30)
        engine = _engine(pos)
        ts = _apply_cycle(engine, pos, bid=0.90, ask=1.00)
        first = evaluate_exit(pos, _now_et(ts))
        second = evaluate_exit(pos, _now_et(ts))
        assert first.reason_code == second.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE
        assert first.quantity == second.quantity == 0
        assert pos.exit_in_flight is False
        assert pos.pending_exit_qty == 0


class TestForcedExitsRemainAuthoritative:
    def test_hard_option_stop_is_not_blocked_by_missing_underlying(self):
        pos = _position("HARD", "CALL", 5.00, 100.0, age_min=30)
        engine = _engine(pos)
        ts = _apply_cycle(engine, pos, bid=3.00, ask=3.20)
        decision = evaluate_exit(pos, _now_et(ts))
        assert decision.should_act is True
        assert decision.reason_code == "HARD_OPTION_STOP"

    def test_confirmed_underlying_stop_is_not_blocked_by_soft_context(self):
        pos = _position("USTP", "CALL", 5.00, 102.0, age_min=30, stop=101.0)
        engine = _engine(pos)
        ts = _apply_cycle(engine, pos, bid=4.90, ask=5.10, underlying_bid=99.9, underlying_ask=100.1, underlying_last=100.0)
        pos._underlying_stop_breach_ts = ts - timedelta(seconds=31)
        decision = evaluate_exit(pos, _now_et(ts))
        assert decision.should_act is True
        assert decision.reason_code == "UNDERLYING_STOP_CONFIRMED"

    def test_eod_close_is_not_blocked_by_missing_underlying(self):
        pos = _position("EOD", "CALL", 5.00, 100.0, age_min=30)
        engine = _engine(pos)
        ts = _apply_cycle(engine, pos, bid=4.90, ask=5.10)
        now_et = ts.astimezone(ET).replace(hour=15, minute=55, second=0, microsecond=0)
        decision = evaluate_exit(pos, now_et)
        assert decision.should_act is True
        assert decision.reason_code == "EOD_FORCE_CLOSE"

    def test_kill_switch_taxonomy_is_never_classified_as_soft(self):
        decision = ExitDecision(action="CLOSE_ALL", quantity=1, reason="DAILY KILL SWITCH — force close", urgency="IMMEDIATE", reason_code="DAILY_KILL_SWITCH")
        assert _canonical_reason_code(decision) == "DAILY_KILL_SWITCH"


class TestTaxonomy:
    def test_actual_authorizing_rules_have_distinct_codes(self):
        cases = {
            "TOUCHED PROFIT STOP — floor crossed": "TOUCHED_PROFIT_FLOOR",
            "THESIS_FAIL_SOFT_STOP — underlying broke": "SOFT_OPTION_STOP",
            "STOP HIT — underlying held below stop for 30s": "UNDERLYING_STOP_CONFIRMED",
            "RUNNER TRAIL EXIT — peak reversed": "RUNNER_TRAIL",
            "SMALL WIN LOCK — protecting gain": "SMALL_WIN_CAPTURE",
            "HARD STOP — max loss": "HARD_OPTION_STOP",
        }
        for reason, expected in cases.items():
            decision = ExitDecision("CLOSE_ALL", 1, reason, "HIGH")
            assert _canonical_reason_code(decision) == expected
