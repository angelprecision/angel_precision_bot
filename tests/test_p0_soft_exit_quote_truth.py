"""P0 regressions for executable option bid and fresh underlying exit truth."""
from __future__ import annotations

import os
import threading
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

from ap.position_quote_monitor import APPositionQuoteMonitor  # noqa: E402
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


def position(ticker="TEST", side="CALL", entry=1.0, underlying_entry=100.0,
             mode="paper", age_min=30.0, stop=0.0, target=0.0):
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


def engine_for(pos):
    engine = APExitEngine.__new__(APExitEngine)
    engine._email = pos.client_id
    engine._lock = threading.RLock()
    engine._positions = [pos]
    engine._positions_by_id = {pos.position_id: pos}
    engine._request_immediate_quote_retry = lambda _pos: True
    engine._persist_soft_exit_truth_to_db = lambda _pos, force=False: False
    engine._emit_exit_event = lambda *args, **kwargs: None
    return engine


def cycle(engine, pos, *, bid, ask, underlying=0.0, ts=None,
          option_ts=None, underlying_ts=None, cycle_id=None,
          provider="tradier", domain="tradier_live_market_data",
          underlying_domain=None):
    ts = ts or datetime.now(timezone.utc)
    option_ts = option_ts or ts
    underlying_ts = underlying_ts or ts
    cycle_id = cycle_id or f"cycle-{ts.timestamp()}"
    underlying_quote = {}
    if underlying > 0:
        underlying_quote = {
            "bid": underlying - 0.02,
            "ask": underlying + 0.02,
            "last": underlying,
        }
    engine._qpm_cycle_context = {
        "option_quotes": {
            pos.option_symbol: {
                "bid": bid,
                "ask": ask,
                "mark": (bid + ask) / 2 if bid and ask else ask,
                "last": (bid + ask) / 2 if bid and ask else ask,
            }
        },
        "underlying_quotes": {pos.ticker: underlying_quote},
        "option_quote_timestamps": {pos.option_symbol: option_ts},
        "underlying_quote_timestamps": {pos.ticker: underlying_ts},
        "original_modes": {pos.position_id: pos.execution_mode},
        "original_modes_by_symbol": {pos.option_symbol: pos.execution_mode},
        "quote_provider": provider,
        "quote_domain": domain,
        "snapshot_timestamp": ts,
        "cycle_id": cycle_id,
    }
    engine.apply_exit_decision_snapshots([{
        "position_id": pos.position_id,
        "option_symbol": pos.option_symbol,
        "ticker": pos.ticker,
        "current_bid": bid,
        "current_ask": ask,
        "current_underlying": underlying,
    }])
    if underlying_domain is not None:
        pos.exit_decision_quote_snapshot["underlying_quote_domain"] = underlying_domain
    return ts


def et(ts):
    return ts.astimezone(ET)


class TestExecutableBidAuthority:
    def test_paper_mid_reaches_four_percent_but_bid_does_not_arm(self):
        pos = position("MID", entry=1.0)
        eng = engine_for(pos)
        ts = cycle(eng, pos, bid=1.0, ask=1.10, underlying=101.0)
        assert pos.analytics_mark_price == 1.05
        assert pos.current_option_price == 1.0
        assert pos.option_pnl_pct == 0.0
        assert pos.touched_profit is False
        assert pos.profit_lock_confirmation_count == 0
        assert evaluate_exit(pos, et(ts)).action == "HOLD"

    def test_paper_and_live_both_use_bid_midpoint_is_analytics_only(self):
        for mode in ("paper", "live"):
            pos = position(mode.upper(), entry=2.0, mode=mode)
            cycle(engine_for(pos), pos, bid=2.02, ask=2.22, underlying=101.0)
            assert pos.current_option_price == 2.02
            assert pos.executable_exit_price == 2.02
            assert pos.analytics_mark_price == 2.12
            assert abs(pos.option_pnl_pct - 0.01) < 1e-9

    def test_one_threshold_poll_does_not_arm(self):
        pos = position("ONE")
        cycle(engine_for(pos), pos, bid=1.04, ask=1.08, underlying=101.0,
              cycle_id="arm-1")
        assert TOUCHED_PROFIT_ARM_PCT == 0.04
        assert pos.profit_lock_confirmation_count == 1
        assert pos.profit_lock_armed_at is None
        assert pos.touched_profit is False

    def test_two_distinct_fresh_threshold_polls_arm_durably(self):
        pos = position("TWO")
        eng = engine_for(pos)
        first = datetime.now(timezone.utc)
        cycle(eng, pos, bid=1.04, ask=1.08, underlying=101.0,
              ts=first, cycle_id="arm-1")
        cycle(eng, pos, bid=1.05, ask=1.09, underlying=101.1,
              ts=first + timedelta(seconds=2), cycle_id="arm-2")
        assert pos.profit_lock_confirmation_count == 2
        assert pos.profit_lock_armed_at == first + timedelta(seconds=2)
        assert pos.profit_lock_arm_bid == 1.05
        assert abs(pos.profit_lock_arm_pnl_pct - 0.05) < 1e-9
        assert pos.peak_executable_bid == 1.05
        assert abs(pos.peak_executable_pnl_pct - 0.05) < 1e-9
        assert pos.touched_profit is True

    def test_same_cycle_cannot_count_twice(self):
        pos = position("DUP")
        eng = engine_for(pos)
        now = datetime.now(timezone.utc)
        for _ in range(2):
            cycle(eng, pos, bid=1.05, ask=1.09, underlying=101.0,
                  ts=now, cycle_id="same")
        assert pos.profit_lock_confirmation_count == 1
        assert pos.touched_profit is False

    def test_missing_underlying_cannot_arm_and_legacy_tick_cannot_bypass(self):
        pos = position("FENCE")
        cycle(engine_for(pos), pos, bid=1.06, ask=1.10, cycle_id="missing-u")
        assert pos.profit_lock_confirmation_count == 0
        pos.touched_profit = True
        pos.touchedprofit = True
        assert pos.touched_profit is False
        assert getattr(pos, "touchedprofit", False) is False


class TestQPMProductionPath:
    def test_real_qpm_cycle_uses_bid_without_mutating_paper_identity(self):
        pos = position("QPM")
        eng = engine_for(pos)

        class Broker:
            base_url = "https://api.tradier.com"

            def get_quotes(self, symbols):
                return {
                    symbol: ({
                        "symbol": symbol,
                        "bid": 100.98,
                        "ask": 101.02,
                        "last": 101.0,
                    } if symbol == pos.ticker else {
                        "symbol": symbol,
                        "bid": 1.0,
                        "ask": 1.10,
                        "mark": 1.05,
                        "last": 1.05,
                    })
                    for symbol in symbols
                }

        qpm = APPositionQuoteMonitor(Broker(), CLIENT, eng)
        qpm._persist_quote_to_db = lambda **kwargs: False
        qpm._persist_mfe_mae_to_orders = lambda **kwargs: False
        qpm._mark_mfe_mae_unavailable = lambda **kwargs: False
        original_mode = pos.execution_mode
        qpm._refresh_once()
        assert pos.execution_mode == original_mode == "paper"
        assert pos.current_option_price == 1.0
        assert pos.analytics_mark_price == 1.05
        assert pos.touched_profit is False
        assert pos.exit_decision_quote_snapshot["execution_mode"] == "paper"
        assert pos.exit_decision_quote_snapshot["option_quote_provider"] == "tradier"


class TestIncidentReplays:
    def test_pep_put_confirming_underlying_suppresses_touched_profit_exit(self):
        pos = position("PEP", "PUT", 1.70, 136.19, age_min=2)
        eng = engine_for(pos)
        first = datetime.now(timezone.utc)
        cycle(eng, pos, bid=1.77, ask=1.83, underlying=133.47,
              ts=first, cycle_id="pep-1")
        cycle(eng, pos, bid=1.78, ask=1.84, underlying=133.47,
              ts=first + timedelta(seconds=2), cycle_id="pep-2")
        assert pos.touched_profit is True
        third = first + timedelta(seconds=4)
        cycle(eng, pos, bid=1.52, ask=1.58, underlying=133.47,
              ts=third, cycle_id="pep-3")
        decision = evaluate_exit(pos, et(third))
        assert decision.action == "HOLD"
        assert decision.reason_code == SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS
        assert pos.exit_in_flight is False

    def test_lulu_put_single_option_reversal_does_not_close_confirming_thesis(self):
        pos = position("LULU", "PUT", 2.60, 300.0, age_min=2)
        ts = cycle(engine_for(pos), pos, bid=2.28, ask=2.42,
                   underlying=296.90, cycle_id="lulu-1")
        decision = evaluate_exit(pos, et(ts))
        assert decision.action == "HOLD"
        assert decision.reason_code == SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS
        assert pos.quantity_remaining == 1

    def test_abt_missing_defers_confirming_holds_nonconfirming_exits(self):
        pos = position("ABT", "CALL", 5.0, 101.68, age_min=30)
        eng = engine_for(pos)
        refreshes = []
        pos._soft_exit_refresh_callback = lambda: refreshes.append(True) or True
        first = cycle(eng, pos, bid=4.50, ask=4.70, cycle_id="abt-missing")
        deferred = evaluate_exit(pos, et(first))
        assert deferred.action == "HOLD"
        assert deferred.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE
        assert refreshes == [True]
        assert pos.exit_in_flight is False

        confirming_ts = first + timedelta(seconds=2)
        cycle(eng, pos, bid=4.50, ask=4.70, underlying=102.70,
              ts=confirming_ts, cycle_id="abt-confirming")
        confirming = evaluate_exit(pos, et(confirming_ts))
        assert confirming.action == "HOLD"
        assert confirming.reason_code == SOFT_EXIT_SUPPRESSED_CONFIRMING_THESIS

        broken_ts = first + timedelta(seconds=4)
        cycle(eng, pos, bid=4.50, ask=4.70, underlying=100.80,
              ts=broken_ts, cycle_id="abt-broken")
        broken = evaluate_exit(pos, et(broken_ts))
        assert broken.should_act is True
        assert broken.reason_code == "SOFT_OPTION_STOP"
        assert broken._exit_truth_cycle_id == "abt-broken"


class TestUnderlyingTruthFailClosed:
    def test_prior_poll_underlying_is_cleared_when_current_cycle_missing(self):
        pos = position("PRIOR")
        pos.current_underlying = 102.0
        cycle(engine_for(pos), pos, bid=0.90, ask=1.0,
              cycle_id="no-current-underlying")
        assert pos.current_underlying == 0.0
        assert pos.exit_decision_quote_snapshot["underlying_midpoint"] == 0.0

    def test_missing_stale_future_cross_cycle_and_cross_domain_are_invalid(self):
        base = datetime.now(timezone.utc)
        cases = []

        missing = position("MIS")
        cycle(engine_for(missing), missing, bid=0.90, ask=1.0, ts=base)
        cases.append((missing, "missing_underlying_quote"))

        stale = position("STL")
        cycle(engine_for(stale), stale, bid=0.90, ask=1.0, underlying=99.0,
              ts=base - timedelta(seconds=30))
        cases.append((stale, "stale"))

        future = position("FUT")
        cycle(engine_for(future), future, bid=0.90, ask=1.0, underlying=99.0,
              ts=base + timedelta(seconds=30))
        cases.append((future, "future"))

        cross_cycle = position("CYCLE")
        cycle(engine_for(cross_cycle), cross_cycle, bid=0.90, ask=1.0,
              underlying=99.0, ts=base, option_ts=base,
              underlying_ts=base - timedelta(seconds=3))
        cases.append((cross_cycle, "cross_cycle"))

        cross_domain = position("DOM")
        cycle(engine_for(cross_domain), cross_domain, bid=0.90, ask=1.0,
              underlying=99.0, ts=base,
              underlying_domain="tradier_sandbox_market_data")
        cases.append((cross_domain, "cross_domain"))

        for pos, fragment in cases:
            status = _validate_exit_truth_snapshot(pos, now_utc=base)
            assert status.valid is False
            assert fragment in status.reason
            decision = evaluate_exit(pos, base.astimezone(ET))
            assert decision.action == "HOLD"
            assert decision.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE

    def test_repeated_defer_is_idempotent_and_never_creates_exit(self):
        pos = position("IDEM")
        ts = cycle(engine_for(pos), pos, bid=0.90, ask=1.0)
        first, second = evaluate_exit(pos, et(ts)), evaluate_exit(pos, et(ts))
        assert first.reason_code == second.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE
        assert first.quantity == second.quantity == 0
        assert pos.exit_in_flight is False
        assert pos.pending_exit_qty == 0

    def test_old_soft_decision_cannot_submit_against_new_cycle(self):
        pos = position("SEAM")
        eng = engine_for(pos)
        first = datetime.now(timezone.utc)
        cycle(eng, pos, bid=0.90, ask=1.0, underlying=98.9,
              ts=first, cycle_id="decision-cycle")
        decision = evaluate_exit(pos, et(first))
        assert decision.should_act is True
        assert decision._exit_truth_cycle_id == "decision-cycle"
        cycle(eng, pos, bid=0.89, ask=0.99, underlying=98.8,
              ts=first + timedelta(seconds=2), cycle_id="new-cycle")
        assert eng._submit_exit_decision(pos, decision) is False
        assert pos.exit_in_flight is False


class TestForcedExitsRemainAuthoritative:
    def test_hard_option_stop_bypasses_missing_underlying(self):
        pos = position("HARD", entry=5.0)
        ts = cycle(engine_for(pos), pos, bid=3.0, ask=3.2)
        decision = evaluate_exit(pos, et(ts))
        assert decision.should_act is True
        assert decision.reason_code == "HARD_OPTION_STOP"

    def test_confirmed_underlying_stop_bypasses_soft_gate(self):
        pos = position("USTP", entry=5.0, underlying_entry=102.0, stop=101.0)
        ts = cycle(engine_for(pos), pos, bid=4.9, ask=5.1, underlying=100.0)
        pos._underlying_stop_breach_ts = ts - timedelta(seconds=31)
        decision = evaluate_exit(pos, et(ts))
        assert decision.should_act is True
        assert decision.reason_code == "UNDERLYING_STOP_CONFIRMED"

    def test_eod_bypasses_missing_underlying(self):
        pos = position("EOD", entry=5.0)
        ts = cycle(engine_for(pos), pos, bid=4.9, ask=5.1)
        now_et = et(ts).replace(hour=15, minute=55, second=0, microsecond=0)
        decision = evaluate_exit(pos, now_et)
        assert decision.should_act is True
        assert decision.reason_code == "EOD_FORCE_CLOSE"

    def test_kill_switch_taxonomy_is_not_soft(self):
        decision = ExitDecision("CLOSE_ALL", 1, "DAILY KILL SWITCH — force close",
                                "IMMEDIATE", reason_code="DAILY_KILL_SWITCH")
        assert _canonical_reason_code(decision) == "DAILY_KILL_SWITCH"


class TestTaxonomy:
    def test_actual_authorizing_rules_are_distinct(self):
        cases = {
            "TOUCHED PROFIT STOP — floor crossed": "TOUCHED_PROFIT_FLOOR",
            "THESIS_FAIL_SOFT_STOP — underlying broke": "SOFT_OPTION_STOP",
            "STOP HIT — underlying held below stop for 30s": "UNDERLYING_STOP_CONFIRMED",
            "RUNNER TRAIL EXIT — peak reversed": "RUNNER_TRAIL",
            "SMALL WIN LOCK — protecting gain": "SMALL_WIN_CAPTURE",
            "HARD STOP — max loss": "HARD_OPTION_STOP",
        }
        for reason, expected in cases.items():
            assert _canonical_reason_code(ExitDecision("CLOSE_ALL", 1, reason, "HIGH")) == expected
