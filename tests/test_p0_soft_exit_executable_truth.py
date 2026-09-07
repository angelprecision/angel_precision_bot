"""P0 regression: executable option bid + same-cycle underlying truth.

Production incidents: Tradefluence PAPER PEP PUT, LULU PUT, and ABT CALL on
2026-07-20. These tests exercise the real quote-monitor shim, real managed
position object, real exit-rule wrapper, and inherited single submit authority.
"""
from __future__ import annotations

import os
import sys
import threading
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch
from zoneinfo import ZoneInfo

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost/test")

import ap_exit_engine as exit_engine_module  # noqa: E402
from ap.position_quote_monitor import APPositionQuoteMonitor  # noqa: E402
from ap_exit_engine import (  # noqa: E402
    APExitEngine,
    ExitDecision,
    ManagedPosition,
    SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE,
    SOFT_EXIT_SUPPRESSED_THESIS_CONFIRMING,
    evaluate_exit,
)

ET = ZoneInfo("America/New_York")
NOW_ET = datetime(2026, 7, 21, 10, 30, tzinfo=ET)
CLIENT = "tradefluence@gmail.com"


class _EngineHarness:
    def __init__(self, positions):
        self._positions = positions
        self._lock = threading.RLock()
        self.snapshots = []

    def active_positions(self):
        return list(self._positions)

    def apply_quote_snapshots(self, snapshots):
        self.snapshots = list(snapshots)


class _Broker:
    base_url = "https://sandbox.tradier.com/v1"


def _position(
    *,
    ticker: str,
    side: str,
    option_entry: float,
    underlying_entry: float,
    mode: str = "paper",
    age_minutes: float = 1.0,
) -> ManagedPosition:
    right = "C" if side == "CALL" else "P"
    return ManagedPosition(
        ticker=ticker,
        option_symbol=f"{ticker}270101{right}00100000",
        side=side,
        quantity=1,
        entry_price=option_entry,
        underlying_entry=underlying_entry,
        underlying_target=(underlying_entry * (1.10 if side == "CALL" else 0.90)),
        underlying_stop=(underlying_entry * (0.95 if side == "CALL" else 1.05)),
        position_id=f"pos-{ticker.lower()}",
        client_id=CLIENT,
        signal_id=f"sig-{ticker.lower()}",
        execution_mode=mode,
        quantity_remaining=1,
        opened_at=datetime.now(timezone.utc) - timedelta(minutes=age_minutes),
    )


def _qpm_for(pos: ManagedPosition) -> APPositionQuoteMonitor:
    engine = _EngineHarness([pos])
    qpm = APPositionQuoteMonitor.__new__(APPositionQuoteMonitor)
    qpm.broker = _Broker()
    qpm.client_id = CLIENT
    qpm.exit_engine = engine
    qpm._alert_fn = lambda _message: None
    qpm._interval = 2.0
    qpm._stop = threading.Event()
    qpm._kick = threading.Event()
    qpm._thread = None
    qpm._health = {}
    qpm._health_lock = threading.Lock()
    qpm._last_push_price = {}
    qpm._last_wake_price = {}
    qpm._last_wake_ts = {}
    qpm._last_immediate_refresh_ts = {}
    qpm._last_db_persist_ts = {}
    qpm._last_db_persist_price = {}
    qpm._orders_meta_available = None
    qpm._cycles = 0
    qpm._consecutive_failures = 0
    qpm._rate_limit_backoff_sec = 1.0
    qpm._last_cycle_ts = 0.0
    qpm._metrics = {"cycles": 0}
    qpm._persist_quote_to_db = MagicMock(return_value=False)
    qpm._persist_mfe_mae_to_orders = MagicMock(return_value=None)
    qpm._mark_mfe_mae_unavailable = MagicMock(return_value=None)
    qpm._classify_health = MagicMock(return_value=None)
    qpm._prune_closed = MagicMock(return_value=None)
    qpm._should_wake = MagicMock(return_value=False)
    qpm.request_immediate_refresh = MagicMock(return_value=True)
    return qpm


def _cycle(
    qpm: APPositionQuoteMonitor,
    pos: ManagedPosition,
    *,
    option_bid: float,
    option_ask: float,
    underlying_bid: float = 0.0,
    underlying_ask: float = 0.0,
    underlying_last: float = 0.0,
    option_extra: dict | None = None,
    underlying_extra: dict | None = None,
):
    option = {
        "symbol": pos.option_symbol,
        "bid": option_bid,
        "ask": option_ask,
        "mark": round((option_bid + option_ask) / 2.0, 4)
        if option_bid > 0 and option_ask > 0
        else 0.0,
        "provider": "tradier",
        "domain": "sandbox",
        **(option_extra or {}),
    }
    underlying = {
        "symbol": pos.ticker,
        "bid": underlying_bid,
        "ask": underlying_ask,
        "last": underlying_last,
        "provider": "tradier",
        "domain": "sandbox",
        **(underlying_extra or {}),
    }

    def _fetch(symbols):
        symbols = {str(symbol).upper() for symbol in symbols}
        if pos.option_symbol.upper() in symbols:
            return {pos.option_symbol.upper(): option}
        if pos.ticker.upper() in symbols:
            return {pos.ticker.upper(): underlying} if any(
                float(underlying.get(key) or 0.0) > 0
                for key in ("bid", "ask", "last", "mark")
            ) else {}
        return {}

    qpm._fetch_batch_cached = MagicMock(side_effect=_fetch)
    qpm._refresh_once()
    return dict(pos.exit_decision_quote_snapshot)


def _arm_two_polls(
    qpm: APPositionQuoteMonitor,
    pos: ManagedPosition,
    *,
    bid: float,
    ask: float,
    underlying: float,
):
    _cycle(
        qpm,
        pos,
        option_bid=bid,
        option_ask=ask,
        underlying_bid=underlying - 0.01,
        underlying_ask=underlying + 0.01,
    )
    _cycle(
        qpm,
        pos,
        option_bid=bid,
        option_ask=ask,
        underlying_bid=underlying - 0.01,
        underlying_ask=underlying + 0.01,
    )


class TestExecutableBidAuthority:
    def test_paper_midpoint_above_four_percent_does_not_arm_when_bid_does_not(self):
        pos = _position(ticker="MID", side="CALL", option_entry=1.00, underlying_entry=100.0)
        qpm = _qpm_for(pos)
        _cycle(qpm, pos, option_bid=1.00, option_ask=1.10, underlying_bid=100.1, underlying_ask=100.2)
        _cycle(qpm, pos, option_bid=1.00, option_ask=1.10, underlying_bid=100.1, underlying_ask=100.2)

        assert pos.analytics_mark_price == pytest.approx(1.05)
        assert pos.current_option_price == pytest.approx(1.00)
        assert pos.touched_profit is False
        assert pos.profit_lock_confirmation_count == 0
        assert evaluate_exit(pos, NOW_ET).should_act is False

    def test_one_fresh_four_percent_bid_poll_does_not_durably_arm(self):
        pos = _position(ticker="ONE", side="CALL", option_entry=1.00, underlying_entry=100.0)
        qpm = _qpm_for(pos)
        _cycle(qpm, pos, option_bid=1.04, option_ask=1.08, underlying_bid=100.1, underlying_ask=100.2)
        assert pos.profit_lock_confirmation_count == 1
        assert pos.profit_lock_armed_at is None
        assert pos.touched_profit is False

    def test_two_fresh_four_percent_bid_polls_arm_durably(self):
        pos = _position(ticker="TWO", side="CALL", option_entry=1.00, underlying_entry=100.0)
        qpm = _qpm_for(pos)
        _arm_two_polls(qpm, pos, bid=1.04, ask=1.08, underlying=100.2)

        assert pos.touched_profit is True
        assert pos.profit_lock_armed_at is not None
        assert pos.profit_lock_arm_bid == pytest.approx(1.04)
        assert pos.profit_lock_arm_pnl_pct == pytest.approx(0.04)
        assert pos.peak_executable_bid == pytest.approx(1.04)
        assert pos.peak_executable_pnl_pct == pytest.approx(0.04)
        assert pos.profit_lock_confirmation_count >= 2
        assert "tradier" in pos.profit_lock_option_quote_source

    @pytest.mark.parametrize("mode", ["paper", "live"])
    def test_paper_and_live_both_decide_from_bid_while_mid_remains_analytics(self, mode):
        pos = _position(
            ticker=f"B{mode[0].upper()}",
            side="CALL",
            option_entry=1.00,
            underlying_entry=100.0,
            mode=mode,
        )
        qpm = _qpm_for(pos)
        _cycle(qpm, pos, option_bid=0.98, option_ask=1.12, underlying_bid=100.0, underlying_ask=100.1)

        assert pos.current_option_price == pytest.approx(0.98)
        assert pos.option_pnl_pct == pytest.approx(-0.02)
        assert pos.analytics_mark_price == pytest.approx(1.05)
        assert pos.live_executable_price_source == "bid"


class TestIncidentReplays:
    def test_pep_put_confirming_underlying_suppresses_touched_profit_exit(self):
        pos = _position(ticker="PEP", side="PUT", option_entry=1.70, underlying_entry=136.19)
        qpm = _qpm_for(pos)
        _arm_two_polls(qpm, pos, bid=1.78, ask=1.82, underlying=135.80)
        assert pos.touched_profit is True

        _cycle(
            qpm,
            pos,
            option_bid=1.53,
            option_ask=1.65,
            underlying_bid=133.46,
            underlying_ask=133.48,
        )
        decision = evaluate_exit(pos, NOW_ET)

        assert decision.action == "HOLD"
        assert decision.reason_code == SOFT_EXIT_SUPPRESSED_THESIS_CONFIRMING
        assert "TOUCHED_PROFIT_FLOOR" in decision.reason
        assert pos.exit_in_flight is False

    def test_lulu_put_single_option_reversal_does_not_prematurely_close(self):
        pos = _position(ticker="LULU", side="PUT", option_entry=2.60, underlying_entry=325.00)
        qpm = _qpm_for(pos)
        _cycle(
            qpm,
            pos,
            option_bid=2.20,
            option_ask=2.40,
            underlying_bid=321.9,
            underlying_ask=322.1,
        )
        decision = evaluate_exit(pos, NOW_ET)

        assert decision.action == "HOLD"
        assert decision.reason_code == SOFT_EXIT_SUPPRESSED_THESIS_CONFIRMING
        assert pos.exit_in_flight is False

    def test_abt_missing_underlying_defers_then_confirming_holds_then_broken_thesis_exits(self, monkeypatch):
        monkeypatch.setattr(exit_engine_module, "SOFT_EXIT_THESIS_GRACE_MINUTES", 45.0)
        pos = _position(
            ticker="ABT",
            side="CALL",
            option_entry=5.00,
            underlying_entry=101.68,
            age_minutes=30.0,
        )
        qpm = _qpm_for(pos)

        _cycle(qpm, pos, option_bid=4.50, option_ask=4.70)
        missing = evaluate_exit(pos, NOW_ET)
        assert missing.action == "HOLD"
        assert missing.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE
        assert pos.exit_in_flight is False

        _cycle(
            qpm,
            pos,
            option_bid=4.50,
            option_ask=4.70,
            underlying_bid=102.69,
            underlying_ask=102.71,
        )
        confirming = evaluate_exit(pos, NOW_ET)
        assert confirming.action == "HOLD"
        assert confirming.reason_code == SOFT_EXIT_SUPPRESSED_THESIS_CONFIRMING

        _cycle(
            qpm,
            pos,
            option_bid=4.50,
            option_ask=4.70,
            underlying_bid=100.0,
            underlying_ask=100.1,
        )
        broken = evaluate_exit(pos, NOW_ET)
        assert broken.should_act is True
        assert broken.reason_code == "SOFT_OPTION_STOP"

        _cycle(qpm, pos, option_bid=3.00, option_ask=3.20)
        hard = evaluate_exit(pos, NOW_ET)
        assert hard.should_act is True
        assert hard.reason_code == "HARD_OPTION_STOP"


class TestUnderlyingTruthFences:
    @pytest.mark.parametrize(
        "mutation,expected_fragment",
        [
            (lambda snap: snap.clear(), "missing_canonical_exit_snapshot"),
            (
                lambda snap: snap.update(
                    option_quote_timestamp=(datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat(),
                    underlying_quote_timestamp=(datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat(),
                    snapshot_timestamp=(datetime.now(timezone.utc) - timedelta(seconds=30)).isoformat(),
                ),
                "stale_",
            ),
            (
                lambda snap: snap.update(
                    underlying_quote_timestamp=(datetime.now(timezone.utc) + timedelta(seconds=30)).isoformat(),
                ),
                "future_dated_underlying_quote",
            ),
            (
                lambda snap: snap.update(
                    same_domain=False,
                    option_quote_domain="sandbox",
                    underlying_quote_domain="live",
                ),
                "cross_domain_quote_pair",
            ),
        ],
    )
    def test_invalid_underlying_truth_cannot_authorize_soft_exit(self, mutation, expected_fragment):
        pos = _position(ticker="FENCE", side="PUT", option_entry=1.00, underlying_entry=100.0)
        qpm = _qpm_for(pos)
        _arm_two_polls(qpm, pos, bid=1.05, ask=1.09, underlying=99.8)
        _cycle(qpm, pos, option_bid=0.95, option_ask=1.01, underlying_bid=99.7, underlying_ask=99.8)

        snapshot = dict(pos.exit_decision_quote_snapshot)
        mutation(snapshot)
        pos.exit_decision_quote_snapshot = snapshot
        decision = evaluate_exit(pos, NOW_ET)

        assert decision.action == "HOLD"
        assert decision.reason_code == SOFT_EXIT_DEFERRED_UNDERLYING_UNAVAILABLE
        assert expected_fragment in decision.reason

    @pytest.mark.parametrize(
        "reason_code,reason",
        [
            ("UNDERLYING_STOP_CONFIRMED", "STOP HIT — underlying stop confirmed"),
            ("EOD_FORCE_CLOSE", "EOD FORCE CLOSE"),
            ("SENTINEL_FORCED_EXIT", "SENTINEL FORCED EXIT — kill_switch"),
        ],
    )
    def test_missing_soft_context_does_not_block_authoritative_exits(
        self, monkeypatch, reason_code, reason
    ):
        pos = _position(ticker="AUTH", side="CALL", option_entry=1.00, underlying_entry=100.0)
        pos.current_bid = 1.00
        pos.current_option_price = 1.00
        pos.executable_quote_valid = True
        pos.exit_decision_quote_snapshot = {}
        legacy = ExitDecision(
            action="CLOSE_ALL",
            quantity=1,
            reason=reason,
            urgency="IMMEDIATE",
            pnl_pct=0.0,
            reason_code=reason_code,
        )
        monkeypatch.setattr(exit_engine_module, "_legacy_evaluate_exit", lambda _p, _n=None: legacy)

        decision = evaluate_exit(pos, NOW_ET)
        assert decision.should_act is True
        assert decision.reason_code == reason_code


class TestSubmitAuthorityAndTaxonomy:
    def test_subclass_does_not_add_a_second_broker_submit_authority(self):
        assert "_submit_exit_decision" not in APExitEngine.__dict__
        assert APExitEngine._submit_exit_decision is exit_engine_module._BaseAPExitEngine._submit_exit_decision

    def test_repeated_submission_is_blocked_by_existing_inflight_owner(self):
        engine = APExitEngine(broker=MagicMock(), email=CLIENT)
        engine.client_id = CLIENT
        pos = _position(ticker="IDEM", side="CALL", option_entry=1.00, underlying_entry=100.0)
        pos.current_bid = 0.60
        pos.current_ask = 0.65
        pos.current_option_price = 0.60
        pos.executable_quote_valid = True
        pos.last_option_quote_update_ts = datetime.now(timezone.utc)
        engine._positions = [pos]
        engine._positions_by_id = {pos.position_id: pos}
        engine.on_exit = MagicMock(
            return_value={
                "accepted": True,
                "local_order_id": "exit-local-1",
                "broker_order_id": "exit-broker-1",
                "status": "submitted",
            }
        )
        safety = types.ModuleType("ap.exit_safety")
        safety.resolve_exit_broker_truth = lambda **_kwargs: {
            "broker_truth_open_qty": 1,
            "is_fresh_exact": True,
            "audit": {},
        }
        safety.evaluate_exit_submission_safety = lambda **_kwargs: {"blocked": False}
        safety.alert_exit_submission_halted = lambda **_kwargs: None
        decision = ExitDecision(
            action="STOP",
            quantity=1,
            reason="HARD OPTION STOP",
            urgency="IMMEDIATE",
            pnl_pct=-0.40,
            reason_code="HARD_OPTION_STOP",
        )

        with patch.dict(sys.modules, {"ap.exit_safety": safety}):
            assert engine._submit_exit_decision(pos, decision) is True
            assert engine._submit_exit_decision(pos, decision) is False

        assert engine.on_exit.call_count == 1
        assert pos.exit_in_flight is True

    @pytest.mark.parametrize(
        "reason,expected",
        [
            ("TOUCHED PROFIT STOP — floor breached", "TOUCHED_PROFIT_FLOOR"),
            ("THESIS_FAIL_SOFT_STOP — underlying broke", "SOFT_OPTION_STOP"),
            ("STOP HIT — underlying held at stop", "UNDERLYING_STOP_CONFIRMED"),
            ("RUNNER TRAIL EXIT — peak drawdown", "RUNNER_TRAIL"),
            ("SMALL WIN LOCK — capture", "SMALL_WIN_CAPTURE"),
            ("HARD STOP — max loss", "HARD_OPTION_STOP"),
        ],
    )
    def test_actual_authorizing_rule_is_preserved(self, reason, expected):
        decision = ExitDecision("CLOSE_ALL", 1, reason, "HIGH")
        assert exit_engine_module._canonical_reason_code(decision) == expected
