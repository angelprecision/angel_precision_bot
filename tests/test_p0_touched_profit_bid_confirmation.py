from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from ap import touched_profit_confirmation_guard as guard


@dataclass
class FakeDecision:
    action: str
    quantity: int
    reason: str
    urgency: str
    pnl_pct: float
    reason_code: str = ""

    @property
    def should_act(self):
        return self.action != "HOLD"


def _position(**overrides):
    now = datetime.now(timezone.utc)
    values = {
        "ticker": "NVDA",
        "execution_mode": "live",
        "quantity_remaining": 2,
        "scale_outs_done": 0,
        "current_bid": 0.72,
        "peak_pnl_pct": 0.0588,
        "max_profit_seen": 0.0588,
        "last_option_bid_update_ts": now,
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _decision(code: str, *, action: str = "CLOSE_ALL", pnl: float = -0.1529):
    return FakeDecision(
        action=action,
        quantity=2 if action != "HOLD" else 0,
        reason=code,
        urgency="IMMEDIATE",
        pnl_pct=pnl,
        reason_code=code,
    )


def _wrapped(sequence):
    decisions = iter(sequence)

    def original(_pos, now_et=None):
        return next(decisions)

    return guard.wrap_evaluate_exit(
        original,
        exit_decision_cls=FakeDecision,
        classify_decision=lambda decision: decision.reason_code,
    )


@pytest.fixture(autouse=True)
def _policy_on(monkeypatch):
    monkeypatch.setenv("TOUCHED_PROFIT_BREACH_CONFIRMATION_ENABLED", "1")
    monkeypatch.setenv("TOUCHED_PROFIT_BREACH_CONFIRMATIONS", "2")


def test_nvda_shape_first_transient_bid_is_held():
    pos = _position()
    wrapped = _wrapped([_decision("TOUCHED_PROFIT_STOP")])

    result = wrapped(pos)

    assert result.action == "HOLD"
    assert result.quantity == 0
    assert result.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert pos._touched_profit_floor_breach_count == 1


def test_repeated_evaluation_of_same_bid_does_not_count_twice():
    pos = _position()
    wrapped = _wrapped([
        _decision("TOUCHED_PROFIT_STOP"),
        _decision("TOUCHED_PROFIT_STOP"),
    ])

    first = wrapped(pos)
    second = wrapped(pos)

    assert first.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert second.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert pos._touched_profit_floor_breach_count == 1


def test_missing_timestamp_does_not_count_toward_confirmation():
    first_ts = datetime.now(timezone.utc)
    pos = _position(last_option_bid_update_ts=None)
    original_decision = _decision("TOUCHED_PROFIT_STOP")
    wrapped = _wrapped([original_decision, original_decision])

    first = wrapped(pos)

    assert first.action == "HOLD"
    assert pos._touched_profit_floor_breach_count == 0

    pos.last_option_bid_update_ts = first_ts
    second = wrapped(pos)

    assert second.action == "HOLD"
    assert pos._touched_profit_floor_breach_count == 1


def test_second_distinct_bid_confirms_sustained_floor_breach():
    first_ts = datetime.now(timezone.utc)
    pos = _position(last_option_bid_update_ts=first_ts)
    original_decision = _decision("TOUCHED_PROFIT_STOP")
    wrapped = _wrapped([original_decision, original_decision])

    held = wrapped(pos)
    pos.last_option_bid_update_ts = first_ts + timedelta(seconds=2)
    pos.current_bid = 0.71
    confirmed = wrapped(pos)

    assert held.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert confirmed is original_decision
    assert confirmed.action == "CLOSE_ALL"
    assert confirmed.reason_code == "TOUCHED_PROFIT_STOP"
    assert pos._touched_profit_floor_breach_count == 0


def test_quote_recovery_resets_confirmation_sequence():
    first_ts = datetime.now(timezone.utc)
    pos = _position(last_option_bid_update_ts=first_ts)
    wrapped = _wrapped([
        _decision("TOUCHED_PROFIT_STOP"),
        _decision("UNKNOWN_EXIT", action="HOLD", pnl=0.0471),
        _decision("TOUCHED_PROFIT_STOP"),
    ])

    first = wrapped(pos)
    pos.last_option_bid_update_ts = first_ts + timedelta(seconds=2)
    recovered = wrapped(pos)
    pos.last_option_bid_update_ts = first_ts + timedelta(seconds=4)
    later = wrapped(pos)

    assert first.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert recovered.reason_code == "UNKNOWN_EXIT"
    assert later.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert pos._touched_profit_floor_breach_count == 1


def test_qqq_runner_trail_passes_through_unchanged():
    pos = _position(
        ticker="QQQ",
        quantity_remaining=1,
        current_bid=1.67,
        peak_pnl_pct=0.525,
        max_profit_seen=0.525,
    )
    runner = _decision("RUNNER_TRAIL", pnl=0.3917)
    wrapped = _wrapped([runner])

    result = wrapped(pos)

    assert result is runner
    assert result.action == "CLOSE_ALL"
    assert pos._touched_profit_floor_breach_count == 0


@pytest.mark.parametrize(
    "code",
    ["HARD_STOP", "STOP_HIT", "EOD_FORCE_CLOSE", "TARGET_HIT"],
)
def test_immediate_money_safety_exits_are_never_delayed(code):
    pos = _position()
    decision = _decision(code, pnl=-0.40)
    wrapped = _wrapped([decision])

    result = wrapped(pos)

    assert result is decision
    assert result.action == "CLOSE_ALL"


def test_paper_touched_profit_behavior_is_unchanged():
    pos = _position(execution_mode="paper")
    decision = _decision("TOUCHED_PROFIT_STOP")
    wrapped = _wrapped([decision])

    result = wrapped(pos)

    assert result is decision


@pytest.mark.parametrize("mode", ["", "unknown", "malformed"])
def test_unknown_mode_is_protected_as_live_risk(mode):
    pos = _position(execution_mode=mode)
    decision = _decision("TOUCHED_PROFIT_STOP")
    wrapped = _wrapped([decision])

    result = wrapped(pos)

    assert result is not decision
    assert result.action == "HOLD"
    assert result.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"


def test_synthetic_confirmation_hold_never_reaches_submit_boundary():
    pos = _position(execution_mode="")
    wrapped = _wrapped([_decision("TOUCHED_PROFIT_STOP")])
    submitted = []

    result = wrapped(pos)
    if result.should_act:
        submitted.append(result)

    assert result.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert submitted == []


def test_feature_off_returns_original_decision(monkeypatch):
    monkeypatch.setenv("TOUCHED_PROFIT_BREACH_CONFIRMATION_ENABLED", "0")
    pos = _position()
    decision = _decision("TOUCHED_PROFIT_STOP")
    wrapped = _wrapped([decision])

    result = wrapped(pos)

    assert result is decision


@pytest.mark.parametrize(
    "raw, expected",
    [("bad", 2), ("1", 2), ("2", 2), ("3", 3), ("99", 4)],
)
def test_confirmation_count_is_bounded(monkeypatch, raw, expected):
    monkeypatch.setenv("TOUCHED_PROFIT_BREACH_CONFIRMATIONS", raw)
    assert guard._required_confirmations() == expected


def test_lifecycle_manifest_marks_guard_required():
    from ap.trade_lifecycle_guards import _GUARDS, _MANDATORY_LIVE_GUARDS

    rows = {name: (module, installer, required) for name, module, installer, required in _GUARDS}
    assert rows["touched_profit_bid_confirmation"] == (
        "ap.touched_profit_confirmation_guard",
        "install_touched_profit_confirmation_guard",
        True,
    )
    assert _MANDATORY_LIVE_GUARDS == {"touched_profit_bid_confirmation"}


def test_missing_touched_profit_guard_blocks_live_but_not_paper(monkeypatch):
    from ap import trade_lifecycle_guards as lifecycle

    manifest = {
        "touched_profit_bid_confirmation": {
            "status": "absent",
            "required_when_present": True,
        },
    }
    monkeypatch.setattr(
        lifecycle,
        "install_trade_lifecycle_guards",
        lambda: manifest,
    )
    monkeypatch.setattr(lifecycle, "_generation_claims_table_exists", lambda: True)

    live_ok, live_diagnostic = lifecycle.lifecycle_guard_preflight("live")
    paper_ok, paper_diagnostic = lifecycle.lifecycle_guard_preflight("paper")

    assert live_ok is False
    assert live_diagnostic["missing_required_guards"] == [
        "touched_profit_bid_confirmation",
    ]
    assert paper_ok is True
    assert paper_diagnostic["missing_required_guards"] == [
        "touched_profit_bid_confirmation",
    ]


def _live_reconciler_with_seeded_position(monkeypatch):
    import ap_reconciler

    class ExitEngine:
        def __init__(self):
            self.positions = []

        def adopt_canonical_position_identity(self, **_kwargs):
            return SimpleNamespace(
                disposition="NO_REPAIR_FOUND",
                adopted=False,
                retryable=False,
                safe_to_seed=True,
            )

        def add_position(self, pos):
            self.positions.append(pos)

        def seed_canonical_position_if_absent(self, pos):
            self.add_position(pos)
            return True, "seeded"

    monkeypatch.setattr(
        ap_reconciler.APBrokerReconciler,
        "_register_health",
        lambda self: None,
    )
    reconciler = ap_reconciler.APBrokerReconciler(
        broker=None,
        client_id="live-client@example.com",
        osm=None,
        pm=None,
        execution_mode="LIVE",
    )
    reconciler.exit_engine = ExitEngine()
    monkeypatch.setattr(
        reconciler,
        "_get_current_underlying_price",
        lambda _symbol: 202.0,
    )
    monkeypatch.setattr(reconciler, "_record_position_reseeded", lambda **_kwargs: None)
    monkeypatch.setattr(reconciler, "_heartbeat", lambda *_args, **_kwargs: None)

    reconciler._seed_exit_engine_from_import(
        pos_id="position-live-recovered",
        contract="NVDA260727P00202500",
        underlying="NVDA",
        side="PUT",
        qty=2,
        entry_px=0.85,
        underlying_entry=202.0,
    )
    assert len(reconciler.exit_engine.positions) == 1
    return reconciler.exit_engine.positions[0]


def test_reconciler_propagates_canonical_live_mode(monkeypatch):
    pos = _live_reconciler_with_seeded_position(monkeypatch)

    assert pos.execution_mode == "live"


def test_recovered_live_position_requires_distinct_real_exit_observations(monkeypatch):
    import ap_exit_engine as engine

    pos = _live_reconciler_with_seeded_position(monkeypatch)
    first_ts = datetime.now(timezone.utc)
    pos.opened_at = first_ts - timedelta(minutes=10)
    pos.quantity_remaining = 2
    pos.touched_profit = True
    pos.peak_pnl_pct = 0.0588
    pos.max_profit_seen = 0.0588
    pos.current_bid = 0.72
    pos.current_option_price = 0.72
    pos.current_underlying = 202.0
    pos.option_bid_valid = True
    pos.option_quote_fresh = True
    pos.underlying_available = True
    pos.underlying_fresh = True
    pos.last_option_bid_update_ts = first_ts
    pos.last_option_quote_update_ts = first_ts
    pos.last_underlying_quote_update_ts = first_ts
    evaluation_time = datetime(2026, 7, 27, 11, 0, tzinfo=engine.ET)

    first = engine.evaluate_exit(pos, evaluation_time)
    repeated = engine.evaluate_exit(pos, evaluation_time)
    pos.last_option_bid_update_ts = first_ts + timedelta(seconds=2)
    pos.last_option_quote_update_ts = first_ts + timedelta(seconds=2)
    second = engine.evaluate_exit(pos, evaluation_time)

    assert first.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert repeated.reason_code == "TOUCHED_PROFIT_STOP_CONFIRMING"
    assert second.action == "CLOSE_ALL"
    assert engine._classify_exit_decision(second) == "TOUCHED_PROFIT_STOP"
