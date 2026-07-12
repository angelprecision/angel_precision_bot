from __future__ import annotations

import importlib.util
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ap.contract_playbook import (
    ET,
    STRIKE_POLICY_LABEL_ATM,
    STRIKE_POLICY_LABEL_NONMATCH,
    STRIKE_POLICY_LABEL_ONE_STEP_OTM,
    STRIKE_POLICY_TIER_ATM,
    STRIKE_POLICY_TIER_NONMATCH,
    STRIKE_POLICY_TIER_ONE_STEP_OTM,
    TIMEFRAME_DAILY,
    TIMEFRAME_INTRADAY_SHORT,
    TIMEFRAME_UNKNOWN,
    TIMEFRAME_WEEKLY,
    _same_calendar_week,
    build_playbook_candidate_context,
    normalize_playbook_timeframe,
    playbook_contract_selection_enabled,
    resolve_contract_playbook,
    resolve_playbook_expiration_order,
)

_REPO = Path(__file__).resolve().parents[1]


def _load_selector_module():
    stubs = {
        "ap.db": MagicMock(),
        "ap.brokers": MagicMock(),
        "ap.brokers.tradier": MagicMock(),
        "ap.observability": MagicMock(
            emit_decision_event=MagicMock(),
            get_git_commit=MagicMock(return_value="test"),
            make_config_hash=MagicMock(return_value="test"),
        ),
        "ap.trace": MagicMock(trace_gate=MagicMock()),
        "yfinance": MagicMock(),
        "requests": MagicMock(),
    }
    mod_name = "ap_contract_selector_playbook_policy_shim"
    old_modules = {name: sys.modules.get(name) for name in stubs}
    try:
        for name, stub in stubs.items():
            sys.modules[name] = stub
        spec = importlib.util.spec_from_file_location(mod_name, _REPO / "ap" / "contract_selector.py")
        mod = importlib.util.module_from_spec(spec)
        sys.modules[mod_name] = mod
        try:
            spec.loader.exec_module(mod)
        finally:
            sys.modules.pop(mod_name, None)
        return mod
    finally:
        for name, original in old_modules.items():
            if original is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = original


def _today_plus(days: int, *, today: date | None = None) -> str:
    base = today or date.today()
    return (base + timedelta(days=days)).isoformat()


def _option(symbol: str, expiration: str, strike: float, *, bid: float = 1.0, ask: float = 1.1, delta: float = 0.4) -> dict:
    return {
        "symbol": symbol,
        "option_type": "call",
        "expiration_date": expiration,
        "strike": strike,
        "bid": bid,
        "ask": ask,
        "volume": 500,
        "open_interest": 1000,
        "greeks": {"delta": delta},
    }


def _plan(
    *,
    ticker: str = "SPY",
    timeframe: str = "1d",
    expiration_override: str | None = None,
    metadata: dict | None = None,
    budget: float = 600.0,
) -> SimpleNamespace:
    meta = dict(metadata or {})
    return SimpleNamespace(
        ticker=ticker,
        side="CALL",
        pattern="2-1-2",
        timeframe=timeframe,
        trigger_price=100.0,
        target_underlying=110.0,
        wick_targets=[],
        metadata=meta,
        max_position_usd=budget,
        execution_mode="LIVE",
        mode="LIVE",
        client_id="client-1",
        signal_id="sig-1",
        contracts=0,
        expiration_override=expiration_override,
        tier="A",
        score=75.0,
    )


def _build_engine(mod, *, mutate_plan: bool = True):
    broker = MagicMock()
    broker.base_url = "https://example.invalid"
    broker.cfg = SimpleNamespace(base_url="https://example.invalid", access_token="token")
    broker.session = MagicMock()
    engine = mod.APContractSelectionEngine(
        broker,
        data_broker=broker,
        mode="live",
        mutate_plan=mutate_plan,
        min_dte=0,
        max_dte=21,
    )
    engine._emit_selector_event = MagicMock()
    engine.earnings_guard = None
    engine.iv_filter = None
    return engine


def _playbook_context(mod, plan, expirations: list[str], *, now_et: datetime, enabled: bool = True):
    ctx = mod._new_selector_request_context(plan.ticker, "live")
    ctx.playbook_enabled = enabled
    ctx.playbook_now_et = now_et
    ctx.playbook_today_et = now_et.date()
    ctx.playbook_spec = resolve_contract_playbook(
        ticker=plan.ticker,
        side=plan.side,
        timeframe=plan.timeframe,
        pattern=plan.pattern,
        underlying_price=plan.trigger_price,
        trigger_price=plan.trigger_price,
        target_underlying=plan.target_underlying,
        wick_targets=plan.wick_targets,
        available_expirations=expirations,
        today=now_et.date(),
        min_dte=0,
        max_dte=21,
        metadata=plan.metadata,
        now_et=now_et,
        expiration_override=getattr(plan, "expiration_override", None),
    )
    ctx.playbook_ordered_expirations = resolve_playbook_expiration_order(
        ctx.playbook_spec,
        expirations,
        today=now_et.date(),
        min_dte=0,
        max_dte=21,
    )
    return ctx


def test_flag_aliases_enable_complete_playbook(monkeypatch):
    monkeypatch.setenv("PLAYBOOK_CONTRACT_SELECTION_ENABLED", "0")
    monkeypatch.setenv("PLAYBOOK_STRIKE_SELECTION_ENABLED", "1")
    assert playbook_contract_selection_enabled() is True


def test_timeframe_normalization_is_explicit():
    assert normalize_playbook_timeframe("5m") == TIMEFRAME_INTRADAY_SHORT
    assert normalize_playbook_timeframe("1d") == TIMEFRAME_DAILY
    assert normalize_playbook_timeframe("1w") == TIMEFRAME_WEEKLY
    assert normalize_playbook_timeframe("mystery") == TIMEFRAME_UNKNOWN


def test_index_intraday_cutoff_controls_0dte_order():
    available = [_today_plus(0), _today_plus(1), _today_plus(2), _today_plus(7)]
    spec_1329 = resolve_contract_playbook(
        ticker="SPY",
        side="CALL",
        timeframe="5m",
        pattern="2-1-2",
        underlying_price=500.0,
        trigger_price=499.0,
        target_underlying=505.0,
        wick_targets=[],
        available_expirations=available,
        today=date.today(),
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime.combine(date.today(), datetime.min.time(), tzinfo=ET).replace(hour=13, minute=29),
    )
    spec_1330 = resolve_contract_playbook(
        ticker="SPY",
        side="CALL",
        timeframe="5m",
        pattern="2-1-2",
        underlying_price=500.0,
        trigger_price=499.0,
        target_underlying=505.0,
        wick_targets=[],
        available_expirations=available,
        today=date.today(),
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime.combine(date.today(), datetime.min.time(), tzinfo=ET).replace(hour=13, minute=30),
    )
    ordered_1329 = resolve_playbook_expiration_order(spec_1329, available, today=date.today(), min_dte=0, max_dte=21)
    ordered_1330 = resolve_playbook_expiration_order(spec_1330, available, today=date.today(), min_dte=0, max_dte=21)
    assert ordered_1329[:3] == available[:3]
    assert available[0] not in ordered_1330
    assert ordered_1330[:2] == available[1:3]


def test_index_daily_and_weekly_do_not_share_intraday_policy():
    today = date(2026, 7, 13)
    available = [_today_plus(0, today=today), _today_plus(1, today=today), _today_plus(2, today=today), _today_plus(7, today=today), _today_plus(14, today=today)]
    daily = resolve_contract_playbook(
        ticker="SPY",
        side="CALL",
        timeframe="1d",
        pattern="2-1-2",
        underlying_price=500.0,
        trigger_price=499.0,
        target_underlying=505.0,
        wick_targets=[],
        available_expirations=available,
        today=today,
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime(2026, 7, 13, 11, 0, tzinfo=ET),
    )
    weekly = resolve_contract_playbook(
        ticker="SPY",
        side="CALL",
        timeframe="1w",
        pattern="2-1-2",
        underlying_price=500.0,
        trigger_price=499.0,
        target_underlying=505.0,
        wick_targets=[],
        available_expirations=available,
        today=today,
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime(2026, 7, 13, 11, 0, tzinfo=ET),
    )
    daily_order = resolve_playbook_expiration_order(daily, available, today=today, min_dte=0, max_dte=21)
    weekly_order = resolve_playbook_expiration_order(weekly, available, today=today, min_dte=0, max_dte=21)
    assert daily.timeframe == TIMEFRAME_DAILY
    assert weekly.timeframe == TIMEFRAME_WEEKLY
    assert daily_order[:2] == available[1:3]
    assert weekly_order[0] == available[3]
    assert weekly_order != daily_order


def test_daily_equity_prefers_same_week_wednesday_then_friday():
    today = date(2026, 7, 13)  # Monday
    available = [
        date(2026, 7, 15).isoformat(),  # Wednesday same week
        date(2026, 7, 17).isoformat(),  # Friday same week
        date(2026, 7, 24).isoformat(),  # next Friday
    ]
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1d",
        pattern="3-1-2",
        underlying_price=100.0,
        trigger_price=99.5,
        target_underlying=105.0,
        wick_targets=[],
        available_expirations=available,
        today=today,
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime(2026, 7, 13, 10, 0, tzinfo=ET),
    )
    ordered = resolve_playbook_expiration_order(spec, available, today=today, min_dte=0, max_dte=21)
    assert ordered[:2] == available[:2]


def test_same_calendar_week_true_across_2025_to_2026_boundary():
    assert _same_calendar_week(date(2025, 12, 29), date(2026, 1, 2)) is True


def test_same_calendar_week_true_across_2026_to_2027_boundary():
    assert _same_calendar_week(date(2026, 12, 31), date(2027, 1, 1)) is True


def test_same_calendar_week_false_for_following_iso_week():
    assert _same_calendar_week(date(2026, 1, 2), date(2026, 1, 9)) is False


def test_daily_equity_new_year_same_week_friday_stays_ahead_of_fallback():
    today = date(2025, 12, 29)  # Monday, ISO week 1 of 2026
    same_week_friday = date(2026, 1, 2).isoformat()
    fallback_friday = date(2026, 1, 9).isoformat()
    available = [same_week_friday, fallback_friday]
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1d",
        pattern="3-1-2",
        underlying_price=100.0,
        trigger_price=99.5,
        target_underlying=105.0,
        wick_targets=[],
        available_expirations=available,
        today=today,
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime(2025, 12, 29, 10, 0, tzinfo=ET),
    )
    ordered = resolve_playbook_expiration_order(spec, available, today=today, min_dte=0, max_dte=21)
    assert ordered[0] == same_week_friday
    assert ordered[1] == fallback_friday


def test_weekly_equity_receives_wider_window_than_daily():
    today = date(2026, 7, 13)
    available = [date(2026, 7, 17).isoformat(), date(2026, 7, 24).isoformat(), date(2026, 7, 31).isoformat()]
    daily = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1d",
        pattern="3-1-2",
        underlying_price=100.0,
        trigger_price=99.5,
        target_underlying=105.0,
        wick_targets=[],
        available_expirations=available,
        today=today,
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime(2026, 7, 13, 10, 0, tzinfo=ET),
    )
    weekly = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1w",
        pattern="3-1-2",
        underlying_price=100.0,
        trigger_price=99.5,
        target_underlying=105.0,
        wick_targets=[],
        available_expirations=available,
        today=today,
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime(2026, 7, 13, 10, 0, tzinfo=ET),
    )
    assert date(2026, 7, 31).isoformat() not in daily.permitted_expirations
    assert date(2026, 7, 31).isoformat() in weekly.permitted_expirations


def test_unknown_timeframe_fails_closed():
    available = [_today_plus(1), _today_plus(7)]
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="mystery",
        pattern=None,
        underlying_price=100.0,
        trigger_price=99.5,
        target_underlying=105.0,
        wick_targets=[],
        available_expirations=available,
        today=date.today(),
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime.combine(date.today(), datetime.min.time(), tzinfo=ET).replace(hour=10, minute=0),
    )
    ordered = resolve_playbook_expiration_order(spec, available, today=date.today(), min_dte=0, max_dte=21)
    assert spec.timeframe == TIMEFRAME_UNKNOWN
    assert spec.diagnostics["error_reason"] == "UNKNOWN_PLAYBOOK_TIMEFRAME"
    assert ordered == []


@pytest.mark.parametrize(
    ("override", "expected_reason"),
    [
        ("bad-date", "INVALID_EXPIRATION_OVERRIDE"),
        ("2099-01-01", "EXPIRATION_OVERRIDE_UNAVAILABLE"),
    ],
)
def test_override_validation_reasons(override, expected_reason):
    available = [_today_plus(1), _today_plus(7)]
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1d",
        pattern=None,
        underlying_price=100.0,
        trigger_price=99.5,
        target_underlying=105.0,
        wick_targets=[],
        available_expirations=available,
        today=date.today(),
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime.combine(date.today(), datetime.min.time(), tzinfo=ET).replace(hour=10, minute=0),
        expiration_override=override,
    )
    assert spec.diagnostics["error_reason"] == expected_reason
    assert spec.permitted_expirations == []


def test_override_out_of_range_reason():
    today = date.today()
    out_of_range = _today_plus(30, today=today)
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1d",
        pattern=None,
        underlying_price=100.0,
        trigger_price=99.5,
        target_underlying=105.0,
        wick_targets=[],
        available_expirations=[out_of_range],
        today=today,
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime.combine(today, datetime.min.time(), tzinfo=ET).replace(hour=10, minute=0),
        expiration_override=out_of_range,
    )
    assert spec.diagnostics["error_reason"] == "EXPIRATION_OVERRIDE_OUT_OF_RANGE"


def test_valid_override_wins_without_broadening_policy():
    today = date(2026, 7, 13)
    override = date(2026, 7, 24).isoformat()
    available = [date(2026, 7, 17).isoformat(), override]
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1w",
        pattern=None,
        underlying_price=100.0,
        trigger_price=99.5,
        target_underlying=105.0,
        wick_targets=[],
        available_expirations=available,
        today=today,
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime(2026, 7, 13, 10, 0, tzinfo=ET),
        expiration_override=override,
    )
    ordered = resolve_playbook_expiration_order(spec, available, today=today, min_dte=0, max_dte=21)
    assert ordered[0] == override
    assert spec.diagnostics["override_applied"] is True


def test_prohibited_0dte_override_after_cutoff_is_blocked():
    today = date.today()
    zero_dte = today.isoformat()
    spec = resolve_contract_playbook(
        ticker="SPY",
        side="CALL",
        timeframe="1d",
        pattern=None,
        underlying_price=500.0,
        trigger_price=499.0,
        target_underlying=505.0,
        wick_targets=[],
        available_expirations=[zero_dte, _today_plus(1, today=today)],
        today=today,
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime.combine(today, datetime.min.time(), tzinfo=ET).replace(hour=14, minute=0),
        expiration_override=zero_dte,
    )
    assert spec.diagnostics["error_reason"] == "EXPIRATION_OVERRIDE_POLICY_BLOCKED"


def test_strike_context_prefers_atm_or_one_step_otm_not_far_pt1():
    candidates = [
        {"symbol": "ATM", "strike": 100.0},
        {"symbol": "ONE_UP", "strike": 101.0},
        {"symbol": "PT1", "strike": 110.0},
        {"symbol": "MALFORMED"},
    ]
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1d",
        pattern="2-1-2",
        underlying_price=100.0,
        trigger_price=100.0,
        target_underlying=110.0,
        wick_targets=[],
        available_expirations=[_today_plus(2)],
        today=date.today(),
        min_dte=0,
        max_dte=21,
        metadata={},
        now_et=datetime.combine(date.today(), datetime.min.time(), tzinfo=ET).replace(hour=10, minute=0),
    )
    ctx = build_playbook_candidate_context(spec, candidates)
    assert ctx["preferred_strikes"][:2] == [100.0, 101.0]
    assert ctx["per_symbol"]["ATM"]["strike_policy_tier"] == STRIKE_POLICY_TIER_ATM
    assert ctx["per_symbol"]["ATM"]["strike_policy_label"] == STRIKE_POLICY_LABEL_ATM
    assert ctx["per_symbol"]["ONE_UP"]["strike_policy_tier"] == STRIKE_POLICY_TIER_ONE_STEP_OTM
    assert ctx["per_symbol"]["ONE_UP"]["strike_policy_label"] == STRIKE_POLICY_LABEL_ONE_STEP_OTM
    assert ctx["per_symbol"]["ATM"]["strike_policy_match"] is True
    assert ctx["per_symbol"]["PT1"]["strike_policy_match"] is False
    assert ctx["per_symbol"]["PT1"]["strike_policy_tier"] == STRIKE_POLICY_TIER_NONMATCH
    assert ctx["per_symbol"]["PT1"]["strike_policy_label"] == STRIKE_POLICY_LABEL_NONMATCH
    assert ctx["per_symbol"]["MALFORMED"] == {
        "strike_policy_tier": STRIKE_POLICY_TIER_NONMATCH,
        "strike_policy_label": STRIKE_POLICY_LABEL_NONMATCH,
        "strike_policy_match": False,
        "distance_to_preferred": None,
    }


def test_irregular_call_strikes_use_actual_next_listed_strike():
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="1d",
        pattern=None,
        underlying_price=100.4,
        trigger_price=100.4,
        target_underlying=110.0,
        wick_targets=[],
        available_expirations=[_today_plus(1)],
        today=date.today(),
        min_dte=0,
        max_dte=21,
        metadata={},
    )
    candidates = [_option(str(strike), _today_plus(1), strike) for strike in (100.0, 102.5, 105.0)]
    ctx = build_playbook_candidate_context(spec, candidates)
    assert ctx["atm_strike"] == 100.0
    assert ctx["one_step_otm_strike"] == 102.5
    assert ctx["preferred_strikes"] == [100.0, 102.5]


def test_put_one_step_otm_uses_next_lower_listed_strike():
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="PUT",
        timeframe="1d",
        pattern=None,
        underlying_price=100.2,
        trigger_price=100.2,
        target_underlying=95.0,
        wick_targets=[],
        available_expirations=[_today_plus(1)],
        today=date.today(),
        min_dte=0,
        max_dte=21,
        metadata={},
    )
    candidates = [_option(str(strike), _today_plus(1), strike) for strike in (95.0, 97.5, 100.0, 102.5)]
    ctx = build_playbook_candidate_context(spec, candidates)
    assert ctx["atm_strike"] == 100.0
    assert ctx["one_step_otm_strike"] == 97.5
    assert ctx["preferred_strikes"] == [100.0, 97.5]


def test_selector_uses_eastern_transaction_date_when_utc_differs():
    mod = _load_selector_module()
    engine = object.__new__(mod.APContractSelectionEngine)
    engine.data_broker = MagicMock()
    engine.min_dte = 0
    engine.max_dte = 21
    engine._fetch_underlying_quote = MagicMock(return_value=500.0)
    engine._fetch_expirations_list = MagicMock(return_value=(["2026-07-12", "2026-07-13"], {}))
    engine._playbook_now_et = MagicMock(return_value=datetime(2026, 7, 12, 23, 30, tzinfo=ET))
    plan = _plan(timeframe="1d")
    ordered, audit = engine._resolve_playbook_probe_order(plan, "SPY", request_context=None)
    assert ordered[0] == "2026-07-13"
    assert audit["resolved_today_et"] == "2026-07-12"
    assert audit["preferred_dte_order"][0] == 1


def test_selector_flag_is_frozen_for_one_request(monkeypatch):
    mod = _load_selector_module()
    engine = _build_engine(mod, mutate_plan=False)
    expiration = _today_plus(1)
    wrong = _option("WRONG", expiration, 104.0)
    atm = _option("ATM", expiration, 100.0)
    one_up = _option("ONEUP", expiration, 101.0)
    chain = [wrong, atm, one_up]
    monkeypatch.setenv("PLAYBOOK_CONTRACT_SELECTION_ENABLED", "1")
    engine._quality_filter = lambda opt, today, **kwargs: None
    engine._rank_score = lambda opt, budget, **kwargs: 100.0 if opt["symbol"] == "WRONG" else 10.0

    def _fetch_chain(ticker, direction, *, expiration_override=None, request_context=None):
        request_context.playbook_spec = resolve_contract_playbook(
            ticker=ticker,
            side=direction,
            timeframe="1d",
            pattern="2-1-2",
            underlying_price=100.0,
            trigger_price=100.0,
            target_underlying=110.0,
            wick_targets=[],
            available_expirations=[expiration],
            today=date(2026, 7, 12),
            min_dte=0,
            max_dte=21,
            metadata={},
            now_et=datetime(2026, 7, 12, 10, 0, tzinfo=ET),
        )
        request_context.playbook_today_et = date(2026, 7, 12)
        monkeypatch.setenv("PLAYBOOK_CONTRACT_SELECTION_ENABLED", "0")
        return chain, 100.0

    engine._fetch_chain_with_price = _fetch_chain
    plan = _plan(metadata={})
    selected = engine.select(plan, expiration_override=expiration, request_context=None)
    assert selected is not None
    assert selected.contract_symbol == "ATM"


def test_playbook_strike_ordering_changes_actual_selected_occ():
    mod = _load_selector_module()
    engine = _build_engine(mod)
    expiration = _today_plus(1)
    wrong = _option("WRONG", expiration, 104.0)
    atm = _option("ATM", expiration, 100.0)
    one_up = _option("ONEUP", expiration, 101.0)
    chain = [wrong, atm, one_up]
    engine._quality_filter = lambda opt, today, **kwargs: None
    scores = {"WRONG": 100.0, "ATM": 60.0, "ONEUP": 90.0}
    engine._rank_score = lambda opt, budget, **kwargs: scores[opt["symbol"]]
    engine._fetch_chain_with_price = lambda *args, **kwargs: (chain, 100.0)
    plan = _plan(metadata={})
    ctx = _playbook_context(mod, plan, [expiration], now_et=datetime(2026, 7, 12, 10, 0, tzinfo=ET), enabled=True)
    selected = engine.select(plan, expiration_override=expiration, request_context=ctx)
    assert selected is not None
    assert selected.contract_symbol == "ATM"
    assert plan.contract_symbol == "ATM"
    assert plan.limit_price == selected.execution_price_per_share
    assert plan.contracts == selected.affordable_contracts
    assert selected.candidate_audit["selected_contract"] == "ATM"
    assert selected.candidate_audit["selected_winner"]["symbol"] == "ATM"
    assert selected.candidate_audit["selected_winner"]["playbook_strike_policy_tier"] == STRIKE_POLICY_TIER_ATM
    assert selected.candidate_audit["selected_winner"]["playbook_strike_policy_label"] == STRIKE_POLICY_LABEL_ATM
    assert selected.candidate_audit["selected_winner"]["playbook_original_rank_score"] == 60.0


def test_one_step_otm_beats_higher_scoring_nonmatch_after_atm_fails():
    mod = _load_selector_module()
    engine = _build_engine(mod, mutate_plan=False)
    expiration = _today_plus(1)
    atm = _option("ATM", expiration, 100.0)
    one_up = _option("ONEUP", expiration, 101.0)
    nonmatch = _option("NONMATCH", expiration, 104.0)
    scores = {"ATM": 70.0, "ONEUP": 60.0, "NONMATCH": 95.0}
    engine._quality_filter = lambda opt, today, **kwargs: None
    engine._rank_score = lambda opt, budget, **kwargs: scores[opt["symbol"]]
    engine._fetch_chain_with_price = lambda *args, **kwargs: ([nonmatch, one_up, atm], 100.0)
    original_build_selected = engine._build_selected
    engine._build_selected = lambda opt, score, budget, today: (
        None if opt["symbol"] == "ATM" else original_build_selected(opt, score, budget, today)
    )
    plan = _plan(metadata={})
    ctx = _playbook_context(mod, plan, [expiration], now_et=datetime(2026, 7, 12, 10, 0, tzinfo=ET), enabled=True)
    selected = engine.select(plan, expiration_override=expiration, request_context=ctx)
    assert selected is not None
    assert selected.contract_symbol == "ONEUP"
    assert selected.candidate_audit["selected_winner"]["playbook_strike_policy_tier"] == STRIKE_POLICY_TIER_ONE_STEP_OTM
    assert selected.candidate_audit["final_candidate_rejections"][0]["symbol"] == "ATM"


def test_feature_disabled_preserves_original_selector_ranking():
    mod = _load_selector_module()
    engine = _build_engine(mod, mutate_plan=False)
    expiration = _today_plus(1)
    wrong = _option("WRONG", expiration, 110.0)
    atm = _option("ATM", expiration, 100.0)
    engine._quality_filter = lambda opt, today, **kwargs: None
    engine._rank_score = lambda opt, budget, **kwargs: 100.0 if opt["symbol"] == "WRONG" else 10.0
    engine._fetch_chain_with_price = MagicMock(return_value=([wrong, atm], 100.0))
    plan = _plan(metadata={})
    original_plan_fields = (plan.contract_symbol if hasattr(plan, "contract_symbol") else None, plan.contracts)
    ctx = _playbook_context(mod, plan, [expiration], now_et=datetime(2026, 7, 12, 10, 0, tzinfo=ET), enabled=False)
    selected = engine.select(plan, expiration_override=expiration, request_context=ctx)
    assert selected is not None
    assert selected.contract_symbol == "WRONG"
    assert engine._fetch_chain_with_price.call_count == 1
    assert (plan.contract_symbol if hasattr(plan, "contract_symbol") else None, plan.contracts) == original_plan_fields


def test_preferred_strikes_fail_then_nonmatches_preserve_original_ranking():
    mod = _load_selector_module()
    engine = _build_engine(mod, mutate_plan=False)
    expiration = _today_plus(1)
    atm = _option("ATM", expiration, 100.0)
    one_up = _option("ONEUP", expiration, 101.0)
    far1 = _option("FAR1", expiration, 104.0)
    far2 = _option("FAR2", expiration, 106.0)
    engine._quality_filter = lambda opt, today, **kwargs: None
    scores = {"ATM": 70.0, "ONEUP": 65.0, "FAR1": 40.0, "FAR2": 80.0}
    engine._rank_score = lambda opt, budget, **kwargs: scores[opt["symbol"]]
    engine._fetch_chain_with_price = lambda *args, **kwargs: ([far2, far1, one_up, atm], 100.0)
    original_build_selected = engine._build_selected
    engine._build_selected = lambda opt, score, budget, today: (
        None if opt["symbol"] in {"ATM", "ONEUP"} else original_build_selected(opt, score, budget, today)
    )
    plan = _plan(metadata={})
    ctx = _playbook_context(mod, plan, [expiration], now_et=datetime(2026, 7, 12, 10, 0, tzinfo=ET), enabled=True)
    selected = engine.select(plan, expiration_override=expiration, request_context=ctx)
    assert selected is not None
    assert selected.contract_symbol == "FAR2"
    assert selected.candidate_audit["selected_winner"]["playbook_strike_policy_tier"] == STRIKE_POLICY_TIER_NONMATCH
    assert selected.candidate_audit["selected_winner"]["playbook_original_rank_score"] == 80.0
    rejected = selected.candidate_audit["final_candidate_rejections"]
    assert [row["symbol"] for row in rejected[:2]] == ["ATM", "ONEUP"]


@pytest.mark.parametrize(
    ("strike", "expected_tier"),
    [(100.0, STRIKE_POLICY_TIER_ATM), (101.0, STRIKE_POLICY_TIER_ONE_STEP_OTM)],
)
def test_original_score_orders_duplicate_candidates_inside_preferred_tier(strike, expected_tier):
    mod = _load_selector_module()
    engine = _build_engine(mod, mutate_plan=False)
    expiration = _today_plus(1)
    baseline = _option("BASELINE", expiration, 100.0)
    low = _option("LOW", expiration, strike)
    high = _option("HIGH", expiration, strike)
    chain = [low, high] if strike == 100.0 else [baseline, low, high]
    engine._quality_filter = lambda opt, today, **kwargs: None
    engine._rank_score = lambda opt, budget, **kwargs: {"BASELINE": 10.0, "LOW": 60.0, "HIGH": 80.0}[opt["symbol"]]
    engine._fetch_chain_with_price = lambda *args, **kwargs: (chain, 100.0)
    if strike == 101.0:
        original_build_selected = engine._build_selected
        engine._build_selected = lambda opt, score, budget, today: (
            None if opt["symbol"] == "BASELINE" else original_build_selected(opt, score, budget, today)
        )
    plan = _plan(metadata={})
    ctx = _playbook_context(mod, plan, [expiration], now_et=datetime(2026, 7, 12, 10, 0, tzinfo=ET), enabled=True)
    selected = engine.select(plan, expiration_override=expiration, request_context=ctx)
    assert selected is not None
    assert selected.contract_symbol == "HIGH"
    assert selected.candidate_audit["selected_winner"]["playbook_strike_policy_tier"] == expected_tier


def test_playbook_audit_records_selected_strike_tier_and_original_score():
    mod = _load_selector_module()
    engine = _build_engine(mod, mutate_plan=False)
    expiration = _today_plus(1)
    atm = _option("ATM", expiration, 100.0)
    one_up = _option("ONEUP", expiration, 101.0)
    engine._quality_filter = lambda opt, today, **kwargs: None
    engine._rank_score = lambda opt, budget, **kwargs: 90.0 if opt["symbol"] == "ONEUP" else 60.0
    engine._fetch_chain_with_price = lambda *args, **kwargs: ([one_up, atm], 100.0)
    plan = _plan(metadata={})
    ctx = _playbook_context(mod, plan, [expiration], now_et=datetime(2026, 7, 12, 10, 0, tzinfo=ET), enabled=True)
    ctx.playbook_audit = {}
    selected = engine.select(plan, expiration_override=expiration, request_context=ctx)
    assert selected is not None
    assert ctx.playbook_audit["selected_strike_policy_tier"] == STRIKE_POLICY_TIER_ATM
    assert ctx.playbook_audit["selected_strike_policy_label"] == STRIKE_POLICY_LABEL_ATM
    assert ctx.playbook_audit["selected_original_rank_score"] == 60.0
    assert ctx.playbook_audit["atm_strike"] == 100.0
    assert ctx.playbook_audit["one_step_otm_strike"] == 101.0


def test_rank_one_final_gate_failure_falls_through_to_rank_two():
    mod = _load_selector_module()
    engine = _build_engine(mod)
    expiration = _today_plus(1)
    atm = _option("ATM", expiration, 100.0, bid=2.0, ask=2.1)
    one_up = _option("ONEUP", expiration, 101.0, bid=0.95, ask=1.0)
    engine._quality_filter = lambda opt, today, **kwargs: None
    engine._rank_score = lambda opt, budget, **kwargs: 60.0 if opt["symbol"] == "ONEUP" else 40.0
    engine._fetch_chain_with_price = lambda *args, **kwargs: ([one_up, atm], 100.0)
    selected_by_symbol = {
        "ATM": mod.SelectedContract(
            contract_symbol="ATM",
            expiration=expiration,
            strike=100.0,
            option_type="CALL",
            bid=2.0,
            ask=2.1,
            mid=2.05,
            spread_pct=0.0488,
            delta=0.4,
            open_interest=1000,
            volume=500,
            premium_per_share=2.1,
            premium_per_contract=210.0,
            affordable_contracts=0,
            selection_reason="ranked",
            selection_score=40.0,
            dte=1,
            scoring_price_per_share=2.05,
            execution_price_per_share=2.1,
            effective_budget=150.0,
            budget_clipped=False,
            pricing_basis="ask",
        ),
        "ONEUP": mod.SelectedContract(
            contract_symbol="ONEUP",
            expiration=expiration,
            strike=101.0,
            option_type="CALL",
            bid=0.95,
            ask=1.0,
            mid=0.975,
            spread_pct=0.0513,
            delta=0.4,
            open_interest=1000,
            volume=500,
            premium_per_share=1.0,
            premium_per_contract=100.0,
            affordable_contracts=1,
            selection_reason="ranked",
            selection_score=60.0,
            dte=1,
            scoring_price_per_share=0.95,
            execution_price_per_share=1.0,
            effective_budget=150.0,
            budget_clipped=False,
            pricing_basis="ask",
        ),
    }
    engine._build_selected = lambda opt, score, budget, today: selected_by_symbol[opt["symbol"]]
    plan = _plan(metadata={}, budget=150.0)
    ctx = _playbook_context(mod, plan, [expiration], now_et=datetime(2026, 7, 12, 10, 0, tzinfo=ET), enabled=True)
    selected = engine.select(plan, expiration_override=expiration, request_context=ctx)
    assert selected is not None
    assert selected.contract_symbol == "ONEUP"
    assert selected.candidate_audit["selected_contract"] == "ONEUP"
    assert selected.candidate_audit["selected_winner"]["symbol"] == "ONEUP"


def test_daily_vs_weekly_plan_changes_real_probe_order(monkeypatch):
    mod = _load_selector_module()
    engine = object.__new__(mod.APContractSelectionEngine)
    engine.dte_ladder_enabled = True
    engine.dte_ladder_probe_per_bucket = 2
    engine.mode = "live"
    engine.data_broker = MagicMock()
    engine.min_dte = 0
    engine.max_dte = 21
    expirations = [_today_plus(0), _today_plus(1), _today_plus(2), _today_plus(7), _today_plus(14)]
    engine._fetch_underlying_quote = MagicMock(return_value=500.0)
    engine._fetch_expirations_list = MagicMock(return_value=(expirations, {}))
    engine._playbook_now_et = MagicMock(return_value=datetime(2026, 7, 12, 10, 0, tzinfo=ET))
    daily_plan = _plan(timeframe="1d", metadata={"deferred_breach_selection": True})
    weekly_plan = _plan(timeframe="1w", metadata={"deferred_breach_selection": True})

    def _collect(plan):
        probed: list[str] = []

        def _fake_select(sub_plan, *, expiration_override=None, request_context=None, _dte_legacy_fallback=False):
            if expiration_override is not None:
                probed.append(expiration_override)
                sub_plan.metadata["selector_failure"] = {"reason_code": "SPREAD_TOO_WIDE", "explanation": expiration_override}
            return None

        engine.select = _fake_select
        monkeypatch.setenv("PLAYBOOK_CONTRACT_SELECTION_ENABLED", "1")
        engine._select_with_dte_ladder(plan)
        return probed

    daily_probed = _collect(daily_plan)
    weekly_probed = _collect(weekly_plan)
    assert daily_probed[:2] == expirations[1:3]
    assert weekly_probed[0] == expirations[3]
    assert weekly_probed != daily_probed


def test_valid_expiration_override_reaches_real_ladder_first(monkeypatch):
    mod = _load_selector_module()
    engine = object.__new__(mod.APContractSelectionEngine)
    engine.dte_ladder_enabled = True
    engine.dte_ladder_probe_per_bucket = 2
    engine.mode = "live"
    engine.data_broker = MagicMock()
    engine.min_dte = 0
    engine.max_dte = 21
    exp_1 = _today_plus(1)
    exp_7 = _today_plus(7)
    engine._fetch_underlying_quote = MagicMock(return_value=500.0)
    engine._fetch_expirations_list = MagicMock(return_value=([exp_1, exp_7], {}))
    engine._playbook_now_et = MagicMock(return_value=datetime(2026, 7, 12, 10, 0, tzinfo=ET))
    plan = _plan(timeframe="1w", expiration_override=exp_7, metadata={"deferred_breach_selection": True})
    probed: list[str] = []

    def _fake_select(sub_plan, *, expiration_override=None, request_context=None, _dte_legacy_fallback=False):
        if expiration_override is not None:
            probed.append(expiration_override)
            sub_plan.metadata["selector_failure"] = {"reason_code": "SPREAD_TOO_WIDE", "explanation": expiration_override}
        return None

    engine.select = _fake_select
    monkeypatch.setenv("PLAYBOOK_CONTRACT_SELECTION_ENABLED", "1")
    engine._select_with_dte_ladder(plan)
    assert probed[0] == exp_7


@pytest.mark.parametrize(
    ("override", "expected_reason"),
    [
        ("bad-date", "INVALID_EXPIRATION_OVERRIDE"),
        ("2099-01-01", "EXPIRATION_OVERRIDE_UNAVAILABLE"),
    ],
)
def test_invalid_override_causes_exact_failure_with_zero_unintended_probe(monkeypatch, override, expected_reason):
    mod = _load_selector_module()
    engine = object.__new__(mod.APContractSelectionEngine)
    engine.dte_ladder_enabled = True
    engine.dte_ladder_probe_per_bucket = 2
    engine.mode = "live"
    engine.data_broker = MagicMock()
    engine.min_dte = 0
    engine.max_dte = 21
    exp_1 = _today_plus(1)
    exp_7 = _today_plus(7)
    engine._fetch_underlying_quote = MagicMock(return_value=500.0)
    engine._fetch_expirations_list = MagicMock(return_value=([exp_1, exp_7], {}))
    engine._playbook_now_et = MagicMock(return_value=datetime(2026, 7, 12, 10, 0, tzinfo=ET))
    plan = _plan(timeframe="1w", expiration_override=override, metadata={"deferred_breach_selection": True})
    probed: list[str] = []

    def _fake_select(sub_plan, *, expiration_override=None, request_context=None, _dte_legacy_fallback=False):
        if expiration_override is not None:
            probed.append(expiration_override)
        return None

    engine.select = _fake_select
    monkeypatch.setenv("PLAYBOOK_CONTRACT_SELECTION_ENABLED", "1")
    result = engine._select_with_dte_ladder(plan)
    assert result is None
    assert probed == []
    assert engine._last_failure["reason_code"] == expected_reason


def test_after_cutoff_0dte_is_never_probed_even_if_later_expirations_fail(monkeypatch):
    mod = _load_selector_module()
    engine = object.__new__(mod.APContractSelectionEngine)
    engine.dte_ladder_enabled = True
    engine.dte_ladder_probe_per_bucket = 2
    engine.mode = "live"
    engine.data_broker = MagicMock()
    engine.min_dte = 0
    engine.max_dte = 21
    today = date.today()
    exp_0 = today.isoformat()
    exp_1 = _today_plus(1, today=today)
    exp_3 = _today_plus(3, today=today)
    engine._fetch_underlying_quote = MagicMock(return_value=500.0)
    engine._fetch_expirations_list = MagicMock(return_value=([exp_0, exp_1, exp_3], {}))
    engine._playbook_now_et = MagicMock(return_value=datetime.combine(today, datetime.min.time(), tzinfo=ET).replace(hour=14, minute=0))
    plan = _plan(timeframe="5m", metadata={"deferred_breach_selection": True})
    probed: list[str] = []

    def _fake_select(sub_plan, *, expiration_override=None, request_context=None, _dte_legacy_fallback=False):
        if expiration_override is not None:
            probed.append(expiration_override)
            sub_plan.metadata["selector_failure"] = {"reason_code": "SPREAD_TOO_WIDE", "explanation": expiration_override}
        return None

    engine.select = _fake_select
    monkeypatch.setenv("PLAYBOOK_CONTRACT_SELECTION_ENABLED", "1")
    result = engine._select_with_dte_ladder(plan)
    assert result is None
    assert probed == [exp_1, exp_3]
    assert exp_0 not in probed
