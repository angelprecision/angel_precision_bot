from __future__ import annotations

import math
from pathlib import Path

import pytest

from ap.expected_move import (
    AtmIvResult,
    ExpectedMoveResult,
    atm_iv_from_chain,
    expected_move_1d,
    expected_move_to,
    feasibility_ratio,
)

ROOT = Path(__file__).resolve().parents[1]


def _chain():
    return [
        {"option_type": "call", "strike": 99, "bid": 2.1, "ask": 2.3, "greeks": {"mid_iv": 0.30}},
        {"option_type": "put", "strike": 99, "bid": 1.9, "ask": 2.1, "greeks": {"mid_iv": 0.32}},
        {"option_type": "call", "strike": 105, "bid": 0.8, "ask": 1.0, "greeks": {"mid_iv": 0.40}},
        {"option_type": "put", "strike": 105, "bid": 0.7, "ask": 0.9, "greeks": {"mid_iv": 0.42}},
    ]


def test_expected_move_1d_golden_hand_calculated():
    result = expected_move_1d(100, 0.32)

    assert result.reason == "ok"
    assert result.value == pytest.approx(100 * 0.32 * math.sqrt(1 / 252))


def test_expected_move_1d_accepts_percent_iv():
    result = expected_move_1d(100, 32)

    assert result == ExpectedMoveResult(pytest.approx(100 * 0.32 * math.sqrt(1 / 252)), "ok")


def test_expected_move_to_golden_hand_calculated():
    result = expected_move_to(100, 0.32, 13, trading_hours_per_day=6.5)

    assert result.reason == "ok"
    assert result.value == pytest.approx(100 * 0.32 * math.sqrt(2 / 252))


def test_missing_iv_returns_unavailable_reason():
    assert expected_move_1d(100, None) == ExpectedMoveResult(None, "invalid_atm_iv")
    assert expected_move_to(100, None, 2) == ExpectedMoveResult(None, "invalid_atm_iv")


def test_zero_iv_returns_unavailable_reason():
    assert expected_move_1d(100, 0) == ExpectedMoveResult(None, "invalid_atm_iv")
    assert expected_move_to(100, -0.1, 2) == ExpectedMoveResult(None, "invalid_atm_iv")


@pytest.mark.parametrize(
    ("price", "reason"),
    [
        (None, "invalid_underlying_price"),
        (0, "invalid_underlying_price"),
        (-10, "invalid_underlying_price"),
        ("bad", "invalid_underlying_price"),
    ],
)
def test_invalid_underlying_price_is_structured(price, reason):
    assert expected_move_1d(price, 0.3) == ExpectedMoveResult(None, reason)
    assert expected_move_to(price, 0.3, 1) == ExpectedMoveResult(None, reason)
    assert atm_iv_from_chain(_chain(), price) == AtmIvResult(None, "unavailable", reason)


@pytest.mark.parametrize(
    ("hours", "reason"),
    [
        (None, "invalid_hours_ahead"),
        (0, "invalid_hours_ahead"),
        (-1, "invalid_hours_ahead"),
        ("bad", "invalid_hours_ahead"),
    ],
)
def test_expected_move_to_rejects_bad_hours(hours, reason):
    assert expected_move_to(100, 0.3, hours) == ExpectedMoveResult(None, reason)


def test_expected_move_to_rejects_bad_trading_hours_per_day():
    assert expected_move_to(100, 0.3, 1, trading_hours_per_day=0) == ExpectedMoveResult(
        None,
        "invalid_trading_hours_per_day",
    )


def test_atm_iv_from_chain_averages_nearest_call_and_put():
    result = atm_iv_from_chain(_chain(), 100)

    assert result == AtmIvResult(0.31, "ok", "ok")


def test_atm_iv_from_chain_accepts_percent_iv():
    result = atm_iv_from_chain(
        [
            {"option_type": "call", "strike": 100, "bid": 1, "ask": 1.2, "greeks": {"mid_iv": 31}},
            {"option_type": "put", "strike": 100, "bid": 1, "ask": 1.2, "greeks": {"mid_iv": 33}},
        ],
        100,
    )

    assert result == AtmIvResult(0.32, "ok", "ok")


def test_atm_iv_missing_chain_legs_returns_unavailable():
    assert atm_iv_from_chain([], 100) == AtmIvResult(None, "unavailable", "missing_chain")
    assert atm_iv_from_chain([{"strike": None}], 100) == AtmIvResult(
        None,
        "unavailable",
        "malformed_chain",
    )


def test_atm_iv_single_leg_fallback():
    result = atm_iv_from_chain(
        [{"option_type": "call", "strike": 100, "bid": 1, "ask": 1.2, "greeks": {"mid_iv": 0.31}}],
        100,
    )

    assert result == AtmIvResult(0.31, "single_leg", "missing_put")


def test_atm_iv_stale_quality():
    result = atm_iv_from_chain(
        [
            {"option_type": "call", "strike": 100, "greeks": {"mid_iv": 0.31}},
            {"option_type": "put", "strike": 100, "greeks": {"mid_iv": 0.33}},
        ],
        100,
    )

    assert result == AtmIvResult(None, "stale", "stale_chain")


def test_atm_iv_missing_iv_on_live_leg_is_unavailable():
    result = atm_iv_from_chain(
        [
            {"option_type": "call", "strike": 100, "bid": 1, "ask": 1.2},
            {"option_type": "put", "strike": 100, "bid": 1, "ask": 1.2},
        ],
        100,
    )

    assert result == AtmIvResult(None, "unavailable", "missing_iv")


def test_feasibility_ratio():
    result = feasibility_ratio(100, 106, 3)

    assert result == ExpectedMoveResult(2.0, "ok")


@pytest.mark.parametrize(
    ("entry", "target", "move", "reason"),
    [
        (None, 105, 2, "invalid_entry"),
        (0, 105, 2, "invalid_entry"),
        (100, None, 2, "invalid_target"),
        (100, 0, 2, "invalid_target"),
        (100, 105, None, "invalid_expected_move"),
        (100, 105, 0, "invalid_expected_move"),
    ],
)
def test_feasibility_ratio_bad_inputs(entry, target, move, reason):
    assert feasibility_ratio(entry, target, move) == ExpectedMoveResult(None, reason)


@pytest.mark.parametrize(
    "fn,args",
    [
        (expected_move_1d, (object(), object())),
        (expected_move_to, (object(), object(), object(), object())),
        (atm_iv_from_chain, ([object(), {"option_type": object(), "strike": object()}], object())),
        (feasibility_ratio, (object(), object(), object())),
    ],
)
def test_no_function_raises_on_malformed_inputs(fn, args):
    result = fn(*args)

    assert result.value is None
    assert result.reason


def test_expected_move_module_imported_by_nothing_else_in_this_pr():
    offenders = []
    for path in ROOT.rglob("*.py"):
        if path.relative_to(ROOT).parts[0] == "tests":
            continue
        if path == ROOT / "ap" / "expected_move.py":
            continue
        text = path.read_text(encoding="utf-8")
        if "ap.expected_move" in text or "from expected_move" in text:
            offenders.append(str(path.relative_to(ROOT)))

    assert offenders == []
