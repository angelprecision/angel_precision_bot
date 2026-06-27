from __future__ import annotations

import copy
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

from ap.market_context_builder import build_market_context_for_signal


def _base_signal() -> dict:
    return {
        "ticker": "WFC",
        "entry_price": 74.25,
        "stop_price": 73.10,
        "target_price": 76.50,
    }


def test_core_candles_are_passed_when_present():
    signal = _base_signal()
    data_sources = {
        "candles": {
            "monthly": [{"open": 70, "high": 77, "low": 68, "close": 74}],
            "weekly": [{"open": 72, "high": 75, "low": 71, "close": 74}],
            "daily": [{"open": 73, "high": 75, "low": 72, "close": 74}],
            "4h": [{"open": 73.5, "high": 74.8, "low": 73.2, "close": 74.2}],
        }
    }

    context = build_market_context_for_signal(signal, data_sources=data_sources)

    assert context["context_version"] == "market_context_v1"
    assert context["ticker"] == "WFC"
    assert context["timeframes_available"] == ["monthly", "weekly", "daily", "4h"]
    assert context["candles"]["monthly"] == data_sources["candles"]["monthly"]
    assert context["candles"]["weekly"] == data_sources["candles"]["weekly"]
    assert context["candles"]["daily"] == data_sources["candles"]["daily"]
    assert context["candles"]["4h"] == data_sources["candles"]["4h"]
    assert "candles.4h" not in context["missing_data"]
    assert context["diagnostics"]["observe_only"] is True


def test_missing_four_hour_candles_are_recorded_not_guessed():
    context = build_market_context_for_signal(
        _base_signal(),
        data_sources={
            "candles": {
                "monthly": [{"close": 74}],
                "weekly": [{"close": 74}],
                "daily": [{"close": 74}],
            }
        },
    )

    assert context["candles"]["4h"] == []
    assert "4h" not in context["timeframes_available"]
    assert "candles.4h" in context["missing_data"]


def test_scanner_levels_are_copied_into_levels():
    signal = {
        "symbol": "WFC",
        "trigger": {"entry": 74.25, "stop": 73.10, "pt1": 76.50},
    }

    context = build_market_context_for_signal(signal)

    assert context["levels"]["scanner_entry"] == 74.25
    assert context["levels"]["scanner_stop"] == 73.10
    assert context["levels"]["scanner_target"] == 76.50


def test_optional_multi_day_candles_are_passed_when_available():
    context = build_market_context_for_signal(
        _base_signal(),
        data_sources={"candles": {"2d": [{"close": 1}], "3d": [{"close": 2}], "4d": [{"close": 3}], "5d": [{"close": 4}]}}
    )

    assert context["candles"]["2d"] == [{"close": 1}]
    assert context["candles"]["3d"] == [{"close": 2}]
    assert context["candles"]["4d"] == [{"close": 3}]
    assert context["candles"]["5d"] == [{"close": 4}]
    assert "2d" in context["timeframes_available"]
    assert "candles.2d" not in context["missing_data"]


def test_context_is_json_safe():
    context = build_market_context_for_signal(
        {"ticker": "WFC", "entry_price": Decimal("74.25"), "stop_price": Decimal("73.10"), "target_price": Decimal("76.50")},
        data_sources={"candles": {"daily": [{"ts": datetime(2026, 6, 26, 12), "close": Decimal("74.25")}]}}
    )

    encoded = json.dumps(context, sort_keys=True)
    assert "2026-06-26T12:00:00" in encoded
    assert context["candles"]["daily"][0]["close"] == 74.25


def test_signal_is_not_mutated():
    signal = {
        "ticker": "WFC",
        "entry_price": 74.25,
        "stop_price": 73.10,
        "target_price": 76.50,
        "candles": {"daily": [{"close": 74.25}]},
    }
    before = copy.deepcopy(signal)

    context = build_market_context_for_signal(signal)
    context["candles"]["daily"][0]["close"] = 1

    assert signal == before


def test_no_broker_order_queue_or_proof_imports():
    src = Path("ap/market_context_builder.py").read_text()
    import_lines = [line for line in src.splitlines() if line.startswith("import ") or line.startswith("from ")]

    banned = ("broker", "tradier", "order", "queue", "position", "proof_trades", "supabase", "ap.db")
    assert not any(any(token in line for token in banned) for line in import_lines)
