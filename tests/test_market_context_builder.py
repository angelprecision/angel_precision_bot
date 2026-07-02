from __future__ import annotations

import copy
import json
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import ap.market_context_builder as market_context_builder
from ap.market_context_builder import build_market_context_for_signal


def _base_signal() -> dict:
    return {
        "ticker": "WFC",
        "entry_price": 74.25,
        "stop_price": 73.10,
        "target_price": 76.50,
    }


def _ohlc(close: float = 74.0) -> dict:
    return {"open": close - 1, "high": close + 1, "low": close - 2, "close": close}


def test_core_candles_are_passed_when_present():
    signal = _base_signal()
    data_sources = {
        "candles": {
            "monthly": [_ohlc(74)],
            "weekly": [_ohlc(74)],
            "daily": [_ohlc(74)],
            "4h": [_ohlc(74.2)],
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
    assert context["diagnostics"]["fake_data_used"] is False


def test_required_shape_has_passive_buckets():
    context = build_market_context_for_signal({"ticker": "WFC"})

    assert set(context) == {
        "context_version",
        "ticker",
        "timeframes_available",
        "candles",
        "levels",
        "trend",
        "volume",
        "sector",
        "news_earnings",
        "screenshot_context",
        "missing_data",
        "warnings",
        "diagnostics",
    }
    assert set(context["candles"]) == {"monthly", "weekly", "daily", "4h", "2d", "3d", "4d", "5d"}
    assert context["news_earnings"] == {
        "earnings_date": None,
        "earnings_risk": None,
        "news_risk": None,
        "catalyst": None,
    }
    assert context["screenshot_context"] is None
    assert isinstance(context["warnings"], list)
    assert context["diagnostics"]["fake_data_used"] is False


def test_missing_four_hour_candles_are_recorded_not_guessed():
    context = build_market_context_for_signal(
        _base_signal(),
        data_sources={
            "candles": {
                "monthly": [_ohlc(74)],
                "weekly": [_ohlc(74)],
                "daily": [_ohlc(74)],
            }
        },
    )

    assert context["candles"]["4h"] == []
    assert "4h" not in context["timeframes_available"]
    assert "candles.4h" in context["missing_data"]
    assert context["diagnostics"]["fake_data_used"] is False


def test_bad_ohlc_shape_records_warning():
    context = build_market_context_for_signal(
        _base_signal(),
        data_sources={"candles": {"4h": [{"close": 74.2}, {"open": "bad", "high": 1, "low": 0, "close": 1}]}}
    )

    assert "bad_ohlc_shape:candles.4h[0]" in context["warnings"]
    assert "bad_ohlc_value:candles.4h[1].open" in context["warnings"]


def test_scanner_levels_are_copied_from_trigger_or_signal():
    from_trigger = build_market_context_for_signal(
        {"symbol": "WFC", "trigger": {"entry": 74.25, "stop": 73.10, "pt1": 76.50}}
    )
    from_signal = build_market_context_for_signal(
        {"symbol": "WFC", "entry_price": 74.30, "stop_price": 73.00, "target_price": 76.75}
    )

    assert from_trigger["levels"]["scanner_entry"] == 74.25
    assert from_trigger["levels"]["scanner_stop"] == 73.10
    assert from_trigger["levels"]["scanner_target"] == 76.50
    assert from_signal["levels"]["scanner_entry"] == 74.30
    assert from_signal["levels"]["scanner_stop"] == 73.00
    assert from_signal["levels"]["scanner_target"] == 76.75


def test_optional_multi_day_candles_are_passed_when_available():
    context = build_market_context_for_signal(
        _base_signal(),
        data_sources={"candles": {"2d": [_ohlc(1)], "3d": [_ohlc(2)], "4d": [_ohlc(3)], "5d": [_ohlc(4)]}}
    )

    assert context["candles"]["2d"] == [_ohlc(1)]
    assert context["candles"]["3d"] == [_ohlc(2)]
    assert context["candles"]["4d"] == [_ohlc(3)]
    assert context["candles"]["5d"] == [_ohlc(4)]
    assert "2d" in context["timeframes_available"]
    assert "candles.2d" not in context["missing_data"]


def test_trend_volume_sector_are_copied():
    context = build_market_context_for_signal(
        _base_signal(),
        data_sources={
            "trend": {"vwap": 74.1, "ema_stack": "bullish", "price_above_vwap": True},
            "volume": {"relative_volume": 1.6, "volume_ratio": 1.4},
            "sector": {"sector": "financials", "sector_direction": "green", "sector_green": True, "sector_red": False},
        },
    )

    assert context["trend"] == {"vwap": 74.1, "ema_stack": "bullish", "price_above_vwap": True}
    assert context["volume"] == {"relative_volume": 1.6, "volume_ratio": 1.4}
    assert context["sector"] == {
        "sector": "financials",
        "sector_direction": "green",
        "sector_green": True,
        "sector_red": False,
    }
    assert "trend.vwap" not in context["missing_data"]
    assert "volume.relative_volume" not in context["missing_data"]
    assert "sector.sector" not in context["missing_data"]


def test_news_earnings_are_copied_into_passive_bucket():
    context = build_market_context_for_signal(
        _base_signal(),
        data_sources={"news_earnings": {"earnings_date": "2026-07-15", "earnings_risk": "low", "news_risk": "none", "catalyst": "bank earnings"}},
    )

    assert context["news_earnings"] == {
        "earnings_date": "2026-07-15",
        "earnings_risk": "low",
        "news_risk": "none",
        "catalyst": "bank earnings",
    }


def test_screenshot_context_passes_through_without_approving_or_blocking():
    screenshot = {"timeframe": "4h", "visual_bias": "bullish", "confidence": Decimal("0.72")}

    context = build_market_context_for_signal(_base_signal(), data_sources={"screenshot_context": screenshot})

    assert context["screenshot_context"] == {"timeframe": "4h", "visual_bias": "bullish", "confidence": 0.72}
    assert "approved" not in context
    assert "blocked" not in context
    assert context["diagnostics"]["observe_only"] is True


def test_external_data_quality_context_is_merged_when_available(monkeypatch):
    def fake_quality(context):
        assert context["context_version"] == "market_context_v1"
        return {
            "context_version": "data_quality_context_v1",
            "observe_only": True,
            "missing_data": ["external.missing"],
            "warnings": ["external.warning"],
            "bad_shape": ["external.bad_shape"],
            "stale_data": ["external.stale"],
        }

    monkeypatch.setattr(market_context_builder, "_DATA_QUALITY_EVALUATOR", fake_quality)

    context = build_market_context_for_signal(_base_signal())

    assert "external.missing" in context["missing_data"]
    assert "external.warning" in context["warnings"]
    assert "bad_shape:external.bad_shape" in context["warnings"]
    assert "stale_data:external.stale" in context["warnings"]
    assert context["diagnostics"]["data_quality_context"]["context_version"] == "data_quality_context_v1"


def test_context_is_json_safe():
    context = build_market_context_for_signal(
        {"ticker": "WFC", "entry_price": Decimal("74.25"), "stop_price": Decimal("73.10"), "target_price": Decimal("76.50")},
        data_sources={"candles": {"daily": [{"ts": datetime(2026, 6, 26, 12), "open": Decimal("74.00"), "high": Decimal("75.00"), "low": Decimal("73.50"), "close": Decimal("74.25")}]}}
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
        "candles": {"daily": [_ohlc(74.25)]},
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
