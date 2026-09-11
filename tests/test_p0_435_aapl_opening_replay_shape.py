"""Production-shaped AAPL LIVE PUT opening-window first-breach fixture.

Generic evidence shape only — no ticker-specific classifier branches,
no fabricated option quotes, no outcome data in scoring inputs.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ap.intelligence_context_materializer import (  # noqa: E402
    _breach_timing,
    _build_breach_evidence,
    classify_entry_readiness_observe_only,
    resolve_breach_lineage,
    resolve_canonical_strategy_pattern,
)


def _aapl_live_put_opening_first_breach_fixture() -> dict:
    """Generic LIVE PUT opening-window first-breach signal shape.

    Timing is first 30 minutes after RTH open. Pattern 2-3 / 1d → 2-3-2.
    Opposing / weak continuation on 5m/15m (bodies against PUT direction).
    """
    # 2026-09-09 is a Wednesday; 09:42 ET ≈ 12 minutes after RTH open.
    crossed = "2026-09-09T09:42:00-04:00"
    return {
        "ticker": "AAPL",
        "symbol": "AAPL",
        "side": "PUT",
        "direction": "PUT",
        "pattern": "2-3",
        "timeframe": "1d",
        "execution_mode": "LIVE",
        "client_id": "fixture-client@example.com",
        "signal_id": "sig-aapl-put-open-first-breach",
        "canonical_signal_id": "canon-aapl-put-open-first-breach",
        "local_order_id": "lo-aapl-put-001",
        "trigger_price": 230.0,
        "stop_price": 232.5,
        "target_price": 224.0,
        "underlying_price": 229.4,
        "current_price": 229.4,
        "breach_price": 229.4,
        "trigger_crossed_at": crossed,
        "trigger_confirmed_at": crossed,
        "breach_count": 1,
        "data_as_of": crossed,
        # Weak / opposing continuation: closes rising while side is PUT.
        "candles": {
            "15m": [
                {
                    "time": "2026-09-09T09:30:00-04:00",
                    "open": 229.0,
                    "high": 230.2,
                    "low": 228.8,
                    "close": 230.0,
                    "volume": 50000,
                },
                {
                    "time": "2026-09-09T09:45:00-04:00",
                    "open": 230.0,
                    "high": 230.8,
                    "low": 229.5,
                    "close": 230.5,
                    "volume": 48000,
                },
            ],
            "5m": [
                {
                    "time": "2026-09-09T09:30:00-04:00",
                    "open": 229.2,
                    "high": 229.8,
                    "low": 229.0,
                    "close": 229.7,
                    "volume": 12000,
                },
                {
                    "time": "2026-09-09T09:35:00-04:00",
                    "open": 229.7,
                    "high": 230.1,
                    "low": 229.5,
                    "close": 230.0,
                    "volume": 11000,
                },
                {
                    "time": "2026-09-09T09:40:00-04:00",
                    "open": 230.0,
                    "high": 230.3,
                    "low": 229.6,
                    "close": 230.1,
                    "volume": 10500,
                },
            ],
        },
        # Explicitly omit option quotes / outcomes from scoring inputs.
    }


def test_canonical_pattern_2_3_daily_maps_to_232():
    signal = _aapl_live_put_opening_first_breach_fixture()
    identity = resolve_canonical_strategy_pattern(signal)
    assert identity["raw_pattern"] == "2-3"
    assert identity["canonical_strategy_pattern"] == "2-3-2"
    assert identity["status"] == "RESOLVED"
    assert (identity.get("diagnostics") or {}).get("timeframe") in {"1d", "d", "daily", "1day"}


def test_opening_window_first_30_minutes_via_breach_timing():
    signal = _aapl_live_put_opening_first_breach_fixture()
    timing = _breach_timing(signal)
    assert timing["timestamp_status"] == "AVAILABLE"
    assert timing["opening_window"] == "FIRST_30_MINUTES"
    assert timing["minutes_since_rth_open"] is not None
    assert 0 <= float(timing["minutes_since_rth_open"]) < 30
    assert timing["exchange_timezone"] == "America/New_York"
    assert timing["rth_session_date"] == "2026-09-09"


def test_first_breach_lineage():
    signal = _aapl_live_put_opening_first_breach_fixture()
    lineage = resolve_breach_lineage(signal)
    assert lineage["breach_lineage"] == "INITIAL_BREACH"
    assert lineage["status"] in {"RESOLVED", "AUTHORITATIVE"}


def test_opposing_weak_continuation_wait_or_rebreach_preferred():
    signal = _aapl_live_put_opening_first_breach_fixture()
    evidence = _build_breach_evidence(
        signal,
        data_sources={"candles": signal["candles"]},
        provenance={
            "fifteen_minute": "frozen_signal_15m",
            "five_minute": "frozen_signal_5min",
        },
        observation={
            "price": signal["underlying_price"],
            "source": "frozen_breach_signal",
            "observed_at": signal["trigger_crossed_at"],
            "status": "AVAILABLE",
        },
    )
    assert evidence["breach_lineage"] == "INITIAL_BREACH"
    assert evidence["canonical_strategy_pattern"] == "2-3-2"

    fifteen = evidence["fifteen_minute_confirmation"]
    five = evidence["five_minute_confirmation"]
    # Opposing closes → follow_through should not be True for PUT.
    assert fifteen.get("follow_through") is not True
    assert five.get("follow_through") is not True

    readiness = evidence["entry_readiness_observe_only"]
    assert readiness["observe_only"] is True
    assert readiness["affected_eligibility"] is False
    assert readiness["classification"] in {
        "WAIT_CONFIRMATION",
        "REBREACH_PREFERRED",
    }

    # Direct classifier call on same evidence shape
    direct = classify_entry_readiness_observe_only(signal, evidence=evidence)
    assert direct["classification"] == readiness["classification"]


def test_no_fabricated_option_quotes_or_outcome_in_scoring_inputs():
    signal = _aapl_live_put_opening_first_breach_fixture()
    forbidden_signal_keys = {
        "option_quote",
        "option_quotes",
        "occ_quote",
        "fill_price",
        "realized_pnl",
        "outcome",
        "trade_outcome",
        "mfe",
        "mae",
        "selected_contract",
        "occ_symbol",
    }
    assert forbidden_signal_keys.isdisjoint(signal.keys())

    evidence = _build_breach_evidence(
        signal,
        data_sources={"candles": signal["candles"]},
        provenance={"fifteen_minute": "frozen_signal_15m", "five_minute": "frozen_signal_5min"},
        observation={
            "price": signal["underlying_price"],
            "source": "frozen_breach_signal",
            "status": "AVAILABLE",
        },
    )
    # Scoring / readiness path must not invent option or outcome fields.
    blob = str(evidence)
    for needle in (
        "option_bid",
        "option_ask",
        "fabricated_quote",
        "realized_pnl",
        "outcome_label",
    ):
        assert needle not in blob

    readiness = evidence["entry_readiness_observe_only"]
    assert "outcome" not in readiness
    assert readiness.get("affected_eligibility") is False
