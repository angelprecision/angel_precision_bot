"""September LIVE cohort + AAPL opening — observe-only first-breach families.

Durable IDs marked UNKNOWN in the fixture JSON are intentional honesty gates:
tests skip full identity assertions until production DB fills them. Classification
uses generic evidence only — no ticker hardcoding in the classifier.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ap.intelligence_context_materializer import (
    classify_entry_readiness_observe_only,
    resolve_breach_lineage,
    resolve_canonical_strategy_pattern,
)

FIXTURES = json.loads(
    (Path(__file__).resolve().parent / "fixtures" / "september_live_cohort_202609.json").read_text()
)


def _family(classification: str, *, opposing: bool | None) -> str:
    if classification == "INVALID":
        return "D"
    if classification == "READY_NOW":
        return "A"
    if opposing:
        return "C"
    return "B"


@pytest.mark.parametrize("case", FIXTURES["cases"], ids=lambda c: c["case_id"])
def test_cohort_case_observe_only_family(case):
    shape = case["breach_shape"]
    signal = {
        "ticker": case["ticker"],
        "side": case["side"],
        "pattern": shape.get("pattern"),
        "timeframe": shape.get("timeframe"),
        "breach_count": shape.get("breach_count"),
        "trigger_price": shape.get("trigger_price") or 100.0,
        "stop_price": shape.get("stop_price") or 102.0,
        "target_price": shape.get("target_price") or 96.0,
        "underlying_price": shape.get("underlying_price") or 99.5,
    }
    if shape.get("pattern") not in (None, "", "UNKNOWN"):
        identity = resolve_canonical_strategy_pattern(signal)
        if case["ticker"] == "AAPL":
            assert identity["canonical_strategy_pattern"] == "2-3-2"

    lineage = resolve_breach_lineage(signal)
    if shape.get("breach_count") == 1 and not signal.get("breach_reset"):
        assert lineage["breach_lineage"] in {"INITIAL_BREACH", "UNKNOWN"}

    fifteen_status = "AVAILABLE" if shape.get("fifteen_follow_through") else "MISSING"
    evidence = {
        "breach_lineage": lineage["breach_lineage"],
        "remaining_opportunity": {
            "remaining_r": shape.get("remaining_r"),
            "percent_move_consumed": shape.get("percent_move_consumed"),
            "target_reached": False,
        },
        "fifteen_minute_confirmation": {
            "status": fifteen_status,
            "follow_through": bool(shape.get("fifteen_follow_through")),
        },
        "five_minute_confirmation": {"status": shape.get("five_status") or "MISSING"},
    }
    # Skip classification when geometry inputs are entirely UNKNOWN/absent.
    if shape.get("remaining_r") is None and shape.get("percent_move_consumed") is None:
        pytest.skip("durable breach geometry UNKNOWN — fill from production before claiming cohort proof")

    result = classify_entry_readiness_observe_only(signal, evidence=evidence)
    assert result["observe_only"] is True
    assert result["affected_eligibility"] is False
    family = _family(result["classification"], opposing=shape.get("opposing_4h_in_path"))
    expected = case.get("expected_family")
    if expected not in (None, "UNKNOWN"):
        assert family == expected, (case["case_id"], result["classification"], family)

    # Never allow fabricated option quotes into scoring inputs.
    durable = case["durable"]
    assert durable.get("option_bid_ask_at_breach") in (None, "UNKNOWN") or isinstance(
        durable.get("option_bid_ask_at_breach"), dict
    )


def test_fixture_marks_missing_durable_ids_explicitly():
    unknown_cases = [
        c for c in FIXTURES["cases"] if c["ticker"] in {"QQQ", "HOOD", "LULU", "NKE"}
    ]
    assert unknown_cases
    for case in unknown_cases:
        assert case["durable"]["signal_id"] == "UNKNOWN"
        assert case["durable"]["option_bid_ask_at_breach"] == "UNKNOWN"


def test_aapl_documented_identity_present():
    aapl = next(c for c in FIXTURES["cases"] if c["ticker"] == "AAPL")
    assert aapl["durable"]["signal_id"] == "83503891-b746-4c79-a39c-19e8cd8d4cd8"
    assert aapl["durable"]["client_id"] == "jasoncosby1@gmail.com"
