"""September LIVE cohort first-breach ABCD fixtures for PR #435.

Loads durable/UNKNOWN cohort envelopes from
``tests/fixtures/september_live_cohort_202609.json`` and exercises tip helpers:

- resolve_canonical_strategy_pattern
- resolve_breach_lineage
- _build_breach_evidence / classify_entry_readiness_observe_only
- freeze_breach_market_structure

ABCD mapping is generic (no ticker branches in the classifier path).
"""

from __future__ import annotations

import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ap.intelligence_breach_market_structure import (  # noqa: E402
    freeze_breach_market_structure,
)
from ap.intelligence_context_materializer import (  # noqa: E402
    _build_breach_evidence,
    classify_entry_readiness_observe_only,
    resolve_breach_lineage,
    resolve_canonical_strategy_pattern,
)

FIXTURE_PATH = _ROOT / "tests" / "fixtures" / "september_live_cohort_202609.json"

# Opposing-FVG relationships that require acceptance / wall handling (class C).
# Generic enums from market_structure — never keyed by ticker.
_OPPOSING_FVG_ACCEPTANCE_RELATIONSHIPS = frozenset(
    {
        "INSIDE_OPPOSING_FVG",
        "INSIDE_OPPOSING_ZONE",
        "AT_OPPOSING_FRONT",
        "APPROACHING_OPPOSING_FVG",
        "OPPOSING_WALL_ACCEPTED",
        "OPPOSING_WALL_REJECTED",
    }
)

_SCORING_FORBIDDEN = (
    "option_bid",
    "option_ask",
    "fill_price",
    "realized_pnl",
    "outcome_label",
    "fabricated_quote",
)


def _load_cohort() -> dict:
    return json.loads(FIXTURE_PATH.read_text())


def _active_cases() -> list[dict]:
    data = _load_cohort()
    return [
        c
        for c in data["cases"]
        if c.get("inclusion") != "deferred_no_durable_evidence" and "signal" in c
    ]


def map_abcd_observe_only(
    *,
    readiness: dict,
    market_structure: dict | None,
) -> str:
    """Map tip readiness + market_structure into ABCD research classes.

    Generic only — no ticker / session hardcoding.
    """
    classification = str((readiness or {}).get("classification") or "")
    rel = str(
        ((market_structure or {}).get("relationship") or {}).get("relationship")
        or "CLEAR_PATH"
    )

    if classification == "INVALID":
        return "D"
    if rel in _OPPOSING_FVG_ACCEPTANCE_RELATIONSHIPS:
        return "C"
    if classification in {"WAIT_CONFIRMATION", "REBREACH_PREFERRED"}:
        return "B"
    if classification == "READY_NOW":
        return "A"
    return "B"


def _run_first_breach(case: dict) -> dict:
    signal = deepcopy(case["signal"])
    candles = (signal.get("candles") or {}) if isinstance(signal.get("candles"), dict) else {}
    market_context = deepcopy(case.get("market_context") or {"candles": {}})

    evidence = _build_breach_evidence(
        signal,
        data_sources={"candles": candles},
        provenance={
            "fifteen_minute": "frozen_cohort_15m",
            "five_minute": "frozen_cohort_5min",
        },
        observation={
            "price": signal.get("underlying_price") or signal.get("breach_price"),
            "source": "september_live_cohort_fixture",
            "observed_at": signal.get("trigger_crossed_at") or signal.get("data_as_of"),
            "status": "AVAILABLE",
        },
    )

    # Optional research overrides (e.g. documented heavy extension) — never option quotes.
    overrides = case.get("evidence_overrides") or {}
    if overrides:
        for key, value in overrides.items():
            if key in {"option_bid", "option_ask", "fill_price", "outcome", "mfe", "mae"}:
                raise AssertionError(f"forbidden scoring override: {key}")
            evidence[key] = deepcopy(value)

    readiness = classify_entry_readiness_observe_only(signal, evidence=evidence)
    # Prefer evidence-embedded readiness when present; must agree on authority flags.
    embedded = evidence.get("entry_readiness_observe_only") or {}
    if embedded:
        assert embedded.get("observe_only") is True
        assert embedded.get("affected_eligibility") is False

    ms = freeze_breach_market_structure(
        signal,
        market_context=market_context,
        data_as_of=signal.get("data_as_of") or signal.get("trigger_crossed_at"),
    )

    abcd = map_abcd_observe_only(readiness=readiness, market_structure=ms)
    return {
        "signal": signal,
        "evidence": evidence,
        "readiness": readiness,
        "market_structure": ms,
        "abcd": abcd,
        "pattern": resolve_canonical_strategy_pattern(signal),
        "lineage": resolve_breach_lineage(signal),
    }


@pytest.mark.parametrize("case", _active_cases(), ids=lambda c: c["case_id"])
def test_september_cohort_abcd_at_first_breach(case: dict):
    result = _run_first_breach(case)

    assert result["readiness"]["observe_only"] is True
    assert result["readiness"]["affected_eligibility"] is False
    assert result["market_structure"].get("observe_only") is True
    assert result["market_structure"].get("affected_eligibility") is False

    assert result["lineage"]["breach_lineage"] in {
        "INITIAL_BREACH",
        "REBREACH_AFTER_RESET",
        "UNKNOWN",
        "DIRECTION_REVERSAL_REBREACH",
        "RECOVERED_BREACH",
    }

    expected = case["expected_abcd"]
    assert result["abcd"] == expected, (
        f"{case['case_id']}: expected {expected}, got {result['abcd']} "
        f"(readiness={result['readiness']['classification']}, "
        f"fvg_rel={((result['market_structure'] or {}).get('relationship') or {}).get('relationship')})"
    )

    # No fabricated option quotes / outcomes in scoring-facing blobs.
    blob = json.dumps(
        {
            "signal": {
                k: v
                for k, v in result["signal"].items()
                if k != "research_only_outcome_sidecar"
            },
            "evidence": result["evidence"],
            "readiness": result["readiness"],
            "market_structure": result["market_structure"],
        },
        default=str,
    ).lower()
    for needle in _SCORING_FORBIDDEN:
        assert needle not in blob


def test_deferred_positive_controls_remain_unknown():
    data = _load_cohort()
    deferred = [
        c for c in data["cases"] if c.get("inclusion") == "deferred_no_durable_evidence"
    ]
    assert {c["ticker"] for c in deferred} >= {"GOOGL", "C"}
    for case in deferred:
        assert case["expected_abcd"] == "UNKNOWN"
        assert case["evidence_status"]["signal_id"] == "UNKNOWN"
        assert "signal" not in case


def test_aapl_durable_signal_id_preserved():
    data = _load_cohort()
    aapl = next(c for c in data["cases"] if c["case_id"] == "aapl_put_opening_20260812")
    assert aapl["signal"]["signal_id"] == "83503891-b746-4c79-a39c-19e8cd8d4cd8"
    assert aapl["signal"]["client_id"] == "jasoncosby1@gmail.com"
    identity = resolve_canonical_strategy_pattern(aapl["signal"])
    assert identity["canonical_strategy_pattern"] == "2-3-2"
    assert identity["status"] == "RESOLVED"

    result = _run_first_breach(aapl)
    assert result["abcd"] == "B"
    assert result["readiness"]["classification"] in {
        "WAIT_CONFIRMATION",
        "REBREACH_PREFERRED",
    }


def test_required_amendment_cases_present():
    data = _load_cohort()
    required = {
        (c["ticker"], c["side"], c["session_date"])
        for c in data["cases"]
        if c.get("cohort_role") == "required_amendment"
    }
    assert ("QQQ", "PUT", "2026-09-09") in required
    assert ("HOOD", "PUT", "2026-09-09") in required
    assert ("LULU", "CALL", "2026-09-08") in required
    assert ("NKE", "CALL", "2026-09-08") in required


def test_mapper_has_no_ticker_hardcoding():
    import inspect

    src = inspect.getsource(map_abcd_observe_only)
    for ticker in ("QQQ", "HOOD", "LULU", "NKE", "AAPL", "IWM", "GOOGL"):
        assert ticker not in src
