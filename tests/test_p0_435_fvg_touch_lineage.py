"""Honesty gate: FVG touch lineage (#5/#6/#17) stays blocked without durable authority.

Do not invent touch_count / first_touch_ts / production history.
See /workspace/pr435/FVG_TOUCH_LINEAGE_BLOCKED.md.
"""

from __future__ import annotations

import ast
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ap.fair_value_gap import FairValueGap, detect_fair_value_gaps  # noqa: E402
from ap.intelligence_breach_market_structure import (  # noqa: E402
    CANDLE_BUCKET_ANCHOR_POLICY,
    SESSION_POLICY_4H_ATTESTATION,
    freeze_breach_market_structure,
    freeze_fvg_zone,
)
from ap.intelligence_context_materializer import (  # noqa: E402
    _build_breach_evidence,
    classify_entry_readiness_observe_only,
)

BLOCKED_DOC = _ROOT / "FVG_TOUCH_LINEAGE_BLOCKED.md"

# Amendment contract fields that require a durable touch ledger / export.
REQUIRED_DURABLE_TOUCH_FIELDS = frozenset(
    {
        "touch_count",
        "first_touch_ts",
        "last_touch_ts",
        "penetration_pct",
        "reclaim_state",
        "rejection_state",
        "touch_kind",
    }
)


def _candle(ts: str, o: float, h: float, l: float, c: float) -> dict:
    return {"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": 1000}


def _bullish_fvg_rows() -> list[dict]:
    # c1.high < c3.low => bullish gap [95, 97]
    return [
        _candle("2026-09-08T09:30:00-04:00", 94.0, 95.0, 93.5, 94.5),
        _candle("2026-09-08T13:30:00-04:00", 94.5, 96.0, 94.0, 95.5),
        _candle("2026-09-09T09:30:00-04:00", 97.0, 98.0, 97.0, 97.5),
    ]


# ---------------------------------------------------------------------------
# Honesty gates (pass = gap locked; fail if someone fabricates lineage)
# ---------------------------------------------------------------------------


def test_blocked_doc_exists_and_names_missing_fields():
    assert BLOCKED_DOC.is_file(), "FVG_TOUCH_LINEAGE_BLOCKED.md must lock the gap"
    text = BLOCKED_DOC.read_text()
    for field in (
        "touch_count",
        "first_touch_ts",
        "last_touch_ts",
        "penetration_pct",
        "reclaim_state",
    ):
        assert field in text, f"blocked doc must name missing field {field}"
    assert "durable" in text.lower()
    assert "UNKNOWN" in text or "MISSING" in text


def test_fair_value_gap_dict_lacks_durable_touch_lineage_fields():
    gap = FairValueGap(
        direction="bullish",
        low=95.0,
        high=97.0,
        midpoint=96.0,
        start_index=0,
        end_index=2,
        timeframe="4h",
        mitigated=False,
        fill_pct=0.0,
        status="unfilled",
    )
    payload = gap.to_dict()
    present = REQUIRED_DURABLE_TOUCH_FIELDS.intersection(payload)
    assert not present, f"FairValueGap must not invent durable touch fields: {present}"


def test_frozen_zone_schema_lacks_durable_touch_lineage_fields():
    rows = _bullish_fvg_rows()
    gaps = detect_fair_value_gaps(rows, timeframe="4h")
    assert gaps, "fixture must produce at least one FVG"
    zone = freeze_fvg_zone(
        gaps[0],
        rows=rows,
        side="CALL",
        data_as_of="2026-09-09T14:05:00+00:00",
        source_provider="test",
        session_policy=SESSION_POLICY_4H_ATTESTATION,
        candle_bucket_anchor_policy=CANDLE_BUCKET_ANCHOR_POLICY["4h"],
    )
    assert zone.get("zone_id")
    present = REQUIRED_DURABLE_TOUCH_FIELDS.intersection(zone)
    assert not present, f"freeze_fvg_zone must not invent durable touch fields: {present}"
    # Single-pass lifecycle may say midpoint_touched / filled — that is NOT touch_count.
    assert "touch_count" not in zone
    assert zone.get("lifecycle_status") in {
        "unfilled",
        "partial_fill",
        "midpoint_touched",
        "filled",
        "broken_reclaimed",
    }


def test_market_structure_freeze_does_not_emit_fabricated_fvg_touch_lineage():
    rows = _bullish_fvg_rows()
    freeze = freeze_breach_market_structure(
        {
            "side": "CALL",
            "trigger_price": 100.0,
            "underlying_price": 100.5,
            "target_price": 105.0,
            "stop_price": 98.0,
            "data_as_of": "2026-09-09T14:05:00+00:00",
        },
        market_context={"candles": {"4h": rows, "1h": rows}},
        data_as_of="2026-09-09T14:05:00+00:00",
    )
    assert freeze.get("observe_only") is True
    assert freeze.get("affected_eligibility") is False
    # Must not claim a known touch lineage blob without durable authority.
    lineage = freeze.get("fvg_touch_lineage")
    if lineage is None:
        return
    assert isinstance(lineage, dict)
    assert lineage.get("observe_only") is True
    assert lineage.get("affected_eligibility") is False
    status = str(lineage.get("status") or lineage.get("lineage_status") or "").upper()
    assert status in {"UNKNOWN", "MISSING", "BLOCKED"}, (
        "without durable authority, fvg_touch_lineage must be UNKNOWN/MISSING/BLOCKED "
        f"(got {status!r}); do not fabricate touch_count"
    )
    assert lineage.get("touch_count") in (None, "UNKNOWN", "MISSING")
    assert lineage.get("first_touch_ts") in (None, "UNKNOWN", "MISSING")


def test_breach_evidence_and_readiness_unaffected_by_absent_touch_lineage():
    signal = {
        "side": "CALL",
        "trigger_price": 100.0,
        "stop_price": 98.0,
        "target_price": 105.0,
        "underlying_price": 100.2,
        "breach_lineage": "INITIAL_BREACH",
        "trigger_crossed_at": "2026-09-09T14:00:00+00:00",
        "data_as_of": "2026-09-09T14:05:00+00:00",
    }
    evidence = _build_breach_evidence(
        signal,
        data_sources={"candles": {"5m": [], "15m": []}},
        provenance={
            "fifteen_minute": "frozen_signal_15m",
            "five_minute": "frozen_signal_5min",
        },
        observation={
            "price": signal["underlying_price"],
            "source": "test",
            "status": "AVAILABLE",
            "observed_at": signal["data_as_of"],
        },
    )
    # No fabricated FVG touch lineage on evidence.
    for key in ("fvg_touch_lineage", "touch_count", "first_touch_ts"):
        val = evidence.get(key)
        if val is None:
            continue
        if isinstance(val, dict):
            st = str(val.get("status") or val.get("lineage_status") or "").upper()
            assert st in {"UNKNOWN", "MISSING", "BLOCKED"}
        else:
            assert str(val).upper() in {"UNKNOWN", "MISSING", "BLOCKED"}

    readiness = classify_entry_readiness_observe_only(signal, evidence=evidence)
    assert readiness.get("observe_only") is True
    assert readiness.get("affected_eligibility") is False
    # Eligibility / classification must not depend on invented touch_count.
    assert "touch_count" not in (readiness.get("diagnostics") or {})


def test_no_resolve_fvg_touch_lineage_invented_in_tip_modules():
    """Lock: do not ship a resolver that fabricates history without authority."""
    targets = [
        _ROOT / "ap" / "intelligence_breach_market_structure.py",
        _ROOT / "ap" / "intelligence_context_materializer.py",
    ]
    for path in targets:
        tree = ast.parse(path.read_text())
        defs = {
            n.name
            for n in tree.body
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
        }
        assert "resolve_fvg_touch_lineage" not in defs, (
            f"{path.name} must not invent resolve_fvg_touch_lineage until durable "
            "touch ledger fields exist (see FVG_TOUCH_LINEAGE_BLOCKED.md)"
        )


# ---------------------------------------------------------------------------
# Deferred capability markers (xfail until production export unlocks)
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "BLOCKED: durable FVG touch ledger missing "
        "(touch_count/first_touch_ts/touch_kind). See FVG_TOUCH_LINEAGE_BLOCKED.md"
    ),
)
def test_known_touch_yields_stable_zone_id_and_kind_WHEN_authority_exists():
    """Placeholder for #5/#6 once production export lands — must not pass early."""
    raise AssertionError(
        "durable touch authority not present; implement resolve_fvg_touch_lineage "
        "only after export"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "BLOCKED: missing candles/zones must surface UNKNOWN/MISSING via real "
        "resolver; not implemented. See FVG_TOUCH_LINEAGE_BLOCKED.md"
    ),
)
def test_missing_candles_zones_honest_unknown_WHEN_resolver_exists():
    raise AssertionError(
        "resolve_fvg_touch_lineage not implemented — authority blocked"
    )
