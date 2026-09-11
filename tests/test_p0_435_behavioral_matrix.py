"""Table-driven #435 behavioral matrix — fixture-only, no live broker.

Covers observe-only entry readiness, lineage, confirmation MISSING honesty,
advisory (non-hard-veto) regime disagreement when exposed on evidence, and
deterministic market_structure zone identity hashing.
"""

from __future__ import annotations

import hashlib
import json
import sys
from copy import deepcopy
from pathlib import Path

import pytest

# Ensure /workspace/pr435 is import root for local ap package.
_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from ap.intelligence_breach_market_structure import (  # noqa: E402
    freeze_breach_market_structure,
)
from ap.intelligence_context_materializer import (  # noqa: E402
    _directional_confirmation,
    classify_entry_readiness_observe_only,
    resolve_breach_lineage,
)


def _geom_signal(*, side: str = "CALL", **overrides) -> dict:
    if side == "CALL":
        base = {
            "side": "CALL",
            "trigger_price": 100.0,
            "stop_price": 98.0,
            "target_price": 106.0,
            "underlying_price": 100.5,
        }
    else:
        base = {
            "side": "PUT",
            "trigger_price": 100.0,
            "stop_price": 102.0,
            "target_price": 94.0,
            "underlying_price": 99.5,
        }
    base.update(overrides)
    return base


def _evidence(
    *,
    lineage: str = "INITIAL_BREACH",
    remaining_r: float = 2.0,
    percent_move_consumed: float = 0.1,
    target_reached: bool = False,
    fifteen: dict | None = None,
    five: dict | None = None,
    extra: dict | None = None,
) -> dict:
    out = {
        "breach_lineage": lineage,
        "remaining_opportunity": {
            "remaining_r": remaining_r,
            "percent_move_consumed": percent_move_consumed,
            "target_reached": target_reached,
            "target_already_reached": target_reached,
        },
        "fifteen_minute_confirmation": fifteen
        if fifteen is not None
        else {"status": "AVAILABLE", "follow_through": True},
        "five_minute_confirmation": five
        if five is not None
        else {"status": "AVAILABLE", "follow_through": True},
    }
    if extra:
        out.update(extra)
    return out


# ---------------------------------------------------------------------------
# Table-driven readiness cases
# ---------------------------------------------------------------------------

CASES = [
    pytest.param(
        "call_clean_continuation_ready_now",
        _geom_signal(side="CALL"),
        _evidence(lineage="INITIAL_BREACH"),
        "READY_NOW",
        id="call_clean_continuation",
    ),
    pytest.param(
        "put_symmetric_ready_now",
        _geom_signal(side="PUT"),
        _evidence(lineage="INITIAL_BREACH"),
        "READY_NOW",
        id="put_symmetric_continuation",
    ),
    pytest.param(
        "heavy_extension_rebreach_preferred",
        _geom_signal(side="CALL", underlying_price=104.5),
        _evidence(percent_move_consumed=0.75, remaining_r=0.4),
        "REBREACH_PREFERRED",
        id="heavy_extension",
    ),
    pytest.param(
        "target_reached_invalid",
        _geom_signal(side="CALL", underlying_price=106.5),
        _evidence(target_reached=True, remaining_r=-0.2, percent_move_consumed=1.0),
        "INVALID",
        id="target_reached_invalid",
    ),
    pytest.param(
        "remaining_r_leq_zero_invalid",
        _geom_signal(side="PUT", underlying_price=93.5),
        _evidence(remaining_r=0.0, percent_move_consumed=0.9),
        "INVALID",
        id="remaining_r_exhausted",
    ),
    pytest.param(
        "missing_5m_stays_wait_on_first_breach",
        _geom_signal(side="CALL"),
        _evidence(
            lineage="INITIAL_BREACH",
            five={"status": "MISSING", "bar_count": 0, "missing_reason": "canonical_5min_source_unavailable"},
            fifteen={"status": "AVAILABLE", "follow_through": True},
        ),
        "WAIT_CONFIRMATION",
        id="missing_5m",
    ),
    pytest.param(
        "missing_15m_stays_wait_on_first_breach",
        _geom_signal(side="PUT"),
        _evidence(
            lineage="INITIAL_BREACH",
            fifteen={"status": "MISSING"},
            five={"status": "AVAILABLE", "follow_through": True},
        ),
        "WAIT_CONFIRMATION",
        id="missing_15m",
    ),
    pytest.param(
        "regime_disagreement_advisory_not_hard_veto",
        _geom_signal(side="CALL"),
        _evidence(
            lineage="INITIAL_BREACH",
            extra={
                "strategy_advisories": ["regime_market_sector_disagreement"],
                "regime_disagreement": True,
                "hard_safety_blocks": [],
            },
        ),
        "READY_NOW",
        id="regime_disagreement_advisory",
    ),
    pytest.param(
        "rebreach_with_continuation_ready",
        _geom_signal(side="CALL"),
        _evidence(
            lineage="REBREACH_AFTER_RESET",
            fifteen={"status": "AVAILABLE", "follow_through": True},
            five={"status": "MISSING"},
        ),
        "READY_NOW",
        id="rebreach_continuation",
    ),
]


@pytest.mark.parametrize("_label,signal,evidence,expected", CASES)
def test_behavioral_matrix_classification(_label, signal, evidence, expected):
    result = classify_entry_readiness_observe_only(signal, evidence=evidence)
    assert result["classification"] == expected
    assert result["observe_only"] is True
    assert result["affected_eligibility"] is False


def test_missing_5m_confirmation_status_stays_missing():
    """Empty 5m rows → status MISSING; never fabricated follow_through."""
    conf = _directional_confirmation([], "CALL", source="frozen_signal_5min")
    assert conf["status"] == "MISSING"
    assert conf.get("follow_through") in (None, False) or "follow_through" not in conf
    assert conf.get("bar_count", 0) == 0


def test_missing_15m_confirmation_status_stays_missing():
    conf = _directional_confirmation([], "PUT", source="frozen_signal_15m")
    assert conf["status"] == "MISSING"
    assert "follow_through" not in conf or not conf.get("follow_through")


def test_first_breach_vs_rebreach_lineage_comparison():
    first = resolve_breach_lineage({"breach_count": 1})
    rebreach = resolve_breach_lineage({"breach_count": 2, "breach_reset": True})
    assert first["breach_lineage"] == "INITIAL_BREACH"
    assert rebreach["breach_lineage"] == "REBREACH_AFTER_RESET"
    assert first["breach_lineage"] != rebreach["breach_lineage"]

    # Same geometry + strong 15m: first breach without 5m waits; rebreach can READY_NOW.
    signal = _geom_signal(side="CALL")
    shared_fifteen = {"status": "AVAILABLE", "follow_through": True}
    shared_five_missing = {"status": "MISSING", "bar_count": 0}
    first_class = classify_entry_readiness_observe_only(
        signal,
        evidence=_evidence(
            lineage=first["breach_lineage"],
            fifteen=shared_fifteen,
            five=shared_five_missing,
        ),
    )
    rebreach_class = classify_entry_readiness_observe_only(
        signal,
        evidence=_evidence(
            lineage=rebreach["breach_lineage"],
            fifteen=shared_fifteen,
            five=shared_five_missing,
        ),
    )
    assert first_class["classification"] == "WAIT_CONFIRMATION"
    assert rebreach_class["classification"] == "READY_NOW"
    assert first_class["observe_only"] and rebreach_class["observe_only"]
    assert first_class["affected_eligibility"] is False
    assert rebreach_class["affected_eligibility"] is False


def test_observe_only_and_affected_eligibility_false_on_all_classes():
    for expected in ("READY_NOW", "WAIT_CONFIRMATION", "REBREACH_PREFERRED", "INVALID"):
        # Pick a fixture that yields each class
        mapping = {
            "READY_NOW": _evidence(),
            "WAIT_CONFIRMATION": _evidence(
                fifteen={"status": "MISSING"}, five={"status": "MISSING"}
            ),
            "REBREACH_PREFERRED": _evidence(percent_move_consumed=0.8),
            "INVALID": _evidence(remaining_r=-1.0, target_reached=True),
        }
        result = classify_entry_readiness_observe_only(
            _geom_signal(), evidence=mapping[expected]
        )
        assert result["classification"] == expected
        assert result["observe_only"] is True
        assert result["affected_eligibility"] is False


def _candle(ts: str, o: float, h: float, l: float, c: float) -> dict:
    return {"time": ts, "open": o, "high": h, "low": l, "close": c, "volume": 1000}


def _structure_hash(freeze: dict) -> str:
    payload = {
        "zone_ids": [z["zone_id"] for z in freeze.get("zones") or []],
        "relationship": (freeze.get("relationship") or {}).get("relationship"),
        "schema_version": freeze.get("schema_version"),
        "model_version": freeze.get("model_version"),
    }
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def test_identical_frozen_market_structure_yields_identical_zone_ids_and_hash():
    as_of = "2026-09-09T14:05:00+00:00"
    rows_4h = [
        _candle("2026-09-04T09:30:00-04:00", 94.0, 95.0, 93.0, 94.5),
        _candle("2026-09-04T13:30:00-04:00", 95.0, 96.5, 94.8, 96.0),
        _candle("2026-09-05T09:30:00-04:00", 97.0, 98.0, 97.0, 97.5),
        _candle("2026-09-05T13:30:00-04:00", 105.5, 107.0, 105.0, 105.2),
        _candle("2026-09-08T09:30:00-04:00", 105.0, 105.5, 103.5, 104.0),
        _candle("2026-09-08T13:30:00-04:00", 103.8, 104.2, 102.5, 103.0),
        _candle("2026-09-09T09:30:00-04:00", 100.0, 101.0, 99.5, 100.5),
    ]
    ctx = {"candles": {"4h": rows_4h, "1h": list(rows_4h)}}
    sig = _geom_signal(
        side="CALL",
        trigger_crossed_at=as_of,
        data_as_of=as_of,
    )
    a = freeze_breach_market_structure(sig, market_context=ctx, data_as_of=as_of)
    b = freeze_breach_market_structure(
        deepcopy(sig), market_context=deepcopy(ctx), data_as_of=as_of
    )
    ids_a = [z["zone_id"] for z in a["zones"]]
    ids_b = [z["zone_id"] for z in b["zones"]]
    assert ids_a == ids_b
    assert _structure_hash(a) == _structure_hash(b)
    assert a["observe_only"] is True
    assert a["affected_eligibility"] is False


def test_regime_disagreement_does_not_populate_hard_veto_on_classifier():
    """If evidence exposes regime disagreement, readiness stays non-INVALID when
    continuation is clean — advisory only (no hard veto via this classifier).
    """
    result = classify_entry_readiness_observe_only(
        _geom_signal(side="CALL"),
        evidence=_evidence(
            extra={
                "strategy_advisories": ["regime_market_sector_disagreement"],
                "regime_disagreement": True,
                "hard_safety_blocks": [],
            }
        ),
    )
    assert result["classification"] == "READY_NOW"
    assert result["classification"] != "INVALID"
    assert result["affected_eligibility"] is False
    assert result["observe_only"] is True
