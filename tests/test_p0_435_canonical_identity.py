"""Focused #435 observe-only identity/lineage/lifecycle regressions."""

from __future__ import annotations

from ap.intelligence_context_materializer import (
    _freeze_value,
    classify_entry_readiness_observe_only,
    resolve_breach_lineage,
    resolve_canonical_strategy_pattern,
    resolve_public_breach_lifecycle,
)


def test_canonical_pattern_daily_2_3_maps_to_232():
    result = resolve_canonical_strategy_pattern({"pattern": "2-3", "timeframe": "1d"})
    assert result["raw_pattern"] == "2-3"
    assert result["canonical_strategy_pattern"] == "2-3-2"
    assert result["status"] == "RESOLVED"


def test_canonical_pattern_unknown_stays_explicit():
    result = resolve_canonical_strategy_pattern({"pattern": "mystery", "timeframe": "15m"})
    assert result["canonical_strategy_pattern"] is None
    assert result["status"] == "AMBIGUOUS"


def test_breach_lineage_initial_and_rebreach():
    first = resolve_breach_lineage({"breach_count": 1})
    assert first["breach_lineage"] == "INITIAL_BREACH"
    rebreach = resolve_breach_lineage({"breach_count": 2, "breach_reset": True})
    assert rebreach["breach_lineage"] == "REBREACH_AFTER_RESET"
    unknown = resolve_breach_lineage({})
    assert unknown["breach_lineage"] == "UNKNOWN"


def test_breach_lineage_direction_reversal_and_recovered():
    rev = resolve_breach_lineage({"direction_reversed": True, "breach_count": 2})
    assert rev["breach_lineage"] == "DIRECTION_REVERSAL_REBREACH"
    recovered = resolve_breach_lineage(
        {"_recovery_pre_claimed": True, "breach_reset": True, "breach_count": 2}
    )
    assert recovered["breach_lineage"] == "RECOVERED_BREACH"


def test_public_lifecycle_resolved_before_private_strip():
    signal = {
        "_recovery_pre_claimed": True,
        "_recovery_pre_claimed_generation": 4,
        "_recovery_pre_claimed_attempt": 2,
        "_recovery_pre_claimed_owner": "watcher-7",
        "_recovery_pre_claimed_client_id": "client@example.com",
        "_recovery_pre_claimed_mode": "LIVE",
    }
    lifecycle = resolve_public_breach_lifecycle(signal)
    frozen = _freeze_value({**signal, "breach_lifecycle": lifecycle})
    assert "_recovery_pre_claimed_generation" not in frozen
    assert frozen["breach_lifecycle"]["generation"] == 4
    assert frozen["breach_lifecycle"]["attempt"] == 2
    assert frozen["breach_lifecycle"]["owner"] == "watcher-7"
    assert frozen["breach_lifecycle"]["preclaimed"] is True


def test_entry_readiness_observe_only_flags_and_first_breach_wait():
    classification = classify_entry_readiness_observe_only(
        {
            "side": "PUT",
            "trigger_price": 100,
            "stop_price": 102,
            "target_price": 96,
            "underlying_price": 99.4,
        },
        evidence={
            "breach_lineage": "INITIAL_BREACH",
            "remaining_opportunity": {
                "remaining_r": 1.5,
                "percent_move_consumed": 0.15,
            },
            "fifteen_minute_confirmation": {"status": "MISSING"},
            "five_minute_confirmation": {"status": "MISSING"},
        },
    )
    assert classification["observe_only"] is True
    assert classification["affected_eligibility"] is False
    assert classification["classification"] == "WAIT_CONFIRMATION"
