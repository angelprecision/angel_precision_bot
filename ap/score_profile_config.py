from __future__ import annotations

from dataclasses import dataclass


# Volume thresholds
RELATIVE_VOLUME_STRONG = 1.8
RELATIVE_VOLUME_OK = 1.4
RELATIVE_VOLUME_BASELINE = 1.0
BREAKOUT_VOLUME_MIN = 1.15
BREAKOUT_VOLUME_STRONG = 1.5
HIGH_VOLUME_REJECTION_RATIO = 1.5

# VWAP thresholds
VWAP_CHOP_ZONE_PCT = 0.15
VWAP_SUPPORT_RESISTANCE_ZONE_PCT = 0.35

# Price stacking / geometry thresholds
PRICE_STACKING_TOLERANCE_PCT = 0.35
MIN_TRIGGER_REWARD_RISK = 0.75
MIN_REMAINING_MOVE_PCT = 0.25
MIN_REMAINING_R = 0.75

# Component max scores
VOLUME_CONFIRMATION_MAX_POINTS = 10.0
VWAP_CONTEXT_MAX_POINTS = 10.0
SECTOR_CONTEXT_MAX_POINTS = 5.0
PRICE_STACKING_MAX_POINTS = 10.0
TRIGGER_GEOMETRY_MAX_POINTS = 10.0
REMAINING_OPPORTUNITY_MAX_POINTS = 10.0

# Rollout flags. These are intentionally inactive in #209-#214.
POSITION_SCORE_PROFILE_OBSERVE_ONLY = True
POSITION_SCORE_PROFILE_FUTURE_GATE_ENABLED = False


@dataclass(frozen=True)
class ScoreProfileConfig:
    # Canonical names requested for tuning safety.
    relative_volume_strong: float = RELATIVE_VOLUME_STRONG
    relative_volume_ok: float = RELATIVE_VOLUME_OK
    relative_volume_baseline: float = RELATIVE_VOLUME_BASELINE
    breakout_volume_min: float = BREAKOUT_VOLUME_MIN
    breakout_volume_strong: float = BREAKOUT_VOLUME_STRONG
    high_volume_rejection_ratio: float = HIGH_VOLUME_REJECTION_RATIO
    vwap_chop_zone_pct: float = VWAP_CHOP_ZONE_PCT
    vwap_support_resistance_zone_pct: float = VWAP_SUPPORT_RESISTANCE_ZONE_PCT
    price_stacking_tolerance_pct: float = PRICE_STACKING_TOLERANCE_PCT
    min_trigger_reward_risk: float = MIN_TRIGGER_REWARD_RISK
    min_remaining_move_pct: float = MIN_REMAINING_MOVE_PCT
    min_remaining_r: float = MIN_REMAINING_R
    volume_confirmation_max_points: float = VOLUME_CONFIRMATION_MAX_POINTS
    vwap_context_max_points: float = VWAP_CONTEXT_MAX_POINTS
    sector_context_max_points: float = SECTOR_CONTEXT_MAX_POINTS
    price_stacking_max_points: float = PRICE_STACKING_MAX_POINTS
    trigger_geometry_max_points: float = TRIGGER_GEOMETRY_MAX_POINTS
    remaining_opportunity_max_points: float = REMAINING_OPPORTUNITY_MAX_POINTS
    observe_only: bool = POSITION_SCORE_PROFILE_OBSERVE_ONLY
    future_gate_enabled: bool = POSITION_SCORE_PROFILE_FUTURE_GATE_ENABLED

    # Backward-compatible aliases for stacked PRs while the book is merged.
    relative_volume_thrust: float = RELATIVE_VOLUME_STRONG
    relative_volume_confirmed: float = RELATIVE_VOLUME_OK
    breakout_volume_minimum: float = BREAKOUT_VOLUME_MIN
    breakout_volume_confirmed: float = BREAKOUT_VOLUME_STRONG
    max_volume_confirmation_score: float = VOLUME_CONFIRMATION_MAX_POINTS
    max_vwap_context_score: float = VWAP_CONTEXT_MAX_POINTS
    max_sector_context_score: float = SECTOR_CONTEXT_MAX_POINTS


DEFAULT_SCORE_PROFILE_CONFIG = ScoreProfileConfig()
