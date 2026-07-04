# =============================================================================
# ap/exit_ladder.py — PR-G: Volatility-Scaled Exit Ladder (DORMANT BY DEFAULT)
#
# Single resolution authority for the exit engine's P&L thresholds.
#
# DESIGN CONTRACT (money safety):
#   1. LEGACY IS THE DEFAULT AND IS BYTE-IDENTICAL. In legacy mode this
#      module returns EXACTLY the values the exit engine passes in — it
#      holds NO copy of any legacy number, so it can never drift from
#      ap_exit_engine constants.
#   2. LIVE IS TRIPLE-LOCKED. vol_scaled can only apply to a position when
#      ALL of: EXIT_LADDER_MODE=vol_scaled, VOL_EXIT_ENABLED=true, and
#      (execution_mode is paper OR (VOL_EXIT_PAPER_ONLY=false AND
#      VOL_EXIT_LIVE_ENABLED=true)). Unknown/empty execution_mode is
#      treated as LIVE (fail-safe, consistent with the P0 unknown-
#      execution-mode fail-closed policy, commit 9513fd0).
#   3. MISSING/INVALID IV META ⇒ LEGACY, per position, stamped with a
#      fallback_reason. Never raises into the exit path.
#   4. This module changes NOTHING about time windows, EOD hard close,
#      exit_in_flight coordination, or fill-confirmed scale-out counting.
#      It resolves THRESHOLD NUMBERS only.
#
# k-multiplier semantics (per PR-G spec): in vol_scaled mode each
# threshold = k × expected_option_daily_range_pct, where the range is the
# option's own approximate one-day expected swing (see ap/expected_move).
# Example: range=0.30 (a typical short-dated near-ATM contract) gives
# TP=+15%, W1=+30%, W2=+18%, W3=+12%, hard=−22.5%, theta=−25.5%.
# A high-IV name (range=0.60) gets bands twice as wide; a low-IV name
# (range=0.15) gets bands half as wide — same rule, per-instrument risk.
# =============================================================================
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Optional

from ap.expected_move import expected_option_daily_range_pct

log = logging.getLogger("ap.exit_ladder")

MODE_LEGACY = "legacy"
MODE_VOL_SCALED = "vol_scaled"

# ── Safety clamps on RESOLVED vol_scaled thresholds ──────────────────────────
# Even with the range clamp in expected_move, k × range must stay inside
# operationally sane bounds. These are wide guardrails, not tuning knobs.
_CLAMP = {
    "immediate_tp": (0.06, 1.00),
    "w1_threshold": (0.05, 1.50),
    "w2_threshold": (0.05, 1.50),
    "w3_threshold": (0.05, 1.50),
    "hard_stop":    (-0.60, -0.12),
    "theta_stop":   (-0.70, -0.15),
    "profit_lock":  (0.04, 1.00),
}


def _env_flag(name: str, default: str) -> bool:
    return os.getenv(name, default).strip().lower() in {"1", "true", "yes", "on"}


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class ExitLadderConfig:
    """Snapshot of env configuration, read once per resolution so a single
    evaluation cycle is internally consistent even if env changes mid-run."""
    mode:              str
    enabled:           bool
    paper_only:        bool
    live_enabled:      bool
    tp_k:              float
    w1_k:              float
    w2_k:              float
    w3_k:              float
    hard_stop_k:       float
    theta_stop_k:      float

    @staticmethod
    def from_env() -> "ExitLadderConfig":
        return ExitLadderConfig(
            mode=os.getenv("EXIT_LADDER_MODE", MODE_LEGACY).strip().lower(),
            enabled=_env_flag("VOL_EXIT_ENABLED", "false"),
            paper_only=_env_flag("VOL_EXIT_PAPER_ONLY", "true"),
            live_enabled=_env_flag("VOL_EXIT_LIVE_ENABLED", "false"),
            tp_k=_env_float("VOL_EXIT_TP_K", 0.5),
            w1_k=_env_float("VOL_EXIT_W1_K", 1.0),
            w2_k=_env_float("VOL_EXIT_W2_K", 0.6),
            w3_k=_env_float("VOL_EXIT_W3_K", 0.4),
            hard_stop_k=_env_float("VOL_EXIT_HARD_STOP_K", -0.75),
            theta_stop_k=_env_float("VOL_EXIT_THETA_STOP_K", -0.85),
        )


@dataclass(frozen=True)
class LegacyThresholds:
    """The exit engine's CURRENT effective values for one position, built by
    the engine from its own constants + _effective_thresholds(pos). This
    module never hardcodes them — zero-drift guarantee."""
    hard_stop:    float
    immediate_tp: float
    profit_lock:  float
    theta_stop:   float
    w1_threshold: float   # engine: SCALE_OUT_1_THRESHOLD (W1 11:00 gate)
    w2_threshold: float   # engine: SCALE_OUT_2_THRESHOLD (W2 13:00 gate)
    w3_threshold: float   # engine: PROTECT_3_THRESHOLD   (W3 14:00 gate)


@dataclass(frozen=True)
class ResolvedLadder:
    mode:            str
    hard_stop:       float
    immediate_tp:    float
    profit_lock:     float
    theta_stop:      float
    w1_threshold:    float
    w2_threshold:    float
    w3_threshold:    float
    expected_range:  Optional[float] = None   # option daily range used (vol_scaled)
    range_quality:   str = ""                 # from expected_move
    fallback_reason: str = ""                 # why legacy applied (when it did)
    k_used:          dict = field(default_factory=dict)
    clamped_fields:  tuple = ()

    def stamp(self) -> dict:
        """Compact audit dict for exit decision events / reason suffixes."""
        out = {"ladder_mode": self.mode}
        if self.mode == MODE_VOL_SCALED:
            out.update({
                "expected_range": round(self.expected_range, 4) if self.expected_range else None,
                "range_quality": self.range_quality,
                "k": self.k_used,
                "resolved": {
                    "tp": round(self.immediate_tp, 4),
                    "w1": round(self.w1_threshold, 4),
                    "w2": round(self.w2_threshold, 4),
                    "w3": round(self.w3_threshold, 4),
                    "hard": round(self.hard_stop, 4),
                    "theta": round(self.theta_stop, 4),
                },
            })
            if self.clamped_fields:
                out["clamped"] = list(self.clamped_fields)
        if self.fallback_reason:
            out["fallback_reason"] = self.fallback_reason
        return out


def _legacy(legacy: LegacyThresholds, reason: str) -> ResolvedLadder:
    return ResolvedLadder(
        mode=MODE_LEGACY,
        hard_stop=legacy.hard_stop,
        immediate_tp=legacy.immediate_tp,
        profit_lock=legacy.profit_lock,
        theta_stop=legacy.theta_stop,
        w1_threshold=legacy.w1_threshold,
        w2_threshold=legacy.w2_threshold,
        w3_threshold=legacy.w3_threshold,
        fallback_reason=reason,
    )


def _clamp(name: str, value: float, clamped: list) -> float:
    lo, hi = _CLAMP[name]
    if value < lo:
        clamped.append(name)
        return lo
    if value > hi:
        clamped.append(name)
        return hi
    return value


def extract_range_from_meta(vol_meta: Optional[dict]) -> tuple:
    """(expected_option_daily_range_pct, quality) from orders.meta.vol_exit.

    Prefers the pre-computed value stamped at selection; falls back to
    recomputing from stamped primitives (mid_iv, delta, premium,
    underlying) so a partially-populated stamp still resolves. Total —
    never raises.
    """
    if not isinstance(vol_meta, dict) or not vol_meta:
        return None, "missing_iv_meta"
    try:
        pre = vol_meta.get("expected_option_daily_range_pct")
        if isinstance(pre, (int, float)) and 0.0 < float(pre) <= 2.0:
            return float(pre), str(vol_meta.get("range_quality") or "ok_precomputed")
        return expected_option_daily_range_pct(
            vol_meta.get("delta"),
            vol_meta.get("premium_per_share"),
            vol_meta.get("underlying_price"),
            vol_meta.get("entry_atm_iv"),
        )
    except Exception as exc:  # pragma: no cover — total-function guarantee
        log.warning("exit_ladder.extract_range_from_meta failed: %s", exc)
        return None, "meta_parse_error"


def resolve_exit_ladder(
    execution_mode: str,
    legacy: LegacyThresholds,
    vol_meta: Optional[dict],
    config: Optional[ExitLadderConfig] = None,
) -> ResolvedLadder:
    """Resolve the per-position threshold set. NEVER raises.

    Fallback-to-legacy ladder (checked in order, first match wins):
      mode_legacy · vol_exit_disabled · live_locked (incl. unknown mode) ·
      missing_iv_meta / iv quality failures
    """
    try:
        cfg = config or ExitLadderConfig.from_env()

        if cfg.mode != MODE_VOL_SCALED:
            return _legacy(legacy, "")           # pure legacy — no stamp noise
        if not cfg.enabled:
            return _legacy(legacy, "vol_exit_disabled")

        em = str(execution_mode or "").strip().lower()
        is_paper = em == "paper"
        if not is_paper:
            # live, "", or unknown — all treated as live-risk (fail-safe).
            if cfg.paper_only or not cfg.live_enabled:
                return _legacy(legacy, "live_locked")

        rng, quality = extract_range_from_meta(vol_meta)
        if rng is None:
            return _legacy(legacy, quality)

        clamped: list = []
        tp = _clamp("immediate_tp", cfg.tp_k * rng, clamped)
        resolved = ResolvedLadder(
            mode=MODE_VOL_SCALED,
            immediate_tp=tp,
            # profit_lock has no dedicated k (spec); mirrors legacy's
            # lock==tp relationship (engine: 0.12/0.12).
            profit_lock=_clamp("profit_lock", cfg.tp_k * rng, clamped),
            w1_threshold=_clamp("w1_threshold", cfg.w1_k * rng, clamped),
            w2_threshold=_clamp("w2_threshold", cfg.w2_k * rng, clamped),
            w3_threshold=_clamp("w3_threshold", cfg.w3_k * rng, clamped),
            hard_stop=_clamp("hard_stop", cfg.hard_stop_k * rng, clamped),
            theta_stop=_clamp("theta_stop", cfg.theta_stop_k * rng, clamped),
            expected_range=rng,
            range_quality=quality,
            k_used={
                "tp": cfg.tp_k, "w1": cfg.w1_k, "w2": cfg.w2_k, "w3": cfg.w3_k,
                "hard": cfg.hard_stop_k, "theta": cfg.theta_stop_k,
            },
            clamped_fields=tuple(clamped),
        )
        return resolved
    except Exception as exc:  # pragma: no cover — exit path must never break
        log.error("exit_ladder.resolve failed — legacy fallback: %s", exc)
        return _legacy(legacy, "resolver_error")
