"""
ap_quality_mode.py — Angel Precision Quality Mode Gate (PR-72)

Enforces a strict signal-quality floor for paper/client-proof operation.
All blocks are non-recoverable within the same evaluate() call — the gate
returns a structured rejection dict that APMasterControl wraps into a
ControlDecision via _block().

Do NOT import broker, exit, or pricing modules here.  This module is
intentionally thin: read env → evaluate signal → return verdict.

Environment variables (all read once at import time):
    QUALITY_MODE_ENABLED            bool  default false
    QUALITY_MODE_SCORE_MIN          float default 70
    QUALITY_MODE_MAX_DAILY_TRADES   int   default 5
    QUALITY_MODE_MAX_POSITIONS      int   default 4
    QUALITY_MODE_MAX_B_TIER_TRADES  int   default 2
    SAME_SYMBOL_REENTRY_COOLDOWN_MINUTES int default 60
    PYRAMIDING_ENABLED              bool  default false
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import Optional

log = logging.getLogger("ap.quality_mode")

# ---------------------------------------------------------------------------
# Config — read once at import time so unit tests can patch os.environ before
# importing, or use the reset_config() helper.
# ---------------------------------------------------------------------------

def _bool_env(key: str, default: bool) -> bool:
    raw = os.getenv(key, "").strip().lower()
    if raw in ("1", "true", "yes"):
        return True
    if raw in ("0", "false", "no", ""):
        return default
    return default


def _load_config() -> dict:
    return {
        "enabled":             _bool_env("QUALITY_MODE_ENABLED", False),
        "score_min":           float(os.getenv("QUALITY_MODE_SCORE_MIN", "70")),
        "max_daily_trades":    int(os.getenv("QUALITY_MODE_MAX_DAILY_TRADES", "5")),
        "max_positions":       int(os.getenv("QUALITY_MODE_MAX_POSITIONS", "4")),
        "max_b_tier_trades":   int(os.getenv("QUALITY_MODE_MAX_B_TIER_TRADES", "2")),
        "reentry_cooldown_s":  int(os.getenv("SAME_SYMBOL_REENTRY_COOLDOWN_MINUTES", "60")) * 60,
        "pyramiding_enabled":  _bool_env("PYRAMIDING_ENABLED", False),
    }


_cfg: dict = _load_config()
_cfg_lock = threading.Lock()


def reset_config() -> None:
    """Re-read all envs.  Call in tests after patching os.environ."""
    global _cfg
    with _cfg_lock:
        _cfg = _load_config()


def get_config() -> dict:
    with _cfg_lock:
        return dict(_cfg)


# ---------------------------------------------------------------------------
# Verdict dataclass
# ---------------------------------------------------------------------------

@dataclass
class QMVerdict:
    allowed: bool
    log_code: str                      # structured log token (QUALITY_MODE_*)
    quality_mode_reason: str           # human-readable
    meta: dict = field(default_factory=dict)  # merged into orders.meta


# ---------------------------------------------------------------------------
# Per-process state (mirrors APMasterControl._seen_signals pattern)
# Keyed by client_id so each client has independent counters.
# State is intentionally in-process; the authoritative source is Postgres.
# This layer is a fast pre-gate — the DB snapshot is the real cap.
# ---------------------------------------------------------------------------

@dataclass
class _ClientState:
    # { "symbol:direction" -> timestamp_float }
    reentry_ts: dict = field(default_factory=dict)
    # { signal_id -> True }
    seen_signal_ids: dict = field(default_factory=dict)
    # { plan_id -> True }
    seen_plan_ids: dict = field(default_factory=dict)
    # Count of B-tier approvals today (UTC-date keyed)
    b_tier_date: str = ""
    b_tier_count: int = 0


_state_lock = threading.Lock()
_client_states: dict[str, _ClientState] = {}


def _get_state(client_id: str) -> _ClientState:
    with _state_lock:
        if client_id not in _client_states:
            _client_states[client_id] = _ClientState()
        return _client_states[client_id]


def _today_utc() -> str:
    import datetime
    return datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _tier_from_score(score: float) -> str:
    """Mirrors ap_tier_engine.Tier.from_score without importing it."""
    if score >= 85:
        return "A+"
    if score >= 75:
        return "A"
    if score >= 65:
        return "B"
    if score >= 35:
        return "SHADOW"
    return "REJECT"


def _base_meta(score: float, tier: str, symbol: str, direction: str,
               timeframe: str, pattern: str) -> dict:
    return {
        "quality_mode_blocked": True,
        "score":      round(score, 1),
        "tier":       tier,
        "symbol":     symbol,
        "direction":  direction,
        "timeframe":  timeframe,
        "pattern":    pattern,
    }


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def check(
    *,
    signal: dict,
    client_id: str,
    intel_status: Optional[str],
    daily_trades_today: int,
    open_positions: int,
    approved_score: float,
) -> QMVerdict:
    """
    Run all Quality Mode gates against a single signal.

    Parameters
    ----------
    signal            : raw signal dict (keys: score, direction/side, symbol/ticker,
                        timeframe, pattern, signal_id, plan_id)
    client_id         : client email / identifier
    intel_status      : intel_status string from intelligence_bridge (may be None)
    daily_trades_today: how many trades the client has completed today (from DB snapshot)
    open_positions    : current open + pending position count (from DB snapshot)
    approved_score    : final effective score that will be approved (float)

    Returns
    -------
    QMVerdict with allowed=True if all gates pass, allowed=False + reason if blocked.
    """
    cfg = get_config()
    if not cfg["enabled"]:
        return QMVerdict(allowed=True, log_code="QUALITY_MODE_DISABLED", quality_mode_reason="")

    score     = approved_score
    symbol    = str(signal.get("symbol") or signal.get("ticker") or "").upper()
    direction = str(signal.get("direction") or signal.get("side") or "").upper()
    timeframe = str(signal.get("timeframe") or "")
    pattern   = str(signal.get("pattern") or signal.get("pattern_id") or "")
    signal_id = str(signal.get("signal_id") or "")
    plan_id   = str(signal.get("plan_id") or "")
    tier      = _tier_from_score(score)

    # ── GATE 1: Hard score floor ──────────────────────────────────────────
    if score < cfg["score_min"]:
        log.warning(
            "QUALITY_MODE_BLOCKED_SCORE | client=%s symbol=%s score=%.1f "
            "min=%.1f tier=%s direction=%s timeframe=%s pattern=%s",
            client_id, symbol, score, cfg["score_min"], tier, direction, timeframe, pattern,
        )
        return QMVerdict(
            allowed=False,
            log_code="QUALITY_MODE_BLOCKED_SCORE",
            quality_mode_reason=f"score {score:.1f} < quality_mode_min {cfg['score_min']:.1f}",
            meta={
                **_base_meta(score, tier, symbol, direction, timeframe, pattern),
                "quality_mode_reason": f"score_below_floor ({score:.1f}<{cfg['score_min']:.1f})",
                "gate_status": "BLOCKED_SCORE",
            },
        )

    # ── GATE 2: Block SKIP_OVERRIDE ───────────────────────────────────────
    if intel_status == "SKIP_OVERRIDE":
        log.warning(
            "QUALITY_MODE_BLOCKED_OVERRIDE | client=%s symbol=%s "
            "intel_status=SKIP_OVERRIDE score=%.1f tier=%s",
            client_id, symbol, score, tier,
        )
        return QMVerdict(
            allowed=False,
            log_code="QUALITY_MODE_BLOCKED_OVERRIDE",
            quality_mode_reason="SKIP_OVERRIDE entries blocked in quality mode",
            meta={
                **_base_meta(score, tier, symbol, direction, timeframe, pattern),
                "quality_mode_reason": "skip_override_blocked",
                "gate_status": "BLOCKED_OVERRIDE",
                "intel_status": "SKIP_OVERRIDE",
            },
        )

    # ── GATE 3: Block RISK_VETO_OVERRIDE unless score >= 78 AND tier A/A+ ─
    if intel_status == "RISK_VETO_OVERRIDE":
        _rvo_tier_ok = tier in ("A", "A+")
        _rvo_score_ok = score >= 78.0
        if not (_rvo_score_ok and _rvo_tier_ok):
            log.warning(
                "QUALITY_MODE_BLOCKED_OVERRIDE | client=%s symbol=%s "
                "intel_status=RISK_VETO_OVERRIDE score=%.1f tier=%s "
                "(requires score>=78 and tier=A/A+)",
                client_id, symbol, score, tier,
            )
            return QMVerdict(
                allowed=False,
                log_code="QUALITY_MODE_BLOCKED_OVERRIDE",
                quality_mode_reason=(
                    f"RISK_VETO_OVERRIDE blocked: score={score:.1f} tier={tier} "
                    f"(requires score>=78 AND tier=A or A+)"
                ),
                meta={
                    **_base_meta(score, tier, symbol, direction, timeframe, pattern),
                    "quality_mode_reason": "risk_veto_override_below_threshold",
                    "gate_status": "BLOCKED_OVERRIDE",
                    "intel_status": "RISK_VETO_OVERRIDE",
                },
            )

    # ── GATE 4: Block score 65–69 even if SCORE65_ALLOW=true ─────────────
    # (Quality mode score floor is 70; gate 1 already caught < score_min.
    #  This gate is explicit about 65-69 so the log code is distinct.)
    if 65.0 <= score < 70.0:
        log.warning(
            "QUALITY_MODE_BLOCKED_SCORE | client=%s symbol=%s score=%.1f "
            "in 65-69 band — blocked regardless of SCORE65_ALLOW",
            client_id, symbol, score,
        )
        return QMVerdict(
            allowed=False,
            log_code="QUALITY_MODE_BLOCKED_SCORE",
            quality_mode_reason=f"score {score:.1f} in 65-69 band: blocked in quality mode regardless of SCORE65_ALLOW",
            meta={
                **_base_meta(score, tier, symbol, direction, timeframe, pattern),
                "quality_mode_reason": "score65_band_blocked_quality_mode",
                "gate_status": "BLOCKED_SCORE",
            },
        )

    # ── GATE 5: Max daily trades per client ───────────────────────────────
    if daily_trades_today >= cfg["max_daily_trades"]:
        log.warning(
            "QUALITY_MODE_BLOCKED_SCORE | client=%s symbol=%s "
            "daily_trades=%d max=%d",
            client_id, symbol, daily_trades_today, cfg["max_daily_trades"],
        )
        return QMVerdict(
            allowed=False,
            log_code="QUALITY_MODE_BLOCKED_SCORE",
            quality_mode_reason=(
                f"max daily trades reached ({daily_trades_today}/{cfg['max_daily_trades']})"
            ),
            meta={
                **_base_meta(score, tier, symbol, direction, timeframe, pattern),
                "quality_mode_reason": "max_daily_trades_reached",
                "gate_status": "BLOCKED_DAILY_LIMIT",
                "quality_mode_blocked": True,
            },
        )

    # ── GATE 6: Max concurrent positions per client ───────────────────────
    if open_positions >= cfg["max_positions"]:
        log.warning(
            "QUALITY_MODE_BLOCKED_SCORE | client=%s symbol=%s "
            "open_positions=%d max=%d",
            client_id, symbol, open_positions, cfg["max_positions"],
        )
        return QMVerdict(
            allowed=False,
            log_code="QUALITY_MODE_BLOCKED_SCORE",
            quality_mode_reason=(
                f"max concurrent positions reached ({open_positions}/{cfg['max_positions']})"
            ),
            meta={
                **_base_meta(score, tier, symbol, direction, timeframe, pattern),
                "quality_mode_reason": "max_positions_reached",
                "gate_status": "BLOCKED_POSITION_LIMIT",
                "quality_mode_blocked": True,
            },
        )

    state = _get_state(client_id)

    # ── GATE 7: Max B-tier trades per client per day ──────────────────────
    if tier == "B":
        today = _today_utc()
        with _state_lock:
            if state.b_tier_date != today:
                state.b_tier_date = today
                state.b_tier_count = 0
            if state.b_tier_count >= cfg["max_b_tier_trades"]:
                log.warning(
                    "QUALITY_MODE_BLOCKED_B_TIER_LIMIT | client=%s symbol=%s "
                    "b_tier_today=%d max=%d score=%.1f",
                    client_id, symbol, state.b_tier_count, cfg["max_b_tier_trades"], score,
                )
                return QMVerdict(
                    allowed=False,
                    log_code="QUALITY_MODE_BLOCKED_B_TIER_LIMIT",
                    quality_mode_reason=(
                        f"B-tier daily limit reached ({state.b_tier_count}/{cfg['max_b_tier_trades']})"
                    ),
                    meta={
                        **_base_meta(score, tier, symbol, direction, timeframe, pattern),
                        "quality_mode_reason": "b_tier_daily_limit",
                        "gate_status": "BLOCKED_B_TIER_LIMIT",
                        "b_tier_today": state.b_tier_count,
                    },
                )

    # ── GATE 8: Same symbol/direction re-entry cooldown ───────────────────
    reentry_key = f"{symbol}:{direction}"
    now = time.monotonic()
    with _state_lock:
        last_ts = state.reentry_ts.get(reentry_key)
        if last_ts is not None:
            elapsed = now - last_ts
            remaining = cfg["reentry_cooldown_s"] - elapsed
            if remaining > 0:
                log.warning(
                    "QUALITY_MODE_BLOCKED_REENTRY | client=%s symbol=%s "
                    "direction=%s cooldown_remaining=%.0fs",
                    client_id, symbol, direction, remaining,
                )
                return QMVerdict(
                    allowed=False,
                    log_code="QUALITY_MODE_BLOCKED_REENTRY",
                    quality_mode_reason=(
                        f"same symbol/direction re-entry blocked: {symbol} {direction} "
                        f"cooldown {int(remaining)}s remaining"
                    ),
                    meta={
                        **_base_meta(score, tier, symbol, direction, timeframe, pattern),
                        "quality_mode_reason": "reentry_cooldown",
                        "gate_status": "BLOCKED_REENTRY",
                        "cooldown_remaining_s": int(remaining),
                    },
                )

    # ── GATE 9: Duplicate signal_id ───────────────────────────────────────
    if signal_id:
        with _state_lock:
            if signal_id in state.seen_signal_ids:
                log.warning(
                    "QUALITY_MODE_BLOCKED_REENTRY | client=%s symbol=%s "
                    "duplicate signal_id=%s",
                    client_id, symbol, signal_id,
                )
                return QMVerdict(
                    allowed=False,
                    log_code="QUALITY_MODE_BLOCKED_REENTRY",
                    quality_mode_reason=f"duplicate signal_id={signal_id}",
                    meta={
                        **_base_meta(score, tier, symbol, direction, timeframe, pattern),
                        "quality_mode_reason": "duplicate_signal_id",
                        "gate_status": "BLOCKED_DUPLICATE",
                        "signal_id": signal_id,
                    },
                )

    # ── GATE 10: Duplicate plan_id ────────────────────────────────────────
    if plan_id:
        with _state_lock:
            if plan_id in state.seen_plan_ids:
                log.warning(
                    "QUALITY_MODE_BLOCKED_REENTRY | client=%s symbol=%s "
                    "duplicate plan_id=%s",
                    client_id, symbol, plan_id,
                )
                return QMVerdict(
                    allowed=False,
                    log_code="QUALITY_MODE_BLOCKED_REENTRY",
                    quality_mode_reason=f"duplicate plan_id={plan_id}",
                    meta={
                        **_base_meta(score, tier, symbol, direction, timeframe, pattern),
                        "quality_mode_reason": "duplicate_plan_id",
                        "gate_status": "BLOCKED_DUPLICATE",
                        "plan_id": plan_id,
                    },
                )

    # ── ALL GATES PASSED — commit state and approve ───────────────────────
    with _state_lock:
        state.reentry_ts[reentry_key] = now
        if signal_id:
            state.seen_signal_ids[signal_id] = True
        if plan_id:
            state.seen_plan_ids[plan_id] = True
        if tier == "B":
            today = _today_utc()
            if state.b_tier_date != today:
                state.b_tier_date = today
                state.b_tier_count = 0
            state.b_tier_count += 1

    log.info(
        "QUALITY_MODE_APPROVED | client=%s symbol=%s score=%.1f tier=%s "
        "direction=%s timeframe=%s pattern=%s",
        client_id, symbol, score, tier, direction, timeframe, pattern,
    )
    return QMVerdict(
        allowed=True,
        log_code="QUALITY_MODE_APPROVED",
        quality_mode_reason="",
        meta={
            "quality_mode_blocked": False,
            "quality_mode_approved": True,
            "score":     round(score, 1),
            "tier":      tier,
            "symbol":    symbol,
            "direction": direction,
            "timeframe": timeframe,
            "pattern":   pattern,
            "gate_status": "APPROVED",
        },
    )
