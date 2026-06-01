"""
ap_quality_mode.py — Angel Precision Quality Mode Gate (PR-72)

Enforces a strict signal-quality floor for paper/client-proof operation.
All blocks are non-recoverable within the same evaluate() call — the gate
returns a structured QMVerdict that APMasterControl wraps into a
ControlDecision via _block().

Source of truth for quality_mode_result: this module builds it on every
path (disabled / approved / blocked). Consumers (PR-73 dashboard, proof
logger, rejection feed) read meta.quality_mode_result and
score_audit.quality_mode_result. They do NOT recompute QM outcome.

Do NOT import broker, exit, or pricing modules here. This module is
intentionally thin: read env → evaluate signal → return verdict.

Environment variables (all read once at import time):
    QUALITY_MODE_ENABLED                 bool  default false
    QUALITY_MODE_SCORE_MIN               float default 70
    QUALITY_MODE_MAX_DAILY_TRADES        int   default 5
    QUALITY_MODE_MAX_POSITIONS           int   default 4
    QUALITY_MODE_MAX_B_TIER_TRADES       int   default 2
    SAME_SYMBOL_REENTRY_COOLDOWN_MINUTES int   default 60
    PYRAMIDING_ENABLED                   bool  default false
"""

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from typing import List, Optional

log = logging.getLogger("ap.quality_mode")

# ---------------------------------------------------------------------------
# Config — read once at import; use reset_config() in tests after env patch.
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
        "enabled":            _bool_env("QUALITY_MODE_ENABLED", False),
        "score_min":          float(os.getenv("QUALITY_MODE_SCORE_MIN", "70")),
        "max_daily_trades":   int(os.getenv("QUALITY_MODE_MAX_DAILY_TRADES", "5")),
        "max_positions":      int(os.getenv("QUALITY_MODE_MAX_POSITIONS", "4")),
        "max_b_tier_trades":  int(os.getenv("QUALITY_MODE_MAX_B_TIER_TRADES", "2")),
        "reentry_cooldown_s": int(os.getenv("SAME_SYMBOL_REENTRY_COOLDOWN_MINUTES", "60")) * 60,
        "pyramiding_enabled": _bool_env("PYRAMIDING_ENABLED", False),
    }


_cfg: dict = _load_config()
_cfg_lock = threading.Lock()


def reset_config() -> None:
    """Re-read all envs. Call in tests after patching os.environ."""
    global _cfg
    with _cfg_lock:
        _cfg = _load_config()


def get_config() -> dict:
    with _cfg_lock:
        return dict(_cfg)


# ---------------------------------------------------------------------------
# quality_mode_result schema
#
# This is the canonical shape emitted by this module and read by all
# consumers (PR-73 dashboard, proof logger, rejection feed, score_audit).
# Any field not applicable on a given path is set to None, not omitted,
# so consumers can rely on key presence.
#
#   {
#     "enabled":       bool          — whether QM was active this evaluation
#     "approved":      bool | None   — True=approved, False=blocked, None=disabled
#     "reason":        str | None    — human-readable verdict reason
#     "block_code":    str | None    — structured log token (QUALITY_MODE_*)
#     "score":         float | None  — effective score evaluated
#     "score_min":     float | None  — configured minimum (None when disabled)
#     "tier":          str | None    — A+/A/B/SHADOW/REJECT
#     "gate_status":   str | None    — APPROVED / BLOCKED_* / DISABLED
#     "gates_applied": list[str]     — ordered list of gate names that ran
#     "blocked_by":    str | None    — name of first gate that blocked, or None
#   }
# ---------------------------------------------------------------------------

def _build_result(
    *,
    enabled: bool,
    approved: Optional[bool],
    reason: Optional[str],
    block_code: Optional[str],
    score: Optional[float],
    score_min: Optional[float],
    tier: Optional[str],
    gate_status: Optional[str],
    gates_applied: List[str],
    blocked_by: Optional[str],
) -> dict:
    """Construct a normalized quality_mode_result dict."""
    return {
        "enabled":       enabled,
        "approved":      approved,
        "reason":        reason,
        "block_code":    block_code,
        "score":         round(score, 1) if score is not None else None,
        "score_min":     score_min,
        "tier":          tier,
        "gate_status":   gate_status,
        "gates_applied": gates_applied,
        "blocked_by":    blocked_by,
    }


def build_disabled_result() -> dict:
    """
    Return the canonical quality_mode_result when Quality Mode is disabled.
    Called by ap_master_control when QUALITY_MODE_ENABLED=false so that
    meta.quality_mode_result and score_audit.quality_mode_result are always
    populated (never absent), enabling PR-73 to show null-safe output.
    """
    return _build_result(
        enabled=False,
        approved=None,
        reason=None,
        block_code=None,
        score=None,
        score_min=None,
        tier=None,
        gate_status="DISABLED",
        gates_applied=[],
        blocked_by=None,
    )


# ---------------------------------------------------------------------------
# QMVerdict — returned by check()
# ---------------------------------------------------------------------------

@dataclass
class QMVerdict:
    allowed: bool
    log_code: str                        # structured log token (QUALITY_MODE_*)
    quality_mode_reason: str             # human-readable
    quality_mode_result: dict = field(default_factory=dict)  # canonical result object
    meta: dict = field(default_factory=dict)                 # merged into orders.meta


# ---------------------------------------------------------------------------
# Per-process state (mirrors APMasterControl._seen_signals pattern).
# Keyed by client_id — each client has independent counters.
# State is in-process; the authoritative source is the Postgres snapshot.
# ---------------------------------------------------------------------------

@dataclass
class _ClientState:
    reentry_ts: dict = field(default_factory=dict)      # "symbol:direction" -> monotonic ts
    seen_signal_ids: dict = field(default_factory=dict)  # signal_id -> True
    seen_plan_ids: dict = field(default_factory=dict)    # plan_id   -> True
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


def _blocked_verdict(
    *,
    log_code: str,
    reason: str,
    blocked_by: str,
    score: float,
    score_min: float,
    tier: str,
    gate_status: str,
    gates_applied: List[str],
    symbol: str,
    direction: str,
    timeframe: str,
    pattern: str,
    extra_meta: Optional[dict] = None,
) -> QMVerdict:
    """Construct a uniform blocked QMVerdict."""
    qm_result = _build_result(
        enabled=True,
        approved=False,
        reason=reason,
        block_code=log_code,
        score=score,
        score_min=score_min,
        tier=tier,
        gate_status=gate_status,
        gates_applied=gates_applied,
        blocked_by=blocked_by,
    )
    meta = {
        "quality_mode_blocked": True,
        "quality_mode_reason":  reason,
        "quality_mode_result":  qm_result,
        "score":     round(score, 1),
        "tier":      tier,
        "symbol":    symbol,
        "direction": direction,
        "timeframe": timeframe,
        "pattern":   pattern,
        "gate_status": gate_status,
    }
    if extra_meta:
        meta.update(extra_meta)
    return QMVerdict(
        allowed=False,
        log_code=log_code,
        quality_mode_reason=reason,
        quality_mode_result=qm_result,
        meta=meta,
    )


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

# Ordered gate names — used for gates_applied tracking.
_GATE_NAMES = [
    "score_floor",
    "skip_override",
    "risk_veto_override",
    "score65_band",
    "max_daily_trades",
    "max_positions",
    "b_tier_daily_cap",
    "reentry_cooldown",
    "duplicate_signal_id",
    "duplicate_plan_id",
]


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
    signal            : raw signal dict (keys: score, direction/side,
                        symbol/ticker, timeframe, pattern, signal_id, plan_id)
    client_id         : client email / identifier
    intel_status      : intel_status string from intelligence_bridge (may be None)
    daily_trades_today: trades the client completed today (from DB snapshot)
    open_positions    : current open + pending position count (from DB snapshot)
    approved_score    : final effective score that will be approved (float)

    Returns
    -------
    QMVerdict with:
      - allowed           bool
      - log_code          structured token
      - quality_mode_reason  human-readable
      - quality_mode_result  canonical dict (always populated)
      - meta              dict merged into orders.meta
    """
    cfg = get_config()

    if not cfg["enabled"]:
        qm_result = build_disabled_result()
        return QMVerdict(
            allowed=True,
            log_code="QUALITY_MODE_DISABLED",
            quality_mode_reason="",
            quality_mode_result=qm_result,
            meta={"quality_mode_result": qm_result},
        )

    score     = approved_score
    symbol    = str(signal.get("symbol") or signal.get("ticker") or "").upper()
    direction = str(signal.get("direction") or signal.get("side") or "").upper()
    timeframe = str(signal.get("timeframe") or "")
    pattern   = str(signal.get("pattern") or signal.get("pattern_id") or "")
    signal_id = str(signal.get("signal_id") or "")
    plan_id   = str(signal.get("plan_id") or "")
    tier      = _tier_from_score(score)
    score_min = cfg["score_min"]

    # Track which gates were evaluated (in order) before a block or approval.
    gates_applied: List[str] = []

    # ── GATE 1: Hard score floor ──────────────────────────────────────────
    gates_applied.append("score_floor")
    if score < score_min:
        reason = f"score {score:.1f} < quality_mode_min {score_min:.1f}"
        log.warning(
            "QUALITY_MODE_BLOCKED_SCORE | client=%s symbol=%s score=%.1f "
            "min=%.1f tier=%s direction=%s timeframe=%s pattern=%s",
            client_id, symbol, score, score_min, tier, direction, timeframe, pattern,
        )
        return _blocked_verdict(
            log_code="QUALITY_MODE_BLOCKED_SCORE",
            reason=reason,
            blocked_by="score_floor",
            score=score, score_min=score_min, tier=tier,
            gate_status="BLOCKED_SCORE",
            gates_applied=list(gates_applied),
            symbol=symbol, direction=direction, timeframe=timeframe, pattern=pattern,
            extra_meta={"quality_mode_reason": f"score_below_floor ({score:.1f}<{score_min:.1f})"},
        )

    # ── GATE 2: Block SKIP_OVERRIDE ───────────────────────────────────────
    gates_applied.append("skip_override")
    if intel_status == "SKIP_OVERRIDE":
        reason = "SKIP_OVERRIDE entries blocked in quality mode"
        log.warning(
            "QUALITY_MODE_BLOCKED_OVERRIDE | client=%s symbol=%s "
            "intel_status=SKIP_OVERRIDE score=%.1f tier=%s",
            client_id, symbol, score, tier,
        )
        return _blocked_verdict(
            log_code="QUALITY_MODE_BLOCKED_OVERRIDE",
            reason=reason,
            blocked_by="skip_override",
            score=score, score_min=score_min, tier=tier,
            gate_status="BLOCKED_OVERRIDE",
            gates_applied=list(gates_applied),
            symbol=symbol, direction=direction, timeframe=timeframe, pattern=pattern,
            extra_meta={"intel_status": "SKIP_OVERRIDE"},
        )

    # ── GATE 3: Block RISK_VETO_OVERRIDE unless score >= 78 AND tier A/A+ ─
    gates_applied.append("risk_veto_override")
    if intel_status == "RISK_VETO_OVERRIDE":
        _rvo_tier_ok  = tier in ("A", "A+")
        _rvo_score_ok = score >= 78.0
        if not (_rvo_score_ok and _rvo_tier_ok):
            reason = (
                f"RISK_VETO_OVERRIDE blocked: score={score:.1f} tier={tier} "
                f"(requires score>=78 AND tier=A or A+)"
            )
            log.warning(
                "QUALITY_MODE_BLOCKED_OVERRIDE | client=%s symbol=%s "
                "intel_status=RISK_VETO_OVERRIDE score=%.1f tier=%s "
                "(requires score>=78 and tier=A/A+)",
                client_id, symbol, score, tier,
            )
            return _blocked_verdict(
                log_code="QUALITY_MODE_BLOCKED_OVERRIDE",
                reason=reason,
                blocked_by="risk_veto_override",
                score=score, score_min=score_min, tier=tier,
                gate_status="BLOCKED_OVERRIDE",
                gates_applied=list(gates_applied),
                symbol=symbol, direction=direction, timeframe=timeframe, pattern=pattern,
                extra_meta={"intel_status": "RISK_VETO_OVERRIDE"},
            )

    # ── GATE 4: Block score 65–69 even if SCORE65_ALLOW=true ─────────────
    # Gate 1 already blocked < score_min (70). This gate is explicit for the
    # 65–69 band so the block_code and blocked_by are unambiguous in audits.
    gates_applied.append("score65_band")
    if 65.0 <= score < 70.0:
        reason = (
            f"score {score:.1f} in 65-69 band: "
            "blocked in quality mode regardless of SCORE65_ALLOW"
        )
        log.warning(
            "QUALITY_MODE_BLOCKED_SCORE | client=%s symbol=%s score=%.1f "
            "in 65-69 band — blocked regardless of SCORE65_ALLOW",
            client_id, symbol, score,
        )
        return _blocked_verdict(
            log_code="QUALITY_MODE_BLOCKED_SCORE",
            reason=reason,
            blocked_by="score65_band",
            score=score, score_min=score_min, tier=tier,
            gate_status="BLOCKED_SCORE",
            gates_applied=list(gates_applied),
            symbol=symbol, direction=direction, timeframe=timeframe, pattern=pattern,
        )

    # ── GATE 5: Max daily trades per client ───────────────────────────────
    gates_applied.append("max_daily_trades")
    if daily_trades_today >= cfg["max_daily_trades"]:
        reason = (
            f"max daily trades reached "
            f"({daily_trades_today}/{cfg['max_daily_trades']})"
        )
        log.warning(
            "QUALITY_MODE_BLOCKED_SCORE | client=%s symbol=%s "
            "daily_trades=%d max=%d",
            client_id, symbol, daily_trades_today, cfg["max_daily_trades"],
        )
        return _blocked_verdict(
            log_code="QUALITY_MODE_BLOCKED_SCORE",
            reason=reason,
            blocked_by="max_daily_trades",
            score=score, score_min=score_min, tier=tier,
            gate_status="BLOCKED_DAILY_LIMIT",
            gates_applied=list(gates_applied),
            symbol=symbol, direction=direction, timeframe=timeframe, pattern=pattern,
        )

    # ── GATE 6: Max concurrent positions per client ───────────────────────
    gates_applied.append("max_positions")
    if open_positions >= cfg["max_positions"]:
        reason = (
            f"max concurrent positions reached "
            f"({open_positions}/{cfg['max_positions']})"
        )
        log.warning(
            "QUALITY_MODE_BLOCKED_SCORE | client=%s symbol=%s "
            "open_positions=%d max=%d",
            client_id, symbol, open_positions, cfg["max_positions"],
        )
        return _blocked_verdict(
            log_code="QUALITY_MODE_BLOCKED_SCORE",
            reason=reason,
            blocked_by="max_positions",
            score=score, score_min=score_min, tier=tier,
            gate_status="BLOCKED_POSITION_LIMIT",
            gates_applied=list(gates_applied),
            symbol=symbol, direction=direction, timeframe=timeframe, pattern=pattern,
        )

    state = _get_state(client_id)

    # ── GATE 7: Max B-tier trades per client per day ──────────────────────
    gates_applied.append("b_tier_daily_cap")
    if tier == "B":
        today = _today_utc()
        with _state_lock:
            if state.b_tier_date != today:
                state.b_tier_date = today
                state.b_tier_count = 0
            if state.b_tier_count >= cfg["max_b_tier_trades"]:
                reason = (
                    f"B-tier daily limit reached "
                    f"({state.b_tier_count}/{cfg['max_b_tier_trades']})"
                )
                log.warning(
                    "QUALITY_MODE_BLOCKED_B_TIER_LIMIT | client=%s symbol=%s "
                    "b_tier_today=%d max=%d score=%.1f",
                    client_id, symbol, state.b_tier_count, cfg["max_b_tier_trades"], score,
                )
                return _blocked_verdict(
                    log_code="QUALITY_MODE_BLOCKED_B_TIER_LIMIT",
                    reason=reason,
                    blocked_by="b_tier_daily_cap",
                    score=score, score_min=score_min, tier=tier,
                    gate_status="BLOCKED_B_TIER_LIMIT",
                    gates_applied=list(gates_applied),
                    symbol=symbol, direction=direction, timeframe=timeframe, pattern=pattern,
                    extra_meta={"b_tier_today": state.b_tier_count},
                )

    # ── GATE 8: Same symbol/direction re-entry cooldown ───────────────────
    gates_applied.append("reentry_cooldown")
    reentry_key = f"{symbol}:{direction}"
    now = time.monotonic()
    with _state_lock:
        last_ts = state.reentry_ts.get(reentry_key)
        if last_ts is not None:
            elapsed = now - last_ts
            remaining = cfg["reentry_cooldown_s"] - elapsed
            if remaining > 0:
                reason = (
                    f"same symbol/direction re-entry blocked: {symbol} {direction} "
                    f"cooldown {int(remaining)}s remaining"
                )
                log.warning(
                    "QUALITY_MODE_BLOCKED_REENTRY | client=%s symbol=%s "
                    "direction=%s cooldown_remaining=%.0fs",
                    client_id, symbol, direction, remaining,
                )
                return _blocked_verdict(
                    log_code="QUALITY_MODE_BLOCKED_REENTRY",
                    reason=reason,
                    blocked_by="reentry_cooldown",
                    score=score, score_min=score_min, tier=tier,
                    gate_status="BLOCKED_REENTRY",
                    gates_applied=list(gates_applied),
                    symbol=symbol, direction=direction, timeframe=timeframe, pattern=pattern,
                    extra_meta={"cooldown_remaining_s": int(remaining)},
                )

    # ── GATE 9: Duplicate signal_id ───────────────────────────────────────
    gates_applied.append("duplicate_signal_id")
    if signal_id:
        with _state_lock:
            if signal_id in state.seen_signal_ids:
                reason = f"duplicate signal_id={signal_id}"
                log.warning(
                    "QUALITY_MODE_BLOCKED_REENTRY | client=%s symbol=%s "
                    "duplicate signal_id=%s",
                    client_id, symbol, signal_id,
                )
                return _blocked_verdict(
                    log_code="QUALITY_MODE_BLOCKED_REENTRY",
                    reason=reason,
                    blocked_by="duplicate_signal_id",
                    score=score, score_min=score_min, tier=tier,
                    gate_status="BLOCKED_DUPLICATE",
                    gates_applied=list(gates_applied),
                    symbol=symbol, direction=direction, timeframe=timeframe, pattern=pattern,
                    extra_meta={"signal_id": signal_id},
                )

    # ── GATE 10: Duplicate plan_id ────────────────────────────────────────
    gates_applied.append("duplicate_plan_id")
    if plan_id:
        with _state_lock:
            if plan_id in state.seen_plan_ids:
                reason = f"duplicate plan_id={plan_id}"
                log.warning(
                    "QUALITY_MODE_BLOCKED_REENTRY | client=%s symbol=%s "
                    "duplicate plan_id=%s",
                    client_id, symbol, plan_id,
                )
                return _blocked_verdict(
                    log_code="QUALITY_MODE_BLOCKED_REENTRY",
                    reason=reason,
                    blocked_by="duplicate_plan_id",
                    score=score, score_min=score_min, tier=tier,
                    gate_status="BLOCKED_DUPLICATE",
                    gates_applied=list(gates_applied),
                    symbol=symbol, direction=direction, timeframe=timeframe, pattern=pattern,
                    extra_meta={"plan_id": plan_id},
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

    qm_result = _build_result(
        enabled=True,
        approved=True,
        reason=None,
        block_code=None,
        score=score,
        score_min=score_min,
        tier=tier,
        gate_status="APPROVED",
        gates_applied=list(gates_applied),
        blocked_by=None,
    )

    log.info(
        "QUALITY_MODE_APPROVED | client=%s symbol=%s score=%.1f tier=%s "
        "direction=%s timeframe=%s pattern=%s gates_applied=%s",
        client_id, symbol, score, tier, direction, timeframe, pattern,
        ",".join(gates_applied),
    )
    return QMVerdict(
        allowed=True,
        log_code="QUALITY_MODE_APPROVED",
        quality_mode_reason="",
        quality_mode_result=qm_result,
        meta={
            "quality_mode_blocked":  False,
            "quality_mode_approved": True,
            "quality_mode_result":   qm_result,
            "score":      round(score, 1),
            "tier":       tier,
            "symbol":     symbol,
            "direction":  direction,
            "timeframe":  timeframe,
            "pattern":    pattern,
            "gate_status": "APPROVED",
        },
    )
