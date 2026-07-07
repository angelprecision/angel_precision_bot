"""P0 live submit safety helpers.

These helpers are deliberately small and side-effect free. They provide the
blocking decisions needed for the META/VZ incident shape without changing
scanner, selector, or scoring behavior.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

_VALID_MODES = {"live", "paper"}


@dataclass(frozen=True)
class SubmitSafetyDecision:
    ok: bool
    reason: str = ""
    detail: str = ""


def normalize_execution_mode(value: Any) -> str | None:
    mode = str(value or "").strip().lower()
    return mode if mode in _VALID_MODES else None


def require_live_identity(*, client_id: Any, execution_mode: Any) -> SubmitSafetyDecision:
    """Fail closed for live submits with missing identity or unsafe mode."""
    cid = str(client_id or "").strip()
    mode = normalize_execution_mode(execution_mode)
    if not cid:
        return SubmitSafetyDecision(False, "LIVE_SUBMIT_BLOCK_MISSING_CLIENT_ID")
    if mode != "live":
        return SubmitSafetyDecision(False, "LIVE_SUBMIT_BLOCK_BLANK_OR_UNKNOWN_EXECUTION_MODE")
    return SubmitSafetyDecision(True)


def require_fresh_trigger(*, trigger_crossed_at: Any, max_age_seconds: float = 120.0) -> SubmitSafetyDecision:
    """Block delayed submits after the breach is stale."""
    if not trigger_crossed_at:
        return SubmitSafetyDecision(False, "LIVE_SUBMIT_BLOCK_MISSING_TRIGGER_CROSSED_AT")
    try:
        if isinstance(trigger_crossed_at, datetime):
            ts = trigger_crossed_at
        else:
            ts = datetime.fromisoformat(str(trigger_crossed_at).replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age = (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds()
    except Exception as exc:
        return SubmitSafetyDecision(False, "LIVE_SUBMIT_BLOCK_BAD_TRIGGER_CROSSED_AT", str(exc))
    if age > float(max_age_seconds):
        return SubmitSafetyDecision(False, "STALE_TRIGGER_BREACH", f"age={age:.1f}s max={float(max_age_seconds):.1f}s")
    return SubmitSafetyDecision(True)


def require_current_quote(*, bid: Any, ask: Any, last: Any = None) -> tuple[SubmitSafetyDecision, float]:
    """Return decision plus mid/last fallback. LIVE callers should fail closed on not ok."""
    try:
        b = float(bid or 0)
        a = float(ask or 0)
        l = float(last or 0)
    except Exception as exc:
        return SubmitSafetyDecision(False, "LIVE_SUBMIT_BLOCK_BAD_UNDERLYING_QUOTE", str(exc)), 0.0
    if b <= 0 and a <= 0 and l <= 0:
        return SubmitSafetyDecision(False, "LIVE_SUBMIT_BLOCK_MISSING_UNDERLYING_QUOTE"), 0.0
    if b > 0 and a > 0:
        return SubmitSafetyDecision(True), (b + a) / 2.0
    return SubmitSafetyDecision(True), max(b, a, l)


def require_remaining_opportunity(*, side: str, current_price: Any, trigger_price: Any, target_price: Any, min_remaining_fraction: float = 0.25) -> SubmitSafetyDecision:
    """Block trades whose trigger move has already been consumed."""
    try:
        cur = float(current_price)
        trig = float(trigger_price)
        tgt = float(target_price)
    except Exception as exc:
        return SubmitSafetyDecision(False, "LIVE_SUBMIT_BLOCK_BAD_TRIGGER_TARGET_PRICE", str(exc))
    if cur <= 0 or trig <= 0 or tgt <= 0:
        return SubmitSafetyDecision(False, "LIVE_SUBMIT_BLOCK_MISSING_TRIGGER_TARGET_PRICE")
    side_u = str(side or "").upper()
    original = abs(tgt - trig)
    if original <= 0:
        return SubmitSafetyDecision(False, "LIVE_SUBMIT_BLOCK_ZERO_TRIGGER_TARGET_DISTANCE")
    if side_u == "CALL":
        if cur < trig:
            return SubmitSafetyDecision(False, "CALL_NOT_STILL_ABOVE_TRIGGER")
        if cur >= tgt:
            return SubmitSafetyDecision(False, "TARGET_ALREADY_INVALID")
        remaining = tgt - cur
    elif side_u == "PUT":
        if cur > trig:
            return SubmitSafetyDecision(False, "PUT_NOT_STILL_BELOW_TRIGGER")
        if cur <= tgt:
            return SubmitSafetyDecision(False, "TARGET_ALREADY_INVALID")
        remaining = cur - tgt
    else:
        return SubmitSafetyDecision(False, "LIVE_SUBMIT_BLOCK_UNKNOWN_SIDE")
    if remaining < original * float(min_remaining_fraction):
        return SubmitSafetyDecision(False, "REMAINING_OPPORTUNITY_TOO_SMALL", f"remaining={remaining:.4f} original={original:.4f}")
    return SubmitSafetyDecision(True)
