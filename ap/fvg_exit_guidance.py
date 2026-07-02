from __future__ import annotations

from dataclasses import dataclass
from typing import Any


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class FvgExitGuidanceDecision:
    action: str
    should_override_target_exit: bool
    should_close_at_fvg_front: bool
    reason: str
    reason_code: str
    suggested_underlying_exit: float | None = None
    diagnostics: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "should_override_target_exit": bool(self.should_override_target_exit),
            "should_close_at_fvg_front": bool(self.should_close_at_fvg_front),
            "reason": self.reason,
            "reason_code": self.reason_code,
            "suggested_underlying_exit": self.suggested_underlying_exit,
            "diagnostics": dict(self.diagnostics or {}),
        }


_NO_GUIDANCE = FvgExitGuidanceDecision(
    action="NO_GUIDANCE",
    should_override_target_exit=False,
    should_close_at_fvg_front=False,
    reason="No FVG target guidance supplied on position.",
    reason_code="NO_FVG_TARGET_GUIDANCE",
    diagnostics={},
)


def _side(pos: Any) -> str:
    return str(getattr(pos, "side", "") or "").upper().strip()


def _price_has_reached(side: str, current: float | None, level: float | None, *, tolerance_pct: float = 0.0015) -> bool:
    if current is None or level is None or level <= 0:
        return False
    tolerance = abs(level) * tolerance_pct
    if side == "CALL":
        return current >= level - tolerance
    if side == "PUT":
        return current <= level + tolerance
    return False


def _price_before_level(side: str, current: float | None, level: float | None, *, tolerance_pct: float = 0.0015) -> bool:
    if current is None or level is None or level <= 0:
        return False
    tolerance = abs(level) * tolerance_pct
    if side == "CALL":
        return current < level - tolerance
    if side == "PUT":
        return current > level + tolerance
    return False


def _target_reached(pos: Any) -> bool:
    try:
        return bool(getattr(pos, "is_at_target"))
    except Exception:
        return False


def _continuation_strong(pos: Any, *, min_option_pnl: float = 0.12) -> tuple[bool, str]:
    """Return whether holding past the scanner target is justified.

    This deliberately uses only position-local state so the exit engine does not
    fetch market data or mutate order state.  Strong means: target is already hit,
    option P&L has reached the normal trail/TP activation area, and underlying is
    still on the correct side of the original target.
    """
    side = _side(pos)
    current = _safe_float(getattr(pos, "current_underlying", None))
    target = _safe_float(getattr(pos, "underlying_target", None))
    option_pnl = _safe_float(getattr(pos, "option_pnl_pct", None))
    peak_pnl = _safe_float(getattr(pos, "peak_pnl_pct", None)) or 0.0
    max_seen = _safe_float(getattr(pos, "max_profit_seen", None)) or 0.0

    if side not in {"CALL", "PUT"}:
        return False, "unknown_side"
    if current is None or target is None:
        return False, "missing_underlying_target_or_current"
    if option_pnl is None:
        return False, "missing_option_pnl"
    if not _price_has_reached(side, current, target):
        return False, "original_target_not_reached"
    if option_pnl < min_option_pnl and max(peak_pnl, max_seen) < min_option_pnl:
        return False, f"option_pnl_below_continuation_threshold_{min_option_pnl:.2f}"
    return True, "target_hit_with_option_strength_and_underlying_continuation"


def evaluate_fvg_exit_guidance(
    pos: Any,
    guidance: dict[str, Any] | None = None,
    *,
    min_extension_option_pnl: float = 0.12,
    enabled: bool = True,
) -> FvgExitGuidanceDecision:
    """Pure FVG-aware exit policy for the exit engine.

    This is intentionally side-effect free. It does not submit/cancel orders and
    does not mutate positions.  The exit engine can call it before the normal
    TARGET HIT branch and translate the returned action into either HOLD or
    CLOSE_ALL.

    Supported guidance actions come from ap.fair_value_gap.evaluate_fvg_context:
      - extension_candidate_to_opposing_fvg_front
      - cap_before_opposing_fvg_or_block_entry
    """
    if not enabled:
        return FvgExitGuidanceDecision(
            action="DISABLED",
            should_override_target_exit=False,
            should_close_at_fvg_front=False,
            reason="FVG exit guidance disabled.",
            reason_code="FVG_EXIT_GUIDANCE_DISABLED",
            diagnostics={},
        )

    guidance = guidance if isinstance(guidance, dict) else getattr(pos, "fvg_target_guidance", None)
    if not isinstance(guidance, dict) or not guidance:
        return _NO_GUIDANCE

    action = str(guidance.get("action") or "none")
    suggested = _safe_float(guidance.get("suggested_target") or guidance.get("suggested_underlying_exit"))
    side = _side(pos)
    current = _safe_float(getattr(pos, "current_underlying", None))
    original_target = _safe_float(guidance.get("original_target") or getattr(pos, "underlying_target", None))
    option_pnl = _safe_float(getattr(pos, "option_pnl_pct", None))
    peak_pnl = _safe_float(getattr(pos, "peak_pnl_pct", None)) or 0.0
    max_seen = _safe_float(getattr(pos, "max_profit_seen", None)) or 0.0
    target_hit = _target_reached(pos)

    diagnostics = {
        "guidance_action": action,
        "side": side,
        "current_underlying": current,
        "original_target": original_target,
        "suggested_underlying_exit": suggested,
        "option_pnl_pct": option_pnl,
        "peak_pnl_pct": peak_pnl,
        "max_profit_seen": max_seen,
        "target_hit": target_hit,
    }

    if side not in {"CALL", "PUT"} or suggested is None or suggested <= 0:
        return FvgExitGuidanceDecision(
            action="INVALID_GUIDANCE",
            should_override_target_exit=False,
            should_close_at_fvg_front=False,
            reason="FVG target guidance is missing side or suggested target.",
            reason_code="INVALID_FVG_TARGET_GUIDANCE",
            suggested_underlying_exit=suggested,
            diagnostics=diagnostics,
        )

    if action == "cap_before_opposing_fvg_or_block_entry":
        if _price_has_reached(side, current, suggested):
            return FvgExitGuidanceDecision(
                action="CLOSE_AT_FVG_FRONT",
                should_override_target_exit=False,
                should_close_at_fvg_front=True,
                reason="Current underlying reached the unconfirmed opposing FVG front; close before the wall/rejection zone.",
                reason_code="FVG_FRONT_TARGET_EXIT",
                suggested_underlying_exit=suggested,
                diagnostics=diagnostics,
            )
        return FvgExitGuidanceDecision(
            action="HOLD_UNTIL_FVG_FRONT_OR_NORMAL_EXIT",
            should_override_target_exit=False,
            should_close_at_fvg_front=False,
            reason="FVG cap guidance present but current price has not reached the FVG front.",
            reason_code="FVG_FRONT_NOT_REACHED",
            suggested_underlying_exit=suggested,
            diagnostics=diagnostics,
        )

    if action == "extension_candidate_to_opposing_fvg_front":
        if _price_has_reached(side, current, suggested):
            return FvgExitGuidanceDecision(
                action="CLOSE_AT_FVG_FRONT",
                should_override_target_exit=False,
                should_close_at_fvg_front=True,
                reason="Current underlying reached the FVG magnet/front; exit near resistance/support before possible rejection.",
                reason_code="FVG_EXTENSION_TARGET_EXIT",
                suggested_underlying_exit=suggested,
                diagnostics=diagnostics,
            )

        strong, strong_reason = _continuation_strong(pos, min_option_pnl=min_extension_option_pnl)
        diagnostics["continuation_strong"] = strong
        diagnostics["continuation_reason"] = strong_reason

        if target_hit and strong and _price_before_level(side, current, suggested):
            return FvgExitGuidanceDecision(
                action="HOLD_FOR_FVG_EXTENSION",
                should_override_target_exit=True,
                should_close_at_fvg_front=False,
                reason="Original target hit with strong continuation; hold toward FVG magnet/front and let normal protective stops/trails still work.",
                reason_code="FVG_HOLD_FOR_EXTENSION",
                suggested_underlying_exit=suggested,
                diagnostics=diagnostics,
            )

        return FvgExitGuidanceDecision(
            action="KEEP_ORIGINAL_TARGET",
            should_override_target_exit=False,
            should_close_at_fvg_front=False,
            reason="FVG extension exists, but continuation is not strong enough to override normal target exit.",
            reason_code="FVG_EXTENSION_NOT_CONFIRMED",
            suggested_underlying_exit=suggested,
            diagnostics=diagnostics,
        )

    return FvgExitGuidanceDecision(
        action="NO_ACTIONABLE_GUIDANCE",
        should_override_target_exit=False,
        should_close_at_fvg_front=False,
        reason="FVG target guidance action is not actionable for exits.",
        reason_code="NO_ACTIONABLE_FVG_GUIDANCE",
        suggested_underlying_exit=suggested,
        diagnostics=diagnostics,
    )
