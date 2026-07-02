"""
Chart screenshot memory confirmation.

This module is intentionally safe-by-default:
- no broker submit
- no broker cancel
- no order mutation
- no position mutation
- no proof_trades mutation
- no queue mutation

It only reads/writes chart confirmation memory and returns an allow/block decision.
Actual submit blocking only happens when both flags are enabled by the caller:
  CHART_VISION_CONFIRMATION_ENABLED=true
  CHART_VISION_BLOCK_SUBMIT=true
"""

from __future__ import annotations

import os
import uuid
import logging
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger(__name__)

SAFE_PASS_STATES = {
    "continuation_confirmed",
    "clean_reclaim",
    "fresh_intraday_high",
    "fresh_intraday_low",
    "clean_rejection",
}

BLOCK_STATES = {
    "late_chase",
    "failed_reclaim",
    "failed_rejection",
    "chop",
    "below_trigger",
    "above_trigger",
    "wick_only",
    "unclear",
    "missing_chart",
}

VALID_EXECUTION_MODES = {"paper", "live"}
VALID_SIDES = {"CALL", "PUT"}


def _env_true(name: str, default: str = "false") -> bool:
    return str(os.getenv(name, default)).strip().lower() in {"1", "true", "yes", "on"}


def chart_memory_enabled() -> bool:
    return _env_true("CHART_VISION_CONFIRMATION_ENABLED", "false")


def chart_memory_blocks_submit() -> bool:
    return _env_true("CHART_VISION_BLOCK_SUBMIT", "false")


def normalize_chart_state(value: Any) -> str:
    if not value:
        return "unclear"
    state = str(value).strip().lower()
    if state in SAFE_PASS_STATES:
        return state
    if state in BLOCK_STATES:
        return state
    return "unclear"


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    if result <= 0:
        return None
    return result


def _required_text(value: Any, field_name: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError(f"missing_{field_name}")
    return text


def build_chart_memory_payload(
    *,
    client_id: str,
    execution_mode: str,
    signal_id: str,
    ticker: str,
    side: str,
    timeframe: str,
    trigger_level: Any = None,
    current_price: Any = None,
    screenshot_url: str | None = None,
    chart_state: str = "unclear",
    reason: str = "",
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a production-shaped diagnostic memory payload.

    The payload preserves client_id and execution_mode so paper/live memory cannot
    bleed together. It intentionally does not infer those values from metadata.
    """

    clean_client_id = _required_text(client_id, "client_id")
    clean_execution_mode = _required_text(execution_mode, "execution_mode").lower()
    if clean_execution_mode not in VALID_EXECUTION_MODES:
        raise ValueError("invalid_execution_mode")

    clean_signal_id = _required_text(signal_id, "signal_id")
    clean_ticker = _required_text(ticker, "ticker").upper()
    clean_side = _required_text(side, "side").upper()
    if clean_side not in VALID_SIDES:
        raise ValueError("invalid_side")

    clean_timeframe = _required_text(timeframe, "timeframe").lower()

    return {
        "id": str(uuid.uuid4()),
        "client_id": clean_client_id,
        "execution_mode": clean_execution_mode,
        "signal_id": clean_signal_id,
        "ticker": clean_ticker,
        "side": clean_side,
        "timeframe": clean_timeframe,
        "trigger_level": _safe_float(trigger_level),
        "current_price": _safe_float(current_price),
        "screenshot_url": screenshot_url or None,
        "chart_state": normalize_chart_state(chart_state),
        "reason": str(reason or ""),
        "metadata": metadata or {},
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


def save_chart_memory_row(supabase: Any, payload: dict[str, Any]) -> dict[str, Any]:
    """Persist one diagnostic chart-memory row.

    This is append-only memory. It does not update orders, positions, trade_queue,
    proof_trades, or broker state.
    """

    supabase.table("chart_memory_confirmations").insert(payload).execute()
    return payload


def latest_chart_memory_for_signal(
    supabase: Any,
    *,
    client_id: str,
    execution_mode: str,
    signal_id: str,
) -> dict[str, Any] | None:
    clean_client_id = _required_text(client_id, "client_id")
    clean_execution_mode = _required_text(execution_mode, "execution_mode").lower()
    clean_signal_id = _required_text(signal_id, "signal_id")

    result = (
        supabase.table("chart_memory_confirmations")
        .select("*")
        .eq("client_id", clean_client_id)
        .eq("execution_mode", clean_execution_mode)
        .eq("signal_id", clean_signal_id)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = getattr(result, "data", None) or []
    return rows[0] if rows else None


def should_allow_entry_from_chart_memory(
    supabase: Any,
    *,
    client_id: str,
    execution_mode: str,
    signal_id: str,
) -> tuple[bool, str, dict[str, Any] | None]:
    """Return (allowed, reason, latest_memory_row).

    Flag behavior:
    - confirmation disabled: allow, no read required
    - confirmation enabled + block disabled: observe-only allow
    - confirmation enabled + block enabled: require latest safe chart state
    """

    if not chart_memory_enabled():
        return True, "chart_memory_disabled", None

    row = latest_chart_memory_for_signal(
        supabase,
        client_id=client_id,
        execution_mode=execution_mode,
        signal_id=signal_id,
    )

    if not row:
        if chart_memory_blocks_submit():
            return False, "chart_memory_missing_blocked", None
        return True, "chart_memory_missing_observe_only", None

    state = normalize_chart_state(row.get("chart_state"))
    if state in SAFE_PASS_STATES:
        return True, f"chart_memory_pass:{state}", row

    if chart_memory_blocks_submit():
        return False, f"chart_memory_block:{state}", row

    return True, f"chart_memory_observe_only:{state}", row
