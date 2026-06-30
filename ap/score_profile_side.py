from __future__ import annotations

from typing import Any

CALL_ALIASES = {"CALL", "BUY", "LONG", "BULL", "BULLISH", "CALLS"}
PUT_ALIASES = {"PUT", "SELL", "SHORT", "BEAR", "BEARISH", "PUTS"}
KNOWN_SIDES = {"CALL", "PUT", "UNKNOWN"}


def normalize_signal_side(value: Any) -> str:
    """Return CALL, PUT, or UNKNOWN without inventing bullish fallback state."""

    raw = str(value or "").upper().strip()
    if raw in CALL_ALIASES:
        return "CALL"
    if raw in PUT_ALIASES:
        return "PUT"
    return "UNKNOWN"


def side_missing(side: str) -> bool:
    return normalize_signal_side(side) == "UNKNOWN"
