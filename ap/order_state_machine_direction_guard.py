from __future__ import annotations

from typing import Any

_ENTRY_SIDE_ALIASES = {
    "BUY": "CALL",
    "LONG": "CALL",
    "CALLS": "CALL",
    "BULLISH": "CALL",
    "SELL": "PUT",
    "SHORT": "PUT",
    "PUTS": "PUT",
    "BEARISH": "PUT",
}


def normalize_entry_direction(plan: Any) -> str:
    raw = getattr(plan, "side", None)
    if raw is None or str(raw or "").strip() == "":
        raw = getattr(plan, "direction", None)
    raw_side = str(raw or "").upper().strip()
    direction = _ENTRY_SIDE_ALIASES.get(raw_side, raw_side)
    if direction in {"CALL", "PUT"}:
        return direction
    raise ValueError(f"invalid_or_missing_entry_direction:{raw_side!r}")
