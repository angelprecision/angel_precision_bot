from __future__ import annotations

import os
from typing import Any

_TRUE_VALUES = {"1", "true", "yes", "on"}


def failed_dir_enabled() -> bool:
    return os.getenv("FAILED_DIR_ENABLED", "0").strip().lower() in _TRUE_VALUES


def payload_pattern_id(payload: dict | None) -> str:
    if not isinstance(payload, dict):
        return ""
    return str(
        payload.get("pattern_id")
        or payload.get("pattern")
        or payload.get("strat_pattern")
        or ""
    ).strip()


def is_failed_dir_pattern(payload: dict | None) -> bool:
    return payload_pattern_id(payload).upper().startswith("FAILED_DIR")


def is_failed_dir_blocked(payload: dict | None) -> bool:
    return (not failed_dir_enabled()) and is_failed_dir_pattern(payload)


def failed_dir_block_result(
    payload: dict,
    *,
    client_id: str,
    execution_mode: str,
    signal_id: str,
    ticker: str,
) -> dict[str, Any]:
    return {
        "stage": "admission",
        "reason": "blocked_pattern_failed_dir_disabled",
        "reason_code": "BLOCKED_PATTERN_FAILED_DIR_DISABLED",
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": signal_id,
        "ticker": ticker,
        "side": payload.get("side") or payload.get("direction"),
        "pattern_id": payload_pattern_id(payload),
        "source_scanner": payload.get("source_scanner"),
        "score": payload.get("score"),
        "backtest_match_source": payload.get("backtest_match_source"),
        "blocked_by_env": "FAILED_DIR_ENABLED=0",
    }
