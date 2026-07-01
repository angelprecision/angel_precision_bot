from __future__ import annotations

import json
import logging
from datetime import datetime, time as dt_time
from typing import Any, Mapping
from zoneinfo import ZoneInfo

from ap.entry_metadata_guard import (
    DAILY_TIMEFRAMES,
    VALID_EXECUTION_MODES,
    ZERO_UNDERLYING,
    validate_entry_metadata,
)

log = logging.getLogger("ap.master_control_metadata_guard")

ET = ZoneInfo("America/New_York")


def _parse_meta(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            decoded = json.loads(raw)
        except Exception:
            return {}
        return decoded if isinstance(decoded, dict) else {}
    return {}


def _read(source: Any, key: str) -> Any:
    if source is None:
        return None
    if isinstance(source, Mapping):
        return source.get(key)
    return getattr(source, key, None)


def _first(source: Any, *keys: str) -> str:
    candidates = [source]
    meta = _parse_meta(_read(source, "meta") or _read(source, "metadata"))
    if meta:
        candidates.append(meta)
    for candidate in candidates:
        for key in keys:
            value = _read(candidate, key)
            if str(value or "").strip():
                return str(value).strip()
    return ""


def _runtime_mode(self: Any) -> str | None:
    raw = getattr(self, "mode", None)
    normalized = str(raw or "").strip().lower()
    return normalized if normalized in VALID_EXECUTION_MODES else None


def _is_regular_session_et(now: datetime | None = None) -> bool:
    current = now or datetime.now(ET)
    return current.weekday() < 5 and dt_time(9, 30) <= current.time() < dt_time(16, 0)


def _is_daily_timeframe(signal: Any) -> bool:
    timeframe = _first(signal, "timeframe", "time_horizon").lower()
    return timeframe in DAILY_TIMEFRAMES


def should_allow_daily_handoff_without_underlying(signal: Any, *, now: datetime | None = None) -> bool:
    return _is_daily_timeframe(signal) and not _is_regular_session_et(now)


def _mark_daily_underlying_data_pending(signal: Any, reason: str) -> None:
    """Mark after-hours daily zero-underlying bypass as handoff-only.

    This keeps the carveout visible to dashboards/downstream code and makes the
    safety invariant explicit: the signal may continue into WATCHING/overnight
    hydration, but it is not execution-ready metadata yet.
    """
    marker = {
        "metadata_validation_status": "DATA_PENDING",
        "metadata_validation_reason": reason,
        "underlying_data_pending": True,
        "allowed_for_handoff": True,
        "allowed_for_execution": False,
    }

    if isinstance(signal, dict):
        signal.update(marker)

        meta = signal.get("metadata")
        if isinstance(meta, dict):
            meta.update(marker)
        else:
            signal["metadata"] = dict(marker)

        payload = signal.get("payload")
        if isinstance(payload, dict):
            payload.update(marker)

        signal_payload = signal.get("signal_payload")
        if isinstance(signal_payload, dict):
            signal_payload.update(marker)
        return

    for key, value in marker.items():
        try:
            setattr(signal, key, value)
        except Exception:
            pass


def install_master_control_metadata_guard() -> None:
    from ap_master_control import APMasterControl, ControlDecision

    if getattr(APMasterControl, "_entry_metadata_guard_installed", False):
        return

    original_evaluate = APMasterControl.evaluate

    def guarded_evaluate(self, signal, *args, **kwargs):
        client_id = kwargs.get("client_id") or getattr(self, "client_id", None)
        result = validate_entry_metadata(
            plan=signal,
            client_id=client_id,
            execution_mode=_runtime_mode(self),
        )
        if not result.ok:
            signal_id = _first(signal, "signal_id", "canonical_signal_id")
            ticker = _first(signal, "ticker", "symbol")
            reason = str(result.reason or "metadata_invalid")
            if reason == ZERO_UNDERLYING and should_allow_daily_handoff_without_underlying(signal):
                _mark_daily_underlying_data_pending(signal, reason)
                log.info(
                    "[%s] ENTRY_METADATA_DATA_PENDING | client=%s signal=%s reason=%s "
                    "action=allow_daily_handoff_without_underlying",
                    ticker or "?",
                    client_id,
                    signal_id,
                    reason,
                )
                return original_evaluate(self, signal, *args, **kwargs)
            log.warning(
                "[%s] ENTRY_METADATA_BLOCKED before contract_selection | client=%s signal=%s "
                "reason=%s details=%s",
                ticker or "?",
                client_id,
                signal_id,
                reason,
                result.details,
            )
            return ControlDecision(
                ok=False,
                stage="metadata_validation",
                reason=reason,
                reason_code=reason,
                signal_id=signal_id,
                ticker=ticker,
                client_id=str(client_id or ""),
            )
        return original_evaluate(self, signal, *args, **kwargs)

    APMasterControl._entry_metadata_guard_original_evaluate = original_evaluate
    APMasterControl.evaluate = guarded_evaluate
    APMasterControl._entry_metadata_guard_installed = True
