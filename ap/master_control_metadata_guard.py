from __future__ import annotations

import json
import logging
from typing import Any, Mapping

from ap.entry_metadata_guard import validate_entry_metadata

log = logging.getLogger("ap.master_control_metadata_guard")


def _parse_meta(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            decoded = json.loads(raw)
            return decoded if isinstance(decoded, dict) else {}
        except Exception:
            return {}
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


def install_master_control_metadata_guard() -> None:
    """Fail closed before contract selection when signal metadata is invalid."""
    from ap_master_control import APMasterControl, ControlDecision

    if getattr(APMasterControl, "_entry_metadata_guard_installed", False):
        return

    original_evaluate = APMasterControl.evaluate

    def guarded_evaluate(self, signal, *args, **kwargs):
        client_id = kwargs.get("client_id") or getattr(self, "client_id", None)
        result = validate_entry_metadata(
            plan=signal,
            client_id=client_id,
            execution_mode=None,  # do not infer/default; require signal-carried mode
        )
        if not result.ok:
            signal_id = _first(signal, "signal_id", "canonical_signal_id")
            ticker = _first(signal, "ticker", "symbol")
            reason = str(result.reason or "metadata_invalid")
            log.warning(
                "[%s] ENTRY_METADATA_BLOCKED before contract_selection | client=%s signal=%s reason=%s details=%s",
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
