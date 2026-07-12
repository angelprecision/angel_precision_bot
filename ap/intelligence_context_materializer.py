from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from ap.intelligence_market_data import (
    build_data_quality_warnings,
    extract_trade_geometry,
    summarize_timeframe,
)
from ap.intelligence_snapshot_store import (
    DEFAULT_PROFILE_VERSION,
    enqueue_intelligence_job,
    get_latest_snapshot,
    normalize_execution_mode,
)

log = logging.getLogger("ap.intelligence_context_materializer")

CONTEXT_REVISION = 1


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_signal_id(signal: dict[str, Any], fallback: str = "") -> str:
    try:
        from ap_canonical_signal import build_canonical_signal_id

        return (
            build_canonical_signal_id(signal)
            or str(signal.get("canonical_signal_id") or signal.get("signal_id") or fallback or "")
        )
    except Exception:
        try:
            from ap_canonical_signal import build_canonical_signal_id

            return (
                build_canonical_signal_id(str(signal.get("signal_id") or fallback or ""), signal)
                or str(signal.get("canonical_signal_id") or signal.get("signal_id") or fallback or "")
            )
        except Exception:
            return str(signal.get("canonical_signal_id") or signal.get("signal_id") or fallback or "")


def _stable_hash(value: Any) -> str:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _git_commit() -> str:
    try:
        from ap.intelligence_evaluation import _CACHED_GIT_COMMIT

        return str(_CACHED_GIT_COMMIT or "")
    except Exception:
        return ""


def _config_hash() -> str:
    try:
        from ap.intelligence_evaluation import _config_hash

        return str(_config_hash({"profile_version": DEFAULT_PROFILE_VERSION}))
    except Exception:
        return _stable_hash({"profile_version": DEFAULT_PROFILE_VERSION})


def build_intelligence_context_payload(
    signal: dict[str, Any],
    *,
    phase: str,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str = "",
    local_order_id: str = "",
    parent_snapshot_id: Optional[str] = None,
) -> dict[str, Any]:
    phase = str(phase or "").upper()
    sig = dict(signal or {})
    canonical_signal_id = canonical_signal_id or _canonical_signal_id(sig)
    geometry = extract_trade_geometry(sig)
    timeframe_context = {
        "monthly": summarize_timeframe(sig, "1mo"),
        "weekly": summarize_timeframe(sig, "1w"),
        "daily": summarize_timeframe(sig, "1d"),
        "4h": summarize_timeframe(sig, "4h"),
        "1h": summarize_timeframe(sig, "1h"),
    }
    fvg_candles = {
        "1h": timeframe_context["1h"],
        "4h": timeframe_context["4h"],
    }
    warnings = build_data_quality_warnings(sig)
    hard_blocks: list[str] = []
    if not geometry["available"]:
        warnings.extend([f"geometry_{name}_missing" for name in geometry.get("missing_data", [])])

    status = "COMPLETE"
    if warnings:
        status = "PARTIAL"
    if not geometry["available"] and len(warnings) >= 3:
        status = "UNAVAILABLE"

    payload = {
        "profile_version": DEFAULT_PROFILE_VERSION,
        "phase": phase,
        "context_revision": CONTEXT_REVISION,
        "client_id": str(client_id or ""),
        "execution_mode": normalize_execution_mode(execution_mode),
        "canonical_signal_id": canonical_signal_id,
        "signal_id": str(sig.get("signal_id") or ""),
        "local_order_id": str(local_order_id or ""),
        "ticker": str(sig.get("ticker") or sig.get("symbol") or ""),
        "side": str(sig.get("side") or sig.get("direction") or "").upper(),
        "pattern": str(sig.get("pattern") or sig.get("pattern_id") or ""),
        "timeframe": str(sig.get("timeframe") or ""),
        "data_as_of": sig.get("data_as_of") or sig.get("queued_at") or _now_iso(),
        "computed_at": _now_iso(),
        "status": status,
        "hard_safety_blocks": hard_blocks,
        "strategy_advisories": [],
        "data_quality_warnings": sorted(set(warnings)),
        "observe_only": True,
        "affected_eligibility": False,
        "trade_geometry": geometry,
        "timeframe_context": timeframe_context,
        "fvg_context": {
            "available": bool(fvg_candles["1h"]["available"] or fvg_candles["4h"]["available"]),
            "source": "signal_candles_only",
            "candles": fvg_candles,
        },
        "market_context": {
            "sector": sig.get("sector"),
            "sector_etf": sig.get("sector_etf"),
            "volume_context": sig.get("volume_context"),
            "vwap_context": sig.get("vwap_context"),
        },
        "parent_snapshot_id": parent_snapshot_id,
        "compatibility_key": "intelligence_evaluation",
    }
    payload["input_hash"] = _stable_hash(
        {
            "phase": phase,
            "client_id": client_id,
            "execution_mode": execution_mode,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": local_order_id,
            "signal": sig,
        }
    )
    payload["config_hash"] = _config_hash()
    payload["git_commit"] = _git_commit()
    return payload


def build_snapshot_kwargs(job: dict[str, Any]) -> dict[str, Any]:
    payload = job.get("payload") or {}
    signal = payload.get("signal") if isinstance(payload.get("signal"), dict) else payload
    phase = str(job.get("phase") or payload.get("phase") or "").upper()
    canonical_signal_id = str(job.get("canonical_signal_id") or payload.get("canonical_signal_id") or "")
    client_id = str(job.get("client_id") or payload.get("client_id") or "")
    execution_mode = normalize_execution_mode(job.get("execution_mode") or payload.get("execution_mode"))
    local_order_id = str(job.get("local_order_id") or payload.get("local_order_id") or "")
    parent_snapshot_id = payload.get("parent_snapshot_id")
    parent_link_status = payload.get("parent_link_status")
    if phase == "PREOPEN" and not parent_snapshot_id:
        parent = get_latest_snapshot(
            client_id=client_id,
            execution_mode=execution_mode,
            canonical_signal_id=canonical_signal_id,
            phase="PRETRIGGER",
        )
        if parent.get("ok"):
            parent_snapshot_id = ((parent.get("snapshot") or {}).get("id") or None)
            parent_link_status = "LINKED" if parent_snapshot_id else "PRETRIGGER_NOT_AVAILABLE"
        else:
            parent_link_status = "PRETRIGGER_LOOKUP_FAILED"
    context_payload = build_intelligence_context_payload(
        dict(signal or {}),
        phase=phase,
        client_id=client_id,
        execution_mode=execution_mode,
        canonical_signal_id=canonical_signal_id,
        local_order_id=local_order_id,
        parent_snapshot_id=parent_snapshot_id,
    )
    if phase == "PREOPEN":
        context_payload["parent_link_status"] = parent_link_status or "PRETRIGGER_NOT_AVAILABLE"
    return {
        "client_id": client_id,
        "execution_mode": execution_mode,
        "canonical_signal_id": canonical_signal_id,
        "signal_id": str(job.get("signal_id") or context_payload.get("signal_id") or ""),
        "local_order_id": local_order_id,
        "phase": phase,
        "context_revision": int(job.get("context_revision") or CONTEXT_REVISION),
        "profile_version": str(job.get("profile_version") or DEFAULT_PROFILE_VERSION),
        "parent_snapshot_id": parent_snapshot_id,
        "input_hash": str(job.get("input_hash") or context_payload["input_hash"]),
        "config_hash": context_payload["config_hash"],
        "git_commit": context_payload["git_commit"],
        "data_as_of": context_payload.get("data_as_of"),
        "status": context_payload["status"],
        "payload": context_payload,
    }


def enqueue_pretrigger_context(
    signal: dict[str, Any],
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str = "",
) -> dict[str, Any]:
    sig = dict(signal or {})
    canonical_signal_id = canonical_signal_id or _canonical_signal_id(sig)
    payload = {
        "phase": "PRETRIGGER",
        "signal": sig,
        "client_id": str(client_id or ""),
        "execution_mode": normalize_execution_mode(execution_mode),
        "canonical_signal_id": canonical_signal_id,
        "observe_only": True,
        "affected_eligibility": False,
    }
    input_hash = _stable_hash(payload)
    return enqueue_intelligence_job(
        client_id=str(client_id or ""),
        execution_mode=normalize_execution_mode(execution_mode),
        canonical_signal_id=canonical_signal_id,
        signal_id=str(sig.get("signal_id") or ""),
        local_order_id="",
        phase="PRETRIGGER",
        context_revision=CONTEXT_REVISION,
        profile_version=DEFAULT_PROFILE_VERSION,
        input_hash=input_hash,
        payload=payload,
    )


def enqueue_preopen_context(
    signal: dict[str, Any],
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str = "",
    local_order_id: str,
) -> dict[str, Any]:
    sig = dict(signal or {})
    canonical_signal_id = canonical_signal_id or _canonical_signal_id(sig)
    parent = get_latest_snapshot(
        client_id=str(client_id or ""),
        execution_mode=normalize_execution_mode(execution_mode),
        canonical_signal_id=canonical_signal_id,
        phase="PRETRIGGER",
    )
    parent_snapshot = parent.get("snapshot") if parent.get("ok") else None
    parent_snapshot_id = (parent_snapshot or {}).get("id")
    parent_link_status = (
        "LINKED" if parent_snapshot_id
        else "PRETRIGGER_NOT_AVAILABLE" if parent.get("ok")
        else "PRETRIGGER_LOOKUP_FAILED"
    )
    payload = {
        "phase": "PREOPEN",
        "signal": sig,
        "client_id": str(client_id or ""),
        "execution_mode": normalize_execution_mode(execution_mode),
        "canonical_signal_id": canonical_signal_id,
        "local_order_id": str(local_order_id or ""),
        "parent_snapshot_id": str(parent_snapshot_id) if parent_snapshot_id else None,
        "parent_link_status": parent_link_status,
        "observe_only": True,
        "affected_eligibility": False,
    }
    input_hash = _stable_hash({key: value for key, value in payload.items()
                               if key not in {"parent_snapshot_id", "parent_link_status"}})
    return enqueue_intelligence_job(
        client_id=str(client_id or ""),
        execution_mode=normalize_execution_mode(execution_mode),
        canonical_signal_id=canonical_signal_id,
        signal_id=str(sig.get("signal_id") or ""),
        local_order_id=str(local_order_id or ""),
        phase="PREOPEN",
        context_revision=CONTEXT_REVISION,
        profile_version=DEFAULT_PROFILE_VERSION,
        input_hash=input_hash,
        payload=payload,
    )
