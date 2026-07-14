from __future__ import annotations

import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import Any, Optional

from ap.intelligence_market_data import (
    build_data_quality_warnings,
    collect_point_in_time_context,
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
    context_revision: int = CONTEXT_REVISION,
    profile_version: str = DEFAULT_PROFILE_VERSION,
    input_hash: str = "",
    broker: Any = None,
) -> dict[str, Any]:
    phase = str(phase or "").upper()
    sig = dict(signal or {})
    canonical_signal_id = canonical_signal_id or _canonical_signal_id(sig)
    point_in_time = collect_point_in_time_context(sig, broker=broker, phase=phase)
    observation = point_in_time.get("underlying_observation") or {}
    observed_price = observation.get("price")
    evaluation_signal = dict(sig)
    evaluation_signal["canonical_signal_id"] = canonical_signal_id
    if observed_price is not None:
        evaluation_signal["underlying_price"] = observed_price
        evaluation_signal["current_price"] = observed_price
    else:
        evaluation_signal.pop("underlying_price", None)
        evaluation_signal.pop("current_price", None)
    data_sources = point_in_time.get("data_sources") or {}
    from ap.market_context_builder import build_market_context_for_signal
    market_context = build_market_context_for_signal(evaluation_signal, data_sources=data_sources)
    market_context["market"] = dict(data_sources.get("market") or {})
    market_context["sector"] = dict(data_sources.get("sector") or {})
    market_context.setdefault("candles", {})["1h"] = list(
        ((data_sources.get("candles") or {}).get("1h") or [])
    )
    from ap.the_strat_confluence import evaluate_higher_timeframe_confluence
    from ap.fair_value_gap import evaluate_fvg_context
    from ap.sector_context import score_sector_context
    from ap.volume_confirmation import score_volume_confirmation
    from ap.vwap_context import score_vwap_context
    from ap.trigger_geometry import score_trigger_geometry
    from ap.score_profile_side import normalize_signal_side
    from ap.position_score_profile import build_position_score_profile
    from ap.intelligence_score import build_intelligence_score
    strat_context = evaluate_higher_timeframe_confluence(evaluation_signal, market_context)
    fvg_context = evaluate_fvg_context(evaluation_signal, market_context)
    sector_context = score_sector_context(evaluation_signal, market_context)
    volume_context = score_volume_confirmation(evaluation_signal, market_context)
    vwap_context = score_vwap_context(evaluation_signal, market_context)
    trigger_context = score_trigger_geometry(
        evaluation_signal,
        normalize_signal_side(evaluation_signal.get("side") or evaluation_signal.get("direction")),
    )
    profile_market_context = dict(market_context)
    profile_market_context["_intelligence_upstream"] = {
        name: {"provided": True, "raw": raw}
        for name, raw in {
            "the_strat_confluence": strat_context,
            "fair_value_gap": fvg_context,
            "sector_context": sector_context,
            "volume_confirmation": volume_context,
            "vwap_context": vwap_context,
            "trigger_geometry": trigger_context,
        }.items()
    }
    position_score_profile = build_position_score_profile(
        evaluation_signal, profile_market_context
    )
    computed_at = _now_iso()
    intelligence_score = build_intelligence_score(
        position_score_profile,
        signal=evaluation_signal,
        client_id=str(client_id or ""),
        execution_mode=normalize_execution_mode(execution_mode),
        scored_at=computed_at,
        data_as_of=point_in_time.get("collected_at") or computed_at,
        source=f"intelligence_context_{phase.lower()}",
        git_commit=_git_commit(),
    )
    geometry = extract_trade_geometry(evaluation_signal)
    timeframe_context = {
        "monthly": summarize_timeframe({"1mo": market_context["candles"].get("monthly")}, "1mo"),
        "weekly": summarize_timeframe({"1w": market_context["candles"].get("weekly")}, "1w"),
        "daily": summarize_timeframe({"1d": market_context["candles"].get("daily")}, "1d"),
        "4h": summarize_timeframe({"4h": market_context["candles"].get("4h")}, "4h"),
        "1h": summarize_timeframe({"1h": market_context["candles"].get("1h")}, "1h"),
    }
    warnings = build_data_quality_warnings(evaluation_signal)
    warnings.extend(point_in_time.get("errors") or [])
    hard_blocks: list[str] = []
    if not geometry["available"]:
        warnings.extend([f"geometry_{name}_missing" for name in geometry.get("missing_data", [])])

    def _component(available: bool, *, error_prefix: str = "", stale: bool = False) -> str:
        if error_prefix and any(str(item).startswith(error_prefix) for item in point_in_time.get("errors") or []):
            return "ERROR"
        if available and stale:
            return "STALE"
        return "AVAILABLE" if available else "MISSING"

    fvg_tf = ((fvg_context.get("diagnostics") or {}).get("timeframes") or {})
    component_statuses = {
        "geometry": _component(bool(geometry.get("available"))),
        "underlying_quote": _component(
            observed_price is not None, error_prefix="underlying_quote",
            stale=bool(observation.get("age_seconds") is not None and observation.get("age_seconds") > 120),
        ),
        "monthly": _component(bool(timeframe_context["monthly"].get("available")), error_prefix="daily_history"),
        "weekly": _component(bool(timeframe_context["weekly"].get("available")), error_prefix="daily_history"),
        "daily": _component(bool(timeframe_context["daily"].get("available")), error_prefix="daily_history"),
        "four_hour": _component(bool(timeframe_context["4h"].get("available")), error_prefix="intraday_history"),
        "one_hour_fvg": _component(bool((fvg_tf.get("1h") or {}).get("available")), error_prefix="intraday_history"),
        "four_hour_fvg": _component(bool((fvg_tf.get("4h") or {}).get("available")), error_prefix="intraday_history"),
        "market": _component((sector_context.get("diagnostics") or {}).get("market_direction") is not None, error_prefix="market_quote"),
        "sector": _component((sector_context.get("diagnostics") or {}).get("sector_direction") is not None, error_prefix="sector_quote"),
        "volume": _component((volume_context.get("diagnostics") or {}).get("relative_volume") is not None),
        "vwap": _component((vwap_context.get("diagnostics") or {}).get("vwap") is not None),
        "intelligence_score": _component(bool(intelligence_score.get("score_valid"))),
    }
    required_values = list(component_statuses.values())
    status = "COMPLETE" if required_values and all(value == "AVAILABLE" for value in required_values) else "PARTIAL"
    if all(value in {"MISSING", "ERROR"} for value in required_values):
        status = "UNAVAILABLE"
    advisories = sorted(set(
        list(strat_context.get("block_recommendations") or [])
        + list(fvg_context.get("block_recommendations") or [])
        + list(sector_context.get("block_recommendations") or [])
        + list(volume_context.get("block_recommendations") or [])
        + list(vwap_context.get("block_recommendations") or [])
    ))

    payload = {
        "profile_version": str(profile_version or DEFAULT_PROFILE_VERSION),
        "phase": phase,
        "context_revision": int(context_revision or CONTEXT_REVISION),
        "client_id": str(client_id or ""),
        "execution_mode": normalize_execution_mode(execution_mode),
        "canonical_signal_id": canonical_signal_id,
        "signal_id": str(sig.get("signal_id") or ""),
        "local_order_id": str(local_order_id or ""),
        "ticker": str(sig.get("ticker") or sig.get("symbol") or ""),
        "side": str(sig.get("side") or sig.get("direction") or "").upper(),
        "pattern": str(sig.get("pattern") or sig.get("pattern_id") or ""),
        "timeframe": str(sig.get("timeframe") or ""),
        "data_as_of": point_in_time.get("collected_at") or _now_iso(),
        "signal_data_as_of": sig.get("data_as_of") or sig.get("queued_at"),
        "computed_at": computed_at,
        "status": status,
        "component_statuses": component_statuses,
        "hard_safety_blocks": hard_blocks,
        "strategy_advisories": advisories,
        "data_quality_warnings": sorted(set(warnings)),
        "observe_only": True,
        "affected_eligibility": False,
        "trade_geometry": geometry,
        "timeframe_context": timeframe_context,
        "strat_context": strat_context,
        "fvg_context": fvg_context,
        "market_context": market_context,
        "sector_context": sector_context,
        "volume_context": volume_context,
        "vwap_context": vwap_context,
        "trigger_context": trigger_context,
        "position_score_profile": position_score_profile,
        "intelligence_score": intelligence_score,
        "underlying_observation": observation,
        "data_provenance": point_in_time.get("provenance") or {},
        "parent_snapshot_id": parent_snapshot_id,
        "compatibility_key": "intelligence_evaluation",
    }
    payload["input_hash"] = str(input_hash or _stable_hash(
        {
            "phase": phase,
            "client_id": client_id,
            "execution_mode": execution_mode,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": local_order_id,
            "signal": sig,
        }
    ))
    payload["config_hash"] = _config_hash()
    payload["git_commit"] = _git_commit()
    return payload


def build_snapshot_kwargs(job: dict[str, Any], *, broker: Any = None) -> dict[str, Any]:
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
        context_revision=int(job.get("context_revision") or CONTEXT_REVISION),
        profile_version=str(job.get("profile_version") or DEFAULT_PROFILE_VERSION),
        input_hash=str(job.get("input_hash") or ""),
        broker=broker,
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


def recover_missing_intelligence_jobs(
    *, client_id: str, execution_mode: str, limit: int = 100
) -> dict[str, Any]:
    """Backfill durable jobs from canonical queue/order truth after process loss."""
    from ap.db import conn, run_with_retry

    mode = normalize_execution_mode(execution_mode)

    def _load() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        with conn() as c:
            c.execute(
                """
                SELECT signal_id, payload
                FROM trade_queue q
                WHERE q.client_id=%s
                  AND upper(COALESCE(q.payload->>'execution_mode', q.payload->>'mode', %s))=%s
                  AND q.status NOT IN ('REJECTED','ERROR','CANCELED','CANCELLED','EXPIRED')
                  AND NOT EXISTS (
                    SELECT 1 FROM ap_intelligence_jobs j
                    WHERE j.client_id=q.client_id
                      AND lower(j.execution_mode)=lower(%s)
                      AND j.signal_id=q.signal_id
                      AND j.phase='PRETRIGGER'
                  )
                ORDER BY q.created_ts DESC
                LIMIT %s
                """,
                (client_id, mode, mode, mode, int(limit or 100)),
            )
            queue_rows = list(c.fetchall() or [])
            c.execute(
                """
                SELECT o.signal_id, o.local_order_id, q.payload
                FROM orders o
                JOIN trade_queue q
                  ON q.client_id=o.client_id AND q.signal_id=o.signal_id
                WHERE o.client_id=%s
                  AND o.kind='ENTRY'
                  AND o.status IN ('PENDING_TRIGGER','WATCHING')
                  AND o.broker_order_id IS NULL
                  AND upper(COALESCE(q.payload->>'execution_mode', q.payload->>'mode', %s))=%s
                  AND NOT EXISTS (
                    SELECT 1 FROM ap_intelligence_jobs j
                    WHERE j.client_id=o.client_id
                      AND lower(j.execution_mode)=lower(%s)
                      AND j.signal_id=o.signal_id
                      AND COALESCE(NULLIF(BTRIM(j.local_order_id), ''), '__none__')=
                          COALESCE(NULLIF(BTRIM(o.local_order_id), ''), '__none__')
                      AND j.phase='PREOPEN'
                  )
                ORDER BY o.created_ts DESC
                LIMIT %s
                """,
                (client_id, mode, mode, mode, int(limit or 100)),
            )
            preopen_rows = list(c.fetchall() or [])
            return queue_rows, preopen_rows

    try:
        queue_rows, preopen_rows = run_with_retry(_load)
    except Exception as exc:
        return {"ok": False, "pretrigger": 0, "preopen": 0, "error": str(exc)[:500]}

    counts = {"pretrigger": 0, "preopen": 0}
    for row in queue_rows:
        payload = row.get("payload") if isinstance(row, dict) else row[1]
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        signal = dict(payload or {})
        signal.setdefault("signal_id", row.get("signal_id") if isinstance(row, dict) else row[0])
        result = enqueue_pretrigger_context(
            signal, client_id=client_id, execution_mode=mode,
            canonical_signal_id=_canonical_signal_id(signal),
        )
        counts["pretrigger"] += int(bool(result.get("inserted")))
    for row in preopen_rows:
        payload = row.get("payload") if isinstance(row, dict) else row[2]
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except Exception:
                payload = {}
        signal = dict(payload or {})
        signal.setdefault("signal_id", row.get("signal_id") if isinstance(row, dict) else row[0])
        local_order_id = row.get("local_order_id") if isinstance(row, dict) else row[1]
        result = enqueue_preopen_context(
            signal, client_id=client_id, execution_mode=mode,
            canonical_signal_id=_canonical_signal_id(signal),
            local_order_id=str(local_order_id or ""),
        )
        counts["preopen"] += int(bool(result.get("inserted")))
    return {"ok": True, **counts}
