"""
Chart memory learning ingestion.

This module turns chart screenshots/reviews into structured training memory.
It is intentionally execution-safe:
- no broker submit
- no broker cancel
- no order mutation
- no position mutation
- no proof_trades mutation
- no trade_queue mutation

It can be used for manual backfill first, then automated chart screenshot
capture later. The output is append-only diagnostic memory that can later be
queried by the entry path after separate review.
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import uuid
from datetime import datetime, timezone
from typing import Any

from ap_chart_memory_confirmation import build_chart_memory_payload, normalize_chart_state


VALID_TRADE_RESULTS = {"winner", "loser", "scratch", "breakeven", "unknown"}
VALID_REVIEW_SOURCES = {"manual_backfill", "operator", "vision_model", "hybrid", "unknown"}

DEFAULT_FEATURES: dict[str, Any] = {
    "trend": "unknown",
    "above_vwap": None,
    "below_vwap": None,
    "higher_high": None,
    "higher_low": None,
    "lower_high": None,
    "lower_low": None,
    "trigger_reclaimed": None,
    "trigger_rejected": None,
    "fresh_extension": None,
    "failed_reclaim": None,
    "failed_rejection": None,
    "wick_rejection": None,
    "volume_expanding": None,
    "gap_direction": "unknown",
    "distance_from_trigger_pct": None,
    "distance_from_target_pct": None,
    "atr_extension": None,
}


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean_text(value: Any, default: str = "") -> str:
    text = str(value or "").strip()
    return text or default


def _clean_upper(value: Any, default: str = "") -> str:
    return _clean_text(value, default).upper()


def _clean_lower(value: Any, default: str = "") -> str:
    return _clean_text(value, default).lower()


def _safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result


def _safe_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_bool(value: Any) -> bool | None:
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def decode_base64_image(image_base64: str) -> bytes:
    """Decode a base64 screenshot payload.

    Supports raw base64 and data URLs like data:image/png;base64,....
    """

    text = _clean_text(image_base64)
    if not text:
        raise ValueError("missing_image_base64")
    if "," in text and text.lower().startswith("data:image/"):
        text = text.split(",", 1)[1]
    return base64.b64decode(text, validate=True)


def image_hash_from_base64(image_base64: str) -> str:
    return sha256_bytes(decode_base64_image(image_base64))


def normalize_features(features: dict[str, Any] | None) -> dict[str, Any]:
    """Normalize structured chart features into a stable production shape."""

    raw = features or {}
    normalized = dict(DEFAULT_FEATURES)

    for key in normalized:
        if key not in raw:
            continue
        value = raw[key]
        if key in {
            "above_vwap",
            "below_vwap",
            "higher_high",
            "higher_low",
            "lower_high",
            "lower_low",
            "trigger_reclaimed",
            "trigger_rejected",
            "fresh_extension",
            "failed_reclaim",
            "failed_rejection",
            "wick_rejection",
            "volume_expanding",
        }:
            normalized[key] = _safe_bool(value)
        elif key in {"distance_from_trigger_pct", "distance_from_target_pct", "atr_extension"}:
            normalized[key] = _safe_float(value)
        else:
            normalized[key] = _clean_lower(value, "unknown")

    # Keep additional feature keys under extra_features so we do not lose evidence,
    # but do not pollute the stable feature schema.
    extras = {k: v for k, v in raw.items() if k not in normalized}
    if extras:
        normalized["extra_features"] = extras

    return normalized


def normalize_trade_result(value: Any) -> str:
    result = _clean_lower(value, "unknown")
    return result if result in VALID_TRADE_RESULTS else "unknown"


def normalize_review_source(value: Any) -> str:
    source = _clean_lower(value, "unknown")
    return source if source in VALID_REVIEW_SOURCES else "unknown"


def normalize_confidence(value: Any) -> int | None:
    confidence = _safe_int(value)
    if confidence is None:
        return None
    return max(0, min(100, confidence))


def build_chart_learning_payload(
    *,
    client_id: str,
    execution_mode: str,
    signal_id: str,
    ticker: str,
    side: str,
    timeframe: str,
    chart_state: str,
    reason: str,
    trigger_level: Any = None,
    current_price: Any = None,
    target_level: Any = None,
    stop_level: Any = None,
    screenshot_url: str | None = None,
    image_hash: str | None = None,
    perceptual_hash: str | None = None,
    chart_confidence: Any = None,
    features: dict[str, Any] | None = None,
    trade_result: str = "unknown",
    proof_trade_id: Any = None,
    position_id: Any = None,
    broker_order_id: Any = None,
    operator_notes: str = "",
    mistake: str = "",
    lesson: str = "",
    review_source: str = "manual_backfill",
    embedding: list[float] | None = None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build one chart-learning row for storage.

    This wraps the smaller confirmation payload with richer learning fields:
    outcome linkage, feature extraction, duplicate hashes, and operator notes.
    """

    normalized_features = normalize_features(features)
    normalized_state = normalize_chart_state(chart_state)
    normalized_result = normalize_trade_result(trade_result)
    normalized_source = normalize_review_source(review_source)
    normalized_confidence = normalize_confidence(chart_confidence)

    enriched_metadata = dict(metadata or {})
    enriched_metadata.update(
        {
            "target_level": _safe_float(target_level),
            "stop_level": _safe_float(stop_level),
            "image_hash": image_hash or None,
            "perceptual_hash": perceptual_hash or None,
            "chart_confidence": normalized_confidence,
            "features": normalized_features,
            "trade_result": normalized_result,
            "proof_trade_id": _clean_text(proof_trade_id) or None,
            "position_id": _clean_text(position_id) or None,
            "broker_order_id": _clean_text(broker_order_id) or None,
            "operator_notes": _clean_text(operator_notes),
            "mistake": _clean_text(mistake),
            "lesson": _clean_text(lesson),
            "review_source": normalized_source,
            "has_embedding": bool(embedding),
            "schema_version": "chart_learning_v1",
        }
    )

    payload = build_chart_memory_payload(
        client_id=client_id,
        execution_mode=execution_mode,
        signal_id=signal_id,
        ticker=ticker,
        side=side,
        timeframe=timeframe,
        trigger_level=trigger_level,
        current_price=current_price,
        screenshot_url=screenshot_url,
        chart_state=normalized_state,
        reason=reason,
        metadata=enriched_metadata,
    )

    # Top-level duplicated fields make Supabase querying easier without forcing
    # every report to parse metadata JSON.
    payload.update(
        {
            "image_hash": image_hash or None,
            "perceptual_hash": perceptual_hash or None,
            "chart_confidence": normalized_confidence,
            "features": normalized_features,
            "trade_result": normalized_result,
            "proof_trade_id": _clean_text(proof_trade_id) or None,
            "position_id": _clean_text(position_id) or None,
            "broker_order_id": _clean_text(broker_order_id) or None,
            "operator_notes": _clean_text(operator_notes),
            "mistake": _clean_text(mistake),
            "lesson": _clean_text(lesson),
            "review_source": normalized_source,
            "embedding": embedding or None,
            "schema_version": "chart_learning_v1",
        }
    )

    return payload


def build_manual_backfill_payload(row: dict[str, Any]) -> dict[str, Any]:
    """Build chart learning payload from a manual backfill dict/CSV row."""

    image_hash = row.get("image_hash")
    image_base64 = row.get("image_base64")
    if not image_hash and image_base64:
        image_hash = image_hash_from_base64(str(image_base64))

    return build_chart_learning_payload(
        client_id=row.get("client_id"),
        execution_mode=row.get("execution_mode"),
        signal_id=row.get("signal_id") or row.get("canonical_signal_id"),
        ticker=row.get("ticker") or row.get("symbol"),
        side=row.get("side") or row.get("direction"),
        timeframe=row.get("timeframe"),
        trigger_level=row.get("trigger_level") or row.get("entry_trigger"),
        current_price=row.get("current_price") or row.get("underlying_entry"),
        target_level=row.get("target_level") or row.get("target_underlying"),
        stop_level=row.get("stop_level") or row.get("stop_underlying"),
        screenshot_url=row.get("screenshot_url"),
        image_hash=image_hash,
        perceptual_hash=row.get("perceptual_hash"),
        chart_state=row.get("chart_state") or "unclear",
        chart_confidence=row.get("chart_confidence"),
        reason=row.get("reason") or row.get("chart_reason") or "manual chart review",
        features=row.get("features") or {},
        trade_result=row.get("trade_result") or "unknown",
        proof_trade_id=row.get("proof_trade_id"),
        position_id=row.get("position_id"),
        broker_order_id=row.get("broker_order_id"),
        operator_notes=row.get("operator_notes") or "",
        mistake=row.get("mistake") or "",
        lesson=row.get("lesson") or "",
        review_source=row.get("review_source") or "manual_backfill",
        embedding=row.get("embedding"),
        metadata=row.get("metadata") or {},
    )


def find_existing_memory_by_hash(
    supabase: Any,
    *,
    client_id: str,
    execution_mode: str,
    image_hash: str,
) -> dict[str, Any] | None:
    """Detect exact duplicate screenshots for a client/mode."""

    clean_hash = _clean_text(image_hash)
    if not clean_hash:
        return None

    result = (
        supabase.table("chart_memory_confirmations")
        .select("*")
        .eq("client_id", _clean_text(client_id))
        .eq("execution_mode", _clean_lower(execution_mode))
        .eq("image_hash", clean_hash)
        .order("created_at", desc=True)
        .limit(1)
        .execute()
    )
    rows = getattr(result, "data", None) or []
    return rows[0] if rows else None


def insert_chart_learning_memory(
    supabase: Any,
    payload: dict[str, Any],
    *,
    skip_duplicate_hash: bool = True,
) -> tuple[dict[str, Any], bool]:
    """Insert one chart-learning row.

    Returns (row, inserted). If an exact hash duplicate exists and skip is true,
    returns the existing row with inserted=False.
    """

    image_hash = payload.get("image_hash")
    if skip_duplicate_hash and image_hash:
        existing = find_existing_memory_by_hash(
            supabase,
            client_id=payload["client_id"],
            execution_mode=payload["execution_mode"],
            image_hash=image_hash,
        )
        if existing:
            return existing, False

    supabase.table("chart_memory_confirmations").insert(payload).execute()
    return payload, True


def build_vision_prompt(*, ticker: str, side: str, timeframe: str, trigger_level: Any, target_level: Any = None, stop_level: Any = None) -> str:
    """Return the exact prompt to use with a vision model for screenshot review."""

    return (
        "Review this trading chart screenshot for entry quality. "
        "Return only JSON. "
        f"Ticker: {_clean_upper(ticker)}. Side: {_clean_upper(side)}. Timeframe: {_clean_lower(timeframe)}. "
        f"Trigger: {trigger_level}. Target: {target_level}. Stop: {stop_level}. "
        "Classify chart_state as one of: continuation_confirmed, clean_reclaim, fresh_intraday_high, "
        "fresh_intraday_low, clean_rejection, late_chase, failed_reclaim, failed_rejection, chop, "
        "wick_only, unclear. Include chart_confidence 0-100, reason, and features with trend, "
        "above_vwap, below_vwap, higher_high, higher_low, lower_high, lower_low, trigger_reclaimed, "
        "trigger_rejected, fresh_extension, failed_reclaim, failed_rejection, wick_rejection, "
        "volume_expanding, gap_direction, distance_from_trigger_pct, distance_from_target_pct, atr_extension."
    )


def parse_vision_json_response(response_text: str) -> dict[str, Any]:
    """Parse a vision model JSON response safely."""

    text = _clean_text(response_text)
    if not text:
        raise ValueError("missing_vision_response")

    # tolerate fenced JSON pasted from tools
    if text.startswith("```"):
        text = text.strip("`")
        if text.lower().startswith("json"):
            text = text[4:].strip()

    data = json.loads(text)
    if not isinstance(data, dict):
        raise ValueError("vision_response_not_object")

    return {
        "chart_state": normalize_chart_state(data.get("chart_state")),
        "chart_confidence": normalize_confidence(data.get("chart_confidence")),
        "reason": _clean_text(data.get("reason")),
        "features": normalize_features(data.get("features") or {}),
    }


def summarize_learning_row(row: dict[str, Any]) -> str:
    """Compact human-readable summary for logs/review."""

    return (
        f"{row.get('ticker')} {row.get('side')} {row.get('timeframe')} "
        f"state={row.get('chart_state')} confidence={row.get('chart_confidence')} "
        f"result={row.get('trade_result')} signal_id={row.get('signal_id')}"
    )
