from __future__ import annotations

import hashlib
import json
import logging
import math
from datetime import datetime, timezone
from collections.abc import Mapping
from typing import Any, Optional
from zoneinfo import ZoneInfo

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
_ET = ZoneInfo("America/New_York")


class BreachMaterializationRejected(ValueError):
    """Permanent BREACH input rejection; never fall through to enrichment."""

    def __init__(self, error_code: str):
        self.error_code = str(error_code or "BREACH_INPUT_REJECTED")
        super().__init__(self.error_code)


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


def _finite_number(value: Any) -> Optional[float]:
    """Return a real finite scalar; bool and malformed values are unavailable."""
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return number if math.isfinite(number) else None


def _parse_timestamp(value: Any) -> Optional[datetime]:
    if isinstance(value, datetime):
        parsed = value
    elif value not in (None, ""):
        try:
            parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
    else:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(timezone.utc)


def _freeze_value(value: Any, *, depth: int = 0) -> Any:
    """Make the handoff payload independent of mutable runtime objects."""
    if depth > 8:
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {
            str(key): _freeze_value(item, depth=depth + 1)
            for key, item in value.items()
            if not str(key).startswith("_")
        }
    if isinstance(value, (list, tuple)):
        return [_freeze_value(item, depth=depth + 1) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)


def _identity_conflict(field: str, values: list[str]) -> Optional[str]:
    normalized = {
        value.strip().lower() if field in {"client_id", "execution_mode"} else value.strip()
        for value in values if value.strip()
    }
    if len(normalized) > 1:
        return f"BREACH_IDENTITY_CONFLICT_{field.upper()}"
    return None


def _strict_positive_int(value: Any) -> Optional[int]:
    if isinstance(value, bool) or value in (None, ""):
        return None
    try:
        parsed = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def resolve_canonical_strategy_pattern(signal: dict[str, Any]) -> dict[str, Any]:
    """Deterministic BREACH identity from production scanner fields.

    Persists both raw_pattern and canonical_strategy_pattern. Unknown or
    ambiguous inputs stay explicit rather than inventing a family.
    """
    sig = signal if isinstance(signal, dict) else {}
    metadata = sig.get("metadata") if isinstance(sig.get("metadata"), dict) else {}
    raw = (
        sig.get("pattern")
        or sig.get("pattern_id")
        or metadata.get("pattern")
        or metadata.get("pattern_id")
    )
    raw_s = str(raw).strip() if raw not in (None, "") else ""
    timeframe = str(
        sig.get("timeframe")
        or sig.get("tf")
        or metadata.get("timeframe")
        or metadata.get("tf")
        or ""
    ).strip().lower()
    authoritative = (
        sig.get("canonical_strategy_pattern")
        or sig.get("canonical_pattern")
        or sig.get("strategy_pattern")
        or metadata.get("canonical_strategy_pattern")
        or metadata.get("canonical_pattern")
        or metadata.get("strategy_pattern")
    )
    authoritative_s = (
        str(authoritative).strip() if authoritative not in (None, "") else ""
    )

    diagnostics: dict[str, Any] = {
        "raw_pattern": raw_s or None,
        "timeframe": timeframe or None,
        "authoritative_input": authoritative_s or None,
    }

    if authoritative_s:
        return {
            "raw_pattern": raw_s or None,
            "canonical_strategy_pattern": authoritative_s,
            "status": "AUTHORITATIVE",
            "diagnostics": diagnostics,
        }

    # Production daily 2-3 scanner label -> daily 2-3-2 family.
    normalized_raw = raw_s.replace("_", "-").replace(" ", "").lower()
    if normalized_raw in {"2-3", "2/3", "23"} and timeframe in {"1d", "d", "daily", "1day"}:
        return {
            "raw_pattern": raw_s,
            "canonical_strategy_pattern": "2-3-2",
            "status": "RESOLVED",
            "diagnostics": {**diagnostics, "rule": "daily_2_3_to_232"},
        }
    if normalized_raw in {"2-3-2", "232"}:
        return {
            "raw_pattern": raw_s,
            "canonical_strategy_pattern": "2-3-2",
            "status": "RESOLVED",
            "diagnostics": {**diagnostics, "rule": "already_232"},
        }

    if not raw_s and not timeframe:
        return {
            "raw_pattern": None,
            "canonical_strategy_pattern": None,
            "status": "UNKNOWN",
            "diagnostics": {**diagnostics, "reason": "missing_pattern_and_timeframe"},
        }
    return {
        "raw_pattern": raw_s or None,
        "canonical_strategy_pattern": None,
        "status": "AMBIGUOUS" if raw_s else "UNKNOWN",
        "diagnostics": {
            **diagnostics,
            "reason": "no_deterministic_mapping",
        },
    }


def resolve_breach_lineage(signal: dict[str, Any]) -> dict[str, Any]:
    """Derive INITIAL/REBREACH/RECOVERED/DIRECTION_REVERSAL from watcher truth.

    Never invents rebreach from timestamps alone. Unprovable lineage stays
    UNKNOWN with diagnostics.
    """
    sig = signal if isinstance(signal, dict) else {}
    metadata = sig.get("metadata") if isinstance(sig.get("metadata"), dict) else {}
    sources = [sig, metadata]
    lifecycle = sig.get("breach_lifecycle") if isinstance(sig.get("breach_lifecycle"), dict) else {}
    lifecycle_sources = (
        sig.get("breach_lifecycle_sources")
        if isinstance(sig.get("breach_lifecycle_sources"), dict)
        else {}
    )

    def _get(*keys: str) -> Any:
        for source in sources:
            for key in keys:
                value = source.get(key)
                if value not in (None, ""):
                    return value
        for key in keys:
            value = lifecycle.get(key)
            if value not in (None, ""):
                return value
            value = lifecycle_sources.get(key)
            if value not in (None, ""):
                return value
        return None

    explicit = str(_get("breach_lineage") or "").strip().upper()
    known = {
        "INITIAL_BREACH",
        "REBREACH_AFTER_RESET",
        "RECOVERED_BREACH",
        "DIRECTION_REVERSAL_REBREACH",
        "UNKNOWN",
    }
    if explicit in known and explicit != "UNKNOWN":
        return {
            "breach_lineage": explicit,
            "status": "AUTHORITATIVE",
            "diagnostics": {"source": "explicit_breach_lineage"},
        }

    direction_reversal = bool(
        _get("direction_reversal_lineage")
        or _get("direction_reversed")
        or _get("reversal_rebreach")
    )
    recovered = bool(
        _get("recovered")
        or _get("recovery_submit_fenced")
        or _get("fenced")
        or lifecycle.get("recovered")
        or _get("_recovery_pre_claimed")
    )
    reset_seen = bool(
        _get("breach_reset")
        or _get("pullback_reset")
        or _get("trigger_reset")
        or _get("reset_after_breach")
    )
    breach_count = _get("breach_count", "trigger_breach_count", "confirmed_breach_count")
    try:
        breach_n = int(str(breach_count).strip()) if breach_count not in (None, "") else None
    except (TypeError, ValueError):
        breach_n = None

    diagnostics = {
        "direction_reversal": direction_reversal,
        "recovered": recovered,
        "reset_seen": reset_seen,
        "breach_count": breach_n,
        "explicit": explicit or None,
    }

    if direction_reversal:
        return {
            "breach_lineage": "DIRECTION_REVERSAL_REBREACH",
            "status": "RESOLVED",
            "diagnostics": diagnostics,
        }
    if recovered and (reset_seen or (breach_n is not None and breach_n > 1)):
        return {
            "breach_lineage": "RECOVERED_BREACH",
            "status": "RESOLVED",
            "diagnostics": diagnostics,
        }
    if reset_seen and breach_n is not None and breach_n > 1:
        return {
            "breach_lineage": "REBREACH_AFTER_RESET",
            "status": "RESOLVED",
            "diagnostics": diagnostics,
        }
    if breach_n == 1 and not reset_seen and not recovered and not direction_reversal:
        return {
            "breach_lineage": "INITIAL_BREACH",
            "status": "RESOLVED",
            "diagnostics": diagnostics,
        }
    if breach_n is not None and breach_n > 1 and reset_seen:
        return {
            "breach_lineage": "REBREACH_AFTER_RESET",
            "status": "RESOLVED",
            "diagnostics": diagnostics,
        }

    return {
        "breach_lineage": "UNKNOWN",
        "status": "UNKNOWN",
        "diagnostics": {**diagnostics, "reason": "insufficient_watcher_lineage_authority"},
    }


def resolve_public_breach_lifecycle(signal: dict[str, Any]) -> dict[str, Any]:
    """Resolve one public lifecycle authority BEFORE private-field stripping."""
    sig = signal if isinstance(signal, dict) else {}
    metadata = sig.get("metadata") if isinstance(sig.get("metadata"), dict) else {}
    existing = sig.get("breach_lifecycle") if isinstance(sig.get("breach_lifecycle"), dict) else {}

    def _pick(*keys: str) -> Any:
        for key in keys:
            for source in (existing, sig, metadata):
                value = source.get(key)
                if value not in (None, ""):
                    return value
        return None

    generation = _pick(
        "generation",
        "materialization_generation",
        "recovery_submit_generation",
        "_recovery_pre_claimed_generation",
        "watcher_generation",
        "lifecycle_generation",
    )
    attempt = _pick(
        "attempt",
        "retry_attempt",
        "retry_attempts",
        "materialization_retry_attempt",
        "materialization_attempts",
        "deferred_retry_attempt",
        "_recovery_pre_claimed_attempt",
    )
    owner = _pick(
        "owner",
        "_recovery_pre_claimed_owner",
        "watcher_owner",
        "ownership_token",
    )
    preclaimed = _pick("preclaimed", "_recovery_pre_claimed")
    recovered = _pick(
        "recovered",
        "recovery_submit_fenced",
        "fenced",
    )
    source = _pick(
        "source",
        "_recovery_pre_claimed_mode",
        "lifecycle_source",
        "recovery_source",
    )
    client_id = _pick("_recovery_pre_claimed_client_id", "client_id")
    mode = _pick("_recovery_pre_claimed_mode", "execution_mode", "mode")

    return {
        "generation": generation,
        "attempt": attempt,
        "owner": owner,
        "preclaimed": bool(preclaimed) if preclaimed not in (None, "") else False,
        "recovered": bool(recovered) if recovered not in (None, "") else False,
        "source": source,
        "client_id": client_id,
        "execution_mode": str(mode).upper() if mode not in (None, "") else None,
    }


def classify_entry_readiness_observe_only(
    signal: dict[str, Any], *, evidence: dict[str, Any]
) -> dict[str, Any]:
    """OBSERVE-ONLY entry-timing classification. Never affects admission."""
    remaining = evidence.get("remaining_opportunity") if isinstance(evidence, dict) else {}
    remaining = remaining if isinstance(remaining, dict) else {}
    fifteen = evidence.get("fifteen_minute_confirmation") if isinstance(evidence, dict) else {}
    fifteen = fifteen if isinstance(fifteen, dict) else {}
    five = evidence.get("five_minute_confirmation") if isinstance(evidence, dict) else {}
    five = five if isinstance(five, dict) else {}
    lineage = str((evidence or {}).get("breach_lineage") or "UNKNOWN").upper()
    reasons: list[str] = []

    geom = extract_trade_geometry(signal if isinstance(signal, dict) else {})
    if not geom.get("available"):
        return {
            "classification": "INVALID",
            "observe_only": True,
            "affected_eligibility": False,
            "diagnostics": {
                "reasons": ["missing_geometry", *(geom.get("missing_data") or [])],
            },
        }

    target_reached = bool(remaining.get("target_reached") or remaining.get("target_already_reached"))
    remaining_r = remaining.get("remaining_r")
    try:
        remaining_r_f = float(remaining_r) if remaining_r is not None else None
    except (TypeError, ValueError):
        remaining_r_f = None
    move_consumed = remaining.get("percent_move_consumed")
    try:
        move_consumed_f = float(move_consumed) if move_consumed is not None else None
    except (TypeError, ValueError):
        move_consumed_f = None

    if target_reached or (remaining_r_f is not None and remaining_r_f <= 0):
        reasons.append("target_or_r_exhausted")
        classification = "INVALID"
    elif move_consumed_f is not None and move_consumed_f >= 0.7:
        reasons.append("heavy_extension")
        classification = "REBREACH_PREFERRED"
    elif lineage in {"INITIAL_BREACH", "UNKNOWN"} and (
        fifteen.get("status") in {"MISSING", "MISSING_OR_INVALID", "UNAVAILABLE"}
        or five.get("status") == "MISSING"
        or not fifteen.get("follow_through")
    ):
        reasons.append("weak_or_missing_continuation_on_first_breach")
        classification = "WAIT_CONFIRMATION"
    elif lineage == "REBREACH_AFTER_RESET" and fifteen.get("follow_through"):
        reasons.append("confirmed_rebreach_with_continuation")
        classification = "READY_NOW"
    elif fifteen.get("follow_through") and five.get("follow_through"):
        reasons.append("strong_5m_15m_continuation")
        classification = "READY_NOW"
    else:
        reasons.append("default_wait_confirmation")
        classification = "WAIT_CONFIRMATION"

    return {
        "classification": classification,
        "observe_only": True,
        "affected_eligibility": False,
        "diagnostics": {
            "reasons": reasons,
            "breach_lineage": lineage,
            "fifteen_status": fifteen.get("status"),
            "five_status": five.get("status"),
            "remaining_r": remaining_r_f,
            "percent_move_consumed": move_consumed_f,
            "target_reached": target_reached,
        },
    }



def _validate_breach_lifecycle(signal: dict[str, Any]) -> Optional[str]:
    """Reject contradictory or incomplete fenced lifecycle metadata."""
    sig = signal if isinstance(signal, dict) else {}
    metadata = sig.get("metadata") if isinstance(sig.get("metadata"), dict) else {}
    lifecycle = sig.get("breach_lifecycle_sources")
    lifecycle = lifecycle if isinstance(lifecycle, dict) else {}

    def _values(*keys: str) -> list[Any]:
        values: list[Any] = []
        for key in keys:
            values.extend([sig.get(key), metadata.get(key)])
        for key in keys:
            value = lifecycle.get(key)
            if isinstance(value, list):
                values.extend(value)
            elif value not in (None, ""):
                values.append(value)
        return [value for value in values if value not in (None, "")]

    def _has_invalid_integer(values: list[Any], *, allow_zero: bool) -> bool:
        for value in values:
            if isinstance(value, bool):
                return True
            try:
                parsed = int(str(value).strip())
            except (TypeError, ValueError):
                return True
            if parsed < 0 or (parsed == 0 and not allow_zero):
                return True
        return False

    generation_raw = _values(
        "materialization_generation", "recovery_submit_generation",
        "_recovery_pre_claimed_generation", "watcher_generation",
        "lifecycle_generation",
    )
    if _has_invalid_integer(generation_raw, allow_zero=True):
        return "BREACH_LIFECYCLE_GENERATION_INVALID"

    generation_values = [
        parsed for parsed in (
            _strict_positive_int(value)
            for value in _values(
                "materialization_generation", "recovery_submit_generation",
                "_recovery_pre_claimed_generation", "watcher_generation",
                "lifecycle_generation",
            )
        ) if parsed is not None
    ]
    if len(set(generation_values)) > 1:
        return "BREACH_LIFECYCLE_GENERATION_CONFLICT"
    fenced = bool(
        sig.get("recovery_submit_fenced") or sig.get("fenced")
        or metadata.get("recovery_submit_fenced") or metadata.get("fenced")
    )
    if fenced and not generation_values:
        return "BREACH_LIFECYCLE_GENERATION_MISSING"

    attempt_raw = _values(
        "retry_attempt", "retry_attempts", "materialization_retry_attempt",
        "materialization_attempts", "deferred_retry_attempt",
    )
    if _has_invalid_integer(attempt_raw, allow_zero=not generation_values and not fenced):
        return "BREACH_LIFECYCLE_ATTEMPT_INVALID"

    attempt_values = [
        parsed for parsed in (
            _strict_positive_int(value)
            for value in _values(
                "retry_attempt", "retry_attempts", "materialization_retry_attempt",
                "materialization_attempts", "deferred_retry_attempt",
            )
        ) if parsed is not None
    ]
    if len(set(attempt_values)) > 1:
        return "BREACH_LIFECYCLE_ATTEMPT_CONFLICT"
    return None


def _resolve_breach_identity(
    signal: dict[str, Any],
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    local_order_id: str,
    signal_id: str = "",
) -> tuple[dict[str, str], Optional[str]]:
    """Resolve the exact BREACH identity or reject it without enqueueing."""
    sig = signal if isinstance(signal, dict) else {}
    metadata = sig.get("metadata") if isinstance(sig.get("metadata"), dict) else {}
    source_hints = sig.get("breach_identity_sources")
    source_maps = [sig, metadata]
    if isinstance(source_hints, dict):
        source_maps.append(source_hints)

    resolved_client = str(client_id or "").strip().lower()
    if not resolved_client:
        return {}, "BREACH_IDENTITY_MISSING_CLIENT_ID"
    def _source_values(*keys: str) -> list[str]:
        values: list[str] = []
        for source in source_maps:
            for key in keys:
                value = source.get(key)
                if isinstance(value, list):
                    values.extend(str(item or "") for item in value)
                else:
                    values.append(str(value or ""))
        return values

    client_values = _source_values("client_id", "client_email")
    conflict = _identity_conflict("client_id", [resolved_client, *client_values])
    if conflict:
        return {}, conflict

    raw_mode = str(execution_mode or "").strip().upper()
    if raw_mode not in {"LIVE", "PAPER"}:
        return {}, "BREACH_IDENTITY_INVALID_EXECUTION_MODE"
    mode_values = _source_values("execution_mode", "mode")
    mode_conflict = _identity_conflict("execution_mode", [raw_mode, *mode_values])
    if mode_conflict:
        return {}, mode_conflict
    if any(value.strip() and value.strip().upper() not in {"LIVE", "PAPER"}
           for value in mode_values):
        return {}, "BREACH_IDENTITY_INVALID_EXECUTION_MODE"

    resolved_signal_id = str(signal_id or "").strip()
    payload_signal_id = str(sig.get("signal_id") or "").strip()
    metadata_signal_id = str(metadata.get("signal_id") or "").strip()
    hint_signal_ids = _source_values("signal_id")
    signal_conflict = _identity_conflict(
        "signal_id", [resolved_signal_id, payload_signal_id, metadata_signal_id, *hint_signal_ids]
    )
    if signal_conflict:
        return {}, signal_conflict
    resolved_signal_id = resolved_signal_id or payload_signal_id or metadata_signal_id
    if not resolved_signal_id:
        return {}, "BREACH_IDENTITY_MISSING_SIGNAL_ID"

    resolved_local = str(local_order_id or "").strip()
    if not resolved_local:
        return {}, "BREACH_IDENTITY_MISSING_LOCAL_ORDER_ID"
    local_values = _source_values("local_order_id")
    local_conflict = _identity_conflict("local_order_id", [resolved_local, *local_values])
    if local_conflict:
        return {}, local_conflict

    explicit_canonical = str(canonical_signal_id or "").strip()
    payload_canonical = str(sig.get("canonical_signal_id") or "").strip()
    metadata_canonical = str(metadata.get("canonical_signal_id") or "").strip()
    hint_canonicals = _source_values("canonical_signal_id")
    canonical_conflict = _identity_conflict(
        "canonical_signal_id",
        [explicit_canonical, payload_canonical, metadata_canonical, *hint_canonicals],
    )
    if canonical_conflict:
        return {}, canonical_conflict
    resolved_canonical = (
        explicit_canonical or payload_canonical or metadata_canonical
        or next((value.strip() for value in hint_canonicals if value.strip()), "")
    )
    if not resolved_canonical:
        canonical_input = dict(sig)
        canonical_input.pop("canonical_signal_id", None)
        resolved_canonical = _canonical_signal_id(canonical_input, fallback=resolved_signal_id)
    if not resolved_canonical:
        return {}, "BREACH_IDENTITY_MISSING_CANONICAL_SIGNAL_ID"

    lifecycle_error = _validate_breach_lifecycle(sig)
    if lifecycle_error:
        return {}, lifecycle_error

    timestamp_sources = sig.get("breach_timestamp_sources")
    timestamp_sources = timestamp_sources if isinstance(timestamp_sources, dict) else {}
    for field in ("trigger_crossed_at", "trigger_confirmed_at"):
        evidence_values = [
            source.get(field)
            for source in (sig, metadata)
            if source.get(field) not in (None, "")
        ]
        hinted_values = timestamp_sources.get(field)
        if isinstance(hinted_values, list):
            evidence_values.extend(value for value in hinted_values if value not in (None, ""))
        normalized_evidence = []
        for value in evidence_values:
            parsed = _parse_timestamp(value)
            if parsed is None:
                return {}, f"BREACH_EVIDENCE_INVALID_{field.upper()}"
            normalized_evidence.append(parsed.isoformat())
        if len(set(normalized_evidence)) > 1:
            return {}, f"BREACH_EVIDENCE_CONFLICT_{field.upper()}"

    return {
        "client_id": resolved_client,
        "execution_mode": raw_mode,
        "signal_id": resolved_signal_id,
        "canonical_signal_id": resolved_canonical,
        "local_order_id": resolved_local,
    }, None


def _frozen_breach_observation(signal: dict[str, Any]) -> dict[str, Any]:
    price = None
    price_source = ""
    invalid_price = False
    for key in ("underlying_price", "current_price", "breach_price"):
        candidate = _finite_number(signal.get(key))
        if candidate is not None:
            if candidate > 0:
                price = candidate
                price_source = key
                break
            invalid_price = True
    crossed = signal.get("trigger_crossed_at")
    confirmed = signal.get("trigger_confirmed_at")
    return {
        "price": price,
        "source": "frozen_breach_signal" if price is not None else None,
        "price_source": price_source or None,
        "observed_at": str(crossed or confirmed or "") or None,
        "source_timestamp": str(crossed or confirmed or "") or None,
        "age_seconds": 0 if price is not None else None,
        "status": "AVAILABLE" if price is not None else (
            "MISSING_OR_INVALID" if invalid_price else "MISSING"
        ),
    }


def _breach_timing(signal: dict[str, Any]) -> dict[str, Any]:
    raw = signal.get("trigger_crossed_at") or signal.get("breach_timestamp")
    parsed = _parse_timestamp(raw)
    result = {
        "timestamp": str(raw or "") or None,
        "timestamp_status": "AVAILABLE" if parsed else "MISSING_OR_INVALID",
        "exchange_timezone": "America/New_York",
        "rth_session_date": None,
        "minutes_since_rth_open": None,
        "opening_window": "UNKNOWN",
    }
    if parsed is None:
        return result
    local = parsed.astimezone(_ET)
    rth_open = local.replace(hour=9, minute=30, second=0, microsecond=0)
    minutes = round((local - rth_open).total_seconds() / 60.0, 4)
    if local.weekday() >= 5:
        window = "WEEKEND"
    elif minutes < 0:
        window = "PREMARKET"
    elif minutes < 30:
        window = "FIRST_30_MINUTES"
    elif local.hour < 16:
        window = "RTH_AFTER_OPENING_WINDOW"
    else:
        window = "AFTER_RTH"
    result.update({
        "rth_session_date": local.date().isoformat(),
        "minutes_since_rth_open": minutes,
        "opening_window": window,
    })
    return result


def _remaining_opportunity(signal: dict[str, Any]) -> dict[str, Any]:
    def _field(*keys: str) -> Optional[float]:
        for key in keys:
            if key in signal and signal.get(key) not in (None, ""):
                return _finite_number(signal.get(key))
        return None

    side = str(signal.get("side") or signal.get("direction") or "").strip().upper()
    trigger = _field("trigger_price", "trigger")
    stop = _field("stop_price", "stop")
    target = _field("target_price", "target", "pt1")
    current = next(
        (value for key in ("underlying_price", "current_price", "breach_price")
         if (value := _finite_number(signal.get(key))) is not None),
        None,
    )
    result = {
        "status": "AVAILABLE",
        "side": side,
        "trigger_to_target": None,
        "current_to_target": None,
        "current_to_stop": None,
        "move_consumed_pct": None,
        "remaining_R": None,
        "target_already_reached": None,
        "stop_geometry_invalid": None,
    }
    if side not in {"CALL", "PUT"} or None in {trigger, stop, target, current}:
        result["status"] = "MISSING_OR_INVALID"
        return result
    direction = 1.0 if side == "CALL" else -1.0
    total = direction * (target - trigger)
    remaining = direction * (target - current)
    risk = direction * (trigger - stop)
    current_to_stop = direction * (current - stop)
    result.update({
        "trigger_to_target": total,
        "current_to_target": remaining,
        "current_to_stop": current_to_stop,
        "move_consumed_pct": round((1.0 - remaining / total) * 100.0, 4)
        if total > 0 else None,
        "remaining_R": round(remaining / risk, 4) if risk > 0 else None,
        "target_already_reached": remaining <= 0,
        "stop_geometry_invalid": risk <= 0 or current_to_stop <= 0,
    })
    if total <= 0 or risk <= 0 or current_to_stop <= 0:
        result["status"] = "INVALID_GEOMETRY"
    elif remaining <= 0:
        result["status"] = "INVALID_OPPORTUNITY"
    return result


def _directional_confirmation(rows: list[dict[str, Any]], side: str, *, source: str) -> dict[str, Any]:
    if not rows:
        return {"status": "MISSING", "source": None, "bar_count": 0}
    direction = 1 if side == "CALL" else -1 if side == "PUT" else 0
    usable = []
    for row in rows[-3:]:
        open_price = _finite_number(row.get("open"))
        close_price = _finite_number(row.get("close"))
        if open_price is None or close_price is None:
            continue
        usable.append({
            "directional": (close_price - open_price) * direction > 0,
            "body": abs(close_price - open_price),
        })
    if not usable or not direction:
        return {"status": "MISSING_OR_INVALID", "source": source, "bar_count": len(usable)}
    return {
        "status": "AVAILABLE",
        "source": source,
        "bar_count": len(usable),
        "directional_closes": sum(int(row["directional"]) for row in usable),
        "follow_through": bool(len(usable) >= 2 and all(row["directional"] for row in usable[-2:])),
        "body_strength": round(sum(row["body"] for row in usable) / len(usable), 6),
    }


def _build_breach_evidence(
    signal: dict[str, Any], *, data_sources: dict[str, Any],
    provenance: dict[str, Any], observation: dict[str, Any]
) -> dict[str, Any]:
    candles = data_sources.get("candles") or {}
    side = str(signal.get("side") or signal.get("direction") or "").upper()
    five = list(candles.get("5m") or [])
    fifteen = list(candles.get("15m") or [])
    fifteen_source = str(provenance.get("fifteen_minute") or "frozen_signal_15m")
    pattern_identity = resolve_canonical_strategy_pattern(signal)
    lineage = resolve_breach_lineage(signal)
    evidence = {
        "breach_timing": _breach_timing(signal),
        "raw_pattern": pattern_identity.get("raw_pattern"),
        "canonical_strategy_pattern": pattern_identity.get("canonical_strategy_pattern"),
        "canonical_strategy_pattern_status": pattern_identity.get("status"),
        "canonical_strategy_pattern_diagnostics": pattern_identity.get("diagnostics"),
        "trigger_crossed_at_provenance": signal.get("trigger_crossed_at_provenance"),
        "breach_lineage": lineage.get("breach_lineage") or "UNKNOWN",
        "breach_lineage_status": lineage.get("status"),
        "breach_lineage_diagnostics": lineage.get("diagnostics"),
        "direction_reversal_lineage": signal.get("direction_reversal_lineage"),
        "breach_lifecycle": (
            signal.get("breach_lifecycle")
            if isinstance(signal.get("breach_lifecycle"), dict)
            else resolve_public_breach_lifecycle(signal)
        ),
        "breach_observation": observation,
        "remaining_opportunity": _remaining_opportunity(signal),
        "fifteen_minute_confirmation": _directional_confirmation(
            fifteen, side, source=fifteen_source
        ),
        "five_minute_confirmation": _directional_confirmation(
            five, side, source="frozen_signal_5min"
        ) if five else {
            "status": "MISSING",
            "source": None,
            "bar_count": 0,
            "missing_reason": "canonical_5min_source_unavailable",
        },
        "volume_imbalance": {
            "status": "MISSING",
            "source": None,
            "missing_reason": "bid_ask_volume_not_available_from_source",
            "approximation_allowed": False,
        },
    }
    evidence["entry_readiness_observe_only"] = classify_entry_readiness_observe_only(
        signal, evidence=evidence
    )
    return evidence


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
    breach_observation = _frozen_breach_observation(sig) if phase == "BREACH" else {}
    if phase == "BREACH" and breach_observation.get("price") is not None:
        observed_price = breach_observation["price"]
    evaluation_signal = dict(sig)
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
    strat_context = evaluate_higher_timeframe_confluence(evaluation_signal, market_context)
    fvg_context = evaluate_fvg_context(evaluation_signal, market_context)
    sector_context = score_sector_context(evaluation_signal, market_context)
    volume_context = score_volume_confirmation(evaluation_signal, market_context)
    vwap_context = score_vwap_context(evaluation_signal, market_context)
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
    }
    if phase == "BREACH":
        breach_candles = data_sources.get("candles") or {}
        component_statuses.update({
            "fifteen_minute": _component(bool(breach_candles.get("15m"))),
            # Current main has no canonical broker-backed 5m source. Keep the
            # absence explicit rather than deriving 5m evidence from 15m bars.
            "five_minute": _component(bool(breach_candles.get("5m"))),
        })
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
        "data_as_of": (
            _breach_timing(sig).get("timestamp")
            if phase == "BREACH" and _breach_timing(sig).get("timestamp_status") == "AVAILABLE"
            else point_in_time.get("collected_at") or _now_iso()
        ),
        "signal_data_as_of": sig.get("data_as_of") or sig.get("queued_at"),
        "computed_at": _now_iso(),
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
        "underlying_observation": (
            breach_observation if phase == "BREACH" and breach_observation.get("price") is not None
            else observation
        ),
        "enrichment_underlying_observation": observation if phase == "BREACH" else None,
        "data_provenance": point_in_time.get("provenance") or {},
        "parent_snapshot_id": parent_snapshot_id,
        "compatibility_key": "intelligence_evaluation",
    }
    if phase == "BREACH":
        payload["breach_evidence"] = _build_breach_evidence(
            sig,
            data_sources=data_sources,
            provenance=point_in_time.get("provenance") or {},
            observation=payload["underlying_observation"],
        )
        from ap.intelligence_breach_market_structure import freeze_breach_market_structure
        payload["market_structure"] = freeze_breach_market_structure(
            evaluation_signal,
            candles_by_tf=(data_sources.get("candles") or {}),
            fvg_context=fvg_context,
            data_as_of=payload.get("data_as_of"),
        )
        # Keep research timing classification inside breach evidence if present.
        readiness = (payload.get("breach_evidence") or {}).get("entry_readiness_observe_only")
        if isinstance(readiness, dict):
            payload["entry_timing_candidate_observe_only"] = {
                **readiness,
                "observe_only": True,
                "affected_eligibility": False,
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
    raw_execution_mode = job.get("execution_mode") or payload.get("execution_mode")
    execution_mode = normalize_execution_mode(raw_execution_mode)
    local_order_id = str(job.get("local_order_id") or payload.get("local_order_id") or "")
    signal_id = str(job.get("signal_id") or payload.get("signal_id") or signal.get("signal_id") or "")
    if phase == "BREACH":
        # Recheck job columns against the frozen payload before parent lookup,
        # history access, or snapshot construction. A post-enqueue column/meta
        # disagreement is a permanent intelligence rejection, not a reason to
        # score whichever copy happens to be convenient.
        for field in (
            "client_id", "execution_mode", "signal_id", "canonical_signal_id", "local_order_id",
        ):
            if job.get(field) in (None, ""):
                raise BreachMaterializationRejected(
                    f"BREACH_IDENTITY_MISSING_JOB_{field.upper()}"
                )
        identity_signal = dict(signal or {})
        existing_sources = identity_signal.get("breach_identity_sources")
        source_hints = dict(existing_sources) if isinstance(existing_sources, dict) else {}
        field_sources = {
            "client_id": [
                job.get("client_id"), payload.get("client_id"), payload.get("client_email"),
                signal.get("client_id"), signal.get("client_email"),
            ],
            "execution_mode": [
                job.get("execution_mode"), payload.get("execution_mode"), payload.get("mode"),
                signal.get("execution_mode"), signal.get("mode"),
            ],
            "signal_id": [job.get("signal_id"), payload.get("signal_id"), signal.get("signal_id")],
            "canonical_signal_id": [
                job.get("canonical_signal_id"), payload.get("canonical_signal_id"),
                signal.get("canonical_signal_id"),
            ],
            "local_order_id": [
                job.get("local_order_id"), payload.get("local_order_id"),
                signal.get("local_order_id"),
            ],
        }
        for field, values in field_sources.items():
            prior = source_hints.get(field)
            if isinstance(prior, list):
                source_hints[field] = [*prior, *values]
            elif prior not in (None, ""):
                source_hints[field] = [prior, *values]
            else:
                source_hints[field] = values
        identity_signal["breach_identity_sources"] = source_hints
        identity, identity_error = _resolve_breach_identity(
            identity_signal,
            client_id=client_id,
            execution_mode=raw_execution_mode,
            canonical_signal_id=canonical_signal_id,
            local_order_id=local_order_id,
            signal_id=signal_id,
        )
        if identity_error:
            raise BreachMaterializationRejected(identity_error)
        client_id = identity["client_id"]
        execution_mode = identity["execution_mode"]
        canonical_signal_id = identity["canonical_signal_id"]
        local_order_id = identity["local_order_id"]
        signal_id = identity["signal_id"]
        signal = dict(signal or {})
        signal.update(identity)
    parent_snapshot_id = payload.get("parent_snapshot_id")
    parent_link_status = payload.get("parent_link_status")
    if phase in {"PREOPEN", "BREACH"} and not parent_snapshot_id:
        parent_phases = ("PRETRIGGER",) if phase == "PREOPEN" else ("PREOPEN", "PRETRIGGER")
        parent_link_status = "PARENT_NOT_AVAILABLE"
        for parent_phase in parent_phases:
            parent = get_latest_snapshot(
                client_id=client_id,
                execution_mode=execution_mode,
                canonical_signal_id=canonical_signal_id,
                phase=parent_phase,
            )
            if parent.get("ok"):
                parent_snapshot_id = ((parent.get("snapshot") or {}).get("id") or None)
                if parent_snapshot_id:
                    parent_link_status = (
                        "LINKED" if phase == "PREOPEN" else f"LINKED_{parent_phase}"
                    )
                    break
                parent_link_status = f"{parent_phase}_NOT_AVAILABLE"
            else:
                parent_link_status = f"{parent_phase}_LOOKUP_FAILED"
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
    if phase in {"PREOPEN", "BREACH"}:
        context_payload["parent_link_status"] = parent_link_status or "PARENT_NOT_AVAILABLE"
    return {
        "client_id": client_id,
        "execution_mode": execution_mode,
        "canonical_signal_id": canonical_signal_id,
        "signal_id": signal_id or str(context_payload.get("signal_id") or ""),
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


def enqueue_breach_context(
    signal: dict[str, Any],
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str = "",
    local_order_id: str,
    signal_id: str = "",
) -> dict[str, Any]:
    """Enqueue one immutable, observe-only BREACH envelope.

    This function has no execution authority. Identity ambiguity is rejected
    before the intelligence job store is touched so a malformed handoff cannot
    be silently attributed to another client, mode, signal, or order.
    """
    sig = dict(signal or {}) if isinstance(signal, dict) else {}
    identity, error_code = _resolve_breach_identity(
        sig,
        client_id=client_id,
        execution_mode=execution_mode,
        canonical_signal_id=canonical_signal_id,
        local_order_id=local_order_id,
        signal_id=signal_id,
    )
    if error_code:
        return {"ok": False, "inserted": False, "error_code": error_code}

    # Resolve public lifecycle + lineage + pattern BEFORE private-key stripping
    # so restart/preclaimed authority survives freeze.
    pattern_identity = resolve_canonical_strategy_pattern(sig)
    lineage = resolve_breach_lineage(sig)
    public_lifecycle = resolve_public_breach_lifecycle(sig)
    sig = dict(sig)
    sig["raw_pattern"] = pattern_identity.get("raw_pattern")
    sig["canonical_strategy_pattern"] = pattern_identity.get("canonical_strategy_pattern")
    sig["canonical_strategy_pattern_status"] = pattern_identity.get("status")
    sig["breach_lineage"] = lineage.get("breach_lineage") or "UNKNOWN"
    sig["breach_lineage_status"] = lineage.get("status")
    sig["breach_lifecycle"] = public_lifecycle

    frozen_signal = _freeze_value(sig)
    if not isinstance(frozen_signal, dict):
        return {
            "ok": False,
            "inserted": False,
            "error_code": "BREACH_SIGNAL_FREEZE_FAILED",
        }
    # These are the resolved authorities, not caller-provided duplicates.
    frozen_signal.update(identity)
    frozen_signal["breach_lifecycle"] = public_lifecycle
    frozen_signal["breach_lineage"] = lineage.get("breach_lineage") or "UNKNOWN"
    frozen_signal["canonical_strategy_pattern"] = pattern_identity.get(
        "canonical_strategy_pattern"
    )
    frozen_signal["raw_pattern"] = pattern_identity.get("raw_pattern")
    payload = {
        "phase": "BREACH",
        "signal": frozen_signal,
        **identity,
        "observe_only": True,
        "affected_eligibility": False,
    }
    input_hash = _stable_hash(payload)
    return enqueue_intelligence_job(
        client_id=identity["client_id"],
        execution_mode=identity["execution_mode"],
        canonical_signal_id=identity["canonical_signal_id"],
        signal_id=identity["signal_id"],
        local_order_id=identity["local_order_id"],
        phase="BREACH",
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

    def _load() -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
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
            c.execute(
                """
                SELECT o.signal_id, o.local_order_id, o.client_id,
                       o.execution_mode, o.meta, q.payload
                FROM orders o
                JOIN trade_queue q
                  ON q.client_id=o.client_id AND q.signal_id=o.signal_id
                WHERE o.client_id=%s
                  AND o.kind='ENTRY'
                  AND (
                    (
                      o.status IN ('PENDING_TRIGGER','WATCHING')
                      AND o.broker_order_id IS NULL
                    )
                    OR (
                      -- Telemetry-only crash recovery after execution advanced:
                      -- rebuild missing BREACH intel when identity+provenance remain exact.
                      o.status NOT IN ('REJECTED','ERROR','CANCELED','CANCELLED','EXPIRED')
                      AND NULLIF(BTRIM(COALESCE(o.meta->>'trigger_crossed_at', o.meta->>'trigger_confirmed_at')), '') IS NOT NULL
                      AND NULLIF(BTRIM(o.local_order_id), '') IS NOT NULL
                    )
                  )
                  AND upper(COALESCE(o.execution_mode, %s))=%s
                  AND (
                    NULLIF(BTRIM(o.meta->>'trigger_crossed_at'), '') IS NOT NULL
                    OR NULLIF(BTRIM(o.meta->>'trigger_confirmed_at'), '') IS NOT NULL
                  )
                  AND NOT EXISTS (
                    SELECT 1 FROM ap_intelligence_jobs j
                    WHERE j.client_id=o.client_id
                      AND lower(j.execution_mode)=lower(%s)
                      AND j.signal_id=o.signal_id
                      AND COALESCE(NULLIF(BTRIM(j.local_order_id), ''), '__none__')=
                          COALESCE(NULLIF(BTRIM(o.local_order_id), ''), '__none__')
                      AND j.phase='BREACH'
                  )
                ORDER BY o.created_ts DESC
                LIMIT %s
                """,
                (client_id, mode, mode, mode, int(limit or 100)),
            )
            breach_rows = list(c.fetchall() or [])
            return queue_rows, preopen_rows, breach_rows

    try:
        queue_rows, preopen_rows, breach_rows = run_with_retry(_load)
    except Exception as exc:
        return {
            "ok": False, "pretrigger": 0, "preopen": 0,
            "breach": 0, "breach_errors": 0, "error": str(exc)[:500],
        }

    counts = {"pretrigger": 0, "preopen": 0, "breach": 0, "breach_errors": 0}
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
    for row in breach_rows:
        if isinstance(row, dict):
            row_signal_id = str(row.get("signal_id") or "").strip()
            row_local_order_id = str(row.get("local_order_id") or "").strip()
            row_client_id = str(row.get("client_id") or client_id).strip()
            row_mode = str(row.get("execution_mode") or mode).strip()
            raw_meta = row.get("meta") or {}
            raw_payload = row.get("payload") or {}
        else:
            row_signal_id, row_local_order_id, row_client_id, row_mode, raw_meta, raw_payload = row
            row_signal_id = str(row_signal_id or "").strip()
            row_local_order_id = str(row_local_order_id or "").strip()
            row_client_id = str(row_client_id or client_id).strip()
            row_mode = str(row_mode or mode).strip()
        if isinstance(raw_meta, str):
            try:
                raw_meta = json.loads(raw_meta)
            except Exception:
                raw_meta = {}
        if not isinstance(raw_meta, dict):
            raw_meta = {}
        if isinstance(raw_payload, str):
            try:
                raw_payload = json.loads(raw_payload)
            except Exception:
                raw_payload = {}
        if not isinstance(raw_payload, dict):
            raw_payload = {}
        payload_signal = raw_payload.get("signal")
        signal = dict(payload_signal if isinstance(payload_signal, dict) else raw_payload)
        payload_metadata = signal.get("metadata")
        if payload_metadata is not None and not isinstance(payload_metadata, dict):
            counts["breach_errors"] += 1
            continue
        merged_metadata = dict(payload_metadata or {})
        duplicate_conflict = False
        for field in (
            "client_id", "execution_mode", "local_order_id", "canonical_signal_id",
            "signal_id", "trigger_crossed_at", "trigger_confirmed_at",
            "materialization_generation", "recovery_submit_generation",
            "_recovery_pre_claimed_generation", "watcher_generation",
            "retry_attempt", "retry_attempts", "materialization_retry_attempt",
            "materialization_attempts", "deferred_retry_attempt",
            "recovery_submit_fenced", "fenced",
        ):
            if field in merged_metadata and field in raw_meta:
                if str(merged_metadata[field] or "") != str(raw_meta[field] or ""):
                    duplicate_conflict = True
                    break
            elif field in raw_meta:
                merged_metadata[field] = raw_meta[field]
        if duplicate_conflict:
            counts["breach_errors"] += 1
            continue
        if merged_metadata:
            signal["metadata"] = merged_metadata
        for field in (
            "trigger_crossed_at", "trigger_crossed_at_provenance",
            "trigger_confirmed_at", "first_breach_bid", "first_breach_ask",
            "breach_price", "underlying_price", "current_price",
            "breach_lineage", "direction_reversal_lineage",
            "materialization_generation", "recovery_submit_generation",
            "_recovery_pre_claimed_generation", "watcher_generation",
            "retry_attempt", "retry_attempts", "materialization_retry_attempt",
            "materialization_attempts", "deferred_retry_attempt",
            "recovery_submit_fenced", "fenced",
        ):
            if field in raw_meta:
                if field in signal and str(signal.get(field) or "") != str(raw_meta[field] or ""):
                    duplicate_conflict = True
                    break
                signal.setdefault(field, raw_meta[field])
        if duplicate_conflict:
            counts["breach_errors"] += 1
            continue
        signal.setdefault("signal_id", row_signal_id)
        signal.setdefault("client_id", row_client_id)
        signal.setdefault("execution_mode", row_mode)
        signal.setdefault("local_order_id", row_local_order_id)
        result = enqueue_breach_context(
            signal,
            client_id=row_client_id,
            execution_mode=row_mode,
            canonical_signal_id=str(signal.get("canonical_signal_id") or ""),
            local_order_id=row_local_order_id,
            signal_id=row_signal_id,
        )
        if result.get("inserted"):
            counts["breach"] += 1
        elif not result.get("duplicate"):
            counts["breach_errors"] += 1
    return {"ok": True, **counts}
