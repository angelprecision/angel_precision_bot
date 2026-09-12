"""Pure current-main owner for observe-only BREACH snapshot identity.

This module is deliberately narrower than the historical BREACH evaluator.  It
normalizes identity, validates already-materialized PRETRIGGER/PREOPEN parent
rows, and assembles a deterministic envelope around immutable BREACH evidence.
It does not collect evidence, persist a snapshot, or know about execution
runtime objects.

The generic snapshot store remains the persistence owner.  The ``structure``
slot is intentionally only a pass-through slot for the later #621 adapter; no
market-structure calculation or interpretation happens here.
"""

from __future__ import annotations

import copy
import hashlib
import json
import math
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Optional

ASSEMBLY_VERSION = "breach_snapshot_assembly_v1"
BREACH_PHASE = "BREACH"

_UNSET = object()
_IDENTITY_FIELDS = (
    "client_id",
    "execution_mode",
    "signal_id",
    "canonical_signal_id",
    "local_order_id",
    "ticker",
    "side",
    "trigger_crossed_at",
    "materialization_generation",
    "profile_version",
    "model_version",
    "phase",
)
_PARENT_COMPLETE_STATUS = "COMPLETE"
_PARENT_NON_AUTHORITATIVE_STATUSES = frozenset(
    {"PARTIAL", "STALE", "UNAVAILABLE", "ERROR"}
)
_PARENT_KNOWN_STATUSES = _PARENT_NON_AUTHORITATIVE_STATUSES | {
    _PARENT_COMPLETE_STATUS
}
_PARENT_LINK_STATUSES = frozenset(
    {"LINKED", "PRETRIGGER_NOT_AVAILABLE", "PRETRIGGER_LOOKUP_FAILED"}
)
_VOLATILE_HASH_KEYS = frozenset(
    {
        "collected_at",
        "computed_at",
        "created_at",
        "updated_at",
        "worker_id",
        "worker_started_at",
        "worker_finished_at",
        "latency",
    }
)
_CANONICAL_CANDLE_KEYS = frozenset(
    {
        "time",
        "timestamp",
        "start",
        "end",
        "datetime",
        "date",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "vwap",
        "trade_count",
    }
)
_CANONICAL_CANDLE_INTERVALS = (
    "monthly",
    "weekly",
    "daily",
    "5m",
    "15m",
    "1h",
    "4h",
)
_CANONICAL_COVERAGE_KEYS = frozenset(
    {
        "status",
        "coverage_complete",
        "latest_expected_close",
        "latest_observed_close",
        "provider_status",
        "authoritative",
        "source",
        "provider_attempted",
        "provider_succeeded",
        "interval_minutes",
        "expected_close",
        "observed_close",
        "error",
        "missing_reason",
    }
)
_CANONICAL_PROVENANCE_KEYS = frozenset(
    {
        "quote",
        "daily",
        "intraday",
        "intraday_5m",
        "market",
        "sector",
        "source",
        "provider",
    }
)
_CANONICAL_OBSERVATION_KEYS = frozenset(
    {
        "price",
        "current_price",
        "underlying_price",
        "breach_price",
        "value",
        "source",
        "observed_at",
        "source_timestamp",
        "as_of",
        "age_seconds",
    }
)


class _CanonicalSignalError(ValueError):
    """Canonical signal derivation failed or returned unusable output."""


def _unique(values: list[str]) -> list[str]:
    return sorted({str(value) for value in values if str(value)})


def _source_mappings(source: Any) -> list[Mapping[str, Any]]:
    """Return only the known current-main snapshot/payload nesting levels."""
    if not isinstance(source, Mapping):
        return []

    result: list[Mapping[str, Any]] = []
    seen: set[int] = set()
    pending: list[Mapping[str, Any]] = [source]
    while pending:
        current = pending.pop(0)
        marker = id(current)
        if marker in seen:
            continue
        seen.add(marker)
        result.append(current)
        for key in ("identity", "payload", "signal", "metadata", "meta"):
            nested = current.get(key)
            if isinstance(nested, Mapping):
                pending.append(nested)
    return result


def _evidence_mappings(source: Any) -> list[Mapping[str, Any]]:
    """Return known #614/#615 wrapper levels without broad recursive scans."""
    if not isinstance(source, Mapping):
        return []
    result: list[Mapping[str, Any]] = []
    seen: set[int] = set()
    pending: list[Mapping[str, Any]] = [source]
    while pending:
        current = pending.pop(0)
        marker = id(current)
        if marker in seen:
            continue
        seen.add(marker)
        result.append(current)
        for key in (
            "point_in_time",
            "evidence",
            "breach_evidence",
            "frozen_evidence",
            "structure",
            "market_structure",
            "identity",
            "payload",
            "metadata",
            "meta",
            "signal",
            "underlying_observation",
            "frozen_underlying_observation",
        ):
            nested = current.get(key)
            if isinstance(nested, Mapping):
                pending.append(nested)
    return result


def _values_for_keys(source: Any, keys: tuple[str, ...]) -> tuple[list[Any], bool]:
    values: list[Any] = []
    invalid = False
    for mapping in _source_mappings(source):
        for key in keys:
            if key not in mapping:
                continue
            value = mapping.get(key)
            if value is None or value == "":
                continue
            values.append(value)
            if not isinstance(value, str) and key not in {
                "materialization_generation",
                "lifecycle_generation",
                "generation",
                "context_revision",
                "trigger_crossed_at",
                "breach_at",
                "trigger_at",
            }:
                invalid = True
    return values, invalid


def _normalize_text_values(
    source: Any,
    keys: tuple[str, ...],
    *,
    field: str,
    errors: list[str],
    required: bool = False,
    uppercase: bool = False,
) -> str:
    values, invalid = _values_for_keys(source, keys)
    if invalid:
        errors.append(f"{field}_invalid_type")
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        value = value.strip()
        if value:
            normalized.append(value.upper() if uppercase else value)
    distinct = sorted(set(normalized))
    if len(distinct) > 1:
        errors.append(f"{field}_conflict")
    result = distinct[0] if distinct else ""
    if required and not result:
        errors.append(f"{field}_missing")
    return result


def _normalize_optional_generation(
    source: Any, *, errors: list[str]
) -> Optional[int]:
    values, _ = _values_for_keys(
        source,
        ("materialization_generation", "lifecycle_generation", "generation"),
    )
    if not values:
        return None
    normalized: list[int] = []
    for value in values:
        # Do not accept bools, strings, floats, or zero as lifecycle authority.
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            errors.append("materialization_generation_invalid")
            continue
        normalized.append(value)
    if len(set(normalized)) > 1:
        errors.append("materialization_generation_conflict")
    return normalized[0] if normalized else None


def _normalize_optional_revision(
    source: Any, *, errors: list[str]
) -> Optional[int]:
    values, _ = _values_for_keys(source, ("context_revision",))
    if not values:
        return None
    normalized: list[int] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            errors.append("context_revision_invalid")
            continue
        normalized.append(value)
    if len(set(normalized)) > 1:
        errors.append("context_revision_conflict")
    return normalized[0] if normalized else None


def _parse_aware_timestamp(value: Any, *, field: str, errors: list[str]) -> Optional[str]:
    if value is None or value == "":
        errors.append(f"{field}_missing")
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str):
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            errors.append(f"{field}_malformed")
            return None
    else:
        errors.append(f"{field}_invalid_type")
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        errors.append(f"{field}_timezone_aware_required")
        return None
    return parsed.astimezone(timezone.utc).isoformat()


def _timestamp_values(source: Any) -> tuple[list[Any], bool]:
    values, invalid = _values_for_keys(
        source,
        ("trigger_crossed_at", "breach_at", "trigger_at"),
    )
    return values, invalid


def _normalize_trigger(source: Any, *, errors: list[str]) -> Optional[str]:
    values, invalid = _timestamp_values(source)
    if invalid:
        errors.append("trigger_crossed_at_invalid_type")
    normalized: list[str] = []
    for value in values:
        local_errors: list[str] = []
        parsed = _parse_aware_timestamp(
            value, field="trigger_crossed_at", errors=local_errors
        )
        if parsed is None:
            errors.extend(local_errors)
        else:
            normalized.append(parsed)
    distinct = sorted(set(normalized))
    if len(distinct) > 1:
        errors.append("trigger_crossed_at_conflict")
    if not distinct and not values:
        errors.append("trigger_crossed_at_missing")
    return distinct[0] if distinct else None


def _canonical_signal_id(signal_id: str, source: Any = None) -> str:
    """Resolve canonical signal authority through the current-main helper."""
    try:
        from ap_canonical_signal import build_canonical_signal_id

        canonical_payload = next(
            (
                mapping
                for mapping in _source_mappings(source)
                if "canonical_signal_id" in mapping
            ),
            None,
        )
        derived = build_canonical_signal_id(signal_id, canonical_payload)
    except Exception as exc:
        raise _CanonicalSignalError("canonical_signal_derivation_failed") from exc
    if not isinstance(derived, str) or not derived.strip():
        raise _CanonicalSignalError("canonical_signal_derivation_unusable")
    return derived.strip()


def _identity_mapping(value: Any) -> Optional[dict[str, Any]]:
    if not isinstance(value, Mapping):
        return None
    nested = value.get("identity")
    if isinstance(nested, Mapping):
        return dict(nested)
    return {
        key: value.get(key)
        for key in _IDENTITY_FIELDS
        if key in value
    }


def _override_source(source: Any, overrides: Mapping[str, Any]) -> dict[str, Any]:
    """Apply explicit keyword fields without mutating the caller's mapping."""
    merged = dict(source) if isinstance(source, Mapping) else {}
    for key, value in overrides.items():
        if value is not _UNSET:
            merged[key] = value
    return merged


def normalize_breach_identity(
    source: Optional[Mapping[str, Any]] = None,
    *,
    signal: Optional[Mapping[str, Any]] = None,
    identity: Optional[Mapping[str, Any]] = None,
    client_id: Any = _UNSET,
    execution_mode: Any = _UNSET,
    signal_id: Any = _UNSET,
    canonical_signal_id: Any = _UNSET,
    local_order_id: Any = _UNSET,
    ticker: Any = _UNSET,
    side: Any = _UNSET,
    trigger_crossed_at: Any = _UNSET,
    materialization_generation: Any = _UNSET,
    lifecycle_generation: Any = _UNSET,
    generation: Any = _UNSET,
    profile_version: Any = _UNSET,
    model_version: Any = _UNSET,
    context_revision: Any = _UNSET,
    phase: Any = _UNSET,
) -> dict[str, Any]:
    """Return one normalized, fail-closed BREACH semantic identity.

    ``source`` can be a signal, a #327 job payload, or a #327 snapshot row.
    Explicit keyword values win over source values.  Required identity fields
    are never synthesized from ticker, time, or a worker clock.  The result
    exposes the canonical mapping both under ``identity`` and at the top
    level for compatibility with existing payload-shaped helpers.
    """
    base: dict[str, Any] = {}
    if isinstance(source, Mapping):
        base.update(dict(source))
    if isinstance(signal, Mapping):
        base["signal"] = dict(signal)
    if isinstance(identity, Mapping):
        base["identity"] = dict(identity)
    base = _override_source(
        base,
        {
            "client_id": client_id,
            "execution_mode": execution_mode,
            "signal_id": signal_id,
            "canonical_signal_id": canonical_signal_id,
            "local_order_id": local_order_id,
            "ticker": ticker,
            "side": side,
            "trigger_crossed_at": trigger_crossed_at,
            "materialization_generation": materialization_generation,
            "lifecycle_generation": lifecycle_generation,
            "generation": generation,
            "profile_version": profile_version,
            "model_version": model_version,
            "context_revision": context_revision,
            "phase": phase,
        },
    )

    errors: list[str] = []
    client = _normalize_text_values(
        base, ("client_id",), field="client_id", errors=errors, required=True
    )
    mode = _normalize_text_values(
        base,
        ("execution_mode", "mode"),
        field="execution_mode",
        errors=errors,
        required=True,
        uppercase=True,
    )
    if mode and mode not in {"LIVE", "PAPER"}:
        errors.append("execution_mode_invalid")
        mode = ""
    raw_signal_id = _normalize_text_values(
        base, ("signal_id",), field="signal_id", errors=errors, required=True
    )
    explicit_canonical = _normalize_text_values(
        base,
        ("canonical_signal_id",),
        field="canonical_signal_id",
        errors=errors,
    )
    canonical = ""
    derived_canonical: Optional[str] = None
    if raw_signal_id:
        try:
            derived_canonical = _canonical_signal_id(raw_signal_id, base)
        except _CanonicalSignalError as exc:
            errors.append(str(exc))
    if explicit_canonical:
        if derived_canonical is None:
            errors.append("canonical_signal_id_unproven")
        else:
            canonical = derived_canonical
    else:
        canonical = derived_canonical or ""
    if not canonical:
        errors.append("canonical_signal_id_missing")

    local_order = _normalize_text_values(
        base, ("local_order_id",), field="local_order_id", errors=errors
    )
    symbol = _normalize_text_values(
        base,
        ("ticker", "symbol"),
        field="ticker",
        errors=errors,
        required=True,
        uppercase=True,
    )
    normalized_side = _normalize_text_values(
        base,
        ("side", "direction"),
        field="side",
        errors=errors,
        required=True,
        uppercase=True,
    )
    if normalized_side and normalized_side not in {"CALL", "PUT"}:
        errors.append("side_invalid")
        normalized_side = ""

    trigger = _normalize_trigger(base, errors=errors)
    generation_value = _normalize_optional_generation(base, errors=errors)
    revision = _normalize_optional_revision(base, errors=errors)

    profile = _normalize_text_values(
        base, ("profile_version",), field="profile_version", errors=errors
    )
    if not profile:
        try:
            from ap.intelligence_snapshot_store import DEFAULT_PROFILE_VERSION

            profile = str(DEFAULT_PROFILE_VERSION)
        except Exception:
            profile = "intelligence_context_v1_observe_only"

    model = _normalize_text_values(
        base,
        ("model_version", "intelligence_model_version"),
        field="model_version",
        errors=errors,
    )
    phase_name = _normalize_text_values(
        base, ("phase",), field="phase", errors=errors
    ) or BREACH_PHASE
    phase_name = phase_name.upper()
    if phase_name != BREACH_PHASE:
        errors.append("phase_mismatch")

    errors = _unique(errors)
    ok = not errors
    identity_value: Optional[dict[str, Any]] = None
    if ok:
        identity_value = {
            "client_id": client,
            "execution_mode": mode,
            "signal_id": raw_signal_id,
            "canonical_signal_id": canonical,
            "local_order_id": local_order,
            "ticker": symbol,
            "side": normalized_side,
            "trigger_crossed_at": trigger,
            "materialization_generation": generation_value,
            "profile_version": profile,
            "model_version": model,
            "phase": BREACH_PHASE,
        }

    result: dict[str, Any] = {
        "ok": ok,
        "valid": ok,
        "accepted": ok,
        "status": "VALID" if ok else "INVALID",
        "identity": copy.deepcopy(identity_value),
        "errors": errors,
        "warnings": [],
        # #327 persistence bookkeeping is deliberately exposed separately.
        # It is not part of BREACH semantic identity or any BREACH hash.
        "context_revision": revision,
        "observe_only": True,
        "affected_eligibility": False,
    }
    if identity_value is not None:
        result.update(copy.deepcopy(identity_value))
    return result


def _expected_identity(
    identity: Any,
    *,
    client_id: Any = _UNSET,
    execution_mode: Any = _UNSET,
    signal_id: Any = _UNSET,
    canonical_signal_id: Any = _UNSET,
    local_order_id: Any = _UNSET,
    ticker: Any = _UNSET,
    side: Any = _UNSET,
    materialization_generation: Any = _UNSET,
    profile_version: Any = _UNSET,
    phase: Any = _UNSET,
) -> tuple[Optional[dict[str, Any]], list[str]]:
    """Resolve parent-validation expectations without requiring trigger time."""
    raw = _identity_mapping(identity) or {}
    overrides = {
        "client_id": client_id,
        "execution_mode": execution_mode,
        "signal_id": signal_id,
        "canonical_signal_id": canonical_signal_id,
        "local_order_id": local_order_id,
        "ticker": ticker,
        "side": side,
        "materialization_generation": materialization_generation,
        "profile_version": profile_version,
        "phase": phase,
    }
    raw = _override_source(raw, overrides)
    errors: list[str] = []
    values: dict[str, Any] = {}
    values["client_id"] = _normalize_text_values(
        raw, ("client_id",), field="client_id", errors=errors, required=True
    )
    values["execution_mode"] = _normalize_text_values(
        raw,
        ("execution_mode", "mode"),
        field="execution_mode",
        errors=errors,
        required=True,
        uppercase=True,
    )
    if values["execution_mode"] not in {"LIVE", "PAPER"}:
        errors.append("execution_mode_invalid")
    values["signal_id"] = _normalize_text_values(
        raw, ("signal_id",), field="signal_id", errors=errors, required=True
    )
    values["canonical_signal_id"] = _normalize_text_values(
        raw,
        ("canonical_signal_id",),
        field="canonical_signal_id",
        errors=errors,
        required=True,
    )
    if values["signal_id"] and values["canonical_signal_id"]:
        try:
            derived_canonical = _canonical_signal_id(values["signal_id"], raw)
        except _CanonicalSignalError as exc:
            errors.append(str(exc))
        else:
            if values["canonical_signal_id"] != derived_canonical:
                errors.append("canonical_signal_id_mismatch")
    values["local_order_id"] = _normalize_text_values(
        raw, ("local_order_id",), field="local_order_id", errors=errors
    )
    values["trigger_crossed_at"] = _normalize_optional_trigger(raw, errors=errors)
    values["model_version"] = _normalize_text_values(
        raw,
        (
            "model_version",
            "intelligence_model_version",
            "breach_model_version",
            "identity_model_version",
        ),
        field="model_version",
        errors=errors,
    )
    values["ticker"] = _normalize_text_values(
        raw, ("ticker", "symbol"), field="ticker", errors=errors, required=True, uppercase=True
    )
    values["side"] = _normalize_text_values(
        raw,
        ("side", "direction"),
        field="side",
        errors=errors,
        required=True,
        uppercase=True,
    )
    if values["side"] not in {"CALL", "PUT"}:
        errors.append("side_invalid")
    values["profile_version"] = _normalize_text_values(
        raw, ("profile_version",), field="profile_version", errors=errors
    )
    if not values["profile_version"]:
        try:
            from ap.intelligence_snapshot_store import DEFAULT_PROFILE_VERSION

            values["profile_version"] = str(DEFAULT_PROFILE_VERSION)
        except Exception:
            values["profile_version"] = "intelligence_context_v1_observe_only"
    values["materialization_generation"] = _normalize_optional_generation(
        raw, errors=errors
    )
    values["phase"] = (
        _normalize_text_values(raw, ("phase",), field="phase", errors=errors)
        or BREACH_PHASE
    ).upper()
    errors = _unique(errors)
    return (values if not errors else None), errors


def _parent_field_values(snapshot: Mapping[str, Any], key_aliases: tuple[str, ...]) -> tuple[list[Any], bool]:
    return _values_for_keys(snapshot, key_aliases)


def _parent_normalized_text(
    snapshot: Mapping[str, Any],
    keys: tuple[str, ...],
    *,
    field: str,
    errors: list[str],
    uppercase: bool = False,
) -> str:
    values, invalid = _parent_field_values(snapshot, keys)
    if invalid:
        errors.append(f"{field}_invalid_type")
    normalized: list[str] = []
    for value in values:
        if not isinstance(value, str):
            continue
        value = value.strip()
        if value:
            normalized.append(value.upper() if uppercase else value)
    distinct = sorted(set(normalized))
    if len(distinct) > 1:
        errors.append(f"{field}_conflict")
    return distinct[0] if distinct else ""


def _normalize_optional_trigger(source: Any, *, errors: list[str]) -> Optional[str]:
    """Normalize an expected trigger when supplied, without requiring it."""
    values, _ = _timestamp_values(source)
    if not values:
        return None
    normalized: list[str] = []
    for value in values:
        local_errors: list[str] = []
        parsed = _parse_aware_timestamp(
            value, field="trigger_crossed_at", errors=local_errors
        )
        if parsed is None:
            errors.extend(local_errors)
        else:
            normalized.append(parsed)
    distinct = sorted(set(normalized))
    if len(distinct) > 1:
        errors.append("trigger_crossed_at_conflict")
    return distinct[0] if distinct else None


def _parent_trigger(
    snapshot: Mapping[str, Any], *, phase: str, errors: list[str]
) -> Optional[str]:
    """Validate every present parent trigger assertion without requiring it."""
    normalized: list[str] = []
    for mapping in _source_mappings(snapshot):
        for key in ("trigger_crossed_at", "breach_at", "trigger_at"):
            if key not in mapping:
                continue
            local_errors: list[str] = []
            parsed = _parse_aware_timestamp(
                mapping.get(key),
                field=f"{phase}_trigger_crossed_at",
                errors=local_errors,
            )
            if parsed is None:
                errors.extend(local_errors)
            else:
                normalized.append(parsed)
    distinct = sorted(set(normalized))
    if len(distinct) > 1:
        errors.append(f"{phase}_trigger_crossed_at_conflict")
    return distinct[0] if distinct else None


def _parent_model_version(
    snapshot: Mapping[str, Any], *, phase: str, errors: list[str]
) -> Optional[str]:
    """Validate every present durable parent model-version assertion."""
    values: list[str] = []
    aliases = (
        "model_version",
        "intelligence_model_version",
        "breach_model_version",
        "identity_model_version",
    )
    for mapping in _source_mappings(snapshot):
        for key in aliases:
            if key not in mapping:
                continue
            value = mapping.get(key)
            if value in (None, ""):
                errors.append(f"{phase}_model_version_missing")
            elif not isinstance(value, str):
                errors.append(f"{phase}_model_version_invalid_type")
            else:
                normalized = value.strip()
                if normalized:
                    values.append(normalized)
                else:
                    errors.append(f"{phase}_model_version_missing")
    distinct = sorted(set(values))
    if len(distinct) > 1:
        errors.append(f"{phase}_model_version_conflict")
    return distinct[0] if distinct else None


def _parent_generation(snapshot: Mapping[str, Any], *, errors: list[str]) -> Optional[int]:
    return _normalize_optional_generation(snapshot, errors=errors)


def _snapshot_id(snapshot: Mapping[str, Any], *, errors: list[str]) -> str:
    value = snapshot.get("id")
    if value is None or value == "":
        errors.append("snapshot_id_missing")
        return ""
    if isinstance(value, (dict, list, set, tuple)):
        errors.append("snapshot_id_invalid_type")
        return ""
    normalized = str(value).strip()
    if not normalized:
        errors.append("snapshot_id_missing")
    return normalized


def _strict_safety_errors(source: Any, *, prefix: str) -> list[str]:
    """Reject safety-flag lookalikes instead of relying on truthiness."""
    errors: list[str] = []
    for mapping in _source_mappings(source):
        if "observe_only" in mapping:
            value = mapping.get("observe_only")
            if type(value) is not bool:
                errors.append(f"{prefix}_observe_only_invalid_type")
            elif value is not True:
                errors.append(f"{prefix}_observe_only_false")
        if "affected_eligibility" in mapping:
            value = mapping.get("affected_eligibility")
            if type(value) is not bool:
                errors.append(f"{prefix}_affected_eligibility_invalid_type")
            elif value is not False:
                errors.append(f"{prefix}_affected_eligibility_true")
    return _unique(errors)


def _parent_status(snapshot: Mapping[str, Any]) -> tuple[Optional[str], list[str]]:
    """Read every row/payload status assertion without selecting proof by preference."""
    mappings: list[Mapping[str, Any]] = []
    pending: list[Mapping[str, Any]] = [snapshot]
    seen: set[int] = set()
    while pending:
        current = pending.pop(0)
        marker = id(current)
        if marker in seen:
            continue
        seen.add(marker)
        mappings.append(current)
        for key in ("payload", "snapshot", "metadata", "meta"):
            nested = current.get(key)
            if isinstance(nested, Mapping):
                pending.append(nested)

    statuses: list[str] = []
    warnings: list[str] = []
    invalid_assertion = False
    for mapping in mappings:
        for key in ("status", "snapshot_status"):
            if key not in mapping:
                continue
            value = mapping.get(key)
            if value in (None, ""):
                warnings.append("parent_snapshot_status_missing")
                invalid_assertion = True
            elif not isinstance(value, str):
                warnings.append("parent_snapshot_status_invalid_type")
                invalid_assertion = True
            else:
                normalized = value.strip().upper()
                if not normalized:
                    warnings.append("parent_snapshot_status_missing")
                    invalid_assertion = True
                else:
                    statuses.append(normalized)
    distinct = sorted(set(statuses))
    if len(distinct) > 1:
        warnings.append("parent_snapshot_status_conflict")
    if any(status not in _PARENT_KNOWN_STATUSES for status in distinct):
        warnings.append("parent_snapshot_status_unrecognized")
    # A malformed, unknown, or contradictory assertion can never be repaired
    # by selecting a recognized COMPLETE assertion beside it.
    if invalid_assertion or len(distinct) > 1 or any(
        status not in _PARENT_KNOWN_STATUSES for status in distinct
    ):
        return None, _unique(warnings)
    if not distinct:
        return None, _unique(warnings)
    return distinct[0], _unique(warnings)


def _parent_pointer(
    snapshot: Mapping[str, Any], *, errors: list[str]
) -> Optional[str]:
    values: list[Any] = []
    for mapping in _source_mappings(snapshot):
        for key in ("parent_snapshot_id", "parent_id"):
            if key not in mapping or mapping.get(key) in (None, ""):
                continue
            values.append(mapping.get(key))
    normalized: list[str] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            errors.append("parent_snapshot_id_invalid_type")
            continue
        text = str(value).strip()
        if text:
            normalized.append(text)
    distinct = sorted(set(normalized))
    if len(distinct) > 1:
        errors.append("parent_snapshot_id_conflict")
    return distinct[0] if distinct else None


def _parent_link_status(
    snapshot: Mapping[str, Any], *, diagnostics: Optional[list[str]] = None
) -> Optional[str]:
    """Read all durable PREOPEN linkage assertions without inventing LINKED."""
    values: list[str] = []
    local_diagnostics: list[str] = []
    for mapping in _source_mappings(snapshot):
        for key in ("parent_link_status",):
            if key not in mapping:
                continue
            value = mapping.get(key)
            if value in (None, ""):
                local_diagnostics.append("parent_link_status_missing")
            elif not isinstance(value, str):
                local_diagnostics.append("parent_link_status_invalid_type")
            elif not value.strip():
                local_diagnostics.append("parent_link_status_missing")
            else:
                values.append(value.strip().upper())
    distinct = sorted(set(values))
    if len(distinct) > 1:
        local_diagnostics.append("parent_link_status_conflict")
    if any(value not in _PARENT_LINK_STATUSES for value in distinct):
        local_diagnostics.append("parent_link_status_unrecognized")
    if local_diagnostics or len(distinct) != 1:
        if diagnostics is not None:
            diagnostics.extend(_unique(local_diagnostics))
        return None
    if diagnostics is not None:
        diagnostics.extend(_unique(local_diagnostics))
    return distinct[0]


def validate_breach_parent_snapshot(
    snapshot: Optional[Mapping[str, Any]],
    *,
    identity: Optional[Mapping[str, Any]] = None,
    expected_identity: Optional[Mapping[str, Any]] = None,
    phase: str,
    client_id: Any = _UNSET,
    execution_mode: Any = _UNSET,
    signal_id: Any = _UNSET,
    canonical_signal_id: Any = _UNSET,
    local_order_id: Any = _UNSET,
    ticker: Any = _UNSET,
    side: Any = _UNSET,
    materialization_generation: Any = _UNSET,
    profile_version: Any = _UNSET,
) -> dict[str, Any]:
    """Validate one supplied #327 PRETRIGGER or PREOPEN snapshot row.

    This function never looks up another row and never joins on ticker or
    time.  A missing parent is explicit PARTIAL/MISSING telemetry.  A supplied
    but incompatible parent is REJECTED and is never returned as accepted.
    """
    requested_phase = str(phase or "").strip().upper()
    errors: list[str] = []
    warnings: list[str] = []
    parent_link_status: Optional[str] = None
    parent_link_diagnostics: list[str] = []
    if requested_phase not in {"PRETRIGGER", "PREOPEN"}:
        errors.append("parent_phase_invalid")
    expected, expected_errors = _expected_identity(
        expected_identity or identity,
        client_id=client_id,
        execution_mode=execution_mode,
        signal_id=signal_id,
        canonical_signal_id=canonical_signal_id,
        local_order_id=local_order_id,
        ticker=ticker,
        side=side,
        materialization_generation=materialization_generation,
        profile_version=profile_version,
        phase="BREACH",
    )
    errors.extend(expected_errors)

    if snapshot is None:
        missing_code = f"{requested_phase or 'PARENT'}_parent_missing"
        return {
            "ok": not errors,
            "accepted": False,
            "proven": False,
            "status": "MISSING",
            "phase": requested_phase,
            "snapshot_id": None,
            "candidate_snapshot_id": None,
            "authoritative_parent_snapshot_id": None,
            "parent_snapshot_id": None,
            "parent_link_status": parent_link_status,
            "parent_link_status_diagnostics": parent_link_diagnostics,
            "snapshot": None,
            "missing": True,
            "mismatches": [],
            "errors": _unique(errors),
            "warnings": _unique([missing_code]),
            "observe_only": True,
            "affected_eligibility": False,
        }

    if not isinstance(snapshot, Mapping):
        errors.append("parent_snapshot_invalid_type")
        return {
            "ok": False,
            "accepted": False,
            "proven": False,
            "status": "REJECTED",
            "phase": requested_phase,
            "snapshot_id": None,
            "candidate_snapshot_id": None,
            "authoritative_parent_snapshot_id": None,
            "parent_snapshot_id": None,
            "parent_link_status": parent_link_status,
            "parent_link_status_diagnostics": parent_link_diagnostics,
            "snapshot": None,
            "missing": False,
            "mismatches": _unique(errors),
            "errors": _unique(errors),
            "warnings": [],
            "observe_only": True,
            "affected_eligibility": False,
        }

    # ``get_latest_snapshot`` returns a result wrapper, while the durable row
    # itself is under ``snapshot``.  Accept that known #327 wrapper without
    # performing a lookup or treating an arbitrary nested object as a parent.
    if (
        "id" not in snapshot
        and isinstance(snapshot.get("snapshot"), Mapping)
        and ("ok" in snapshot or "error" in snapshot)
    ):
        snapshot = snapshot["snapshot"]

    candidate_errors: list[str] = []
    candidate_snapshot_id = _snapshot_id(snapshot, errors=candidate_errors)
    parent_pointer = _parent_pointer(snapshot, errors=errors)
    if requested_phase == "PREOPEN":
        parent_link_status = _parent_link_status(
            snapshot, diagnostics=parent_link_diagnostics
        )
        warnings.extend(parent_link_diagnostics)
    snapshot_copy = copy.deepcopy(dict(snapshot))
    status_name, status_warnings = _parent_status(snapshot)
    warnings.extend(status_warnings)

    if expected is None:
        errors.extend(candidate_errors)
        errors = _unique(errors)
        return {
            "ok": False,
            "accepted": False,
            "proven": False,
            "status": "REJECTED",
            "phase": requested_phase,
            "snapshot_id": None,
            "candidate_snapshot_id": candidate_snapshot_id or None,
            "authoritative_parent_snapshot_id": None,
            "parent_snapshot_id": parent_pointer,
            "parent_link_status": parent_link_status,
            "parent_link_status_diagnostics": parent_link_diagnostics,
            "snapshot": None,
            "missing": False,
            "mismatches": errors,
            "errors": errors,
            "warnings": _unique(warnings),
            "observe_only": True,
            "affected_eligibility": False,
        }

    snapshot_id = candidate_snapshot_id
    actual_phase = _parent_normalized_text(
        snapshot, ("phase",), field="phase", errors=errors, uppercase=True
    )
    if actual_phase != requested_phase:
        errors.append(f"{requested_phase}_phase_mismatch")

    actual = {
        "client_id": _parent_normalized_text(
            snapshot, ("client_id",), field="client_id", errors=errors
        ),
        "execution_mode": _parent_normalized_text(
            snapshot, ("execution_mode", "mode"), field="execution_mode", errors=errors, uppercase=True
        ),
        "signal_id": _parent_normalized_text(
            snapshot, ("signal_id",), field="signal_id", errors=errors
        ),
        "canonical_signal_id": _parent_normalized_text(
            snapshot, ("canonical_signal_id",), field="canonical_signal_id", errors=errors
        ),
        "ticker": _parent_normalized_text(
            snapshot, ("ticker", "symbol"), field="ticker", errors=errors, uppercase=True
        ),
        "side": _parent_normalized_text(
            snapshot, ("side", "direction"), field="side", errors=errors, uppercase=True
        ),
        "profile_version": _parent_normalized_text(
            snapshot, ("profile_version",), field="profile_version", errors=errors
        ),
        "local_order_id": _parent_normalized_text(
            snapshot, ("local_order_id",), field="local_order_id", errors=errors
        ),
    }
    actual["trigger_crossed_at"] = _parent_trigger(
        snapshot, phase=requested_phase, errors=errors
    )
    actual["model_version"] = _parent_model_version(
        snapshot, phase=requested_phase, errors=errors
    )
    actual["materialization_generation"] = _parent_generation(snapshot, errors=errors)
    actual_context_revision = _normalize_optional_revision(snapshot, errors=errors)

    # Required parent identity fields must be present and exactly compatible.
    # context_revision is deliberately absent: it is #327 bookkeeping, not
    # BREACH semantic identity or cross-phase authority.
    for field in (
        "client_id",
        "execution_mode",
        "signal_id",
        "canonical_signal_id",
        "ticker",
        "side",
        "profile_version",
    ):
        expected_value = expected[field]
        actual_value = actual[field]
        if not actual_value:
            errors.append(f"{requested_phase}_{field}_missing")
        elif actual_value != expected_value:
            errors.append(f"{requested_phase}_{field}_mismatch")

    # PREOPEN is local-order scoped. PRETRIGGER is deliberately before the
    # order exists, so an absent PRETRIGGER local order is not a mismatch.
    expected_local = expected.get("local_order_id") or ""
    actual_local = actual.get("local_order_id") or ""
    if requested_phase == "PREOPEN":
        if expected_local and not actual_local:
            errors.append("PREOPEN_local_order_id_missing")
        elif expected_local != actual_local:
            errors.append("PREOPEN_local_order_id_mismatch")
    elif actual_local and expected_local and actual_local != expected_local:
        errors.append("PRETRIGGER_local_order_id_mismatch")
    elif actual_local and not expected_local:
        errors.append("PRETRIGGER_local_order_id_unexpected")

    expected_trigger = expected.get("trigger_crossed_at")
    actual_trigger = actual.get("trigger_crossed_at")
    if expected_trigger and actual_trigger and actual_trigger != expected_trigger:
        errors.append(f"{requested_phase}_trigger_crossed_at_mismatch")

    expected_model = expected.get("model_version") or ""
    actual_model = actual.get("model_version") or ""
    if expected_model and actual_model and actual_model != expected_model:
        errors.append(f"{requested_phase}_model_version_mismatch")

    expected_generation = expected.get("materialization_generation")
    actual_generation = actual.get("materialization_generation")
    authority_proven = True
    if expected_generation is not None:
        if actual_generation is None:
            warnings.append(f"{requested_phase}_materialization_generation_unavailable")
            authority_proven = False
        elif actual_generation != expected_generation:
            errors.append(f"{requested_phase}_materialization_generation_mismatch")
    elif actual_generation is not None:
        errors.append(f"{requested_phase}_materialization_generation_unexpected")

    errors.extend(_strict_safety_errors(snapshot, prefix=f"{requested_phase}_parent"))

    if status_name != _PARENT_COMPLETE_STATUS:
        authority_proven = False
        if status_name is None:
            warnings.append(f"{requested_phase}_parent_status_unproven")
        elif status_name in _PARENT_NON_AUTHORITATIVE_STATUSES:
            warnings.append(f"{requested_phase}_parent_status_{status_name.lower()}")
        else:
            warnings.append(f"{requested_phase}_parent_status_unrecognized")

    errors.extend(candidate_errors)
    errors = _unique(errors)
    warnings = _unique(warnings)
    if errors:
        return {
            "ok": False,
            "accepted": False,
            "proven": False,
            "status": "REJECTED",
            "phase": requested_phase,
            "snapshot_id": None,
            "candidate_snapshot_id": snapshot_id or None,
            "authoritative_parent_snapshot_id": None,
            "parent_snapshot_id": parent_pointer,
            "parent_link_status": parent_link_status,
            "parent_link_status_diagnostics": parent_link_diagnostics,
            "context_revision": actual_context_revision,
            "snapshot": None,
            "missing": False,
            "mismatches": errors,
            "errors": errors,
            "warnings": warnings,
            "observe_only": True,
            "affected_eligibility": False,
        }
    if not authority_proven:
        return {
            "ok": True,
            "accepted": False,
            "proven": False,
            "status": "UNPROVEN",
            "phase": requested_phase,
            "snapshot_id": None,
            "candidate_snapshot_id": snapshot_id or None,
            "authoritative_parent_snapshot_id": None,
            "parent_snapshot_id": parent_pointer,
            "parent_link_status": parent_link_status,
            "parent_link_status_diagnostics": parent_link_diagnostics,
            "context_revision": actual_context_revision,
            "snapshot": snapshot_copy,
            "missing": False,
            "mismatches": [],
            "errors": [],
            "warnings": warnings,
            "observe_only": True,
            "affected_eligibility": False,
        }
    return {
        "ok": True,
        "accepted": True,
        "proven": True,
        "status": "PROVEN",
        "phase": requested_phase,
        "snapshot_id": snapshot_id,
        "candidate_snapshot_id": snapshot_id,
        "authoritative_parent_snapshot_id": snapshot_id,
        "parent_snapshot_id": parent_pointer,
        "parent_link_status": parent_link_status,
        "parent_link_status_diagnostics": parent_link_diagnostics,
        "context_revision": actual_context_revision,
        "snapshot": snapshot_copy,
        "missing": False,
        "mismatches": [],
        "errors": [],
        "warnings": warnings,
        "observe_only": True,
        "affected_eligibility": False,
    }


def _canonicalize(value: Any, *, omit_volatile: bool = True) -> Any:
    """Convert JSON-shaped input to a strict deterministic JSON value."""
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite number cannot be hashed")
        return 0.0 if value == 0 else value
    if isinstance(value, datetime):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("naive datetime cannot be hashed")
        return value.astimezone(timezone.utc).isoformat()
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        for key in sorted(value, key=lambda item: str(item)):
            if not isinstance(key, str):
                raise TypeError("hash input mapping keys must be strings")
            if omit_volatile and key.lower() in _VOLATILE_HASH_KEYS:
                continue
            result[key] = _canonicalize(value[key], omit_volatile=omit_volatile)
        return result
    if isinstance(value, (list, tuple)):
        return [_canonicalize(item, omit_volatile=omit_volatile) for item in value]
    if isinstance(value, (set, frozenset)):
        raise TypeError("unordered collections cannot be hashed")
    raise TypeError(f"unsupported hash input type: {type(value).__name__}")


def _canonicalize_assembly_proof(value: Any) -> Any:
    """Canonicalize the non-volatile fields sealed by the assembly proof.

    This is intentionally stricter than the compatibility canonicalizer above:
    proof inputs must already be JSON-shaped, with string keys, lists instead
    of tuples, and no datetime/object fallback.  Volatile fields are rejected
    instead of being silently incorporated into the proof.
    """
    if value is None or isinstance(value, (str, bool, int)):
        return value
    if isinstance(value, float):
        if not math.isfinite(value):
            raise ValueError("non-finite assembly proof value cannot be hashed")
        return 0.0 if value == 0 else value
    if isinstance(value, Mapping):
        for key in value:
            if not isinstance(key, str):
                raise TypeError("assembly proof mapping keys must be strings")
            if key.lower() in _VOLATILE_HASH_KEYS:
                raise ValueError("volatile assembly proof field cannot be hashed")
        return {
            key: _canonicalize_assembly_proof(value[key])
            for key in sorted(value)
        }
    if isinstance(value, list):
        return [_canonicalize_assembly_proof(item) for item in value]
    raise TypeError(
        "unsupported assembly proof type: "
        f"{type(value).__name__}"
    )


def _identity_for_hash(identity: Any) -> dict[str, Any]:
    candidate = _identity_mapping(identity)
    if candidate is None:
        raise ValueError("canonical BREACH identity is required")
    normalized = normalize_breach_identity(candidate)
    if not normalized.get("ok"):
        details = ",".join(normalized.get("errors") or [])
        raise ValueError("invalid canonical BREACH identity" + (": " + details if details else ""))
    candidate = normalized["identity"]
    # Keep the exact canonical field set; aliases and wrapper bookkeeping do
    # not become alternate identities.
    return {
        "client_id": candidate.get("client_id"),
        "execution_mode": str(candidate.get("execution_mode") or "").upper(),
        "signal_id": candidate.get("signal_id"),
        "canonical_signal_id": candidate.get("canonical_signal_id"),
        "local_order_id": candidate.get("local_order_id") or "",
        "ticker": str(candidate.get("ticker") or "").upper(),
        "side": str(candidate.get("side") or "").upper(),
        "trigger_crossed_at": candidate.get("trigger_crossed_at"),
        "materialization_generation": candidate.get("materialization_generation"),
        "profile_version": candidate.get("profile_version"),
        "model_version": candidate.get("model_version") or "",
        "phase": BREACH_PHASE,
    }


def _parent_ids(value: Any) -> dict[str, Optional[str]]:
    if not isinstance(value, Mapping):
        return {"PRETRIGGER": None, "PREOPEN": None}
    result: dict[str, Optional[str]] = {}
    for phase in ("PRETRIGGER", "PREOPEN"):
        candidate = value.get(phase) or value.get(phase.lower())
        if isinstance(candidate, Mapping):
            candidate = candidate.get("id") or candidate.get("snapshot_id")
        if candidate in (None, ""):
            result[phase] = None
        elif isinstance(candidate, bool) or not isinstance(candidate, (str, int)):
            raise TypeError("parent snapshot IDs must be strings or integers")
        else:
            result[phase] = str(candidate).strip() or None
    return result


def hash_breach_snapshot_input(
    identity: Mapping[str, Any],
    evidence: Optional[Mapping[str, Any]] = None,
    *,
    parent_snapshot_ids: Optional[Mapping[str, Any]] = None,
    parent_ids: Optional[Mapping[str, Any]] = None,
    structure: Any = None,
    frozen_structure: Any = None,
) -> str:
    """Hash canonical identity + immutable evidence + exact parent links.

    Only the explicit canonical #614 evidence projection is hashed.  Worker
    timestamps and arbitrary caller mappings cannot become BREACH identity.
    ``as_of`` and ``trigger_crossed_at`` remain part of the hash.  Strict JSON
    conversion rejects object reprs and unordered collections instead of
    turning them into accidental identity inputs.
    """
    identity_value = _identity_for_hash(identity)
    selected_structure = structure if structure is not None else frozen_structure
    selected_evidence = project_breach_evidence(evidence) if evidence is not None else {}
    hash_input = {
        "assembly_version": ASSEMBLY_VERSION,
        "identity": identity_value,
        "parent_snapshot_ids": _parent_ids(
            parent_snapshot_ids if parent_snapshot_ids is not None else parent_ids
        ),
        "evidence": selected_evidence,
        "structure": _project_structure(selected_structure),
    }
    canonical = _canonicalize(hash_input)
    encoded = json.dumps(
        canonical,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def hash_breach_identity(identity: Mapping[str, Any]) -> str:
    """Hash only the canonical BREACH identity (no parent/evidence input)."""
    return hash_breach_snapshot_input(identity, {}, parent_snapshot_ids={})


def hash_breach_assembly_proof(envelope: Mapping[str, Any]) -> str:
    """Hash the final #625 assembly authority fields.

    The proof binds the exact status, both parent maps, parent validation and
    lineage, missing phases, safety flags, and the already-computed #625
    identity/input hashes.  It deliberately excludes evidence bodies and all
    worker/collection/current timestamps; those are covered by the existing
    input hash or are not assembly authority.
    """
    if not isinstance(envelope, Mapping):
        raise TypeError("BREACH assembly proof input must be a mapping")
    proof_input = {
        "assembly_version": ASSEMBLY_VERSION,
        "identity_hash": envelope.get("identity_hash"),
        "input_hash": envelope.get("input_hash"),
        "status": envelope.get("status"),
        "candidate_parent_snapshot_ids": envelope.get(
            "candidate_parent_snapshot_ids"
        ),
        "authoritative_parent_snapshot_ids": envelope.get(
            "authoritative_parent_snapshot_ids"
        ),
        "parent_validation": envelope.get("parent_validation"),
        "parent_lineage": envelope.get("parent_lineage"),
        "missing_parent_phases": envelope.get("missing_parent_phases"),
        "observe_only": envelope.get("observe_only"),
        "affected_eligibility": envelope.get("affected_eligibility"),
    }
    canonical = _canonicalize_assembly_proof(proof_input)
    encoded = json.dumps(
        canonical,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _evidence_as_of(evidence: Mapping[str, Any], *, errors: list[str]) -> Optional[str]:
    candidates: list[Any] = []
    for mapping in _evidence_mappings(evidence):
        for key in ("as_of", "data_as_of", "trigger_crossed_at", "breach_at"):
            if key not in mapping or mapping.get(key) in (None, ""):
                continue
            value = mapping.get(key)
            if key == "data_as_of" and isinstance(value, Mapping):
                value = value.get("BREACH") or value.get("breach")
            if value not in (None, ""):
                candidates.append(value)
    if not candidates:
        errors.append("breach_evidence_as_of_missing")
        return None
    parsed: list[str] = []
    for value in candidates:
        local_errors: list[str] = []
        normalized = _parse_aware_timestamp(value, field="breach_evidence_as_of", errors=local_errors)
        if normalized is None:
            errors.extend(local_errors)
        else:
            parsed.append(normalized)
    distinct = sorted(set(parsed))
    if len(distinct) > 1:
        errors.append("breach_evidence_as_of_conflict")
    return distinct[0] if distinct else None


def _canonical_evidence_root(source: Mapping[str, Any]) -> Mapping[str, Any]:
    """Select the canonical #614/#615 PIT mapping, ignoring wrapper metadata."""
    candidates = _evidence_mappings(source)
    for candidate in candidates:
        if "data_sources" in candidate or "underlying_observation" in candidate:
            return candidate
    for candidate in candidates:
        if any(key in candidate for key in ("as_of", "data_as_of", "provenance")):
            return candidate
    return source


def _projection_timestamp(value: Any, *, field: str) -> str:
    errors: list[str] = []
    normalized = _parse_aware_timestamp(value, field=field, errors=errors)
    if normalized is None:
        raise ValueError(f"{field}_invalid")
    return normalized


def _project_observation(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("canonical underlying observation must be a mapping")
    result: dict[str, Any] = {}
    for key in sorted(value):
        if key not in _CANONICAL_OBSERVATION_KEYS:
            continue
        item = value[key]
        if key in {"as_of", "observed_at", "source_timestamp"} and item not in (None, ""):
            result[key] = _projection_timestamp(item, field=f"underlying_observation_{key}")
        else:
            result[key] = copy.deepcopy(item)
    return result


def _project_candle_row(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TypeError("canonical candle rows must be mappings")
    return {
        key: copy.deepcopy(value[key])
        for key in sorted(value)
        if key in _CANONICAL_CANDLE_KEYS
    }


def _project_candles(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("canonical candles must be a mapping")
    result: dict[str, Any] = {}
    for timeframe in _CANONICAL_CANDLE_INTERVALS:
        if timeframe not in value:
            continue
        rows = value[timeframe]
        if rows is None:
            result[timeframe] = None
        elif isinstance(rows, list):
            result[timeframe] = [_project_candle_row(row) for row in rows]
        else:
            raise TypeError(f"canonical {timeframe} candles must be a list")
    return result


def _project_coverage_mapping(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("canonical coverage must be a mapping")

    def _one(item: Any) -> dict[str, Any]:
        if not isinstance(item, Mapping):
            raise TypeError("canonical coverage entries must be mappings")
        result: dict[str, Any] = {}
        for key in sorted(item):
            if key not in _CANONICAL_COVERAGE_KEYS:
                continue
            field_value = item[key]
            if key in {
                "latest_expected_close",
                "latest_observed_close",
                "expected_close",
                "observed_close",
            } and field_value not in (None, ""):
                result[key] = _projection_timestamp(field_value, field=f"coverage_{key}")
            else:
                result[key] = copy.deepcopy(field_value)
        return result

    result: dict[str, Any] = {}
    interval_keys = set(_CANONICAL_CANDLE_INTERVALS)
    if any(key in value for key in interval_keys):
        for key in sorted(value):
            if key in interval_keys:
                result[key] = _one(value[key])
    else:
        result = _one(value)
    return result


def _project_provenance(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("canonical provenance must be a mapping")
    return {
        key: copy.deepcopy(value[key])
        for key in sorted(value)
        if key in _CANONICAL_PROVENANCE_KEYS
    }


def _same_numeric(left: Any, right: Any) -> bool:
    if isinstance(left, bool) or isinstance(right, bool):
        return False
    try:
        left_float = float(left)
        right_float = float(right)
    except (TypeError, ValueError):
        return left == right
    return math.isfinite(left_float) and math.isfinite(right_float) and left_float == right_float


def project_breach_evidence(source: Optional[Mapping[str, Any]]) -> dict[str, Any]:
    """Project only canonical, immutable #614 decision-time evidence.

    Worker timestamps, arbitrary caller extras, and later quote aliases are
    intentionally not part of this projection.  A price-like value is kept
    when it is located in the canonical frozen observation; a data-source
    ``current_price`` is kept only when it agrees with that observation.
    """
    if source is None:
        return {}
    if not isinstance(source, Mapping):
        raise TypeError("BREACH evidence must be a mapping")
    root = _canonical_evidence_root(source)
    result: dict[str, Any] = {}

    phase = root.get("phase")
    if phase not in (None, ""):
        result["phase"] = str(phase).strip().upper() if isinstance(phase, str) else phase

    as_of = root.get("as_of")
    if as_of in (None, ""):
        as_of = root.get("data_as_of")
        if isinstance(as_of, Mapping):
            as_of = as_of.get("BREACH") or as_of.get("breach")
    if as_of not in (None, ""):
        result["as_of"] = _projection_timestamp(as_of, field="breach_evidence_as_of")

    data_sources = root.get("data_sources")
    if not isinstance(data_sources, Mapping):
        data_sources = {}
    data_sources_result: dict[str, Any] = {}
    raw_candles = data_sources.get("candles")
    if raw_candles is None and "candles" in root:
        raw_candles = root.get("candles")
    if raw_candles is not None:
        data_sources_result["candles"] = _project_candles(raw_candles)

    raw_coverage = data_sources.get("coverage")
    if raw_coverage is None:
        raw_coverage = root.get("coverage")
    if raw_coverage is None:
        raw_coverage = root.get("data_coverage")
    if raw_coverage is not None:
        data_sources_result["coverage"] = _project_coverage_mapping(raw_coverage)

    observation = root.get("underlying_observation")
    if observation is None:
        observation = root.get("frozen_underlying_observation")
    if observation is not None:
        result["underlying_observation"] = _project_observation(observation)
        data_sources_result["underlying_observation"] = copy.deepcopy(
            result["underlying_observation"]
        )

    trend = data_sources.get("trend")
    if isinstance(trend, Mapping) and isinstance(observation, Mapping):
        current_price = trend.get("current_price")
        observed_price = next(
            (
                observation.get(key)
                for key in ("price", "current_price", "underlying_price", "breach_price", "value")
                if observation.get(key) not in (None, "")
            ),
            None,
        )
        if current_price not in (None, "") and _same_numeric(current_price, observed_price):
            data_sources_result["trend"] = {"current_price": copy.deepcopy(current_price)}
    if data_sources_result:
        result["data_sources"] = data_sources_result

    provenance = root.get("provenance")
    if provenance is None:
        provenance = root.get("data_provenance")
    if provenance is not None:
        result["provenance"] = _project_provenance(provenance)

    for key in (
        "schema_version",
        "source_authority",
        "canonical_errors",
        "errors",
        "warnings",
        "component_statuses",
    ):
        if key not in root:
            continue
        value = root.get(key)
        if key in {"errors", "warnings", "canonical_errors"} and value is not None:
            if not isinstance(value, list):
                raise TypeError(f"canonical evidence {key} must be a list")
            result[key] = [copy.deepcopy(item) for item in value]
        else:
            result[key] = copy.deepcopy(value)
    return result


def _project_structure(value: Any) -> Any:
    if value is None:
        return None
    if not isinstance(value, Mapping):
        raise TypeError("BREACH structure must be a mapping")
    result: dict[str, Any] = {}
    direct_keys = {
        "schema_version",
        "model_version",
        "observe_only",
        "affected_eligibility",
        "data_as_of",
        "side",
        "underlying_price",
        "underlying_price_source",
        "trigger_price",
        "session_policy",
        "fvg_zones",
        "relevant_opposing_fvg",
        "fvg_penetration",
        "volume_imbalance",
        "pullback_reclaim_rebreach",
        "regime",
        "setup",
        "invariants",
    }
    for key in sorted(direct_keys):
        if key not in value:
            continue
        field_value = value[key]
        if key == "data_as_of" and field_value not in (None, ""):
            result[key] = _projection_timestamp(field_value, field="structure_data_as_of")
        else:
            result[key] = copy.deepcopy(field_value)
    if "point_in_time" in value:
        result["point_in_time"] = project_breach_evidence(value.get("point_in_time"))
    if "data_coverage" in value:
        result["data_coverage"] = _project_coverage_mapping(value.get("data_coverage"))
    if "data_provenance" in value:
        result["data_provenance"] = _project_provenance(value.get("data_provenance"))
    if "underlying_observation" in value:
        result["underlying_observation"] = _project_observation(value.get("underlying_observation"))
    market_data = value.get("market_data")
    if isinstance(market_data, Mapping) and "candles" in market_data:
        result["market_data"] = {"candles": _project_candles(market_data.get("candles"))}
    return result


def _cross_validate_identity(
    source: Mapping[str, Any],
    identity: Mapping[str, Any],
    *,
    prefix: str,
    structure: bool = False,
) -> list[str]:
    """Reject present conflicting identity fields; absent optional fields pass."""
    errors: list[str] = []
    mappings = _evidence_mappings(source)

    text_fields: tuple[tuple[str, tuple[str, ...], bool, bool], ...] = (
        ("client_id", ("client_id",), False, True),
        ("execution_mode", ("execution_mode", "mode"), True, True),
        ("signal_id", ("signal_id",), False, True),
        ("canonical_signal_id", ("canonical_signal_id",), False, True),
        ("local_order_id", ("local_order_id",), False, False),
        ("ticker", ("ticker", "symbol"), True, True),
        ("side", ("side", "direction"), True, True),
        ("profile_version", ("profile_version",), False, True),
    )
    if structure:
        model_aliases = (
            "identity_model_version",
            "breach_model_version",
            "intelligence_model_version",
        )
        if str(source.get("schema_version") or "").strip() != "breach_market_structure_v1":
            model_aliases += ("model_version",)
    else:
        model_aliases = (
            "identity_model_version",
            "breach_model_version",
            "intelligence_model_version",
            "model_version",
        )
    text_fields += (("model_version", model_aliases, False, False),)

    for field, aliases, uppercase, required_in_identity in text_fields:
        expected_value = identity.get(field)
        if expected_value in (None, "") and not required_in_identity:
            continue
        values: list[str] = []
        for mapping in mappings:
            for key in aliases:
                if key not in mapping or mapping.get(key) in (None, ""):
                    continue
                value = mapping.get(key)
                if not isinstance(value, str):
                    errors.append(f"{prefix}_{field}_invalid_type")
                    continue
                text_value = value.strip()
                if text_value:
                    values.append(text_value.upper() if uppercase else text_value)
        distinct = sorted(set(values))
        if len(distinct) > 1:
            errors.append(f"{prefix}_{field}_conflict")
        if not distinct:
            continue
        if expected_value not in (None, ""):
            normalized_expected = str(expected_value).upper() if uppercase else str(expected_value)
            if any(value != normalized_expected for value in distinct):
                errors.append(f"{prefix}_{field}_mismatch")

    generation_values: list[int] = []
    for mapping in mappings:
        for key in ("materialization_generation", "lifecycle_generation", "generation"):
            if key not in mapping or mapping.get(key) in (None, ""):
                continue
            value = mapping.get(key)
            if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
                errors.append(f"{prefix}_materialization_generation_invalid_type")
            else:
                generation_values.append(value)
    if len(set(generation_values)) > 1:
        errors.append(f"{prefix}_materialization_generation_conflict")
    expected_generation = identity.get("materialization_generation")
    if generation_values and expected_generation is not None and any(
        value != expected_generation for value in generation_values
    ):
        errors.append(f"{prefix}_materialization_generation_mismatch")

    expected_trigger = identity.get("trigger_crossed_at")
    for mapping in mappings:
        for key in (
            "trigger_crossed_at",
            "breach_at",
            "trigger_at",
            "as_of",
            "data_as_of",
        ):
            if key not in mapping or mapping.get(key) in (None, ""):
                continue
            value = mapping.get(key)
            if key == "data_as_of" and isinstance(value, Mapping):
                value = value.get("BREACH") or value.get("breach")
            if value in (None, ""):
                continue
            local_errors: list[str] = []
            normalized = _parse_aware_timestamp(
                value, field=f"{prefix}_{key}", errors=local_errors
            )
            if normalized is None:
                errors.extend(local_errors)
            elif normalized != expected_trigger:
                errors.append(f"{prefix}_{key}_mismatch")
    return _unique(errors)


def _evidence_phase_errors(evidence: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for mapping in _evidence_mappings(evidence):
        phase = mapping.get("phase")
        if phase not in (None, "") and (
            not isinstance(phase, str) or phase.strip().upper() != BREACH_PHASE
        ):
            errors.append("breach_evidence_phase_mismatch")
    errors.extend(_strict_safety_errors(evidence, prefix="breach_evidence"))
    return errors


def _structure_phase_errors(structure: Mapping[str, Any]) -> list[str]:
    """Validate phase and safety metadata on an already-frozen structure slot."""
    errors: list[str] = []
    for mapping in _evidence_mappings(structure):
        phase = mapping.get("phase")
        if phase not in (None, "") and (
            not isinstance(phase, str) or phase.strip().upper() != BREACH_PHASE
        ):
            errors.append("breach_structure_phase_mismatch")
    errors.extend(_strict_safety_errors(structure, prefix="breach_structure"))
    return errors


def _compact_parent_result(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": result.get("status"),
        "accepted": bool(result.get("accepted")),
        "proven": bool(result.get("proven")),
        "snapshot_id": result.get("snapshot_id"),
        "candidate_snapshot_id": result.get("candidate_snapshot_id"),
        "authoritative_parent_snapshot_id": result.get(
            "authoritative_parent_snapshot_id"
        ),
        "parent_snapshot_id": result.get("parent_snapshot_id"),
        "parent_link_status": result.get("parent_link_status"),
        "parent_link_status_diagnostics": list(
            result.get("parent_link_status_diagnostics") or []
        ),
        "context_revision": result.get("context_revision"),
        "mismatches": list(result.get("mismatches") or []),
        "errors": list(result.get("errors") or []),
        "warnings": list(result.get("warnings") or []),
    }


def _validate_parent_lineage(
    pretrigger_result: Mapping[str, Any],
    preopen_result: Mapping[str, Any],
) -> dict[str, Any]:
    """Validate exact #327 pointer and durable PREOPEN link status."""
    pretrigger_candidate = pretrigger_result.get("candidate_snapshot_id")
    preopen_pointer = preopen_result.get("parent_snapshot_id")
    parent_link_status = preopen_result.get("parent_link_status")
    parent_link_diagnostics = list(
        preopen_result.get("parent_link_status_diagnostics") or []
    )

    def _link_warnings(*, pointer_present: bool) -> list[str]:
        warnings = list(parent_link_diagnostics)
        if parent_link_status is None:
            if not parent_link_diagnostics:
                warnings.append("PREOPEN_parent_link_status_missing")
        elif parent_link_status == "LINKED":
            if not pointer_present:
                warnings.append("PREOPEN_parent_link_status_linked_without_pointer")
        elif parent_link_status == "PRETRIGGER_LOOKUP_FAILED":
            warnings.append("PREOPEN_parent_link_status_lookup_failed")
        elif parent_link_status == "PRETRIGGER_NOT_AVAILABLE":
            warnings.append("PREOPEN_parent_link_status_not_available")
        else:
            warnings.append("PREOPEN_parent_link_status_unproven")
        return _unique(warnings)

    def _result(
        *, status: str, errors: list[str], warnings: list[str]
    ) -> dict[str, Any]:
        return {
            "status": status,
            "preopen_parent_snapshot_id": preopen_pointer,
            "expected_pretrigger_snapshot_id": pretrigger_candidate,
            "parent_link_status": parent_link_status,
            "parent_link_status_diagnostics": parent_link_diagnostics,
            "errors": _unique(errors),
            "warnings": _unique(warnings),
        }

    if preopen_result.get("status") == "MISSING":
        return _result(
            status="MISSING",
            errors=[],
            warnings=["PREOPEN_parent_lineage_missing"],
        )

    if preopen_pointer:
        if not pretrigger_candidate:
            return _result(
                status="UNPROVEN",
                errors=[],
                warnings=_link_warnings(pointer_present=True)
                + ["PREOPEN_parent_lineage_unproven"],
            )
        if str(preopen_pointer) != str(pretrigger_candidate):
            return _result(
                status="REJECTED",
                errors=["PREOPEN_parent_snapshot_id_mismatch"],
                warnings=_link_warnings(pointer_present=True),
            )
        if parent_link_status != "LINKED":
            return _result(
                status="UNPROVEN",
                errors=[],
                warnings=_link_warnings(pointer_present=True)
                + ["PREOPEN_parent_lineage_unproven"],
            )
        if not pretrigger_result.get("proven") or not preopen_result.get("proven"):
            return _result(
                status="UNPROVEN",
                errors=[],
                warnings=_link_warnings(pointer_present=True)
                + ["PREOPEN_parent_lineage_unproven"],
            )
        return _result(status="PROVEN", errors=[], warnings=[])

    # A missing pointer is preserved as missing.  Even when a PRETRIGGER row
    # is supplied later, this owner must not invent a historical relationship.
    return _result(
        status="UNPROVEN",
        errors=[],
        warnings=_link_warnings(pointer_present=False)
        + ["PREOPEN_parent_lineage_unproven"],
    )


def build_breach_snapshot_envelope(
    identity: Mapping[str, Any],
    evidence: Optional[Mapping[str, Any]] = None,
    *,
    breach_evidence: Optional[Mapping[str, Any]] = None,
    frozen_evidence: Optional[Mapping[str, Any]] = None,
    pretrigger_snapshot: Optional[Mapping[str, Any]] = None,
    preopen_snapshot: Optional[Mapping[str, Any]] = None,
    pretrigger: Optional[Mapping[str, Any]] = None,
    preopen: Optional[Mapping[str, Any]] = None,
    parent_snapshots: Optional[Mapping[str, Any]] = None,
    structure: Any = None,
    frozen_structure: Any = None,
) -> dict[str, Any]:
    """Build a deterministic, observe-only BREACH assembly envelope.

    Parent mappings are supplied by the caller and validated in place.  This
    owner intentionally has no store lookup, worker clock, broker, selector,
    watcher, or order authority.
    """
    if not isinstance(identity, Mapping) or identity.get("ok") is False:
        return {
            "ok": False,
            "status": "REJECTED",
            "identity": None,
            "identity_hash": None,
            "input_hash": None,
            "assembly_proof_hash": None,
            "parent_snapshot_ids": {"PRETRIGGER": None, "PREOPEN": None},
            "candidate_parent_snapshot_ids": {"PRETRIGGER": None, "PREOPEN": None},
            "authoritative_parent_snapshot_ids": {"PRETRIGGER": None, "PREOPEN": None},
            "structure": None,
            "assembly_version": ASSEMBLY_VERSION,
            "context_revision": None,
            "observe_only": True,
            "affected_eligibility": False,
            "errors": ["canonical_breach_identity_invalid"],
            "warnings": [],
        }

    normalized_identity = normalize_breach_identity(identity)
    if not normalized_identity.get("ok"):
        return {
            "ok": False,
            "status": "REJECTED",
            "identity": None,
            "identity_hash": None,
            "input_hash": None,
            "assembly_proof_hash": None,
            "parent_snapshot_ids": {"PRETRIGGER": None, "PREOPEN": None},
            "candidate_parent_snapshot_ids": {"PRETRIGGER": None, "PREOPEN": None},
            "authoritative_parent_snapshot_ids": {"PRETRIGGER": None, "PREOPEN": None},
            "structure": None,
            "assembly_version": ASSEMBLY_VERSION,
            "context_revision": normalized_identity.get("context_revision"),
            "observe_only": True,
            "affected_eligibility": False,
            "errors": ["canonical_breach_identity_invalid"]
            + list(normalized_identity.get("errors") or []),
            "warnings": [],
        }
    identity_value = dict(normalized_identity["identity"])
    context_revision = normalized_identity.get("context_revision")

    supplied_evidence = (
        evidence
        if evidence is not None
        else breach_evidence
        if breach_evidence is not None
        else frozen_evidence
    )
    errors: list[str] = []
    warnings: list[str] = []
    if not isinstance(supplied_evidence, Mapping):
        errors.append("breach_evidence_missing")
        supplied_evidence = {}
        evidence_as_of = None
    else:
        evidence_as_of = _evidence_as_of(supplied_evidence, errors=errors)
        errors.extend(_evidence_phase_errors(supplied_evidence))
        errors.extend(
            _cross_validate_identity(
                supplied_evidence,
                identity_value,
                prefix="breach_evidence",
            )
        )
        for mapping in _evidence_mappings(supplied_evidence):
            if "errors" in mapping and mapping.get("errors") is not None and not isinstance(
                mapping.get("errors"), list
            ):
                errors.append("breach_evidence_errors_invalid_type")
    trigger = identity_value["trigger_crossed_at"]
    if evidence_as_of is not None and trigger != evidence_as_of:
        errors.append("trigger_crossed_at_evidence_as_of_mismatch")

    parent_source = dict(parent_snapshots or {})
    parent_source.setdefault("PRETRIGGER", pretrigger_snapshot if pretrigger_snapshot is not None else pretrigger)
    parent_source.setdefault("PREOPEN", preopen_snapshot if preopen_snapshot is not None else preopen)
    pretrigger_result = validate_breach_parent_snapshot(
        parent_source.get("PRETRIGGER"), identity=identity_value, phase="PRETRIGGER"
    )
    preopen_result = validate_breach_parent_snapshot(
        parent_source.get("PREOPEN"), identity=identity_value, phase="PREOPEN"
    )
    parent_results = {
        "PRETRIGGER": _compact_parent_result(pretrigger_result),
        "PREOPEN": _compact_parent_result(preopen_result),
    }
    parent_errors = list(pretrigger_result.get("errors") or []) + list(
        preopen_result.get("errors") or []
    )
    parent_rejected = any(
        result.get("status") == "REJECTED"
        for result in (pretrigger_result, preopen_result)
    )
    if parent_rejected:
        errors.extend(parent_errors)
    warnings.extend(pretrigger_result.get("warnings") or [])
    warnings.extend(preopen_result.get("warnings") or [])
    lineage = _validate_parent_lineage(pretrigger_result, preopen_result)
    errors.extend(lineage.get("errors") or [])
    warnings.extend(lineage.get("warnings") or [])
    parent_ids = {
        "PRETRIGGER": pretrigger_result.get("authoritative_parent_snapshot_id"),
        "PREOPEN": preopen_result.get("authoritative_parent_snapshot_id"),
    }
    candidate_parent_ids = {
        "PRETRIGGER": pretrigger_result.get("candidate_snapshot_id"),
        "PREOPEN": preopen_result.get("candidate_snapshot_id"),
    }

    selected_structure = structure if structure is not None else frozen_structure
    if selected_structure is None:
        warnings.append("breach_structure_reserved_for_621")
    elif isinstance(selected_structure, Mapping):
        errors.extend(_structure_phase_errors(selected_structure))
        errors.extend(
            _cross_validate_identity(
                selected_structure,
                identity_value,
                prefix="breach_structure",
                structure=True,
            )
        )
    else:
        errors.append("breach_structure_invalid_type")
        selected_structure = None
    evidence_copy = copy.deepcopy(dict(supplied_evidence))
    structure_copy = copy.deepcopy(selected_structure)
    evidence_errors: list[str] = []
    if isinstance(supplied_evidence, Mapping):
        for mapping in _evidence_mappings(supplied_evidence):
            values = mapping.get("errors")
            if isinstance(values, list):
                evidence_errors.extend(str(item) for item in values)

    errors = _unique(errors)
    warnings = _unique(warnings)
    identity_hash: Optional[str] = None
    input_hash: Optional[str] = None
    canonical_evidence: dict[str, Any] = {}
    if not errors:
        try:
            canonical_evidence = project_breach_evidence(evidence_copy)
            identity_hash = hash_breach_identity(identity_value)
            input_hash = hash_breach_snapshot_input(
                identity_value,
                evidence_copy,
                parent_snapshot_ids=candidate_parent_ids,
                structure=structure_copy,
            )
        except (TypeError, ValueError) as exc:
            errors.append(f"breach_input_hash_invalid:{type(exc).__name__}")

    structure_mapping = structure_copy if isinstance(structure_copy, Mapping) else {}
    source_versions = {
        "assembly_version": ASSEMBLY_VERSION,
        "profile_version": identity_value["profile_version"],
        "model_version": identity_value["model_version"]
        or str(structure_mapping.get("model_version") or ""),
        "structure_schema_version": str(structure_mapping.get("schema_version") or ""),
        "structure_model_version": str(structure_mapping.get("model_version") or ""),
    }
    missing_parents = [
        phase
        for phase, result in (("PRETRIGGER", pretrigger_result), ("PREOPEN", preopen_result))
        if result.get("status") == "MISSING"
    ]
    unproven_parent_authority = any(
        result.get("status") == "UNPROVEN"
        for result in (pretrigger_result, preopen_result)
    )
    status = "REJECTED" if errors else "PARTIAL" if (
        missing_parents
        or unproven_parent_authority
        or lineage.get("status") != "PROVEN"
        or evidence_errors
        or structure_copy is None
    ) else "COMPLETE"
    assembly_proof_hash: Optional[str] = None
    if not errors:
        try:
            assembly_proof_hash = hash_breach_assembly_proof(
                {
                    "identity_hash": identity_hash,
                    "input_hash": input_hash,
                    "status": status,
                    "candidate_parent_snapshot_ids": candidate_parent_ids,
                    "authoritative_parent_snapshot_ids": parent_ids,
                    "parent_validation": parent_results,
                    "parent_lineage": lineage,
                    "missing_parent_phases": missing_parents,
                    "observe_only": True,
                    "affected_eligibility": False,
                }
            )
        except (TypeError, ValueError, OverflowError) as exc:
            errors.append(f"breach_assembly_proof_invalid:{type(exc).__name__}")
            status = "REJECTED"
    result: dict[str, Any] = {
        "ok": not errors,
        "status": status,
        "assembly_version": ASSEMBLY_VERSION,
        "identity": copy.deepcopy(identity_value),
        "breach_identity": copy.deepcopy(identity_value),
        "identity_hash": identity_hash,
        "input_hash": input_hash,
        "assembly_proof_hash": assembly_proof_hash,
        "client_id": identity_value["client_id"],
        "execution_mode": identity_value["execution_mode"],
        "signal_id": identity_value["signal_id"],
        "canonical_signal_id": identity_value["canonical_signal_id"],
        "local_order_id": identity_value["local_order_id"],
        "ticker": identity_value["ticker"],
        "side": identity_value["side"],
        "trigger_crossed_at": identity_value["trigger_crossed_at"],
        "evidence_as_of": evidence_as_of,
        "as_of": evidence_as_of,
        "materialization_generation": identity_value["materialization_generation"],
        "profile_version": identity_value["profile_version"],
        "model_version": identity_value["model_version"],
        "context_revision": context_revision,
        "phase": BREACH_PHASE,
        "parent_snapshot_ids": parent_ids,
        "candidate_parent_snapshot_ids": candidate_parent_ids,
        "authoritative_parent_snapshot_ids": copy.deepcopy(parent_ids),
        "parent_validation": parent_results,
        "parent_lineage": lineage,
        "missing_parent_phases": missing_parents,
        "source_versions": source_versions,
        "evidence": evidence_copy,
        "breach_evidence": copy.deepcopy(evidence_copy),
        "canonical_evidence": canonical_evidence,
        "structure": structure_copy,
        "structure_slot": {
            "status": "ATTACHED" if structure_copy is not None else "RESERVED_FOR_621",
            "value": copy.deepcopy(structure_copy),
        },
        "identity_warnings": list(warnings),
        "identity_errors": errors,
        "evidence_errors": sorted(set(evidence_errors)),
        "warnings": warnings,
        "errors": errors,
        "observe_only": True,
        "affected_eligibility": False,
    }
    return result


# Small aliases make the owner easy for the later adapter to discover without
# creating another persistence or runtime seam.
build_breach_snapshot = build_breach_snapshot_envelope
validate_parent_snapshot = validate_breach_parent_snapshot


__all__ = [
    "ASSEMBLY_VERSION",
    "BREACH_PHASE",
    "build_breach_snapshot",
    "build_breach_snapshot_envelope",
    "hash_breach_assembly_proof",
    "hash_breach_identity",
    "hash_breach_snapshot_input",
    "normalize_breach_identity",
    "project_breach_evidence",
    "validate_breach_parent_snapshot",
    "validate_parent_snapshot",
]
