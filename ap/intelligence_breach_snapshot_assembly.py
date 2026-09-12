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
    "context_revision",
    "phase",
)
_REQUIRED_IDENTITY_FIELDS = (
    "client_id",
    "execution_mode",
    "signal_id",
    "canonical_signal_id",
    "ticker",
    "side",
    "trigger_crossed_at",
    "profile_version",
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


def _unique(values: list[str]) -> list[str]:
    return sorted({str(value) for value in values if str(value)})


def _text(value: Any, *, field: str, errors: list[str], required: bool = False) -> str:
    if value is None or value == "":
        if required:
            errors.append(f"{field}_missing")
        return ""
    if not isinstance(value, str):
        errors.append(f"{field}_invalid_type")
        return ""
    value = value.strip()
    if not value and required:
        errors.append(f"{field}_missing")
    return value


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


def _normalize_optional_revision(source: Any, *, errors: list[str]) -> int:
    values, _ = _values_for_keys(source, ("context_revision",))
    if not values:
        return 1
    normalized: list[int] = []
    for value in values:
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            errors.append("context_revision_invalid")
            continue
        normalized.append(value)
    if len(set(normalized)) > 1:
        errors.append("context_revision_conflict")
    return normalized[0] if normalized else 1


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


def _canonical_signal_id(signal_id: str, source: Any) -> str:
    try:
        from ap_canonical_signal import build_canonical_signal_id

        return str(build_canonical_signal_id(signal_id, source) or "").strip()
    except Exception:
        return signal_id


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
    canonical = explicit_canonical or _canonical_signal_id(raw_signal_id, base)
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
            "context_revision": revision,
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
    values["local_order_id"] = _normalize_text_values(
        raw, ("local_order_id",), field="local_order_id", errors=errors
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
    values["context_revision"] = _normalize_optional_revision(raw, errors=errors)
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
            "status": "MISSING",
            "phase": requested_phase,
            "snapshot_id": None,
            "parent_snapshot_id": None,
            "snapshot": None,
            "missing": True,
            "mismatches": [],
            "errors": _unique(errors),
            "warnings": _unique([missing_code]),
        }
    if not isinstance(snapshot, Mapping):
        errors.append("parent_snapshot_invalid_type")
        return {
            "ok": False,
            "accepted": False,
            "status": "REJECTED",
            "phase": requested_phase,
            "snapshot_id": None,
            "parent_snapshot_id": None,
            "snapshot": None,
            "missing": False,
            "mismatches": _unique(errors),
            "errors": _unique(errors),
            "warnings": [],
        }
    if expected is None:
        return {
            "ok": False,
            "accepted": False,
            "status": "REJECTED",
            "phase": requested_phase,
            "snapshot_id": None,
            "parent_snapshot_id": None,
            "snapshot": None,
            "missing": False,
            "mismatches": _unique(errors),
            "errors": _unique(errors),
            "warnings": [],
        }

    snapshot_id = _snapshot_id(snapshot, errors=errors)
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
        "context_revision": _normalize_optional_revision(snapshot, errors=errors),
    }
    actual["materialization_generation"] = _parent_generation(snapshot, errors=errors)

    # Required parent identity fields must be present and exactly compatible.
    for field in (
        "client_id",
        "execution_mode",
        "signal_id",
        "canonical_signal_id",
        "ticker",
        "side",
        "profile_version",
        "context_revision",
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

    expected_generation = expected.get("materialization_generation")
    actual_generation = actual.get("materialization_generation")
    if expected_generation is not None:
        if actual_generation is None:
            # The current #327 PRETRIGGER/PREOPEN payload shape does not
            # project lifecycle generation into the snapshot row.  Absence is
            # therefore an explicit unproven warning; a supplied stale or
            # malformed generation remains a hard rejection below.
            warnings.append(f"{requested_phase}_materialization_generation_unavailable")
        elif actual_generation != expected_generation:
            errors.append(f"{requested_phase}_materialization_generation_mismatch")
    elif actual_generation is not None:
        errors.append(f"{requested_phase}_materialization_generation_unexpected")

    payload_candidates = _source_mappings(snapshot)
    for payload in payload_candidates:
        if payload.get("observe_only") is False:
            errors.append(f"{requested_phase}_observe_only_false")
        if payload.get("affected_eligibility") is True:
            errors.append(f"{requested_phase}_affected_eligibility_true")

    errors = _unique(errors)
    if errors:
        return {
            "ok": False,
            "accepted": False,
            "status": "REJECTED",
            "phase": requested_phase,
            "snapshot_id": None,
            "parent_snapshot_id": None,
            "snapshot": None,
            "missing": False,
            "mismatches": errors,
            "errors": errors,
            "warnings": _unique(warnings),
        }
    return {
        "ok": True,
        "accepted": True,
        "status": "ACCEPTED",
        "phase": requested_phase,
        "snapshot_id": snapshot_id,
        "parent_snapshot_id": snapshot_id,
        "snapshot": copy.deepcopy(dict(snapshot)),
        "missing": False,
        "mismatches": [],
        "errors": [],
        "warnings": _unique(warnings),
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
        "context_revision": candidate.get("context_revision") or 1,
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

    ``collected_at`` and other worker lifecycle telemetry are removed at every
    nested mapping level.  ``as_of`` and ``trigger_crossed_at`` remain part of
    the hash.  Strict JSON conversion rejects object reprs and unordered
    collections instead of turning them into accidental identity inputs.
    """
    identity_value = _identity_for_hash(identity)
    selected_structure = structure if structure is not None else frozen_structure
    hash_input = {
        "assembly_version": ASSEMBLY_VERSION,
        "identity": identity_value,
        "parent_snapshot_ids": _parent_ids(parent_snapshot_ids or parent_ids),
        "evidence": evidence if evidence is not None else {},
        "structure": selected_structure,
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


def _evidence_phase_errors(evidence: Mapping[str, Any]) -> list[str]:
    errors: list[str] = []
    for mapping in _evidence_mappings(evidence):
        phase = mapping.get("phase")
        if phase not in (None, "") and (
            not isinstance(phase, str) or phase.strip().upper() != BREACH_PHASE
        ):
            errors.append("breach_evidence_phase_mismatch")
        if mapping.get("observe_only") is False:
            errors.append("breach_evidence_observe_only_false")
        if mapping.get("affected_eligibility") is True:
            errors.append("breach_evidence_affected_eligibility_true")
    return errors


def _structure_phase_errors(structure: Mapping[str, Any]) -> list[str]:
    """Validate only safety metadata on an already-frozen structure slot."""
    errors: list[str] = []
    for mapping in _evidence_mappings(structure):
        phase = mapping.get("phase")
        if phase not in (None, "") and (
            not isinstance(phase, str) or phase.strip().upper() != BREACH_PHASE
        ):
            errors.append("breach_structure_phase_mismatch")
        if mapping.get("observe_only") is False:
            errors.append("breach_structure_observe_only_false")
        if mapping.get("affected_eligibility") is True:
            errors.append("breach_structure_affected_eligibility_true")
    return errors


def _compact_parent_result(result: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "status": result.get("status"),
        "accepted": bool(result.get("accepted")),
        "snapshot_id": result.get("snapshot_id"),
        "mismatches": list(result.get("mismatches") or []),
        "errors": list(result.get("errors") or []),
        "warnings": list(result.get("warnings") or []),
    }


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
    raw_identity = _identity_mapping(identity)
    if not isinstance(identity, Mapping) or raw_identity is None or identity.get("ok") is False:
        return {
            "ok": False,
            "status": "REJECTED",
            "identity": None,
            "identity_hash": None,
            "input_hash": None,
            "parent_snapshot_ids": {"PRETRIGGER": None, "PREOPEN": None},
            "structure": None,
            "assembly_version": ASSEMBLY_VERSION,
            "observe_only": True,
            "affected_eligibility": False,
            "errors": ["canonical_breach_identity_invalid"],
            "warnings": [],
        }

    normalized_identity = normalize_breach_identity(raw_identity)
    if not normalized_identity.get("ok"):
        return {
            "ok": False,
            "status": "REJECTED",
            "identity": None,
            "identity_hash": None,
            "input_hash": None,
            "parent_snapshot_ids": {"PRETRIGGER": None, "PREOPEN": None},
            "structure": None,
            "assembly_version": ASSEMBLY_VERSION,
            "observe_only": True,
            "affected_eligibility": False,
            "errors": ["canonical_breach_identity_invalid"]
            + list(normalized_identity.get("errors") or []),
            "warnings": [],
        }
    identity_value = dict(normalized_identity["identity"])

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
    parent_ids = {
        "PRETRIGGER": pretrigger_result.get("snapshot_id")
        if pretrigger_result.get("accepted")
        else None,
        "PREOPEN": preopen_result.get("snapshot_id")
        if preopen_result.get("accepted")
        else None,
    }

    selected_structure = structure if structure is not None else frozen_structure
    if selected_structure is None:
        warnings.append("breach_structure_reserved_for_621")
    elif isinstance(selected_structure, Mapping):
        errors.extend(_structure_phase_errors(selected_structure))
    evidence_copy = copy.deepcopy(dict(supplied_evidence))
    structure_copy = copy.deepcopy(selected_structure)
    evidence_errors = supplied_evidence.get("errors") if isinstance(supplied_evidence, Mapping) else []
    if isinstance(evidence_errors, list):
        evidence_errors = [str(item) for item in evidence_errors]
    else:
        evidence_errors = []

    errors = _unique(errors)
    warnings = _unique(warnings)
    identity_hash: Optional[str] = None
    input_hash: Optional[str] = None
    if not errors:
        try:
            identity_hash = hash_breach_identity(identity_value)
            input_hash = hash_breach_snapshot_input(
                identity_value,
                evidence_copy,
                parent_snapshot_ids=parent_ids,
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
        any("_unavailable" in str(warning) for warning in result.get("warnings") or [])
        for result in (pretrigger_result, preopen_result)
    )
    status = "REJECTED" if errors else "PARTIAL" if (
        missing_parents or unproven_parent_authority or evidence_errors or structure_copy is None
    ) else "COMPLETE"
    result: dict[str, Any] = {
        "ok": not errors,
        "status": status,
        "assembly_version": ASSEMBLY_VERSION,
        "identity": copy.deepcopy(identity_value),
        "breach_identity": copy.deepcopy(identity_value),
        "identity_hash": identity_hash,
        "input_hash": input_hash,
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
        "phase": BREACH_PHASE,
        "parent_snapshot_ids": parent_ids,
        "parent_validation": parent_results,
        "missing_parent_phases": missing_parents,
        "source_versions": source_versions,
        "evidence": evidence_copy,
        "breach_evidence": copy.deepcopy(evidence_copy),
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
    "hash_breach_identity",
    "hash_breach_snapshot_input",
    "normalize_breach_identity",
    "validate_breach_parent_snapshot",
    "validate_parent_snapshot",
]
