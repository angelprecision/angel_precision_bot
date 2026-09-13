"""PR #622: bounded, observe-only BREACH runtime sideband.

The execution callback owns the trade.  This module owns only a best-effort
copy of the already-confirmed BREACH identity and any evidence that is
already attached to the watcher/plan.  The copy is handed to the existing
bounded intelligence executor; the worker then calls the canonical #615,
#625, and #621 owners in that order.

This module deliberately has no selector, watcher, order, broker, position,
queue, database, history, or network dependency.  A failure here is
telemetry-only and cannot change the trading lifecycle.
"""

from __future__ import annotations

import copy
import logging
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from ap.intelligence_context_handoff import submit_intelligence_enqueue

BRIDGE_VERSION = "breach_runtime_bridge_v1"
BREACH_PHASE = "BREACH"
MAX_SEEN_IDENTITIES = 4096

_MISSING = object()
_SEEN_LOCK = threading.Lock()
_SEEN_IDENTITIES: OrderedDict[tuple[str, ...], None] = OrderedDict()
_LATEST_GENERATIONS: OrderedDict[tuple[str, ...], int] = OrderedDict()

log = logging.getLogger("ap.intelligence_breach_runtime_bridge")

# Only fields consumed by the canonical #615 freezer are copied from a live
# signal/plan.  In particular, current quotes and runtime objects are not
# copied into the background payload.
_STRUCTURE_SIGNAL_FIELDS = (
    "ticker",
    "symbol",
    "side",
    "direction",
    "trigger_price",
    "entry_trigger",
    "trigger",
    "trigger_crossed_at",
    "breach_at",
    "breach_lineage",
    "regime",
    "regime_context",
    "volume_imbalance",
)


def _safe_copy(value: Any) -> Any:
    try:
        return copy.deepcopy(value)
    except Exception:
        return _MISSING


def _mapping(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _read(source: Any, key: str, default: Any = _MISSING) -> Any:
    try:
        if isinstance(source, Mapping):
            return source.get(key, default)
        return getattr(source, key, default)
    except Exception:
        return default


def _source_maps(signal: Mapping[str, Any], plan: Any, watched: Any) -> list[Mapping[str, Any]]:
    """Return shallow, selected source views without copying live objects."""
    sources: list[Mapping[str, Any]] = [signal]
    for source in (
        _mapping(signal.get("metadata")),
        _mapping(signal.get("meta")),
    ):
        if source is not None:
            sources.append(source)

    if isinstance(plan, Mapping):
        sources.append(plan)
    else:
        plan_values: dict[str, Any] = {}
        for key in (
            "client_id",
            "execution_mode",
            "mode",
            "signal_id",
            "canonical_signal_id",
            "local_order_id",
            "ticker",
            "symbol",
            "side",
            "direction",
            "trigger_crossed_at",
            "breach_at",
            "materialization_generation",
            "lifecycle_generation",
            "generation",
            "profile_version",
            "model_version",
            "intelligence_model_version",
            "phase",
            "metadata",
        ):
            value = _read(plan, key)
            if value is not _MISSING:
                plan_values[key] = value
        if plan_values:
            sources.append(plan_values)
    for source in (
        _mapping(_read(plan, "metadata")),
        _mapping(_read(plan, "meta")),
    ):
        if source is not None:
            sources.append(source)

    watched_values: dict[str, Any] = {}
    for key in (
        "ticker",
        "side",
        "signal_id",
        "local_order_id",
        "trigger_crossed_at",
    ):
        value = _read(watched, key)
        if value is not _MISSING:
            watched_values[key] = value
    if watched_values:
        sources.append(watched_values)
    return sources


def _resolve_text(
    sources: list[Mapping[str, Any]],
    keys: tuple[str, ...],
    *,
    explicit: Any = _MISSING,
    field: str,
    required: bool = False,
    uppercase: bool = False,
) -> tuple[Any, str | None]:
    values: list[str] = []
    if explicit is not _MISSING and explicit not in (None, ""):
        candidates = (explicit,)
    else:
        candidates = ()
    raw_values: list[Any] = list(candidates)
    for source in sources:
        for key in keys:
            if key in source and source.get(key) not in (None, ""):
                raw_values.append(source.get(key))

    for raw in raw_values:
        if not isinstance(raw, str):
            return None, f"{field}_invalid_type"
        value = raw.strip()
        if value:
            values.append(value.upper() if uppercase else value)
    distinct = sorted(set(values))
    if len(distinct) > 1:
        return None, f"{field}_conflict"
    if not distinct:
        return (None, f"{field}_missing") if required else (None, None)
    return distinct[0], None


def _resolve_generation(
    sources: list[Mapping[str, Any]], explicit: Any = _MISSING
) -> tuple[int | None, str | None]:
    raw_values: list[Any] = []
    if explicit is not _MISSING and explicit is not None:
        raw_values.append(explicit)
    for source in sources:
        for key in ("materialization_generation", "lifecycle_generation", "generation"):
            if key in source and source.get(key) is not None:
                raw_values.append(source.get(key))
    values: list[int] = []
    for raw in raw_values:
        if isinstance(raw, bool) or not isinstance(raw, int) or raw <= 0:
            return None, "materialization_generation_invalid"
        values.append(raw)
    if len(set(values)) > 1:
        return None, "materialization_generation_conflict"
    return (values[0] if values else None), None


def _parse_aware(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        except ValueError:
            return None
    else:
        return None
    try:
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def _resolve_trigger(
    sources: list[Mapping[str, Any]], watched: Any
) -> tuple[Any, datetime | None, str | None]:
    raw_values: list[Any] = []
    watched_trigger = _read(watched, "trigger_crossed_at")
    if watched_trigger is not _MISSING and watched_trigger not in (None, ""):
        raw_values.append(watched_trigger)
    for source in sources:
        for key in ("trigger_crossed_at", "breach_at", "trigger_at"):
            if key in source and source.get(key) not in (None, ""):
                raw_values.append(source.get(key))
    if not raw_values:
        return None, None, "trigger_crossed_at_missing"

    parsed_values: list[datetime] = []
    for raw in raw_values:
        parsed = _parse_aware(raw)
        if parsed is None:
            return None, None, "trigger_crossed_at_invalid"
        parsed_values.append(parsed)
    if len(set(parsed_values)) > 1:
        return None, None, "trigger_crossed_at_conflict"
    # Preserve the watcher value when it exists.  #625 performs the canonical
    # UTC representation; this layer must not substitute a worker clock.
    chosen = raw_values[0]
    return chosen, parsed_values[0], None


def _derive_canonical_signal_id(signal_id: str, supplied: Any) -> tuple[str | None, str | None]:
    """Use the current canonical helper; never reimplement its wire format."""
    if supplied not in (None, ""):
        return supplied, None
    try:
        from ap_canonical_signal import build_canonical_signal_id

        derived = build_canonical_signal_id(signal_id)
    except Exception as exc:
        return None, f"canonical_signal_id_derivation_failed:{type(exc).__name__}"
    if not isinstance(derived, str) or not derived.strip():
        return None, "canonical_signal_id_derivation_unusable"
    return derived.strip(), None


def _first_mapping(
    sources: list[Mapping[str, Any]], keys: tuple[str, ...], explicit: Any = _MISSING
) -> tuple[dict[str, Any] | None, str | None]:
    candidates: list[Any] = []
    if explicit is not _MISSING and explicit is not None:
        candidates.append(explicit)
    for source in sources:
        for key in keys:
            if key in source and source.get(key) is not None:
                candidates.append(source.get(key))
    for candidate in candidates:
        if not isinstance(candidate, Mapping):
            return None, "mapping_invalid"
        copied = _safe_copy(dict(candidate))
        if copied is _MISSING or not isinstance(copied, dict):
            return None, "mapping_copy_failed"
        return copied, None
    return None, None


def _extract_point_in_time(
    sources: list[Mapping[str, Any]],
    *,
    explicit_point_in_time: Any = _MISSING,
    evidence: Mapping[str, Any] | None,
) -> tuple[dict[str, Any] | None, str | None]:
    direct, direct_error = _first_mapping(
        sources,
        (
            "point_in_time",
            "pit_evidence",
            "breach_point_in_time",
            "frozen_point_in_time",
        ),
        explicit_point_in_time,
    )
    if direct_error:
        return None, f"point_in_time_{direct_error}"
    if direct is not None:
        return direct, None
    if isinstance(evidence, Mapping):
        nested = evidence.get("point_in_time")
        if isinstance(nested, Mapping):
            copied = _safe_copy(dict(nested))
            return (
                copied if isinstance(copied, dict) else None,
                None if isinstance(copied, dict) else "point_in_time_copy_failed",
            )
        if any(key in evidence for key in ("as_of", "data_as_of", "underlying_observation", "data_sources")):
            copied = _safe_copy(dict(evidence))
            return (
                copied if isinstance(copied, dict) else None,
                None if isinstance(copied, dict) else "point_in_time_copy_failed",
            )
    return None, None


def _extract_evidence(
    sources: list[Mapping[str, Any]], explicit_evidence: Any = _MISSING
) -> tuple[dict[str, Any] | None, str | None]:
    return _first_mapping(
        sources,
        ("breach_evidence", "evidence", "frozen_evidence", "canonical_evidence"),
        explicit_evidence,
    )


def _extract_parents(
    sources: list[Mapping[str, Any]], explicit_parents: Any = _MISSING
) -> tuple[dict[str, Any], str | None]:
    candidate: Any = explicit_parents
    if candidate is _MISSING or candidate is None:
        for source in sources:
            for key in (
                "parent_snapshots",
                "intelligence_parent_snapshots",
                "breach_parent_snapshots",
            ):
                if key in source and source.get(key) is not None:
                    candidate = source.get(key)
                    break
            if candidate is not _MISSING and candidate is not None:
                break
    if candidate is not _MISSING and candidate is not None:
        if not isinstance(candidate, Mapping):
            return {}, "parent_snapshots_invalid"
        copied = _safe_copy(dict(candidate))
        return (copied, None) if isinstance(copied, dict) else ({}, "parent_snapshots_copy_failed")

    result: dict[str, Any] = {}
    for phase, keys in (
        ("PRETRIGGER", ("pretrigger_snapshot", "pretrigger")),
        ("PREOPEN", ("preopen_snapshot", "preopen")),
    ):
        for source in sources:
            for key in keys:
                value = source.get(key) if key in source else None
                if value is not None:
                    if not isinstance(value, Mapping):
                        return {}, "parent_snapshot_invalid"
                    copied = _safe_copy(dict(value))
                    if not isinstance(copied, dict):
                        return {}, "parent_snapshot_copy_failed"
                    result[phase] = copied
                    break
            if phase in result:
                break
    return result, None


def _diagnostic_evidence(trigger: Any, reason: str) -> dict[str, Any]:
    """Build an explicit PARTIAL marker without inventing market evidence."""
    pit = {
        "phase": BREACH_PHASE,
        "as_of": trigger,
        "data_sources": {"candles": {}, "coverage": {}},
        "underlying_observation": None,
        "provenance": {},
        "errors": [reason],
    }
    return {
        "phase": BREACH_PHASE,
        "as_of": trigger,
        "point_in_time": pit,
        "errors": [reason],
    }


def _evidence_as_of(value: Mapping[str, Any]) -> Any:
    candidates: list[Any] = []
    for key in ("as_of", "data_as_of", "trigger_crossed_at", "breach_at"):
        if key in value and value.get(key) not in (None, ""):
            raw = value.get(key)
            if key == "data_as_of" and isinstance(raw, Mapping):
                raw = raw.get(BREACH_PHASE) or raw.get("breach")
            if raw not in (None, ""):
                candidates.append(raw)
    nested = value.get("point_in_time")
    if isinstance(nested, Mapping):
        nested_value = _evidence_as_of(nested)
        if nested_value not in (None, ""):
            candidates.append(nested_value)
    return candidates[0] if candidates else None


def _prepare_evidence(
    trigger: Any,
    trigger_dt: datetime,
    pit: Mapping[str, Any] | None,
    evidence: Mapping[str, Any] | None,
    evidence_error: str | None,
) -> tuple[dict[str, Any], str, str | None]:
    candidate = evidence if isinstance(evidence, Mapping) else pit
    if pit is None and candidate is None:
        reason = evidence_error or "BREACH_INTEL_EVIDENCE_MISSING"
        return _diagnostic_evidence(trigger, reason), "MISSING", reason

    candidate_as_of = _evidence_as_of(candidate) if isinstance(candidate, Mapping) else None
    pit_as_of = _evidence_as_of(pit) if isinstance(pit, Mapping) else None
    parsed_values: list[datetime] = []
    for raw in (candidate_as_of, pit_as_of):
        if raw not in (None, ""):
            parsed = _parse_aware(raw)
            if parsed is None:
                reason = "BREACH_INTEL_EVIDENCE_MALFORMED"
                return _diagnostic_evidence(trigger, reason), "MALFORMED", reason
            parsed_values.append(parsed)
    if not parsed_values or any(value != trigger_dt for value in parsed_values):
        reason = (
            "BREACH_INTEL_EVIDENCE_MALFORMED"
            if not parsed_values
            else "BREACH_INTEL_EVIDENCE_STALE_OR_MISMATCH"
        )
        return _diagnostic_evidence(trigger, reason), "STALE" if parsed_values else "MALFORMED", reason

    if isinstance(evidence, Mapping):
        prepared = _safe_copy(dict(evidence))
        if not isinstance(prepared, dict):
            reason = "BREACH_INTEL_EVIDENCE_COPY_FAILED"
            return _diagnostic_evidence(trigger, reason), "MALFORMED", reason
        # A separate PIT mapping is retained under the canonical wrapper so
        # #615 and #625 consume the same exact cutoff.
        if isinstance(pit, Mapping) and not isinstance(prepared.get("point_in_time"), Mapping):
            prepared["point_in_time"] = _safe_copy(dict(pit))
        return prepared, "AVAILABLE", None
    prepared = {"point_in_time": _safe_copy(dict(pit or {}))}
    return prepared, "AVAILABLE", None


def _selected_signal(
    signal: Mapping[str, Any],
    plan: Any,
    identity: Mapping[str, Any],
) -> dict[str, Any]:
    sources = _source_maps(signal, plan, None)
    result: dict[str, Any] = {}
    for key in _STRUCTURE_SIGNAL_FIELDS:
        for source in sources:
            if key not in source or source.get(key) in (None, ""):
                continue
            copied = _safe_copy(source.get(key))
            if copied is not _MISSING:
                result[key] = copied
            break
    # The canonical identity values are authoritative for the frozen structure
    # input.  They are added after selected signal fields and are all copies.
    for key in (
        "client_id",
        "execution_mode",
        "signal_id",
        "canonical_signal_id",
        "local_order_id",
        "ticker",
        "side",
        "trigger_crossed_at",
        "materialization_generation",
    ):
        value = identity.get(key)
        if value is not None:
            result[key] = _safe_copy(value)
    return result


def freeze_breach_runtime_artifact(
    signal: Mapping[str, Any] | None,
    approved_plan: Any = None,
    watched: Any = None,
    *,
    client_id: Any = _MISSING,
    execution_mode: Any = _MISSING,
    local_order_id: Any = _MISSING,
    materialization_generation: Any = _MISSING,
    point_in_time: Any = _MISSING,
    evidence: Any = _MISSING,
    parent_snapshots: Any = _MISSING,
) -> dict[str, Any]:
    """Freeze a small, fail-closed runtime artifact without external I/O."""
    started = time.perf_counter()
    if not isinstance(signal, Mapping):
        return {
            "ok": False,
            "bridge_version": BRIDGE_VERSION,
            "fallback_reason": "signal_not_mapping",
            "freeze_ms": round((time.perf_counter() - started) * 1000.0, 3),
        }

    sources = _source_maps(signal, approved_plan, watched)
    errors: list[str] = []
    client, error = _resolve_text(
        sources, ("client_id", "client_email"), explicit=client_id,
        field="client_id", required=True,
    )
    if error:
        errors.append(error)
    mode, error = _resolve_text(
        sources, ("execution_mode", "mode"), explicit=execution_mode,
        field="execution_mode", required=True, uppercase=True,
    )
    if error:
        errors.append(error)
    if mode and mode not in {"LIVE", "PAPER"}:
        errors.append("execution_mode_invalid")
    signal_id, error = _resolve_text(
        sources, ("signal_id",), field="signal_id", required=True,
    )
    if error:
        errors.append(error)
    canonical, error = _resolve_text(
        sources, ("canonical_signal_id",), field="canonical_signal_id",
    )
    if error:
        errors.append(error)
    order_id, error = _resolve_text(
        sources, ("local_order_id",), explicit=local_order_id,
        field="local_order_id", required=True,
    )
    if error:
        errors.append(error)
    ticker, error = _resolve_text(
        sources, ("ticker", "symbol"), field="ticker", required=True,
        uppercase=True,
    )
    if error:
        errors.append(error)
    side, error = _resolve_text(
        sources, ("side", "direction"), field="side", required=True,
        uppercase=True,
    )
    if error:
        errors.append(error)
    if side and side not in {"CALL", "PUT"}:
        errors.append("side_invalid")
    generation, error = _resolve_generation(sources, materialization_generation)
    if error:
        errors.append(error)
    elif generation is None:
        errors.append("materialization_generation_missing")
    profile, error = _resolve_text(
        sources, ("profile_version",), field="profile_version",
    )
    if error:
        errors.append(error)
    model, error = _resolve_text(
        sources, ("model_version", "intelligence_model_version"),
        field="model_version",
    )
    if error:
        errors.append(error)
    phase, error = _resolve_text(
        sources, ("phase",), field="phase",
    )
    if error:
        errors.append(error)
    if phase and phase != BREACH_PHASE:
        errors.append("phase_mismatch")

    trigger, trigger_dt, error = _resolve_trigger(sources, watched)
    if error:
        errors.append(error)

    if not errors:
        canonical, error = _derive_canonical_signal_id(signal_id, canonical)
        if error:
            errors.append(error)

    if errors:
        return {
            "ok": False,
            "bridge_version": BRIDGE_VERSION,
            "fallback_reason": ";".join(sorted(set(errors))),
            "identity": {
                key: _safe_copy(value)
                for key, value in (
                    ("client_id", client),
                    ("execution_mode", mode),
                    ("signal_id", signal_id),
                    ("canonical_signal_id", canonical),
                    ("local_order_id", order_id),
                    ("ticker", ticker),
                    ("side", side),
                    ("trigger_crossed_at", trigger),
                    ("materialization_generation", generation),
                )
                if value is not None and value is not _MISSING
            },
            "freeze_ms": round((time.perf_counter() - started) * 1000.0, 3),
        }

    evidence_mapping, evidence_error = _extract_evidence(sources, evidence)
    pit, pit_error = _extract_point_in_time(
        sources,
        explicit_point_in_time=point_in_time,
        evidence=evidence_mapping,
    )
    # Explicit malformed evidence is a sideband fallback, not a callback
    # exception.  _prepare_evidence turns it into a hashable PARTIAL marker.
    if pit_error and pit is None:
        pit = None
    prepared_evidence, evidence_status, fallback_reason = _prepare_evidence(
        trigger, trigger_dt, pit, evidence_mapping, evidence_error or pit_error
    )
    parents, parent_error = _extract_parents(sources, parent_snapshots)
    if parent_error:
        fallback_reason = fallback_reason or parent_error

    identity: dict[str, Any] = {
        "client_id": client,
        "execution_mode": mode,
        "signal_id": signal_id,
        "local_order_id": order_id,
        "ticker": ticker,
        "side": side,
        "trigger_crossed_at": _safe_copy(trigger),
        "materialization_generation": generation,
        "profile_version": profile,
        "model_version": model,
        "phase": BREACH_PHASE,
    }
    if canonical is not None:
        identity["canonical_signal_id"] = canonical

    artifact = {
        "ok": True,
        "bridge_version": BRIDGE_VERSION,
        "identity": identity,
        "signal": _selected_signal(signal, approved_plan, identity),
        "point_in_time": _safe_copy(pit) if isinstance(pit, Mapping) else None,
        "evidence": prepared_evidence,
        "parent_snapshots": parents,
        "evidence_status": evidence_status,
        "fallback_reason": fallback_reason,
        "evidence_lookup_ms": round((time.perf_counter() - started) * 1000.0, 3),
    }
    artifact["freeze_ms"] = round((time.perf_counter() - started) * 1000.0, 3)
    return artifact


def _identity_key(identity: Mapping[str, Any]) -> tuple[str, ...]:
    """Use semantic tuple fields only; no second identity hash is created."""
    def part(key: str, *, upper: bool = False) -> str:
        value = identity.get(key)
        if isinstance(value, datetime):
            parsed = _parse_aware(value)
            value = parsed.isoformat() if parsed is not None else value.isoformat()
        if not isinstance(value, str):
            return ""
        value = value.strip()
        if key == "trigger_crossed_at":
            parsed = _parse_aware(value)
            if parsed is not None:
                value = parsed.isoformat()
        return value.upper() if upper else value

    return (
        part("client_id"),
        part("execution_mode", upper=True),
        part("signal_id"),
        part("canonical_signal_id"),
        part("local_order_id"),
        part("ticker", upper=True),
        part("side", upper=True),
        part("trigger_crossed_at"),
        str(identity.get("materialization_generation") or ""),
    )


def _reserve_identity(identity: Mapping[str, Any]) -> tuple[bool, str | None, tuple[str, ...]]:
    key = _identity_key(identity)
    generation = identity.get("materialization_generation")
    scope_key = key[:-1]
    with _SEEN_LOCK:
        previous_generation = _LATEST_GENERATIONS.get(scope_key)
        if generation is None and previous_generation is not None:
            return False, "generation_unproven", key
        if (
            previous_generation is not None
            and isinstance(generation, int)
            and generation < previous_generation
        ):
            return False, "stale_generation", key
        if isinstance(generation, int):
            if previous_generation is None or generation > previous_generation:
                _LATEST_GENERATIONS[scope_key] = generation
                _LATEST_GENERATIONS.move_to_end(scope_key)
                while len(_LATEST_GENERATIONS) > MAX_SEEN_IDENTITIES:
                    _LATEST_GENERATIONS.popitem(last=False)
        if key in _SEEN_IDENTITIES:
            _SEEN_IDENTITIES.move_to_end(key)
            return False, "duplicate", key
        _SEEN_IDENTITIES[key] = None
        while len(_SEEN_IDENTITIES) > MAX_SEEN_IDENTITIES:
            _SEEN_IDENTITIES.popitem(last=False)
        return True, None, key


def _release_identity(key: tuple[str, ...]) -> None:
    with _SEEN_LOCK:
        _SEEN_IDENTITIES.pop(key, None)


def reset_breach_bridge_state_for_tests() -> None:
    """Clear the bounded process-local duplicate guard for isolated tests."""
    with _SEEN_LOCK:
        _SEEN_IDENTITIES.clear()
        _LATEST_GENERATIONS.clear()


def _generation_is_current(identity: Mapping[str, Any]) -> tuple[bool, str | None]:
    key = _identity_key(identity)
    scope_key = key[:-1]
    generation = identity.get("materialization_generation")
    with _SEEN_LOCK:
        latest = _LATEST_GENERATIONS.get(scope_key)
    if latest is None:
        return True, None
    if generation is None:
        return False, "generation_unproven"
    if not isinstance(generation, int) or generation < latest:
        return False, "stale_generation"
    return True, None


def _assemble_and_enqueue(artifact: Mapping[str, Any]) -> dict[str, Any]:
    """Background-only #615 -> #625 -> #621 chain."""
    started = time.perf_counter()
    background_started_at = time.time()

    raw_identity = artifact.get("identity") if isinstance(artifact, Mapping) else {}
    telemetry_identity = raw_identity if isinstance(raw_identity, Mapping) else {}

    def _background_result(ok: bool, status: str, **fields: Any) -> dict[str, Any]:
        completed_at = time.time()
        duration_ms = round((time.perf_counter() - started) * 1000.0, 3)
        result = {
            "ok": ok,
            "bridge_version": BRIDGE_VERSION,
            "status": status,
            "background_started_at": background_started_at,
            "background_completed_at": completed_at,
            "background_duration_ms": duration_ms,
            "background_ms": duration_ms,
            **fields,
        }
        # Keep the asynchronous outcome observable without logging the frozen
        # market-data payload.  These are identity/timing/status fields only.
        log.info(
            "BREACH_INTELLIGENCE_BRIDGE_BACKGROUND "
            "client_id=%s mode=%s canonical_signal_id=%s local_order_id=%s "
            "generation=%s trigger_crossed_at=%s status=%s "
            "background_ms=%.3f assembly_status=%s snapshot_enqueue_status=%s "
            "fallback_reason=%s",
            telemetry_identity.get("client_id"),
            telemetry_identity.get("execution_mode"),
            telemetry_identity.get("canonical_signal_id"),
            telemetry_identity.get("local_order_id"),
            telemetry_identity.get("materialization_generation"),
            telemetry_identity.get("trigger_crossed_at"),
            status,
            duration_ms,
            result.get("assembly_status"),
            result.get("snapshot_enqueue_status"),
            result.get("fallback_reason"),
        )
        return result

    try:
        from ap.intelligence_breach_market_structure import (
            freeze_breach_market_structure_from_pit,
        )
        from ap.intelligence_breach_snapshot_assembly import (
            build_breach_snapshot_envelope,
            normalize_breach_identity,
        )
        from ap.intelligence_breach_snapshot_adapter import enqueue_breach_snapshot_job

        identity = copy.deepcopy(dict(artifact.get("identity") or {}))
        current, generation_reason = _generation_is_current(identity)
        if not current:
            return _background_result(
                False,
                "STALE_GENERATION",
                fallback_reason=generation_reason,
                assembly_status="NOT_ATTEMPTED",
                snapshot_enqueue_status="NOT_ATTEMPTED",
            )
        signal = copy.deepcopy(dict(artifact.get("signal") or {}))
        evidence = copy.deepcopy(dict(artifact.get("evidence") or {}))
        pit = copy.deepcopy(artifact.get("point_in_time"))
        if not isinstance(pit, Mapping):
            diagnostic_pit = evidence.get("point_in_time")
            pit = copy.deepcopy(diagnostic_pit) if isinstance(diagnostic_pit, Mapping) else None
        parents = copy.deepcopy(dict(artifact.get("parent_snapshots") or {}))
        identity_result = normalize_breach_identity(identity)
        if not identity_result.get("ok"):
            return _background_result(
                False,
                "IDENTITY_REJECTED",
                fallback_reason=";".join(identity_result.get("errors") or []),
                assembly_status="NOT_ATTEMPTED",
                snapshot_enqueue_status="NOT_ATTEMPTED",
            )

        freeze_started = time.perf_counter()
        structure = freeze_breach_market_structure_from_pit(signal, pit)
        structure_ms = round((time.perf_counter() - freeze_started) * 1000.0, 3)

        # A newer callback may have advanced this opportunity while #615 was
        # running.  Do not let the old worker reach assembly or persistence.
        current, generation_reason = _generation_is_current(identity)
        if not current:
            return _background_result(
                False,
                "STALE_GENERATION",
                fallback_reason=generation_reason,
                structure_ms=structure_ms,
                assembly_status="NOT_ATTEMPTED",
                snapshot_enqueue_status="NOT_ATTEMPTED",
            )

        assembly_started = time.perf_counter()
        envelope = build_breach_snapshot_envelope(
            identity_result,
            evidence,
            parent_snapshots=parents,
            structure=structure,
        )
        assembly_ms = round((time.perf_counter() - assembly_started) * 1000.0, 3)
        if not envelope.get("ok"):
            return _background_result(
                False,
                "ASSEMBLY_REJECTED",
                fallback_reason=";".join(envelope.get("errors") or []),
                structure_ms=structure_ms,
                assembly_ms=assembly_ms,
                assembly_status=envelope.get("status"),
                snapshot_enqueue_status="NOT_ATTEMPTED",
            )

        # Assembly is pure but may still be slower than a new generation
        # callback.  Recheck immediately before the only durable handoff.
        current, generation_reason = _generation_is_current(identity)
        if not current:
            return _background_result(
                False,
                "STALE_GENERATION",
                fallback_reason=generation_reason,
                structure_ms=structure_ms,
                assembly_ms=assembly_ms,
                assembly_status=envelope.get("status"),
                snapshot_enqueue_status="NOT_ATTEMPTED",
            )

        enqueue_started = time.perf_counter()
        enqueue_result = enqueue_breach_snapshot_job(envelope)
        enqueue_ms = round((time.perf_counter() - enqueue_started) * 1000.0, 3)
        ok = bool(enqueue_result.get("ok"))
        return _background_result(
            ok,
            "ENQUEUED" if ok else "ENQUEUE_FAILED",
            fallback_reason=None if ok else enqueue_result.get("error_code"),
            enqueue_result=enqueue_result,
            structure_ms=structure_ms,
            assembly_ms=assembly_ms,
            enqueue_ms=enqueue_ms,
            assembly_status=envelope.get("status"),
            snapshot_enqueue_status="ENQUEUED" if ok else enqueue_result.get("error_code"),
        )
    except Exception as exc:
        log.warning("BREACH_INTELLIGENCE_BRIDGE_BACKGROUND_FAILED: %s", exc)
        return _background_result(
            False,
            "BACKGROUND_FAILED",
            fallback_reason=f"{type(exc).__name__}:{str(exc)[:200]}",
            assembly_status="UNKNOWN",
            snapshot_enqueue_status="UNKNOWN",
        )


def submit_breach_intelligence_nonblocking(
    signal: Mapping[str, Any] | None,
    approved_plan: Any,
    watched: Any,
    *,
    client_id: Any = _MISSING,
    execution_mode: Any = _MISSING,
    local_order_id: Any = _MISSING,
    materialization_generation: Any = _MISSING,
    point_in_time: Any = _MISSING,
    evidence: Any = _MISSING,
    parent_snapshots: Any = _MISSING,
) -> dict[str, Any]:
    """Freeze and hand off BREACH intelligence without waiting.

    The return value is diagnostic only.  It must never be used as a trading
    disposition by the caller.
    """
    started = time.perf_counter()
    artifact = freeze_breach_runtime_artifact(
        signal,
        approved_plan,
        watched,
        client_id=client_id,
        execution_mode=execution_mode,
        local_order_id=local_order_id,
        materialization_generation=materialization_generation,
        point_in_time=point_in_time,
        evidence=evidence,
        parent_snapshots=parent_snapshots,
    )
    base = {
        "bridge_version": BRIDGE_VERSION,
        "identity": copy.deepcopy(artifact.get("identity") or {}),
        "evidence_status": artifact.get("evidence_status"),
        "fallback_reason": artifact.get("fallback_reason"),
        "evidence_lookup_ms": artifact.get("evidence_lookup_ms"),
        "freeze_ms": artifact.get("freeze_ms"),
    }
    identity_for_telemetry = base["identity"]
    if isinstance(identity_for_telemetry, Mapping):
        for key in (
            "client_id",
            "execution_mode",
            "canonical_signal_id",
            "local_order_id",
            "materialization_generation",
            "trigger_crossed_at",
        ):
            base[key] = copy.deepcopy(identity_for_telemetry.get(key))
        base["generation"] = copy.deepcopy(
            identity_for_telemetry.get("materialization_generation")
        )
    base.update(
        freeze_duration_ms=artifact.get("freeze_ms"),
        handoff_submit_duration_ms=None,
        handoff_status="NOT_ATTEMPTED",
        background_started_at=None,
        background_completed_at=None,
        background_duration_ms=None,
        assembly_status=None,
        snapshot_enqueue_status=None,
    )
    if not artifact.get("ok"):
        return {
            **base,
            "ok": True,
            "accepted": False,
            "handoff_status": "INVALID_ARTIFACT",
            "total_ms": round((time.perf_counter() - started) * 1000.0, 3),
        }

    identity = artifact.get("identity")
    if not isinstance(identity, Mapping):
        return {
            **base,
            "ok": True,
            "accepted": False,
            "handoff_status": "INVALID_ARTIFACT",
            "fallback_reason": "identity_missing",
            "total_ms": round((time.perf_counter() - started) * 1000.0, 3),
        }
    reserved, reserve_reason, key = _reserve_identity(identity)
    if not reserved:
        reserve_status = (
            "DUPLICATE"
            if reserve_reason == "duplicate"
            else "STALE_GENERATION"
            if reserve_reason in {"stale_generation", "generation_unproven"}
            else "REJECTED"
        )
        return {
            **base,
            "ok": True,
            "accepted": False,
            "duplicate": reserve_reason == "duplicate",
            "handoff_status": reserve_status,
            "fallback_reason": reserve_reason,
            "total_ms": round((time.perf_counter() - started) * 1000.0, 3),
        }

    try:
        handoff_started = time.perf_counter()
        result = submit_intelligence_enqueue(
            _assemble_and_enqueue,
            artifact,
            phase=BREACH_PHASE,
            signal_id=str(identity.get("signal_id") or ""),
        )
        handoff_ms = round((time.perf_counter() - handoff_started) * 1000.0, 3)
        accepted = bool(result.get("ok") and result.get("accepted"))
        if not accepted:
            _release_identity(key)
        return {
            **base,
            "ok": True,
            "accepted": accepted,
            "handoff_status": "ACCEPTED" if accepted else "SATURATED_OR_REJECTED",
            "handoff_ms": handoff_ms,
            "handoff_submit_duration_ms": handoff_ms,
            "handoff_result": result,
            "total_ms": round((time.perf_counter() - started) * 1000.0, 3),
        }
    except Exception as exc:
        _release_identity(key)
        log.debug("BREACH_INTELLIGENCE_BRIDGE_HANDOFF_FAILED: %s", exc)
        return {
            **base,
            "ok": True,
            "accepted": False,
            "handoff_status": "HANDOFF_FAILED",
            "fallback_reason": f"{type(exc).__name__}:{str(exc)[:200]}",
            "total_ms": round((time.perf_counter() - started) * 1000.0, 3),
        }


__all__ = [
    "BREACH_PHASE",
    "BRIDGE_VERSION",
    "MAX_SEEN_IDENTITIES",
    "freeze_breach_runtime_artifact",
    "reset_breach_bridge_state_for_tests",
    "submit_breach_intelligence_nonblocking",
]
