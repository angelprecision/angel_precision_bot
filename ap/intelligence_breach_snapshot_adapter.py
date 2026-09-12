"""
PR #621 — Durable BREACH Snapshot Adapter (435-C).

Bridges the merged #625 BREACH assembly envelope
(:mod:`ap.intelligence_breach_snapshot_assembly`) into the merged #327 durable
intelligence job/snapshot substrate
(:mod:`ap.intelligence_snapshot_store`).

This module is *persistence infrastructure only*.  It has:

- zero broker / order / position / proof / queue / risk mutation;
- zero market-data or history calls;
- zero eligibility, admission, watcher, selector, or LIVE/PAPER authority.

Failure is *fail-soft*: rejected envelopes, enqueue failures, hash failures, and
worker validation failures return a telemetry dict.  They never raise into a
trading caller and never terminalize a trade, watcher, or setup.

The adapter also does *not* recompute market structure, refetch PIT evidence,
detect FVGs, or reconstruct #614/#615.  It hashes exactly what #625 accepted
and persists exactly what #625 emitted.

Public surface
--------------

- :data:`FROZEN_BREACH_PAYLOAD_KIND` — the exact marker string that flags a job
  payload as a frozen-BREACH job for the worker dispatch seam.
- :data:`ADAPTER_VERSION` — stable version tag stamped into every persisted
  payload for future replay migration.
- :func:`enqueue_breach_snapshot_job` — enqueue a #327 job from a #625
  envelope.
- :func:`build_breach_snapshot_kwargs` — worker-side dispatch that converts a
  claimed job into ``write_snapshot(**kwargs)`` shape (called by
  :mod:`ap.intelligence_context_materializer` when its dispatch guard fires).
- :func:`hash_frozen_breach_structure` — deterministic checksum for the
  already-frozen #615 structure (payload integrity, not semantic identity).
- :func:`is_frozen_breach_job` — cheap predicate used by the dispatch seam.
"""

from __future__ import annotations

import copy
import hashlib
import json
import logging
from typing import Any, Mapping, Optional

from ap.intelligence_breach_snapshot_assembly import (
    ASSEMBLY_VERSION as _BREACH_ASSEMBLY_VERSION,
    BREACH_PHASE,
)
from ap.intelligence_snapshot_store import (
    DEFAULT_PROFILE_VERSION,
    SNAPSHOT_STATUSES,
    enqueue_intelligence_job,
    normalize_execution_mode,
    normalize_phase,
    normalized_local_order_id,
)

log = logging.getLogger("ap.intelligence_breach_snapshot_adapter")


# ---------------------------------------------------------------------------
# Public constants
# ---------------------------------------------------------------------------

#: The exact marker string a #327 job payload must carry to be treated as a
#: frozen BREACH job by :func:`build_breach_snapshot_kwargs`.  Any other value
#: (including missing / empty / a different BREACH kind) MUST leave the generic
#: PRETRIGGER/PREOPEN/BREACH path in
#: :mod:`ap.intelligence_context_materializer` unchanged.
FROZEN_BREACH_PAYLOAD_KIND: str = "FROZEN_BREACH_V1"

#: Stable adapter version stamped into every persisted frozen-BREACH payload
#: so future replay tooling can migrate deterministically.
ADAPTER_VERSION: str = "breach_snapshot_adapter_v1"

#: Snapshot status returned to #327 for a #625 ``COMPLETE`` envelope.
_STATUS_COMPLETE: str = "COMPLETE"

#: Snapshot status returned to #327 for a #625 ``PARTIAL`` envelope.
_STATUS_PARTIAL: str = "PARTIAL"

#: Envelope statuses that MUST NOT produce an authoritative snapshot.  These
#: are turned into fail-soft telemetry at the enqueue seam.
_REJECTED_ENVELOPE_STATUSES: frozenset[str] = frozenset({"REJECTED"})


# ---------------------------------------------------------------------------
# Fail-soft telemetry helper
# ---------------------------------------------------------------------------


def _telemetry(ok: bool, **fields: Any) -> dict[str, Any]:
    """Return a fail-soft telemetry dict.  Never raises.

    Callers are trading-adjacent code paths that must never see an exception
    from this adapter.  Every failure mode routes through here.
    """
    return {"ok": bool(ok), **fields}


# ---------------------------------------------------------------------------
# Deterministic structure hash (payload integrity, NOT semantic identity)
# ---------------------------------------------------------------------------


def _canonicalize_for_hash(value: Any) -> Any:
    """Deterministically canonicalize a value for JSON hashing.

    - Mappings become dicts with keys sorted lexicographically.  Dict insertion
      order therefore cannot influence the hash.
    - Lists/tuples are hashed in their given order (order is meaningful for
      candle rows and lifecycle histories).
    - Scalars are returned as-is; unknown types are represented by their str()
      form only after being wrapped in a stable ``{"__repr__": str(value)}``
      envelope, so no accidental raw ``repr()`` embedding sneaks in.
    """
    if isinstance(value, Mapping):
        return {
            str(key): _canonicalize_for_hash(value[key])
            for key in sorted(value.keys(), key=lambda item: str(item))
        }
    if isinstance(value, (list, tuple)):
        return [_canonicalize_for_hash(item) for item in value]
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return {"__repr__": str(value)}


def hash_frozen_breach_structure(structure: Any) -> str:
    """Return a deterministic checksum for the already-frozen #615 structure.

    This is a *payload integrity / replay* checksum.  It is NOT a competing
    semantic identity, and it is NOT computed by re-running the #615 freezer.

    The requirements from the #621 spec (§9):

    - JSON-shaped data only;
    - sorted keys;
    - stable separators;
    - no object ``repr``;
    - no worker timestamp;
    - no current quote;
    - dict insertion order cannot change the hash;
    - changing actual frozen FVG / strong-break / VI / reclaim evidence must
      change the hash.

    ``None`` returns the empty-structure hash tag so replay bookkeeping stays
    stable even when a PARTIAL envelope has no attached structure.
    """
    canonical = _canonicalize_for_hash(structure)
    if canonical is None:
        canonical = {"__frozen_breach_structure__": "MISSING"}
    payload = json.dumps(canonical, sort_keys=True, separators=(",", ":"), default=None)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    return f"sha256:{digest}"


# ---------------------------------------------------------------------------
# Envelope inspection helpers
# ---------------------------------------------------------------------------


def _get_first(mapping: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    """Return the first present, non-None value among ``keys`` in ``mapping``."""
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key]
    return None


def _envelope_status(envelope: Mapping[str, Any]) -> str:
    """Return the uppercased #625 envelope status string, if any."""
    raw = envelope.get("status")
    return str(raw).strip().upper() if raw is not None else ""


def _envelope_identity(envelope: Mapping[str, Any]) -> Optional[Mapping[str, Any]]:
    """Return the ``identity`` mapping from a #625 envelope, if valid."""
    identity = envelope.get("identity") or envelope.get("breach_identity")
    return identity if isinstance(identity, Mapping) else None


def _envelope_evidence_as_of(envelope: Mapping[str, Any]) -> Optional[str]:
    """Extract the canonical trigger-time as-of from the #625 envelope.

    Preference order matches the spec (§8):

    - ``evidence_as_of`` (the explicit #625 field);
    - ``as_of`` (the equivalent alias);
    - never ``collected_at``, worker start, worker completion, or
      snapshot insert time.
    """
    value = _get_first(envelope, ("evidence_as_of", "as_of"))
    return str(value) if isinstance(value, str) and value else None


def _envelope_structure(envelope: Mapping[str, Any]) -> Any:
    """Return the frozen #615 structure attached to the envelope, if any."""
    return _get_first(envelope, ("structure", "frozen_structure"))


def _authoritative_parent_ids(envelope: Mapping[str, Any]) -> dict[str, Optional[str]]:
    """Return the exact authoritative parent map from the envelope."""
    raw = envelope.get("authoritative_parent_snapshot_ids")
    if not isinstance(raw, Mapping):
        return {"PRETRIGGER": None, "PREOPEN": None}
    return {
        "PRETRIGGER": raw.get("PRETRIGGER"),
        "PREOPEN": raw.get("PREOPEN"),
    }


def _candidate_parent_ids(envelope: Mapping[str, Any]) -> dict[str, Optional[str]]:
    """Return the exact candidate parent map from the envelope."""
    raw = envelope.get("candidate_parent_snapshot_ids")
    if not isinstance(raw, Mapping):
        return {"PRETRIGGER": None, "PREOPEN": None}
    return {
        "PRETRIGGER": raw.get("PRETRIGGER"),
        "PREOPEN": raw.get("PREOPEN"),
    }


def _top_level_parent_snapshot_id(envelope: Mapping[str, Any]) -> Optional[str]:
    """Return the single top-level ``parent_snapshot_id`` value per spec §11.

    Prefer ONLY the exact authoritative PREOPEN parent when proven.  If no
    authoritative PREOPEN parent exists, return ``None``.  Never fall back to
    a candidate PREOPEN id.  Never invent PRETRIGGER as the direct BREACH
    parent merely because PREOPEN authority is missing — the full lineage is
    preserved inside the immutable payload metadata instead.
    """
    authoritative = _authoritative_parent_ids(envelope)
    preopen = authoritative.get("PREOPEN")
    return str(preopen) if isinstance(preopen, str) and preopen else None


# ---------------------------------------------------------------------------
# Enqueue-time validation
# ---------------------------------------------------------------------------


def _validate_envelope_for_enqueue(envelope: Any) -> tuple[bool, Optional[str], list[str]]:
    """Validate a candidate #625 envelope for #327 enqueue.

    Returns ``(ok, error_code, errors)``.  On success ``error_code`` is
    ``None``.  On failure ``ok`` is ``False`` and ``error_code`` is a stable
    machine-readable tag suitable for fail-soft telemetry.

    The rules (spec §5):

    - mapping input;
    - ``phase == "BREACH"``;
    - ``observe_only is True``;
    - ``affected_eligibility is False``;
    - valid #625 identity;
    - non-empty identity_hash;
    - non-empty input_hash;
    - exact canonical ``trigger_crossed_at``;
    - exact evidence_as_of;
    - attached structure for COMPLETE snapshots;
    - structure is a mapping when present;
    - REJECTED envelopes never enqueue.
    """
    errors: list[str] = []
    if not isinstance(envelope, Mapping):
        return False, "BREACH_ENVELOPE_NOT_MAPPING", ["envelope_not_mapping"]

    status = _envelope_status(envelope)
    if status in _REJECTED_ENVELOPE_STATUSES:
        return False, "BREACH_ENVELOPE_REJECTED", ["envelope_rejected"]
    if status not in {_STATUS_COMPLETE, _STATUS_PARTIAL}:
        errors.append("envelope_status_invalid")

    if envelope.get("observe_only") is not True:
        errors.append("envelope_observe_only_must_be_true")
    if envelope.get("affected_eligibility") is not False:
        errors.append("envelope_affected_eligibility_must_be_false")
    if str(envelope.get("phase") or "").upper() != BREACH_PHASE:
        errors.append("envelope_phase_not_breach")

    identity = _envelope_identity(envelope)
    if identity is None:
        errors.append("envelope_identity_missing")

    identity_hash = envelope.get("identity_hash")
    if not isinstance(identity_hash, str) or not identity_hash:
        errors.append("envelope_identity_hash_missing")

    input_hash = envelope.get("input_hash")
    if not isinstance(input_hash, str) or not input_hash:
        errors.append("envelope_input_hash_missing")

    trigger = envelope.get("trigger_crossed_at")
    if identity is not None:
        identity_trigger = identity.get("trigger_crossed_at")
        if trigger != identity_trigger or not isinstance(trigger, str) or not trigger:
            errors.append("envelope_trigger_crossed_at_missing_or_inconsistent")
    else:
        if not isinstance(trigger, str) or not trigger:
            errors.append("envelope_trigger_crossed_at_missing")

    evidence_as_of = _envelope_evidence_as_of(envelope)
    if evidence_as_of is None:
        errors.append("envelope_evidence_as_of_missing")

    structure = _envelope_structure(envelope)
    if structure is None:
        if status == _STATUS_COMPLETE:
            errors.append("envelope_structure_required_for_complete")
    elif not isinstance(structure, Mapping):
        errors.append("envelope_structure_invalid_type")

    if errors:
        return False, "BREACH_ENVELOPE_INVALID", errors

    return True, None, []


def _identity_key_from_envelope(envelope: Mapping[str, Any]) -> Mapping[str, str]:
    """Extract the identity fields required by #327 enqueue.

    Falls back through envelope top-level -> ``identity`` mapping so we support
    both places without ever mixing them.  Every value is coerced to string and
    normalized where #327 normalizes.
    """
    identity = _envelope_identity(envelope) or {}
    return {
        "client_id": str(
            envelope.get("client_id") or identity.get("client_id") or ""
        ),
        "execution_mode": normalize_execution_mode(
            envelope.get("execution_mode") or identity.get("execution_mode")
        ),
        "signal_id": str(
            envelope.get("signal_id") or identity.get("signal_id") or ""
        ),
        "canonical_signal_id": str(
            envelope.get("canonical_signal_id")
            or identity.get("canonical_signal_id")
            or ""
        ),
        "local_order_id": normalized_local_order_id(
            envelope.get("local_order_id") or identity.get("local_order_id") or ""
        ),
        "profile_version": str(
            envelope.get("profile_version")
            or identity.get("profile_version")
            or DEFAULT_PROFILE_VERSION
        ),
    }


# ---------------------------------------------------------------------------
# Frozen payload construction
# ---------------------------------------------------------------------------


def _build_frozen_payload(envelope: Mapping[str, Any]) -> dict[str, Any]:
    """Build the immutable frozen-BREACH job payload from a #625 envelope.

    Every field required by spec §7 is included.  The payload contains no
    mutable latest-market aliases and no worker-time fallbacks.  The frozen
    structure is deep-copied so later mutation of caller data cannot rewrite
    what #327 persists.
    """
    identity = _envelope_identity(envelope) or {}
    structure = _envelope_structure(envelope)
    structure_copy = copy.deepcopy(structure) if structure is not None else None
    evidence = envelope.get("canonical_evidence") or envelope.get("evidence") or {}
    evidence_copy = copy.deepcopy(evidence) if isinstance(evidence, Mapping) else {}

    parent_lineage = envelope.get("parent_lineage")
    if not isinstance(parent_lineage, Mapping):
        parent_lineage = {}

    source_versions = envelope.get("source_versions")
    if not isinstance(source_versions, Mapping):
        source_versions = {}

    structure_mapping = structure_copy if isinstance(structure_copy, Mapping) else {}
    structure_schema_version = str(
        source_versions.get("structure_schema_version")
        or structure_mapping.get("schema_version")
        or ""
    )
    structure_model_version = str(
        source_versions.get("structure_model_version")
        or structure_mapping.get("model_version")
        or ""
    )

    return {
        # Marker and versioning — must be checked by the dispatch seam.
        "payload_kind": FROZEN_BREACH_PAYLOAD_KIND,
        "adapter_version": ADAPTER_VERSION,
        "assembly_version": envelope.get("assembly_version") or _BREACH_ASSEMBLY_VERSION,
        # Complete #625 canonical identity (immutable).
        "identity": copy.deepcopy(dict(identity)),
        "identity_hash": envelope.get("identity_hash"),
        "input_hash": envelope.get("input_hash"),
        # Timing authority.
        "trigger_crossed_at": envelope.get("trigger_crossed_at"),
        "evidence_as_of": _envelope_evidence_as_of(envelope),
        # Parents — candidate and authoritative both persisted per spec §11.
        "candidate_parent_snapshot_ids": _candidate_parent_ids(envelope),
        "authoritative_parent_snapshot_ids": _authoritative_parent_ids(envelope),
        "parent_lineage": copy.deepcopy(dict(parent_lineage)),
        "parent_validation": copy.deepcopy(envelope.get("parent_validation") or {}),
        "missing_parent_phases": list(envelope.get("missing_parent_phases") or []),
        # Evidence (frozen #614/#625 canonical projection).
        "canonical_evidence": evidence_copy,
        # Frozen #615 structure.
        "structure": structure_copy,
        "structure_hash": hash_frozen_breach_structure(structure_copy),
        "structure_schema_version": structure_schema_version,
        "structure_model_version": structure_model_version,
        "source_versions": copy.deepcopy(dict(source_versions)),
        # Safety flags.
        "observe_only": True,
        "affected_eligibility": False,
        # Explicit envelope status carried forward for the worker mapping.
        "envelope_status": _envelope_status(envelope),
    }


# ---------------------------------------------------------------------------
# Public dispatch predicate
# ---------------------------------------------------------------------------


def is_frozen_breach_job(job: Mapping[str, Any]) -> bool:
    """Return True iff ``job`` is a frozen-BREACH job this adapter owns.

    Used by :mod:`ap.intelligence_context_materializer.build_snapshot_kwargs`
    to decide whether to delegate to :func:`build_breach_snapshot_kwargs` or
    fall through to the generic PRETRIGGER/PREOPEN/BREACH path.

    A job qualifies only when BOTH conditions hold:

    - ``phase == "BREACH"``;
    - job payload carries ``payload_kind == FROZEN_BREACH_PAYLOAD_KIND``.

    Any other BREACH job (including generic ones from older callers) leaves
    the existing behavior untouched.
    """
    if not isinstance(job, Mapping):
        return False
    if str(job.get("phase") or "").upper() != BREACH_PHASE:
        return False
    payload = job.get("payload")
    if not isinstance(payload, Mapping):
        return False
    return str(payload.get("payload_kind") or "") == FROZEN_BREACH_PAYLOAD_KIND


# ---------------------------------------------------------------------------
# Enqueue seam
# ---------------------------------------------------------------------------


def enqueue_breach_snapshot_job(envelope: Any) -> dict[str, Any]:
    """Enqueue a durable BREACH snapshot job from a #625 envelope.

    Returns a fail-soft telemetry dict.  On any validation failure the
    return value has ``ok=False`` and ``enqueued=False`` with a stable
    ``error_code``; the intelligence subsystem may inspect it but MUST NOT
    let it terminalize a trade, watcher, or setup.

    A ``REJECTED`` envelope returns ``error_code="BREACH_ENVELOPE_REJECTED"``.

    On success the return value carries the #327 job id, the #327-allocated
    ``context_revision``, and a duplicate flag when idempotency short-circuits
    to an existing job with the same ``input_hash``.

    The immutable frozen payload built here is stored inside the #327 job row;
    the worker later hands the job back through
    :func:`build_breach_snapshot_kwargs` for persistence.
    """
    ok, error_code, errors = _validate_envelope_for_enqueue(envelope)
    if not ok:
        log.warning(
            "BREACH_ENQUEUE_REJECTED error_code=%s errors=%s",
            error_code,
            errors,
        )
        return _telemetry(
            False,
            enqueued=False,
            error_code=error_code,
            errors=errors,
        )

    assert isinstance(envelope, Mapping)  # narrows type for mypy / readers

    try:
        identity_fields = _identity_key_from_envelope(envelope)
        input_hash = str(envelope["input_hash"])
        payload = _build_frozen_payload(envelope)
    except Exception as exc:  # defensive; must never propagate
        log.exception("BREACH_ENQUEUE_PAYLOAD_BUILD_FAILED")
        return _telemetry(
            False,
            enqueued=False,
            error_code="BREACH_ENQUEUE_PAYLOAD_BUILD_FAILED",
            error=str(exc),
        )

    try:
        result = enqueue_intelligence_job(
            client_id=identity_fields["client_id"],
            execution_mode=identity_fields["execution_mode"],
            canonical_signal_id=identity_fields["canonical_signal_id"],
            signal_id=identity_fields["signal_id"],
            local_order_id=identity_fields["local_order_id"],
            phase=BREACH_PHASE,
            profile_version=identity_fields["profile_version"],
            input_hash=input_hash,
            payload=payload,
        )
    except Exception as exc:  # defensive; never propagate into trading
        log.exception("BREACH_ENQUEUE_STORE_FAILED")
        return _telemetry(
            False,
            enqueued=False,
            error_code="BREACH_ENQUEUE_STORE_FAILED",
            error=str(exc),
        )

    if not result.get("ok"):
        return _telemetry(
            False,
            enqueued=False,
            error_code="BREACH_ENQUEUE_STORE_ERROR",
            store_result=result,
        )

    inserted = bool(result.get("inserted"))
    duplicate = bool(result.get("duplicate"))
    return _telemetry(
        True,
        enqueued=inserted,
        duplicate=duplicate,
        duplicate_same_input=bool(result.get("duplicate_same_input")),
        job_id=result.get("job_id"),
        context_revision=result.get("context_revision"),
        input_hash=input_hash,
        identity_hash=envelope.get("identity_hash"),
        envelope_status=_envelope_status(envelope),
    )


# ---------------------------------------------------------------------------
# Worker dispatch (build_snapshot_kwargs delegate)
# ---------------------------------------------------------------------------


def _job_vs_envelope_identity_ok(
    job: Mapping[str, Any], envelope: Mapping[str, Any]
) -> tuple[bool, list[str]]:
    """Cross-check a claimed job against its immutable frozen envelope.

    Spec §12 requires equality on: client_id, execution_mode, signal_id,
    canonical_signal_id, local_order_id, phase, profile_version, and
    input_hash.  On mismatch the caller MUST fail closed for intelligence
    persistence (do not write the snapshot).
    """
    errors: list[str] = []

    envelope_identity = _envelope_identity(envelope) or {}
    envelope_fields = {
        "client_id": str(
            envelope.get("client_id") or envelope_identity.get("client_id") or ""
        ),
        "execution_mode": normalize_execution_mode(
            envelope.get("execution_mode") or envelope_identity.get("execution_mode")
        ),
        "signal_id": str(
            envelope.get("signal_id") or envelope_identity.get("signal_id") or ""
        ),
        "canonical_signal_id": str(
            envelope.get("canonical_signal_id")
            or envelope_identity.get("canonical_signal_id")
            or ""
        ),
        "local_order_id": normalized_local_order_id(
            envelope.get("local_order_id")
            or envelope_identity.get("local_order_id")
            or ""
        ),
        "profile_version": str(
            envelope.get("profile_version")
            or envelope_identity.get("profile_version")
            or DEFAULT_PROFILE_VERSION
        ),
    }
    job_fields = {
        "client_id": str(job.get("client_id") or ""),
        "execution_mode": normalize_execution_mode(job.get("execution_mode")),
        "signal_id": str(job.get("signal_id") or ""),
        "canonical_signal_id": str(job.get("canonical_signal_id") or ""),
        "local_order_id": normalized_local_order_id(job.get("local_order_id") or ""),
        "profile_version": str(job.get("profile_version") or DEFAULT_PROFILE_VERSION),
    }
    for key, envelope_value in envelope_fields.items():
        if job_fields[key] != envelope_value:
            errors.append(f"job_identity_mismatch:{key}")

    if normalize_phase(job.get("phase") or "") != BREACH_PHASE:
        errors.append("job_phase_not_breach")

    envelope_input_hash = envelope.get("input_hash")
    if str(job.get("input_hash") or "") != str(envelope_input_hash or ""):
        errors.append("job_input_hash_mismatch")

    return (not errors), errors


def build_breach_snapshot_kwargs(job: Mapping[str, Any]) -> dict[str, Any]:
    """Return ``write_snapshot(**kwargs)`` for a frozen-BREACH job.

    Called by :mod:`ap.intelligence_context_materializer.build_snapshot_kwargs`
    when its dispatch guard fires (``is_frozen_breach_job(job) is True``).

    The worker path performs ZERO market-data reads.  Every value below comes
    exclusively from the immutable frozen payload persisted at enqueue time.

    On identity mismatch this raises a :class:`RuntimeError` so the existing
    #327 worker retry/terminal machinery marks the intelligence job (not the
    trade) failed.  Trading is unaffected — see spec §14.
    """
    if not is_frozen_breach_job(job):
        raise RuntimeError(
            "build_breach_snapshot_kwargs invoked on non-frozen-BREACH job; "
            "dispatch seam must gate on is_frozen_breach_job(job)."
        )

    payload = job["payload"]  # type: ignore[index]  # is_frozen_breach_job proves shape
    assert isinstance(payload, Mapping)

    ok, errors = _job_vs_envelope_identity_ok(job, payload)
    if not ok:
        # Fail closed for intelligence persistence only.  The worker will
        # translate this via mark_job_retry / mark_job_terminal — no trading
        # path is aware of this branch.
        raise RuntimeError(
            "BREACH_JOB_IDENTITY_MISMATCH errors=" + ",".join(errors)
        )

    envelope_status = str(payload.get("envelope_status") or "").upper()
    if envelope_status == _STATUS_COMPLETE:
        snapshot_status = "COMPLETE"
    elif envelope_status == _STATUS_PARTIAL:
        snapshot_status = "PARTIAL"
    else:
        # A payload should never reach the worker with any other status —
        # enqueue rejects REJECTED envelopes, and the store never fabricates
        # a new one — but if it does, refuse rather than invent a status.
        raise RuntimeError(
            f"BREACH_JOB_STATUS_INVALID:{envelope_status or 'MISSING'}"
        )
    assert snapshot_status in SNAPSHOT_STATUSES  # invariant check

    data_as_of = payload.get("evidence_as_of")
    if not isinstance(data_as_of, str) or not data_as_of:
        raise RuntimeError("BREACH_JOB_DATA_AS_OF_MISSING")

    # Top-level parent per spec §11 — authoritative PREOPEN only, else None.
    top_parent = _top_level_parent_snapshot_id(payload)

    # Composite payload persisted into ap_intelligence_snapshots.payload.
    # This is the replayable artifact required by spec §13.
    persisted_payload: dict[str, Any] = {
        "payload_kind": FROZEN_BREACH_PAYLOAD_KIND,
        "adapter_version": ADAPTER_VERSION,
        "assembly_version": payload.get("assembly_version") or _BREACH_ASSEMBLY_VERSION,
        # IDENTITY
        "identity": copy.deepcopy(payload.get("identity") or {}),
        "identity_hash": payload.get("identity_hash"),
        "input_hash": payload.get("input_hash"),
        "context_revision": int(job.get("context_revision") or 1),
        # TIMING
        "trigger_crossed_at": payload.get("trigger_crossed_at"),
        "evidence_as_of": data_as_of,
        # PARENTS
        "candidate_parent_snapshot_ids": copy.deepcopy(
            payload.get("candidate_parent_snapshot_ids") or {}
        ),
        "authoritative_parent_snapshot_ids": copy.deepcopy(
            payload.get("authoritative_parent_snapshot_ids") or {}
        ),
        "parent_lineage": copy.deepcopy(payload.get("parent_lineage") or {}),
        "parent_validation": copy.deepcopy(payload.get("parent_validation") or {}),
        "missing_parent_phases": list(payload.get("missing_parent_phases") or []),
        # EVIDENCE
        "canonical_evidence": copy.deepcopy(payload.get("canonical_evidence") or {}),
        # STRUCTURE
        "structure": copy.deepcopy(payload.get("structure")),
        "structure_hash": payload.get("structure_hash"),
        "structure_schema_version": payload.get("structure_schema_version"),
        "structure_model_version": payload.get("structure_model_version"),
        "source_versions": copy.deepcopy(payload.get("source_versions") or {}),
        # SAFETY
        "observe_only": True,
        "affected_eligibility": False,
        # STATUS
        "envelope_status": envelope_status,
        "snapshot_status": snapshot_status,
    }

    return {
        "client_id": str(job.get("client_id") or ""),
        "execution_mode": normalize_execution_mode(job.get("execution_mode")),
        "canonical_signal_id": str(job.get("canonical_signal_id") or ""),
        "signal_id": str(job.get("signal_id") or ""),
        "local_order_id": normalized_local_order_id(job.get("local_order_id") or ""),
        "phase": BREACH_PHASE,
        "context_revision": int(job.get("context_revision") or 1),
        "profile_version": str(job.get("profile_version") or DEFAULT_PROFILE_VERSION),
        "parent_snapshot_id": top_parent,
        "input_hash": str(job.get("input_hash") or payload.get("input_hash") or ""),
        # config_hash and git_commit are deliberately blank here — the frozen
        # payload already carries adapter/assembly versions and hashes, and
        # these two #327 columns are populated by the generic PRETRIGGER /
        # PREOPEN materializer path from its own config identity.
        "config_hash": "",
        "git_commit": "",
        "data_as_of": data_as_of,
        "status": snapshot_status,
        "payload": persisted_payload,
    }


__all__ = [
    "ADAPTER_VERSION",
    "FROZEN_BREACH_PAYLOAD_KIND",
    "build_breach_snapshot_kwargs",
    "enqueue_breach_snapshot_job",
    "hash_frozen_breach_structure",
    "is_frozen_breach_job",
]
