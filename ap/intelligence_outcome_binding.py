"""Exact, read-only intelligence-to-proof outcome binding.

This module is the evidence-plane authority for PR #432.  It reads immutable
intelligence snapshots and terminal ``proof_trades`` rows, then records only
the identity binding in ``ap_intelligence_outcome_bindings``.  It never
rewrites either source, derives P&L, calls a broker, or changes trade state.

The direct join is deliberately narrow: normalized client identity, exact
execution mode, and the originating ENTRY ``local_order_id``.  Signal IDs,
contracts, tickers, and timestamps are supporting evidence only and are never
used as fallbacks.
"""
from __future__ import annotations

import logging
import os
from collections import Counter
from typing import Any, Iterable, Mapping

log = logging.getLogger("ap.intelligence_outcome_binding")

VALID_EXECUTION_MODES = frozenset({"live", "paper"})
VALID_PHASES = frozenset({"PRETRIGGER", "PREOPEN", "BREACH", "CONTRACT_SELECTED"})
BINDING_VERSION = "p0-432-v1"
DIRECT_BINDING_METHOD = "EXACT_CLIENT_MODE_LOCAL_ORDER_ID"
LINEAGE_BINDING_METHOD = "EXACT_PARENT_SNAPSHOT_LINEAGE"


def _db_conn():
    from ap.db import conn

    return conn


def _run_with_retry(fn):
    from ap.db import run_with_retry

    return run_with_retry(fn)


def normalize_client_id(value: Any) -> str:
    """Normalize only transport noise; email identifiers are case-folded."""
    raw = str(value or "").strip()
    return raw.casefold() if "@" in raw else raw


def normalize_execution_mode(value: Any) -> str:
    raw = str(value or "").strip().casefold()
    return raw if raw in VALID_EXECUTION_MODES else ""


def _text(value: Any) -> str:
    return str(value or "").strip()


def _is_true(value: Any) -> bool:
    if value is True:
        return True
    return isinstance(value, str) and value.strip().casefold() in {"1", "true", "yes", "on"}


def _is_false_or_missing(value: Any) -> bool:
    if value is None or value is False:
        return True
    if isinstance(value, str):
        return value.strip().casefold() in {"", "0", "false", "no", "off"}
    return False


def _row_dict(row: Any) -> dict[str, Any]:
    if isinstance(row, Mapping):
        return dict(row)
    try:
        return dict(row)
    except Exception:
        return {}


def _result(
    *,
    snapshot_id: str,
    disposition: str,
    ok: bool = True,
    **fields: Any,
) -> dict[str, Any]:
    return {
        "ok": bool(ok),
        "bound": disposition in {"BOUND", "ALREADY_BOUND"},
        "snapshot_id": snapshot_id,
        "disposition": disposition,
        **fields,
    }


def _capture_enabled() -> bool:
    return os.getenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", "0").strip().casefold() in {
        "1", "true", "yes", "on",
    }


def _snapshot_identity(
    snapshot: Mapping[str, Any],
    *,
    expected_client_id: str | None = None,
    expected_execution_mode: str | None = None,
) -> dict[str, Any]:
    snapshot_id = _text(snapshot.get("id"))
    client_id = normalize_client_id(snapshot.get("client_id"))
    mode = normalize_execution_mode(snapshot.get("execution_mode"))
    canonical_signal_id = _text(snapshot.get("canonical_signal_id"))
    phase = _text(snapshot.get("phase")).upper()
    profile_version = _text(snapshot.get("profile_version"))
    input_hash = _text(snapshot.get("input_hash"))
    config_hash = _text(snapshot.get("config_hash"))

    if expected_client_id is not None and client_id != normalize_client_id(expected_client_id):
        return {
            "ok": False,
            "disposition": "CLIENT_CONFLICT",
            "reason": "snapshot_client_does_not_match_requested_client",
        }
    expected_mode = normalize_execution_mode(expected_execution_mode) if expected_execution_mode else ""
    if expected_mode and mode != expected_mode:
        return {
            "ok": False,
            "disposition": "SNAPSHOT_IDENTITY_UNPROVEN",
            "reason": "snapshot_execution_mode_unproven_or_mismatched",
        }
    if not snapshot_id or not client_id or not mode or not canonical_signal_id:
        return {
            "ok": False,
            "disposition": "SNAPSHOT_IDENTITY_UNPROVEN",
            "reason": "snapshot_core_identity_missing_or_invalid",
        }
    if phase not in VALID_PHASES or not profile_version or not input_hash or not config_hash:
        return {
            "ok": False,
            "disposition": "SNAPSHOT_IDENTITY_UNPROVEN",
            "reason": "snapshot_binding_fields_missing_or_invalid",
        }
    return {
        "ok": True,
        "snapshot_id": snapshot_id,
        "client_id": client_id,
        "execution_mode": mode,
        "canonical_signal_id": canonical_signal_id,
        "phase": phase,
        "profile_version": profile_version,
        "input_hash": input_hash,
        "config_hash": config_hash,
        "local_order_id": _text(snapshot.get("local_order_id")),
    }


def _proof_client_state(row: Mapping[str, Any], expected_client_id: str) -> str:
    identities = []
    for key in ("client_email", "client_id"):
        value = normalize_client_id(row.get(key))
        if value:
            identities.append(value)
    if not identities:
        return "CONFLICT"
    if any(value != expected_client_id for value in identities):
        return "CONFLICT"
    return "MATCH"


def _classify_proof_row(
    row: Mapping[str, Any],
    *,
    client_id: str,
    execution_mode: str,
    local_order_id: str,
) -> dict[str, Any]:
    if _proof_client_state(row, client_id) != "MATCH":
        return {"eligible": False, "disposition": "CLIENT_CONFLICT", "reason": "proof_client_mismatch"}
    proof_local_order_id = _text(row.get("local_order_id"))
    if not proof_local_order_id:
        return {"eligible": False, "disposition": "LOCAL_ORDER_ID_MISSING", "reason": "proof_local_order_id_missing"}
    if proof_local_order_id != local_order_id:
        return {"eligible": False, "disposition": "PROOF_NOT_FOUND", "reason": "proof_local_order_id_mismatch"}

    proof_execution_mode = normalize_execution_mode(row.get("execution_mode"))
    proof_secondary_mode = normalize_execution_mode(row.get("mode"))
    if not proof_execution_mode or not proof_secondary_mode:
        return {
            "eligible": False,
            "disposition": "PROOF_TAXONOMY_INELIGIBLE",
            "reason": "proof_execution_mode_or_mode_unproven",
        }
    if proof_execution_mode != proof_secondary_mode or proof_execution_mode != execution_mode:
        return {
            "eligible": False,
            "disposition": "MODE_CONFLICT",
            "reason": "proof_execution_mode_and_mode_disagree_or_mismatch_snapshot",
        }

    taxonomy = _text(row.get("performance_taxonomy")).upper()
    if execution_mode == "live":
        eligible = (
            taxonomy == "LIVE_OFFICIAL"
            and _is_true(row.get("training_eligible"))
            and _is_true(row.get("official_live_performance_eligible"))
        )
        if not eligible:
            return {
                "eligible": False,
                "disposition": "PROOF_TAXONOMY_INELIGIBLE",
                "reason": "proof_is_not_exact_live_official_training_truth",
            }
    else:
        eligible = (
            taxonomy == "PAPER_UNVERIFIED"
            and _is_false_or_missing(row.get("training_eligible"))
            and _is_false_or_missing(row.get("official_live_performance_eligible"))
        )
        if not eligible:
            return {
                "eligible": False,
                "disposition": "PROOF_TAXONOMY_INELIGIBLE",
                "reason": "proof_is_not_exact_paper_research_truth",
            }

    return {
        "eligible": True,
        "disposition": "BOUND",
        "reason": "exact_proof_identity_and_taxonomy_verified",
        "proof_trade_id": row.get("id"),
        "proof_row": dict(row),
    }


def _resolve_direct_proof(
    snapshot_identity: Mapping[str, Any],
    proof_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    snapshot_id = _text(snapshot_identity.get("snapshot_id"))
    local_order_id = _text(snapshot_identity.get("local_order_id"))
    if not local_order_id:
        return _result(
            snapshot_id=snapshot_id,
            disposition="LOCAL_ORDER_ID_MISSING",
            reason="direct_binding_requires_originating_entry_local_order_id",
        )

    client_id = normalize_client_id(snapshot_identity.get("client_id"))
    execution_mode = normalize_execution_mode(snapshot_identity.get("execution_mode"))
    rows = [_row_dict(row) for row in proof_rows]
    if not rows:
        return _result(snapshot_id=snapshot_id, disposition="PROOF_NOT_FOUND", reason="no_proof_row_for_local_order_id")

    # Every row returned for the exact local order is part of the candidate
    # set.  A conflicting secondary identity cannot be discarded merely
    # because another row looks valid: that would turn malformed/ambiguous
    # proof truth into an official binding.
    conflicting_rows = [row for row in rows if _proof_client_state(row, client_id) != "MATCH"]
    if conflicting_rows:
        return _result(
            snapshot_id=snapshot_id,
            disposition="CLIENT_CONFLICT",
            reason="proof_client_identity_conflict_among_local_order_candidates",
            candidate_count=len(rows),
        )

    matching_rows = rows

    classifications = [
        _classify_proof_row(
            row,
            client_id=client_id,
            execution_mode=execution_mode,
            local_order_id=local_order_id,
        )
        for row in matching_rows
    ]
    mode_conflicts = [item for item in classifications if item.get("disposition") == "MODE_CONFLICT"]
    if mode_conflicts:
        return _result(snapshot_id=snapshot_id, disposition="MODE_CONFLICT", reason=mode_conflicts[0].get("reason"))
    if len(matching_rows) != 1:
        return _result(
            snapshot_id=snapshot_id,
            disposition="PROOF_AMBIGUOUS",
            reason="more_than_one_proof_row_has_exact_client_and_local_order_identity",
            candidate_count=len(matching_rows),
        )

    classification = classifications[0]
    if not classification.get("eligible"):
        return _result(
            snapshot_id=snapshot_id,
            disposition=str(classification.get("disposition") or "PROOF_TAXONOMY_INELIGIBLE"),
            reason=classification.get("reason"),
        )
    return _result(
        snapshot_id=snapshot_id,
        disposition="BOUND",
        proof_trade_id=classification.get("proof_trade_id"),
        proof_row=classification.get("proof_row") or {},
        research_only=execution_mode == "paper",
        training_eligible=execution_mode == "live",
    )


def _fetch_snapshot(c: Any, snapshot_id: str) -> dict[str, Any] | None:
    c.execute("SELECT * FROM ap_intelligence_snapshots WHERE id=%s", (snapshot_id,))
    row = c.fetchone()
    return _row_dict(row) if row else None


def _fetch_existing_binding(c: Any, snapshot_id: str) -> dict[str, Any] | None:
    c.execute(
        "SELECT * FROM ap_intelligence_outcome_bindings WHERE snapshot_id=%s LIMIT 1",
        (snapshot_id,),
    )
    row = c.fetchone()
    return _row_dict(row) if row else None


def _fetch_proofs_for_local_order(c: Any, local_order_id: str) -> list[dict[str, Any]]:
    c.execute(
        """
        SELECT *
        FROM proof_trades
        WHERE NULLIF(BTRIM(local_order_id), '')=%s
        """,
        (local_order_id,),
    )
    return [_row_dict(row) for row in (c.fetchall() or [])]


def _fetch_descendants(c: Any, snapshot_id: str) -> list[dict[str, Any]]:
    c.execute(
        """
        WITH RECURSIVE snapshot_tree AS (
            SELECT s.*, ARRAY[s.id] AS lineage_ids
            FROM ap_intelligence_snapshots s
            WHERE s.id=%s
            UNION ALL
            SELECT child.*, array_append(tree.lineage_ids, child.id)
            FROM ap_intelligence_snapshots child
            JOIN snapshot_tree tree
              ON child.parent_snapshot_id=tree.id
             AND child.client_id=tree.client_id
             AND lower(BTRIM(child.execution_mode))=lower(BTRIM(tree.execution_mode))
             AND child.canonical_signal_id=tree.canonical_signal_id
            WHERE child.id <> ALL(tree.lineage_ids)
        )
        SELECT * FROM snapshot_tree WHERE id<>%s
        """,
        (snapshot_id, snapshot_id),
    )
    return [_row_dict(row) for row in (c.fetchall() or [])]


def _proof_trade_id(value: Any) -> str:
    return _text(value)


def _already_bound(snapshot_id: str, binding: Mapping[str, Any]) -> dict[str, Any]:
    return _result(
        snapshot_id=snapshot_id,
        disposition="ALREADY_BOUND",
        binding_id=binding.get("id"),
        proof_trade_id=binding.get("proof_trade_id"),
        binding_method=binding.get("binding_method"),
        research_only=normalize_execution_mode(binding.get("execution_mode")) == "paper",
        training_eligible=normalize_execution_mode(binding.get("execution_mode")) == "live",
    )


def _binding_identity_matches_snapshot(
    binding: Mapping[str, Any],
    snapshot_identity: Mapping[str, Any],
) -> bool:
    expected_method = (
        DIRECT_BINDING_METHOD
        if snapshot_identity.get("local_order_id")
        else LINEAGE_BINDING_METHOD
    )
    if _text(binding.get("snapshot_id")) != _text(snapshot_identity.get("snapshot_id")):
        return False
    if normalize_client_id(binding.get("client_id")) != snapshot_identity.get("client_id"):
        return False
    if normalize_execution_mode(binding.get("execution_mode")) != snapshot_identity.get("execution_mode"):
        return False
    if _text(binding.get("canonical_signal_id")) != snapshot_identity.get("canonical_signal_id"):
        return False
    if _text(binding.get("phase")).upper() != snapshot_identity.get("phase"):
        return False
    if _text(binding.get("profile_version")) != snapshot_identity.get("profile_version"):
        return False
    if _text(binding.get("input_hash")) != snapshot_identity.get("input_hash"):
        return False
    if _text(binding.get("config_hash")) != snapshot_identity.get("config_hash"):
        return False
    if _text(binding.get("binding_method")) != expected_method:
        return False
    if _text(binding.get("binding_version")) != BINDING_VERSION:
        return False
    originating_local_order_id = _text(binding.get("originating_local_order_id"))
    if not originating_local_order_id:
        return False
    if (
        snapshot_identity.get("local_order_id")
        and originating_local_order_id != snapshot_identity.get("local_order_id")
    ):
        return False
    return True


def _binding_matches_attempt(
    binding: Mapping[str, Any],
    *,
    snapshot_identity: Mapping[str, Any],
    proof_row: Mapping[str, Any],
    binding_method: str,
) -> bool:
    """Compare every immutable field in a conflicting insert attempt."""
    return (
        _text(binding.get("snapshot_id")) == _text(snapshot_identity.get("snapshot_id"))
        and _proof_trade_id(binding.get("proof_trade_id")) == _proof_trade_id(proof_row.get("id"))
        and normalize_client_id(binding.get("client_id")) == snapshot_identity.get("client_id")
        and normalize_execution_mode(binding.get("execution_mode")) == snapshot_identity.get("execution_mode")
        and _text(binding.get("originating_local_order_id")) == _text(proof_row.get("local_order_id"))
        and _text(binding.get("canonical_signal_id")) == _text(snapshot_identity.get("canonical_signal_id"))
        and _text(binding.get("phase")).upper() == _text(snapshot_identity.get("phase")).upper()
        and _text(binding.get("profile_version")) == _text(snapshot_identity.get("profile_version"))
        and _text(binding.get("input_hash")) == _text(snapshot_identity.get("input_hash"))
        and _text(binding.get("config_hash")) == _text(snapshot_identity.get("config_hash"))
        and _text(binding.get("binding_method")) == _text(binding_method)
        and _text(binding.get("binding_version")) == BINDING_VERSION
    )


def _validate_existing_binding(
    c: Any,
    *,
    binding: Mapping[str, Any],
    snapshot_identity: Mapping[str, Any],
) -> dict[str, Any]:
    """Re-prove an existing row before treating it as durable authority."""
    snapshot_id = _text(snapshot_identity.get("snapshot_id"))
    if not _binding_identity_matches_snapshot(binding, snapshot_identity):
        return _result(
            snapshot_id=snapshot_id,
            disposition="SNAPSHOT_IDENTITY_UNPROVEN",
            ok=False,
            reason="existing_binding_identity_does_not_match_snapshot",
        )

    proof_trade_id = _proof_trade_id(binding.get("proof_trade_id"))
    if not proof_trade_id:
        return _result(
            snapshot_id=snapshot_id,
            disposition="SNAPSHOT_IDENTITY_UNPROVEN",
            ok=False,
            reason="existing_binding_proof_identity_missing",
        )

    c.execute("SELECT * FROM proof_trades WHERE id=%s", (binding.get("proof_trade_id"),))
    proof = c.fetchone()
    if not proof:
        return _result(
            snapshot_id=snapshot_id,
            disposition="PROOF_NOT_FOUND",
            ok=False,
            reason="existing_binding_proof_row_missing",
        )
    proof_row = _row_dict(proof)
    if _proof_trade_id(proof_row.get("id")) != proof_trade_id:
        return _result(
            snapshot_id=snapshot_id,
            disposition="SNAPSHOT_IDENTITY_UNPROVEN",
            ok=False,
            reason="existing_binding_proof_id_does_not_match_selected_row",
        )

    proof_local_order_id = _text(proof_row.get("local_order_id"))
    if not proof_local_order_id:
        return _result(
            snapshot_id=snapshot_id,
            disposition="LOCAL_ORDER_ID_MISSING",
            ok=False,
            reason="existing_binding_proof_local_order_id_missing",
        )
    if _text(binding.get("originating_local_order_id")) != proof_local_order_id:
        return _result(
            snapshot_id=snapshot_id,
            disposition="SNAPSHOT_IDENTITY_UNPROVEN",
            ok=False,
            reason="existing_binding_local_order_id_does_not_match_proof",
        )

    if snapshot_identity.get("local_order_id"):
        current_proof_set = _fetch_proofs_for_local_order(
            c,
            snapshot_identity["local_order_id"],
        )
        direct = _resolve_direct_proof(snapshot_identity, current_proof_set)
        if direct.get("disposition") != "BOUND":
            return {**direct, "ok": False, "bound": False}
        current_proof_id = _proof_trade_id(direct.get("proof_trade_id"))
        if current_proof_id != proof_trade_id:
            return _result(
                snapshot_id=snapshot_id,
                disposition="BINDING_CONFLICT",
                ok=False,
                reason="existing_binding_proof_does_not_match_current_direct_resolution",
                existing_proof_trade_id=proof_trade_id,
                current_proof_trade_id=current_proof_id,
            )
        return _result(
            snapshot_id=snapshot_id,
            disposition="BOUND",
            proof_trade_id=proof_trade_id,
            proof_row=direct.get("proof_row") or proof_row,
        )

    classification = _classify_proof_row(
        proof_row,
        client_id=snapshot_identity["client_id"],
        execution_mode=snapshot_identity["execution_mode"],
        # PRETRIGGER lineage has no local order of its own; validate the
        # proof's nonblank child order while still enforcing client/mode/
        # taxonomy truth.
        local_order_id=snapshot_identity["local_order_id"] or proof_local_order_id,
    )
    if not classification.get("eligible"):
        return _result(
            snapshot_id=snapshot_id,
            disposition=str(classification.get("disposition") or "SNAPSHOT_IDENTITY_UNPROVEN"),
            ok=False,
            reason=classification.get("reason"),
        )
    return _result(
        snapshot_id=snapshot_id,
        disposition="BOUND",
        proof_trade_id=proof_trade_id,
        proof_row=classification.get("proof_row") or proof_row,
    )


def _insert_binding(
    c: Any,
    *,
    snapshot_identity: Mapping[str, Any],
    proof_row: Mapping[str, Any],
    binding_method: str,
) -> dict[str, Any]:
    snapshot_id = _text(snapshot_identity.get("snapshot_id"))
    proof_id = proof_row.get("id")
    originating_local_order_id = _text(proof_row.get("local_order_id"))
    if not _proof_trade_id(proof_id) or not originating_local_order_id:
        return _result(
            snapshot_id=snapshot_id,
            disposition="PERSIST_FAILED",
            ok=False,
            reason="binding_requires_proof_id_and_originating_local_order_id",
        )
    c.execute(
        """
        INSERT INTO ap_intelligence_outcome_bindings (
            snapshot_id,
            proof_trade_id,
            client_id,
            execution_mode,
            originating_local_order_id,
            canonical_signal_id,
            phase,
            profile_version,
            input_hash,
            config_hash,
            binding_method,
            binding_version
        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (snapshot_id) DO NOTHING
        RETURNING id
        """,
        (
            snapshot_id,
            proof_id,
            snapshot_identity["client_id"],
            snapshot_identity["execution_mode"],
            originating_local_order_id,
            snapshot_identity["canonical_signal_id"],
            snapshot_identity["phase"],
            snapshot_identity["profile_version"],
            snapshot_identity["input_hash"],
            snapshot_identity["config_hash"],
            binding_method,
            BINDING_VERSION,
        ),
    )
    inserted = c.fetchone()
    if inserted:
        inserted_row = _row_dict(inserted)
        return _result(
            snapshot_id=snapshot_id,
            disposition="BOUND",
            binding_id=inserted_row.get("id"),
            proof_trade_id=proof_id,
            binding_method=binding_method,
            research_only=snapshot_identity["execution_mode"] == "paper",
            training_eligible=snapshot_identity["execution_mode"] == "live",
        )

    existing = _fetch_existing_binding(c, snapshot_id)
    if existing:
        if not _binding_matches_attempt(
            existing,
            snapshot_identity=snapshot_identity,
            proof_row=proof_row,
            binding_method=binding_method,
        ):
            return _result(
                snapshot_id=snapshot_id,
                disposition="BINDING_CONFLICT",
                ok=False,
                reason="existing_binding_does_not_match_attempted_immutable_tuple",
                existing_proof_trade_id=existing.get("proof_trade_id"),
                attempted_proof_trade_id=proof_id,
            )
        validated = _validate_existing_binding(
            c,
            binding=existing,
            snapshot_identity=snapshot_identity,
        )
        if validated.get("ok"):
            return _already_bound(snapshot_id, existing)
        return validated
    return _result(
        snapshot_id=snapshot_id,
        disposition="PERSIST_FAILED",
        ok=False,
        reason="binding_insert_conflicted_without_existing_row",
    )


def _bind_parent_lineage(
    c: Any,
    *,
    snapshot: Mapping[str, Any],
    snapshot_identity: Mapping[str, Any],
) -> dict[str, Any]:
    snapshot_id = _text(snapshot_identity["snapshot_id"])
    descendants = _fetch_descendants(c, snapshot_id)
    if not descendants:
        return _result(
            snapshot_id=snapshot_id,
            disposition="PARENT_LINEAGE_UNPROVEN",
            reason="pretrigger_has_no_proven_descendant_snapshot",
        )

    proof_rows_by_id: dict[str, dict[str, Any]] = {}
    unresolved = []
    for child in descendants:
        child_id = _text(child.get("id"))
        child_identity = _snapshot_identity(
            child,
            expected_client_id=snapshot_identity["client_id"],
            expected_execution_mode=snapshot_identity["execution_mode"],
        )
        if not child_identity.get("ok"):
            if _text(child.get("local_order_id")):
                unresolved.append(child_identity.get("disposition"))
            continue
        if child_identity["canonical_signal_id"] != snapshot_identity["canonical_signal_id"]:
            if _text(child.get("local_order_id")):
                unresolved.append("PARENT_LINEAGE_UNPROVEN")
            continue
        if not _text(child.get("local_order_id")):
            continue

        existing = _fetch_existing_binding(c, child_id)
        if existing:
            existing_proof_id = _proof_trade_id(existing.get("proof_trade_id"))
            if existing_proof_id:
                validated = _validate_existing_binding(
                    c,
                    binding=existing,
                    snapshot_identity=child_identity,
                )
                if validated.get("ok"):
                    proof_rows_by_id[existing_proof_id] = validated.get("proof_row") or {}
                else:
                    unresolved.append(
                        str(validated.get("disposition") or "PARENT_LINEAGE_UNPROVEN")
                    )
            else:
                unresolved.append("PARENT_LINEAGE_UNPROVEN")
            continue

        proof_rows = _fetch_proofs_for_local_order(c, _text(child.get("local_order_id")))
        direct = _resolve_direct_proof(child_identity, proof_rows)
        if direct.get("disposition") != "BOUND":
            unresolved.append(direct.get("disposition"))
            continue
        child_binding = _insert_binding(
            c,
            snapshot_identity=child_identity,
            proof_row=direct["proof_row"],
            binding_method=DIRECT_BINDING_METHOD,
        )
        if not child_binding.get("ok"):
            return _result(
                snapshot_id=snapshot_id,
                disposition="PERSIST_FAILED",
                ok=False,
                reason=child_binding.get("reason") or "child_binding_persist_failed",
            )
        child_proof_id = _proof_trade_id(child_binding.get("proof_trade_id"))
        if child_proof_id:
            proof_rows_by_id.setdefault(child_proof_id, direct["proof_row"])
        else:
            unresolved.append("PARENT_LINEAGE_UNPROVEN")

    if len(proof_rows_by_id) != 1 or unresolved:
        return _result(
            snapshot_id=snapshot_id,
            disposition="PARENT_LINEAGE_UNPROVEN",
            reason="descendants_do_not_prove_one_economic_proof",
            proof_trade_ids=sorted(proof_rows_by_id),
            unresolved_descendants=[item for item in unresolved if item],
        )

    proof_row = next(iter(proof_rows_by_id.values()))
    if not proof_row:
        return _result(
            snapshot_id=snapshot_id,
            disposition="PARENT_LINEAGE_UNPROVEN",
            reason="proven_child_binding_proof_row_unavailable",
        )
    return _insert_binding(
        c,
        snapshot_identity=snapshot_identity,
        proof_row=proof_row,
        binding_method=LINEAGE_BINDING_METHOD,
    )


def _bind_snapshot_in_connection(c: Any, snapshot_id: str) -> dict[str, Any]:
    snapshot = _fetch_snapshot(c, snapshot_id)
    if not snapshot:
        return _result(
            snapshot_id=snapshot_id,
            disposition="SNAPSHOT_IDENTITY_UNPROVEN",
            reason="snapshot_not_found",
        )
    identity = _snapshot_identity(snapshot)
    if not identity.get("ok"):
        return _result(
            snapshot_id=snapshot_id,
            disposition=str(identity.get("disposition") or "SNAPSHOT_IDENTITY_UNPROVEN"),
            reason=identity.get("reason"),
        )

    existing = _fetch_existing_binding(c, snapshot_id)
    if existing:
        validated = _validate_existing_binding(
            c,
            binding=existing,
            snapshot_identity=identity,
        )
        if not validated.get("ok"):
            return validated
        if not identity["local_order_id"]:
            if identity["phase"] != "PRETRIGGER":
                return _result(
                    snapshot_id=snapshot_id,
                    disposition="LOCAL_ORDER_ID_MISSING",
                    reason="only_pretrigger_can_use_parent_lineage_without_local_order_id",
                )
            # A PRETRIGGER binding is only durable authority when its
            # descendant lineage remains provable on repeat reconciliation.
            return _bind_parent_lineage(c, snapshot=snapshot, snapshot_identity=identity)
        return _already_bound(snapshot_id, existing)

    if identity["local_order_id"]:
        proof_rows = _fetch_proofs_for_local_order(c, identity["local_order_id"])
        direct = _resolve_direct_proof(identity, proof_rows)
        if direct.get("disposition") != "BOUND":
            return direct
        return _insert_binding(
            c,
            snapshot_identity=identity,
            proof_row=direct["proof_row"],
            binding_method=DIRECT_BINDING_METHOD,
        )

    if identity["phase"] != "PRETRIGGER":
        return _result(
            snapshot_id=snapshot_id,
            disposition="LOCAL_ORDER_ID_MISSING",
            reason="only_pretrigger_can_use_parent_lineage_without_local_order_id",
        )
    return _bind_parent_lineage(c, snapshot=snapshot, snapshot_identity=identity)


def bind_snapshot_to_proof(snapshot_id: str) -> dict[str, Any]:
    """Bind one snapshot, or return an explicit non-binding disposition."""
    normalized_snapshot_id = _text(snapshot_id)
    if not normalized_snapshot_id:
        return _result(
            snapshot_id="",
            disposition="SNAPSHOT_IDENTITY_UNPROVEN",
            reason="snapshot_id_missing",
        )

    try:
        def _fn():
            with _db_conn()() as c:
                return _bind_snapshot_in_connection(c, normalized_snapshot_id)

        return _run_with_retry(_fn)
    except Exception as exc:  # noqa: BLE001 - classify persistence failures explicitly
        log.error("intelligence outcome binding failed snapshot_id=%s error=%s", normalized_snapshot_id, exc)
        return _result(
            snapshot_id=normalized_snapshot_id,
            disposition="PERSIST_FAILED",
            ok=False,
            reason=str(exc)[:500],
        )


def _snapshot_scope_sql(client_id: str, execution_mode: str) -> tuple[str, tuple[Any, ...]]:
    # Email client IDs are case-insensitive; non-email IDs remain exact after
    # trimming.  Both branches are explicit so arbitrary client IDs are not
    # silently case-folded.
    return (
        """
        AND (
            (POSITION('@' IN s.client_id)>0 AND lower(BTRIM(s.client_id))=lower(%s))
            OR (POSITION('@' IN s.client_id)=0 AND BTRIM(s.client_id)=%s)
        )
        AND lower(BTRIM(s.execution_mode))=%s
        """,
        (client_id, client_id, execution_mode),
    )


def reconcile_intelligence_outcome_bindings(
    *,
    client_id: str,
    execution_mode: str,
    limit: int = 50,
) -> dict[str, Any]:
    """Reconcile a bounded client/mode snapshot slice without guessing."""
    normalized_client_id = normalize_client_id(client_id)
    normalized_mode = normalize_execution_mode(execution_mode)
    if not normalized_client_id or not normalized_mode:
        return {
            "ok": False,
            "client_id": normalized_client_id,
            "execution_mode": normalized_mode,
            "processed": 0,
            "dispositions": {"SNAPSHOT_IDENTITY_UNPROVEN": 1},
            "results": [],
            "error": "client_id_and_execution_mode_must_be_explicit_live_or_paper",
        }
    try:
        bounded_limit = max(1, min(int(limit or 50), 1000))
    except (TypeError, ValueError):
        bounded_limit = 50

    try:
        def _load():
            with _db_conn()() as c:
                scope_sql, scope_params = _snapshot_scope_sql(normalized_client_id, normalized_mode)
                c.execute(
                    f"""
                    SELECT s.*
                    FROM ap_intelligence_snapshots s
                    WHERE NOT EXISTS (
                        SELECT 1 FROM ap_intelligence_outcome_bindings b
                        WHERE b.snapshot_id=s.id
                    )
                    {scope_sql}
                    ORDER BY s.computed_at ASC, s.created_at ASC, s.id ASC
                    LIMIT %s
                    """,
                    (*scope_params, bounded_limit),
                )
                return [_row_dict(row) for row in (c.fetchall() or [])]

        snapshots = _run_with_retry(_load)
    except Exception as exc:  # noqa: BLE001 - caller needs durable failure truth
        log.error(
            "intelligence outcome reconciliation load failed client=%s mode=%s error=%s",
            normalized_client_id, normalized_mode, exc,
        )
        return {
            "ok": False,
            "client_id": normalized_client_id,
            "execution_mode": normalized_mode,
            "processed": 0,
            "dispositions": {"PERSIST_FAILED": 1},
            "results": [],
            "error": str(exc)[:500],
        }

    results = [bind_snapshot_to_proof(_text(snapshot.get("id"))) for snapshot in snapshots]
    dispositions = Counter(str(item.get("disposition") or "PERSIST_FAILED") for item in results)
    return {
        "ok": all(bool(item.get("ok")) for item in results),
        "client_id": normalized_client_id,
        "execution_mode": normalized_mode,
        "processed": len(results),
        "bound": sum(1 for item in results if item.get("disposition") == "BOUND"),
        "already_bound": sum(1 for item in results if item.get("disposition") == "ALREADY_BOUND"),
        "dispositions": dict(dispositions),
        "results": results,
    }


def intelligence_truth_health(
    *,
    client_id: str | None = None,
    execution_mode: str | None = None,
    limit: int = 1000,
) -> dict[str, Any]:
    """Return read-only capture, schema, binding, and ambiguity truth."""
    enabled = _capture_enabled()
    report: dict[str, Any] = {
        "ok": False,
        "status": "CAPTURE_DISABLED" if not enabled else "UNKNOWN",
        "context_capture_enabled": enabled,
        "capture_status": "CAPTURE_ENABLED" if enabled else "CAPTURE_DISABLED",
        "capture_diagnostic": (
            "INTELLIGENCE_CONTEXT_CAPTURE_ENABLED"
            if enabled else "INTELLIGENCE_CONTEXT_CAPTURE_DISABLED"
        ),
        "snapshot_table_present": False,
        "snapshot_count": 0,
        "jobs_pending": 0,
        "jobs_failed_terminal": 0,
        "counterfactual_table_present": False,
        "binding_table_present": False,
        "live_official_proofs_available": 0,
        "live_bound_snapshots": 0,
        "paper_bound_snapshots": 0,
        "unbound_counts_truncated": False,
        "unbound_missing_identity": 0,
        "unbound_ambiguous_proof": 0,
        "mode_conflicts": 0,
        "schema_healthy": None,
        "error": None,
    }
    try:
        from ap.schema_attestation import attest_intelligence_schema

        schema = attest_intelligence_schema(strict=False)
        schema_checked = not bool(schema.get("skipped"))
        report["schema_healthy"] = bool(schema.get("ok")) and schema_checked
        report["schema_missing_tables"] = list(schema.get("missing_tables") or [])
        report["schema_missing_columns"] = dict(schema.get("missing_columns") or {})
        missing_tables = set(report["schema_missing_tables"])
        report["snapshot_table_present"] = schema_checked and "ap_intelligence_snapshots" not in missing_tables
        report["counterfactual_table_present"] = schema_checked and "blocked_signal_counterfactuals" not in missing_tables
        report["binding_table_present"] = schema_checked and "ap_intelligence_outcome_bindings" not in missing_tables
    except Exception as exc:  # noqa: BLE001 - health must report unavailable truth
        report["schema_healthy"] = False
        report["error"] = f"schema_attestation_failed:{exc}"
        return report

    if not report["schema_healthy"]:
        report["status"] = "CAPTURE_DISABLED" if not enabled else "DEGRADED_SCHEMA_UNHEALTHY"
        return report

    normalized_client_id = normalize_client_id(client_id) if client_id else ""
    normalized_mode = normalize_execution_mode(execution_mode) if execution_mode else ""
    try:
        bounded_limit = max(1, min(int(limit or 1000), 5000))
    except (TypeError, ValueError):
        bounded_limit = 1000

    def _read():
        with _db_conn()() as c:
            scope = []
            params: list[Any] = []
            if normalized_client_id:
                scope.append(
                    "((POSITION('@' IN s.client_id)>0 AND lower(BTRIM(s.client_id))=lower(%s)) "
                    "OR (POSITION('@' IN s.client_id)=0 AND BTRIM(s.client_id)=%s))"
                )
                params.extend([normalized_client_id, normalized_client_id])
            if normalized_mode:
                scope.append("lower(BTRIM(s.execution_mode))=%s")
                params.append(normalized_mode)
            scope_sql = (" AND " + " AND ".join(scope)) if scope else ""

            c.execute(f"SELECT COUNT(*) AS count FROM ap_intelligence_snapshots s WHERE TRUE{scope_sql}", tuple(params))
            snapshot_count = int((_row_dict(c.fetchone()).get("count") or 0))
            c.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM ap_intelligence_jobs j
                WHERE j.status IN ('PENDING','RUNNING','RETRY_PENDING')
                {scope_sql.replace('s.', 'j.')}
                """,
                tuple(params),
            )
            jobs_pending = int((_row_dict(c.fetchone()).get("count") or 0))
            c.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM ap_intelligence_jobs j
                WHERE j.status='FAILED_TERMINAL'
                {scope_sql.replace('s.', 'j.')}
                """,
                tuple(params),
            )
            jobs_failed_terminal = int((_row_dict(c.fetchone()).get("count") or 0))

            proof_scope = []
            proof_params: list[Any] = []
            if normalized_client_id:
                proof_scope.append(
                    "((POSITION('@' IN p.client_email)>0 AND lower(BTRIM(p.client_email))=lower(%s)) "
                    "OR (POSITION('@' IN p.client_email)=0 AND BTRIM(p.client_email)=%s))"
                )
                proof_params.extend([normalized_client_id, normalized_client_id])
            proof_scope_sql = (" AND " + " AND ".join(proof_scope)) if proof_scope else ""
            c.execute(
                f"""
                SELECT COUNT(*) AS count
                FROM proof_trades p
                WHERE lower(BTRIM(COALESCE(p.execution_mode,'')))='live'
                  AND lower(BTRIM(COALESCE(p.mode,'')))='live'
                  AND p.performance_taxonomy='LIVE_OFFICIAL'
                  AND p.training_eligible IS TRUE
                  AND p.official_live_performance_eligible IS TRUE
                  AND NULLIF(BTRIM(p.local_order_id),'') IS NOT NULL
                  {proof_scope_sql}
                """,
                tuple(proof_params),
            )
            live_official = int((_row_dict(c.fetchone()).get("count") or 0))

            binding_scope = []
            binding_params: list[Any] = []
            if normalized_client_id:
                binding_scope.append(
                    "((POSITION('@' IN b.client_id)>0 AND lower(BTRIM(b.client_id))=lower(%s)) "
                    "OR (POSITION('@' IN b.client_id)=0 AND BTRIM(b.client_id)=%s))"
                )
                binding_params.extend([normalized_client_id, normalized_client_id])
            binding_scope_sql = (" AND " + " AND ".join(binding_scope)) if binding_scope else ""
            c.execute(
                f"SELECT COUNT(*) AS count FROM ap_intelligence_outcome_bindings b "
                f"WHERE lower(BTRIM(b.execution_mode))='live'{binding_scope_sql}",
                tuple(binding_params),
            )
            live_bound = int((_row_dict(c.fetchone()).get("count") or 0))
            c.execute(
                f"SELECT COUNT(*) AS count FROM ap_intelligence_outcome_bindings b "
                f"WHERE lower(BTRIM(b.execution_mode))='paper'{binding_scope_sql}",
                tuple(binding_params),
            )
            paper_bound = int((_row_dict(c.fetchone()).get("count") or 0))

            c.execute(
                f"""
                SELECT s.*
                FROM ap_intelligence_snapshots s
                WHERE NOT EXISTS (
                    SELECT 1 FROM ap_intelligence_outcome_bindings b WHERE b.snapshot_id=s.id
                )
                {scope_sql}
                ORDER BY s.computed_at ASC, s.created_at ASC, s.id ASC
                LIMIT %s
                """,
                (*params, bounded_limit),
            )
            unbound = [_row_dict(row) for row in (c.fetchall() or [])]
            unbound_counts_truncated = len(unbound) >= bounded_limit
            ambiguous = 0
            mode_conflicts = 0
            missing_identity = 0
            for snapshot in unbound:
                snapshot_identity = _snapshot_identity(snapshot)
                if not _text(snapshot.get("local_order_id")):
                    missing_identity += 1
                    continue
                if not snapshot_identity.get("ok"):
                    continue
                proof_rows = _fetch_proofs_for_local_order(c, snapshot_identity["local_order_id"])
                disposition = _resolve_direct_proof(snapshot_identity, proof_rows).get("disposition")
                if disposition == "PROOF_AMBIGUOUS":
                    ambiguous += 1
                elif disposition == "MODE_CONFLICT":
                    mode_conflicts += 1
            return snapshot_count, jobs_pending, jobs_failed_terminal, live_official, live_bound, paper_bound, unbound_counts_truncated, missing_identity, ambiguous, mode_conflicts

    try:
        (
            report["snapshot_count"],
            report["jobs_pending"],
            report["jobs_failed_terminal"],
            report["live_official_proofs_available"],
            report["live_bound_snapshots"],
            report["paper_bound_snapshots"],
            report["unbound_counts_truncated"],
            report["unbound_missing_identity"],
            report["unbound_ambiguous_proof"],
            report["mode_conflicts"],
        ) = _run_with_retry(_read)
    except Exception as exc:  # noqa: BLE001 - surface health failure explicitly
        report["error"] = str(exc)[:500]
        report["status"] = "CAPTURE_DISABLED" if not enabled else "DEGRADED_READ_FAILED"
        return report

    if not enabled:
        report["status"] = "CAPTURE_DISABLED"
    elif report["jobs_failed_terminal"]:
        report["status"] = "DEGRADED_JOBS_FAILED_TERMINAL"
    elif report["jobs_pending"] and not report["snapshot_count"]:
        report["status"] = "DEGRADED_SNAPSHOT_CAPTURE_NOT_PROGRESSING"
    elif report["unbound_counts_truncated"]:
        report["status"] = "DEGRADED_UNBOUND_COUNTS_TRUNCATED"
    elif not report["snapshot_count"]:
        report["status"] = "ENABLED_NO_OBSERVATIONS"
    else:
        report["status"] = "HEALTHY"
    report["ok"] = bool(report["schema_healthy"]) and report["status"] == "HEALTHY"
    return report


__all__ = [
    "BINDING_VERSION",
    "bind_snapshot_to_proof",
    "intelligence_truth_health",
    "normalize_client_id",
    "normalize_execution_mode",
    "reconcile_intelligence_outcome_bindings",
]
