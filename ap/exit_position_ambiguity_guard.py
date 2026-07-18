"""Fail closed when an EXIT fill cannot identify one active real position.

Multiple positions for the same client and OCC contract are allowed by the
current schema. A synthetic/null EXIT ``position_id`` must never be repaired
by choosing the newest active row — that can apply fills and P&L to the wrong
economic position.

Resolution policy (by priority):

    real position_id          → delegate to the canonical resolver unchanged
    exactly one active cand.  → return that candidate; emit repair diagnostic
    two or more active cands. → quarantine and return None; never guess
    zero active candidates    → delegate to the canonical resolver so its
                                recently-closed lifecycle logic remains sole
                                authority; do not implement a second path here
    lookup failure            → return None and fail closed

``Active`` means status in (OPEN, CLOSING) with positive quantity_remaining
and entry_ts at or before the broker fill timestamp.  Terminal rows — even
recently closed ones — are excluded from the candidate set.
"""
from __future__ import annotations

from typing import Any, Callable

from ap.logger import get_logger

log = get_logger("ap.exit_position_ambiguity_guard")

_PATCHED_ATTR  = "_AP_EXIT_POSITION_AMBIGUITY_GUARD_PATCHED"
_ORIGINAL_ATTR = "_AP_EXIT_POSITION_AMBIGUITY_GUARD_ORIGINAL"

# Active status values accepted as candidates.  Anything outside this set is
# terminal or otherwise inactive and must not receive an ambiguous fill repair.
_ACTIVE_STATUSES = ("OPEN", "CLOSING")


def _is_synthetic_position_id(value: Any) -> bool:
    """True for null, blank, or broker-repair-* position IDs."""
    text = str(value or "").strip().lower()
    return not text or text.startswith("broker-repair-")


def _active_exact_contract_candidates(
    c: Any,
    *,
    client_id: str,
    contract: str,
    fill_ts: Any,
) -> list[dict[str, Any]]:
    """Return active, economically open positions matching this fill exactly.

    Candidates must satisfy ALL of:
    - exact client_id scope
    - exact normalized OCC contract (case-insensitive)
    - status in OPEN or CLOSING only (no recently-terminal fallback)
    - positive quantity_remaining
    - entry_ts at or before the broker fill timestamp (not future)
    - held FOR UPDATE on the caller's reconciliation transaction

    Column names (entry_ts, created_at, quantity_remaining) match the schema
    already used by the canonical reducer in exit_fill_truth_guard._resolve_position.
    """
    client_id = str(client_id or "").strip()
    contract  = str(contract  or "").strip().upper()
    if not client_id or not contract:
        return []

    rows = c.execute(
        "SELECT * FROM positions "
        "WHERE client_id = %s "
        "AND UPPER(contract) = UPPER(%s) "
        "AND UPPER(COALESCE(status, '')) IN ('OPEN', 'CLOSING') "
        "AND COALESCE(quantity_remaining, 0) > 0 "
        "AND COALESCE(entry_ts, created_at) <= COALESCE(%s, NOW()) "
        "ORDER BY COALESCE(entry_ts, created_at) DESC "
        "LIMIT 3 "
        "FOR UPDATE",
        (client_id, contract, fill_ts),
    ).fetchall()
    return [dict(row) for row in rows]


def _validate_sole_candidate(
    candidate: dict[str, Any],
    *,
    client_id: str,
    contract: str,
) -> bool:
    """Defensive invariant check before returning the sole active candidate.

    Returns True when all invariants pass; logs a critical diagnostic and
    returns False when any invariant fails so the caller can return None.
    """
    cand_id = str(candidate.get("id") or "").strip()
    if not cand_id or cand_id.lower().startswith("broker-repair-"):
        log.critical(
            "[%s] EXIT_POSITION_AMBIGUITY_CANDIDATE_INVALID_ID contract=%s candidate_id=%r",
            client_id, contract, cand_id or None,
        )
        return False

    cand_client = str(candidate.get("client_id") or "").strip()
    if cand_client != client_id:
        log.critical(
            "[%s] EXIT_POSITION_AMBIGUITY_CANDIDATE_CLIENT_MISMATCH "
            "expected=%s got=%s contract=%s",
            client_id, client_id, cand_client, contract,
        )
        return False

    cand_contract = str(candidate.get("contract") or "").strip().upper()
    if cand_contract != contract.upper():
        log.critical(
            "[%s] EXIT_POSITION_AMBIGUITY_CANDIDATE_CONTRACT_MISMATCH "
            "expected=%s got=%s id=%s",
            client_id, contract, cand_contract, cand_id,
        )
        return False

    cand_status = str(candidate.get("status") or "").strip().upper()
    if cand_status not in _ACTIVE_STATUSES:
        log.critical(
            "[%s] EXIT_POSITION_AMBIGUITY_CANDIDATE_STATUS_NOT_ACTIVE "
            "status=%s id=%s",
            client_id, cand_status, cand_id,
        )
        return False

    try:
        qty = int(candidate.get("quantity_remaining") or 0)
    except (TypeError, ValueError):
        qty = 0
    if qty <= 0:
        log.critical(
            "[%s] EXIT_POSITION_AMBIGUITY_CANDIDATE_ZERO_QUANTITY id=%s",
            client_id, cand_id,
        )
        return False

    return True


def wrap_resolve_position(
    original: Callable[[Any, dict, Any], dict | None],
) -> Callable[[Any, dict, Any], dict | None]:
    """Wrap ``_resolve_position`` with an active-only ambiguity fence.

    This wrapper does not alter resolution for real position IDs; it only
    intercepts synthetic/null IDs to enforce the active-only candidate policy.
    """
    def guarded(c: Any, order: dict, fill_ts: Any) -> dict | None:
        requested_id = str(order.get("position_id") or "").strip()

        # Real position ID: delegate to the canonical resolver unchanged.
        if not _is_synthetic_position_id(requested_id):
            return original(c, order, fill_ts)

        client_id = str(order.get("client_id") or "").strip()
        contract  = str(
            order.get("contract") or order.get("symbol") or ""
        ).strip().upper()

        try:
            candidates = _active_exact_contract_candidates(
                c,
                client_id=client_id,
                contract=contract,
                fill_ts=fill_ts,
            )
        except Exception as exc:
            log.critical(
                "[%s] EXIT_POSITION_AMBIGUITY_LOOKUP_FAILED contract=%s "
                "order=%s broker=%s error=%s",
                client_id, contract,
                order.get("local_order_id"),
                order.get("broker_order_id"),
                exc,
                exc_info=True,
            )
            return None

        if len(candidates) == 1:
            candidate = candidates[0]
            if not _validate_sole_candidate(
                candidate, client_id=client_id, contract=contract
            ):
                return None
            log.warning(
                "[%s] EXIT_POSITION_ID_REPAIRED_FROM_SOLE_ACTIVE_CONTRACT "
                "position=%s contract=%s order=%s broker=%s",
                client_id,
                candidate.get("id"),
                contract,
                order.get("local_order_id"),
                order.get("broker_order_id"),
            )
            return candidate

        if len(candidates) > 1:
            log.critical(
                "[%s] EXIT_POSITION_ID_AMBIGUOUS_QUARANTINED contract=%s "
                "active_positions=%s order=%s broker=%s",
                client_id,
                contract,
                [str(row.get("id") or "") for row in candidates],
                order.get("local_order_id"),
                order.get("broker_order_id"),
            )
            return None

        # Zero active candidates: delegate to the canonical resolver so its
        # recently-closed lifecycle logic (NOT re-implemented here) remains the
        # sole authority for recently closed positions.
        log.info(
            "[%s] EXIT_POSITION_AMBIGUITY_NO_ACTIVE_CANDIDATES contract=%s "
            "order=%s — delegating to canonical resolver",
            client_id, contract, order.get("local_order_id"),
        )
        return original(c, order, fill_ts)

    return guarded


def install_exit_position_ambiguity_guard() -> None:
    """Idempotent installation onto ``exit_fill_truth_guard._resolve_position``."""
    from ap import exit_fill_truth_guard

    if getattr(exit_fill_truth_guard, _PATCHED_ATTR, False):
        return
    original = exit_fill_truth_guard._resolve_position
    setattr(exit_fill_truth_guard, _ORIGINAL_ATTR, original)
    exit_fill_truth_guard._resolve_position = wrap_resolve_position(original)
    setattr(exit_fill_truth_guard, _PATCHED_ATTR, True)
