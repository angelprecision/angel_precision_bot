# ap/attribution_integrity.py — P0: lineage recovery for broker-imported positions
# =============================================================================
# WHY THIS MODULE EXISTS
# ──────────────────────
# The 2026-07-03 research audit found the trade record cannot support edge
# validation: 118 of 178 closed positions (66%) carry pattern BROKER_IMPORT,
# blank, or NULL — no scanner hypothesis attribution. Without attribution,
# per-pattern expectancy, the FVG experiment (#283), and any client-facing
# performance claim are impossible.
#
# Root cause (ap_reconciler._create_imported_position): when the reconciler
# finds a broker-open position missing from the DB, it FABRICATES identity —
#     signal_id = "reconciled:{contract}:{uuid8}"
#     pattern   = "BROKER_IMPORT"
# — without ever checking the orders table. Measured on live data 2026-07-04:
# **78 of 132 BROKER_IMPORT positions have a matching ENTRY order for the same
# client + contract with a real signal_id within 21 days** (122/132 with a
# relaxed window). The bot ordered those contracts itself; the reconciler
# threw the lineage away. A restart or DB hiccup converts an attributed bot
# trade into unattributable noise — permanently.
#
# Secondary cause: the reconciler's full-SQL fallback INSERT omits the
# pattern column entirely → NULL-pattern rows (8 in the closed record).
#
# WHAT THIS MODULE DOES
# ─────────────────────
# import_identity() is the single authority the reconciler consults before
# creating an imported position:
#   1. try recover_lineage(): newest ENTRY order for (client_id, contract)
#      inside the lookback carrying a non-fabricated signal_id — FILLED
#      preferred over any other status — then join ap_signals for the pattern.
#   2. recovered  → identity carries the REAL signal_id + REAL pattern.
#      Provenance is NOT lost: tier stays 'RECONCILED' and plan_id keeps the
#      'reconciled:' prefix, so imports remain queryable as imports while the
#      research record regains its hypothesis linkage.
#   3. not recovered → exactly today's fabricated identity (honest
#      BROKER_IMPORT / BROKER_IMPORT_PRICE_UNTRUSTED), byte-compatible with
#      existing analytics.
#
# DESIGN INVARIANTS
# ─────────────────
# 1. READ-ONLY against orders/ap_signals. This module never writes.
# 2. FAIL-CLOSED TO CURRENT BEHAVIOR. Any query error, timeout, or shape
#    surprise → fabricated identity, identical to pre-PR. Recovery can only
#    ADD attribution, never block an import (imports are a safety mechanism;
#    a position must always be created).
# 3. NEVER INVENT ATTRIBUTION. A recovered pattern comes only from the
#    matched order's signal row. No fallback defaults, no guesses — the
#    FALLBACK_DEFAULT lesson applies to lineage too.
# 4. PRICE-TRUST SEPARATION. price_untrusted describes the ENTRY PRICE, not
#    the lineage. When lineage is recovered under price_untrusted, the real
#    pattern is used and price distrust continues to travel via the existing
#    close_confidence/exit-engine seeding paths; attribution_source records
#    'recovered_price_untrusted' so analytics can segment.
# =============================================================================

from __future__ import annotations

import hashlib
import json
import math
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Optional

from ap.logger import get_logger

log = get_logger("ap.attribution_integrity")

LOOKBACK_DAYS_DEFAULT = 21
FABRICATED_PREFIX = "reconciled:"
_FILLED_ENTRY_STATUSES = frozenset({
    "FILLED", "PARTIAL_FILL", "PARTIALLY_FILLED",
})


@dataclass(frozen=True)
class ImportIdentity:
    plan_id: str
    signal_id: str
    pattern: str
    attributed: bool
    attribution_source: str          # recovered | recovered_price_untrusted | fabricated
    matched_order_id: Optional[str] = None
    matched_order_status: Optional[str] = None
    durable_fingerprint: str = ""
    identity_valid: bool = True
    identity_reason: str = ""
    # The orders.id value is only a row key.  Preserve the actual durable
    # ENTRY identifiers as well so a broker-only restart import can hand the
    # identity through to positions and the canonical owner installer.
    matched_local_order_id: Optional[str] = None
    matched_broker_order_id: Optional[str] = None
    # Broad attribution may still be useful when an ENTRY never filled.  These
    # fields are promoted into the canonical position only for a fill-proven
    # ENTRY identity.
    entry_identity_proven: bool = False
    entry_identity_reason: str = ""

    def to_log(self) -> str:
        return (
            f"attributed={self.attributed} source={self.attribution_source} "
            f"signal_id={self.signal_id} pattern={self.pattern} "
            f"order={self.matched_order_id or '-'}({self.matched_order_status or '-'})"
        )


def _row_get(row: Any, key: str, index: int) -> Any:
    if row is None:
        return None
    try:
        if key in row:  # type: ignore[operator]
            return row[key]
    except (TypeError, AttributeError):
        pass
    try:
        return row[index]
    except (KeyError, IndexError, TypeError):
        return None


def _entry_fill_proof(
    row: Any,
    *,
    broker_position: Optional[dict[str, Any]] = None,
    broker_quantity: int = 0,
    broker_cost_basis: float = 0.0,
) -> tuple[bool, str]:
    """Validate durable facts required before ENTRY IDs become authoritative."""
    status = str(_row_get(row, "order_status", 2) or "").strip().upper()
    if status not in _FILLED_ENTRY_STATUSES:
        return False, "entry_status_not_filled"

    local_id = str(_row_get(row, "entry_local_order_id", 3) or "").strip()
    broker_id = str(_row_get(row, "entry_broker_order_id", 4) or "").strip()
    if not local_id and not broker_id:
        return False, "entry_order_identity_missing"
    if not local_id or not broker_id:
        return False, "entry_order_identity_incomplete"

    raw_fill_price = _row_get(row, "entry_fill_price", 5)
    raw_filled_qty = _row_get(row, "entry_filled_qty", 6)
    raw_filled_ts = _row_get(row, "entry_filled_ts", 7)
    if isinstance(raw_fill_price, bool):
        return False, "entry_fill_price_malformed"
    try:
        fill_price = float(raw_fill_price)
    except (TypeError, ValueError, OverflowError):
        return False, "entry_fill_price_malformed"
    if not math.isfinite(fill_price) or fill_price <= 0:
        return False, "entry_fill_price_malformed"

    if isinstance(raw_filled_qty, bool):
        return False, "entry_filled_qty_malformed"
    try:
        filled_qty = float(raw_filled_qty)
    except (TypeError, ValueError, OverflowError):
        return False, "entry_filled_qty_malformed"
    if (
        not math.isfinite(filled_qty)
        or filled_qty <= 0
        or filled_qty != int(filled_qty)
    ):
        return False, "entry_filled_qty_malformed"
    try:
        broker_qty = int(broker_quantity or 0)
    except (TypeError, ValueError, OverflowError):
        broker_qty = 0
    if broker_qty > 0 and int(filled_qty) < broker_qty:
        # A single historical ENTRY cannot account for more contracts than it
        # filled.  More than one plausible fill must remain broker-truth-only.
        return False, "entry_broker_quantity_conflict"

    try:
        cost_basis = abs(float(broker_cost_basis or 0.0))
    except (TypeError, ValueError, OverflowError):
        cost_basis = 0.0
    if cost_basis > 0 and broker_qty > 0:
        expected_basis = fill_price * broker_qty * 100.0
        if not math.isfinite(expected_basis) or not math.isclose(
            cost_basis, expected_basis, rel_tol=1e-6, abs_tol=1e-6
        ):
            return False, "entry_broker_cost_basis_conflict"

    broker = broker_position if isinstance(broker_position, dict) else {}
    for key in ("entry_price", "avg_fill", "average_price", "average_cost"):
        raw_broker_price = broker.get(key)
        if raw_broker_price in (None, ""):
            continue
        if isinstance(raw_broker_price, bool):
            return False, "entry_broker_price_malformed"
        try:
            broker_price = float(raw_broker_price)
        except (TypeError, ValueError, OverflowError):
            return False, "entry_broker_price_malformed"
        if not math.isfinite(broker_price) or broker_price <= 0:
            return False, "entry_broker_price_malformed"
        if not math.isclose(broker_price, fill_price, rel_tol=1e-6, abs_tol=1e-6):
            return False, "entry_broker_price_conflict"
        break

    timestamp_valid = isinstance(raw_filled_ts, datetime)
    if isinstance(raw_filled_ts, str) and raw_filled_ts.strip():
        try:
            datetime.fromisoformat(raw_filled_ts.strip().replace("Z", "+00:00"))
            timestamp_valid = True
        except ValueError:
            timestamp_valid = False
    if not timestamp_valid:
        return False, "entry_filled_ts_malformed"
    return True, ""


def canonical_signal_id_for_lookup(signal_id: str) -> str:
    raw = str(signal_id or "").strip()
    if raw.startswith("REEVAL:"):
        parts = raw.split(":")
        if len(parts) >= 2 and parts[1]:
            return parts[1]
    return raw


def recover_lineage(
    *,
    contract: str,
    client_id: str,
    lookback_days: int = LOOKBACK_DAYS_DEFAULT,
    execution_mode: str = "",
    broker_position: Optional[dict[str, Any]] = None,
    broker_quantity: int = 0,
    broker_cost_basis: float = 0.0,
) -> Optional[dict[str, Any]]:
    """Find the true (signal_id, pattern) for a broker-open contract.

    Match: newest ENTRY order for the same client + contract whose signal_id
    is real (not 'reconciled:*'), with fill-like statuses ranked first, within
    the lookback. This broad match recovers research attribution. The returned
    execution IDs are separately marked as proven only when one unambiguous
    filled ENTRY also has valid fill quantity/price/time and broker economics.
    When supplied, execution_mode is resolved from the durable order
    column/metadata. Pattern joined from ap_signals via the composite
    (signal_id, client_email) identity (#260); ap_signals.signal_id is uuid,
    compared ::text so legacy string ids no-op instead of raising.

    Returns None on no match or ANY error (invariant 2).
    """
    if not contract or not client_id:
        return None
    try:
        from ap.db import conn, run_with_retry

        def _q() -> Optional[dict[str, Any]]:
            with conn() as c:
                c.execute(
                    f"""
                    SELECT
                      o.id              AS order_id,
                      o.signal_id       AS order_signal_id,
                      o.status          AS order_status,
                      o.local_order_id  AS entry_local_order_id,
                      o.broker_order_id AS entry_broker_order_id,
                      o.fill_price      AS entry_fill_price,
                      o.filled_qty      AS entry_filled_qty,
                      o.filled_ts       AS entry_filled_ts,
                      o.contract        AS entry_contract
                    FROM orders o
                    WHERE o.client_id = %s
                      AND o.contract = %s
                      AND upper(o.kind) = 'ENTRY'
                      AND o.signal_id IS NOT NULL
                      AND o.signal_id NOT LIKE %s
                      {mode_clause}
                      AND o.created_ts > now() - (%s || ' days')::interval
                    ORDER BY
                      (upper(btrim(o.status)) IN ('FILLED', 'PARTIAL_FILL', 'PARTIALLY_FILLED')) DESC,
                      (upper(btrim(o.status)) = 'FILLED') DESC,
                      o.created_ts DESC
                    LIMIT 2
                    """,
                    (client_id, contract, FABRICATED_PREFIX + "%", *mode_params,
                     str(int(lookback_days))),
                )
                try:
                    rows = c.fetchall()
                except AttributeError:
                    first = c.fetchone()
                    rows = [first] if first else []
                if not rows:
                    return None
                row = rows[0]
                order_id = str(_row_get(row, "order_id", 0) or "")
                order_signal_id = str(_row_get(row, "order_signal_id", 1) or "")
                order_status = str(_row_get(row, "order_status", 2) or "")
                signal_id = canonical_signal_id_for_lookup(order_signal_id)
                if not signal_id:
                    return None

                c.execute(
                    """
                    SELECT COALESCE(
                             NULLIF(raw_payload->>'pattern', ''),
                             NULLIF(signal_payload->>'pattern', '')
                           ) AS pattern
                    FROM ap_signals
                    WHERE signal_id::text = %s AND client_email = %s
                    LIMIT 1
                    """,
                    (signal_id, client_id),
                )
                prow = c.fetchone()
                _pattern = _row_get(prow, "pattern", 0)
                pattern = str(_pattern) if _pattern else None
                if not pattern:
                    # Invariant 3: a signal row without a pattern yields lineage
                    # for signal_id only if we can attribute honestly — we
                    # cannot, so no recovery. Never invent a pattern.
                    log.info(
                        "attribution_integrity: order %s matched but signal %s "
                        "has no pattern — not recovering", order_id, signal_id,
                    )
                    return None

                # The first row remains the broad attribution winner.  Only a
                # single fill-like candidate may donate execution identity;
                # two fill-like candidates are ambiguous even when one is
                # newer, so neither can be promoted into positions.*.
                filled_candidates = [
                    candidate for candidate in rows
                    if str(_row_get(candidate, "order_status", 2) or "")
                    .strip().upper() in _FILLED_ENTRY_STATUSES
                ]
                entry_identity_proven = False
                entry_identity_reason = "entry_identity_not_proven"
                identity_candidate = None
                if len(filled_candidates) > 1:
                    entry_identity_reason = "entry_identity_ambiguous"
                else:
                    identity_candidate = filled_candidates[0] if filled_candidates else row
                    candidate_contract = str(
                        _row_get(identity_candidate, "entry_contract", 8) or contract
                    ).strip().upper()
                    mode_proven = normalized_mode in {"live", "paper"}
                    contract_proven = (
                        candidate_contract == str(contract).strip().upper()
                    )
                    if mode_proven and contract_proven:
                        entry_identity_proven, entry_identity_reason = _entry_fill_proof(
                            identity_candidate,
                            broker_position=broker_position,
                            broker_quantity=broker_quantity,
                            broker_cost_basis=broker_cost_basis,
                        )
                    elif not mode_proven:
                        entry_identity_reason = "entry_execution_mode_unproven"
                    else:
                        entry_identity_reason = "entry_contract_conflict"
                lineage = {
                    "signal_id": signal_id,
                    "pattern": pattern,
                    "order_id": order_id,
                    "order_status": order_status,
                    "order_signal_id": order_signal_id,
                }
                # Keep the historical return shape stable for callers/tests
                # whose order fixture predates these columns.  Production rows
                # with recovered IDs carry them explicitly.
                candidate_local_order_id = ""
                candidate_broker_order_id = ""
                if identity_candidate is not None:
                    candidate_local_order_id = str(
                        _row_get(identity_candidate, "entry_local_order_id", 3) or ""
                    ).strip()
                    candidate_broker_order_id = str(
                        _row_get(identity_candidate, "entry_broker_order_id", 4) or ""
                    ).strip()
                if candidate_local_order_id and entry_identity_proven:
                    lineage["entry_local_order_id"] = candidate_local_order_id
                if candidate_broker_order_id and entry_identity_proven:
                    lineage["entry_broker_order_id"] = candidate_broker_order_id
                lineage["entry_identity_proven"] = entry_identity_proven
                lineage["entry_identity_reason"] = entry_identity_reason
                return lineage

        # A mode is part of durable order identity.  When it is known, use the
        # same normalized column/metadata resolver as the rest of the order
        # truth paths; malformed or contradictory rows cannot donate lineage.
        mode_clause = ""
        mode_params: tuple[str, ...] = ()
        normalized_mode = str(execution_mode or "").strip().lower()
        if normalized_mode in {"live", "paper"}:
            from ap.order_state_machine import _DURABLE_EXECUTION_MODE_SQL

            mode_clause = f"AND {_DURABLE_EXECUTION_MODE_SQL}"
            mode_params = (normalized_mode,)

        return run_with_retry(_q)
    except Exception as exc:
        log.warning(
            "attribution_integrity: lineage recovery failed for %s/%s: %s "
            "(falling back to fabricated identity)", client_id, contract, exc,
        )
        return None


def import_identity(
    *,
    contract: str,
    client_id: str,
    execution_mode: str = "unknown",
    broker_position: Optional[dict[str, Any]] = None,
    broker_quantity: int = 0,
    broker_cost_basis: float = 0.0,
    price_untrusted: bool = False,
    lookback_days: int = LOOKBACK_DAYS_DEFAULT,
) -> ImportIdentity:
    """Single authority for stable imported-position identity. Never raises.

    The fingerprint deliberately excludes poll time.  It prefers durable broker
    lot/order identity when the broker exposes one, then uses the exact
    client/mode/contract/economic snapshot and recovered ENTRY lineage.  That
    makes identical reconciliation polls idempotent without collapsing two
    broker lots that carry distinct durable identifiers.
    """
    normalized_client = str(client_id or "").strip().lower()
    normalized_mode = str(execution_mode or "").strip().lower()
    normalized_contract = str(contract or "").strip().upper()
    broker_position = dict(broker_position or {})

    try:
        lineage = recover_lineage(
            contract=contract,
            client_id=client_id,
            lookback_days=lookback_days,
            execution_mode=execution_mode,
            broker_position=broker_position,
            broker_quantity=broker_quantity,
            broker_cost_basis=broker_cost_basis,
        )
    except Exception:  # defense in depth; recover_lineage already never raises
        lineage = None

    durable_broker_id = ""
    for key in (
        "broker_order_id", "order_id", "position_id", "positionId",
        "lot_id", "lotId", "id",
    ):
        value = str(broker_position.get(key) or "").strip()
        if value:
            durable_broker_id = f"{key}:{value}"
            break

    try:
        normalized_qty = int(broker_quantity or broker_position.get("quantity") or 0)
    except (TypeError, ValueError):
        normalized_qty = 0
    try:
        normalized_cost = round(float(
            broker_cost_basis or broker_position.get("cost_basis") or 0.0
        ), 8)
    except (TypeError, ValueError):
        normalized_cost = 0.0

    canonical = {
        "client_id": normalized_client,
        "execution_mode": normalized_mode,
        "contract": normalized_contract,
        "broker_quantity": normalized_qty,
        "broker_cost_basis": normalized_cost,
        "durable_broker_id": durable_broker_id,
        "entry_order_id": str((lineage or {}).get("order_id") or ""),
        "signal_id": str((lineage or {}).get("signal_id") or ""),
    }
    missing = [
        name for name, value in (
            ("client_id", normalized_client),
            ("execution_mode", normalized_mode if normalized_mode in {"paper", "live"} else ""),
            ("contract", normalized_contract),
            ("broker_quantity", normalized_qty if normalized_qty > 0 else 0),
        ) if not value
    ]
    fingerprint = hashlib.sha256(
        json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]
    plan_id = f"{FABRICATED_PREFIX}{normalized_contract}:{fingerprint}"
    identity_valid = not missing
    identity_reason = "" if identity_valid else f"missing_durable_identity:{','.join(missing)}"

    if lineage:
        ident = ImportIdentity(
            plan_id=plan_id,
            signal_id=lineage["signal_id"],
            pattern=lineage["pattern"],
            attributed=True,
            attribution_source=(
                "recovered_price_untrusted" if price_untrusted else "recovered"
            ),
            matched_order_id=lineage.get("order_id"),
            matched_order_status=lineage.get("order_status"),
            matched_local_order_id=(
                lineage.get("entry_local_order_id")
                if lineage.get("entry_identity_proven") else None
            ),
            matched_broker_order_id=(
                lineage.get("entry_broker_order_id")
                if lineage.get("entry_identity_proven") else None
            ),
            entry_identity_proven=bool(lineage.get("entry_identity_proven")),
            entry_identity_reason=str(lineage.get("entry_identity_reason") or ""),
            durable_fingerprint=fingerprint,
            identity_valid=identity_valid,
            identity_reason=identity_reason,
        )
    else:
        ident = ImportIdentity(
            plan_id=plan_id,
            signal_id=f"{FABRICATED_PREFIX}{normalized_contract}:{fingerprint}",
            pattern="BROKER_IMPORT_PRICE_UNTRUSTED" if price_untrusted else "BROKER_IMPORT",
            attributed=False,
            attribution_source="fabricated",
            durable_fingerprint=fingerprint,
            identity_valid=identity_valid,
            identity_reason=identity_reason,
        )
    log.info("attribution_integrity: import identity %s/%s → %s",
             client_id, contract, ident.to_log())
    return ident
