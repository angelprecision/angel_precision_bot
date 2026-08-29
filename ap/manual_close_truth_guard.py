"""Downstream truth guard for broker-confirmed manual/external closes.

A broker-confirmed manual close is still real LIVE performance and must remain
eligible for official realized-P&L reporting.  It is *not* autonomous exit
behavior and therefore must not be fed back into autonomous-training history as
if the bot chose/submitted the exit.

This guard installs two idempotent patches:

1. proof taxonomy: preserve LIVE_OFFICIAL / broker proof while setting
   training_eligible=False when the position's durable EXIT evidence is an
   ``external-exit:*`` row; also preserve that external local order identity in
   proof_trades.exit_local_order_id.
2. manual-close finalization: after canonical broker-confirmed finalization
   succeeds, terminalize only stale nonterminal trade_queue ownership for the
   exact client + originating signal, using a guarded status predicate.

No broker submit/cancel, order fill, position economics, or realized P&L is
changed by this guard.
"""

from __future__ import annotations

import json
import logging
import math
from typing import Any

from ap import db

log = logging.getLogger("ap.manual_close_truth_guard")

_PATCHED = "_AP_MANUAL_CLOSE_TRUTH_GUARD_PATCHED"
_EXTERNAL_PREFIX = "external-exit:"
_STALE_QUEUE_STATES = ("NEW", "PROCESSING", "WATCHING", "ARMED")
_MANUAL_TAXONOMY_REASON = (
    "tradier_exit_proof_lock_passed_manual_external_close_training_excluded"
)
_VALID_EXECUTION_MODES = frozenset({"live", "paper"})


def _external_exit_identity(
    client_id: str,
    position_id: str,
    *,
    execution_mode: str | None = None,
) -> dict | None:
    """Return one fully-proven durable external EXIT identity.

    The reconciler performs the economic close validation. This second fence
    prevents an arbitrary legacy or malformed row from driving proof taxonomy
    or downstream queue mutation.
    """
    client_id = str(client_id or "").strip().lower()
    position_id = str(position_id or "").strip()
    mode = str(execution_mode or "").strip().lower()
    if (
        not client_id
        or not position_id
        or (mode and mode not in _VALID_EXECUTION_MODES)
    ):
        return None

    from ap.manual_close_reconciliation import (
        BROKER_FILL_TIMESTAMP_KEYS,
        BROKER_FILL_TIMESTAMP_SOURCE,
        DURABLE_EXIT_FILLED_STATUSES,
        is_valid_occ_contract,
        normalize_contract,
        parse_broker_fill_timestamp,
        positive_float,
        positive_int,
    )

    def _read():
        with db.conn() as c:
            c.execute(
                """
                SELECT
                    o.local_order_id,
                    o.broker_order_id,
                    o.filled_ts,
                    o.filled_qty,
                    o.fill_price,
                    o.execution_mode,
                    o.contract,
                    o.direction,
                    o.meta,
                    p.contract AS position_contract,
                    p.direction AS position_direction
                FROM orders o
                JOIN positions p
                  ON p.client_id=o.client_id
                 AND p.id::text=o.position_id::text
                WHERE o.client_id=%s
                  AND o.position_id::text=%s
                  AND UPPER(COALESCE(o.kind,''))='EXIT'
                  AND UPPER(COALESCE(o.status,'')) = ANY(%s)
                  AND LOWER(COALESCE(o.execution_mode,'')) =
                      LOWER(COALESCE(p.execution_mode,''))
                  AND LOWER(COALESCE(o.execution_mode,'')) = ANY(%s)
                  AND COALESCE(o.local_order_id,'') LIKE %s
                  AND COALESCE(o.broker_order_id,'') <> ''
                  AND COALESCE(o.filled_qty,0) > 0
                  AND o.fill_price IS NOT NULL
                  AND o.fill_price > 0
                  AND COALESCE(o.meta->>'source','') =
                      'manual_client_close_broker_fill'
                  AND COALESCE(o.meta->>'external_broker_order','') = 'true'
                  AND COALESCE(o.meta->>'adopted_without_submit','') = 'true'
                  AND COALESCE(o.meta->>'exit_fill_timestamp_source','')=%s
                  AND COALESCE(o.meta->>'exit_fill_timestamp_key','') = ANY(%s)
                ORDER BY o.filled_ts DESC NULLS LAST, o.local_order_id DESC
                LIMIT 1
                """,
                (
                    client_id,
                    position_id,
                    list(DURABLE_EXIT_FILLED_STATUSES),
                    [mode] if mode else sorted(_VALID_EXECUTION_MODES),
                    f"{_EXTERNAL_PREFIX}%",
                    BROKER_FILL_TIMESTAMP_SOURCE,
                    list(BROKER_FILL_TIMESTAMP_KEYS),
                ),
            )
            row = c.fetchone()
            if not row:
                return None
            identity = dict(row)

            metadata = identity.get("meta")
            if isinstance(metadata, str):
                try:
                    metadata = json.loads(metadata)
                except Exception:
                    metadata = None
            if not isinstance(metadata, dict):
                return None
            if (
                metadata.get("source") != "manual_client_close_broker_fill"
                or metadata.get("external_broker_order") is not True
                or metadata.get("adopted_without_submit") is not True
                or metadata.get("exit_fill_timestamp_source")
                != BROKER_FILL_TIMESTAMP_SOURCE
                or metadata.get("exit_fill_timestamp_key")
                not in BROKER_FILL_TIMESTAMP_KEYS
            ):
                return None

            broker_order_id = str(identity.get("broker_order_id") or "").strip()
            local_order_id = str(identity.get("local_order_id") or "").strip()
            if local_order_id != f"{_EXTERNAL_PREFIX}{client_id}:{broker_order_id}":
                return None

            actual_mode = str(identity.get("execution_mode") or "").strip().lower()
            if (
                actual_mode not in _VALID_EXECUTION_MODES
                or (mode and actual_mode != mode)
            ):
                return None

            exit_contract = normalize_contract(identity.get("contract"))
            position_contract = normalize_contract(identity.get("position_contract"))
            if (
                not is_valid_occ_contract(exit_contract)
                or not is_valid_occ_contract(position_contract)
                or exit_contract != position_contract
            ):
                return None

            exit_direction = str(identity.get("direction") or "").strip().upper()
            position_direction = str(
                identity.get("position_direction") or ""
            ).strip().upper()
            if (
                exit_direction not in {"CALL", "PUT"}
                or position_direction not in {"CALL", "PUT"}
                or exit_direction != position_direction
            ):
                return None

            if positive_int(identity.get("filled_qty")) <= 0:
                return None
            fill_price = positive_float(identity.get("fill_price"))
            if fill_price <= 0 or not math.isfinite(fill_price):
                return None
            if parse_broker_fill_timestamp(identity.get("filled_ts")) is None:
                return None
            return identity

    try:
        return db.run_with_retry(_read)
    except Exception as exc:
        log.error(
            "manual external EXIT identity lookup failed client=%s position=%s error=%s",
            client_id, position_id, exc,
        )
        return None


def quarantine_manual_external_close_stamp(
    stamp: dict[str, Any], *, client_id: str, position_id: str
) -> dict[str, Any]:
    """Preserve official LIVE proof while excluding external exits from training."""
    out = dict(stamp or {})
    stamp_mode = str(out.get("execution_mode") or "").strip().lower()
    if stamp_mode != "live":
        return out
    external = _external_exit_identity(
        client_id,
        position_id,
        execution_mode=stamp_mode,
    )
    if not external:
        return out

    local_order_id = str(external.get("local_order_id") or "").strip()
    if not local_order_id.startswith(_EXTERNAL_PREFIX):
        return out

    # This is real broker performance. Do not demote LIVE_OFFICIAL or clear the
    # existing official lock. Only the autonomous-learning dimension changes.
    out["exit_local_order_id"] = local_order_id
    out["training_eligible"] = False
    out["taxonomy_reason"] = _MANUAL_TAXONOMY_REASON
    out["manual_external_close"] = True
    return out


def _persist_manual_close_proof_truth(
    *, client_id: str, position_id: str, external_local_order_id: str
) -> int:
    """Best-effort immediate proof correction after canonical finalization."""
    client_id = str(client_id or "").strip().lower()
    position_id = str(position_id or "").strip()
    external_local_order_id = str(external_local_order_id or "").strip()
    if (
        not client_id
        or not position_id
        or not external_local_order_id.startswith(f"{_EXTERNAL_PREFIX}{client_id}:")
        or not external_local_order_id[len(f"{_EXTERNAL_PREFIX}{client_id}:"):]
    ):
        return 0

    def _write():
        with db.conn() as c:
            c.execute(
                """
                SELECT id, exit_local_order_id
                FROM proof_trades
                WHERE client_email=%s
                  AND position_id::text=%s
                  AND LOWER(COALESCE(execution_mode,''))='live'
                ORDER BY id
                LIMIT 2
                """,
                (client_id, position_id),
            )
            candidates = [dict(row) for row in (c.fetchall() or [])]
            if len(candidates) != 1:
                log.error(
                    "manual close proof truth candidate cardinality invalid "
                    "client=%s position=%s candidates=%s",
                    client_id,
                    position_id,
                    len(candidates),
                )
                return 0
            proof_id = candidates[0].get("id")
            if proof_id in (None, ""):
                return 0
            existing_exit_id = str(
                candidates[0].get("exit_local_order_id") or ""
            ).strip()
            if existing_exit_id and existing_exit_id != external_local_order_id:
                log.error(
                    "manual close proof truth identity conflict client=%s "
                    "position=%s existing=%s requested=%s",
                    client_id,
                    position_id,
                    existing_exit_id,
                    external_local_order_id,
                )
                return 0

            cur = c.execute(
                """
                UPDATE proof_trades
                SET exit_local_order_id = CASE
                        WHEN COALESCE(exit_local_order_id,'') = '' THEN %s
                        ELSE exit_local_order_id
                    END,
                    training_eligible = FALSE,
                    taxonomy_reason = %s
                WHERE id=%s
                  AND client_email=%s
                  AND position_id::text=%s
                  AND LOWER(COALESCE(execution_mode,''))='live'
                  AND (
                      COALESCE(exit_local_order_id,'') = ''
                      OR exit_local_order_id=%s
                  )
                """,
                (
                    external_local_order_id,
                    _MANUAL_TAXONOMY_REASON,
                    proof_id,
                    client_id,
                    position_id,
                    external_local_order_id,
                ),
            )
            return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

    try:
        return int(db.run_with_retry(_write) or 0)
    except Exception as exc:
        log.error(
            "manual close proof truth persist failed client=%s position=%s error=%s",
            client_id, position_id, exc,
        )
        return 0


def _terminalize_stale_queue_after_manual_close(
    *, client_id: str, position_id: str, broker_exit_order_id: str
) -> int:
    """CAS-terminalize stale queue ownership after a proven external close.

    Queue is an entry/admission lifecycle. Once a canonical position has opened
    and is now broker-confirmed closed, a leftover ARMED/WATCHING/PROCESSING/NEW
    row for that exact originating signal is stale ownership. We move only those
    nonterminal states to FILLED, which is already a canonical terminal queue
    state. Existing terminal states are never overwritten.
    """
    client_id = str(client_id or "").strip().lower()
    position_id = str(position_id or "").strip()
    broker_exit_order_id = str(broker_exit_order_id or "").strip()
    if not client_id or not position_id or not broker_exit_order_id:
        return 0

    from ap.position_manager import PositionStatus

    def _write():
        with db.conn() as c:
            c.execute(
                """
                SELECT DISTINCT p.execution_mode, o.signal_id
                FROM positions p
                JOIN orders o
                  ON o.client_id=p.client_id
                 AND o.position_id::text=p.id::text
                WHERE p.client_id=%s
                  AND p.id::text=%s
                  AND UPPER(COALESCE(p.status,'')) = ANY(%s)
                  AND COALESCE(p.quantity_remaining,0) <= 0
                  AND LOWER(COALESCE(p.execution_mode,'')) =
                      LOWER(COALESCE(o.execution_mode,''))
                  AND LOWER(COALESCE(p.execution_mode,'')) = ANY(%s)
                  AND UPPER(COALESCE(o.kind,''))='ENTRY'
                  AND UPPER(COALESCE(o.status,'')) IN
                      ('FILLED','PARTIAL_FILL','PARTIALLY_FILLED')
                  AND COALESCE(o.filled_qty,0) > 0
                  AND NULLIF(BTRIM(COALESCE(o.signal_id,'')),'') IS NOT NULL
                LIMIT 2
                """,
                (
                    client_id,
                    position_id,
                    sorted(PositionStatus.TERMINAL),
                    sorted(_VALID_EXECUTION_MODES),
                ),
            )
            identity_rows = [dict(row) for row in (c.fetchall() or [])]
            if len(identity_rows) != 1:
                if len(identity_rows) > 1:
                    log.error(
                        "manual close queue identity cardinality invalid "
                        "client=%s position=%s rows=%s",
                        client_id,
                        position_id,
                        len(identity_rows),
                    )
                return 0
            position = identity_rows[0]
            signal_id = str(position.get("signal_id") or "").strip()
            mode = str(position.get("execution_mode") or "").strip().lower()
            if not signal_id or mode not in _VALID_EXECUTION_MODES:
                return 0

            c.execute(
                """
                SELECT id
                FROM trade_queue
                WHERE client_id=%s
                  AND signal_id=%s
                  AND LOWER(COALESCE(payload->>'execution_mode',''))=%s
                  AND UPPER(COALESCE(status,'')) = ANY(%s)
                ORDER BY id
                LIMIT 2
                FOR UPDATE
                """,
                (client_id, signal_id, mode, list(_STALE_QUEUE_STATES)),
            )
            queue_rows = [dict(row) for row in (c.fetchall() or [])]
            if len(queue_rows) != 1:
                if len(queue_rows) > 1:
                    log.error(
                        "manual close stale queue cardinality invalid client=%s "
                        "position=%s signal=%s rows=%s",
                        client_id,
                        position_id,
                        signal_id,
                        len(queue_rows),
                    )
                return 0
            queue_id = queue_rows[0].get("id")
            if queue_id in (None, ""):
                return 0

            cur = c.execute(
                """
                UPDATE trade_queue
                SET status='FILLED',
                    finished_ts=COALESCE(finished_ts, NOW()),
                    last_error='terminalized_after_manual_external_close_position_closed',
                    result_json=COALESCE(result_json, '{}'::jsonb)
                        || jsonb_build_object(
                            'manual_external_close_terminalized', TRUE,
                            'position_id', %s,
                            'broker_exit_order_id', %s,
                            'execution_mode', %s
                        )
                WHERE id=%s
                  AND client_id=%s
                  AND signal_id=%s
                  AND LOWER(COALESCE(payload->>'execution_mode',''))=%s
                  AND UPPER(COALESCE(status,'')) = ANY(%s)
                """,
                (
                    position_id,
                    broker_exit_order_id,
                    mode,
                    queue_id,
                    client_id,
                    signal_id,
                    mode,
                    list(_STALE_QUEUE_STATES),
                ),
            )
            return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

    try:
        updated = int(db.run_with_retry(_write) or 0)
        if updated:
            log.warning(
                "MANUAL_CLOSE_STALE_QUEUE_TERMINALIZED client=%s position=%s rows=%s",
                client_id, position_id, updated,
            )
        return updated
    except Exception as exc:
        log.error(
            "manual close stale queue terminalization failed client=%s position=%s error=%s",
            client_id, position_id, exc,
        )
        return 0


def _install_proof_stamp_patch() -> None:
    import ap.proof_taxonomy_guard as taxonomy

    original = getattr(taxonomy, "_lifecycle_proof_stamp", None)
    if not callable(original) or getattr(original, _PATCHED, False):
        return

    def wrapped(identity):
        stamp = original(identity)
        if identity is None:
            return stamp
        client_id = str(getattr(identity, "client_id", "") or "").strip().lower()
        position_id = str(getattr(identity, "position_id", "") or "").strip()
        return quarantine_manual_external_close_stamp(
            stamp,
            client_id=client_id,
            position_id=position_id,
        )

    setattr(wrapped, _PATCHED, True)
    setattr(taxonomy, "_lifecycle_proof_stamp", wrapped)


def _install_manual_finalizer_patch() -> None:
    import ap.manual_close_reconciliation as manual

    original = getattr(manual, "_finalize_position", None)
    if not callable(original) or getattr(original, _PATCHED, False):
        return

    def wrapped(*, finalizer, client_id, position_id, contract, evidence):
        ok = bool(
            original(
                finalizer=finalizer,
                client_id=client_id,
                position_id=position_id,
                contract=contract,
                evidence=evidence,
            )
        )
        if not ok:
            return False

        external = _external_exit_identity(
            client_id,
            position_id,
        )
        if not external:
            log.error(
                "manual close finalizer succeeded without one exact durable "
                "external EXIT identity client=%s position=%s — downstream "
                "mutations deferred",
                client_id,
                position_id,
            )
            return True
        external_local_order_id = str(external.get("local_order_id") or "").strip()
        broker_exit_order_id = str(external.get("broker_order_id") or "").strip()
        _persist_manual_close_proof_truth(
            client_id=client_id,
            position_id=position_id,
            external_local_order_id=external_local_order_id,
        )
        _terminalize_stale_queue_after_manual_close(
            client_id=client_id,
            position_id=position_id,
            broker_exit_order_id=broker_exit_order_id,
        )
        return True

    setattr(wrapped, _PATCHED, True)
    setattr(manual, "_finalize_position", wrapped)


def _recover_manual_close_downstream_truth(
    *,
    client_id: str,
    position_id: str,
    broker_exit_order_id: str,
    execution_mode: str,
) -> None:
    """Repair proof/queue downstream state from validated restart evidence."""
    external = _external_exit_identity(
        client_id,
        position_id,
        execution_mode=execution_mode,
    )
    expected_broker_id = str(broker_exit_order_id or "").strip()
    if (
        not external
        or str(external.get("broker_order_id") or "").strip() != expected_broker_id
    ):
        log.error(
            "manual close restart downstream identity changed client=%s "
            "position=%s expected_broker=%s",
            client_id,
            position_id,
            expected_broker_id,
        )
        return
    _persist_manual_close_proof_truth(
        client_id=client_id,
        position_id=position_id,
        external_local_order_id=str(external.get("local_order_id") or "").strip(),
    )
    _terminalize_stale_queue_after_manual_close(
        client_id=client_id,
        position_id=position_id,
        broker_exit_order_id=expected_broker_id,
    )


def install_manual_close_truth_guard() -> None:
    """Install both patches. Idempotent and safe to call repeatedly."""
    import ap.manual_close_reconciliation as manual

    _install_proof_stamp_patch()
    _install_manual_finalizer_patch()
    setattr(
        manual,
        "_recover_manual_close_downstream_truth",
        _recover_manual_close_downstream_truth,
    )
