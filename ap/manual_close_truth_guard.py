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

import logging
from typing import Any

from ap import db

log = logging.getLogger("ap.manual_close_truth_guard")

_PATCHED = "_AP_MANUAL_CLOSE_TRUTH_GUARD_PATCHED"
_EXTERNAL_PREFIX = "external-exit:"
_STALE_QUEUE_STATES = ("NEW", "PROCESSING", "WATCHING", "ARMED")
_MANUAL_TAXONOMY_REASON = (
    "tradier_exit_proof_lock_passed_manual_external_close_training_excluded"
)


def _external_exit_identity(client_id: str, position_id: str) -> dict | None:
    """Return the final durable external EXIT identity for one exact position."""
    client_id = str(client_id or "").strip().lower()
    position_id = str(position_id or "").strip()
    if not client_id or not position_id:
        return None

    def _read():
        with db.conn() as c:
            c.execute(
                """
                SELECT local_order_id, broker_order_id, filled_ts, filled_qty, fill_price
                FROM orders
                WHERE client_id=%s
                  AND position_id::text=%s
                  AND UPPER(COALESCE(kind,''))='EXIT'
                  AND UPPER(COALESCE(status,'')) IN ('EXIT_FILLED','EXIT_PARTIAL_FILL')
                  AND COALESCE(local_order_id,'') LIKE %s
                  AND COALESCE(broker_order_id,'') <> ''
                  AND COALESCE(filled_qty,0) > 0
                  AND fill_price IS NOT NULL
                ORDER BY filled_ts DESC NULLS LAST, updated_ts DESC NULLS LAST, id DESC
                LIMIT 1
                """,
                (client_id, position_id, f"{_EXTERNAL_PREFIX}%"),
            )
            row = c.fetchone()
            return dict(row) if row else None

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
    external = _external_exit_identity(client_id, position_id)
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
        or not external_local_order_id.startswith(_EXTERNAL_PREFIX)
    ):
        return 0

    def _write():
        with db.conn() as c:
            c.execute(
                """
                UPDATE proof_trades
                SET exit_local_order_id = CASE
                        WHEN COALESCE(exit_local_order_id,'') = '' THEN %s
                        ELSE exit_local_order_id
                    END,
                    training_eligible = FALSE,
                    taxonomy_reason = %s
                WHERE client_email=%s
                  AND position_id::text=%s
                  AND LOWER(COALESCE(execution_mode, mode, ''))='live'
                """,
                (
                    external_local_order_id,
                    _MANUAL_TAXONOMY_REASON,
                    client_id,
                    position_id,
                ),
            )
            return int(getattr(c, "rowcount", 0) or 0)

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
    if not client_id or not position_id:
        return 0

    def _write():
        with db.conn() as c:
            c.execute(
                """
                SELECT signal_id, execution_mode
                FROM positions
                WHERE client_id=%s AND id::text=%s
                LIMIT 1
                """,
                (client_id, position_id),
            )
            row = c.fetchone()
            if not row:
                return 0
            position = dict(row)
            signal_id = str(position.get("signal_id") or "").strip()
            mode = str(position.get("execution_mode") or "").strip().lower()
            if not signal_id or mode not in {"live", "paper"}:
                return 0

            c.execute(
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
                WHERE client_id=%s
                  AND signal_id=%s
                  AND UPPER(COALESCE(status,'')) = ANY(%s)
                """,
                (
                    position_id,
                    broker_exit_order_id,
                    mode,
                    client_id,
                    signal_id,
                    list(_STALE_QUEUE_STATES),
                ),
            )
            return int(getattr(c, "rowcount", 0) or 0)

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

        broker_exit_order_id = str(evidence.get("broker_order_id") or "").strip()
        external_local_order_id = (
            f"{_EXTERNAL_PREFIX}{str(client_id or '').strip().lower()}:{broker_exit_order_id}"
            if broker_exit_order_id
            else ""
        )
        if external_local_order_id:
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


def install_manual_close_truth_guard() -> None:
    """Install both patches. Idempotent and safe to call repeatedly."""
    _install_proof_stamp_patch()
    _install_manual_finalizer_patch()
