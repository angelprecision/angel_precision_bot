"""Originating-entry proof identity and mode-isolated performance history.

This guard repairs the proof writer at the source and prevents paper/unknown
rows from becoming LIVE learning input. It does not submit/cancel orders and it
does not alter signal admission. The only live-behavior effect is that Kelly
history is restricted to rows that passed the existing official Tradier proof
lock; insufficient history falls back to the existing tier sizing path.
"""
from __future__ import annotations

import inspect
import json
from dataclasses import dataclass
from typing import Any, Callable

from ap import db
from ap.logger import get_logger
from ap.operator.live_execution_journal import (
    PRICE_SOURCE_PAPER_BROKER,
    PRICE_SOURCE_TRADIER_ENTRY,
    PRICE_SOURCE_TRADIER_EXIT,
    classify_official,
)

log = get_logger("ap.proof_taxonomy_guard")

_PATCHED_ATTR = "_AP_PROOF_TAXONOMY_GUARD_PATCHED"
_VALID_MODES = {"live", "paper"}
_ENTRY_FILLED_STATUSES = {"FILLED", "PARTIAL_FILL"}
_EXIT_FILLED_STATUSES = {"EXIT_FILLED", "EXIT_PARTIAL_FILL"}


@dataclass(frozen=True)
class EntryIdentity:
    client_id: str
    position_id: str
    local_order_id: str
    broker_order_id: str
    execution_mode: str
    signal_id: str
    canonical_signal_id: str
    filled_qty: int
    fill_price: float
    filled_ts: Any
    synthetic_entry: bool


def _mode(value: Any) -> str:
    value = str(value or "").strip().lower()
    return value if value in _VALID_MODES else "unknown"


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _meta(row: dict) -> dict:
    value = row.get("meta") or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            value = {}
    return value if isinstance(value, dict) else {}


def _identity_from_order(client_id: str, position_id: str, row: dict) -> EntryIdentity:
    meta = _meta(row)
    status = str(row.get("status") or "").strip().upper()
    filled_qty = _int(row.get("filled_qty"))
    fill_price = _float(row.get("fill_price"))
    synthetic = bool(row.get("synthetic_entry") or meta.get("synthetic_entry"))
    if status not in _ENTRY_FILLED_STATUSES or filled_qty <= 0 or fill_price <= 0:
        filled_qty = max(0, filled_qty)
        fill_price = max(0.0, fill_price)
    return EntryIdentity(
        client_id=client_id,
        position_id=str(position_id or row.get("position_id") or "").strip(),
        local_order_id=str(row.get("local_order_id") or "").strip(),
        broker_order_id=str(row.get("broker_order_id") or "").strip(),
        execution_mode=_mode(row.get("execution_mode") or meta.get("execution_mode")),
        signal_id=str(row.get("signal_id") or meta.get("signal_id") or "").strip(),
        canonical_signal_id=str(
            row.get("canonical_signal_id") or meta.get("canonical_signal_id") or ""
        ).strip(),
        filled_qty=filled_qty,
        fill_price=fill_price,
        filled_ts=row.get("filled_ts"),
        synthetic_entry=synthetic,
    )


def resolve_originating_entry_identity(
    *, client_id: str, position_id: str = "", supplied_local_order_id: str = ""
) -> EntryIdentity | None:
    """Resolve one exact ENTRY order; never accept an EXIT row or another client."""
    client_id = str(client_id or "").strip()
    position_id = str(position_id or "").strip()
    supplied_local_order_id = str(supplied_local_order_id or "").strip()
    if not client_id:
        return None

    def _query() -> EntryIdentity | None:
        with db.conn() as c:
            position = None
            if position_id and not position_id.lower().startswith("broker-repair-"):
                row = c.execute(
                    "SELECT * FROM positions WHERE client_id=%s AND id::text=%s LIMIT 1",
                    (client_id, position_id),
                ).fetchone()
                position = dict(row) if row else None

            if supplied_local_order_id:
                row = c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND local_order_id=%s "
                    "AND kind='ENTRY' LIMIT 1",
                    (client_id, supplied_local_order_id),
                ).fetchone()
                if row:
                    candidate = dict(row)
                    candidate_position_id = str(candidate.get("position_id") or "").strip()
                    position_local_id = str((position or {}).get("local_order_id") or "").strip()
                    identity_matches_position = (
                        not position_id
                        or candidate_position_id == position_id
                        or position_local_id == supplied_local_order_id
                    )
                    if identity_matches_position:
                        return _identity_from_order(client_id, position_id, candidate)
                    log.error(
                        "supplied ENTRY identity rejected: position mismatch client=%s "
                        "proof_position=%s order_position=%s order=%s",
                        client_id,
                        position_id,
                        candidate_position_id,
                        supplied_local_order_id,
                    )

            if position:
                position_local_id = str(position.get("local_order_id") or "").strip()
                if position_local_id:
                    row = c.execute(
                        "SELECT * FROM orders WHERE client_id=%s AND local_order_id=%s "
                        "AND kind='ENTRY' LIMIT 1",
                        (client_id, position_local_id),
                    ).fetchone()
                    if row:
                        return _identity_from_order(client_id, position_id, dict(row))

                rows = c.execute(
                    "SELECT * FROM orders WHERE client_id=%s AND position_id::text=%s "
                    "AND kind='ENTRY' ORDER BY filled_ts DESC NULLS LAST LIMIT 2",
                    (client_id, position_id),
                ).fetchall()
                candidates = [dict(row) for row in rows]
                if len(candidates) == 1:
                    return _identity_from_order(client_id, position_id, candidates[0])

            return None

    try:
        return db.run_with_retry(_query)
    except Exception as exc:
        log.error(
            "originating ENTRY identity lookup failed client=%s position=%s supplied_order=%s error=%s",
            client_id,
            position_id,
            supplied_local_order_id,
            exc,
        )
        return None


def classify_performance_taxonomy(row: dict[str, Any]) -> dict[str, Any]:
    """Conservative taxonomy; only the existing official lock enables learning."""
    mode = _mode(row.get("execution_mode"))
    official = bool(row.get("official_live_performance_eligible"))
    if mode == "live" and official:
        return {
            "performance_taxonomy": "LIVE_OFFICIAL",
            "training_eligible": True,
            "taxonomy_reason": "tradier_exit_proof_lock_passed",
            "quote_domain_consistent": True,
        }
    if mode == "live":
        return {
            "performance_taxonomy": "LIVE_UNRECONCILED",
            "training_eligible": False,
            "taxonomy_reason": "live_row_missing_complete_broker_proof",
            "quote_domain_consistent": False,
        }
    if mode == "paper":
        return {
            "performance_taxonomy": "PAPER_UNVERIFIED",
            "training_eligible": False,
            "taxonomy_reason": "paper_execution_excluded_from_live_learning",
            "quote_domain_consistent": False,
        }
    return {
        "performance_taxonomy": "UNKNOWN_QUARANTINED",
        "training_eligible": False,
        "taxonomy_reason": "originating_entry_identity_or_execution_mode_unknown",
        "quote_domain_consistent": None,
    }


def _lifecycle_proof_stamp(identity: EntryIdentity | None) -> dict[str, Any]:
    if identity is None or not identity.position_id:
        base = {"execution_mode": "unknown", "official_live_performance_eligible": False}
        return {**base, **classify_performance_taxonomy(base)}

    def _query() -> dict[str, Any]:
        with db.conn() as c:
            position_row = c.execute(
                "SELECT * FROM positions WHERE client_id=%s AND id::text=%s LIMIT 1",
                (identity.client_id, identity.position_id),
            ).fetchone()
            position = dict(position_row) if position_row else {}
            exit_rows = c.execute(
                "SELECT local_order_id, broker_order_id, filled_qty, fill_price, filled_ts, status "
                "FROM orders WHERE client_id=%s AND position_id::text=%s AND kind='EXIT' "
                "AND status IN %s AND COALESCE(filled_qty,0)>0 AND fill_price IS NOT NULL "
                "ORDER BY filled_ts ASC",
                (identity.client_id, identity.position_id, tuple(_EXIT_FILLED_STATUSES)),
            ).fetchall()
            exits = [dict(row) for row in exit_rows]

            position_qty = _int(position.get("qty") or position.get("contracts"))
            exit_qty = sum(_int(row.get("filled_qty")) for row in exits)
            exit_cost_units = sum(
                _int(row.get("filled_qty")) * _float(row.get("fill_price")) for row in exits
            )
            weighted_exit = exit_cost_units / exit_qty if exit_qty > 0 else 0.0
            all_exit_ids = bool(exits) and all(
                str(row.get("broker_order_id") or "").strip() for row in exits
            )
            final_exit_id = str(exits[-1].get("broker_order_id") or "").strip() if exits else ""
            fully_closed = position_qty > 0 and exit_qty == position_qty
            mode = identity.execution_mode
            live_mode = mode == "live"
            proof_row = {
                "execution_mode": mode,
                "broker_reconciled": bool(
                    fully_closed
                    and identity.broker_order_id
                    and identity.filled_qty > 0
                    and identity.fill_price > 0
                    and all_exit_ids
                    and weighted_exit > 0
                ),
                "synthetic_entry": identity.synthetic_entry,
                "entry_price_source": (
                    PRICE_SOURCE_TRADIER_ENTRY if live_mode else PRICE_SOURCE_PAPER_BROKER
                ),
                "exit_price_source": (
                    PRICE_SOURCE_TRADIER_EXIT if live_mode else PRICE_SOURCE_PAPER_BROKER
                ),
                "broker_entry_order_id": identity.broker_order_id,
                "broker_exit_order_id": final_exit_id,
                "broker_entry_filled_qty": identity.filled_qty,
                "broker_exit_filled_qty": exit_qty,
                "entry_option_price": identity.fill_price,
                "exit_fill_price": weighted_exit,
            }
            official = bool(fully_closed and all_exit_ids and classify_official(proof_row).is_official)
            stamp = {
                "execution_mode": mode,
                "signal_id": identity.signal_id or None,
                "canonical_signal_id": identity.canonical_signal_id or None,
                "local_order_id": identity.local_order_id or None,
                "position_id": identity.position_id,
                "broker_entry_order_id": identity.broker_order_id or None,
                "broker_exit_order_id": final_exit_id or None,
                "broker_entry_fill_ts": identity.filled_ts,
                "broker_exit_fill_ts": exits[-1].get("filled_ts") if exits else None,
                "broker_entry_filled_qty": identity.filled_qty or None,
                "broker_exit_filled_qty": exit_qty or None,
                "entry_price_source": proof_row["entry_price_source"],
                "exit_price_source": proof_row["exit_price_source"],
                "exit_fill_price": round(weighted_exit, 6) if weighted_exit > 0 else None,
                "broker_reconciled": proof_row["broker_reconciled"],
                "official_live_performance_eligible": official,
            }
            return {**stamp, **classify_performance_taxonomy(stamp)}

    try:
        return db.run_with_retry(_query)
    except Exception as exc:
        log.error(
            "proof lifecycle stamp failed client=%s position=%s error=%s",
            identity.client_id,
            identity.position_id,
            exc,
        )
        base = {
            "execution_mode": identity.execution_mode,
            "official_live_performance_eligible": False,
        }
        return {**base, **classify_performance_taxonomy(base)}


def _persist_stamp(proof_logger: Any, result: dict, stamp: dict) -> None:
    if not result.get("_proof_persisted"):
        return
    client_id = str(getattr(proof_logger, "email", "") or "").strip()
    position_id = str(stamp.get("position_id") or result.get("position_id") or "").strip()
    local_order_id = str(stamp.get("local_order_id") or result.get("local_order_id") or "").strip()
    if not client_id or (not position_id and not local_order_id):
        log.critical(
            "[PROOF] taxonomy stamp skipped: no canonical client/position/ENTRY identity client=%s",
            client_id,
        )
        return

    def _update() -> int:
        with db.conn() as c:
            columns = {
                str(dict(row).get("column_name") or "")
                for row in c.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='public' AND table_name='proof_trades'"
                ).fetchall()
            }
            updates = {key: value for key, value in stamp.items() if key in columns}
            if not updates:
                return 0
            set_sql = ", ".join(f"{key}=%s" for key in updates)
            if position_id:
                cur = c.execute(
                    f"UPDATE proof_trades SET {set_sql} "
                    "WHERE client_email=%s AND position_id::text=%s",
                    tuple(updates.values()) + (client_id, position_id),
                )
            else:
                cur = c.execute(
                    f"UPDATE proof_trades SET {set_sql} "
                    "WHERE client_email=%s AND local_order_id=%s",
                    tuple(updates.values()) + (client_id, local_order_id),
                )
            return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

    try:
        updated = db.run_with_retry(_update)
        if not updated:
            log.critical(
                "[PROOF] taxonomy stamp matched zero rows client=%s position=%s entry_order=%s",
                client_id,
                position_id,
                local_order_id,
            )
    except Exception as exc:
        log.critical(
            "[PROOF] performance taxonomy stamp failed client=%s position=%s entry_order=%s error=%s "
            "run migrations/20260716_proof_performance_taxonomy.sql",
            client_id,
            position_id,
            local_order_id,
            exc,
        )


def wrap_log_trade(original: Callable[..., dict]) -> Callable[..., dict]:
    signature = inspect.signature(original)

    def guarded(self, *args, **kwargs) -> dict:
        bound = signature.bind(self, *args, **kwargs)
        bound.apply_defaults()
        supplied_local_id = str(bound.arguments.get("local_order_id") or "").strip()
        position_id = str(bound.arguments.get("position_id") or "").strip()
        identity = resolve_originating_entry_identity(
            client_id=str(getattr(self, "email", "") or ""),
            position_id=position_id,
            supplied_local_order_id=supplied_local_id,
        )

        if identity is not None:
            bound.arguments["local_order_id"] = identity.local_order_id
            bound.arguments["position_id"] = identity.position_id or position_id
            bound.arguments["execution_mode"] = identity.execution_mode
            bound.arguments["synthetic_entry"] = identity.synthetic_entry
        elif supplied_local_id or position_id:
            bound.arguments["local_order_id"] = ""
            bound.arguments["execution_mode"] = "unknown"

        result = original(*bound.args, **bound.kwargs)
        stamp = _lifecycle_proof_stamp(identity)
        result.update(stamp)
        _persist_stamp(self, result, stamp)
        return result

    return guarded


def wrap_master_control_init(original: Callable[..., Any]) -> Callable[..., Any]:
    def guarded(self, *args, **kwargs):
        original(self, *args, **kwargs)
        sizer = getattr(self, "sizer", None)
        if sizer is not None:
            sizer.execution_mode = _mode(getattr(self, "mode", None))

    return guarded


def wrap_fetch_history(original: Callable[..., list[dict]]) -> Callable[..., list[dict]]:
    """Only official LIVE proof history may drive Kelly; PAPER uses tier fallback."""
    def guarded(self, client_id: str) -> list[dict]:
        mode = _mode(getattr(self, "execution_mode", None))
        if mode != "live":
            log.info(
                "Kelly history excluded client=%s mode=%s; using tier fallback",
                client_id,
                mode,
            )
            return []

        def _query():
            with db.conn() as c:
                rows = c.execute(
                    "SELECT p.realized_pnl, p.avg_fill, p.exit_price, p.qty "
                    "FROM positions p WHERE p.client_id=%s "
                    "AND LOWER(COALESCE(p.execution_mode,''))='live' "
                    "AND p.status IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED') "
                    "AND p.realized_pnl IS NOT NULL "
                    "AND EXISTS (SELECT 1 FROM proof_trades pt "
                    "            WHERE pt.position_id::text=p.id::text "
                    "              AND pt.client_email=p.client_id "
                    "              AND pt.training_eligible IS TRUE) "
                    "ORDER BY p.entry_ts DESC LIMIT 100",
                    (client_id,),
                ).fetchall()
                return [dict(row) for row in rows]

        try:
            rows = db.run_with_retry(_query) or []
            return [
                {
                    "realized_pnl": row.get("realized_pnl"),
                    "avg_fill": row.get("avg_fill"),
                    "exit_price": row.get("exit_price"),
                    "qty": row.get("qty"),
                }
                for row in rows
            ]
        except Exception as exc:
            log.critical(
                "official LIVE Kelly history unavailable client=%s error=%s; tier fallback active. "
                "Run migrations/20260716_proof_performance_taxonomy.sql",
                client_id,
                exc,
            )
            return []

    return guarded


def install_proof_taxonomy_guard() -> None:
    import ap_proof_logger
    import ap_master_control
    from ap import position_sizer

    if getattr(ap_proof_logger, _PATCHED_ATTR, False):
        return

    ap_proof_logger.APProofLogger.log_trade = wrap_log_trade(
        ap_proof_logger.APProofLogger.log_trade
    )
    ap_master_control.APMasterControl.__init__ = wrap_master_control_init(
        ap_master_control.APMasterControl.__init__
    )
    position_sizer.APPositionSizer._fetch_history = wrap_fetch_history(
        position_sizer.APPositionSizer._fetch_history
    )
    setattr(ap_proof_logger, _PATCHED_ATTR, True)
