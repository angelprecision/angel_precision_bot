"""Canonical broker-fill reconciliation for EXIT orders.

This guard replaces the legacy proof-only EXIT sync with one transactionally
consistent projection from broker-confirmed EXIT order fills. It never submits
or cancels orders; it runs only after OSM has accepted an EXIT fill.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Iterable

from ap.db import conn, run_with_retry
from ap.logger import get_logger
from ap.operator.live_execution_journal import (
    PRICE_SOURCE_PAPER_BROKER,
    PRICE_SOURCE_TRADIER_ENTRY,
    PRICE_SOURCE_TRADIER_EXIT,
    classify_official,
)

log = get_logger("ap.exit_fill_truth_guard")

_PATCHED_ATTR = "_AP_CANONICAL_EXIT_FILL_TRUTH_PATCHED"
_ORIGINAL_ATTR = "_AP_CANONICAL_EXIT_FILL_TRUTH_ORIGINAL"
_TERMINAL_POSITION_STATUSES = {
    "CLOSED", "CLOSED_REPAIR", "EXPIRED", "STOPPED", "TAKEN_PROFIT",
    "ERROR", "CANCELED", "CANCELLED",
}
_EXIT_FILL_STATUSES = ("EXIT_FILLED", "EXIT_PARTIAL_FILL")


class LifecycleProjectionError(ValueError):
    """Broker fill rows cannot describe exactly one valid position lifecycle."""


@dataclass(frozen=True)
class LifecycleProjection:
    exited_qty: int
    remaining_qty: int
    weighted_exit_price: float
    realized_pnl: float
    realized_pnl_pct: float
    final_fill_ts: Any
    closed: bool


def _float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _int(value: Any, default: int = 0) -> int:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def project_position_from_exit_fills(position: dict, fills: Iterable[dict]) -> LifecycleProjection:
    """Pure cumulative fill reducer using one row per EXIT order."""
    qty = _int(position.get("qty") or position.get("contracts"))
    entry_price = _float(position.get("avg_fill") or position.get("entry_price"))
    if qty <= 0:
        raise LifecycleProjectionError("position_qty_missing_or_invalid")
    if entry_price <= 0:
        raise LifecycleProjectionError("position_entry_price_missing_or_invalid")

    normalized: list[tuple[Any, int, float]] = []
    for row in fills:
        fill_qty = _int(row.get("filled_qty"))
        fill_price = _float(row.get("fill_price"))
        if fill_qty > 0 and fill_price > 0:
            normalized.append((row.get("filled_ts"), fill_qty, fill_price))
    normalized.sort(key=lambda item: str(item[0] or ""))

    exited_qty = sum(item[1] for item in normalized)
    if exited_qty <= 0:
        raise LifecycleProjectionError("no_positive_exit_fills")
    if exited_qty > qty:
        raise LifecycleProjectionError(f"exit_overfill:{exited_qty}>{qty}")

    proceeds = sum(fill_qty * fill_price * 100.0 for _, fill_qty, fill_price in normalized)
    cost = entry_price * exited_qty * 100.0
    realized_pnl = proceeds - cost
    remaining_qty = qty - exited_qty
    return LifecycleProjection(
        exited_qty=exited_qty,
        remaining_qty=remaining_qty,
        weighted_exit_price=round(proceeds / (exited_qty * 100.0), 6),
        realized_pnl=round(realized_pnl, 2),
        realized_pnl_pct=round((realized_pnl / cost * 100.0) if cost > 0 else 0.0, 4),
        final_fill_ts=normalized[-1][0],
        closed=remaining_qty == 0,
    )


def official_live_eligibility(
    *,
    execution_mode: str,
    closed: bool,
    entry_broker_order_id: str,
    exit_broker_order_id: str,
    all_exit_fills_broker_backed: bool,
    entry_filled_qty: int = 1,
    exit_filled_qty: int = 1,
    entry_fill_price: float = 1.0,
    exit_fill_price: float = 1.0,
    synthetic_entry: bool = False,
) -> bool:
    """Apply the repository's existing Tradier Exit Proof Lock exactly."""
    if not closed or not all_exit_fills_broker_backed:
        return False
    mode = str(execution_mode or "").strip().lower()
    row = {
        "execution_mode": mode,
        "broker_reconciled": bool(entry_broker_order_id and exit_broker_order_id),
        "synthetic_entry": bool(synthetic_entry),
        "entry_price_source": (
            PRICE_SOURCE_TRADIER_ENTRY if mode == "live" else PRICE_SOURCE_PAPER_BROKER
        ),
        "exit_price_source": (
            PRICE_SOURCE_TRADIER_EXIT if mode == "live" else PRICE_SOURCE_PAPER_BROKER
        ),
        "broker_entry_order_id": str(entry_broker_order_id or ""),
        "broker_exit_order_id": str(exit_broker_order_id or ""),
        "broker_entry_filled_qty": entry_filled_qty,
        "broker_exit_filled_qty": exit_filled_qty,
        "entry_option_price": entry_fill_price,
        "exit_fill_price": exit_fill_price,
    }
    return classify_official(row).is_official


def _table_columns(c, table_name: str) -> set[str]:
    rows = c.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema='public' AND table_name=%s",
        (table_name,),
    ).fetchall()
    return {str(dict(row).get("column_name") or "") for row in rows}


def _synthetic_position_id(value: Any) -> bool:
    text = str(value or "").strip().lower()
    return not text or text.startswith("broker-repair-")


def _position_entry_key(row: dict) -> str:
    return str(row.get("entry_ts") or row.get("created_at") or "")


def _resolve_position(c, order: dict, fill_ts: Any) -> dict | None:
    client_id = str(order.get("client_id") or "").strip()
    contract = str(order.get("contract") or order.get("symbol") or "").strip().upper()
    requested_id = str(order.get("position_id") or "").strip()
    if not client_id or not contract:
        return None

    if requested_id and not _synthetic_position_id(requested_id):
        row = c.execute(
            "SELECT * FROM positions WHERE client_id=%s AND id::text=%s LIMIT 1",
            (client_id, requested_id),
        ).fetchone()
        if row:
            resolved = dict(row)
            if str(resolved.get("contract") or "").strip().upper() == contract:
                return resolved

    rows = c.execute(
        "SELECT * FROM positions WHERE client_id=%s AND UPPER(contract)=UPPER(%s) "
        "AND COALESCE(entry_ts, created_at, NOW()) <= COALESCE(%s, NOW()) "
        "ORDER BY COALESCE(entry_ts, created_at) DESC LIMIT 2",
        (client_id, contract, fill_ts),
    ).fetchall()
    candidates = [dict(row) for row in rows]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]

    active = [
        row for row in candidates
        if str(row.get("status") or "").upper() not in _TERMINAL_POSITION_STATUSES
        and _int(row.get("quantity_remaining"), _int(row.get("qty"))) > 0
    ]
    if len(active) == 1:
        return active[0]
    if _position_entry_key(candidates[0]) and _position_entry_key(candidates[0]) != _position_entry_key(candidates[1]):
        return candidates[0]
    return None


def _load_exit_fills(c, position: dict, order: dict) -> list[dict]:
    client_id = str(order.get("client_id") or "").strip()
    contract = str(position.get("contract") or order.get("contract") or "").strip().upper()
    position_id = str(position.get("id") or "").strip()
    entry_ts = position.get("entry_ts") or position.get("created_at")
    rows = c.execute(
        "SELECT local_order_id, broker_order_id, position_id, filled_qty, fill_price, filled_ts, status "
        "FROM orders WHERE client_id=%s AND kind='EXIT' AND UPPER(contract)=UPPER(%s) "
        "AND status IN %s AND COALESCE(filled_qty,0)>0 AND fill_price IS NOT NULL "
        "AND (%s IS NULL OR filled_ts >= %s) "
        "AND (position_id::text=%s OR position_id IS NULL OR position_id::text='' "
        "OR position_id::text LIKE 'broker-repair-%%') "
        "ORDER BY filled_ts ASC, created_ts ASC",
        (client_id, contract, _EXIT_FILL_STATUSES, entry_ts, entry_ts, position_id),
    ).fetchall()
    return [dict(row) for row in rows]


def _load_entry_order(c, position: dict, order: dict) -> dict:
    client_id = str(order.get("client_id") or "").strip()
    position_id = str(position.get("id") or "").strip()
    local_order_id = str(position.get("local_order_id") or "").strip()
    if local_order_id:
        row = c.execute(
            "SELECT * FROM orders WHERE client_id=%s AND local_order_id=%s AND kind='ENTRY' LIMIT 1",
            (client_id, local_order_id),
        ).fetchone()
        if row:
            return dict(row)

    row = c.execute(
        "SELECT * FROM orders WHERE client_id=%s AND kind='ENTRY' AND position_id::text=%s "
        "ORDER BY filled_ts DESC NULLS LAST LIMIT 1",
        (client_id, position_id),
    ).fetchone()
    if row:
        return dict(row)

    entry_ts = position.get("entry_ts") or position.get("created_at")
    contract = str(position.get("contract") or order.get("contract") or "").strip().upper()
    if not entry_ts or not contract:
        return {}
    rows = c.execute(
        "SELECT * FROM orders WHERE client_id=%s AND kind='ENTRY' AND status='FILLED' "
        "AND UPPER(contract)=UPPER(%s) "
        "AND filled_ts BETWEEN %s - INTERVAL '10 minutes' AND %s + INTERVAL '10 minutes' "
        "ORDER BY ABS(EXTRACT(EPOCH FROM (filled_ts - %s))) ASC LIMIT 2",
        (client_id, contract, entry_ts, entry_ts, entry_ts),
    ).fetchall()
    candidates = [dict(candidate) for candidate in rows]
    return candidates[0] if len(candidates) == 1 else {}


def _dynamic_update(c, table: str, updates: dict[str, Any], where_sql: str, params: tuple[Any, ...]) -> int:
    if not updates:
        return 0
    set_sql = ", ".join(f"{column}=%s" for column in updates)
    cur = c.execute(
        f"UPDATE {table} SET {set_sql} WHERE {where_sql}",
        tuple(updates.values()) + tuple(params),
    )
    return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)


def _reconcile_exit_fill(order: dict, result: dict) -> dict:
    client_id = str(order.get("client_id") or "").strip()
    fill_ts = result.get("filled_ts") or order.get("filled_ts") or datetime.now(timezone.utc)

    def _tx() -> dict:
        with conn() as c:
            position = _resolve_position(c, order, fill_ts)
            if not position:
                raise LifecycleProjectionError("canonical_position_unresolved_or_ambiguous")
            position_id = str(position.get("id") or "").strip()
            if not position_id:
                raise LifecycleProjectionError("canonical_position_id_missing")

            fills = _load_exit_fills(c, position, order)
            projection = project_position_from_exit_fills(position, fills)
            entry_order = _load_entry_order(c, position, order)
            entry_broker_id = str(entry_order.get("broker_order_id") or position.get("broker_order_id") or "").strip()
            exit_ids = [str(row.get("broker_order_id") or "").strip() for row in fills]
            all_exit_broker_backed = bool(fills) and all(exit_ids)
            exit_ids = [value for value in exit_ids if value]
            final_exit_broker_id = exit_ids[-1] if exit_ids else str(order.get("broker_order_id") or "").strip()
            mode = str(
                position.get("execution_mode") or entry_order.get("execution_mode")
                or order.get("execution_mode") or "unknown"
            ).strip().lower()
            signal_id = str(
                position.get("signal_id") or entry_order.get("signal_id")
                or order.get("signal_id") or ""
            ).strip()
            canonical_signal_id = str(
                entry_order.get("canonical_signal_id") or order.get("canonical_signal_id") or ""
            ).strip()
            entry_fill_price = _float(entry_order.get("fill_price") or position.get("avg_fill") or position.get("entry_price"))
            entry_filled_qty = _int(entry_order.get("filled_qty") or position.get("qty"))
            synthetic_entry = bool(position.get("synthetic_entry") or entry_order.get("synthetic_entry"))

            position_columns = _table_columns(c, "positions")
            position_updates = {
                key: value
                for key, value in {
                    "contracts_exited": projection.exited_qty,
                    "quantity_remaining": projection.remaining_qty,
                    "exit_price": projection.weighted_exit_price,
                    "realized_pnl": projection.realized_pnl,
                    "realized_pnl_pct": projection.realized_pnl_pct,
                    "exit_in_flight": False,
                    "pending_exit_action": None,
                    "pending_exit_reason": None,
                    "pending_exit_qty": None,
                    "pending_exit_local_order_id": None,
                    "pending_exit_broker_order_id": None,
                    "updated_at": datetime.now(timezone.utc),
                    **({"status": "CLOSED", "exit_ts": projection.final_fill_ts or fill_ts} if projection.closed else {}),
                }.items()
                if key in position_columns
            }
            _dynamic_update(c, "positions", position_updates, "client_id=%s AND id::text=%s", (client_id, position_id))

            local_ids = [str(row.get("local_order_id") or "").strip() for row in fills]
            local_ids = [value for value in local_ids if value]
            if local_ids:
                c.execute(
                    "UPDATE orders SET position_id=%s, updated_ts=NOW() WHERE client_id=%s "
                    "AND local_order_id IN %s AND (position_id IS NULL OR position_id::text='' "
                    "OR position_id::text=%s OR position_id::text LIKE 'broker-repair-%%')",
                    (position_id, client_id, tuple(local_ids), position_id),
                )
            if entry_order.get("local_order_id"):
                c.execute(
                    "UPDATE orders SET position_id=%s, updated_ts=NOW() WHERE client_id=%s "
                    "AND local_order_id=%s AND (position_id IS NULL OR position_id::text='')",
                    (position_id, client_id, entry_order.get("local_order_id")),
                )

            proof_updated = 0
            eligible = False
            if projection.closed:
                proof_columns = _table_columns(c, "proof_trades")
                eligible = official_live_eligibility(
                    execution_mode=mode,
                    closed=True,
                    entry_broker_order_id=entry_broker_id,
                    exit_broker_order_id=final_exit_broker_id,
                    all_exit_fills_broker_backed=all_exit_broker_backed,
                    entry_filled_qty=entry_filled_qty,
                    exit_filled_qty=projection.exited_qty,
                    entry_fill_price=entry_fill_price,
                    exit_fill_price=projection.weighted_exit_price,
                    synthetic_entry=synthetic_entry,
                )
                live_mode = mode == "live"
                proof_updates = {
                    key: value
                    for key, value in {
                        "position_id": position_id,
                        "execution_mode": mode if mode in {"live", "paper"} else "unknown",
                        "signal_id": signal_id or None,
                        "canonical_signal_id": canonical_signal_id or None,
                        "broker_entry_order_id": entry_broker_id or None,
                        "broker_exit_order_id": final_exit_broker_id or None,
                        "broker_entry_fill_ts": entry_order.get("filled_ts"),
                        "broker_exit_fill_ts": projection.final_fill_ts,
                        "broker_entry_filled_qty": entry_filled_qty or None,
                        "broker_exit_filled_qty": projection.exited_qty,
                        "entry_price_source": (
                            PRICE_SOURCE_TRADIER_ENTRY if live_mode else PRICE_SOURCE_PAPER_BROKER
                        ),
                        "exit_price_source": (
                            PRICE_SOURCE_TRADIER_EXIT if live_mode else PRICE_SOURCE_PAPER_BROKER
                        ),
                        "exit_option_price": projection.weighted_exit_price,
                        "exit_fill_price": projection.weighted_exit_price,
                        "option_pnl_pct": projection.realized_pnl_pct,
                        "win": projection.realized_pnl > 0,
                        "broker_reconciled": bool(
                            entry_broker_id and all_exit_broker_backed
                            and entry_filled_qty > 0 and entry_fill_price > 0
                        ),
                        "official_live_performance_eligible": eligible,
                    }.items()
                    if key in proof_columns
                }
                entry_local_id = str(entry_order.get("local_order_id") or "")
                where_sql = "(position_id::text=%s OR (%s<>'' AND local_order_id=%s))"
                where_params: tuple[Any, ...] = (position_id, entry_local_id, entry_local_id)
                if "client_email" in proof_columns:
                    where_sql = "client_email=%s AND " + where_sql
                    where_params = (client_id,) + where_params
                proof_updated = _dynamic_update(c, "proof_trades", proof_updates, where_sql, where_params)

            return {
                "position_id": position_id,
                "projection": projection,
                "execution_mode": mode,
                "proof_rows_updated": proof_updated,
                "official_live_performance_eligible": eligible,
            }

    return run_with_retry(_tx)


def install_exit_fill_truth_guard() -> None:
    from ap import fill_monitor

    if getattr(fill_monitor, _PATCHED_ATTR, False):
        return
    setattr(fill_monitor, _ORIGINAL_ATTR, fill_monitor._sync_exit_price)

    def guarded_sync_exit_price(order: dict, result: dict):
        try:
            reconciled = _reconcile_exit_fill(order, result)
            projection = reconciled["projection"]
            log.info(
                "[%s] CANONICAL_EXIT_FILL_RECONCILED position=%s exited=%s remaining=%s "
                "exit_price=%.4f realized_pnl=%.2f proof_rows=%s official_live=%s",
                order.get("client_id"), reconciled.get("position_id"),
                projection.exited_qty, projection.remaining_qty,
                projection.weighted_exit_price, projection.realized_pnl,
                reconciled.get("proof_rows_updated"),
                reconciled.get("official_live_performance_eligible"),
            )
            try:
                from ap.exit_price_sync import sync_exit_price_to_dashboard
                sync_exit_price_to_dashboard(
                    position_id=str(reconciled.get("position_id") or ""),
                    exit_avg_fill=projection.weighted_exit_price,
                    entry_price=None,
                    ticker=str(order.get("symbol") or "").upper(),
                )
            except Exception:
                pass
            return reconciled
        except Exception as exc:
            log.critical(
                "[%s] CANONICAL_EXIT_FILL_RECONCILE_FAILED order=%s broker=%s position=%s contract=%s error=%s",
                order.get("client_id"), order.get("local_order_id"),
                order.get("broker_order_id"), order.get("position_id"),
                order.get("contract") or order.get("symbol"), exc,
                exc_info=True,
            )
            return None

    fill_monitor._sync_exit_price = guarded_sync_exit_price
    setattr(fill_monitor, _PATCHED_ATTR, True)
