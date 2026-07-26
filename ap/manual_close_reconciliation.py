"""Broker-truth reconciliation for externally closed client positions.

Delegated to from ``ClientRunner._detect_manual_closes``. Never submits or
cancels broker orders. Adopts already-filled external Tradier EXIT orders
into the durable order ledger as EXIT_FILLED rows, then finalizes the
position via ``APPositionManager.close_position_from_exit_fill`` with
weighted-aggregate broker truth.

Post-review amendments (PR #386 hardening):

  1. Multi-fill adoption is now atomic per position — every fill for a
     given position is INSERTed inside ONE connection + advisory lock. If
     any fill fails, the transaction rolls back and NO evidence is left
     partially adopted. That eliminates the previous stranding path where
     fill A committed, fill B failed, and the next scan short-circuited on
     A appearing in known_exit_order_ids.

  2. The scanner distinguishes bot-submitted EXIT ids (a legitimate
     ownership fence) from externally-adopted EXIT ids (our own past work
     that must not fence us out of resuming/finalizing the SAME position).

  3. When previously-adopted external EXIT rows exist for a still-active
     position, the scanner reconstitutes the weighted aggregate directly
     from those durable rows and re-invokes the finalizer — closes the
     "adopted but finalizer failed" recovery gap without relying on the
     row-at-a-time reconciler (which overwrites position-level P&L).

  4. Position scan expanded from status='OPEN' only to the canonical
     active family {OPEN, CLOSING}. A CLOSING position with retained
     external evidence is no longer stranded.

  5. Broker orders fetched via paginated /v1/accounts/{id}/orders with an
     explicit high limit. Falls back to broker.list_orders() with a
     WARNING when pagination is unavailable, since that path is capped at
     ~25 by Tradier defaults.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
import time
from datetime import datetime, timezone
from typing import Any

log = logging.getLogger("client_runner.manual_close")

MANUAL_CLOSE_INTERVAL_SEC = float(
    os.getenv("MANUAL_CLOSE_DETECT_INTERVAL_SEC", "120")
)
MANUAL_CLOSE_FUTURE_SKEW_SEC = float(
    os.getenv("MANUAL_CLOSE_ORDER_FUTURE_SKEW_SEC", "300")
)
MANUAL_CLOSE_ORDERS_PAGE_LIMIT = int(
    os.getenv("MANUAL_CLOSE_ORDERS_PAGE_LIMIT", "500")
)
VALID_EXECUTION_MODES = frozenset({"paper", "live"})
MANUAL_CLOSE_SIDES = frozenset(
    {"sell_to_close", "sell-to-close", "selltoclose", "stc"}
)
FILLED_ORDER_STATUSES = frozenset(
    {"filled", "partially_filled", "partial_fill", "partially-filled"}
)
DURABLE_EXIT_FILLED_STATUSES = frozenset({"EXIT_FILLED", "EXIT_PARTIAL_FILL"})
ACTIVE_POSITION_STATUSES = ("OPEN", "CLOSING", "PARTIAL", "ACTIVE")
EXTERNAL_LOCAL_ID_PREFIX = "external-exit:"
OCC_RE = re.compile(r"^[A-Z0-9.]{1,6}\d{6}[CP]\d{8}$")


def normalize_contract(value: Any) -> str:
    return str(value or "").upper().replace(" ", "").strip()


def position_direction(position: dict) -> str:
    return str(
        position.get("side") or position.get("direction") or ""
    ).upper().strip()


def positive_float(value: Any) -> float:
    try:
        parsed = float(value)
    except Exception:
        return 0.0
    if not math.isfinite(parsed) or parsed <= 0:
        return 0.0
    return parsed


def positive_int(value: Any) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        return 0
    return parsed if parsed > 0 else 0


def parse_timestamp(value: Any) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000.0
        try:
            parsed = datetime.fromtimestamp(raw, tz=timezone.utc)
        except Exception:
            return None
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except Exception:
            parsed = None
            for fmt in (
                "%Y-%m-%dT%H:%M:%S.%f%z",
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%d %H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%S.%f",
                "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%d %H:%M:%S",
            ):
                try:
                    parsed = datetime.strptime(text, fmt)
                    break
                except Exception:
                    continue
            if parsed is None:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def normalize_positions_payload(payload: Any) -> list[dict]:
    if not isinstance(payload, dict) or "positions" not in payload:
        raise ValueError("BROKER_POSITIONS_PAYLOAD_MALFORMED")

    positions_node = payload.get("positions")
    if positions_node is None or positions_node == "null":
        return []
    if not isinstance(positions_node, dict):
        raise ValueError("BROKER_POSITIONS_NODE_MALFORMED")

    rows = positions_node.get("position")
    if rows is None or rows == "null":
        return []
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        raise ValueError("BROKER_POSITION_ROWS_MALFORMED")

    normalized: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        contract = normalize_contract(
            row.get("option_symbol") or row.get("contract") or row.get("symbol")
        )
        quantity = positive_int(row.get("quantity") or row.get("qty"))
        if contract and quantity > 0:
            normalized.append(
                {"symbol": contract, "quantity": quantity, "raw": dict(row)}
            )
    return normalized


def fetch_authoritative_broker_positions(broker: Any) -> list[dict]:
    authoritative = getattr(broker, "list_positions_authoritative", None)
    if callable(authoritative):
        rows = authoritative()
        if not isinstance(rows, list):
            raise ValueError("AUTHORITATIVE_POSITIONS_RESULT_MALFORMED")
        return [dict(row) for row in rows if isinstance(row, dict)]

    raw_get = getattr(broker, "_get", None)
    cfg = getattr(broker, "cfg", None)
    account_id = str(getattr(cfg, "account_id", "") or "").strip()
    if not callable(raw_get) or not account_id:
        raise RuntimeError("AUTHORITATIVE_BROKER_POSITIONS_UNAVAILABLE")

    payload = raw_get(f"/v1/accounts/{account_id}/positions")
    return normalize_positions_payload(payload)


def _normalize_orders_page(payload: Any) -> list[dict]:
    """Tradier orders payload → list of dict orders. Empty on empty node.
    Raises on structurally malformed payloads (never silently drop truth)."""
    if payload is None or payload == "null":
        return []
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if not isinstance(payload, dict):
        raise ValueError("BROKER_ORDERS_PAGE_MALFORMED")
    node = payload.get("orders")
    if node is None or node == "null":
        return []
    if isinstance(node, list):
        return [dict(row) for row in node if isinstance(row, dict)]
    if not isinstance(node, dict):
        raise ValueError("BROKER_ORDERS_NODE_MALFORMED")
    rows = node.get("order")
    if rows is None or rows == "null":
        return []
    if isinstance(rows, dict):
        return [dict(rows)]
    if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, dict)]
    raise ValueError("BROKER_ORDERS_ROWS_MALFORMED")


def fetch_all_current_session_orders(broker: Any) -> list[dict]:
    """Fetch every current-session order via paginated broker call.

    Tradier's /v1/accounts/{id}/orders defaults to ~25 rows without an
    explicit limit and returns only the current market session. This
    helper requests an explicit high limit and paginates until an empty
    page. If the broker exposes no ``_get`` / ``cfg.account_id``, falls
    back to ``list_orders()`` with a WARNING logged so the caller and
    operators know the 25-cap risk is in play.
    """
    raw_get = getattr(broker, "_get", None)
    cfg = getattr(broker, "cfg", None)
    account_id = str(getattr(cfg, "account_id", "") or "").strip()

    if not callable(raw_get) or not account_id:
        list_orders = getattr(broker, "list_orders", None)
        if not callable(list_orders):
            raise RuntimeError("BROKER_ORDERS_UNAVAILABLE")
        log.warning(
            "MANUAL_CLOSE_ORDERS_PAGINATION_UNAVAILABLE using list_orders "
            "(subject to ~25-row default cap on Tradier)"
        )
        return normalize_broker_orders(list_orders())

    all_rows: list[dict] = []
    seen_ids: set[str] = set()
    page = 1
    limit = MANUAL_CLOSE_ORDERS_PAGE_LIMIT
    while True:
        payload = raw_get(
            f"/v1/accounts/{account_id}/orders?includeTags=true"
            f"&page={page}&limit={limit}"
        )
        rows = _normalize_orders_page(payload)
        if not rows:
            break
        added = 0
        for row in rows:
            oid = str(row.get("id") or row.get("order_id") or "").strip()
            if oid and oid in seen_ids:
                continue
            if oid:
                seen_ids.add(oid)
            all_rows.append(row)
            added += 1
        # Terminate when page is short (last page) or when nothing new was
        # added (defensive; broker occasionally returns overlapping pages).
        if added == 0 or len(rows) < limit:
            break
        page += 1
        if page > 50:  # hard ceiling — 50 * 500 = 25k orders
            log.warning(
                "MANUAL_CLOSE_ORDERS_PAGE_HARD_STOP page=%s total=%s",
                page, len(all_rows),
            )
            break
    return all_rows


def order_legs(order: dict) -> list[dict]:
    legs = order.get("leg") or order.get("legs")
    if isinstance(legs, dict):
        nested = legs.get("leg") if "leg" in legs else legs
        if isinstance(nested, dict):
            return [nested]
        if isinstance(nested, list):
            return [item for item in nested if isinstance(item, dict)]
        return []
    if isinstance(legs, list):
        return [item for item in legs if isinstance(item, dict)]
    return []


def order_contract(order: dict) -> str:
    for key in ("option_symbol", "contract"):
        contract = normalize_contract(order.get(key))
        if contract:
            return contract

    for leg in order_legs(order):
        for key in ("option_symbol", "contract", "symbol"):
            contract = normalize_contract(leg.get(key))
            if contract and OCC_RE.fullmatch(contract):
                return contract

    symbol = normalize_contract(order.get("symbol"))
    return symbol if OCC_RE.fullmatch(symbol) else ""


def order_side(order: dict) -> str:
    raw = str(order.get("side") or "").lower().replace(" ", "_").strip()
    if raw:
        return raw
    legs = order_legs(order)
    if len(legs) == 1:
        return str(legs[0].get("side") or "").lower().replace(" ", "_").strip()
    return ""


def order_status(order: dict) -> str:
    return str(order.get("status") or "").lower().strip()


def order_id(order: dict) -> str:
    for key in ("id", "order_id", "broker_order_id"):
        value = str(order.get(key) or "").strip()
        if value:
            return value
    return ""


def order_filled_qty(order: dict) -> int:
    for key in (
        "exec_quantity",
        "filled_quantity",
        "filled_qty",
        "executed_quantity",
    ):
        qty = positive_int(order.get(key))
        if qty > 0:
            return qty
    if order_status(order) == "filled":
        return positive_int(order.get("quantity") or order.get("qty"))
    return 0


def order_fill_price(order: dict) -> float:
    for key in (
        "avg_fill_price",
        "average_fill_price",
        "fill_price",
        "last_fill_price",
    ):
        price = positive_float(order.get(key))
        if price > 0:
            return price
    return 0.0


def order_created_at(order: dict) -> datetime | None:
    for key in ("create_date", "created_at", "submitted_at"):
        parsed = parse_timestamp(order.get(key))
        if parsed is not None:
            return parsed
    return None


def order_filled_at(order: dict) -> datetime | None:
    for key in (
        "last_fill_date",
        "filled_at",
        "filled_ts",
        "transaction_date",
        "update_date",
        "updated_at",
    ):
        parsed = parse_timestamp(order.get(key))
        if parsed is not None:
            return parsed
    return None


def normalize_broker_orders(raw_orders: Any) -> list[dict]:
    if raw_orders is None:
        return []
    if isinstance(raw_orders, dict):
        raw_orders = [raw_orders]
    if not isinstance(raw_orders, list):
        raise ValueError("BROKER_ORDERS_RESULT_MALFORMED")
    return [dict(order) for order in raw_orders if isinstance(order, dict)]


def _normalize_fill(order: dict) -> dict | None:
    broker_order_id = order_id(order)
    filled_qty = order_filled_qty(order)
    fill_price = order_fill_price(order)
    filled_at = order_filled_at(order)
    if not broker_order_id or filled_qty <= 0 or fill_price <= 0 or filled_at is None:
        return None
    return {
        "broker_order_id": broker_order_id,
        "filled_qty": filled_qty,
        "fill_price": fill_price,
        "filled_at": filled_at,
        "created_at": order_created_at(order),
        "raw_status": order_status(order),
        "raw_side": order_side(order),
    }



def _validate_durable_fills(
    fills: list[dict],
    *,
    position: dict,
    detected_at: datetime,
    client_id: str = "",
) -> list[dict]:
    """Re-prove durable adopted fills against the current position.

    PR #386 hardening: identity is MANDATORY. A durable row that merely
    shares a position_id but lacks any of the identity/economics below is
    corrupt evidence and cannot silently inflate the weighted-close
    aggregate. Every field must be present and exact:

      * broker_order_id: nonempty
      * filled_qty > 0
      * fill_price > 0
      * filled_at is a datetime instance (not None, not string, not epoch)
      * filled_at >= position entry timestamp (when entry is known)
      * db_status: nonempty AND in {EXIT_FILLED, EXIT_PARTIAL_FILL}
      * db_contract: nonempty AND exactly equals position contract
      * db_direction: nonempty AND in {CALL, PUT} AND equals position direction
      * pos_direction: valid CALL/PUT — reject the whole row if the position's
        own direction is unknown, since the direction check cannot then be made

    Rejected rows are logged loudly and dropped; no partial-credit acceptance.
    """
    pos_contract = normalize_contract(position.get("contract"))
    pos_direction = str(
        position.get("side") or position.get("direction") or ""
    ).upper().strip()
    opened_at = parse_timestamp(
        position.get("entry_ts") or position.get("opened_at")
    )
    position_id = str(position.get("id") or "").strip()

    # Position direction validity is a precondition — if the position row
    # itself lacks a valid CALL/PUT direction we cannot prove alignment for
    # any fill.  Reject the entire durable set for this position.
    if pos_direction not in {"CALL", "PUT"}:
        for f in fills:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_POSITION_DIRECTION_INVALID "
                "pos=%s broker_id=%s pos_direction=%r — rejected",
                client_id, position_id,
                str(f.get("broker_order_id") or "").strip(), pos_direction,
            )
        return []

    valid: list[dict] = []
    for f in fills:
        bid = str(f.get("broker_order_id") or "").strip()
        db_contract = str(f.get("db_contract") or "").upper().strip()
        db_direction = str(f.get("db_direction") or "").upper().strip()
        db_status = str(f.get("raw_status") or "").upper().strip()
        filled_qty = positive_int(f.get("filled_qty"))
        fill_price = positive_float(f.get("fill_price"))
        filled_at = f.get("filled_at")

        if not bid:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_NO_ID pos=%s — rejected",
                client_id, position_id,
            )
            continue
        if filled_qty <= 0 or fill_price <= 0:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_ECONOMICS_INVALID pos=%s "
                "broker_id=%s qty=%s price=%s — rejected",
                client_id, position_id, bid, filled_qty, fill_price,
            )
            continue
        if not isinstance(filled_at, datetime):
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_TIMESTAMP_INVALID pos=%s "
                "broker_id=%s filled_at_type=%s — rejected",
                client_id, position_id, bid, type(filled_at).__name__,
            )
            continue
        if not db_status or db_status not in DURABLE_EXIT_FILLED_STATUSES:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_STATUS_REJECTED pos=%s "
                "broker_id=%s status=%r — rejected",
                client_id, position_id, bid, db_status,
            )
            continue
        if not db_contract:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_CONTRACT_MISSING pos=%s "
                "broker_id=%s — rejected",
                client_id, position_id, bid,
            )
            continue
        if db_contract != pos_contract:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_CONTRACT_MISMATCH pos=%s "
                "broker_id=%s expected=%s got=%s — rejected",
                client_id, position_id, bid, pos_contract, db_contract,
            )
            continue
        if not db_direction or db_direction not in {"CALL", "PUT"}:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_DIRECTION_INVALID pos=%s "
                "broker_id=%s direction=%r — rejected",
                client_id, position_id, bid, db_direction,
            )
            continue
        if db_direction != pos_direction:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_DIRECTION_MISMATCH pos=%s "
                "broker_id=%s expected=%s got=%s — rejected",
                client_id, position_id, bid, pos_direction, db_direction,
            )
            continue
        if opened_at is not None and filled_at < opened_at:
            log.warning(
                "[%s] MANUAL_CLOSE_DURABLE_FILL_TIMESTAMP_STALE pos=%s "
                "broker_id=%s fill_ts=%s entry_ts=%s — rejected",
                client_id, position_id, bid, filled_at, opened_at,
            )
            continue
        valid.append(f)

    return valid


def select_external_close_fills(
    *,
    orders: list[dict],
    position: dict,
    bot_exit_order_ids: set[str],
    adopted_fills: list[dict] | None = None,
    detected_at: datetime,
) -> tuple[dict | None, str]:
    """Pick the exact broker fill(s) that closed a missing position.

    Distinguishes three categories of matching orders:

      * bot_exit_order_ids  → legitimate bot EXIT ownership. If ANY match,
        the whole evidence set is rejected — the bot's own path owns this.
      * adopted_fills → pre-loaded from DB: our own previously-adopted
        external EXIT rows for THIS position with full fill evidence. Used
        directly as adopted_hits without re-scanning broker orders, which
        enables cross-session recovery when broker no longer returns
        previous-session orders. Never re-adopted.
      * everything else → candidate external fills to adopt now.

    Aggregate quantity constraint remains exact: adopted-so-far plus new
    candidates must equal the position's remaining quantity. Anything
    else is ambiguous and rejected.
    """
    contract = normalize_contract(position.get("contract"))
    opened_at = parse_timestamp(position.get("entry_ts") or position.get("opened_at"))
    required_qty = positive_int(
        position.get("quantity_remaining")
        if position.get("quantity_remaining") is not None
        else position.get("qty")
    )

    if not contract:
        return None, "position_contract_missing"
    if opened_at is None:
        return None, "position_opened_at_missing"
    if required_qty <= 0:
        return None, "position_quantity_missing"

    bot_ids = {str(v or "").strip() for v in bot_exit_order_ids if v}
    # adopted_fills: pre-loaded from DB. Re-validate each fill against this
    # position's contract, direction, status, and entry timestamp before
    # allowing it to contribute to the weighted-close aggregate. A durable row
    # that merely shares a position_id must not silently corrupt the evidence.
    client_id_ctx = str(position.get("client_id") or "")
    pre_adopted: list[dict] = _validate_durable_fills(
        list(adopted_fills or []),
        position=position,
        detected_at=detected_at,
        client_id=client_id_ctx,
    )
    adopted_ids: set[str] = {
        str(f.get("broker_order_id") or "").strip()
        for f in pre_adopted
        if f.get("broker_order_id")
    }

    bot_hit = False
    external_fills: list[dict] = []

    for order in orders:
        if order_contract(order) != contract:
            continue
        if order_side(order) not in MANUAL_CLOSE_SIDES:
            continue
        if order_status(order) not in FILLED_ORDER_STATUSES:
            continue

        normalized = _normalize_fill(order)
        if normalized is None:
            continue
        if normalized["filled_at"] < opened_at:
            continue
        if (normalized["filled_at"] - detected_at).total_seconds() > MANUAL_CLOSE_FUTURE_SKEW_SEC:
            continue

        broker_order_id = normalized["broker_order_id"]
        if broker_order_id in bot_ids:
            bot_hit = True
            break
        if broker_order_id in adopted_ids:
            # Already durably adopted; do not re-adopt.
            continue
        external_fills.append(normalized)

    # Use DB-loaded adopted fills directly — do NOT re-scan broker orders for them.
    adopted_hits: list[dict] = pre_adopted

    if bot_hit:
        return None, "bot_owned_exit_order_present"

    # Dedupe both sets by broker_order_id, keeping the latest filled_at row.
    def _dedupe(rows: list[dict]) -> list[dict]:
        best: dict[str, dict] = {}
        for r in rows:
            cur = best.get(r["broker_order_id"])
            if cur is None or r["filled_at"] > cur["filled_at"]:
                best[r["broker_order_id"]] = r
        return sorted(best.values(), key=lambda r: r["filled_at"])

    # Dedupe: adopted_hits come from DB rows; external_fills from broker orders.
    adopted_hits = _dedupe(adopted_hits)
    external_fills = _dedupe(external_fills)

    if not adopted_hits and not external_fills:
        return None, "no_exact_external_filled_exit_order"

    adopted_qty = sum(int(f["filled_qty"]) for f in adopted_hits)
    new_qty = sum(int(f["filled_qty"]) for f in external_fills)
    total_qty = adopted_qty + new_qty
    if total_qty != required_qty:
        return None, f"external_fill_qty_ambiguous:{total_qty}/{required_qty}"

    # Weighted aggregate covers BOTH adopted + new — that's the real
    # weighted broker truth for this position's close.
    all_fills = sorted(adopted_hits + external_fills, key=lambda r: r["filled_at"])
    weighted_notional = sum(
        float(f["fill_price"]) * int(f["filled_qty"]) for f in all_fills
    )
    average_fill = weighted_notional / total_qty if total_qty > 0 else 0.0
    if average_fill <= 0:
        return None, "external_fill_price_invalid"

    return {
        "fills": external_fills,          # only NEW fills need adoption
        "adopted_fills": adopted_hits,    # already durable; do not re-insert
        "all_fills": all_fills,           # aggregate view for the finalizer
        "filled_qty": required_qty,
        "fill_price": round(average_fill, 6),
        "filled_ts": all_fills[-1]["filled_at"].isoformat(),
        "broker_order_id": all_fills[-1]["broker_order_id"],
        "broker_order_ids": [r["broker_order_id"] for r in all_fills],
    }, "exact_external_broker_fill"


def load_manual_close_state(
    client_id: str,
    execution_mode: str,
) -> tuple[list[dict], set[str], dict[str, list[dict]]]:
    """Return (active_positions, bot_exit_ids, adopted_fills_by_position).

    * active_positions covers the canonical active family (OPEN, CLOSING,
      PARTIAL, ACTIVE) plus any row with quantity_remaining > 0, scoped to
      the exact execution_mode of this runner. A PARTIAL or ACTIVE position
      with retained external evidence must remain scannable.
    * bot_exit_ids is the client's EXIT rows whose local_order_id does NOT
      start with the external-adoption prefix, filtered to the exact
      execution_mode so PAPER IDs never pollute the LIVE fence and vice versa.
    * adopted_fills_by_position maps position_id → list of full fill dicts
      (broker_order_id, filled_qty, fill_price, filled_at, created_at,
      raw_status, raw_side) for previously-adopted external EXIT rows. Loads
      the complete fill evidence from durable orders rows so that next-session
      finalization can reconstruct the weighted aggregate without depending on
      the broker returning previous-session orders.
    """
    from ap.db import conn, run_with_retry
    norm_mode = str(execution_mode or "").strip().lower()

    def _read():
        with conn() as cursor:
            cursor.execute(
                """
                SELECT
                    id,
                    client_id,
                    contract,
                    underlying,
                    avg_fill,
                    qty,
                    quantity_remaining,
                    side,
                    direction,
                    local_order_id,
                    entry_ts,
                    opened_at,
                    execution_mode,
                    status,
                    exit_in_flight,
                    pending_exit_broker_order_id,
                    pending_exit_local_order_id
                FROM positions
                WHERE client_id=%s
                  AND LOWER(COALESCE(execution_mode,'')) = %s
                  AND (
                      UPPER(COALESCE(status,'')) = ANY(%s)
                      OR COALESCE(quantity_remaining, 0) > 0
                  )
                """,
                (client_id, norm_mode, list(ACTIVE_POSITION_STATUSES)),
            )
            positions = [dict(row) for row in (cursor.fetchall() or [])]

            cursor.execute(
                """
                SELECT
                    broker_order_id,
                    local_order_id,
                    position_id,
                    filled_qty,
                    fill_price,
                    filled_ts,
                    execution_mode,
                    status,
                    contract,
                    direction
                FROM orders
                WHERE client_id=%s
                  AND LOWER(COALESCE(execution_mode,'')) = %s
                  AND UPPER(COALESCE(kind,''))='EXIT'
                  AND COALESCE(broker_order_id,'') <> ''
                """,
                (client_id, norm_mode),
            )
            bot_ids: set[str] = set()
            adopted_fills_by_pos: dict[str, list[dict]] = {}
            for row in (cursor.fetchall() or []):
                broker_order_id = str(row.get("broker_order_id") or "").strip()
                if not broker_order_id:
                    continue
                local_order_id = str(row.get("local_order_id") or "").strip()
                position_id = str(row.get("position_id") or "").strip()
                if local_order_id.startswith(EXTERNAL_LOCAL_ID_PREFIX):
                    if not position_id:
                        continue
                    row_status = str(row.get("status") or "").upper().strip()
                    if row_status not in DURABLE_EXIT_FILLED_STATUSES:
                        # Row exists with an external local_id prefix but has a
                        # non-durable status (e.g. still pending). Not valid as
                        # recovery evidence; skip without adding to bot_ids.
                        continue
                    filled_qty = positive_int(row.get("filled_qty"))
                    fill_price = positive_float(row.get("fill_price"))
                    filled_at = parse_timestamp(row.get("filled_ts"))
                    db_contract = str(row.get("contract") or "").upper().strip()
                    db_direction = str(row.get("direction") or "").upper().strip()
                    if filled_qty > 0 and fill_price > 0 and filled_at is not None:
                        fill_dict: dict = {
                            "broker_order_id": broker_order_id,
                            "filled_qty": filled_qty,
                            "fill_price": fill_price,
                            "filled_at": filled_at,
                            "created_at": None,
                            "raw_status": row_status,
                            "raw_side": "sell_to_close",
                            # DB-sourced identity fields for cross-position validation.
                            "db_contract": db_contract,
                            "db_direction": db_direction,
                        }
                        adopted_fills_by_pos.setdefault(position_id, []).append(fill_dict)
                else:
                    bot_ids.add(broker_order_id)
            return positions, bot_ids, adopted_fills_by_pos

    return run_with_retry(_read)


def _external_local_order_id(client_id: str, broker_order_id: str) -> str:
    return f"{EXTERNAL_LOCAL_ID_PREFIX}{client_id}:{broker_order_id}"


def _row_matches_expected(
    row: dict,
    *,
    client_id: str,
    position: dict,
    fill: dict,
    execution_mode: str,
) -> bool:
    if not isinstance(row, dict):
        return False
    expected_contract = normalize_contract(position.get("contract"))
    expected_side = position_direction(position)
    actual_mode = str(row.get("execution_mode") or "").lower().strip()
    try:
        actual_fill_price = float(row.get("fill_price") or 0)
    except Exception:
        actual_fill_price = 0.0
    return bool(
        str(row.get("client_id") or "").strip().lower() == client_id.lower()
        and str(row.get("position_id") or "").strip() == str(position.get("id") or "").strip()
        and str(row.get("kind") or "").upper().strip() == "EXIT"
        and str(row.get("status") or "").upper().strip() in DURABLE_EXIT_FILLED_STATUSES
        and normalize_contract(row.get("contract")) == expected_contract
        and str(row.get("direction") or "").upper().strip() == expected_side
        and str(row.get("broker_order_id") or "").strip() == fill["broker_order_id"]
        and positive_int(row.get("filled_qty")) == int(fill["filled_qty"])
        and abs(actual_fill_price - float(fill["fill_price"])) < 0.000001
        and actual_mode == execution_mode
    )


def adopt_external_exit_fills(
    *,
    client_id: str,
    execution_mode: str,
    position: dict,
    evidence: dict,
) -> tuple[bool, str]:
    """Atomically adopt every NEW external fill for one position.

    All fills for the same position run inside a single connection with
    ONE position-scoped advisory lock. If any fill fails validation or
    INSERT, the transaction is rolled back and nothing is persisted —
    the next scan re-observes the missing broker position and retries
    the whole evidence set. Prevents partial-adoption stranding.
    """
    from ap.db import conn, run_with_retry

    position_id = str(position.get("id") or "").strip()
    contract = normalize_contract(position.get("contract"))
    symbol = str(position.get("underlying") or "").upper().strip()
    direction = position_direction(position)
    if (
        not client_id
        or not position_id
        or not contract
        or direction not in {"CALL", "PUT"}
        or execution_mode not in VALID_EXECUTION_MODES
    ):
        return False, "external_exit_adoption_identity_invalid"

    fills = list(evidence.get("fills") or [])
    if not fills:
        # Nothing NEW to adopt (all fills were already durable). Caller
        # can still proceed to finalize with the aggregate.
        return True, "external_exit_adoption_nothing_to_do"

    lock_key = f"manual-close-adopt:{client_id}:{position_id}"

    def _adopt_all() -> tuple[bool, str]:
        with conn() as cursor:
            cursor.execute(
                "SELECT pg_advisory_xact_lock(('x' || md5(%s))::bit(64)::bigint)",
                (lock_key,),
            )
            for fill in fills:
                broker_order_id = str(fill.get("broker_order_id") or "").strip()
                filled_qty = positive_int(fill.get("filled_qty"))
                fill_price = positive_float(fill.get("fill_price"))
                filled_at = parse_timestamp(fill.get("filled_at"))
                created_at = parse_timestamp(fill.get("created_at"))
                if not broker_order_id or filled_qty <= 0 or fill_price <= 0 or filled_at is None:
                    raise RuntimeError("external_exit_adoption_fill_invalid")

                local_order_id = _external_local_order_id(client_id, broker_order_id)
                meta = json.dumps(
                    {
                        "source": "manual_client_close_broker_fill",
                        "external_broker_order": True,
                        "broker_order_side": str(fill.get("raw_side") or "sell_to_close"),
                        "broker_order_status": str(fill.get("raw_status") or "filled"),
                        "broker_create_date_present": created_at is not None,
                        "adopted_without_submit": True,
                        "position_id": position_id,
                        "client_id": client_id,
                        "execution_mode": execution_mode,
                    },
                    separators=(",", ":"),
                    sort_keys=True,
                )

                cursor.execute(
                    """
                    SELECT *
                    FROM orders
                    WHERE client_id=%s
                      AND broker_order_id=%s
                    LIMIT 2
                    """,
                    (client_id, broker_order_id),
                )
                existing = [dict(row) for row in (cursor.fetchall() or [])]
                if len(existing) > 1:
                    raise RuntimeError("external_exit_adoption_broker_id_ambiguous")
                if len(existing) == 1:
                    if not _row_matches_expected(
                        existing[0],
                        client_id=client_id,
                        position=position,
                        fill=fill,
                        execution_mode=execution_mode,
                    ):
                        raise RuntimeError("external_exit_adoption_existing_row_mismatch")
                    continue

                cursor.execute(
                    """
                    INSERT INTO orders (
                        client_id,
                        local_order_id,
                        broker_order_id,
                        position_id,
                        kind,
                        status,
                        symbol,
                        contract,
                        direction,
                        qty,
                        filled_qty,
                        fill_price,
                        created_ts,
                        updated_ts,
                        submitted_ts,
                        filled_ts,
                        meta,
                        execution_mode
                    )
                    VALUES (
                        %s,%s,%s,%s,
                        'EXIT','EXIT_FILLED',
                        %s,%s,%s,%s,%s,%s,
                        %s,%s,%s,%s,
                        %s::jsonb,%s
                    )
                    ON CONFLICT (local_order_id) DO NOTHING
                    RETURNING *
                    """,
                    (
                        client_id,
                        local_order_id,
                        broker_order_id,
                        position_id,
                        symbol or contract,
                        contract,
                        direction,
                        filled_qty,
                        filled_qty,
                        fill_price,
                        created_at or filled_at,
                        datetime.now(timezone.utc),
                        created_at,
                        filled_at,
                        meta,
                        execution_mode,
                    ),
                )
                inserted = cursor.fetchone()
                if inserted:
                    if not _row_matches_expected(
                        dict(inserted),
                        client_id=client_id,
                        position=position,
                        fill=fill,
                        execution_mode=execution_mode,
                    ):
                        raise RuntimeError("external_exit_adoption_inserted_row_mismatch")
                    continue

                # ON CONFLICT DO NOTHING: another actor beat us. Reload
                # and validate identity.
                cursor.execute(
                    """
                    SELECT *
                    FROM orders
                    WHERE local_order_id=%s
                    LIMIT 2
                    """,
                    (local_order_id,),
                )
                collided = [dict(row) for row in (cursor.fetchall() or [])]
                if len(collided) != 1:
                    raise RuntimeError("external_exit_adoption_local_id_unresolved")
                if not _row_matches_expected(
                    collided[0],
                    client_id=client_id,
                    position=position,
                    fill=fill,
                    execution_mode=execution_mode,
                ):
                    raise RuntimeError("external_exit_adoption_existing_local_id_mismatch")
            return True, "external_exit_adoption_complete"

    try:
        return run_with_retry(_adopt_all)
    except Exception as exc:
        log.error(
            "[%s] MANUAL_CLOSE_EXIT_ADOPTION_FAILED pos=%s contract=%s "
            "fill_count=%s err=%s",
            client_id,
            position_id,
            contract,
            len(fills),
            exc,
        )
        return False, f"external_exit_adoption_error:{exc}"




def _finalize_position(
    *,
    finalizer,
    client_id: str,
    position_id: str,
    contract: str,
    evidence: dict,
) -> bool:
    # Canonical idempotency is handled inside APPositionManager
    # .close_position_from_exit_fill() under its SELECT ... FOR UPDATE row
    # lock. A detached pre-read (with no lock) cannot prevent a race and is
    # therefore removed. The finalizer returns True for already-terminal
    # positions and False for missing positions or DB errors, which is the
    # correct tri-state contract: terminal→evict, unknown/error→retain.
    broker_ids = ",".join(evidence["broker_order_ids"])
    exit_reason = (
        "MANUAL_CLIENT_CLOSE_BROKER_CONFIRMED "
        f"broker_order_ids={broker_ids}"
    )
    try:
        return bool(
            finalizer(
                position_id=position_id,
                exit_price=float(evidence["fill_price"]),
                filled_qty=int(evidence["filled_qty"]),
                filled_ts=str(evidence["filled_ts"]),
                broker_order_id=str(evidence["broker_order_id"]),
                close_source="manual_client_close_broker_fill",
                close_confidence="HIGH",
                exit_reason=exit_reason,
            )
        )
    except Exception as exc:
        log.critical(
            "[%s] MANUAL_CLOSE_FINALIZE_EXCEPTION pos=%s contract=%s "
            "broker_order_ids=%s err=%s",
            client_id,
            position_id,
            contract,
            broker_ids,
            exc,
        )
        return False



def _evict_exit_engine(
    runner: Any,
    *,
    client_id: str,
    position_id: str,
    contract: str,
) -> None:
    """Evict a finalized position from the in-memory exit engine."""
    core = getattr(runner, "core", None)
    exit_engine = getattr(core, "exit_eng", None) if core is not None else None
    mark_closed = getattr(exit_engine, "mark_position_closed", None)
    if callable(mark_closed):
        try:
            mark_closed(position_id)
        except Exception as exc:
            log.warning(
                "[%s] MANUAL_CLOSE_EXIT_ENGINE_EVICT_FAILED pos=%s contract=%s err=%s",
                client_id, position_id, contract, exc,
            )


def detect_manual_closes(self) -> None:
    """Finalize externally closed positions from exact broker fill truth.

    Throttled to MANUAL_CLOSE_INTERVAL_SEC. Two passes per tick:

      PASS 1 — Durable recovery (no broker call required):
        For each active position whose adopted EXIT rows already cover
        the full required quantity, re-validate the durable fills and call
        the canonical finalizer directly. This handles cross-session
        restarts (broker no longer returns previous-session orders) and
        the case where PASS 2 previously adopted fills but finalization
        failed. Runs even if the current-session broker order endpoint
        is unavailable.

      PASS 2 — New external fill discovery:
        Fetch authoritative broker positions to find contracts no longer
        held by the broker. For each missing position not yet resolved by
        PASS 1, fetch current-session broker orders, select exact external
        close evidence, adopt atomically, and finalize. Broker orders are
        fetched only AFTER durable-covered positions are handled so a
        transient order-endpoint failure cannot block already-evidenced
        recovery.
    """
    now_epoch = time.time()
    last_check = float(getattr(self, "_last_manual_close_check_ts", 0.0) or 0.0)
    if now_epoch - last_check < MANUAL_CLOSE_INTERVAL_SEC:
        return
    self._last_manual_close_check_ts = now_epoch

    client_id = str(getattr(self, "email", "") or "").strip().lower()
    runner_mode = str(getattr(self, "mode", "") or "").strip().lower()
    broker = getattr(self, "broker", None)

    if not client_id or runner_mode not in VALID_EXECUTION_MODES:
        log.error(
            "MANUAL_CLOSE_SCAN_SKIPPED invalid runner identity client=%s mode=%s",
            client_id,
            runner_mode,
        )
        return
    if broker is None:
        return

    # Always load durable DB state first — safe regardless of broker availability.
    try:
        active_positions, bot_exit_ids, adopted_fills_by_pos = load_manual_close_state(
            client_id, runner_mode
        )
    except Exception as exc:
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_DB_READ_FAILED mode=%s err=%s",
            client_id,
            runner_mode,
            exc,
        )
        return

    if not active_positions:
        return

    pm = getattr(self, "position_manager", None)
    finalizer = getattr(pm, "close_position_from_exit_fill", None)
    if not callable(finalizer):
        log.critical(
            "[%s] MANUAL_CLOSE_CANONICAL_FINALIZER_UNAVAILABLE active=%s",
            client_id,
            len(active_positions),
        )
        return

    detected_at = datetime.fromtimestamp(now_epoch, tz=timezone.utc)

    def _check_fences(position: dict, position_id: str, contract: str) -> bool:
        """Return True if the position passes all ownership fences."""
        position_mode = str(position.get("execution_mode") or "").strip().lower()
        if position_mode not in VALID_EXECUTION_MODES or position_mode != runner_mode:
            log.critical(
                "[%s] MANUAL_CLOSE_MODE_FENCE pos=%s contract=%s "
                "runner_mode=%s position_mode=%s — no mutation",
                client_id, position_id, contract,
                runner_mode, position_mode or "missing",
            )
            return False
        if (
            bool(position.get("exit_in_flight"))
            or str(position.get("pending_exit_broker_order_id") or "").strip()
            or str(position.get("pending_exit_local_order_id") or "").strip()
        ):
            log.info(
                "[%s] MANUAL_CLOSE_SKIPPED_EXISTING_EXIT_OWNER pos=%s contract=%s",
                client_id, position_id, contract,
            )
            return False
        return True

    # ─── PASS 1: Durable recovery — finalize from DB evidence alone ───────────
    # Positions whose adopted EXIT rows already cover the full required quantity
    # are finalized here without any broker call. This handles:
    #   a) Cross-session restarts (broker no longer returns previous-session orders)
    #   b) Positions where adoption succeeded but finalization failed last scan
    #   c) Cases where the current-session broker order endpoint is unavailable
    # ──────────────────────────────────────────────────────────────────────────
    needs_broker_scan: list[dict] = []

    for position in active_positions:
        position_id = str(position.get("id") or "").strip()
        contract = normalize_contract(position.get("contract"))
        if not position_id or not contract:
            continue
        if not _check_fences(position, position_id, contract):
            continue

        required_qty = positive_int(
            position.get("quantity_remaining")
            if position.get("quantity_remaining") is not None
            else position.get("qty")
        )
        if required_qty <= 0:
            continue

        durable_fills = adopted_fills_by_pos.get(position_id, [])
        if not durable_fills:
            needs_broker_scan.append(position)
            continue

        valid_durable = _validate_durable_fills(
            durable_fills,
            position=position,
            detected_at=detected_at,
            client_id=client_id,
        )
        if not valid_durable:
            needs_broker_scan.append(position)
            continue

        adopted_qty = sum(int(f["filled_qty"]) for f in valid_durable)
        if adopted_qty != required_qty:
            # Partial durable coverage — broker scan may supply remaining fills.
            needs_broker_scan.append(position)
            continue

        # Full durable coverage. Build aggregate and finalize without broker.
        all_fills = sorted(valid_durable, key=lambda r: r["filled_at"])
        weighted_notional = sum(
            float(f["fill_price"]) * int(f["filled_qty"]) for f in all_fills
        )
        avg_price = weighted_notional / adopted_qty

        durable_evidence: dict = {
            "fills": [],
            "adopted_fills": all_fills,
            "all_fills": all_fills,
            "filled_qty": adopted_qty,
            "fill_price": round(avg_price, 6),
            "filled_ts": all_fills[-1]["filled_at"].isoformat(),
            "broker_order_id": all_fills[-1]["broker_order_id"],
            "broker_order_ids": [f["broker_order_id"] for f in all_fills],
        }

        finalized = _finalize_position(
            finalizer=finalizer,
            client_id=client_id,
            position_id=position_id,
            contract=contract,
            evidence=durable_evidence,
        )
        if finalized:
            _evict_exit_engine(
                self, client_id=client_id, position_id=position_id, contract=contract
            )
            log.info(
                "[%s] MANUAL_CLOSE_PASS1_FINALIZED pos=%s contract=%s "
                "exit=%.4f qty=%s broker_order_ids=%s "
                "proof_path=canonical taxonomy_source=durable_exit_orders",
                client_id, position_id, contract,
                avg_price, adopted_qty,
                ",".join(durable_evidence["broker_order_ids"]),
            )
        else:
            log.critical(
                "[%s] MANUAL_CLOSE_PASS1_FINALIZE_FAILED pos=%s contract=%s "
                "— durable EXIT rows retained; next scan will retry",
                client_id, position_id, contract,
            )

    if not needs_broker_scan:
        return

    # ─── PASS 2: New external fill discovery via broker ────────────────────────
    # Broker calls happen here — AFTER durable-covered positions are handled.
    # A transient broker-positions or broker-orders failure does not block
    # positions that already have complete durable evidence.
    # ──────────────────────────────────────────────────────────────────────────
    try:
        broker_positions = fetch_authoritative_broker_positions(broker)
    except Exception as exc:
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_positions_unavailable "
            "mode=%s err=%s — no state mutation",
            client_id, runner_mode, exc,
        )
        return

    broker_contract_qty: dict[str, int] = {}
    for broker_position in broker_positions:
        bpos_contract = normalize_contract(
            broker_position.get("symbol")
            or broker_position.get("option_symbol")
            or broker_position.get("contract")
        )
        quantity = positive_int(
            broker_position.get("quantity") or broker_position.get("qty")
        )
        if bpos_contract and quantity > 0:
            broker_contract_qty[bpos_contract] = (
                broker_contract_qty.get(bpos_contract, 0) + quantity
            )

    missing_positions = [
        position
        for position in needs_broker_scan
        if normalize_contract(position.get("contract")) not in broker_contract_qty
    ]
    if not missing_positions:
        return

    # Fetch current-session orders only now — only for positions that still
    # need new external fill discovery (not for durable-covered ones).
    try:
        broker_orders = fetch_all_current_session_orders(broker)
    except Exception as exc:
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_orders_failed "
            "missing_positions=%s err=%s — no state mutation",
            client_id, len(missing_positions), exc,
        )
        return

    for position in missing_positions:
        position_id = str(position.get("id") or "").strip()
        contract = normalize_contract(position.get("contract"))
        if not position_id or not contract:
            continue
        if not _check_fences(position, position_id, contract):
            continue

        adopted_fills_for_pos = adopted_fills_by_pos.get(position_id, [])
        evidence, reason_code = select_external_close_fills(
            orders=broker_orders,
            position=position,
            bot_exit_order_ids=bot_exit_ids,
            adopted_fills=adopted_fills_for_pos,
            detected_at=detected_at,
        )
        if evidence is None:
            log.warning(
                "[%s] MANUAL_CLOSE_EVIDENCE_MISSING pos=%s contract=%s "
                "reason=%s — position remains active",
                client_id, position_id, contract, reason_code,
            )
            continue

        adopted, adoption_reason = adopt_external_exit_fills(
            client_id=client_id,
            execution_mode=runner_mode,
            position=position,
            evidence=evidence,
        )
        if not adopted:
            log.critical(
                "[%s] MANUAL_CLOSE_ADOPTION_FAILED pos=%s contract=%s reason=%s "
                "— position remains active; next scan will retry",
                client_id, position_id, contract, adoption_reason,
            )
            continue

        finalized = _finalize_position(
            finalizer=finalizer,
            client_id=client_id,
            position_id=position_id,
            contract=contract,
            evidence=evidence,
        )
        if not finalized:
            log.critical(
                "[%s] MANUAL_CLOSE_FINALIZE_FAILED pos=%s contract=%s "
                "broker_order_ids=%s — durable EXIT rows retained; next "
                "scan will re-invoke finalizer with weighted aggregate",
                client_id, position_id, contract,
                ",".join(evidence["broker_order_ids"]),
            )
            continue

        _evict_exit_engine(
            self, client_id=client_id, position_id=position_id, contract=contract
        )
        log.info(
            "[%s] MANUAL_CLOSE_FINALIZED pos=%s contract=%s exit=%.4f "
            "qty=%s broker_order_ids=%s "
            "proof_path=canonical taxonomy_source=durable_exit_orders",
            client_id, position_id, contract,
            float(evidence["fill_price"]),
            int(evidence["filled_qty"]),
            ",".join(evidence["broker_order_ids"]),
        )
