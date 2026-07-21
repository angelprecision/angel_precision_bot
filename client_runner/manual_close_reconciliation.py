"""Broker-truth reconciliation for externally closed client positions.

This module is imported only by the ``client_runner`` shadow package. It does
not submit or cancel broker orders. It adopts already-filled external Tradier
EXIT orders into the durable order ledger, then delegates position/proof
finalization to ``APPositionManager.close_position_from_exit_fill``.
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
VALID_EXECUTION_MODES = frozenset({"paper", "live"})
MANUAL_CLOSE_SIDES = frozenset(
    {"sell_to_close", "sell-to-close", "selltoclose", "stc"}
)
FILLED_ORDER_STATUSES = frozenset(
    {"filled", "partially_filled", "partial_fill", "partially-filled"}
)
DURABLE_EXIT_FILLED_STATUSES = frozenset({"EXIT_FILLED", "EXIT_PARTIAL_FILL"})
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


def order_filled_at(order: dict) -> datetime | None:
    for key in (
        "last_fill_date",
        "filled_at",
        "filled_ts",
        "transaction_date",
        "update_date",
        "updated_at",
        "create_date",
        "created_at",
        "submitted_at",
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


def select_external_close_fills(
    *,
    orders: list[dict],
    position: dict,
    known_exit_order_ids: set[str],
    detected_at: datetime,
) -> tuple[dict | None, str]:
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

    known_ids = {str(value or "").strip() for value in known_exit_order_ids if value}
    matching_known: list[dict] = []
    external_fills: list[dict] = []

    for order in orders:
        if order_contract(order) != contract:
            continue
        if order_side(order) not in MANUAL_CLOSE_SIDES:
            continue
        if order_status(order) not in FILLED_ORDER_STATUSES:
            continue

        broker_order_id = order_id(order)
        filled_qty = order_filled_qty(order)
        fill_price = order_fill_price(order)
        filled_at = order_filled_at(order)

        if not broker_order_id or filled_qty <= 0 or fill_price <= 0 or filled_at is None:
            continue
        if filled_at < opened_at:
            continue
        if (filled_at - detected_at).total_seconds() > MANUAL_CLOSE_FUTURE_SKEW_SEC:
            continue

        normalized = {
            "broker_order_id": broker_order_id,
            "filled_qty": filled_qty,
            "fill_price": fill_price,
            "filled_at": filled_at,
            "raw_status": order_status(order),
            "raw_side": order_side(order),
        }
        if broker_order_id in known_ids:
            matching_known.append(normalized)
        else:
            external_fills.append(normalized)

    if matching_known:
        return None, "bot_owned_exit_order_present"
    if not external_fills:
        return None, "no_exact_external_filled_exit_order"

    # Tradier list responses can repeat the same order. Deduplicate by durable
    # broker identity before quantity comparison.
    deduped: dict[str, dict] = {}
    for fill in external_fills:
        current = deduped.get(fill["broker_order_id"])
        if current is None or fill["filled_at"] > current["filled_at"]:
            deduped[fill["broker_order_id"]] = fill

    fills = sorted(deduped.values(), key=lambda item: item["filled_at"])
    total_qty = sum(int(item["filled_qty"]) for item in fills)
    if total_qty != required_qty:
        return None, f"external_fill_qty_ambiguous:{total_qty}/{required_qty}"

    weighted_notional = sum(
        float(item["fill_price"]) * int(item["filled_qty"]) for item in fills
    )
    average_fill = weighted_notional / total_qty if total_qty > 0 else 0.0
    if average_fill <= 0:
        return None, "external_fill_price_invalid"

    return {
        "fills": fills,
        "filled_qty": required_qty,
        "fill_price": round(average_fill, 6),
        "filled_ts": fills[-1]["filled_at"].isoformat(),
        "broker_order_id": fills[-1]["broker_order_id"],
        "broker_order_ids": [item["broker_order_id"] for item in fills],
    }, "exact_external_broker_fill"


def load_manual_close_state(client_id: str) -> tuple[list[dict], set[str]]:
    from ap.db import conn, run_with_retry

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
                    exit_in_flight,
                    pending_exit_broker_order_id,
                    pending_exit_local_order_id
                FROM positions
                WHERE client_id=%s
                  AND status='OPEN'
                """,
                (client_id,),
            )
            positions = [dict(row) for row in (cursor.fetchall() or [])]

            cursor.execute(
                """
                SELECT broker_order_id
                FROM orders
                WHERE client_id=%s
                  AND UPPER(COALESCE(kind,''))='EXIT'
                  AND COALESCE(broker_order_id,'') <> ''
                """,
                (client_id,),
            )
            known_ids = {
                str(row.get("broker_order_id") or "").strip()
                for row in (cursor.fetchall() or [])
                if row.get("broker_order_id")
            }
            return positions, known_ids

    return run_with_retry(_read)


def _external_local_order_id(client_id: str, broker_order_id: str) -> str:
    return f"external-exit:{client_id}:{broker_order_id}"


def _validate_adopted_exit_row(
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
    """Persist exact external fills as durable EXIT_FILLED lifecycle rows.

    This is an adoption of already-executed broker truth, not a broker submit.
    It is idempotent by globally unique local_order_id plus a transaction-scoped
    advisory lock. Existing rows must match the full client/position/mode/fill
    identity or the adoption fails closed.
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
        return False, "external_exit_adoption_no_fills"

    for fill in fills:
        broker_order_id = str(fill.get("broker_order_id") or "").strip()
        filled_qty = positive_int(fill.get("filled_qty"))
        fill_price = positive_float(fill.get("fill_price"))
        filled_at = parse_timestamp(fill.get("filled_at"))
        if not broker_order_id or filled_qty <= 0 or fill_price <= 0 or filled_at is None:
            return False, "external_exit_adoption_fill_invalid"

        local_order_id = _external_local_order_id(client_id, broker_order_id)
        lock_key = f"manual-close-adopt:{client_id}:{broker_order_id}"
        meta = json.dumps(
            {
                "source": "manual_client_close_broker_fill",
                "external_broker_order": True,
                "broker_order_side": str(fill.get("raw_side") or "sell_to_close"),
                "broker_order_status": str(fill.get("raw_status") or "filled"),
                "adopted_without_submit": True,
                "position_id": position_id,
                "client_id": client_id,
                "execution_mode": execution_mode,
            },
            separators=(",", ":"),
            sort_keys=True,
        )

        def _adopt_one():
            with conn() as cursor:
                cursor.execute(
                    "SELECT pg_advisory_xact_lock(('x' || md5(%s))::bit(64)::bigint)",
                    (lock_key,),
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
                    return False, "external_exit_adoption_broker_id_ambiguous"
                if len(existing) == 1:
                    return (
                        _validate_adopted_exit_row(
                            existing[0],
                            client_id=client_id,
                            position=position,
                            fill=fill,
                            execution_mode=execution_mode,
                        ),
                        "external_exit_adoption_existing_row",
                    )

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
                        filled_at,
                        filled_at,
                        filled_at,
                        filled_at,
                        meta,
                        execution_mode,
                    ),
                )
                inserted = cursor.fetchone()
                if inserted:
                    row = dict(inserted)
                    return (
                        _validate_adopted_exit_row(
                            row,
                            client_id=client_id,
                            position=position,
                            fill=fill,
                            execution_mode=execution_mode,
                        ),
                        "external_exit_adoption_inserted",
                    )

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
                    return False, "external_exit_adoption_local_id_unresolved"
                return (
                    _validate_adopted_exit_row(
                        collided[0],
                        client_id=client_id,
                        position=position,
                        fill=fill,
                        execution_mode=execution_mode,
                    ),
                    "external_exit_adoption_local_id_existing",
                )

        try:
            ok, reason = run_with_retry(_adopt_one)
        except Exception as exc:
            log.error(
                "[%s] MANUAL_CLOSE_EXIT_ADOPTION_FAILED pos=%s contract=%s "
                "broker_order_id=%s err=%s",
                client_id,
                position_id,
                contract,
                broker_order_id,
                exc,
            )
            return False, "external_exit_adoption_db_error"
        if not ok:
            log.critical(
                "[%s] MANUAL_CLOSE_EXIT_ADOPTION_BLOCKED pos=%s contract=%s "
                "broker_order_id=%s reason=%s",
                client_id,
                position_id,
                contract,
                broker_order_id,
                reason,
            )
            return False, reason

    return True, "external_exit_adoption_complete"


def detect_manual_closes(self) -> None:
    """Finalize externally closed positions only from exact broker fill truth."""
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

    try:
        broker_positions = fetch_authoritative_broker_positions(broker)
    except Exception as exc:
        # An unavailable/malformed positions query must never become "empty".
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_positions_unavailable "
            "mode=%s err=%s — no state mutation",
            client_id,
            runner_mode,
            exc,
        )
        return

    broker_contract_qty: dict[str, int] = {}
    for broker_position in broker_positions:
        contract = normalize_contract(
            broker_position.get("symbol")
            or broker_position.get("option_symbol")
            or broker_position.get("contract")
        )
        quantity = positive_int(
            broker_position.get("quantity") or broker_position.get("qty")
        )
        if contract and quantity > 0:
            broker_contract_qty[contract] = (
                broker_contract_qty.get(contract, 0) + quantity
            )

    try:
        open_positions, known_exit_order_ids = load_manual_close_state(client_id)
    except Exception as exc:
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_DB_READ_FAILED mode=%s err=%s",
            client_id,
            runner_mode,
            exc,
        )
        return

    missing_positions = [
        position
        for position in open_positions
        if normalize_contract(position.get("contract")) not in broker_contract_qty
    ]
    if not missing_positions:
        return

    list_orders = getattr(broker, "list_orders", None)
    if not callable(list_orders):
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_orders_unavailable "
            "missing_positions=%s — no state mutation",
            client_id,
            len(missing_positions),
        )
        return

    try:
        broker_orders = normalize_broker_orders(list_orders())
    except Exception as exc:
        log.error(
            "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_orders_failed "
            "missing_positions=%s err=%s — no state mutation",
            client_id,
            len(missing_positions),
            exc,
        )
        return

    detected_at = datetime.fromtimestamp(now_epoch, tz=timezone.utc)
    pm = getattr(self, "position_manager", None)
    finalizer = getattr(pm, "close_position_from_exit_fill", None)
    if not callable(finalizer):
        log.critical(
            "[%s] MANUAL_CLOSE_CANONICAL_FINALIZER_UNAVAILABLE "
            "missing_positions=%s",
            client_id,
            len(missing_positions),
        )
        return

    for position in missing_positions:
        position_id = str(position.get("id") or "").strip()
        contract = normalize_contract(position.get("contract"))
        position_mode = str(position.get("execution_mode") or "").strip().lower()

        if not position_id or not contract:
            continue
        if position_mode not in VALID_EXECUTION_MODES or position_mode != runner_mode:
            log.critical(
                "[%s] MANUAL_CLOSE_MODE_FENCE pos=%s contract=%s "
                "runner_mode=%s position_mode=%s — no mutation",
                client_id,
                position_id,
                contract,
                runner_mode,
                position_mode or "missing",
            )
            continue
        if bool(position.get("exit_in_flight")) or str(
            position.get("pending_exit_broker_order_id") or ""
        ).strip() or str(position.get("pending_exit_local_order_id") or "").strip():
            log.info(
                "[%s] MANUAL_CLOSE_SKIPPED_EXISTING_EXIT_OWNER pos=%s contract=%s",
                client_id,
                position_id,
                contract,
            )
            continue

        evidence, reason_code = select_external_close_fills(
            orders=broker_orders,
            position=position,
            known_exit_order_ids=known_exit_order_ids,
            detected_at=detected_at,
        )
        if evidence is None:
            log.warning(
                "[%s] MANUAL_CLOSE_EVIDENCE_MISSING pos=%s contract=%s "
                "reason=%s — DB remains OPEN",
                client_id,
                position_id,
                contract,
                reason_code,
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
                "[%s] MANUAL_CLOSE_NOT_FINALIZED pos=%s contract=%s "
                "reason=%s — DB remains OPEN",
                client_id,
                position_id,
                contract,
                adoption_reason,
            )
            continue

        broker_ids = ",".join(evidence["broker_order_ids"])
        exit_reason = (
            "MANUAL_CLIENT_CLOSE_BROKER_CONFIRMED "
            f"broker_order_ids={broker_ids}"
        )
        finalized = bool(
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
        if not finalized:
            log.critical(
                "[%s] MANUAL_CLOSE_FINALIZE_FAILED pos=%s contract=%s "
                "broker_order_ids=%s — durable EXIT rows retained for reconciler recovery",
                client_id,
                position_id,
                contract,
                broker_ids,
            )
            continue

        core = getattr(self, "core", None)
        exit_engine = getattr(core, "exit_eng", None) if core is not None else None
        mark_closed = getattr(exit_engine, "mark_position_closed", None)
        if callable(mark_closed):
            try:
                mark_closed(position_id)
            except Exception as exc:
                log.warning(
                    "[%s] MANUAL_CLOSE_EXIT_ENGINE_EVICT_FAILED "
                    "pos=%s contract=%s err=%s",
                    client_id,
                    position_id,
                    contract,
                    exc,
                )

        log.info(
            "[%s] MANUAL_CLOSE_FINALIZED pos=%s contract=%s exit=%.4f "
            "qty=%s broker_order_ids=%s proof_path=canonical taxonomy_source=durable_exit_orders",
            client_id,
            position_id,
            contract,
            float(evidence["fill_price"]),
            int(evidence["filled_qty"]),
            broker_ids,
        )
