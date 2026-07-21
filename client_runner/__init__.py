"""P0 manual-close proof reconciliation shim.

This package shadows the legacy top-level ``client_runner.py`` module, re-exports
its public surface, and overrides only ``ClientRunner._detect_manual_closes``.

Production incident addressed:
- a client/operator closed a LIVE option directly at Tradier;
- the runner observed that the contract disappeared from broker positions;
- the legacy detector marked ``positions`` CLOSED with no exit price/P&L;
- it then called terminal-proof binding with entry price, no broker fill, and
  ``allow_fallback_insert=False``;
- the position disappeared from management while no ``proof_trades`` row was
  created.

The override fails closed on broker-query uncertainty, requires exact
broker-confirmed ``sell_to_close`` fill evidence for the same contract after the
position opened, rejects bot-owned EXIT orders and mode mismatches, then invokes
``APPositionManager.close_position_from_exit_fill``. That canonical finalizer
persists exit price/P&L and writes or repairs proof using the originating entry
identity.

Scope:
- active for PAPER and LIVE manual broker closes;
- reads broker positions and order history;
- mutates positions/proof only through the canonical fill finalizer;
- does not submit or cancel broker orders;
- does not mutate entry orders, queues, signals, or positions without exact fill
  evidence.
"""

from __future__ import annotations

import importlib.util as _importlib_util
import math as _math
import os as _os
import re as _re
import sys as _sys
import time as _time
from datetime import datetime as _datetime, timezone as _timezone
from pathlib import Path as _Path
from typing import Any as _Any


_BASE_PATH = _Path(__file__).resolve().parent.parent / "client_runner.py"
_BASE_MODULE_NAME = "_client_runner_base"

_spec = _importlib_util.spec_from_file_location(_BASE_MODULE_NAME, _BASE_PATH)
if _spec is None or _spec.loader is None:  # pragma: no cover
    raise ImportError(f"Unable to load legacy client runner from {_BASE_PATH}")

_base = _importlib_util.module_from_spec(_spec)
_sys.modules[_BASE_MODULE_NAME] = _base
_spec.loader.exec_module(_base)

for _name in dir(_base):
    if not _name.startswith("__") or _name == "__doc__":
        globals()[_name] = getattr(_base, _name)

_BaseClientRunner = _base.ClientRunner
_logger = _base.logger

_MANUAL_CLOSE_INTERVAL_SEC = float(
    _os.getenv("MANUAL_CLOSE_DETECT_INTERVAL_SEC", "120")
)
_MANUAL_CLOSE_FUTURE_SKEW_SEC = float(
    _os.getenv("MANUAL_CLOSE_ORDER_FUTURE_SKEW_SEC", "300")
)
_VALID_EXECUTION_MODES = frozenset({"paper", "live"})
_MANUAL_CLOSE_SIDES = frozenset(
    {"sell_to_close", "sell-to-close", "selltoclose", "stc"}
)
_FILLED_ORDER_STATUSES = frozenset(
    {"filled", "partially_filled", "partial_fill", "partially-filled"}
)
_OCC_RE = _re.compile(r"^[A-Z0-9.]{1,6}\d{6}[CP]\d{8}$")


def _normalize_contract(value: _Any) -> str:
    return str(value or "").upper().replace(" ", "").strip()


def _positive_float(value: _Any) -> float:
    try:
        parsed = float(value)
    except Exception:
        return 0.0
    if not _math.isfinite(parsed) or parsed <= 0:
        return 0.0
    return parsed


def _positive_int(value: _Any) -> int:
    try:
        parsed = int(float(value))
    except Exception:
        return 0
    return parsed if parsed > 0 else 0


def _parse_timestamp(value: _Any) -> _datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, _datetime):
        parsed = value
    elif isinstance(value, (int, float)):
        raw = float(value)
        if raw > 10_000_000_000:
            raw /= 1000.0
        try:
            parsed = _datetime.fromtimestamp(raw, tz=_timezone.utc)
        except Exception:
            return None
    else:
        text = str(value).strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = _datetime.fromisoformat(text)
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
                    parsed = _datetime.strptime(text, fmt)
                    break
                except Exception:
                    continue
            if parsed is None:
                return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_timezone.utc)
    return parsed.astimezone(_timezone.utc)


def _normalize_positions_payload(payload: _Any) -> list[dict]:
    if not isinstance(payload, dict) or "positions" not in payload:
        raise ValueError("BROKER_POSITIONS_PAYLOAD_MALFORMED")

    positions_node = payload.get("positions")
    if positions_node is None or positions_node == "null":
        return []

    if isinstance(positions_node, dict):
        rows = positions_node.get("position")
    else:
        raise ValueError("BROKER_POSITIONS_NODE_MALFORMED")

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
        contract = _normalize_contract(
            row.get("option_symbol") or row.get("contract") or row.get("symbol")
        )
        quantity = _positive_int(row.get("quantity") or row.get("qty"))
        if contract and quantity > 0:
            normalized.append(
                {"symbol": contract, "quantity": quantity, "raw": dict(row)}
            )
    return normalized


def _fetch_authoritative_broker_positions(broker: _Any) -> list[dict]:
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
    return _normalize_positions_payload(payload)


def _order_legs(order: dict) -> list[dict]:
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


def _order_contract(order: dict) -> str:
    for key in ("option_symbol", "contract"):
        contract = _normalize_contract(order.get(key))
        if contract:
            return contract

    for leg in _order_legs(order):
        for key in ("option_symbol", "contract", "symbol"):
            contract = _normalize_contract(leg.get(key))
            if contract and _OCC_RE.fullmatch(contract):
                return contract

    symbol = _normalize_contract(order.get("symbol"))
    return symbol if _OCC_RE.fullmatch(symbol) else ""


def _order_side(order: dict) -> str:
    raw = str(order.get("side") or "").lower().replace(" ", "_").strip()
    if raw:
        return raw
    legs = _order_legs(order)
    if len(legs) == 1:
        return str(legs[0].get("side") or "").lower().replace(" ", "_").strip()
    return ""


def _order_status(order: dict) -> str:
    return str(order.get("status") or "").lower().strip()


def _order_id(order: dict) -> str:
    for key in ("id", "order_id", "broker_order_id"):
        value = str(order.get(key) or "").strip()
        if value:
            return value
    return ""


def _order_filled_qty(order: dict) -> int:
    for key in (
        "exec_quantity",
        "filled_quantity",
        "filled_qty",
        "executed_quantity",
    ):
        qty = _positive_int(order.get(key))
        if qty > 0:
            return qty

    if _order_status(order) == "filled":
        return _positive_int(order.get("quantity") or order.get("qty"))
    return 0


def _order_fill_price(order: dict) -> float:
    for key in (
        "avg_fill_price",
        "average_fill_price",
        "fill_price",
        "last_fill_price",
    ):
        price = _positive_float(order.get(key))
        if price > 0:
            return price
    return 0.0


def _order_filled_at(order: dict) -> _datetime | None:
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
        parsed = _parse_timestamp(order.get(key))
        if parsed is not None:
            return parsed
    return None


def _normalize_broker_orders(raw_orders: _Any) -> list[dict]:
    if raw_orders is None:
        return []
    if isinstance(raw_orders, dict):
        raw_orders = [raw_orders]
    if not isinstance(raw_orders, list):
        raise ValueError("BROKER_ORDERS_RESULT_MALFORMED")
    return [dict(order) for order in raw_orders if isinstance(order, dict)]


def _select_external_close_fill(
    *,
    orders: list[dict],
    position: dict,
    known_exit_order_ids: set[str],
    detected_at: _datetime,
) -> tuple[dict | None, str]:
    contract = _normalize_contract(position.get("contract"))
    opened_at = _parse_timestamp(position.get("entry_ts") or position.get("opened_at"))
    required_qty = _positive_int(
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
    matching_known = []
    external_fills = []

    for order in orders:
        if _order_contract(order) != contract:
            continue
        if _order_side(order) not in _MANUAL_CLOSE_SIDES:
            continue
        if _order_status(order) not in _FILLED_ORDER_STATUSES:
            continue

        broker_order_id = _order_id(order)
        filled_qty = _order_filled_qty(order)
        fill_price = _order_fill_price(order)
        filled_at = _order_filled_at(order)

        if not broker_order_id or filled_qty <= 0 or fill_price <= 0 or filled_at is None:
            continue
        if filled_at < opened_at:
            continue
        if (
            filled_at - detected_at
        ).total_seconds() > _MANUAL_CLOSE_FUTURE_SKEW_SEC:
            continue

        normalized = {
            "broker_order_id": broker_order_id,
            "filled_qty": filled_qty,
            "fill_price": fill_price,
            "filled_at": filled_at,
        }
        if broker_order_id in known_ids:
            matching_known.append(normalized)
        else:
            external_fills.append(normalized)

    if matching_known:
        return None, "bot_owned_exit_order_present"
    if not external_fills:
        return None, "no_exact_external_filled_exit_order"

    # Deduplicate account-order payloads by broker order id. Tradier can repeat an
    # order in wrapper/list shapes; counting the same fill twice would over-close.
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

    latest = fills[-1]
    return {
        "broker_order_id": latest["broker_order_id"],
        "broker_order_ids": [item["broker_order_id"] for item in fills],
        "filled_qty": required_qty,
        "broker_filled_qty": total_qty,
        "fill_price": round(average_fill, 6),
        "filled_ts": latest["filled_at"].isoformat(),
    }, "exact_external_broker_fill"


def _load_manual_close_state(client_id: str) -> tuple[list[dict], set[str]]:
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


class ClientRunner(_BaseClientRunner):
    def _detect_manual_closes(self):
        """Finalize externally closed positions only from exact broker fill truth."""
        now_epoch = _time.time()
        last_check = float(getattr(self, "_last_manual_close_check_ts", 0.0) or 0.0)
        if now_epoch - last_check < _MANUAL_CLOSE_INTERVAL_SEC:
            return
        self._last_manual_close_check_ts = now_epoch

        client_id = str(getattr(self, "email", "") or "").strip()
        runner_mode = str(getattr(self, "mode", "") or "").strip().lower()
        broker = getattr(self, "broker", None)

        if not client_id or runner_mode not in _VALID_EXECUTION_MODES:
            _logger.error(
                "MANUAL_CLOSE_SCAN_SKIPPED invalid runner identity client=%s mode=%s",
                client_id,
                runner_mode,
            )
            return
        if broker is None:
            return

        try:
            broker_positions = _fetch_authoritative_broker_positions(broker)
        except Exception as exc:
            # Critical safety invariant: an unavailable/malformed positions query
            # must never be interpreted as an empty account.
            _logger.error(
                "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_positions_unavailable "
                "mode=%s err=%s — no position/proof mutation",
                client_id,
                runner_mode,
                exc,
            )
            return

        broker_contract_qty: dict[str, int] = {}
        for broker_position in broker_positions:
            contract = _normalize_contract(
                broker_position.get("symbol")
                or broker_position.get("option_symbol")
                or broker_position.get("contract")
            )
            quantity = _positive_int(
                broker_position.get("quantity") or broker_position.get("qty")
            )
            if contract and quantity > 0:
                broker_contract_qty[contract] = (
                    broker_contract_qty.get(contract, 0) + quantity
                )

        try:
            open_positions, known_exit_order_ids = _load_manual_close_state(client_id)
        except Exception as exc:
            _logger.error(
                "[%s] MANUAL_CLOSE_SCAN_DB_READ_FAILED mode=%s err=%s",
                client_id,
                runner_mode,
                exc,
            )
            return

        missing_positions = [
            position
            for position in open_positions
            if _normalize_contract(position.get("contract")) not in broker_contract_qty
        ]
        if not missing_positions:
            return

        list_orders = getattr(broker, "list_orders", None)
        if not callable(list_orders):
            _logger.error(
                "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_orders_unavailable "
                "missing_positions=%s — no position/proof mutation",
                client_id,
                len(missing_positions),
            )
            return

        try:
            broker_orders = _normalize_broker_orders(list_orders())
        except Exception as exc:
            _logger.error(
                "[%s] MANUAL_CLOSE_SCAN_UNCERTAIN broker_orders_failed "
                "missing_positions=%s err=%s — no position/proof mutation",
                client_id,
                len(missing_positions),
                exc,
            )
            return

        detected_at = _datetime.fromtimestamp(now_epoch, tz=_timezone.utc)
        pm = getattr(self, "position_manager", None)
        finalizer = getattr(pm, "close_position_from_exit_fill", None)
        if not callable(finalizer):
            _logger.critical(
                "[%s] MANUAL_CLOSE_CANONICAL_FINALIZER_UNAVAILABLE "
                "missing_positions=%s",
                client_id,
                len(missing_positions),
            )
            return

        for position in missing_positions:
            position_id = str(position.get("id") or "").strip()
            contract = _normalize_contract(position.get("contract"))
            position_mode = str(position.get("execution_mode") or "").strip().lower()

            if not position_id or not contract:
                continue
            if position_mode not in _VALID_EXECUTION_MODES or position_mode != runner_mode:
                _logger.critical(
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
                _logger.info(
                    "[%s] MANUAL_CLOSE_SKIPPED_EXISTING_EXIT_OWNER pos=%s contract=%s",
                    client_id,
                    position_id,
                    contract,
                )
                continue

            fill, reason_code = _select_external_close_fill(
                orders=broker_orders,
                position=position,
                known_exit_order_ids=known_exit_order_ids,
                detected_at=detected_at,
            )
            if fill is None:
                _logger.warning(
                    "[%s] MANUAL_CLOSE_EVIDENCE_MISSING pos=%s contract=%s "
                    "reason=%s — DB remains OPEN",
                    client_id,
                    position_id,
                    contract,
                    reason_code,
                )
                continue

            broker_ids = ",".join(fill["broker_order_ids"])
            exit_reason = (
                "MANUAL_CLIENT_CLOSE_BROKER_CONFIRMED "
                f"broker_order_ids={broker_ids}"
            )
            finalized = bool(
                finalizer(
                    position_id=position_id,
                    exit_price=float(fill["fill_price"]),
                    filled_qty=int(fill["filled_qty"]),
                    filled_ts=str(fill["filled_ts"]),
                    broker_order_id=str(fill["broker_order_id"]),
                    close_source="manual_client_close_broker_fill",
                    close_confidence="HIGH",
                    exit_reason=exit_reason,
                )
            )
            if not finalized:
                _logger.critical(
                    "[%s] MANUAL_CLOSE_FINALIZE_FAILED pos=%s contract=%s "
                    "broker_order_ids=%s — DB/proof finalizer rejected fill",
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
                    _logger.warning(
                        "[%s] MANUAL_CLOSE_EXIT_ENGINE_EVICT_FAILED "
                        "pos=%s contract=%s err=%s",
                        client_id,
                        position_id,
                        contract,
                        exc,
                    )

            _logger.info(
                "[%s] MANUAL_CLOSE_FINALIZED pos=%s contract=%s exit=%.4f "
                "qty=%s broker_order_ids=%s proof_path=canonical",
                client_id,
                position_id,
                contract,
                float(fill["fill_price"]),
                int(fill["filled_qty"]),
                broker_ids,
            )


# Functions defined in the legacy module resolve ``ClientRunner`` from their own
# module globals at call time. Patch that binding so supervisor-created runners
# use the hardened subclass, not the unmodified legacy class.
_base.ClientRunner = ClientRunner
globals()["ClientRunner"] = ClientRunner
