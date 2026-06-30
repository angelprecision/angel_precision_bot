# ap/fill_monitor.py — Fill Monitor hardening shim
"""
Hardened public fill-monitor module.

The original OSM-routed implementation is preserved verbatim in
ap.fill_monitor_legacy. This shim patches the unsafe production edges while
keeping the existing public import path stable:

- no missing direction -> CALL fallback on confirmed entry fills
- recover CALL/PUT from OCC contract when orders.direction is missing
- block broker FILLED / EXIT_FILLED payloads that report filled_qty <= 0
- use an explicit data_broker for underlying_entry capture when provided
- always release the entry symbol lock on terminal ENTRY cleanup, even when
  reserved_cost cannot be reconstructed
- persist dashboard-visible last_error when filled-entry side effects fail

It intentionally does not bypass OSM, does not poll PENDING_TRIGGER, does not
submit/cancel new broker orders beyond the pre-existing pair-cancel path, and
does not mutate queue/proof_trades behavior except through the legacy module's
pre-existing exit-price sync.
"""
from __future__ import annotations

import re
from typing import Optional

from ap import fill_monitor_legacy as _legacy
from ap.fill_monitor_legacy import *  # noqa: F401,F403 - preserve public API

_OCC_SIDE_RE = re.compile(r"\d{6}([CP])")

_ORIG_CHECK_ORDER_WITH_BROKER = _legacy.check_order_with_broker
_ORIG_RELEASE_ENTRY_GUARDS = _legacy._release_entry_guards
_ORIG_CANCEL_PAIR_OPPOSITE = _legacy._cancel_pair_opposite
_ORIG_OPEN_POSITION_SAFE = _legacy._open_position_safe
_ORIG_SEED_EXIT_ENGINE = _legacy._seed_exit_engine
_ORIG_PROCESS_PENDING_ORDER = _legacy.process_pending_order
_ORIG_FILL_MONITOR_LOOP = _legacy.fill_monitor_loop
_ORIG_LEGACY_CREATE_POSITION_FROM_FILL = _legacy._legacy_create_position_from_fill


def __getattr__(name: str):
    """Expose private legacy helpers for tests/backward-compatible imports."""
    return getattr(_legacy, name)


def _resolve_order_option_side(order: dict) -> tuple[Optional[str], str]:
    """
    Resolve option thesis side without guessing.

    Priority:
      1. orders.direction / order side when already canonical CALL or PUT
      2. OCC option symbol C/P marker
      3. unresolved — caller must fail closed / quarantine

    We intentionally do NOT map BUY/SELL here. For option orders those are
    execution actions, not thesis direction, and mapping them would pollute
    CALL/PUT taxonomy.
    """
    raw = str(order.get("direction") or order.get("side") or "").strip().upper()
    if raw in {"CALL", "PUT"}:
        return raw, "order_direction"

    contract = str(
        order.get("contract")
        or order.get("option_symbol")
        or order.get("symbol")
        or ""
    ).strip().upper()
    m = _OCC_SIDE_RE.search(contract)
    if m:
        return ("CALL" if m.group(1) == "C" else "PUT"), "occ_contract"

    return None, "missing_or_unparseable"


def _with_resolved_direction(order: dict, side: str, source: str) -> dict:
    patched = dict(order)
    patched["direction"] = side
    patched["_fill_monitor_side_source"] = source
    return patched


def _broker_base_url(broker) -> str | None:
    cfg_obj = getattr(broker, "cfg", None)
    return (
        getattr(broker, "base_url", None)
        or getattr(cfg_obj, "base_url", None)
        or getattr(cfg_obj, "baseurl", None)
        or getattr(broker, "_base_url", None)
    )


def _select_quote_broker(execution_broker, data_broker=None):
    return data_broker or getattr(execution_broker, "data_broker", None) or execution_broker


def _emit_side_unresolved(order: dict, *, reason_code: str, alert_fn=None) -> None:
    client_id = str(order.get("client_id") or "default")
    payload = {
        "local_order_id": order.get("local_order_id"),
        "broker_order_id": order.get("broker_order_id"),
        "symbol": order.get("symbol"),
        "contract": order.get("contract"),
        "direction": order.get("direction"),
        "side": order.get("side"),
        "reason": "missing_or_unparseable_side",
    }
    _legacy.log.critical("[%s] %s | %s", client_id, reason_code, payload)
    _legacy.audit(client_id, "CRITICAL", reason_code, payload)
    _legacy.emit_fill_event(
        order,
        decision="ERROR",
        reason_code=reason_code,
        explanation="Confirmed broker fill could not be mapped to CALL/PUT without guessing; normal position/exit seeding blocked.",
        result={},
        extra_context=payload,
    )
    _legacy._safe_alert(
        alert_fn,
        f"[fill_monitor:{client_id}] {reason_code} | order={order.get('local_order_id')} broker={order.get('broker_order_id')}",
    )


def _record_filled_side_effect_failure(order: dict, reason: str) -> None:
    """Persist dashboard-visible order failure context without changing OSM state."""
    client_id = str(order.get("client_id") or "default")
    local_id = order.get("local_order_id")
    if not local_id:
        return

    try:
        from ap.db import conn as _fm_conn, run_with_retry as _fm_retry

        def _write_last_error():
            with _fm_conn() as c:
                c.execute(
                    "UPDATE orders SET last_error=%s, updated_ts=NOW() "
                    "WHERE client_id=%s AND local_order_id=%s",
                    (reason, client_id, local_id),
                )

        _fm_retry(_write_last_error)
    except Exception as exc:
        _legacy.log.debug(
            "[%s] failed to persist filled side-effect failure for %s: %s",
            client_id,
            local_id,
            exc,
        )

    _legacy.audit(
        client_id,
        "CRITICAL",
        reason,
        {
            "local_order_id": local_id,
            "broker_order_id": order.get("broker_order_id"),
            "symbol": order.get("symbol"),
            "contract": order.get("contract"),
        },
    )


def check_order_with_broker(broker, order: dict) -> dict:
    """Broker check wrapper that blocks impossible filled payloads before OSM."""
    result = _ORIG_CHECK_ORDER_WITH_BROKER(broker, order)
    mapped = str(result.get("status") or "UNKNOWN").upper()
    kind = str(order.get("kind") or "ENTRY").upper()

    if mapped in {"FILLED", "EXIT_FILLED"}:
        try:
            filled_qty = int(result.get("filled_qty") or 0)
        except Exception:
            filled_qty = 0

        if filled_qty <= 0:
            reason = "BROKER_FILLED_ZERO_QTY"
            client_id = str(order.get("client_id") or "default")
            payload = {
                "local_order_id": order.get("local_order_id"),
                "broker_order_id": order.get("broker_order_id"),
                "kind": kind,
                "mapped_status": mapped,
                "filled_qty": result.get("filled_qty"),
                "broker_reason": result.get("reason"),
            }
            _legacy.log.critical("[%s] %s | %s", client_id, reason, payload)
            _legacy.audit(client_id, "CRITICAL", reason, payload)
            _legacy.emit_fill_event(
                order,
                decision="ERROR",
                reason_code=reason,
                explanation="Broker reported FILLED but no positive cumulative filled quantity; OSM transition and side effects blocked pending next broker/reconciler pass.",
                result=result,
                extra_context=payload,
            )
            return {
                **result,
                "status": "ERROR",
                "filled_qty": 0,
                "reason": reason,
            }

    if mapped == "FILLED" and kind == "ENTRY":
        side, source = _resolve_order_option_side(order)
        if not side:
            _emit_side_unresolved(order, reason_code="FILLED_ORDER_SIDE_UNRESOLVED")
            return {
                **result,
                "status": "ERROR",
                "reason": "FILLED_ORDER_SIDE_UNRESOLVED",
            }
        order["direction"] = side
        order["_fill_monitor_side_source"] = source

    return result


def _release_entry_guards(order: dict):
    """Release reserved equity when known and always release the symbol lock."""
    client_id = order["client_id"]
    symbol = order.get("symbol")

    cost = None
    if order.get("reserved_cost") is not None:
        try:
            cost = float(order["reserved_cost"])
        except Exception:
            cost = None

    if cost is None:
        try:
            cost = (
                float(order.get("limit_price") or 0.0)
                * int(order.get("qty") or 0)
                * _legacy.OPT_MULTIPLIER
            )
        except Exception:
            cost = None

    if cost and cost > 0:
        release_equity(client_id, cost)
    else:
        _legacy.log.warning(
            "[%s] _release_entry_guards: cost is zero/unknown for order=%s — equity may not be fully released",
            order.get("client_id"),
            order.get("local_order_id"),
        )

    if symbol:
        release_symbol_lock(client_id, symbol)
    else:
        _legacy.log.warning(
            "[%s] _release_entry_guards: symbol missing for order=%s — symbol lock could not be released",
            order.get("client_id"),
            order.get("local_order_id"),
        )


def _cancel_pair_opposite(order: dict, broker, osm, alert_fn=None) -> None:
    side, source = _resolve_order_option_side(order)
    if not side:
        _emit_side_unresolved(order, reason_code="PAIR_CANCEL_SIDE_UNRESOLVED", alert_fn=alert_fn)
        return
    return _ORIG_CANCEL_PAIR_OPPOSITE(
        _with_resolved_direction(order, side, source),
        broker,
        osm,
        alert_fn=alert_fn,
    )


def _open_position_safe(
    pm,
    *,
    order: dict,
    result: dict,
    plan_id: str,
    signal_id: str,
    local_id: str,
    broker=None,
    quote_broker=None,
) -> Optional[str]:
    side, source = _resolve_order_option_side(order)
    if not side:
        _emit_side_unresolved(order, reason_code="POSITION_OPEN_SIDE_UNRESOLVED")
        _record_filled_side_effect_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
        return None

    effective_order = _with_resolved_direction(order, side, source)
    selected_quote_broker = _select_quote_broker(
        broker,
        quote_broker or effective_order.get("_fill_monitor_data_broker"),
    )

    if selected_quote_broker is not broker:
        _legacy.audit(
            str(order.get("client_id") or "default"),
            "INFO",
            "FILL_MONITOR_UNDERLYING_ENTRY_QUOTE_BROKER_SELECTED",
            {
                "local_order_id": order.get("local_order_id"),
                "broker_order_id": order.get("broker_order_id"),
                "symbol": order.get("symbol"),
                "contract": order.get("contract"),
                "underlying_entry_source": "data_broker",
                "quote_broker_base_url": _broker_base_url(selected_quote_broker),
                "execution_broker_base_url": _broker_base_url(broker),
                "quote_broker_is_data_broker": True,
                "side": side,
                "side_source": source,
            },
        )

    try:
        position_id = _ORIG_OPEN_POSITION_SAFE(
            pm,
            order=effective_order,
            result=result,
            plan_id=plan_id,
            signal_id=signal_id,
            local_id=local_id,
            broker=selected_quote_broker,
        )
    except Exception:
        _record_filled_side_effect_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
        raise

    if not position_id:
        _record_filled_side_effect_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
    return position_id


def _seed_exit_engine(exit_engine, position_id: str, order: dict, result: dict, signal_id: str):
    side, source = _resolve_order_option_side(order)
    if not side:
        _emit_side_unresolved(order, reason_code="EXIT_ENGINE_SEED_SIDE_UNRESOLVED")
        return
    return _ORIG_SEED_EXIT_ENGINE(
        exit_engine,
        position_id,
        _with_resolved_direction(order, side, source),
        result,
        signal_id,
    )


def process_pending_order(
    broker,
    order: dict,
    osm=None,
    pm=None,
    exit_engine=None,
    alert_fn=None,
    data_broker=None,
):
    effective_order = dict(order)
    if data_broker is not None:
        effective_order["_fill_monitor_data_broker"] = data_broker
    return _ORIG_PROCESS_PENDING_ORDER(
        broker,
        effective_order,
        osm=osm,
        pm=pm,
        exit_engine=exit_engine,
        alert_fn=alert_fn,
    )


def fill_monitor_loop(
    broker,
    poll_seconds: float = 10.0,
    osm=None,
    pm=None,
    exit_engine=None,
    stop_event=None,
    client_id: str | None = None,
    alert_fn=None,
    data_broker=None,
):
    if data_broker is not None:
        try:
            setattr(broker, "data_broker", data_broker)
        except Exception:
            _legacy.log.debug("fill_monitor_loop could not attach data_broker to execution broker")
    return _ORIG_FILL_MONITOR_LOOP(
        broker,
        poll_seconds=poll_seconds,
        osm=osm,
        pm=pm,
        exit_engine=exit_engine,
        stop_event=stop_event,
        client_id=client_id,
        alert_fn=alert_fn,
    )


def _legacy_create_position_from_fill(order: dict, avg_fill_price: float, filled_qty: int):
    side, source = _resolve_order_option_side(order)
    if not side:
        _emit_side_unresolved(order, reason_code="LEGACY_POSITION_SIDE_UNRESOLVED")
        raise RuntimeError("LEGACY_POSITION_SIDE_UNRESOLVED")
    return _ORIG_LEGACY_CREATE_POSITION_FROM_FILL(
        _with_resolved_direction(order, side, source),
        avg_fill_price,
        filled_qty,
    )


# Patch legacy module globals so functions defined in ap.fill_monitor_legacy keep
# resolving their internal helper references to the hardened implementations.
_legacy.check_order_with_broker = check_order_with_broker
_legacy._release_entry_guards = _release_entry_guards
_legacy._cancel_pair_opposite = _cancel_pair_opposite
_legacy._open_position_safe = _open_position_safe
_legacy._seed_exit_engine = _seed_exit_engine
_legacy.process_pending_order = process_pending_order
_legacy.fill_monitor_loop = fill_monitor_loop
_legacy._legacy_create_position_from_fill = _legacy_create_position_from_fill
