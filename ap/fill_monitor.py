# ap/fill_monitor.py — Fill Monitor (OSM-routed, rich final)
"""
Fill Monitor — broker reconciliation poller + side-effect orchestrator.

This file preserves the production behaviors from the larger legacy monitor:
- audit_log writes
- structured observability events
- entry equity/symbol-lock release
- 1-1 pair cancel after confirmed entry fill
- optional broker-side standing stop after local position persistence
- exit price/dashboard sync
- legacy fallback helpers, but disabled in production unless explicitly allowed

Architecture:
- APOrderStateMachine owns order lifecycle truth.
- APPositionManager owns position truth.
- APExitEngine owns exit behavior, scale-outs, runner state, and full-close logic.
- Fill monitor polls broker reality, maps broker status to canonical OSM states,
  enforces cumulative fill sanity, calls OSM, then runs side effects only after
  successful OSM confirmation.

Critical safety rules:
- MUST filter by client_id.
- MUST support stop_event so old runners do not become zombie reconcilers.
- MUST NOT poll non-broker PENDING_TRIGGER watch plans.
- MUST NOT use legacy direct DB lifecycle writes in production unless
  ALLOW_LEGACY_FILL_MONITOR=1.
- MUST persist local position before optional broker-side standing stop.
- MUST cancel broker order before marking pair-opposite local order CANCELED.
- filled_qty MUST be cumulative broker fill quantity, not incremental.
"""

from __future__ import annotations

import inspect
import os
import time
from datetime import datetime, timezone
from typing import Optional

from ap.trace import trace_gate
from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso, json_dumps
from ap.logger import get_logger
from ap.config import Config
from ap.state import release_equity, release_symbol_lock
from ap.broker import BrokerAdapter
from ap.observability import emit_decision_event, get_git_commit

log = get_logger("ap.fill_monitor")
cfg = Config()

OPT_MULTIPLIER = 100
RUN_ID = os.getenv("AP_RUN_ID", "unknown")
STRATEGY_VERSION = os.getenv("AP_STRATEGY_VERSION", "ap_live_beta")
GIT_COMMIT = get_git_commit()

ALLOW_LEGACY_FILL_MONITOR = (
    os.getenv("ALLOW_LEGACY_FILL_MONITOR", "0").strip().lower()
    in {"1", "true", "yes", "on"}
)

ACTIVE_BROKER_STATUSES = {
    "OPEN",
    "PENDING",
    "ACCEPTED",
    "WORKING",
    "LIVE",
    "QUEUED",
    "HELD",
    "ROUTED",
    "NEW",
    "PENDING_REVIEW",
}

TERMINAL_FAILURE_STATUSES = {"REJECTED", "CANCELED", "EXPIRED"}

UNKNOWN_ERROR_ESCALATE_AFTER = int(os.getenv("FILL_MONITOR_UNKNOWN_ERROR_ESCALATE_AFTER", "3"))
FILL_ANOMALY_STATUS = os.getenv("FILL_MONITOR_ANOMALY_STATUS", "BROKER_FILL_ANOMALY").strip().upper()

_BROKER_STATE_ANOMALY_COUNTS: dict[str, int] = {}


def _order_count_key(client_id: str, local_order_id: str, broker_order_id: str | None = None) -> str:
    return f"{client_id}:{local_order_id}:{broker_order_id or ''}"


def _reset_broker_anomaly_count(client_id: str, local_order_id: str, broker_order_id: str | None = None) -> None:
    _BROKER_STATE_ANOMALY_COUNTS.pop(_order_count_key(client_id, local_order_id, broker_order_id), None)


def _increment_broker_anomaly_count(client_id: str, local_order_id: str, broker_order_id: str | None = None) -> int:
    key = _order_count_key(client_id, local_order_id, broker_order_id)
    count = int(_BROKER_STATE_ANOMALY_COUNTS.get(key, 0)) + 1
    _BROKER_STATE_ANOMALY_COUNTS[key] = count
    return count


# =============================================================================
# AUDIT / OBSERVABILITY
# =============================================================================

def audit(client_id: str, level: str, event: str, payload: dict):
    """Best-effort audit write. Audit failures must never block reconciliation."""
    try:
        with conn() as c:
            run_with_retry(
                lambda: c.execute(
                    "INSERT INTO audit_log (ts, level, event, payload, client_id) "
                    "VALUES (%s,%s,%s,%s,%s)",
                    (now_utc_iso(), level, event, json_dumps(payload), client_id),
                )
            )
    except Exception as exc:
        log.warning(
            "Audit write failed non-fatal | client=%s event=%s error=%s",
            client_id,
            event,
            exc,
        )


def emit_fill_event(
    order: dict,
    *,
    decision: str,
    reason_code: str,
    explanation: str,
    result: dict | None = None,
    stage: str = "fill_monitor",
    extra_inputs: dict | None = None,
    extra_context: dict | None = None,
):
    """Emit structured fill-monitor observability without blocking reconciliation."""
    try:
        result = result or {}
        emit_decision_event(
            run_id=RUN_ID,
            candidate_id=str(order.get("signal_id") or order.get("local_order_id") or ""),
            trade_id=str(order.get("local_order_id") or ""),
            position_id=str(order.get("position_id") or ""),
            client_id=str(order.get("client_id") or "default"),
            stage=stage,
            decision=decision,
            reason_code=reason_code,
            explanation=explanation,
            symbol=order.get("symbol"),
            contract=order.get("contract"),
            setup_type=order.get("pattern"),
            timeframe=order.get("timeframe"),
            strategy_version=STRATEGY_VERSION,
            git_commit=GIT_COMMIT,
            inputs={
                "kind": order.get("kind"),
                "broker_order_id": order.get("broker_order_id"),
                "local_order_id": order.get("local_order_id"),
                "filled_qty": result.get("filled_qty"),
                "avg_fill": result.get("avg_fill"),
                "broker_reason": result.get("reason"),
                **(extra_inputs or {}),
            },
            context=extra_context or {},
        )
    except Exception as e:
        log.debug("Fill monitor observability emit failed (non-critical): %s", e)


# =============================================================================
# DB QUERY — CLIENT-SAFE BROKER-BACKED ORDERS ONLY
# =============================================================================

def get_pending_orders(client_id: str) -> list[dict]:
    """
    Return broker-backed orders for one client.

    PENDING_TRIGGER is intentionally excluded because those are watcher plans,
    not live broker orders. They should only enter this monitor after the watcher
    submits to broker and OSM moves them to SUBMITTED.
    """
    if not client_id:
        raise ValueError("get_pending_orders requires client_id")

    with conn() as c:
        rows = run_with_retry(
            lambda: c.execute(
                """
                SELECT
                    client_id,
                    local_order_id,
                    broker_order_id,
                    position_id,
                    kind,
                    symbol,
                    contract,
                    direction,
                    qty,
                    limit_price,
                    reserved_cost,
                    status,
                    created_ts,
                    plan_id,
                    signal_id,
                    tier,
                    score,
                    pattern,
                    stop_underlying,
                    target_underlying,
                    trigger_price,
                    timeframe,
                    filled_qty,
                    fill_price
                FROM orders
                WHERE client_id = %s
                  AND kind IN ('ENTRY','EXIT')
                  AND status IN (
                    'SUBMITTED',
                    'ACKNOWLEDGED',
                    'PARTIAL_FILL',
                    'EXIT_SUBMITTED',
                    'EXIT_ACKNOWLEDGED',
                    'EXIT_PARTIAL_FILL'
                  )
                  AND broker_order_id IS NOT NULL
                  AND broker_order_id != ''
                  AND broker_order_id != 'N/A'
                ORDER BY created_ts ASC
                """,
                (client_id,),
            ).fetchall()
        )
    return [dict(r) for r in rows]


# =============================================================================
# BROKER CHECK
# =============================================================================

def check_order_with_broker(broker: BrokerAdapter, order: dict) -> dict:
    """
    Query broker for actual order status.

    Contract:
    - returned filled_qty is broker cumulative filled quantity, not incremental
    - raw order quantity is used as fallback only when broker status is truly FILLED
    - active broker statuses map to ACKNOWLEDGED / EXIT_ACKNOWLEDGED
    """
    broker_order_id = order.get("broker_order_id")
    if not broker_order_id or broker_order_id == "N/A":
        return {
            "status": "UNKNOWN",
            "filled_qty": 0,
            "avg_fill": 0.0,
            "reason": "NO_BROKER_ID",
            "raw": {},
        }

    kind = (order.get("kind") or "ENTRY").upper()

    try:
        raw = broker.get_order(broker_order_id)
        status = (raw.get("status") or "").upper()

        if kind == "EXIT":
            status_map = {
                "FILLED": "EXIT_FILLED",
                "PARTIALLY_FILLED": "EXIT_PARTIAL_FILL",
                "PARTIAL": "EXIT_PARTIAL_FILL",
                "CANCELED": "CANCELED",
                "CANCELLED": "CANCELED",
                "REJECTED": "REJECTED",
                "EXPIRED": "EXPIRED",
            }
            our = "EXIT_ACKNOWLEDGED" if status in ACTIVE_BROKER_STATUSES else status_map.get(status, "UNKNOWN")
        else:
            status_map = {
                "FILLED": "FILLED",
                "PARTIALLY_FILLED": "PARTIAL_FILL",
                "PARTIAL": "PARTIAL_FILL",
                "CANCELED": "CANCELED",
                "CANCELLED": "CANCELED",
                "REJECTED": "REJECTED",
                "EXPIRED": "EXPIRED",
            }
            our = "ACKNOWLEDGED" if status in ACTIVE_BROKER_STATUSES else status_map.get(status, "UNKNOWN")

        explicit_filled_qty = raw.get("exec_quantity") or raw.get("filled_quantity")
        if explicit_filled_qty is not None:
            filled_qty = int(explicit_filled_qty or 0)
        elif our in ("FILLED", "EXIT_FILLED"):
            filled_qty = int(raw.get("quantity") or 0)
        else:
            filled_qty = 0

        avg_fill = float(raw.get("avg_fill_price") or raw.get("price") or 0.0)

        return {
            "status": our,
            "filled_qty": filled_qty,
            "avg_fill": avg_fill,
            "reason": raw.get("reason") or status,
            "raw": raw,
        }

    except Exception as e:
        audit(
            str(order.get("client_id") or "default"),
            "ERROR",
            "FILL_CHECK_FAILED",
            {
                "error": str(e),
                "broker_order_id": broker_order_id,
                "local_order_id": order.get("local_order_id"),
            },
        )
        return {"status": "ERROR", "filled_qty": 0, "avg_fill": 0.0, "reason": str(e), "raw": {}}


# =============================================================================
# ENTRY GUARDS / LOCK RELEASE
# =============================================================================

def _release_entry_guards(order: dict):
    """Release reserved equity and symbol lock for ENTRY orders."""
    client_id = order["client_id"]
    symbol = order["symbol"]

    cost = None
    if order.get("reserved_cost") is not None:
        try:
            cost = float(order["reserved_cost"])
        except Exception:
            cost = None

    if cost is None:
        cost = (
            float(order.get("limit_price") or 0.0)
            * int(order.get("qty") or 0)
            * OPT_MULTIPLIER
        )

    if cost and cost > 0:
        release_equity(client_id, cost)
        release_symbol_lock(client_id, symbol)


# =============================================================================
# PAIR MANAGER HELPER — BROKER CANCEL FIRST
# =============================================================================

def _cancel_pair_opposite(order: dict, broker: BrokerAdapter, osm) -> None:
    """
    On ENTRY fill: cancel the opposite side of a 1-1 pair.

    Broker cancel is attempted before local CANCELED transition.
    If broker id cannot be resolved, local order is not marked canceled.
    """
    if not osm:
        return

    try:
        from ap.signal_pair_manager import get_pair_manager

        pair_manager = get_pair_manager()
        ticker = (order.get("symbol") or "").upper()
        side = (order.get("direction") or "CALL").upper()
        filled_local_id = order.get("local_order_id", "")

        cancel_local_id = pair_manager.on_fill(
            ticker=ticker,
            side=side,
            local_order_id=filled_local_id,
        )

        if not cancel_local_id:
            return

        log.warning(
            "[%s] 1-1 PAIR FILL — canceling opposite local_order_id=%s",
            ticker,
            cancel_local_id,
        )

        resolved_broker_id = None
        try:
            existing = osm.get_order(cancel_local_id)
            resolved_broker_id = (existing or {}).get("broker_order_id")
        except Exception as exc:
            log.warning("[%s] Could not resolve opposite broker id: %s", ticker, exc)

        if not resolved_broker_id:
            log.warning(
                "[%s] Pair cancel skipped — no broker_order_id for local_order_id=%s",
                ticker,
                cancel_local_id,
            )
            return

        try:
            if hasattr(broker, "cancel_order"):
                broker.cancel_order(resolved_broker_id)
            else:
                _cancel_with_session(broker, resolved_broker_id)
        except Exception as exc:
            log.warning(
                "[%s] Broker pair cancel failed | local=%s broker=%s error=%s",
                ticker,
                cancel_local_id,
                resolved_broker_id,
                exc,
            )
            return

        ok = osm.transition(
            cancel_local_id,
            "CANCELED",
            broker_order_id=resolved_broker_id,
            last_error="pair_fill_cancel_broker_confirmed",
        )
        if ok:
            log.info(
                "[%s] Opposite side CANCELED | local=%s broker=%s",
                ticker,
                cancel_local_id,
                resolved_broker_id,
            )

    except ImportError:
        pass
    except Exception as exc:
        log.debug("Pair manager cancel failed (non-critical): %s", exc)


def _cancel_with_session(broker: BrokerAdapter, broker_order_id: str):
    base_url = (
        getattr(broker, "base_url", None)
        or getattr(getattr(broker, "cfg", None), "base_url", None)
        or getattr(broker, "_base_url", None)
    )
    account_id = (
        getattr(broker, "account_id", None)
        or getattr(getattr(broker, "cfg", None), "account_id", None)
        or getattr(broker, "_account_id", None)
    )
    if not base_url or not account_id or not getattr(broker, "session", None):
        raise RuntimeError("broker cancel not available: missing base_url/account_id/session")

    resp = broker.session.delete(
        f"{base_url}/v1/accounts/{account_id}/orders/{broker_order_id}",
        headers={"Accept": "application/json"},
        timeout=10,
    )
    if getattr(resp, "status_code", 500) >= 300:
        raise RuntimeError(f"broker cancel failed HTTP {resp.status_code}: {getattr(resp, 'text', '')[:200]}")


# =============================================================================
# SAFE HELPERS
# =============================================================================

def _call_with_supported_kwargs(fn, **kwargs):
    """Call a function with only supported keyword args for compatibility."""
    sig = inspect.signature(fn)
    supported = {k: v for k, v in kwargs.items() if k in sig.parameters}
    return fn(**supported)


def _get_existing_position_by_order(pm, local_order_id: str, broker_order_id: Optional[str]):
    for method_name, arg in (
        ("get_position_by_local_order", local_order_id),
        ("get_position_by_broker_order", broker_order_id),
    ):
        if not arg:
            continue
        method = getattr(pm, method_name, None)
        if callable(method):
            try:
                found = method(arg)
                if found:
                    return found
            except Exception:
                pass
    return None


def _open_position_safe(pm, *, order: dict, result: dict, plan_id: str, signal_id: str, local_id: str) -> Optional[str]:
    """Open position with idempotency keys when the PM supports them."""
    existing = _get_existing_position_by_order(pm, local_id, order.get("broker_order_id"))
    if existing:
        return existing.get("id") or existing.get("position_id")

    kwargs = {
        "plan_id": plan_id,
        "signal_id": signal_id,
        "ticker": (order.get("symbol") or "").upper(),
        "contract": order.get("contract") or order.get("symbol") or "",
        "side": (order.get("direction") or "CALL").upper(),
        "qty": int(result.get("filled_qty") or order.get("qty") or 0),
        "entry_price": float(result.get("avg_fill") or 0.0),
        "tier": str(order.get("tier") or "B"),
        "score": float(order.get("score") or 0),
        "pattern": str(order.get("pattern") or ""),
        "stop_underlying": float(order.get("stop_underlying")) if order.get("stop_underlying") is not None else None,
        "target_underlying": float(order.get("target_underlying")) if order.get("target_underlying") is not None else None,
        "local_order_id": local_id,
        "broker_order_id": order.get("broker_order_id"),
    }

    try:
        return pm.open_position(**kwargs)
    except TypeError:
        # Compatibility with older PM signature that does not yet accept local/broker order ids.
        return _call_with_supported_kwargs(pm.open_position, **kwargs)


def _place_standing_stop_best_effort(
    *,
    broker: BrokerAdapter,
    order: dict,
    qty: int,
    entry_price: float,
):
    """Optional secondary broker-side stop, after local position persistence."""
    try:
        if qty <= 0 or entry_price <= 0:
            return

        stop_pct = float(os.getenv("BROKER_STANDING_STOP_PCT", "0.30"))
        stop_px = round(entry_price * (1 - stop_pct), 2)
        contract = order.get("contract", "")
        ticker = (order.get("symbol") or "").upper()

        if hasattr(broker, "place_stop_order"):
            broker.place_stop_order(symbol=contract, qty=qty, stop_price=stop_px)
            log.info("[%s] Standing stop placed via broker helper @ $%.2f", ticker, stop_px)
            return

        base_url = (
            getattr(broker, "base_url", None)
            or getattr(getattr(broker, "cfg", None), "base_url", None)
            or getattr(broker, "_base_url", None)
        )
        account_id = (
            getattr(broker, "account_id", None)
            or getattr(getattr(broker, "cfg", None), "account_id", None)
            or getattr(broker, "_account_id", None)
        )

        if not base_url or not account_id or not getattr(broker, "session", None):
            log.warning("[%s] Standing stop skipped — broker stop interface unavailable", ticker)
            return

        resp = broker.session.post(
            f"{base_url}/v1/accounts/{account_id}/orders",
            data={
                "class": "option",
                "option_symbol": contract,
                "side": "sell_to_close",
                "quantity": qty,
                "type": "stop",
                "stop": stop_px,
                "duration": "gtc",
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )

        if resp.status_code < 300:
            stop_data = (resp.json() or {}).get("order", {}) or {}
            stop_id = stop_data.get("id", "?")
            stop_stat = stop_data.get("status", "unknown")
            log.info(
                "[%s] Standing stop placed @ $%.2f | broker_stop=%s status=%s",
                ticker,
                stop_px,
                stop_id,
                stop_stat,
            )
        else:
            err_body = getattr(resp, "text", "")[:200]
            log.warning("[%s] Standing stop FAILED — exit engine sole protection | %s", ticker, err_body)
            audit(
                order["client_id"],
                "WARNING",
                "STOP_ORDER_FAILED",
                {
                    "local_order_id": order.get("local_order_id"),
                    "ticker": ticker,
                    "stop_px": stop_px,
                    "body": err_body,
                },
            )

    except Exception as exc:
        log.warning("[%s] Standing stop placement error: %s", order.get("symbol", "?"), exc)


def _seed_exit_engine(exit_engine, position_id: str, order: dict, result: dict, signal_id: str):
    if not exit_engine or not position_id:
        return

    try:
        getter = getattr(exit_engine, "get_position", None)
        if callable(getter) and getter(position_id):
            return
    except Exception:
        pass

    try:
        if hasattr(exit_engine, "seed_position"):
            exit_engine.seed_position(position_id, order, result)
            return
    except Exception as exc:
        log.debug("exit_engine.seed_position failed; trying add_position path: %s", exc)

    try:
        from ap_exit_engine import ManagedPosition as _MP

        mp = _MP(
            ticker=(order.get("symbol") or "").upper(),
            option_symbol=order.get("contract") or order.get("symbol") or "",
            side=(order.get("direction") or "CALL").upper(),
            quantity=int(result.get("filled_qty") or order.get("qty") or 0),
            entry_price=float(result.get("avg_fill") or 0.0),
            underlying_entry=float(order.get("trigger_price") or order.get("underlying_entry") or 0),
            underlying_target=float(order.get("target_underlying") or order.get("underlying_target") or 0),
            underlying_stop=float(order.get("stop_underlying") or order.get("underlying_stop") or 0),
        )
        mp.position_id = position_id
        mp.client_id = str(order.get("client_id") or "")
        mp.signal_id = signal_id
        mp.signal = {
            "signal_id": signal_id,
            "pattern": str(order.get("pattern") or ""),
            "tier": str(order.get("tier") or "B"),
            "score": float(order.get("score") or 0),
            "timeframe": str(order.get("timeframe") or "1d"),
            "side": (order.get("direction") or "CALL").upper(),
        }
        exit_engine.add_position(mp)
        log.info("[%s] Exit engine seeded for pos=%s", order.get("client_id"), position_id)
    except Exception as exc:
        log.error("[%s] exit_engine.add_position failed: %s", order.get("client_id"), exc)


# =============================================================================
# BROKER FILL SANITY / ANOMALY ESCALATION
# =============================================================================

def _sanitize_cumulative_filled(order: dict, result: dict) -> tuple[int, bool]:
    """Clamp broker cumulative fill to local order qty and audit impossible broker values."""
    client_id = str(order.get("client_id") or "default")
    local_id = str(order.get("local_order_id") or "")
    broker_id = order.get("broker_order_id")

    raw_filled = int(result.get("filled_qty") or 0)
    order_qty = int(order.get("qty") or 0)

    if order_qty > 0 and raw_filled > order_qty:
        clamped = order_qty
        log.critical(
            "[%s] Broker overfill anomaly clamped | order=%s broker=%s raw_filled=%s order_qty=%s",
            client_id,
            local_id,
            broker_id,
            raw_filled,
            order_qty,
        )
        result["filled_qty_raw"] = raw_filled
        result["filled_qty"] = clamped
        audit(
            client_id,
            "CRITICAL",
            "BROKER_OVERFILL_CLAMPED",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "raw_filled_qty": raw_filled,
                "clamped_filled_qty": clamped,
                "order_qty": order_qty,
                "broker_reason": result.get("reason"),
            },
        )
        emit_fill_event(
            order,
            decision="ERROR",
            reason_code="BROKER_OVERFILL_CLAMPED",
            explanation="Broker returned cumulative filled quantity greater than local order quantity; clamped before OSM/PM side effects.",
            result=result,
            extra_context={
                "raw_filled_qty": raw_filled,
                "clamped_filled_qty": clamped,
                "order_qty": order_qty,
            },
        )
        return clamped, True

    return raw_filled, False


def _mark_broker_fill_anomaly(osm, order: dict, *, reason: str, mapped: str | None = None) -> bool:
    """Best-effort special OSM quarantine transition for impossible broker/fill data."""
    if not osm:
        return False

    local_id = order.get("local_order_id")
    broker_id = order.get("broker_order_id")
    try:
        return bool(
            osm.transition(
                local_id,
                FILL_ANOMALY_STATUS,
                broker_order_id=broker_id,
                last_error=reason,
            )
        )
    except Exception as exc:
        log.error(
            "[%s] OSM anomaly transition failed | order=%s status=%s mapped=%s error=%s",
            order.get("client_id"),
            local_id,
            FILL_ANOMALY_STATUS,
            mapped,
            exc,
        )
        return False

# =============================================================================
# CORE — PROCESS ONE PENDING ORDER
# =============================================================================

def process_pending_order(
    broker: BrokerAdapter,
    order: dict,
    osm=None,
    pm=None,
    exit_engine=None,
):
    if osm is None and not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("fill_monitor requires OSM unless ALLOW_LEGACY_FILL_MONITOR=1")

    client_id = order["client_id"]
    local_id = order["local_order_id"]
    broker_id = order.get("broker_order_id")
    kind = (order.get("kind") or "ENTRY").upper()

    result = check_order_with_broker(broker, order)
    mapped = result.get("status", "UNKNOWN")

    prev_filled = int(order.get("filled_qty") or 0)
    new_filled, overfill_clamped = _sanitize_cumulative_filled(order, result)

    if mapped not in ("UNKNOWN", "ERROR"):
        _reset_broker_anomaly_count(client_id, local_id, broker_id)

    if new_filled < prev_filled:
        log.critical(
            "[%s] Fill regression blocked | order=%s broker=%s prev=%s new=%s",
            client_id,
            local_id,
            broker_id,
            prev_filled,
            new_filled,
        )
        if osm:
            osm.increment_retry(local_id)
        audit(
            client_id,
            "ERROR",
            "FILL_REGRESSION_BLOCKED",
            {"local_order_id": local_id, "broker_order_id": broker_id, "prev": prev_filled, "new": new_filled},
        )
        _mark_broker_fill_anomaly(
            osm,
            order,
            reason=f"broker fill regression prev={prev_filled} new={new_filled}",
            mapped=mapped,
        )
        return

    # ── FILLED / EXIT_FILLED ────────────────────────────────────────────────
    if mapped in ("FILLED", "EXIT_FILLED"):
        emit_fill_event(
            order,
            decision="CONFIRMED",
            reason_code="ORDER_FILLED" if kind == "ENTRY" else "EXIT_FILLED",
            explanation=f"{kind} filled via broker",
            result=result,
            extra_context={"osm_status": mapped},
        )

        ok = False
        if osm:
            try:
                ok = osm.transition(
                    local_id,
                    mapped,
                    filled_qty=new_filled,
                    fill_price=result.get("avg_fill"),
                    broker_order_id=broker_id,
                )
            except Exception as exc:
                log.error("[%s] OSM transition %s failed for %s: %s", client_id, mapped, local_id, exc)
        else:
            _legacy_update_order_status(local_id, "FILLED", filled_qty=new_filled)
            ok = True

        if ok and kind == "ENTRY" and pm:
            position_id = None
            try:
                plan_id = order.get("plan_id") or order.get("signal_id") or local_id
                signal_id = order.get("signal_id") or local_id
                ticker = (order.get("symbol") or "").upper()
                qty = int(new_filled or order.get("qty") or 0)
                price = float(result.get("avg_fill") or 0.0)

                trace_gate(
                    str(signal_id),
                    ticker,
                    "ORDER_FILLED",
                    "PASS",
                    reason="entry_filled",
                    trigger_price=price,
                    contracts=qty,
                    side=(order.get("direction") or "CALL").upper(),
                    kind="ENTRY",
                    tier=str(order.get("tier") or "B"),
                    plan_id=str(plan_id or ""),
                )

                # Pair cancel is broker-first/local-second.
                _cancel_pair_opposite(order, broker, osm)

                # Persist local position BEFORE placing optional standing stop.
                position_id = _open_position_safe(
                    pm,
                    order=order,
                    result=result,
                    plan_id=plan_id,
                    signal_id=signal_id,
                    local_id=local_id,
                )

                # Secondary broker-side stop is best-effort only.
                _place_standing_stop_best_effort(
                    broker=broker,
                    order=order,
                    qty=qty,
                    entry_price=price,
                )

                _seed_exit_engine(exit_engine, position_id, order, result, signal_id)

            except Exception as exc:
                log.error("[%s] ENTRY fill side-effects failed for %s: %s", client_id, local_id, exc)

        elif ok and kind == "EXIT":
            _sync_exit_price(order, result)

        if kind == "ENTRY":
            _release_entry_guards(order)

        audit(
            client_id,
            "INFO",
            "ORDER_FILLED",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "kind": kind,
                "osm_status": mapped,
                "filled_qty": new_filled,
                "avg_fill": result.get("avg_fill"),
            },
        )
        return

    # ── PARTIAL FILL / EXIT_PARTIAL_FILL ─────────────────────────────────
    if mapped in ("PARTIAL_FILL", "EXIT_PARTIAL_FILL"):
        emit_fill_event(
            order,
            decision="PARTIAL_FILL",
            reason_code="ORDER_PARTIAL_FILL" if kind == "ENTRY" else "EXIT_PARTIAL_FILL",
            explanation=f"{kind} partially filled via broker",
            result=result,
            extra_context={"osm_status": mapped},
        )

        if osm:
            try:
                current_status = str(order.get("status") or "").upper()
                if current_status == mapped and current_status in ("PARTIAL_FILL", "EXIT_PARTIAL_FILL"):
                    osm.apply_fill_update(
                        local_order_id=local_id,
                        cumulative_filled=new_filled,
                        fill_price=result.get("avg_fill"),
                        broker_order_id=broker_id,
                    )
                else:
                    osm.transition(
                        local_id,
                        mapped,
                        filled_qty=new_filled,
                        fill_price=result.get("avg_fill"),
                        broker_order_id=broker_id,
                    )
            except Exception as exc:
                log.error("[%s] OSM partial update %s failed for %s: %s", client_id, mapped, local_id, exc)
        else:
            _legacy_update_order_status(local_id, "PARTIAL_FILL", filled_qty=new_filled)

        audit(
            client_id,
            "INFO",
            "ORDER_PARTIAL",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "kind": kind,
                "osm_status": mapped,
                "filled_qty": new_filled,
                "total_qty": int(order.get("qty") or 0),
            },
        )
        return

    # ── ACKNOWLEDGED / BROKER ACTIVE ─────────────────────────────────────
    if mapped in ("ACKNOWLEDGED", "EXIT_ACKNOWLEDGED"):
        if osm:
            try:
                current_status = str(order.get("status") or "").upper()
                if current_status != mapped:
                    osm.transition(local_id, mapped, broker_order_id=broker_id)
            except Exception as exc:
                log.error("[%s] OSM ack transition %s failed for %s: %s", client_id, mapped, local_id, exc)

        _audit_long_pending(order, kind, client_id, local_id, broker_id)
        return

    # ── TERMINAL FAILURES ────────────────────────────────────────────────
    if mapped in TERMINAL_FAILURE_STATUSES:
        emit_fill_event(
            order,
            decision="REJECT",
            reason_code=f"ORDER_{mapped}",
            explanation=f"{kind} terminal broker status: {mapped} — {result.get('reason')}",
            result=result,
            extra_context={"terminal_status": mapped},
        )

        if osm:
            try:
                osm.transition(
                    local_id,
                    mapped,
                    broker_order_id=broker_id,
                    last_error=result.get("reason"),
                )
            except Exception as exc:
                log.error("[%s] OSM terminal transition %s failed for %s: %s", client_id, mapped, local_id, exc)
        else:
            _legacy_update_order_status(local_id, mapped, error=result.get("reason"))

        # IMPORTANT: no direct positions table mutation here.
        # EXIT failure repair is handled by OSM + exit-engine hooks.

        if kind == "ENTRY":
            _release_entry_guards(order)

        audit(
            client_id,
            "WARNING",
            f"ORDER_{mapped}",
            {"local_order_id": local_id, "broker_order_id": broker_id, "kind": kind, "reason": result.get("reason")},
        )
        return

    # ── UNKNOWN / ERROR ──────────────────────────────────────────────────
    if mapped in ("UNKNOWN", "ERROR"):
        emit_fill_event(
            order,
            decision="ERROR" if mapped == "ERROR" else "ALERT",
            reason_code="BROKER_FILL_CHECK_ERROR" if mapped == "ERROR" else "BROKER_STATUS_UNKNOWN",
            explanation=f"Broker fill check unresolved: {result.get('reason')}",
            result=result,
        )
        log.warning(
            "[%s] Unresolved broker state | order=%s broker=%s status=%s reason=%s",
            client_id,
            local_id,
            broker_id,
            mapped,
            result.get("reason"),
        )
        anomaly_count = _increment_broker_anomaly_count(client_id, local_id, broker_id)

        if osm:
            osm.increment_retry(local_id)

        audit(
            client_id,
            "ERROR" if mapped == "ERROR" else "WARNING",
            "ORDER_CHECK_ERROR" if mapped == "ERROR" else "ORDER_STATUS_UNKNOWN",
            {
                "local_order_id": local_id,
                "broker_order_id": broker_id,
                "reason": result.get("reason"),
                "consecutive_count": anomaly_count,
                "escalate_after": UNKNOWN_ERROR_ESCALATE_AFTER,
            },
        )

        if anomaly_count >= UNKNOWN_ERROR_ESCALATE_AFTER:
            reason = (
                f"consecutive broker {mapped} states reached {anomaly_count}/"
                f"{UNKNOWN_ERROR_ESCALATE_AFTER}: {result.get('reason')}"
            )
            log.critical(
                "[%s] Broker state anomaly escalation | order=%s broker=%s status=%s count=%s reason=%s",
                client_id,
                local_id,
                broker_id,
                mapped,
                anomaly_count,
                result.get("reason"),
            )
            emit_fill_event(
                order,
                decision="ERROR",
                reason_code="BROKER_FILL_ANOMALY",
                explanation=reason,
                result=result,
                extra_context={
                    "mapped_status": mapped,
                    "consecutive_count": anomaly_count,
                    "escalate_after": UNKNOWN_ERROR_ESCALATE_AFTER,
                    "anomaly_status": FILL_ANOMALY_STATUS,
                },
            )
            audit(
                client_id,
                "CRITICAL",
                "BROKER_FILL_ANOMALY_ESCALATED",
                {
                    "local_order_id": local_id,
                    "broker_order_id": broker_id,
                    "mapped_status": mapped,
                    "reason": result.get("reason"),
                    "consecutive_count": anomaly_count,
                    "anomaly_status": FILL_ANOMALY_STATUS,
                },
            )
            _mark_broker_fill_anomaly(osm, order, reason=reason, mapped=mapped)

        return

    # Fallback guard
    log.warning(
        "[%s] Unhandled broker mapped status | order=%s broker=%s mapped=%s",
        client_id,
        local_id,
        broker_id,
        mapped,
    )
    if osm:
        osm.increment_retry(local_id)


def _audit_long_pending(order: dict, kind: str, client_id: str, local_id: str, broker_id: str):
    try:
        created_raw = order.get("created_ts")
        if isinstance(created_raw, str):
            created = datetime.fromisoformat(created_raw)
        else:
            created = created_raw
        if not created:
            return
        age = (datetime.now(timezone.utc) - created).total_seconds()
        if age > 300:
            audit(
                client_id,
                "WARNING",
                "ORDER_PENDING_LONG",
                {"local_order_id": local_id, "broker_order_id": broker_id, "kind": kind, "age_seconds": age},
            )
    except Exception:
        pass


def _sync_exit_price(order: dict, result: dict):
    try:
        from ap.exit_price_sync import sync_exit_price_to_dashboard

        pos_id = order.get("position_id")
        avg_fill = result.get("avg_fill")
        entry_price = order.get("entry_price") or order.get("fill_price")
        ticker = (order.get("symbol") or "").upper()

        if pos_id and avg_fill is not None:
            sync_exit_price_to_dashboard(
                position_id=str(pos_id),
                exit_avg_fill=float(avg_fill),
                entry_price=float(entry_price) if entry_price else None,
                ticker=ticker,
            )
    except ImportError:
        pass
    except Exception as exc:
        log.debug("[%s] exit_price_sync failed (non-critical): %s", order.get("client_id"), exc)


# =============================================================================
# MAIN LOOP — STOP-EVENT SAFE
# =============================================================================

def fill_monitor_loop(
    broker: BrokerAdapter,
    poll_seconds: float = 10.0,
    osm=None,
    pm=None,
    exit_engine=None,
    stop_event=None,
    client_id: str | None = None,
):
    """Fill monitor must never pause on kill switch — it reconciles reality."""
    if osm is None and not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("fill_monitor_loop requires OSM unless ALLOW_LEGACY_FILL_MONITOR=1")

    if not client_id:
        client_id = getattr(osm, "client_id", None) or getattr(pm, "client_id", None)
    if not client_id:
        raise ValueError("fill_monitor_loop requires client_id or osm/pm with client_id")

    log.info(
        "Fill monitor started | client_id=%s osm=%s pm=%s ee=%s",
        client_id,
        "wired" if osm else "legacy-fallback",
        "wired" if pm else "none",
        "wired" if exit_engine else "none",
    )

    while not (stop_event and stop_event.is_set()):
        try:
            pending = get_pending_orders(client_id)
            for order in pending:
                try:
                    process_pending_order(
                        broker,
                        order,
                        osm=osm,
                        pm=pm,
                        exit_engine=exit_engine,
                    )
                except Exception as exc:
                    log.exception("Failed to process order %s: %s", order.get("local_order_id"), exc)

            if stop_event:
                stop_event.wait(poll_seconds)
            else:
                time.sleep(poll_seconds)

        except Exception as exc:
            log.exception("Fill monitor loop error: %s", exc)
            if stop_event:
                stop_event.wait(poll_seconds * 2)
            else:
                time.sleep(poll_seconds * 2)


# =============================================================================
# LEGACY FALLBACK HELPERS (deprecated — used only when ALLOW_LEGACY_FILL_MONITOR=1)
# =============================================================================

def _legacy_update_order_status(
    local_order_id: str,
    status: str,
    filled_qty: int | None = None,
    error: str | None = None,
):
    """DEPRECATED: Direct DB write. Use osm.transition() instead."""
    if not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("legacy fill monitor path disabled")

    with conn() as c:
        updates = ["status=%s", "updated_ts=%s"]
        params = [status, now_utc_iso()]
        if filled_qty is not None:
            updates.append("filled_qty=%s")
            params.append(int(filled_qty))
        if error is not None:
            updates.append("last_error=%s")
            params.append(error)
        params.append(local_order_id)
        sql = f"UPDATE orders SET {', '.join(updates)} WHERE local_order_id=%s"
        run_with_retry(lambda: c.execute(sql, params))


def _legacy_create_position_from_fill(order: dict, avg_fill_price: float, filled_qty: int):
    """DEPRECATED: Direct DB write. OSM/PM path should handle position opening."""
    if not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("legacy fill monitor path disabled")

    import uuid

    pos_id = str(uuid.uuid4())
    client_id = order["client_id"]
    direction = (order.get("direction") or "CALL").upper()

    with conn() as c:
        run_with_retry(
            lambda: c.execute(
                """
                INSERT INTO positions (
                    id, client_id, underlying, contract, direction, qty, avg_fill,
                    entry_ts, tp_pct, sl_pct, status
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    pos_id,
                    client_id,
                    order["symbol"],
                    order["contract"],
                    direction,
                    int(filled_qty),
                    float(avg_fill_price),
                    now_utc_iso(),
                    float(cfg.TAKE_PROFIT_PCT),
                    float(cfg.STOP_LOSS_PCT),
                    "OPEN",
                ),
            )
        )

    with conn() as c:
        run_with_retry(
            lambda: c.execute(
                "UPDATE orders SET position_id=%s WHERE local_order_id=%s",
                (pos_id, order["local_order_id"]),
            )
        )

    audit(
        client_id,
        "INFO",
        "POSITION_CREATED_FROM_FILL_LEGACY",
        {
            "position_id": pos_id,
            "local_order_id": order["local_order_id"],
            "contract": order["contract"],
            "qty": int(filled_qty),
            "avg_fill": float(avg_fill_price),
        },
    )
    return pos_id


def _legacy_close_position_from_exit_fill(order: dict, avg_fill_price: float):
    """DEPRECATED: Direct DB write. OSM.transition(EXIT_FILLED) via exit engine should handle this."""
    if not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("legacy fill monitor path disabled")

    client_id = order["client_id"]
    position_id = order.get("position_id")

    if not position_id:
        log.error("Exit order has no position_id: %s", order.get("local_order_id"))
        return

    with conn() as c:
        pos_row = run_with_retry(
            lambda: c.execute(
                "SELECT * FROM positions WHERE id=%s AND client_id=%s",
                (position_id, client_id),
            ).fetchone()
        )

    if not pos_row:
        log.error("Position not found: %s", position_id)
        return

    pos = dict(pos_row)
    entry_price = float(pos["avg_fill"])
    qty = int(pos["qty"])
    exit_px = float(avg_fill_price)
    realized_pnl = (exit_px - entry_price) * qty * OPT_MULTIPLIER
    realized_pnl_pct = round(((exit_px - entry_price) / entry_price) * 100, 2) if entry_price > 0 else 0.0

    with conn() as c:
        run_with_retry(
            lambda: c.execute(
                """
                UPDATE positions
                SET status='CLOSED', exit_ts=%s, exit_price=%s,
                    realized_pnl=%s, realized_pnl_pct=%s,
                    close_source=%s, close_confidence=%s
                WHERE id=%s AND client_id=%s
                """,
                (
                    now_utc_iso(),
                    exit_px,
                    float(realized_pnl),
                    realized_pnl_pct,
                    "FILL_MONITOR",
                    "HIGH",
                    position_id,
                    client_id,
                ),
            )
        )

    with conn() as c:
        run_with_retry(
            lambda: c.execute(
                "UPDATE client_state "
                "SET realized_pnl_today = COALESCE(realized_pnl_today, 0.0) + %s "
                "WHERE client_id=%s",
                (float(realized_pnl), client_id),
            )
        )

    audit(
        client_id,
        "INFO",
        "FINALIZED_TRADE_FROM_FILL_MONITOR",
        {
            "position_id": position_id,
            "contract": pos["contract"],
            "entry_price": entry_price,
            "exit_price": exit_px,
            "qty": qty,
            "realized_pnl": float(realized_pnl),
            "realized_pnl_pct": realized_pnl_pct,
            "close_source": "FILL_MONITOR",
            "close_confidence": "HIGH",
        },
    )

    try:
        epx = exit_px
        pct = realized_pnl_pct
        win = realized_pnl_pct > 0
        cid = client_id

        def _update_proof():
            with conn() as c2:
                c2.execute(
                    """
                    UPDATE proof_trades
                    SET exit_option_price = %s,
                        option_pnl_pct    = %s,
                        win               = %s
                    WHERE client_email = %s
                      AND closed_at >= NOW() - INTERVAL '4 hours'
                      AND ABS(COALESCE(exit_option_price,0) - %s) > 0.05
                    """,
                    (epx, pct, win, cid, epx),
                )
                return c2.rowcount

        updated = run_with_retry(_update_proof) or 0
        if updated:
            log.info("[%s] proof_trades corrected with actual fill $%.4f pnl=%.1f%%", client_id, exit_px, realized_pnl_pct)
    except Exception as exc:
        log.debug("[%s] proof_trades correction (non-critical): %s", client_id, exc)
