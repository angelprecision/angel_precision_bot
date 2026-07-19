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

import importlib
import inspect
import os
import re
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

AP_ENV = os.getenv("AP_ENV", os.getenv("ENV", "production")).strip().lower()
PRODUCTION_MODE = AP_ENV in {"prod", "production", "live"} or os.getenv("AP_LIVE_TRADING", "0").strip().lower() in {"1", "true", "yes", "on"}

if PRODUCTION_MODE and ALLOW_LEGACY_FILL_MONITOR:
    raise RuntimeError(
        "ALLOW_LEGACY_FILL_MONITOR=1 is forbidden in production/live mode; "
        "fill_monitor must use OSM/PM as the source-of-truth path."
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
    def _fn():
        with conn() as c:
            c.execute(
                "INSERT INTO audit_log (ts, level, event, payload, client_id) "
                "VALUES (%s,%s,%s,%s,%s)",
                (now_utc_iso(), level, event, json_dumps(payload), client_id),
            )
    try:
        run_with_retry(_fn)
    except Exception as exc:
        log.warning(
            "Audit write failed non-fatal | client=%s event=%s error=%s",
            client_id,
            event,
            exc,
        )



def _safe_alert(alert_fn, msg: str) -> None:
    """Best-effort operator alert. Never block reconciliation."""
    if not alert_fn:
        return
    try:
        alert_fn(msg)
    except Exception as exc:
        log.debug("Fill monitor alert_fn failed non-critical: %s", exc)

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

    def _fn():
        with conn() as c:
            rows = c.execute(
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
                    fill_price,
                    execution_mode,
                    filled_ts,
                    meta,
                    meta->>'canonical_signal_id' AS canonical_signal_id,
                    meta->>'underlying_entry'    AS underlying_entry_meta,
                    meta->>'entry_underlying'    AS entry_underlying_meta,
                    meta->>'last_underlying_price' AS last_underlying_price_meta
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
            return [dict(r) for r in rows]
    return run_with_retry(_fn)


# =============================================================================
# PR #235 — Fill monitor safety helpers (inlined from prior shim).
#
# All logic here supports the eight hardening pieces:
#   1. Confirmed broker fills must not default a missing side to CALL.
#   2. Side is resolved from orders.direction first, then OCC C/P marker.
#   3. Broker FILLED / EXIT_FILLED with cumulative filled_qty <= 0 must NOT
#      transition to filled.
#   4. underlying-at-fill is read from an explicit data broker when provided.
#   5. Terminal ENTRY cleanup always releases the symbol lock, even when
#      reserved_cost cannot be reconstructed.
#   6. Position-creation failure after confirmed fill persists a dashboard-
#      visible orders.last_error.
#   7. data_broker is an optional argument on process_pending_order and
#      fill_monitor_loop.
#   8. BUY/SELL are NEVER mapped to CALL/PUT in the fill monitor — they are
#      execution actions, not thesis direction.
# =============================================================================

# OCC option symbol format: <root><YYMMDD><C|P><strike>.  We only need the
# side marker; strike/root normalization stays in the OCC parser used elsewhere.
_OCC_SIDE_RE = re.compile(r"\d{6}([CP])")


def _resolve_order_option_side(order: dict) -> tuple[Optional[str], str]:
    """Resolve option thesis side without guessing.

    Priority:
      1. orders.direction / order.side when already canonical CALL or PUT.
      2. OCC option symbol C/P marker.
      3. unresolved — caller MUST fail closed / quarantine.  Never default
         to CALL, and never map BUY/SELL → CALL/PUT (those are execution
         actions on option orders, not thesis direction).

    Returns (side, source) where source is one of:
      "order_direction" | "occ_contract" | "missing_or_unparseable"
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
    """Return a shallow copy of the order with canonical direction + provenance."""
    patched = dict(order)
    patched["direction"] = side
    patched["_fill_monitor_side_source"] = source
    return patched


def _broker_base_url(broker) -> Optional[str]:
    """Best-effort broker base URL extraction for audit context."""
    cfg_obj = getattr(broker, "cfg", None)
    return (
        getattr(broker, "base_url", None)
        or getattr(cfg_obj, "base_url", None)
        or getattr(cfg_obj, "baseurl", None)
        or getattr(broker, "_base_url", None)
    )


def _select_quote_broker(execution_broker, data_broker=None):
    """Prefer an explicit data broker over the execution broker for quote reads.

    Priority:
      1. explicit data_broker argument
      2. execution_broker.data_broker attribute (attached via fill_monitor_loop)
      3. execution_broker itself (fallback — matches legacy behavior)
    """
    return data_broker or getattr(execution_broker, "data_broker", None) or execution_broker


def _emit_side_unresolved(order: dict, *, reason_code: str, alert_fn=None) -> None:
    """Emit critical audit + fill event when option side cannot be resolved.

    Callers MUST also skip whatever side-effect they were about to run
    (position creation, exit engine seed, pair-opposite cancel, etc.).
    """
    client_id = str(order.get("client_id") or "default")
    payload = {
        "local_order_id":  order.get("local_order_id"),
        "broker_order_id": order.get("broker_order_id"),
        "symbol":          order.get("symbol"),
        "contract":        order.get("contract"),
        "direction":       order.get("direction"),
        "side":            order.get("side"),
        "reason":          "missing_or_unparseable_side",
    }
    log.critical("[%s] %s | %s", client_id, reason_code, payload)
    audit(client_id, "CRITICAL", reason_code, payload)
    emit_fill_event(
        order,
        decision="ERROR",
        reason_code=reason_code,
        explanation=(
            "Confirmed broker fill could not be mapped to CALL/PUT without "
            "guessing; normal position/exit seeding blocked."
        ),
        result={},
        extra_context=payload,
    )
    _safe_alert(
        alert_fn,
        f"[fill_monitor:{client_id}] {reason_code} | "
        f"order={order.get('local_order_id')} broker={order.get('broker_order_id')}",
    )


def _record_position_create_failure(order: dict, reason: str = "FILLED_ORDER_POSITION_CREATE_FAILED") -> None:
    """Persist dashboard-visible orders.last_error when a confirmed fill's
    downstream side effect (position create, exit engine seed, etc.) fails.

    Never raises — this is observability, not a control-flow gate.
    """
    client_id = str(order.get("client_id") or "default")
    local_order_id = order.get("local_order_id")
    if not local_order_id:
        return

    def _write():
        with conn() as c:
            c.execute(
                """
                UPDATE orders
                   SET last_error = %s,
                       updated_ts = NOW()
                 WHERE client_id = %s
                   AND local_order_id = %s
                """,
                (reason, client_id, local_order_id),
            )

    try:
        run_with_retry(_write)
    except Exception as exc:
        log.debug(
            "[%s] failed to persist %s for %s: %s",
            client_id, reason, local_order_id, exc,
        )

    audit(
        client_id,
        "CRITICAL",
        reason,
        {
            "local_order_id":  local_order_id,
            "broker_order_id": order.get("broker_order_id"),
            "symbol":          order.get("symbol"),
            "contract":        order.get("contract"),
        },
    )


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

        result = {
            "status": our,
            "filled_qty": filled_qty,
            "avg_fill": avg_fill,
            "reason": raw.get("reason") or status,
            "raw": raw,
        }

        # PR #235 (hardening #3): broker FILLED / EXIT_FILLED with cumulative
        # filled_qty <= 0 is impossible truth for filled or partial-fill states.  Block the OSM transition and
        # emit a critical audit + fill event so operators see it.  The
        # reconciler/next broker poll will re-check on the next tick.
        if our in {"FILLED", "PARTIAL_FILL", "EXIT_FILLED", "EXIT_PARTIAL_FILL"} and int(filled_qty or 0) <= 0:
            reason = "BROKER_FILLED_ZERO_QTY"
            client_id = str(order.get("client_id") or "default")
            payload = {
                "local_order_id":  order.get("local_order_id"),
                "broker_order_id": broker_order_id,
                "kind":            kind,
                "mapped_status":   our,
                "filled_qty":      filled_qty,
                "broker_reason":   raw.get("reason") or status,
            }
            log.critical("[%s] %s | %s", client_id, reason, payload)
            audit(client_id, "CRITICAL", reason, payload)
            emit_fill_event(
                order,
                decision="ERROR",
                reason_code=reason,
                explanation=(
                    "Broker reported a fill/partial fill but no positive "
                    "cumulative filled quantity; OSM transition and side "
                    "effects blocked pending next broker/reconciler pass."
                ),
                result=result,
                extra_context=payload,
            )
            return {
                **result,
                "status": "ERROR",
                "filled_qty": 0,
                "reason": reason,
            }

        # PR #235 (hardening #1 + #2): on confirmed ENTRY FILLED, resolve
        # option side from orders.direction (canonical) or the OCC C/P
        # marker on the contract symbol.  If neither resolves, emit critical
        # and return ERROR — do NOT silently default to CALL and do NOT map
        # BUY/SELL.  This mutates `order` in-place so callers downstream of
        # check_order_with_broker see the canonical direction.
        if our == "FILLED" and kind == "ENTRY":
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
    symbol = order.get("symbol")

    # PR #235 (hardening #5): compute reserved cost defensively but do NOT
    # let a missing cost skip the symbol lock release.  Pre-#235 an
    # early-return here would leak the entry symbol lock forever if
    # reserved_cost was None and limit_price × qty was also unavailable
    # (e.g. broker-repair rows).
    cost: Optional[float] = None
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
                * OPT_MULTIPLIER
            )
        except Exception:
            cost = None

    if cost and cost > 0:
        release_equity(client_id, cost)
    else:
        log.warning(
            "[%s] _release_entry_guards: cost is zero/unknown for order=%s — equity may not be fully released",
            order.get("client_id"), order.get("local_order_id"),
        )

    # Always release the symbol lock, regardless of cost resolution.
    if symbol:
        release_symbol_lock(client_id, symbol)
    else:
        log.warning(
            "[%s] _release_entry_guards: symbol missing for order=%s — symbol lock could not be released",
            order.get("client_id"), order.get("local_order_id"),
        )


# =============================================================================
# PAIR MANAGER HELPER — BROKER CANCEL FIRST
# =============================================================================

def _cancel_pair_opposite(order: dict, broker: BrokerAdapter, osm, alert_fn=None) -> None:
    """
    On ENTRY fill: cancel the opposite side of a 1-1 pair.

    Broker cancel is attempted before local CANCELED transition.
    If broker id cannot be resolved, local order is not marked canceled.
    """
    if not osm:
        return

    try:
        from ap.signal_pair_manager import get_pair_manager

        # PR #235 (hardening #2): resolve side from order/OCC — do NOT default
        # to CALL when direction is missing.  If side is unresolvable, we
        # cannot safely reason about which pair-opposite to cancel.
        _pair_side, _pair_side_source = _resolve_order_option_side(order)
        if not _pair_side:
            _emit_side_unresolved(order, reason_code="PAIR_CANCEL_SIDE_UNRESOLVED", alert_fn=alert_fn)
            return

        pair_manager = get_pair_manager()
        ticker = (order.get("symbol") or "").upper()
        side = _pair_side
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
            msg = f"PAIR_CANCEL_SKIPPED_NO_BROKER_ID | {ticker} | opposite_local={cancel_local_id} | filled_local={filled_local_id}"
            log.warning("[%s] %s", ticker, msg)
            audit(
                str(order.get("client_id") or "default"),
                "CRITICAL",
                "PAIR_CANCEL_SKIPPED_NO_BROKER_ID",
                {
                    "filled_local_order_id": filled_local_id,
                    "opposite_local_order_id": cancel_local_id,
                    "symbol": ticker,
                    "side": side,
                    "reason": "opposite order has no broker_order_id",
                },
            )
            emit_fill_event(
                order,
                decision="ERROR",
                reason_code="PAIR_CANCEL_SKIPPED_NO_BROKER_ID",
                explanation=msg,
                result={},
                extra_context={
                    "filled_local_order_id": filled_local_id,
                    "opposite_local_order_id": cancel_local_id,
                },
            )
            _safe_alert(alert_fn, f"[fill_monitor:{order.get('client_id')}] {msg}")
            return

        try:
            if hasattr(broker, "cancel_order"):
                broker.cancel_order(resolved_broker_id)
            else:
                _cancel_with_session(broker, resolved_broker_id)
        except Exception as exc:
            msg = (
                f"PAIR_CANCEL_BROKER_FAILED | {ticker} | opposite_local={cancel_local_id} "
                f"broker={resolved_broker_id} error={exc}"
            )
            log.warning("[%s] %s", ticker, msg)
            audit(
                str(order.get("client_id") or "default"),
                "CRITICAL",
                "PAIR_CANCEL_BROKER_FAILED",
                {
                    "filled_local_order_id": filled_local_id,
                    "opposite_local_order_id": cancel_local_id,
                    "opposite_broker_order_id": resolved_broker_id,
                    "symbol": ticker,
                    "side": side,
                    "error": str(exc),
                },
            )
            emit_fill_event(
                order,
                decision="ERROR",
                reason_code="PAIR_CANCEL_BROKER_FAILED",
                explanation=msg,
                result={},
                extra_context={
                    "filled_local_order_id": filled_local_id,
                    "opposite_local_order_id": cancel_local_id,
                    "opposite_broker_order_id": resolved_broker_id,
                },
            )
            _safe_alert(alert_fn, f"[fill_monitor:{order.get('client_id')}] {msg}")
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
            audit(
                str(order.get("client_id") or "default"),
                "INFO",
                "PAIR_CANCEL_CONFIRMED",
                {
                    "filled_local_order_id": filled_local_id,
                    "opposite_local_order_id": cancel_local_id,
                    "opposite_broker_order_id": resolved_broker_id,
                    "symbol": ticker,
                    "side": side,
                },
            )

    except ImportError:
        pass
    except Exception as exc:
        msg = f"PAIR_CANCEL_MANAGER_FAILED | {order.get('symbol','?')} | local={order.get('local_order_id')} error={exc}"
        log.warning(msg)
        audit(
            str(order.get("client_id") or "default"),
            "WARNING",
            "PAIR_CANCEL_MANAGER_FAILED",
            {
                "local_order_id": order.get("local_order_id"),
                "broker_order_id": order.get("broker_order_id"),
                "symbol": order.get("symbol"),
                "error": str(exc),
            },
        )
        _safe_alert(alert_fn, f"[fill_monitor:{order.get('client_id')}] {msg}")


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


def _extract_underlying_from_contract(contract: str, fallback: str = "") -> str:
    """
    Extract the underlying root from an OCC option symbol.

    Example:
        META260515C00615000 -> META

    This prevents the corrupted ticker bug where slicing an OCC symbol produced
    invalid underlyings such as META26, causing quote monitors to fetch a ticker
    that does not exist.
    """
    import re as _re
    sym = str(contract or "").upper().strip()
    fallback = str(fallback or "").upper().strip()
    if sym:
        m = _re.search(r"\d{6}[CP]", sym)
        if m:
            root = sym[:m.start()].strip()
            if root:
                return root
        root = _re.sub(r"\d+$", "", sym).strip()
        if root:
            return root
    return fallback


def _get_underlying_price_at_fill(broker: BrokerAdapter, ticker: str) -> float:
    """
    Capture underlying price at the moment an entry fill is confirmed.

    FIX: the original implementation only tried abstract method names that
    TradierBroker does not expose, silently returning 0.0 every time.
    This caused underlying_entry = NULL in the positions table, disabling
    all underlying-based exit logic (stop_hit, target_hit, progress exit).

    Strategy (in priority order):
      1. Abstract broker helper methods (forwards-compatible with any broker)
      2. Tradier REST API via broker.session (guaranteed path for TradierBroker)
      3. Hard fallback: 0.0 — now logged as WARNING so failures are never silent
    """
    if not broker or not ticker:
        return 0.0

    ticker = str(ticker).upper().strip()

    # ── 1. Abstract broker helpers ────────────────────────────────────────────
    for method_name in ("get_quote", "get_underlying_price", "get_last_price", "quote"):
        method = getattr(broker, method_name, None)
        if not callable(method):
            continue
        try:
            result = method(ticker)
            if isinstance(result, (int, float)) and float(result) > 0:
                return float(result)
            if isinstance(result, dict):
                for key in ("last", "last_price", "price", "mark", "bid", "close"):
                    val = result.get(key)
                    if val is not None:
                        try:
                            fv = float(val)
                            if fv > 0:
                                return fv
                        except Exception:
                            pass
        except Exception:
            continue

    # ── 2. Tradier REST API via broker session (primary path for TradierBroker) ─
    # TradierBroker exposes .session (requests.Session) and .cfg.base_url.
    # Using the broker's existing authenticated session avoids credential duplication.
    try:
        session = getattr(broker, "session", None)
        cfg = getattr(broker, "cfg", None)
        base_url = (
            getattr(cfg, "base_url", None)
            or getattr(cfg, "baseurl", None)
            or getattr(broker, "base_url", None)
            or getattr(broker, "_base_url", None)
        )
        if session and base_url:
            resp = session.get(
                f"{base_url}/v1/markets/quotes",
                params={"symbols": ticker, "greeks": "false"},
                headers={"Accept": "application/json"},
                timeout=4,
            )
            if getattr(resp, "status_code", 500) < 300:
                data = resp.json() or {}
                raw = data.get("quotes", {}).get("quote", {})
                if isinstance(raw, list):
                    raw = raw[0] if raw else {}
                if isinstance(raw, dict):
                    for key in ("last", "ask", "bid", "close", "prevclose"):
                        val = raw.get(key)
                        if val is not None:
                            try:
                                fv = float(val)
                                if fv > 0:
                                    log.debug(
                                        "[fill_monitor] underlying_entry=%s for %s "
                                        "via Tradier REST (%s)",
                                        fv, ticker, key,
                                    )
                                    return fv
                            except Exception:
                                pass
    except Exception as exc:
        log.debug(
            "[fill_monitor] Tradier REST underlying price fetch failed for %s: %s",
            ticker, exc,
        )

    # ── 3. Hard fallback ───────────────────────────────────────────────────────
    log.warning(
        "[fill_monitor] _get_underlying_price_at_fill: could not fetch price for '%s' "
        "— underlying_entry will be NULL. "
        "Check broker session/base_url are accessible at fill time.",
        ticker,
    )
    return 0.0

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
            except Exception as _resolve_err:
                log.debug("Method %s failed during resolution: %s", method.__name__ if hasattr(method, "__name__") else method, _resolve_err)
    return None


def _open_position_safe(
    pm,
    *,
    order: dict,
    result: dict,
    plan_id: str,
    signal_id: str,
    local_id: str,
    broker: BrokerAdapter = None,
    quote_broker=None,
) -> Optional[str]:
    """Open position with idempotency keys when the PM supports them.

    PR #235 (hardening #1 + #2 + #4 + #6):
      - Resolve side without guessing; fail closed on unresolved.
      - Use quote_broker (or broker.data_broker) for the underlying-at-fill
        quote read.
      - Persist orders.last_error = FILLED_ORDER_POSITION_CREATE_FAILED when
        position creation raises or returns falsy.
    """
    existing = _get_existing_position_by_order(pm, local_id, order.get("broker_order_id"))
    if existing:
        return existing.get("id") or existing.get("position_id")

    # PR #235: resolve canonical CALL/PUT — never default to CALL.
    _side, _side_source = _resolve_order_option_side(order)
    if not _side:
        _emit_side_unresolved(order, reason_code="POSITION_OPEN_SIDE_UNRESOLVED")
        _record_position_create_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
        return None

    # PR #235: pick an explicit data broker for the underlying-at-fill quote
    # when the caller supplied one (or attached one to the execution broker).
    _quote_broker = _select_quote_broker(broker, quote_broker)
    if _quote_broker is not broker:
        audit(
            str(order.get("client_id") or "default"),
            "INFO",
            "FILL_MONITOR_UNDERLYING_ENTRY_QUOTE_BROKER_SELECTED",
            {
                "local_order_id":            order.get("local_order_id"),
                "broker_order_id":           order.get("broker_order_id"),
                "symbol":                    order.get("symbol"),
                "contract":                  order.get("contract"),
                "underlying_entry_source":   "data_broker",
                "quote_broker_base_url":     _broker_base_url(_quote_broker),
                "execution_broker_base_url": _broker_base_url(broker),
                "quote_broker_is_data_broker": True,
                "side":                      _side,
                "side_source":               _side_source,
            },
        )

    contract = order.get("contract") or order.get("symbol") or ""
    ticker = _extract_underlying_from_contract(contract, fallback=order.get("symbol") or "")
    underlying_entry = _safe_float(
        order.get("underlying_entry")
        or order.get("entry_underlying")
        or order.get("last_underlying_price")
        or order.get("trigger_price")
        or 0.0
    )
    if underlying_entry <= 0:
        underlying_entry = _get_underlying_price_at_fill(_quote_broker, ticker)

    kwargs = {
        "plan_id": plan_id,
        "signal_id": signal_id,
        "ticker": ticker,
        "contract": contract,
        "side": _side,
        "qty": int(result.get("filled_qty") or order.get("qty") or 0),
        "entry_price": float(result.get("avg_fill") or 0.0),
        "underlying_entry": underlying_entry if underlying_entry > 0 else None,
        "tier": str(order.get("tier") or "B"),
        "score": float(order.get("score") or 0),
        "pattern": str(order.get("pattern") or ""),
        "stop_underlying": float(order.get("stop_underlying")) if order.get("stop_underlying") is not None else None,
        "target_underlying": float(order.get("target_underlying")) if order.get("target_underlying") is not None else None,
        "local_order_id": local_id,
        "broker_order_id": order.get("broker_order_id"),
        "execution_mode": order.get("execution_mode"),
    }

    try:
        position_id = pm.open_position(**kwargs)
    except TypeError:
        # Compatibility with older PM signature that does not yet accept every field.
        try:
            position_id = _call_with_supported_kwargs(pm.open_position, **kwargs)
        except Exception:
            _record_position_create_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
            raise
    except Exception:
        _record_position_create_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
        raise

    if not position_id:
        _record_position_create_failure(order, "FILLED_ORDER_POSITION_CREATE_FAILED")
    return position_id


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
            stop_resp = broker.place_stop_order(symbol=contract, qty=qty, stop_price=stop_px)
            stop_id = None
            stop_stat = "unknown"
            if isinstance(stop_resp, dict):
                stop_id = stop_resp.get("id") or stop_resp.get("order_id") or stop_resp.get("broker_order_id")
                stop_stat = str(stop_resp.get("status") or stop_resp.get("state") or "unknown")
            log.info("[%s] Standing stop placed via broker helper @ $%.2f | broker_stop=%s status=%s", ticker, stop_px, stop_id or "?", stop_stat)
            audit(
                order["client_id"],
                "INFO",
                "STOP_ORDER_PLACED",
                {
                    "local_order_id": order.get("local_order_id"),
                    "ticker": ticker,
                    "contract": contract,
                    "qty": int(qty),
                    "stop_px": stop_px,
                    "broker_stop_order_id": stop_id,
                    "broker_stop_status": stop_stat,
                    "source": "broker_helper",
                },
            )
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
            audit(
                order["client_id"],
                "INFO",
                "STOP_ORDER_PLACED",
                {
                    "local_order_id": order.get("local_order_id"),
                    "ticker": ticker,
                    "contract": contract,
                    "qty": int(qty),
                    "stop_px": stop_px,
                    "broker_stop_order_id": stop_id,
                    "broker_stop_status": stop_stat,
                    "source": "rest",
                },
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


def _load_managed_position_class():
    """Resolve ManagedPosition across legacy/hardened exit-engine module paths."""
    configured = os.getenv("AP_MANAGED_POSITION_MODULE", "").strip()
    candidates = []
    if configured:
        candidates.append(configured)

    # Keep all known paths so fill-monitor seeding does not break during repo renames.
    candidates.extend([
        "ap_exit_engine",
        "ap.exit_engine",
        "ap.exitengine",
        "ap.exit_engine_hardened",
    ])

    seen = set()
    last_error = None
    for module_name in candidates:
        if not module_name or module_name in seen:
            continue
        seen.add(module_name)
        try:
            module = importlib.import_module(module_name)
            cls = getattr(module, "ManagedPosition", None)
            if cls is not None:
                return cls
            last_error = RuntimeError(f"{module_name}.ManagedPosition missing")
        except Exception as exc:
            last_error = exc

    raise ImportError(f"Could not import ManagedPosition from known exit-engine paths: {last_error}")


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _seed_exit_engine(exit_engine, position_id: str, order: dict, result: dict, signal_id: str):
    """Seed the exit engine after confirmed ENTRY fill without faking live quote state.

    PR #235 (hardening #2): resolve side from order/OCC — fail-closed on
    unresolved, do NOT default to CALL.

    Repair 4: If the exit engine already holds a broker-repair position for
    the same contract, upgrade it to canonical identity instead of creating
    a second in-memory position.  This closes the BA production incident:
    the broker-repair position held synthetic ID and unknown execution_mode;
    exits and proof rows were never attributed to the canonical fill.
    """
    if not exit_engine or not position_id:
        return

    # Resolve canonical side up front — used by both the ManagedPosition
    # constructor and the mp.signal dict below.  Emit critical + skip if
    # unresolvable rather than silently seeding a wrong-direction position.
    _side, _side_source = _resolve_order_option_side(order)
    if not _side:
        _emit_side_unresolved(order, reason_code="EXIT_ENGINE_SEED_SIDE_UNRESOLVED")
        return

    # ── Repair 4: canonical adoption of broker-repair position ───────────────
    # Before creating a new position, check whether the exit engine is already
    # tracking a broker-repair position for the same contract. If yes, upgrade
    # it atomically so we never have two in-memory positions for the same open
    # trade and all subsequent exits use canonical identity.
    _contract_for_adopt = str(order.get("contract") or order.get("symbol") or "").upper().strip()
    _adopt_fn = getattr(exit_engine, "adopt_canonical_position_identity", None)
    if callable(_adopt_fn) and _contract_for_adopt and position_id:
        try:
            _entry_fill_for_adopt = _safe_float(
                result.get("avg_fill") or order.get("fill_price") or 0.0
            )
            # Blocker 4: canonical fill timestamp (broker > order fallback > now).
            from datetime import datetime, timezone as _tz
            _now_utc = datetime.now(_tz.utc)
            _entry_ts_for_adopt = (
                result.get("filled_ts")
                or result.get("filled_at")
                or result.get("timestamp")
                or order.get("filled_ts")
                or _now_utc
            )
            # Blocker 4: underlying entry with same precedence as normal seeding.
            _underlying_entry_for_adopt = _safe_float(
                order.get("underlying_entry")
                or order.get("entry_underlying")
                or order.get("last_underlying_price")
                or order.get("last_underlying_price_meta")
                or order.get("entry_underlying_meta")
                or order.get("underlying_entry_meta")
                or order.get("trigger_price")
                or 0.0
            )
            _adopted = _adopt_fn(
                contract              = _contract_for_adopt,
                canonical_position_id = position_id,
                local_order_id        = str(order.get("local_order_id") or ""),
                broker_order_id       = str(order.get("broker_order_id") or ""),
                signal_id             = signal_id or str(order.get("signal_id") or ""),
                canonical_signal_id   = str(order.get("canonical_signal_id") or ""),
                entry_fill            = _entry_fill_for_adopt,
                entry_ts              = _entry_ts_for_adopt,
                order_filled_ts       = order.get("filled_ts"),
                execution_mode        = str(order.get("execution_mode") or ""),
                client_id             = str(order.get("client_id") or ""),
                underlying_entry      = _underlying_entry_for_adopt,
                score                 = _safe_float(order.get("score") or 0.0),
                tier                  = str(order.get("tier") or ""),
                pattern               = str(order.get("pattern") or ""),
                direction             = str(order.get("direction") or _side or ""),
                timeframe             = str(order.get("timeframe") or ""),
                underlying_stop       = _safe_float(order.get("stop_underlying") or order.get("underlying_stop") or 0.0),
                underlying_target     = _safe_float(order.get("target_underlying") or order.get("underlying_target") or 0.0),
            )

            # Handle CanonicalAdoptionResult (structured) or legacy bool.
            _disposition = getattr(_adopted, "disposition", None)
            if _disposition in ("ADOPTED", "ALREADY_CANONICAL_REPAIR_REMOVED"):
                log.info(
                    "[%s] _seed_exit_engine: %s contract=%s position_id=%s",
                    order.get("client_id"), _disposition, _contract_for_adopt, position_id,
                )
                return
            elif _disposition == "NO_REPAIR_FOUND":
                pass  # fall through to normal seed
            elif _disposition and _disposition.startswith("RETRY_"):
                # Adoption conflict — must NOT fall through to add_position.
                # Retaining existing protective monitoring; do not create duplicate exit owner.
                log.critical(
                    "[%s] CANONICAL_ADOPTION_RETRY_REQUIRED | "
                    "disposition=%s contract=%s position_id=%s reason=%s — "
                    "protective monitoring retained; no new exit owner created",
                    order.get("client_id"), _disposition,
                    _contract_for_adopt, position_id,
                    getattr(_adopted, "reason", ""),
                )
                return
            elif _adopted is True:  # legacy bool path
                return
            elif _adopted is False:  # legacy bool — no repair found, seed normally
                pass
        except Exception as _adopt_err:
            log.critical(
                "[%s] CANONICAL_ADOPTION_RETRY_REQUIRED | "
                "disposition=RETRY_ADOPTION_ERROR contract=%s: %s — "
                "protective monitoring retained; NOT falling through to add_position",
                order.get("client_id"), _contract_for_adopt, _adopt_err,
            )
            return

    try:
        getter = getattr(exit_engine, "get_position", None)
        if callable(getter) and getter(position_id):
            return
    except Exception as _ee_err:
        log.warning("Exit engine position check failed for %s: %s", position_id, _ee_err)

    try:
        if hasattr(exit_engine, "seed_position"):
            exit_engine.seed_position(position_id, order, result)
            return
    except Exception as exc:
        log.debug("exit_engine.seed_position failed; trying add_position path: %s", exc)

    try:
        _MP = _load_managed_position_class()
        contract = order.get("contract") or order.get("symbol") or ""
        ticker = _extract_underlying_from_contract(contract, fallback=order.get("symbol") or "")
        underlying_entry = _safe_float(
            order.get("underlying_entry")
            or order.get("entry_underlying")
            or order.get("last_underlying_price")
            or order.get("trigger_price")
            or 0.0
        )
        entry_option_price = _safe_float(result.get("avg_fill") or order.get("fill_price") or 0.0)

        mp = _MP(
            ticker=ticker,
            option_symbol=contract,
            side=_side,
            quantity=int(result.get("filled_qty") or order.get("qty") or 0),
            entry_price=entry_option_price,
            underlying_entry=underlying_entry,
            underlying_target=_safe_float(order.get("target_underlying") or order.get("underlying_target") or 0.0),
            underlying_stop=_safe_float(order.get("stop_underlying") or order.get("underlying_stop") or 0.0),
        )
        mp.position_id = position_id
        mp.client_id = str(order.get("client_id") or "")
        mp.signal_id = signal_id
        # Repair 5: preserve execution_mode from the order so proof rows
        # record "live" not "unknown".
        try:
            mp.execution_mode = str(order.get("execution_mode") or "")
        except Exception:
            pass

        try:
            mp.current_underlying = _safe_float(order.get("last_underlying_price") or 0.0)
            mp.current_option_price = entry_option_price
            mp.current_bid = 0.0
            mp.current_ask = 0.0
            mp.quote_fresh = False
            mp.quote_source = "fill_monitor_seed_unhydrated"
            mp.quote_ts = None
            mp.last_quote_ts = None
            mp.needs_quote_refresh = True
        except Exception:
            pass

        mp.signal = {
            "signal_id": signal_id,
            "pattern": str(order.get("pattern") or ""),
            "tier": str(order.get("tier") or "B"),
            "score": float(order.get("score") or 0),
            "timeframe": str(order.get("timeframe") or "1d"),
            "side": _side,
            "quote_fresh": False,
            "seed_source": "fill_monitor",
        }
        if not getattr(mp, "client_id", ""):
            mp.client_id = getattr(exit_engine, "_email", "") or getattr(exit_engine, "client_id", "")
        exit_engine.add_position(mp)

        refresher = getattr(exit_engine, "request_quote_refresh", None)
        if callable(refresher):
            try:
                refresher(position_id)
            except Exception as exc:
                log.debug("[%s] exit_engine.request_quote_refresh failed non-critical for %s: %s", order.get("client_id"), position_id, exc)

        log.info(
            "[%s] Exit engine seeded for pos=%s ticker=%s quote_fresh=False current_underlying=%s",
            order.get("client_id"),
            position_id,
            ticker,
            getattr(mp, "current_underlying", None),
        )
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

    local_id  = order.get("local_order_id")
    broker_id = order.get("broker_order_id")
    client_id = order.get("client_id", "?")

    # BROKER_FILL_ANOMALY is a diagnostic alert condition, not a legal OSM
    # lifecycle state. Attempting to transition to it always produces:
    #   CRITICAL: ILLEGAL TRANSITION -- EXIT_ACKNOWLEDGED -> BROKER_FILL_ANOMALY
    # Guard here so the alert fires but OSM is never touched.
    if FILL_ANOMALY_STATUS == "BROKER_FILL_ANOMALY":
        log.critical(
            "[%s] Broker fill anomaly retained as alert only | order=%s broker=%s reason=%s "
            "| NOT transitioning OSM — BROKER_FILL_ANOMALY is not a legal lifecycle state",
            client_id, local_id, broker_id, reason,
        )
        return False

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
            client_id, local_id, FILL_ANOMALY_STATUS, mapped, exc,
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
    alert_fn=None,
    data_broker=None,
):
    if PRODUCTION_MODE and ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("ALLOW_LEGACY_FILL_MONITOR=1 is forbidden in production/live mode")
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
                _cancel_pair_opposite(order, broker, osm, alert_fn=alert_fn)

                # Persist local position BEFORE placing optional standing stop.
                position_id = _open_position_safe(
                    pm,
                    order=order,
                    result=result,
                    plan_id=plan_id,
                    signal_id=signal_id,
                    local_id=local_id,
                    broker=broker,
                    quote_broker=data_broker,
                )

                # Secondary broker-side stop is best-effort only.
                _place_standing_stop_best_effort(
                    broker=broker,
                    order=order,
                    qty=qty,
                    entry_price=price,
                )

                _seed_exit_engine(exit_engine, position_id, order, result, signal_id)

                # Write position_id back to orders row.
                # osm.transition above was called without position_id because the
                # position didn't exist yet — _open_position_safe created it after.
                # Without this write the orders row has position_id=null forever.
                log.info(
                    "[%s] order_filled_detected order=%s contract=%s qty=%d "
                    "position_link_result=%s",
                    client_id, local_id,
                    order.get("contract") or order.get("symbol"),
                    int(new_filled or 0),
                    "success" if position_id else "MISSING",
                )
                if position_id:
                    try:
                        from ap.db import conn as _fm_conn, run_with_retry as _fm_retry
                        def _link_back():
                            with _fm_conn() as c:
                                c.execute(
                                    "UPDATE orders "
                                    "SET position_id=%s, updated_ts=NOW() "
                                    "WHERE client_id=%s AND local_order_id=%s "
                                    "AND (position_id IS NULL OR position_id='')",
                                    (position_id, client_id, local_id),
                                )
                        _fm_retry(_link_back)
                        log.info(
                            "[%s] order_position_link_success order=%s position=%s "
                            "contract=%s",
                            client_id, local_id, position_id,
                            order.get("contract") or order.get("symbol"),
                        )
                    except Exception as _link_err:
                        log.error(
                            "[%s] order_position_link_failed order=%s position=%s "
                            "err=%s",
                            client_id, local_id, position_id, _link_err,
                        )

                if not position_id:
                    log.critical(
                        "[%s] filled_order_missing_position_p0 order=%s contract=%s "
                        "(fill confirmed but no position record created — "
                        "exit engine BLIND to this position)",
                        client_id, local_id,
                        order.get("contract") or order.get("symbol"),
                    )

            except Exception as exc:
                log.critical(
                    "[%s] ENTRY fill side-effects FAILED for %s — position NOT created, "
                    "exit engine BLIND to this position. Error: %s",
                    client_id, local_id, exc, exc_info=True,
                )

        elif ok and kind == "EXIT":
            _sync_exit_price(order, result)

        # Release equity/symbol lock only after OSM confirmed the fill.
        # If ok=False (OSM transition failed), position is not confirmed —
        # releasing equity here would let a new trade consume capital that
        # is still reserved for this unresolved fill.
        if ok and kind == "ENTRY":
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

        partial_applied = False
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
                partial_applied = True
            except Exception as exc:
                log.error("[%s] OSM partial update %s failed for %s: %s", client_id, mapped, local_id, exc)
        else:
            _legacy_update_order_status(local_id, "PARTIAL_FILL", filled_qty=new_filled)
            partial_applied = True

        # Canonical accounting must advance on every confirmed EXIT fill, not
        # only when the broker order becomes terminal.  Stamp the normalized
        # OSM status into the result so the reducer preserves durable in-flight
        # ownership while the broker still owns the remainder.
        if partial_applied and kind == "EXIT":
            partial_result = dict(result)
            partial_result["status"] = "EXIT_PARTIAL_FILL"
            partial_result["filled_qty"] = new_filled
            partial_result["broker_order_id"] = broker_id
            _sync_exit_price(order, partial_result)

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
            _safe_alert(
                alert_fn,
                f"[fill_monitor:{client_id}] BROKER_FILL_ANOMALY_ESCALATED | order={local_id} broker={broker_id} status={mapped} count={anomaly_count} reason={result.get('reason')}",
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
    """
    Update proof_trades with the REAL Tradier avg_fill price.
    The execution core logs exit_option_price = limit_price at close time.
    This corrects it to the actual broker fill price and recalculates PnL.
    Runs directly against Supabase proof_trades — no dashboard API hop needed.
    """
    try:
        pos_id      = order.get("position_id")
        avg_fill    = result.get("avg_fill")
        entry_price = float(order.get("entry_price") or order.get("fill_price") or 0)
        client_id   = order.get("client_id", "")
        ticker      = (order.get("symbol") or "").upper()

        if not pos_id or avg_fill is None:
            return

        exit_px = float(avg_fill)

        # Recalculate PnL from broker fill price
        opt_pnl_pct = None
        win = None
        if entry_price > 0:
            opt_pnl_pct = round((exit_px - entry_price) / entry_price * 100, 2)
            win = opt_pnl_pct > 0

        # 1. Update proof_trades directly in Supabase
        try:
            from ap.db import conn, run_with_retry
            def _update_proof():
                with conn() as c:
                    params = [exit_px]
                    set_clause = "exit_option_price = %s"
                    if opt_pnl_pct is not None:
                        set_clause += ", option_pnl_pct = %s, win = %s"
                        params += [opt_pnl_pct, win]
                    params += [str(pos_id)]
                    cur = c.execute(
                        f"UPDATE proof_trades SET {set_clause} WHERE position_id = %s",
                        params,
                    )
                    primary_rowcount = getattr(cur, "rowcount", getattr(c, "rowcount", 0))
                    # Fallback: repair ONE unresolved orphan row only.
                    # Primary position_id match remains the source of truth.
                    if primary_rowcount == 0 and client_id and ticker:
                        params2 = [exit_px]
                        set2 = "exit_option_price = %s"
                        if opt_pnl_pct is not None:
                            set2 += ", option_pnl_pct = %s, win = %s"
                            params2 += [opt_pnl_pct, win]
                        params2 += [client_id, ticker]
                        cur2 = c.execute(
                            f"UPDATE proof_trades SET {set2} "
                            "WHERE id = ("
                            "  SELECT id FROM proof_trades "
                            "  WHERE client_email = %s "
                            "    AND ticker = %s "
                            "    AND closed_at >= NOW() - INTERVAL '60 minutes' "
                            "    AND (position_id IS NULL OR position_id = '') "
                            "    AND exit_option_price IS NULL "
                            "  ORDER BY closed_at DESC "
                            "  LIMIT 1"
                            ")",
                            params2,
                        )
                        return getattr(cur2, "rowcount", getattr(c, "rowcount", 0))
                    return primary_rowcount
            updated = run_with_retry(_update_proof) or 0
            if updated:
                log.info(
                    "[%s] EXIT PRICE SYNCED | %s broker_fill=$%.4f entry=$%.4f pnl=%.1f%% win=%s",
                    client_id, ticker, exit_px, entry_price,
                    opt_pnl_pct if opt_pnl_pct is not None else 0,
                    win,
                )
            else:
                log.warning(
                    "[%s] EXIT PRICE SYNC: no eligible row found "
                    "(primary by position_id=%s and narrowed fallback both empty)",
                    client_id,
                    pos_id,
                )
        except Exception as db_exc:
            log.debug("[%s] proof_trades exit sync DB error (non-critical): %s", client_id, db_exc)

        # 2. Also notify dashboard API if configured (belt-and-suspenders)
        try:
            from ap.exit_price_sync import sync_exit_price_to_dashboard
            sync_exit_price_to_dashboard(
                position_id=str(pos_id),
                exit_avg_fill=exit_px,
                entry_price=entry_price if entry_price else None,
                ticker=ticker,
            )
        except ImportError:
            pass
        except Exception:
            pass

    except Exception as exc:
        log.debug("[%s] _sync_exit_price failed (non-critical): %s", order.get("client_id"), exc)


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
    alert_fn=None,
    data_broker=None,
):
    """Fill monitor must never pause on kill switch — it reconciles reality.

    PR #235 (hardening #4 + #7): data_broker is an optional argument.  When
    supplied, it becomes the quote source for underlying-at-fill reads
    (see _select_quote_broker), keeping the execution broker (paper) and
    the data broker (Polygon/prod-quotes) separated.
    """
    if PRODUCTION_MODE and ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("ALLOW_LEGACY_FILL_MONITOR=1 is forbidden in production/live mode")
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
                        alert_fn=alert_fn,
                        data_broker=data_broker,
                    )
                except Exception as exc:
                    log.exception("Failed to process order %s: %s", order.get("local_order_id"), exc)

            # Idle gate: when no orders are in flight, poll slowly to reduce
            # Tradier API calls across 10 concurrent clients. 10 clients × 6
            # calls/min = 60 wasted calls/min hitting broker for nothing.
            # When active orders exist, use the normal fast poll cadence.
            _sleep = poll_seconds if pending else min(poll_seconds * 3, 30.0)
            if stop_event:
                stop_event.wait(_sleep)
            else:
                time.sleep(_sleep)

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
    def _fn():
        with conn() as c:
            c.execute(sql, params)
    run_with_retry(_fn)


def _legacy_create_position_from_fill(order: dict, avg_fill_price: float, filled_qty: int):
    """DEPRECATED: Direct DB write. OSM/PM path should handle position opening."""
    if not ALLOW_LEGACY_FILL_MONITOR:
        raise RuntimeError("legacy fill monitor path disabled")

    import uuid

    pos_id = str(uuid.uuid4())
    client_id = order["client_id"]

    # PR #235 (hardening #2): resolve side without guessing.  This legacy
    # fallback path is guarded by ALLOW_LEGACY_FILL_MONITOR=1 already, but
    # even in that mode we must not persist a fabricated direction.
    _leg_side, _leg_side_source = _resolve_order_option_side(order)
    if not _leg_side:
        _emit_side_unresolved(order, reason_code="LEGACY_POSITION_SIDE_UNRESOLVED")
        raise RuntimeError("LEGACY_POSITION_SIDE_UNRESOLVED")
    direction = _leg_side

    def _insert_pos():
        with conn() as c:
            c.execute(
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
    run_with_retry(_insert_pos)

    def _link_order():
        with conn() as c:
            c.execute(
                "UPDATE orders SET position_id=%s WHERE local_order_id=%s",
                (pos_id, order["local_order_id"]),
            )
    run_with_retry(_link_order)

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

    def _fetch_pos():
        with conn() as c:
            return c.execute(
                "SELECT * FROM positions WHERE id=%s AND client_id=%s",
                (position_id, client_id),
            ).fetchone()
    pos_row = run_with_retry(_fetch_pos)

    if not pos_row:
        log.error("Position not found: %s", position_id)
        return

    pos = dict(pos_row)
    entry_price = float(pos["avg_fill"])
    qty = int(pos["qty"])
    exit_px = float(avg_fill_price)
    realized_pnl = (exit_px - entry_price) * qty * OPT_MULTIPLIER
    realized_pnl_pct = round(((exit_px - entry_price) / entry_price) * 100, 2) if entry_price > 0 else 0.0

    def _close_pos():
        with conn() as c:
            c.execute(
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
    run_with_retry(_close_pos)

    def _update_pnl():
        with conn() as c:
            c.execute(
                "UPDATE client_state "
                "SET realized_pnl_today = COALESCE(realized_pnl_today, 0.0) + %s "
                "WHERE client_id=%s",
                (float(realized_pnl), client_id),
            )
    run_with_retry(_update_pnl)

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
