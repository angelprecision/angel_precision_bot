


ap_overnight_reeval.py — Overnight Daily Signal Re-Evaluation Engine
=====================================================================
This is the missing piece of the full trading loop.

FLOW:
  1. Market close → scanner runs → signals arrive with timeframe=1d
  2. Bot marks them WATCHING (audit record, not yet armed)
  3. *** THIS MODULE *** runs at 9:15 AM ET (15 min before open)
  4. For each WATCHING signal:
     a. Fetch prior-day high/low from Tradier history
     b. Run overnight_daily_validator (directional invalidation check)
     c. If VALID: select contract, create OSM entry order, arm entry_watcher
     d. If INVALID: mark REJECTED with reason code, log to ap_signals
  5. At 9:30 AM ET open: entry_watcher polls quotes, waits for breach
  6. On breach: on_trigger fires → OSM submits entry → fill monitor takes over

WHEN IT RUNS:
  - Called by client_runner's health loop at ~9:15 AM ET on trading days
  - Also callable via /admin/overnight_reeval for manual trigger
  - ONLY processes signals with date == yesterday (no stale signals)

ENTRY TRIGGER LOGIC:
  - If scanner provides entry_trigger: use it directly
  - If not: use prior_day_high (CALL) or prior_day_low (PUT) as the breach level
    This matches The Strat: we enter on prior-day boundary breach

FAIL-CLOSED:
  - Missing prior levels → REJECTED
  - Broker unavailable → skip (will retry on next poll)
  - No contract found → REJECTED with reason
"""
from __future__ import annotations

import logging
import os
import time
from datetime import date, datetime, timezone, timedelta
from typing import TYPE_CHECKING, Optional

log = logging.getLogger("ap.overnight_reeval")

if TYPE_CHECKING:
    pass

# How many calendar days back a signal is still considered "fresh"
# e.g. a Friday signal is valid Monday morning = 3 days
OVERNIGHT_SIGNAL_MAX_AGE_DAYS = int(os.getenv("OVERNIGHT_SIGNAL_MAX_AGE_DAYS", "4"))


def _et_now() -> datetime:
    from zoneinfo import ZoneInfo
    return datetime.now(ZoneInfo("America/New_York"))


def _is_trading_day(dt: datetime) -> bool:
    return dt.weekday() < 5  # Mon-Fri


def _signal_date(signal: dict) -> Optional[date]:
    """Extract the date the signal was generated (not when we process it).
    Falls back to parsing the signal_id itself (format: YYYY-MM-DD:...).
    """
    for key in ("created_at", "signal_date", "date", "timestamp_iso"):
        val = signal.get(key, "")
        if val and len(str(val)) >= 10:
            try:
                return date.fromisoformat(str(val)[:10])
            except ValueError:
                continue
    # Try parsing from signal_id: "2026-05-05:1-1:AAPL:Weekly:CALL"
    signal_id = signal.get("signal_id", "")
    if signal_id and len(signal_id) >= 10:
        try:
            return date.fromisoformat(str(signal_id)[:10])
        except ValueError:
            pass
    return None


def run_overnight_reeval(
    *,
    client_id: str,
    broker,
    master_control,
    contract_selector,
    order_state_machine,
    entry_watcher,
    position_manager=None,
    exit_eng=None,
    on_split_brain=None,
    force: bool = False,
) -> dict:
    """
    Re-evaluate all WATCHING signals for client_id.
    Called at ~9:15 AM ET before market open.

    Returns summary dict: {processed, armed, rejected, skipped, errors}
    """
    from ap.overnight_daily_validator import (
... (559 lines left)

ap_overnight_reeval.py
29 KB
# ap_exit_engine.py -- Angel Precision Time-Aware Exit Engine
# =============================================================================
# 0DTE / short-dated option exit protection.
#
# Current constants in this file:
#   - Poll interval: 8 seconds
#   - Profit protect W1: 11:00 AM ET, scale out 50% if option P&L >= +40%
#   - Profit protect W2:  1:00 PM ET, scale out 75% if option P&L >= +25%
#   - Profit protect W3:  2:00 PM ET, close all if option P&L >= +15%
#   - EOD hard close: 3:45 PM ET, close all remaining contracts
#   - Theta stop: after noon, close if option P&L <= -35%
#   - Immediate TP: +18% option P&L; 1 contract closes all, multi-contract scales
#   - Hard stop: DTE/instrument-adjusted; default -30%, tighter on 0DTE index
#
# Money-safety invariant:
#   - v9: missing callback identity is a visible safe-lock, not a silent deadlock.
#   - Submitting an exit order is NOT a fill.
#   - scale_outs_done increments only after broker-confirmed exit fill.
#   - Every exit path, including sentinels, must respect exit_in_flight gating.
#
# Bug-fix history (see inline FIX-N tags):
#   FIX-1  Discord webhook POST moved out of evaluate_exit() and out of the engine
#          lock. evaluate_exit() is now a pure function with no side effects.
#          Runner alert fires in _submit_exit_decision() after successful callback,
#          outside both lock sections. Previously the POST (timeout=3) held
#          self._lock for up to 3 seconds on every profitable runner close.
#   FIX-2  Kill-switch filter unpacked (pos, decision, bool) 3-tuples as 2-tuples,
#          raising ValueError on every iteration when KILL_BLOCKS_NON_PROTECTIVE_EXITS=1.
#          Fixed to unpack as (p, d, _) throughout.
#   FIX-3  Expired contract cleanup iterated and reassigned self._positions without
#          holding self._lock — race with concurrent add_position/fill hooks.
#          Entire expired-contract block now runs inside with self._lock.
#   FIX-4  replacement_proof was unconditionally set True, making all three
#          (not replacement_proof) guard blocks permanently dead code. Variable
#          removed; guards now execute unconditionally so stale-time, equivalent-runner,
#          and non-emergency checks actually run.
#   FIX-5  Inline W1/W2 window comments had wrong times (1:30 PM / 2:30 PM).
#          Corrected to match PROFIT_PROTECT_1_HOUR=11 (11:00 AM) and
#          PROFIT_PROTECT_2_HOUR=13 (1:00 PM).
#   FIX-6  Per-order cumulative fill watermark dict was cleared immediately on
#          full-fill. Late duplicate callbacks (fill monitor + reconciler dual path)
#          saw prev=0 and double-counted the fill. Dict is now preserved after
#          pending order completes and only reset in _mark_exit_submitted() when
#          a new exit order generation begins.
#   FIX-7  Added get_position(position_id) method for O(1) lookup. OSM's
#          _get_exit_engine_position() checks for this method first before
#          falling back to O(n) linear scan.
#   FIX-8  last_rejection_ts standardized from Optional[float] (epoch) to
#          Optional[datetime] (UTC) to match every other timestamp field on
#          ManagedPosition. Callers no longer need to know which fields are float.
#   FIX-9  Healer reference captured once before _exit_loop() while-loop instead
#          of re-importing every 8 seconds.
#   FIX-10 Extra blank line inside def on_exit_failure() signature removed.
# =============================================================================

from __future__ import annotations

import os
import time
import threading
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional, Callable
from zoneinfo import ZoneInfo


try:
    from ap.observability import emit_decision_event, get_git_commit
except Exception:
    emit_decision_event = None

    def get_git_commit(default: str = "unknown") -> str:
        return default

log = logging.getLogger("ap.exit_engine")
ET  = ZoneInfo("America/New_York")

# ── TIME THRESHOLDS (ET) ──────────────────────────────────────────────────────
PROFIT_PROTECT_1_HOUR = 11   # 11:00 AM -- scale out 50% if +40%
PROFIT_PROTECT_1_MIN  = 0
PROFIT_PROTECT_2_HOUR = 13   #  1:00 PM -- scale out 75% if +25%
PROFIT_PROTECT_2_MIN  = 0
PROFIT_PROTECT_3_HOUR = 14   #  2:00 PM -- exit all if +15%
PROFIT_PROTECT_3_MIN  = 0
EOD_HARD_CLOSE_HOUR   = 15   #  3:50 PM -- EXIT EVERYTHING before close
EOD_HARD_CLOSE_MIN    = 50   # Changed from 3:45 to give more time for fills
POLL_INTERVAL_SEC     = 8    # check every 8 seconds

# Kill switch policy: exits reduce risk, so the engine must never pause
# evaluation under kill switch. By default, all exit actions are allowed.
# Set EXIT_ENGINE_KILL_BLOCKS_NON_PROTECTIVE=1 only if you explicitly want
# kill switch to block non-protective exits while still allowing stops/EOD/theta.
KILL_BLOCKS_NON_PROTECTIVE_EXITS = (
    os.getenv("EXIT_ENGINE_KILL_BLOCKS_NON_PROTECTIVE", "0").strip().lower()
    in {"1", "true", "yes", "on"}
)

# ── P&L THRESHOLDS ────────────────────────────────────────────────────────────
THETA_STOP_LOSS_PCT   = -0.35  # -35% on option → stop... (117 KB left)

ap_exit_engine.py
167 KB
# ap/fill_monitor.py — Fill Monitor (OSM-routed, rich final)
"""
Fill Monitor — broker reconciliation poller + side-effect orchestrator.

This file preserves the production behaviors from the larger legacy monitor:
- audit_log writes... (15 KB left)

fill_monitor.py
65 KB
# ap/position_manager.py — APPositionManager
# =============================================================================
# Client-relative position truth backed by Supabase Postgres.
#
# Money-safety fixes:
# - Dedup guards only block ACTIVE positions (OPEN/CLOSING), not historical CLOSED rows.

position_manager.py
38 KB
﻿
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
            return [dict(r) for r in rows]
    return run_with_retry(_fn)


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

    if not cost or cost <= 0:
        log.warning(
            "[%s] _release_entry_guards: cost is zero for order=%s — equity may not be fully released",
            order.get("client_id"), order.get("local_order_id"),
        )
        return
    release_equity(client_id, cost)
    release_symbol_lock(client_id, symbol)


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
    """Best-effort capture of underlying price at the moment an entry fill is confirmed."""
    if not broker or not ticker:
        return 0.0
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
                    if val is not None and float(val) > 0:
                        return float(val)
        except Exception:
            continue
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
            except Exception:
                pass
    return None


def _open_position_safe(pm, *, order: dict, result: dict, plan_id: str, signal_id: str, local_id: str, broker: BrokerAdapter = None) -> Optional[str]:
    """Open position with idempotency keys when the PM supports them."""
    existing = _get_existing_position_by_order(pm, local_id, order.get("broker_order_id"))
    if existing:
        return existing.get("id") or existing.get("position_id")

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
        underlying_entry = _get_underlying_price_at_fill(broker, ticker)

    kwargs = {
        "plan_id": plan_id,
        "signal_id": signal_id,
        "ticker": ticker,
        "contract": contract,
        "side": (order.get("direction") or "CALL").upper(),
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
    }

    try:
        return pm.open_position(**kwargs)
    except TypeError:
        # Compatibility with older PM signature that does not yet accept every field.
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
    """Seed the exit engine after confirmed ENTRY fill without faking live quote state."""
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
            side=(order.get("direction") or "CALL").upper(),
            quantity=int(result.get("filled_qty") or order.get("qty") or 0),
            entry_price=entry_option_price,
            underlying_entry=underlying_entry,
            underlying_target=_safe_float(order.get("target_underlying") or order.get("underlying_target") or 0.0),
            underlying_stop=_safe_float(order.get("stop_underlying") or order.get("underlying_stop") or 0.0),
        )
        mp.position_id = position_id
        mp.client_id = str(order.get("client_id") or "")
        mp.signal_id = signal_id

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
            "side": (order.get("direction") or "CALL").upper(),
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
    alert_fn=None,
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
                )

                # Secondary broker-side stop is best-effort only.
                _place_standing_stop_best_effort(
                    broker=broker,
                    order=order,
                    qty=qty,
                    entry_price=price,
                )

                _seed_exit_engine(exit_engine, position_id, order, result, signal_id)

                if not position_id:
                    log.critical(
                        "[%s] CRITICAL: _open_position_safe returned None for %s %s "
                        "(fill confirmed but no position record created — exit engine BLIND to this position)",
                        client_id, order.get("symbol"), local_id,
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
            _safe_alert(
                alert_fn,
         ... (15 KB left)
