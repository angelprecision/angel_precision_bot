"""
ap_recovery.py — Full Restart Recovery
=======================================
Restores all runtime state for a ClientRunner after a restart:

  1. Open positions   — re-register with APPositionManager
  2. Pending entries  — re-submit stale CREATED/SUBMITTED/ACKNOWLEDGED to
                        fill_monitor by verifying broker status first
  3. Pending exits    — reattach exit protections to all CLOSING positions
  4. Reserved buying power — recompute from open + pending orders
  5. Dedup/session state  — repopulate master_control._seen_signals from
                            recent trade_queue rows (prevents re-trading
                            signals that were already queued pre-restart)

Usage:
    recovery = APStartupRecovery(
        client_id=email,
        broker=broker,
        osm=order_state_machine,
        pm=position_manager,
        master_control=master_control,
        exit_engine=exit_eng,
    )
    recovered = recovery.run()
    log.info("Recovery: %s", recovered)
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
import types
from datetime import datetime, timezone, timedelta
from typing import Optional

log = logging.getLogger("ap.recovery")

# How far back to look for signals to re-seed dedup state
DEDUP_LOOKBACK_HOURS = int(os.getenv("DEDUP_LOOKBACK_HOURS", "8"))

# Statuses that mean an order is still live (entry side)
ENTRY_LIVE_STATUSES  = frozenset({"CREATED", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL"})

# Statuses that mean an exit is in-flight
EXIT_LIVE_STATUSES   = frozenset({"EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL"})

# Position statuses that still consume runtime management after restart.
# PARTIAL and ACTIVE are legacy/scale-out compatible; remaining qty always wins.
ACTIVE_POSITION_STATUSES = ("OPEN", "CLOSING", "PARTIAL", "ACTIVE")

_OCC_SIDE_RE = re.compile(r"\d{6}([CP])\d{8}$")
_VALID_EXECUTION_MODES = frozenset({"PAPER", "LIVE"})


def _safe_int(value, default: int = 0) -> int:
    try:
        if value is None or value == "":
            return default
        return int(float(value))
    except Exception:
        return default


def _safe_float(value, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(value)
    except Exception:
        return default


def _extract_explicit_fill_qty(raw: dict) -> Optional[int]:
    """Extract explicit cumulative filled qty from broker payload.

    Startup recovery must never invent qty=0 for a broker-filled order.  A
    missing value means the adapter did not give us enough fill truth yet; leave
    the order for fill_monitor/reconciler instead of terminalizing it.
    """
    for key in (
        "filled_qty",
        "filled_quantity",
        "cumulative_filled_qty",
        "cumulative_filled_quantity",
        "exec_quantity",
        "executed_quantity",
        "filled",
    ):
        if key not in raw:
            continue
        val = raw.get(key)
        if val is None or val == "":
            continue
        try:
            qty = int(float(val))
            if qty > 0:
                return qty
        except Exception:
            continue
    return None


def _extract_avg_fill_price(raw: dict) -> Optional[float]:
    """Extract explicit execution price from broker payload.

    Do not use generic broker "price" / "avg_price" fields here: those can be
    limit/stop prices, not execution truth.
    """
    for key in (
        "avg_fill_price",
        "average_fill_price",
        "fill_price",
        "filled_avg_price",
    ):
        if key not in raw:
            continue
        val = raw.get(key)
        if val is None or val == "":
            continue
        try:
            px = float(val)
            if px > 0:
                return px
        except Exception:
            continue
    return None


def _write_fill_truth_blocked_meta(
    client_id: str,
    local_order_id: str,
    *,
    reason: str,
    broker_order_id: str,
    broker_status: str,
    extracted_qty,
    extracted_price,
) -> None:
    """
    P0 (PR #259): durable diagnostic when recovery REFUSES to mark a
    broker-"filled" order FILLED because the payload lacks positive executed
    quantity and/or average fill price.

    The refusal itself already existed (RECOVERY_FILL_TRUTH_MISSING /
    RECOVERY_EXIT_FILL_TRUTH_MISSING logged, no transition, row left for
    fill_monitor/reconciler). What was missing is a DURABLE record: the
    block lived only in logs and the in-memory result dict, so a row that
    kept failing extraction across restarts carried no forensic trail in
    the database. This helper merges the block reason, the broker order id,
    the raw broker status, and the (invalid) extracted values into
    orders.meta.

    Strictly additive and best-effort: jsonb concat (never overwrites
    existing meta), no status change, no position writes, never raises —
    a diagnostics failure must not affect the recovery pass.
    """
    try:
        from ap.db import conn as _conn, run_with_retry as _rwr

        _patch = json.dumps({
            "recovery_fill_truth_block": {
                "reason": reason,
                "broker_order_id": str(broker_order_id or ""),
                "broker_status_raw": str(broker_status or ""),
                "extracted_filled_qty": extracted_qty,
                "extracted_avg_fill_price": extracted_price,
                "blocked_at": datetime.now(timezone.utc).isoformat(),
                "recorded_by": "ap_recovery",
            }
        })

        def _merge():
            with _conn() as c:
                c.execute(
                    """
                    UPDATE orders
                    SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
                    WHERE local_order_id = %s AND client_id = %s
                    """,
                    (_patch, local_order_id, client_id),
                )

        _rwr(_merge)
    except Exception as _exc:
        log.debug(
            "[%s] fill-truth block meta write failed (non-fatal): %s",
            client_id, _exc,
        )


def _normalize_execution_mode(value) -> str | None:
    mode = str(value or "").strip().upper()
    return mode if mode in _VALID_EXECUTION_MODES else None


def _resolve_side_from_order_or_meta(order: dict, meta: dict | None = None) -> tuple[str | None, str]:
    """Resolve strategy side without ever defaulting missing direction to CALL."""
    meta = meta or {}
    raw = str(
        order.get("direction")
        or order.get("side")
        or meta.get("direction")
        or meta.get("side")
        or ""
    ).upper().strip()

    if raw in {"CALL", "PUT"}:
        return raw, "field"

    if raw in {"BUY", "LONG", "CALLS", "BULLISH"}:
        return "CALL", "alias"

    if raw in {"SELL", "SHORT", "PUTS", "BEARISH"}:
        return "PUT", "alias"

    contract = str(
        order.get("contract")
        or order.get("symbol")
        or meta.get("selected_contract")
        or meta.get("contract_symbol")
        or meta.get("contract")
        or ""
    ).upper().strip()

    m = _OCC_SIDE_RE.search(contract)
    if m:
        return ("CALL" if m.group(1) == "C" else "PUT"), "occ_contract"

    return None, "unresolved"


def _position_reserved_cost(pos: dict) -> float:
    """Return reserved position capital with fill-cost fallback."""
    for key in ("reserved_cost", "cost"):
        cost = _safe_float(pos.get(key), 0.0)
        if cost > 0:
            return cost

    px = (
        _safe_float(pos.get("avg_fill"), 0.0)
        or _safe_float(pos.get("entry_price"), 0.0)
        or _safe_float(pos.get("fill_price"), 0.0)
    )
    qty = (
        _safe_int(pos.get("quantity_remaining"), 0)
        or _safe_int(pos.get("qty"), 0)
        or _safe_int(pos.get("quantity"), 0)
        or _safe_int(pos.get("contracts"), 0)
    )
    if px > 0 and qty > 0:
        return float(px) * int(qty) * 100.0
    return 0.0


class APStartupRecovery:
    """
    Runs once on ClientRunner startup to restore in-memory state from DB.
    Designed to be idempotent — safe to call even if nothing needs recovery.
    """

    def __init__(
        self,
        client_id: str,
        broker,
        osm,              # APOrderStateMachine
        pm,               # APPositionManager
        master_control,   # APMasterControl
        exit_engine=None, # APExitEngine (optional — needed for exit re-attachment)
        entry_watcher=None,  # APEntryWatcher (optional — needed for watcher reseed)
    ):
        self.client_id     = str(client_id or "").strip().lower()
        self.broker        = broker
        self.osm           = osm
        self.pm            = pm
        self.mc            = master_control
        self.exit_engine   = exit_engine
        self.entry_watcher = entry_watcher

    # ──────────────────────────────────────────────────────────────────────────
    # Entry point
    # ──────────────────────────────────────────────────────────────────────────

    def run(self, *, include_watcher_reseed: bool = True) -> dict:
        result = {
            "client_id":          self.client_id,
            "positions_recovered": 0,
            "entries_verified":    0,
            "entries_corrected":   0,
            "exits_reattached":    0,
            "buying_power_reserved": 0.0,
            "dedup_seeded":        0,
            "watchers_requeued":   0,
            "errors":              [],
        }

        log.info("[%s] Startup recovery beginning...", self.client_id)

        if self._execution_mode() is None:
            log.error("[%s] RECOVERY_BLOCKED unknown execution_mode", self.client_id)
            result["errors"].append("recovery_unknown_execution_mode")
            return result

        try:
            self._recover_positions(result)
        except Exception as e:
            log.error("[%s] Position recovery error: %s", self.client_id, e)
            result["errors"].append(f"positions: {e}")

        try:
            self._verify_pending_entries(result)
        except Exception as e:
            log.error("[%s] Entry verification error: %s", self.client_id, e)
            result["errors"].append(f"entries: {e}")

        try:
            self._reattach_exit_protections(result)
        except Exception as e:
            log.error("[%s] Exit reattachment error: %s", self.client_id, e)
            result["errors"].append(f"exits: {e}")

        try:
            self._recompute_buying_power(result)
        except Exception as e:
            log.error("[%s] Buying power recompute error: %s", self.client_id, e)
            result["errors"].append(f"buying_power: {e}")

        try:
            self._reseed_dedup(result)
        except Exception as e:
            log.error("[%s] Dedup reseed error: %s", self.client_id, e)
            result["errors"].append(f"dedup: {e}")

        if include_watcher_reseed:
            try:
                self._reseed_watchers(result)
            except Exception as e:
                log.error("[%s] Watcher reseed error: %s", self.client_id, e)
                result["errors"].append(f"watchers: {e}")

        log.info(
            "[%s] Recovery complete | positions=%d entries_verified=%d "
            "entries_corrected=%d exits=%d dedup=%d watchers_requeued=%d buying_power=$%.2f errors=%d",
            self.client_id,
            result["positions_recovered"],
            result["entries_verified"],
            result["entries_corrected"],
            result["exits_reattached"],
            result["dedup_seeded"],
            result["watchers_requeued"],
            result["buying_power_reserved"],
            len(result["errors"]),
        )
        return result

    def _execution_mode(self) -> str | None:
        return _normalize_execution_mode(getattr(self.mc, "mode", None))

    # ──────────────────────────────────────────────────────────────────────────
    # Active position loading
    # ──────────────────────────────────────────────────────────────────────────

    def _load_active_positions(self) -> list[dict]:
        """Load all economically active position rows for this client."""
        from ap.db import conn, run_with_retry

        def _query():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id = %s
                      AND (
                        UPPER(COALESCE(status, '')) IN ('OPEN','CLOSING','PARTIAL','ACTIVE')
                        OR COALESCE(quantity_remaining, 0) > 0
                      )
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    """,
                    (self.client_id,),
                )
                return c.fetchall()

        return run_with_retry(_query) or []

    # ──────────────────────────────────────────────────────────────────────────
    # 1. Position recovery
    # ──────────────────────────────────────────────────────────────────────────

    def _recover_positions(self, result: dict):
        """Re-register active positions with PositionManager in-memory state."""
        all_active = self._load_active_positions()

        for pos in all_active:
            pos_id     = pos.get("id")
            underlying = pos.get("underlying") or pos.get("ticker", "?")
            status     = pos.get("status", "OPEN")
            qty        = _safe_int(pos.get("quantity_remaining"), 0) or _safe_int(pos.get("qty"), 0) or _safe_int(pos.get("quantity"), 0)
            direction, side_source = _resolve_side_from_order_or_meta(pos)
            direction_label = direction or "UNKNOWN"

            # Re-register with PositionManager if supported
            try:
                if self.pm and hasattr(self.pm, "register_recovered_position"):
                    self.pm.register_recovered_position(pos)
                elif self.pm and hasattr(self.pm, "add_position"):
                    self.pm.add_position(pos)
            except Exception as e:
                log.error(
                    "[%s] RECOVERY: failed to register recovered position %s: %s",
                    self.client_id, pos_id, e,
                )
                result.setdefault("errors", []).append(f"positions_register:{pos_id}:{e}")
                continue

            # Bump in-memory position count on master_control
            try:
                if hasattr(self.mc, "_position_count"):
                    self.mc._position_count += 1
            except Exception as _e:
                log.warning("recovery_position_count_inc_failed: %s", _e)

            # Bump sector counts on master_control if tracked
            try:
                sector = pos.get("sector", "")
                if sector and hasattr(self.mc, "_sector_counts"):
                    self.mc._sector_counts[sector] = (
                        self.mc._sector_counts.get(sector, 0) + 1
                    )
            except Exception as _e:
                log.warning("recovery_sector_counts_inc_failed: %s", _e)

            log.info(
                "[%s] RECOVERY: position restored | %s %s qty=%d status=%s pos=%s side_source=%s",
                self.client_id, underlying, direction_label, qty, status, pos_id, side_source,
            )
            result["positions_recovered"] += 1

    # ──────────────────────────────────────────────────────────────────────────
    # 2. Pending entry verification
    # ──────────────────────────────────────────────────────────────────────────

    def _verify_pending_entries(self, result: dict):
        """
        For every open entry order in DB, query broker for real status.
        If broker disagrees, advance the OSM to broker truth.
        This prevents phantom 'open' entries from blocking new trades after restart.
        """
        if self._execution_mode() is None:
            result.setdefault("errors", []).append("recovery_unknown_execution_mode")
            return
        from ap.db import get_open_orders_for_reconcile, run_with_retry

        orders = run_with_retry(
            lambda: get_open_orders_for_reconcile(client_id=self.client_id)
        )
        # Only entry orders here
        entry_orders = [o for o in (orders or []) if o.get("kind") == "ENTRY"]
        result["entries_verified"] = len(entry_orders)

        for order in entry_orders:
            local_id   = order.get("local_order_id") or order.get("id")
            broker_oid = order.get("broker_order_id")
            db_status  = (order.get("status") or "").upper()
            contract   = order.get("contract") or "?"

            if not broker_oid or broker_oid in ("N/A", "PENDING", ""):
                log.warning(
                    "[%s] RECOVERY: entry %s has no broker_order_id (db_status=%s) — "
                    "may have been lost pre-restart",
                    self.client_id, local_id, db_status,
                )
                continue
            try:
                broker_raw    = self.broker.get_order(broker_oid) or {}
                broker_status = str(broker_raw.get("status") or "").lower()
            except Exception as e:
                log.warning("[%s] RECOVERY: broker get_order failed for %s: %s",
                            self.client_id, broker_oid, e)
                continue

            from ap_reconciler import BROKER_TO_OSM, BROKER_FILLED, BROKER_TERMINAL

            if broker_status in BROKER_FILLED:
                new_status = BROKER_TO_OSM.get(broker_status)
                if not new_status or new_status == db_status:
                    continue

                filled_qty = _extract_explicit_fill_qty(broker_raw)
                avg_fill   = _extract_avg_fill_price(broker_raw)
                if not filled_qty or filled_qty <= 0 or not avg_fill or avg_fill <= 0:
                    msg = (
                        f"RECOVERY_FILL_TRUTH_MISSING entry local={local_id} "
                        f"broker={broker_oid} contract={contract} broker_status={broker_status} "
                        "missing explicit filled_qty/avg_fill; leaving for fill_monitor/reconciler"
                    )
                    log.critical("[%s] %s", self.client_id, msg)
                    result.setdefault("errors", []).append(msg)
                    # P0 (PR #259): durable forensic trail on the row itself.
                    _write_fill_truth_blocked_meta(
                        self.client_id, local_id,
                        reason="recovery_fill_truth_missing",
                        broker_order_id=broker_oid,
                        broker_status=broker_status,
                        extracted_qty=filled_qty,
                        extracted_price=avg_fill,
                    )
                    continue

                try:
                    ok = self.osm.transition(
                        local_id, new_status,
                        filled_qty=filled_qty,
                        fill_price=avg_fill,
                    )
                    if ok:
                        result["entries_corrected"] += 1
                        log.info(
                            "[%s] RECOVERY: corrected filled entry %s | %s | %s → %s qty=%d avg=$%.4f",
                            self.client_id, local_id, contract, db_status, new_status, filled_qty, avg_fill,
                        )
                except Exception as e:
                    log.error("[%s] RECOVERY: OSM fill transition failed: %s", self.client_id, e)
                continue

            if broker_status in BROKER_TERMINAL:
                new_status = BROKER_TO_OSM.get(broker_status)
                if new_status and new_status != db_status:
                    try:
                        ok = self.osm.transition(
                            local_id, new_status,
                            last_error=f"recovery: broker_status={broker_status}",
                        )
                        if ok:
                            result["entries_corrected"] += 1
                            log.info(
                                "[%s] RECOVERY: corrected terminal entry %s | %s | %s → %s",
                                self.client_id, local_id, contract, db_status, new_status,
                            )
                    except Exception as e:
                        log.error("[%s] RECOVERY: OSM terminal transition failed: %s", self.client_id, e)

    # ──────────────────────────────────────────────────────────────────────────
    # 3. Exit protection reattachment
    # ──────────────────────────────────────────────────────────────────────────

    def _reattach_exit_protections(self, result: dict):
        """
        For every CLOSING position, verify the exit order is still live at
        broker. If the exit filled while we were down, advance the position.
        If exit canceled/expired, revert position to OPEN.
        """
        if self._execution_mode() is None:
            result.setdefault("errors", []).append("recovery_unknown_execution_mode")
            return
        from ap.db import list_positions, run_with_retry, conn

        closings = run_with_retry(
            lambda: list_positions(client_id=self.client_id, status="CLOSING")
        )
        if not closings:
            return

        for pos in closings:
            pos_id     = pos.get("id")
            underlying = pos.get("underlying") or pos.get("ticker", "?")

            # Find the active exit order for this position
            def _get_exit_order(pid=pos_id):
                with conn() as c:
                    c.execute(
                        """
                        SELECT local_order_id, broker_order_id, status
                        FROM orders
                        WHERE client_id=%s AND position_id=%s AND kind='EXIT'
                          AND status NOT IN ('EXIT_FILLED','REJECTED','CANCELED','EXPIRED','ERROR')
                        ORDER BY created_ts DESC LIMIT 1
                        """,
                        (self.client_id, pid),
                    )
                    return c.fetchone()

            try:
                exit_order = run_with_retry(_get_exit_order)
            except Exception as e:
                log.error("[%s] RECOVERY: exit order lookup failed for pos %s: %s",
                          self.client_id, pos_id, e)
                continue

            if not exit_order:
                # CLOSING position with no exit order — needs manual exit
                log.error(
                    "[%s] RECOVERY: CLOSING position %s (%s) has NO active exit order — "
                    "exit must be re-submitted manually",
                    self.client_id, pos_id, underlying,
                )
                continue

            broker_oid = exit_order.get("broker_order_id")
            local_id   = exit_order.get("local_order_id")
            db_status  = exit_order.get("status", "")

            if not broker_oid or broker_oid in ("N/A", "PENDING", ""):
                log.warning(
                    "[%s] RECOVERY: exit order %s for pos %s has no broker_order_id",
                    self.client_id, local_id, pos_id,
                )
                continue

            # Verify at broker
            try:
                broker_raw    = self.broker.get_order(broker_oid) or {}
                broker_status = str(broker_raw.get("status") or "").lower()
            except Exception as e:
                log.warning("[%s] RECOVERY: broker exit order check failed: %s",
                            self.client_id, e)
                continue

            from ap_reconciler import BROKER_FILLED, BROKER_TERMINAL, BROKER_TO_OSM

            if broker_status in BROKER_FILLED:
                # Exit actually filled while we were down — close the position
                filled_qty = _extract_explicit_fill_qty(broker_raw)
                avg_fill   = _extract_avg_fill_price(broker_raw)
                if not filled_qty or filled_qty <= 0 or not avg_fill or avg_fill <= 0:
                    msg = (
                        f"RECOVERY_EXIT_FILL_TRUTH_MISSING local={local_id} "
                        f"broker={broker_oid} pos={pos_id} broker_status={broker_status} "
                        "missing explicit filled_qty/avg_fill; keeping CLOSING for reconciler/fill_monitor"
                    )
                    log.critical("[%s] %s", self.client_id, msg)
                    result.setdefault("errors", []).append(msg)
                    # P0 (PR #259): durable forensic trail on the row itself.
                    _write_fill_truth_blocked_meta(
                        self.client_id, local_id,
                        reason="recovery_exit_fill_truth_missing",
                        broker_order_id=broker_oid,
                        broker_status=broker_status,
                        extracted_qty=filled_qty,
                        extracted_price=avg_fill,
                    )
                    continue

                try:
                    self.osm.transition(
                        local_id, "EXIT_FILLED",
                        filled_qty=filled_qty,
                        fill_price=avg_fill,
                    )
                    log.info(
                        "[%s] RECOVERY: exit filled during downtime | pos=%s %s | "
                        "qty=%d avg=$%.2f",
                        self.client_id, pos_id, underlying, filled_qty, avg_fill,
                    )
                    result["exits_reattached"] += 1
                except Exception as e:
                    log.error("[%s] RECOVERY: EXIT_FILLED transition failed: %s",
                              self.client_id, e)

            elif broker_status in BROKER_TERMINAL:
                # Exit canceled — revert position to OPEN so it can be re-exited
                try:
                    self.osm.transition(
                        local_id, BROKER_TO_OSM.get(broker_status, "CANCELED"),
                        last_error=f"recovery: broker_status={broker_status}",
                    )
                    # Revert position
                    from ap.db import conn as _conn
                    def _revert(pid=pos_id):
                        with _conn() as c:
                            c.execute(
                                "UPDATE positions SET status='OPEN', exit_reason=NULL, "
                                "updated_ts=NOW() WHERE id=%s AND client_id=%s",
                                (pid, self.client_id),
                            )
                    run_with_retry(_revert)
                    log.warning(
                        "[%s] RECOVERY: exit was canceled during downtime | "
                        "pos=%s %s reverted to OPEN — EXIT MUST BE RETRIED",
                        self.client_id, pos_id, underlying,
                    )
                    result["exits_reattached"] += 1
                except Exception as e:
                    log.error("[%s] RECOVERY: exit revert failed: %s", self.client_id, e)
            else:
                # Exit is still live at broker — reattach to exit engine if available
                log.info(
                    "[%s] RECOVERY: exit order still live at broker | "
                    "pos=%s %s | broker_status=%s",
                    self.client_id, pos_id, underlying, broker_status,
                )
                result["exits_reattached"] += 1

                if self.exit_engine:
                    try:
                        if hasattr(self.exit_engine, "seed_from_db"):
                            # Idempotent global seed; safe to call again on startup.
                            self.exit_engine.seed_from_db()
                        elif hasattr(self.exit_engine, "mark_exit_in_flight"):
                            self.exit_engine.mark_exit_in_flight(
                                position_id=pos_id,
                                reason="recovery: exit order live at broker",
                            )
                    except Exception as e:
                        log.error(
                            "[%s] RECOVERY: failed to reattach exit protection for pos %s: %s",
                            self.client_id, pos_id, e,
                        )
                        result.setdefault("errors", []).append(f"exits_reattach:{pos_id}:{e}")

    # ──────────────────────────────────────────────────────────────────────────
    # 4. Buying power reservation
    # ──────────────────────────────────────────────────────────────────────────

    def _recompute_buying_power(self, result: dict):
        """
        Recompute how much capital is reserved by active positions.
        Updates master_control.account_equity if needed.
        """
        active_positions = self._load_active_positions()

        total_reserved = 0.0
        for pos in active_positions:
            total_reserved += _position_reserved_cost(pos)

        result["buying_power_reserved"] = total_reserved

        # Sync into master_control if it tracks this
        if hasattr(self.mc, "_reserved_capital"):
            self.mc._reserved_capital = total_reserved
        if hasattr(self.mc, "reserved_capital"):
            self.mc.reserved_capital = total_reserved

        log.info(
            "[%s] RECOVERY: buying power reserved=$%.2f across %d active positions",
            self.client_id, total_reserved, len(active_positions),
        )

    # ──────────────────────────────────────────────────────────────────────────
    # 5. Dedup / session state reseed
    # ──────────────────────────────────────────────────────────────────────────

    def _reseed_dedup(self, result: dict):
        """
        Repopulate master_control._seen_signals from recent trade_queue rows.
        Prevents re-processing signals that were already queued before restart.
        """
        from ap.db import run_with_retry, conn

        cutoff_hours = DEDUP_LOOKBACK_HOURS

        def _get_recent():
            with conn() as c:
                c.execute(
                    """
                    SELECT signal_id,
                           payload->>'ticker'            AS ticker,
                           payload->>'direction'         AS direction,
                           payload->>'side'              AS side,
                           payload->>'contract'          AS contract,
                           payload->>'selected_contract' AS selected_contract,
                           payload->>'contract_symbol'   AS contract_symbol,
                           payload->>'timeframe'         AS timeframe,
                           client_id, status
                    FROM trade_queue
                    WHERE client_id=%s
                      AND created_ts > NOW() - (%s * INTERVAL '1 hour')
                      AND status IN ('NEW','PROCESSING','DONE','COMPLETED','ERROR')
                    ORDER BY created_ts DESC
                    LIMIT 500
                    """,
                    (self.client_id, cutoff_hours),
                )
                return c.fetchall()

        try:
            rows = run_with_retry(_get_recent)
        except Exception as e:
            log.warning("[%s] RECOVERY: dedup reseed DB error: %s", self.client_id, e)
            return

        if not hasattr(self.mc, "_seen_signals"):
            log.debug("[%s] RECOVERY: master_control has no _seen_signals set — skipping",
                      self.client_id)
            return

        count = 0
        for row in (rows or []):
            signal_id = row.get("signal_id")
            ticker    = str(row.get("ticker") or "").upper()
            side, side_source = _resolve_side_from_order_or_meta(row)
            timeframe = str(row.get("timeframe") or "1d")

            # Re-add signal_id even when side is unresolved. Do NOT create a
            # ticker:CALL setup key from missing/dirty side metadata.
            _ts = time.time()
            if signal_id:
                self.mc._seen_signals[f"sig:{signal_id}:{self.client_id}"] = _ts
            if ticker and side:
                setup_key = f"{self.client_id}:{ticker}:{side}:{timeframe}"
                self.mc._seen_signals[setup_key] = _ts
            elif ticker:
                log.warning(
                    "[%s] RECOVERY: dedup setup key skipped for ticker=%s signal_id=%s side_source=%s",
                    self.client_id, ticker, signal_id, side_source,
                )
            count += 1

        result["dedup_seeded"] = count
        log.info(
            "[%s] RECOVERY: dedup reseeded with %d signals (lookback=%dh)",
            self.client_id, count, cutoff_hours,
        )

    # ──────────────────────────────────────────────────────────────────────────
    # 6. Watcher reseed — re-queue WATCHING signals so entry_watcher picks up
    #    after a restart. WATCHING in DB means "queued for entry watcher" but
    #    the watcher is in-memory; it loses state on restart.
    # ──────────────────────────────────────────────────────────────────────────

    @staticmethod
    def _coerce_order_meta(raw_meta) -> dict:
        if isinstance(raw_meta, dict):
            return dict(raw_meta)
        if isinstance(raw_meta, str) and raw_meta.strip():
            try:
                parsed = json.loads(raw_meta)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                return {}
        return {}

    def _build_recovery_plan_from_order(self, order: dict):
        meta = self._coerce_order_meta(order.get("meta"))

        contract = (
            order.get("contract")
            or meta.get("selected_contract")
            or meta.get("contract_symbol")
            or ""
        )
        direction, side_source = _resolve_side_from_order_or_meta(order, meta)
        if direction not in {"CALL", "PUT"}:
            log.warning(
                "[%s] RECOVERY: invalid_or_missing_side for local_order_id=%s signal_id=%s contract=%s side_source=%s",
                self.client_id,
                order.get("local_order_id"),
                order.get("signal_id"),
                contract,
                side_source,
            )
            return None

        ticker = str(
            order.get("symbol")
            or meta.get("symbol")
            or meta.get("ticker")
            or ""
        ).upper()
        trigger = (
            order.get("trigger_price")
            if order.get("trigger_price") is not None
            else meta.get("signal_entry_price")
        )
        stop = (
            order.get("stop_underlying")
            if order.get("stop_underlying") is not None
            else meta.get("stop_underlying")
        )
        target = (
            order.get("target_underlying")
            if order.get("target_underlying") is not None
            else meta.get("target_underlying")
        )

        metadata = dict(meta)
        metadata["recovery_side_source"] = side_source

        return types.SimpleNamespace(
            signal_id=str(
                order.get("signal_id")
                or meta.get("signal_id")
                or order.get("local_order_id")
                or ""
            ),
            plan_id=str(order.get("plan_id") or meta.get("plan_id") or ""),
            ticker=ticker,
            side=direction,
            direction=direction,
            score=float(order.get("score") or meta.get("score") or 65.0),
            tier=str(order.get("tier") or meta.get("tier") or "B"),
            trigger_price=float(trigger or 0) if trigger is not None else None,
            stop_underlying=float(stop or 0) if stop is not None else None,
            target_underlying=float(target or 0) if target is not None else None,
            contract_symbol=str(contract or ""),
            pattern=str(order.get("pattern") or meta.get("pattern") or ""),
            timeframe=str(order.get("timeframe") or meta.get("timeframe") or "1d"),
            prior_day_high=meta.get("prior_day_high"),
            prior_day_low=meta.get("prior_day_low"),
            strategy_type=str(meta.get("strategy_type") or ""),
            metadata=metadata,
        )

    def _reseed_watchers(self, result: dict):
        """
        Reset WATCHING signals back to NEW so the queue worker re-processes
        them and re-hands them to APEntryWatcher on startup.

        Only resets signals created today (ET) to avoid re-triggering stale
        multi-day signals that should have expired.
        """
        from ap.db import conn, run_with_retry
        from datetime import datetime, timezone, timedelta
        from zoneinfo import ZoneInfo

        ET = ZoneInfo("America/New_York")
        now_et   = datetime.now(ET)
        # Default 48 hours covers Sunday evening scanner signals for Monday open.
        # The exact reseed window is operator-tunable via
        # STARTUP_WATCHER_RESEED_LOOKBACK_HOURS.
        _lookback_hours = int(os.getenv("STARTUP_WATCHER_RESEED_LOOKBACK_HOURS", "48"))
        cutoff_utc = (now_et.astimezone(timezone.utc) - timedelta(hours=_lookback_hours)).isoformat()

        def _reset():
            # PR #143: tag rescued rows in payload.recovery_rescue so the
            # queue dispatch can recognize them as eligible for PAPER
            # immediate-entry submission after contract selection.
            # Payload is a JSONB column — merge via || so the existing
            # signal_id, ticker, score, etc. are preserved verbatim.
            # The marker is also stamped with a UTC timestamp + the lookback
            # window for audit. LIVE clients are not exempted in this query;
            # the LIVE/PAPER gate lives in queue._dispatch so the LIVE path
            # ignores the marker.
            with conn() as c:
                _marker_payload = (
                    '{"recovery_rescue":true,'
                    '"recovery_rescue_ts":"' + now_et.astimezone(timezone.utc).isoformat() + '",'
                    '"recovery_rescue_lookback_hours":' + str(int(_lookback_hours)) + '}'
                )
                c.execute(
                    """
                    UPDATE trade_queue
                    SET    status  = 'NEW',
                           payload = COALESCE(payload, '{}'::jsonb) || %s::jsonb
                    WHERE  client_id  = %s
                      AND  status     = 'WATCHING'
                      AND  created_ts >= %s
                      AND  NOT EXISTS (
                             SELECT 1
                             FROM orders o
                             WHERE o.client_id = trade_queue.client_id
                               AND o.signal_id = trade_queue.signal_id
                               AND o.kind = 'ENTRY'
                               AND o.status = 'PENDING_TRIGGER'
                               AND o.broker_order_id IS NULL
                               AND o.submitted_ts IS NULL
                               AND o.filled_ts IS NULL
                           )
                    """,
                    (_marker_payload, self.client_id, cutoff_utc),
                )
                return c.rowcount

        def _load_orphaned_pending_trigger_orders():
            with conn() as c:
                c.execute(
                    """
                    SELECT local_order_id,
                           signal_id,
                           plan_id,
                           symbol,
                           contract,
                           direction,
                           score,
                           tier,
                           trigger_price,
                           stop_underlying,
                           target_underlying,
                           pattern,
                           timeframe,
                           meta
                    FROM orders
                    WHERE client_id = %s
                      AND kind = 'ENTRY'
                      AND status = 'PENDING_TRIGGER'
                      AND created_ts >= %s
                      AND broker_order_id IS NULL
                      AND submitted_ts IS NULL
                      AND filled_ts IS NULL
                    ORDER BY created_ts ASC
                    """,
                    (self.client_id, cutoff_utc),
                )
                return c.fetchall()

        # ── PR #143 regression fix: LIVE never replays WATCHING rows ─────
        # The _reset() block above resets WATCHING → NEW and tags rows with
        # payload.recovery_rescue=true. PR #143 used this for paper-mode
        # immediate-entry recovery. On 2026-06-16 Jason's LIVE pod replayed
        # 23 WATCHING rows through MC/contract_selector with current (post-
        # trigger) market data. The selector's earnings/IV/chain gates
        # rejected all 23 with contract_selection:no_contract_found. Zero
        # live trades. LIVE must NEVER replay stale signals through the
        # selector — it can only reattach watcher ownership to current-
        # session WATCHING/PENDING_TRIGGER rows.
        #
        # Detection: read mc.mode (canonical mode source per execution_core
        # PR-B / FIX-3). Default to PAPER on lookup failure to preserve PR
        # #143 paper behavior — never silently fall into LIVE replay if mc
        # is mis-shaped.
        _mc_mode = "PAPER"
        try:
            _mc_mode = str(getattr(self.mc, "mode", "PAPER") or "PAPER").upper()
        except Exception:
            _mc_mode = "PAPER"
        _is_live = (_mc_mode == "LIVE")

        if _is_live:
            # Audit-required log; emitted before any DB write so it's
            # visible even if downstream paths fail.
            log.warning(
                "LIVE_RECOVERY_REPLAY_SKIPPED client_id=%s mode=%s "
                "reason=live_no_replay_policy lookback_hours=%d cutoff=%s "
                "| WATCHING rows are NOT reset for LIVE clients; only "
                "orphaned PENDING_TRIGGER watcher reattachment will run",
                self.client_id, _mc_mode, _lookback_hours, cutoff_utc[:19],
            )
            count = 0  # No WATCHING rows reset for LIVE.
        else:
            count = run_with_retry(_reset) or 0
        rearmed = 0
        if self.entry_watcher is None:
            log.warning(
                "[%s] RECOVERY: entry_watcher missing — cannot reseed orphaned PENDING_TRIGGER orders",
                self.client_id,
            )
        else:
            rows = run_with_retry(_load_orphaned_pending_trigger_orders) or []
            for row in rows:
                order = dict(row or {})
                local_order_id = str(order.get("local_order_id") or "").strip()
                if not local_order_id:
                    continue
                try:
                    if hasattr(self.entry_watcher, "has_order") and self.entry_watcher.has_order(local_order_id):
                        log.info(
                            "[%s] RECOVERY: watcher already owns local_order_id=%s — skipping duplicate reseed",
                            self.client_id, local_order_id,
                        )
                        continue
                except Exception as exc:
                    log.warning(
                        "[%s] RECOVERY: watcher ownership check failed for local_order_id=%s: %s",
                        self.client_id, local_order_id, exc,
                    )

                plan = self._build_recovery_plan_from_order(order)
                if plan is None:
                    log.warning(
                        "[%s] RECOVERY: cannot reseed local_order_id=%s — invalid_or_missing_side",
                        self.client_id, local_order_id,
                    )
                    continue
                if not getattr(plan, "ticker", "") or getattr(plan, "trigger_price", None) in (None, 0, 0.0):
                    log.warning(
                        "[%s] RECOVERY: cannot reseed local_order_id=%s — missing ticker or trigger_price",
                        self.client_id, local_order_id,
                    )
                    continue
                if str(getattr(plan, "contract_symbol", "") or "").upper().startswith("DEFERRED:"):
                    try:
                        if not hasattr(plan, "metadata") or plan.metadata is None:
                            plan.metadata = {}
                        plan.metadata["contract_deferred"] = True
                    except Exception:
                        pass
                try:
                    armed = bool(self.entry_watcher.watch(plan, local_order_id))
                except Exception as exc:
                    log.error(
                        "[%s] RECOVERY: watcher reseed exception | local_order_id=%s signal_id=%s error=%s",
                        self.client_id, local_order_id, order.get("signal_id"), exc,
                    )
                    continue
                if armed:
                    rearmed += 1
                    log.info(
                        "[%s] RECOVERY: watcher re-armed | local_order_id=%s signal_id=%s contract=%s trigger=%s",
                        self.client_id,
                        local_order_id,
                        order.get("signal_id"),
                        getattr(plan, "contract_symbol", ""),
                        getattr(plan, "trigger_price", None),
                    )
                else:
                    log.warning(
                        "[%s] RECOVERY: watcher reseed failed | local_order_id=%s signal_id=%s reason=%s",
                        self.client_id,
                        local_order_id,
                        order.get("signal_id"),
                        getattr(self.entry_watcher, "_last_reject_reason", None),
                    )

        result["watchers_requeued"] = count + rearmed
        log.info(
            "[%s] RECOVERY: %d WATCHING signals reset to NEW and %d orphaned PENDING_TRIGGER orders re-armed "
            "for watcher reseed (lookback=%dh cutoff=%s)",
            self.client_id, count, rearmed, _lookback_hours, cutoff_utc[:19],
        )
