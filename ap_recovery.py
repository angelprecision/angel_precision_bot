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
        self.client_id     = client_id
        self.broker        = broker
        self.osm           = osm
        self.pm            = pm
        self.mc            = master_control
        self.exit_engine   = exit_engine
        self.entry_watcher = entry_watcher

    # ──────────────────────────────────────────────────────────────────────────
    # Entry point
    # ──────────────────────────────────────────────────────────────────────────

    def run(self) -> dict:
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

    # ──────────────────────────────────────────────────────────────────────────
    # 1. Position recovery
    # ──────────────────────────────────────────────────────────────────────────

    def _recover_positions(self, result: dict):
        """Re-register open positions with PositionManager in-memory state."""
        from ap.db import list_positions, run_with_retry

        opens = run_with_retry(
            lambda: list_positions(client_id=self.client_id, status="OPEN")
        )
        closings = run_with_retry(
            lambda: list_positions(client_id=self.client_id, status="CLOSING")
        )
        all_active = (opens or []) + (closings or [])

        for pos in all_active:
            pos_id     = pos.get("id")
            underlying = pos.get("underlying") or pos.get("ticker", "?")
            status     = pos.get("status", "OPEN")
            qty        = int(pos.get("qty") or pos.get("quantity") or 0)
            direction  = pos.get("direction", "CALL")

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
                "[%s] RECOVERY: position restored | %s %s qty=%d status=%s pos=%s",
                self.client_id, underlying, direction, qty, status, pos_id,
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
                broker_raw    = self.broker.get_order(broker_oid)
                broker_status = str(broker_raw.get("status") or "").lower()
            except Exception as e:
                log.warning("[%s] RECOVERY: broker get_order failed for %s: %s",
                            self.client_id, broker_oid, e)
                continue

            # Map broker → OSM status
            from ap_reconciler import BROKER_TO_OSM, BROKER_FILLED, BROKER_TERMINAL
            if broker_status in BROKER_FILLED or broker_status in BROKER_TERMINAL:
                new_status = BROKER_TO_OSM.get(broker_status)
                if new_status and new_status != db_status:
                    filled_qty = int(broker_raw.get("exec_quantity") or 0)
                    avg_fill   = float(broker_raw.get("avg_fill_price") or 0.0)
                    try:
                        ok = self.osm.transition(
                            local_id, new_status,
                            filled_qty=filled_qty,
                            fill_price=avg_fill,
                            last_error=(
                                f"recovery: broker_status={broker_status}"
                                if broker_status in BROKER_TERMINAL else None
                            ),
                        )
                        if ok:
                            result["entries_corrected"] += 1
                            log.info(
                                "[%s] RECOVERY: corrected entry %s | %s | %s → %s",
                                self.client_id, local_id, contract, db_status, new_status,
                            )
                    except Exception as e:
                        log.error("[%s] RECOVERY: OSM transition failed: %s", self.client_id, e)

    # ──────────────────────────────────────────────────────────────────────────
    # 3. Exit protection reattachment
    # ──────────────────────────────────────────────────────────────────────────

    def _reattach_exit_protections(self, result: dict):
        """
        For every CLOSING position, verify the exit order is still live at
        broker. If the exit filled while we were down, advance the position.
        If exit canceled/expired, revert position to OPEN.
        """
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
                broker_raw    = self.broker.get_order(broker_oid)
                broker_status = str(broker_raw.get("status") or "").lower()
            except Exception as e:
                log.warning("[%s] RECOVERY: broker exit order check failed: %s",
                            self.client_id, e)
                continue

            from ap_reconciler import BROKER_FILLED, BROKER_TERMINAL, BROKER_TO_OSM

            if broker_status in BROKER_FILLED:
                # Exit actually filled while we were down — close the position
                filled_qty = int(broker_raw.get("exec_quantity") or 0)
                avg_fill   = float(broker_raw.get("avg_fill_price") or 0.0)
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
                        local_id, "CANCELED",
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
        Recompute how much capital is reserved by open positions + pending entries.
        Updates master_control.account_equity if needed.
        """
        from ap.db import list_positions, run_with_retry

        opens = run_with_retry(
            lambda: list_positions(client_id=self.client_id, status="OPEN")
        ) or []
        closings = run_with_retry(
            lambda: list_positions(client_id=self.client_id, status="CLOSING")
        ) or []

        total_reserved = 0.0
        for pos in opens + closings:
            cost = float(pos.get("reserved_cost") or pos.get("cost") or 0.0)
            total_reserved += cost

        result["buying_power_reserved"] = total_reserved

        # Sync into master_control if it tracks this
        if hasattr(self.mc, "_reserved_capital"):
            self.mc._reserved_capital = total_reserved
        if hasattr(self.mc, "reserved_capital"):
            self.mc.reserved_capital = total_reserved

        log.info(
            "[%s] RECOVERY: buying power reserved=$%.2f across %d open + %d closing positions",
            self.client_id, total_reserved, len(opens), len(closings),
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
                           payload->>'ticker'    AS ticker,
                           payload->>'direction' AS direction,
                           payload->>'timeframe' AS timeframe,
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
            direction = str(row.get("direction") or "CALL").upper()
            timeframe = str(row.get("timeframe") or "1d")

            # Re-add both the signal_id key and the setup key
            # _seen_signals is a dict[str, float] — use direct assignment not .add()
            _ts = time.time()
            if signal_id:
                self.mc._seen_signals[f"sig:{signal_id}:{self.client_id}"] = _ts
            if ticker:
                setup_key = f"{self.client_id}:{ticker}:{direction}:{timeframe}"
                self.mc._seen_signals[setup_key] = _ts
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
        direction = str(
            order.get("direction")
            or meta.get("direction")
            or meta.get("side")
            or "CALL"
        ).upper()
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
            metadata=dict(meta),
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
        # Look back 48 hours — covers Sunday evening scanner signals for Monday open.
        # 48h ensures Sunday 4:15 PM ET signals are still in window at any
        # Monday restart time (Sunday 4:15 PM ET = ~28h before Monday close).
        _lookback_hours = 48
        cutoff_utc = (now_et.astimezone(timezone.utc) - timedelta(hours=_lookback_hours)).isoformat()

        def _reset():
            with conn() as c:
                c.execute(
                    """
                    UPDATE trade_queue
                    SET    status = 'NEW'
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
                               AND (
                                     o.broker_order_id IS NULL
                                  OR TRIM(COALESCE(o.broker_order_id, '')) = ''
                                  OR UPPER(TRIM(COALESCE(o.broker_order_id, ''))) IN ('N/A', 'NA', 'NONE', 'NULL')
                               )
                               AND o.submitted_ts IS NULL
                               AND o.filled_ts IS NULL
                           )
                    """,
                    (self.client_id, cutoff_utc),
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
                      AND (
                            broker_order_id IS NULL
                         OR TRIM(COALESCE(broker_order_id, '')) = ''
                         OR UPPER(TRIM(COALESCE(broker_order_id, ''))) IN ('N/A', 'NA', 'NONE', 'NULL')
                      )
                      AND submitted_ts IS NULL
                      AND filled_ts IS NULL
                    ORDER BY created_ts ASC
                    """,
                    (self.client_id, cutoff_utc),
                )
                return c.fetchall()

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
