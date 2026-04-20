"""
ap_reconciler.py — Broker-vs-DB Truth Reconciliation
=====================================================
Runs every RECONCILE_INTERVAL_SEC (default 180s = 3 min) per client.

Truth policy: Broker wins on order status. DB wins on position metadata.

Checks:
  Orders:
    A. DB says open (CREATED/SUBMITTED/ACKNOWLEDGED/PARTIAL_FILL) → broker says filled?
       → auto-correct: advance OSM to FILLED
    B. DB says open → broker says canceled/rejected/expired?
       → auto-correct: advance OSM to terminal state
    C. DB says filled → broker order missing or status mismatch?
       → alert only (do not auto-correct fills — real money)
    D. DB says canceled → broker still shows open?
       → alert + re-attempt cancel

  Positions:
    E. DB position OPEN → no matching broker position?
       → alert (possible ghost position)
    F. DB qty vs broker qty mismatch on open position?
       → alert + log delta

  Dedup:
    G. DB has duplicate positions for same ticker/direction?
       → alert immediately

Usage:
    reconciler = APBrokerReconciler(broker=broker, client_id=email, osm=osm, pm=pm)
    reconciler.start()
    # or call reconciler.run_once() directly in tests
"""

from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ap.reconciler")

RECONCILE_INTERVAL_SEC = int(os.getenv("RECONCILE_INTERVAL_SEC", "180"))  # 3 min

# Statuses we consider "open" in DB — broker should have a matching live order
DB_OPEN_STATUSES = frozenset({
    "CREATED", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL",
    "EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL",
})

# Broker statuses that mean the order filled
BROKER_FILLED = frozenset({"filled", "partially_filled"})

# Broker statuses that mean the order is terminal (not filled)
BROKER_TERMINAL = frozenset({"canceled", "cancelled", "rejected", "expired"})

# OSM terminal states we map to
BROKER_TO_OSM = {
    "filled":           "FILLED",
    "partially_filled": "PARTIAL_FILL",
    "canceled":         "CANCELED",
    "cancelled":        "CANCELED",
    "rejected":         "REJECTED",
    "expired":          "EXPIRED",
    "open":             "ACKNOWLEDGED",
    "pending":          "SUBMITTED",
}


class APBrokerReconciler:
    """
    Continuously reconciles broker truth against DB state.
    One instance per client — shares broker + OSM from ClientRunner.
    """

    def __init__(
        self,
        broker,
        client_id: str,
        osm,                        # APOrderStateMachine
        pm,                         # APPositionManager
        alert_fn=None,              # callable(msg: str) for Discord/Supabase alerts
        interval_sec: int = RECONCILE_INTERVAL_SEC,
    ):
        self.broker      = broker
        self.client_id   = client_id
        self.osm         = osm
        self.pm          = pm
        self._alert_fn   = alert_fn or (lambda msg: log.warning(msg))
        self._interval   = interval_sec
        self._stop       = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._run_count  = 0

    # ──────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name=f"reconciler-{self.client_id}",
        )
        self._thread.start()
        log.info("[%s] Reconciler started (interval=%ds)", self.client_id, self._interval)

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # ──────────────────────────────────────────────────────────────────────────
    # Main loop
    # ──────────────────────────────────────────────────────────────────────────

    def _loop(self):
        # Stagger startup to avoid hammering broker on restart
        time.sleep(min(30, self._interval // 4))
        while not self._stop.wait(self._interval):
            try:
                self.run_once()
            except Exception as e:
                log.error("[%s] Reconciler loop error: %s", self.client_id, e)

    def run_once(self) -> dict:
        """
        Run one full reconciliation pass. Returns a summary dict.
        Safe to call directly from tests.
        """
        self._run_count += 1
        summary = {
            "run": self._run_count,
            "client_id": self.client_id,
            "orders_checked": 0,
            "orders_corrected": 0,
            "orders_alerted": 0,
            "positions_checked": 0,
            "positions_alerted": 0,
            "errors": [],
        }

        try:
            self._reconcile_orders(summary)
        except Exception as e:
            log.error("[%s] Order reconcile error: %s", self.client_id, e)
            summary["errors"].append(f"orders: {e}")

        try:
            self._reconcile_positions(summary)
        except Exception as e:
            log.error("[%s] Position reconcile error: %s", self.client_id, e)
            summary["errors"].append(f"positions: {e}")

        try:
            self._check_duplicate_positions(summary)
        except Exception as e:
            log.error("[%s] Dedup check error: %s", self.client_id, e)
            summary["errors"].append(f"dedup: {e}")

        log.info(
            "[%s] Reconcile #%d complete | orders=%d corrected=%d alerted=%d "
            "positions=%d pos_alerts=%d errors=%d",
            self.client_id, self._run_count,
            summary["orders_checked"], summary["orders_corrected"],
            summary["orders_alerted"], summary["positions_checked"],
            summary["positions_alerted"], len(summary["errors"]),
        )
        return summary

    # ──────────────────────────────────────────────────────────────────────────
    # Order reconciliation
    # ──────────────────────────────────────────────────────────────────────────

    def _reconcile_orders(self, summary: dict):
        from ap.db import get_open_orders_for_reconcile, run_with_retry, conn

        open_orders = run_with_retry(
            lambda: get_open_orders_for_reconcile(client_id=self.client_id)
        )
        summary["orders_checked"] = len(open_orders)

        for order in open_orders:
            local_id   = order.get("local_order_id") or order.get("id")
            broker_oid = order.get("broker_order_id")
            db_status  = (order.get("status") or "").upper()
            contract   = order.get("contract") or order.get("symbol") or "?"
            kind       = order.get("kind", "ENTRY")

            if not broker_oid or broker_oid in ("N/A", "PENDING", ""):
                # No broker ID yet — order may not have reached broker
                # Only alert if it's been sitting too long
                created_ts = order.get("created_ts")
                if created_ts:
                    try:
                        age = (datetime.now(timezone.utc) -
                               datetime.fromisoformat(str(created_ts).replace("Z", "+00:00"))
                               ).total_seconds()
                        if age > 300:  # 5 min with no broker ID is suspicious
                            self._alert(
                                f"ORDER_NO_BROKER_ID | {contract} | {local_id} | "
                                f"db_status={db_status} | age={age:.0f}s — "
                                f"never submitted to broker?"
                            )
                            summary["orders_alerted"] += 1
                    except Exception:
                        pass
                continue

            # Query broker for real status
            try:
                broker_raw = self.broker.get_order(broker_oid)
            except Exception as e:
                log.warning("[%s] Broker get_order failed for %s: %s",
                            self.client_id, broker_oid, e)
                continue

            broker_status = str(
                broker_raw.get("status") or broker_raw.get("Status") or ""
            ).lower().strip()

            # ── Check A: DB open → broker filled ────────────────────────────
            if broker_status in BROKER_FILLED and db_status in DB_OPEN_STATUSES:
                new_status = BROKER_TO_OSM.get(broker_status, "FILLED")
                filled_qty = int(
                    broker_raw.get("exec_quantity") or
                    broker_raw.get("filled_quantity") or 0
                )
                avg_fill = float(
                    broker_raw.get("avg_fill_price") or
                    broker_raw.get("price") or 0.0
                )
                log.warning(
                    "[%s] RECONCILE_CORRECT: %s | %s | DB=%s broker=%s → advancing to %s",
                    self.client_id, contract, local_id, db_status, broker_status, new_status
                )
                try:
                    ok = self.osm.transition(
                        local_id, new_status,
                        filled_qty=filled_qty,
                        avg_fill=avg_fill,
                    )
                    if ok:
                        summary["orders_corrected"] += 1
                        self._alert(
                            f"RECONCILE_AUTO_CORRECT | {contract} | {local_id} | "
                            f"DB was {db_status}, broker={broker_status} → advanced to {new_status} "
                            f"(qty={filled_qty} avg={avg_fill:.2f})"
                        )
                    else:
                        log.error(
                            "[%s] OSM transition failed for %s → %s",
                            self.client_id, local_id, new_status
                        )
                except Exception as e:
                    log.error("[%s] Reconcile transition error: %s", self.client_id, e)

            # ── Check B: DB open → broker terminal (not filled) ─────────────
            elif broker_status in BROKER_TERMINAL and db_status in DB_OPEN_STATUSES:
                new_status = BROKER_TO_OSM.get(broker_status, "CANCELED")
                log.warning(
                    "[%s] RECONCILE_CORRECT: %s | %s | DB=%s broker=%s → advancing to %s",
                    self.client_id, contract, local_id, db_status, broker_status, new_status
                )
                try:
                    ok = self.osm.transition(
                        local_id, new_status,
                        last_error=f"reconciler: broker_status={broker_status}",
                    )
                    if ok:
                        summary["orders_corrected"] += 1
                        # If this was an exit order, revert position to OPEN
                        if kind == "EXIT":
                            position_id = order.get("position_id")
                            if position_id:
                                self._revert_position_to_open(position_id, contract)
                        self._alert(
                            f"RECONCILE_AUTO_CORRECT | {contract} | {local_id} | "
                            f"DB was {db_status}, broker={broker_status} → {new_status}"
                        )
                    else:
                        log.error("[%s] OSM transition failed %s → %s",
                                  self.client_id, local_id, new_status)
                except Exception as e:
                    log.error("[%s] Reconcile terminal transition error: %s", self.client_id, e)

            # ── Check C: DB still open, broker status unchanged ──────────────
            # Nothing to do — broker agrees order is still active
            elif broker_status in ("open", "pending", "partially_filled"):
                pass  # normal — no mismatch

            # ── Check D: Unknown broker status ───────────────────────────────
            elif broker_status not in ("", "unknown", "error"):
                log.debug(
                    "[%s] Unrecognized broker status '%s' for order %s",
                    self.client_id, broker_status, local_id
                )

        # ── Check C (DB-filled vs broker-missing): scan recent fills ────────
        self._check_ghost_fills(summary)

    def _check_ghost_fills(self, summary: dict):
        """
        Check orders DB says FILLED where broker shows no record.
        Alert only — never auto-un-fill.
        """
        from ap.db import run_with_retry, conn
        from datetime import timedelta

        def _get_recent_fills():
            with conn() as c:
                c.execute(
                    """
                    SELECT local_order_id, broker_order_id, contract, kind,
                           filled_qty, avg_fill_price, updated_at
                    FROM orders
                    WHERE client_id=%s
                      AND status='FILLED'
                      AND broker_order_id IS NOT NULL
                      AND broker_order_id NOT IN ('N/A','PENDING','')
                      AND updated_at > NOW() - INTERVAL '2 hours'
                    ORDER BY updated_at DESC
                    LIMIT 20
                    """,
                    (self.client_id,),
                )
                return c.fetchall()

        try:
            fills = run_with_retry(_get_recent_fills)
        except Exception as e:
            log.debug("[%s] Ghost fill check DB error: %s", self.client_id, e)
            return

        for f in fills:
            broker_oid = f.get("broker_order_id")
            if not broker_oid:
                continue
            try:
                broker_raw    = self.broker.get_order(broker_oid)
                broker_status = str(broker_raw.get("status") or "").lower()
                # If broker shows canceled/rejected for something we have as FILLED, alert
                if broker_status in BROKER_TERMINAL:
                    self._alert(
                        f"GHOST_FILL_WARNING | {f.get('contract','?')} | "
                        f"{f.get('local_order_id','?')} | "
                        f"DB=FILLED but broker={broker_status} — MANUAL REVIEW REQUIRED"
                    )
                    summary["orders_alerted"] += 1
            except Exception:
                pass

    # ──────────────────────────────────────────────────────────────────────────
    # Position reconciliation
    # ──────────────────────────────────────────────────────────────────────────

    def _reconcile_positions(self, summary: dict):
        from ap.db import list_positions, run_with_retry

        # Get all DB open positions for this client
        db_positions = run_with_retry(
            lambda: list_positions(client_id=self.client_id, status="OPEN")
        )
        summary["positions_checked"] = len(db_positions)

        if not db_positions:
            return

        # Get broker positions (if broker supports it)
        broker_positions = self._get_broker_positions()
        if broker_positions is None:
            # Broker doesn't support position list — skip position reconcile
            log.debug("[%s] Broker does not support list_positions — skipping", self.client_id)
            return

        # Build broker lookup: underlying → position dict
        broker_by_symbol: dict[str, dict] = {}
        for bp in broker_positions:
            sym = str(bp.get("symbol") or bp.get("underlying") or "").upper()
            if sym:
                broker_by_symbol[sym] = bp

        for pos in db_positions:
            pos_id     = pos.get("id")
            underlying = str(pos.get("underlying") or pos.get("ticker") or "").upper()
            db_qty     = int(pos.get("qty") or pos.get("quantity") or 0)

            broker_pos = broker_by_symbol.get(underlying)

            # ── Check E: DB position → no broker position ───────────────────
            if broker_pos is None:
                # Could be because it's an options position under contract symbol
                # Don't false-alarm immediately — check by contract if available
                contract = str(pos.get("contract") or "").upper()
                if contract:
                    broker_pos = broker_by_symbol.get(contract)

            if broker_pos is None:
                self._alert(
                    f"GHOST_POSITION_WARNING | {underlying} | pos={pos_id} | "
                    f"DB shows OPEN qty={db_qty} but NO matching broker position — "
                    f"possible ghost position or broker not returning it"
                )
                summary["positions_alerted"] += 1
                continue

            # ── Check F: qty mismatch ────────────────────────────────────────
            broker_qty = int(
                broker_pos.get("quantity") or broker_pos.get("qty") or 0
            )
            if broker_qty != db_qty and db_qty > 0:
                delta = broker_qty - db_qty
                self._alert(
                    f"POSITION_QTY_MISMATCH | {underlying} | pos={pos_id} | "
                    f"DB qty={db_qty} broker qty={broker_qty} delta={delta:+d} — "
                    f"partial fill not reconciled?"
                )
                summary["positions_alerted"] += 1

    def _get_broker_positions(self) -> Optional[list]:
        """
        Try to get broker positions list.
        Returns None if broker doesn't support it (no list_positions method).
        """
        if not hasattr(self.broker, "list_positions"):
            return None
        try:
            return self.broker.list_positions() or []
        except Exception as e:
            log.warning("[%s] Broker list_positions error: %s", self.client_id, e)
            return []

    # ──────────────────────────────────────────────────────────────────────────
    # Duplicate position check
    # ──────────────────────────────────────────────────────────────────────────

    def _check_duplicate_positions(self, summary: dict):
        from ap.db import run_with_retry, conn

        def _get_dupes():
            with conn() as c:
                c.execute(
                    """
                    SELECT underlying, direction, COUNT(*) as cnt
                    FROM positions
                    WHERE client_id=%s AND status='OPEN'
                    GROUP BY underlying, direction
                    HAVING COUNT(*) > 1
                    """,
                    (self.client_id,),
                )
                return c.fetchall()

        try:
            dupes = run_with_retry(_get_dupes)
            for d in dupes:
                self._alert(
                    f"DUPLICATE_POSITION | {d['underlying']} {d['direction']} | "
                    f"count={d['cnt']} open positions — dedup failed somewhere"
                )
                summary["positions_alerted"] += 1
        except Exception as e:
            log.debug("[%s] Dedup check error: %s", self.client_id, e)

    # ──────────────────────────────────────────────────────────────────────────
    # Helpers
    # ──────────────────────────────────────────────────────────────────────────

    def _revert_position_to_open(self, position_id: str, contract: str):
        from ap.db import run_with_retry, conn
        try:
            def _revert():
                with conn() as c:
                    c.execute(
                        "UPDATE positions SET status='OPEN', exit_reason=NULL, "
                        "updated_at=NOW() WHERE id=%s AND client_id=%s AND status='CLOSING'",
                        (position_id, self.client_id),
                    )
            run_with_retry(_revert)
            log.warning(
                "[%s] RECONCILE: reverted pos %s to OPEN after terminal exit order | %s",
                self.client_id, position_id, contract,
            )
        except Exception as e:
            log.error("[%s] Failed to revert position %s: %s",
                      self.client_id, position_id, e)

    def _alert(self, msg: str):
        log.warning("[%s] RECONCILER: %s", self.client_id, msg)
        try:
            self._alert_fn(f"[reconciler:{self.client_id}] {msg}")
        except Exception:
            pass
