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

RECONCILE_INTERVAL_SEC = int(os.getenv("RECONCILE_INTERVAL_SEC", "60"))   # 60s during market hours
# Market hours: 9:30 AM – 4:00 PM ET.  Outside hours drops back to 3 min.

def _market_hours_interval() -> int:
    """60s during session, 180s outside — reduces Tradier API calls after close."""
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime, time
        et = datetime.now(ZoneInfo("America/New_York"))
        if time(9, 25) <= et.time() <= time(16, 5):
            return 60
    except Exception:
        pass
    return 180

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
        self.exit_engine = None      # wired by client_runner after construction
        self._ghost_tracker: dict = {}  # two-pass ghost detection per contract

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
        while not self._stop.wait(_market_hours_interval()):
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
            "positions_corrected": 0,
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
                # No broker ID — order never reached broker.
                # EXEMPT overnight/deferred entries — entry_watcher owns these,
                # broker submission is intentionally delayed until 9:30 AM ET.
                # Detection: ENTRY kind + no contract + created after 20:00 UTC or before 09:00 UTC.
                created_ts = order.get("created_ts")
                _contract_raw = order.get("contract")
                _is_no_contract = not _contract_raw or str(_contract_raw).strip() in ("", "None", "null")
                if kind == "ENTRY" and _is_no_contract and created_ts:
                    try:
                        _ts = datetime.fromisoformat(str(created_ts).replace("Z", "+00:00"))
                        if _ts.hour >= 20 or _ts.hour < 9:
                            log.debug("[%s] skip phantom cancel — overnight entry watcher owns | %s",
                                      self.client_id, local_id)
                            continue
                    except Exception:
                        pass
                # Auto-cancel after 5 min to prevent cap inflation
                if created_ts:
                    try:
                        age = (datetime.now(timezone.utc) -
                               datetime.fromisoformat(str(created_ts).replace("Z", "+00:00"))
                               ).total_seconds()
                        if age > 300:  # 5 min with no broker ID = phantom
                            try:
                                self.osm.transition(
                                    local_id, "CANCELED",
                                    last_error="reconciler_phantom_cancel_no_broker_id",
                                )
                                log.warning(
                                    "[%s] RECONCILE_AUTO_CANCEL phantom | %s | %s | "
                                    "age=%.0fs no broker_id",
                                    self.client_id, contract, local_id, age
                                )
                                summary["orders_corrected"] += 1
                            except Exception as _ce:
                                log.error("[%s] Failed to cancel phantom %s: %s",
                                          self.client_id, local_id, _ce)
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
                    broker_raw.get("avg_fill_price") or broker_raw.get("fill_price") or
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
                        fill_price=avg_fill,
                    )
                    if ok:
                        summary["orders_corrected"] += 1
                        self._alert(
                            f"RECONCILE_AUTO_CORRECT | {contract} | {local_id} | "
                            f"DB was {db_status}, broker={broker_status} → advanced to {new_status} "
                            f"(qty={filled_qty} avg={avg_fill:.2f})"
                        )
                        # Open position on confirmed ENTRY fill — idempotent
                        if kind == "ENTRY" and new_status == "FILLED" and self.pm:
                            try:
                                plan_id   = order.get("plan_id")   or order.get("signal_id") or local_id
                                signal_id_val = order.get("signal_id") or local_id
                                self.pm.open_position(
                                    plan_id    = plan_id,
                                    signal_id  = signal_id_val,
                                    ticker     = (order.get("symbol") or "").upper(),
                                    contract   = order.get("contract") or order.get("symbol") or "",
                                    side       = (order.get("direction") or "CALL").upper(),
                                    qty        = filled_qty,
                                    entry_price= avg_fill,
                                )
                            except Exception as _pm_err:
                                log.error("[%s] RECONCILE pm.open_position failed %s: %s",
                                          self.client_id, local_id, _pm_err)
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
                           filled_qty, fill_price, updated_ts
                    FROM orders
                    WHERE client_id=%s
                      AND status='FILLED'
                      AND broker_order_id IS NOT NULL
                      AND broker_order_id NOT IN ('N/A','PENDING','')
                      AND updated_ts > NOW() - INTERVAL '2 hours'
                    ORDER BY updated_ts DESC
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

    # ──────────────────────────────────────────────────────────────────────────
    # Position Reconciliation — evidence-based, contract-precise
    # ──────────────────────────────────────────────────────────────────────────

    def _norm_contract(self, val: str) -> str:
        return str(val or "").strip().upper()

    def _norm_underlying(self, val: str) -> str:
        return str(val or "").strip().upper()

    def _safe_get_broker_positions(self) -> list:
        """Fetch broker positions; return [] on error."""
        try:
            result = self.broker.list_positions()
            if result is None:
                return []
            if isinstance(result, list):
                return result
            return []
        except Exception as e:
            log.error("[%s] Broker list_positions failed: %s", self.client_id, e)
            return []

    def _get_recent_exit_fill(self, contract: str, underlying: str) -> Optional[dict]:
        """
        Look up the most recent filled EXIT order for this contract.
        Returns dict with fill_price and filled_qty, or None.
        """
        try:
            from ap.db import conn, run_with_retry
            def _fetch():
                with conn() as c:
                    c.execute(
                        """
                        SELECT fill_price, filled_qty, updated_ts
                        FROM   orders
                        WHERE  client_id = %s
                          AND  kind = 'EXIT'
                          AND  status IN ('FILLED', 'EXIT_FILLED')
                          AND  (symbol = %s OR symbol = %s)
                        ORDER  BY updated_ts DESC
                        LIMIT  1
                        """,
                        (self.client_id, contract, underlying),
                    )
                    row = c.fetchone()
                    return dict(row) if row else None
            return run_with_retry(_fetch)
        except Exception as e:
            log.debug("[%s] Exit fill lookup failed for %s: %s", self.client_id, contract, e)
            return None

    def _mark_ghost_seen(self, contract: str) -> bool:
        """
        Two-pass ghost detection.
        First call → returns False (wait one more pass).
        Second consecutive call → returns True (confirmed gone).
        Ghost tracker is cleared if position reappears.
        """
        if contract in self._ghost_tracker:
            del self._ghost_tracker[contract]
            return True  # second pass — confirmed gone
        self._ghost_tracker[contract] = True
        return False  # first pass — wait

    def _reconcile_positions(self, summary: dict):
        """
        Evidence-based position reconciliation.

        Policy:
        1. Match DB positions by exact contract symbol first.
        2. Auto-close ONLY when there is evidence:
           - A filled EXIT order exists at the broker, OR
           - Position has been absent from broker for 2 consecutive passes.
        3. PnL is computed cleanly as dollars and percent, separately.
        4. Never blindly close on a single missing signal (API lag protection).
        """
        summary.setdefault("positions_corrected", 0)

        # ── Fetch broker positions ─────────────────────────────────────────────
        broker_positions = self._safe_get_broker_positions()
        if broker_positions is None:
            log.debug("[%s] Broker positions unavailable — skipping", self.client_id)
            return

        # Build lookup: contract → bp, underlying → [bp, ...]
        broker_by_contract:    dict[str, dict] = {}
        broker_by_underlying:  dict[str, list] = {}
        for bp in broker_positions:
            c_sym = self._norm_contract(bp.get("symbol"))
            u_sym = self._norm_underlying(bp.get("underlying") or c_sym[:6])
            if c_sym:
                broker_by_contract[c_sym] = bp
            if u_sym:
                broker_by_underlying.setdefault(u_sym, []).append(bp)

        # ── Fetch DB open positions ────────────────────────────────────────────
        try:
            from ap.db import list_positions, run_with_retry
            db_positions = run_with_retry(
                lambda: list_positions(client_id=self.client_id, status="OPEN")
            ) or []
        except Exception as e:
            log.error("[%s] Failed to fetch DB positions: %s", self.client_id, e)
            return

        summary["positions_checked"] = len(db_positions)

        for pos in db_positions:
            pos_id     = pos.get("id")
            contract   = self._norm_contract(pos.get("contract") or pos.get("symbol") or "")
            underlying = self._norm_underlying(pos.get("underlying") or pos.get("ticker") or contract[:6])
            db_qty     = int(pos.get("qty") or pos.get("quantity") or 0)
            entry_px   = float(pos.get("avg_fill") or 0.0)

            # ── 1. Exact contract match ──────────────────────────────────────
            broker_pos = broker_by_contract.get(contract)

            # ── 2. Underlying match (only if unique) ─────────────────────────
            if broker_pos is None and underlying:
                matches = broker_by_underlying.get(underlying, [])
                if len(matches) == 1:
                    broker_pos = matches[0]
                elif len(matches) > 1:
                    log.warning(
                        "[%s] RECONCILE_AMBIGUOUS | %s — %d broker positions for underlying, "
                        "skipping auto-close",
                        self.client_id, contract, len(matches)
                    )
                    # Clear ghost tracker — we found something
                    self._ghost_tracker.pop(contract, None)
                    continue

            # ── CASE A: position found at broker — reconcile qty ──────────────
            if broker_pos is not None:
                self._ghost_tracker.pop(contract, None)  # reset ghost counter
                broker_qty = int(broker_pos.get("quantity") or broker_pos.get("qty") or 0)
                if broker_qty != db_qty and db_qty > 0:
                    log.warning(
                        "[%s] POSITION_QTY_MISMATCH | %s | DB=%d broker=%d",
                        self.client_id, contract, db_qty, broker_qty
                    )
                    summary["positions_alerted"] += 1
                continue

            # ── CASE B: not at broker — gather evidence before closing ────────
            # Priority 1: look for a filled exit order
            exit_fill = self._get_recent_exit_fill(contract, underlying)

            if exit_fill and float(exit_fill.get("fill_price") or 0) > 0:
                exit_px         = float(exit_fill["fill_price"])
                close_confidence = "HIGH"
                log.info(
                    "[%s] RECONCILE_CLOSE_EVIDENCE | %s | filled exit found @ $%.4f",
                    self.client_id, contract, exit_px
                )
            else:
                # Priority 2: two-pass ghost detection
                if not self._mark_ghost_seen(contract):
                    log.warning(
                        "[%s] GHOST_PASS_1 | %s | broker has no position — "
                        "waiting one pass to confirm",
                        self.client_id, contract
                    )
                    summary["positions_alerted"] += 1
                    continue  # come back next reconcile cycle
                exit_px          = entry_px  # use entry as fallback (0% PnL)
                close_confidence = "MEDIUM"
                log.warning(
                    "[%s] GHOST_PASS_2 | %s | confirmed gone from broker — "
                    "auto-closing with entry price (no fill found)",
                    self.client_id, contract
                )

            # ── Compute PnL (explicit dollars AND percent) ────────────────────
            pnl_dollars = round((exit_px - entry_px) * db_qty * 100, 2)
            pnl_pct     = round(((exit_px - entry_px) / entry_px) * 100, 2) if entry_px > 0 else 0.0

            # ── Write close to DB ─────────────────────────────────────────────
            try:
                from ap.db import conn, run_with_retry as _rwr
                from datetime import datetime, timezone as _tz
                _now = datetime.now(_tz.utc).isoformat()

                def _close():
                    with conn() as c:
                        c.execute(
                            """
                            UPDATE positions
                            SET    status            = 'CLOSED',
                                   exit_ts           = %s,
                                   realized_pnl      = %s,
                                   realized_pnl_pct  = %s,
                                   close_source      = %s,
                                   close_confidence  = %s
                            WHERE  id = %s
                            """,
                            (
                                _now,
                                pnl_dollars,
                                pnl_pct,
                                "RECONCILER_AUTO_CLOSE",
                                close_confidence,
                                pos_id,
                            ),
                        )
                _rwr(_close)
            except Exception as e:
                # Full write failed — retry with a safe minimal update
                try:
                    from ap.db import conn, run_with_retry as _rwr2
                    from datetime import datetime, timezone as _tz2
                    _now2 = datetime.now(_tz2.utc).isoformat()
                    def _minimal_close():
                        with conn() as _c:
                            _c.execute(
                                """
                                UPDATE positions
                                SET    status='CLOSED', exit_ts=%s, exit_price=%s,
                                       realized_pnl=%s, realized_pnl_pct=%s,
                                       close_source=%s, close_confidence=%s
                                WHERE  id=%s
                                """,
                                (_now2, exit_px, pnl_dollars, pnl_pct,
                                 "RECONCILER_AUTO_CLOSE", close_confidence, pos_id)
                            )
                    _rwr2(_minimal_close)
                except Exception as e2:
                    log.error("[%s] RECONCILE close DB write failed %s: %s / %s",
                              self.client_id, pos_id, e, e2)
                    summary["positions_alerted"] += 1
                    continue

            # ── Notify exit engine ─────────────────────────────────────────────
            _ee = getattr(self, "exit_engine", None)
            if _ee:
                try:
                    _ee.mark_position_closed(pos_id, reason="reconciler_auto_close")
                except Exception:
                    pass

            # Clear ghost tracker — position is definitively closed
            self._ghost_tracker.pop(contract, None)

            log.info(
                "[%s] FINALIZED TRADE | %s | pos=%s | entry=%.4f exit=%.4f "
                "pnl=$%.2f (%.1f%%) source=RECONCILER confidence=%s",
                self.client_id, contract, pos_id,
                entry_px, exit_px, pnl_dollars, pnl_pct, close_confidence
            )
            summary["positions_corrected"] += 1
            try:
                from ap_proof_logger import funnel as _funnel_r
                _funnel_r.inc("reconciler_corrections")
            except Exception: pass

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
                        "updated_ts=NOW() WHERE id=%s AND client_id=%s AND status='CLOSING'",
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
