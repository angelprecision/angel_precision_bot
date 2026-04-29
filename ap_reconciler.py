"""
ap_reconciler.py — Broker-vs-DB Truth Reconciliation
=====================================================
Runs every RECONCILE_INTERVAL_SEC (default 180s = 3 min) per client.

Truth policy:
  - Broker wins on order status.
  - DB wins on position metadata when a position exists.
  - Broker-open option positions must NEVER be silently ignored if DB is missing them.

Critical recovery invariant:
  If the broker has an open option position and the DB has no matching OPEN/CLOSING
  position, the reconciler imports a conservative OPEN position into DB and seeds
  the exit engine. This prevents restart/orphan failures.

Checks:
  Orders:
    A. DB says open → broker says filled
       → auto-correct: advance OSM to FILLED
    B. DB says open → broker says canceled/rejected/expired
       → auto-correct: advance OSM to terminal state
    C. DB says filled → broker order terminal
       → alert only
    D. Phantom DB orders with no broker id
       → auto-cancel after safety window, except deferred overnight watcher entries

  Positions:
    E. DB OPEN/CLOSING → broker position missing
       → evidence-based close only after filled exit evidence or two-pass ghost confirm
    F. DB qty vs broker qty mismatch
       → alert
    G. Broker OPEN → DB missing
       → import DB position + seed exit engine + critical alert

  Dedup:
    H. DB has duplicate OPEN positions for same underlying/direction
       → alert immediately

Usage:
    reconciler = APBrokerReconciler(broker=broker, client_id=email, osm=osm, pm=pm)
    reconciler.exit_engine = exit_engine
    reconciler.start()
    # or call reconciler.run_once() directly in tests
"""

from __future__ import annotations

import logging
import os
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ap.reconciler")

RECONCILE_INTERVAL_SEC = int(os.getenv("RECONCILE_INTERVAL_SEC", "60"))
IMPORT_MISSING_BROKER_POSITIONS = (
    os.getenv("RECONCILER_IMPORT_MISSING_BROKER_POSITIONS", "1")
    .strip()
    .lower()
    in {"1", "true", "yes", "on"}
)

# Statuses we consider "open" in DB — broker should have a matching live order.
DB_OPEN_STATUSES = frozenset({
    "CREATED", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL",
    "EXIT_REQUESTED", "EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL",
})

DB_OPEN_POSITION_STATUSES = ("OPEN", "CLOSING")

BROKER_FILLED = frozenset({"filled", "partially_filled"})
BROKER_TERMINAL = frozenset({"canceled", "cancelled", "rejected", "expired"})

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


def _market_hours_interval() -> int:
    """60s during session, 180s outside — reduces broker API calls after close."""
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt, time as _time

        et = _dt.now(ZoneInfo("America/New_York"))
        if _time(9, 25) <= et.time() <= _time(16, 5):
            return 60
    except Exception:
        pass
    return 180


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
        self._ghost_tracker: dict[str, bool] = {}  # two-pass ghost detection per contract

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
        log.info(
            "[%s] Reconciler started (interval=%ds import_missing_broker_positions=%s)",
            self.client_id,
            self._interval,
            IMPORT_MISSING_BROKER_POSITIONS,
        )

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
        # Stagger startup to avoid hammering broker on restart.
        time.sleep(min(30, max(1, self._interval // 4)))
        while not self._stop.wait(_market_hours_interval()):
            try:
                self.run_once()
            except Exception as e:
                log.error("[%s] Reconciler loop error: %s", self.client_id, e, exc_info=True)

    def run_once(self) -> dict:
        """Run one full reconciliation pass. Safe to call directly from tests."""
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
            "positions_imported": 0,
            "errors": [],
        }

        try:
            self._reconcile_orders(summary)
        except Exception as e:
            log.error("[%s] Order reconcile error: %s", self.client_id, e, exc_info=True)
            summary["errors"].append(f"orders: {e}")

        try:
            self._reconcile_positions(summary)
        except Exception as e:
            log.error("[%s] Position reconcile error: %s", self.client_id, e, exc_info=True)
            summary["errors"].append(f"positions: {e}")

        try:
            self._check_duplicate_positions(summary)
        except Exception as e:
            log.error("[%s] Dedup check error: %s", self.client_id, e, exc_info=True)
            summary["errors"].append(f"dedup: {e}")

        log.info(
            "[%s] Reconcile #%d complete | orders=%d corrected=%d alerted=%d "
            "positions=%d pos_corrected=%d pos_imported=%d pos_alerts=%d errors=%d",
            self.client_id,
            self._run_count,
            summary["orders_checked"],
            summary["orders_corrected"],
            summary["orders_alerted"],
            summary["positions_checked"],
            summary["positions_corrected"],
            summary["positions_imported"],
            summary["positions_alerted"],
            len(summary["errors"]),
        )
        return summary

    # ──────────────────────────────────────────────────────────────────────────
    # Order reconciliation
    # ──────────────────────────────────────────────────────────────────────────

    def _reconcile_orders(self, summary: dict):
        from ap.db import get_open_orders_for_reconcile, run_with_retry

        open_orders = run_with_retry(
            lambda: get_open_orders_for_reconcile(client_id=self.client_id)
        ) or []
        summary["orders_checked"] = len(open_orders)

        for order in open_orders:
            local_id   = order.get("local_order_id") or order.get("id")
            broker_oid = order.get("broker_order_id")
            db_status  = (order.get("status") or "").upper()
            contract   = order.get("contract") or order.get("symbol") or "?"
            kind       = (order.get("kind") or "ENTRY").upper()

            if not broker_oid or broker_oid in ("N/A", "PENDING", ""):
                self._handle_order_without_broker_id(order, summary)
                continue

            try:
                broker_raw = self.broker.get_order(broker_oid)
            except Exception as e:
                log.warning("[%s] Broker get_order failed for %s: %s",
                            self.client_id, broker_oid, e)
                continue

            broker_status = str(
                broker_raw.get("status") or broker_raw.get("Status") or ""
            ).lower().strip()

            if broker_status in BROKER_FILLED and db_status in DB_OPEN_STATUSES:
                self._advance_order_to_broker_fill(order, broker_raw, broker_status, summary)
            elif broker_status in BROKER_TERMINAL and db_status in DB_OPEN_STATUSES:
                self._advance_order_to_terminal(order, broker_status, summary)
            elif broker_status in ("open", "pending", "partially_filled"):
                pass
            elif broker_status not in ("", "unknown", "error"):
                log.debug(
                    "[%s] Unrecognized broker status '%s' for order %s",
                    self.client_id, broker_status, local_id
                )

        self._check_ghost_fills(summary)

    def _handle_order_without_broker_id(self, order: dict, summary: dict) -> None:
        local_id   = order.get("local_order_id") or order.get("id")
        contract   = order.get("contract") or order.get("symbol") or "?"
        kind       = (order.get("kind") or "ENTRY").upper()
        created_ts = order.get("created_ts")

        # Deferred overnight/watcher entries may intentionally not have broker ids yet.
        _contract_raw = order.get("contract")
        _is_no_contract = not _contract_raw or str(_contract_raw).strip() in ("", "None", "null")
        if kind == "ENTRY" and _is_no_contract and created_ts:
            try:
                _ts = datetime.fromisoformat(str(created_ts).replace("Z", "+00:00"))
                if _ts.hour >= 20 or _ts.hour < 9:
                    log.debug("[%s] skip phantom cancel — overnight entry watcher owns | %s",
                              self.client_id, local_id)
                    return
            except Exception:
                pass

        if created_ts:
            try:
                created = datetime.fromisoformat(str(created_ts).replace("Z", "+00:00"))
                age = (datetime.now(timezone.utc) - created).total_seconds()
                if age > 300:
                    try:
                        self.osm.transition(
                            local_id,
                            "CANCELED",
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

    def _advance_order_to_broker_fill(
        self,
        order: dict,
        broker_raw: dict,
        broker_status: str,
        summary: dict,
    ) -> None:
        local_id  = order.get("local_order_id") or order.get("id")
        contract  = order.get("contract") or order.get("symbol") or "?"
        db_status = (order.get("status") or "").upper()
        kind      = (order.get("kind") or "ENTRY").upper()

        new_status = "FILLED" if kind == "ENTRY" else "EXIT_FILLED"
        if broker_status == "partially_filled":
            new_status = "PARTIAL_FILL" if kind == "ENTRY" else "EXIT_PARTIAL_FILL"

        filled_qty = int(
            broker_raw.get("exec_quantity")
            or broker_raw.get("filled_quantity")
            or broker_raw.get("quantity")
            or 0
        )
        avg_fill = float(
            broker_raw.get("avg_fill_price")
            or broker_raw.get("fill_price")
            or broker_raw.get("price")
            or 0.0
        )

        log.warning(
            "[%s] RECONCILE_CORRECT: %s | %s | DB=%s broker=%s → advancing to %s",
            self.client_id, contract, local_id, db_status, broker_status, new_status
        )

        try:
            ok = self.osm.transition(
                local_id,
                new_status,
                filled_qty=filled_qty,
                fill_price=avg_fill,
            )
            if not ok:
                log.error("[%s] OSM transition failed for %s → %s",
                          self.client_id, local_id, new_status)
                return

            summary["orders_corrected"] += 1
            self._alert(
                f"RECONCILE_AUTO_CORRECT | {contract} | {local_id} | "
                f"DB was {db_status}, broker={broker_status} → advanced to {new_status} "
                f"(qty={filled_qty} avg={avg_fill:.2f})"
            )

            if kind == "ENTRY" and new_status == "FILLED" and self.pm:
                self._ensure_position_for_filled_entry(order, filled_qty, avg_fill, summary)
        except Exception as e:
            log.error("[%s] Reconcile transition error: %s", self.client_id, e)

    def _advance_order_to_terminal(self, order: dict, broker_status: str, summary: dict) -> None:
        local_id  = order.get("local_order_id") or order.get("id")
        contract  = order.get("contract") or order.get("symbol") or "?"
        db_status = (order.get("status") or "").upper()
        kind      = (order.get("kind") or "ENTRY").upper()
        new_status = BROKER_TO_OSM.get(broker_status, "CANCELED")

        log.warning(
            "[%s] RECONCILE_CORRECT: %s | %s | DB=%s broker=%s → advancing to %s",
            self.client_id, contract, local_id, db_status, broker_status, new_status
        )

        try:
            ok = self.osm.transition(
                local_id,
                new_status,
                last_error=f"reconciler: broker_status={broker_status}",
            )
            if not ok:
                log.error("[%s] OSM transition failed %s → %s",
                          self.client_id, local_id, new_status)
                return

            summary["orders_corrected"] += 1
            if kind == "EXIT":
                position_id = order.get("position_id")
                if position_id:
                    self._revert_position_to_open(position_id, contract)
            self._alert(
                f"RECONCILE_AUTO_CORRECT | {contract} | {local_id} | "
                f"DB was {db_status}, broker={broker_status} → {new_status}"
            )
        except Exception as e:
            log.error("[%s] Reconcile terminal transition error: %s", self.client_id, e)

    def _ensure_position_for_filled_entry(
        self,
        order: dict,
        filled_qty: int,
        avg_fill: float,
        summary: dict,
    ) -> Optional[str]:
        """Idempotently ensure a DB + exit-engine position exists after broker-confirmed entry fill."""
        if filled_qty <= 0 or avg_fill <= 0:
            log.error(
                "[%s] Cannot ensure filled-entry position for %s: qty=%s avg_fill=%s",
                self.client_id,
                order.get("local_order_id"),
                filled_qty,
                avg_fill,
            )
            return None

        contract = self._norm_contract(order.get("contract") or order.get("symbol") or "")
        existing = self._find_db_position_by_contract(contract)
        if existing:
            pos_id = str(existing.get("id") or existing.get("position_id") or "")
            self._seed_exit_engine_from_position(existing)
            return pos_id

        try:
            plan_id = order.get("plan_id") or order.get("signal_id") or order.get("local_order_id")
            signal_id_val = order.get("signal_id") or order.get("local_order_id")
            pos_id = self.pm.open_position(
                plan_id=plan_id,
                signal_id=signal_id_val,
                ticker=(order.get("symbol") or "").upper(),
                contract=order.get("contract") or order.get("symbol") or "",
                side=(order.get("direction") or "CALL").upper(),
                qty=filled_qty,
                entry_price=avg_fill,
                tier=str(order.get("tier") or "B"),
                score=float(order.get("score") or 0),
                pattern=str(order.get("pattern") or ""),
                stop_underlying=float(order.get("stop_underlying"))
                if order.get("stop_underlying")
                else None,
                target_underlying=float(order.get("target_underlying"))
                if order.get("target_underlying")
                else None,
            )
            summary["positions_imported"] += 1
            self._alert(
                f"POSITION_CREATED_FROM_RECONCILED_FILL | {contract} | pos={pos_id} "
                f"qty={filled_qty} avg={avg_fill:.2f}"
            )
            row = self._find_db_position_by_id(pos_id) or self._find_db_position_by_contract(contract)
            if row:
                self._seed_exit_engine_from_position(row)
            return str(pos_id) if pos_id else None
        except Exception as _pm_err:
            log.error("[%s] RECONCILE pm.open_position failed %s: %s",
                      self.client_id, order.get("local_order_id"), _pm_err)
            summary["positions_alerted"] += 1
            return None

    def _check_ghost_fills(self, summary: dict):
        """Check orders DB says FILLED where broker shows terminal. Alert only."""
        from ap.db import run_with_retry, conn

        def _get_recent_fills():
            with conn() as c:
                c.execute(
                    """
                    SELECT local_order_id, broker_order_id, contract, kind,
                           filled_qty, fill_price, updated_ts
                    FROM orders
                    WHERE client_id=%s
                      AND status IN ('FILLED','EXIT_FILLED')
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
            fills = run_with_retry(_get_recent_fills) or []
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

    def _norm_contract(self, val: str) -> str:
        return str(val or "").strip().upper()

    def _norm_underlying(self, val: str) -> str:
        return str(val or "").strip().upper()

    def _safe_get_broker_positions(self) -> list[dict]:
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

    def _broker_position_contract(self, bp: dict) -> str:
        return self._norm_contract(
            bp.get("symbol")
            or bp.get("option_symbol")
            or bp.get("contract")
            or bp.get("instrument")
            or ""
        )

    def _broker_position_underlying(self, bp: dict) -> str:
        c_sym = self._broker_position_contract(bp)
        return self._norm_underlying(
            bp.get("underlying")
            or bp.get("underlying_symbol")
            or bp.get("root_symbol")
            or bp.get("ticker")
            or c_sym[:6]
        )

    def _broker_position_qty(self, bp: dict) -> int:
        raw = (
            bp.get("quantity")
            or bp.get("qty")
            or bp.get("long_quantity")
            or bp.get("short_quantity")
            or 0
        )
        try:
            return abs(int(float(raw)))
        except Exception:
            return 0

    def _broker_position_entry_price(self, bp: dict) -> float:
        """
        Conservative entry-price extraction. Tradier-like responses often include
        cost_basis. If avg price is unavailable, derive from cost_basis / qty / 100.
        """
        for key in (
            "avg_fill",
            "avg_price",
            "average_price",
            "average_cost",
            "cost_per_share",
            "price",
            "last_price",
        ):
            try:
                val = bp.get(key)
                if val is not None and float(val) > 0:
                    return float(val)
            except Exception:
                pass

        try:
            qty = self._broker_position_qty(bp)
            cost_basis = float(bp.get("cost_basis") or bp.get("costbasis") or 0)
            if qty > 0 and cost_basis > 0:
                return abs(cost_basis) / qty / 100.0
        except Exception:
            pass

        # Last resort: do not invent a valid price silently.
        return 0.0

    def _broker_position_side(self, bp: dict) -> str:
        side = str(bp.get("direction") or bp.get("side") or "").upper()
        contract = self._broker_position_contract(bp)
        if side in {"CALL", "PUT"}:
            return side
        if "P" in contract[-16:]:
            # Option symbology may contain C/P before strike.
            # Better exact parser is not guaranteed here; fallback keeps DB usable.
            return "PUT"
        return "CALL"

    def _get_open_db_positions(self) -> list[dict]:
        """Fetch DB OPEN/CLOSING positions with a direct SQL fallback."""
        try:
            from ap.db import run_with_retry, conn

            def _fetch():
                with conn() as c:
                    c.execute(
                        """
                        SELECT *
                        FROM positions
                        WHERE client_id=%s
                          AND status IN ('OPEN','CLOSING')
                        ORDER BY entry_ts DESC NULLS LAST
                        """,
                        (self.client_id,),
                    )
                    return c.fetchall()
            rows = run_with_retry(_fetch) or []
            return [dict(r) for r in rows]
        except Exception as e:
            log.error("[%s] Failed to fetch DB open/closing positions: %s", self.client_id, e)
            return []

    def _get_recent_exit_fill(self, contract: str, underlying: str) -> Optional[dict]:
        """Look up the most recent filled EXIT order for this contract."""
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
                          AND  (contract = %s OR symbol = %s OR symbol = %s)
                        ORDER  BY updated_ts DESC
                        LIMIT  1
                        """,
                        (self.client_id, contract, contract, underlying),
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
        First call → False (wait one pass).
        Second consecutive call → True (confirmed gone).
        """
        if contract in self._ghost_tracker:
            del self._ghost_tracker[contract]
            return True
        self._ghost_tracker[contract] = True
        return False

    def _reconcile_positions(self, summary: dict):
        """
        Evidence-based position reconciliation plus broker-open import.

        Policy:
          1. Match DB positions by exact contract symbol first.
          2. Auto-close DB positions only with filled exit evidence or two-pass ghost confirm.
          3. Import broker-open positions missing from DB so restarts cannot orphan trades.
        """
        summary.setdefault("positions_corrected", 0)
        summary.setdefault("positions_imported", 0)

        broker_positions = self._safe_get_broker_positions()
        broker_by_contract: dict[str, dict] = {}
        broker_by_underlying: dict[str, list[dict]] = {}

        for bp in broker_positions:
            c_sym = self._broker_position_contract(bp)
            u_sym = self._broker_position_underlying(bp)
            qty = self._broker_position_qty(bp)
            if qty <= 0:
                continue
            if c_sym:
                broker_by_contract[c_sym] = bp
            if u_sym:
                broker_by_underlying.setdefault(u_sym, []).append(bp)

        db_positions = self._get_open_db_positions()
        summary["positions_checked"] = len(db_positions)

        db_contracts: set[str] = set()
        db_underlyings: set[str] = set()

        # DB → broker checks.
        for pos in db_positions:
            pos_id     = pos.get("id") or pos.get("position_id")
            contract   = self._norm_contract(pos.get("contract") or pos.get("symbol") or "")
            underlying = self._norm_underlying(pos.get("underlying") or pos.get("ticker") or contract[:6])
            db_qty     = int(pos.get("qty") or pos.get("quantity") or 0)
            entry_px   = float(pos.get("avg_fill") or pos.get("entry_price") or 0.0)

            if contract:
                db_contracts.add(contract)
            if underlying:
                db_underlyings.add(underlying)

            broker_pos = broker_by_contract.get(contract)

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
                    self._ghost_tracker.pop(contract, None)
                    summary["positions_alerted"] += 1
                    continue

            if broker_pos is not None:
                self._ghost_tracker.pop(contract, None)
                broker_qty = self._broker_position_qty(broker_pos)
                if broker_qty != db_qty and db_qty > 0:
                    log.warning(
                        "[%s] POSITION_QTY_MISMATCH | %s | DB=%d broker=%d",
                        self.client_id, contract, db_qty, broker_qty
                    )
                    summary["positions_alerted"] += 1

                # Belt-and-suspenders: make sure exit engine is tracking this DB-open position.
                self._seed_exit_engine_from_position(pos)
                continue

            self._handle_db_position_missing_at_broker(
                pos=pos,
                contract=contract,
                underlying=underlying,
                db_qty=db_qty,
                entry_px=entry_px,
                summary=summary,
            )

        # Broker → DB import checks. This is the key orphan-prevention layer.
        self._import_broker_positions_missing_from_db(
            broker_positions=broker_positions,
            db_contracts=db_contracts,
            db_underlyings=db_underlyings,
            summary=summary,
        )

    def _handle_db_position_missing_at_broker(
        self,
        *,
        pos: dict,
        contract: str,
        underlying: str,
        db_qty: int,
        entry_px: float,
        summary: dict,
    ) -> None:
        pos_id = pos.get("id") or pos.get("position_id")
        exit_fill = self._get_recent_exit_fill(contract, underlying)

        if exit_fill and float(exit_fill.get("fill_price") or 0) > 0:
            exit_px = float(exit_fill["fill_price"])
            close_confidence = "HIGH"
            log.info(
                "[%s] RECONCILE_CLOSE_EVIDENCE | %s | filled exit found @ $%.4f",
                self.client_id, contract, exit_px
            )
        else:
            if not self._mark_ghost_seen(contract):
                log.warning(
                    "[%s] GHOST_PASS_1 | %s | broker has no position — waiting one pass",
                    self.client_id, contract
                )
                summary["positions_alerted"] += 1
                return
            exit_px = entry_px
            close_confidence = "MEDIUM"
            log.warning(
                "[%s] GHOST_PASS_2 | %s | confirmed gone from broker — auto-closing with entry price",
                self.client_id, contract
            )

        pnl_dollars = round((exit_px - entry_px) * db_qty * 100, 2)
        pnl_pct = round(((exit_px - entry_px) / entry_px) * 100, 2) if entry_px > 0 else 0.0

        try:
            from ap.db import conn, run_with_retry as _rwr

            _now = datetime.now(timezone.utc).isoformat()

            def _close():
                with conn() as c:
                    c.execute(
                        """
                        UPDATE positions
                        SET    status            = 'CLOSED',
                               exit_ts           = %s,
                               exit_price        = %s,
                               realized_pnl      = %s,
                               realized_pnl_pct  = %s,
                               close_source      = %s,
                               close_confidence  = %s
                        WHERE  id = %s AND client_id = %s
                        """,
                        (
                            _now,
                            exit_px,
                            pnl_dollars,
                            pnl_pct,
                            "RECONCILER_AUTO_CLOSE",
                            close_confidence,
                            pos_id,
                            self.client_id,
                        ),
                    )

            _rwr(_close)
        except Exception as e:
            log.error("[%s] RECONCILE close DB write failed %s: %s",
                      self.client_id, pos_id, e)
            summary["positions_alerted"] += 1
            return

        _ee = getattr(self, "exit_engine", None)
        if _ee:
            try:
                _ee.mark_position_closed(str(pos_id), reason="reconciler_auto_close")
            except Exception:
                pass

        self._ghost_tracker.pop(contract, None)
        log.info(
            "[%s] FINALIZED TRADE | %s | pos=%s | entry=%.4f exit=%.4f "
            "pnl=$%.2f (%.1f%%) source=RECONCILER confidence=%s",
            self.client_id, contract, pos_id, entry_px, exit_px,
            pnl_dollars, pnl_pct, close_confidence
        )
        summary["positions_corrected"] += 1
        try:
            from ap_proof_logger import funnel as _funnel_r
            _funnel_r.inc("reconciler_corrections")
        except Exception:
            pass

    def _import_broker_positions_missing_from_db(
        self,
        *,
        broker_positions: list[dict],
        db_contracts: set[str],
        db_underlyings: set[str],
        summary: dict,
    ) -> None:
        if not IMPORT_MISSING_BROKER_POSITIONS:
            return

        for bp in broker_positions:
            contract = self._broker_position_contract(bp)
            if not contract:
                continue

            qty = self._broker_position_qty(bp)
            if qty <= 0:
                continue

            if contract in db_contracts:
                continue

            underlying = self._broker_position_underlying(bp)
            entry_px = self._broker_position_entry_price(bp)

            # If entry price is unknown, still import at a tiny safe placeholder so the
            # exit engine can track/alert. The operator should review immediately.
            if entry_px <= 0:
                entry_px = 0.01
                self._alert(
                    f"BROKER_POSITION_IMPORT_PRICE_UNKNOWN | {contract} | "
                    "using placeholder entry=0.01 — MANUAL REVIEW REQUIRED"
                )

            side = self._broker_position_side(bp)

            # Double-check DB directly in case another thread imported it.
            existing = self._find_db_position_by_contract(contract)
            if existing:
                self._seed_exit_engine_from_position(existing)
                continue

            pos_id = self._create_imported_position(
                contract=contract,
                underlying=underlying,
                side=side,
                qty=qty,
                entry_px=entry_px,
                broker_position=bp,
            )

            if not pos_id:
                summary["positions_alerted"] += 1
                continue

            summary["positions_imported"] += 1
            summary["positions_corrected"] += 1
            db_contracts.add(contract)
            if underlying:
                db_underlyings.add(underlying)

            row = self._find_db_position_by_id(pos_id) or self._find_db_position_by_contract(contract)
            if row:
                self._seed_exit_engine_from_position(row)
            else:
                self._seed_exit_engine_from_import(
                    pos_id=pos_id,
                    contract=contract,
                    underlying=underlying,
                    side=side,
                    qty=qty,
                    entry_px=entry_px,
                )

            msg = (
                f"BROKER_POSITION_IMPORTED | {underlying or '?'} {side} | "
                f"{contract} | qty={qty} entry={entry_px:.4f} pos={pos_id}"
            )
            log.critical("[%s] %s", self.client_id, msg)
            self._alert(msg)
            try:
                from ap_proof_logger import funnel as _funnel_r
                _funnel_r.inc("reconciler_imported_positions")
            except Exception:
                pass

    def _create_imported_position(
        self,
        *,
        contract: str,
        underlying: str,
        side: str,
        qty: int,
        entry_px: float,
        broker_position: dict,
    ) -> Optional[str]:
        """
        Create an OPEN DB row for a broker-open position missing from DB.
        Prefer APPositionManager, fall back to direct SQL with broad/minimal compatibility.
        """
        imported_plan_id = f"reconciled:{contract}:{int(time.time())}"
        imported_signal_id = f"reconciled:{contract}:{uuid.uuid4().hex[:8]}"

        if self.pm is not None:
            try:
                pos_id = self.pm.open_position(
                    plan_id=imported_plan_id,
                    signal_id=imported_signal_id,
                    ticker=underlying or contract[:6],
                    contract=contract,
                    side=side,
                    qty=qty,
                    entry_price=entry_px,
                    tier="RECONCILED",
                    score=0.0,
                    pattern="BROKER_IMPORT",
                    stop_underlying=None,
                    target_underlying=None,
                )
                return str(pos_id) if pos_id else None
            except Exception as pm_err:
                log.error(
                    "[%s] pm.open_position import failed for %s: %s — trying SQL fallback",
                    self.client_id, contract, pm_err
                )

        try:
            from ap.db import conn, run_with_retry

            pos_id = str(uuid.uuid4())
            now_iso = datetime.now(timezone.utc).isoformat()

            def _insert_full():
                with conn() as c:
                    c.execute(
                        """
                        INSERT INTO positions (
                            id, client_id, underlying, contract, direction, qty, avg_fill,
                            entry_ts, status, plan_id, signal_id, close_source,
                            close_confidence
                        ) VALUES (
                            %s,%s,%s,%s,%s,%s,%s,
                            %s,'OPEN',%s,%s,%s,%s
                        )
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (
                            pos_id,
                            self.client_id,
                            underlying or contract[:6],
                            contract,
                            side,
                            int(qty),
                            float(entry_px),
                            now_iso,
                            imported_plan_id,
                            imported_signal_id,
                            "RECONCILER_IMPORT",
                            "BROKER_OPEN",
                        ),
                    )

            try:
                run_with_retry(_insert_full)
                return pos_id
            except Exception as full_err:
                log.warning(
                    "[%s] full imported position insert failed for %s: %s — trying minimal schema",
                    self.client_id, contract, full_err
                )

            def _insert_minimal():
                with conn() as c:
                    c.execute(
                        """
                        INSERT INTO positions (
                            id, client_id, underlying, contract, direction, qty, avg_fill,
                            entry_ts, status
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'OPEN')
                        ON CONFLICT (id) DO NOTHING
                        """,
                        (
                            pos_id,
                            self.client_id,
                            underlying or contract[:6],
                            contract,
                            side,
                            int(qty),
                            float(entry_px),
                            now_iso,
                        ),
                    )

            run_with_retry(_insert_minimal)
            return pos_id
        except Exception as sql_err:
            log.error("[%s] SQL import failed for broker position %s: %s",
                      self.client_id, contract, sql_err)
            return None

    def _find_db_position_by_contract(self, contract: str) -> Optional[dict]:
        contract = self._norm_contract(contract)
        if not contract:
            return None
        try:
            from ap.db import conn, run_with_retry

            def _fetch():
                with conn() as c:
                    c.execute(
                        """
                        SELECT *
                        FROM positions
                        WHERE client_id=%s
                          AND status IN ('OPEN','CLOSING')
                          AND UPPER(contract)=%s
                        ORDER BY entry_ts DESC NULLS LAST
                        LIMIT 1
                        """,
                        (self.client_id, contract),
                    )
                    row = c.fetchone()
                    return dict(row) if row else None

            return run_with_retry(_fetch)
        except Exception as e:
            log.debug("[%s] find DB position by contract failed for %s: %s",
                      self.client_id, contract, e)
            return None

    def _find_db_position_by_id(self, pos_id: str) -> Optional[dict]:
        if not pos_id:
            return None
        try:
            from ap.db import conn, run_with_retry

            def _fetch():
                with conn() as c:
                    c.execute(
                        "SELECT * FROM positions WHERE client_id=%s AND id=%s LIMIT 1",
                        (self.client_id, pos_id),
                    )
                    row = c.fetchone()
                    return dict(row) if row else None

            return run_with_retry(_fetch)
        except Exception:
            return None

    def _seed_exit_engine_from_position(self, pos: dict) -> None:
        if not pos:
            return

        pos_id = str(pos.get("id") or pos.get("position_id") or "")
        contract = self._norm_contract(pos.get("contract") or pos.get("symbol") or "")
        underlying = self._norm_underlying(pos.get("underlying") or pos.get("ticker") or contract[:6])
        side = str(pos.get("direction") or pos.get("side") or "CALL").upper()
        qty = int(pos.get("qty") or pos.get("quantity") or pos.get("quantity_remaining") or 0)
        entry_px = float(pos.get("avg_fill") or pos.get("entry_price") or 0.0)

        if qty <= 0 or entry_px <= 0 or not contract:
            return

        self._seed_exit_engine_from_import(
            pos_id=pos_id,
            contract=contract,
            underlying=underlying,
            side=side,
            qty=qty,
            entry_px=entry_px,
            stop_underlying=pos.get("stop_underlying") or pos.get("underlying_stop") or 0.0,
            target_underlying=pos.get("target_underlying") or pos.get("underlying_target") or 0.0,
        )

    def _seed_exit_engine_from_import(
        self,
        *,
        pos_id: str,
        contract: str,
        underlying: str,
        side: str,
        qty: int,
        entry_px: float,
        stop_underlying=0.0,
        target_underlying=0.0,
    ) -> None:
        _ee = getattr(self, "exit_engine", None)
        if not _ee:
            log.critical(
                "[%s] Cannot seed exit engine for imported/open position %s — exit_engine not wired",
                self.client_id, contract
            )
            return

        try:
            from ap_exit_engine import ManagedPosition

            mp = ManagedPosition(
                ticker=underlying or contract[:6],
                option_symbol=contract,
                side=side,
                quantity=int(qty),
                entry_price=float(entry_px),
                underlying_entry=0.0,
                underlying_target=float(target_underlying or 0.0),
                underlying_stop=float(stop_underlying or 0.0),
            )
            mp.position_id = str(pos_id or "")
            mp.client_id = self.client_id
            mp.signal_id = f"reconciled:{contract}"
            mp.current_option_price = float(entry_px)
            _ee.add_position(mp)
            log.critical(
                "[%s] EXIT_ENGINE_SEEDED_FROM_RECONCILER | %s | qty=%s entry=%.4f pos=%s",
                self.client_id, contract, qty, entry_px, pos_id or "n/a"
            )
        except Exception as e:
            log.error("[%s] Failed to seed exit engine for %s: %s",
                      self.client_id, contract, e)

    def _get_broker_positions(self) -> Optional[list]:
        """Compatibility wrapper."""
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
            dupes = run_with_retry(_get_dupes) or []
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
