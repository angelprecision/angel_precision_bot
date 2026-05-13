"""
ap_reconciler.py — Broker-vs-DB Truth Reconciliation
=====================================================
Runs every RECONCILE_INTERVAL_SEC (default 15s) per client during live market protection.

Truth policy:
  - Broker wins on order status.
  - DB wins on position metadata when a position exists.
  - Broker-open option positions must NEVER be silently ignored if DB is missing them.

Critical recovery invariant:
  If the broker has an open option position and the DB has no matching OPEN/CLOSING
  position, the reconciler imports a conservative OPEN position into DB and seeds
  the exit engine. This prevents restart/orphan failures. Imported/reseeded
  positions must carry the best available underlying_entry and price-trust flags
  so exit logic does not silently operate on fake state.

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
       → evidence-based close only after filled exit evidence or three-pass ghost confirm
    F. DB qty vs broker qty mismatch
       → alert
    G. Broker OPEN → DB missing
       → import DB position + seed exit engine + critical alert

  Dedup:
    H. DB has duplicate OPEN positions for same underlying/direction
       → alert immediately

Idempotency guard (built-in, no external patch needed):
  run_once() is protected by a per-instance RLock and a minimum-interval gate
  (RECONCILER_RUN_ONCE_MIN_INTERVAL_SEC, default 3s). Concurrent or rapid-fire
  callers receive the last summary without re-executing broker/DB/OSM logic.

Bug-fix history (see inline comments tagged FIX-N):
  FIX-1  _broker_position_side: replaced "P in contract[-16:]" heuristic with a
         proper OCC-format regex so single-letter-P tickers (e.g. Prudential "P")
         are never misidentified as PUTs.
  FIX-2  _advance_order_to_terminal: use _order_family_from_kind_and_status instead
         of (order.get("kind") or "ENTRY") so NULL-kind EXIT orders still revert
         their position to OPEN rather than being silently treated as ENTRY cancels.
  FIX-3  _create_imported_position / _backfill_position_underlying_entry: underlying_entry
         is now persisted to the DB row (both PM and SQL paths). Previously it was only
         seeded into the in-process exit engine and lost on restart.
  FIX-4  _check_ghost_fills: reduced window from 2 hours to 30 minutes and capped
         broker get_order calls at 10 per pass to prevent API flooding.
  FIX-5  run_once skipped-summary shape: the short-circuit dict now includes all
         standard summary keys so callers cannot KeyError on orders_checked etc.
  FIX-6  _safe_get_broker_open_orders: added error-response guard so a broker dict
         like {"error": "rate_limited"} is never wrapped and processed as a fake order.
  FIX-7  _import_broker_positions_missing_from_db / _reconcile_positions: removed
         db_underlyings — the set was built and passed but never used for filtering,
         creating dead state that could mislead future readers.
  FIX-8  run_once: added pass timing (elapsed_sec in summary + log line).

Usage:
    reconciler = APBrokerReconciler(broker=broker, client_id=email, osm=osm, pm=pm)
    reconciler.exit_engine = exit_engine
    reconciler.start()
    # or call reconciler.run_once() directly in tests
"""

from __future__ import annotations

import logging
import os
import re
import threading
import time
import uuid
from datetime import datetime, timezone
from typing import Optional

log = logging.getLogger("ap.reconciler")

# ── Optional observability hooks ─────────────────────────────────────────────
# These imports are deliberately defensive so the reconciler can still run in
# isolation/tests before ap_lifecycle.py or ap_health_registry.py are deployed.
try:
    from ap_lifecycle import (
        signal_recovered_position,
        signal_position_reseeded,
        signal_rejected,
        RejectionCategory,
        RejectionSeverity,
        LifecycleOwner,
    )
except Exception:  # pragma: no cover - runtime safety for partial deployments
    signal_recovered_position = None
    signal_position_reseeded = None
    signal_rejected = None
    RejectionCategory = None
    RejectionSeverity = None
    LifecycleOwner = None

try:
    from ap_health_registry import HEALTH, Criticality
except Exception:  # pragma: no cover - runtime safety for partial deployments
    HEALTH = None
    Criticality = None
# ─────────────────────────────────────────────────────────────────────────────

RECONCILE_INTERVAL_SEC = int(os.getenv("RECONCILE_INTERVAL_SEC", "15"))

# Minimum seconds between two effective run_once() executions on the same instance.
# Rapid-fire callers (watchdogs, fill monitors, the reconciler loop itself) receive
# the cached last summary and skip the full broker/DB/OSM pass.
RUN_ONCE_MIN_INTERVAL_SEC = float(os.getenv("RECONCILER_RUN_ONCE_MIN_INTERVAL_SEC", "3.0"))

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

# Canonical set of position statuses treated as live — used in SQL queries throughout.
DB_OPEN_POSITION_STATUSES = ("OPEN", "CLOSING")

BROKER_FILLED   = frozenset({"filled", "partially_filled"})
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

# OCC option symbology: root(variable) + YYMMDD(6) + C|P(1) + 8-digit strike.
# Anchored to end-of-string so it cannot match a P inside the root ticker.
_OCC_CP_RE = re.compile(r'([CP])\d{8}$')


def _market_hours_interval(configured_interval: int = RECONCILE_INTERVAL_SEC) -> int:
    """
    15s market-hours reconciler safety net. Dedicated fill monitor should still
    forward incremental broker fills to OSM in under 10 seconds.
    """
    try:
        from zoneinfo import ZoneInfo
        from datetime import datetime as _dt, time as _time
        et = _dt.now(ZoneInfo("America/New_York"))
        if _time(9, 25) <= et.time() <= _time(16, 5):
            return max(5, min(int(configured_interval or 15), 15))
    except Exception:
        pass
    return max(15, int(configured_interval or 60))


def _empty_summary(client_id: str, run: int = 0) -> dict:
    """
    Return a zeroed summary dict with all standard keys present.
    Used for both real runs and skip-path returns so callers never KeyError.
    """
    return {
        "run":                 run,
        "client_id":           client_id,
        "orders_checked":      0,
        "orders_corrected":    0,
        "orders_alerted":      0,
        "positions_checked":   0,
        "positions_alerted":   0,
        "positions_corrected": 0,
        "positions_imported":  0,
        "elapsed_sec":         0.0,
        "errors":              [],
        "skipped":             False,
    }


class APBrokerReconciler:
    """
    Continuously reconciles broker truth against DB state.
    One instance per client — shares broker + OSM from ClientRunner.

    Idempotency guarantee
    ---------------------
    run_once() is serialized by a per-instance RLock.  If a second caller arrives
    within RUN_ONCE_MIN_INTERVAL_SEC of the previous execution completing, it
    receives the cached summary dict immediately without re-entering broker/DB/OSM
    logic.  This removes the need for any external patch or decorator.
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
        self._ghost_tracker: dict[str, int] = {}  # ghost detection count per contract
        self._ghost_fill_confirmed: set[str] = set()  # broker_oids already confirmed terminal — skip re-check
        self._missing_id_exit_tracker: dict[str, int] = {}  # missing broker-id EXIT recovery pass count
        self.fill_monitor = None      # optional fill_monitor_final_hardened-7.py instance
        self.require_fill_monitor = (
            os.getenv("RECONCILER_REQUIRE_FILL_MONITOR", "0").strip().lower()
            in {"1", "true", "yes", "on"}
        )

        # ── Idempotency state (native, no external patch required) ────────────
        self._run_once_lock: threading.RLock = threading.RLock()
        self._last_run_once_ts: float = 0.0
        self._last_run_once_summary: Optional[dict] = None

        # ── Observability identity ───────────────────────────────────────────
        self._health_name = f"ap_reconciler:{self.client_id}"
        self._register_health()
        # ─────────────────────────────────────────────────────────────────────

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
        self._verify_fill_monitor_or_alert()
        self._heartbeat("started", thread_alive=True)
        log.info(
            "[%s] Reconciler started (interval=%ds market_hours_effective<=15s "
            "import_missing_broker_positions=%s run_once_min_interval=%.1fs)",
            self.client_id,
            self._interval,
            IMPORT_MISSING_BROKER_POSITIONS,
            RUN_ONCE_MIN_INTERVAL_SEC,
        )

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=10)
        self._heartbeat("stopped", thread_alive=False)

    def is_alive(self) -> bool:
        return bool(self._thread and self._thread.is_alive())

    # ──────────────────────────────────────────────────────────────────────────
    # Main loop
    # ──────────────────────────────────────────────────────────────────────────

    def _loop(self):
        # Stagger startup to avoid hammering broker on restart.
        time.sleep(min(30, max(1, self._interval // 4)))
        while not self._stop.wait(_market_hours_interval(self._interval)):
            try:
                self.run_once()
            except Exception as e:
                log.error("[%s] Reconciler loop error: %s", self.client_id, e, exc_info=True)
                self._report_health_error(f"loop_error: {e}", fatal=False)

    def run_once(self) -> dict:
        """
        Run one full reconciliation pass. Safe to call directly from tests.

        Idempotency guarantee
        ---------------------
        Serialized by a per-instance RLock.  If a concurrent or rapid-fire caller
        arrives within RUN_ONCE_MIN_INTERVAL_SEC of the previous run completing, it
        receives a standardized cached summary immediately (with skipped=True and all
        standard keys present) without re-running broker/DB/OSM logic.  This
        eliminates duplicate correction pressure from the reconciler loop, fill
        monitors, and any external watchdog that all call run_once().

        FIX-5: skipped-path dict now contains all standard summary keys so callers
        cannot KeyError on orders_checked, positions_checked, etc.
        FIX-8: elapsed_sec is recorded and included in both the summary and log line.
        """
        with self._run_once_lock:
            now     = time.monotonic()
            elapsed = now - self._last_run_once_ts

            if self._last_run_once_ts and elapsed < RUN_ONCE_MIN_INTERVAL_SEC:
                # Return a properly-shaped skipped summary so callers never KeyError.
                # If a prior real summary exists, copy it and mark skipped=True.
                # FIX-5: guarantees all standard keys are always present.
                if self._last_run_once_summary is not None:
                    cached = dict(self._last_run_once_summary)
                    cached["skipped"]     = True
                    cached["skip_reason"] = "run_once_called_too_soon"
                else:
                    cached = _empty_summary(self.client_id, self._run_count)
                    cached["skipped"]         = True
                    cached["skip_reason"]     = "run_once_called_too_soon"
                    cached["min_interval_sec"] = RUN_ONCE_MIN_INTERVAL_SEC
                log.debug(
                    "[%s] Reconciler run_once skipped: %.2fs since last run (min=%.1fs)",
                    self.client_id,
                    elapsed,
                    RUN_ONCE_MIN_INTERVAL_SEC,
                )
                return cached

            # Stamp *before* executing so a long run does not create an artificially
            # short cool-down if another caller checks mid-execution.
            self._last_run_once_ts = now

            self._run_count += 1
            summary = _empty_summary(self.client_id, self._run_count)

            _pass_start = time.monotonic()

            try:
                self._reconcile_orders(summary)
            except Exception as e:
                log.error("[%s] Order reconcile error: %s", self.client_id, e, exc_info=True)
                summary["errors"].append(f"orders: {e}")
                self._report_health_error(f"orders_reconcile_error: {e}", fatal=False)

            # Backup fill-truth heal: must run after orders, before positions.
            # Catches any EXIT_FILLED order whose linked position wasn't finalized
            # (manual exits, restart gaps, fill monitor delays, OSM misses).
            try:
                self._heal_exit_filled_positions_from_orders(summary)
            except Exception as e:
                log.error("[%s] Exit-filled position heal error: %s", self.client_id, e, exc_info=True)
                summary["errors"].append(f"exit_fill_heal: {e}")

            try:
                self._reconcile_positions(summary)
            except Exception as e:
                log.error("[%s] Position reconcile error: %s", self.client_id, e, exc_info=True)
                summary["errors"].append(f"positions: {e}")
                self._report_health_error(f"positions_reconcile_error: {e}", fatal=False)

            try:
                self._check_duplicate_positions(summary)
            except Exception as e:
                log.error("[%s] Dedup check error: %s", self.client_id, e, exc_info=True)
                summary["errors"].append(f"dedup: {e}")
                self._report_health_error(f"dedup_reconcile_error: {e}", fatal=False)

            # FIX-8: record and surface pass timing.
            summary["elapsed_sec"] = round(time.monotonic() - _pass_start, 3)

            self._heartbeat(
                "run_once_complete",
                run=self._run_count,
                orders_checked=summary.get("orders_checked", 0),
                orders_corrected=summary.get("orders_corrected", 0),
                orders_alerted=summary.get("orders_alerted", 0),
                positions_checked=summary.get("positions_checked", 0),
                positions_corrected=summary.get("positions_corrected", 0),
                positions_imported=summary.get("positions_imported", 0),
                positions_alerted=summary.get("positions_alerted", 0),
                errors=len(summary.get("errors", [])),
                elapsed_sec=summary.get("elapsed_sec", 0.0),
            )

            log.info(
                "[%s] Reconcile #%d complete | orders=%d corrected=%d alerted=%d "
                "positions=%d pos_corrected=%d pos_imported=%d pos_alerts=%d "
                "errors=%d elapsed=%.2fs",
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
                summary["elapsed_sec"],
            )

            self._last_run_once_summary = summary
            return summary

    def _verify_fill_monitor_or_alert(self) -> bool:
        """Confirm the dedicated fill monitor is wired/alive when exposed."""
        fm = getattr(self, "fill_monitor", None) or getattr(self, "fill_mon", None)
        alive = False
        if fm is not None:
            try:
                if hasattr(fm, "is_alive") and callable(fm.is_alive):
                    alive = bool(fm.is_alive())
                elif hasattr(fm, "_thread"):
                    alive = bool(getattr(fm, "_thread") and fm._thread.is_alive())
                elif hasattr(fm, "running"):
                    alive = bool(getattr(fm, "running"))
            except Exception:
                alive = False
        if not alive:
            msg = (
                "FILL_MONITOR_NOT_CONFIRMED | fill_monitor_final_hardened-7.py "
                "not wired/alive from reconciler view; using tightened reconciler "
                f"interval={self._interval}s as safety net."
            )
            if self.require_fill_monitor:
                raise RuntimeError(msg)
            self._alert(msg)
            return False
        return True

    def _extract_explicit_cumulative_fill_qty(self, broker_raw: dict) -> Optional[int]:
        """Require normalized broker cumulative fill quantity; never invent qty=0."""
        for key in (
            "filled_qty", "filled_quantity", "cumulative_filled_qty",
            "cumulative_filled_quantity", "exec_quantity",
            "executed_quantity", "filled",
        ):
            if key not in broker_raw:
                continue
            val = broker_raw.get(key)
            if val is None or val == "":
                continue
            try:
                qty = int(float(val))
                if qty >= 0:
                    return qty
            except Exception:
                continue
        return None

    def _extract_avg_fill_price(self, broker_raw: dict) -> Optional[float]:
        for key in (
            "avg_fill_price", "average_fill_price", "fill_price",
            "filled_avg_price", "avg_price", "price",
        ):
            if key not in broker_raw:
                continue
            val = broker_raw.get(key)
            if val is None or val == "":
                continue
            try:
                px = float(val)
                if px > 0:
                    return px
            except Exception:
                continue
        return None

    def _db_order_filled_qty(self, order: dict) -> int:
        for key in ("filled_qty", "filled_quantity", "exec_quantity", "quantity_filled"):
            try:
                val = order.get(key)
                if val is not None and val != "":
                    return max(0, int(float(val)))
            except Exception:
                pass
        return 0

    def _db_order_requested_qty(self, order: dict) -> int:
        for key in ("qty", "quantity", "contracts", "order_qty"):
            try:
                val = order.get(key)
                if val is not None and val != "":
                    return max(0, int(float(val)))
            except Exception:
                pass
        return 0

    def _order_family_from_kind_and_status(self, order: dict, db_status: str = "") -> str | None:
        """
        Return ENTRY or EXIT only when kind/status agree enough for safe state mapping.

        Reconciler must not push an ENTRY status onto an EXIT order or vice versa if
        orders.kind is dirty. EXIT status-family wins only when kind is EXIT or the
        DB status is explicitly exit-prefixed. Generic SUBMITTED/ACKNOWLEDGED states
        require a trustworthy kind. Ambiguous rows are skipped and alerted.
        """
        kind   = str(order.get("kind") or "").strip().upper()
        status = str(db_status or order.get("status") or "").strip().upper()

        exit_statuses = {
            "EXIT_REQUESTED", "EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED",
            "EXIT_PARTIAL_FILL", "EXIT_FILLED", "PENDING_CANCEL",
        }
        entry_statuses = {
            "PENDING_TRIGGER", "CREATED", "SUBMITTED", "ACKNOWLEDGED",
            "PARTIAL_FILL", "FILLED",
        }

        if kind == "EXIT":
            return "EXIT"
        if kind == "ENTRY":
            if status in exit_statuses:
                return None
            return "ENTRY"
        if status in exit_statuses or status.startswith("EXIT_"):
            return "EXIT"
        if status in entry_statuses:
            return "ENTRY"
        return None

    def _apply_osm_fill_update(
        self,
        local_id: str,
        status: str,
        filled_qty: int,
        fill_price: float,
        broker_order_id: str | None = None,
    ) -> bool:
        """Compatibility bridge for OSM v3 apply_fill_update()."""
        status_u = str(status or "").upper()
        apply_fn = getattr(self.osm, "apply_fill_update", None)
        if callable(apply_fn) and status_u in {"PARTIAL_FILL", "EXIT_PARTIAL_FILL"}:
            try:
                return bool(apply_fn(
                    local_id,
                    cumulative_filled=int(filled_qty),
                    fill_price=float(fill_price) if fill_price is not None else None,
                    broker_order_id=broker_order_id,
                ))
            except TypeError as te:
                log.warning(
                    "[%s] OSM apply_fill_update signature mismatch for %s; falling back to transition(): %s",
                    self.client_id, local_id, te,
                )
        return bool(self.osm.transition(
            local_id,
            status_u,
            filled_qty=int(filled_qty),
            fill_price=float(fill_price) if fill_price is not None else None,
        ))

    # ──────────────────────────────────────────────────────────────────────────
    # Exit-fill position heal (backup truth repair)
    # ──────────────────────────────────────────────────────────────────────────

    def _heal_exit_filled_positions_from_orders(self, summary: dict) -> None:
        """
        Backup fill-truth repair. If any EXIT order is EXIT_FILLED with a confirmed
        fill_price, but the linked position is still missing exit_price / realized_pnl
        / realized_pnl_pct, finalize the position from the order row.

        Catches: manual exits, restart gaps, fill monitor delays, OSM misses.
        Production rule: orders table receives broker truth first.
                         positions table is finalized from orders table.
                         dashboard reads positions only after broker truth is copied.
        """
        try:
            from ap.position_manager import APPositionManager

            def _fn():
                with conn() as c:
                    c.execute(
                        """
                        SELECT
                            o.local_order_id,
                            o.position_id,
                            o.fill_price,
                            o.filled_qty,
                            o.filled_ts,
                            o.broker_order_id
                        FROM orders o
                        JOIN positions p ON p.id = o.position_id AND p.client_id = o.client_id
                        WHERE o.client_id = %s
                          AND o.kind = 'EXIT'
                          AND o.status = 'EXIT_FILLED'
                          AND o.fill_price IS NOT NULL
                          AND COALESCE(o.filled_qty, 0) > 0
                          AND p.avg_fill IS NOT NULL
                          AND p.avg_fill > 0
                          AND (
                              p.exit_price IS NULL
                              OR p.realized_pnl IS NULL
                              OR p.realized_pnl_pct IS NULL
                              OR p.quantity_remaining IS NULL
                          )
                        ORDER BY o.filled_ts DESC
                        LIMIT 50
                        """,
                        (self.client_id,),
                    )
                    return c.fetchall()

            rows = run_with_retry(_fn) or []
            if not rows:
                return

            pm = APPositionManager(self.client_id)
            healed = 0
            for row in rows:
                row = dict(row) if not isinstance(row, dict) else row
                ok = pm.close_position_from_exit_fill(
                    position_id=str(row["position_id"]),
                    exit_price=float(row["fill_price"]),
                    filled_qty=int(row["filled_qty"]),
                    filled_ts=str(row["filled_ts"]) if row.get("filled_ts") else None,
                    local_order_id=str(row.get("local_order_id") or ""),
                    broker_order_id=str(row.get("broker_order_id") or ""),
                    close_source="reconciler_broker_exit_fill",
                    close_confidence="HIGH",
                )
                if ok:
                    healed += 1

            if healed:
                summary["positions_corrected"] = int(summary.get("positions_corrected") or 0) + healed
                log.warning(
                    "[%s] Reconciler healed %d EXIT_FILLED position finalization gap(s)",
                    self.client_id, healed,
                )

        except Exception as exc:
            log.error(
                "[%s] _heal_exit_filled_positions_from_orders failed: %s",
                self.client_id, exc, exc_info=True,
            )
            try:
                summary.setdefault("errors", []).append(f"exit_fill_heal: {exc}")
            except Exception:
                pass

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
                    self.client_id, broker_status, local_id,
                )

        self._check_ghost_fills(summary)

    def _resolve_missing_id_exit_truth(self, order: dict, summary: dict, *, reason: str = "") -> bool:
        """Resolve an EXIT order that has no broker_order_id into exactly one safe endpoint.

        Endpoints:
          - broker open exit confidently recovered -> OSM EXIT_ACKNOWLEDGED + exit_engine.set_pending_exit_order
          - recent fill exists -> OSM EXIT_FILLED + exit_engine mark/partial hooks through OSM
          - no broker order/fill after repeated proof -> mark replacement safe and terminal-cancel local order
          - ambiguous/unknown -> alert and keep quarantine
        """
        local_id      = str(order.get("local_order_id") or order.get("id") or "")
        pos_id        = str(order.get("position_id") or order.get("positionId") or "").strip()
        contract      = self._norm_contract(order.get("contract") or order.get("symbol") or "")
        underlying    = self._norm_underlying(
            order.get("underlying") or order.get("ticker") or contract[:6]
        )
        requested_qty = self._db_order_requested_qty(order)

        if not local_id or not pos_id:
            self._alert(
                f"MISSING_ID_EXIT_RESOLVE_BLOCKED | {contract or '?'} | {local_id or '?'} | "
                "missing local_id or position_id; manual review required"
            )
            summary["orders_alerted"] += 1
            return False

        # Endpoint 1: recover an active broker order identity if possible.
        try:
            if self._recover_missing_broker_id_exit(order, summary):
                broker_oid = None
                try:
                    refreshed  = self.osm.get_order(local_id) if hasattr(self.osm, "get_order") else None
                    broker_oid = refreshed.get("broker_order_id") if refreshed else None
                except Exception:
                    broker_oid = None
                if self.exit_engine and broker_oid:
                    try:
                        self.exit_engine.set_pending_exit_order(
                            pos_id,
                            local_order_id=local_id,
                            broker_order_id=str(broker_oid),
                            qty=requested_qty,
                            reason="missing_id_exit_recovered_by_reconciler",
                        )
                    except Exception as exc:
                        log.warning(
                            "[%s] exit_engine set_pending after missing-id recovery failed: %s",
                            self.client_id, exc,
                        )
                return True
        except Exception as exc:
            log.warning(
                "[%s] missing-id broker recovery errored for %s: %s",
                self.client_id, local_id, exc,
            )

        # Endpoint 2: recent fill evidence. Prefer to advance OSM so OSM owns hooks.
        recent_fill = self._get_recent_exit_fill(contract, underlying)
        if recent_fill:
            fill_qty = self._safe_int(recent_fill.get("filled_qty"), requested_qty or 0)
            fill_px  = self._safe_float(recent_fill.get("fill_price"), 0.0)
            if fill_qty > 0 and fill_px > 0:
                try:
                    self.osm.transition(
                        local_id,
                        "EXIT_FILLED",
                        filled_qty=fill_qty,
                        fill_price=fill_px,
                        last_error="reconciler_missing_id_recent_exit_fill_resolved",
                    )
                    self._alert(
                        f"MISSING_ID_EXIT_RESOLVED_BY_RECENT_FILL | {contract or '?'} | {local_id} | "
                        f"pos={pos_id} qty={fill_qty} price={fill_px:.4f}"
                    )
                    summary["orders_corrected"] += 1
                    return True
                except Exception as exc:
                    self._alert(
                        f"MISSING_ID_EXIT_RECENT_FILL_OSM_FAILED | {contract or '?'} | {local_id} | {exc}"
                    )
                    summary["orders_alerted"] += 1
                    return False
            self._alert(
                f"MISSING_ID_EXIT_RECENT_FILL_INCOMPLETE | {contract or '?'} | {local_id} | "
                "recent fill found but qty/price missing; keeping quarantine"
            )
            summary["orders_alerted"] += 1
            return False

        # Endpoint 3: negative proof after recovery passes. No broker order and no recent fill.
        # Mark replacement safe BEFORE terminalizing local order so the engine can allow one
        # future protective replacement path if needed.  OSM terminal transition will also clear
        # in-flight via on_exit_failure/clear_exit_in_flight hooks.
        if self.exit_engine:
            try:
                if hasattr(self.exit_engine, "mark_exit_replacement_safe"):
                    self.exit_engine.mark_exit_replacement_safe(
                        pos_id,
                        reason=reason or "missing_id_exit_negative_broker_checks",
                        local_order_id=local_id,
                        broker_order_id="",
                        reconciled=True,
                    )
                elif hasattr(self.exit_engine, "clear_exit_in_flight"):
                    self.exit_engine.clear_exit_in_flight(
                        pos_id,
                        reason=reason or "missing_id_exit_negative_broker_checks",
                        local_order_id=local_id,
                        broker_order_id="",
                        rejected=False,
                        reconciled=True,
                    )
            except Exception as exc:
                log.error(
                    "[%s] exit_engine quarantine release failed pos=%s order=%s: %s",
                    self.client_id, pos_id, local_id, exc,
                )

        try:
            ok = self.osm.transition(
                local_id,
                "CANCELED",
                last_error=reason or "reconciler_missing_id_exit_negative_broker_checks_replacement_safe",
            )
            if ok:
                self._alert(
                    f"MISSING_ID_EXIT_RESOLVED_REPLACEMENT_SAFE | {contract or '?'} | {local_id} | "
                    f"pos={pos_id}; no broker open order/recent fill after repeated checks"
                )
                summary["orders_corrected"] += 1
                self._missing_id_exit_tracker.pop(str(local_id), None)
                return True
        except Exception as exc:
            log.error(
                "[%s] failed to terminal-cancel missing-id exit %s: %s",
                self.client_id, local_id, exc,
            )

        # Endpoint 4: unresolved, visible quarantine.
        self._alert(
            f"MISSING_ID_EXIT_UNRESOLVED_QUARANTINE | {contract or '?'} | {local_id} | "
            f"pos={pos_id}; manual review required"
        )
        summary["orders_alerted"] += 1
        return False

    def _handle_order_without_broker_id(self, order: dict, summary: dict) -> None:
        local_id   = order.get("local_order_id") or order.get("id")
        contract   = self._norm_contract(order.get("contract") or order.get("symbol") or "?")
        kind       = (order.get("kind") or "ENTRY").upper()
        db_status  = (order.get("status") or "").upper()
        created_ts = order.get("created_ts")

        # Deferred overnight/watcher entries may intentionally not have broker ids yet.
        _contract_raw   = order.get("contract")
        _is_no_contract = not _contract_raw or str(_contract_raw).strip() in ("", "None", "null")
        if kind == "ENTRY" and _is_no_contract and created_ts:
            try:
                _ts = datetime.fromisoformat(str(created_ts).replace("Z", "+00:00"))
                from zoneinfo import ZoneInfo as _ZI
                _ts_et = _ts.astimezone(_ZI("America/New_York"))
                if _ts_et.hour >= 20 or _ts_et.hour < 9:
                    log.debug(
                        "[%s] skip phantom cancel — overnight entry watcher owns | %s",
                        self.client_id, local_id,
                    )
                    return
            except Exception:
                pass

        # Missing-ID protective exits are special. Do not phantom-cancel on age alone:
        # first attempt broker identity recovery by contract/qty/sell-to-close evidence.
        if kind == "EXIT" and db_status in {
            "EXIT_REQUESTED", "EXIT_SUBMITTED", "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL",
            "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL",
        }:
            if self._recover_missing_broker_id_exit(order, summary):
                self._missing_id_exit_tracker.pop(str(local_id), None)
                return

            recent_fill = self._get_recent_exit_fill(
                contract,
                self._norm_underlying(
                    order.get("underlying") or order.get("ticker") or contract[:6]
                ),
            )
            if recent_fill:
                self._alert(
                    f"MISSING_ID_EXIT_RECENT_FILL_FOUND | {contract} | {local_id} | "
                    "not phantom-canceling; waiting for fill/OSM convergence"
                )
                summary["orders_alerted"] += 1
                return

            passes = self._missing_id_exit_tracker.get(str(local_id), 0) + 1
            self._missing_id_exit_tracker[str(local_id)] = passes
            if passes < 2:
                self._alert(
                    f"MISSING_ID_EXIT_RECOVERY_PASS_{passes} | {contract} | {local_id} | "
                    "broker_id missing; no broker match yet; refusing age-only phantom cancel"
                )
                summary["orders_alerted"] += 1
                return

            # Full recovery contract for v8/v9 exit-engine quarantine:
            # after repeated negative broker-open-order and recent-fill checks,
            # resolve the exit into exactly one endpoint instead of falling into
            # generic phantom-cancel logic and leaving the exit engine locked.
            if self._resolve_missing_id_exit_truth(
                order,
                summary,
                reason="reconciler_missing_id_exit_after_two_negative_passes",
            ):
                return
            return

        if created_ts:
            try:
                created = datetime.fromisoformat(str(created_ts).replace("Z", "+00:00"))
                age     = (datetime.now(timezone.utc) - created).total_seconds()
                if age > 300:
                    try:
                        self.osm.transition(
                            local_id,
                            "CANCELED",
                            last_error="reconciler_phantom_cancel_no_broker_id_after_negative_broker_checks",
                        )
                        log.warning(
                            "[%s] RECONCILE_AUTO_CANCEL phantom | %s | %s | age=%.0fs no broker_id",
                            self.client_id, contract, local_id, age,
                        )
                        summary["orders_corrected"] += 1
                    except Exception as _ce:
                        log.error("[%s] Failed to cancel phantom %s: %s",
                                  self.client_id, local_id, _ce)
            except Exception:
                pass

    def _safe_get_broker_open_orders(self) -> list[dict]:
        """
        Best-effort broker open-order fetch across adapter method names.

        FIX-6: error-response guard added. If the broker returns a bare dict without
        a recognized list key, and it contains error/message indicators, we skip it
        rather than wrapping the error payload as a fake order object.
        """
        for method_name in ("list_open_orders", "get_open_orders", "list_orders", "orders"):
            method = getattr(self.broker, method_name, None)
            if not callable(method):
                continue
            try:
                try:
                    result = method(status="open")
                except TypeError:
                    result = method()
                if result is None:
                    continue
                if isinstance(result, dict):
                    for key in ("orders", "data", "results"):
                        if isinstance(result.get(key), list):
                            return [dict(x) for x in result.get(key) if isinstance(x, dict)]
                    # FIX-6: guard — error dicts must not become fake order objects.
                    if result.get("error") or result.get("errors") or result.get("message"):
                        log.warning(
                            "[%s] broker %s returned error-like dict; skipping: %s",
                            self.client_id, method_name, result,
                        )
                        continue
                    # Bare dict with no known list key and no error indicator —
                    # treat as a single-order response only if it has an id field.
                    if result.get("id") or result.get("order_id") or result.get("broker_order_id"):
                        return [result]
                    log.warning(
                        "[%s] broker %s returned unrecognized dict shape; skipping: keys=%s",
                        self.client_id, method_name, list(result.keys())[:10],
                    )
                    continue
                if isinstance(result, list):
                    return [dict(x) for x in result if isinstance(x, dict)]
            except Exception as exc:
                log.debug(
                    "[%s] broker %s failed during missing-id recovery: %s",
                    self.client_id, method_name, exc,
                )
        return []

    def _broker_order_id_from_raw(self, raw: dict) -> str:
        return str(
            raw.get("broker_order_id")
            or raw.get("order_id")
            or raw.get("id")
            or raw.get("orderId")
            or ""
        ).strip()

    def _broker_order_contract_from_raw(self, raw: dict) -> str:
        return self._norm_contract(
            raw.get("contract")
            or raw.get("option_symbol")
            or raw.get("symbol")
            or raw.get("instrument")
            or ""
        )

    def _broker_order_qty_from_raw(self, raw: dict) -> int:
        for key in ("quantity", "qty", "order_qty", "remaining_quantity", "remaining_qty"):
            try:
                val = raw.get(key)
                if val is not None and val != "":
                    return abs(int(float(val)))
            except Exception:
                pass
        return 0

    def _broker_order_side_action_from_raw(self, raw: dict) -> str:
        """
        Normalize broker order side/action/instruction.

        Strong matches are explicit sell-to-close variants. A plain "sell" is only a
        weak fallback because some broker payloads are sparse, but it should not be
        allowed to win recovery unless contract/qty/freshness also line up.
        """
        fields = (
            "side", "action", "instruction", "order_action",
            "transaction_type", "trade_action", "type", "order_type",
        )
        text    = " ".join(str(raw.get(k) or "") for k in fields).strip().lower()
        compact = text.replace("_", "").replace("-", "").replace(" ", "")
        if compact in {"selltoclose", "stc"} or "sell to close" in text or "sell_to_close" in text:
            return "SELL_TO_CLOSE"
        if compact in {"buytoclose", "btc"} or "buy to close" in text or "buy_to_close" in text:
            return "BUY_TO_CLOSE"
        if "sell" in text:
            return "SELL"
        if "buy" in text:
            return "BUY"
        return ""

    def _broker_order_is_exit_like(self, raw: dict) -> bool:
        action = self._broker_order_side_action_from_raw(raw)
        if action in {"SELL_TO_CLOSE", "SELL"}:
            return True
        extra_text = " ".join(
            str(raw.get(k) or "")
            for k in ("description", "tag", "client_order_id", "client_tag", "memo", "notes")
        ).lower()
        compact = extra_text.replace("_", "").replace("-", "").replace(" ", "")
        return "selltoclose" in compact or "stc" == compact

    def _raw_identity_values(self, raw: dict) -> set[str]:
        vals: set[str] = set()
        for key in (
            "position_id", "positionId", "ap_position_id", "local_position_id",
            "client_position_id", "related_position_id", "parent_position_id",
            "client_tag", "client_order_id", "clientOrderId", "tag",
            "memo", "notes", "strategy_id",
        ):
            val = raw.get(key)
            if val is None:
                continue
            sval = str(val).strip()
            if sval:
                vals.add(sval)
        return vals

    def _order_identity_values(self, order: dict) -> set[str]:
        vals: set[str] = set()
        for key in (
            "position_id", "positionId", "ap_position_id", "local_position_id",
            "client_position_id", "related_position_id", "parent_position_id",
            "client_tag", "client_order_id", "tag", "signal_id", "plan_id",
        ):
            val = order.get(key)
            if val is None:
                continue
            sval = str(val).strip()
            if sval:
                vals.add(sval)
        return vals

    def _parse_broker_order_time(self, raw: dict) -> Optional[datetime]:
        for key in (
            "created_ts", "created_at", "submitted_at", "transaction_date",
            "timestamp", "time", "date", "updated_at",
        ):
            val = raw.get(key)
            if not val:
                continue
            try:
                if isinstance(val, datetime):
                    dt = val
                else:
                    txt = str(val).strip().replace("Z", "+00:00")
                    dt  = datetime.fromisoformat(txt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
        return None

    def _parse_db_order_time(self, order: dict) -> Optional[datetime]:
        for key in ("created_ts", "submitted_ts", "updated_ts"):
            val = order.get(key)
            if not val:
                continue
            try:
                if isinstance(val, datetime):
                    dt = val
                else:
                    dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                return dt.astimezone(timezone.utc)
            except Exception:
                continue
        return None

    def _score_missing_id_exit_candidate(self, order: dict, raw: dict) -> tuple[int, list[str]]:
        """
        Score a broker open order as a recovery candidate for a missing-ID EXIT.

        Hard filters happen in _recover_missing_broker_id_exit(); this score ranks
        remaining matches. It favors explicit identity metadata, sell-to-close
        semantics, exact qty, and submit-time proximity. Loose text-only exit
        evidence is deliberately low value.
        """
        score   = 0
        reasons: list[str] = []

        order_ids = self._order_identity_values(order)
        raw_ids   = self._raw_identity_values(raw)
        if order_ids and raw_ids and (order_ids & raw_ids):
            score += 100
            reasons.append("identity")

        action = self._broker_order_side_action_from_raw(raw)
        if action == "SELL_TO_CLOSE":
            score += 40
            reasons.append("explicit_stc")
        elif action == "SELL":
            score += 15
            reasons.append("sell_fallback")
        else:
            reasons.append("no_exit_action")

        requested_qty = self._db_order_requested_qty(order)
        broker_qty    = self._broker_order_qty_from_raw(raw)
        if requested_qty > 0 and broker_qty > 0:
            if requested_qty == broker_qty:
                score += 25
                reasons.append("qty_exact")
            else:
                score -= 100
                reasons.append("qty_mismatch")

        db_ts     = self._parse_db_order_time(order)
        broker_ts = self._parse_broker_order_time(raw)
        if db_ts and broker_ts:
            delta = abs((broker_ts - db_ts).total_seconds())
            if delta <= 30:
                score += 35
                reasons.append("time_30s")
            elif delta <= 120:
                score += 25
                reasons.append("time_120s")
            elif delta <= 300:
                score += 10
                reasons.append("time_300s")
            else:
                score -= 50
                reasons.append("stale_time")
        else:
            reasons.append("time_unknown")

        return score, reasons

    def _recover_missing_broker_id_exit(self, order: dict, summary: dict) -> bool:
        local_id      = str(order.get("local_order_id") or order.get("id") or "")
        contract      = self._norm_contract(order.get("contract") or order.get("symbol") or "")
        requested_qty = self._db_order_requested_qty(order)
        db_ts         = self._parse_db_order_time(order)
        position_id   = str(order.get("position_id") or order.get("positionId") or "").strip()

        scored: list[tuple[int, str, dict, list[str]]] = []
        rejected_count = 0

        for raw in self._safe_get_broker_open_orders():
            status = str(raw.get("status") or raw.get("Status") or "").lower().strip()
            if status and status in BROKER_TERMINAL:
                continue

            broker_contract = self._broker_order_contract_from_raw(raw)
            if contract and broker_contract and broker_contract != contract:
                continue
            if contract and not broker_contract:
                rejected_count += 1
                continue

            bqty = self._broker_order_qty_from_raw(raw)
            if requested_qty > 0 and bqty > 0 and bqty != requested_qty:
                continue

            action       = self._broker_order_side_action_from_raw(raw)
            raw_ids      = self._raw_identity_values(raw)
            has_identity = bool(position_id and position_id in raw_ids)
            if action not in {"SELL_TO_CLOSE", "SELL"} and not has_identity:
                continue

            # Freshness guard: stale old replacement orders must not win missing-ID recovery.
            broker_ts = self._parse_broker_order_time(raw)
            if db_ts and broker_ts:
                delta = abs((broker_ts - db_ts).total_seconds())
                if delta > 300 and not has_identity:
                    continue

            broker_oid = self._broker_order_id_from_raw(raw)
            if not broker_oid:
                continue

            score, reasons = self._score_missing_id_exit_candidate(order, raw)
            if score < 40:
                rejected_count += 1
                continue
            scored.append((score, broker_oid, raw, reasons))

        if not scored:
            return False

        scored.sort(key=lambda x: x[0], reverse=True)
        top_score, broker_oid, raw, reasons = scored[0]
        tied = [x for x in scored if x[0] == top_score]
        if len(tied) > 1:
            self._alert(
                f"MISSING_ID_EXIT_AMBIGUOUS | {contract or '?'} | {local_id} | "
                f"{len(tied)} tied broker candidates score={top_score}; manual review required"
            )
            summary["orders_alerted"] += 1
            return False

        if len(scored) > 1 and (top_score - scored[1][0]) < 25:
            self._alert(
                f"MISSING_ID_EXIT_AMBIGUOUS_CLOSE_SCORE | {contract or '?'} | {local_id} | "
                f"top={top_score} second={scored[1][0]} reasons={','.join(reasons)}; manual review required"
            )
            summary["orders_alerted"] += 1
            return False

        try:
            self._backfill_order_broker_id(local_id, broker_oid)
        except Exception as exc:
            log.warning(
                "[%s] broker id DB backfill failed for %s -> %s: %s",
                self.client_id, local_id, broker_oid, exc,
            )

        try:
            self.osm.transition(local_id, "EXIT_ACKNOWLEDGED", broker_order_id=broker_oid, last_error=None)
        except TypeError:
            self.osm.transition(local_id, "EXIT_ACKNOWLEDGED", broker_order_id=broker_oid)
        except Exception as exc:
            log.warning(
                "[%s] OSM ack refresh failed after broker-id recovery for %s: %s",
                self.client_id, local_id, exc,
            )

        self._alert(
            f"MISSING_ID_EXIT_RECOVERED | {contract or '?'} | {local_id} | "
            f"broker_order_id={broker_oid} score={top_score} reasons={','.join(reasons)}"
        )
        summary["orders_corrected"] += 1
        return True

    def _backfill_order_broker_id(self, local_id: str, broker_order_id: str) -> None:
        if not local_id or not broker_order_id:
            return
        from ap.db import conn, run_with_retry

        def _update():
            with conn() as c:
                c.execute(
                    """
                    UPDATE orders
                    SET broker_order_id=%s, updated_ts=NOW()
                    WHERE client_id=%s AND local_order_id=%s
                    """,
                    (broker_order_id, self.client_id, local_id),
                )

        run_with_retry(_update)

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
        family    = self._order_family_from_kind_and_status(order, db_status)
        if family not in {"ENTRY", "EXIT"}:
            self._alert(
                f"RECONCILE_ORDER_FAMILY_AMBIGUOUS | {contract} | {local_id} | "
                f"kind={order.get('kind')} status={db_status} broker={broker_status}; "
                "skipping fill correction to avoid wrong ENTRY/EXIT state-family transition"
            )
            summary["orders_alerted"] += 1
            return

        new_status = "FILLED" if family == "ENTRY" else "EXIT_FILLED"
        if broker_status == "partially_filled":
            new_status = "PARTIAL_FILL" if family == "ENTRY" else "EXIT_PARTIAL_FILL"

        filled_qty = self._extract_explicit_cumulative_fill_qty(broker_raw)
        avg_fill   = self._extract_avg_fill_price(broker_raw)
        if filled_qty is None:
            self._alert(
                f"BROKER_FILL_QTY_NOT_NORMALIZED | {contract} | {local_id} | "
                "broker returned filled/partial status without explicit cumulative fill qty; "
                "skipping OSM correction until broker adapter normalizes this field"
            )
            summary["orders_alerted"] += 1
            return
        if avg_fill is None:
            self._alert(
                f"BROKER_FILL_PRICE_NOT_NORMALIZED | {contract} | {local_id} | "
                "broker returned filled/partial status without explicit avg fill price; "
                "skipping OSM correction until broker adapter normalizes this field"
            )
            summary["orders_alerted"] += 1
            return

        requested_qty    = self._db_order_requested_qty(order)
        db_filled_before = self._db_order_filled_qty(order)
        if (
            requested_qty > 0
            and db_filled_before > 0
            and db_filled_before < requested_qty
            and filled_qty >= requested_qty
        ):
            partial_status = "PARTIAL_FILL" if family == "ENTRY" else "EXIT_PARTIAL_FILL"
            try:
                self._apply_osm_fill_update(local_id, partial_status, db_filled_before, avg_fill)
                log.warning(
                    "[%s] RECONCILE_INTERMEDIATE_FILL_UPDATE | %s | %s | qty=%s before terminal qty=%s",
                    self.client_id, contract, local_id, db_filled_before, filled_qty,
                )
            except Exception as ie:
                log.error("[%s] Intermediate fill update failed for %s: %s",
                          self.client_id, local_id, ie)

        log.warning(
            "[%s] RECONCILE_CORRECT: %s | %s | DB=%s broker=%s → advancing to %s",
            self.client_id, contract, local_id, db_status, broker_status, new_status,
        )

        try:
            ok = self._apply_osm_fill_update(
                local_id,
                new_status,
                filled_qty=filled_qty,
                fill_price=avg_fill,
                broker_order_id=str(
                    broker_raw.get("id")
                    or broker_raw.get("order_id")
                    or broker_raw.get("broker_order_id")
                    or ""
                ) or None,
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

            if family == "ENTRY" and new_status == "FILLED" and self.pm:
                self._ensure_position_for_filled_entry(order, filled_qty, avg_fill, summary)
        except Exception as e:
            log.error("[%s] Reconcile transition error: %s", self.client_id, e)

    def _advance_order_to_terminal(self, order: dict, broker_status: str, summary: dict) -> None:
        """
        Advance a DB-open order to its terminal broker state.

        FIX-2: previously used (order.get("kind") or "ENTRY") which silently defaulted
        NULL-kind EXIT orders to ENTRY, preventing _revert_position_to_open from being
        called when a real exit order was rejected or canceled. Now uses the same
        _order_family_from_kind_and_status resolver as _advance_order_to_broker_fill so
        that db_status EXIT-prefixed rows are correctly identified even when kind is NULL.
        """
        local_id   = order.get("local_order_id") or order.get("id")
        contract   = order.get("contract") or order.get("symbol") or "?"
        db_status  = (order.get("status") or "").upper()
        new_status = BROKER_TO_OSM.get(broker_status, "CANCELED")

        # FIX-2: use the family resolver so NULL kind on an EXIT row still works.
        family = self._order_family_from_kind_and_status(order, db_status)
        # If family is unresolvable (genuinely ambiguous), fall back to raw kind rather
        # than default-to-ENTRY; log a warning so the ambiguity is visible.
        if family is None:
            raw_kind = (order.get("kind") or "").strip().upper()
            if raw_kind in {"ENTRY", "EXIT"}:
                family = raw_kind
            else:
                log.warning(
                    "[%s] RECONCILE_TERMINAL_FAMILY_AMBIGUOUS | %s | %s | "
                    "kind=%s db_status=%s — skipping position revert to be safe",
                    self.client_id, contract, local_id,
                    order.get("kind"), db_status,
                )
                family = "ENTRY"   # conservative: don't revert, don't corrupt

        log.warning(
            "[%s] RECONCILE_CORRECT: %s | %s | DB=%s broker=%s → advancing to %s",
            self.client_id, contract, local_id, db_status, broker_status, new_status,
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
            if family == "EXIT":
                position_id = order.get("position_id")
                if position_id:
                    self._revert_position_to_open(
                        position_id,
                        contract,
                        failed_exit_order_id=local_id,
                    )
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
            plan_id       = order.get("plan_id") or order.get("signal_id") or order.get("local_order_id")
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
                if order.get("stop_underlying") else None,
                target_underlying=float(order.get("target_underlying"))
                if order.get("target_underlying") else None,
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
        """
        Check orders DB says FILLED where broker shows terminal. Alert only.

        FIX-4: window reduced from 2 hours to 30 minutes and hard-capped at 10 rows
        to prevent up to 20 sequential broker get_order calls per reconcile pass on
        a busy trading day.
        """
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
                      AND updated_ts > NOW() - INTERVAL '30 minutes'
                    ORDER BY updated_ts DESC
                    LIMIT 10
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
            if broker_oid in self._ghost_fill_confirmed:
                continue   # already confirmed as truly filled on a prior pass
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
                else:
                    # Broker confirms it is still filled — cache so we skip next pass
                    self._ghost_fill_confirmed.add(broker_oid)
            except Exception:
                pass

    # ──────────────────────────────────────────────────────────────────────────
    # Position reconciliation
    # ──────────────────────────────────────────────────────────────────────────

    def _norm_contract(self, val: str) -> str:
        return str(val or "").strip().upper()

    def _norm_underlying(self, val: str) -> str:
        return str(val or "").strip().upper()

    def _safe_int(self, value, default: int = 0) -> int:
        try:
            if value is None or value == "":
                return int(default)
            return int(float(value))
        except Exception:
            return int(default)

    def _safe_float(self, value, default: float = 0.0) -> float:
        try:
            if value is None or value == "":
                return float(default)
            out = float(value)
            if out != out:  # NaN guard
                return float(default)
            return out
        except Exception:
            return float(default)

    def _safe_get_broker_positions(self) -> list[dict]:
        """Fetch broker positions; return [] on error."""
        try:
            result = self.broker.list_positions()
            if result is None:
                return []
            if isinstance(result, list):
                return [dict(x) for x in result if isinstance(x, dict)]
            if isinstance(result, dict):
                # Tradier: {"positions": {"position": [...]}} or {"positions": []}
                inner = result.get("positions") or result.get("data") or result.get("results")
                if isinstance(inner, list):
                    return [dict(x) for x in inner if isinstance(x, dict)]
                if isinstance(inner, dict):
                    pos = inner.get("position")
                    if isinstance(pos, list):
                        return [dict(x) for x in pos if isinstance(x, dict)]
                    if isinstance(pos, dict):
                        return [pos]
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
            "avg_fill", "avg_price", "average_price", "average_cost",
            "cost_per_share", "price", "last_price",
        ):
            try:
                val = bp.get(key)
                if val is not None and float(val) > 0:
                    return float(val)
            except Exception:
                pass

        try:
            qty        = self._broker_position_qty(bp)
            cost_basis = float(bp.get("cost_basis") or bp.get("costbasis") or 0)
            if qty > 0 and cost_basis > 0:
                return abs(cost_basis) / qty / 100.0
        except Exception:
            pass

        # Last resort: do not invent a valid price silently.
        return 0.0

    def _broker_position_side(self, bp: dict) -> str:
        """
        Determine CALL or PUT from broker position payload.

        FIX-1: the previous implementation used `if "P" in contract[-16:]` which
        misidentifies CALLs as PUTs whenever the root ticker itself contains a "P"
        and the contract string is short enough that the root character falls within
        the last-16 window (e.g. single-character ticker "P" for Prudential Financial,
        or any other instrument whose OCC symbol starts with P).

        The correct approach is to anchor a regex to the end of the OCC string and
        match the C/P character that immediately precedes the 8-digit strike.
        Standard OCC format: {root}{YYMMDD}{C|P}{8-digit strike}
        """
        side = str(bp.get("direction") or bp.get("side") or "").upper()
        if side in {"CALL", "PUT"}:
            return side

        contract = self._broker_position_contract(bp)
        # FIX-1: use compiled regex anchored to end-of-string.
        m = _OCC_CP_RE.search(contract)
        if m:
            return "PUT" if m.group(1) == "P" else "CALL"

        # Fallback: if contract is non-standard or empty, default to CALL and
        # emit a warning so the ambiguity is visible in logs.
        if contract:
            log.warning(
                "[%s] _broker_position_side: could not parse C/P from contract '%s'; "
                "defaulting to CALL — verify broker position payload",
                self.client_id, contract,
            )
        return "CALL"

    def _get_open_db_positions(self) -> list[dict]:
        """Fetch DB OPEN/CLOSING positions using the canonical DB_OPEN_POSITION_STATUSES constant."""
        try:
            from ap.db import run_with_retry, conn

            def _fetch():
                with conn() as c:
                    c.execute(
                        """
                        SELECT *
                        FROM positions
                        WHERE client_id=%s
                          AND status = ANY(%s)
                        ORDER BY entry_ts DESC NULLS LAST
                        """,
                        (self.client_id, list(DB_OPEN_POSITION_STATUSES)),
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
        """Three-pass ghost detection to reduce false closes from broker API gaps."""
        key   = self._norm_contract(contract)
        count = int(self._ghost_tracker.get(key, 0)) + 1
        self._ghost_tracker[key] = count
        if count >= 3:
            del self._ghost_tracker[key]
            return True
        return False

    def _reconcile_positions(self, summary: dict):
        """
        Evidence-based position reconciliation plus broker-open import.

        Policy:
          1. Match DB positions by exact contract symbol first.
          2. Auto-close DB positions only with filled exit evidence or three-pass ghost confirm.
          3. Import broker-open positions missing from DB so restarts cannot orphan trades.

        FIX-7: db_underlyings removed — it was constructed and passed to
        _import_broker_positions_missing_from_db but never used as a filter condition
        inside that method, creating misleading dead state.
        """
        summary.setdefault("positions_corrected", 0)
        summary.setdefault("positions_imported", 0)

        broker_positions = self._safe_get_broker_positions()
        broker_by_contract:   dict[str, dict]       = {}
        broker_by_underlying: dict[str, list[dict]] = {}

        for bp in broker_positions:
            c_sym = self._broker_position_contract(bp)
            u_sym = self._broker_position_underlying(bp)
            qty   = self._broker_position_qty(bp)
            if qty <= 0:
                continue
            if c_sym:
                broker_by_contract[c_sym] = bp
            if u_sym:
                broker_by_underlying.setdefault(u_sym, []).append(bp)

        db_positions = self._get_open_db_positions()
        summary["positions_checked"] = len(db_positions)

        # FIX-7: only track db_contracts — the set actually used for dedup in the import pass.
        db_contracts: set[str] = set()

        # DB → broker checks.
        for pos in db_positions:
            pos_id     = pos.get("id") or pos.get("position_id")
            contract   = self._norm_contract(pos.get("contract") or pos.get("symbol") or "")
            underlying = self._norm_underlying(pos.get("underlying") or pos.get("ticker") or contract[:6])
            db_qty     = int(pos.get("qty") or pos.get("quantity") or 0)
            entry_px   = float(pos.get("avg_fill") or pos.get("entry_price") or 0.0)

            if contract:
                db_contracts.add(contract)

            broker_pos = broker_by_contract.get(contract)

            if broker_pos is None and underlying:
                matches = broker_by_underlying.get(underlying, [])
                if len(matches) == 1:
                    broker_pos = matches[0]
                elif len(matches) > 1:
                    log.warning(
                        "[%s] RECONCILE_AMBIGUOUS | %s — %d broker positions for underlying, "
                        "skipping auto-close",
                        self.client_id, contract, len(matches),
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
                        self.client_id, contract, db_qty, broker_qty,
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
        pos_id    = pos.get("id") or pos.get("position_id")
        exit_fill = self._get_recent_exit_fill(contract, underlying)

        if exit_fill and float(exit_fill.get("fill_price") or 0) > 0:
            exit_px          = float(exit_fill["fill_price"])
            close_confidence = "HIGH"
            log.info(
                "[%s] RECONCILE_CLOSE_EVIDENCE | %s | filled exit found @ $%.4f",
                self.client_id, contract, exit_px,
            )
        else:
            pos_id_str       = str(pos_id or "")
            active_exit      = self._active_exit_order_exists(position_id=pos_id_str) if pos_id_str else None
            broker_open_exit = self._broker_open_exit_exists_for_contract(contract)
            if active_exit or broker_open_exit:
                self._ghost_tracker.pop(contract, None)
                self._alert(
                    f"GHOST_CLOSE_BLOCKED_ACTIVE_EXIT | {contract} | pos={pos_id_str or '?'} | "
                    "broker position missing but active local/broker exit evidence exists; keeping DB OPEN"
                )
                summary["positions_alerted"] += 1
                return

            pass_count = int(self._ghost_tracker.get(self._norm_contract(contract), 0)) + 1
            if not self._mark_ghost_seen(contract):
                log.warning(
                    "[%s] GHOST_PASS_%d | %s | broker has no position — waiting for stronger evidence",
                    self.client_id, pass_count, contract,
                )
                summary["positions_alerted"] += 1
                return
            _current_px = self._get_current_option_price(contract)
            if _current_px > 0:
                exit_px          = _current_px
                close_confidence = "MEDIUM_THREE_PASS_CURRENT_MARK"
                log.warning(
                    "[%s] GHOST_PASS_3 | %s | no broker position, no active exit — "
                    "auto-closing at current mark $%.4f",
                    self.client_id, contract, _current_px,
                )
            else:
                exit_px          = entry_px
                close_confidence = "MEDIUM_THREE_PASS_NO_EXIT_EVIDENCE"
                log.warning(
                    "[%s] GHOST_PASS_3 | %s | no broker position, no active exit, "
                    "no live quote — auto-closing at entry price (P&L = $0)",
                    self.client_id, contract,
                )

        # Lifecycle visibility: ghost/autoclose is a major data-correction event.
        # Record it before the DB row is changed so a future trace can explain
        # exactly why the reconciler decided broker truth required closing DB truth.
        self._record_reconciler_rejection(
            signal_id=str(pos_id or f"ghost:{contract}"),
            ticker=underlying or contract[:6],
            category_name="DATA",
            severity_name="WARNING",
            reason_code="BROKER_POSITION_MISSING_THREE_PASS_CONFIRM",
            human_reason="broker position missing after three-pass confirmation; reconciler auto-closing DB position",
            contract=contract,
            pos_id=pos_id,
            db_qty=db_qty,
            entry_px=entry_px,
            exit_px=exit_px,
            close_confidence=close_confidence,
            client_id=self.client_id,
        )

        pnl_dollars = round((exit_px - entry_px) * db_qty * 100, 2)
        pnl_pct     = round(((exit_px - entry_px) / entry_px) * 100, 2) if entry_px > 0 else 0.0

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
            pnl_dollars, pnl_pct, close_confidence,
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
        summary: dict,
    ) -> None:
        """
        Import broker-open positions not found in DB to prevent orphan trades after restarts.

        FIX-7: db_underlyings parameter removed — it was never used as a filter inside
        this method. Only db_contracts is needed and used for dedup within this pass.
        """
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

            underlying      = self._broker_position_underlying(bp)
            entry_px        = self._broker_position_entry_price(bp)
            price_untrusted = False

            # Prefer real broker cost-basis/avg price. If missing, try a contract mark/last
            # before falling back to a tiny placeholder. Placeholder imports are clearly
            # flagged so the exit engine/dashboard do not silently trust distorted PnL math.
            if entry_px <= 0:
                entry_px = self._broker_position_mark_price(bp)
            if entry_px <= 0:
                entry_px = self._get_current_option_price(contract)
            if entry_px <= 0:
                entry_px = 0.01
                price_untrusted = True
                self._alert(
                    f"BROKER_POSITION_IMPORT_PRICE_UNKNOWN | {contract} | "
                    "using emergency placeholder entry=0.01; price_untrusted=True — MANUAL REVIEW REQUIRED"
                )

            underlying_entry = self._derive_underlying_entry_from_broker_position(
                bp,
                underlying=underlying,
                contract=contract,
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
                underlying_entry=underlying_entry,
                price_untrusted=price_untrusted,
            )

            if not pos_id:
                summary["positions_alerted"] += 1
                self._record_reconciler_rejection(
                    signal_id=f"reconciled:{contract}",
                    ticker=underlying or contract[:6],
                    category_name="DATA",
                    severity_name="WARNING",
                    reason_code="BROKER_POSITION_IMPORT_FAILED",
                    human_reason="broker had open option position but DB import returned no position id",
                    contract=contract,
                    qty=qty,
                    entry_px=entry_px,
                    price_untrusted=price_untrusted,
                    underlying_entry=underlying_entry,
                )
                continue

            self._record_recovered_position(
                signal_id=str(pos_id or f"reconciled:{contract}"),
                ticker=underlying or contract[:6],
                reason="broker_open_position_missing_from_db_imported",
                contract=contract,
                qty=qty,
                side=side,
                entry_px=entry_px,
                price_untrusted=price_untrusted,
                underlying_entry=underlying_entry,
                client_id=self.client_id,
                broker_position=bp,
                db_position_missing=True,
            )

            summary["positions_imported"] += 1
            summary["positions_corrected"] += 1
            db_contracts.add(contract)

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
                    underlying_entry=underlying_entry,
                    price_untrusted=price_untrusted,
                )

            msg = (
                f"BROKER_POSITION_IMPORTED | {underlying or '?'} {side} | "
                f"{contract} | qty={qty} entry={entry_px:.4f} "
                f"underlying_entry={underlying_entry:.4f} "
                f"price_untrusted={price_untrusted} pos={pos_id}"
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
        underlying_entry: float = 0.0,
        price_untrusted: bool = False,
    ) -> Optional[str]:
        """
        Create an OPEN DB row for a broker-open position missing from DB.
        Prefer APPositionManager, fall back to direct SQL with broad/minimal compatibility.

        FIX-3: underlying_entry is now persisted to the DB row in all paths. Previously
        it was only seeded into the in-process exit engine, so after a process restart
        the exit engine received underlying_entry=0 (untrusted current price) instead of
        the actual entry value, corrupting stop/target progress math for the position's
        entire remaining lifetime.
        """
        imported_plan_id   = f"reconciled:{contract}:{int(time.time())}"
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
                    pattern="BROKER_IMPORT_PRICE_UNTRUSTED" if price_untrusted else "BROKER_IMPORT",
                    stop_underlying=None,
                    target_underlying=None,
                )
                if pos_id:
                    # FIX-3: backfill underlying_entry after PM creates the row, since
                    # pm.open_position does not accept underlying_entry as a parameter.
                    if underlying_entry > 0:
                        try:
                            self._backfill_position_underlying_entry(str(pos_id), underlying_entry)
                        except Exception as ue:
                            log.warning(
                                "[%s] underlying_entry backfill failed for imported pos %s: %s",
                                self.client_id, pos_id, ue,
                            )
                    return str(pos_id)
                return None
            except Exception as pm_err:
                log.error(
                    "[%s] pm.open_position import failed for %s: %s — trying SQL fallback",
                    self.client_id, contract, pm_err,
                )

        try:
            from ap.db import conn, run_with_retry

            pos_id  = str(uuid.uuid4())
            now_iso = datetime.now(timezone.utc).isoformat()

            # FIX-3: underlying_entry included in the full-schema insert.
            def _insert_full():
                with conn() as c:
                    c.execute(
                        """
                        INSERT INTO positions (
                            id, client_id, underlying, contract, direction, qty, avg_fill,
                            entry_ts, status, plan_id, signal_id, close_source,
                            close_confidence, underlying_entry
                        ) VALUES (
                            %s,%s,%s,%s,%s,%s,%s,
                            %s,'OPEN',%s,%s,%s,%s,%s
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
                            "BROKER_OPEN_PRICE_UNTRUSTED" if price_untrusted else "BROKER_OPEN",
                            float(underlying_entry) if underlying_entry > 0 else None,
                        ),
                    )

            try:
                run_with_retry(_insert_full)
                return pos_id
            except Exception as full_err:
                log.warning(
                    "[%s] full imported position insert failed for %s: %s — trying minimal schema",
                    self.client_id, contract, full_err,
                )

            # Minimal schema fallback: broadest possible compatibility.
            # FIX-3: attempt to include underlying_entry; if the column does not exist
            # in this schema version the except block retries without it.
            def _insert_with_ue():
                with conn() as c:
                    c.execute(
                        """
                        INSERT INTO positions (
                            id, client_id, underlying, contract, direction, qty, avg_fill,
                            entry_ts, status, underlying_entry
                        ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'OPEN',%s)
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
                            float(underlying_entry) if underlying_entry > 0 else None,
                        ),
                    )

            try:
                run_with_retry(_insert_with_ue)
                return pos_id
            except Exception:
                pass  # column may not exist — fall through to bare minimal

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
            # FIX-3: even on bare minimal insert, attempt a follow-up UPDATE for
            # underlying_entry so restarts don't run on untrusted zero.
            if underlying_entry > 0:
                try:
                    self._backfill_position_underlying_entry(pos_id, underlying_entry)
                except Exception:
                    pass  # non-fatal; exit engine seeding still carries the value
            return pos_id
        except Exception as sql_err:
            log.error("[%s] SQL import failed for broker position %s: %s",
                      self.client_id, contract, sql_err)
            return None

    def _backfill_position_underlying_entry(self, pos_id: str, underlying_entry: float) -> None:
        """
        Persist underlying_entry to an existing DB position row.

        FIX-3 support method: called after pm.open_position (which does not accept
        underlying_entry as a parameter) and after minimal-schema SQL inserts that
        may lack the column in the INSERT column list.
        """
        if not pos_id or underlying_entry <= 0:
            return
        from ap.db import conn, run_with_retry

        def _update():
            with conn() as c:
                c.execute(
                    """
                    UPDATE positions
                    SET underlying_entry = %s
                    WHERE id = %s AND client_id = %s
                    """,
                    (float(underlying_entry), pos_id, self.client_id),
                )

        run_with_retry(_update)

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
                          AND status = ANY(%s)
                          AND UPPER(contract)=%s
                        ORDER BY entry_ts DESC NULLS LAST
                        LIMIT 1
                        """,
                        (self.client_id, list(DB_OPEN_POSITION_STATUSES), contract),
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

        pos_id     = str(pos.get("id") or pos.get("position_id") or "")
        contract   = self._norm_contract(pos.get("contract") or pos.get("symbol") or "")
        underlying = self._norm_underlying(pos.get("underlying") or pos.get("ticker") or contract[:6])
        side       = str(pos.get("direction") or pos.get("side") or "CALL").upper()
        qty        = int(
            pos.get("qty")
            or pos.get("quantity")
            or pos.get("quantity_remaining")
            or 0
        )
        entry_px         = self._safe_float(pos.get("avg_fill") or pos.get("entry_price"), 0.0)
        underlying_entry = self._derive_underlying_entry_from_position(
            pos,
            underlying=underlying,
            contract=contract,
        )
        price_untrusted = bool(
            pos.get("price_untrusted")
            or str(pos.get("close_confidence") or "").upper().endswith("PRICE_UNTRUSTED")
        )

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
            underlying_entry=underlying_entry,
            price_untrusted=price_untrusted,
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
        underlying_entry: float = 0.0,
        price_untrusted: bool = False,
    ) -> None:
        _ee = getattr(self, "exit_engine", None)
        if not _ee:
            log.critical(
                "[%s] Cannot seed exit engine for imported/open position %s — exit_engine not wired",
                self.client_id, contract,
            )
            self._record_reconciler_rejection(
                signal_id=str(pos_id or f"reconciled:{contract}"),
                ticker=underlying or contract[:6],
                category_name="HEALTH",
                severity_name="CRITICAL",
                reason_code="EXIT_ENGINE_NOT_WIRED",
                human_reason="reconciler could not seed imported/open position because exit_engine is not wired",
                contract=contract,
                pos_id=pos_id,
                qty=qty,
                entry_px=entry_px,
                price_untrusted=price_untrusted,
                underlying_entry=underlying_entry,
            )
            return

        try:
            from ap_exit_engine import ManagedPosition

            stop_u             = self._safe_float(stop_underlying, 0.0)
            target_u           = self._safe_float(target_underlying, 0.0)
            underlying_entry_u = self._safe_float(underlying_entry, 0.0)
            if underlying_entry_u <= 0:
                underlying_entry_u = self._get_current_underlying_price(underlying)
            if underlying_entry_u <= 0:
                self._alert(
                    f"EXIT_ENGINE_SEED_UNDERLYING_ENTRY_UNKNOWN | {contract} | "
                    "underlying_entry remains 0.0; underlying_price_untrusted=True — MANUAL REVIEW REQUIRED"
                )

            mp = ManagedPosition(
                ticker=underlying or contract[:6],
                option_symbol=contract,
                side=side,
                quantity=int(qty),
                entry_price=float(entry_px),
                underlying_entry=float(underlying_entry_u),
                underlying_target=float(target_u),
                underlying_stop=float(stop_u),
            )
            mp.position_id              = str(pos_id or "")
            mp.client_id                = self.client_id
            mp.signal_id                = f"reconciled:{contract}"
            mp.current_option_price     = float(entry_px)
            mp.price_untrusted          = bool(price_untrusted)
            mp.underlying_entry_untrusted = bool(underlying_entry_u <= 0)
            mp.imported_by_reconciler   = True
            _ee.add_position(mp)
            log.critical(
                "[%s] EXIT_ENGINE_SEEDED_FROM_RECONCILER | %s | qty=%s entry=%.4f "
                "underlying_entry=%.4f price_untrusted=%s pos=%s",
                self.client_id, contract, qty, entry_px,
                underlying_entry_u, bool(price_untrusted), pos_id or "n/a",
            )
            self._record_position_reseeded(
                signal_id=str(pos_id or f"reconciled:{contract}"),
                ticker=underlying or contract[:6],
                reason="position_seeded_into_exit_engine_by_reconciler",
                contract=contract,
                pos_id=pos_id,
                qty=qty,
                side=side,
                entry_px=entry_px,
                underlying_entry=underlying_entry_u,
                price_untrusted=price_untrusted,
                exit_engine_seeded=True,
                quote_monitor_attached=False,
                client_id=self.client_id,
            )
            self._heartbeat(
                "exit_engine_seed",
                contract=contract,
                position_id=str(pos_id or ""),
                qty=qty,
                entry_px=entry_px,
                underlying_entry=underlying_entry_u,
                price_untrusted=bool(price_untrusted),
            )
        except Exception as e:
            log.error("[%s] Failed to seed exit engine for %s: %s",
                      self.client_id, contract, e)
            self._record_reconciler_rejection(
                signal_id=str(pos_id or f"reconciled:{contract}"),
                ticker=underlying or contract[:6],
                category_name="EXECUTION",
                severity_name="CRITICAL",
                reason_code="EXIT_ENGINE_SEED_FAILED",
                human_reason=f"failed to seed exit engine from reconciler: {e}",
                contract=contract,
                pos_id=pos_id,
                qty=qty,
                side=side,
                entry_px=entry_px,
                underlying_entry=underlying_entry,
                price_untrusted=price_untrusted,
            )
            self._report_health_error(f"exit_engine_seed_failed: {e}", fatal=False)

    def _broker_position_mark_price(self, bp: dict) -> float:
        """Best-effort option mark/last extraction when cost basis is missing."""
        for key in (
            "mark", "mark_price", "last", "last_price",
            "close", "current_price", "market_value_price",
        ):
            val = self._safe_float(bp.get(key), 0.0)
            if val > 0:
                return val

        bid = self._safe_float(bp.get("bid"), 0.0)
        ask = self._safe_float(bp.get("ask"), 0.0)
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2.0, 4)
        return 0.0

    def _get_current_option_price(self, contract: str) -> float:
        """Best-effort contract mark/last from broker quote APIs, if available."""
        contract = self._norm_contract(contract)
        if not contract:
            return 0.0

        quote = None
        for method_name, args in (
            ("get_option_quote", (contract,)),
            ("get_quote",        (contract,)),
            ("quote",            (contract,)),
        ):
            method = getattr(self.broker, method_name, None)
            if not callable(method):
                continue
            try:
                quote = method(*args)
                if quote:
                    break
            except Exception:
                continue

        if isinstance(quote, list) and quote:
            quote = quote[0]
        if not isinstance(quote, dict):
            return 0.0

        for key in ("mark", "last", "last_price", "price", "close", "mid"):
            val = self._safe_float(quote.get(key), 0.0)
            if val > 0:
                return val
        bid = self._safe_float(quote.get("bid"), 0.0)
        ask = self._safe_float(quote.get("ask"), 0.0)
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2.0, 4)
        return 0.0

    def _get_current_underlying_price(self, underlying: str) -> float:
        """Best-effort underlying mark/last from broker quote APIs, if available."""
        underlying = self._norm_underlying(underlying)
        if not underlying:
            return 0.0

        quote = None
        for method_name in ("get_quote", "quote", "get_stock_quote"):
            method = getattr(self.broker, method_name, None)
            if not callable(method):
                continue
            try:
                quote = method(underlying)
                if quote:
                    break
            except Exception:
                continue

        if isinstance(quote, list) and quote:
            quote = quote[0]
        if not isinstance(quote, dict):
            return 0.0

        for key in ("last", "last_price", "price", "mark", "close", "mid"):
            val = self._safe_float(quote.get(key), 0.0)
            if val > 0:
                return val
        bid = self._safe_float(quote.get("bid"), 0.0)
        ask = self._safe_float(quote.get("ask"), 0.0)
        if bid > 0 and ask > 0:
            return round((bid + ask) / 2.0, 4)
        return 0.0

    def _derive_underlying_entry_from_broker_position(
        self,
        bp: dict,
        *,
        underlying: str,
        contract: str,
    ) -> float:
        """
        Imported positions should not seed the exit engine with underlying_entry=0
        if the broker or quote path exposes anything better.
        """
        for key in (
            "underlying_entry", "underlying_entry_price", "entry_underlying",
            "underlying_price_at_entry", "underlying_price", "underlier_price",
            "root_price", "underlying_last", "current_underlying",
        ):
            val = self._safe_float(bp.get(key), 0.0)
            if val > 0:
                return val

        # Last resort: current underlying is safer than hardcoded zero, but still
        # treated as approximate by the exit engine via underlying_entry_untrusted.
        return self._get_current_underlying_price(underlying or contract[:6])

    def _derive_underlying_entry_from_position(
        self,
        pos: dict,
        *,
        underlying: str,
        contract: str,
    ) -> float:
        """Use DB metadata first when reseeding an existing open position."""
        for key in (
            "underlying_entry", "entry_underlying", "trigger_price",
            "underlying_entry_price", "entry_underlying_price", "opened_underlying",
        ):
            val = self._safe_float(pos.get(key), 0.0)
            if val > 0:
                return val

        # Do NOT infer entry from stop/target; that corrupts progress math.
        return self._get_current_underlying_price(underlying or contract[:6])

    def _active_exit_order_exists(
        self,
        *,
        position_id: str,
        failed_exit_order_id: str | None = None,
    ) -> Optional[dict]:
        """
        Return a newer/other active exit order for this position, if present.
        This prevents the reconciler from reopening a position while a replacement
        exit is already working.
        """
        if not position_id:
            return None
        try:
            from ap.db import conn, run_with_retry

            active_statuses = (
                "CREATED",
                "SUBMITTED",
                "ACKNOWLEDGED",
                "PARTIAL_FILL",
                "EXIT_REQUESTED",
                "EXIT_SUBMITTED",
                "EXIT_ACKNOWLEDGED",
                "EXIT_PARTIAL_FILL",
                "PENDING_CANCEL",
            )

            def _fetch():
                with conn() as c:
                    c.execute(
                        """
                        SELECT local_order_id, broker_order_id, status, created_ts, updated_ts
                        FROM orders
                        WHERE client_id=%s
                          AND kind='EXIT'
                          AND position_id=%s
                          AND (%s IS NULL OR local_order_id <> %s)
                          AND status = ANY(%s)
                        ORDER BY created_ts DESC NULLS LAST, updated_ts DESC NULLS LAST
                        LIMIT 1
                        """,
                        (
                            self.client_id,
                            position_id,
                            failed_exit_order_id,
                            failed_exit_order_id,
                            list(active_statuses),
                        ),
                    )
                    row = c.fetchone()
                    return dict(row) if row else None

            return run_with_retry(_fetch)
        except Exception as exc:
            log.warning(
                "[%s] Could not check active replacement exit for pos=%s: %s",
                self.client_id, position_id, exc,
            )
            # Fail safe: unknown means do not force reopen.
            return {"status": "UNKNOWN_CHECK_FAILED", "error": str(exc)}

    def _broker_open_exit_exists_for_contract(self, contract: str) -> bool:
        """Return True when broker still shows an open sell-to-close order for contract."""
        contract = self._norm_contract(contract)
        if not contract:
            return False
        for raw in self._safe_get_broker_open_orders():
            status = str(raw.get("status") or raw.get("Status") or "").lower().strip()
            if status in BROKER_TERMINAL:
                continue
            if self._broker_order_contract_from_raw(raw) != contract:
                continue
            if self._broker_order_is_exit_like(raw):
                return True
        return False

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
                    WHERE client_id=%s AND status = ANY(%s)
                    GROUP BY underlying, direction
                    HAVING COUNT(*) > 1
                    """,
                    (self.client_id, list(DB_OPEN_POSITION_STATUSES)),
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

    def _revert_position_to_open(
        self,
        position_id: str,
        contract: str,
        *,
        failed_exit_order_id: str | None = None,
    ):
        """
        Reopen a CLOSING position only when the terminal EXIT order is the only
        active exit path. This avoids "old canceled exit reopens position while a
        newer replacement exit is already working" races.
        """
        from ap.db import run_with_retry, conn

        replacement = self._active_exit_order_exists(
            position_id=str(position_id),
            failed_exit_order_id=failed_exit_order_id,
        )
        if replacement:
            self._alert(
                f"RECONCILE_SKIP_REOPEN_ACTIVE_EXIT | {contract} | pos={position_id} | "
                f"failed_exit={failed_exit_order_id or 'unknown'} | "
                f"active_exit={replacement.get('local_order_id','?')} status={replacement.get('status','?')}"
            )
            return

        try:
            def _revert():
                with conn() as c:
                    c.execute(
                        "UPDATE positions SET status='OPEN', exit_reason=NULL, "
                        "updated_ts=NOW() WHERE id=%s AND client_id=%s AND status='CLOSING'",
                        (position_id, self.client_id),
                    )
                    return getattr(c, "rowcount", 0)

            changed = run_with_retry(_revert) or 0
            if changed:
                log.warning(
                    "[%s] RECONCILE: reverted pos %s to OPEN after terminal exit order | %s",
                    self.client_id, position_id, contract,
                )
                _ee_cleared = False
                if self.exit_engine:
                    try:
                        self.exit_engine.clear_exit_in_flight(
                            position_id,
                            reason="reconciler_revert_position_to_open",
                        )
                        _ee_cleared = True
                    except Exception as ee_err:
                        log.warning(
                            "[%s] clear_exit_in_flight failed on revert for pos=%s: %s",
                            self.client_id, position_id, ee_err,
                        )
                if not _ee_cleared:
                    self._alert(
                        f"REVERT_EXIT_ENGINE_NOT_CLEARED | {contract} | pos={position_id} | "
                        "position reverted to OPEN in DB but exit engine in-flight flag may be stale. "
                        "Exit engine will not re-submit a protective exit without manual intervention."
                    )
            else:
                log.info(
                    "[%s] RECONCILE: no OPEN revert needed for pos %s | %s "
                    "(not currently CLOSING or already handled)",
                    self.client_id, position_id, contract,
                )
        except Exception as e:
            log.error("[%s] Failed to revert position %s: %s",
                      self.client_id, position_id, e)


    # ──────────────────────────────────────────────────────────────────────────
    # Observability helpers: lifecycle + health
    # ──────────────────────────────────────────────────────────────────────────

    def _owner(self):
        """Return LifecycleOwner.RECONCILER when available, else a safe string."""
        try:
            return LifecycleOwner.RECONCILER if LifecycleOwner is not None else "RECONCILER"
        except Exception:
            return "RECONCILER"

    def _enum_value(self, enum_cls, name: str, fallback: str):
        """Resolve optional enum values without hard-failing older lifecycle files."""
        try:
            if enum_cls is not None and hasattr(enum_cls, name):
                return getattr(enum_cls, name)
        except Exception:
            pass
        return fallback

    def _register_health(self) -> None:
        if HEALTH is None or Criticality is None:
            return
        try:
            HEALTH.register(self._health_name, Criticality.HIGH, stale_after_s=max(45.0, float(self._interval) * 4.0))
        except Exception as exc:
            log.debug("[%s] health register failed: %s", self.client_id, exc)

    def _heartbeat(self, event: str = "heartbeat", **metrics) -> None:
        if HEALTH is None:
            return
        try:
            clean_metrics = {"event": event}
            for k, v in metrics.items():
                if isinstance(v, (int, float, bool)):
                    clean_metrics[k] = float(v) if isinstance(v, bool) else v
                elif v is not None:
                    clean_metrics[k] = str(v)[:120]
            HEALTH.heartbeat(self._health_name, metrics=clean_metrics)
        except Exception as exc:
            log.debug("[%s] health heartbeat failed: %s", self.client_id, exc)

    def _report_health_error(self, error: str, fatal: bool = False) -> None:
        if HEALTH is None:
            return
        try:
            HEALTH.report_error(self._health_name, str(error), fatal=fatal)
        except Exception as exc:
            log.debug("[%s] health report failed: %s", self.client_id, exc)

    def _record_recovered_position(self, *, signal_id: str, ticker: str, reason: str, **meta) -> None:
        if signal_recovered_position is None:
            log.warning(
                "[%s] RECOVERED_POSITION | %s | %s | %s",
                self.client_id, ticker, signal_id, reason,
            )
            return
        try:
            signal_recovered_position(
                signal_id=str(signal_id),
                ticker=str(ticker or "?").upper(),
                owner=self._owner(),
                reason=reason,
                **meta,
            )
        except Exception as exc:
            log.error("[%s] lifecycle RECOVERED_POSITION write failed: %s", self.client_id, exc)

    def _record_position_reseeded(self, *, signal_id: str, ticker: str, reason: str, **meta) -> None:
        if signal_position_reseeded is None:
            log.warning(
                "[%s] POSITION_RESEEDED | %s | %s | %s",
                self.client_id, ticker, signal_id, reason,
            )
            return
        try:
            signal_position_reseeded(
                signal_id=str(signal_id),
                ticker=str(ticker or "?").upper(),
                owner=self._owner(),
                reason=reason,
                **meta,
            )
        except Exception as exc:
            log.error("[%s] lifecycle POSITION_RESEEDED write failed: %s", self.client_id, exc)

    def _record_reconciler_rejection(
        self,
        *,
        signal_id: str,
        ticker: str,
        category_name: str,
        severity_name: str,
        reason_code: str,
        human_reason: str,
        **meta,
    ) -> None:
        if signal_rejected is None:
            log.warning(
                "[%s] RECONCILER_REJECTION | %s | %s | %s | %s",
                self.client_id, ticker, signal_id, reason_code, human_reason,
            )
            return
        try:
            category = self._enum_value(RejectionCategory, category_name, category_name)
            severity = self._enum_value(RejectionSeverity, severity_name, severity_name)
            signal_rejected(
                signal_id=str(signal_id),
                ticker=str(ticker or "?").upper(),
                owner=self._owner(),
                category=category,
                reason_code=reason_code,
                severity=severity,
                reason=human_reason,
                client_id=self.client_id,
                **meta,
            )
        except Exception as exc:
            log.error("[%s] lifecycle rejection write failed: %s", self.client_id, exc)

    def _alert(self, msg: str):
        log.warning("[%s] RECONCILER: %s", self.client_id, msg)
        try:
            self._alert_fn(f"[reconciler:{self.client_id}] {msg}")
        except Exception:
            pass
