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
       → close only after exact filled EXIT evidence; broker-flat passes remain HOLD diagnostics
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
    reconciler = APBrokerReconciler(
        broker=broker, client_id=email, osm=osm, pm=pm,
        execution_mode="live",
    )
    reconciler.exit_engine = exit_engine
    reconciler.start()
    # or call reconciler.run_once() directly in tests
"""

from __future__ import annotations

import logging
import math
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

RECONCILE_INTERVAL_SEC = int(os.getenv("RECONCILE_INTERVAL_SEC", "60"))  # was 15s — 10 clients × 4/min = 40 calls/min excess

# Minimum seconds between two effective run_once() executions on the same instance.
# Rapid-fire callers (watchdogs, fill monitors, the reconciler loop itself) receive
# the cached last summary and skip the full broker/DB/OSM pass.
RUN_ONCE_MIN_INTERVAL_SEC = float(os.getenv("RECONCILER_RUN_ONCE_MIN_INTERVAL_SEC", "3.0"))
_VALID_EXECUTION_MODES = frozenset({"paper", "live"})

# PR fix/health-and-reconciler-startup-noise:
# Grace window after reconciler thread start during which a missing
# fill_monitor wire is NOT alerted. _start_reconciler() in client_runner.py
# fires BEFORE _start_fill_monitor() by design (other subsystems depend
# on reconciler existence first). The first reconciler cycle therefore
# sees fill_monitor=None for ~100ms until _start_fill_monitor assigns it.
# 5s default covers that gap with margin; configurable per-deployment.
FILL_MONITOR_WIRE_GRACE_SEC = float(
    os.getenv("FILL_MONITOR_WIRE_GRACE_SEC", "5.0")
)

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
# PARTIAL is added so scale-out rows are visible to the reconciler; ACTIVE kept
# for legacy rows produced by earlier schema versions.
DB_OPEN_POSITION_STATUSES = ("OPEN", "CLOSING", "PARTIAL", "ACTIVE")

# P0-PARTIAL-CLOSE: Status transitions for the reconciler auto-close path.
# A position is only CLOSED when quantity_remaining reaches 0.
# Anything with remaining qty must stay managed.
_RECONCILER_CLOSED_WITH_REMAINING_REPAIR_STATUSES = frozenset({"CLOSED", "CLOSED_REPAIR"})

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


def _row_first_value(row, key: str = "id"):
    """Extract the first/named value from a DB row regardless of row shape.

    ap.db.conn() uses RealDictCursor by default — rows are dict-like, so
    `row[0]` raises KeyError(0). Other call sites may produce tuple rows
    (raw psycopg2 default) or None on no-match.

    This helper supports all three shapes safely:
      - dict / dict-like (RealDictRow): looks up by `key` (default 'id')
      - tuple / list (legacy cursor): returns the first element
      - None / empty: returns None

    Returns the raw value (str cast is left to the caller).
    """
    if row is None:
        return None
    # dict-like: RealDictRow, dict, sqlite3.Row with mapping access, etc.
    try:
        # Prefer the named key when present (works for RealDictRow / dict)
        if key in row:  # type: ignore[operator]
            return row[key]
    except (TypeError, AttributeError):
        pass
    # tuple / list / Row-by-index
    try:
        return row[0]
    except (KeyError, IndexError, TypeError):
        return None


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

    P0-PARTIAL-CLOSE diagnostic counters (added):
      closed_positions_with_remaining_qty_count   — total CLOSED rows w/ qty_remaining>0 seen
      closed_positions_with_remaining_qty_recent  — those seen in this specific pass
      broker_positions_hidden_by_closed_status_count — broker-live but DB-CLOSED, caught by repair
      reconciler_partial_close_preserved_count    — auto-close attempts downgraded to PARTIAL
      reconciler_full_close_count                 — auto-closes that legitimately set CLOSED
    """
    return {
        "run":                 run,
        "client_id":           client_id,
        "orders_checked":      0,
        "orders_corrected":    0,
        "orders_alerted":      0,
        "orders_invalid_execution_mode": 0,
        "positions_checked":   0,
        "positions_alerted":   0,
        "positions_corrected": 0,
        "positions_imported":  0,
        "elapsed_sec":         0.0,
        "errors":              [],
        "skipped":             False,
        # ── P0-PARTIAL-CLOSE diagnostics ──────────────────────────────────────
        "closed_positions_with_remaining_qty_count":    0,
        "closed_positions_with_remaining_qty_recent":   0,
        "broker_positions_hidden_by_closed_status_count": 0,
        "reconciler_partial_close_preserved_count":     0,
        "reconciler_full_close_count":                  0,
        "reconciler_proof_repair_candidates":           0,
        "reconciler_proof_repair_persisted":             0,
        "reconciler_proof_repair_already_exists":        0,
        "reconciler_proof_repair_failures":              0,
    }


def _normalize_execution_mode(value: object) -> str | None:
    mode = str(value or "").strip().lower()
    return mode if mode in _VALID_EXECUTION_MODES else None


def _parse_reconciler_timestamp(value: object) -> Optional[datetime]:
    """Parse a durable timestamp without accepting malformed/naive truth."""
    if value is None or value == "":
        return None
    try:
        if isinstance(value, datetime):
            parsed = value
        else:
            parsed = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc)
    except (TypeError, ValueError, OverflowError):
        return None


def _strict_positive_finite_float(value: object) -> float | None:
    """Return a positive finite scalar without allowing bool coercion."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _strict_positive_whole_number(value: object) -> int | None:
    """Return a positive whole-number scalar without allowing bool coercion."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed <= 0 or not parsed.is_integer():
        return None
    return int(parsed)


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
        execution_mode: str | None = None,
        supabase_client=None,       # Requirement 1: optional, backward-compatible
    ):
        self.broker          = broker
        self.client_id       = client_id
        self.osm             = osm
        self.pm              = pm
        self._alert_fn       = alert_fn or (lambda msg: log.warning(msg))
        self._interval       = interval_sec
        self.execution_mode  = _normalize_execution_mode(execution_mode)
        self.supabase_client = supabase_client  # Requirement 2: stored for proof logging
        self._stop       = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._run_count  = 0
        self.exit_engine = None      # wired by client_runner after construction
        self.master_control = None   # wired by client_runner — P0-3 tick self-check
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

    def _record_unknown_execution_mode(self, summary: dict, ref: str) -> None:
        summary.setdefault("errors", []).append("reconciler_unknown_execution_mode")
        summary["orders_alerted"] = int(summary.get("orders_alerted", 0)) + 1
        log.error("[%s] RECONCILER_BLOCKED unknown execution_mode ref=%s", self.client_id, ref)

    def _write_order_last_error(self, local_id: str, reason: str) -> None:
        if not local_id:
            return
        try:
            from ap.db import conn, run_with_retry

            def _upd():
                with conn() as c:
                    c.execute(
                        """
                        UPDATE orders
                        SET last_error=%s, updated_ts=NOW()
                        WHERE client_id=%s AND local_order_id=%s
                        """,
                        (reason, self.client_id, local_id),
                    )

            run_with_retry(_upd)
        except Exception as exc:
            log.debug("[%s] reconciler last_error update failed for %s: %s", self.client_id, local_id, exc)

    # ──────────────────────────────────────────────────────────────────────────
    # Lifecycle
    # ──────────────────────────────────────────────────────────────────────────

    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        # PR fix/health-and-reconciler-startup-noise:
        # Record start timestamp so _verify_fill_monitor_or_alert can apply
        # a grace window during which a not-yet-wired fill_monitor does
        # NOT emit FILL_MONITOR_NOT_CONFIRMED.
        self._started_ts = time.time()
        # Per-instance copy of the module-level grace constant so tests can
        # override without touching env state.
        self._fill_monitor_wire_grace_sec = FILL_MONITOR_WIRE_GRACE_SEC
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name=f"reconciler-{self.client_id}",
        )
        self._thread.start()
        self._verify_fill_monitor_or_alert()
        self._heartbeat("started", thread_alive=True)
        # P0: on startup, link any FILLED orders that have position_id=null
        try:
            self._backfill_missing_position_links()
        except Exception as _bf_err:
            log.error("[%s] startup backfill_missing_position_links error: %s",
                      self.client_id, _bf_err)
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

            # ── P0-3 tick-level safety: daily-loss self-check ─────────────────
            # Belt-and-suspenders with exit-engine tick check. If exit engine
            # is degraded but reconciler is alive, this still catches a daily-
            # loss breach mid-session. Idempotent: only fires once per session.
            _mc_rec = getattr(self, "master_control", None)
            if _mc_rec is not None:
                try:
                    breached, _bsnap = _mc_rec.check_daily_loss_breach()
                    if breached and not _mc_rec.is_force_close_requested():
                        pnl = float(_bsnap.get("realized_pnl_today", 0.0)) if isinstance(_bsnap, dict) else 0.0
                        limit = getattr(_mc_rec, "max_daily_loss", -0.0)
                        _mc_rec.request_force_close_all(
                            reason=(
                                f"daily_loss_limit_reconciler_detected "
                                f"${pnl:.2f} <= ${limit:.2f}"
                            )
                        )
                        log.critical(
                            "[%s] RECONCILER_DAILY_LOSS_BREACH_DETECTED | "
                            "pnl=$%.2f limit=$%.2f | force-close requested",
                            self.client_id, pnl, limit,
                        )
                except Exception as _e:
                    log.warning("reconciler_daily_loss_self_check_failed: %s", _e)

            try:
                self._reconcile_orders(summary)
            except Exception as e:
                log.error("[%s] Order reconcile error: %s", self.client_id, e, exc_info=True)
                summary["errors"].append(f"orders: {e}")
                self._report_health_error(f"orders_reconcile_error: {e}", fatal=False)

            # Check for stale acknowledged exits before position heal.
            # An acknowledged exit with filled_qty=0 sitting too long means
            # broker truth has not been reflected — resolve or alert.
            try:
                self._handle_stale_acknowledged_exits(summary)
            except Exception as e:
                log.error("[%s] Stale ack exit handler error: %s", self.client_id, e, exc_info=True)
                summary["errors"].append(f"stale_ack_exits: {e}")

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

            # P0-PARTIAL-CLOSE: repair any CLOSED rows that still have
            # quantity_remaining > 0.  Runs after _reconcile_positions so the
            # normal ghost-close path has already run; this pass catches any
            # rows that were incorrectly marked CLOSED before this fix was
            # deployed, as well as future regressions.
            try:
                self._repair_closed_positions_with_remaining_qty(summary)
            except Exception as e:
                log.error(
                    "[%s] Partial-close repair error: %s", self.client_id, e, exc_info=True
                )
                summary["errors"].append(f"partial_close_repair: {e}")

            # P0-428: repair the only durable close state that can be left
            # without proof when the process dies after the positions commit.
            # This pass is proof-only; it never closes/reopens positions or
            # changes order lifecycle state.
            try:
                self._repair_missing_reconciler_proofs(summary)
            except Exception as e:
                log.error(
                    "[%s] Missing reconciler proof repair error: %s",
                    self.client_id,
                    e,
                    exc_info=True,
                )
                summary["errors"].append(f"reconciler_proof_repair: {e}")

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
        """Confirm the dedicated fill monitor is wired/alive when exposed.

        PR fix/health-and-reconciler-startup-noise:
        Apply a grace window after start() so the spurious cold-start alert
        (reconciler thread spins up ~100ms before _start_fill_monitor assigns
        the fill_monitor attribute) is suppressed. After the grace expires
        the original behavior is restored: missing/dead fill_monitor alerts,
        and require_fill_monitor=True raises RuntimeError.
        """
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
            # Grace window check: if reconciler has been running for less
            # than _fill_monitor_wire_grace_sec, treat as "not yet wired,
            # no action". Only applies when _started_ts is set (i.e. came
            # through start()); otherwise fall through to original behavior.
            started_ts = getattr(self, "_started_ts", None)
            grace_sec  = getattr(self, "_fill_monitor_wire_grace_sec",
                                 FILL_MONITOR_WIRE_GRACE_SEC)
            if started_ts is not None:
                elapsed = time.time() - float(started_ts)
                if elapsed < float(grace_sec):
                    # Within grace — DO NOT alert and DO NOT raise even if
                    # require_fill_monitor is True. The cold-start race must
                    # resolve within grace_sec, or the next reconciler cycle
                    # (interval_sec later) will hit the post-grace branch
                    # and alert/raise as before.
                    return True
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
        # Only use confirmed execution-fill keys. "price" and "avg_price" are
        # NOT used — "price" is Tradier's limit/stop price, not the fill price.
        # Using limit price as fill price produces false P&L and silently wrong
        # proof records. Only avg_fill_price / fill_price / filled_avg_price are
        # real execution prices from Tradier's order response.
        for key in (
            "avg_fill_price",
            "average_fill_price",
            "fill_price",
            "filled_avg_price",
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

    def _handle_stale_acknowledged_exits(self, summary: dict) -> None:
        """
        Find EXIT orders stuck in EXIT_SUBMITTED / EXIT_ACKNOWLEDGED with zero fill
        and verify broker truth. Acts on what broker reports:
          - broker filled   → advance OSM to EXIT_FILLED
          - broker terminal → advance OSM to CANCELED/REJECTED/EXPIRED
          - broker pending  → alert for manual review / cancel-replace

        Prevents profitable exits from sitting silently while price moves away.
        Threshold controlled by EXIT_ACK_STALE_SEC env var (default 20s).
        """
        try:
            import os as _os
            from ap.db import conn, run_with_retry
            max_age_sec = int(_os.getenv("EXIT_ACK_STALE_SEC", "20"))

            def _fn():
                with conn() as c:
                    c.execute(
                        """
                        SELECT
                            local_order_id, broker_order_id, position_id,
                            contract, symbol, status, qty, filled_qty,
                            submitted_ts, updated_ts,
                            EXTRACT(EPOCH FROM (
                                NOW() - submitted_ts
                            )) AS age_sec
                        FROM orders
                        WHERE client_id = %s
                          AND kind = 'EXIT'
                          AND status IN ('EXIT_SUBMITTED', 'EXIT_ACKNOWLEDGED')
                          AND COALESCE(filled_qty, 0) = 0
                          AND broker_order_id IS NOT NULL
                          AND broker_order_id <> ''
                          AND submitted_ts IS NOT NULL
                          AND EXTRACT(EPOCH FROM (
                              NOW() - submitted_ts
                          )) >= %s
                          AND EXTRACT(EPOCH FROM (NOW() - submitted_ts)) < 3600
                        ORDER BY submitted_ts ASC NULLS LAST
                        LIMIT 25
                        """,
                        (self.client_id, max_age_sec),
                    )
                    return c.fetchall()

            rows = run_with_retry(_fn) or []
            if not rows:
                return

            for row in rows:
                row = dict(row)
                local_id  = str(row.get("local_order_id")  or "")
                broker_id = str(row.get("broker_order_id") or "")
                contract  = str(row.get("contract") or row.get("symbol") or "")
                age_sec   = float(row.get("age_sec") or 0)
                db_status = str(row.get("status") or "")

                broker_raw = {}
                try:
                    broker_raw = self.broker.get_order(broker_id) or {}
                except Exception as exc:
                    log.warning(
                        "[%s] stale_ack_exit broker.get_order failed | "
                        "order=%s broker=%s age=%.1fs err=%s",
                        self.client_id, local_id, broker_id, age_sec, exc,
                    )
                    continue

                broker_status = str(
                    broker_raw.get("status") or broker_raw.get("Status") or ""
                ).lower().strip()

                fill_qty = self._extract_explicit_cumulative_fill_qty(broker_raw)
                fill_px  = self._extract_avg_fill_price(broker_raw)

                if broker_status in BROKER_FILLED and fill_qty and fill_px:
                    family     = self._order_family_from_kind_and_status(row, db_status)
                    new_status = "EXIT_FILLED" if family == "EXIT" else "FILLED"
                    try:
                        self.osm.transition(
                            local_id, new_status,
                            broker_order_id=broker_id,
                            filled_qty=int(fill_qty),
                            fill_price=float(fill_px),
                            last_error="stale_ack_exit_broker_filled",
                        )
                        summary["orders_corrected"] = int(summary.get("orders_corrected", 0)) + 1
                        log.warning(
                            "[%s] STALE_EXIT_RESOLVED FILLED | %s | local=%s broker=%s age=%.1fs",
                            self.client_id, contract, local_id, broker_id, age_sec,
                        )
                    except Exception as exc:
                        log.error("[%s] stale_ack_exit OSM transition failed: %s", self.client_id, exc)
                    continue

                if broker_status in BROKER_TERMINAL:
                    mapped = BROKER_TO_OSM.get(broker_status, "CANCELED")
                    try:
                        self.osm.transition(
                            local_id, mapped,
                            broker_order_id=broker_id,
                            last_error=f"stale_ack_exit_broker_terminal:{broker_status}",
                        )
                        summary["orders_corrected"] = int(summary.get("orders_corrected", 0)) + 1
                        log.warning(
                            "[%s] STALE_EXIT_RESOLVED TERMINAL | %s | local=%s broker=%s "
                            "broker_status=%s age=%.1fs",
                            self.client_id, contract, local_id, broker_id, broker_status, age_sec,
                        )
                    except Exception as exc:
                        log.error("[%s] stale_ack_exit OSM terminal transition failed: %s", self.client_id, exc)
                    continue

                # Still open at broker — auto-cancel if flag enabled, otherwise alert
                _auto_cancel = (
                    _os.getenv("EXIT_AUTO_CANCEL_STALE_ACK", "0").strip().lower()
                    in {"1", "true", "yes", "on"}
                )

                if not _auto_cancel:
                    self._alert(
                        f"STALE_EXIT_ACKNOWLEDGED | {contract} | local={local_id} "
                        f"broker={broker_id} broker_status={broker_status} age={age_sec:.0f}s "
                        f"| needs cancel/replace or manual review"
                    )
                    summary["orders_alerted"] = int(summary.get("orders_alerted", 0)) + 1
                    log.warning(
                        "[%s] STALE_EXIT_ACKNOWLEDGED unresolved | %s | local=%s broker=%s "
                        "broker_status=%s age=%.1fs | EXIT_AUTO_CANCEL_STALE_ACK=0 (alert only)",
                        self.client_id, contract, local_id, broker_id, broker_status, age_sec,
                    )
                    continue

                # Auto-cancel path — only runs when EXIT_AUTO_CANCEL_STALE_ACK=1
                cancel_fn = getattr(self.broker, "cancel_order", None)
                if not callable(cancel_fn):
                    self._alert(
                        f"STALE_EXIT_CANCEL_UNAVAILABLE | {contract} | local={local_id} "
                        f"broker={broker_id} | broker.cancel_order not found"
                    )
                    summary["orders_alerted"] = int(summary.get("orders_alerted", 0)) + 1
                    continue

                try:
                    cancel_res    = cancel_fn(broker_id) or {}
                    cancel_ok     = bool(cancel_res.get("ok"))
                    cancel_status = str(cancel_res.get("status") or "").lower().strip()
                except Exception as _cex:
                    log.error(
                        "[%s] stale_ack_exit cancel_order exception | local=%s broker=%s | %s",
                        self.client_id, local_id, broker_id, _cex,
                    )
                    self._alert(
                        f"STALE_EXIT_CANCEL_ERROR | {contract} | local={local_id} "
                        f"broker={broker_id} | err={_cex}"
                    )
                    summary["orders_alerted"] = int(summary.get("orders_alerted", 0)) + 1
                    continue

                if cancel_ok or cancel_status in {"canceled", "cancelled", "ok"}:
                    try:
                        self.osm.transition(
                            local_id,
                            "CANCELED",
                            broker_order_id=broker_id,
                            last_error="stale_exit_ack_auto_cancelled_for_fresh_resubmit",
                        )
                    except Exception as _tex:
                        log.error(
                            "[%s] stale_ack_exit OSM CANCELED transition failed | local=%s | %s",
                            self.client_id, local_id, _tex,
                        )

                    # Clear exit identity so exit engine can submit a fresh order next cycle
                    _pos_id = str(row.get("position_id") or "")
                    if _pos_id:
                        try:
                            _ee = getattr(self, "exit_engine", None)
                            if _ee and hasattr(_ee, "clear_pending_exit_order"):
                                _ee.clear_pending_exit_order(
                                    _pos_id,
                                    reason="stale_ack_exit_cancelled_for_fresh_resubmit",
                                    local_order_id=local_id,
                                    broker_order_id=broker_id,
                                )
                            elif _ee and hasattr(_ee, "clear_exit_in_flight"):
                                _ee.clear_exit_in_flight(
                                    _pos_id,
                                    local_order_id=local_id,
                                    broker_order_id=broker_id,
                                )
                        except Exception as _eex:
                            log.debug(
                                "[%s] exit engine clear after cancel failed (non-fatal): %s",
                                self.client_id, _eex,
                            )

                    summary["orders_corrected"] = int(summary.get("orders_corrected", 0)) + 1
                    self._alert(
                        f"STALE_EXIT_AUTO_CANCELED | {contract} | local={local_id} "
                        f"broker={broker_id} age={age_sec:.0f}s | fresh exit allowed next cycle"
                    )
                    log.warning(
                        "[%s] STALE_EXIT_AUTO_CANCELED | %s | local=%s broker=%s age=%.1fs",
                        self.client_id, contract, local_id, broker_id, age_sec,
                    )
                else:
                    self._alert(
                        f"STALE_EXIT_CANCEL_FAILED | {contract} | local={local_id} "
                        f"broker={broker_id} cancel_status={cancel_status} "
                        f"error={cancel_res.get('error')}"
                    )
                    summary["orders_alerted"] = int(summary.get("orders_alerted", 0)) + 1
                    log.error(
                        "[%s] STALE_EXIT_CANCEL_FAILED | %s | local=%s broker=%s status=%s",
                        self.client_id, contract, local_id, broker_id, cancel_status,
                    )

        except Exception as exc:
            log.error(
                "[%s] _handle_stale_acknowledged_exits failed: %s",
                self.client_id, exc, exc_info=True,
            )
            try:
                summary.setdefault("errors", []).append(f"stale_ack_exits: {exc}")
            except Exception as _e:
                log.warning("reconciler_stale_ack_summary_append_failed: %s", _e)

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
            from ap.db import conn, run_with_retry
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
                          AND o.filled_ts IS NOT NULL
                          AND COALESCE(p.entry_ts, p.opened_at) IS NOT NULL
                          AND o.filled_ts >= COALESCE(p.entry_ts, p.opened_at)
                          AND LOWER(TRIM(COALESCE(o.execution_mode, ''))) =
                              LOWER(TRIM(COALESCE(p.execution_mode, '')))
                          AND UPPER(TRIM(COALESCE(o.contract, ''))) =
                              UPPER(TRIM(COALESCE(p.contract, '')))
                          AND (
                              NULLIF(TRIM(COALESCE(p.pending_exit_local_order_id, '')), '') IS NOT NULL
                              OR
                              NULLIF(TRIM(COALESCE(p.pending_exit_broker_order_id, '')), '') IS NOT NULL
                          )
                          AND (
                              NULLIF(TRIM(COALESCE(p.pending_exit_local_order_id, '')), '') IS NULL
                              OR p.pending_exit_local_order_id = o.local_order_id
                          )
                          AND (
                              NULLIF(TRIM(COALESCE(p.pending_exit_broker_order_id, '')), '') IS NULL
                              OR p.pending_exit_broker_order_id = o.broker_order_id
                          )
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
            except Exception as _e:
                log.warning("reconciler_exit_fill_heal_summary_append_failed: %s", _e)

    # ──────────────────────────────────────────────────────────────────────────
    # Order reconciliation
    # ──────────────────────────────────────────────────────────────────────────

    def _reconcile_orders(self, summary: dict):
        from ap.db import (
            get_open_orders_for_reconcile,
            get_open_orders_with_invalid_execution_mode,
            run_with_retry,
        )

        if self.execution_mode is None:
            summary.setdefault("errors", []).append(
                "reconciler_expected_execution_mode_missing"
            )
            summary["orders_alerted"] = int(summary.get("orders_alerted", 0)) + 1
            log.error(
                "[%s] RECONCILER_BLOCKED expected execution_mode missing",
                self.client_id,
            )
            return

        # Invalid/NULL modes have no safe runner owner.  Audit them separately
        # from the exact-mode broker selection so they are visible without ever
        # reaching broker.get_order(), OSM.transition(), or recovery mutation.
        try:
            invalid_mode_orders = run_with_retry(
                lambda: get_open_orders_with_invalid_execution_mode(
                    client_id=self.client_id,
                )
            ) or []
        except Exception as exc:
            invalid_mode_orders = []
            summary.setdefault("errors", []).append(
                "reconciler_invalid_execution_mode_audit_failed"
            )
            summary["orders_alerted"] = int(summary.get("orders_alerted", 0)) + 1
            log.error(
                "[%s] invalid execution-mode order audit failed: %s",
                self.client_id, exc,
            )

        summary["orders_invalid_execution_mode"] = len(invalid_mode_orders)
        for invalid_order in invalid_mode_orders:
            local_id = invalid_order.get("local_order_id") or invalid_order.get("id")
            contract = invalid_order.get("contract") or invalid_order.get("symbol") or "?"
            self._write_order_last_error(
                str(local_id or ""), "reconciler_unknown_execution_mode"
            )
            self._record_unknown_execution_mode(
                summary, str(local_id or contract or "?")
            )

        open_orders = run_with_retry(
            lambda: get_open_orders_for_reconcile(
                client_id=self.client_id,
                execution_mode=self.execution_mode,
            )
        ) or []
        summary["orders_checked"] = len(open_orders)

        for order in open_orders:
            local_id   = order.get("local_order_id") or order.get("id")
            broker_oid = order.get("broker_order_id")
            db_status  = (order.get("status") or "").upper()
            contract   = order.get("contract") or order.get("symbol") or "?"

            row_mode = _normalize_execution_mode(order.get("execution_mode"))
            if row_mode is None:
                self._write_order_last_error(str(local_id or ""), "reconciler_unknown_execution_mode")
                self._record_unknown_execution_mode(summary, str(local_id or contract or "?"))
                continue
            if row_mode != self.execution_mode:
                # A valid row owned by the other execution mode is strictly
                # read-only here.  Alert locally, but never let a PAPER
                # reconciler write a LIVE row (or vice versa), even if the DB
                # selection layer regresses and leaks it into this result.
                summary.setdefault("errors", []).append(
                    "reconciler_execution_mode_mismatch"
                )
                summary["orders_alerted"] = int(summary.get("orders_alerted", 0)) + 1
                log.error(
                    "[%s] RECONCILER_BLOCKED row mode mismatch order=%s "
                    "expected=%s actual=%s",
                    self.client_id, local_id, self.execution_mode, row_mode,
                )
                continue

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
                self._advance_order_to_terminal(
                    order, broker_status, summary, broker_raw=broker_raw
                )
            elif broker_status in ("open", "pending", "partially_filled"):
                pass
            elif broker_status not in ("", "unknown", "error"):
                log.debug(
                    "[%s] Unrecognized broker status '%s' for order %s",
                    self.client_id, broker_status, local_id,
                )

            # A split-brain flag exists only because broker ownership was
            # previously ambiguous.  Once get_order(exact broker ID) returns a
            # recognized state, clear that quarantine through the OSM's
            # client/mode/ID-fenced writer.  Unknown/error payloads remain held.
            meta = order.get("meta") or {}
            if isinstance(meta, str):
                try:
                    import json as _json
                    meta = _json.loads(meta)
                except Exception:
                    meta = {}
            is_split_brain = bool(
                str(order.get("last_error") or "").startswith("SPLIT_BRAIN:")
                or (isinstance(meta, dict) and meta.get("split_brain_quarantine"))
            )
            broker_truth_known = bool(
                broker_status in BROKER_FILLED
                or broker_status in BROKER_TERMINAL
                or broker_status in {"open", "pending", "partially_filled"}
            )
            if is_split_brain and broker_truth_known:
                resolver = getattr(self.osm, "resolve_split_brain_quarantine", None)
                mode = _normalize_execution_mode(order.get("execution_mode"))
                resolved = bool(
                    callable(resolver)
                    and mode
                    and resolver(
                        str(local_id or ""),
                        broker_order_id=str(broker_oid),
                        execution_mode=mode,
                        broker_status=broker_status,
                    )
                )
                if resolved:
                    summary["split_brain_resolved"] = int(
                        summary.get("split_brain_resolved") or 0
                    ) + 1
                else:
                    log.warning(
                        "[%s] split-brain quarantine resolution failed | order=%s "
                        "broker=%s status=%s",
                        self.client_id, local_id, broker_oid, broker_status,
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
            order.get("underlying") or order.get("ticker") or self._norm_underlying(contract)
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
        recent_fill = self._get_recent_exit_fill(
            contract,
            position_id=pos_id,
            execution_mode=order.get("execution_mode") or "",
            local_order_id=local_id,
            broker_order_id=str(order.get("broker_order_id") or "").strip(),
        )
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
                position_id=str(
                    order.get("position_id") or order.get("positionId") or ""
                ).strip(),
                execution_mode=order.get("execution_mode") or "",
                local_order_id=str(local_id or "").strip(),
                broker_order_id=str(order.get("broker_order_id") or "").strip(),
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
                _rec_pos_id = self._ensure_position_for_filled_entry(
                    order, filled_qty, avg_fill, summary
                )
                if _rec_pos_id:
                    try:
                        self._link_order_to_position(
                            str(order.get("local_order_id") or ""), _rec_pos_id
                        )
                    except Exception as _le:
                        log.error(
                            "[%s] order_position_link_failed reconciler order=%s pos=%s err=%s",
                            self.client_id,
                            order.get("local_order_id"), _rec_pos_id, _le,
                        )
        except Exception as e:
            log.error("[%s] Reconcile transition error: %s", self.client_id, e)

    def _advance_order_to_terminal(
        self, order: dict, broker_status: str, summary: dict, broker_raw: dict | None = None
    ) -> None:
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
        # FIX-2: use the family resolver so NULL kind on an EXIT row still works.
        family = self._order_family_from_kind_and_status(order, db_status)

        # Map broker status to OSM status, aware of order family (ENTRY vs EXIT)
        if family == "EXIT":
            _EXIT_BROKER_TO_OSM = {
                "open":             "EXIT_ACKNOWLEDGED",
                "pending":          "EXIT_SUBMITTED",
                "partially_filled": "EXIT_PARTIAL_FILL",
                "filled":           "EXIT_FILLED",
                "canceled":         "CANCELED",
                "cancelled":        "CANCELED",
                "rejected":         "REJECTED",
                "expired":          "EXPIRED",
            }
            new_status = _EXIT_BROKER_TO_OSM.get(broker_status, "CANCELED")
        else:
            new_status = BROKER_TO_OSM.get(broker_status, "CANCELED")
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

        # P1 (2026-07-02): persist the broker's rejection reason. The
        # 2026-06-25 SMCI incident left 355+ REJECTED rows whose only
        # forensic record was the word "REJECTED" — Tradier returns
        # reason_description on rejected orders and the reconciler had the
        # full payload in hand (broker_raw at the call site) but dropped it
        # at this boundary. Every broker-side rejection was unexplainable
        # after the fact. Reason is truncated so last_error stays sane.
        _broker_reason = ""
        if isinstance(broker_raw, dict):
            _broker_reason = str(
                broker_raw.get("reason_description")
                or broker_raw.get("reason")
                or broker_raw.get("ReasonDescription")
                or ""
            ).strip()
        _last_error = f"reconciler: broker_status={broker_status}"
        if _broker_reason:
            _last_error = f"{_last_error} reason={_broker_reason[:160]}"

        log.warning(
            "[%s] RECONCILE_CORRECT: %s | %s | DB=%s broker=%s → advancing to %s%s",
            self.client_id, contract, local_id, db_status, broker_status, new_status,
            f" | broker_reason={_broker_reason[:160]}" if _broker_reason else "",
        )

        try:
            ok = self.osm.transition(
                local_id,
                new_status,
                last_error=_last_error,
            )
            if not ok:
                log.error("[%s] OSM transition failed %s → %s",
                          self.client_id, local_id, new_status)
                return

            # Best-effort structured forensics: merge the broker's terminal
            # payload reason into orders.meta so dashboards can query it
            # without parsing last_error. Never blocks the correction.
            if _broker_reason:
                try:
                    import json as _json
                    from ap.db import conn as _conn, run_with_retry as _rwr

                    _meta_patch = _json.dumps({
                        "broker_reject_reason": _broker_reason[:400],
                        "broker_terminal_status": broker_status,
                        "broker_terminal_recorded_by": "ap_reconciler",
                    })

                    def _merge_meta():
                        with _conn() as _c:
                            _c.execute(
                                """
                                UPDATE orders
                                SET meta = COALESCE(meta, '{}'::jsonb) || %s::jsonb
                                WHERE local_order_id = %s AND client_id = %s
                                """,
                                (_meta_patch, local_id, self.client_id),
                            )

                    _rwr(_merge_meta)
                except Exception as _meta_exc:
                    log.debug(
                        "[%s] broker reject reason meta merge failed (non-fatal): %s",
                        self.client_id, _meta_exc,
                    )

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
                + (f" | reason={_broker_reason[:120]}" if _broker_reason else "")
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
        order_execution_mode = _normalize_execution_mode(order.get("execution_mode"))
        reconciler_execution_mode = _normalize_execution_mode(self.execution_mode)
        if (
            order_execution_mode is None
            or reconciler_execution_mode is None
            or order_execution_mode != reconciler_execution_mode
        ):
            log.error(
                "[%s] RECONCILE_FILLED_ENTRY_POSITION_BLOCKED invalid execution_mode "
                "order=%s reconciler=%s local_order_id=%s",
                self.client_id,
                order.get("execution_mode"),
                self.execution_mode,
                order.get("local_order_id"),
            )
            summary.setdefault("errors", []).append(
                "reconciler_filled_entry_execution_mode_unproven"
            )
            summary["positions_alerted"] = int(summary.get("positions_alerted", 0)) + 1
            return None
        existing = self._find_db_position_by_contract(contract)
        if existing:
            pos_id = str(existing.get("id") or existing.get("position_id") or "")
            self._seed_exit_engine_from_position(existing)
            try:
                self._link_order_to_position(
                    str(order.get("local_order_id") or ""), pos_id
                )
            except Exception:
                pass
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
                local_order_id=order.get("local_order_id"),
                broker_order_id=order.get("broker_order_id"),
                execution_mode=order_execution_mode,
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

    def _link_order_to_position(self, local_order_id: str, position_id: str) -> None:
        """Write orders.position_id. Idempotent — only updates NULL rows."""
        if not local_order_id or not position_id:
            return
        from ap.db import conn, run_with_retry
        def _write():
            with conn() as c:
                c.execute(
                    "UPDATE orders SET position_id=%s "
                    "WHERE client_id=%s AND local_order_id=%s "
                    "AND (position_id IS NULL OR position_id='')",
                    (position_id, self.client_id, local_order_id),
                )
        run_with_retry(_write)
        log.info("[%s] order_position_link_success order=%s position=%s",
                 self.client_id, local_order_id, position_id)

    @staticmethod
    def _occ_expiry(contract: str):
        """
        Parse expiry date from OCC symbol: {root}{YYMMDD}{C|P}{strike}.
        Returns a datetime.date or None if parsing fails.
        """
        import re as _re, datetime as _dt
        m = _re.search(r'(\d{6})[CP]', contract.upper())
        if not m:
            return None
        try:
            return _dt.datetime.strptime(m.group(1), "%y%m%d").date()
        except ValueError:
            return None

    def _classify_orphan(self, o: dict, today) -> str:
        """
        Classify a FILLED orphan order into one of four buckets.

        Returns one of:
          'current_live'           execution_mode=live + unexpired contract → P0
          'historical_live'        execution_mode=live + expired contract   → historical debt
          'historical_null_mode'   execution_mode IS NULL + expired         → legacy skip
          'manual_review'          execution_mode IS NULL + unexpired       → unknown exposure
        """
        mode       = (o.get("execution_mode") or "").lower().strip()
        occ_symbol = (o.get("option_symbol")
                      or o.get("contract")
                      or o.get("symbol")
                      or "")
        expiry     = self._occ_expiry(occ_symbol)
        expired  = (expiry is not None and expiry < today)

        if mode == "live":
            return "current_live" if not expired else "historical_live"
        # execution_mode IS NULL or non-live
        return "historical_null_mode" if expired else "manual_review"

    def _backfill_missing_position_links(self) -> None:
        """
        Startup repair. Classifies each FILLED orphan ENTRY order before acting:

          current_live           → ERROR/P0, attempt create+link, increment
                                   orphan_backfill_failed_current_live on failure.
          historical_live        → downgrade severity; safe CLOSED_REPAIR only
                                   if enough fill data; summarize, do not P0 per row.
          historical_null_mode   → do not P0, do not create active position,
                                   summarize as INFO once.
          manual_review          → do not auto-P0; mark MANUAL_REVIEW_REQUIRED
                                   unless broker confirms live exposure.

        Exception messages are logged as type(e).__name__: str(e) to eliminate
        "create+link failed: 0" noise.

        Heartbeat counters:
          live_orphan_filled_orders
          historical_expired_orphan_filled_orders
          historical_null_mode_orphan_filled_orders
          historical_orphan_backfill_skipped
          manual_review_required_orphan_filled_orders
          orphan_backfill_failed_current_live
        """
        import datetime as _dt
        try:
            from ap.db import conn, run_with_retry
            today = _dt.date.today()

            def _fetch():
                with conn() as c:
                    c.execute(
                        """
                        SELECT local_order_id, broker_order_id,
                               contract,
                               contract AS option_symbol,
                               symbol,
                               filled_ts, fill_price, filled_qty, direction,
                               execution_mode
                        FROM orders
                        WHERE client_id = %s
                          AND kind = 'ENTRY'
                          AND status = 'FILLED'
                          AND (position_id IS NULL OR position_id = '')
                        ORDER BY filled_ts DESC NULLS LAST
                        LIMIT 200
                        """,
                        (self.client_id,),
                    )
                    rows = c.fetchall()
                    return [dict(r) for r in rows]

            orphans = run_with_retry(_fetch)
            if not orphans:
                return

            # ── Classify all orphans upfront ─────────────────────────────────
            buckets: dict = {
                "current_live":        [],
                "historical_live":     [],
                "historical_null_mode": [],
                "manual_review":       [],
            }
            for o in orphans:
                buckets[self._classify_orphan(o, today)].append(o)

            n_live   = len(buckets["current_live"])
            n_hist_l = len(buckets["historical_live"])
            n_hist_n = len(buckets["historical_null_mode"])
            n_mr     = len(buckets["manual_review"])

            # Single top-level summary — no per-row P0 for non-live buckets
            log.info(
                "[%s] backfill_missing_position_links: total_orphans=%d "
                "live_orphan_filled_orders=%d "
                "historical_expired_orphan_filled_orders=%d "
                "historical_null_mode_orphan_filled_orders=%d "
                "manual_review_required_orphan_filled_orders=%d",
                self.client_id, len(orphans),
                n_live, n_hist_l, n_hist_n, n_mr,
            )

            # ── Broker truth (only needed for current_live + manual_review) ───
            broker_open_syms = set()
            broker_truth_ok  = False
            if buckets["current_live"] or buckets["manual_review"]:
                try:
                    if self.broker and hasattr(self.broker, "list_positions"):
                        for bp in (self.broker.list_positions() or []):
                            sym = str(bp.get("symbol") or "").upper()
                            if sym:
                                broker_open_syms.add(sym)
                        broker_truth_ok = True
                except Exception as _bpe:
                    log.warning(
                        "[%s] EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE backfill: %s — "
                        "created positions will be OPEN unmanaged=True",
                        self.client_id, _bpe,
                    )

            # ── Process counters ─────────────────────────────────────────────
            linked = created = failed = 0
            hist_skipped                     = n_hist_l + n_hist_n
            orphan_backfill_failed_current_live = 0

            # ── 1. historical_null_mode — skip, log once ──────────────────────
            if n_hist_n:
                log.info(
                    "[%s] historical_null_mode_orphan_filled_orders=%d "
                    "historical_orphan_backfill_skipped=%d "
                    "— expired contracts, execution_mode IS NULL; "
                    "no active-position creation attempted",
                    self.client_id, n_hist_n, n_hist_n,
                )

            # ── 2. historical_live — safe CLOSED_REPAIR only if fill data ──────
            for o in buckets["historical_live"]:
                raw_contract = (
                    o.get("option_symbol")
                    or o.get("contract")
                    or o.get("symbol")
                    or ""
                )
                contract = self._norm_contract(raw_contract)
                local_id = str(o.get("local_order_id") or "")
                if not contract:
                    continue
                try:
                    pos_id = run_with_retry(
                        lambda c=contract, ts=o.get("filled_ts"): self._find_position_by_contract(c, ts)
                    )
                    if pos_id:
                        self._link_order_to_position(local_id, pos_id)
                        linked += 1
                    else:
                        qty      = int(o.get("filled_qty") or 0)
                        entry_px = float(o.get("fill_price") or 0.0)
                        if qty > 0 and entry_px > 0:
                            pos_id = self._create_closed_repair_position(
                                o, contract, "BROKER_MANUAL_CLOSE_IMPORT",
                                "HISTORICAL_LIVE_DEBT"
                            )
                            if pos_id:
                                self._link_order_to_position(local_id, pos_id)
                                created += 1
                            else:
                                log.warning(
                                    "[%s] historical_live_orphan_skipped "
                                    "order=%s contract=%s reason=insufficient_fill_data",
                                    self.client_id, local_id, contract,
                                )
                        else:
                            log.info(
                                "[%s] historical_live_orphan_skipped "
                                "order=%s contract=%s reason=no_fill_data_for_closed_repair",
                                self.client_id, local_id, contract,
                            )
                except Exception as _he:
                    log.info(
                        "[%s] historical_live_orphan_skipped "
                        "order=%s contract=%s err=%s: %s",
                        self.client_id, local_id, contract,
                        type(_he).__name__, _he,
                    )

            # ── 3. manual_review — escalate to P0 only if broker confirms ──────
            for o in buckets["manual_review"]:
                raw_contract = (
                    o.get("option_symbol")
                    or o.get("contract")
                    or o.get("symbol")
                    or ""
                )
                contract = self._norm_contract(raw_contract)
                local_id = str(o.get("local_order_id") or "")
                if not contract:
                    continue
                broker_live = broker_truth_ok and contract.upper() in broker_open_syms
                if broker_live:
                    # Broker confirms live exposure → escalate to P0 repair
                    log.error(
                        "[%s] filled_order_missing_position_p0 "
                        "order=%s contract=%s execution_mode=null "
                        "broker_confirmed_live=True — escalating to current_live repair",
                        self.client_id, local_id, contract,
                    )
                    buckets["current_live"].append(o)
                else:
                    log.warning(
                        "[%s] manual_review_required_orphan_filled_order "
                        "order=%s contract=%s execution_mode=null "
                        "broker_confirmed_live=False broker_truth_ok=%s",
                        self.client_id, local_id, contract, broker_truth_ok,
                    )

            # ── 4. current_live — full P0 repair ─────────────────────────────
            for o in buckets["current_live"]:
                raw_contract = (
                    o.get("option_symbol")
                    or o.get("contract")
                    or o.get("symbol")
                    or ""
                )
                contract = self._norm_contract(raw_contract)
                local_id  = str(o.get("local_order_id")  or "")
                broker_id = str(o.get("broker_order_id") or "")
                if not contract:
                    failed += 1
                    orphan_backfill_failed_current_live += 1
                    continue

                try:
                    pos_id = run_with_retry(
                        lambda c=contract, ts=o.get("filled_ts"): self._find_position_by_contract(c, ts)
                    )
                    if pos_id:
                        self._link_order_to_position(local_id, pos_id)
                        linked += 1
                        continue

                    underlying = self._norm_underlying(contract)
                    side       = (o.get("direction") or "CALL").upper()
                    qty        = int(o.get("filled_qty") or 0)
                    entry_px   = float(o.get("fill_price") or 0.0)
                    filled_ts  = o.get("filled_ts")

                    if broker_truth_ok:
                        broker_holds      = contract.upper() in broker_open_syms
                        repair_status     = "OPEN" if broker_holds else "CLOSED_REPAIR"
                        qty_remaining_val = qty if broker_holds else 0
                        close_src         = "REPAIR_FROM_FILLED_ORDER" if broker_holds else "BROKER_MANUAL_CLOSE_IMPORT"
                        close_confidence  = "HIGH"
                        exit_reason       = None if broker_holds else "manual_or_external_close_unpriced"
                    else:
                        broker_holds      = None
                        repair_status     = "OPEN"
                        qty_remaining_val = qty
                        close_src         = "REPAIR_FROM_FILLED_ORDER"
                        close_confidence  = "MANUAL_REVIEW"
                        exit_reason       = None

                    log.error(
                        "[%s] filled_order_missing_position_p0 "
                        "order=%s contract=%s side=%s qty=%d entry_px=%.4f "
                        "broker_holds=%s — attempting create+link",
                        self.client_id, local_id, contract,
                        side, qty, entry_px, broker_holds,
                    )

                    pos_id = None
                    if self.pm is not None and qty > 0 and entry_px > 0:
                        try:
                            pos_id = self._create_imported_position(
                                contract=contract, underlying=underlying,
                                side=side, qty=qty, entry_px=entry_px,
                                broker_position={}, underlying_entry=0.0,
                                price_untrusted=False,
                            )
                        except Exception as _cip_err:
                            log.warning(
                                "[%s] _create_imported_position failed for %s: %s: %s"
                                " — falling back to direct SQL",
                                self.client_id, contract,
                                type(_cip_err).__name__, _cip_err,
                            )

                    if not pos_id:
                        import uuid as _uuid
                        _pid = str(_uuid.uuid4())
                        def _insert(pid=_pid, c=contract, u=underlying,
                                    s=side, q=qty, qr=qty_remaining_val,
                                    px=entry_px, ts=filled_ts,
                                    b=broker_id, li=local_id,
                                    cs=close_src, cc=close_confidence):
                            with conn() as cur:
                                cur.execute(
                                    """
                                    INSERT INTO positions (
                                        id, client_id,
                                        underlying, contract, option_symbol,
                                        direction, side,
                                        qty, quantity_remaining,
                                        avg_fill, entry_price,
                                        entry_ts, created_at, updated_at,
                                        status, unmanaged,
                                        close_source, close_confidence,
                                        local_order_id, broker_order_id
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s,
                                        %s, %s, %s, %s,
                                        COALESCE(%s::timestamptz, NOW()),
                                        NOW(), NOW(),
                                        'OPEN', TRUE, %s, %s, %s, %s
                                    )
                                    ON CONFLICT (id) DO NOTHING
                                    RETURNING id
                                    """,
                                    (pid, self.client_id, u, c, c, s, s,
                                     q, qr, px, px, ts, cs, cc, li, b),
                                )
                                row = cur.fetchone()
                                # row may be RealDictRow (dict-like), tuple, or None.
                                # row[0] crashed with KeyError(0) on RealDictRow —
                                # use _row_first_value to handle every shape.
                                _row_id = _row_first_value(row, "id")
                                return str(_row_id) if _row_id else pid
                        pos_id = run_with_retry(_insert)

                    if not pos_id:
                        raise RuntimeError("position creation returned no id")

                    try:
                        def _patch(pid=pos_id, st=repair_status, qr=qty_remaining_val,
                                   cs=close_src, cc=close_confidence, er=exit_reason):
                            with conn() as cur:
                                cur.execute(
                                    """
                                    UPDATE positions
                                    SET status=%s, quantity_remaining=%s,
                                        unmanaged=TRUE, close_source=%s,
                                        close_confidence=%s, exit_reason=%s,
                                        updated_at=NOW()
                                    WHERE id=%s AND client_id=%s
                                    """,
                                    (st, qr, cs, cc, er, pid, self.client_id),
                                )
                        run_with_retry(_patch)
                    except Exception as _pe:
                        log.warning("[%s] backfill patch status failed pos=%s: %s: %s",
                                    self.client_id, pos_id, type(_pe).__name__, _pe)

                    self._link_order_to_position(local_id, pos_id)
                    created += 1
                    log.info(
                        "[%s] missing_position_created_from_filled_order "
                        "order=%s position=%s contract=%s side=%s qty=%d "
                        "entry_px=%.4f repair_status=%s broker_holds=%s",
                        self.client_id, local_id, pos_id, contract,
                        side, qty, entry_px, repair_status, broker_holds,
                    )

                except Exception as _be:
                    failed += 1
                    orphan_backfill_failed_current_live += 1
                    log.error(
                        "[%s] filled_order_missing_position_p0 "
                        "order=%s contract=%s — create+link failed: %s: %s",
                        self.client_id, local_id, contract,
                        type(_be).__name__, _be,
                    )
                    # Structured marker so the operator can quickly find every
                    # orphan that needs manual reconciliation. Carries the
                    # identifying fields per PR spec: order, client, contract,
                    # broker_order_id, execution_mode (live by construction here).
                    try:
                        log.error(
                            "[%s] MANUAL_REVIEW_REQUIRED reason=orphan_backfill_failed "
                            "order=%s client=%s contract=%s broker_order_id=%s "
                            "execution_mode=live",
                            self.client_id, local_id, self.client_id, contract,
                            o.get("broker_order_id") if isinstance(o, dict) else None,
                        )
                    except Exception:
                        # Logging must never raise; if _o is unexpectedly shaped
                        # we still recorded the primary error above.
                        pass

            log.info(
                "[%s] backfill_missing_position_links done: "
                "linked=%d created=%d failed=%d total=%d "
                "live_orphan_filled_orders=%d "
                "historical_expired_orphan_filled_orders=%d "
                "historical_null_mode_orphan_filled_orders=%d "
                "historical_orphan_backfill_skipped=%d "
                "manual_review_required_orphan_filled_orders=%d "
                "orphan_backfill_failed_current_live=%d",
                self.client_id,
                linked, created, failed, len(orphans),
                n_live, n_hist_l, n_hist_n, hist_skipped, n_mr,
                orphan_backfill_failed_current_live,
            )
        except Exception as _bfe:
            log.error("[%s] _backfill_missing_position_links error: %s: %s",
                      self.client_id, type(_bfe).__name__, _bfe)

    def _find_position_by_contract(self, contract: str, ts=None):
        """Look up existing position by contract + timestamp proximity."""
        from ap.db import conn, run_with_retry
        def _q():
            with conn() as cur:
                cur.execute(
                    """
                    SELECT id FROM positions
                    WHERE client_id = %s
                      AND (LOWER(contract) = LOWER(%s)
                           OR LOWER(option_symbol) = LOWER(%s))
                    ORDER BY ABS(EXTRACT(EPOCH FROM
                      (COALESCE(entry_ts, created_at)
                       - COALESCE(%s::timestamptz, NOW()))))
                      ASC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, contract, contract, ts),
                )
                row = cur.fetchone()
                # row may be RealDictRow (dict-like), tuple, or None.
                _row_id = _row_first_value(row, "id")
                return str(_row_id) if _row_id else None
        return run_with_retry(_q)

    def _create_closed_repair_position(self, o: dict, contract: str,
                                       close_src: str, close_confidence: str):
        """
        Create a CLOSED_REPAIR position for a historical filled order.
        Only called when fill data (qty + price) is available.
        """
        from ap.db import conn, run_with_retry
        import uuid as _uuid
        underlying = self._norm_underlying(contract)
        side       = (o.get("direction") or "CALL").upper()
        qty        = int(o.get("filled_qty") or 0)
        entry_px   = float(o.get("fill_price") or 0.0)
        filled_ts  = o.get("filled_ts")
        broker_id  = str(o.get("broker_order_id") or "")
        local_id   = str(o.get("local_order_id")  or "")
        _pid       = str(_uuid.uuid4())

        def _ins(pid=_pid):
            with conn() as cur:
                cur.execute(
                    """
                    INSERT INTO positions (
                        id, client_id, underlying, contract, option_symbol,
                        direction, side, qty, quantity_remaining,
                        avg_fill, entry_price,
                        entry_ts, created_at, updated_at,
                        status, unmanaged,
                        close_source, close_confidence, exit_reason,
                        local_order_id, broker_order_id
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s, 0,
                        %s, %s,
                        COALESCE(%s::timestamptz, NOW()),
                        NOW(), NOW(),
                        'CLOSED_REPAIR', TRUE,
                        %s, %s, 'historical_fill_import',
                        %s, %s
                    )
                    ON CONFLICT (id) DO NOTHING
                    RETURNING id
                    """,
                    (pid, self.client_id, underlying, contract, contract,
                     side, side, qty,
                     entry_px, entry_px,
                     filled_ts,
                     close_src, close_confidence,
                     local_id, broker_id),
                )
                row = cur.fetchone()
                # row may be RealDictRow (dict-like), tuple, or None.
                _row_id = _row_first_value(row, "id")
                return str(_row_id) if _row_id else pid
        return run_with_retry(_ins)

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
        """
        Normalize a ticker/underlying value.
        Handles OCC contract symbols like TSLA260515C00452500 → TSLA.
        OCC format: <TICKER><6-digit-date><C|P><8-digit-strike>
        Ticker is 1-6 alpha chars at the start.
        """
        s = str(val or "").strip().upper()
        if not s:
            return s
        # If it looks like an OCC symbol (contains digits after letters), extract ticker
        import re as _re
        m = _re.match(r'^([A-Z]{1,6})\d{6}[CP]\d+$', s)
        if m:
            return m.group(1)
        # Plain ticker — return as-is
        return s

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
        # PR: sizing-bootstrap-fix
        # Previous fallback `c_sym[:6]` produced corrupted underlyings like
        # 'MO2605' from 'MO260529P00074000', because `_norm_underlying`'s
        # OCC regex requires the FULL option-symbol pattern
        # (^[A-Z]{1,6}\d{6}[CP]\d+$) and falls through to return its input
        # unchanged when the pattern doesn't match. Pre-truncating to 6
        # chars killed that regex and stored garbage in positions.underlying,
        # hiding the position from the exit engine (queries by underlying='MO').
        #
        # Correct fallback: pass the full contract symbol to _norm_underlying,
        # which parses the OCC root ticker correctly. If the contract isn't a
        # parsable OCC symbol either, return whatever's left (still better
        # than a hard 6-char slice).
        c_sym = self._broker_position_contract(bp)
        return self._norm_underlying(
            bp.get("underlying")
            or bp.get("underlying_symbol")
            or bp.get("root_symbol")
            or bp.get("ticker")
            or c_sym
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

    def _get_recent_exit_fill(
        self,
        contract: str,
        *,
        position_id: str,
        execution_mode: str,
        local_order_id: str = "",
        broker_order_id: str = "",
    ) -> Optional[dict]:
        """Return one exact current-generation broker EXIT fill, if proven.

        Position identity alone is deliberately insufficient here: a position can
        have multiple EXIT generations over its lifetime.  Callers must provide
        the current local and/or broker EXIT identity; without one, a historical
        fill is not safe evidence for a new close or missing-ID recovery.
        """
        contract = self._norm_contract(contract)
        position_id = str(position_id or "").strip()
        mode = _normalize_execution_mode(execution_mode)
        local_order_id = str(local_order_id or "").strip()
        broker_order_id = str(broker_order_id or "").strip()
        if local_order_id.lower() in {"none", "null"}:
            local_order_id = ""
        if broker_order_id.lower() in {"none", "null"}:
            broker_order_id = ""
        if (
            not contract
            or not position_id
            or mode is None
            or not (local_order_id or broker_order_id)
        ):
            return None

        try:
            from ap.db import conn, run_with_retry

            def _fetch():
                with conn() as c:
                    identity_predicates = []
                    identity_params = []
                    if local_order_id:
                        identity_predicates.append("o.local_order_id = %s")
                        identity_params.append(local_order_id)
                    if broker_order_id:
                        identity_predicates.append("o.broker_order_id = %s")
                        identity_params.append(broker_order_id)

                    c.execute(
                        f"""
                        SELECT o.broker_order_id,
                               o.local_order_id,
                               o.position_id,
                               o.contract,
                               o.execution_mode,
                               o.status,
                               o.fill_price,
                               o.filled_qty,
                               o.filled_ts,
                               o.updated_ts,
                               COALESCE(p.entry_ts, p.opened_at) AS position_entry_ts
                        FROM   orders o
                        JOIN   positions p
                          ON   p.client_id = o.client_id
                         AND   p.id::text = o.position_id::text
                        WHERE  o.client_id = %s
                          AND  LOWER(TRIM(COALESCE(o.execution_mode, ''))) = %s
                          AND  o.kind = 'EXIT'
                          AND  o.status IN ('FILLED', 'EXIT_FILLED')
                          AND  UPPER(TRIM(COALESCE(o.contract, ''))) = %s
                          AND  o.position_id::text = %s
                          AND  {' AND '.join(identity_predicates)}
                        ORDER  BY COALESCE(o.filled_ts, o.updated_ts) DESC
                        LIMIT  2
                        """,
                        (self.client_id, mode, contract, position_id, *identity_params),
                    )
                    rows = c.fetchall() or []

                    # More than one exact candidate is unresolved economic truth;
                    # never newest-wins two fills for the same position identity.
                    if len(rows) > 1:
                        self._alert(
                            "RECONCILER_EXIT_FILL_IDENTITY_AMBIGUOUS | "
                            f"client={self.client_id} mode={mode} "
                            f"position_id={position_id} contract={contract} "
                            f"candidate_count={len(rows)}"
                        )
                        return None

                    if not rows:
                        return None

                    row = dict(rows[0])
                    row_broker_order_id = str(row.get("broker_order_id") or "").strip()
                    row_local_order_id = str(row.get("local_order_id") or "").strip()
                    row_status = str(row.get("status") or "").upper().strip()
                    filled_ts = _parse_reconciler_timestamp(row.get("filled_ts"))
                    position_entry_ts = _parse_reconciler_timestamp(
                        row.get("position_entry_ts")
                    )
                    if (
                        not row_broker_order_id
                        or row_broker_order_id.lower() in {"none", "null"}
                        or not row_local_order_id
                        or row_status not in {"FILLED", "EXIT_FILLED"}
                        or self._norm_contract(row.get("contract")) != contract
                        or str(row.get("position_id") or "").strip() != position_id
                        or _normalize_execution_mode(row.get("execution_mode")) != mode
                        or (local_order_id and row_local_order_id != local_order_id)
                        or (broker_order_id and row_broker_order_id != broker_order_id)
                        or filled_ts is None
                        or position_entry_ts is None
                        or filled_ts < position_entry_ts
                    ):
                        return None

                    fill_price = _strict_positive_finite_float(row.get("fill_price"))
                    filled_qty = _strict_positive_whole_number(row.get("filled_qty"))
                    if fill_price is None or filled_qty is None:
                        return None
                    return row

            return run_with_retry(_fetch)
        except Exception as e:
            log.debug("[%s] Exit fill lookup failed for %s: %s", self.client_id, contract, e)
            return None

    def _mark_ghost_seen(self, contract: str) -> bool:
        """Track repeated broker-flat observations for diagnostics."""
        key   = self._norm_contract(contract)
        count = int(self._ghost_tracker.get(key, 0)) + 1
        self._ghost_tracker[key] = count
        return count >= 3

    def _reconcile_positions(self, summary: dict):
        """
        Evidence-based position reconciliation plus broker-open import.

        Policy:
          1. Match DB positions by exact contract symbol first.
          2. Auto-close DB positions only with exact filled EXIT evidence; broker-flat
             observations without that evidence remain open for manual-close recovery.
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
            underlying = self._norm_underlying(pos.get("underlying") or pos.get("ticker") or self._norm_underlying(contract))
            db_qty     = int(pos.get("qty") or pos.get("quantity") or 0)
            entry_px   = float(pos.get("avg_fill") or pos.get("entry_price") or 0.0)

            position_execution_mode = _normalize_execution_mode(pos.get("execution_mode"))
            if position_execution_mode is None:
                summary.setdefault("errors", []).append("reconciler_unknown_execution_mode")
                summary["positions_alerted"] = int(summary.get("positions_alerted", 0)) + 1
                log.error("[%s] RECONCILER_POSITION_BLOCKED unknown execution_mode pos=%s contract=%s", self.client_id, pos_id, contract)
                continue
            if position_execution_mode != _normalize_execution_mode(self.execution_mode):
                summary.setdefault("errors", []).append("reconciler_execution_mode_mismatch")
                summary["positions_alerted"] = int(summary.get("positions_alerted", 0)) + 1
                log.error(
                    "[%s] RECONCILER_POSITION_BLOCKED execution_mode mismatch "
                    "pos=%s contract=%s position_mode=%s reconciler_mode=%s",
                    self.client_id,
                    pos_id,
                    contract,
                    position_execution_mode,
                    self.execution_mode,
                )
                continue

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
        position_mode = _normalize_execution_mode(pos.get("execution_mode"))
        current_exit_local_id = str(
            pos.get("pending_exit_local_order_id") or ""
        ).strip()
        current_exit_broker_id = str(
            pos.get("pending_exit_broker_order_id") or ""
        ).strip()
        if not current_exit_local_id or not current_exit_broker_id:
            self._alert(
                "RECONCILER_EXIT_FILL_IDENTITY_UNPROVEN | "
                f"client_id={self.client_id} "
                f"position_id={str(pos_id or '') or '?'} "
                f"contract={contract or '?'} "
                "reason=current_exit_local_and_broker_identity_pair_missing"
            )
            summary["positions_alerted"] += 1
            return
        exit_fill = self._get_recent_exit_fill(
            contract,
            position_id=str(pos_id or "").strip(),
            execution_mode=position_mode or "",
            local_order_id=current_exit_local_id,
            broker_order_id=current_exit_broker_id,
        )

        if exit_fill and float(exit_fill.get("fill_price") or 0) > 0:
            try:
                filled_qty_value = float(exit_fill.get("filled_qty"))
            except (TypeError, ValueError):
                filled_qty_value = 0.0

            raw_remaining = pos.get("quantity_remaining")
            if raw_remaining is None:
                raw_remaining = pos.get("qty") or db_qty
            try:
                remaining_value = float(raw_remaining)
            except (TypeError, ValueError):
                remaining_value = 0.0

            if (
                not math.isfinite(filled_qty_value)
                or not filled_qty_value.is_integer()
                or filled_qty_value <= 0
                or not math.isfinite(remaining_value)
                or not remaining_value.is_integer()
                or remaining_value <= 0
                or int(filled_qty_value) != int(remaining_value)
            ):
                self._alert(
                    "RECONCILER_EXIT_FILL_QTY_COVERAGE_UNPROVEN | "
                    f"client_id={self.client_id} "
                    f"execution_mode={position_mode or '?'} "
                    f"position_id={str(pos_id or '') or '?'} "
                    f"contract={contract or '?'} "
                    f"filled_qty={filled_qty_value:g} "
                    f"quantity_remaining={remaining_value:g} "
                    "reason=single_exit_fill_does_not_cover_unresolved_position"
                )
                summary["positions_alerted"] += 1
                return

            filled_qty = int(filled_qty_value)
            exit_px          = float(exit_fill["fill_price"])
            close_confidence = "HIGH"
            exact_exit_evidence = {
                "client_id": self.client_id,
                "broker_order_id": str(exit_fill.get("broker_order_id") or "").strip(),
                "exit_local_order_id": str(exit_fill.get("local_order_id") or "").strip(),
                "filled_ts": exit_fill.get("filled_ts"),
                "filled_qty": filled_qty,
                "fill_price": exit_px,
                "position_id": str(exit_fill.get("position_id") or "").strip(),
                "contract": self._norm_contract(exit_fill.get("contract")),
                "execution_mode": _normalize_execution_mode(
                    exit_fill.get("execution_mode")
                ),
                "status": str(exit_fill.get("status") or "").upper().strip(),
            }
            log.info(
                "[%s] RECONCILE_CLOSE_EVIDENCE | %s | local=%s broker=%s "
                "filled_ts=%s filled_qty=%d @ $%.4f",
                self.client_id,
                contract,
                exact_exit_evidence["exit_local_order_id"] or "?",
                exact_exit_evidence["broker_order_id"] or "?",
                exact_exit_evidence["filled_ts"] or "?",
                filled_qty,
                exit_px,
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
            self._mark_ghost_seen(contract)
            log.warning(
                "[%s] GHOST_PASS_%d | %s | broker has no position — "
                "exact broker EXIT fill still required",
                self.client_id, pass_count, contract,
            )
            self._alert(
                "BROKER_POSITION_MISSING_EXIT_FILL_UNPROVEN | "
                f"client_id={self.client_id} execution_mode={position_mode or '?'} "
                f"position_id={str(pos_id or '') or '?'} contract={contract or '?'} "
                f"underlying={underlying or '?'} ghost_pass={pass_count} "
                "local_active_exit=false broker_working_exit=false "
                "reason=exact_broker_exit_fill_missing"
            )
            summary["positions_alerted"] += 1
            return

        # Lifecycle visibility: ghost/autoclose is a major data-correction event.
        # Record it before the DB row is changed so a future trace can explain
        # exactly why the reconciler decided broker truth required closing DB truth.
        self._record_reconciler_rejection(
            signal_id=str(pos_id or f"ghost:{contract}"),
            ticker=self._norm_underlying(underlying or contract),
            category_name="DATA",
            severity_name="WARNING",
            reason_code="BROKER_POSITION_MISSING_EXACT_EXIT_FILL_CONFIRMED",
            human_reason=(
                "broker position missing; exact current-generation broker EXIT fill "
                "confirmed; reconciler auto-closing DB position"
            ),
            contract=contract,
            pos_id=pos_id,
            db_qty=db_qty,
            entry_px=entry_px,
            exit_px=exit_px,
            close_confidence=close_confidence,
            broker_exit_order_id=exact_exit_evidence["broker_order_id"],
            exit_local_order_id=exact_exit_evidence["exit_local_order_id"],
            broker_exit_fill_ts=exact_exit_evidence["filled_ts"],
            broker_exit_filled_qty=exact_exit_evidence["filled_qty"],
            client_id=self.client_id,
        )

        self._execute_reconciler_close(
            pos=pos,
            contract=contract,
            underlying=underlying,
            db_qty=db_qty,
            entry_px=entry_px,
            exit_px=exit_px,
            close_confidence=close_confidence,
            summary=summary,
            exact_exit_fill_qty=filled_qty,
            exact_exit_evidence=exact_exit_evidence,
            side=side if "side" in dir() else (pos.get("side") or pos.get("direction") or "CALL"),
        )

    def _execute_reconciler_close(
        self,
        *,
        pos: dict,
        contract: str,
        underlying: str,
        db_qty: int,
        entry_px: float,
        exit_px: float,
        close_confidence: str,
        summary: dict,
        exact_exit_fill_qty: int | None = None,
        exact_exit_evidence: Optional[dict] = None,
        side: str = "CALL",
    ) -> None:
        """
        P0-PARTIAL-CLOSE: Extracted auto-close DB write.
        Called by _handle_db_position_missing_at_broker after exact EXIT evidence
        is accepted. The ONLY place that writes RECONCILER_AUTO_CLOSE to positions.

        Rule: status=CLOSED iff quantity_remaining becomes 0.
              Otherwise status=PARTIAL (broker is flat but prior scale-outs exist).
        """
        pos_id = pos.get("id") or pos.get("position_id")
        evidence = dict(exact_exit_evidence or {})
        evidence_broker_order_id = str(
            evidence.get("broker_order_id") or ""
        ).strip()
        evidence_local_order_id = str(
            evidence.get("exit_local_order_id") or evidence.get("local_order_id") or ""
        ).strip()
        evidence_position_id = str(evidence.get("position_id") or "").strip()
        evidence_contract = self._norm_contract(evidence.get("contract"))
        evidence_mode = _normalize_execution_mode(evidence.get("execution_mode"))
        evidence_status = str(evidence.get("status") or "").upper().strip()
        evidence_filled_ts = _parse_reconciler_timestamp(evidence.get("filled_ts"))
        evidence_filled_qty_value = _strict_positive_whole_number(
            evidence.get("filled_qty")
        )
        evidence_fill_price = _strict_positive_finite_float(evidence.get("fill_price"))
        expected_exit_price = _strict_positive_finite_float(exit_px)
        expected_entry_price = _strict_positive_finite_float(entry_px)
        expected_mode = _normalize_execution_mode(
            pos.get("execution_mode") or self.execution_mode
        )
        position_entry_ts = _parse_reconciler_timestamp(
            pos.get("entry_ts")
            or pos.get("opened_at")
            or evidence.get("position_entry_ts")
        )
        provided_exact_qty = (
            _strict_positive_whole_number(exact_exit_fill_qty)
            if exact_exit_fill_qty is not None
            else None
        )
        evidence_valid = (
            bool(evidence)
            and bool(evidence_broker_order_id)
            and bool(evidence_local_order_id)
            and evidence_position_id == str(pos_id or "").strip()
            and evidence_contract == self._norm_contract(contract)
            and expected_mode is not None
            and evidence_mode == expected_mode
            and evidence_status in {"FILLED", "EXIT_FILLED"}
            and evidence_filled_ts is not None
            and position_entry_ts is not None
            and evidence_filled_ts >= position_entry_ts
            and evidence_filled_qty_value is not None
            and evidence_fill_price is not None
            and expected_exit_price is not None
            and expected_entry_price is not None
            and math.isclose(
                evidence_fill_price,
                expected_exit_price,
                rel_tol=0.0,
                abs_tol=1e-9,
            )
            and (
                exact_exit_fill_qty is None
                or provided_exact_qty == evidence_filled_qty_value
            )
        )
        if not evidence_valid:
            self._alert(
                "RECONCILER_EXIT_FILL_EVIDENCE_UNPROVEN | "
                f"client_id={self.client_id} position_id={str(pos_id or '') or '?'} "
                f"contract={contract or '?'} reason=invalid_or_missing_exact_exit_bundle"
            )
            summary["positions_alerted"] += 1
            return

        exact_exit_fill_qty = evidence_filled_qty_value
        exit_px = evidence_fill_price
        entry_px = expected_entry_price
        pnl_dollars = round((exit_px - entry_px) * db_qty * 100, 2)
        pnl_pct     = round(((exit_px - entry_px) / entry_px) * 100, 2) if entry_px > 0 else 0.0

        try:
            from ap.db import conn, run_with_retry as _rwr

            _now = datetime.now(timezone.utc).isoformat()

            # ── P0-PARTIAL-CLOSE SAFETY: fetch current quantity_remaining ─────
            # The reconciler only knows db_qty (original entry quantity).
            # quantity_remaining reflects any prior scale-outs.  We must
            # honour that value: if broker is flat (no position) we close the
            # remaining contracts, but we do NOT set status=CLOSED unless
            # that reduces quantity_remaining to 0.
            #
            # Rule:
            #   remaining_after_close = max(0, current_remaining - db_qty_to_close)
            #   if remaining_after_close <= 0 → status = 'CLOSED'
            #   else                          → status = 'PARTIAL'   (keep managed)
            #
            # We use db_qty as the "quantity to close" because the caller
            # confirmed broker has zero contracts for this position.

            def _close():
                with conn() as c:
                    # Re-fetch the row under lock so we use the freshest remaining qty.
                    c.execute(
                        "SELECT quantity_remaining, qty, "
                        "pending_exit_local_order_id, pending_exit_broker_order_id "
                        "FROM positions "
                        "WHERE id = %s AND client_id = %s FOR UPDATE",
                        (pos_id, self.client_id),
                    )
                    row = c.fetchone()
                    if not row:
                        return None

                    _row = dict(row)
                    locked_local_order_id = str(
                        _row.get("pending_exit_local_order_id") or ""
                    ).strip()
                    locked_broker_order_id = str(
                        _row.get("pending_exit_broker_order_id") or ""
                    ).strip()
                    if (
                        not (locked_local_order_id or locked_broker_order_id)
                        or (
                            locked_local_order_id
                            and locked_local_order_id != evidence_local_order_id
                        )
                        or (
                            locked_broker_order_id
                            and locked_broker_order_id != evidence_broker_order_id
                        )
                    ):
                        return {
                            "blocked_reason": "RECONCILER_EXIT_FILL_EVIDENCE_CHANGED",
                            "broker_order_id": evidence_broker_order_id,
                            "local_order_id": evidence_local_order_id,
                        }

                    # Re-read the exact broker EXIT row in the same transaction
                    # immediately before the position UPDATE.  Position identity
                    # alone is not enough: an older EXIT generation for the same
                    # position must never authorize this mutation.
                    c.execute(
                        """
                        SELECT broker_order_id,
                               local_order_id,
                               position_id,
                               contract,
                               execution_mode,
                               status,
                               fill_price,
                               filled_qty,
                               filled_ts
                        FROM orders
                        WHERE client_id = %s
                          AND kind = 'EXIT'
                          AND status IN ('FILLED', 'EXIT_FILLED')
                          AND position_id::text = %s
                          AND UPPER(TRIM(COALESCE(contract, ''))) = %s
                          AND LOWER(TRIM(COALESCE(execution_mode, ''))) = %s
                          AND local_order_id = %s
                          AND broker_order_id = %s
                        FOR SHARE
                        """,
                        (
                            self.client_id,
                            str(pos_id or "").strip(),
                            self._norm_contract(contract),
                            expected_mode,
                            evidence_local_order_id,
                            evidence_broker_order_id,
                        ),
                    )
                    durable_evidence_row = c.fetchone()
                    if not durable_evidence_row:
                        return {
                            "blocked_reason": "RECONCILER_EXIT_FILL_EVIDENCE_CHANGED",
                            "broker_order_id": evidence_broker_order_id,
                            "local_order_id": evidence_local_order_id,
                        }
                    durable_evidence_row = dict(durable_evidence_row)
                    durable_filled_ts = _parse_reconciler_timestamp(
                        durable_evidence_row.get("filled_ts")
                    )
                    try:
                        durable_filled_qty = float(durable_evidence_row.get("filled_qty"))
                        durable_fill_price = float(durable_evidence_row.get("fill_price"))
                    except (TypeError, ValueError):
                        durable_filled_qty = 0.0
                        durable_fill_price = 0.0
                    if (
                        str(durable_evidence_row.get("broker_order_id") or "").strip()
                        != evidence_broker_order_id
                        or str(durable_evidence_row.get("local_order_id") or "").strip()
                        != evidence_local_order_id
                        or str(durable_evidence_row.get("position_id") or "").strip()
                        != evidence_position_id
                        or self._norm_contract(durable_evidence_row.get("contract"))
                        != evidence_contract
                        or _normalize_execution_mode(
                            durable_evidence_row.get("execution_mode")
                        )
                        != evidence_mode
                        or str(durable_evidence_row.get("status") or "").upper().strip()
                        not in {"FILLED", "EXIT_FILLED"}
                        or durable_filled_ts != evidence_filled_ts
                        or not math.isfinite(durable_filled_qty)
                        or not durable_filled_qty.is_integer()
                        or int(durable_filled_qty) != exact_exit_fill_qty
                        or not math.isfinite(durable_fill_price)
                        or not math.isclose(
                            durable_fill_price,
                            evidence_fill_price,
                            rel_tol=0.0,
                            abs_tol=1e-9,
                        )
                    ):
                        return {
                            "blocked_reason": "RECONCILER_EXIT_FILL_EVIDENCE_CHANGED",
                            "broker_order_id": evidence_broker_order_id,
                            "local_order_id": evidence_local_order_id,
                        }

                    _stored_remaining = _row.get("quantity_remaining")
                    _stored_qty       = int(_row.get("qty") or db_qty or 0)

                    # Resolve current remaining; fall back to full qty if null
                    if _stored_remaining is None:
                        current_remaining = _stored_qty
                    else:
                        current_remaining = int(_stored_remaining)

                    if (
                        exact_exit_fill_qty is not None
                        and int(exact_exit_fill_qty) != current_remaining
                    ):
                        return {
                            "blocked_reason": "RECONCILER_EXIT_FILL_QTY_COVERAGE_UNPROVEN",
                            "filled_qty": int(exact_exit_fill_qty),
                            "quantity_remaining": current_remaining,
                        }

                    # How many contracts does this auto-close account for?
                    # Broker says zero — so we close whatever is remaining.
                    close_qty         = current_remaining  # all that's left, per broker
                    new_remaining     = 0                   # broker is flat

                    # ── SAFETY RULE: qty_remaining > 0 → cannot be CLOSED ────
                    if new_remaining <= 0:
                        final_status = "CLOSED"
                    else:
                        # Should not happen (close_qty = current_remaining above),
                        # but belt-and-suspenders: never hide live exposure.
                        final_status = "PARTIAL"
                        log.warning(
                            "[%s] P0-PARTIAL-CLOSE-GUARD | %s | pos=%s | "
                            "new_remaining=%d > 0 after auto-close attempt — "
                            "setting PARTIAL instead of CLOSED",
                            self.client_id, contract, pos_id, new_remaining,
                        )

                    # Recompute P&L using the contracts actually being closed
                    _pnl_closed   = round((exit_px - entry_px) * max(close_qty, 1) * 100, 2)
                    _pnl_pct      = round(((exit_px - entry_px) / entry_px) * 100, 2) if entry_px > 0 else 0.0

                    c.execute(
                        """
                        UPDATE positions
                        SET    status             = %s,
                               exit_ts            = %s,
                               exit_price         = %s,
                               realized_pnl       = %s,
                               realized_pnl_pct   = %s,
                               quantity_remaining = %s,
                               close_source       = %s,
                               close_confidence   = %s
                        WHERE  id = %s AND client_id = %s
                        """,
                        (
                            final_status,
                            _now,
                            exit_px,
                            _pnl_closed,
                            _pnl_pct,
                            new_remaining,
                            "RECONCILER_AUTO_CLOSE",
                            close_confidence,
                            pos_id,
                            self.client_id,
                        ),
                    )
                    return {
                        "final_status":   final_status,
                        "close_qty":      close_qty,
                        "new_remaining":  new_remaining,
                        "pnl_dollars":    _pnl_closed,
                        "pnl_pct":        _pnl_pct,
                    }

            _result = _rwr(_close)
        except Exception as e:
            log.error("[%s] RECONCILE close DB write failed %s: %s",
                      self.client_id, pos_id, e)
            summary["positions_alerted"] += 1
            return

        if _result is None:
            log.warning("[%s] RECONCILE close: pos %s not found during lock-fetch", self.client_id, pos_id)
            summary["positions_alerted"] += 1
            return

        if _result.get("blocked_reason"):
            self._alert(
                f"{_result.get('blocked_reason')} | "
                f"client_id={self.client_id} contract={contract or '?'} "
                f"position_id={str(pos_id or '') or '?'} "
                f"broker_order_id={_result.get('broker_order_id', evidence_broker_order_id)} "
                f"local_order_id={_result.get('local_order_id', evidence_local_order_id)} "
                "reason="
                + (
                    "position_remaining_changed_before_close"
                    if _result.get("blocked_reason")
                    == "RECONCILER_EXIT_FILL_QTY_COVERAGE_UNPROVEN"
                    else "durable_exit_evidence_changed_before_close"
                )
            )
            summary["positions_alerted"] += 1
            return

        final_status  = _result["final_status"]
        close_qty     = _result["close_qty"]
        new_remaining = _result["new_remaining"]
        pnl_dollars   = _result["pnl_dollars"]
        pnl_pct       = _result["pnl_pct"]

        # Update diagnostic counters
        if final_status == "CLOSED":
            summary["reconciler_full_close_count"] = \
                int(summary.get("reconciler_full_close_count", 0)) + 1
        else:
            summary["reconciler_partial_close_preserved_count"] = \
                int(summary.get("reconciler_partial_close_preserved_count", 0)) + 1

        # Only notify exit engine if position is truly fully closed
        _ee = getattr(self, "exit_engine", None)
        _canonical_proof_persisted = False
        if _ee and final_status == "CLOSED":
            try:
                _canonical_proof_persisted = (
                    _ee.mark_position_closed(
                        str(pos_id),
                        reason="reconciler_auto_close",
                        qty_filled=exact_exit_fill_qty,
                        fill_price=exit_px,
                        local_order_id=evidence_local_order_id,
                        broker_order_id=evidence_broker_order_id,
                        broker_exit_order_id=evidence_broker_order_id,
                        broker_exit_fill_ts=evidence_filled_ts,
                        broker_exit_filled_qty=exact_exit_fill_qty,
                        reconciled=True,
                    ) is True
                )
            except Exception as _e:
                log.warning("reconciler_mark_position_closed_failed: %s", _e)

        self._ghost_tracker.pop(contract, None)
        log.info(
            "[%s] FINALIZED TRADE | %s | pos=%s | entry=%.4f exit=%.4f "
            "closed_qty=%d remaining=%d pnl=$%.2f (%.1f%%) "
            "status=%s source=RECONCILER confidence=%s",
            self.client_id, contract, pos_id, entry_px, exit_px,
            close_qty, new_remaining, pnl_dollars, pnl_pct,
            final_status, close_confidence,
        )
        summary["positions_corrected"] += 1

        # ── Auto-log to proof_trades only on full close ───────────────────────
        # Partial reconciler closes should not generate a proof_trade because the
        # position is still open and will produce a final proof entry on full exit.
        if final_status != "CLOSED":
            log.info(
                "[%s] PARTIAL_RECONCILER_CLOSE skipping proof_trade | %s | remaining=%d",
                self.client_id, contract, new_remaining,
            )
            return

        if _canonical_proof_persisted:
            log.info(
                "[%s] RECONCILER_PROOF_CANONICAL_CALLBACK_PERSISTED contract=%s "
                "position_id=%s entry_local_order_id=%s exit_local_order_id=%s "
                "broker_exit_order_id=%s — fallback proof write skipped",
                self.client_id,
                contract,
                pos_id,
                str(pos.get("local_order_id") or pos.get("entry_local_order_id") or "").strip(),
                evidence_local_order_id,
                evidence_broker_order_id,
            )
            try:
                from ap_proof_logger import funnel as _funnel_r
                _funnel_r.inc("reconciler_corrections")
            except Exception as _funnel_err:
                log.debug(
                    "[%s] canonical reconciler proof funnel increment failed: %s",
                    self.client_id,
                    _funnel_err,
                )
            return

        # ── Auto-log to proof_trades so manual/reconciler closes appear in ledger ──
        # The helper validates exact EXIT truth and originating ENTRY identity,
        # confirms durable persistence, and is also used by restart repair.
        proof_result = self._persist_exact_reconciler_proof(
            pos=pos,
            exact_exit_evidence={
                **evidence,
                "client_id": self.client_id,
                "position_entry_ts": pos.get("entry_ts") or pos.get("opened_at"),
            },
            close_qty=close_qty,
            entry_px=entry_px,
            exit_px=exit_px,
            close_confidence=close_confidence,
        )
        if proof_result.get("success") is True:
            log.info(
                "[%s] RECONCILER_PROOF_%s contract=%s position_id=%s "
                "entry_local_order_id=%s exit_local_order_id=%s broker_exit_order_id=%s",
                self.client_id,
                proof_result.get("disposition", "CONFIRMED"),
                contract,
                pos_id,
                proof_result.get("entry_local_order_id", ""),
                evidence_local_order_id,
                evidence_broker_order_id,
            )
        else:
            summary.setdefault("proof_write_failures", 0)
            summary["proof_write_failures"] += 1
            log.error(
                "[%s] RECONCILER_PROOF_WRITE_FAILED contract=%s position_id=%s "
                "disposition=%s reason=%s (non-fatal — position close is complete)",
                self.client_id,
                contract,
                pos_id,
                proof_result.get("disposition", "PERSIST_FAILED"),
                proof_result.get("reason", "persistence_not_confirmed"),
            )
        try:
            from ap_proof_logger import funnel as _funnel_r
            _funnel_r.inc("reconciler_corrections")
        except Exception:
            pass

    def _persist_exact_reconciler_proof(
        self,
        *,
        pos: dict,
        exact_exit_evidence: dict,
        close_qty: int,
        entry_px: float,
        exit_px: float,
        close_confidence: str,
    ) -> dict:
        """Persist one exact reconciler close proof, with durable confirmation.

        This is deliberately proof-only.  It does not close a position, change an
        order, or contact the broker.  The caller supplies the exact EXIT evidence
        that already authorized the reconciler close; restart repair obtains a
        fresh copy through ``_get_recent_exit_fill`` before calling here.

        ``success`` is true only for a confirmed durable row or an exact durable
        row found by the idempotency lookup.  The logger's in-process cache and a
        successful-but-unconfirmed insert are never treated as proof success.
        """

        def _result(
            disposition: str,
            *,
            success: bool = False,
            reason: str = "",
            **extra,
        ) -> dict:
            return {
                "success": bool(success),
                "disposition": disposition,
                "reason": reason,
                **extra,
            }

        pos_id = str(pos.get("id") or pos.get("position_id") or "").strip()
        pos_client_id = str(pos.get("client_id") or "").strip()
        if not pos_id or pos_client_id != str(self.client_id or "").strip():
            return _result(
                "IDENTITY_UNPROVEN",
                reason="position_client_or_id_missing_or_mismatched",
            )

        expected_mode = _normalize_execution_mode(pos.get("execution_mode"))
        reconciler_mode = _normalize_execution_mode(self.execution_mode)
        if expected_mode is None or reconciler_mode is None or expected_mode != reconciler_mode:
            return _result(
                "EVIDENCE_UNPROVEN",
                reason="position_execution_mode_missing_or_mismatched",
            )

        contract = self._norm_contract(
            pos.get("contract") or pos.get("option_symbol") or pos.get("symbol") or ""
        )
        pending_local = str(pos.get("pending_exit_local_order_id") or "").strip()
        pending_broker = str(pos.get("pending_exit_broker_order_id") or "").strip()
        evidence = dict(exact_exit_evidence or {})
        evidence_client_id = str(evidence.get("client_id") or "").strip()
        evidence_position_id = str(evidence.get("position_id") or "").strip()
        evidence_contract = self._norm_contract(evidence.get("contract"))
        evidence_mode = _normalize_execution_mode(evidence.get("execution_mode"))
        evidence_local = str(
            evidence.get("exit_local_order_id") or evidence.get("local_order_id") or ""
        ).strip()
        evidence_broker = str(
            evidence.get("broker_order_id") or evidence.get("broker_exit_order_id") or ""
        ).strip()
        evidence_status = str(evidence.get("status") or "").upper().strip()
        evidence_filled_ts = _parse_reconciler_timestamp(
            evidence.get("filled_ts") or evidence.get("broker_exit_fill_ts")
        )
        entry_ts = _parse_reconciler_timestamp(
            pos.get("entry_ts")
            or pos.get("opened_at")
            or evidence.get("position_entry_ts")
        )
        evidence_fill_price = _strict_positive_finite_float(evidence.get("fill_price"))
        expected_exit_price = _strict_positive_finite_float(exit_px)
        evidence_filled_qty = _strict_positive_whole_number(
            evidence.get("filled_qty")
            if evidence.get("filled_qty") is not None
            else evidence.get("broker_exit_filled_qty")
        )
        expected_close_qty = _strict_positive_whole_number(close_qty)
        expected_entry_price = _strict_positive_finite_float(entry_px)

        if (
            not pending_local
            or not pending_broker
            or not contract
            or evidence_client_id != str(self.client_id or "").strip()
            or evidence_position_id != pos_id
            or evidence_contract != contract
            or evidence_mode != expected_mode
            or evidence_local != pending_local
            or evidence_broker != pending_broker
            or evidence_status not in {"FILLED", "EXIT_FILLED"}
            or evidence_filled_ts is None
            or entry_ts is None
            or evidence_filled_ts < entry_ts
            or evidence_fill_price is None
            or expected_exit_price is None
            or not math.isclose(
                evidence_fill_price,
                expected_exit_price,
                rel_tol=0.0,
                abs_tol=1e-9,
            )
            or evidence_filled_qty is None
            or expected_close_qty is None
            or evidence_filled_qty != expected_close_qty
            or expected_entry_price is None
        ):
            return _result(
                "EVIDENCE_UNPROVEN",
                reason="invalid_or_mismatched_exact_exit_bundle",
            )

        try:
            from ap.proof_taxonomy_guard import resolve_originating_entry_identity

            identity = resolve_originating_entry_identity(
                client_id=self.client_id,
                position_id=pos_id,
                supplied_local_order_id=str(
                    pos.get("local_order_id") or pos.get("entry_local_order_id") or ""
                ).strip(),
            )
        except Exception as identity_exc:
            log.error(
                "[%s] RECONCILER_PROOF_ENTRY_IDENTITY_LOOKUP_FAILED position_id=%s error=%s",
                self.client_id,
                pos_id,
                identity_exc,
            )
            identity = None

        entry_local_order_id = str(getattr(identity, "local_order_id", "") or "").strip()
        identity_position_id = str(getattr(identity, "position_id", "") or "").strip()
        identity_client_id = str(getattr(identity, "client_id", "") or "").strip()
        if (
            identity is None
            or not entry_local_order_id
            or identity_position_id != pos_id
            or identity_client_id != str(self.client_id or "").strip()
        ):
            return _result(
                "IDENTITY_UNPROVEN",
                reason="originating_entry_identity_missing_or_mismatched",
            )

        # Unknown origin mode is intentionally retained as quarantine truth.  It
        # may not be promoted from the current reconciler runtime mode.
        origin_mode = _normalize_execution_mode(getattr(identity, "execution_mode", None))
        proof_mode = origin_mode or "unknown"

        if not self.supabase_client:
            return _result("PERSIST_FAILED", reason="missing_supabase_client")

        def _exact_rows() -> list[dict]:
            response = (
                self.supabase_client
                .table("proof_trades")
                .select("id,client_email,position_id,local_order_id")
                .eq("client_email", self.client_id)
                .eq("position_id", pos_id)
                .eq("local_order_id", entry_local_order_id)
                .limit(2)
                .execute()
            )
            rows = getattr(response, "data", None) if response is not None else None
            if rows is None:
                raise RuntimeError("proof_identity_confirmation_missing_response")
            if not isinstance(rows, list):
                rows = list(rows)
            return [dict(row) for row in rows if isinstance(row, dict)]

        try:
            existing_rows = _exact_rows()
        except Exception as idempotency_exc:
            return _result(
                "PERSIST_FAILED",
                reason=f"proof_identity_lookup_failed:{idempotency_exc}",
            )

        if len(existing_rows) > 1:
            return _result(
                "PERSIST_FAILED",
                reason="duplicate_exact_proof_rows",
                entry_local_order_id=entry_local_order_id,
            )
        if existing_rows:
            return _result(
                "ALREADY_EXISTS",
                success=True,
                reason="exact_durable_proof_confirmed",
                entry_local_order_id=entry_local_order_id,
                proof_id=existing_rows[0].get("id"),
            )

        try:
            from ap_proof_logger import APProofLogger

            pnl_pct = round(
                ((evidence_fill_price - expected_entry_price) / expected_entry_price) * 100,
                2,
            )
            proof = APProofLogger(
                supabase_client=self.supabase_client,
                client_email=self.client_id,
                mode=proof_mode,
            )
            proof_result = proof.log_trade(
                ticker=self._norm_underlying(
                    pos.get("underlying") or pos.get("ticker") or contract
                ),
                pattern="",
                side=pos.get("side") or pos.get("direction") or "CALL",
                timeframe="1d",
                score=0,
                tier="A",
                context_score=0,
                setup_status="reconciler_auto_close",
                entry_trigger=expected_entry_price,
                entry_option_price=expected_entry_price,
                exit_option_price=evidence_fill_price,
                underlying_entry=0.0,
                underlying_exit=0.0,
                contracts=expected_close_qty,
                exit_reason=(
                    f"RECONCILER_AUTO_CLOSE | {close_confidence} | "
                    "broker_position_missing"
                ),
                option_pnl_pct=pnl_pct,
                underlying_pnl_pct=0.0,
                win=evidence_fill_price > expected_entry_price,
                spread_pct=0.0,
                chain_grade="",
                opened_at=entry_ts,
                closed_at=evidence_filled_ts,
                synthetic_entry=False,
                position_id=pos_id,
                local_order_id=entry_local_order_id,
                exit_local_order_id=evidence_local,
                execution_mode=proof_mode,
                broker_exit_order_id=evidence_broker,
                broker_exit_fill_ts=evidence_filled_ts,
                broker_exit_filled_qty=evidence_filled_qty,
            )
            if not (
                isinstance(proof_result, dict)
                and proof_result.get("_proof_persisted") is True
            ):
                return _result(
                    "PERSIST_FAILED",
                    reason=(
                        proof_result.get("_proof_persistence_error")
                        if isinstance(proof_result, dict)
                        else "persistence_not_confirmed"
                    )
                    or "persistence_not_confirmed",
                    entry_local_order_id=entry_local_order_id,
                )
        except Exception as proof_exc:
            log.error(
                "[%s] RECONCILER_PROOF_INSERT_FAILED position_id=%s error=%s",
                self.client_id,
                pos_id,
                proof_exc,
            )
            return _result(
                "PERSIST_FAILED",
                reason=f"proof_insert_failed:{proof_exc}",
                entry_local_order_id=entry_local_order_id,
            )

        try:
            confirmed_rows = _exact_rows()
        except Exception as confirmation_exc:
            return _result(
                "PERSIST_FAILED",
                reason=f"proof_durable_confirmation_failed:{confirmation_exc}",
                entry_local_order_id=entry_local_order_id,
            )
        if len(confirmed_rows) == 1:
            return _result(
                "PERSISTED",
                success=True,
                reason="exact_durable_proof_confirmed_after_insert",
                entry_local_order_id=entry_local_order_id,
                proof_id=confirmed_rows[0].get("id"),
            )
        if len(confirmed_rows) > 1:
            return _result(
                "PERSIST_FAILED",
                reason="duplicate_exact_proof_rows_after_insert",
                entry_local_order_id=entry_local_order_id,
            )
        return _result(
            "PERSIST_FAILED",
            reason="proof_insert_not_durably_confirmed",
            entry_local_order_id=entry_local_order_id,
        )

    def _repair_missing_reconciler_proofs(self, summary: dict) -> None:
        """Repair proof only for already-closed reconciler positions.

        The scan is intentionally bounded and has no position/order mutation
        authority.  Each candidate must survive a fresh exact EXIT lookup and
        the same strict proof helper used by the immediate close path.
        """
        for key in (
            "reconciler_proof_repair_candidates",
            "reconciler_proof_repair_persisted",
            "reconciler_proof_repair_already_exists",
            "reconciler_proof_repair_failures",
        ):
            summary.setdefault(key, 0)

        expected_mode = _normalize_execution_mode(self.execution_mode)
        if expected_mode is None:
            summary["reconciler_proof_repair_failures"] += 1
            self._alert(
                "RECONCILER_PROOF_REPAIR_HOLD | "
                f"client_id={self.client_id} reason=unknown_reconciler_execution_mode"
            )
            return

        try:
            from ap.db import conn, run_with_retry

            def _scan():
                with conn() as c:
                    c.execute(
                        """
                        SELECT p.*
                        FROM positions p
                        WHERE p.client_id = %s
                          AND UPPER(TRIM(COALESCE(p.status, ''))) = 'CLOSED'
                          AND UPPER(TRIM(COALESCE(p.close_source, ''))) =
                              'RECONCILER_AUTO_CLOSE'
                        ORDER BY p.exit_ts DESC NULLS LAST
                        LIMIT 50
                        """,
                        (self.client_id,),
                    )
                    return [dict(row) for row in (c.fetchall() or [])]

            candidates = run_with_retry(_scan) or []
        except Exception as scan_exc:
            summary["reconciler_proof_repair_failures"] += 1
            summary.setdefault("proof_write_failures", 0)
            summary["proof_write_failures"] += 1
            self._alert(
                "RECONCILER_PROOF_REPAIR_HOLD | "
                f"client_id={self.client_id} reason=closed_candidate_scan_failed "
                f"error={scan_exc}"
            )
            log.error(
                "[%s] reconciler closed proof candidate scan failed: %s",
                self.client_id,
                scan_exc,
            )
            return

        summary["reconciler_proof_repair_candidates"] += len(candidates)

        for pos in candidates:
            pos_id = str(pos.get("id") or pos.get("position_id") or "").strip()
            contract = self._norm_contract(
                pos.get("contract") or pos.get("option_symbol") or pos.get("symbol") or ""
            )
            mode = _normalize_execution_mode(pos.get("execution_mode"))
            local_id = str(pos.get("pending_exit_local_order_id") or "").strip()
            broker_id = str(pos.get("pending_exit_broker_order_id") or "").strip()

            def _hold(reason: str) -> None:
                summary["reconciler_proof_repair_failures"] += 1
                summary.setdefault("proof_write_failures", 0)
                summary["proof_write_failures"] += 1
                self._alert(
                    "RECONCILER_PROOF_REPAIR_HOLD | "
                    f"client_id={self.client_id} execution_mode={mode or '?'} "
                    f"position_id={pos_id or '?'} contract={contract or '?'} "
                    f"reason={reason}"
                )

            if (
                str(pos.get("client_id") or "").strip() != self.client_id
                or str(pos.get("status") or "").upper().strip() != "CLOSED"
                or str(pos.get("close_source") or "").upper().strip()
                != "RECONCILER_AUTO_CLOSE"
            ):
                _hold("candidate_identity_changed")
                continue
            if not pos_id or not contract:
                _hold("position_identity_missing")
                continue
            if mode is None or mode != expected_mode:
                _hold("execution_mode_missing_or_mismatched")
                continue
            if not local_id or not broker_id:
                _hold("current_exit_identity_missing")
                continue

            exit_fill = self._get_recent_exit_fill(
                contract,
                position_id=pos_id,
                execution_mode=mode,
                local_order_id=local_id,
                broker_order_id=broker_id,
            )
            if not exit_fill:
                _hold("exact_durable_exit_evidence_missing_or_ambiguous")
                continue

            exact_exit_evidence = dict(exit_fill)
            exact_exit_evidence["client_id"] = self.client_id
            exact_exit_evidence.setdefault(
                "position_entry_ts",
                pos.get("entry_ts") or pos.get("opened_at"),
            )
            close_qty = _strict_positive_whole_number(
                exact_exit_evidence.get("filled_qty")
            )
            exit_px = _strict_positive_finite_float(
                exact_exit_evidence.get("fill_price")
            )
            entry_px = _strict_positive_finite_float(
                pos.get("avg_fill") or pos.get("entry_price")
            )
            if close_qty is None or exit_px is None or entry_px is None:
                _hold("malformed_exact_exit_or_entry_economics")
                continue

            proof_result = self._persist_exact_reconciler_proof(
                pos=pos,
                exact_exit_evidence=exact_exit_evidence,
                close_qty=close_qty,
                entry_px=entry_px,
                exit_px=exit_px,
                close_confidence="HIGH",
            )
            if proof_result.get("success") is True:
                if proof_result.get("disposition") == "PERSISTED":
                    summary["reconciler_proof_repair_persisted"] += 1
                else:
                    summary["reconciler_proof_repair_already_exists"] += 1
                log.info(
                    "[%s] RECONCILER_PROOF_REPAIR_%s position_id=%s "
                    "entry_local_order_id=%s exit_local_order_id=%s broker_exit_order_id=%s",
                    self.client_id,
                    proof_result.get("disposition", "CONFIRMED"),
                    pos_id,
                    proof_result.get("entry_local_order_id", ""),
                    local_id,
                    broker_id,
                )
            else:
                _hold(
                    proof_result.get("reason")
                    or proof_result.get("disposition")
                    or "proof_persistence_not_confirmed"
                )

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

        contracts_present_before_poll = set(db_contracts)
        for bp in broker_positions:
            contract = self._broker_position_contract(bp)
            if not contract:
                continue

            qty = self._broker_position_qty(bp)
            if qty <= 0:
                continue

            from zoneinfo import ZoneInfo as _ZoneInfo
            expiration = self._occ_expiry(contract)
            session_date = datetime.now(_ZoneInfo("America/New_York")).date()
            if expiration is not None and expiration < session_date:
                # --- exact-mode lookup: LIMIT 2 to detect ambiguity --------
                expired_candidates = self._find_db_positions_for_expired_cleanup(
                    contract, self.execution_mode
                )
                existing_id: str = ""
                if len(expired_candidates) == 1:
                    # Exactly one same-mode active row — safe to close.
                    existing_id = str(expired_candidates[0].get("id") or "")
                    if existing_id and self.pm is not None:
                        try:
                            self.pm.close_expired_position(
                                position_id=existing_id,
                                reason="expired_contract_broker_import_quarantine",
                                exit_reason="expired_contract",
                                close_source="expired_contract_cleanup",
                                close_confidence="SYSTEM",
                            )
                        except Exception as exc:
                            summary.setdefault("errors", []).append(
                                "expired_broker_import_existing_close_failed"
                            )
                            log.error(
                                "[%s] expired broker import existing close failed "
                                "contract=%s position_id=%s error=%s",
                                self.client_id, contract, existing_id, exc,
                            )
                elif len(expired_candidates) >= 2:
                    # Duplicate active rows in the same mode — ambiguity detected.
                    # Closing an arbitrary row would worsen the incident family this
                    # PR repairs.  Emit a dedicated code and close nothing.
                    ambiguous_ids = [str(r.get("id") or "") for r in expired_candidates]
                    log.error(
                        "[%s] EXPIRED_BROKER_IMPORT_POSITION_AMBIGUOUS "
                        "contract=%s execution_mode=%s candidate_ids=%s",
                        self.client_id, contract, self.execution_mode, ambiguous_ids,
                    )
                    self._alert(
                        f"EXPIRED_BROKER_IMPORT_POSITION_AMBIGUOUS | "
                        f"client={self.client_id} "
                        f"execution_mode={self.execution_mode or 'unknown'} "
                        f"contract={contract} candidate_ids={ambiguous_ids}"
                    )
                    summary.setdefault("errors", []).append(
                        "expired_broker_import_position_ambiguous"
                    )
                    existing_id = ""  # no mutation when ambiguous
                # len == 0: no same-mode active row; nothing to close.
                # ---------------------------------------------------------------------------

                cost_basis = self._safe_float(bp.get("cost_basis"), 0.0)
                reason_code = "BROKER_IMPORT_EXPIRED_CONTRACT_QUARANTINED"
                summary["positions_quarantined"] = int(
                    summary.get("positions_quarantined", 0)
                ) + 1
                self._record_reconciler_rejection(
                    signal_id=f"reconciled:{contract}:expired-quarantine",
                    ticker=self._norm_underlying(
                        self._broker_position_underlying(bp) or contract
                    ),
                    category_name="DATA",
                    severity_name="WARNING",
                    reason_code=reason_code,
                    human_reason=(
                        "broker reported an unmatched contract expired before the "
                        "current Eastern session; import was quarantined"
                    ),
                    execution_mode=self.execution_mode,
                    contract=contract,
                    expiration=expiration.isoformat(),
                    broker_quantity=qty,
                    broker_cost_basis=cost_basis,
                    existing_position_id=existing_id,
                    broker_mutation="none",
                )
                self._alert(
                    f"{reason_code} | client={self.client_id} "
                    f"execution_mode={self.execution_mode or 'unknown'} "
                    f"contract={contract} expiration={expiration.isoformat()} "
                    f"broker_qty={qty} broker_cost_basis={cost_basis:.8f} "
                    f"existing_position_id={existing_id or 'none'}"
                )
                db_contracts.add(contract)
                continue

            if contract in contracts_present_before_poll:
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

            from ap.attribution_integrity import import_identity
            import_ident = import_identity(
                contract=contract,
                client_id=self.client_id,
                execution_mode=self.execution_mode or "",
                broker_position=bp,
                broker_quantity=qty,
                broker_cost_basis=self._safe_float(bp.get("cost_basis"), 0.0),
                price_untrusted=price_untrusted,
            )
            if not import_ident.identity_valid:
                summary["positions_alerted"] += 1
                self._record_reconciler_rejection(
                    signal_id=import_ident.signal_id,
                    ticker=self._norm_underlying(underlying or contract),
                    category_name="DATA",
                    severity_name="CRITICAL",
                    reason_code="BROKER_IMPORT_IDENTITY_UNPROVEN",
                    human_reason=import_ident.identity_reason,
                    execution_mode=self.execution_mode,
                    contract=contract,
                    broker_quantity=qty,
                    broker_cost_basis=self._safe_float(bp.get("cost_basis"), 0.0),
                )
                continue

            historical = self._find_db_position_by_import_identity(
                import_ident.plan_id,
                execution_mode=self.execution_mode or "",
            )
            if historical:
                if str(historical.get("status") or "").upper() in DB_OPEN_POSITION_STATUSES:
                    self._seed_exit_engine_from_position(historical)
                summary["positions_import_idempotent"] = int(
                    summary.get("positions_import_idempotent", 0)
                ) + 1
                db_contracts.add(contract)
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
                import_identity_record=import_ident,
            )

            if not pos_id:
                summary["positions_alerted"] += 1
                self._record_reconciler_rejection(
                    signal_id=f"reconciled:{contract}",
                    ticker=self._norm_underlying(underlying or contract),
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
                ticker=self._norm_underlying(underlying or contract),
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
        import_identity_record=None,
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
        # ── ATTRIBUTION INTEGRITY (P0, 2026-07-04) ──────────────────────────
        # Before fabricating identity, attempt lineage recovery: 78 of 132
        # historical BROKER_IMPORT positions had a matching ENTRY order with a
        # real signal_id for the same client+contract — the bot ordered them
        # itself and the import path threw the attribution away. Recovery is
        # read-only and fail-closed: any error yields exactly the previous
        # fabricated identity. Provenance is preserved either way (tier stays
        # RECONCILED, plan_id keeps the 'reconciled:' prefix).
        from ap.attribution_integrity import import_identity

        _ident = import_identity_record or import_identity(
            contract=contract,
            client_id=self.client_id,
            execution_mode=self.execution_mode or "",
            broker_position=broker_position,
            broker_quantity=qty,
            broker_cost_basis=self._safe_float(
                (broker_position or {}).get("cost_basis"), 0.0
            ),
            price_untrusted=price_untrusted,
        )
        if not _ident.identity_valid:
            log.error(
                "[%s] BROKER_IMPORT_IDENTITY_UNPROVEN contract=%s reason=%s",
                self.client_id, contract, _ident.identity_reason,
            )
            return None
        imported_plan_id   = _ident.plan_id
        imported_signal_id = _ident.signal_id
        imported_pattern   = _ident.pattern

        if self.pm is not None:
            try:
                pos_id = self.pm.open_position(
                    plan_id=imported_plan_id,
                    signal_id=imported_signal_id,
                    ticker=self._norm_underlying(underlying or contract),
                    contract=contract,
                    side=side,
                    qty=qty,
                    entry_price=entry_px,
                    tier="RECONCILED",
                    score=0.0,
                    pattern=imported_pattern,
                    stop_underlying=None,
                    target_underlying=None,
                    execution_mode=self.execution_mode,
                    historical_plan_idempotency=True,
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

            # ATTRIBUTION INTEGRITY: pattern + tier now included — this INSERT
            # previously omitted the pattern column entirely, producing the
            # NULL-pattern rows in the closed record (8 of 178 measured).
            def _insert_full():
                with conn() as c:
                    c.execute(
                        "SELECT pg_advisory_xact_lock(hashtextextended(%s, 0))",
                        (f"broker-import:{self.client_id}:{self.execution_mode}:{imported_plan_id}",),
                    )
                    c.execute(
                        """
                        SELECT id FROM positions
                        WHERE client_id=%s
                          AND LOWER(COALESCE(execution_mode,''))=%s
                          AND plan_id=%s
                        ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                        LIMIT 2
                        """,
                        (self.client_id, self.execution_mode, imported_plan_id),
                    )
                    existing = c.fetchall() or []
                    if len(existing) == 1:
                        return str(existing[0]["id"])
                    if len(existing) > 1:
                        raise RuntimeError("broker_import_identity_ambiguous")
                    c.execute(
                        """
                        INSERT INTO positions (
                            id, client_id, underlying, contract, direction, qty, avg_fill,
                            entry_ts, status, plan_id, signal_id, pattern, tier, close_source,
                            close_confidence, underlying_entry, execution_mode
                        ) VALUES (
                            %s,%s,%s,%s,%s,%s,%s,
                            %s,'OPEN',%s,%s,%s,'RECONCILED',%s,%s,%s,%s
                        )
                        ON CONFLICT DO NOTHING
                        RETURNING id
                        """,
                        (
                            pos_id,
                            self.client_id,
                            self._norm_underlying(underlying or contract),
                            contract,
                            side,
                            int(qty),
                            float(entry_px),
                            now_iso,
                            imported_plan_id,
                            imported_signal_id,
                            imported_pattern,
                            "RECONCILER_IMPORT",
                            "BROKER_OPEN_PRICE_UNTRUSTED" if price_untrusted else "BROKER_OPEN",
                            float(underlying_entry) if underlying_entry > 0 else None,
                            self.execution_mode,
                        ),
                    )
                    inserted = c.fetchone()
                    if not inserted:
                        raise RuntimeError("broker_import_insert_returned_no_identity")
                    return str(inserted["id"])

            try:
                return str(run_with_retry(_insert_full))
            except Exception as full_err:
                log.warning(
                    "[%s] exact-mode imported position insert failed for %s: %s — failing closed",
                    self.client_id, contract, full_err,
                )
                return None

        except Exception as sql_err:
            log.error("[%s] SQL import failed for broker position %s: %s",
                      self.client_id, contract, sql_err)
            return None

    def _backfill_position_underlying_entry(self, pos_id: str, underlying_entry: float) -> None:
        """
        Persist underlying_entry to an existing DB position row.

        FIX-3 support method retained for existing callers that need to repair
        underlying entry truth after position creation.
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
        execution_mode = _normalize_execution_mode(self.execution_mode)
        if not contract or execution_mode is None:
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
                          AND LOWER(TRIM(COALESCE(execution_mode,'')))=%s
                          AND status = ANY(%s)
                          AND UPPER(contract)=%s
                        ORDER BY entry_ts DESC NULLS LAST
                        LIMIT 1
                        """,
                        (
                            self.client_id,
                            execution_mode,
                            list(DB_OPEN_POSITION_STATUSES),
                            contract,
                        ),
                    )
                    row = c.fetchone()
                    return dict(row) if row else None

            return run_with_retry(_fetch)
        except Exception as e:
            log.debug("[%s] find DB position by contract failed for %s: %s",
                      self.client_id, contract, e)
            return None

    def _find_db_positions_for_expired_cleanup(
        self, contract: str, execution_mode: str | None
    ) -> list[dict]:
        """Exact-mode lookup used exclusively by the expired broker-import cleanup path.

        Returns at most 2 rows so the caller can distinguish:
          - 0 rows  → nothing to close; quarantine the observation only
          - 1 row   → close that exact position ID
          - 2 rows  → EXPIRED_BROKER_IMPORT_POSITION_AMBIGUOUS; close nothing

        Filters on exact client_id, normalised contract, normalised execution_mode,
        and active status set.  Never falls back to a mode-agnostic search.
        """
        contract = self._norm_contract(contract)
        normalized_mode = _normalize_execution_mode(execution_mode)
        if not contract or normalized_mode is None:
            return []
        try:
            from ap.db import conn, run_with_retry

            def _fetch() -> list[dict]:
                with conn() as c:
                    c.execute(
                        """
                        SELECT *
                        FROM positions
                        WHERE client_id=%s
                          AND LOWER(COALESCE(execution_mode,''))=%s
                          AND UPPER(contract)=%s
                          AND status = ANY(%s)
                        ORDER BY entry_ts DESC NULLS LAST
                        LIMIT 2
                        """,
                        (
                            self.client_id,
                            normalized_mode,
                            contract,
                            list(DB_OPEN_POSITION_STATUSES),
                        ),
                    )
                    rows = c.fetchall()
                    return [dict(r) for r in rows]

            return run_with_retry(_fetch)
        except Exception as e:
            log.debug(
                "[%s] _find_db_positions_for_expired_cleanup failed contract=%s mode=%s: %s",
                self.client_id, contract, execution_mode, e,
            )
            return []

    def _find_db_position_by_import_identity(
        self, plan_id: str, *, execution_mode: str
    ) -> Optional[dict]:
        """Find a synthetic import lifecycle across active and terminal rows."""
        normalized_mode = _normalize_execution_mode(execution_mode)
        if not plan_id or normalized_mode is None:
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
                          AND LOWER(COALESCE(execution_mode,''))=%s
                          AND plan_id=%s
                        ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                        LIMIT 2
                        """,
                        (self.client_id, normalized_mode, plan_id),
                    )
                    rows = c.fetchall() or []
                    if len(rows) > 1:
                        log.critical(
                            "[%s] BROKER_IMPORT_IDENTITY_AMBIGUOUS plan_id=%s rows=%d",
                            self.client_id, plan_id, len(rows),
                        )
                        return None
                    return dict(rows[0]) if rows else None

            return run_with_retry(_fetch)
        except Exception as exc:
            log.error(
                "[%s] broker import identity lookup failed plan_id=%s error=%s",
                self.client_id, plan_id, exc,
            )
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
        underlying = self._norm_underlying(pos.get("underlying") or pos.get("ticker") or self._norm_underlying(contract))
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
                ticker=self._norm_underlying(underlying or contract),
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
                ticker=self._norm_underlying(underlying or contract),
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
            mp.execution_mode           = str(self.execution_mode or "").strip().lower()
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
                ticker=self._norm_underlying(underlying or contract),
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
                ticker=self._norm_underlying(underlying or contract),
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
        return self._get_current_underlying_price(self._norm_underlying(underlying or contract))

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
        return self._get_current_underlying_price(self._norm_underlying(underlying or contract))

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
    # P0-PARTIAL-CLOSE: broker-truth repair for CLOSED rows with remaining qty
    # ──────────────────────────────────────────────────────────────────────────

    def _repair_closed_positions_with_remaining_qty(self, summary: dict) -> None:
        """
        P0-PARTIAL-CLOSE repair pass.

        Detects rows where status=CLOSED but quantity_remaining > 0 — invalid state
        that hides live broker exposure from operator views and exit engine.

        For each such row:
          1. Check broker truth.
          2. Broker still holds the contract  → restore to PARTIAL (or OPEN if never
             partially exited), update quantity_remaining from broker, log warning.
          3. Broker is flat                   → set quantity_remaining=0, keep CLOSED,
             add close_source=CLOSED_REPAIR so the row is traceable.
          4. Broker unavailable               → flag manual review, do NOT change status
             (conservative: never hide live exposure on broker API errors).

        Called by run_once() after _reconcile_positions().
        Zero-impact if no such rows exist.
        """
        try:
            from ap.db import conn, run_with_retry

            # ── Step 1: find all CLOSED rows with quantity_remaining > 0 ──────
            def _scan():
                with conn() as c:
                    c.execute(
                        """
                        SELECT id, contract, option_symbol, underlying, ticker,
                               qty, quantity_remaining, avg_fill, entry_price,
                               close_source, client_id
                        FROM   positions
                        WHERE  client_id           = %s
                          AND  UPPER(status)        = 'CLOSED'
                          AND  COALESCE(quantity_remaining, 0) > 0
                        ORDER  BY entry_ts DESC NULLS LAST
                        LIMIT  100
                        """,
                        (self.client_id,),
                    )
                    return [dict(r) for r in c.fetchall()]

            bad_rows = run_with_retry(_scan) or []
            if not bad_rows:
                return

            count = len(bad_rows)
            summary["closed_positions_with_remaining_qty_count"] = \
                int(summary.get("closed_positions_with_remaining_qty_count", 0)) + count
            summary["closed_positions_with_remaining_qty_recent"] = \
                int(summary.get("closed_positions_with_remaining_qty_recent", 0)) + count

            log.warning(
                "[%s] P0-PARTIAL-CLOSE-REPAIR | found %d CLOSED row(s) with "
                "quantity_remaining > 0 — running broker-truth check",
                self.client_id, count,
            )

            # ── Step 2: fetch broker truth once ──────────────────────────────
            broker_open_by_contract: dict[str, int] = {}
            broker_truth_available  = False
            try:
                if self.broker and hasattr(self.broker, "list_positions"):
                    bp_list = self.broker.list_positions() or []
                    for bp in bp_list:
                        sym = self._norm_contract(
                            str(bp.get("symbol") or bp.get("contract") or "")
                        )
                        qty = self._broker_position_qty(bp)
                        if sym and qty > 0:
                            broker_open_by_contract[sym] = qty
                    broker_truth_available = True
            except Exception as _bpe:
                log.warning(
                    "[%s] P0-PARTIAL-CLOSE-REPAIR broker fetch failed: %s — "
                    "flagging rows for manual review without changing status",
                    self.client_id, _bpe,
                )

            # ── Step 3: per-row repair ────────────────────────────────────────
            for row in bad_rows:
                pos_id     = row.get("id")
                contract   = self._norm_contract(
                    row.get("contract") or row.get("option_symbol") or ""
                )
                rem_qty    = int(row.get("quantity_remaining") or 0)
                full_qty   = int(row.get("qty") or rem_qty)

                if not broker_truth_available:
                    # Cannot verify — flag for operator, do not touch status
                    log.error(
                        "[%s] P0-PARTIAL-CLOSE-REPAIR MANUAL-REVIEW-REQUIRED | "
                        "pos=%s contract=%s quantity_remaining=%d | "
                        "broker truth unavailable; status left as CLOSED to avoid "
                        "re-opening a genuinely closed position",
                        self.client_id, pos_id, contract, rem_qty,
                    )
                    summary["broker_positions_hidden_by_closed_status_count"] = \
                        int(summary.get("broker_positions_hidden_by_closed_status_count", 0)) + 1
                    continue

                broker_qty = broker_open_by_contract.get(contract, 0)

                if broker_qty > 0:
                    # Broker still holds this contract — restore to managed status
                    restore_status = "PARTIAL" if full_qty > rem_qty else "OPEN"
                    restore_remaining = min(broker_qty, rem_qty)  # trust broker qty

                    def _restore(pid=pos_id, st=restore_status, rq=restore_remaining):
                        with conn() as c:
                            c.execute(
                                """
                                UPDATE positions
                                SET    status             = %s,
                                       quantity_remaining = %s,
                                       close_source       = 'PARTIAL_CLOSE_REPAIR',
                                       updated_at         = NOW()
                                WHERE  id         = %s
                                  AND  client_id  = %s
                                  AND  UPPER(status) = 'CLOSED'
                                """,
                                (st, rq, pid, self.client_id),
                            )
                            return c.rowcount

                    updated = run_with_retry(_restore)
                    if updated:
                        summary["broker_positions_hidden_by_closed_status_count"] = \
                            int(summary.get("broker_positions_hidden_by_closed_status_count", 0)) + 1
                        log.warning(
                            "[%s] P0-PARTIAL-CLOSE-REPAIR RESTORED | "
                            "pos=%s contract=%s | was CLOSED qty_remaining=%d | "
                            "broker holds qty=%d → status=%s quantity_remaining=%d | "
                            "close_source set to PARTIAL_CLOSE_REPAIR",
                            self.client_id, pos_id, contract,
                            rem_qty, broker_qty, restore_status, restore_remaining,
                        )
                        # Re-seed the exit engine so this position gets managed again
                        self._seed_exit_engine_from_position(dict(row) | {
                            "status": restore_status,
                            "quantity_remaining": restore_remaining,
                        })
                else:
                    # Broker is flat — fix the DB row (zero remaining, stay CLOSED)
                    def _flatten(pid=pos_id):
                        with conn() as c:
                            c.execute(
                                """
                                UPDATE positions
                                SET    quantity_remaining = 0,
                                       close_source       = 'CLOSED_REPAIR',
                                       updated_at         = NOW()
                                WHERE  id        = %s
                                  AND  client_id = %s
                                  AND  UPPER(status) = 'CLOSED'
                                """,
                                (pid, self.client_id),
                            )
                            return c.rowcount

                    run_with_retry(_flatten)
                    log.info(
                        "[%s] P0-PARTIAL-CLOSE-REPAIR FLATTEN | "
                        "pos=%s contract=%s | broker is flat, setting "
                        "quantity_remaining=0, close_source=CLOSED_REPAIR",
                        self.client_id, pos_id, contract,
                    )

        except Exception as exc:
            log.error(
                "[%s] _repair_closed_positions_with_remaining_qty failed: %s",
                self.client_id, exc, exc_info=True,
            )

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
        except Exception as _e:
            log.warning("reconciler_alert_fn_failed: %s", _e)
