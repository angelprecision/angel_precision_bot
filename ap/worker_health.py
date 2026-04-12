# ap/worker_health.py — APWorkerHealthMonitor
# =============================================================================
# Detects silent thread death in ClientRunner worker loops and alerts via:
#   1. Discord webhook (immediate)
#   2. Supabase bot_status table (dashboard pickup)
#
# Monitored threads per client:
#   - worker thread (queue dispatch)
#   - order monitor thread
#   - equity refresh thread
#   - execution core threads (entry watcher, exit engine, fill monitor)
#
# Alert payload includes:
#   - dead client_id
#   - dead thread name
#   - last known signal (from trade_queue)
#   - last heartbeat timestamp
#   - current open position count
#
# Run as a singleton daemon in app.py or start_multi_client_supervisor().
# =============================================================================

from __future__ import annotations

import json
import logging
import os
import threading
import time
from datetime import datetime, timezone
from typing import Optional

import requests

log = logging.getLogger("ap.worker_health")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
POLL_INTERVAL       = int(os.getenv("HEALTH_MONITOR_POLL", "30"))   # seconds
ALERT_COOLDOWN      = int(os.getenv("HEALTH_ALERT_COOLDOWN", "300")) # 5 min between repeat alerts


def _now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


class APWorkerHealthMonitor:
    """
    Singleton health monitor. Watches all ClientRunner threads for silent death.

    Usage in app.py or supervisor:
        from ap.worker_health import APWorkerHealthMonitor
        monitor = APWorkerHealthMonitor(supabase_client=sb)
        monitor.start()

    Usage in ClientRunner:
        monitor.register(runner)       # call after runner.start()
        monitor.unregister(email)      # call when runner stops
    """

    def __init__(self, supabase_client=None):
        self.sb              = supabase_client
        self._runners: dict  = {}          # email → ClientRunner
        self._lock           = threading.Lock()
        self._stop_event     = threading.Event()
        self._last_alert: dict[str, float] = {}   # email → last alert time
        self._thread: Optional[threading.Thread]  = None

    def start(self):
        self._thread = threading.Thread(
            target=self._run,
            daemon=True,
            name="worker-health-monitor",
        )
        self._thread.start()
        log.info(f"APWorkerHealthMonitor started (poll={POLL_INTERVAL}s)")

    def stop(self):
        self._stop_event.set()

    def register(self, runner):
        """Register a ClientRunner for monitoring."""
        with self._lock:
            self._runners[runner.email] = runner
        log.info(f"[{runner.email}] registered with health monitor")

    def unregister(self, email: str):
        """Remove a runner (called when it intentionally stops)."""
        with self._lock:
            self._runners.pop(email, None)

    # =========================================================================
    # MAIN LOOP
    # =========================================================================

    def _run(self):
        while not self._stop_event.wait(POLL_INTERVAL):
            try:
                self._check_all_runners()
            except Exception as e:
                log.error(f"Health monitor loop error: {e}")

    def _check_all_runners(self):
        with self._lock:
            runners = dict(self._runners)

        for email, runner in runners.items():
            try:
                self._check_runner(email, runner)
            except Exception as e:
                log.error(f"Health check failed for {email}: {e}")

    def _check_runner(self, email: str, runner):
        dead_threads = []

        # ── Check runner thread itself ────────────────────────────────────────
        if not runner.is_alive():
            dead_threads.append(("runner_thread", runner.name))

        # ── Check named daemon threads ────────────────────────────────────────
        live_threads = {t.name: t for t in threading.enumerate()}

        watch_names = [
            f"worker-{email}",
            f"order-monitor-{email}",
            f"equity-refresh-{email}",
            f"queue-sub-{email}",
        ]
        for tname in watch_names:
            t = live_threads.get(tname)
            if t is None:
                dead_threads.append((tname, tname))
            elif not t.is_alive():
                dead_threads.append((tname, tname))

        # ── Check execution core threads ──────────────────────────────────────
        core = getattr(runner, "core", None)
        if core:
            core_threads = {
                "entry_watcher":  getattr(core, "_watcher_thread", None),
                "exit_engine":    getattr(core, "_exit_thread", None),
                "fill_monitor":   getattr(core, "_fill_thread", None),
                "signal_tracker": getattr(core, "_tracker_thread", None),
            }
            for cname, cthread in core_threads.items():
                if cthread is not None and not cthread.is_alive():
                    dead_threads.append((cname, f"core.{cname}"))

        if dead_threads:
            self._fire_alert(email, runner, dead_threads)
        else:
            log.debug(f"[{email}] health check OK")

    # =========================================================================
    # ALERT
    # =========================================================================

    def _fire_alert(self, email: str, runner, dead_threads: list[tuple[str, str]]):
        """Rate-limited alert — fires once per ALERT_COOLDOWN seconds per client."""
        now = time.time()
        last = self._last_alert.get(email, 0)
        if now - last < ALERT_COOLDOWN:
            log.debug(f"[{email}] alert suppressed (cooldown)")
            return
        self._last_alert[email] = now

        # Build context
        last_signal  = self._get_last_signal(email)
        open_pos     = self._get_open_position_count(email)
        dead_names   = [name for _, name in dead_threads]
        timestamp    = _now_utc()

        log.error(
            f"🚨 THREAD DEATH DETECTED | client={email} | "
            f"dead={dead_names} | open_positions={open_pos} | "
            f"last_signal={last_signal.get('ticker','?')} @ {last_signal.get('created_ts','?')}"
        )

        # Build alert message
        alert_msg = self._build_alert(
            email=email,
            dead_threads=dead_names,
            open_positions=open_pos,
            last_signal=last_signal,
            timestamp=timestamp,
        )

        # Send to Discord
        self._send_discord(alert_msg)

        # Write to Supabase bot_status (dashboard pickup)
        self._write_dashboard_alert(
            email=email,
            dead_threads=dead_names,
            open_positions=open_pos,
            last_signal=last_signal,
            timestamp=timestamp,
        )

    def _build_alert(
        self,
        email: str,
        dead_threads: list[str],
        open_positions: int,
        last_signal: dict,
        timestamp: str,
    ) -> str:
        ticker     = last_signal.get("ticker", "unknown")
        sig_ts     = last_signal.get("created_ts", "unknown")
        sig_id     = last_signal.get("signal_id", "unknown")[:8]

        lines = [
            "🚨 **ANGEL PRECISION — THREAD DEATH ALERT**",
            f"**Client:** `{email}`",
            f"**Time:** `{timestamp}`",
            f"**Dead threads:** {', '.join(f'`{t}`' for t in dead_threads)}",
            f"**Open positions:** {open_positions}",
            f"**Last signal:** `{ticker}` @ `{sig_ts}` (id: `{sig_id}...`)",
            "",
            "⚠️ **Action required:** Check Render logs and restart runner if needed.",
        ]
        return "\n".join(lines)

    # =========================================================================
    # DISCORD
    # =========================================================================

    def _send_discord(self, message: str):
        if not DISCORD_WEBHOOK_URL:
            log.warning("DISCORD_WEBHOOK_URL not set — Discord alert skipped")
            return
        try:
            resp = requests.post(
                DISCORD_WEBHOOK_URL,
                json={"content": message},
                timeout=10,
            )
            if resp.status_code in (200, 204):
                log.info("Discord alert sent")
            else:
                log.warning(f"Discord alert failed: {resp.status_code} {resp.text[:100]}")
        except Exception as e:
            log.error(f"Discord alert error: {e}")

    # =========================================================================
    # DASHBOARD — write to bot_status table
    # =========================================================================

    def _write_dashboard_alert(
        self,
        email: str,
        dead_threads: list[str],
        open_positions: int,
        last_signal: dict,
        timestamp: str,
    ):
        """
        Write alert to Supabase bot_status table so the dashboard can display it.
        Uses upsert on client_id so there's always one current record per client.
        """
        if not self.sb:
            return
        try:
            self.sb.table("client_health").upsert({
                "client_id":      email,
                "status":         "THREAD_DEAD",
                "alert_type":     "thread_death",
                "dead_threads":   dead_threads,
                "open_positions": open_positions,
                "last_signal_ticker": last_signal.get("ticker", ""),
                "last_signal_ts":     last_signal.get("created_ts", ""),
                "last_signal_id":     last_signal.get("signal_id", ""),
                "alerted_at":     timestamp,
                "updated_at":     timestamp,
            }, on_conflict="client_id").execute()
            log.info(f"[{email}] Dashboard alert written to client_health")
        except Exception as e:
            log.warning(f"Dashboard alert write failed: {e}")

    def clear_dashboard_alert(self, email: str):
        """
        Call this when runner is restarted to clear the alert from dashboard.
        """
        if not self.sb:
            return
        try:
            self.sb.table("client_health").upsert({
                "client_id":  email,
                "status":     "RUNNING",
                "alert_type": None,
                "updated_at": _now_utc(),
            }, on_conflict="client_id").execute()
        except Exception as e:
            log.debug(f"Clear dashboard alert failed: {e}")

    # =========================================================================
    # DB HELPERS
    # =========================================================================

    def _get_last_signal(self, client_id: str) -> dict:
        """Fetch most recent signal from trade_queue for this client."""
        try:
            from ap.db import conn, run_with_retry
            def _fn():
                with conn() as c:
                    c.execute(
                        """
                        SELECT signal_id, payload->>'ticker' AS ticker, created_ts
                        FROM trade_queue
                        WHERE client_id=%s
                        ORDER BY created_ts DESC
                        LIMIT 1
                        """,
                        (client_id,),
                    )
                    row = c.fetchone()
                    return dict(row) if row else {}
            return run_with_retry(_fn)
        except Exception as e:
            log.debug(f"Last signal fetch failed: {e}")
            return {}

    def _get_open_position_count(self, email: str) -> int:
        """Get open position count from positions table for this client."""
        try:
            from ap.db import conn, run_with_retry
            def _fn():
                with conn() as c:
                    c.execute(
                        "SELECT COUNT(*) AS n FROM positions "
                        "WHERE client_id=%s AND status IN ('OPEN','CLOSING')",
                        (email,),
                    )
                    row = c.fetchone()
                    return int((row or {}).get("n") or 0)
            return run_with_retry(_fn)
        except Exception as e:
            log.debug(f"Position count fetch failed: {e}")
            return -1


# =============================================================================
# SINGLETON ACCESSOR
# =============================================================================

_monitor: Optional[APWorkerHealthMonitor] = None

def get_monitor() -> Optional[APWorkerHealthMonitor]:
    return _monitor

def init_monitor(supabase_client=None) -> APWorkerHealthMonitor:
    """Call once from app.py at startup."""
    global _monitor
    _monitor = APWorkerHealthMonitor(supabase_client=supabase_client)
    _monitor.start()
    return _monitor
