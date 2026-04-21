# ap/self_healing.py — APSelfHealingSystem
# =============================================================================
# Self-healing layer for Angel Precision Bot.
#
# Three capabilities:
#
# 1. THREAD AUTO-RESTART
#    Detects dead threads per component and restarts them safely.
#    Restart policy is component-specific — some are safe to restart
#    automatically, others require human escalation first.
#
# 2. FAST RECONCILE LOOP
#    Runs every 5s (vs health monitor's 30s) specifically for:
#    - Unmanaged positions (kill mid-poll)
#    - Split-brain states (broker has order, DB doesn't)
#    - CLOSING positions older than timeout without EXIT_FILLED
#
# 3. ESCALATION RULES
#    Three-tier escalation:
#    WARNING  — stalled (thread alive but not making progress)
#    CRITICAL — thread dead, auto-restart attempted
#    FATAL    — repeated restart failures → human intervention required
#
# Integration:
#    from ap.self_healing import APSelfHealingSystem, init_self_healing
#    healer = init_self_healing(supabase_client=sb)
#    healer.register(runner)
#
# =============================================================================

from __future__ import annotations

import json
import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

import requests

log = logging.getLogger("ap.self_healing")

# ── Config ────────────────────────────────────────────────────────────────────
DISCORD_WEBHOOK_URL      = os.getenv("DISCORD_WEBHOOK_URL", "")
HEALTH_POLL_SEC          = int(os.getenv("SELF_HEAL_POLL",        "30"))   # normal health poll
RECONCILE_POLL_SEC       = int(os.getenv("RECONCILE_POLL",        "5"))    # fast reconcile
MAX_RESTART_ATTEMPTS     = int(os.getenv("MAX_RESTART_ATTEMPTS",  "3"))    # before FATAL
RESTART_COOLDOWN_SEC     = int(os.getenv("RESTART_COOLDOWN_SEC",  "30"))   # between restarts
STALL_THRESHOLD_SEC      = int(os.getenv("STALL_THRESHOLD_SEC",   "120"))  # WARNING: 2 min no activity
CLOSING_TIMEOUT_SEC      = int(os.getenv("CLOSING_TIMEOUT_SEC",   "600"))  # 10 min CLOSING → escalate
ALERT_COOLDOWN_SEC       = int(os.getenv("HEAL_ALERT_COOLDOWN",   "300"))  # 5 min between repeat alerts


def _now() -> datetime:
    return datetime.now(timezone.utc)

def _now_iso() -> str:
    return _now().isoformat()

def _to_utc(dt) -> datetime:
    """Safely convert DB timestamp to UTC-aware datetime.
    Postgres returns timezone-aware datetimes — only localize if naive.
    Calling .replace(tzinfo=...) on an already-aware datetime distorts time math.
    """
    if dt is None:
        return _now()
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        return dt.astimezone(timezone.utc)   # already aware — convert cleanly
    return dt.replace(tzinfo=timezone.utc)   # naive — safe to localize


# =============================================================================
# ESCALATION TIERS
# =============================================================================

class HealthState:
    OK       = "OK"        # thread alive, progressing normally
    WARNING  = "WARNING"   # alive but stalled / slow
    CRITICAL = "CRITICAL"  # dead — auto-restart attempted
    FATAL    = "FATAL"     # repeated failures — human required


@dataclass
class ComponentHealth:
    """Health record for one thread component of one client."""
    client_id:        str
    component:        str          # e.g. "worker", "order_monitor", "exit_engine"
    state:            str = HealthState.OK
    restart_count:    int = 0
    last_seen:        datetime = field(default_factory=_now)
    last_restart:     datetime = field(default_factory=_now)
    last_alert:       float    = 0.0   # time.time() of last alert
    error_log:        list     = field(default_factory=list)

    def record_error(self, msg: str):
        self.error_log.append(f"{_now_iso()}: {msg}")
        if len(self.error_log) > 20:
            self.error_log = self.error_log[-20:]

    def cooldown_ok(self) -> bool:
        return time.time() - self.last_alert > ALERT_COOLDOWN_SEC

    def restart_allowed(self) -> bool:
        return (self.restart_count < MAX_RESTART_ATTEMPTS and
                (time.time() - self.last_restart.timestamp()) > RESTART_COOLDOWN_SEC)


# =============================================================================
# RESTART REGISTRY
# =============================================================================

# Maps component name → restart function
# Registered per runner. Returns True if restart succeeded.
RestartFn = Callable[[], bool]


class RestartRegistry:
    """
    Stores restart functions per client per component.
    Safe restart = idempotent, bounded retries, error-handled.
    """
    def __init__(self):
        self._fns: dict[str, dict[str, RestartFn]] = {}

    def register(self, client_id: str, component: str, fn: RestartFn):
        self._fns.setdefault(client_id, {})[component] = fn

    def get(self, client_id: str, component: str) -> Optional[RestartFn]:
        return self._fns.get(client_id, {}).get(component)

    def unregister_client(self, client_id: str):
        self._fns.pop(client_id, None)


restart_registry = RestartRegistry()


# =============================================================================
# SELF-HEALING SYSTEM
# =============================================================================

class APSelfHealingSystem:
    """
    Central self-healing controller.
    Runs two loops:
      - Health loop (HEALTH_POLL_SEC): checks thread liveness, drives escalation
      - Reconcile loop (RECONCILE_POLL_SEC): fast check for unmanaged positions
    """

    def __init__(self, supabase_client=None):
        self.sb              = supabase_client
        self._runners:  dict = {}            # email → ClientRunner
        self._health:   dict = {}            # (email, component) → ComponentHealth
        self._lock           = threading.Lock()
        self._stop_event     = threading.Event()
        self._health_thread: Optional[threading.Thread] = None
        self._reconcile_thread: Optional[threading.Thread] = None

    def start(self):
        self._health_thread = threading.Thread(
            target=self._health_loop, daemon=True, name="self-heal-health"
        )
        self._reconcile_thread = threading.Thread(
            target=self._reconcile_loop, daemon=True, name="self-heal-reconcile"
        )
        self._health_thread.start()
        self._reconcile_thread.start()
        log.info(
            f"APSelfHealingSystem started "
            f"(health={HEALTH_POLL_SEC}s reconcile={RECONCILE_POLL_SEC}s)"
        )

    def stop(self):
        self._stop_event.set()

    def register(self, runner):
        """Register a ClientRunner and wire its restart functions."""
        with self._lock:
            self._runners[runner.email] = runner
        self._register_restart_fns(runner)
        log.info(f"[{runner.email}] registered with self-healing system")

    def unregister(self, email: str):
        with self._lock:
            self._runners.pop(email, None)
            self._health = {k: v for k, v in self._health.items()
                            if k[0] != email}
        restart_registry.unregister_client(email)

    # =========================================================================
    # RESTART FUNCTION WIRING
    # =========================================================================

    def _register_restart_fns(self, runner):
        """
        Wire restart functions for each restartable component.
        Safe restarts: worker thread, order monitor, equity refresh.
        Escalate-only: runner itself, execution core (too much state to restart safely).
        """
        email = runner.email

        # Worker thread restart
        def restart_worker():
            try:
                log.warning(f"[{email}] AUTO-RESTART: worker thread")
                runner._start_worker_thread(
                    getattr(runner, "_broker_ref", None) or
                    getattr(runner.core, "broker", None)
                )
                return True
            except Exception as e:
                log.error(f"[{email}] Worker restart failed: {e}")
                return False

        # Order monitor restart
        def restart_order_monitor():
            try:
                log.warning(f"[{email}] AUTO-RESTART: order monitor")
                if runner.order_monitor:
                    runner.order_monitor.stop()
                from ap.order_monitor import APOrderMonitor
                broker = getattr(getattr(runner, "core", None), "broker", None)
                if not broker:
                    return False
                runner.order_monitor = APOrderMonitor(
                    client_id=email,
                    broker=broker,
                    order_state_machine=runner.order_state_machine,
                    position_manager=runner.position_manager,
                )
                runner.order_monitor.start()
                return True
            except Exception as e:
                log.error(f"[{email}] Order monitor restart failed: {e}")
                return False

        # Equity refresh restart — lightweight
        def restart_equity_refresh():
            try:
                log.warning(f"[{email}] AUTO-RESTART: equity refresh")
                broker = getattr(getattr(runner, "core", None), "broker", None)
                if broker:
                    runner._start_equity_refresh(broker)
                return True
            except Exception as e:
                log.error(f"[{email}] Equity refresh restart failed: {e}")
                return False

        restart_registry.register(email, "worker",         restart_worker)
        restart_registry.register(email, "order_monitor",  restart_order_monitor)
        restart_registry.register(email, "equity_refresh", restart_equity_refresh)
        # runner and execution core: ESCALATE ONLY — no auto-restart

    # =========================================================================
    # HEALTH LOOP
    # =========================================================================

    def _health_loop(self):
        while not self._stop_event.wait(HEALTH_POLL_SEC):
            try:
                self._check_all_runners()
            except Exception as e:
                log.error(f"Self-heal health loop error: {e}")

    def _check_all_runners(self):
        with self._lock:
            runners = dict(self._runners)

        for email, runner in runners.items():
            try:
                self._check_runner(email, runner)
            except Exception as e:
                log.error(f"Health check failed for {email}: {e}")

    def _check_runner(self, email: str, runner):
        live_threads = {t.name: t for t in threading.enumerate()}

        # Component → (thread_name, auto_restart_ok, severity_if_dead)
        components = {
            "runner":         (runner.name,                        False, HealthState.FATAL),
            "worker":         (f"worker-{email}",                  True,  HealthState.CRITICAL),
            "order_monitor":  (f"order-monitor-{email}",           True,  HealthState.CRITICAL),
            # equity_refresh: WARNING on first death, CRITICAL after first failed restart
            # Stale equity means capital caps become unreliable over time
            "equity_refresh": (f"equity-refresh-{email}",          True,  HealthState.WARNING),
            # exit_engine is client-scoped — thread name uses email for isolation
            "exit_engine":    (f"ap-exit-engine-{email}",          False, HealthState.CRITICAL),
        }

        # Promote equity_refresh to CRITICAL if it has had any failed restart
        eq_health = self._get_health(email, "equity_refresh")
        if eq_health.restart_count > 0 and eq_health.state != HealthState.OK:
            components["equity_refresh"] = (
                f"equity-refresh-{email}", True, HealthState.CRITICAL
            )

        for comp, (tname, auto_restart, dead_severity) in components.items():
            health = self._get_health(email, comp)
            t      = live_threads.get(tname)
            alive  = (t is not None and t.is_alive())

            if alive:
                # Check for stall: alive but not progressing.
                # last_seen is ONLY updated by heartbeat() calls from the
                # component itself — never by the health checker.  This
                # ensures stalls are detected even when the thread is alive.
                age_secs = (_now() - health.last_seen).total_seconds()
                # equity_refresh sleeps 900s between runs — use a higher threshold
                _comp_threshold = 1000 if comp == "equity_refresh" else STALL_THRESHOLD_SEC
                if age_secs > _comp_threshold and health.state == HealthState.OK:
                    health.state = HealthState.WARNING
                    log.warning(
                        f"[{email}] {comp} STALLED — alive but no heartbeat "
                        f"for {age_secs:.0f}s > {_comp_threshold}s threshold"
                    )
                    if health.cooldown_ok():
                        self._alert(
                            email, comp, HealthState.WARNING,
                            f"Thread alive but stalled for {age_secs:.0f}s "
                            f"(threshold={_comp_threshold}s). "
                            f"Consider manual investigation.",
                            health
                        )
                elif age_secs <= _comp_threshold:
                    health.state = HealthState.OK
            else:
                self._handle_dead_component(
                    email, runner, comp, health,
                    auto_restart=auto_restart,
                    dead_severity=dead_severity,
                    components=components,
                )

        # Fix 7: if exit_engine is dead/unhealthy, gate new entries
        exit_health = self._get_health(email, "exit_engine")
        mc = getattr(runner, "master_control", None)
        if mc is not None:
            try:
                if exit_health.state != HealthState.OK:
                    mc.exit_engine_down = True
                    log.warning(
                        f"[{email}] exit_engine dead/unhealthy — "
                        f"master_control.exit_engine_down=True (new entries blocked)"
                    )
                else:
                    mc.exit_engine_down = False
            except Exception:
                pass

    def _handle_dead_component(
        self, email: str, runner, comp: str,
        health: ComponentHealth, auto_restart: bool, dead_severity: str,
        components: dict = None,
    ):
        # Already FATAL — don't retry, just keep alerting
        if health.state == HealthState.FATAL:
            if health.cooldown_ok():
                self._alert(
                    email, comp, HealthState.FATAL,
                    f"Component {comp} is FATAL — {health.restart_count} restart attempts failed. "
                    f"Manual intervention required.",
                    health
                )
            return

        # Try auto-restart if allowed
        if auto_restart and health.restart_allowed():
            fn = restart_registry.get(email, comp)
            if fn:
                health.restart_count += 1
                health.last_restart   = _now()
                health.state          = HealthState.CRITICAL
                health.record_error(f"Dead — restart attempt {health.restart_count}")

                log.warning(
                    f"[{email}] {comp} DEAD — "
                    f"auto-restart attempt {health.restart_count}/{MAX_RESTART_ATTEMPTS}"
                )
                success = fn()

                if success:
                    # Verify the thread is actually alive — not just "restart ran"
                    time.sleep(2)   # give thread 2s to start
                    live_after = {t.name: t for t in threading.enumerate()}
                    thread_name = components[comp][0]
                    thread_alive = (thread_name in live_after and
                                    live_after[thread_name].is_alive())
                    if thread_alive:
                        log.info(f"[{email}] {comp} restarted and VERIFIED alive")
                        health.state     = HealthState.OK
                        health.last_seen = _now()
                        if health.cooldown_ok():
                            self._alert(
                                email, comp, HealthState.OK,
                                f"Component {comp} restarted and verified alive "
                                f"(attempt {health.restart_count}/{MAX_RESTART_ATTEMPTS})",
                                health
                            )
                    else:
                        log.error(f"[{email}] {comp} restart returned True but thread NOT alive")
                        success = False   # treat as failure
                        health.record_error("Restart returned True but thread not alive")

                if not success:
                    if health.restart_count >= MAX_RESTART_ATTEMPTS:
                        health.state = HealthState.FATAL
                        self._alert(
                            email, comp, HealthState.FATAL,
                            f"Component {comp} failed to restart after "
                            f"{MAX_RESTART_ATTEMPTS} attempts. MANUAL INTERVENTION REQUIRED.",
                            health
                        )
                    else:
                        health.state = HealthState.CRITICAL
                        if health.cooldown_ok():
                            self._alert(email, comp, HealthState.CRITICAL,
                                        f"Restart attempt {health.restart_count} failed "
                                        f"(or thread not alive after restart) — "
                                        f"will retry in {RESTART_COOLDOWN_SEC}s", health)
            return

        # No auto-restart — escalate immediately
        health.state = dead_severity
        if health.cooldown_ok():
            self._alert(
                email, comp, dead_severity,
                f"Component {comp} is {dead_severity}. "
                + ("Auto-restart not enabled for this component." if not auto_restart
                   else f"Cooldown active — will retry in {RESTART_COOLDOWN_SEC}s"),
                health
            )

    # =========================================================================
    # FAST RECONCILE LOOP
    # =========================================================================

    def _reconcile_loop(self):
        """
        Fast loop (RECONCILE_POLL_SEC = 5s) for urgent state consistency checks:
        - Unmanaged positions (kill mid-poll)
        - CLOSING positions stuck > CLOSING_TIMEOUT_SEC without EXIT_FILLED
        - Split-brain: broker has order, DB still shows OPEN
        """
        while not self._stop_event.wait(RECONCILE_POLL_SEC):
            try:
                self._reconcile_all_clients()
            except Exception as e:
                log.error(f"Reconcile loop error: {e}")

    def _reconcile_all_clients(self):
        with self._lock:
            runners = dict(self._runners)

        for email, runner in runners.items():
            pm  = getattr(runner, "position_manager", None)
            osm = getattr(runner, "order_state_machine", None)
            if not pm or not osm:
                continue
            try:
                self._reconcile_client(email, pm, osm)
            except Exception as e:
                log.debug(f"[{email}] Reconcile error: {e}")

    def _reconcile_client(self, email: str, pm, osm):
        from ap.db import conn, run_with_retry

        # ── Check 1: Unmanaged positions (flag set by exit manager kill) ──────
        def _get_unmanaged():
            with conn() as c:
                c.execute(
                    """
                    SELECT id, underlying, status, updated_at
                    FROM positions
                    WHERE client_id=%s
                      AND status IN ('OPEN','CLOSING')
                      AND unmanaged = TRUE
                    """,
                    (email,),
                )
                return c.fetchall()

        try:
            unmanaged = run_with_retry(_get_unmanaged)
            for pos in unmanaged:
                age_secs = (_now() - _to_utc(pos["updated_at"])).total_seconds() \
                           if pos.get("updated_at") else 9999
                log.warning(
                    f"[{email}] RECONCILE: unmanaged position {pos['id']} "
                    f"({pos['underlying']} {pos['status']}) "
                    f"age={age_secs:.0f}s"
                )
                # Clear unmanaged flag ONLY if position is still OPEN (not mid-close)
                # and no active exit order is already in-flight.
                # Blind clearing can mask genuine issues.
                pos_status = pos.get("status", "")
                pos_id     = pos["id"]

                def _check_exit_order(pid=pos_id):
                    with conn() as c:
                        c.execute(
                            """
                            SELECT 1 FROM orders
                            WHERE client_id=%s AND position_id=%s
                              AND kind='EXIT'
                              AND status NOT IN (
                                  'EXIT_FILLED','REJECTED','CANCELED','EXPIRED','ERROR'
                              )
                            LIMIT 1
                            """,
                            (email, pid),
                        )
                        return c.fetchone() is not None

                has_exit_order = False
                try:
                    has_exit_order = run_with_retry(_check_exit_order)
                except Exception:
                    pass

                if pos_status == "OPEN" and not has_exit_order:
                    # Safe to clear — no in-flight exit, position is OPEN
                    def _clear(pid=pos_id):
                        with conn() as c:
                            c.execute(
                                "UPDATE positions SET unmanaged=FALSE, updated_at=NOW() "
                                "WHERE id=%s AND client_id=%s AND status='OPEN'",
                                (pid, email),
                            )
                    run_with_retry(_clear)
                    log.info(
                        f"[{email}] RECONCILE: cleared unmanaged flag for {pos_id} "
                        f"({pos['underlying']}) — no exit order in-flight"
                    )
                else:
                    # CLOSING or exit order exists — escalate, do not auto-clear
                    log.warning(
                        f"[{email}] RECONCILE: unmanaged {pos_id} NOT auto-cleared "
                        f"(status={pos_status} has_exit_order={has_exit_order}) — "
                        f"manual review required"
                    )
                    self._alert(
                        email, "reconcile", HealthState.WARNING,
                        f"Unmanaged position {pos_id} ({pos['underlying']}) in status="
                        f"{pos_status} has_exit_order={has_exit_order}. "
                        f"Auto-clear skipped — manual review required.",
                        None
                    )
        except Exception as e:
            log.debug(f"[{email}] Unmanaged check failed: {e}")

        # ── Check 2: Stuck CLOSING positions ──────────────────────────────────
        def _get_stuck_closing():
            with conn() as c:
                c.execute(
                    f"""
                    SELECT p.id, p.underlying, p.updated_at,
                           o.local_order_id, o.status as order_status,
                           o.broker_order_id
                    FROM positions p
                    LEFT JOIN orders o
                      ON o.client_id = p.client_id
                     AND o.position_id = p.id
                     AND o.kind = 'EXIT'
                    WHERE p.client_id=%s
                      AND p.status = 'CLOSING'
                      AND p.updated_at < NOW() - INTERVAL '{CLOSING_TIMEOUT_SEC} seconds'
                    """,
                    (email,),
                )
                return c.fetchall()

        try:
            stuck = run_with_retry(_get_stuck_closing)
            for row in stuck:
                age = (_now() - _to_utc(row["updated_at"])).total_seconds() \
                      if row.get("updated_at") else 9999
                log.error(
                    f"[{email}] RECONCILE: CLOSING stuck {age:.0f}s | "
                    f"pos={row['id']} {row['underlying']} | "
                    f"exit_order={row.get('local_order_id','none')} "
                    f"status={row.get('order_status','none')} "
                    f"broker={row.get('broker_order_id','none')}"
                )
                # Escalate — don't auto-fix CLOSING (real money at stake)
                self._alert(
                    email, "reconcile", HealthState.CRITICAL,
                    f"STUCK CLOSING: position {row['id']} ({row['underlying']}) "
                    f"stuck in CLOSING for {age:.0f}s. "
                    f"Exit order: {row.get('broker_order_id','none')}. "
                    f"Manual check required.",
                    None
                )
        except Exception as e:
            log.debug(f"[{email}] Stuck closing check failed: {e}")

    # =========================================================================
    # ALERTING
    # =========================================================================

    def _get_health(self, email: str, component: str) -> ComponentHealth:
        key = (email, component)
        if key not in self._health:
            self._health[key] = ComponentHealth(
                client_id=email, component=component
            )
        return self._health[key]

    def _alert(
        self,
        email: str,
        component: str,
        state: str,
        message: str,
        health: Optional[ComponentHealth],
    ):
        if health:
            health.last_alert = time.time()

        icon = {"OK": "✅", "WARNING": "⚠️", "CRITICAL": "🔴", "FATAL": "🚨"}.get(state, "❓")
        restarts = f" | restarts={health.restart_count}/{MAX_RESTART_ATTEMPTS}" if health else ""

        log.error(f"{icon} SELF-HEAL [{state}] [{email}] [{component}]: {message}{restarts}")

        full_msg = (
            f"{icon} **ANGEL PRECISION — SELF-HEAL {state}**\n"
            f"**Client:** `{email}`\n"
            f"**Component:** `{component}`\n"
            f"**State:** `{state}`\n"
            f"**Time:** `{_now_iso()}`\n"
            f"**Message:** {message}"
            + (f"\n**Restart attempts:** {health.restart_count}/{MAX_RESTART_ATTEMPTS}" if health else "")
        )

        self._send_discord(full_msg)
        self._write_dashboard(email, component, state, message, health)

    def _send_discord(self, message: str):
        if not DISCORD_WEBHOOK_URL:
            return
        try:
            resp = requests.post(
                DISCORD_WEBHOOK_URL,
                json={"content": message[:1900]},
                timeout=8,
            )
            if resp.status_code not in (200, 204):
                log.debug(f"Discord alert HTTP {resp.status_code}")
        except Exception as e:
            log.debug(f"Discord alert failed: {e}")

    def _write_dashboard(
        self, email: str, component: str, state: str,
        message: str, health: Optional[ComponentHealth]
    ):
        """
        Upserts one row per (client_id, component) — not one per client.
        Requires client_health table to have PK on (client_id, component).
        See worker_health_schema.sql for the updated schema.
        """
        if not self.sb:
            return
        try:
            self.sb.table("client_health").upsert({
                "client_id":      email,
                "component":      component,
                "status":         state,
                "alert_type":     f"self_heal_{component}",
                "restart_count":  health.restart_count if health else 0,
                "message":        message[:500],
                "alerted_at":     _now_iso(),
                "updated_at":     _now_iso(),
            }, on_conflict="client_id,component").execute()
        except Exception as e:
            log.debug(f"Dashboard write failed: {e}")

    def heartbeat(self, email: str, component: str):
        """
        Call from long-running loops to advance last_seen timestamp.
        Prevents false stall alerts for healthy but slow threads.

        Example — call from worker_loop every poll cycle:
            healer = get_healer()
            if healer:
                healer.heartbeat(email, "worker")
        """
        health = self._get_health(email, component)
        health.last_seen = _now()
        if health.state == HealthState.WARNING:
            health.state = HealthState.OK   # clear stall if heartbeat resumed

    def get_health_summary(self) -> list[dict]:
        """Return current health state of all components."""
        with self._lock:
            return [
                {
                    "client_id":     h.client_id,
                    "component":     h.component,
                    "state":         h.state,
                    "restart_count": h.restart_count,
                    "last_seen":     h.last_seen.isoformat(),
                }
                for h in self._health.values()
            ]


# =============================================================================
# SINGLETON
# =============================================================================

_healer: Optional[APSelfHealingSystem] = None

def get_healer() -> Optional[APSelfHealingSystem]:
    return _healer

def init_self_healing(supabase_client=None) -> APSelfHealingSystem:
    """Call once from start_multi_client_supervisor()."""
    global _healer
    _healer = APSelfHealingSystem(supabase_client=supabase_client)
    _healer.start()
    return _healer
