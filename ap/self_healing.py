# ap/self_healing.py — APSelfHealingSystem
# =============================================================================
# Self-healing layer for Angel Precision Bot.
#
# Preserves the existing public API:
#   - init_self_healing(...)
#   - get_healer()
#   - APSelfHealingSystem.register/unregister/heartbeat/get_health_summary
#
# Production guards:
#   - Thread liveness/stall checks
#   - Fast stuck-CLOSING alert loop
#   - Exit quarantine watchdog
#   - Autonomous broker-truth exit recovery fallback
#
# Money-safety rule:
#   Self-healing may query broker truth and call identity-bound recovery hooks,
#   but it must never blindly clear quarantine or blindly submit duplicate exits.
# =============================================================================

from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Optional

import requests

log = logging.getLogger("ap.self_healing")

DISCORD_WEBHOOK_URL = os.getenv("DISCORD_WEBHOOK_URL", "")
HEALTH_POLL_SEC = int(os.getenv("SELF_HEAL_POLL", "30"))
RECONCILE_POLL_SEC = int(os.getenv("RECONCILE_POLL", "5"))
MAX_RESTART_ATTEMPTS = int(os.getenv("MAX_RESTART_ATTEMPTS", "3"))
RESTART_COOLDOWN_SEC = int(os.getenv("RESTART_COOLDOWN_SEC", "30"))
STALL_THRESHOLD_SEC = int(os.getenv("STALL_THRESHOLD_SEC", "120"))
CLOSING_TIMEOUT_SEC = int(os.getenv("CLOSING_TIMEOUT_SEC", "600"))
ALERT_COOLDOWN_SEC = int(os.getenv("HEAL_ALERT_COOLDOWN", "300"))
EXIT_QUARANTINE_WARN_SEC = int(os.getenv("EXIT_QUARANTINE_WARN_SEC", "30"))
EXIT_QUARANTINE_CRITICAL_SEC = int(os.getenv("EXIT_QUARANTINE_CRITICAL_SEC", "60"))
EXIT_INFLIGHT_WARN_SEC = int(os.getenv("EXIT_INFLIGHT_WARN_SEC", "45"))


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _to_utc(dt) -> datetime:
    if dt is None:
        return _now()
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        return dt.astimezone(timezone.utc)
    return dt.replace(tzinfo=timezone.utc)


def _age_seconds(dt) -> float:
    try:
        return max(0.0, (_now() - _to_utc(dt)).total_seconds())
    except Exception:
        return 999999.0


class HealthState:
    OK = "OK"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
    FATAL = "FATAL"


@dataclass
class ComponentHealth:
    client_id: str
    component: str
    state: str = HealthState.OK
    restart_count: int = 0
    last_seen: datetime = field(default_factory=_now)
    last_restart: datetime = field(default_factory=_now)
    last_alert: float = 0.0
    error_log: list = field(default_factory=list)

    def record_error(self, msg: str):
        self.error_log.append(f"{_now_iso()}: {msg}")
        if len(self.error_log) > 20:
            self.error_log = self.error_log[-20:]

    def cooldown_ok(self) -> bool:
        return time.time() - self.last_alert > ALERT_COOLDOWN_SEC

    def restart_allowed(self) -> bool:
        return (
            self.restart_count < MAX_RESTART_ATTEMPTS
            and (time.time() - self.last_restart.timestamp()) > RESTART_COOLDOWN_SEC
        )


RestartFn = Callable[[], bool]


class RestartRegistry:
    def __init__(self):
        self._fns: dict[str, dict[str, RestartFn]] = {}

    def register(self, client_id: str, component: str, fn: RestartFn):
        self._fns.setdefault(client_id, {})[component] = fn

    def get(self, client_id: str, component: str) -> Optional[RestartFn]:
        return self._fns.get(client_id, {}).get(component)

    def unregister_client(self, client_id: str):
        self._fns.pop(client_id, None)


restart_registry = RestartRegistry()


class APSelfHealingSystem:
    def __init__(self, supabase_client=None):
        self.sb = supabase_client
        self._runners: dict = {}
        self._health: dict = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._health_thread: Optional[threading.Thread] = None
        self._reconcile_thread: Optional[threading.Thread] = None

    def start(self):
        self._health_thread = threading.Thread(
            target=self._health_loop,
            daemon=True,
            name="self-heal-health",
        )
        self._reconcile_thread = threading.Thread(
            target=self._reconcile_loop,
            daemon=True,
            name="self-heal-reconcile",
        )
        self._health_thread.start()
        self._reconcile_thread.start()
        log.info("APSelfHealingSystem started (health=%ss reconcile=%ss)", HEALTH_POLL_SEC, RECONCILE_POLL_SEC)

    def stop(self):
        self._stop_event.set()

    def register(self, runner):
        with self._lock:
            self._runners[runner.email] = runner
        self._register_restart_fns(runner)
        log.info("[%s] registered with self-healing system", runner.email)

    def unregister(self, email: str):
        with self._lock:
            self._runners.pop(email, None)
            self._health = {k: v for k, v in self._health.items() if k[0] != email}
        restart_registry.unregister_client(email)

    def _register_restart_fns(self, runner):
        email = runner.email

        def restart_worker():
            try:
                log.warning("[%s] AUTO-RESTART: worker thread", email)
                broker = getattr(runner, "_broker_ref", None) or getattr(getattr(runner, "core", None), "broker", None)
                runner._start_worker_thread(broker)
                return True
            except Exception as e:
                log.error("[%s] Worker restart failed: %s", email, e)
                return False

        def restart_order_monitor():
            try:
                log.warning("[%s] AUTO-RESTART: order monitor", email)
                if getattr(runner, "order_monitor", None):
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
                    exit_engine=getattr(getattr(runner, "core", None), "exit_eng", None),
                )
                runner.order_monitor.start()
                return True
            except Exception as e:
                log.error("[%s] Order monitor restart failed: %s", email, e)
                return False

        def restart_equity_refresh():
            try:
                log.warning("[%s] AUTO-RESTART: equity refresh", email)
                broker = getattr(getattr(runner, "core", None), "broker", None)
                if broker:
                    runner._start_equity_refresh(broker)
                return True
            except Exception as e:
                log.error("[%s] Equity refresh restart failed: %s", email, e)
                return False

        def restart_or_start_reconciler():
            try:
                log.warning("[%s] AUTO-START/RESTART: broker reconciler", email)
                broker = getattr(getattr(runner, "core", None), "broker", None)
                exit_eng = getattr(getattr(runner, "core", None), "exit_eng", None)
                if getattr(runner, "reconciler", None):
                    try:
                        runner.reconciler.stop()
                    except Exception:
                        pass
                if hasattr(runner, "_start_reconciler") and broker and exit_eng:
                    runner._start_reconciler(broker, exit_eng)
                    rec = getattr(runner, "reconciler", None)
                    return bool(rec and (not hasattr(rec, "is_alive") or rec.is_alive()))
                return False
            except Exception as e:
                log.error("[%s] Reconciler auto-start/restart failed: %s", email, e)
                return False

        restart_registry.register(email, "worker", restart_worker)
        restart_registry.register(email, "order_monitor", restart_order_monitor)
        restart_registry.register(email, "equity_refresh", restart_equity_refresh)
        restart_registry.register(email, "reconciler", restart_or_start_reconciler)

    def _health_loop(self):
        while not self._stop_event.wait(HEALTH_POLL_SEC):
            try:
                self._check_all_runners()
            except Exception as e:
                log.error("Self-heal health loop error: %s", e)

    def _check_all_runners(self):
        with self._lock:
            runners = dict(self._runners)
        for email, runner in runners.items():
            try:
                self._check_runner(email, runner)
            except Exception as e:
                log.error("Health check failed for %s: %s", email, e)

    def _check_runner(self, email: str, runner):
        live_threads = {t.name: t for t in threading.enumerate()}
        components = {
            "runner": (runner.name, False, HealthState.FATAL),
            "worker": (f"worker-{email}", True, HealthState.CRITICAL),
            "order_monitor": (f"order-monitor-{email}", True, HealthState.CRITICAL),
            "equity_refresh": (f"equity-refresh-{email}", True, HealthState.WARNING),
            "exit_engine": (f"ap-exit-engine-{email}", False, HealthState.CRITICAL),
        }
        for comp, (tname, auto_restart, dead_severity) in components.items():
            health = self._get_health(email, comp)
            t = live_threads.get(tname)
            alive = bool(t is not None and t.is_alive())
            if alive:
                threshold = 1000 if comp == "equity_refresh" else STALL_THRESHOLD_SEC
                age_secs = (_now() - health.last_seen).total_seconds()
                if age_secs > threshold and health.state == HealthState.OK:
                    health.state = HealthState.WARNING
                    if health.cooldown_ok():
                        self._alert(email, comp, HealthState.WARNING, f"Thread alive but stalled for {age_secs:.0f}s", health)
                elif age_secs <= threshold:
                    health.state = HealthState.OK
            else:
                self._handle_dead_component(email, runner, comp, health, auto_restart, dead_severity, components)

        self._check_exit_quarantine_watchdog(email, runner)

        exit_health = self._get_health(email, "exit_engine")
        mc = getattr(runner, "master_control", None)
        if mc is not None:
            try:
                mc.exit_engine_down = exit_health.state != HealthState.OK
            except Exception:
                pass

    def _handle_dead_component(self, email: str, runner, comp: str, health: ComponentHealth, auto_restart: bool, dead_severity: str, components: dict | None = None):
        if health.state == HealthState.FATAL:
            if health.cooldown_ok():
                self._alert(email, comp, HealthState.FATAL, f"Component {comp} is FATAL; manual intervention required", health)
            return
        if auto_restart and health.restart_allowed():
            fn = restart_registry.get(email, comp)
            if fn:
                health.restart_count += 1
                health.last_restart = _now()
                health.state = HealthState.CRITICAL
                health.record_error(f"Dead — restart attempt {health.restart_count}")
                success = fn()
                if success:
                    health.state = HealthState.OK
                    health.last_seen = _now()
                    if health.cooldown_ok():
                        self._alert(email, comp, HealthState.OK, f"Component {comp} restarted", health)
                else:
                    health.state = HealthState.FATAL if health.restart_count >= MAX_RESTART_ATTEMPTS else HealthState.CRITICAL
                    if health.cooldown_ok():
                        self._alert(email, comp, health.state, f"Restart attempt {health.restart_count} failed", health)
                return
        health.state = dead_severity
        if health.cooldown_ok():
            self._alert(email, comp, dead_severity, f"Component {comp} is {dead_severity}; auto_restart={auto_restart}", health)

    def _reconcile_loop(self):
        while not self._stop_event.wait(RECONCILE_POLL_SEC):
            try:
                self._reconcile_all_clients()
            except Exception as e:
                log.error("Reconcile loop error: %s", e)

    def _reconcile_all_clients(self):
        with self._lock:
            runners = dict(self._runners)
        for email, runner in runners.items():
            try:
                pm = getattr(runner, "position_manager", None)
                osm = getattr(runner, "order_state_machine", None)
                if pm and osm:
                    self._reconcile_client(email, pm, osm)
                self._check_exit_quarantine_watchdog(email, runner)
            except Exception as e:
                log.debug("[%s] Reconcile error: %s", email, e)

    def _reconcile_client(self, email: str, pm, osm):
        from ap.db import conn, run_with_retry

        def _get_stuck_closing():
            with conn() as c:
                c.execute(
                    f"""
                    SELECT p.id, p.underlying, p.updated_at, o.local_order_id,
                           o.status as order_status, o.broker_order_id
                    FROM positions p
                    LEFT JOIN orders o ON o.client_id=p.client_id AND o.position_id=p.id AND o.kind='EXIT'
                    WHERE p.client_id=%s AND p.status='CLOSING'
                      AND p.updated_at < NOW() - INTERVAL '{CLOSING_TIMEOUT_SEC} seconds'
                    """,
                    (email,),
                )
                return c.fetchall()

        try:
            for row in run_with_retry(_get_stuck_closing) or []:
                age = _age_seconds(row.get("updated_at")) if hasattr(row, "get") else 9999
                self._alert(
                    email,
                    "reconcile",
                    HealthState.CRITICAL,
                    f"STUCK CLOSING pos={row.get('id')} age={age:.0f}s exit={row.get('broker_order_id','none')}",
                    None,
                )
        except Exception as e:
            log.debug("[%s] Stuck closing check failed: %s", email, e)

    def _run_autonomous_exit_recovery(self, email: str, runner, health: ComponentHealth) -> list:
        """Run direct broker-truth recovery as a fallback to reconciler.

        The helper itself enforces the safety rule: no blind clear, no blind duplicate submit.
        """
        actions = []
        try:
            core = getattr(runner, "core", None)
            exit_eng = getattr(core, "exit_eng", None)
            broker = getattr(core, "broker", None)
            osm = getattr(runner, "order_state_machine", None)
            if not (exit_eng and broker):
                health.record_error("autonomous recovery skipped: missing exit_eng or broker")
                return actions

            from ap.exit_autonomous_recovery import recover_exit_engine

            actions = recover_exit_engine(exit_engine=exit_eng, broker=broker, osm=osm)
            if actions:
                summary = ", ".join(f"{getattr(a, 'position_id', '?')}:{getattr(a, 'action', '?')}" for a in actions[:8])
                health.record_error(f"autonomous recovery actions: {summary}")
                log.warning("[%s] autonomous exit recovery actions: %s", email, summary)
        except Exception as e:
            health.record_error(f"autonomous recovery failed: {e}")
            log.error("[%s] autonomous exit recovery failed: %s", email, e, exc_info=True)
        return actions

    def _check_exit_quarantine_watchdog(self, email: str, runner) -> None:
        """Self-heal watchdog for quarantined/stale exit states.

        Recovery order:
          1. Block new entries / mark degraded.
          2. Try to start/restart reconciler and run one pass.
          3. Run autonomous direct broker-truth recovery fallback.
          4. Alert/dashboard with action details.
        """
        exit_eng = getattr(getattr(runner, "core", None), "exit_eng", None)
        if exit_eng is None:
            return

        problematic = []
        try:
            if hasattr(exit_eng, "active_positions"):
                positions = list(exit_eng.active_positions())
            elif hasattr(exit_eng, "_positions"):
                positions = [p for p in getattr(exit_eng, "_positions", []) if not getattr(p, "closed", False)]
            else:
                positions = []

            for pos in positions:
                in_flight = bool(getattr(pos, "exit_in_flight", False))
                quarantine = bool(getattr(pos, "exit_identity_quarantine", False) or getattr(pos, "last_callback_identity_missing", False))
                signal_ts = getattr(pos, "last_exit_signal_ts", None) or getattr(pos, "last_callback_identity_missing_ts", None)
                age = _age_seconds(signal_ts) if signal_ts else 0.0
                stale_inflight = in_flight and age >= EXIT_INFLIGHT_WARN_SEC
                stale_quarantine = quarantine and age >= EXIT_QUARANTINE_WARN_SEC
                if stale_quarantine or stale_inflight:
                    problematic.append((pos, age, stale_quarantine, stale_inflight))
        except Exception as e:
            log.debug("[%s] exit quarantine watchdog scan failed: %s", email, e)
            return

        if not problematic:
            return

        health = self._get_health(email, "exit_quarantine")
        worst_age = max(age for _, age, _, _ in problematic)
        health.state = HealthState.CRITICAL if worst_age >= EXIT_QUARANTINE_CRITICAL_SEC else HealthState.WARNING
        health.record_error(f"exit quarantine/stale inflight count={len(problematic)} worst_age={worst_age:.0f}s")

        try:
            if hasattr(runner, "entries_allowed"):
                runner.entries_allowed.clear()
            if hasattr(runner, "degraded"):
                runner.degraded.set()
            if hasattr(runner, "degraded_reasons"):
                runner.degraded_reasons.add(f"exit_quarantine:{len(problematic)}")
        except Exception:
            pass

        reconciler = getattr(runner, "reconciler", None)
        rec_alive = bool(reconciler and (not hasattr(reconciler, "is_alive") or reconciler.is_alive()))
        if not rec_alive:
            rec_health = self._get_health(email, "reconciler")
            if rec_health.restart_allowed():
                self._handle_dead_component(email, runner, "reconciler", rec_health, True, HealthState.CRITICAL, {})
                reconciler = getattr(runner, "reconciler", None)

        try:
            reconciler = getattr(runner, "reconciler", None)
            if reconciler and hasattr(reconciler, "run_once"):
                reconciler.run_once()
        except Exception as e:
            health.record_error(f"reconciler run_once failed: {e}")

        autonomous_actions = self._run_autonomous_exit_recovery(email, runner, health)

        if health.cooldown_ok():
            details = []
            for pos, age, q, s in problematic[:5]:
                details.append(
                    f"{getattr(pos, 'ticker', '?')} pos={getattr(pos, 'position_id', '?')} "
                    f"age={age:.0f}s q={q} stale_inflight={s} "
                    f"local={getattr(pos, 'pending_exit_local_order_id', '') or '?'} "
                    f"broker={getattr(pos, 'pending_exit_broker_order_id', '') or '?'}"
                )
            action_summary = ""
            if autonomous_actions:
                action_summary = " | autonomous=" + ", ".join(
                    f"{getattr(a, 'position_id', '?')}:{getattr(a, 'action', '?')}" for a in autonomous_actions[:8]
                )
            self._alert(
                email,
                "exit_quarantine",
                health.state,
                "Exit quarantine/stale in-flight detected. Entries blocked; reconciler kicked; autonomous broker-truth recovery attempted. "
                + " | ".join(details)
                + action_summary,
                health,
            )

    def _get_health(self, email: str, component: str) -> ComponentHealth:
        key = (email, component)
        if key not in self._health:
            self._health[key] = ComponentHealth(client_id=email, component=component)
        return self._health[key]

    def _alert(self, email: str, component: str, state: str, message: str, health: Optional[ComponentHealth]):
        if health:
            health.last_alert = time.time()
        icon = {"OK": "✅", "WARNING": "⚠️", "CRITICAL": "🔴", "FATAL": "🚨"}.get(state, "❓")
        restarts = f" | restarts={health.restart_count}/{MAX_RESTART_ATTEMPTS}" if health else ""
        log.error("%s SELF-HEAL [%s] [%s] [%s]: %s%s", icon, state, email, component, message, restarts)
        full_msg = (
            f"{icon} **ANGEL PRECISION — SELF-HEAL {state}**\n"
            f"**Client:** `{email}`\n**Component:** `{component}`\n**State:** `{state}`\n"
            f"**Time:** `{_now_iso()}`\n**Message:** {message}"
            + (f"\n**Restart attempts:** {health.restart_count}/{MAX_RESTART_ATTEMPTS}" if health else "")
        )
        self._send_discord(full_msg)
        self._write_dashboard(email, component, state, message, health)

    def _send_discord(self, message: str):
        if not DISCORD_WEBHOOK_URL:
            return
        try:
            resp = requests.post(DISCORD_WEBHOOK_URL, json={"content": message[:1900]}, timeout=8)
            if resp.status_code not in (200, 204):
                log.debug("Discord alert HTTP %s", resp.status_code)
        except Exception as e:
            log.debug("Discord alert failed: %s", e)

    def _write_dashboard(self, email: str, component: str, state: str, message: str, health: Optional[ComponentHealth]):
        if not self.sb:
            return
        try:
            self.sb.table("client_health").upsert({
                "client_id": email,
                "component": component,
                "status": state,
                "alert_type": f"self_heal_{component}",
                "restart_count": health.restart_count if health else 0,
                "message": message[:500],
                "alerted_at": _now_iso(),
                "updated_at": _now_iso(),
            }, on_conflict="client_id,component").execute()
        except Exception as e:
            log.debug("Dashboard write failed: %s", e)

    def heartbeat(self, email: str, component: str):
        health = self._get_health(email, component)
        health.last_seen = _now()
        if health.state == HealthState.WARNING:
            health.state = HealthState.OK

    def get_health_summary(self) -> list[dict]:
        with self._lock:
            return [
                {
                    "client_id": h.client_id,
                    "component": h.component,
                    "state": h.state,
                    "restart_count": h.restart_count,
                    "last_seen": h.last_seen.isoformat(),
                }
                for h in self._health.values()
            ]


_healer: Optional[APSelfHealingSystem] = None


def get_healer() -> Optional[APSelfHealingSystem]:
    return _healer


def init_self_healing(supabase_client=None) -> APSelfHealingSystem:
    global _healer
    _healer = APSelfHealingSystem(supabase_client=supabase_client)
    _healer.start()
    return _healer
