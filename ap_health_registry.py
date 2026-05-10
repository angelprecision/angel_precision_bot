"""
ap_health_registry.py
=====================
Central nervous system health registry for Angel Precision Intelligence.

Every organ registers here and emits heartbeats.
The brain/master control/entry gate can check HEALTH.is_system_healthy()
before authorizing any trade.

If a CRITICAL organ flatlines or fatally fails, the kill function is called.

This module is a singleton:
    from ap_health_registry import HEALTH, Criticality

Usage:

    HEALTH.register("ap_exit_engine", Criticality.CRITICAL, stale_after_s=30)
    HEALTH.heartbeat("ap_exit_engine", metrics={"active_positions": 3})
    HEALTH.report_error("ap_exit_engine", "quote fetch failed", fatal=False)

Recommended critical organs:
- ap_quote_monitor
- ap_exit_engine
- ap_broker
- ap_execution_core
- ap_fill_monitor
- ap_reconciler

Recommended high organs:
- ap_entry_watcher
- ap_overnight_signal_manager
- ap_master_control

Recommended normal organs:
- scanners
- reports
- content/marketing agents
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("ap.health")


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class HealthStatus(str, Enum):
    BOOTING  = "BOOTING"
    HEALTHY  = "HEALTHY"
    DEGRADED = "DEGRADED"
    STALE    = "STALE"
    FAILED   = "FAILED"
    STOPPED  = "STOPPED"


class Criticality(str, Enum):
    CRITICAL = "CRITICAL"   # halt if dead/stale/failed
    HIGH     = "HIGH"       # alert if dead/stale/failed
    NORMAL   = "NORMAL"     # log only


# ---------------------------------------------------------------------------
# Organ record
# ---------------------------------------------------------------------------

@dataclass
class OrganHealth:
    name:               str
    criticality:        Criticality
    status:             HealthStatus = HealthStatus.BOOTING
    last_heartbeat_ts:  float = 0.0
    stale_after_s:      float = 60.0
    last_error:         Optional[str] = None
    metrics:            Dict[str, Any] = field(default_factory=dict)
    error_count:        int = 0
    registered_at:      float = field(default_factory=time.time)
    last_status_change_ts: float = field(default_factory=time.time)

    def heartbeat_age_s(self) -> float:
        return time.time() - self.last_heartbeat_ts if self.last_heartbeat_ts else 9e9

    def is_stale(self) -> bool:
        if self.status in (HealthStatus.BOOTING, HealthStatus.STOPPED):
            return False
        return self.heartbeat_age_s() > self.stale_after_s

    def is_ok(self) -> bool:
        return self.status in (HealthStatus.HEALTHY, HealthStatus.BOOTING)

    def mark_status(self, status: HealthStatus) -> None:
        if self.status != status:
            self.status = status
            self.last_status_change_ts = time.time()


# ---------------------------------------------------------------------------
# Singleton registry
# ---------------------------------------------------------------------------

class HealthRegistry:
    """
    Thread-safe singleton health registry.
    Import HEALTH at module level.
    """
    _instance: Optional["HealthRegistry"] = None
    _class_lock = threading.Lock()

    def __new__(cls) -> "HealthRegistry":
        with cls._class_lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._organs: Dict[str, OrganHealth] = {}
                inst._organ_lock = threading.RLock()
                inst._kill_fn: Optional[Callable[[str], None]] = None
                inst._alert_fn: Optional[Callable[[str], None]] = None
                cls._instance = inst
            return cls._instance

    # ------------------------------------------------------------------
    # Registration
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        criticality: Criticality,
        stale_after_s: float = 60.0,
        overwrite: bool = True,
    ) -> None:
        """
        Register an organ.

        overwrite=True updates existing registration.
        overwrite=False preserves existing registration.
        """
        with self._organ_lock:
            if name in self._organs and not overwrite:
                return

            existing = self._organs.get(name)
            self._organs[name] = OrganHealth(
                name=name,
                criticality=criticality,
                stale_after_s=stale_after_s,
                last_heartbeat_ts=time.time(),
                metrics=dict(existing.metrics) if existing else {},
                error_count=existing.error_count if existing else 0,
                last_error=existing.last_error if existing else None,
            )
            log.info("[HEALTH] Registered: %-40s criticality=%s stale_after=%ss", name, criticality.value, stale_after_s)

    def ensure_registered(
        self,
        name: str,
        criticality: Criticality = Criticality.NORMAL,
        stale_after_s: float = 120.0,
    ) -> None:
        with self._organ_lock:
            if name not in self._organs:
                self.register(name, criticality, stale_after_s=stale_after_s)

    # ------------------------------------------------------------------
    # Heartbeat
    # ------------------------------------------------------------------

    def heartbeat(
        self,
        name: str,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._organ_lock:
            organ = self._organs.get(name)
            if not organ:
                # Auto-register unknown heartbeat sources as NORMAL.
                self.register(name, Criticality.NORMAL, stale_after_s=120.0)
                organ = self._organs[name]

            organ.last_heartbeat_ts = time.time()
            if organ.status in (
                HealthStatus.BOOTING,
                HealthStatus.STALE,
                HealthStatus.DEGRADED,
                HealthStatus.FAILED,
            ):
                organ.mark_status(HealthStatus.HEALTHY)
                log.info("[HEALTH] %s recovered -> HEALTHY", name)

            if metrics:
                organ.metrics.update(metrics)

    # ------------------------------------------------------------------
    # Status and error reporting
    # ------------------------------------------------------------------

    def set_status(
        self,
        name: str,
        status: HealthStatus,
        reason: Optional[str] = None,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._organ_lock:
            organ = self._organs.get(name)
            if not organ:
                self.register(name, Criticality.NORMAL, stale_after_s=120.0)
                organ = self._organs[name]

            organ.mark_status(status)
            if reason:
                organ.last_error = reason
            if metrics:
                organ.metrics.update(metrics)

        log.warning("[HEALTH] STATUS %-40s -> %s reason=%s", name, status.value, reason or "")

    def report_error(
        self,
        name: str,
        error: str,
        fatal: bool = False,
        metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        with self._organ_lock:
            organ = self._organs.get(name)
            if not organ:
                self.register(name, Criticality.NORMAL, stale_after_s=120.0)
                organ = self._organs[name]

            organ.last_error = str(error)
            organ.error_count += 1
            organ.mark_status(HealthStatus.FAILED if fatal else HealthStatus.DEGRADED)
            if metrics:
                organ.metrics.update(metrics)
            criticality = organ.criticality

        level = "FATAL" if fatal else "ERROR"
        log.error("[HEALTH] %s %s: %s", name, level, error)

        if fatal and criticality == Criticality.CRITICAL:
            self._fire_kill(f"{name} FATAL: {error}")

        self._fire_alert(f"{'🚨 FATAL' if fatal else '⚠️ ERROR'} [{name}]: {error}")

    def stopped(self, name: str, reason: str = "stopped") -> None:
        self.set_status(name, HealthStatus.STOPPED, reason=reason)

    # ------------------------------------------------------------------
    # Sweep — call every 10s from monitor thread
    # ------------------------------------------------------------------

    def sweep(self) -> List[str]:
        """
        Check all organs for staleness. Returns list of newly stale names.
        Call from a daemon thread every 10 seconds.
        """
        stale_now: List[str] = []
        kills_to_fire: List[str] = []
        alerts_to_fire: List[str] = []

        with self._organ_lock:
            for organ in self._organs.values():
                if organ.is_stale() and organ.status != HealthStatus.STALE:
                    organ.mark_status(HealthStatus.STALE)
                    age = organ.heartbeat_age_s()
                    stale_now.append(organ.name)
                    log.error(
                        "[HEALTH] STALE: %-40s age=%.0fs threshold=%.0fs",
                        organ.name,
                        age,
                        organ.stale_after_s,
                    )
                    if organ.criticality == Criticality.CRITICAL:
                        kills_to_fire.append(f"{organ.name} STALE — no heartbeat for {age:.0f}s")
                    alerts_to_fire.append(f"⚠️ STALE [{organ.name}]: no heartbeat for {age:.0f}s")

        # Fire callbacks outside the lock.
        for reason in kills_to_fire:
            self._fire_kill(reason)
        for msg in alerts_to_fire:
            self._fire_alert(msg)

        return stale_now

    # ------------------------------------------------------------------
    # System health check
    # ------------------------------------------------------------------

    def is_system_healthy(self) -> bool:
        """Returns False if ANY CRITICAL organ is not healthy/booting."""
        with self._organ_lock:
            for o in self._organs.values():
                if o.criticality == Criticality.CRITICAL and not o.is_ok():
                    return False
        return True

    def unhealthy_critical_organs(self) -> List[str]:
        with self._organ_lock:
            return [
                name for name, o in self._organs.items()
                if o.criticality == Criticality.CRITICAL and not o.is_ok()
            ]

    def get(self, name: str) -> Optional[OrganHealth]:
        with self._organ_lock:
            return self._organs.get(name)

    def snapshot(self) -> Dict[str, dict]:
        with self._organ_lock:
            return {
                name: {
                    "status": o.status.value,
                    "criticality": o.criticality.value,
                    "heartbeat_age_s": round(o.heartbeat_age_s(), 1),
                    "stale_threshold_s": o.stale_after_s,
                    "error_count": o.error_count,
                    "last_error": o.last_error,
                    "metrics": dict(o.metrics),
                    "registered_at": round(o.registered_at, 3),
                    "last_status_change_ts": round(o.last_status_change_ts, 3),
                }
                for name, o in self._organs.items()
            }

    def compact_snapshot(self) -> Dict[str, str]:
        with self._organ_lock:
            return {name: o.status.value for name, o in self._organs.items()}

    # ------------------------------------------------------------------
    # Wiring
    # ------------------------------------------------------------------

    def set_kill_fn(self, fn: Callable[[str], None]) -> None:
        self._kill_fn = fn

    def set_alert_fn(self, fn: Callable[[str], None]) -> None:
        self._alert_fn = fn

    # ------------------------------------------------------------------
    # Internal callbacks
    # ------------------------------------------------------------------

    def _fire_kill(self, reason: str) -> None:
        if self._kill_fn:
            try:
                self._kill_fn(reason)
            except Exception as e:
                log.error("[HEALTH] kill_fn raised: %s", e)

    def _fire_alert(self, msg: str) -> None:
        if self._alert_fn:
            try:
                self._alert_fn(msg)
            except Exception as e:
                log.error("[HEALTH] alert_fn raised: %s", e)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
HEALTH = HealthRegistry()


# ---------------------------------------------------------------------------
# Optional helper: register your common organs in one call
# ---------------------------------------------------------------------------

def register_default_organs() -> None:
    """
    Call this once at startup if you want consistent organ names.
    Adjust stale_after_s as needed for your deployment.
    """
    HEALTH.register("ap_quote_monitor", Criticality.CRITICAL, stale_after_s=30)
    HEALTH.register("ap_exit_engine", Criticality.CRITICAL, stale_after_s=30)
    HEALTH.register("ap_broker", Criticality.CRITICAL, stale_after_s=60)
    HEALTH.register("ap_execution_core", Criticality.CRITICAL, stale_after_s=45)
    HEALTH.register("ap_fill_monitor", Criticality.CRITICAL, stale_after_s=45)
    HEALTH.register("ap_reconciler", Criticality.CRITICAL, stale_after_s=60)

    HEALTH.register("ap_entry_watcher", Criticality.HIGH, stale_after_s=45)
    HEALTH.register("ap_overnight_signal_manager", Criticality.HIGH, stale_after_s=120)
    HEALTH.register("ap_master_control", Criticality.HIGH, stale_after_s=60)

    HEALTH.register("ap_scanner", Criticality.NORMAL, stale_after_s=180)
    HEALTH.register("ap_reporter", Criticality.NORMAL, stale_after_s=300)
