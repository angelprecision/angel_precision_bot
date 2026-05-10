"""
ap_health_registry.py
=====================
Every organ registers here. Reports heartbeats. Brain checks before trading.
Kill switch fires if a CRITICAL organ flatlines.
"""
from __future__ import annotations

import os
import time
import threading
import logging
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, Optional, Callable, List

log = logging.getLogger("ap.health")


class HealthStatus(str, Enum):
    BOOTING = "BOOTING"
    HEALTHY = "HEALTHY"
    DEGRADED = "DEGRADED"
    STALE = "STALE"
    FAILED = "FAILED"
    STOPPED = "STOPPED"


class Criticality(str, Enum):
    CRITICAL = "CRITICAL"  # QPM, Exit Engine, Broker — halt if dead
    HIGH = "HIGH"          # OSM, Watcher, Reconciler — alert if dead
    NORMAL = "NORMAL"      # Scanner, Reports — log only


@dataclass
class OrganHealth:
    name: str
    criticality: Criticality
    status: HealthStatus = HealthStatus.BOOTING
    last_heartbeat_ts: float = 0.0
    stale_after_s: float = 60.0
    last_error: Optional[str] = None
    metrics: Dict[str, float] = field(default_factory=dict)
    error_count: int = 0

    def is_stale(self) -> bool:
        if self.status in (HealthStatus.BOOTING, HealthStatus.STOPPED):
            return False
        return (time.time() - self.last_heartbeat_ts) > self.stale_after_s


class HealthRegistry:
    _instance: Optional["HealthRegistry"] = None
    _lock = threading.Lock()

    def __new__(cls):
        with cls._lock:
            if cls._instance is None:
                inst = super().__new__(cls)
                inst._organs: Dict[str, OrganHealth] = {}
                inst._organ_lock = threading.RLock()
                inst._kill_fn: Optional[Callable] = None
                inst._alert_fn: Optional[Callable] = None
                cls._instance = inst
            return cls._instance

    def register(self, name: str, criticality: Criticality, stale_after_s: float = 60.0) -> None:
        with self._organ_lock:
            self._organs[name] = OrganHealth(
                name=name, criticality=criticality,
                stale_after_s=stale_after_s,
                last_heartbeat_ts=time.time(),
            )
            log.info("[HEALTH] Registered: %s (%s)", name, criticality.value)

    def heartbeat(self, name: str, metrics: Optional[Dict[str, float]] = None) -> None:
        with self._organ_lock:
            organ = self._organs.get(name)
            if not organ:
                return
            organ.last_heartbeat_ts = time.time()
            if organ.status in (HealthStatus.BOOTING, HealthStatus.STALE, HealthStatus.DEGRADED):
                organ.status = HealthStatus.HEALTHY
            if metrics:
                organ.metrics.update(metrics)

    def report_error(self, name: str, error: str, fatal: bool = False) -> None:
        with self._organ_lock:
            organ = self._organs.get(name)
            if not organ:
                return
            organ.last_error = error
            organ.error_count += 1
            organ.status = HealthStatus.FAILED if fatal else HealthStatus.DEGRADED
            log.error("[HEALTH] %s %s: %s", name, "FATAL" if fatal else "ERROR", error)
            if fatal and organ.criticality == Criticality.CRITICAL and self._kill_fn:
                self._kill_fn(f"{name} FATAL: {error}")
            if self._alert_fn:
                try:
                    self._alert_fn(f"{'🚨' if fatal else '⚠️'} {name}: {error}")
                except Exception:
                    pass

    def sweep(self) -> List[str]:
        """Call every 10s from a monitor thread. Returns stale organs."""
        sick = []
        with self._organ_lock:
            for organ in self._organs.values():
                if organ.is_stale() and organ.status != HealthStatus.STALE:
                    organ.status = HealthStatus.STALE
                    sick.append(organ.name)
                    log.error("[HEALTH] STALE: %s (%.0fs since heartbeat)",
                              organ.name, time.time() - organ.last_heartbeat_ts)
                    if organ.criticality == Criticality.CRITICAL and self._kill_fn:
                        self._kill_fn(f"{organ.name} STALE — no heartbeat")
        return sick

    def is_system_healthy(self) -> bool:
        with self._organ_lock:
            for o in self._organs.values():
                if o.criticality == Criticality.CRITICAL and o.status not in (
                    HealthStatus.HEALTHY, HealthStatus.BOOTING
                ):
                    return False
        return True

    def snapshot(self) -> Dict[str, dict]:
        with self._organ_lock:
            return {
                name: {
                    "status": o.status.value,
                    "criticality": o.criticality.value,
                    "heartbeat_age_s": round(time.time() - o.last_heartbeat_ts, 1),
                    "error_count": o.error_count,
                    "last_error": o.last_error,
                    "metrics": o.metrics,
                }
                for name, o in self._organs.items()
            }

    def set_kill_fn(self, fn: Callable[[str], None]) -> None:
        self._kill_fn = fn

    def set_alert_fn(self, fn: Callable[[str], None]) -> None:
        self._alert_fn = fn


HEALTH = HealthRegistry()
