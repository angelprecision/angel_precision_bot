"""
ap_bootstrap.py
===============
Call bootstrap() ONCE at app startup (inside create_app() in app.py).

Wires:
- Kill switch callback into health registry
- Alert callback into health registry
- Ledger DB writer (optional)
- Health sweep thread (daemon)

IMPORTANT: bootstrap() does NOT pre-register organs.
Each organ registers itself via HEALTH.ensure_registered() when its thread
starts. Pre-registering organs creates a startup race where the sweep marks
them STALE before they've had a chance to start, triggering the kill switch.

Usage in app.py:
    from ap_bootstrap import bootstrap
    from ap_health_endpoints import health_bp

    app = Flask(__name__)
    bootstrap(alert_fn=send_discord_alert)
    app.register_blueprint(health_bp)
"""
from __future__ import annotations

import logging
import threading
import time
from typing import Callable, Optional

log = logging.getLogger("ap.bootstrap")

_bootstrapped = False
_bootstrap_lock = threading.Lock()

# Sweep does NOT fire the kill switch until this many seconds after bootstrap.
# Gives all organs time to start their threads and register their first heartbeat.
STARTUP_GRACE_S = float(120)
_bootstrap_ts: float = 0.0


def bootstrap(
    alert_fn: Optional[Callable[[str], None]] = None,
    db_writer_fn: Optional[Callable[[dict], None]] = None,
) -> None:
    """
    Initialize the Angel Precision Intelligence infrastructure layer.

    Idempotent — safe to call multiple times; only executes once.

    Args:
        alert_fn:     Called with a message string on organ failures/stale alerts.
                      Wire to your Discord/Supabase alert system.
        db_writer_fn: Called with a dict payload on every lifecycle/rejection entry.
                      payload["_table"] = "signal_lifecycle" or "trade_rejections".
    """
    global _bootstrapped, _bootstrap_ts
    with _bootstrap_lock:
        if _bootstrapped:
            return
        _bootstrapped = True
        _bootstrap_ts = time.time()

    from ap_health_registry import HEALTH
    from ap_kill_switch import KILL
    from ap_lifecycle import LEDGER

    # Wire the local kill switch into the health registry.
    HEALTH.set_kill_fn(_guarded_kill(KILL.kill))

    # Wire Discord/Supabase alert function.
    if alert_fn:
        HEALTH.set_alert_fn(alert_fn)

    # Wire lifecycle ledger to DB persistence.
    if db_writer_fn:
        LEDGER.wire_db_writer(db_writer_fn)

    # Start the health sweep daemon thread.
    t = threading.Thread(
        target=_sweep_loop,
        daemon=True,
        name="ap-health-sweep",
    )
    t.start()

    log.info(
        "[BOOTSTRAP] Angel Precision Intelligence initialized | "
        "kill_switch=wired | alert_fn=%s | db_writer=%s | startup_grace=%ss",
        "wired" if alert_fn else "none",
        "wired" if db_writer_fn else "none",
        STARTUP_GRACE_S,
    )


def _guarded_kill(kill_fn: Callable[[str], None]) -> Callable[[str], None]:
    """
    Wrap kill_fn with a startup grace period.

    The kill switch must NOT fire during the startup grace window.
    Organs take time to start their threads and register their first heartbeat.
    Firing kill during startup would permanently block trading on every deploy.
    """
    def _wrapped(reason: str) -> None:
        if time.time() - _bootstrap_ts < STARTUP_GRACE_S:
            log.warning(
                "[BOOTSTRAP] Kill switch suppressed during startup grace (%ds): %s",
                STARTUP_GRACE_S, reason,
            )
            return
        log.critical("[BOOTSTRAP] Kill switch fired: %s", reason)
        kill_fn(reason)
    return _wrapped


def _sweep_loop() -> None:
    """Daemon thread: sweeps health registry every 30 seconds."""
    while True:
        try:
            from ap_health_registry import HEALTH
            HEALTH.sweep()
        except Exception as e:
            log.error("[BOOTSTRAP] sweep error: %s", e)
        time.sleep(30)
