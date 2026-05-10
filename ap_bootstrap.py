"""
ap_bootstrap.py
===============
Call bootstrap() ONCE at app startup (inside create_app() in app.py).

Wires:
- Health registry organs
- Kill switch callback into health registry
- Alert callback into health registry
- Ledger DB writer (optional)
- Health sweep thread (daemon)

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
                      payload["_table"] indicates "signal_lifecycle" or "trade_rejections".
    """
    global _bootstrapped
    with _bootstrap_lock:
        if _bootstrapped:
            return
        _bootstrapped = True

    from ap_health_registry import HEALTH, Criticality, register_default_organs
    from ap_kill_switch import KILL
    from ap_lifecycle import LEDGER

    # Register all organs with correct criticality and staleness thresholds.
    register_default_organs()

    # Wire the local kill switch into the health registry.
    # When a CRITICAL organ flatlines, health registry calls KILL.kill(reason).
    HEALTH.set_kill_fn(KILL.kill)

    # Wire Discord/Supabase alert function.
    if alert_fn:
        HEALTH.set_alert_fn(alert_fn)

    # Wire lifecycle ledger to DB persistence.
    if db_writer_fn:
        LEDGER.wire_db_writer(db_writer_fn)

    # Start the health sweep daemon thread.
    # Checks every 10 seconds for stale organs and fires kill/alert accordingly.
    t = threading.Thread(
        target=_sweep_loop,
        daemon=True,
        name="ap-health-sweep",
    )
    t.start()

    organs = HEALTH.snapshot()
    log.info(
        "[BOOTSTRAP] Angel Precision Intelligence initialized — "
        "%d organs registered | kill_switch=%s | alert_fn=%s | db_writer=%s",
        len(organs),
        "wired",
        "wired" if alert_fn else "none",
        "wired" if db_writer_fn else "none",
    )


def _sweep_loop() -> None:
    """Daemon thread: sweeps health registry every 10 seconds."""
    while True:
        try:
            from ap_health_registry import HEALTH
            HEALTH.sweep()
        except Exception as e:
            log.error("[BOOTSTRAP] sweep error: %s", e)
        time.sleep(10)
