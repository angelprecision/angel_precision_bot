# ap/exit_price_sync.py -- Angel Precision | Exit Price Dashboard Sync
# =============================================================================
# Called after every confirmed exit fill to update proof_trades on the
# dashboard with the actual broker avg_fill price instead of the limit price.
#
# Wire into ap/order_monitor.py _advance_from_broker_status()
# when kind == EXIT and new_status == EXIT_FILLED
# =============================================================================

from __future__ import annotations

import logging
import os
from typing import Optional

import requests

log = logging.getLogger("ap.exit_price_sync")

DASHBOARD_URL   = (os.getenv("AP_DASHBOARD_URL") or "").rstrip("/")
INTERNAL_SECRET = os.getenv("INTERNAL_SECRET", "")


def sync_exit_price_to_dashboard(
    position_id: str,
    exit_avg_fill: float,
    entry_price: Optional[float] = None,
    ticker: str = "",
) -> bool:
    """
    Notify the dashboard of the real broker exit fill price.
    Updates proof_trades.exit_option_price and recalculates P&L.

    Call this once per confirmed exit fill — after OSM transitions
    to EXIT_FILLED and avg_fill is known from broker.

    Returns True if update succeeded, False otherwise.
    Non-blocking — all exceptions are caught so this never crashes the bot.
    """
    if not DASHBOARD_URL or not INTERNAL_SECRET:
        log.debug("[EXIT_SYNC] Disabled — missing AP_DASHBOARD_URL or INTERNAL_SECRET")
        return False

    if not position_id or exit_avg_fill is None:
        log.debug("[EXIT_SYNC] Missing position_id or exit_avg_fill — skipping")
        return False

    payload = {
        "secret":      INTERNAL_SECRET,
        "trade_id":    position_id,
        "avg_fill":    round(float(exit_avg_fill), 4),
        "entry_price": round(float(entry_price), 4) if entry_price else None,
    }

    try:
        resp = requests.post(
            f"{DASHBOARD_URL}/api/proof/update_exit_price",
            json=payload,
            timeout=3,
        )
        if resp.ok:
            j = resp.json()
            log.info(
                "[EXIT_SYNC] %s | updated=%s avg_fill=%.4f pnl=%s",
                ticker or position_id,
                j.get("updated"),
                exit_avg_fill,
                j.get("option_pnl_pct", "?"),
            )
            return True
        else:
            log.warning(
                "[EXIT_SYNC] %s | status=%s body=%s",
                ticker or position_id,
                resp.status_code,
                resp.text[:100],
            )
            return False
    except requests.exceptions.Timeout:
        log.debug("[EXIT_SYNC] %s | timeout (non-critical)", ticker or position_id)
    except Exception as e:
        log.debug("[EXIT_SYNC] %s | error=%s (non-critical)", ticker or position_id, e)
    return False
