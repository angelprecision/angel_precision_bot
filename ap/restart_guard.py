# ap/restart_guard.py -- Angel Precision | Restart Guard
# =============================================================================
# Prevents overnight signals from re-firing when the bot restarts
# during market hours.
#
# Rule:
#   Previous-day signal + market hours = SKIP
#   Previous-day signal before market open = ALLOW (morning revalidation)
#   Today's signal = ALLOW
#   No timestamp = ALLOW (log warning)
#
# Wire into queue._dispatch() as the FIRST check:
#
#   from ap.restart_guard import should_skip_on_restart
#   if should_skip_on_restart(signal):
#       log.warning("[%s] RESTART GUARD — skipping overnight signal", ticker)
#       _mark_job(job_id, "REJECTED", error="restart_guard:overnight_skip")
#       return
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

log = logging.getLogger("ap.restart_guard")

ET = ZoneInfo("America/New_York")
_BOT_START_TIME: datetime = datetime.now(timezone.utc)


def _is_market_hours_now() -> bool:
    """True if current ET time is within regular market hours (9:30–16:00 M–F)."""
    now_et = datetime.now(ET)
    return (
        now_et.weekday() < 5
        and (
            (now_et.hour > 9 or (now_et.hour == 9 and now_et.minute >= 30))
            and now_et.hour < 16
        )
    )


def should_skip_on_restart(signal: dict) -> bool:
    """
    Returns True if this signal should be skipped.

    Blocks previous-day signals during market hours (9:30–16:00 ET).
    Allows previous-day signals through before market open so morning
    revalidation can process them normally.
    """
    if not _is_market_hours_now():
        return False

    created_raw = (
        signal.get("created_at")
        or signal.get("timestamp_iso")
        or signal.get("created_ts")
        or ""
    )

    if not created_raw:
        log.warning("[RESTART_GUARD] Signal has no timestamp — allowing through")
        return False

    try:
        created_str = str(created_raw).replace("Z", "+00:00")
        created_dt  = datetime.fromisoformat(created_str)
        if created_dt.tzinfo is None:
            created_dt = created_dt.replace(tzinfo=timezone.utc)
        created_date = created_dt.astimezone(ET).date().isoformat()
    except Exception:
        log.warning("[RESTART_GUARD] Unparseable timestamp %r — allowing through", created_raw)
        return False

    today = datetime.now(ET).date().isoformat()

    if created_date < today:
        log.warning(
            "[RESTART_GUARD] Blocking overnight signal created=%s (today=%s) "
            "— bot restarted at %s during market hours",
            created_date, today,
            _BOT_START_TIME.strftime("%H:%M:%S UTC"),
        )
        return True

    return False


def get_bot_start_time() -> datetime:
    return _BOT_START_TIME


def log_startup():
    log.info(
        "[RESTART_GUARD] Bot started at %s | market_hours=%s",
        _BOT_START_TIME.strftime("%Y-%m-%d %H:%M:%S UTC"),
        _is_market_hours_now(),
    )
