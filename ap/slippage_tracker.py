# ap/slippage_tracker.py — APSlippageTracker
# =============================================================================
# Tracks fill quality (slippage) for every trade.
#
# Computes per-fill:
#   expected_entry  : mid price at signal time (passed in by caller)
#   slippage_pct    : (actual_fill - expected_entry) / expected_entry * 100
#   slippage_usd    : (actual_fill - expected_entry) * qty * 100
#   tod_bucket      : PRE | OPEN | MID | CLOSE  (based on ET wall-clock time)
#
# Time-of-day buckets (Eastern Time):
#   PRE   : before 09:30
#   OPEN  : 09:30 – 10:30  (first hour)
#   MID   : 10:30 – 14:00
#   CLOSE : 14:00 – 16:00
#   (any other time falls through to "PRE" for simplicity — pre/after-hours)
#
# All DB writes go through ap.db conn() + run_with_retry with %s placeholders.
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Optional

from zoneinfo import ZoneInfo

from ap.db import conn, run_with_retry

log = logging.getLogger("ap.slippage_tracker")

_ET = ZoneInfo("America/New_York")


# ── Time-of-day bucket ────────────────────────────────────────────────────────

def _tod_bucket(entry_ts: str) -> str:
    """
    Convert a UTC ISO-8601 timestamp string to an Eastern-time wall-clock
    bucket.

    Args:
        entry_ts: ISO-8601 string in UTC, e.g. "2026-04-12T10:45:00Z"

    Returns:
        One of "PRE", "OPEN", "MID", "CLOSE".
    """
    # Parse — accept trailing Z or +00:00
    ts = entry_ts.replace("Z", "+00:00")
    dt_utc = datetime.fromisoformat(ts)
    if dt_utc.tzinfo is None:
        dt_utc = dt_utc.replace(tzinfo=timezone.utc)

    dt_et = dt_utc.astimezone(_ET)
    hhmm = dt_et.hour * 60 + dt_et.minute   # minutes since midnight ET

    if hhmm < 9 * 60 + 30:          # before 09:30
        return "PRE"
    elif hhmm < 10 * 60 + 30:       # 09:30 – 10:29
        return "OPEN"
    elif hhmm < 14 * 60:             # 10:30 – 13:59
        return "MID"
    elif hhmm < 16 * 60:             # 14:00 – 15:59
        return "CLOSE"
    else:                             # after 16:00 (post-market)
        return "PRE"


# =============================================================================
# MAIN CLASS
# =============================================================================

class APSlippageTracker:
    """
    Records fill slippage metrics onto the positions table and exposes
    analytics aggregated by ticker and time-of-day bucket.

    Usage::

        tracker = APSlippageTracker()

        # On FILLED transition:
        tracker.record_fill(
            position_id="uuid",
            client_id="user@example.com",
            ticker="AAPL",
            expected_entry=3.50,
            actual_fill=3.65,
            qty=2,
            entry_ts="2026-04-12T10:45:00Z",
        )

        # Analytics (last N days):
        report = tracker.slippage_report(client_id="user@example.com", days=30)
    """

    # ── record_fill ───────────────────────────────────────────────────────────

    def record_fill(
        self,
        position_id: str,
        client_id: str,
        ticker: str,
        expected_entry: float,
        actual_fill: float,
        qty: int,
        entry_ts: str,
    ) -> None:
        """
        Compute slippage metrics and persist them to the positions row.

        Args:
            position_id:    UUID of the position row to update.
            client_id:      Client e-mail / identifier (used for logging).
            ticker:         Underlying ticker symbol, e.g. "AAPL".
            expected_entry: Mid price at signal time (contract mid or
                            underlying_at_signal).
            actual_fill:    Average fill price from the broker.
            qty:            Number of contracts filled.
            entry_ts:       UTC ISO-8601 fill timestamp, e.g.
                            "2026-04-12T10:45:00Z".
        """
        # ── Compute metrics ─────────────────────────────────────────────────
        if expected_entry and expected_entry != 0:
            slippage_pct = (actual_fill - expected_entry) / expected_entry * 100.0
        else:
            slippage_pct = 0.0

        slippage_usd = (actual_fill - expected_entry) * qty * 100.0
        bucket = _tod_bucket(entry_ts)

        log.info(
            "record_fill position_id=%s ticker=%s client=%s "
            "expected=%.4f actual=%.4f qty=%d "
            "slippage_pct=%.4f slippage_usd=%.2f bucket=%s",
            position_id, ticker, client_id,
            expected_entry, actual_fill, qty,
            slippage_pct, slippage_usd, bucket,
        )

        # ── Persist ─────────────────────────────────────────────────────────
        def _fn() -> None:
            with conn() as c:
                c.execute(
                    """
                    UPDATE positions
                       SET expected_entry = %s,
                           slippage_pct   = %s,
                           slippage_usd   = %s,
                           tod_bucket     = %s
                     WHERE position_id = %s
                    """,
                    (
                        expected_entry,
                        round(slippage_pct, 6),
                        round(slippage_usd, 4),
                        bucket,
                        position_id,
                    ),
                )

        run_with_retry(_fn)
        log.debug(
            "Slippage persisted position_id=%s bucket=%s", position_id, bucket
        )

    # ── slippage_report ───────────────────────────────────────────────────────

    def slippage_report(
        self,
        client_id: str,
        days: int = 30,
    ) -> dict[str, Any]:
        """
        Return aggregated slippage analytics for a client over the last N days.

        Returns a dict with::

            {
                "avg_slippage_pct":  float,
                "total_slippage_usd": float,
                "worst_ticker":      str | None,
                "worst_tod":         str | None,
                "by_ticker": {
                    "<TICKER>": {"avg_pct": float, "count": int},
                    ...
                },
                "by_tod": {
                    "<BUCKET>": {"avg_pct": float, "count": int},
                    ...
                },
            }
        """
        def _fetch() -> dict[str, Any]:
            with conn() as c:
                # ── Per-ticker aggregation ───────────────────────────────────
                c.execute(
                    """
                    SELECT ticker,
                           AVG(slippage_pct)    AS avg_pct,
                           COUNT(*)             AS cnt
                      FROM positions
                     WHERE client_id   = %s
                       AND slippage_pct IS NOT NULL
                       AND entry_ts >= NOW() - (%s || ' days')::INTERVAL
                     GROUP BY ticker
                    """,
                    (client_id, str(days)),
                )
                ticker_rows = c.fetchall() or []

                # ── Per-bucket aggregation ───────────────────────────────────
                c.execute(
                    """
                    SELECT tod_bucket,
                           AVG(slippage_pct)    AS avg_pct,
                           COUNT(*)             AS cnt
                      FROM positions
                     WHERE client_id    = %s
                       AND slippage_pct  IS NOT NULL
                       AND tod_bucket    IS NOT NULL
                       AND entry_ts >= NOW() - (%s || ' days')::INTERVAL
                     GROUP BY tod_bucket
                    """,
                    (client_id, str(days)),
                )
                tod_rows = c.fetchall() or []

                # ── Overall aggregation ──────────────────────────────────────
                c.execute(
                    """
                    SELECT AVG(slippage_pct)  AS avg_pct,
                           SUM(slippage_usd)  AS total_usd
                      FROM positions
                     WHERE client_id   = %s
                       AND slippage_pct IS NOT NULL
                       AND entry_ts >= NOW() - (%s || ' days')::INTERVAL
                    """,
                    (client_id, str(days)),
                )
                overall = c.fetchone() or {}

            return {
                "ticker_rows": list(ticker_rows),
                "tod_rows":    list(tod_rows),
                "overall":     dict(overall) if overall else {},
            }

        raw = run_with_retry(_fetch)

        # ── Build output dict ────────────────────────────────────────────────
        ticker_rows = raw.get("ticker_rows", [])
        tod_rows    = raw.get("tod_rows", [])
        overall     = raw.get("overall", {})

        by_ticker: dict[str, dict[str, Any]] = {}
        for row in ticker_rows:
            ticker  = row.get("ticker") or ""
            avg_pct = float(row.get("avg_pct") or 0.0)
            count   = int(row.get("cnt") or 0)
            by_ticker[ticker] = {"avg_pct": round(avg_pct, 4), "count": count}

        by_tod: dict[str, dict[str, Any]] = {}
        for row in tod_rows:
            bucket  = row.get("tod_bucket") or ""
            avg_pct = float(row.get("avg_pct") or 0.0)
            count   = int(row.get("cnt") or 0)
            by_tod[bucket] = {"avg_pct": round(avg_pct, 4), "count": count}

        # worst ticker = highest avg slippage_pct (most positive = most cost)
        worst_ticker: Optional[str] = None
        if by_ticker:
            worst_ticker = max(by_ticker, key=lambda t: by_ticker[t]["avg_pct"])

        # worst tod bucket = highest avg slippage_pct
        worst_tod: Optional[str] = None
        if by_tod:
            worst_tod = max(by_tod, key=lambda b: by_tod[b]["avg_pct"])

        avg_slippage_pct  = float(overall.get("avg_pct") or 0.0)
        total_slippage_usd = float(overall.get("total_usd") or 0.0)

        report: dict[str, Any] = {
            "avg_slippage_pct":   round(avg_slippage_pct, 4),
            "total_slippage_usd": round(total_slippage_usd, 2),
            "worst_ticker":       worst_ticker,
            "worst_tod":          worst_tod,
            "by_ticker":          by_ticker,
            "by_tod":             by_tod,
        }

        log.info(
            "slippage_report client=%s days=%d avg_pct=%.4f total_usd=%.2f "
            "worst_ticker=%s worst_tod=%s",
            client_id, days,
            report["avg_slippage_pct"],
            report["total_slippage_usd"],
            worst_ticker,
            worst_tod,
        )

        return report
