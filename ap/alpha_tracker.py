# ap/alpha_tracker.py — Signal Decay + Pattern Attribution
# =============================================================================
# Joins signals to outcomes and measures edge quality per client.
#
# Capabilities:
#   1. Pattern attribution  — group positions by pattern; compute count,
#      win_rate, avg_pnl, profit_factor, avg_hold_minutes, status
#   2. Score-decay table    — bucket positions by score (60-70/70-80/80-90/90+)
#                             and measure win_rate per bucket
#   3. Regime detection     — SPY 5-day realized vol proxy; HIGH_VOL_REGIME
#                             if annualized vol > 25%
#   4. Recommendations      — auto-generated strings based on results
#   5. Persist              — kv table: key="alpha_attribution:{cid}:{date}"
#
# DB: ap.db conn() + run_with_retry, %s placeholders
# No ML / external stats libs — pure Python math only
# =============================================================================

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso

log = logging.getLogger("ap.alpha_tracker")

# ── Constants ─────────────────────────────────────────────────────────────────

_SCORE_BUCKETS = [
    ("60-70", 60, 70),
    ("70-80", 70, 80),
    ("80-90", 80, 90),
    ("90+",   90, float("inf")),
]

_STATUS_ELITE  = "ELITE"
_STATUS_VALID  = "VALID"
_STATUS_WEAK   = "WEAK"
_STATUS_REMOVE = "REMOVE"

_HIGH_VOL_THRESHOLD = 0.25   # 25% annualised
_REGIME_SENSITIVE_DROP = 0.20  # >20% win_rate drop → regime-sensitive


# ── Helpers ───────────────────────────────────────────────────────────────────

def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


# MED-011: removed duplicate, use now_utc_iso from ap.utils
def _now_iso() -> str:
    return now_utc_iso()


def _today_str() -> str:
    return _now_utc().strftime("%Y-%m-%d")


def _score_bucket(score: float | None) -> str | None:
    """Return the label of the score bucket, or None if out of range."""
    if score is None:
        return None
    for label, lo, hi in _SCORE_BUCKETS:
        if lo <= score < hi:
            return label
    return None


def _pattern_status(win_rate: float, profit_factor: float) -> str:
    if win_rate > 0.65 and profit_factor > 1.5:
        return _STATUS_ELITE
    if win_rate > 0.50 and profit_factor >= 1.0:
        return _STATUS_VALID
    if win_rate > 0.40:
        return _STATUS_WEAK
    return _STATUS_REMOVE


def _safe_div(numerator: float, denominator: float, fallback: float = 0.0) -> float:
    """Division with zero-division guard."""
    if denominator == 0:
        return fallback
    return numerator / denominator


def _profit_factor(trades: list[dict]) -> float:
    """Sum of winners / abs(sum of losers). Returns 0 if no losers."""
    gross_win  = sum(t["realized_pnl"] for t in trades if t["realized_pnl"] > 0)
    gross_loss = sum(abs(t["realized_pnl"]) for t in trades if t["realized_pnl"] < 0)
    return _safe_div(gross_win, gross_loss, fallback=0.0 if gross_loss > 0 else float("inf"))


def _win_rate(trades: list[dict]) -> float:
    if not trades:
        return 0.0
    wins = sum(1 for t in trades if t["realized_pnl"] > 0)
    return wins / len(trades)


def _avg_pnl(trades: list[dict]) -> float:
    if not trades:
        return 0.0
    return sum(t["realized_pnl"] for t in trades) / len(trades)


def _avg_hold(trades: list[dict]) -> float:
    vals = [t["hold_minutes"] for t in trades if t.get("hold_minutes") is not None]
    if not vals:
        return 0.0
    return sum(vals) / len(vals)


# ── SPY Regime Detection ──────────────────────────────────────────────────────

def _fetch_spy_regime() -> str:
    """
    Fetch SPY 5-day close prices via yfinance and compute realised vol.
    If annualised vol > 25% → HIGH_VOL_REGIME, else LOW_VOL_REGIME.
    Falls back to UNKNOWN on any error.
    """
    if not _YF_AVAILABLE:
        log.warning("yfinance not installed — regime detection unavailable")
        return "UNKNOWN"
    try:
        ticker = yf.Ticker("SPY")
        hist = ticker.history(period="7d", interval="1d")
        if hist is None or len(hist) < 2:
            log.warning("Insufficient SPY history for regime detection")
            return "UNKNOWN"
        closes = list(hist["Close"])
        # Daily log-returns
        returns = [
            math.log(closes[i] / closes[i - 1])
            for i in range(1, len(closes))
            if closes[i - 1] > 0
        ]
        if len(returns) < 2:
            return "UNKNOWN"
        # Sample std dev of daily log-returns
        mean_r = sum(returns) / len(returns)
        variance = sum((r - mean_r) ** 2 for r in returns) / (len(returns) - 1)
        daily_vol = math.sqrt(variance)
        annualised_vol = daily_vol * math.sqrt(252)
        log.info(
            "SPY 5d realized vol: daily=%.4f annualised=%.4f threshold=%.2f",
            daily_vol, annualised_vol, _HIGH_VOL_THRESHOLD,
        )
        return "HIGH_VOL_REGIME" if annualised_vol > _HIGH_VOL_THRESHOLD else "LOW_VOL_REGIME"
    except Exception as exc:
        log.warning("Regime detection error: %s", exc)
        return "UNKNOWN"


# ── Main class ────────────────────────────────────────────────────────────────

class APAlphaTracker:
    """
    Signal decay and pattern attribution engine for Angel Precision Bot.

    Usage:
        tracker = APAlphaTracker(supabase_client=sb)   # sb ignored; DB via ap.db
        report  = tracker.compute_attribution("user@example.com", days=90)
        tracker.persist_attribution("user@example.com", report)
        tracker.run_all_clients(["a@test.com", "b@test.com"])
    """

    # The supabase_client param is accepted for interface compatibility but
    # the implementation routes all queries through ap.db conn() / run_with_retry.
    def __init__(self, supabase_client: Any = None):
        self._sb = supabase_client

    # ── Private DB helpers ─────────────────────────────────────────────────

    def _fetch_positions(self, client_id: str, days: int = 90) -> list[dict]:
        """Return closed positions for client_id within the last `days` days."""

        sql = """
            SELECT
                p.pattern,
                p.score,
                p.tier,
                p.direction,
                p.realized_pnl,
                p.avg_fill,
                p.exit_price,
                p.qty,
                p.entry_ts,
                p.exit_ts,
                EXTRACT(EPOCH FROM (p.exit_ts - p.entry_ts)) / 60 AS hold_minutes
            FROM positions p
            WHERE p.client_id = %s
              AND p.status IN ('CLOSED', 'STOPPED', 'TAKEN_PROFIT', 'EXPIRED')
              AND p.realized_pnl IS NOT NULL
              AND p.entry_ts >= NOW() - INTERVAL '%s days'
            ORDER BY p.entry_ts DESC
        """

        def _query():
            with conn() as c:
                c.execute(sql, (client_id, days))
                return c.fetchall()

        try:
            rows = run_with_retry(_query)
            return [dict(r) for r in rows] if rows else []
        except Exception as exc:
            log.error("fetch_positions error for %s: %s", client_id, exc)
            return []

    def _fetch_positions_interval(self, client_id: str,
                                   start_ts: datetime, end_ts: datetime) -> list[dict]:
        """Return closed positions within an explicit datetime range."""

        sql = """
            SELECT
                p.pattern,
                p.score,
                p.tier,
                p.direction,
                p.realized_pnl,
                p.avg_fill,
                p.exit_price,
                p.qty,
                p.entry_ts,
                p.exit_ts,
                EXTRACT(EPOCH FROM (p.exit_ts - p.entry_ts)) / 60 AS hold_minutes
            FROM positions p
            WHERE p.client_id = %s
              AND p.status IN ('CLOSED', 'STOPPED', 'TAKEN_PROFIT', 'EXPIRED')
              AND p.realized_pnl IS NOT NULL
              AND p.entry_ts >= %s
              AND p.entry_ts < %s
            ORDER BY p.entry_ts DESC
        """

        def _query():
            with conn() as c:
                c.execute(sql, (client_id, start_ts, end_ts))
                return c.fetchall()

        try:
            rows = run_with_retry(_query)
            return [dict(r) for r in rows] if rows else []
        except Exception as exc:
            log.error("fetch_positions_interval error for %s: %s", client_id, exc)
            return []

    # ── Attribution sub-computations ───────────────────────────────────────

    def _compute_by_pattern(self, trades: list[dict]) -> dict[str, dict]:
        """Group trades by pattern and compute attribution metrics."""
        grouped: dict[str, list[dict]] = defaultdict(list)
        for t in trades:
            pattern = t.get("pattern") or "unknown"
            grouped[pattern].append(t)

        result: dict[str, dict] = {}
        for pattern, pts in grouped.items():
            wr = _win_rate(pts)
            pf = _profit_factor(pts)
            result[pattern] = {
                "count":            len(pts),
                "win_rate":         round(wr, 4),
                "avg_pnl":          round(_avg_pnl(pts), 4),
                "profit_factor":    round(pf, 4),
                "avg_hold_minutes": round(_avg_hold(pts), 2),
                "status":           _pattern_status(wr, pf),
            }
        return result

    def _compute_by_score_bucket(self, trades: list[dict]) -> dict[str, dict]:
        """Bucket trades by signal score and compute win_rate per bucket."""
        grouped: dict[str, list[dict]] = defaultdict(list)
        for t in trades:
            bucket = _score_bucket(t.get("score"))
            if bucket:
                grouped[bucket].append(t)

        result: dict[str, dict] = {}
        for label, _, _ in _SCORE_BUCKETS:
            pts = grouped.get(label, [])
            result[label] = {
                "count":    len(pts),
                "win_rate": round(_win_rate(pts), 4) if pts else 0.0,
            }
        return result

    def _compute_regime_sensitivity(
        self,
        trades: list[dict],
        regime: str,
    ) -> str:
        """
        Compare win_rate under HIGH_VOL_REGIME vs LOW_VOL_REGIME.
        Requires entry_ts datetimes on each trade.
        Returns HIGH, LOW, or UNKNOWN.
        """
        if regime == "UNKNOWN" or not trades:
            return "UNKNOWN"

        # We can only split historically if we have vol data per-bar.
        # Without historical vol per trade we classify globally:
        # If the overall regime is HIGH_VOL and overall win_rate is low → HIGH
        overall_wr = _win_rate(trades)
        if regime == "HIGH_VOL_REGIME" and overall_wr < 0.40:
            return "HIGH"
        return "LOW"

    def _compute_recommendations(
        self,
        by_pattern: dict[str, dict],
        by_score_bucket: dict[str, dict],
        regime_sensitivity: str,
    ) -> list[str]:
        """Auto-generate actionable recommendation strings."""
        recs: list[str] = []

        # Pattern-level recommendations
        for pattern, stats in by_pattern.items():
            status = stats["status"]
            if status == _STATUS_REMOVE:
                recs.append(
                    f"Remove '{pattern}' pattern — win_rate={stats['win_rate']:.0%} "
                    f"profit_factor={stats['profit_factor']:.2f}"
                )
            elif status == _STATUS_WEAK:
                recs.append(
                    f"Review '{pattern}' pattern — win_rate={stats['win_rate']:.0%} "
                    f"below 50% threshold"
                )
            elif status == _STATUS_ELITE:
                recs.append(
                    f"Increase allocation for '{pattern}' pattern — "
                    f"ELITE edge (win_rate={stats['win_rate']:.0%}, "
                    f"PF={stats['profit_factor']:.2f})"
                )

        # Score floor recommendation
        # Find lowest bucket with win_rate > 0.5; suggest floor = bottom of that bucket
        floor_recs = []
        for label, lo, _ in _SCORE_BUCKETS:
            bkt = by_score_bucket.get(label, {})
            if bkt.get("count", 0) > 0 and bkt["win_rate"] <= 0.50:
                floor_recs.append((lo, label, bkt["win_rate"]))

        if floor_recs:
            worst_lo, worst_label, worst_wr = floor_recs[-1]
            # Suggest raising floor to top of worst bucket
            # If worst is 60-70, suggest raising floor to 70
            # If worst is 70-80, suggest raising floor to 80
            suggest = worst_lo + 10 if worst_lo < 90 else 90
            recs.append(
                f"Raise score floor to {suggest} — bucket {worst_label} "
                f"win_rate={worst_wr:.0%} below 50%"
            )

        # Regime sensitivity
        if regime_sensitivity == "HIGH":
            recs.append(
                "Reduce position size during HIGH_VOL_REGIME — "
                "win_rate drops significantly in volatile markets"
            )

        if not recs:
            recs.append("No immediate action required — edge quality is acceptable")

        return recs

    # ── Public API ─────────────────────────────────────────────────────────

    def compute_attribution(
        self,
        client_id: str,
        days: int = 90,
    ) -> dict:
        """
        Compute the full attribution report for a client.

        Returns a dict with keys:
            generated_at, client_id, total_trades, by_pattern,
            by_score_bucket, regime, regime_sensitivity, recommendations
        """
        log.info("compute_attribution: client=%s days=%d", client_id, days)

        trades = self._fetch_positions(client_id, days)

        if not trades:
            log.warning("No trades found for client %s — returning empty report", client_id)
            return {
                "generated_at":       _now_iso(),
                "client_id":          client_id,
                "total_trades":       0,
                "by_pattern":         {},
                "by_score_bucket":    {
                    label: {"count": 0, "win_rate": 0.0}
                    for label, _, _ in _SCORE_BUCKETS
                },
                "regime":             "UNKNOWN",
                "regime_sensitivity": "UNKNOWN",
                "recommendations":    [],
                "message":            "No closed trades found in the requested window.",
            }

        # Regime
        regime = _fetch_spy_regime()

        # Sub-computations
        by_pattern      = self._compute_by_pattern(trades)
        by_score_bucket = self._compute_by_score_bucket(trades)
        regime_sens     = self._compute_regime_sensitivity(trades, regime)
        recommendations = self._compute_recommendations(
            by_pattern, by_score_bucket, regime_sens
        )

        report = {
            "generated_at":       _now_iso(),
            "client_id":          client_id,
            "total_trades":       len(trades),
            "by_pattern":         by_pattern,
            "by_score_bucket":    by_score_bucket,
            "regime":             regime,
            "regime_sensitivity": regime_sens,
            "recommendations":    recommendations,
        }

        log.info(
            "Attribution complete: client=%s trades=%d patterns=%d regime=%s",
            client_id, len(trades), len(by_pattern), regime,
        )
        return report

    def persist_attribution(self, client_id: str, report: dict) -> None:
        """
        Upsert the attribution report into the kv table.
        key  = "alpha_attribution:{client_id}:{YYYY-MM-DD}"
        value = JSON string of report
        """
        key   = f"alpha_attribution:{client_id}:{_today_str()}"
        value = json.dumps(report, default=str)

        sql_upsert = """
            INSERT INTO kv (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key)
            DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """

        def _write():
            with conn() as c:
                c.execute(sql_upsert, (key, value))

        try:
            run_with_retry(_write)
            log.info("persist_attribution: stored key=%s", key)
        except Exception as exc:
            log.error("persist_attribution error for %s: %s", client_id, exc)
            raise

    def run_all_clients(
        self,
        client_ids: list[str],
        days: int = 90,
        persist: bool = True,
    ) -> dict[str, dict]:
        """
        Run compute_attribution (and optionally persist) for every client_id.

        Returns a dict mapping client_id → report.
        Errors for one client are logged and do not abort others.
        """
        results: dict[str, dict] = {}
        log.info("run_all_clients: processing %d clients", len(client_ids))

        for cid in client_ids:
            try:
                report = self.compute_attribution(client_id=cid, days=days)
                if persist:
                    self.persist_attribution(cid, report)
                results[cid] = report
            except Exception as exc:
                log.error("run_all_clients: error for client %s: %s", cid, exc)
                results[cid] = {
                    "generated_at": _now_iso(),
                    "client_id":    cid,
                    "error":        str(exc),
                }

        log.info("run_all_clients: complete (%d/%d succeeded)",
                 sum(1 for r in results.values() if "error" not in r),
                 len(client_ids))
        return results
