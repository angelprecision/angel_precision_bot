 ap/graduation_gate.py — Paper-to-Live Graduation Gate
# =============================================================================
# Enforces that a client must prove paper-trading performance before switching
# to live mode.  All five criteria must pass for the client to be eligible.
#
# Criteria:
#   1. min_paper_trades      — >= 20 completed paper trades
#   2. positive_expectancy   — avg realized_pnl per trade > 0
#   3. max_drawdown_pct      — < 25 % (peak-to-trough on equity curve)
#   4. zero_orphaned_positions — no OPEN positions where unmanaged=TRUE
#   5. profit_factor         — gross_wins / abs(gross_losses) >= 1.0
#
# Usage:
#   gate = APGraduationGate(supabase_client=sb)
#   result = gate.evaluate(client_id="email@test.com")
#   gate.enforce(client_id, current_mode="PAPER", target_mode="LIVE")
#
# Wire into ClientRunner.run():
#   if AP_MODE == "LIVE":
#       gate.enforce(email, "PAPER", "LIVE")   # raises GraduationBlockedError
# =============================================================================

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

import ap.db as _db

log = logging.getLogger("ap.graduation_gate")

# ── Constants ─────────────────────────────────────────────────────────────────

MIN_PAPER_TRADES: int = 20
MAX_DRAWDOWN_PCT: float = 25.0
MIN_PROFIT_FACTOR: float = 1.0

# Statuses that count as a completed (closed) paper trade
_CLOSED_STATUSES = ("CLOSED", "STOPPED", "TAKEN_PROFIT", "EXPIRED")


# ── GraduationResult type alias ───────────────────────────────────────────────

GraduationResult = dict[str, Any]


# ── Custom exception ──────────────────────────────────────────────────────────


class GraduationBlockedError(Exception):
    """
    Raised by APGraduationGate.enforce() when the client has not yet met all
    graduation criteria and therefore cannot switch to LIVE mode.

    Attributes
    ----------
    result : GraduationResult
        The full evaluation result dict, so callers can inspect blocking criteria
        and surface a meaningful message to the user / Discord alert.
    """

    def __init__(self, message: str, result: GraduationResult) -> None:
        super().__init__(message)
        self.result: GraduationResult = result


# ── Helpers ───────────────────────────────────────────────────────────────────


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _compute_max_drawdown(pnl_list: list[float]) -> float:
    """
    Build an equity curve from a sorted (chronological) list of realized P&L
    values and return the maximum peak-to-trough drawdown as a percentage.

    Returns 0.0 if the list is empty or there is no drawdown.
    The result is a positive percentage (e.g. 12.3 means 12.3 % drawdown).
    """
    if not pnl_list:
        return 0.0

    equity = 0.0
    peak = 0.0
    max_dd_pct = 0.0

    for pnl in pnl_list:
        equity += pnl
        if equity > peak:
            peak = equity
        if peak > 0:
            dd_pct = (peak - equity) / peak * 100.0
            if dd_pct > max_dd_pct:
                max_dd_pct = dd_pct

    return round(max_dd_pct, 4)


def _compute_profit_factor(pnl_list: list[float]) -> float:
    """
    Gross wins / abs(gross losses).

    Returns float('inf') when there are no losing trades (perfect record).
    Returns 0.0 when there are no winning trades.
    """
    gross_wins = sum(p for p in pnl_list if p > 0)
    gross_losses = abs(sum(p for p in pnl_list if p < 0))

    if gross_losses == 0:
        return float("inf") if gross_wins > 0 else 0.0
    return round(gross_wins / gross_losses, 4)


# ── Main class ────────────────────────────────────────────────────────────────


class APGraduationGate:
    """
    Evaluates whether a client has met all paper-trading performance criteria
    required to graduate to live trading.

    Parameters
    ----------
    supabase_client :
        Optional Supabase client (currently unused for data queries — all data
        is read directly from Postgres via ap.db).  Kept in the constructor
        signature so the gate can be instantiated identically to other AP
        services and extended in the future.
    """

    def __init__(self, supabase_client: Any = None) -> None:
        self._sb = supabase_client

    # ── Internal DB helpers ───────────────────────────────────────────────────

    def _fetch_paper_trades(self, client_id: str) -> list[dict]:
        """
        Return all closed paper trades for *client_id* from the positions table.

        A row is considered a paper trade if paper_sim=TRUE (preferred) or if
        mode='paper'.  We fall back gracefully when neither column exists.
        """

        def _query() -> list[dict]:
            with _db.conn() as c:
                # Primary attempt: paper_sim column (boolean flag set by
                # execution engine when running in paper-simulation mode)
                try:
                    c.execute(
                        """
                        SELECT realized_pnl, closed_at
                        FROM   positions
                        WHERE  client_id = %s
                          AND  status    IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
                          AND  paper_sim = TRUE
                        ORDER  BY closed_at ASC NULLS LAST
                        """,
                        (client_id,),
                    )
                    rows = c.fetchall()
                    log.debug(
                        "[%s] paper_sim query returned %d rows", client_id, len(rows)
                    )
                    return rows
                except Exception as primary_err:
                    # paper_sim column may not exist — fall back to mode column
                    log.debug(
                        "[%s] paper_sim query failed (%s), trying mode='paper'",
                        client_id,
                        primary_err,
                    )

                try:
                    c.execute(
                        """
                        SELECT realized_pnl, closed_at
                        FROM   positions
                        WHERE  client_id = %s
                          AND  status    IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
                          AND  mode      = 'paper'
                        ORDER  BY closed_at ASC NULLS LAST
                        """,
                        (client_id,),
                    )
                    rows = c.fetchall()
                    log.debug(
                        "[%s] mode='paper' query returned %d rows", client_id, len(rows)
                    )
                    return rows
                except Exception as fallback_err:
                    # Neither column exists — treat all closed trades as paper
                    log.warning(
                        "[%s] mode column also unavailable (%s). "
                        "Falling back to all closed trades.",
                        client_id,
                        fallback_err,
                    )

                c.execute(
                    """
                    SELECT realized_pnl, closed_at
                    FROM   positions
                    WHERE  client_id = %s
                      AND  status    IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
                    ORDER  BY closed_at ASC NULLS LAST
                    """,
                    (client_id,),
                )
                rows = c.fetchall()
                log.debug(
                    "[%s] all-closed-trades fallback returned %d rows",
                    client_id,
                    len(rows),
                )
                return rows

        return _db.run_with_retry(_query)

    def _fetch_orphaned_count(self, client_id: str) -> int:
        """Return number of OPEN positions where unmanaged=TRUE for *client_id*."""

        def _query() -> int:
            with _db.conn() as c:
                c.execute(
                    """
                    SELECT COUNT(*) AS cnt
                    FROM   positions
                    WHERE  client_id  = %s
                      AND  status     = 'OPEN'
                      AND  unmanaged  = TRUE
                    """,
                    (client_id,),
                )
                row = c.fetchone()
                return int(row["cnt"]) if row else 0

        return _db.run_with_retry(_query)

    # ── Public API ────────────────────────────────────────────────────────────

    def evaluate(self, client_id: str) -> GraduationResult:
        """
        Run all five graduation criteria for *client_id* and return a
        GraduationResult dict.

        The result contains:
          - eligible          : bool  — True only when ALL criteria pass
          - score             : int   — number of criteria that passed
          - criteria          : dict  — per-criterion detail
          - blocking_criteria : list  — names of criteria that did NOT pass
          - message           : str   — human-readable summary
          - paper_trades      : int   — total completed paper trades found
          - evaluated_at      : str   — ISO-8601 UTC timestamp
        """
        log.info("[%s] Starting graduation evaluation", client_id)

        # ── Fetch raw data ────────────────────────────────────────────────────
        try:
            trade_rows = self._fetch_paper_trades(client_id)
        except Exception as exc:
            log.error("[%s] DB error fetching paper trades: %s", client_id, exc)
            trade_rows = []

        try:
            orphaned_count = self._fetch_orphaned_count(client_id)
        except Exception as exc:
            log.error("[%s] DB error fetching orphaned positions: %s", client_id, exc)
            orphaned_count = 0

        # ── Build P&L list (None/NULL rows treated as 0) ─────────────────────
        pnl_list: list[float] = [
            float(row.get("realized_pnl") or 0) for row in trade_rows
        ]
        num_trades = len(pnl_list)

        # ── Criterion 1: min_paper_trades ─────────────────────────────────────
        c1_value = num_trades
        c1_passed = c1_value >= MIN_PAPER_TRADES

        # ── Criterion 2: positive_expectancy ─────────────────────────────────
        if num_trades > 0:
            c2_value = round(sum(pnl_list) / num_trades, 4)
        else:
            c2_value = 0.0
        c2_passed = c2_value > 0

        # ── Criterion 3: max_drawdown_pct ─────────────────────────────────────
        c3_value = _compute_max_drawdown(pnl_list)
        c3_passed = c3_value < MAX_DRAWDOWN_PCT

        # ── Criterion 4: zero_orphaned_positions ─────────────────────────────
        c4_value = orphaned_count
        c4_passed = c4_value == 0

        # ── Criterion 5: profit_factor ────────────────────────────────────────
        c5_value = _compute_profit_factor(pnl_list)
        c5_display = c5_value if c5_value != float("inf") else 9999.0
        c5_passed = c5_value >= MIN_PROFIT_FACTOR

        # ── Aggregate ─────────────────────────────────────────────────────────
        criteria: dict[str, dict] = {
            "min_paper_trades": {
                "passed":   c1_passed,
                "value":    c1_value,
                "required": MIN_PAPER_TRADES,
            },
            "positive_expectancy": {
                "passed":   c2_passed,
                "value":    c2_value,
                "required": ">0",
            },
            "max_drawdown_pct": {
                "passed":   c3_passed,
                "value":    c3_value,
                "required": f"<{MAX_DRAWDOWN_PCT}",
            },
            "zero_orphaned_positions": {
                "passed":   c4_passed,
                "value":    c4_value,
                "required": 0,
            },
            "profit_factor": {
                "passed":   c5_passed,
                "value":    round(c5_display, 4),
                "required": f">={MIN_PROFIT_FACTOR}",
            },
        }

        blocking: list[str] = [k for k, v in criteria.items() if not v["passed"]]
        score = len(criteria) - len(blocking)
        eligible = len(blocking) == 0

        if eligible:
            message = (
                f"Eligible for live trading. "
                f"All {len(criteria)} criteria passed. "
                f"Paper trades evaluated: {num_trades}."
            )
        else:
            details = []
            for name in blocking:
                c = criteria[name]
                details.append(f"{name} {c['value']} (required {c['required']})")
            message = f"Not eligible: {'; '.join(details)}"

        result: GraduationResult = {
            "eligible":           eligible,
            "score":              score,
            "criteria":           criteria,
            "blocking_criteria":  blocking,
            "message":            message,
            "paper_trades":       num_trades,
            "evaluated_at":       _now_iso(),
        }

        log.info(
            "[%s] Graduation evaluation complete — eligible=%s score=%d/%d blocking=%s",
            client_id,
            eligible,
            score,
            len(criteria),
            blocking,
        )
        return result

    def enforce(
        self,
        client_id: str,
        current_mode: str = "PAPER",
        target_mode: str = "LIVE",
    ) -> bool:
        """
        Call before switching a client from *current_mode* to *target_mode*.

        Returns True if the client is eligible to switch.
        Raises GraduationBlockedError (with result attached) if not eligible.

        This is a no-op (always returns True) when *target_mode* is not 'LIVE'.
        """
        if target_mode.upper() != "LIVE":
            log.debug(
                "[%s] enforce() called for non-LIVE target (%s → %s) — skipping",
                client_id,
                current_mode,
                target_mode,
            )
            return True

        log.info(
            "[%s] Enforcing graduation gate before %s → %s transition",
            client_id,
            current_mode,
            target_mode,
        )

        result = self.evaluate(client_id)

        if not result["eligible"]:
            msg = (
                f"[{client_id}] Graduation blocked ({current_mode} → {target_mode}): "
                f"{result['message']}"
            )
            log.error(msg)
            raise GraduationBlockedError(msg, result)

        log.info(
            "[%s] Graduation gate PASSED — clearing %s → %s transition",
            client_id,
            current_mode,
            target_mode,
        )
        return True
