# ap/position_manager.py — APPositionManager
# =============================================================================
# Client-relative position truth backed by Supabase Postgres.
#
# Aligned to actual positions table schema:
#   id, client_id, underlying, contract, direction, qty, avg_fill,
#   entry_ts, tp_pct, sl_pct, status, exit_ts, exit_reason, realized_pnl,
#   stop_underlying, target_underlying, plan_id, signal_id, score, tier,
#   pattern, exit_price, created_at, updated_at
#
# Used by:
#   - APMasterControl.evaluate() → snapshot(), open_count(), has_open_position()
#   - APExecutionCore            → open_position() on fill
#   - APExitEngine               → close_position() on exit
#   - Dashboard API              → snapshot(), get_all_positions()
# =============================================================================

from __future__ import annotations

import uuid
import logging
from datetime import datetime, timezone
from typing import Optional

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso

log = logging.getLogger("ap.position_manager")


# =============================================================================
# STATUS CONSTANTS
# =============================================================================

class PositionStatus:
    OPEN         = "OPEN"
    CLOSING      = "CLOSING"
    CLOSED       = "CLOSED"
    EXPIRED      = "EXPIRED"
    STOPPED      = "STOPPED"
    TAKEN_PROFIT = "TAKEN_PROFIT"
    ERROR        = "ERROR"

    # Still represent real risk / capital exposure
    ACTIVE   = {"OPEN", "CLOSING"}
    # Fully settled — slot is free
    TERMINAL = {"CLOSED", "EXPIRED", "STOPPED", "TAKEN_PROFIT", "ERROR"}

    @classmethod
    def is_terminal(cls, status: str) -> bool:
        return status in cls.TERMINAL

    @classmethod
    def is_active(cls, status: str) -> bool:
        return status in cls.ACTIVE


# Pending entry order statuses — must match OrderStatus in ap/order_state_machine.py exactly
_PENDING_ENTRY_STATUSES = ("CREATED", "SUBMITTED", "ACKNOWLEDGED", "PARTIAL_FILL")
_PENDING_EXIT_STATUSES  = ("EXIT_REQUESTED", "EXIT_SUBMITTED",
                            "EXIT_ACKNOWLEDGED", "EXIT_PARTIAL_FILL")


# =============================================================================
# POSITION MANAGER
# =============================================================================

class APPositionManager:
    """
    Client-scoped position + order truth layer backed by Supabase Postgres.
    One instance per ClientRunner. Injected into APMasterControl + APExecutionCore.
    """

    def __init__(self, client_id: str):
        self.client_id = client_id
        log.info(f"[{client_id}] APPositionManager initialized")

    # =========================================================================
    # READ — position counts (OPEN + CLOSING = active exposure)
    # =========================================================================

    def open_count(self) -> int:
        """
        Active position count for master control gate.
        Includes CLOSING — those are still real risk until exit fills.
        """
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT COUNT(*) AS n FROM positions "
                    "WHERE client_id=%s AND status IN ('OPEN','CLOSING')",
                    (self.client_id,),
                )
                return int((c.fetchone() or {}).get("n") or 0)
        return run_with_retry(_fn)

    def has_open_position(self, ticker: str) -> bool:
        """
        True if client has OPEN or CLOSING exposure in this underlying.
        Includes CLOSING — prevents re-entry before exit is confirmed.
        """
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT 1 FROM positions "
                    "WHERE client_id=%s AND underlying=%s "
                    "AND status IN ('OPEN','CLOSING') LIMIT 1",
                    (self.client_id, ticker.upper()),
                )
                return c.fetchone() is not None
        return run_with_retry(_fn)

    def get_open_positions(self) -> list[dict]:
        """All OPEN positions for this client, newest first."""
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM positions "
                    "WHERE client_id=%s AND status='OPEN' "
                    "ORDER BY entry_ts DESC",
                    (self.client_id,),
                )
                return c.fetchall()
        return run_with_retry(_fn)

    def get_active_positions(self) -> list[dict]:
        """OPEN + CLOSING — full active exposure."""
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM positions "
                    "WHERE client_id=%s AND status IN ('OPEN','CLOSING') "
                    "ORDER BY entry_ts DESC",
                    (self.client_id,),
                )
                return c.fetchall()
        return run_with_retry(_fn)

    def get_position(self, position_id: str) -> Optional[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM positions WHERE id=%s AND client_id=%s",
                    (position_id, self.client_id),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def get_position_by_contract(self, contract: str) -> Optional[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM positions "
                    "WHERE client_id=%s AND contract=%s "
                    "AND status IN ('OPEN','CLOSING') LIMIT 1",
                    (self.client_id, contract),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def get_position_by_signal(self, signal_id: str) -> Optional[dict]:
        """Dedup guard — same signal already opened a position."""
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM positions "
                    "WHERE client_id=%s AND signal_id=%s LIMIT 1",
                    (self.client_id, signal_id),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def get_position_by_plan(self, plan_id: str) -> Optional[dict]:
        """Dedup guard — same plan already opened a position."""
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM positions "
                    "WHERE client_id=%s AND plan_id=%s LIMIT 1",
                    (self.client_id, plan_id),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def get_all_positions(self, limit: int = 200,
                          status: str | None = None) -> list[dict]:
        def _fn():
            with conn() as c:
                if status:
                    c.execute(
                        "SELECT * FROM positions "
                        "WHERE client_id=%s AND status=%s "
                        "ORDER BY entry_ts DESC LIMIT %s",
                        (self.client_id, status, limit),
                    )
                else:
                    c.execute(
                        "SELECT * FROM positions WHERE client_id=%s "
                        "ORDER BY entry_ts DESC LIMIT %s",
                        (self.client_id, limit),
                    )
                return c.fetchall()
        return run_with_retry(_fn)

    # =========================================================================
    # PENDING ORDER AWARENESS
    # =========================================================================

    def pending_entry_count(self) -> int:
        """
        In-flight entry orders that have not yet filled.
        Only counts CREATED orders with no broker_order_id (truly unsubmitted).
        SUBMITTED/ACKNOWLEDGED with a broker_id = real order at broker = already
        counted as a position slot via the WATCHING mechanism.
        Excludes them to prevent WATCHING signals from inflating the position cap.
        """
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT COUNT(*) AS n FROM orders "
                    "WHERE client_id=%s AND kind='ENTRY' "
                    "AND status = 'CREATED' "
                    "AND (broker_order_id IS NULL OR broker_order_id = '')",
                    (self.client_id,),
                )
                return int((c.fetchone() or {}).get("n") or 0)
        return run_with_retry(_fn)

    def pending_exit_count(self) -> int:
        """Exit orders submitted but not yet confirmed filled."""
        def _fn():
            with conn() as c:
                placeholders = ",".join(["%s"] * len(_PENDING_EXIT_STATUSES))
                c.execute(
                    f"SELECT COUNT(*) AS n FROM orders "
                    f"WHERE client_id=%s AND kind='EXIT' "
                    f"AND status IN ({placeholders})",
                    (self.client_id, *_PENDING_EXIT_STATUSES),
                )
                return int((c.fetchone() or {}).get("n") or 0)
        return run_with_retry(_fn)

    def has_pending_entry(self, ticker: str) -> bool:
        """
        True if there is an in-flight entry order for this underlying.
        Prevents double-entry when position not yet opened but order is live.
        """
        def _fn():
            with conn() as c:
                placeholders = ",".join(["%s"] * len(_PENDING_ENTRY_STATUSES))
                c.execute(
                    f"SELECT 1 FROM orders "
                    f"WHERE client_id=%s AND symbol=%s AND kind='ENTRY' "
                    f"AND status IN ({placeholders}) LIMIT 1",
                    (self.client_id, ticker.upper(), *_PENDING_ENTRY_STATUSES),
                )
                return c.fetchone() is not None
        return run_with_retry(_fn)

    def has_pending_exit(self, position_id: str) -> bool:
        """True if an exit order is already in-flight for this position."""
        def _fn():
            with conn() as c:
                placeholders = ",".join(["%s"] * len(_PENDING_EXIT_STATUSES))
                c.execute(
                    f"SELECT 1 FROM orders "
                    f"WHERE client_id=%s AND position_id=%s AND kind='EXIT' "
                    f"AND status IN ({placeholders}) LIMIT 1",
                    (self.client_id, position_id, *_PENDING_EXIT_STATUSES),
                )
                return c.fetchone() is not None
        return run_with_retry(_fn)

    # =========================================================================
    # EXPOSURE BREAKDOWN
    # =========================================================================

    def open_tickers(self) -> set[str]:
        """Active underlying tickers (OPEN + CLOSING)."""
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT DISTINCT underlying FROM positions "
                    "WHERE client_id=%s AND status IN ('OPEN','CLOSING')",
                    (self.client_id,),
                )
                return {row["underlying"] for row in c.fetchall()}
        return run_with_retry(_fn)

    def open_count_for_ticker(self, ticker: str) -> int:
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT COUNT(*) AS n FROM positions "
                    "WHERE client_id=%s AND underlying=%s "
                    "AND status IN ('OPEN','CLOSING')",
                    (self.client_id, ticker.upper()),
                )
                return int((c.fetchone() or {}).get("n") or 0)
        return run_with_retry(_fn)

    def open_count_for_direction(self, direction: str) -> int:
        """Active CALL or PUT count."""
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT COUNT(*) AS n FROM positions "
                    "WHERE client_id=%s AND direction=%s "
                    "AND status IN ('OPEN','CLOSING')",
                    (self.client_id, direction.upper()),
                )
                return int((c.fetchone() or {}).get("n") or 0)
        return run_with_retry(_fn)

    def capital_deployed(self) -> float:
        """Total USD in active (OPEN+CLOSING) positions. avg_fill * qty * 100."""
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT COALESCE(SUM(avg_fill * qty * 100), 0) AS deployed "
                    "FROM positions "
                    "WHERE client_id=%s AND status IN ('OPEN','CLOSING')",
                    (self.client_id,),
                )
                return float((c.fetchone() or {}).get("deployed") or 0)
        return run_with_retry(_fn)

    # =========================================================================
    # WRITE — open, update, close
    # =========================================================================

    def open_position(
        self,
        *,
        plan_id:           str,
        signal_id:         str,
        ticker:            str,
        contract:          str,
        side:              str,
        qty:               int,
        entry_price:       float,
        tier:              str   = "B",
        score:             float = 0.0,
        pattern:           str   = "",
        tp_pct:            float = 0.20,
        sl_pct:            float = 0.35,
        stop_underlying:   Optional[float] = None,
        target_underlying: Optional[float] = None,
    ) -> str:
        """
        Insert a new OPEN position. Returns position_id.
        Guards against duplicate opens from same plan_id or signal_id.
        """
        existing = self.get_position_by_plan(plan_id)
        if existing:
            log.warning(f"[{self.client_id}] open_position SKIPPED — plan {plan_id} already exists as {existing['id']}")
            return existing["id"]

        existing = self.get_position_by_signal(signal_id)
        if existing:
            log.warning(f"[{self.client_id}] open_position SKIPPED — signal {signal_id} already exists as {existing['id']}")
            return existing["id"]

        position_id = str(uuid.uuid4())
        ts = now_utc_iso()

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    INSERT INTO positions (
                        id, client_id, plan_id, signal_id,
                        underlying, contract, direction, qty, avg_fill,
                        tier, score, pattern,
                        tp_pct, sl_pct,
                        stop_underlying, target_underlying,
                        status, entry_ts, created_at, updated_at
                    ) VALUES (
                        %s,%s,%s,%s,
                        %s,%s,%s,%s,%s,
                        %s,%s,%s,
                        %s,%s,
                        %s,%s,
                        'OPEN',%s,%s,%s
                    )
                    ON CONFLICT (id) DO NOTHING
                    """,
                    (
                        position_id, self.client_id, plan_id, signal_id,
                        ticker.upper(), contract, side.upper(), int(qty),
                        float(entry_price),
                        tier, float(score), pattern,
                        float(tp_pct), float(sl_pct),
                        float(stop_underlying) if stop_underlying else None,
                        float(target_underlying) if target_underlying else None,
                        ts, ts, ts,
                    ),
                )
        run_with_retry(_fn)
        log.info(
            f"[{self.client_id}] POSITION OPENED | {ticker} {contract} "
            f"x{qty} @ ${entry_price:.2f} | id={position_id} tier={tier}"
        )
        return position_id

    def update_position(
        self,
        position_id: str,
        *,
        status:       Optional[str]   = None,
        exit_price:   Optional[float] = None,
        realized_pnl: Optional[float] = None,
        exit_reason:  Optional[str]   = None,
        exit_ts:      Optional[str]   = None,
    ):
        """Generic update. Always stamps updated_at."""
        updates = ["updated_at=NOW()"]
        params  = []
        if status is not None:
            updates.append("status=%s");       params.append(status)
        if exit_price is not None:
            updates.append("exit_price=%s");   params.append(float(exit_price))
        if realized_pnl is not None:
            updates.append("realized_pnl=%s"); params.append(float(realized_pnl))
        if exit_reason is not None:
            updates.append("exit_reason=%s");  params.append(exit_reason)
        if exit_ts is not None:
            updates.append("exit_ts=%s");      params.append(exit_ts)
        params.extend([position_id, self.client_id])
        sql = f"UPDATE positions SET {', '.join(updates)} WHERE id=%s AND client_id=%s"
        def _fn():
            with conn() as c:
                c.execute(sql, tuple(params))
        run_with_retry(_fn)

    def close_position(
        self,
        position_id:  str,
        *,
        exit_price:   float,
        realized_pnl: float,
        close_reason: str = "closed",
        exit_ts:      Optional[str] = None,
    ):
        """
        Mark position closed. Double-close safe (Python + DB guards).
        close_reason → status:
          take_profit → TAKEN_PROFIT
          stop_loss   → STOPPED
          expired     → EXPIRED
          *           → CLOSED
        """
        current = self.get_position(position_id)
        if not current:
            log.warning(f"[{self.client_id}] close_position: {position_id} not found")
            return
        if PositionStatus.is_terminal(current.get("status", "")):
            log.warning(f"[{self.client_id}] close_position SKIPPED — {position_id} already {current['status']}")
            return

        status_map = {
            "take_profit": PositionStatus.TAKEN_PROFIT,
            "stop_loss":   PositionStatus.STOPPED,
            "expired":     PositionStatus.EXPIRED,
        }
        final_status = status_map.get(close_reason, PositionStatus.CLOSED)
        ts = exit_ts or now_utc_iso()

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    UPDATE positions SET
                        status=%s, exit_price=%s, realized_pnl=%s,
                        exit_reason=%s, exit_ts=%s, updated_at=NOW()
                    WHERE id=%s AND client_id=%s
                    AND status NOT IN ('CLOSED','EXPIRED','STOPPED','TAKEN_PROFIT','ERROR')
                    """,
                    (final_status, float(exit_price), float(realized_pnl),
                     close_reason, ts, position_id, self.client_id),
                )
        run_with_retry(_fn)
        log.info(
            f"[{self.client_id}] POSITION CLOSED | id={position_id} "
            f"exit=${exit_price:.2f} pnl=${realized_pnl:+.2f} "
            f"reason={close_reason} → {final_status}"
        )

    def mark_closing(self, position_id: str):
        """Exit order submitted — keep slot gated until fill confirmed."""
        current = self.get_position(position_id)
        if not current or PositionStatus.is_terminal(current.get("status", "")):
            return
        self.update_position(position_id, status=PositionStatus.CLOSING)
        log.info(f"[{self.client_id}] POSITION CLOSING | id={position_id}")

    # =========================================================================
    # DAILY SUMMARY
    # =========================================================================

    def daily_summary(self) -> dict:
        """
        Today's stats. trades_today = positions opened today (entry_ts::date).
        Realized PnL only counts closed positions with today's entry date.
        """
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE status IN ('OPEN','CLOSING'))         AS active_count,
                        COUNT(*) FILTER (WHERE entry_ts::date = %s::date)            AS trades_today,
                        COALESCE(SUM(realized_pnl)
                            FILTER (WHERE entry_ts::date = %s::date
                                    AND status IN ('CLOSED','STOPPED',
                                                   'TAKEN_PROFIT','EXPIRED')), 0)   AS realized_pnl_today,
                        COALESCE(SUM(avg_fill * qty * 100)
                            FILTER (WHERE status IN ('OPEN','CLOSING')), 0)          AS capital_deployed
                    FROM positions WHERE client_id=%s
                    """,
                    (today, today, self.client_id),
                )
                row = c.fetchone() or {}
                return {
                    "open_count":         int(row.get("active_count") or 0),
                    "trades_today":       int(row.get("trades_today") or 0),
                    "realized_pnl_today": float(row.get("realized_pnl_today") or 0),
                    "capital_deployed":   float(row.get("capital_deployed") or 0),
                }
        return run_with_retry(_fn)

    def realized_pnl_today(self) -> float:
        return self.daily_summary()["realized_pnl_today"]

    def trades_today(self) -> int:
        return self.daily_summary()["trades_today"]

    # =========================================================================
    # ACCOUNT SNAPSHOT — full truth package
    # =========================================================================

    def snapshot(self) -> dict:
        """
        Complete truth package for one client in one DB round-trip-equivalent.
        Used directly by APMasterControl gates.

        Returns:
          open_positions      — list[dict] OPEN only
          closing_positions   — list[dict] CLOSING only
          open_count          — OPEN + CLOSING total
          open_tickers        — set of active underlying tickers
          calls_open          — active CALL count
          puts_open           — active PUT count
          capital_deployed    — USD in active positions
          pending_entries     — in-flight entry order count
          pending_exits       — in-flight exit order count
          trades_today        — positions opened today
          realized_pnl_today  — settled PnL today
        """
        # HIGH-005: raise on error instead of returning fake zeros
        active          = self.get_active_positions()
        opens           = [p for p in active if p["status"] == "OPEN"]
        closing         = [p for p in active if p["status"] == "CLOSING"]
        summary         = self.daily_summary()
        pending_entries = self.pending_entry_count()
        pending_exits   = self.pending_exit_count()

        return {
            "open_positions":     opens,
            "closing_positions":  closing,
            "open_count":         len(active),
            "open_tickers":       {p["underlying"] for p in active},
            "calls_open":         sum(1 for p in active if p.get("direction") == "CALL"),
            "puts_open":          sum(1 for p in active if p.get("direction") == "PUT"),
            "capital_deployed":   summary["capital_deployed"],
            "pending_entries":    pending_entries,
            "pending_exits":      pending_exits,
            "trades_today":       summary["trades_today"],
            "realized_pnl_today": summary["realized_pnl_today"],
        }
