# ap/position_manager.py — APPositionManager
# =============================================================================
# Client-relative position truth backed by Supabase Postgres.
#
# Money-safety fixes:
# - Dedup guards only block ACTIVE positions (OPEN/CLOSING), not historical CLOSED rows.
# - Dedup lookups are deterministic: ORDER BY entry_ts/created_at DESC.
# - open_position accepts optional local_order_id/broker_order_id idempotency keys.
# - open_position is serialized by a Postgres advisory transaction lock on the
#   strongest available idempotency key, so concurrent fill processors cannot
#   double-open the same economic position even before DB unique indexes land.
# - If matching columns + unique indexes exist, generic ON CONFLICT DO NOTHING
#   catches duplicate execution rows and returns the existing position id.
# - pending_entry_count counts all active entry lifecycle states, not only CREATED
#   ghost orders, so master control sees real pending risk.
# - snapshot()/daily_summary use America/New_York session-day ranges, not UTC dates.
# - snapshot() reads through one DB connection and exposes active_count separately.
# =============================================================================

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso

log = logging.getLogger("ap.position_manager")
ET = ZoneInfo("America/New_York")


class PositionStatus:
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    EXPIRED = "EXPIRED"
    STOPPED = "STOPPED"
    TAKEN_PROFIT = "TAKEN_PROFIT"
    ERROR = "ERROR"

    ACTIVE = {OPEN, CLOSING}
    TERMINAL = {CLOSED, EXPIRED, STOPPED, TAKEN_PROFIT, ERROR}

    @classmethod
    def is_terminal(cls, status: str) -> bool:
        return status in cls.TERMINAL

    @classmethod
    def is_active(cls, status: str) -> bool:
        return status in cls.ACTIVE


_PENDING_ENTRY_STATUSES = (
    "CREATED",
    "PENDING_TRIGGER",
    "SUBMITTED",
    "ACKNOWLEDGED",
    "PARTIAL_FILL",
)
_PENDING_EXIT_STATUSES = (
    "EXIT_REQUESTED",
    "EXIT_SUBMITTED",
    "EXIT_ACKNOWLEDGED",
    "EXIT_PARTIAL_FILL",
)


class APPositionManager:
    """Client-scoped position + order truth layer backed by Postgres."""

    def __init__(self, client_id: str):
        self.client_id = client_id
        self._position_columns_cache: Optional[set[str]] = None
        log.info("[%s] APPositionManager initialized", client_id)

    # ------------------------------------------------------------------
    # Schema helpers — allows local_order_id/broker_order_id support when
    # your DB has the columns, without breaking older deployed schemas.
    # ------------------------------------------------------------------

    def _position_columns(self) -> set[str]:
        if self._position_columns_cache is not None:
            return self._position_columns_cache

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = 'positions'
                    """
                )
                return {str(r["column_name"]) for r in c.fetchall()}

        try:
            self._position_columns_cache = run_with_retry(_fn)
        except Exception as e:
            log.debug("[%s] Could not inspect positions columns: %s", self.client_id, e)
            self._position_columns_cache = set()
        return self._position_columns_cache

    def _has_position_column(self, name: str) -> bool:
        return name in self._position_columns()

    def refresh_schema_cache(self) -> None:
        """Force column-cache refresh after running migrations in a live process."""
        self._position_columns_cache = None
        self._position_columns()

    # ------------------------------------------------------------------
    # Market/session-day helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _market_day_bounds_utc(now: Optional[datetime] = None) -> tuple[datetime, datetime, str]:
        """Return ET calendar-day [start,end) bounds converted to UTC."""
        now_et = (now or datetime.now(timezone.utc)).astimezone(ET)
        start_et = datetime(now_et.year, now_et.month, now_et.day, tzinfo=ET)
        end_et = start_et + timedelta(days=1)
        return start_et.astimezone(timezone.utc), end_et.astimezone(timezone.utc), start_et.date().isoformat()

    @staticmethod
    def _nullable_float(value) -> Optional[float]:
        return None if value is None else float(value)

    # ------------------------------------------------------------------
    # Basic reads
    # ------------------------------------------------------------------

    def open_count(self) -> int:
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM positions
                    WHERE client_id=%s AND status IN ('OPEN','CLOSING')
                    """,
                    (self.client_id,),
                )
                return int((c.fetchone() or {}).get("n") or 0)
        return run_with_retry(_fn)

    def has_open_position(self, ticker: str) -> bool:
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT 1
                    FROM positions
                    WHERE client_id=%s AND underlying=%s
                      AND status IN ('OPEN','CLOSING')
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, ticker.upper()),
                )
                return c.fetchone() is not None
        return run_with_retry(_fn)

    def get_open_positions(self) -> list[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id=%s AND status='OPEN'
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    """,
                    (self.client_id,),
                )
                return c.fetchall()
        return run_with_retry(_fn)

    def get_active_positions(self) -> list[dict]:
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id=%s AND status IN ('OPEN','CLOSING')
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    """,
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
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id=%s AND contract=%s
                      AND status IN ('OPEN','CLOSING')
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, contract),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    # ------------------------------------------------------------------
    # Dedup reads — fixed Bugs 41/42.
    # These intentionally only block active risk. Closed/expired/error rows
    # are history and should not permanently suppress new positions.
    # ------------------------------------------------------------------

    def get_position_by_signal(self, signal_id: str) -> Optional[dict]:
        if not signal_id:
            return None

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id=%s AND signal_id=%s
                      AND status IN ('OPEN','CLOSING')
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, signal_id),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def get_position_by_plan(self, plan_id: str) -> Optional[dict]:
        if not plan_id:
            return None

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id=%s AND plan_id=%s
                      AND status IN ('OPEN','CLOSING')
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, plan_id),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def get_position_by_local_order(self, local_order_id: str) -> Optional[dict]:
        if not local_order_id or not self._has_position_column("local_order_id"):
            return None

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id=%s AND local_order_id=%s
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, local_order_id),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def get_position_by_broker_order(self, broker_order_id: str) -> Optional[dict]:
        if not broker_order_id or not self._has_position_column("broker_order_id"):
            return None

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id=%s AND broker_order_id=%s
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, broker_order_id),
                )
                return c.fetchone()
        return run_with_retry(_fn)

    def get_all_positions(self, limit: int = 200, status: str | None = None) -> list[dict]:
        def _fn():
            with conn() as c:
                if status:
                    c.execute(
                        """
                        SELECT *
                        FROM positions
                        WHERE client_id=%s AND status=%s
                        ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                        LIMIT %s
                        """,
                        (self.client_id, status, limit),
                    )
                else:
                    c.execute(
                        """
                        SELECT *
                        FROM positions
                        WHERE client_id=%s
                        ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                        LIMIT %s
                        """,
                        (self.client_id, limit),
                    )
                return c.fetchall()
        return run_with_retry(_fn)

    # ------------------------------------------------------------------
    # Pending order awareness — fixed Bug 44.
    # Count all active entry orders, including PENDING_TRIGGER and broker-id
    # rows. This is safer for master-control risk gating.
    # ------------------------------------------------------------------

    def pending_entry_count(self) -> int:
        def _fn():
            with conn() as c:
                placeholders = ",".join(["%s"] * len(_PENDING_ENTRY_STATUSES))
                c.execute(
                    f"""
                    SELECT COUNT(*) AS n
                    FROM orders
                    WHERE client_id=%s
                      AND kind='ENTRY'
                      AND status IN ({placeholders})
                    """,
                    (self.client_id, *_PENDING_ENTRY_STATUSES),
                )
                return int((c.fetchone() or {}).get("n") or 0)
        return run_with_retry(_fn)

    def pending_exit_count(self) -> int:
        def _fn():
            with conn() as c:
                placeholders = ",".join(["%s"] * len(_PENDING_EXIT_STATUSES))
                c.execute(
                    f"""
                    SELECT COUNT(*) AS n
                    FROM orders
                    WHERE client_id=%s
                      AND kind='EXIT'
                      AND status IN ({placeholders})
                    """,
                    (self.client_id, *_PENDING_EXIT_STATUSES),
                )
                return int((c.fetchone() or {}).get("n") or 0)
        return run_with_retry(_fn)

    def has_pending_entry(self, ticker: str) -> bool:
        def _fn():
            with conn() as c:
                placeholders = ",".join(["%s"] * len(_PENDING_ENTRY_STATUSES))
                c.execute(
                    f"""
                    SELECT 1
                    FROM orders
                    WHERE client_id=%s
                      AND symbol=%s
                      AND kind='ENTRY'
                      AND status IN ({placeholders})
                    ORDER BY created_ts DESC NULLS LAST, updated_ts DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, ticker.upper(), *_PENDING_ENTRY_STATUSES),
                )
                return c.fetchone() is not None
        return run_with_retry(_fn)

    def has_pending_exit(self, position_id: str) -> bool:
        def _fn():
            with conn() as c:
                placeholders = ",".join(["%s"] * len(_PENDING_EXIT_STATUSES))
                c.execute(
                    f"""
                    SELECT 1
                    FROM orders
                    WHERE client_id=%s
                      AND position_id=%s
                      AND kind='EXIT'
                      AND status IN ({placeholders})
                    ORDER BY created_ts DESC NULLS LAST, updated_ts DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, position_id, *_PENDING_EXIT_STATUSES),
                )
                return c.fetchone() is not None
        return run_with_retry(_fn)

    # ------------------------------------------------------------------
    # Exposure breakdown
    # ------------------------------------------------------------------

    def open_tickers(self) -> set[str]:
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT DISTINCT underlying
                    FROM positions
                    WHERE client_id=%s AND status IN ('OPEN','CLOSING')
                    """,
                    (self.client_id,),
                )
                return {row["underlying"] for row in c.fetchall()}
        return run_with_retry(_fn)

    def open_count_for_ticker(self, ticker: str) -> int:
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM positions
                    WHERE client_id=%s AND underlying=%s
                      AND status IN ('OPEN','CLOSING')
                    """,
                    (self.client_id, ticker.upper()),
                )
                return int((c.fetchone() or {}).get("n") or 0)
        return run_with_retry(_fn)

    def open_count_for_direction(self, direction: str) -> int:
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM positions
                    WHERE client_id=%s AND direction=%s
                      AND status IN ('OPEN','CLOSING')
                    """,
                    (self.client_id, direction.upper()),
                )
                return int((c.fetchone() or {}).get("n") or 0)
        return run_with_retry(_fn)

    def capital_deployed(self) -> float:
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT COALESCE(SUM(avg_fill * qty * 100), 0) AS deployed
                    FROM positions
                    WHERE client_id=%s AND status IN ('OPEN','CLOSING')
                    """,
                    (self.client_id,),
                )
                return float((c.fetchone() or {}).get("deployed") or 0)
        return run_with_retry(_fn)

    # ------------------------------------------------------------------
    # Transaction/idempotency helpers
    # ------------------------------------------------------------------

    def _position_idempotency_key(
        self,
        *,
        local_order_id: Optional[str] = None,
        broker_order_id: Optional[str] = None,
        plan_id: Optional[str] = None,
        signal_id: Optional[str] = None,
    ) -> str:
        """Strongest-to-weakest key used to serialize open_position()."""
        if local_order_id:
            return f"position-open:client={self.client_id}:local_order={local_order_id}"
        if broker_order_id:
            return f"position-open:client={self.client_id}:broker_order={broker_order_id}"
        if plan_id:
            return f"position-open:client={self.client_id}:plan={plan_id}"
        if signal_id:
            return f"position-open:client={self.client_id}:signal={signal_id}"
        return f"position-open:client={self.client_id}:unknown"

    def _acquire_position_open_lock(self, c, key: str) -> None:
        """
        Serialize position opens inside the current DB transaction.

        This closes the read-before-insert race even when the deployed schema
        does not yet have local_order_id/broker_order_id unique indexes.
        pg_advisory_xact_lock is transaction-scoped and releases automatically
        when conn() commits or rolls back.

        Uses md5-based 64-bit key to eliminate the 32-bit hashtext() collision
        space risk. hashtext() collisions across different client/order keys
        would cause false lock contention in multi-client production.
        """
        c.execute(
            "SELECT pg_advisory_xact_lock(('x' || md5(%s))::bit(64)::bigint)",
            (key,),
        )

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def open_position(
        self,
        *,
        plan_id: str,
        signal_id: str,
        ticker: str,
        contract: str,
        side: str,
        qty: int,
        entry_price: float,
        underlying_entry: Optional[float] = None,
        tier: str = "B",
        score: float = 0.0,
        pattern: str = "",
        tp_pct: float = 0.20,
        sl_pct: float = 0.35,
        stop_underlying: Optional[float] = None,
        target_underlying: Optional[float] = None,
        local_order_id: Optional[str] = None,
        broker_order_id: Optional[str] = None,
    ) -> str:
        """
        Atomically insert a new OPEN position and return position_id.

        Hardening rules:
        - Serialize by advisory transaction lock on local_order_id/broker_order_id
          when present, else plan_id/signal_id fallback.
        - Re-check all idempotency guards inside the same locked transaction.
        - Insert with generic ON CONFLICT DO NOTHING so deployed unique indexes
          on local_order_id/broker_order_id are honored immediately.
        - Never rely on ON CONFLICT(id), because id is a fresh UUID and does not
          represent economic/execution uniqueness.
        """
        if int(qty or 0) <= 0:
            raise ValueError(f"qty must be positive, got {qty}")
        if float(entry_price or 0) <= 0:
            raise ValueError(f"entry_price must be positive, got {entry_price}")

        has_local_col = self._has_position_column("local_order_id")
        has_broker_col = self._has_position_column("broker_order_id")
        has_underlying_entry_col = self._has_position_column("underlying_entry")

        position_id = str(uuid.uuid4())
        ts = now_utc_iso()
        lock_key = self._position_idempotency_key(
            local_order_id=local_order_id if has_local_col else None,
            broker_order_id=broker_order_id if has_broker_col else None,
            plan_id=plan_id,
            signal_id=signal_id,
        )

        columns = [
            "id", "client_id", "plan_id", "signal_id",
            "underlying", "contract", "direction", "qty", "avg_fill",
            "tier", "score", "pattern", "tp_pct", "sl_pct",
            "stop_underlying", "target_underlying", "status",
            "entry_ts", "created_at", "updated_at",
        ]
        values = [
            position_id, self.client_id, plan_id, signal_id,
            ticker.upper(), contract, side.upper(), int(qty), float(entry_price),
            tier, float(score), pattern, float(tp_pct), float(sl_pct),
            self._nullable_float(stop_underlying),
            self._nullable_float(target_underlying),
            PositionStatus.OPEN, ts, ts, ts,
        ]

        if has_underlying_entry_col:
            columns.append("underlying_entry")
            values.append(self._nullable_float(underlying_entry))

        if local_order_id and has_local_col:
            columns.append("local_order_id")
            values.append(local_order_id)
        if broker_order_id and has_broker_col:
            columns.append("broker_order_id")
            values.append(broker_order_id)

        placeholders = ",".join(["%s"] * len(columns))
        col_sql = ", ".join(columns)

        def _select_existing_locked(c) -> Optional[dict]:
            # Strict execution idempotency: order IDs should not be reused. If a
            # fill is replayed after the position is closed, return the historical
            # row rather than opening a second position for the same broker fill.
            if local_order_id and has_local_col:
                c.execute(
                    """
                    SELECT * FROM positions
                    WHERE client_id=%s AND local_order_id=%s
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, local_order_id),
                )
                row = c.fetchone()
                if row:
                    return row

            if broker_order_id and has_broker_col:
                c.execute(
                    """
                    SELECT * FROM positions
                    WHERE client_id=%s AND broker_order_id=%s
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, broker_order_id),
                )
                row = c.fetchone()
                if row:
                    return row

            # Active-only business fallback: plan/signal should not permanently
            # block future re-entries after terminal close.
            if plan_id:
                c.execute(
                    """
                    SELECT * FROM positions
                    WHERE client_id=%s AND plan_id=%s
                      AND status IN ('OPEN','CLOSING')
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, plan_id),
                )
                row = c.fetchone()
                if row:
                    return row

            if signal_id:
                c.execute(
                    """
                    SELECT * FROM positions
                    WHERE client_id=%s AND signal_id=%s
                      AND status IN ('OPEN','CLOSING')
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, signal_id),
                )
                row = c.fetchone()
                if row:
                    return row

            return None

        def _fn():
            with conn() as c:
                self._acquire_position_open_lock(c, lock_key)

                existing = _select_existing_locked(c)
                if existing:
                    return str(existing["id"]), False

                # Generic ON CONFLICT DO NOTHING honors any deployed unique index
                # without requiring a specific constraint name. After a conflict,
                # select again to return the real existing id.
                c.execute(
                    f"""
                    INSERT INTO positions ({col_sql})
                    VALUES ({placeholders})
                    ON CONFLICT DO NOTHING
                    RETURNING id
                    """,
                    tuple(values),
                )
                inserted = c.fetchone()
                if inserted:
                    return str(inserted["id"]), True

                existing_after_conflict = _select_existing_locked(c)
                if existing_after_conflict:
                    return str(existing_after_conflict["id"]), False

                raise RuntimeError(
                    "positions insert returned no row and no idempotent existing row was found; "
                    "check deployed unique constraints / schema"
                )

        final_id, inserted = run_with_retry(_fn)
        if inserted:
            log.info(
                "[%s] POSITION OPENED | %s %s x%s @ $%.2f | id=%s tier=%s underlying_entry=%s local_order=%s broker_order=%s",
                self.client_id, ticker, contract, qty, entry_price, final_id, tier,
                underlying_entry if underlying_entry is not None else "", local_order_id or "", broker_order_id or "",
            )
        else:
            log.warning(
                "[%s] open_position IDEMPOTENT RETURN | %s %s | id=%s local_order=%s broker_order=%s plan=%s signal=%s",
                self.client_id, ticker, contract, final_id,
                local_order_id or "", broker_order_id or "", plan_id or "", signal_id or "",
            )
        return final_id

    def update_position(
        self,
        position_id: str,
        *,
        status: Optional[str] = None,
        exit_price: Optional[float] = None,
        realized_pnl: Optional[float] = None,
        exit_reason: Optional[str] = None,
        exit_ts: Optional[str] = None,
    ):
        updates = ["updated_at=NOW()"]
        params = []
        if status is not None:
            updates.append("status=%s")
            params.append(status)
        if exit_price is not None:
            updates.append("exit_price=%s")
            params.append(float(exit_price))
        if realized_pnl is not None:
            updates.append("realized_pnl=%s")
            params.append(float(realized_pnl))
        if exit_reason is not None:
            updates.append("exit_reason=%s")
            params.append(exit_reason)
        if exit_ts is not None:
            updates.append("exit_ts=%s")
            params.append(exit_ts)
        params.extend([position_id, self.client_id])
        sql = f"UPDATE positions SET {', '.join(updates)} WHERE id=%s AND client_id=%s"

        def _fn():
            with conn() as c:
                c.execute(sql, tuple(params))
                return getattr(c, "rowcount", None)

        rowcount = run_with_retry(_fn)
        if rowcount == 0:
            log.warning("[%s] update_position NOOP | id=%s", self.client_id, position_id)
        return rowcount

    def close_position(
        self,
        position_id: str,
        *,
        exit_price: float,
        realized_pnl: float,
        close_reason: str = "closed",
        exit_ts: Optional[str] = None,
    ):
        """Atomically mark position terminal; logs based on affected row count."""
        status_map = {
            "take_profit": PositionStatus.TAKEN_PROFIT,
            "stop_loss": PositionStatus.STOPPED,
            "expired": PositionStatus.EXPIRED,
        }
        final_status = status_map.get(close_reason, PositionStatus.CLOSED)
        ts = exit_ts or now_utc_iso()

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    UPDATE positions
                    SET status=%s,
                        exit_price=%s,
                        realized_pnl=%s,
                        exit_reason=%s,
                        exit_ts=%s,
                        updated_at=NOW()
                    WHERE id=%s
                      AND client_id=%s
                      AND status NOT IN ('CLOSED','EXPIRED','STOPPED','TAKEN_PROFIT','ERROR')
                    RETURNING id, status
                    """,
                    (
                        final_status, float(exit_price), float(realized_pnl),
                        close_reason, ts, position_id, self.client_id,
                    ),
                )
                return c.fetchone()

        row = run_with_retry(_fn)
        if not row:
            current = self.get_position(position_id)
            if not current:
                log.warning("[%s] close_position NOOP — %s not found", self.client_id, position_id)
            else:
                log.warning(
                    "[%s] close_position NOOP — %s already terminal/status=%s",
                    self.client_id, position_id, current.get("status"),
                )
            return

        log.info(
            "[%s] POSITION CLOSED | id=%s exit=$%.2f pnl=$%+.2f reason=%s → %s",
            self.client_id, position_id, exit_price, realized_pnl, close_reason, final_status,
        )

    def close_position_from_exit_fill(
        self,
        *,
        position_id: str,
        exit_price: float,
        filled_qty: int,
        filled_ts: Optional[str] = None,
        local_order_id: str = "",
        broker_order_id: str = "",
        close_source: str = "broker_exit_fill",
        close_confidence: str = "HIGH",
        exit_reason: str = "exit_filled",
    ) -> bool:
        """
        Canonical broker-truth position finalizer.

        The ONLY clean path to finalize a position from a confirmed exit fill.
        Uses confirmed broker/order fill data only — never quote, mid, bid, mark,
        chart price, or estimated option price.

        Writes: exit_price, realized_pnl, realized_pnl_pct, quantity_remaining,
                exit_ts, close_source, close_confidence, status (CLOSED when full).

        Called by:
          - APOrderStateMachine.transition() when EXIT_FILLED succeeds
          - APBrokerReconciler._heal_exit_filled_positions_from_orders() as backup
        """
        try:
            exit_px  = float(exit_price or 0)
            fill_qty = int(filled_qty or 0)
        except Exception:
            log.error(
                "[%s] close_position_from_exit_fill invalid inputs | pos=%s exit_price=%r filled_qty=%r",
                self.client_id, position_id, exit_price, filled_qty,
            )
            return False

        if not position_id or exit_px <= 0 or fill_qty <= 0:
            log.warning(
                "[%s] close_position_from_exit_fill blocked | pos=%s exit_price=%s filled_qty=%s",
                self.client_id, position_id, exit_px, fill_qty,
            )
            return False

        ts = filled_ts or now_utc_iso()

        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM positions WHERE id=%s AND client_id=%s FOR UPDATE",
                    (position_id, self.client_id),
                )
                pos = c.fetchone()
                if not pos:
                    return False, "position_not_found"

                avg_fill = float(pos.get("avg_fill") or pos.get("entry_price") or 0)
                qty      = int(pos.get("qty") or 0)
                current_remaining = pos.get("quantity_remaining")
                if current_remaining is None:
                    current_remaining = qty
                current_remaining = int(current_remaining or 0)

                if avg_fill <= 0 or qty <= 0:
                    return False, "invalid_position_cost_basis"

                close_qty    = min(fill_qty, current_remaining if current_remaining > 0 else fill_qty)
                new_remaining = max(current_remaining - close_qty, 0)

                realized_pnl     = round((exit_px - avg_fill) * close_qty * 100, 2)
                realized_pnl_pct = round(((exit_px - avg_fill) / avg_fill) * 100, 2) if avg_fill else 0.0

                new_status = PositionStatus.CLOSED if new_remaining <= 0 else PositionStatus.CLOSING

                # Build update dynamically so missing columns don't crash
                has = self._has_position_column
                sets, vals = ["updated_at=NOW()"], []

                def _add(col, val):
                    if has(col):
                        sets.append(f"{col}=%s")
                        vals.append(val)

                _add("exit_price",         exit_px)
                _add("realized_pnl",       realized_pnl)
                _add("realized_pnl_pct",   realized_pnl_pct)
                _add("quantity_remaining", new_remaining)
                _add("exit_ts",            ts)
                _add("exit_reason",        exit_reason)
                _add("close_source",       close_source)
                _add("close_confidence",   close_confidence)
                sets.append("status=%s"); vals.append(new_status)
                if local_order_id and has("local_order_id"):
                    sets.append("local_order_id=COALESCE(local_order_id,%s)"); vals.append(local_order_id)
                if broker_order_id and has("broker_order_id"):
                    sets.append("broker_order_id=COALESCE(broker_order_id,%s)"); vals.append(str(broker_order_id))

                vals.extend([position_id, self.client_id])
                c.execute(
                    f"UPDATE positions SET {', '.join(sets)} "
                    f"WHERE id=%s AND client_id=%s RETURNING id, status",
                    tuple(vals),
                )
                row = c.fetchone()
                if not row:
                    return False, "update_no_row"
                return True, {
                    "status": row.get("status"), "exit_price": exit_px,
                    "filled_qty": close_qty, "remaining": new_remaining,
                    "realized_pnl": realized_pnl, "realized_pnl_pct": realized_pnl_pct,
                }

        ok, detail = run_with_retry(_fn)
        if ok:
            log.info(
                "[%s] POSITION FINALIZED FROM EXIT FILL | pos=%s exit=$%.2f qty=%s "
                "pnl=$%+.2f (%.1f%%) source=%s broker=%s",
                self.client_id, position_id, exit_px, fill_qty,
                detail.get("realized_pnl", 0), detail.get("realized_pnl_pct", 0),
                close_source, broker_order_id or "",
            )
            return True

        log.warning(
            "[%s] close_position_from_exit_fill failed | pos=%s reason=%s",
            self.client_id, position_id, detail,
        )
        return False

    def mark_closing(self, position_id: str):
        current = self.get_position(position_id)
        if not current or PositionStatus.is_terminal(current.get("status", "")):
            return
        self.update_position(position_id, status=PositionStatus.CLOSING)
        log.info("[%s] POSITION CLOSING | id=%s", self.client_id, position_id)

    # ------------------------------------------------------------------
    # Daily summary
    # ------------------------------------------------------------------

    def daily_summary(self) -> dict:
        """Today's stats using the America/New_York market/session calendar day."""
        start_utc, end_utc, session_day = self._market_day_bounds_utc()

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE status IN ('OPEN','CLOSING')) AS active_count,
                        COUNT(*) FILTER (WHERE entry_ts >= %s AND entry_ts < %s) AS trades_today,
                        COALESCE(SUM(realized_pnl) FILTER (
                            WHERE entry_ts >= %s AND entry_ts < %s
                              AND status IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
                        ), 0) AS realized_pnl_today,
                        COALESCE(SUM(avg_fill * qty * 100) FILTER (
                            WHERE status IN ('OPEN','CLOSING')
                        ), 0) AS capital_deployed
                    FROM positions
                    WHERE client_id=%s
                    """,
                    (start_utc, end_utc, start_utc, end_utc, self.client_id),
                )
                row = c.fetchone() or {}
                active_count = int(row.get("active_count") or 0)
                return {
                    "open_count": active_count,       # backward-compatible alias
                    "active_count": active_count,     # precise meaning: OPEN + CLOSING
                    "session_day": session_day,
                    "trades_today": int(row.get("trades_today") or 0),
                    "realized_pnl_today": float(row.get("realized_pnl_today") or 0),
                    "capital_deployed": float(row.get("capital_deployed") or 0),
                }
        return run_with_retry(_fn)

    def realized_pnl_today(self) -> float:
        return self.daily_summary()["realized_pnl_today"]

    def trades_today(self) -> int:
        return self.daily_summary()["trades_today"]

    # ------------------------------------------------------------------
    # Atomic-ish snapshot — fixed Bug 45.
    # Uses one repeatable-read transaction so master-control gates don't mix
    # before/after states under heavy churn.
    # ------------------------------------------------------------------

    def snapshot(self) -> dict:
        """
        Consistent-ish account snapshot for master-control gates.

        Uses one DB connection and attempts to set REPEATABLE READ before any
        SELECT. No manual BEGIN/COMMIT is issued here; conn() owns transaction
        lifecycle, which avoids brittle nested transaction behavior.
        """
        start_utc, end_utc, session_day = self._market_day_bounds_utc()

        def _fn():
            with conn() as c:
                try:
                    c.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")
                except Exception as e:
                    log.debug("[%s] snapshot isolation not set; using conn default: %s", self.client_id, e)

                c.execute(
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id=%s AND status IN ('OPEN','CLOSING')
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    """,
                    (self.client_id,),
                )
                active = c.fetchall()

                c.execute(
                    """
                    SELECT
                        COUNT(*) FILTER (WHERE entry_ts >= %s AND entry_ts < %s) AS trades_today,
                        COALESCE(SUM(realized_pnl) FILTER (
                            WHERE entry_ts >= %s AND entry_ts < %s
                              AND status IN ('CLOSED','STOPPED','TAKEN_PROFIT','EXPIRED')
                        ), 0) AS realized_pnl_today,
                        COALESCE(SUM(avg_fill * qty * 100) FILTER (
                            WHERE status IN ('OPEN','CLOSING')
                        ), 0) AS capital_deployed
                    FROM positions
                    WHERE client_id=%s
                    """,
                    (start_utc, end_utc, start_utc, end_utc, self.client_id),
                )
                summary = c.fetchone() or {}

                entry_placeholders = ",".join(["%s"] * len(_PENDING_ENTRY_STATUSES))
                c.execute(
                    f"""
                    SELECT COUNT(*) AS n
                    FROM orders
                    WHERE client_id=%s AND kind='ENTRY'
                      AND status IN ({entry_placeholders})
                    """,
                    (self.client_id, *_PENDING_ENTRY_STATUSES),
                )
                pending_entries = int((c.fetchone() or {}).get("n") or 0)

                exit_placeholders = ",".join(["%s"] * len(_PENDING_EXIT_STATUSES))
                c.execute(
                    f"""
                    SELECT COUNT(*) AS n
                    FROM orders
                    WHERE client_id=%s AND kind='EXIT'
                      AND status IN ({exit_placeholders})
                    """,
                    (self.client_id, *_PENDING_EXIT_STATUSES),
                )
                pending_exits = int((c.fetchone() or {}).get("n") or 0)

                opens = [p for p in active if p.get("status") == PositionStatus.OPEN]
                closing = [p for p in active if p.get("status") == PositionStatus.CLOSING]

                return {
                    "open_positions": opens,
                    "closing_positions": closing,
                    "open_count": len(active),        # backward-compatible alias for active_count
                    "active_count": len(active),
                    "session_day": session_day,
                    "open_tickers": {p["underlying"] for p in active},
                    "calls_open": sum(1 for p in active if p.get("direction") == "CALL"),
                    "puts_open": sum(1 for p in active if p.get("direction") == "PUT"),
                    "capital_deployed": float(summary.get("capital_deployed") or 0),
                    "pending_entries": pending_entries,
                    "pending_exits": pending_exits,
                    "trades_today": int(summary.get("trades_today") or 0),
                    "realized_pnl_today": float(summary.get("realized_pnl_today") or 0),
                }

        return run_with_retry(_fn)
