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
import math
import os
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional
from zoneinfo import ZoneInfo

from ap.db import conn, run_with_retry
from ap.utils import now_utc_iso

log = logging.getLogger("ap.position_manager")
ET = ZoneInfo("America/New_York")


def _strict_positive_finite_float(value: object) -> float | None:
    """Return a positive finite scalar without allowing bool coercion."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed <= 0:
        return None
    return parsed


def _strict_positive_whole_number(value: object) -> int | None:
    """Return a positive whole-number scalar without allowing bool coercion."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed <= 0 or not parsed.is_integer():
        return None
    return int(parsed)


def _strict_nonnegative_whole_number(value: object) -> int | None:
    """Return a non-negative whole-number scalar without allowing bool coercion."""
    if isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or parsed < 0 or not parsed.is_integer():
        return None
    return int(parsed)


# ─────────────────────────────────────────────────────────────────────────────
# PR #237 — Position Manager fail-closed side normalization.
#
# Final DB guardrail: even if an upstream fill / recovery / manual-repair path
# passes a raw or aliased direction (BUY, SHORT, "bullish", etc.), the
# positions table only ever receives canonical CALL / PUT.  Anything outside
# the alias table raises ValueError before the INSERT — no silent CALL
# default, no direction-taxonomy contamination in the truth layer.
#
# Non-goals (see PR #237 spec): snapshot accounting, advisory locks,
# partial-close math, proof_trades repair, capital/exposure accounting,
# broker submit/cancel behavior.  None of those touch this helper.
# ─────────────────────────────────────────────────────────────────────────────
_POSITION_SIDE_ALIASES: dict[str, str] = {
    "CALL":    "CALL",
    "BUY":     "CALL",
    "LONG":    "CALL",
    "CALLS":   "CALL",
    "BULLISH": "CALL",
    "PUT":     "PUT",
    "SELL":    "PUT",
    "SHORT":   "PUT",
    "PUTS":    "PUT",
    "BEARISH": "PUT",
}


def _normalize_position_side(side) -> str:
    """Canonical CALL/PUT normalizer for the Position Manager insert path.

    Raises ValueError("invalid_or_missing_position_side:<raw>") when the input
    is missing, empty, or not in the alias table.  Callers MUST NOT catch and
    substitute a default — the raise is the fail-closed guardrail.
    """
    raw = str(side or "").strip().upper()
    normalized = _POSITION_SIDE_ALIASES.get(raw)
    if normalized not in {"CALL", "PUT"}:
        raise ValueError(f"invalid_or_missing_position_side:{raw!r}")
    return normalized


def _normalize_proof_side(side) -> Optional[str]:
    try:
        return _normalize_position_side(side)
    except Exception:
        return None


def _normalize_proof_contract(contract) -> str:
    return str(contract or "").strip().upper().replace(" ", "")


class PositionStatus:
    OPEN = "OPEN"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    CLOSED_REPAIR = "CLOSED_REPAIR"
    EXPIRED = "EXPIRED"
    STOPPED = "STOPPED"
    TAKEN_PROFIT = "TAKEN_PROFIT"
    ERROR = "ERROR"
    CANCELED = "CANCELED"
    CANCELLED = "CANCELLED"

    # PR #237: widened to match the runtime active-position queries at
    # get_active_positions and the reconciler active-status filters, which
    # have always accepted OPEN/CLOSING/PARTIAL/ACTIVE in the DB.  The
    # symbolic constant was drifted narrower than the actual SQL, so
    # membership checks against PositionStatus.ACTIVE were silently missing
    # PARTIAL and ACTIVE rows even though those rows are queried elsewhere.
    ACTIVE = {OPEN, CLOSING, "PARTIAL", "ACTIVE"}
    TERMINAL = {CLOSED, CLOSED_REPAIR, EXPIRED, STOPPED, TAKEN_PROFIT, ERROR, CANCELED, CANCELLED}

    @classmethod
    def is_terminal(cls, status: str) -> bool:
        return status in cls.TERMINAL

    @classmethod
    def is_active(cls, status: str) -> bool:
        return status in cls.ACTIVE


# PR #237: module-level twin of PositionStatus.ACTIVE for direct use in raw
# SQL parameterization (e.g. status IN %s).  Kept as a set of literal strings
# so callers do not need to import PositionStatus just to filter by active
# status.  Membership MUST stay identical to PositionStatus.ACTIVE.
ACTIVE_DB_STATUSES: set[str] = {"OPEN", "CLOSING", "PARTIAL", "ACTIVE"}


def _validate_persisted_terminal_truth(row: dict) -> tuple[bool, str]:
    """PR #386 blocker 4: fail-closed persisted-truth gate.

    Every canonical field required to write terminal proof must be present
    and shaped correctly on the persisted position row. Callers must not
    manufacture fallbacks (no ``now_utc_iso()``, no ``max(qty, 1)``, no
    zero economics). Missing truth remains discoverable and alertable
    rather than becoming a polished-looking proof record.
    """
    from datetime import datetime as _dt
    import math as _math

    if not isinstance(row, dict):
        return False, "row_not_dict"
    contract = str(row.get("contract") or "").strip()
    if not contract:
        return False, "missing_contract"
    side = str(row.get("side") or row.get("direction") or "").upper().strip()
    if side not in {"CALL", "PUT"}:
        return False, "invalid_side"
    mode = str(row.get("execution_mode") or "").strip().lower()
    if mode not in {"live", "paper"}:
        return False, "invalid_execution_mode"
    local_order_id = str(row.get("local_order_id") or "").strip()
    if not local_order_id:
        return False, "missing_local_order_id"
    raw_pnl_pct = row.get("realized_pnl_pct")
    if raw_pnl_pct is None:
        return False, "missing_realized_pnl_pct"
    if isinstance(raw_pnl_pct, str) and not raw_pnl_pct.strip():
        return False, "missing_realized_pnl_pct"

    avg_fill_raw = row.get("avg_fill")
    if avg_fill_raw is None or (
        isinstance(avg_fill_raw, str) and not avg_fill_raw.strip()
    ):
        avg_fill_raw = row.get("entry_price")
    qty = _strict_positive_whole_number(row.get("qty"))
    avg_fill = _strict_positive_finite_float(avg_fill_raw)
    exit_price = _strict_positive_finite_float(row.get("exit_price"))
    if isinstance(raw_pnl_pct, bool):
        return False, "invalid_realized_pnl_pct"
    try:
        pnl_pct = float(raw_pnl_pct)
    except (TypeError, ValueError, OverflowError):
        return False, "unparseable_numeric"
    if qty is None:
        return False, "invalid_qty"
    # PR #386 amendment 2: every numeric field entering canonical proof
    # must be finite (rejects +inf, -inf, and NaN). A value that "looks"
    # positive but is infinity would otherwise silently pass the >0 gate.
    if avg_fill is None:
        return False, "invalid_entry_price"
    if exit_price is None:
        return False, "invalid_exit_price"
    if not _math.isfinite(pnl_pct):
        return False, "non_finite_pnl_pct"
    entry_ts_str = str(row.get("entry_ts") or "").strip()
    exit_ts_str = str(row.get("exit_ts") or "").strip()
    if not entry_ts_str or not exit_ts_str:
        return False, "missing_timestamps"
    # PR #386 amendment 2: normalize timestamps to timezone-aware UTC so a
    # naive/aware mismatch never raises inside the comparison. Any parse
    # failure returns a stable validation failure rather than propagating.
    def _to_utc(text: str):
        try:
            dt = _dt.fromisoformat(text.replace("Z", "+00:00"))
        except (TypeError, ValueError):
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    entry_dt = _to_utc(entry_ts_str)
    exit_dt = _to_utc(exit_ts_str)
    if entry_dt is None or exit_dt is None:
        return False, "unparseable_timestamps"
    try:
        if exit_dt < entry_dt:
            return False, "exit_before_entry"
    except TypeError:
        return False, "timestamp_comparison_failed"
    return True, "ok"


_PENDING_ENTRY_STATUSES = (
    "CREATED",
    "PENDING_TRIGGER",
    "SUBMITTED",
    "ACKNOWLEDGED",
    "PARTIAL_FILL",
)

# Statuses that consume an actual position slot — broker order is live or filled.
# PENDING_TRIGGER / WATCHING / DEFERRED / CREATED are watcher or pre-submit states:
# no broker order exists yet, no capital is committed at the broker.
# These must NOT count toward max_positions / max_calls / max_puts slot limits.
_SLOT_CONSUMING_STATUSES = (
    "SUBMITTED",
    "ACKNOWLEDGED",
    "PARTIAL_FILL",
    "PARTIALLY_FILLED",   # alternate spelling in some OSM versions
)

# Watcher/pre-submit states that must never consume a slot.
# SELECTED = contract candidate only; no broker submission yet.
# RETRY_ELIGIBLE = marked for retry; not yet re-submitted.
# DEFERRED = placeholder; contract field may be DEFERRED:<ticker>.
_WATCHER_ENTRY_STATUSES = (
    "NEW",
    "PROCESSING",
    "WATCHING",
    "PENDING_TRIGGER",
    "DEFERRED",
    "SELECTED",
    "RETRY_ELIGIBLE",
)

_BROKER_CONFIRMED_ENTRY_FILL_STATUSES = (
    "PARTIAL_FILL",
    "PARTIALLY_FILLED",
    "FILLED",
)


def _broker_confirmed_entry_trades_today(
    cursor,
    *,
    client_id: str,
    execution_mode: str,
    start_utc: datetime,
    end_utc: datetime,
) -> dict[str, int | str]:
    """Count distinct broker-confirmed economic ENTRY fills for one session.

    Broker order identity is authoritative when present. An exact local ENTRY
    order id is the fallback only after durable fill quantity, state, and time
    are proven. Position rows never contribute to the count.
    """
    mode = str(execution_mode or "").strip().lower()
    if mode not in {"paper", "live"}:
        raise ValueError(f"invalid_trade_count_execution_mode:{execution_mode}")

    cursor.execute(
        """
        WITH filled_entries AS (
            SELECT
                execution_mode,
                CASE
                    WHEN COALESCE(broker_order_id,'') <> ''
                        THEN 'broker:' || broker_order_id
                    WHEN COALESCE(local_order_id,'') <> ''
                        THEN 'local:' || local_order_id
                    ELSE NULL
                END AS canonical_entry_identity
            FROM orders
            WHERE client_id=%s
              AND UPPER(COALESCE(kind,''))='ENTRY'
              AND COALESCE(filled_qty,0) > 0
              AND UPPER(COALESCE(status,'')) = ANY(%s)
              AND filled_ts >= %s
              AND filled_ts < %s
        )
        SELECT
            COUNT(DISTINCT canonical_entry_identity) FILTER (
                WHERE LOWER(COALESCE(execution_mode,''))=%s
                  AND canonical_entry_identity IS NOT NULL
            ) AS trades_today,
            COUNT(*) FILTER (
                WHERE execution_mode IS NULL OR BTRIM(execution_mode)=''
            ) AS null_mode_fills_ignored,
            COUNT(*) FILTER (
                WHERE execution_mode IS NOT NULL
                  AND BTRIM(execution_mode)<>''
                  AND LOWER(execution_mode)<>%s
            ) AS wrong_mode_fills_ignored,
            COUNT(*) FILTER (
                WHERE canonical_entry_identity IS NULL
            ) AS missing_identity_fills_ignored
        FROM filled_entries
        """,
        (
            client_id,
            list(_BROKER_CONFIRMED_ENTRY_FILL_STATUSES),
            start_utc,
            end_utc,
            mode,
            mode,
        ),
    )
    row = cursor.fetchone() or {}

    cursor.execute(
        """
        SELECT COUNT(*) AS n
        FROM positions
        WHERE client_id=%s
          AND entry_ts >= %s
          AND entry_ts < %s
          AND (
              plan_id LIKE 'reconciled:%%'
              OR close_source='expired_contract_cleanup'
          )
        """,
        (client_id, start_utc, end_utc),
    )
    position_row = cursor.fetchone() or {}
    return {
        "trades_today": int(row.get("trades_today") or 0),
        "trades_today_source": "broker_confirmed_entry_orders",
        "synthetic_position_rows_ignored": int(position_row.get("n") or 0),
        "null_mode_fills_ignored": int(row.get("null_mode_fills_ignored") or 0),
        "wrong_mode_fills_ignored": int(row.get("wrong_mode_fills_ignored") or 0),
        "missing_identity_fills_ignored": int(
            row.get("missing_identity_fills_ignored") or 0
        ),
        "trade_count_query_status": "ok",
    }


_PENDING_EXIT_STATUSES = (
    "EXIT_REQUESTED",
    "EXIT_SUBMITTED",
    "EXIT_ACKNOWLEDGED",
    "EXIT_PARTIAL_FILL",
)


def _filled_entry_row_cost(row: dict) -> float:
    fill_price = row.get("fill_price")
    filled_qty = int(row.get("filled_qty") or 0)
    reserved_cost = row.get("reserved_cost")
    if fill_price is not None and filled_qty > 0:
        return float(fill_price) * filled_qty * 100
    if filled_qty > 0 and reserved_cost is not None:
        return float(reserved_cost or 0.0)
    return 0.0


def _normalize_match_value(value, *, uppercase: bool = False) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return text.upper() if uppercase else text.lower()


def _order_identifier(row: dict) -> str:
    for key in ("local_order_id", "broker_order_id", "id", "signal_id", "plan_id", "contract"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    return ""


def _active_position_match_indexes(active_positions: list[dict]) -> dict[str, dict[str, dict]]:
    indexes = {
        "position_id": {},
        "broker_order_id": {},
        "local_order_id": {},
        "signal_id": {},
        "plan_id": {},
        "contract": {},
    }
    for position in active_positions or []:
        normalized_pairs = (
            ("position_id", _normalize_match_value(position.get("id"))),
            ("broker_order_id", _normalize_match_value(position.get("broker_order_id"))),
            ("local_order_id", _normalize_match_value(position.get("local_order_id"))),
            ("signal_id", _normalize_match_value(position.get("signal_id"))),
            ("plan_id", _normalize_match_value(position.get("plan_id"))),
            ("contract", _normalize_match_value(position.get("contract"), uppercase=True)),
        )
        for key, normalized_value in normalized_pairs:
            if normalized_value and normalized_value not in indexes[key]:
                indexes[key][normalized_value] = position
    return indexes


def _match_fill_row_to_active_position(fill_row: dict, active_position_indexes: dict[str, dict[str, dict]]) -> tuple[dict | None, str]:
    match_order = (
        ("position_id", _normalize_match_value(fill_row.get("position_id"))),
        ("broker_order_id", _normalize_match_value(fill_row.get("broker_order_id"))),
        ("local_order_id", _normalize_match_value(fill_row.get("local_order_id"))),
        ("signal_id", _normalize_match_value(fill_row.get("signal_id"))),
        ("plan_id", _normalize_match_value(fill_row.get("plan_id"))),
        ("contract", _normalize_match_value(fill_row.get("contract"), uppercase=True)),
    )
    for match_key, match_value in match_order:
        if not match_value:
            continue
        matched_position = active_position_indexes.get(match_key, {}).get(match_value)
        if matched_position:
            return matched_position, match_key
    return None, ""


# Canonical set of status values that signal a position is fully settled.
# Kept in sync with PositionStatus.TERMINAL for snapshot/exposure accounting.
# CLOSED_REPAIR: reconciler repair path that terminally closes positions whose
#   broker fill confirmed exit but the OSM close path ran a second time.
# CANCELED/CANCELLED: both spellings appear in production due to broker
#   response normalization differences.
_TERMINAL_STATUS_SET = frozenset({
    "CLOSED",
    "CLOSED_REPAIR",
    "EXPIRED",
    "STOPPED",
    "TAKEN_PROFIT",
    "ERROR",
    "CANCELED",
    "CANCELLED",
})


def _is_position_row_terminal(p: dict) -> bool:
    """Return True if a position row should be treated as terminal for the
    purposes of exposure/snapshot accounting.

    Two paths to terminal (OR logic — either is sufficient):
      1. Status is in the canonical terminal set (primary check).
      2. quantity_remaining = 0 AND exit_ts IS NOT NULL (defensive check).
         Covers repair statuses, unknown variants, or rows where the status
         field has an unexpected value but the position is economically settled.

    Active statuses (OPEN, CLOSING, PARTIAL) are NOT considered terminal
    unless path 2 fires — meaning a row with status=OPEN but qty_remaining=0
    and an exit_ts is treated as defensively settled (edge case from partial
    close reconciler bugs).
    """
    status_upper = str(p.get("status") or "").upper().strip()
    if status_upper in _TERMINAL_STATUS_SET:
        return True
    # Defensive: settled by quantity_remaining=0 AND exit_ts present
    qty_remaining = p.get("quantity_remaining")
    exit_ts = p.get("exit_ts")
    try:
        qty_zero = int(qty_remaining if qty_remaining is not None else 1) == 0
    except (TypeError, ValueError):
        qty_zero = False
    if qty_zero and exit_ts is not None and exit_ts != "":
        return True
    return False


def _build_terminal_position_id_set(terminal_positions: list[dict]) -> set[str]:
    """Return a set of lowercased position IDs for positions that are terminal.

    Each position is evaluated by _is_position_row_terminal():
      - status in _TERMINAL_STATUS_SET (expanded to include CLOSED_REPAIR,
        CANCELED/CANCELLED, and other repair variants), OR
      - quantity_remaining=0 AND exit_ts IS NOT NULL (defensive fallback for
        unexpected status values on economically settled positions).
    """
    result: set[str] = set()
    for p in terminal_positions or []:
        if not _is_position_row_terminal(p):
            continue
        pid = str(p.get("id") or "").strip().lower()
        if pid:
            result.add(pid)
    return result


def _summarize_fill_truth_rows(
    fill_rows: list[dict],
    active_positions: list[dict],
    *,
    terminal_positions: list[dict] | None = None,
    client_id: str = "",
    execution_mode: str = "",
) -> dict[str, float | int | list]:
    active_position_indexes = _active_position_match_indexes(active_positions or [])
    terminal_position_id_set = _build_terminal_position_id_set(terminal_positions or [])

    filled_unreconciled_entry_capital = 0.0
    ignored_already_reconciled_fill_capital = 0.0
    ignored_already_reconciled_order_ids: list[str] = []
    ignored_reconciled_match_keys: list[str] = []
    pending_entries = 0
    filled_unreconciled_calls = 0
    filled_unreconciled_puts = 0

    # P0: track terminal-linked fills we suppress
    _terminal_ignored_order_ids: list[str] = []
    _terminal_ignored_position_ids: list[str] = []
    _terminal_ignored_capital: float = 0.0
    # P0: track filled entries with a position_id that resolves to nothing
    _missing_position_rows: list[dict] = []

    # Build a set of all known position IDs (active + terminal) for resolution checks
    _all_known_position_ids: set[str] = set()
    for _p in (active_positions or []):
        _pid = str(_p.get("id") or "").strip().lower()
        if _pid:
            _all_known_position_ids.add(_pid)
    _all_known_position_ids |= terminal_position_id_set

    for row in fill_rows or []:
        row_cost = _filled_entry_row_cost(row)
        order_id = _order_identifier(row)

        # ── P0: terminal-linked-fill guard ──────────────────────────────────
        # A FILLED ENTRY order linked to a TERMINAL (CLOSED/EXPIRED/etc.)
        # position MUST NOT count as unreconciled exposure. The position already
        # settled — it is done. Without this guard, a CLOSED position's fill
        # keeps consuming capital headroom forever (Jason regression: BAC+WFC).
        #
        # Resolution priority:
        #   1. position_id in row → check terminal set (definitive)
        #   2. position_id in row but resolves to nothing → warn, count as
        #      unreconciled (fail-closed: unknown state, assume live exposure)
        #   3. No position_id → normal active-position match logic applies
        row_position_id = str(row.get("position_id") or "").strip()
        row_position_id_lower = row_position_id.lower()

        if row_position_id:
            # Has a position_id — check if it resolves to a terminal position
            if row_position_id_lower in terminal_position_id_set:
                # Definitively terminal — suppress this fill from exposure
                _terminal_ignored_order_ids.append(order_id or str(row.get("id") or ""))
                _terminal_ignored_position_ids.append(row_position_id)
                _terminal_ignored_capital += row_cost
                continue

            # position_id present but not in active OR terminal — orphan warning
            if row_position_id_lower not in _all_known_position_ids:
                _missing_position_rows.append({
                    "order_id": order_id or str(row.get("id") or ""),
                    "position_id": row_position_id,
                    "capital": row_cost,
                })
                # Fail-closed: count as unreconciled since we cannot confirm terminal
                pending_entries += 1
                filled_unreconciled_entry_capital += row_cost
                direction = str(row.get("direction") or "").upper().strip()
                if direction == "CALL":
                    filled_unreconciled_calls += 1
                elif direction == "PUT":
                    filled_unreconciled_puts += 1
                continue

        # ── Standard active-position reconciliation check ───────────────────
        matched_position, match_key = _match_fill_row_to_active_position(row, active_position_indexes)
        if matched_position:
            ignored_already_reconciled_fill_capital += row_cost
            if order_id:
                ignored_already_reconciled_order_ids.append(order_id)
            matched_position_id = str(matched_position.get("id") or "").strip()
            if match_key:
                if order_id:
                    ignored_reconciled_match_keys.append(
                        f"{order_id}:{match_key}:{matched_position_id or 'active_position'}"
                    )
                else:
                    ignored_reconciled_match_keys.append(
                        f"{match_key}:{matched_position_id or 'active_position'}"
                    )
            continue

        pending_entries += 1
        filled_unreconciled_entry_capital += row_cost

        direction = str(row.get("direction") or "").upper().strip()
        if direction == "CALL":
            filled_unreconciled_calls += 1
        elif direction == "PUT":
            filled_unreconciled_puts += 1

    # ── Structured logging for terminal-suppressed fills ────────────────────
    if _terminal_ignored_order_ids:
        log.info(
            "SNAPSHOT_RECONCILED_TERMINAL_FILLED_IGNORED "
            "client=%s execution_mode=%s order_ids=%s position_ids=%s ignored_capital=%.2f",
            client_id, execution_mode,
            _terminal_ignored_order_ids,
            _terminal_ignored_position_ids,
            _terminal_ignored_capital,
        )

    # ── Structured warning for fills with unresolvable position_id ──────────
    for _mp in _missing_position_rows:
        log.warning(
            "SNAPSHOT_FILLED_ENTRY_MISSING_POSITION_COUNTS_UNRECONCILED "
            "client=%s execution_mode=%s order_id=%s position_id=%s capital=%.2f",
            client_id, execution_mode,
            _mp["order_id"], _mp["position_id"], _mp["capital"],
        )

    return {
        "pending_entries": pending_entries,
        "filled_unreconciled_calls": filled_unreconciled_calls,
        "filled_unreconciled_puts": filled_unreconciled_puts,
        "filled_unreconciled_entry_capital": filled_unreconciled_entry_capital,
        "ignored_already_reconciled_fill_capital": ignored_already_reconciled_fill_capital,
        "ignored_already_reconciled_order_ids": ignored_already_reconciled_order_ids,
        "ignored_reconciled_match_keys": ignored_reconciled_match_keys,
        # P0 audit fields
        "terminal_ignored_order_ids": _terminal_ignored_order_ids,
        "terminal_ignored_position_ids": _terminal_ignored_position_ids,
        "terminal_ignored_capital": _terminal_ignored_capital,
        "missing_position_rows": _missing_position_rows,
    }


class APPositionManager:
    """Client-scoped position + order truth layer backed by Postgres."""

    def __init__(self, client_id: str):
        # PR #237: normalize client_id once at the boundary.  All downstream
        # SQL runs against self.client_id, so if we do not canonicalize here,
        # a caller passing "Jason ", "JASON", or "jason\n" writes and queries
        # under different keys and quietly loses reconciliation.
        self.client_id = str(client_id or "").strip().lower()
        self._position_columns_cache: Optional[set[str]] = None
        log.info("[%s] APPositionManager initialized", self.client_id)

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

    def _proof_row_exists(self, *, position_id: str = "", local_order_id: str = "") -> Optional[bool]:
        state = self._proof_row_binding_state(position_id=position_id, local_order_id=local_order_id)
        if state is None:
            return None
        return state in {"bound_position", "local_order_only"}

    def _proof_row_binding_state(self, *, position_id: str = "", local_order_id: str = "") -> Optional[str | bool]:
        def _fn():
            with conn() as c:
                if position_id:
                    c.execute(
                        "SELECT 1 FROM proof_trades WHERE client_email=%s AND position_id=%s LIMIT 1",
                        (self.client_id, position_id),
                    )
                    if c.fetchone():
                        return "bound_position"
                if local_order_id:
                    c.execute(
                        "SELECT 1 FROM proof_trades WHERE client_email=%s AND local_order_id=%s LIMIT 1",
                        (self.client_id, local_order_id),
                    )
                    if c.fetchone():
                        return "local_order_only"
                return False

        try:
            return run_with_retry(_fn)
        except Exception as exc:
            log.error("[%s] proof row existence check failed: %s", self.client_id, exc)
            return None

    def _load_signal_side(self, signal_id: str) -> Optional[str]:
        canonical_signal_id = str(signal_id or "").strip()
        if not canonical_signal_id:
            return None
        try:
            from ap_signal_store import canonical_signal_id as _canon_signal_id
            canonical_signal_id = str(_canon_signal_id(canonical_signal_id) or "").strip()
        except Exception:
            canonical_signal_id = str(signal_id or "").strip()
        if not canonical_signal_id:
            return None

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT side, signal_payload
                    FROM ap_signals
                    WHERE signal_id::text=%s
                      AND client_email=%s
                    LIMIT 2
                    """,
                    (canonical_signal_id, self.client_id),
                )
                return c.fetchall()

        try:
            rows = list(run_with_retry(_fn) or [])
        except Exception as exc:
            log.error(
                "[%s] signal side lookup failed | signal_id=%s err=%s",
                self.client_id,
                canonical_signal_id,
                exc,
            )
            return None

        if len(rows) != 1:
            if len(rows) > 1:
                log.critical(
                    "[%s] TERMINAL_CLOSE_SIGNAL_IDENTITY_AMBIGUOUS signal_id=%s rows=%s",
                    self.client_id,
                    canonical_signal_id,
                    len(rows),
                )
            return None

        row = dict(rows[0] or {})
        payload = row.get("signal_payload")
        if isinstance(payload, str):
            try:
                import json as _json
                payload = _json.loads(payload)
            except Exception:
                payload = None
        return _normalize_proof_side(
            row.get("side")
            or (payload or {}).get("side")
            or (payload or {}).get("direction")
        )

    def _resolve_terminal_strategy_metadata(self, entry_order: dict) -> dict:
        entry_order = entry_order if isinstance(entry_order, dict) else {}
        entry_meta = entry_order.get("meta")
        if isinstance(entry_meta, str):
            try:
                import json as _json
                entry_meta = _json.loads(entry_meta)
            except Exception:
                entry_meta = None
        if not isinstance(entry_meta, dict):
            entry_meta = {}

        def _first_text(*values) -> str:
            for value in values:
                text = str(value or "").strip()
                if text:
                    return text
            return ""

        def _first_float(*values) -> float:
            for value in values:
                try:
                    return float(value)
                except Exception:
                    continue
            return 0.0

        pattern = _first_text(
            entry_order.get("pattern"),
            entry_order.get("setup_pattern"),
            entry_meta.get("pattern"),
            entry_meta.get("setup_pattern"),
        )
        timeframe = _first_text(
            entry_order.get("timeframe"),
            entry_order.get("tf"),
            entry_meta.get("timeframe"),
            entry_meta.get("tf"),
        )
        tier = _first_text(
            entry_order.get("tier"),
            entry_order.get("setup_tier"),
            entry_meta.get("tier"),
            entry_meta.get("setup_tier"),
        )
        score = _first_float(
            entry_order.get("score"),
            entry_order.get("setup_score"),
            entry_meta.get("score"),
            entry_meta.get("setup_score"),
        )
        context_score = _first_float(
            entry_order.get("context_score"),
            entry_order.get("ctx_score"),
            entry_meta.get("context_score"),
            entry_meta.get("ctx_score"),
        )

        if pattern and timeframe and tier and score > 0:
            return {
                "pattern": pattern,
                "timeframe": timeframe,
                "score": score,
                "tier": tier,
                "context_score": context_score,
                "metadata_quarantined": False,
            }

        return {
            "pattern": "UNKNOWN_QUARANTINED",
            "timeframe": "unknown",
            "score": 0.0,
            "tier": "UNKNOWN_QUARANTINED",
            "context_score": 0.0,
            "metadata_quarantined": True,
        }

    def _resolve_terminal_proof_identity(
        self,
        *,
        position_id: str,
        local_order_id: str,
        side: str,
    ) -> dict:
        from ap.db import get_order_by_id

        candidate_local_order_id = str(local_order_id or "").strip()
        entry_local_order_id = candidate_local_order_id
        entry_order = None
        invalid_origin = False
        if entry_local_order_id:
            try:
                entry_order = get_order_by_id(entry_local_order_id, client_id=self.client_id)
            except Exception as exc:
                log.error(
                    "[%s] terminal proof entry-order lookup failed | local_order_id=%s err=%s",
                    self.client_id,
                    entry_local_order_id,
                    exc,
                )
            if not entry_order or str(entry_order.get("kind") or "").upper() != "ENTRY":
                log.critical(
                    "[%s] TERMINAL_CLOSE_ENTRY_IDENTITY_INVALID pos=%s local_order_id=%s kind=%s",
                    self.client_id,
                    position_id,
                    entry_local_order_id,
                    str((entry_order or {}).get("kind") or "missing"),
                )
                entry_order = None
                entry_local_order_id = ""
                invalid_origin = True

        entry_meta = (entry_order or {}).get("meta")
        if isinstance(entry_meta, str):
            try:
                import json as _json
                entry_meta = _json.loads(entry_meta)
            except Exception:
                entry_meta = None
        if not isinstance(entry_meta, dict):
            entry_meta = {}

        signal_side = self._load_signal_side(str((entry_order or {}).get("signal_id") or ""))
        order_side = _normalize_proof_side(
            (entry_order or {}).get("direction")
            or (entry_order or {}).get("side")
            or entry_meta.get("direction")
            or entry_meta.get("side")
        )
        position_side = _normalize_proof_side(side)

        candidates = [
            ("position", position_side),
            ("entry_order", order_side),
            ("signal", signal_side),
        ]
        concrete = [(src, value) for src, value in candidates if value in {"CALL", "PUT"}]
        distinct_sides = {value for _, value in concrete}
        if len(distinct_sides) > 1:
            log.critical(
                "[%s] TERMINAL_CLOSE_SIDE_AMBIGUOUS pos=%s local_order_id=%s candidates=%s",
                self.client_id,
                position_id,
                entry_local_order_id,
                concrete,
            )
            resolved_side = "UNKNOWN_QUARANTINED"
            side_quarantined = True
        elif concrete:
            resolved_side = concrete[0][1]
            side_quarantined = False
        else:
            log.critical(
                "[%s] TERMINAL_CLOSE_SIDE_MISSING pos=%s local_order_id=%s",
                self.client_id,
                position_id,
                entry_local_order_id,
            )
            resolved_side = "UNKNOWN_QUARANTINED"
            side_quarantined = True

        resolved_mode = str((entry_order or {}).get("execution_mode") or "").strip().lower()
        if resolved_mode not in {"live", "paper"}:
            resolved_mode = str(entry_meta.get("execution_mode") or "").strip().lower()
        if resolved_mode not in {"live", "paper"}:
            if entry_local_order_id:
                log.critical(
                    "[%s] TERMINAL_CLOSE_EXECUTION_MODE_UNPROVEN pos=%s local_order_id=%s",
                    self.client_id,
                    position_id,
                    entry_local_order_id,
                )
            resolved_mode = "unknown"

        return {
            "entry_local_order_id": entry_local_order_id,
            "entry_order": entry_order or {},
            "invalid_origin": invalid_origin,
            "resolved_side": resolved_side,
            "side_quarantined": side_quarantined,
            "resolved_execution_mode": resolved_mode,
        }

    def _claim_recent_broker_repair_proof(
        self,
        *,
        position_id: str,
        contract: str,
        closed_at: str,
        local_order_id: str = "",
        execution_mode: str = "",
        side: str = "",
        contracts: int = 0,
        entry_option_price: float = 0.0,
    ) -> bool:
        normalized_contract = _normalize_proof_contract(contract)
        repair_position_id = f"broker-repair-{self.client_id}-{normalized_contract}"
        entry_local_order_id = str(local_order_id or "").strip()
        resolved_mode = str(execution_mode or "").strip().lower()
        resolved_side = _normalize_proof_side(side)
        try:
            expected_contracts = int(contracts or 0)
        except Exception:
            expected_contracts = 0
        try:
            expected_entry_price = float(entry_option_price or 0)
        except Exception:
            expected_entry_price = 0.0

        if (
            not position_id
            or not normalized_contract
            or not closed_at
            or not entry_local_order_id
            or resolved_mode not in {"live", "paper"}
            or resolved_side not in {"CALL", "PUT"}
        ):
            log.critical(
                "[%s] BROKER_REPAIR_PROOF_QUARANTINED pos=%s contract=%s reason=missing_lifecycle_identity",
                self.client_id, position_id, normalized_contract,
            )
            return False

        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT id
                    FROM proof_trades
                    WHERE client_email = %s
                      AND position_id = %s
                      AND local_order_id = %s
                      AND LOWER(COALESCE(NULLIF(BTRIM(execution_mode), ''), NULLIF(BTRIM(mode), ''), '')) = %s
                      AND UPPER(COALESCE(side, '')) = %s
                      AND (
                            %s <= 0
                         OR COALESCE(contracts, %s) = %s
                      )
                      AND (
                            %s <= 0
                         OR COALESCE(entry_option_price, 0) <= 0
                         OR ABS(COALESCE(entry_option_price, 0) - %s) < 0.0001
                      )
                    LIMIT 2
                    """,
                    (
                        self.client_id,
                        repair_position_id,
                        entry_local_order_id,
                        resolved_mode,
                        resolved_side,
                        expected_contracts,
                        expected_contracts,
                        expected_contracts,
                        expected_entry_price,
                        expected_entry_price,
                    ),
                )
                candidates = c.fetchall() or []
                if len(candidates) != 1:
                    return {"claimed": False, "candidate_count": len(candidates)}
                candidate = dict(candidates[0] or {})
                c.execute(
                    """
                    UPDATE proof_trades
                       SET position_id = %s,
                           local_order_id = CASE
                               WHEN COALESCE(local_order_id, '') = '' AND %s <> ''
                               THEN %s
                               ELSE local_order_id
                           END
                     WHERE id = %s
                       AND client_email = %s
                       AND position_id = %s
                       AND local_order_id = %s
                       AND LOWER(COALESCE(NULLIF(BTRIM(execution_mode), ''), NULLIF(BTRIM(mode), ''), '')) = %s
                       AND UPPER(COALESCE(side, '')) = %s
                       AND (
                             %s <= 0
                          OR COALESCE(contracts, %s) = %s
                       )
                       AND (
                             %s <= 0
                          OR COALESCE(entry_option_price, 0) <= 0
                          OR ABS(COALESCE(entry_option_price, 0) - %s) < 0.0001
                       )
                    RETURNING id
                    """,
                    (
                        position_id,
                        entry_local_order_id,
                        entry_local_order_id,
                        candidate.get("id"),
                        self.client_id,
                        repair_position_id,
                        entry_local_order_id,
                        resolved_mode,
                        resolved_side,
                        expected_contracts,
                        expected_contracts,
                        expected_contracts,
                        expected_entry_price,
                        expected_entry_price,
                    ),
                )
                rows = c.fetchall() or []
                return {
                    "claimed": len(rows) == 1,
                    "candidate_count": len(candidates),
                    "updated_count": len(rows),
                }

        try:
            outcome = dict(run_with_retry(_fn) or {})
        except Exception as exc:
            log.warning(
                "[%s] broker repair proof claim failed | pos=%s contract=%s err=%s",
                self.client_id, position_id, contract, exc,
            )
            return False

        claimed = bool(outcome.get("claimed"))
        if not claimed:
            log.critical(
                "[%s] BROKER_REPAIR_PROOF_QUARANTINED pos=%s contract=%s candidate_count=%s updated_count=%s",
                self.client_id,
                position_id,
                contract,
                outcome.get("candidate_count"),
                outcome.get("updated_count"),
            )
            return False

        if claimed:
            log.info(
                "[%s] broker repair proof claimed | pos=%s contract=%s",
                self.client_id, position_id, contract,
            )
        return claimed

    def _write_missing_terminal_proof(
        self,
        *,
        position_id: str,
        local_order_id: str,
        contract: str,
        underlying: str,
        side: str,
        opened_at: str,
        closed_at: str,
        entry_option_price: float,
        exit_option_price: float,
        contracts: int,
        exit_reason: str,
        option_pnl_pct: float,
        setup_status: str,
        execution_mode: str = "",
        exit_fill_price: Optional[float] = None,
        synthetic_entry: bool = False,
    ) -> bool:
        identity = self._resolve_terminal_proof_identity(
            position_id=position_id,
            local_order_id=local_order_id,
            side=side,
        )
        if identity.get("invalid_origin"):
            log.critical(
                "[%s] TERMINAL_CLOSE_PROOF_ORIGIN_INVALID pos=%s local_order_id=%s source=%s",
                self.client_id, position_id, local_order_id, setup_status,
            )
            return False
        resolved_mode = str(identity.get("resolved_execution_mode") or "unknown")
        resolved_side = str(identity.get("resolved_side") or "UNKNOWN_QUARANTINED")
        strategy = self._resolve_terminal_strategy_metadata(dict(identity.get("entry_order") or {}))
        quarantined = (
            bool(identity.get("side_quarantined"))
            or resolved_mode not in {"live", "paper"}
            or bool(strategy.get("metadata_quarantined"))
        )
        proof_setup_status = str(setup_status or "")
        if strategy.get("metadata_quarantined"):
            log.critical(
                "[%s] TERMINAL_CLOSE_STRATEGY_METADATA_QUARANTINED pos=%s local_order_id=%s",
                self.client_id, position_id, local_order_id,
            )
            proof_setup_status = f"{proof_setup_status}|TERMINAL_METADATA_QUARANTINED"

        try:
            from ap.queue import _get_sb_client
            from ap_proof_logger import APProofLogger
        except Exception as exc:
            log.error(
                "[%s] missing terminal proof logger unavailable | pos=%s contract=%s err=%s",
                self.client_id, position_id, contract, exc,
            )
            return False

        try:
            sb = _get_sb_client()
        except Exception as exc:
            log.error(
                "[%s] missing terminal proof Supabase lookup failed | pos=%s contract=%s err=%s",
                self.client_id, position_id, contract, exc,
            )
            return False

        if sb is None:
            log.error(
                "[%s] missing terminal proof blocked | pos=%s contract=%s reason=missing_supabase_client",
                self.client_id, position_id, contract,
            )
            return False

        try:
            proof = APProofLogger(
                supabase_client=sb,
                client_email=self.client_id,
                mode=resolved_mode,
            )
            result = proof.log_trade(
                ticker=underlying or contract,
                pattern=strategy["pattern"],
                side=resolved_side,
                timeframe=strategy["timeframe"],
                score=float(strategy["score"]),
                tier=strategy["tier"],
                context_score=float(strategy["context_score"]),
                setup_status=proof_setup_status,
                entry_trigger=float(entry_option_price or 0),
                entry_option_price=float(entry_option_price or 0),
                exit_option_price=float(exit_option_price or 0),
                underlying_entry=0.0,
                underlying_exit=0.0,
                contracts=max(int(contracts or 1), 1),
                exit_reason=exit_reason,
                option_pnl_pct=round(float(option_pnl_pct or 0), 2),
                underlying_pnl_pct=0.0,
                win=float(option_pnl_pct or 0) > 0,
                spread_pct=0.0,
                chain_grade="",
                opened_at=datetime.fromisoformat(str(opened_at)),
                closed_at=datetime.fromisoformat(str(closed_at)),
                synthetic_entry=bool(synthetic_entry or quarantined),
                position_id=position_id or "",
                local_order_id=local_order_id or "",
                execution_mode=resolved_mode,
                exit_fill_price=(float(exit_fill_price) if exit_fill_price is not None else None),
            )
        except Exception as exc:
            log.error(
                "[%s] missing terminal proof write failed | pos=%s contract=%s err=%s",
                self.client_id, position_id, contract, exc,
            )
            return False

        if result.get("_proof_persisted") is True:
            return True

        log.error(
            "[%s] missing terminal proof write failed | pos=%s contract=%s err=%s",
            self.client_id,
            position_id,
            contract,
            result.get("_proof_persistence_error") or "persistence_not_confirmed",
        )
        return False

    def _with_terminal_proof_lock(self, position_id: str, fn):
        """PR #386 blocker 3: serialize the entire terminal-proof-binding
        sequence (binding-state read → repair-claim → fallback insert →
        final binding re-read) across processes/workers.

        Uses a Postgres transaction-scoped advisory lock keyed by
        ``terminal-proof-bind:{client_id}:{position_id}``. Overlapping
        callers on the same key block at acquisition and the second caller
        observes ``bound_position`` on the re-read, converging to
        idempotent success without a duplicate proof insert.
        """
        lock_key = f"terminal-proof-bind:{self.client_id}:{position_id}"

        def _guarded():
            with conn() as c:
                c.execute(
                    "SELECT pg_advisory_xact_lock(('x' || md5(%s))::bit(64)::bigint)",
                    (lock_key,),
                )
                result = fn()
                # Final binding re-read (still under the advisory lock).
                # A returned True MUST be witnessed by a bound canonical
                # proof row for this position; otherwise fail so the exit
                # engine is not evicted.
                if result is True:
                    state = self._proof_row_binding_state(position_id=position_id)
                    if state != "bound_position":
                        log.critical(
                            "[%s] TERMINAL_PROOF_LOCK_REREAD_UNBOUND pos=%s state=%s",
                            self.client_id, position_id, state,
                        )
                        return False
                return result

        try:
            return run_with_retry(_guarded)
        except Exception as exc:
            log.error(
                "[%s] TERMINAL_PROOF_LOCK_FAILED pos=%s err=%s",
                self.client_id, position_id, exc,
            )
            return False

    def _ensure_terminal_close_proof(
        self,
        *,
        position_id: str,
        local_order_id: str,
        contract: str,
        underlying: str,
        side: str,
        opened_at: str,
        closed_at: str,
        entry_option_price: float,
        exit_option_price: float,
        contracts: int,
        exit_reason: str,
        option_pnl_pct: float,
        setup_status: str,
        execution_mode: str = "",
        exit_fill_price: Optional[float] = None,
        allow_fallback_insert: bool,
        missing_reason_code: str,
    ) -> bool:
        identity = self._resolve_terminal_proof_identity(
            position_id=position_id,
            local_order_id=local_order_id,
            side=side,
        )
        entry_local_order_id = str(identity.get("entry_local_order_id") or "")
        if identity.get("invalid_origin"):
            log.critical(
                "[%s] TERMINAL_CLOSE_PROOF_ORIGIN_INVALID pos=%s local_order_id=%s source=%s",
                self.client_id, position_id, local_order_id, setup_status,
            )
            proof_exists = self._proof_row_exists(position_id=position_id)
            if proof_exists is True:
                return True
            return False

        proof_state = self._proof_row_binding_state(
            position_id=position_id,
            local_order_id=entry_local_order_id,
        )
        if proof_state == "bound_position":
            return True

        if proof_state is None:
            log.critical(
                "[%s] TERMINAL_CLOSE_PROOF_UNVERIFIED pos=%s local_order_id=%s source=%s",
                self.client_id, position_id, entry_local_order_id, setup_status,
            )
            return False

        claimed = self._claim_recent_broker_repair_proof(
            position_id=position_id,
            contract=contract,
            closed_at=closed_at,
            local_order_id=entry_local_order_id,
            execution_mode=str(identity.get("resolved_execution_mode") or execution_mode or ""),
            side=str(identity.get("resolved_side") or side or ""),
            contracts=contracts,
            entry_option_price=entry_option_price,
        )
        if claimed:
            return True

        if proof_state == "local_order_only":
            log.critical(
                "[%s] TERMINAL_CLOSE_PROOF_REPAIR_BIND_FAILED pos=%s local_order_id=%s source=%s",
                self.client_id, position_id, entry_local_order_id, setup_status,
            )
            return False

        if not allow_fallback_insert:
            log.critical(
                "[%s] %s pos=%s contract=%s source=%s",
                self.client_id, missing_reason_code, position_id, contract, setup_status,
            )
            return False

        persisted = self._write_missing_terminal_proof(
            position_id=position_id,
            local_order_id=entry_local_order_id,
            contract=contract,
            underlying=underlying,
            side=str(identity.get("resolved_side") or side),
            opened_at=opened_at,
            closed_at=closed_at,
            entry_option_price=entry_option_price,
            exit_option_price=exit_option_price,
            contracts=contracts,
            exit_reason=exit_reason,
            option_pnl_pct=option_pnl_pct,
            setup_status=setup_status,
            execution_mode=str(identity.get("resolved_execution_mode") or execution_mode or "unknown"),
            exit_fill_price=exit_fill_price,
            synthetic_entry=bool(identity.get("side_quarantined")) or str(identity.get("resolved_execution_mode") or "") not in {"live", "paper"},
        )
        if persisted:
            log.info(
                "[%s] proof_trades logged via APProofLogger | pos=%s source=%s",
                self.client_id, position_id, setup_status,
            )
            return True

        log.error(
            "[%s] %s pos=%s contract=%s source=%s",
            self.client_id, missing_reason_code, position_id, contract, setup_status,
        )
        return False

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
                    WHERE client_id=%s
                      AND (
                        UPPER(COALESCE(status,'')) IN ('OPEN','CLOSING','PARTIAL','ACTIVE')
                        OR COALESCE(quantity_remaining, 0) > 0
                      )
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
                      AND (
                        UPPER(COALESCE(status,'')) IN ('OPEN','CLOSING','PARTIAL','ACTIVE')
                        OR COALESCE(quantity_remaining, 0) > 0
                      )
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
        """Return all positions that are live and need exit management.

        P0-PARTIAL-CLOSE: expands status filter to include PARTIAL and ACTIVE,
        and adds COALESCE(quantity_remaining,0)>0 safety guard so any row
        incorrectly marked CLOSED with remaining contracts is still included.
        """
        def _fn():
            with conn() as c:
                c.execute(
                    """
                    SELECT *
                    FROM positions
                    WHERE client_id=%s
                      AND (
                        UPPER(COALESCE(status,'')) IN ('OPEN','CLOSING','PARTIAL','ACTIVE')
                        OR COALESCE(quantity_remaining, 0) > 0
                      )
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
        # P0 FIX (2026-05-21): apply the same phantom-grace exclusion that
        # snapshot() uses. Without this, a CREATED order from a previous day
        # that never made it to the broker (no broker_order_id, lost handoff)
        # would block every future signal on this ticker FOREVER, because the
        # row sits in CREATED status until the order_monitor's 120s watchdog
        # cleans it up — and across restarts that watchdog may not re-process
        # historical phantoms.
        #
        # Phantom rule (matches snapshot() in this file):
        #   exclude orders that are simultaneously
        #     - status in (CREATED, PENDING_TRIGGER), AND
        #     - no broker_order_id, AND
        #     - older than PENDING_ENTRY_PHANTOM_GRACE_SEC (default 30s)
        def _fn():
            with conn() as c:
                _phantom_grace_sec = int(os.getenv("PENDING_ENTRY_PHANTOM_GRACE_SEC", "30"))
                placeholders = ",".join(["%s"] * len(_PENDING_ENTRY_STATUSES))
                c.execute(
                    f"""
                    SELECT 1
                    FROM orders
                    WHERE client_id=%s
                      AND symbol=%s
                      AND kind='ENTRY'
                      AND status IN ({placeholders})
                      AND NOT (
                        status IN ('CREATED', 'PENDING_TRIGGER')
                        AND (broker_order_id IS NULL OR broker_order_id = '')
                        AND created_ts < NOW() - (%s || ' seconds')::interval
                      )
                    ORDER BY created_ts DESC NULLS LAST, updated_ts DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, ticker.upper(), *_PENDING_ENTRY_STATUSES, str(_phantom_grace_sec)),
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
        execution_mode: Optional[str] = None,
        historical_plan_idempotency: bool = False,
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
        numeric_entry_price = float(entry_price or 0)
        if not math.isfinite(numeric_entry_price) or numeric_entry_price <= 0:
            raise ValueError(f"entry_price must be positive, got {entry_price}")

        # PR #237: fail-CLOSED on missing/invalid side.  Raises
        # ValueError("invalid_or_missing_position_side:<raw>") — never
        # substitute a default direction here.  This is the last line of
        # defense before persisting the row to the positions truth layer.
        _side = _normalize_position_side(side)

        _execution_mode = str(execution_mode or "").strip().lower()
        if _execution_mode not in {"paper", "live"}:
            raise ValueError(f"invalid_or_missing_position_execution_mode:{execution_mode}")

        has_local_col = self._has_position_column("local_order_id")
        has_broker_col = self._has_position_column("broker_order_id")
        has_underlying_entry_col = self._has_position_column("underlying_entry")
        has_execution_mode_col = self._has_position_column("execution_mode")
        if not has_execution_mode_col:
            raise RuntimeError("required_position_column_missing:execution_mode")

        position_id = str(uuid.uuid4())
        ts = now_utc_iso()
        lock_key = self._position_idempotency_key(
            local_order_id=local_order_id if has_local_col else None,
            broker_order_id=broker_order_id if has_broker_col else None,
            plan_id=plan_id,
            signal_id=signal_id,
        )

        # PR: sizing-bootstrap-fix
        # entry_price was missing from the INSERT — only avg_fill was written.
        # ManagedPosition.option_pnl_pct reads entry_price; without it the
        # property returns 0.0 forever and profit-take/trailing-stop are dead.
        # We also persist entry_price to opened_at + entry_ts (same timestamp).
        # Existing column auto-detect handles deployments where entry_price
        # column may not yet exist (graceful no-op).
        has_entry_price_col = self._has_position_column("entry_price")
        has_opened_at_col   = self._has_position_column("opened_at")

        columns = [
            "id", "client_id", "plan_id", "signal_id",
            "underlying", "contract", "direction", "qty", "avg_fill",
            "tier", "score", "pattern", "tp_pct", "sl_pct",
            "stop_underlying", "target_underlying", "status",
            "entry_ts", "created_at", "updated_at",
        ]
        values = [
            position_id, self.client_id, plan_id, signal_id,
            ticker.upper(), contract, _side, int(qty), float(entry_price),
            tier, float(score), pattern, float(tp_pct), float(sl_pct),
            self._nullable_float(stop_underlying),
            self._nullable_float(target_underlying),
            PositionStatus.OPEN, ts, ts, ts,
        ]
        if has_entry_price_col:
            columns.append("entry_price")
            values.append(float(entry_price))
        if has_opened_at_col:
            columns.append("opened_at")
            values.append(ts)

        if has_underlying_entry_col:
            columns.append("underlying_entry")
            values.append(self._nullable_float(underlying_entry))

        if local_order_id and has_local_col:
            columns.append("local_order_id")
            values.append(local_order_id)
        if broker_order_id and has_broker_col:
            columns.append("broker_order_id")
            values.append(broker_order_id)
        columns.append("execution_mode")
        values.append(_execution_mode)

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
                status_clause = "" if historical_plan_idempotency else "AND status IN ('OPEN','CLOSING')"
                mode_clause = (
                    "AND LOWER(COALESCE(execution_mode,''))=%s"
                    if _execution_mode and has_execution_mode_col else ""
                )
                c.execute(
                    f"""
                    SELECT * FROM positions
                    WHERE client_id=%s AND plan_id=%s
                      {status_clause}
                      {mode_clause}
                    ORDER BY entry_ts DESC NULLS LAST, created_at DESC NULLS LAST
                    LIMIT 1
                    """,
                    (self.client_id, plan_id, *([_execution_mode] if mode_clause else [])),
                )
                row = c.fetchone()
                if row:
                    return row

            # Deterministic broker-import plan IDs are the economic lifecycle
            # identity. Two legitimate broker lots may share one recovered
            # scanner signal, so signal_id must not collapse them.
            if signal_id and not historical_plan_idempotency:
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
        expected_pending_exit_local_order_id: str = "",
        expected_pending_exit_broker_order_id: str = "",
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

        The reconciler backup path supplies the EXIT identity pair it proved
        before entering this method. When supplied, that pair is rechecked
        against the position row while the position lock is held, so a
        replacement or one-sided pending EXIT can never finalize from stale
        healer evidence.
        """
        expected_pending_local = str(
            expected_pending_exit_local_order_id or ""
        ).strip()
        expected_pending_broker = str(
            expected_pending_exit_broker_order_id or ""
        ).strip()
        supplied_local = str(local_order_id or "").strip()
        supplied_broker = str(broker_order_id or "").strip()
        if expected_pending_local or expected_pending_broker:
            if (
                not expected_pending_local
                or not expected_pending_broker
                or supplied_local != expected_pending_local
                or supplied_broker != expected_pending_broker
            ):
                log.critical(
                    "[%s] close_position_from_exit_fill blocked | pos=%s "
                    "invalid expected EXIT identity pair local=%s broker=%s "
                    "supplied_local=%s supplied_broker=%s",
                    self.client_id,
                    position_id,
                    expected_pending_local,
                    expected_pending_broker,
                    supplied_local,
                    supplied_broker,
                )
                return False

        exit_px = _strict_positive_finite_float(exit_price)
        fill_qty = _strict_positive_whole_number(filled_qty)
        if not position_id or exit_px is None or fill_qty is None:
            log.warning(
                "[%s] close_position_from_exit_fill blocked | pos=%s "
                "invalid finite-positive exit_price=%r or positive-whole filled_qty=%r",
                self.client_id, position_id, exit_price, filled_qty,
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

                locked_pending_local = str(
                    pos.get("pending_exit_local_order_id") or ""
                ).strip()
                locked_pending_broker = str(
                    pos.get("pending_exit_broker_order_id") or ""
                ).strip()
                if expected_pending_local or expected_pending_broker:
                    if (
                        not locked_pending_local
                        or not locked_pending_broker
                        or locked_pending_local != expected_pending_local
                        or locked_pending_broker != expected_pending_broker
                    ):
                        log.warning(
                            "[%s] close_position_from_exit_fill blocked | pos=%s "
                            "EXIT identity changed before finalization "
                            "expected_local=%s expected_broker=%s "
                            "locked_local=%s locked_broker=%s",
                            self.client_id,
                            position_id,
                            expected_pending_local,
                            expected_pending_broker,
                            locked_pending_local,
                            locked_pending_broker,
                        )
                        return False, "exit_identity_changed_before_finalize"
                elif supplied_local or supplied_broker:
                    # OSM historically called this shared boundary without
                    # expected-* arguments. A pending EXIT pair is still
                    # authorization-bearing, so a stale callback must not
                    # finalize merely because the optional expectation was
                    # omitted by that caller.
                    if (
                        (locked_pending_local or locked_pending_broker)
                        and (
                            not locked_pending_local
                            or not locked_pending_broker
                            or supplied_local != locked_pending_local
                            or supplied_broker != locked_pending_broker
                        )
                    ):
                        log.warning(
                            "[%s] close_position_from_exit_fill blocked | pos=%s "
                            "caller EXIT identity does not match locked pending pair "
                            "supplied_local=%s supplied_broker=%s "
                            "locked_local=%s locked_broker=%s",
                            self.client_id,
                            position_id,
                            supplied_local,
                            supplied_broker,
                            locked_pending_local,
                            locked_pending_broker,
                        )
                        return False, "caller_exit_identity_does_not_match_pending"

                # ── Canonical state classification under FOR UPDATE (PR #386) ─
                # The row lock is the ONLY correct serialization point. Any
                # detached pre-read cannot prevent overlapping callers from
                # racing here. We classify (status, quantity_remaining) into
                # four exact outcomes:
                #
                #   1. status in TERMINAL and remaining <= 0
                #      → idempotent success. The first successful finalization
                #        owns economic truth; do NOT recalculate position P&L
                #        and do NOT overwrite position exit fields. Caller
                #        sees True and MUST NOT rewrite proof (enforced above
                #        run_with_retry via detail["idempotent"]).
                #   2. status in TERMINAL and remaining > 0
                #      → invariant broken: terminal position still carries
                #        remaining quantity. Refuse to mutate; no evict.
                #   3. status not in TERMINAL and remaining <= 0
                #      → invariant broken: active position has zero remaining.
                #        Refuse to mutate; no evict.
                #   4. status not in TERMINAL and remaining > 0
                #      → continue through canonical finalization below.
                _idm_status = str(pos.get("status") or "").upper().strip()
                _idm_terminal = _idm_status in PositionStatus.TERMINAL
                _idm_remaining_raw = pos.get("quantity_remaining")
                _idm_qty = _strict_positive_whole_number(pos.get("qty"))
                _idm_remaining = (
                    _idm_qty if _idm_remaining_raw is None
                    else _strict_nonnegative_whole_number(_idm_remaining_raw)
                )
                _avg_fill_raw = pos.get("avg_fill")
                if _avg_fill_raw is None or (
                    isinstance(_avg_fill_raw, str) and not _avg_fill_raw.strip()
                ):
                    _avg_fill_raw = pos.get("entry_price")
                _avg_fill = _strict_positive_finite_float(_avg_fill_raw)
                if _idm_qty is None or _idm_remaining is None or _avg_fill is None:
                    log.critical(
                        "[%s] close_position_from_exit_fill invariant | pos=%s "
                        "malformed persisted position economics qty=%r remaining=%r avg_fill=%r "
                        "— refusing mutation",
                        self.client_id,
                        position_id,
                        pos.get("qty"),
                        _idm_remaining_raw,
                        _avg_fill_raw,
                    )
                    return False, "invalid_persisted_position_economics"
                if _idm_terminal and _idm_remaining <= 0:
                    log.info(
                        "[%s] close_position_from_exit_fill idempotent | "
                        "pos=%s status=%s remaining=%s",
                        self.client_id, position_id, _idm_status, _idm_remaining,
                    )
                    # PR #386 fix 1: return full PERSISTED terminal truth so
                    # the outside-of-txn idempotent branch can repair/verify
                    # canonical proof using ONLY these persisted values, never
                    # the current caller's exit_price/broker_order_id/reason.
                    return True, {
                        "position_id": position_id,
                        "status": _idm_status,
                        "quantity_remaining": _idm_remaining,
                        "qty": _idm_qty,
                        "avg_fill": _avg_fill,
                        "entry_option_price": _avg_fill,
                        "exit_price": _strict_positive_finite_float(pos.get("exit_price")) or 0.0,
                        "filled_qty": 0,
                        "remaining": 0,
                        "realized_pnl": float(pos.get("realized_pnl") or 0),
                        "realized_pnl_pct": pos.get("realized_pnl_pct"),
                        "contract": str(pos.get("contract") or ""),
                        "underlying": str(pos.get("underlying") or pos.get("ticker") or ""),
                        "side": str(pos.get("side") or ""),
                        "direction": str(pos.get("direction") or pos.get("side") or ""),
                        "entry_ts": str(pos.get("entry_ts") or ""),
                        "opened_at": str(pos.get("entry_ts") or ""),
                        "exit_ts": str(pos.get("exit_ts") or ""),
                        "closed_at": str(pos.get("exit_ts") or ""),
                        "local_order_id": str(pos.get("local_order_id") or ""),
                        "broker_order_id": str(pos.get("broker_order_id") or ""),
                        "exit_reason": str(pos.get("exit_reason") or ""),
                        "close_source": str(pos.get("close_source") or ""),
                        "execution_mode": str(pos.get("execution_mode") or ""),
                        "idempotent": True,
                    }
                if _idm_terminal and _idm_remaining > 0:
                    log.critical(
                        "[%s] close_position_from_exit_fill invariant | "
                        "pos=%s status=%s remaining=%s reason=%s — refusing mutation",
                        self.client_id, position_id, _idm_status, _idm_remaining,
                        "terminal_position_has_remaining_quantity",
                    )
                    return False, "terminal_position_has_remaining_quantity"
                if (not _idm_terminal) and _idm_remaining <= 0:
                    log.critical(
                        "[%s] close_position_from_exit_fill invariant | "
                        "pos=%s status=%s remaining=%s reason=%s — refusing mutation",
                        self.client_id, position_id, _idm_status, _idm_remaining,
                        "nonterminal_position_has_zero_remaining",
                    )
                    return False, "nonterminal_position_has_zero_remaining"
                # ── End canonical state classification ────────────────────────

                avg_fill = _avg_fill
                qty      = _idm_qty
                current_remaining = pos.get("quantity_remaining")
                if current_remaining is None:
                    current_remaining = qty
                current_remaining = _strict_nonnegative_whole_number(current_remaining)

                if avg_fill is None or qty is None or current_remaining is None or current_remaining <= 0:
                    return False, "invalid_position_cost_basis"

                # current_remaining is guaranteed > 0 here (idempotency guard
                # handled the zero-remaining case above).
                close_qty    = min(fill_qty, current_remaining)
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
                # PR #386 amendment 3: full persisted snapshot from the
                # locked position row so the outside-of-txn proof path can
                # use ONLY persisted truth — no fabricated timestamps, no
                # manufactured contract counts, no empty execution_mode.
                return True, {
                    "status": row.get("status"),
                    "exit_price": exit_px,
                    "filled_qty": close_qty,
                    "remaining": new_remaining,
                    "realized_pnl": realized_pnl,
                    "realized_pnl_pct": realized_pnl_pct,
                    "contract": str(pos.get("contract") or ""),
                    "underlying": str(pos.get("underlying") or pos.get("ticker") or ""),
                    "side": str(pos.get("side") or pos.get("direction") or ""),
                    "direction": str(pos.get("direction") or pos.get("side") or ""),
                    "position_id": position_id,
                    "qty": int(pos.get("qty") or 0),
                    "avg_fill": avg_fill,
                    "entry_ts": str(pos.get("entry_ts") or ""),
                    "exit_ts": str(ts),
                    "opened_at": str(pos.get("entry_ts") or ""),
                    "closed_at": str(ts),
                    "entry_option_price": avg_fill,
                    "local_order_id": str(pos.get("local_order_id") or ""),
                    "exit_local_order_id": str(local_order_id or ""),
                    "broker_order_id": str(pos.get("broker_order_id") or broker_order_id or ""),
                    "exit_reason": str(exit_reason or pos.get("exit_reason") or ""),
                    "close_source": str(close_source or pos.get("close_source") or ""),
                    "execution_mode": str(pos.get("execution_mode") or ""),
                }

        ok, detail = run_with_retry(_fn)
        if ok:
            # PR #386 fix 1: on idempotent terminal rows we NEVER rewrite
            # economics with caller evidence, but we MUST preserve proof-
            # recovery liveness. A crash after position commit but before
            # proof persist leaves a terminal row without canonical proof;
            # the idempotent path repairs it from PERSISTED values only.
            if isinstance(detail, dict) and detail.get("idempotent"):
                persisted = detail
                _pt_ok, _pt_reason = _validate_persisted_terminal_truth(persisted)
                if not _pt_ok:
                    log.critical(
                        "[%s] MANUAL_CLOSE_IDEMPOTENT_PERSISTED_TRUTH_INSUFFICIENT "
                        "pos=%s reason=%s — refusing to write proof from incomplete row",
                        self.client_id, position_id, _pt_reason,
                    )
                    return False
                proof_ok = self._with_terminal_proof_lock(
                    position_id,
                    lambda: self._ensure_terminal_close_proof(
                        position_id=str(persisted.get("position_id") or position_id),
                        local_order_id=str(persisted.get("local_order_id") or ""),
                        contract=str(persisted.get("contract") or ""),
                        underlying=str(
                            persisted.get("underlying") or persisted.get("contract") or ""
                        ),
                        side=str(persisted.get("side") or persisted.get("direction") or ""),
                        opened_at=str(persisted.get("opened_at") or persisted.get("entry_ts") or ""),
                        closed_at=str(persisted.get("closed_at") or persisted.get("exit_ts") or ""),
                        entry_option_price=float(
                            persisted.get("entry_option_price") or persisted.get("avg_fill") or 0
                        ),
                        exit_option_price=float(persisted.get("exit_price") or 0),
                        contracts=int(persisted.get("qty") or 0),
                        exit_reason=str(persisted.get("exit_reason") or ""),
                        option_pnl_pct=float(persisted.get("realized_pnl_pct") or 0),
                        setup_status=str(persisted.get("close_source") or ""),
                        execution_mode=str(persisted.get("execution_mode") or ""),
                        exit_fill_price=float(persisted.get("exit_price") or 0),
                        allow_fallback_insert=True,
                        missing_reason_code="MANUAL_CLOSE_IDEMPOTENT_PROOF_REPAIR",
                    ),
                )
                if proof_ok:
                    log.info(
                        "[%s] close_position_from_exit_fill idempotent + proof OK | "
                        "pos=%s status=%s",
                        self.client_id, position_id, persisted.get("status"),
                    )
                    return True
                log.critical(
                    "[%s] close_position_from_exit_fill idempotent PROOF_REPAIR_FAILED | "
                    "pos=%s status=%s — caller must not evict exit engine; "
                    "next scan will retry proof-only recovery",
                    self.client_id, position_id, persisted.get("status"),
                )
                return False
            log.info(
                "[%s] POSITION FINALIZED FROM EXIT FILL | pos=%s exit=$%.2f qty=%s "
                "pnl=$%+.2f (%.1f%%) source=%s broker=%s",
                self.client_id, position_id, exit_px, fill_qty,
                detail.get("realized_pnl", 0), detail.get("realized_pnl_pct", 0),
                close_source, broker_order_id or "",
            )
            # ── Repair proof_trades with actual broker fill price ─────────────
            # proof_trades is written at exit-decision time using bid price.
            # The real fill price comes in later — always update with broker truth.
            try:
                pnl_pct    = detail.get("realized_pnl_pct", 0)
                is_win     = pnl_pct > 0
                dollar_pnl = detail.get("realized_pnl", 0)
                # Breakeven: loss < $10 and % loss < 3% — don't punish the win rate
                BREAKEVEN_DOLLAR = float(os.getenv("BREAKEVEN_DOLLAR_THRESHOLD", "10"))
                BREAKEVEN_PCT    = float(os.getenv("BREAKEVEN_PCT_THRESHOLD", "3.0"))
                if not is_win and abs(dollar_pnl) < BREAKEVEN_DOLLAR and abs(pnl_pct) < BREAKEVEN_PCT:
                    is_win = True  # breakeven — count as win for rate calculation

                entry_local_order_id = str(detail.get("local_order_id") or "")

                def _proof_update():
                    with conn() as c:
                        c.execute(
                            """
                            SELECT id
                            FROM proof_trades
                            WHERE client_email = %s
                              AND system_version = 'v2'
                              AND synthetic_entry = FALSE
                              AND (
                                (position_id = %s AND %s <> '')
                                OR (local_order_id = %s AND %s <> '')
                              )
                            LIMIT 2
                            """,
                            (
                                self.client_id,
                                position_id,
                                position_id,
                                entry_local_order_id,
                                entry_local_order_id,
                            ),
                        )
                        candidates = c.fetchall() or []
                        if len(candidates) != 1:
                            return {"updated": False, "candidate_count": len(candidates)}
                        c.execute(
                            """
                            UPDATE proof_trades
                               SET exit_option_price = %s,
                                   option_pnl_pct    = %s,
                                   win               = %s,
                                   exit_reason       = CASE
                                     WHEN exit_reason IS NULL OR exit_reason = ''
                                     THEN %s ELSE exit_reason END
                             WHERE id = %s
                               AND client_email = %s
                            RETURNING id
                            """,
                            (
                                round(exit_px, 4),
                                round(pnl_pct, 2),
                                is_win,
                                exit_reason,
                                candidates[0].get("id"),
                                self.client_id,
                            ),
                        )
                        updated = c.fetchall() or []
                        return {
                            "updated": len(updated) == 1,
                            "candidate_count": len(candidates),
                            "updated_count": len(updated),
                        }

                proof_update = run_with_retry(_proof_update) or {}
                if proof_update.get("updated"):
                    log.info(
                        "[%s] proof_trades updated with broker fill | pos=%s entry_order=%s "
                        "exit=$%.2f pnl=%.1f%% win=%s",
                        self.client_id, position_id, entry_local_order_id, exit_px, pnl_pct, is_win,
                    )
                else:
                    log.critical(
                        "[%s] PROOF_FILL_REPAIR_CARDINALITY_BLOCKED pos=%s entry_order=%s "
                        "candidate_count=%s updated_count=%s",
                        self.client_id,
                        position_id,
                        entry_local_order_id,
                        proof_update.get("candidate_count"),
                        proof_update.get("updated_count"),
                    )
            except Exception as _proof_err:
                log.warning(
                    "[%s] proof_trades fill repair failed (non-fatal): %s",
                    self.client_id, _proof_err,
                )
            if int(detail.get("remaining") or 0) <= 0:
                # PR #386 amendment 1+2: fail closed on first-finalization
                # proof binding failure. Use persisted qty, persisted mode,
                # and persisted timestamps — never manufactured fallbacks.
                pt_ok, pt_reason = _validate_persisted_terminal_truth(detail)
                if not pt_ok:
                    log.critical(
                        "[%s] FIRST_CLOSE_PERSISTED_TRUTH_INSUFFICIENT "
                        "pos=%s reason=%s — refusing to write proof, "
                        "returning False so exit engine is not evicted; "
                        "PASS 0 will discover and repair on the next scan",
                        self.client_id, position_id, pt_reason,
                    )
                    return False
                proof_ok = self._with_terminal_proof_lock(
                    position_id,
                    lambda: self._ensure_terminal_close_proof(
                        position_id=position_id,
                        local_order_id=str(detail.get("local_order_id") or ""),
                        contract=str(detail.get("contract") or ""),
                        underlying=str(detail.get("underlying") or detail.get("contract") or ""),
                        side=str(detail.get("side") or detail.get("direction") or ""),
                        opened_at=str(detail.get("opened_at") or detail.get("entry_ts") or ""),
                        closed_at=str(detail.get("closed_at") or detail.get("exit_ts") or ""),
                        entry_option_price=float(detail.get("entry_option_price") or detail.get("avg_fill") or 0),
                        exit_option_price=float(detail.get("exit_price") or exit_px),
                        contracts=int(detail.get("qty") or 0),
                        exit_reason=str(detail.get("exit_reason") or exit_reason or ""),
                        option_pnl_pct=float(detail.get("realized_pnl_pct") or 0),
                        setup_status=str(detail.get("close_source") or close_source or "broker_exit_fill"),
                        execution_mode=str(detail.get("execution_mode") or ""),
                        exit_fill_price=float(detail.get("exit_price") or exit_px),
                        allow_fallback_insert=True,
                        missing_reason_code="BROKER_TRUTH_CLOSE_PROOF_WRITE_FAILED",
                    ),
                )
                if not proof_ok:
                    log.critical(
                        "[%s] FIRST_CLOSE_PROOF_BINDING_FAILED pos=%s — "
                        "returning False; exit engine must not evict; "
                        "PASS 0 restart-recovery will repair on next scan",
                        self.client_id, position_id,
                    )
                    return False
            return True

        log.warning(
            "[%s] close_position_from_exit_fill failed | pos=%s reason=%s",
            self.client_id, position_id, detail,
        )
        return False

    def repair_terminal_proof_from_persisted(
        self,
        position_id: str,
        *,
        expected_execution_mode: str = "",
        durable_exit_evidence: Optional[dict] = None,
    ) -> tuple[bool, str]:
        """PR #386 fix 2: proof-only restart recovery.

        For terminal positions with zero remaining quantity whose canonical
        proof is absent or unbound (e.g. crash after position commit but
        before proof persist). Reads persisted position economics under
        SELECT ... FOR UPDATE; never mutates the position; repairs proof
        exclusively via the canonical terminal-proof function using ONLY
        the persisted values.

        Returns (True, reason) once proof binding is proven; (False, reason)
        on any invariant violation or repair failure. Callers must not
        evict exit-engine tracking unless True is returned.
        """
        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM positions WHERE id=%s AND client_id=%s FOR UPDATE",
                    (position_id, self.client_id),
                )
                pos = c.fetchone()
                if not pos:
                    return None, "position_not_found"
                status = str(pos.get("status") or "").upper().strip()
                if status not in PositionStatus.TERMINAL:
                    return None, "position_not_terminal"
                qty_rem_raw = pos.get("quantity_remaining")
                qty_rem = int(pos.get("qty") or 0) if qty_rem_raw is None else int(qty_rem_raw or 0)
                if qty_rem > 0:
                    return None, "position_has_remaining_quantity"
                return dict(pos), "persisted_snapshot_taken"

        try:
            persisted, reason = run_with_retry(_fn)
        except Exception as exc:
            log.error(
                "[%s] repair_terminal_proof_from_persisted db_read_failed | pos=%s err=%s",
                self.client_id, position_id, exc,
            )
            return False, f"db_read_failed:{type(exc).__name__}"

        if persisted is None:
            return False, str(reason)

        # PR #386 blocker 4: fail closed on incomplete persisted truth.
        # Refuse to synthesize proof from a row missing any of the required
        # identity/economic fields, and refuse when the caller-declared
        # execution mode doesn't match the persisted mode exactly.
        expected_mode = str(expected_execution_mode or "").strip().lower()
        row_mode = str(persisted.get("execution_mode") or "").strip().lower()
        if expected_mode and expected_mode != row_mode:
            log.critical(
                "[%s] TERMINAL_PROOF_RESTART_RECOVERY_MODE_MISMATCH "
                "pos=%s expected=%s row=%s",
                self.client_id, position_id, expected_mode, row_mode,
            )
            return False, "execution_mode_mismatch"
        pt_ok, pt_reason = _validate_persisted_terminal_truth(persisted)
        if not pt_ok:
            log.critical(
                "[%s] TERMINAL_PROOF_RESTART_RECOVERY_INCOMPLETE "
                "pos=%s reason=%s — no proof written",
                self.client_id, position_id, pt_reason,
            )
            return False, f"persisted_truth_insufficient:{pt_reason}"

        # PR #386 blocker 2 hook: if the caller supplied a validated durable
        # EXIT aggregate (from _validate_durable_fills), require its
        # quantity to equal persisted qty and its weighted price to agree
        # with persisted exit_price within a small tolerance.
        if durable_exit_evidence is not None:
            try:
                aggregate_qty = int(durable_exit_evidence.get("filled_qty") or 0)
                aggregate_price = float(durable_exit_evidence.get("fill_price") or 0)
            except (TypeError, ValueError):
                aggregate_qty, aggregate_price = 0, 0.0
            persisted_qty = int(persisted.get("qty") or 0)
            persisted_exit = float(persisted.get("exit_price") or 0)
            if aggregate_qty != persisted_qty:
                log.critical(
                    "[%s] TERMINAL_PROOF_RESTART_RECOVERY_QTY_DISAGREE "
                    "pos=%s durable=%s persisted=%s",
                    self.client_id, position_id, aggregate_qty, persisted_qty,
                )
                return False, "durable_qty_mismatch"
            if persisted_exit > 0 and abs(aggregate_price - persisted_exit) > 0.005:
                log.critical(
                    "[%s] TERMINAL_PROOF_RESTART_RECOVERY_PRICE_DISAGREE "
                    "pos=%s durable=%.4f persisted=%.4f",
                    self.client_id, position_id, aggregate_price, persisted_exit,
                )
                return False, "durable_price_mismatch"

        proof_ok = self._with_terminal_proof_lock(
            position_id,
            lambda: self._ensure_terminal_close_proof(
                position_id=position_id,
                local_order_id=str(persisted.get("local_order_id") or ""),
                contract=str(persisted.get("contract") or ""),
                underlying=str(
                    persisted.get("underlying") or persisted.get("ticker") or persisted.get("contract") or ""
                ),
                side=str(persisted.get("side") or persisted.get("direction") or ""),
                opened_at=str(persisted.get("entry_ts") or persisted.get("opened_at") or ""),
                closed_at=str(persisted.get("exit_ts") or persisted.get("closed_at") or ""),
                entry_option_price=float(
                    persisted.get("avg_fill") or persisted.get("entry_price") or 0
                ),
                exit_option_price=float(persisted.get("exit_price") or 0),
                contracts=int(persisted.get("qty") or 0),
                exit_reason=str(persisted.get("exit_reason") or ""),
                option_pnl_pct=float(persisted.get("realized_pnl_pct") or 0),
                setup_status=str(persisted.get("close_source") or ""),
                execution_mode=str(persisted.get("execution_mode") or ""),
                exit_fill_price=float(persisted.get("exit_price") or 0),
                allow_fallback_insert=True,
                missing_reason_code="TERMINAL_PROOF_RESTART_RECOVERY",
            ),
        )
        if proof_ok:
            log.info(
                "[%s] TERMINAL_PROOF_RESTART_RECOVERY_OK | pos=%s status=%s",
                self.client_id, position_id, persisted.get("status"),
            )
            return True, "proof_bound"
        log.critical(
            "[%s] TERMINAL_PROOF_RESTART_RECOVERY_FAILED | pos=%s status=%s — "
            "caller must not evict; state remains discoverable for next scan",
            self.client_id, position_id, persisted.get("status"),
        )
        return False, "proof_repair_failed"

    def close_expired_position(
        self,
        *,
        position_id: str,
        expired_ts: Optional[str] = None,
        close_source: str = "expired_contract_cleanup",
        close_confidence: str = "SYSTEM",
        exit_reason: str = "expired_contract",
        reason: str = "expired_contract_local_cleanup",
    ) -> bool:
        """
        Terminal cleanup for a contract that has expired out of the market.

        This is separate from close_position_from_exit_fill(): there is no
        broker-confirmed fill price here, so we must not fabricate realized
        P&L or proof-trade fill data just to make the row non-active.
        """
        if not position_id:
            log.warning("[%s] close_expired_position blocked | missing position_id", self.client_id)
            return False

        ts = expired_ts or now_utc_iso()

        def _fn():
            with conn() as c:
                c.execute(
                    "SELECT * FROM positions WHERE id=%s AND client_id=%s FOR UPDATE",
                    (position_id, self.client_id),
                )
                pos = c.fetchone()
                if not pos:
                    return False, "position_not_found"

                current_status = str(pos.get("status") or "").upper()
                has = self._has_position_column
                sets, vals = ["updated_at=NOW()"], []

                def _add(col, val):
                    if has(col):
                        sets.append(f"{col}=%s")
                        vals.append(val)

                if not PositionStatus.is_terminal(current_status):
                    sets.insert(0, "status=%s")
                    vals.insert(0, PositionStatus.EXPIRED)

                _add("quantity_remaining", 0)
                _add("exit_ts", ts)
                _add("close_source", close_source)
                _add("close_confidence", close_confidence)
                _add("exit_reason", exit_reason)
                _add("close_reason", reason)

                vals.extend([position_id, self.client_id])
                c.execute(
                    f"UPDATE positions SET {', '.join(sets)} "
                    "WHERE id=%s AND client_id=%s",
                    tuple(vals),
                )
                detail = {
                    "result": (
                        "expired"
                        if not PositionStatus.is_terminal(current_status)
                        else f"already_terminal_repaired_remaining:{current_status}"
                    ),
                    "contract": str(pos.get("contract") or ""),
                    "underlying": str(pos.get("underlying") or pos.get("ticker") or ""),
                    "side": str(pos.get("side") or ""),
                    "opened_at": str(pos.get("entry_ts") or ts),
                    "closed_at": str(ts),
                    "entry_option_price": float(pos.get("avg_fill") or pos.get("entry_price") or 0),
                    "contracts": int(pos.get("quantity_remaining") or pos.get("qty") or 1),
                    "local_order_id": str(pos.get("local_order_id") or ""),
                }
                return True, detail

        ok, detail = run_with_retry(_fn)
        if ok:
            log.info(
                "[%s] POSITION EXPIRED | id=%s reason=%s detail=%s",
                self.client_id,
                position_id,
                reason,
                detail.get("result") if isinstance(detail, dict) else detail,
            )
            # PR #386 amendment 4: every path that reads, repairs, inserts,
            # or binds terminal proof must go through the single advisory
            # lock keyed by terminal-proof-bind:{client_id}:{position_id}.
            self._with_terminal_proof_lock(
                position_id,
                lambda: self._ensure_terminal_close_proof(
                    position_id=position_id,
                    local_order_id=str((detail or {}).get("local_order_id") or ""),
                    contract=str((detail or {}).get("contract") or ""),
                    underlying=str((detail or {}).get("underlying") or (detail or {}).get("contract") or ""),
                    side=str((detail or {}).get("side") or ""),
                    opened_at=str((detail or {}).get("opened_at") or ts),
                    closed_at=str((detail or {}).get("closed_at") or ts),
                    entry_option_price=float((detail or {}).get("entry_option_price") or 0),
                    exit_option_price=0.0,
                    contracts=int((detail or {}).get("contracts") or 1),
                    exit_reason=exit_reason,
                    option_pnl_pct=0.0,
                    setup_status=str(close_source or "expired_contract_cleanup"),
                    execution_mode="",
                    exit_fill_price=None,
                    allow_fallback_insert=False,
                    missing_reason_code="EXPIRED_CLOSE_PROOF_SKIPPED_NO_BROKER_TRUTH",
                ),
            )
        else:
            log.warning(
                "[%s] close_expired_position failed | pos=%s detail=%s",
                self.client_id,
                position_id,
                detail,
            )
        return ok

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

    def snapshot(self, *, mode: Optional[str] = None) -> dict:
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

                _snap_mode = str(mode or "").strip().lower()
                if _snap_mode not in {"paper", "live"}:
                    raise ValueError(f"snapshot_execution_mode_required:{mode}")

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

                # P0 (hotfix/p0-closed-filled-entry-exposure) — amended:
                # Fetch terminal positions so _summarize_fill_truth_rows can
                # suppress FILLED entry orders whose linked position already settled.
                #
                # Two terminal detection paths (OR logic):
                #   1. UPPER(status) IN canonical terminal set — includes
                #      CLOSED_REPAIR (reconciler repair path), CANCELED/CANCELLED
                #      (broker spelling variants), and all original statuses.
                #   2. COALESCE(quantity_remaining,0)=0 AND exit_ts IS NOT NULL —
                #      defensive catch for unknown repair variants or positions
                #      that are economically settled but carry an unexpected status.
                #
                # Fetch id, status, quantity_remaining, exit_ts so
                # _is_position_row_terminal() can evaluate both conditions.
                # Active rows (OPEN/CLOSING with qty>0 and no exit_ts) are
                # excluded at the DB level by the WHERE clause, keeping the
                # result set minimal.
                c.execute(
                    """
                    SELECT id, status, quantity_remaining, exit_ts
                    FROM positions
                    WHERE client_id=%s
                      AND (
                        UPPER(COALESCE(status,'')) IN (
                          'CLOSED','CLOSED_REPAIR','EXPIRED','STOPPED',
                          'TAKEN_PROFIT','ERROR','CANCELED','CANCELLED'
                        )
                        OR (
                          COALESCE(quantity_remaining, 0) = 0
                          AND exit_ts IS NOT NULL
                        )
                      )
                    """,
                    (self.client_id,),
                )
                terminal_positions = c.fetchall() or []

                c.execute(
                    """
                    SELECT
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
                    (start_utc, end_utc, self.client_id),
                )
                summary = c.fetchone() or {}
                position_capital_deployed = float(summary.get("capital_deployed") or 0.0)

                try:
                    trade_count = _broker_confirmed_entry_trades_today(
                        c,
                        client_id=self.client_id,
                        execution_mode=_snap_mode,
                        start_utc=start_utc,
                        end_utc=end_utc,
                    )
                except Exception as trade_count_error:
                    log.critical(
                        "[%s] TRADE_COUNT_QUERY_FAILED source=broker_confirmed_entry_orders "
                        "execution_mode=%s error=%s",
                        self.client_id,
                        _snap_mode,
                        trade_count_error,
                    )
                    raise RuntimeError(
                        "broker_confirmed_entry_trade_count_unavailable"
                    ) from trade_count_error

                # PR: sizing-bootstrap-fix
                # total_trades = durable count of confirmed broker fills for
                # this client across all time. Defined as ENTRY+EXIT fills in
                # the orders table (the broker-truth side), NOT positions
                # rows (which can be reset/reconciled). master_control reads
                # this to decide whether bootstrap_mode is still active:
                #   LIVE: bootstrap until total_trades >= threshold (safety)
                #   PAPER: bootstrap is skipped entirely
                # Without this field master_control reads 0 → bootstrap is
                # permanently True → every order forced to qty=1 → the 10%
                # POSITION_RISK_PCT path is unreachable.
                try:
                    c.execute(
                        """
                        SELECT COUNT(*) AS n
                        FROM orders
                        WHERE client_id = %s
                          AND status IN ('FILLED','EXIT_FILLED')
                        """,
                        (self.client_id,),
                    )
                    _tt_row = c.fetchone() or {}
                    total_trades = int(_tt_row.get("n") or 0)
                except Exception as _tt_exc:  # pragma: no cover - defensive
                    log.warning(
                        "[%s] total_trades count failed (non-fatal): %s",
                        self.client_id, _tt_exc,
                    )
                    total_trades = 0

                # REAL POSITION SLOT ACCOUNTING (PR#106):
                # A slot is consumed only after broker-confirmed fill truth.
                # filled_qty > 0 OR fill_price IS NOT NULL OR status is a
                # broker-fill state. SUBMITTED/ACKNOWLEDGED/CREATED without
                # fill are entry-attempt locks only — not position slots.
                _phantom_grace_sec = int(os.getenv("PENDING_ENTRY_PHANTOM_GRACE_SEC", "30"))
                # pending_entries = broker-confirmed fills NOT yet reconciled
                # to the positions table (position_id IS NULL).
                # Filtered to current execution mode (live/paper) so historical
                # null-mode orphans and wrong-mode fills never block active slots.
                # Fail-closed: if mode is unknown, exclude null-mode rows only
                # (IS NOT NULL) rather than counting everything.
                _mode_predicate = (
                    "LOWER(COALESCE(execution_mode, '')) = LOWER(%s)"
                    if _snap_mode
                    else "execution_mode IS NOT NULL"
                )
                _mode_params = (_snap_mode,) if _snap_mode else ()
                # orphan_wrong_mode always uses one explicit %s (mode or '')
                # so it works whether or not _snap_mode is known.
                _wrong_mode_val = _snap_mode or ""
                c.execute(
                    f"""
                    SELECT
                      COUNT(*) FILTER (
                        WHERE {_mode_predicate}
                      ) AS n,
                      COUNT(*) FILTER (
                        WHERE execution_mode IS NULL
                      ) AS orphan_null_mode,
                      COUNT(*) FILTER (
                        WHERE execution_mode IS NOT NULL
                          AND LOWER(execution_mode) != LOWER(%s)
                      ) AS orphan_wrong_mode,
                      COUNT(*) FILTER (
                        WHERE {_mode_predicate}
                          AND UPPER(COALESCE(direction,'')) = 'CALL'
                      ) AS calls_unreconciled,
                      COUNT(*) FILTER (
                        WHERE {_mode_predicate}
                          AND UPPER(COALESCE(direction,'')) = 'PUT'
                      ) AS puts_unreconciled
                    FROM orders
                    WHERE client_id = %s AND kind = 'ENTRY'
                      AND (
                        COALESCE(filled_qty, 0) > 0
                        OR fill_price IS NOT NULL
                        OR UPPER(COALESCE(status, '')) IN (
                            'PARTIAL_FILL', 'PARTIALLY_FILLED', 'FILLED', 'OPEN'
                        )
                      )
                      AND UPPER(COALESCE(status, '')) NOT IN (
                        'CANCELED', 'CANCELLED', 'EXPIRED', 'REJECTED',
                        'ERROR', 'FAILED', 'CLOSED'
                      )
                      AND UPPER(COALESCE(contract, '')) NOT LIKE 'DEFERRED:%%'
                      AND (position_id IS NULL OR position_id = '')
                    """,
                    (*_mode_params, _wrong_mode_val, *_mode_params, *_mode_params, self.client_id),
                )
                _slot_row = c.fetchone() or {}
                pending_entries              = int(_slot_row.get("n")                   or 0)
                filled_unreconciled_calls    = int(_slot_row.get("calls_unreconciled")  or 0)
                filled_unreconciled_puts     = int(_slot_row.get("puts_unreconciled")   or 0)
                _orphan_null_mode            = int(_slot_row.get("orphan_null_mode")    or 0)
                _orphan_wrong_mode           = int(_slot_row.get("orphan_wrong_mode")   or 0)
                if _orphan_null_mode or _orphan_wrong_mode:
                    log.info(
                        "[%s] SNAPSHOT_ORPHAN_FILLED_IGNORED "
                        "client=%s null_mode_count=%d wrong_mode_count=%d expected_mode=%s",
                        self.client_id, self.client_id,
                        _orphan_null_mode, _orphan_wrong_mode, _snap_mode or "unknown",
                    )

                # Entry-attempt lock: in-flight broker submits (duplicate-submit
                # protection only; NOT counted as position slots or real exposure).
                c.execute(
                    f"""
                    SELECT COUNT(*) AS n,
                           COALESCE(SUM(
                               COALESCE(NULLIF(reserved_cost, 0),
                                   CASE WHEN limit_price > 0
                                        THEN limit_price * qty * 100
                                        ELSE 0 END)
                           ), 0) AS reserved
                    FROM orders
                    WHERE client_id = %s AND kind = 'ENTRY'
                      AND {_mode_predicate}
                      AND UPPER(COALESCE(status, '')) IN (
                        'CREATED', 'SUBMITTED', 'ACKNOWLEDGED', 'PENDING',
                        'OPEN', 'ACCEPTED', 'PARTIAL_FILL',
                        'PARTIALLY_FILLED', 'PENDING_SUBMIT'
                      )
                      AND (
                        (broker_order_id IS NOT NULL AND broker_order_id <> '')
                        OR submitted_ts IS NOT NULL
                      )
                      AND COALESCE(filled_qty, 0) = 0
                      AND fill_price IS NULL
                      AND UPPER(COALESCE(contract, '')) NOT LIKE 'DEFERRED:%%'
                      AND UPPER(COALESCE(status, '')) NOT IN (
                        'CANCELED', 'CANCELLED', 'EXPIRED', 'REJECTED',
                        'ERROR', 'FAILED', 'CLOSED'
                      )
                    """,
                    (self.client_id, *_mode_params),
                )
                _lock_row = c.fetchone() or {}
                entry_attempt_lock_count    = int(_lock_row.get("n") or 0)
                entry_attempt_reserved_cost = float(_lock_row.get("reserved") or 0.0)

                # Watcher/pre-submit rows for audit/logging only.
                watcher_placeholders = ",".join(["%s"] * len(_WATCHER_ENTRY_STATUSES))
                c.execute(
                    f"""
                    SELECT COUNT(*) AS n
                    FROM orders
                    WHERE client_id = %s AND kind = 'ENTRY'
                      AND status IN ({watcher_placeholders})
                      AND NOT (
                        status = 'PENDING_TRIGGER'
                        AND (broker_order_id IS NULL OR broker_order_id = '')
                        AND created_ts < NOW() - (%s || ' seconds')::interval
                      )
                    """,
                    (self.client_id, *_WATCHER_ENTRY_STATUSES, str(_phantom_grace_sec)),
                )
                watcher_count = int((c.fetchone() or {}).get("n") or 0)

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
                open_position_ids = [
                    str(p["id"])
                    for p in active
                    if p.get("id")
                ]

                # real_deployed_capital: fill-truth cost of confirmed positions.
                # Uses fill_price * filled_qty * 100 (broker-confirmed fill).
                # Falls back to positions table capital_deployed for OPEN positions.
                # Does NOT count reserved_cost from unfilled submit attempts.
                try:
                    c.execute(
                        f"""
                        SELECT COALESCE(SUM(
                            CASE
                                WHEN fill_price IS NOT NULL AND COALESCE(filled_qty,0) > 0
                                THEN fill_price * filled_qty * 100
                                WHEN COALESCE(filled_qty,0) > 0 AND reserved_cost IS NOT NULL
                                THEN reserved_cost
                                ELSE 0
                            END
                        ), 0) AS cap
                        FROM orders
                        WHERE client_id = %s
                          AND kind = 'ENTRY'
                          AND {_mode_predicate}
                          AND (
                            COALESCE(filled_qty, 0) > 0
                            OR fill_price IS NOT NULL
                            OR UPPER(COALESCE(status,'')) IN (
                                'PARTIAL_FILL', 'PARTIALLY_FILLED', 'FILLED', 'OPEN'
                            )
                          )
                          AND UPPER(COALESCE(status,'')) NOT IN (
                              'CANCELED', 'CANCELLED', 'EXPIRED', 'REJECTED',
                              'ERROR', 'FAILED', 'CLOSED'
                          )
                          AND UPPER(COALESCE(contract, '')) NOT LIKE 'DEFERRED:%%'
                          AND (position_id IS NULL OR position_id = '')
                        """,
                        (self.client_id, *_mode_params),
                    )
                    _cap_row = c.fetchone() or {}
                    filled_unreconciled_entry_capital = float(_cap_row.get("cap") or 0.0)
                except Exception as _pec_err:
                    log.warning("[%s] filled_unreconciled_entry_capital query failed (non-fatal): %s",
                                self.client_id, _pec_err)
                    filled_unreconciled_entry_capital = None

                try:
                    c.execute(
                        f"""
                        SELECT
                            id,
                            position_id,
                            broker_order_id,
                            local_order_id,
                            signal_id,
                            plan_id,
                            contract,
                            direction,
                            fill_price,
                            filled_qty,
                            reserved_cost
                        FROM orders
                        WHERE client_id = %s
                          AND kind = 'ENTRY'
                          AND {_mode_predicate}
                          AND (
                            COALESCE(filled_qty, 0) > 0
                            OR fill_price IS NOT NULL
                            OR UPPER(COALESCE(status,'')) IN (
                                'PARTIAL_FILL', 'PARTIALLY_FILLED', 'FILLED', 'OPEN'
                            )
                          )
                          AND UPPER(COALESCE(status,'')) NOT IN (
                              'CANCELED', 'CANCELLED', 'EXPIRED', 'REJECTED',
                              'ERROR', 'FAILED', 'CLOSED'
                          )
                          AND UPPER(COALESCE(contract, '')) NOT LIKE 'DEFERRED:%%'
                        """,
                        (self.client_id, *_mode_params),
                    )
                    _fill_rows = c.fetchall() or []
                    _fill_summary = _summarize_fill_truth_rows(
                        _fill_rows,
                        active,
                        terminal_positions=terminal_positions,
                        client_id=self.client_id,
                        execution_mode=_snap_mode or "",
                    )
                    pending_entries = int(_fill_summary.get("pending_entries") or 0)
                    filled_unreconciled_calls = int(_fill_summary.get("filled_unreconciled_calls") or 0)
                    filled_unreconciled_puts = int(_fill_summary.get("filled_unreconciled_puts") or 0)
                    filled_unreconciled_entry_capital = float(
                        _fill_summary.get("filled_unreconciled_entry_capital") or 0.0
                    )
                    ignored_already_reconciled_fill_capital = float(
                        _fill_summary.get("ignored_already_reconciled_fill_capital") or 0.0
                    )
                    ignored_already_reconciled_order_ids = list(
                        _fill_summary.get("ignored_already_reconciled_order_ids") or []
                    )
                    ignored_reconciled_match_keys = list(
                        _fill_summary.get("ignored_reconciled_match_keys") or []
                    )
                    # P0 audit fields — terminal-linked fill suppression
                    terminal_ignored_order_ids: list = list(
                        _fill_summary.get("terminal_ignored_order_ids") or []
                    )
                    terminal_ignored_position_ids: list = list(
                        _fill_summary.get("terminal_ignored_position_ids") or []
                    )
                    terminal_ignored_capital: float = float(
                        _fill_summary.get("terminal_ignored_capital") or 0.0
                    )
                    missing_position_rows: list = list(
                        _fill_summary.get("missing_position_rows") or []
                    )
                except Exception as _fill_summary_err:
                    log.warning(
                        "[%s] ignored_already_reconciled_fill_capital query failed (non-fatal): %s",
                        self.client_id,
                        _fill_summary_err,
                    )
                    ignored_already_reconciled_fill_capital = 0.0
                    ignored_already_reconciled_order_ids = []
                    ignored_reconciled_match_keys = []
                    # P0 audit defaults on failure
                    terminal_ignored_order_ids: list = []
                    terminal_ignored_position_ids: list = []
                    terminal_ignored_capital: float = 0.0
                    missing_position_rows: list = []

                pending_entry_capital = filled_unreconciled_entry_capital

                return {
                    "snapshot_ts":        datetime.now(timezone.utc).isoformat(),
                    "generated_at":       datetime.now(timezone.utc).isoformat(),
                    "open_positions":     opens,
                    "closing_positions":  closing,
                    "open_count":         len(active),
                    "active_count":       len(active),
                    "session_day":        session_day,
                    "open_tickers":       {p["underlying"] for p in active},
                    "open_position_ids":  open_position_ids,
                    "calls_open":         sum(1 for p in active if p.get("direction") == "CALL"),
                    "puts_open":          sum(1 for p in active if p.get("direction") == "PUT"),
                    "capital_deployed":   position_capital_deployed,
                    "position_capital_deployed": position_capital_deployed,
                    "pending_entry_capital": pending_entry_capital,
                    "filled_unreconciled_entry_capital": filled_unreconciled_entry_capital,
                    "pending_entries":           pending_entries,
                    "filled_unreconciled_calls":  filled_unreconciled_calls,
                    "filled_unreconciled_puts":   filled_unreconciled_puts,
                    "entry_attempt_lock_count":    entry_attempt_lock_count,
                    "entry_attempt_reserved_cost": entry_attempt_reserved_cost,
                    "ignored_already_reconciled_fill_capital": ignored_already_reconciled_fill_capital,
                    "ignored_already_reconciled_order_ids": ignored_already_reconciled_order_ids,
                    "ignored_reconciled_match_keys": ignored_reconciled_match_keys,
                    # P0 (hotfix/p0-closed-filled-entry-exposure) audit fields
                    "terminal_ignored_order_ids":    terminal_ignored_order_ids,
                    "terminal_ignored_position_ids": terminal_ignored_position_ids,
                    "terminal_ignored_capital":      terminal_ignored_capital,
                    "missing_position_rows":         missing_position_rows,
                    "watcher_count":      watcher_count,
                    "pending_exits":      pending_exits,
                    "trades_today":       int(trade_count["trades_today"]),
                    "trades_today_source": trade_count["trades_today_source"],
                    "synthetic_position_rows_ignored": int(
                        trade_count["synthetic_position_rows_ignored"]
                    ),
                    "null_mode_fills_ignored": int(
                        trade_count["null_mode_fills_ignored"]
                    ),
                    "wrong_mode_fills_ignored": int(
                        trade_count["wrong_mode_fills_ignored"]
                    ),
                    "missing_identity_fills_ignored": int(
                        trade_count["missing_identity_fills_ignored"]
                    ),
                    "trade_count_query_status": trade_count["trade_count_query_status"],
                    "realized_pnl_today": float(summary.get("realized_pnl_today") or 0),
                    # PR: sizing-bootstrap-fix — propagate the durable
                    # fill count up to master_control. See SELECT above for
                    # definition (orders table, status IN FILLED/EXIT_FILLED).
                    "total_trades":       int(total_trades),
                    # Required by APMasterControl LIVE snapshot freshness check
                    "snapshot_ts": __import__("datetime").datetime.now(
                        __import__("datetime").timezone.utc).isoformat(),
                }

        return run_with_retry(_fn)
