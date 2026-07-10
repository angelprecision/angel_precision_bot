"""
ap/live_entry_brake.py

LIVE pre-entry account-state brake — PR #317.

Prevents new LIVE entries while account, order, or exit state is unresolved
for the requesting client. Runs before deferred materialization and broker
entry POST. This module is structurally dual to ap/live_submit_gates.py:
those gates protect a single submit attempt; this brake protects the entire
account from entering while any systemic unresolved condition persists.

════════════════════════════════════════════════════════════════════════════════
THIS MODULE IS NEVER IMPORTED OR CALLED FROM EXIT PATHS.

Protective exit ordering is independent of entry safety. Exit engines must
never gate on entry-side classifiers — gating exits on account state would
create a deadlock where a bad state blocks the exit that would resolve it.
════════════════════════════════════════════════════════════════════════════════

BLOCK CONDITIONS (all per-client; one client's state never bleeds to another):

  1. SPLIT_BRAIN_ORDER
       An order row for this client has a SPLIT_BRAIN: last_error prefix and
       is not in a terminal status. Broker accepted but DB transition failed —
       we cannot prove account state until reconciliation resolves it.

  2. CIRCUIT_BREAKER_TRIPPED
       A nonterminal position for this client has
       exit_circuit_breaker_tripped=True in its JSONB metadata column. The
       protective exit path is stuck; entering compound exposure on top of a
       position we cannot exit is capital destruction.

  3. UNKNOWN_MODE_EXPOSURE
       A nonterminal position for this client has a blank, NULL, or
       unresolvable execution_mode. Cannot prove the account boundary;
       entering against an ambiguous position risks cross-mode capital bleed.

  4. POSITION_MISMATCH
       Caller provided a broker position snapshot AND a local nonterminal
       position's quantity does not match the broker-confirmed quantity.
       Local state disagrees with ground truth; entry must wait.

  5. STALE_EXIT_IN_FLIGHT
       A nonterminal EXIT order for this client has been stuck in a
       nonterminal status (NEW / ACK / PARTIAL) for longer than
       EXIT_IN_FLIGHT_STALE_SEC (default 300s, env-tunable). The account
       has an unresolved exit; adding an entry compounds exposure ambiguity.

  6. BROKER_SNAPSHOT_UNAVAILABLE
       Caller explicitly signals that the broker position snapshot failed AND
       this client has at least one nonterminal local position. We cannot
       verify whether broker-side exposure matches local expectations.

ERROR POLICY:
  - Explicit LIVE  + classifier/DB error → block; reason LIVE_ENTRY_BRAKE_STATE_UNAVAILABLE
  - Explicit PAPER → always pass through; this module never blocks paper
  - Blank/unknown execution_mode → block; reason LIVE_ENTRY_BRAKE_UNKNOWN_EXECUTION_MODE

USAGE:
    from ap.live_entry_brake import check_live_entry_brake, BrakeCode, BrakeResult

    result = check_live_entry_brake(
        client_id=client_id,
        execution_mode=execution_mode,          # "live" | "paper"
        broker_positions_available=True,        # or False if snapshot call failed
        broker_positions=broker_pos_list,       # list[dict] from broker adapter
    )
    if result.blocked:
        log.warning("[%s] entry brake: %s | %s", client_id,
                    result.reason_code, result.detail)
        _stamp_brake_audit(order_id, result)
        return BLOCKED
    # proceed to deferred materialization / broker POST
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from ap.db import conn, run_with_retry
from ap.logger import get_logger

log = get_logger("ap.live_entry_brake")


# ─────────────────────────────────────────────────────────────────────────────
# Canonical reason codes
# ─────────────────────────────────────────────────────────────────────────────

class BrakeCode:
    """Canonical block reasons stamped into orders.meta under 'live_entry_brake'."""

    SPLIT_BRAIN_ORDER              = "LIVE_ENTRY_BRAKE_SPLIT_BRAIN_ORDER"
    CIRCUIT_BREAKER_TRIPPED        = "LIVE_ENTRY_BRAKE_CIRCUIT_BREAKER_TRIPPED"
    UNKNOWN_MODE_EXPOSURE          = "LIVE_ENTRY_BRAKE_UNKNOWN_MODE_EXPOSURE"
    POSITION_MISMATCH              = "LIVE_ENTRY_BRAKE_POSITION_MISMATCH"
    STALE_EXIT_IN_FLIGHT           = "LIVE_ENTRY_BRAKE_STALE_EXIT_IN_FLIGHT"
    BROKER_SNAPSHOT_UNAVAILABLE    = "LIVE_ENTRY_BRAKE_BROKER_SNAPSHOT_UNAVAILABLE"
    UNKNOWN_EXECUTION_MODE         = "LIVE_ENTRY_BRAKE_UNKNOWN_EXECUTION_MODE"
    STATE_UNAVAILABLE              = "LIVE_ENTRY_BRAKE_STATE_UNAVAILABLE"
    CLEAR                          = "LIVE_ENTRY_BRAKE_CLEAR"


# ─────────────────────────────────────────────────────────────────────────────
# Result type
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class BrakeResult:
    """
    Uniform return type from check_live_entry_brake().

    Fields:
        blocked:     True → DO NOT submit entry to broker.
        reason_code: Canonical code from BrakeCode (CLEAR on pass).
        detail:      Short human-readable explanation for logs / audit trail.
        evidence:    All state gathered for the decision; stamp into orders.meta
                     under the key 'live_entry_brake' so operators can replay
                     the decision without re-querying live state.
    """
    blocked: bool
    reason_code: str = BrakeCode.CLEAR
    detail: str = ""
    evidence: dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Internal schema cache (positions metadata column name varies by deploy)
# ─────────────────────────────────────────────────────────────────────────────

_SCHEMA_CACHE: dict[str, set[str]] = {}
_SCHEMA_LOCK = threading.Lock()

# Nonterminal order statuses — orders still "in flight" toward broker resolution
_NONTERMINAL_ORDER_STATUSES_SQL = (
    "'NEW','ACK','PARTIAL','PENDING','PENDING_TRIGGER','DEFERRED',"
    "'SUBMITTED','ARMED','WATCHER_ARMED','BROKER_SUBMITTED','BROKER_ACKED',"
    "'ORDER_CREATED'"
)

# Nonterminal position statuses — positions still holding exposure
_NONTERMINAL_POSITION_STATUSES_SQL = "'OPEN','CLOSING','PARTIAL','ACTIVE'"

# Terminal order statuses for split-brain exclusion
_TERMINAL_ORDER_STATUSES_SQL = (
    "'FILLED','EXPIRED','CANCELED','MISSED','INTERNAL_ERROR',"
    "'BROKER_REJECTED','WATCHER_INVALIDATED',"
    "'ENTRY_CONFIRMATION_FAILED','CLIENT_SKIPPED'"
)


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default)).strip()))
    except (TypeError, ValueError):
        return default


def _get_table_columns(table: str) -> set[str]:
    """Return the set of column names for a table; cached per process."""
    with _SCHEMA_LOCK:
        if table in _SCHEMA_CACHE:
            return _SCHEMA_CACHE[table]

    def _fetch():
        with conn() as c:
            c.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public' AND table_name = %s
                """,
                (table,),
            )
            return {str(r["column_name"]) for r in c.fetchall()}

    try:
        cols = run_with_retry(_fetch) or set()
    except Exception as exc:
        log.debug("live_entry_brake schema probe failed for %s: %s", table, exc)
        cols = set()

    with _SCHEMA_LOCK:
        _SCHEMA_CACHE[table] = cols
    return cols


def _positions_meta_col() -> str:
    """Resolve the JSONB metadata column name on the positions table."""
    cols = _get_table_columns("positions")
    if "meta" in cols:
        return "meta"
    if "metadata" in cols:
        return "metadata"
    return ""


def _safe_jsonb_bool(value, key: str) -> bool:
    """Extract a boolean from a JSONB value that may arrive as dict or JSON str."""
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except Exception:
            return False
    if isinstance(value, dict):
        return bool(value.get(key))
    return False


def _parse_iso_utc(dt_str) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        s = str(dt_str).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        d = datetime.fromisoformat(s)
        return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Individual classifiers
# Each returns None (condition clear) or a BrakeResult (blocked=True).
# Each is scoped strictly to client_id — never cross-client.
# ─────────────────────────────────────────────────────────────────────────────

def _check_split_brain(client_id: str) -> Optional[BrakeResult]:
    """
    Block if any order for this client carries a SPLIT_BRAIN: last_error prefix
    and is not yet in a terminal status.

    The split-brain condition means the broker accepted an order but the local
    DB transition failed. The position's true P&L and qty are unknown until
    the reconciler resolves it; entering on top compounds the uncertainty.
    """
    def _fn():
        with conn() as c:
            c.execute(
                f"""
                SELECT local_order_id,
                       broker_order_id,
                       LEFT(last_error, 120) AS last_error_prefix,
                       status
                FROM orders
                WHERE client_id = %s
                  AND last_error LIKE 'SPLIT_BRAIN:%%'
                  AND UPPER(COALESCE(status, '')) NOT IN ({_TERMINAL_ORDER_STATUSES_SQL})
                ORDER BY created_ts DESC
                LIMIT 5
                """,
                (client_id,),
            )
            return [dict(r) for r in c.fetchall()]

    rows = run_with_retry(_fn) or []
    if not rows:
        return None

    return BrakeResult(
        blocked=True,
        reason_code=BrakeCode.SPLIT_BRAIN_ORDER,
        detail=(
            f"{len(rows)} unresolved split-brain order(s) — broker accepted "
            "but DB transition failed; reconciler must resolve before new entry"
        ),
        evidence={
            "split_brain_order_count": len(rows),
            "split_brain_orders": [
                {
                    "local_order_id": r.get("local_order_id"),
                    "broker_order_id": r.get("broker_order_id"),
                    "status": r.get("status"),
                    "last_error_prefix": r.get("last_error_prefix"),
                }
                for r in rows
            ],
        },
    )


def _check_circuit_breaker(client_id: str) -> Optional[BrakeResult]:
    """
    Block if any nonterminal position for this client has
    exit_circuit_breaker_tripped=True in its JSONB metadata.

    A tripped circuit breaker means the protective exit engine halted repeated
    rejections. We cannot safely enter additional exposure on top of a position
    we cannot exit.
    """
    meta_col = _positions_meta_col()
    if not meta_col:
        # Cannot inspect metadata — fail closed: treat as potential trip
        return BrakeResult(
            blocked=True,
            reason_code=BrakeCode.STATE_UNAVAILABLE,
            detail="positions table has no recognized metadata column — cannot verify circuit breaker state",
            evidence={"meta_col_missing": True},
        )

    def _fn():
        with conn() as c:
            c.execute(
                f"""
                SELECT position_id,
                       ticker,
                       status,
                       {meta_col} AS meta_val
                FROM positions
                WHERE client_id = %s
                  AND UPPER(COALESCE(status, '')) IN ({_NONTERMINAL_POSITION_STATUSES_SQL})
                """,
                (client_id,),
            )
            return [dict(r) for r in c.fetchall()]

    rows = run_with_retry(_fn) or []
    tripped = [
        r for r in rows
        if _safe_jsonb_bool(r.get("meta_val"), "exit_circuit_breaker_tripped")
    ]
    if not tripped:
        return None

    return BrakeResult(
        blocked=True,
        reason_code=BrakeCode.CIRCUIT_BREAKER_TRIPPED,
        detail=(
            f"{len(tripped)} position(s) have exit_circuit_breaker_tripped=True — "
            "protective exit stuck; new entry blocked until circuit breaker is cleared"
        ),
        evidence={
            "circuit_breaker_position_count": len(tripped),
            "circuit_breaker_positions": [
                {
                    "position_id": r.get("position_id"),
                    "ticker": r.get("ticker"),
                    "status": r.get("status"),
                }
                for r in tripped
            ],
        },
    )


def _check_unknown_mode_exposure(client_id: str) -> Optional[BrakeResult]:
    """
    Block if any nonterminal position for this client has a blank, NULL, or
    unresolvable execution_mode.

    We cannot prove which account boundary (LIVE / PAPER) an ambiguous position
    belongs to. Entering against it risks cross-mode capital bleed.
    """
    def _fn():
        with conn() as c:
            c.execute(
                f"""
                SELECT position_id,
                       ticker,
                       status,
                       execution_mode
                FROM positions
                WHERE client_id = %s
                  AND UPPER(COALESCE(status, '')) IN ({_NONTERMINAL_POSITION_STATUSES_SQL})
                  AND LOWER(COALESCE(TRIM(execution_mode), '')) NOT IN ('live', 'paper')
                ORDER BY created_ts DESC
                LIMIT 10
                """,
                (client_id,),
            )
            return [dict(r) for r in c.fetchall()]

    rows = run_with_retry(_fn) or []
    if not rows:
        return None

    return BrakeResult(
        blocked=True,
        reason_code=BrakeCode.UNKNOWN_MODE_EXPOSURE,
        detail=(
            f"{len(rows)} nonterminal position(s) have blank/unknown execution_mode — "
            "cannot prove account boundary; entry blocked until mode is resolved"
        ),
        evidence={
            "unknown_mode_position_count": len(rows),
            "unknown_mode_positions": [
                {
                    "position_id": r.get("position_id"),
                    "ticker": r.get("ticker"),
                    "status": r.get("status"),
                    "execution_mode": r.get("execution_mode"),
                }
                for r in rows
            ],
        },
    )


def _check_stale_exit_in_flight(client_id: str) -> Optional[BrakeResult]:
    """
    Block if any nonterminal EXIT order for this client has been stuck in a
    nonterminal status for longer than EXIT_IN_FLIGHT_STALE_SEC.

    DB proxy for in-memory exit_in_flight: a nonterminal EXIT order paired to
    a nonterminal position is the durable evidence that an exit was dispatched
    but has not resolved. Staleness means the order monitor has not cleared it.
    """
    stale_sec = _int_env("EXIT_IN_FLIGHT_STALE_SEC", 300)

    def _fn():
        with conn() as c:
            c.execute(
                f"""
                SELECT
                    p.position_id,
                    p.ticker,
                    p.status  AS position_status,
                    o.local_order_id,
                    o.status  AS exit_order_status,
                    o.created_ts,
                    o.updated_ts,
                    EXTRACT(EPOCH FROM (NOW() - o.created_ts))::int AS age_seconds
                FROM positions p
                JOIN orders o
                  ON o.position_id = p.position_id
                 AND o.client_id   = p.client_id
                 AND o.kind        = 'EXIT'
                 AND UPPER(COALESCE(o.status, '')) IN ('NEW', 'ACK', 'PARTIAL')
                WHERE p.client_id = %s
                  AND UPPER(COALESCE(p.status, '')) IN ({_NONTERMINAL_POSITION_STATUSES_SQL})
                  AND EXTRACT(EPOCH FROM (NOW() - o.created_ts)) > %s
                ORDER BY o.created_ts ASC
                LIMIT 10
                """,
                (client_id, stale_sec),
            )
            return [dict(r) for r in c.fetchall()]

    rows = run_with_retry(_fn) or []
    if not rows:
        return None

    return BrakeResult(
        blocked=True,
        reason_code=BrakeCode.STALE_EXIT_IN_FLIGHT,
        detail=(
            f"{len(rows)} nonterminal EXIT order(s) stuck beyond {stale_sec}s — "
            "exit_in_flight unresolved; entry blocked until exit clears or order monitor recovers"
        ),
        evidence={
            "stale_exit_count": len(rows),
            "stale_sec_threshold": stale_sec,
            "stale_exits": [
                {
                    "position_id": r.get("position_id"),
                    "ticker": r.get("ticker"),
                    "position_status": r.get("position_status"),
                    "exit_order_status": r.get("exit_order_status"),
                    "age_seconds": r.get("age_seconds"),
                    "local_order_id": r.get("local_order_id"),
                }
                for r in rows
            ],
        },
    )


def _check_broker_snapshot(
    client_id: str,
    broker_positions_available: Optional[bool],
    broker_positions: Optional[list],
) -> Optional[BrakeResult]:
    """
    Two sub-checks gated on whether the caller provided broker snapshot data:

    Sub-check A (condition 6) — BROKER_SNAPSHOT_UNAVAILABLE:
        broker_positions_available=False AND local nonterminal positions exist.
        We cannot verify account state; block entry.

    Sub-check B (condition 4) — POSITION_MISMATCH:
        broker_positions_available=True AND broker-confirmed qty=0 for a
        contract where local DB shows qty>0 open. The account is already
        not what we think it is.

    If broker_positions_available=None, caller did not provide a snapshot
    (e.g. pre-market, or snapshot was not part of the flow). Both sub-checks
    are skipped.
    """
    if broker_positions_available is None:
        return None

    def _local_open_positions():
        with conn() as c:
            c.execute(
                f"""
                SELECT position_id,
                       ticker,
                       status,
                       contract,
                       COALESCE(qty_remaining, qty_open, qty, 0)::int AS local_qty
                FROM positions
                WHERE client_id = %s
                  AND UPPER(COALESCE(status, '')) IN ({_NONTERMINAL_POSITION_STATUSES_SQL})
                """,
                (client_id,),
            )
            return [dict(r) for r in c.fetchall()]

    local_rows = run_with_retry(_local_open_positions) or []
    has_local_exposure = bool(local_rows)

    # Sub-check A: snapshot unavailable + local exposure
    if not broker_positions_available:
        if has_local_exposure:
            return BrakeResult(
                blocked=True,
                reason_code=BrakeCode.BROKER_SNAPSHOT_UNAVAILABLE,
                detail=(
                    f"broker position snapshot unavailable with {len(local_rows)} "
                    "local nonterminal position(s) — cannot verify ground-truth account state"
                ),
                evidence={
                    "broker_positions_available": False,
                    "local_position_count": len(local_rows),
                    "local_positions": [
                        {
                            "position_id": r.get("position_id"),
                            "ticker": r.get("ticker"),
                            "local_qty": r.get("local_qty"),
                        }
                        for r in local_rows
                    ],
                },
            )
        # No local exposure + no snapshot → no risk; pass
        return None

    # Sub-check B: broker snapshot available — check for qty mismatches
    if not has_local_exposure:
        return None  # No local exposure; nothing to cross-check

    if not broker_positions:
        # Snapshot was available but returned empty — broker says flat.
        # If local says open, that is itself a mismatch.
        broker_positions = []

    # Build broker qty map keyed by OCC contract symbol (uppercase)
    broker_qty_map: dict[str, int] = {}
    for bp in broker_positions:
        sym = str(bp.get("symbol") or bp.get("contract") or "").strip().upper()
        try:
            qty = int(bp.get("quantity") or bp.get("qty") or 0)
        except (TypeError, ValueError):
            qty = 0
        if sym:
            broker_qty_map[sym] = qty

    mismatches = []
    for r in local_rows:
        contract = str(r.get("contract") or "").strip().upper()
        if not contract:
            continue
        local_qty = int(r.get("local_qty") or 0)
        if local_qty <= 0:
            continue
        broker_qty = broker_qty_map.get(contract, 0)
        if broker_qty == 0:
            mismatches.append(
                {
                    "position_id": r.get("position_id"),
                    "ticker": r.get("ticker"),
                    "contract": contract,
                    "local_qty": local_qty,
                    "broker_qty": broker_qty,
                    "mismatch_type": "local_open_broker_flat",
                }
            )

    if not mismatches:
        return None

    return BrakeResult(
        blocked=True,
        reason_code=BrakeCode.POSITION_MISMATCH,
        detail=(
            f"{len(mismatches)} position(s): local shows qty>0 but broker shows flat — "
            "broker/local state diverged; reconciler must resolve before new entry"
        ),
        evidence={
            "mismatch_count": len(mismatches),
            "mismatches": mismatches,
        },
    )


# ─────────────────────────────────────────────────────────────────────────────
# Public API
# ─────────────────────────────────────────────────────────────────────────────

def check_live_entry_brake(
    *,
    client_id: str,
    execution_mode: str,
    broker_positions_available: Optional[bool] = None,
    broker_positions: Optional[list] = None,
    checked_at: Optional[datetime] = None,
) -> BrakeResult:
    """
    Run all account-state brake classifiers for client_id and return a decision.

    CONTRACT:
      - Never raises — classifier/DB errors in LIVE mode fail closed with
        LIVE_ENTRY_BRAKE_STATE_UNAVAILABLE.
      - PAPER always passes through regardless of account state.
      - Blank/unknown execution_mode always blocks.
      - Every DB query is scoped to client_id — one client's unsafe state
        cannot block or reveal state of another client.
      - Must be called before deferred materialization and broker entry POST.
      - Must never be called from exit paths (exit_engine, exit_safety,
        exit_manager, protective close handlers).

    Args:
        client_id:                  The client to evaluate. Required.
        execution_mode:             "live" or "paper" (case-insensitive).
        broker_positions_available: True if broker snapshot succeeded and is
                                    trustworthy; False if the snapshot call
                                    failed; None to skip broker-aware checks.
        broker_positions:           Broker position list when available.
                                    Each element must have 'symbol'/'contract'
                                    and 'quantity'/'qty' keys.
        checked_at:                 Override UTC check timestamp for testing.

    Returns:
        BrakeResult with blocked=True if any condition fires.
        Stamp result.evidence into orders.meta['live_entry_brake'] for audit.
    """
    _mode = str(execution_mode or "").strip().lower()
    _cid  = str(client_id or "").strip()
    _now  = checked_at or _now_utc()

    base_evidence: dict = {
        "client_id":       _cid,
        "execution_mode":  _mode or None,
        "checked_at":      _now.isoformat(),
    }

    # ── Paper pass-through ────────────────────────────────────────────────────
    if _mode == "paper":
        return BrakeResult(
            blocked=False,
            reason_code=BrakeCode.CLEAR,
            detail="paper mode — account-state entry brake does not apply",
            evidence={**base_evidence, "paper_passthrough": True},
        )

    # ── Unknown execution_mode blocks — cannot route safely ──────────────────
    if _mode != "live":
        return BrakeResult(
            blocked=True,
            reason_code=BrakeCode.UNKNOWN_EXECUTION_MODE,
            detail=(
                f"execution_mode={_mode!r} is not 'live' or 'paper' — "
                "blocking to prevent misrouted capital"
            ),
            evidence={**base_evidence},
        )

    # ── LIVE path — run classifiers; fail closed on any error ─────────────────
    try:
        classifiers = [
            lambda: _check_split_brain(_cid),
            lambda: _check_circuit_breaker(_cid),
            lambda: _check_unknown_mode_exposure(_cid),
            lambda: _check_stale_exit_in_flight(_cid),
            lambda: _check_broker_snapshot(_cid, broker_positions_available, broker_positions),
        ]

        for classifier in classifiers:
            result = classifier()
            if result is not None and result.blocked:
                # Merge base evidence into the classifier's evidence dict
                result.evidence.update(base_evidence)
                log.warning(
                    "[%s] live_entry_brake TRIGGERED | reason=%s | %s",
                    _cid, result.reason_code, result.detail,
                )
                return result

        log.debug(
            "[%s] live_entry_brake CLEAR | all six conditions passed",
            _cid,
        )
        return BrakeResult(
            blocked=False,
            reason_code=BrakeCode.CLEAR,
            detail="all account-state checks clear",
            evidence={**base_evidence, "all_clear": True},
        )

    except Exception as exc:
        # Classifier or DB error in LIVE mode — fail closed.
        # Logging at CRITICAL because this means real money entry is blocked
        # by an infrastructure issue that must be investigated.
        log.critical(
            "[%s] live_entry_brake CLASSIFIER FAILED | error=%s | "
            "failing closed to protect client capital — "
            "fix the infrastructure issue and redeploy",
            _cid, exc,
            exc_info=True,
        )
        return BrakeResult(
            blocked=True,
            reason_code=BrakeCode.STATE_UNAVAILABLE,
            detail=(
                f"brake classifier error — failing closed: "
                f"{type(exc).__name__}: {exc}"
            ),
            evidence={
                **base_evidence,
                "classifier_error": str(exc),
                "classifier_error_type": type(exc).__name__,
            },
        )
