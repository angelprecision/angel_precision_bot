from __future__ import annotations

import json
import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from typing import Any, Optional

from ap.db import conn, run_with_retry
from ap.notify import post_discord
from ap.utils import now_utc_iso

log = logging.getLogger("ap.exit_safety")

_SCHEMA_CACHE_LOCK = threading.Lock()
_SCHEMA_CACHE: dict[str, set[str]] = {}
_ALERT_CACHE_LOCK = threading.Lock()
_ALERT_CACHE: dict[tuple[str, str, str, str, str], float] = {}

_DEFAULT_LOOKBACK_MINUTES = 60
_DEFAULT_THRESHOLD = 5
_ALERT_COOLDOWN_SECONDS = 900

_TERMINAL_CLOSE_SOURCES = {
    "reconciler_auto_close",
    "operator_manual_close",
    "expired",
    "expiry",
    "manual_close",
    "broker_position_missing",
}

_BROKER_REJECT_LAST_ERROR_PATTERNS = (
    "%broker_rejected_exit%",
    "%broker_http_4%",
    "%broker_status:rejected%",
    "%permanent_broker_reject%",
)


def _normalize_text(value: Any) -> str:
    return str(value or "").strip().lower()


def _normalize_mode(value: Any) -> Optional[str]:
    normalized = _normalize_text(value)
    return normalized or None


def _safe_int(value: Any) -> Optional[int]:
    try:
        if value in (None, ""):
            return None
        return int(float(value))
    except Exception:
        return None


def _normalize_contract(value: Any) -> str:
    return str(value or "").strip().upper().replace(" ", "")


def _extract_broker_account_id(broker: Any) -> str:
    return _normalize_text(
        getattr(broker, "account_id", None)
        or getattr(getattr(broker, "cfg", None), "account_id", None)
        or getattr(broker, "_account_id", None)
        or ""
    )


def _extract_position_account_id(raw: dict[str, Any]) -> str:
    nested = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    for key in ("account_id", "account", "account_number"):
        value = raw.get(key)
        if value not in (None, ""):
            return _normalize_text(value)
        value = nested.get(key)
        if value not in (None, ""):
            return _normalize_text(value)
    return ""


def _extract_position_contract(raw: dict[str, Any]) -> str:
    nested = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    for key in ("contract", "option_symbol", "symbol", "instrument"):
        value = raw.get(key)
        if value not in (None, ""):
            return _normalize_contract(value)
        value = nested.get(key)
        if value not in (None, ""):
            return _normalize_contract(value)
    return ""


def _extract_long_position_qty(raw: dict[str, Any]) -> Optional[int]:
    """Extract the long, open quantity for a matched exact-contract broker
    position row.

    P0 invariant (spec: p0_tradier_exact_quantity_flat_guard_20260817):
    malformed, negative, non-integral, boolean, or short-side quantity truth
    must NEVER silently collapse to 0 and be summed as though it were a
    confirmed-flat/absent contribution. This helper returns exactly one of:

      - a POSITIVE int  -> a confirmed, valid, open long quantity.
      - int ``0``       -> an explicit, well-formed zero. This is
        deliberately distinct from ``None``: the caller (``resolve_exit_
        broker_truth``) applies the "an exact-contract row asserting
        quantity=0 is not, by itself, authoritative flat truth" policy —
        real broker-flat is normally row *absence*, not an explicit zero
        row, so a zero here is suspicious/conflicting rather than trusted.
      - ``None``        -> malformed / negative / non-integral / boolean /
        short-side / unparseable / missing quantity. The caller must never
        treat this as 0 and must never sum it toward an open-quantity total.

    NOTE: for TradierBroker-sourced rows specifically, ``list_positions()``
    already rejects bool/fractional/negative/non-finite quantities at the
    adapter boundary (raises before this function is ever reached for that
    row). This function is defense-in-depth for any other broker/raw-dict
    source that does not enforce the same adapter-level contract.
    """
    nested = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    raw_value: Any = None
    found = False
    for key in ("quantity", "qty", "quantity_remaining", "remaining_quantity"):
        candidate = raw.get(key)
        if candidate not in (None, ""):
            raw_value = candidate
            found = True
            break
        candidate = nested.get(key)
        if candidate not in (None, ""):
            raw_value = candidate
            found = True
            break

    side_text = " ".join(
        str(v or "")
        for v in (
            raw.get("side"),
            raw.get("position_type"),
            raw.get("direction"),
            nested.get("side"),
            nested.get("position_type"),
            nested.get("direction"),
        )
    ).strip().lower()

    if not found:
        # No quantity field present at all -- cannot establish truth. This is
        # NOT the same as a confirmed-absent contract (that case never
        # reaches this function; absence is handled by resolve_exit_broker_
        # truth's "no matched rows" branch). A row that IS present but
        # carries no quantity is malformed.
        return None

    # bool is a numeric subtype in Python (float(False) == 0.0); it must
    # never masquerade as a real quantity.
    if isinstance(raw_value, bool):
        return None

    try:
        qty_float = float(raw_value)
    except (TypeError, ValueError):
        return None

    if not math.isfinite(qty_float):
        return None

    if not qty_float.is_integer():
        # A fractional quantity is not a valid whole-contract count and must
        # not be silently truncated toward a believable integer.
        return None

    qty = int(qty_float)

    if qty < 0:
        # Negative quantity is broker exposure, not flat. Never coerce to 0.
        return None

    if "short" in side_text:
        # Short-side exposure conflicts with AP's long-only lifecycle here;
        # never coerce to 0 either -- this is a conflict, not an absence.
        return None

    return qty


def resolve_exit_broker_truth(
    *,
    broker: Any,
    client_id: str,
    contract: str,
) -> dict[str, Any]:
    checked_at = now_utc_iso()
    normalized_contract = _normalize_contract(contract)
    account_id = _extract_broker_account_id(broker)
    audit = {
        "source": "broker.list_positions",
        "checked_at": checked_at,
        "client_id": str(client_id or "").strip().lower(),
        "account": account_id,
        "contract": str(contract or ""),
        "normalized_contract": normalized_contract,
        "exact_contract_match": False,
    }

    # P0 amendment 2 (spec: p0_tradier_exact_quantity_flat_guard_20260817,
    # surgical follow-up): a missing/unproven contract identity must NEVER
    # be treated as authoritative broker-flat truth. Without this guard, an
    # empty normalized_contract would match no row in any successful
    # snapshot -- including a non-empty one -- and the "no matched rows"
    # branch below would incorrectly manufacture fresh broker-flat truth
    # (broker_truth_open_qty=0, is_fresh_exact=True) for a contract we never
    # actually established identity for.
    #
    # ABSENCE CAN ONLY PROVE FLATNESS WHEN WE KNOW EXACTLY WHICH BROKER
    # CONTRACT WE WERE TRYING TO FIND. Missing/unproven identity is UNKNOWN
    # truth, never broker-flat truth.
    if not normalized_contract:
        audit["snapshot_status"] = "contract_identity_unavailable"
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    list_positions = getattr(broker, "list_positions", None)
    if not callable(list_positions):
        audit["snapshot_status"] = "broker_positions_unavailable"
        audit["error"] = "broker_list_positions_missing"
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    try:
        rows = list_positions()
    except Exception as exc:
        audit["snapshot_status"] = "broker_positions_error"
        audit["error"] = str(exc)
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    if rows is None:
        rows = []
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        audit["snapshot_status"] = "broker_positions_malformed"
        audit["error"] = f"unexpected_payload:{type(rows).__name__}"
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    matched_rows: list[dict[str, Any]] = []
    conflict_rows: list[dict[str, Any]] = []
    broker_truth_open_qty = 0
    for raw in rows:
        if not isinstance(raw, dict):
            continue
        row_contract = _extract_position_contract(raw)
        if not row_contract or row_contract != normalized_contract:
            continue
        row_account = _extract_position_account_id(raw)
        if row_account and account_id and row_account != account_id:
            continue
        long_qty = _extract_long_position_qty(raw)
        # P0 amendment (spec: p0_tradier_exact_quantity_flat_guard_20260817):
        # _extract_long_position_qty now returns three distinct outcomes and
        # each must be handled differently -- None and 0 are NOT the same
        # as "no exposure" and must never be silently summed as 0 the way
        # they were before this amendment (which allowed a malformed,
        # negative, or explicit-zero exact-match row to manufacture fresh
        # broker-flat truth).
        if long_qty is None:
            # Malformed / negative / non-integral / boolean / short-side
            # quantity on an exact-match row. Cannot establish truth for
            # this row; the WHOLE resolution must become unknown rather
            # than silently contributing 0 toward the open-quantity sum.
            conflict_rows.append(
                {
                    "contract": row_contract,
                    "account": row_account or account_id,
                    "long_qty": None,
                    "conflict_reason": "malformed_or_conflicting_quantity",
                }
            )
            continue
        if long_qty == 0:
            # An exact-contract row explicitly asserting quantity=0. Real
            # broker-flat is normally row ABSENCE, not an explicit zero row
            # (see the "no matched rows" branch below) — a well-formed but
            # explicit zero on an exact match is itself suspicious/
            # conflicting rather than trusted authoritative flat truth.
            conflict_rows.append(
                {
                    "contract": row_contract,
                    "account": row_account or account_id,
                    "long_qty": 0,
                    "conflict_reason": "explicit_zero_exact_match_row",
                }
            )
            continue
        broker_truth_open_qty += long_qty
        matched_rows.append(
            {
                "contract": row_contract,
                "account": row_account or account_id,
                "long_qty": long_qty,
            }
        )

    if conflict_rows:
        # Any conflicting/malformed/explicit-zero exact-match row makes the
        # ENTIRE resolution unknown -- fail closed, consistent with the
        # existing per-row malformed-payload philosophy in
        # TradierBroker.list_positions() (one bad row invalidates the whole
        # snapshot's truth for this contract rather than being averaged
        # away or silently dropped).
        audit.update(
            {
                "snapshot_status": "exact_match_conflict_unknown_quantity",
                "exact_contract_match": True,
                "matched_row_count": len(matched_rows),
                "matched_rows": matched_rows,
                "conflict_row_count": len(conflict_rows),
                "conflict_rows": conflict_rows,
            }
        )
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    if not matched_rows:
        # Production-shape fix: list_positions() succeeded and returned a valid list
        # but this OCC contract is absent. In real broker shape, a flat/closed option
        # simply disappears from the positions snapshot — it does NOT appear as a row
        # with qty=0. "Contract absent from fresh snapshot" = "broker confirms qty=0".
        #
        # This is the VZ/META production failure class: a stale synthetic local
        # position that the broker had already closed was flowing through as
        # "no broker truth" because the missing row was treated as unknown.
        # Treat absent-from-snapshot as exact flat only when list_positions() itself
        # succeeded and returned a parseable list (including empty list).
        # list_positions() error / malformed payload / missing method → None (no change).
        audit["snapshot_status"]      = "contract_absent_open_qty_zero"
        audit["exact_contract_match"] = False
        audit["broker_position_count"] = len(rows)
        return {
            "broker_truth_open_qty": 0,
            "is_fresh_exact": True,
            "audit": audit,
        }

    audit.update(
        {
            "snapshot_status": "exact_match",
            "exact_contract_match": True,
            "matched_row_count": len(matched_rows),
            "matched_rows": matched_rows,
            "broker_truth_open_qty": broker_truth_open_qty,
        }
    )
    return {
        "broker_truth_open_qty": int(broker_truth_open_qty),
        "is_fresh_exact": True,
        "audit": audit,
    }


def _table_columns(table_name: str) -> set[str]:
    with _SCHEMA_CACHE_LOCK:
        cached = _SCHEMA_CACHE.get(table_name)
        if cached is not None:
            return cached

    def _fn():
        with conn() as c:
            c.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = %s
                """,
                (table_name,),
            )
            return {str(r["column_name"]) for r in c.fetchall()}

    try:
        columns = run_with_retry(_fn) or set()
    except Exception as exc:
        log.debug("schema inspection failed for %s: %s", table_name, exc)
        columns = set()

    with _SCHEMA_CACHE_LOCK:
        _SCHEMA_CACHE[table_name] = columns
    return columns


def refresh_schema_cache() -> None:
    with _SCHEMA_CACHE_LOCK:
        _SCHEMA_CACHE.clear()


def has_orders_column(name: str) -> bool:
    return name in _table_columns("orders")


def has_positions_column(name: str) -> bool:
    return name in _table_columns("positions")


def _parse_entry_ts(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _parse_max_exit_rejections_threshold() -> int:
    raw = os.getenv("MAX_EXIT_REJECTIONS_BEFORE_HALT")
    if raw is None or str(raw).strip() == "":
        return _DEFAULT_THRESHOLD
    try:
        threshold = int(str(raw).strip())
    except Exception:
        log.warning(
            "MAX_EXIT_REJECTIONS_BEFORE_HALT=%r invalid; using default=%d",
            raw,
            _DEFAULT_THRESHOLD,
        )
        return _DEFAULT_THRESHOLD
    if threshold <= 0:
        log.warning(
            "MAX_EXIT_REJECTIONS_BEFORE_HALT=%d disables exit rejection circuit breaker",
            threshold,
        )
    return threshold


def _persist_circuit_breaker_marker(
    db_conn,
    *,
    position_id: str,
    client_id: str,
    rejection_count: int,
) -> bool:
    position_columns = _table_columns("positions")
    metadata_col = "metadata" if "metadata" in position_columns else ("meta" if "meta" in position_columns else "")
    if not metadata_col:
        return False

    updates = [f"{metadata_col} = COALESCE({metadata_col}, '{{}}'::jsonb) || %s::jsonb"]
    params: list[Any] = [
        json.dumps(
            {
                "exit_circuit_breaker_tripped": True,
                "exit_circuit_breaker_tripped_at": now_utc_iso(),
                "exit_circuit_breaker_rejection_count": int(rejection_count),
            }
        )
    ]

    if "updated_ts" in position_columns:
        updates.append("updated_ts = NOW()")
    elif "updated_at" in position_columns:
        updates.append("updated_at = NOW()")

    params.extend([position_id, client_id])
    db_conn.execute(
        f"""
        UPDATE positions
        SET {", ".join(updates)}
        WHERE id = %s
          AND client_id = %s
        """,
        tuple(params),
    )
    return True


def _rate_limit_key(*, client_id: str, execution_mode: Optional[str], position_id: str, contract: str, reason: str) -> tuple[str, str, str, str, str]:
    return (
        str(client_id or "").strip().lower(),
        str(execution_mode or "").strip().lower(),
        str(position_id or "").strip(),
        str(contract or "").strip().upper(),
        str(reason or "").strip().lower(),
    )


def alert_exit_submission_halted(
    *,
    client_id: str,
    execution_mode: Optional[str],
    position_id: str,
    contract: str,
    reason: str,
    rejection_count: Optional[int] = None,
    threshold: Optional[int] = None,
) -> bool:
    cache_key = _rate_limit_key(
        client_id=client_id,
        execution_mode=execution_mode,
        position_id=position_id,
        contract=contract,
        reason=reason,
    )
    now = time.time()
    with _ALERT_CACHE_LOCK:
        last_ts = _ALERT_CACHE.get(cache_key, 0.0)
        if now - last_ts < _ALERT_COOLDOWN_SECONDS:
            return False
        _ALERT_CACHE[cache_key] = now

    details = [
        "EXIT HALT",
        f"client={client_id}",
        f"mode={execution_mode or 'unknown'}",
        f"position_id={position_id}",
        f"contract={contract}",
        f"reason={reason}",
    ]
    if rejection_count is not None:
        details.append(f"rejections={rejection_count}")
    if threshold is not None:
        details.append(f"threshold={threshold}")
    try:
        post_discord(" | ".join(details))
    except Exception as exc:
        log.warning("exit halt alert failed: %s", exc)
    return True


def _exit_position_terminal_state(
    db_conn,
    *,
    position_id,
    client_id,
    execution_mode=None,
    contract=None,
    broker_truth_open_qty=None,
    allow_missing_position_with_broker_truth: bool = False,
) -> dict:
    position_columns = _table_columns("positions")
    selected_columns = ["status"]
    for optional_col in ("quantity_remaining", "close_source", "entry_ts"):
        if optional_col in position_columns:
            selected_columns.append(optional_col)

    params: list[Any] = [position_id, client_id]
    sql = (
        f"SELECT {', '.join(selected_columns)} "
        "FROM positions "
        "WHERE id = %s AND client_id = %s"
    )
    normalized_mode = _normalize_mode(execution_mode)
    if normalized_mode and "execution_mode" in position_columns:
        sql += " AND LOWER(COALESCE(execution_mode, '')) = %s"
        params.append(normalized_mode)
    sql += " LIMIT 1"

    db_conn.execute(sql, tuple(params))
    row = db_conn.fetchone()
    if not row:
        broker_truth_qty = _safe_int(broker_truth_open_qty)
        synthetic_repair_id = str(position_id or "").startswith("broker-repair-")
        if broker_truth_qty is not None and broker_truth_qty > 0 and (
            allow_missing_position_with_broker_truth or synthetic_repair_id
        ):
            log.info(
                "exit guard allowing missing position from broker truth | client_id=%s execution_mode=%s position_id=%s contract=%s broker_truth_open_qty=%s synthetic_repair=%s",
                client_id,
                normalized_mode or "",
                position_id,
                contract or "",
                broker_truth_qty,
                synthetic_repair_id,
            )
            return {
                "blocked": False,
                "reason": None,
                "status": None,
                "quantity_remaining": broker_truth_qty,
                "close_source": None,
                "entry_ts": None,
            }
        log.warning(
            "exit guard blocked missing position | client_id=%s execution_mode=%s position_id=%s contract=%s",
            client_id,
            normalized_mode or "",
            position_id,
            contract or "",
        )
        return {
            "blocked": True,
            "reason": "position_missing",
            "status": None,
            "quantity_remaining": None,
            "close_source": None,
            "entry_ts": None,
        }

    status = row.get("status")
    quantity_remaining = row.get("quantity_remaining") if "quantity_remaining" in row else None
    close_source = row.get("close_source") if "close_source" in row else None
    entry_ts = row.get("entry_ts") if "entry_ts" in row else None

    status_norm = _normalize_text(status)
    close_source_norm = _normalize_text(close_source)
    qty_remaining_int = _safe_int(quantity_remaining)

    if status_norm == "closed":
        return {
            "blocked": True,
            "reason": "position_already_closed",
            "status": status,
            "quantity_remaining": qty_remaining_int,
            "close_source": close_source,
            "entry_ts": entry_ts,
        }

    if qty_remaining_int is not None and qty_remaining_int <= 0:
        return {
            "blocked": True,
            "reason": "position_quantity_depleted",
            "status": status,
            "quantity_remaining": qty_remaining_int,
            "close_source": close_source,
            "entry_ts": entry_ts,
        }

    if close_source_norm in _TERMINAL_CLOSE_SOURCES:
        return {
            "blocked": True,
            "reason": f"terminal_close_source:{close_source_norm}",
            "status": status,
            "quantity_remaining": qty_remaining_int,
            "close_source": close_source,
            "entry_ts": entry_ts,
        }

    return {
        "blocked": False,
        "reason": None,
        "status": status,
        "quantity_remaining": qty_remaining_int,
        "close_source": close_source,
        "entry_ts": entry_ts,
    }


def _should_halt_exit_after_rejections(
    db_conn,
    *,
    position_id,
    client_id,
    execution_mode=None,
    contract,
    entry_ts=None,
    broker_truth_open_qty: Optional[int] = None,
) -> dict:
    threshold = _parse_max_exit_rejections_threshold()
    if threshold <= 0:
        return {
            "blocked": False,
            "reason": None,
            "rejection_count": 0,
            "threshold": threshold,
        }

    order_columns = _table_columns("orders")
    normalized_mode = _normalize_mode(execution_mode)
    params: list[Any] = [client_id, contract]
    sql = [
        "SELECT COUNT(*) AS rejection_count",
        "FROM orders",
        "WHERE client_id = %s",
        "  AND kind = 'EXIT'",
        "  AND contract = %s",
        "  AND (",
        "        status = 'REJECTED'",
    ]

    if "last_error" in order_columns:
        sql.extend(
            [
                "        OR (",
                "            status = 'ERROR'",
                "            AND (",
            ]
        )
        for idx, pattern in enumerate(_BROKER_REJECT_LAST_ERROR_PATTERNS):
            sql.append("                COALESCE(last_error, '') ILIKE %s")
            if idx < len(_BROKER_REJECT_LAST_ERROR_PATTERNS) - 1:
                sql.append("                OR")
            params.append(pattern)
        sql.extend(
            [
                "            )",
                "        )",
            ]
        )
    sql.append("      )")

    if normalized_mode and "execution_mode" in order_columns:
        sql.append("  AND LOWER(COALESCE(execution_mode, '')) = %s")
        params.append(normalized_mode)

    timestamp_expr = None
    if "updated_ts" in order_columns and "created_ts" in order_columns:
        timestamp_expr = "COALESCE(updated_ts, created_ts)"
    elif "updated_ts" in order_columns:
        timestamp_expr = "updated_ts"
    elif "created_ts" in order_columns:
        timestamp_expr = "created_ts"

    parsed_entry_ts = _parse_entry_ts(entry_ts)
    if timestamp_expr and parsed_entry_ts is not None:
        sql.append(f"  AND {timestamp_expr} >= %s")
        params.append(parsed_entry_ts)
    elif timestamp_expr:
        sql.append(f"  AND {timestamp_expr} >= NOW() - (%s * INTERVAL '1 minute')")
        params.append(_DEFAULT_LOOKBACK_MINUTES)

    db_conn.execute("\n".join(sql), tuple(params))
    row = db_conn.fetchone() or {}
    rejection_count = int(row.get("rejection_count") or 0)
    blocked = rejection_count >= threshold

    if not blocked:
        return {
            "blocked": False,
            "reason": None,
            "rejection_count": rejection_count,
            "threshold": threshold,
        }

    # ── P0 (PR #307): broker-truth circuit breaker override ─────────────────
    # The circuit breaker fires on repeated exit rejections to prevent
    # dangerous flapping. But it must not trap a LIVE open broker position
    # with no way to close it.
    #
    # Case A — broker truth confirms open qty > 0:
    #   Allow one controlled protective close regardless of rejection count.
    #   The circuit breaker is designed to stop repeated blind exits, not to
    #   prevent closing a confirmed real position. Stamp
    #   PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH so the caller knows this was
    #   an override. The duplicate-in-flight guard in the exit engine still
    #   prevents concurrent double-submits.
    #
    # Case B — broker truth confirms flat (qty == 0) while circuit breaker
    #   keeps firing:
    #   The local/synthetic position believes it is open but the broker says
    #   it is flat. Stop repeated firing by returning a specific reason code
    #   SYNTHETIC_POSITION_STALE_BROKER_FLAT. The OSM caller must then mark
    #   the synthetic position stale so the exit engine stops evaluating it.
    #   Do NOT submit an exit order — there is nothing to close.
    _broker_qty = None
    try:
        _broker_qty = int(broker_truth_open_qty) if broker_truth_open_qty is not None else None
    except (TypeError, ValueError):
        _broker_qty = None

    if _broker_qty is not None:
        if _broker_qty > 0:
            # Case A: real broker exposure — allow the protective close
            log.warning(
                "exit_circuit_breaker OVERRIDE: broker_truth_open_qty=%d > 0 "
                "for position_id=%s client_id=%s contract=%s — "
                "allowing protective close despite %d rejections (threshold=%d). "
                "Duplicate-in-flight guard still active.",
                _broker_qty, position_id, client_id, contract,
                rejection_count, threshold,
            )
            return {
                "blocked": False,
                "reason": "PROTECTIVE_EXIT_ALLOWED_BY_BROKER_TRUTH",
                "rejection_count": rejection_count,
                "threshold": threshold,
                "broker_truth_open_qty": _broker_qty,
                "circuit_breaker_overridden": True,
            }
        else:
            # Case B: broker is flat — stop repeated exit firing
            log.warning(
                "exit_circuit_breaker STALE_POSITION: broker_truth_open_qty=0 "
                "for position_id=%s client_id=%s contract=%s — "
                "synthetic position is stale; broker is flat. "
                "Blocking with SYNTHETIC_POSITION_STALE_BROKER_FLAT "
                "to stop repeated exit firing.",
                position_id, client_id, contract,
            )
            # Persist the circuit breaker marker so the entry brake (#306)
            # and the health dashboard can see this position is stuck.
            try:
                _persist_circuit_breaker_marker(
                    db_conn,
                    position_id=str(position_id),
                    client_id=str(client_id),
                    rejection_count=rejection_count,
                )
            except Exception as _cb_exc:
                log.debug("circuit breaker marker persistence failed: %s", _cb_exc)
            return {
                "blocked": True,
                "reason": "SYNTHETIC_POSITION_STALE_BROKER_FLAT",
                "rejection_count": rejection_count,
                "threshold": threshold,
                "broker_truth_open_qty": 0,
                "synthetic_stale": True,
            }

    # No broker truth supplied — original behavior: persist marker and block.
    if position_id:
        try:
            _persist_circuit_breaker_marker(
                db_conn,
                position_id=str(position_id),
                client_id=str(client_id),
                rejection_count=rejection_count,
            )
        except Exception as exc:
            log.debug("exit circuit breaker marker persistence failed: %s", exc)

    return {
        "blocked": blocked,
        "reason": "exit_circuit_breaker_tripped" if blocked else None,
        "rejection_count": rejection_count,
        "threshold": threshold,
    }


def evaluate_exit_submission_safety(
    *,
    position_id: str,
    client_id: str,
    execution_mode: Optional[str],
    contract: str,
    broker_truth_open_qty: Optional[int] = None,
    allow_missing_position_with_broker_truth: bool = False,
) -> dict:
    def _fn():
        with conn() as c:
            position_state = _exit_position_terminal_state(
                c,
                position_id=position_id,
                client_id=client_id,
                execution_mode=execution_mode,
                contract=contract,
                broker_truth_open_qty=broker_truth_open_qty,
                allow_missing_position_with_broker_truth=allow_missing_position_with_broker_truth,
            )
            if position_state.get("blocked"):
                return {
                    "blocked": True,
                    "reason": position_state.get("reason"),
                    "position_state": position_state,
                    "circuit_breaker": None,
                }

            circuit_breaker = _should_halt_exit_after_rejections(
                c,
                position_id=position_id,
                client_id=client_id,
                execution_mode=execution_mode,
                contract=contract,
                entry_ts=position_state.get("entry_ts"),
                broker_truth_open_qty=broker_truth_open_qty,
            )
            return {
                "blocked": bool(circuit_breaker.get("blocked")),
                "reason": circuit_breaker.get("reason"),
                "position_state": position_state,
                "circuit_breaker": circuit_breaker,
            }

    try:
        return run_with_retry(_fn)
    except Exception as exc:
        log.warning(
            "exit safety preflight unavailable; allowing submit to preserve existing flow | "
            "client_id=%s execution_mode=%s position_id=%s contract=%s err=%s",
            client_id,
            execution_mode or "",
            position_id,
            contract,
            exc,
        )
        return {
            "blocked": False,
            "reason": None,
            "position_state": None,
            "circuit_breaker": None,
        }
