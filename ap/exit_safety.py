from __future__ import annotations

import json
import logging
import os
import re
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
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
    """Parse a non-negative integral broker quantity without lossy coercion."""
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        quantity = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if (
        not quantity.is_finite()
        or quantity != quantity.to_integral_value()
        or quantity < 0
    ):
        return None
    return int(quantity)


def _safe_signed_int(value: Any) -> Optional[int]:
    """Parse a signed integral broker quantity without lossy coercion."""
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        quantity = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError):
        return None
    if (
        not quantity.is_finite()
        or quantity != quantity.to_integral_value()
    ):
        return None
    return int(quantity)


OCC_CONTRACT_RE = re.compile(r"^[A-Z0-9.]{1,6}\d{6}[CP]\d{8}$")
_PADDED_OCC_CONTRACT_RE = re.compile(r"^([A-Z0-9.]{1,6})\s+(\d{6}[CP]\d{8})$")
_UNDERLYING_SYMBOL_RE = re.compile(r"^[A-Z0-9.]{1,6}$")
_POSITION_IDENTITY_FIELDS = (
    "option_symbol",
    "optionSymbol",
    "contract",
    "instrument",
    "option_contract",
    "optionContract",
    "symbol",
)


def _normalize_contract(value: Any) -> str:
    text = str(value or "").strip().upper()
    if OCC_CONTRACT_RE.fullmatch(text):
        return text
    # Tradier may return the OCC root padded before the date.  Accept only that
    # provider shape; whitespace elsewhere is not an exact contract identity.
    padded = _PADDED_OCC_CONTRACT_RE.fullmatch(text)
    if padded:
        return f"{padded.group(1)}{padded.group(2)}"
    return text


def is_valid_exact_occ_contract(value: Any) -> bool:
    return bool(OCC_CONTRACT_RE.fullmatch(_normalize_contract(value)))


def _occ_root(contract: str) -> str:
    match = OCC_CONTRACT_RE.fullmatch(contract)
    return match.group(0)[:-15] if match else ""


def _iter_position_identity_values(raw: dict[str, Any]) -> tuple[str, str]:
    """Return (state, exact OCC) for one broker position row.

    A bare ``symbol`` can be an underlying row in an account snapshot and is
    therefore non-target data.  A value supplied as an explicit contract field
    must be an exact OCC identity.  Conflicting or malformed option identity
    stays unknown rather than becoming a false flat result.
    """
    records: list[dict[str, Any]] = []
    if isinstance(raw, dict):
        records.append(raw)
        nested = raw.get("raw")
        if isinstance(nested, dict):
            records.append(nested)

    exact_values: set[str] = set()
    underlying_values: set[str] = set()
    saw_identity = False
    invalid = False
    for record in records:
        for key in _POSITION_IDENTITY_FIELDS:
            if key not in record or record.get(key) in (None, ""):
                continue
            saw_identity = True
            value = record.get(key)
            normalized = _normalize_contract(value)
            if is_valid_exact_occ_contract(value):
                exact_values.add(normalized)
            elif key in {"symbol", "instrument"} and _UNDERLYING_SYMBOL_RE.fullmatch(normalized):
                underlying_values.add(normalized)
            else:
                invalid = True

    if len(exact_values) > 1:
        return "ambiguous", ""
    if exact_values:
        exact = next(iter(exact_values))
        if invalid or any(value != _occ_root(exact) for value in underlying_values):
            return "invalid", ""
        return "valid", exact
    if invalid:
        return "invalid", ""
    if underlying_values or not saw_identity:
        return "missing", ""
    return "invalid", ""


def _has_underlying_position_identity(raw: dict[str, Any]) -> bool:
    records: list[dict[str, Any]] = []
    if isinstance(raw, dict):
        records.append(raw)
        nested = raw.get("raw")
        if isinstance(nested, dict):
            records.append(nested)
    return any(
        key in record
        and record.get(key) not in (None, "")
        and _UNDERLYING_SYMBOL_RE.fullmatch(_normalize_contract(record.get(key)))
        for record in records
        for key in ("symbol", "instrument")
    )


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
    state, exact = _iter_position_identity_values(raw)
    return exact if state == "valid" else ""


def _extract_position_value(raw: dict[str, Any], *keys: str) -> Any:
    """Return the first explicitly supplied value from a broker row.

    The raw Tradier row is retained by the authoritative normalizer so
    recovery callers can use non-identity economics (for example
    ``cost_basis``) without reimplementing the envelope/identity parser.
    ``None`` and empty strings are treated as absent, but numeric zero is
    preserved as an explicit broker value.
    """
    nested = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    for key in keys:
        for record in (raw, nested):
            if key in record and record.get(key) not in (None, ""):
                return record.get(key)
    return None


def _extract_position_symbol(raw: dict[str, Any], exact: str) -> str:
    if exact:
        return exact
    nested = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    for record in (raw, nested):
        for key in ("symbol", "instrument"):
            value = _normalize_contract(record.get(key))
            if _UNDERLYING_SYMBOL_RE.fullmatch(value):
                return value
    return ""


def _extract_long_position_qty(raw: dict[str, Any]) -> Optional[int]:
    nested = raw.get("raw") if isinstance(raw.get("raw"), dict) else {}
    quantities: list[int] = []
    for key in ("quantity", "qty", "quantity_remaining", "remaining_quantity"):
        for record in (raw, nested):
            if key not in record or record.get(key) in (None, ""):
                continue
            parsed = _safe_signed_int(record.get(key))
            if parsed is None:
                return None
            quantities.append(parsed)
    if not quantities or len(set(quantities)) > 1:
        return None
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
    if "short" in side_text and "long" in side_text:
        return None
    if "short" in side_text:
        # Keep a signed negative value visible to the target-contract guard.
        # A short row with a positive broker quantity is not long exposure and
        # remains normalized to zero, but a negative target quantity is
        # contradictory evidence and must stay UNKNOWN rather than flat.
        if quantities[0] < 0:
            return quantities[0]
        return 0
    return quantities[0]


def _authoritative_position_payload(broker: Any) -> tuple[str, Any, str]:
    """Fetch positions without allowing adapter errors to look like flatness."""
    authoritative = getattr(type(broker), "list_positions_authoritative", None)
    if callable(authoritative):
        try:
            return "available", broker.list_positions_authoritative(), "broker.list_positions_authoritative"
        except Exception as exc:
            return "error", None, f"{type(exc).__name__}:{exc}"

    raw_get = getattr(type(broker), "_get", None)
    account_value = str(_extract_broker_account_id(broker) or "").strip()
    account_id = _normalize_text(account_value)
    if callable(raw_get) and account_id:
        try:
            payload = broker._get(f"/v1/accounts/{account_value}/positions")
            return "available", payload, "broker._get:/positions"
        except Exception as exc:
            return "error", None, f"{type(exc).__name__}:{exc}"

    list_positions = getattr(broker, "list_positions", None)
    if not callable(list_positions):
        return "unavailable", None, "broker_list_positions_missing"
    try:
        return "available", list_positions(), "broker.list_positions"
    except Exception as exc:
        return "error", None, f"{type(exc).__name__}:{exc}"


def _parse_authoritative_position_payload(payload: Any) -> tuple[str, list[dict[str, Any]]]:
    if isinstance(payload, list):
        rows = payload
    elif isinstance(payload, dict):
        if any(payload.get(key) not in (None, "") for key in ("error", "errors", "message")):
            return "malformed", []
        if _normalize_text(payload.get("status")) in {"error", "failed", "failure", "unavailable"}:
            return "malformed", []
        if "positions" not in payload:
            return "malformed", []
        positions = payload.get("positions")
        if positions is None or positions == "null":
            return "available", []
        # A bare empty object is an incomplete provider envelope, not the
        # documented empty-position shape.  Treating it as an authoritative
        # empty account could suppress a real EXIT during a malformed read.
        if isinstance(positions, dict) and not positions:
            return "malformed", []
        if not isinstance(positions, dict) or "position" not in positions:
            return "malformed", []
        rows = positions.get("position")
        if rows is None or rows == "null":
            return "available", []
        if isinstance(rows, dict):
            rows = [rows]
    else:
        return "malformed", []

    if not isinstance(rows, list):
        return "malformed", []
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            return "malformed", []
        identity_state, exact = _iter_position_identity_values(row)
        if identity_state in {"invalid", "ambiguous"}:
            return "malformed", []
        if identity_state == "missing" and not _has_underlying_position_identity(row):
            return "malformed", []
        quantity = _extract_long_position_qty(row)
        if quantity is None:
            return "malformed", []
        normalized_row = dict(row)
        # Preserve the provider row for audit/economic fields while replacing
        # only the fields whose semantics were proven by this normalizer.
        normalized_row.setdefault("raw", dict(row))
        normalized_row["contract"] = exact
        normalized_row["symbol"] = _extract_position_symbol(row, exact)
        normalized_row["quantity"] = quantity
        normalized_row["account"] = _extract_position_account_id(row)
        for key, aliases in {
            "cost_basis": ("cost_basis", "costBasis"),
            "date_acquired": ("date_acquired", "dateAcquired"),
            "side": ("side", "position_type", "positionType"),
        }.items():
            value = _extract_position_value(row, *aliases)
            if value is not None:
                if normalized_row.get(key) in (None, ""):
                    normalized_row[key] = value
        normalized.append(normalized_row)
    return "available", normalized


def resolve_authoritative_broker_positions(*, broker: Any) -> dict[str, Any]:
    """Return one parsed, authoritative broker-position snapshot.

    This is the shared PR #566 truth seam for consumers that need the full
    normalized position rows.  Fetch errors, malformed provider envelopes,
    contradictory identity, and invalid quantities all return
    ``is_fresh_exact=False``; only a successfully parsed list (including a
    documented empty list) is considered fresh broker truth.
    """
    checked_at = now_utc_iso()
    account_id = _normalize_text(_extract_broker_account_id(broker))
    audit = {
        "source": "broker.authoritative_positions",
        "checked_at": checked_at,
        "account": account_id,
    }

    fetch_state, payload, fetch_detail = _authoritative_position_payload(broker)
    audit["source_detail"] = fetch_detail
    if fetch_state != "available":
        audit["snapshot_status"] = (
            "broker_positions_error"
            if fetch_state == "error"
            else "broker_positions_unavailable"
        )
        audit["error"] = fetch_detail
        return {
            "positions": [],
            "is_fresh_exact": False,
            "audit": audit,
        }

    parse_state, rows = _parse_authoritative_position_payload(payload)
    if parse_state != "available":
        audit.update({
            "snapshot_status": "broker_positions_malformed",
            "error": "authoritative_position_payload_malformed",
        })
        return {
            "positions": [],
            "is_fresh_exact": False,
            "audit": audit,
        }

    audit.update({
        "snapshot_status": "available",
        "broker_position_count": len(rows),
    })
    return {
        "positions": rows,
        "is_fresh_exact": True,
        "audit": audit,
    }


def resolve_exit_broker_truth(
    *,
    broker: Any,
    client_id: str,
    contract: str,
) -> dict[str, Any]:
    checked_at = now_utc_iso()
    normalized_contract = _normalize_contract(contract)
    account_id = _normalize_text(_extract_broker_account_id(broker))
    audit = {
        "source": "broker.authoritative_positions",
        "checked_at": checked_at,
        "client_id": str(client_id or "").strip().lower(),
        "account": account_id,
        "contract": str(contract or ""),
        "normalized_contract": normalized_contract,
        "exact_contract_match": False,
    }

    if not is_valid_exact_occ_contract(normalized_contract):
        audit["snapshot_status"] = "contract_identity_unproven"
        audit["error"] = "exact_occ_contract_required"
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    fetch_state, payload, fetch_detail = _authoritative_position_payload(broker)
    audit["source_detail"] = fetch_detail
    if fetch_state != "available":
        audit["snapshot_status"] = (
            "broker_positions_error"
            if fetch_state == "error"
            else "broker_positions_unavailable"
        )
        audit["error"] = fetch_detail
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    parse_state, rows = _parse_authoritative_position_payload(payload)
    if parse_state != "available":
        audit["snapshot_status"] = "broker_positions_malformed"
        audit["error"] = "authoritative_position_payload_malformed"
        return {
            "broker_truth_open_qty": None,
            "is_fresh_exact": False,
            "audit": audit,
        }

    matched_rows: list[dict[str, Any]] = []
    broker_truth_open_qty = 0
    for row in rows:
        row_contract = row.get("contract") or ""
        if row_contract != normalized_contract:
            continue
        row_account = _normalize_text(row.get("account"))
        if row_account and not account_id:
            audit["snapshot_status"] = "broker_positions_account_identity_unproven"
            audit["error"] = "position_account_present_broker_account_missing"
            return {
                "broker_truth_open_qty": None,
                "is_fresh_exact": False,
                "audit": audit,
            }
        if row_account and account_id and row_account != account_id:
            continue
        long_qty = row.get("quantity")
        if not isinstance(long_qty, int) or isinstance(long_qty, bool) or long_qty < 0:
            audit["snapshot_status"] = "broker_positions_malformed"
            audit["error"] = "invalid_normalized_quantity"
            return {
                "broker_truth_open_qty": None,
                "is_fresh_exact": False,
                "audit": audit,
            }
        broker_truth_open_qty += long_qty
        matched_rows.append(
            {
                "contract": row_contract,
                "account": row_account or account_id,
                "long_qty": long_qty,
            }
        )

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
