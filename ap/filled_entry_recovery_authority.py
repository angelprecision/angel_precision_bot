"""Read-only current-risk authority for interrupted FILLED ENTRY handoffs.

An historical broker order in ``FILLED`` state proves that an execution
happened in the past.  It does not prove that the option position is still
open.  This module is the narrow, mutation-free authority boundary used by
``ap.fill_monitor`` before it repairs a terminal FILLED ENTRY handoff.

The module only reads broker positions and local position state.  It never
submits, cancels, changes an order or position, writes proof, or changes queue
state.
"""

from __future__ import annotations

import math
import re
from datetime import date, datetime, timezone
from typing import Any


OCC_RE = re.compile(r"[A-Z0-9.]{1,6}\d{6}[CP]\d{8}")
OCC_EXPIRATION_RE = re.compile(r"([A-Z0-9.]{1,6})(\d{6})[CP]\d{8}")
VALID_EXECUTION_MODES = frozenset({"live", "paper"})
TERMINAL_POSITION_STATUSES = frozenset(
    {
        "CLOSED",
        "CLOSED_REPAIR",
        "EXPIRED",
        "STOPPED",
        "TAKEN_PROFIT",
        "ERROR",
        "CANCELED",
        "CANCELLED",
    }
)
_UNSET = object()


def _normalize_contract(value: Any) -> str:
    return str(value or "").upper().replace(" ", "").strip()


def is_valid_occ_contract(value: Any) -> bool:
    contract = _normalize_contract(value)
    return bool(OCC_RE.fullmatch(contract))


def _positive_integral(value: Any) -> int:
    if value is None or isinstance(value, bool):
        return 0
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0
    if not math.isfinite(parsed) or parsed <= 0 or not parsed.is_integer():
        return 0
    return int(parsed)


def _structural_quantity(value: Any) -> float | None:
    """Return a finite numeric quantity if the value is structurally valid.

    Structural validity is deliberately distinct from AP long-option
    authority.  A whole Tradier account snapshot can legitimately contain
    unrelated rows the AP recovery flow does not own or manage: short
    positions (negative quantity), fractional equity holdings, or other
    contracts.  Those are valid broker truth and must survive account-wide
    normalization unchanged so they cannot poison the exact-target-OCC
    evaluation that happens later in ``evaluate_filled_entry_recovery_authority``.

    Only genuinely malformed values are rejected here: missing, boolean,
    non-numeric, NaN, and infinite.  Negative, zero, and fractional values
    are preserved as-is.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed):
        return None
    return parsed


def _structural_broker_quantity(row: dict) -> float | None:
    """Return quantity as a finite float when every supplied alias agrees.

    Mirrors ``_broker_quantity`` except it does not force AP long-option
    authority (strictly positive integral).  Negative, zero, and fractional
    single-valued quantities are preserved; missing quantity, malformed
    values, and alias disagreement remain structurally invalid.
    """
    containers = [row]
    raw = row.get("raw") if isinstance(row, dict) else None
    if isinstance(raw, dict) and raw is not row:
        containers.append(raw)
    return _resolve_quantity_aliases(containers, _structural_quantity)


def _resolve_quantity_aliases(containers: list, parse) -> Any:
    """Resolve one quantity across ``quantity``/``qty`` aliases.

    PRESENCE is validated separately from VALUE.  A recognized alias key
    that is literally present in a container -- even with ``None``, an
    empty/whitespace string, a boolean, NaN, inf, or other unusable
    content -- is NOT equivalent to that alias being absent.  Silently
    dropping a present-but-unusable alias in favor of a different,
    parseable alias is a broker-truth laundering seam: it can hide
    contradictory/unproven quantity evidence beneath a normalized value
    that looks clean.

    ``parse`` converts one raw alias value to a validated result, or
    returns ``None`` if that value is unusable (its own semantics --
    e.g. ``_structural_quantity`` treats negative/zero/fractional as
    valid, ``_positive_integral``-based parsers do not).

    Returns the single agreed value, or ``None`` if no alias was
    supplied at all, any supplied alias was individually unusable, or
    multiple supplied aliases disagree.
    """
    values = []
    any_present = False
    for container in containers:
        for key in ("quantity", "qty"):
            if key not in container:
                continue
            any_present = True
            parsed = parse(container[key])
            if parsed is None:
                return None
            values.append(parsed)
    if not any_present:
        return None
    if len(set(values)) != 1:
        return None
    return values[0]


def _positive_finite(value: Any) -> float:
    if value is None or isinstance(value, bool):
        return 0.0
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    if not math.isfinite(parsed) or parsed <= 0:
        return 0.0
    return parsed


def _integral_or_none(value: Any) -> int | None:
    """Return an integer if the value is structurally a whole number.

    Distinct from ``_positive_integral``: zero and negative integral
    values are returned (not collapsed to ``None``/0) so presence-aware
    alias resolution can distinguish "structurally a number, just not
    positive" from "not a number at all".  AP long-only positivity is
    enforced by the caller after alias agreement is established.
    """
    if value is None or isinstance(value, bool):
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    if not math.isfinite(parsed) or not parsed.is_integer():
        return None
    return int(parsed)


def _broker_quantity(row: dict) -> int:
    """Return the AP-long-authority quantity only when every supplied
    quantity alias is individually valid and in exact agreement.

    A recognized alias that is PRESENT with an unusable value (None, "",
    whitespace, bool, NaN, inf, non-numeric, non-integer) is a validation
    failure -- it is never silently dropped in favor of a different,
    parseable alias.  That would launder contradictory/unproven broker
    evidence into an apparently-clean quantity.
    """
    containers = [row]
    raw = row.get("raw") if isinstance(row, dict) else None
    if isinstance(raw, dict) and raw is not row:
        containers.append(raw)
    resolved = _resolve_quantity_aliases(containers, _integral_or_none)
    if resolved is None or resolved <= 0:
        return 0
    return resolved


def _position_value(position: Any, name: str, default: Any = None) -> Any:
    if isinstance(position, dict):
        return position.get(name, default)
    return getattr(position, name, default)


def _position_id(position: Any) -> str:
    return str(
        _position_value(position, "id", "")
        or _position_value(position, "position_id", "")
        or ""
    ).strip()


def _position_contract(position: Any) -> str:
    return _normalize_contract(
        _position_value(position, "contract", "")
        or _position_value(position, "option_symbol", "")
        or _position_value(position, "symbol", "")
    )


def _position_is_terminal(position: Any) -> bool:
    status = str(_position_value(position, "status", "") or "").strip().upper()
    if status in TERMINAL_POSITION_STATUSES:
        return True
    if bool(_position_value(position, "closed", False)):
        return True
    remaining = _position_value(position, "quantity_remaining", None)
    if remaining is not None:
        return _positive_integral(remaining) <= 0
    return False


def _position_quantity(position: Any) -> int:
    for key in ("quantity_remaining", "qty", "quantity", "contracts"):
        value = _positive_integral(_position_value(position, key, None))
        if value > 0:
            return value
    return 0


def _result(disposition: str, reason_code: str, **extra: Any) -> dict:
    return {
        "disposition": disposition,
        "reason_code": reason_code,
        **extra,
    }


def _occ_expiration(contract: str) -> date | None:
    match = OCC_EXPIRATION_RE.fullmatch(_normalize_contract(contract))
    if not match:
        return None
    try:
        return datetime.strptime(match.group(2), "%y%m%d").date()
    except (TypeError, ValueError):
        return None


def _normalize_positions_payload(payload: Any) -> list[dict]:
    """Normalize the recovery broker payload without dropping malformed rows."""
    if not isinstance(payload, dict) or "positions" not in payload:
        raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_PAYLOAD_MALFORMED")

    positions_node = payload.get("positions")
    if positions_node is None or positions_node == "null":
        return []
    if not isinstance(positions_node, dict):
        raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_NODE_MALFORMED")

    # Authoritative-empty: Tradier returns {} (no positions in account).
    if positions_node == {}:
        return []

    # Any dict without the "position" key is an uninterpretable broker shape.
    # Silently treating it as [] would collapse to NONACTIONABLE and destroy
    # position/owner reconstruction authority with no retry.  Raise so the
    # caller routes to HOLD/retryable instead.
    if "position" not in positions_node:
        raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_NODE_MISSING")

    rows = positions_node["position"]
    if rows is None or rows == "null":
        return []
    if isinstance(rows, dict):
        rows = [rows]
    if not isinstance(rows, list):
        raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_ROWS_MALFORMED")

    normalized: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_ROW_MALFORMED")

        containers = [row]
        raw = row.get("raw")
        if isinstance(raw, dict) and raw is not row:
            containers.append(raw)

        # Raw Tradier transport contract: canonical `symbol` is required,
        # exactly as #481's TradierBroker.list_positions() requires a
        # non-empty `symbol` on every raw row.  This raw _get() fallback
        # -- the actual path production TradierBroker takes when
        # list_positions_authoritative() is absent -- must not become
        # structurally more permissive by letting an option_symbol/contract
        # ALIAS manufacture identity when canonical `symbol` is missing or
        # blank, or override canonical identity when it disagrees.  Aliases
        # may only CONFIRM identity here; they may never replace or upgrade
        # it.  (Already-normalized/internal rows reached via
        # list_positions_authoritative() are a separate compatibility path
        # and are unaffected -- see fetch_current_broker_positions.)
        if "symbol" not in row:
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_IDENTITY_MISSING")
        canonical_symbol = _normalize_contract(row.get("symbol"))
        if not canonical_symbol:
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_IDENTITY_MISSING")
        for container in containers:
            for key in ("option_symbol", "contract"):
                alias_value = container.get(key)
                if alias_value is None or not str(alias_value).strip():
                    continue
                if _normalize_contract(alias_value) != canonical_symbol:
                    raise ValueError(
                        "FILLED_ENTRY_RECOVERY_BROKER_POSITION_IDENTITY_AMBIGUOUS"
                    )
        contract = canonical_symbol

        # #481 established that account-level broker snapshot validity is
        # not the same thing as AP exact-contract long-option authority.
        # An unrelated row (a short position, a fractional equity holding,
        # a different contract) can carry a structurally valid negative or
        # fractional quantity; that must not raise here and poison the
        # whole account snapshot.  Only genuinely malformed quantity data
        # (missing, boolean, non-numeric, NaN/inf, or alias disagreement)
        # is rejected at this account-wide normalization boundary.  Strict
        # positive-integral AP-long validation happens only once the
        # exact target OCC contract is matched, in
        # ``evaluate_filled_entry_recovery_authority``.
        #
        # Raw Tradier transport contract: the canonical `quantity` field
        # is required.  #481's TradierBroker.list_positions() enforces
        # this on the adapter path; this raw _get() fallback -- the
        # actual path production TradierBroker takes when
        # list_positions_authoritative() is absent -- must not become
        # more permissive by accepting `qty` alone when the canonical
        # field is entirely missing from the row.  This check is scoped
        # to this raw-transport parser only; already-normalized internal
        # rows consumed via list_positions_authoritative() may still use
        # `qty` as a standalone compatibility alias (see
        # _structural_broker_quantity).
        if not any("quantity" in container for container in containers):
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_QUANTITY_INVALID")

        quantity_value = _resolve_quantity_aliases(containers, _structural_quantity)
        if quantity_value is None:
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_QUANTITY_INVALID")
        normalized.append(
            {"symbol": contract, "quantity": quantity_value, "raw": dict(row)}
        )
    return normalized


def _fetch_authoritative_broker_positions(broker: Any) -> list[dict]:
    """Fetch the exact broker snapshot used only by FILLED ENTRY recovery."""
    authoritative = getattr(broker, "list_positions_authoritative", None)
    if callable(authoritative):
        rows = authoritative()
        if not isinstance(rows, list):
            raise ValueError("FILLED_ENTRY_RECOVERY_AUTHORITATIVE_POSITIONS_MALFORMED")
        if any(not isinstance(row, dict) for row in rows):
            raise ValueError("FILLED_ENTRY_RECOVERY_AUTHORITATIVE_POSITION_ROW_MALFORMED")
        return [dict(row) for row in rows]

    raw_get = getattr(broker, "_get", None)
    cfg = getattr(broker, "cfg", None)
    account_id = str(getattr(cfg, "account_id", "") or "").strip()
    if not callable(raw_get) or not account_id:
        raise RuntimeError("FILLED_ENTRY_RECOVERY_AUTHORITATIVE_POSITIONS_UNAVAILABLE")
    return _normalize_positions_payload(
        raw_get(f"/v1/accounts/{account_id}/positions")
    )


def fetch_current_broker_positions(broker: Any) -> list[dict]:
    """Fetch and structurally validate one current broker position snapshot.

    ``fetch_authoritative_broker_positions`` is the current-main read-only
    broker-position adapter.  This wrapper additionally rejects malformed
    authoritative rows instead of silently treating malformed truth as an
    empty account.  Equity rows are allowed and ignored by the exact-OCC
    matcher; option rows must have an exact OCC symbol and positive integral
    quantity.
    """
    rows = _fetch_authoritative_broker_positions(broker)
    if not isinstance(rows, list):
        raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_MALFORMED")

    normalized: list[dict] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_ROW_MALFORMED")

        containers = [row]
        raw = row.get("raw")
        if isinstance(raw, dict) and raw is not row:
            containers.append(raw)
        explicit_contracts = {
            _normalize_contract(container.get(key))
            for container in containers
            for key in ("option_symbol", "contract")
            if container.get(key) is not None
            and str(container.get(key)).strip()
        }
        symbols = {
            _normalize_contract(container.get("symbol"))
            for container in containers
            if container.get("symbol") is not None
            and str(container.get("symbol")).strip()
        }
        if len(explicit_contracts) > 1:
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_IDENTITY_AMBIGUOUS")
        if explicit_contracts and any(
            is_valid_occ_contract(symbol)
            for symbol in symbols - explicit_contracts
        ):
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_IDENTITY_AMBIGUOUS")
        identities = explicit_contracts or symbols
        if len(identities) > 1:
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_IDENTITY_AMBIGUOUS")
        if not identities:
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_IDENTITY_MISSING")
        contract = next(iter(identities))

        # A symbol-only non-OCC row can be an equity position and is not part
        # of the option-risk snapshot.  Explicit option_symbol/contract rows
        # must always be exact OCC rows.
        explicit_option = bool(explicit_contracts)
        if not is_valid_occ_contract(contract):
            if explicit_option:
                raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_OCC_INVALID")
            continue

        # See _normalize_positions_payload: account-wide snapshot validity
        # is not AP-long authority.  Preserve structurally valid negative,
        # zero, and fractional quantities for unrelated rows here; the
        # exact-target-OCC matcher in
        # evaluate_filled_entry_recovery_authority enforces strict
        # positive-integral AP-long authority only for the one row whose
        # contract equals the order being recovered.
        raw_row = row.get("raw") if isinstance(row.get("raw"), dict) else row
        quantity = _structural_broker_quantity(raw_row)
        if quantity is None:
            raise ValueError("FILLED_ENTRY_RECOVERY_BROKER_POSITION_QUANTITY_INVALID")
        normalized.append({
            "symbol": contract,
            "quantity": quantity,
            "raw": dict(row),
        })
    return normalized


def _append_unique(candidates: list[Any], value: Any) -> None:
    if not value:
        return
    value_id = _position_id(value)
    if value_id:
        if any(_position_id(existing) == value_id for existing in candidates):
            return
    elif any(existing is value for existing in candidates):
        return
    candidates.append(value)


def _load_local_candidates(pm: Any, order: dict) -> tuple[list[Any], str | None]:
    """Read local rows that could own this exact local/broker identity."""
    if pm is None:
        return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_LOOKUP_FAILED"

    candidates: list[Any] = []
    try:
        get_active = getattr(pm, "get_active_positions", None)
        if callable(get_active):
            active_rows = get_active()
            if not isinstance(active_rows, (list, tuple)):
                return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITIONS_MALFORMED"
            for row in active_rows:
                if not isinstance(row, (dict,)) and not hasattr(row, "__dict__"):
                    return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_ROW_MALFORMED"
                row_id = _position_id(row)
                row_local = str(_position_value(row, "local_order_id", "") or "").strip()
                row_broker = str(_position_value(row, "broker_order_id", "") or "").strip()
                same_risk_domain = (
                    str(_position_value(row, "client_id", "") or "").strip()
                    == str(order.get("client_id") or "").strip()
                    and _position_value(row, "execution_mode", None)
                    == order.get("execution_mode")
                    and _position_contract(row)
                    == _normalize_contract(order.get("contract"))
                )
                exact_identity = (
                    (order.get("position_id") and row_id == str(order.get("position_id")).strip())
                    or (row_local and row_local == str(order.get("local_order_id") or "").strip())
                    or (row_broker and row_broker == str(order.get("broker_order_id") or "").strip())
                )
                if same_risk_domain and not exact_identity:
                    return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_AMBIGUOUS"
                if exact_identity:
                    _append_unique(candidates, row)
    except Exception:
        return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_LOOKUP_FAILED"

    # These exact identity lookups also see terminal rows and rows omitted by
    # get_active_positions(), which is required to distinguish a local
    # terminal + broker-open contradiction from a safe recreate.
    lookup_specs = []
    position_id = str(order.get("position_id") or "").strip()
    if position_id:
        lookup_specs.append(("get_position", position_id))
    lookup_specs.extend(
        [
            ("get_position_by_local_order", str(order.get("local_order_id") or "").strip()),
            ("get_position_by_broker_order", str(order.get("broker_order_id") or "").strip()),
        ]
    )
    for method_name, identity in lookup_specs:
        if not identity:
            continue
        method = getattr(pm, method_name, None)
        if not callable(method):
            continue
        try:
            found = method(identity)
        except Exception:
            return [], "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_LOOKUP_FAILED"
        _append_unique(candidates, found)
    return candidates, None


def evaluate_filled_entry_recovery_authority(
    *,
    pm: Any,
    broker: Any = None,
    order: dict,
    runtime_execution_mode: str | None = None,
    expected_client_id: str | None = None,
    broker_positions: Any = _UNSET,
    broker_positions_error: Exception | str | None = None,
    today: date | None = None,
) -> dict:
    """Classify an interrupted terminal FILLED ENTRY handoff.

    ``ACTIVE_EXISTING`` permits repair against one exact active local
    position.  ``ACTIVE_RECREATE`` permits local recreation only when the
    current broker quantity exactly equals durable filled quantity.  The two
    non-active dispositions grant no position or owner mutation authority.
    """
    if not isinstance(order, dict):
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_ORDER_INVALID", retryable=False)

    client_id = str(order.get("client_id") or "").strip()
    expected_client = str(expected_client_id or "").strip()
    local_order_id = str(order.get("local_order_id") or "").strip()
    broker_order_id = str(order.get("broker_order_id") or "").strip()
    execution_mode = order.get("execution_mode")
    contract = _normalize_contract(order.get("contract"))
    kind = str(order.get("kind") or "")
    status = str(order.get("status") or "")

    if expected_client and client_id != expected_client:
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_CLIENT_MISMATCH", retryable=False)
    if not client_id or not local_order_id or not broker_order_id:
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_IDENTITY_UNPROVEN", retryable=False)
    if broker_order_id.upper() in {
        "0", "N/A", "NA", "NONE", "NULL", "UNKNOWN", "UNDEFINED", "NIL", "TRUE", "FALSE",
    }:
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_BROKER_ORDER_ID_UNPROVEN", retryable=False)
    if kind != "ENTRY" or status != "FILLED":
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_ORDER_STATE_INVALID", retryable=False)
    if execution_mode not in VALID_EXECUTION_MODES:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_EXECUTION_MODE_UNPROVEN",
            execution_mode=execution_mode,
            retryable=False,
        )

    runtime_mode = runtime_execution_mode
    if runtime_mode not in VALID_EXECUTION_MODES:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_RUNTIME_MODE_UNPROVEN",
            execution_mode=execution_mode,
            retryable=False,
        )
    if runtime_mode != execution_mode:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_MODE_CONFLICT",
            execution_mode=execution_mode,
            runtime_execution_mode=runtime_mode,
            retryable=False,
        )
    if not is_valid_occ_contract(contract):
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_CONTRACT_INVALID",
            contract=contract,
            retryable=False,
        )

    durable_qty = _positive_integral(order.get("filled_qty"))
    durable_price = _positive_finite(order.get("fill_price"))
    if durable_qty <= 0:
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_QUANTITY_INVALID", retryable=False)
    if durable_price <= 0:
        return _result("HOLD", "FILLED_ENTRY_RECOVERY_FILL_PRICE_INVALID", retryable=False)

    expiration = _occ_expiration(contract)
    current_date = today or datetime.now(timezone.utc).date()
    if expiration is None:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_CONTRACT_EXPIRATION_UNPROVEN",
            contract=contract,
            retryable=False,
        )
    if expiration < current_date:
        return _result(
            "NONACTIONABLE",
            "HISTORICAL_FILLED_RECOVERY_EXPIRED_CONTRACT",
            contract=contract,
            expiration=expiration.isoformat(),
            current_date=current_date.isoformat(),
            retryable=False,
        )

    if broker_positions_error is not None:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_UNAVAILABLE",
            contract=contract,
            exception=str(broker_positions_error),
            retryable=True,
        )

    if broker_positions is _UNSET:
        try:
            broker_positions = fetch_current_broker_positions(broker)
        except Exception as exc:
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_UNAVAILABLE",
                contract=contract,
                exception_type=type(exc).__name__,
                exception=str(exc),
                retryable=True,
            )
    if not isinstance(broker_positions, list):
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_MALFORMED",
            contract=contract,
            retryable=True,
        )

    matches = []
    for row in broker_positions:
        if not isinstance(row, dict):
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_BROKER_POSITIONS_MALFORMED",
                contract=contract,
                retryable=True,
            )
        row_contract = _normalize_contract(
            row.get("option_symbol") or row.get("contract") or row.get("symbol")
        )
        if not row_contract:
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_BROKER_POSITION_IDENTITY_MISSING",
                contract=contract,
                retryable=True,
            )
        explicit_option = any(
            row.get(key) not in (None, "")
            for key in ("option_symbol", "contract")
        )
        if not is_valid_occ_contract(row_contract):
            if explicit_option:
                return _result(
                    "HOLD",
                    "FILLED_ENTRY_RECOVERY_BROKER_POSITION_OCC_INVALID",
                    contract=contract,
                    retryable=True,
                )
            # Non-option rows (for example equity positions) are outside
            # this exact OCC risk domain and are safely ignored.
            continue
        if row_contract == contract:
            row_qty = _broker_quantity(row)
            if row_qty <= 0:
                return _result(
                    "HOLD",
                    "FILLED_ENTRY_RECOVERY_BROKER_QUANTITY_UNPROVEN",
                    contract=contract,
                    retryable=True,
                )
            matches.append((row, row_qty))

    if not matches:
        return _result(
            "NONACTIONABLE",
            "HISTORICAL_FILLED_RECOVERY_NO_CURRENT_BROKER_POSITION",
            contract=contract,
            broker_position_count=0,
            retryable=False,
        )
    if len(matches) != 1:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_BROKER_POSITION_AMBIGUOUS",
            contract=contract,
            broker_position_count=len(matches),
            retryable=True,
        )
    broker_qty = matches[0][1]

    candidates, lookup_error = _load_local_candidates(pm, order)
    if lookup_error:
        return _result(
            "HOLD",
            lookup_error,
            contract=contract,
            broker_qty=broker_qty,
            retryable=True,
        )

    durable_position_id = str(order.get("position_id") or "").strip()
    if len(candidates) > 1:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_AMBIGUOUS",
            contract=contract,
            broker_qty=broker_qty,
            candidate_count=len(candidates),
            retryable=False,
        )

    if candidates:
        position = candidates[0]
        position_id = _position_id(position)
        position_client = str(_position_value(position, "client_id", "") or "").strip()
        position_mode = _position_value(position, "execution_mode", "")
        position_contract = _position_contract(position)
        row_local = str(_position_value(position, "local_order_id", "") or "").strip()
        row_broker = str(_position_value(position, "broker_order_id", "") or "").strip()

        if (
            not position_id
            or (durable_position_id and position_id != durable_position_id)
            or position_client != client_id
            or position_mode != execution_mode
            or position_contract != contract
            or (row_local and row_local != local_order_id)
            or (row_broker and row_broker != broker_order_id)
        ):
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_IDENTITY_CONFLICT",
                contract=contract,
                position_id=position_id,
                retryable=False,
            )
        if _position_is_terminal(position):
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_LOCAL_TERMINAL_BROKER_PRESENT_CONFLICT",
                contract=contract,
                position_id=position_id,
                broker_qty=broker_qty,
                retryable=False,
            )
        local_qty = _position_quantity(position)
        if local_qty <= 0 or local_qty != broker_qty:
            return _result(
                "HOLD",
                "FILLED_ENTRY_RECOVERY_POSITION_QUANTITY_CONFLICT",
                contract=contract,
                position_id=position_id,
                broker_qty=broker_qty,
                local_qty=local_qty,
                retryable=True,
            )
        return _result(
            "ACTIVE_EXISTING",
            "FILLED_ENTRY_RECOVERY_CURRENT_POSITION_PROVEN",
            contract=contract,
            position_id=position_id,
            existing_position=position,
            broker_qty=broker_qty,
            local_qty=local_qty,
            retryable=True,
        )

    if durable_position_id:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_LOCAL_POSITION_MISSING_DURABLE_IDENTITY",
            contract=contract,
            position_id=durable_position_id,
            broker_qty=broker_qty,
            retryable=True,
        )
    if durable_qty != broker_qty:
        return _result(
            "HOLD",
            "FILLED_ENTRY_RECOVERY_RECREATE_QUANTITY_UNPROVEN",
            contract=contract,
            broker_qty=broker_qty,
            durable_filled_qty=durable_qty,
            retryable=True,
        )
    return _result(
        "ACTIVE_RECREATE",
        "FILLED_ENTRY_RECOVERY_CURRENT_POSITION_PROVEN",
        contract=contract,
        broker_qty=broker_qty,
        durable_filled_qty=durable_qty,
        durable_fill_price=durable_price,
        retryable=True,
    )


__all__ = [
    "VALID_EXECUTION_MODES",
    "fetch_current_broker_positions",
    "evaluate_filled_entry_recovery_authority",
    "is_valid_occ_contract",
]
