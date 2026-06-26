from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

log = logging.getLogger("ap.entry_metadata_guard")

VALID_EXECUTION_MODES = {"paper", "live"}
VALID_DIRECTIONS = {"CALL", "PUT"}
DAILY_TIMEFRAMES = {"1d", "daily", "d", "overnight"}

MISSING_CLIENT_ID = "metadata_invalid:missing_client_id"
UNKNOWN_EXECUTION_MODE = "metadata_invalid:unknown_execution_mode"
MISSING_SIGNAL_ID = "metadata_invalid:missing_signal_id"
MISSING_SYMBOL = "metadata_invalid:missing_symbol"
INVALID_DIRECTION = "metadata_invalid:invalid_direction"
MISSING_TIMEFRAME = "metadata_invalid:missing_timeframe"
MISSING_PATTERN = "metadata_invalid:missing_pattern"
ZERO_SCORE = "metadata_invalid:zero_score"
ZERO_TRIGGER = "metadata_invalid:zero_trigger"
ZERO_UNDERLYING = "metadata_invalid:zero_underlying"
MISSING_TARGET = "metadata_invalid:missing_target"
MISSING_STOP = "metadata_invalid:missing_stop"


@dataclass(frozen=True)
class EntryMetadataValidationResult:
    ok: bool
    reason: Optional[str] = None
    details: dict[str, Any] = field(default_factory=dict)


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _parse_meta(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            decoded = json.loads(raw)
            return dict(decoded) if isinstance(decoded, dict) else {}
        except Exception:
            return {}
    return {}


def _source_meta(source: Any) -> dict[str, Any]:
    if source is None:
        return {}
    if isinstance(source, Mapping):
        return _parse_meta(source.get("meta") or source.get("metadata"))
    return _parse_meta(getattr(source, "meta", None) or getattr(source, "metadata", None))


def _read_direct(source: Any, key: str) -> tuple[bool, Any]:
    if source is None:
        return False, None
    if "." in key:
        head, *tail = key.split(".")
        present, value = _read_direct(source, head)
        if not present:
            return False, None
        for part in tail:
            present, value = _read_direct(value, part)
            if not present:
                return False, None
        return True, value
    if isinstance(source, Mapping):
        if key in source:
            return True, source.get(key)
        return False, None
    if hasattr(source, key):
        return True, getattr(source, key)
    return False, None


def _sources(*, plan: Any = None, order: Any = None, caller_meta: Any = None, client_id: Any = None, execution_mode: Any = None) -> list[Any]:
    result: list[Any] = []
    explicit = {"client_id": client_id, "execution_mode": execution_mode}
    result.append(explicit)
    if order is not None:
        result.append(order)
        meta = _source_meta(order)
        if meta:
            result.append(meta)
    if plan is not None:
        result.append(plan)
        meta = _source_meta(plan)
        if meta:
            result.append(meta)
    caller = _parse_meta(caller_meta)
    if caller:
        result.append(caller)
    return result


def _first_non_blank(sources: list[Any], keys: tuple[str, ...]) -> Any:
    for source in sources:
        for key in keys:
            present, value = _read_direct(source, key)
            if present and _clean_text(value):
                return value
    return None


def _first_present_positive(sources: list[Any], keys: tuple[str, ...]) -> tuple[bool, Any]:
    """Return the first present numeric field and whether it is positive.

    A present zero must block instead of falling back to a later duplicate field.
    That is the production-shape invariant: if the top-level order row has
    score=0 or trigger_price=0, downstream diagnostics will read the zero.
    """
    for source in sources:
        for key in keys:
            present, value = _read_direct(source, key)
            if not present or value is None or value == "":
                continue
            try:
                num = float(value)
            except Exception:
                return False, value
            return num > 0, num
    return False, None


def _normalize_execution_mode(value: Any) -> str:
    return _clean_text(value).lower()


def _normalize_direction(value: Any) -> str:
    return _clean_text(value).upper()


def _normalize_timeframe(value: Any) -> str:
    return _clean_text(value).lower()


def _is_daily_timeframe(value: Any) -> bool:
    return _normalize_timeframe(value) in DAILY_TIMEFRAMES


def validate_entry_metadata(
    *,
    plan: Any = None,
    order: Any = None,
    caller_meta: Any = None,
    client_id: Any = None,
    execution_mode: Any = None,
) -> EntryMetadataValidationResult:
    """Validate that an ENTRY carries production-shaped thesis metadata.

    This is intentionally read-only. It does not repair, default, mutate, submit,
    cancel, or write. Callers decide how to reject/skip after receiving a result.
    """
    src = _sources(
        plan=plan,
        order=order,
        caller_meta=caller_meta,
        client_id=client_id,
        execution_mode=execution_mode,
    )

    resolved_client_id = _first_non_blank(src, ("client_id",))
    if not _clean_text(resolved_client_id):
        return EntryMetadataValidationResult(False, MISSING_CLIENT_ID)

    resolved_execution_mode = _first_non_blank(src, ("execution_mode",))
    normalized_mode = _normalize_execution_mode(resolved_execution_mode)
    if normalized_mode not in VALID_EXECUTION_MODES:
        return EntryMetadataValidationResult(
            False,
            UNKNOWN_EXECUTION_MODE,
            {"execution_mode": resolved_execution_mode},
        )

    resolved_signal_id = _first_non_blank(src, ("signal_id", "canonical_signal_id"))
    if not _clean_text(resolved_signal_id):
        return EntryMetadataValidationResult(False, MISSING_SIGNAL_ID)

    resolved_symbol = _first_non_blank(src, ("ticker", "symbol"))
    if not _clean_text(resolved_symbol):
        return EntryMetadataValidationResult(False, MISSING_SYMBOL)

    resolved_direction = _first_non_blank(src, ("side", "direction"))
    normalized_direction = _normalize_direction(resolved_direction)
    if normalized_direction not in VALID_DIRECTIONS:
        return EntryMetadataValidationResult(
            False,
            INVALID_DIRECTION,
            {"direction": resolved_direction},
        )

    resolved_timeframe = _first_non_blank(src, ("timeframe",))
    if not _clean_text(resolved_timeframe):
        return EntryMetadataValidationResult(False, MISSING_TIMEFRAME)

    resolved_pattern = _first_non_blank(src, ("pattern", "pattern_id"))
    if not _clean_text(resolved_pattern):
        return EntryMetadataValidationResult(False, MISSING_PATTERN)

    score_ok, score = _first_present_positive(src, ("score", "ev_score", "scanner_score"))
    if not score_ok:
        return EntryMetadataValidationResult(False, ZERO_SCORE, {"score": score})

    trigger_ok, trigger = _first_present_positive(
        src,
        (
            "trigger_price",
            "entry_trigger",
            "trigger.entry",
            "entry_price",
            "signal_entry_price",
        ),
    )
    if not trigger_ok:
        return EntryMetadataValidationResult(False, ZERO_TRIGGER, {"trigger": trigger})

    underlying_ok, underlying = _first_present_positive(
        src,
        (
            "underlying_entry",
            "underlying_at_signal",
            "underlying_price",
            "current_underlying_price",
            "current_underlying",
            "signal_underlying_price",
            "price_at_signal",
        ),
    )
    if not underlying_ok:
        return EntryMetadataValidationResult(False, ZERO_UNDERLYING, {"underlying": underlying})

    if _is_daily_timeframe(resolved_timeframe):
        target_ok, target = _first_present_positive(
            src,
            (
                "target_underlying",
                "target_price",
                "target",
                "take_profit_underlying",
                "trigger.pt1",
                "trigger.target",
            ),
        )
        if not target_ok:
            return EntryMetadataValidationResult(False, MISSING_TARGET, {"target": target})

        stop_ok, stop = _first_present_positive(
            src,
            (
                "stop_underlying",
                "stop_price",
                "stop",
                "stop_loss_underlying",
                "trigger.stop",
            ),
        )
        if not stop_ok:
            return EntryMetadataValidationResult(False, MISSING_STOP, {"stop": stop})

    return EntryMetadataValidationResult(True)


def _execution_mode_from(plan: Any, caller_meta: Any, requested: Any) -> Optional[str]:
    src = _sources(plan=plan, caller_meta=caller_meta, execution_mode=requested, client_id="metadata-probe")
    mode = _first_non_blank(src, ("execution_mode",))
    normalized = _normalize_execution_mode(mode)
    return normalized if normalized in VALID_EXECUTION_MODES else None


def install_entry_metadata_guard() -> None:
    """Install ENTRY metadata fail-closed wrappers on APOrderStateMachine.

    The wrappers run before order creation and before broker submit. They do not
    touch exits, broker cancel, positions, fills, or proof-trade writes.
    """
    from ap.order_state_machine import APOrderStateMachine, OrderStatus

    if getattr(APOrderStateMachine, "_entry_metadata_guard_installed", False):
        return

    original_create = APOrderStateMachine.create_entry_order
    original_submit_existing = APOrderStateMachine.submit_existing_entry
    original_submit_entry = APOrderStateMachine.submit_entry

    def guarded_create_entry_order(self, plan, *args, **kwargs):
        caller_meta = kwargs.get("meta")
        requested_mode = kwargs.get("execution_mode")
        result = validate_entry_metadata(
            plan=plan,
            caller_meta=caller_meta,
            client_id=getattr(self, "client_id", None),
            execution_mode=requested_mode,
        )
        if not result.ok:
            log.warning(
                "[%s] ENTRY_METADATA_BLOCKED before create_entry_order | reason=%s details=%s",
                getattr(self, "client_id", "?"),
                result.reason,
                result.details,
            )
            raise ValueError(result.reason)

        if not requested_mode:
            # Preserve a valid production-supplied mode if it was carried on the
            # plan/metadata instead of the kwarg. This is not a default; invalid
            # or missing mode already blocked above.
            resolved_mode = _execution_mode_from(plan, caller_meta, requested_mode)
            if resolved_mode:
                kwargs["execution_mode"] = resolved_mode

        return original_create(self, plan, *args, **kwargs)

    def guarded_submit_existing_entry(self, *, local_order_id: str, broker, plan=None, limit_price=None):
        current = self._get_order(local_order_id)
        if current:
            current = dict(current)
            result = validate_entry_metadata(
                order=current,
                plan=plan,
                client_id=getattr(self, "client_id", None),
                execution_mode=current.get("execution_mode"),
            )
            if not result.ok:
                log.warning(
                    "[%s] ENTRY_METADATA_BLOCKED before broker submit | local=%s reason=%s details=%s",
                    getattr(self, "client_id", "?"),
                    local_order_id,
                    result.reason,
                    result.details,
                )
                return {
                    "ok": False,
                    "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": str(current.get("status") or OrderStatus.ERROR),
                    "error": result.reason,
                    "metadata_blocked": True,
                    "details": result.details,
                }

        return original_submit_existing(
            self,
            local_order_id=local_order_id,
            broker=broker,
            plan=plan,
            limit_price=limit_price,
        )

    def guarded_submit_entry(self, *, broker, plan, limit_price=None, reserved_cost=None):
        result = validate_entry_metadata(
            plan=plan,
            client_id=getattr(self, "client_id", None),
            execution_mode=getattr(plan, "execution_mode", None),
        )
        if not result.ok:
            log.warning(
                "[%s] ENTRY_METADATA_BLOCKED before direct submit_entry | reason=%s details=%s",
                getattr(self, "client_id", "?"),
                result.reason,
                result.details,
            )
            return {
                "ok": False,
                "local_order_id": None,
                "broker_order_id": None,
                "status": OrderStatus.ERROR,
                "error": result.reason,
                "metadata_blocked": True,
                "details": result.details,
            }

        return original_submit_entry(
            self,
            broker=broker,
            plan=plan,
            limit_price=limit_price,
            reserved_cost=reserved_cost,
        )

    APOrderStateMachine._entry_metadata_guard_original_create = original_create
    APOrderStateMachine._entry_metadata_guard_original_submit_existing = original_submit_existing
    APOrderStateMachine._entry_metadata_guard_original_submit_entry = original_submit_entry
    APOrderStateMachine.create_entry_order = guarded_create_entry_order
    APOrderStateMachine.submit_existing_entry = guarded_submit_existing_entry
    APOrderStateMachine.submit_entry = guarded_submit_entry
    APOrderStateMachine._entry_metadata_guard_installed = True
