from __future__ import annotations

import json, logging, os, re
from dataclasses import dataclass, field
from typing import Any, Mapping, Optional

log = logging.getLogger("ap.entry_metadata_guard")
VALID_EXECUTION_MODES = {"paper", "live"}
VALID_DIRECTIONS = {"CALL", "PUT"}
DAILY_TIMEFRAMES = {"1d", "daily", "d", "overnight"}
DEFAULT_LIVE_ALLOWED_ENTRY_TIMEFRAMES = "1d,daily,d,overnight,1w,weekly,w"
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
ENTRY_GEOMETRY_MISSING = "ENTRY_GEOMETRY_MISSING"
ENTRY_GEOMETRY_NONNUMERIC = "ENTRY_GEOMETRY_NONNUMERIC"
ENTRY_GEOMETRY_CALL_INVALID = "ENTRY_GEOMETRY_CALL_INVALID"
ENTRY_GEOMETRY_PUT_INVALID = "ENTRY_GEOMETRY_PUT_INVALID"
ENTRY_GEOMETRY_STOP_EQUALS_TRIGGER = "ENTRY_GEOMETRY_STOP_EQUALS_TRIGGER"
ENTRY_GEOMETRY_TARGET_EQUALS_TRIGGER = "ENTRY_GEOMETRY_TARGET_EQUALS_TRIGGER"
ENTRY_GEOMETRY_STOP_EQUALS_TARGET = "ENTRY_GEOMETRY_STOP_EQUALS_TARGET"
ENTRY_GEOMETRY_CONFLICT = "ENTRY_GEOMETRY_CONFLICT"
ENTRY_PATTERN_SIDE_CONFLICT = "ENTRY_PATTERN_SIDE_CONFLICT"
LIVE_FAILED_DIRECTION_PATTERN_BLOCKED = "LIVE_FAILED_DIRECTION_PATTERN_BLOCKED"
LIVE_TIMEFRAME_NOT_ALLOWED = "LIVE_TIMEFRAME_NOT_ALLOWED"
ENTRY_GEOMETRY_CHANGED_AFTER_ARM = "ENTRY_GEOMETRY_CHANGED_AFTER_ARM"

@dataclass(frozen=True)
class EntryMetadataValidationResult:
    ok: bool
    reason: Optional[str] = None
    details: dict[str, Any] = field(default_factory=dict)

EntryStrategyValidationResult = EntryMetadataValidationResult

def _txt(v: Any) -> str:
    return str(v or "").strip()

def _truthy(v: Any) -> bool:
    if isinstance(v, bool):
        return v
    return _txt(v).lower() in {"1", "true", "yes", "y", "on"}

def _meta(raw: Any) -> dict[str, Any]:
    if isinstance(raw, dict): return dict(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            obj = json.loads(raw)
            return dict(obj) if isinstance(obj, dict) else {}
        except Exception:
            return {}
    return {}

def _source_meta(src: Any) -> dict[str, Any]:
    if src is None: return {}
    if isinstance(src, Mapping): return _meta(src.get("meta") or src.get("metadata"))
    return _meta(getattr(src, "meta", None) or getattr(src, "metadata", None))

def _read(src: Any, key: str) -> tuple[bool, Any]:
    if src is None: return False, None
    if "." in key:
        head, *tail = key.split(".")
        ok, val = _read(src, head)
        if not ok: return False, None
        for part in tail:
            ok, val = _read(val, part)
            if not ok: return False, None
        return True, val
    if isinstance(src, Mapping):
        return (True, src.get(key)) if key in src else (False, None)
    return (True, getattr(src, key)) if hasattr(src, key) else (False, None)

def _sources(*, plan=None, order=None, caller_meta=None, client_id=None, execution_mode=None) -> list[Any]:
    out: list[Any] = [{"client_id": client_id, "execution_mode": execution_mode}]
    for src in (order, plan):
        if src is not None:
            out.append(src)
            m = _source_meta(src)
            if m: out.append(m)
    cm = _meta(caller_meta)
    if cm: out.append(cm)
    return out

def _first(srcs: list[Any], keys: tuple[str, ...]) -> Any:
    for src in srcs:
        for k in keys:
            ok, val = _read(src, k)
            if ok and _txt(val): return val
    return None

def _positive(srcs: list[Any], keys: tuple[str, ...]) -> tuple[bool, Any]:
    """
    Return (True, value) for the FIRST strictly-positive numeric value found
    across sources/keys.

    P0 (2026-07-02) — key-shadowing fix. The previous implementation
    returned on the first key that merely PARSED: a placeholder zero (or an
    unparseable string) in an early key like `underlying_entry` shadowed a
    valid positive value in a later key like `trigger.current_price`, and
    the whole signal was rejected `metadata_invalid:zero_*` even though
    valid data was present. This is the exact mechanism behind the
    2026-07-01 mass rejection wave (366 daily signals in one session) —
    #241/#242 patched it by adding key aliases, but the shadowing semantics
    remained and would bite again on the next producer that emits a zeroed
    placeholder field.

    New semantics (matches `_first_positive_value` introduced at the broker
    submit boundary in #251, so both guards agree):
      - skip missing/empty keys
      - skip unparseable values (keep searching)
      - skip zero/negative values (keep searching)
      - succeed on the first strictly-positive number
      - only fail after ALL sources and keys are exhausted; the returned
        value is the last non-missing candidate seen, for diagnostics.

    Fail-closed behavior is preserved: a payload with NO positive value in
    any accepted key is still rejected.
    """
    last_seen: Any = None
    for src in srcs:
        for k in keys:
            ok, val = _read(src, k)
            if not ok or val is None or val == "": continue
            last_seen = val
            try: num = float(val)
            except Exception: continue
            if num > 0:
                return True, num
    return False, last_seen

def _infer_timeframe(srcs: list[Any]) -> Any:
    """Resolve production scanner timeframe without mutating payloads.

    Scanner payloads may omit top-level `timeframe`, but can still carry
    explicit scanner-owned timeframe evidence. Inference is intentionally
    narrow: only trusted scanner fields can establish weekly/daily/overnight.
    Arbitrary free text containing "weekly" is not enough.
    """
    explicit = _first(srcs, ("timeframe", "time_horizon"))
    if _txt(explicit):
        return explicit

    source = _first(srcs, ("scanner_source", "scanner_type", "scanner_name", "source", "trigger.source"))
    signal_id = _first(srcs, ("signal_id", "canonical_signal_id"))
    expiry_hint = _first(srcs, ("expiry_hint", "trigger.expiry_hint"))
    source_txt = _txt(source).lower()
    expiry_hint_txt = _txt(expiry_hint).upper()
    signal_segments = [seg.strip().lower() for seg in _txt(signal_id).split(":") if seg.strip()]

    if (
        expiry_hint_txt == "WEEKLY"
        or "scanner_consolidation_v3_weekly" in source_txt
        or "weekly" in signal_segments
        or "1w" in signal_segments
    ):
        return "weekly"
    if (
        expiry_hint_txt == "DAILY"
        or "scanner_consolidation_v3_daily" in source_txt
        or "daily" in signal_segments
        or "1d" in signal_segments
    ):
        return "1d"
    if (
        expiry_hint_txt == "OVERNIGHT"
        or "scanner_consolidation_v3_overnight" in source_txt
        or "overnight" in signal_segments
    ):
        return "overnight"
    return None

def _mode(v: Any) -> str: return _txt(v).lower()
def _dir(v: Any) -> str: return _txt(v).upper()
def _tf(v: Any) -> str: return _txt(v).lower()

_TRIGGER_KEYS = (
    "trigger_price", "entry_trigger", "trigger.entry", "entry_price",
    "signal_entry_price",
)
_TARGET_KEYS = (
    "target_underlying", "target_price", "target", "take_profit_underlying",
    "trigger.pt1", "trigger.target",
)
_STOP_KEYS = (
    "stop_underlying", "stop_price", "stop", "stop_loss_underlying",
    "trigger.stop",
)

def _source_label(src: Any, idx: int) -> str:
    if isinstance(src, Mapping):
        if idx == 0 and set(src.keys()).issubset({"client_id", "execution_mode"}):
            return "runtime"
        if "local_order_id" in src or "status" in src or "kind" in src:
            return "order"
        if "broker_ready" in src or "materialization_status" in src:
            return "order_meta"
        return "mapping"
    return type(src).__name__

def _numeric_candidates(srcs: list[Any], keys: tuple[str, ...], field: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    candidates: list[dict[str, Any]] = []
    invalid: list[dict[str, Any]] = []
    for idx, src in enumerate(srcs):
        for key in keys:
            ok, raw = _read(src, key)
            if not ok or raw is None or raw == "":
                continue
            try:
                number = float(raw)
            except Exception:
                invalid.append({
                    "field": field,
                    "key": key,
                    "value": raw,
                    "source": _source_label(src, idx),
                })
                continue
            if number <= 0:
                invalid.append({
                    "field": field,
                    "key": key,
                    "value": raw,
                    "source": _source_label(src, idx),
                })
                continue
            candidates.append({
                "field": field,
                "key": key,
                "value": number,
                "source": _source_label(src, idx),
            })
    return candidates, invalid

def _resolve_numeric_truth(srcs: list[Any], field: str, keys: tuple[str, ...]) -> tuple[bool, float | None, str | None, dict[str, Any]]:
    candidates, invalid = _numeric_candidates(srcs, keys, field)
    if invalid:
        return False, None, ENTRY_GEOMETRY_NONNUMERIC, {
            "field": field,
            "invalid": invalid,
        }
    if not candidates:
        return False, None, ENTRY_GEOMETRY_MISSING, {"field": field}
    unique = {round(float(c["value"]), 8) for c in candidates}
    if len(unique) > 1:
        return False, None, ENTRY_GEOMETRY_CONFLICT, {
            "field": field,
            "candidates": candidates,
        }
    return True, float(candidates[0]["value"]), None, {
        "field": field,
        "value": float(candidates[0]["value"]),
        "source": candidates[0]["source"],
        "key": candidates[0]["key"],
        "candidates": candidates,
    }

def _allowed_timeframes_from_env(name: str, default: str) -> set[str]:
    raw = os.getenv(name, default)
    return {part.strip().lower() for part in str(raw or "").split(",") if part.strip()}

def _is_failed_direction_pattern(value: Any) -> bool:
    text = _txt(value).upper()
    if not text:
        return False
    return bool(re.search(r"(^|[^A-Z0-9])FAILED(?:_|\s|-)?DIR(?:ECTION)?([^A-Z0-9]|$)", text))

def _explicit_final_direction_token(srcs: list[Any], pattern: Any) -> str:
    explicit = _first(srcs, (
        "final_direction_token", "pattern_direction_token", "direction_token",
        "final_bar_type", "final_strat_token", "pattern_final_token",
    ))
    explicit_txt = _txt(explicit).upper()
    if explicit_txt in {"2U", "2D"}:
        return explicit_txt
    pattern_txt = _txt(pattern).upper()
    if not pattern_txt:
        return ""
    parts = [part for part in re.split(r"[_\-\s:]+", pattern_txt) if part]
    if parts and parts[-1] in {"2U", "2D"}:
        return parts[-1]
    return ""

def _strategy_details(
    *,
    client_id: Any,
    execution_mode: Any,
    signal_id: Any,
    canonical_signal_id: Any,
    order_id: Any,
    ticker: Any,
    side: Any,
    pattern: Any,
    timeframe: Any,
    trigger: Any = None,
    stop: Any = None,
    target: Any = None,
    source: Any = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    details = {
        "client_id": _txt(client_id),
        "execution_mode": _mode(execution_mode),
        "signal_id": _txt(signal_id),
        "canonical_signal_id": _txt(canonical_signal_id),
        "order_id": _txt(order_id),
        "ticker": _txt(ticker).upper(),
        "side": _dir(side),
        "pattern": _txt(pattern),
        "timeframe": _tf(timeframe),
        "trigger": trigger,
        "stop": stop,
        "target": target,
        "source": source,
    }
    if extra:
        details.update(extra)
    return details

def validate_entry_strategy_truth(
    *,
    plan=None,
    order=None,
    caller_meta=None,
    client_id=None,
    execution_mode=None,
) -> EntryStrategyValidationResult:
    srcs = _sources(
        plan=plan,
        order=order,
        caller_meta=caller_meta,
        client_id=client_id,
        execution_mode=execution_mode,
    )
    raw_mode = _first(srcs, ("execution_mode", "mode"))
    mode = _mode(raw_mode)
    raw_side = _first(srcs, ("side", "direction"))
    side = _dir(raw_side)
    raw_tf = _infer_timeframe(srcs)
    timeframe = _tf(raw_tf)
    pattern = _first(srcs, (
        "pattern", "pattern_id", "pattern_family", "setup_type",
        "scanner_pattern", "metadata.pattern",
    ))
    signal_id = _first(srcs, ("signal_id",))
    canonical_signal_id = _first(srcs, ("canonical_signal_id",))
    ticker = _first(srcs, ("ticker", "symbol"))
    resolved_client_id = _first(srcs, ("client_id",))
    order_id = _first(srcs, ("local_order_id", "order_id", "id"))
    source = _first(srcs, ("source", "scanner_source", "scanner_type", "trigger.source"))

    if side not in VALID_DIRECTIONS:
        return EntryStrategyValidationResult(False, INVALID_DIRECTION, _strategy_details(
            client_id=resolved_client_id, execution_mode=mode, signal_id=signal_id,
            canonical_signal_id=canonical_signal_id, order_id=order_id, ticker=ticker,
            side=side, pattern=pattern, timeframe=timeframe, source=source,
        ))

    for field, keys in (
        ("trigger", _TRIGGER_KEYS),
        ("stop", _STOP_KEYS),
        ("target", _TARGET_KEYS),
    ):
        ok, value, reason, details = _resolve_numeric_truth(srcs, field, keys)
        if not ok:
            return EntryStrategyValidationResult(False, reason, _strategy_details(
                client_id=resolved_client_id, execution_mode=mode, signal_id=signal_id,
                canonical_signal_id=canonical_signal_id, order_id=order_id,
                ticker=ticker, side=side, pattern=pattern, timeframe=timeframe,
                source=source, extra=details,
            ))
        if field == "trigger":
            trigger = value
            trigger_source = details
        elif field == "stop":
            stop = value
            stop_source = details
        else:
            target = value
            target_source = details

    details_common = _strategy_details(
        client_id=resolved_client_id,
        execution_mode=mode,
        signal_id=signal_id,
        canonical_signal_id=canonical_signal_id,
        order_id=order_id,
        ticker=ticker,
        side=side,
        pattern=pattern,
        timeframe=timeframe,
        trigger=trigger,
        stop=stop,
        target=target,
        source=source,
        extra={
            "trigger_source": trigger_source,
            "stop_source": stop_source,
            "target_source": target_source,
        },
    )

    if float(stop) == float(trigger):
        return EntryStrategyValidationResult(False, ENTRY_GEOMETRY_STOP_EQUALS_TRIGGER, details_common)
    if float(target) == float(trigger):
        return EntryStrategyValidationResult(False, ENTRY_GEOMETRY_TARGET_EQUALS_TRIGGER, details_common)
    if float(stop) == float(target):
        return EntryStrategyValidationResult(False, ENTRY_GEOMETRY_STOP_EQUALS_TARGET, details_common)
    if side == "CALL" and not (float(stop) < float(trigger) < float(target)):
        return EntryStrategyValidationResult(False, ENTRY_GEOMETRY_CALL_INVALID, details_common)
    if side == "PUT" and not (float(target) < float(trigger) < float(stop)):
        return EntryStrategyValidationResult(False, ENTRY_GEOMETRY_PUT_INVALID, details_common)

    failed_direction_values = [
        pattern,
        _first(srcs, ("pattern_family", "scanner_family", "setup_family", "scanner_source", "source")),
    ]
    failed_direction = any(_is_failed_direction_pattern(value) for value in failed_direction_values)
    paper_blocks_failed_direction = _truthy(os.getenv("PAPER_BLOCK_FAILED_DIRECTION_PATTERNS", "0"))
    if failed_direction and (mode == "live" or paper_blocks_failed_direction):
        return EntryStrategyValidationResult(False, LIVE_FAILED_DIRECTION_PATTERN_BLOCKED, details_common)

    final_token = _explicit_final_direction_token(srcs, pattern)
    if (final_token == "2U" and side != "CALL") or (final_token == "2D" and side != "PUT"):
        return EntryStrategyValidationResult(False, ENTRY_PATTERN_SIDE_CONFLICT, {
            **details_common,
            "final_direction_token": final_token,
        })

    if mode == "live":
        allowed_live_timeframes = _allowed_timeframes_from_env(
            "LIVE_ALLOWED_ENTRY_TIMEFRAMES",
            DEFAULT_LIVE_ALLOWED_ENTRY_TIMEFRAMES,
        )
        if not timeframe or timeframe not in allowed_live_timeframes:
            return EntryStrategyValidationResult(False, LIVE_TIMEFRAME_NOT_ALLOWED, details_common)

    return EntryStrategyValidationResult(True, None, details_common)

def validate_entry_metadata(*, plan=None, order=None, caller_meta=None, client_id=None, execution_mode=None) -> EntryMetadataValidationResult:
    srcs = _sources(plan=plan, order=order, caller_meta=caller_meta, client_id=client_id, execution_mode=execution_mode)
    if not _txt(_first(srcs, ("client_id",))): return EntryMetadataValidationResult(False, MISSING_CLIENT_ID)
    raw_mode = _first(srcs, ("execution_mode", "mode"))
    if _mode(raw_mode) not in VALID_EXECUTION_MODES: return EntryMetadataValidationResult(False, UNKNOWN_EXECUTION_MODE, {"execution_mode": raw_mode})
    if not _txt(_first(srcs, ("signal_id", "canonical_signal_id"))): return EntryMetadataValidationResult(False, MISSING_SIGNAL_ID)
    if not _txt(_first(srcs, ("ticker", "symbol"))): return EntryMetadataValidationResult(False, MISSING_SYMBOL)
    raw_side = _first(srcs, ("side", "direction"))
    if _dir(raw_side) not in VALID_DIRECTIONS: return EntryMetadataValidationResult(False, INVALID_DIRECTION, {"direction": raw_side})
    raw_tf = _infer_timeframe(srcs)
    if not _txt(raw_tf): return EntryMetadataValidationResult(False, MISSING_TIMEFRAME)
    ok, val = _positive(srcs, ("score", "ev_score", "scanner_score"))
    if not ok: return EntryMetadataValidationResult(False, ZERO_SCORE, {"score": val})
    ok, val = _positive(srcs, ("trigger_price", "entry_trigger", "trigger.entry", "entry_price", "signal_entry_price"))
    if not ok: return EntryMetadataValidationResult(False, ZERO_TRIGGER, {"trigger": val})
    ok, val = _positive(srcs, ("underlying_entry", "underlying_at_signal", "underlying_price", "current_underlying_price", "current_underlying", "signal_underlying_price", "price_at_signal", "trigger.current_price"))
    if not ok: return EntryMetadataValidationResult(False, ZERO_UNDERLYING, {"underlying": val})
    if _tf(raw_tf) in DAILY_TIMEFRAMES:
        ok, val = _positive(srcs, ("target_underlying", "target_price", "target", "take_profit_underlying", "trigger.pt1", "trigger.target"))
        if not ok: return EntryMetadataValidationResult(False, MISSING_TARGET, {"target": val})
        ok, val = _positive(srcs, ("stop_underlying", "stop_price", "stop", "stop_loss_underlying", "trigger.stop"))
        if not ok: return EntryMetadataValidationResult(False, MISSING_STOP, {"stop": val})
    return EntryMetadataValidationResult(True)

def _execution_mode_from(plan: Any, caller_meta: Any, requested: Any) -> Optional[str]:
    raw = _first(_sources(plan=plan, caller_meta=caller_meta, client_id="probe", execution_mode=requested), ("execution_mode", "mode"))
    return _mode(raw) if _mode(raw) in VALID_EXECUTION_MODES else None

def _is_pending_trigger_status(value: Any) -> bool:
    raw = _txt(value).upper()
    return raw == "PENDING_TRIGGER" or raw.endswith("PENDING_TRIGGER")

def _allow_deferred_overnight_watcher_create(*, plan: Any, caller_meta: Any, initial_status: Any) -> bool:
    """Allow only watcher materialization, not execution, when underlying is pending.

    Overnight reeval may arm a PENDING_TRIGGER watcher with contract_deferred=True
    so breach-time execution can select a live contract later. At this stage the
    row is not broker-executable. We allow the watcher row to exist only when the
    plan/caller metadata proves all of the following:
      - initial_status is PENDING_TRIGGER
      - contract_deferred is true
      - overnight is true
      - a positive trigger/entry level exists
      - for daily-style timeframes, positive stop and target levels exist

    validate_entry_metadata() itself still fails closed on zero underlying, and
    submit_existing_entry()/submit_entry still use that strict path before any
    broker submit.
    """
    if not _is_pending_trigger_status(initial_status):
        return False
    srcs = _sources(plan=plan, caller_meta=caller_meta, client_id="probe", execution_mode="paper")
    if not _truthy(_first(srcs, ("contract_deferred",))):
        return False
    if not _truthy(_first(srcs, ("overnight",))):
        return False
    ok, _val = _positive(srcs, ("trigger_price", "entry_trigger", "trigger.entry", "entry_price", "signal_entry_price"))
    if not ok:
        return False
    raw_tf = _infer_timeframe(srcs)
    if _tf(raw_tf) in DAILY_TIMEFRAMES:
        target_ok, _target = _positive(srcs, ("target_underlying", "target_price", "target", "take_profit_underlying", "trigger.pt1", "trigger.target"))
        stop_ok, _stop = _positive(srcs, ("stop_underlying", "stop_price", "stop", "stop_loss_underlying", "trigger.stop"))
        if not target_ok or not stop_ok:
            return False
    return True

def _mark_deferred_watcher_data_pending(plan: Any, caller_meta: Any, reason: str) -> None:
    marker = {
        "metadata_validation_status": "DATA_PENDING",
        "metadata_validation_reason": reason,
        "underlying_data_pending": True,
        "allowed_for_watcher": True,
        "allowed_for_execution": False,
        "watcher_handoff_only": True,
    }
    targets: list[Any] = []
    if isinstance(caller_meta, dict):
        targets.append(caller_meta)
    if isinstance(plan, Mapping):
        meta = plan.get("metadata")
        if isinstance(meta, dict):
            targets.append(meta)
        else:
            plan["metadata"] = dict(marker)
            targets.append(plan["metadata"])
        targets.append(plan)
    elif plan is not None:
        meta = getattr(plan, "metadata", None)
        if isinstance(meta, dict):
            targets.append(meta)
        else:
            try:
                setattr(plan, "metadata", dict(marker))
                targets.append(getattr(plan, "metadata"))
            except Exception:
                pass
        targets.append(plan)
    for target in targets:
        try:
            if isinstance(target, dict):
                target.update(marker)
            else:
                for key, value in marker.items():
                    setattr(target, key, value)
        except Exception:
            pass

def _terminalize_strategy_rejection(osm: Any, local_order_id: str, result: EntryStrategyValidationResult) -> bool:
    reason = str(result.reason or "ENTRY_STRATEGY_TRUTH_INVALID")
    diagnostics = {
        "entry_strategy_truth": {
            "ok": False,
            "reason_code": reason,
            **(result.details or {}),
        },
        "metadata_validation_status": "BLOCKED",
        "metadata_validation_reason": reason,
    }
    terminalize = getattr(osm, "terminalize_deferred_breach", None)
    if callable(terminalize):
        try:
            if terminalize(
                local_order_id,
                reason_code=reason,
                terminal_status="ERROR",
                diagnostics=diagnostics,
            ):
                return True
        except Exception:
            pass
    transition = getattr(osm, "transition", None)
    if callable(transition):
        try:
            return bool(transition(local_order_id, "ERROR", last_error=reason))
        except Exception:
            return False
    return False

def install_entry_metadata_guard() -> None:
    from ap.order_state_machine import APOrderStateMachine, OrderStatus
    if getattr(APOrderStateMachine, "_entry_metadata_guard_installed", False): return
    original_create = APOrderStateMachine.create_entry_order
    original_submit_existing = APOrderStateMachine.submit_existing_entry
    original_submit_entry = APOrderStateMachine.submit_entry

    def guarded_create_entry_order(self, plan, *args, **kwargs):
        caller_meta = kwargs.get("meta")
        requested_mode = kwargs.get("execution_mode")
        result = validate_entry_metadata(plan=plan, caller_meta=caller_meta, client_id=getattr(self, "client_id", None), execution_mode=requested_mode)
        if not result.ok:
            if result.reason == ZERO_UNDERLYING and _allow_deferred_overnight_watcher_create(
                plan=plan,
                caller_meta=caller_meta,
                initial_status=kwargs.get("initial_status"),
            ):
                _mark_deferred_watcher_data_pending(plan, caller_meta, result.reason)
                log.warning(
                    "[%s] ENTRY_METADATA_DATA_PENDING before create_entry_order | reason=%s "
                    "action=allow_deferred_overnight_watcher_create allowed_for_execution=false",
                    getattr(self, "client_id", "?"),
                    result.reason,
                )
            else:
                log.warning("[%s] ENTRY_METADATA_BLOCKED before create_entry_order | reason=%s details=%s", getattr(self, "client_id", "?"), result.reason, result.details)
                raise ValueError(result.reason)
        strategy_result = validate_entry_strategy_truth(
            plan=plan,
            caller_meta=caller_meta,
            client_id=getattr(self, "client_id", None),
            execution_mode=requested_mode,
        )
        if not strategy_result.ok:
            log.warning(
                "[%s] ENTRY_STRATEGY_TRUTH_BLOCKED before create_entry_order | reason=%s details=%s",
                getattr(self, "client_id", "?"),
                strategy_result.reason,
                strategy_result.details,
            )
            raise ValueError(strategy_result.reason)
        if not requested_mode:
            resolved = _execution_mode_from(plan, caller_meta, requested_mode)
            if resolved: kwargs["execution_mode"] = resolved
        return original_create(self, plan, *args, **kwargs)

    def guarded_submit_existing_entry(self, *, local_order_id: str, broker, plan=None, limit_price=None):
        current = self._get_order(local_order_id)
        if current:
            current = dict(current)
            result = validate_entry_metadata(order=current, plan=plan, client_id=getattr(self, "client_id", None), execution_mode=current.get("execution_mode"))
            if not result.ok:
                log.warning("[%s] ENTRY_METADATA_BLOCKED before broker submit | local=%s reason=%s details=%s", getattr(self, "client_id", "?"), local_order_id, result.reason, result.details)
                return {"ok": False, "local_order_id": local_order_id, "broker_order_id": current.get("broker_order_id"), "status": str(current.get("status") or OrderStatus.ERROR), "error": result.reason, "metadata_blocked": True, "details": result.details}
            strategy_result = validate_entry_strategy_truth(
                order=current,
                plan=plan,
                client_id=getattr(self, "client_id", None),
                execution_mode=current.get("execution_mode"),
            )
            if not strategy_result.ok:
                _terminalized = _terminalize_strategy_rejection(self, local_order_id, strategy_result)
                log.warning(
                    "[%s] ENTRY_STRATEGY_TRUTH_BLOCKED before broker submit | local=%s reason=%s terminalized=%s details=%s",
                    getattr(self, "client_id", "?"),
                    local_order_id,
                    strategy_result.reason,
                    _terminalized,
                    strategy_result.details,
                )
                return {
                    "ok": False,
                    "local_order_id": local_order_id,
                    "broker_order_id": current.get("broker_order_id"),
                    "status": OrderStatus.ERROR,
                    "error": strategy_result.reason,
                    "metadata_blocked": True,
                    "strategy_truth_blocked": True,
                    "terminalized": _terminalized,
                    "details": strategy_result.details,
                }
        return original_submit_existing(self, local_order_id=local_order_id, broker=broker, plan=plan, limit_price=limit_price)

    def guarded_submit_entry(self, *, broker, plan, limit_price=None, reserved_cost=None):
        result = validate_entry_metadata(plan=plan, client_id=getattr(self, "client_id", None), execution_mode=getattr(plan, "execution_mode", None) or getattr(plan, "mode", None))
        if not result.ok:
            log.warning("[%s] ENTRY_METADATA_BLOCKED before direct submit_entry | reason=%s details=%s", getattr(self, "client_id", "?"), result.reason, result.details)
            return {"ok": False, "local_order_id": None, "broker_order_id": None, "status": OrderStatus.ERROR, "error": result.reason, "metadata_blocked": True, "details": result.details}
        strategy_result = validate_entry_strategy_truth(
            plan=plan,
            client_id=getattr(self, "client_id", None),
            execution_mode=getattr(plan, "execution_mode", None) or getattr(plan, "mode", None),
        )
        if not strategy_result.ok:
            log.warning("[%s] ENTRY_STRATEGY_TRUTH_BLOCKED before direct submit_entry | reason=%s details=%s", getattr(self, "client_id", "?"), strategy_result.reason, strategy_result.details)
            return {"ok": False, "local_order_id": None, "broker_order_id": None, "status": OrderStatus.ERROR, "error": strategy_result.reason, "metadata_blocked": True, "strategy_truth_blocked": True, "details": strategy_result.details}
        return original_submit_entry(self, broker=broker, plan=plan, limit_price=limit_price, reserved_cost=reserved_cost)

    APOrderStateMachine._entry_metadata_guard_original_create = original_create
    APOrderStateMachine._entry_metadata_guard_original_submit_existing = original_submit_existing
    APOrderStateMachine._entry_metadata_guard_original_submit_entry = original_submit_entry
    APOrderStateMachine.create_entry_order = guarded_create_entry_order
    APOrderStateMachine.submit_existing_entry = guarded_submit_existing_entry
    APOrderStateMachine.submit_entry = guarded_submit_entry
    APOrderStateMachine._entry_metadata_guard_installed = True
