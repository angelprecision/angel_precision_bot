from __future__ import annotations

import json, logging
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
    for src in srcs:
        for k in keys:
            ok, val = _read(src, k)
            if not ok or val is None or val == "": continue
            try: num = float(val)
            except Exception: return False, val
            return num > 0, num
    return False, None

def _infer_timeframe(srcs: list[Any]) -> Any:
    """Resolve production scanner timeframe without mutating payloads.

    Legacy scanner_consolidation_v3_weekly payloads have no top-level
    `timeframe`, but they carry the timeframe in both the deterministic
    signal_id (`...:Weekly:...`) and trigger.source
    (`scanner_consolidation_v3_weekly`). Treat those as real metadata rather
    than inventing a default. If no explicit clue exists, fail closed with
    MISSING_TIMEFRAME as before.
    """
    explicit = _first(srcs, ("timeframe", "time_horizon"))
    if _txt(explicit):
        return explicit

    source = _first(srcs, ("scanner_source", "scanner_type", "scanner_name", "source", "trigger.source"))
    signal_id = _first(srcs, ("signal_id", "canonical_signal_id"))
    expiry_hint = _first(srcs, ("expiry_hint", "trigger.expiry_hint"))
    haystack = " ".join(_txt(v).lower() for v in (source, signal_id, expiry_hint) if _txt(v))

    if "weekly" in haystack or ":1w:" in haystack or haystack.endswith(":1w"):
        return "weekly"
    if "daily" in haystack or ":1d:" in haystack or haystack.endswith(":1d"):
        return "1d"
    if "overnight" in haystack:
        return "overnight"
    return None

def _mode(v: Any) -> str: return _txt(v).lower()
def _dir(v: Any) -> str: return _txt(v).upper()
def _tf(v: Any) -> str: return _txt(v).lower()

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
        return original_submit_existing(self, local_order_id=local_order_id, broker=broker, plan=plan, limit_price=limit_price)

    def guarded_submit_entry(self, *, broker, plan, limit_price=None, reserved_cost=None):
        result = validate_entry_metadata(plan=plan, client_id=getattr(self, "client_id", None), execution_mode=getattr(plan, "execution_mode", None) or getattr(plan, "mode", None))
        if not result.ok:
            log.warning("[%s] ENTRY_METADATA_BLOCKED before direct submit_entry | reason=%s details=%s", getattr(self, "client_id", "?"), result.reason, result.details)
            return {"ok": False, "local_order_id": None, "broker_order_id": None, "status": OrderStatus.ERROR, "error": result.reason, "metadata_blocked": True, "details": result.details}
        return original_submit_entry(self, broker=broker, plan=plan, limit_price=limit_price, reserved_cost=reserved_cost)

    APOrderStateMachine._entry_metadata_guard_original_create = original_create
    APOrderStateMachine._entry_metadata_guard_original_submit_existing = original_submit_existing
    APOrderStateMachine._entry_metadata_guard_original_submit_entry = original_submit_entry
    APOrderStateMachine.create_entry_order = guarded_create_entry_order
    APOrderStateMachine.submit_existing_entry = guarded_submit_existing_entry
    APOrderStateMachine.submit_entry = guarded_submit_entry
    APOrderStateMachine._entry_metadata_guard_installed = True