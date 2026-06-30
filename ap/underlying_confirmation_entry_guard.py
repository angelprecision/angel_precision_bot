from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping, Optional

log = logging.getLogger("ap.underlying_confirmation_entry_guard")

STAGE = "underlying_confirmation_missing"
NO_UNDERLYING_DATA = f"{STAGE}:no_underlying_data"
STALE_UNDERLYING_QUOTE = f"{STAGE}:stale_underlying_quote"
ZERO_UNDERLYING = f"{STAGE}:zero_underlying"
CANNOT_EVALUATE_DIRECTION = f"{STAGE}:cannot_evaluate_direction"

_REASON_CODES = {
    NO_UNDERLYING_DATA: "UNDERLYING_CONFIRMATION_MISSING_NO_UNDERLYING_DATA",
    STALE_UNDERLYING_QUOTE: "UNDERLYING_CONFIRMATION_MISSING_STALE_UNDERLYING_QUOTE",
    ZERO_UNDERLYING: "UNDERLYING_CONFIRMATION_MISSING_ZERO_UNDERLYING",
    CANNOT_EVALUATE_DIRECTION: "UNDERLYING_CONFIRMATION_MISSING_CANNOT_EVALUATE_DIRECTION",
}
DAILY_TIMEFRAMES = {"1d", "d", "daily", "1day", "1 day", "overnight"}
PRICE_KEYS = ("last", "last_price", "price", "mark", "mid", "close", "current_underlying", "underlying_price")
TS_KEYS = ("timestamp", "ts", "time", "datetime", "updated_at", "fetched_at", "quote_ts", "quote_timestamp", "asof")


@dataclass(frozen=True)
class UnderlyingConfirmationResult:
    passed: bool
    reason: Optional[str] = None
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def reason_detail(self) -> str:
        return self.reason.split(":", 1)[1] if self.reason and ":" in self.reason else str(self.reason or "")

    @property
    def reason_code(self) -> str:
        return _REASON_CODES.get(str(self.reason or ""), "UNDERLYING_CONFIRMATION_MISSING")


class UnderlyingConfirmationUnavailable(Exception):
    def __init__(self, result: UnderlyingConfirmationResult):
        super().__init__(result.reason or STAGE)
        self.result = result
        self.reason = result.reason or STAGE
        self.reason_detail = result.reason_detail
        self.reason_code = result.reason_code
        self.metadata = result.metadata


def _truthy_env(key: str, default: bool) -> bool:
    raw = os.getenv(key)
    return default if raw is None else str(raw).strip().lower() in {"1", "true", "yes", "on"}


def _float_env(key: str, default: float) -> float:
    try:
        return float(os.getenv(key, str(default)))
    except Exception:
        return default


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _safe_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


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


def _read(source: Any, key: str) -> tuple[bool, Any]:
    if source is None:
        return False, None
    if "." in key:
        head, *tail = key.split(".")
        ok, value = _read(source, head)
        if not ok:
            return False, None
        for part in tail:
            ok, value = _read(value, part)
            if not ok:
                return False, None
        return True, value
    if isinstance(source, Mapping):
        return (key in source), source.get(key)
    if hasattr(source, key):
        return True, getattr(source, key)
    return False, None


def _meta(source: Any) -> dict[str, Any]:
    if isinstance(source, Mapping):
        return _parse_meta(source.get("metadata") or source.get("meta"))
    return _parse_meta(getattr(source, "metadata", None) or getattr(source, "meta", None))


def _sources(*items: Any) -> list[Any]:
    out: list[Any] = []
    for item in items:
        if item is None:
            continue
        out.append(item)
        meta = _meta(item)
        if meta:
            out.append(meta)
        if isinstance(item, Mapping):
            raw = item.get("raw_payload") or item.get("signal_payload") or item.get("payload")
            if isinstance(raw, Mapping):
                out.append(raw)
                raw_meta = _meta(raw)
                if raw_meta:
                    out.append(raw_meta)
    return out


def _first_text(sources: list[Any], *keys: str) -> Any:
    for source in sources:
        for key in keys:
            ok, value = _read(source, key)
            if ok and _clean(value):
                return value
    return None


def _first_float(sources: list[Any], *keys: str) -> Optional[float]:
    for source in sources:
        for key in keys:
            ok, value = _read(source, key)
            parsed = _safe_float(value) if ok else None
            if parsed is not None:
                return parsed
    return None


def requires_underlying_confirmation(*, plan: Any = None, payload: Any = None, order: Any = None) -> bool:
    if not _truthy_env("UNDERLYING_CONFIRMATION_ENTRY_GUARD_ENABLED", True):
        return False
    sources = _sources(plan, payload, order)
    explicit = _first_text(
        sources,
        "underlying_confirmation_required",
        "requires_underlying_confirmation",
        "hybrid_client_quality_gate.confirmation_required",
    )
    if explicit is not None:
        return str(explicit).strip().lower() in {"1", "true", "yes", "on"}
    timeframe = _clean(_first_text(sources, "timeframe")).lower()
    return timeframe in DAILY_TIMEFRAMES or bool(
        _first_text(sources, "overnight", "contract_deferred", "force_overnight_reeval_only")
    )


def _parse_ts(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    if isinstance(value, (int, float)):
        raw = float(value) / 1000.0 if float(value) > 1_000_000_000_000 else float(value)
        try:
            return datetime.fromtimestamp(raw, tz=timezone.utc)
        except Exception:
            return None
    try:
        dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt.astimezone(timezone.utc)
    except Exception:
        return None


def _quote_dicts(raw: Any) -> list[Any]:
    if raw is None:
        return []
    out = [raw]
    if isinstance(raw, Mapping):
        for key in ("quote", "underlying", "data"):
            if isinstance(raw.get(key), Mapping):
                out.append(raw[key])
        quotes = raw.get("quotes")
        if isinstance(quotes, Mapping):
            quote = quotes.get("quote")
            out.extend(quote if isinstance(quote, list) else [quote] if isinstance(quote, Mapping) else [])
    return out


def _quote_price(raw: Any) -> Optional[float]:
    scalar = _safe_float(raw)
    if scalar is not None:
        return scalar
    for quote in _quote_dicts(raw):
        for key in PRICE_KEYS:
            ok, value = _read(quote, key)
            parsed = _safe_float(value) if ok else None
            if parsed is not None:
                return parsed
    return None


def _quote_ts(raw: Any) -> Optional[datetime]:
    for quote in _quote_dicts(raw):
        for key in TS_KEYS:
            ok, value = _read(quote, key)
            parsed = _parse_ts(value) if ok else None
            if parsed is not None:
                return parsed
    return None


def _broker_quote(ticker: str, broker: Any) -> tuple[Any, str]:
    for source in (getattr(broker, "data_broker", None), getattr(broker, "market_data_broker", None), broker):
        if source is None:
            continue
        for name in ("get_underlying_quote", "get_stock_quote", "get_equity_quote", "get_quote", "quote", "fetch_quote"):
            method = getattr(source, name, None)
            if not callable(method):
                continue
            try:
                quote = method(ticker)
            except TypeError:
                continue
            except Exception as exc:
                log.debug("underlying quote fetch failed via %s: %s", name, exc)
                continue
            if quote:
                return quote, f"{type(source).__name__}.{name}"
    return None, ""


def _metadata_quote(sources: list[Any]) -> tuple[Any, str]:
    for source in sources:
        for key in ("underlying_quote", "last_underlying_quote", "current_underlying_quote", "quote_snapshot.underlying"):
            ok, value = _read(source, key)
            if ok and value:
                return value, f"metadata.{key}"
    price = _first_float(sources, "current_underlying", "current_underlying_price", "underlying_last", "underlying_price")
    ts = _first_text(sources, "underlying_quote_ts", "underlying_quote_timestamp", "quote_ts")
    if price is not None:
        return {"price": price, "timestamp": ts}, "metadata.current_underlying"
    return None, ""


def _block(reason: str, metadata: dict[str, Any]) -> UnderlyingConfirmationResult:
    payload = {
        **metadata,
        "stage": STAGE,
        "reason": reason.split(":", 1)[1],
        "reason_full": reason,
        "reason_code": _REASON_CODES[reason],
        "passed": False,
    }
    return UnderlyingConfirmationResult(False, reason, payload)


def check_underlying_confirmation(
    *,
    plan: Any = None,
    payload: Any = None,
    order: Any = None,
    broker: Any = None,
    client_id: str = "",
    execution_mode: str = "",
    now: datetime | None = None,
    max_age_sec: float | None = None,
) -> UnderlyingConfirmationResult:
    sources = _sources({"client_id": client_id, "execution_mode": execution_mode}, plan, payload, order)
    required = requires_underlying_confirmation(plan=plan, payload=payload, order=order)
    ticker = _clean(_first_text(sources, "ticker", "symbol"))
    side = _clean(_first_text(sources, "side", "direction", "option_type")).upper()
    timeframe = _clean(_first_text(sources, "timeframe"))
    trigger = _first_float(sources, "trigger_price", "entry_trigger", "trigger.entry", "entry_price", "signal_entry_price")
    stop = _first_float(sources, "stop_underlying", "stop_price", "stop", "trigger.stop")
    target = _first_float(sources, "target_underlying", "target_price", "target", "trigger.pt1", "trigger.target")
    age_limit = float(max_age_sec if max_age_sec is not None else _float_env("UNDERLYING_CONFIRMATION_MAX_AGE_SEC", 30.0))
    clock = now or datetime.now(timezone.utc)
    clock = clock.replace(tzinfo=timezone.utc) if clock.tzinfo is None else clock.astimezone(timezone.utc)
    base = {
        "underlying_confirmation_required": required,
        "client_id": _clean(client_id or _first_text(sources, "client_id", "client_email")),
        "execution_mode": _clean(execution_mode or _first_text(sources, "execution_mode", "mode")).lower(),
        "ticker": ticker,
        "side": side,
        "timeframe": timeframe,
        "trigger_price": trigger,
        "stop_underlying": stop,
        "target_underlying": target,
        "max_quote_age_seconds": age_limit,
    }
    if not required:
        return UnderlyingConfirmationResult(True, None, {**base, "passed": True, "skipped": True})
    if not ticker:
        return _block(NO_UNDERLYING_DATA, base)
    quote, source = _broker_quote(ticker, broker)
    if not quote:
        quote, source = _metadata_quote(sources)
    if not quote:
        return _block(NO_UNDERLYING_DATA, {**base, "quote_source": None})
    current = _quote_price(quote)
    quote_ts = _quote_ts(quote)
    quote_age = max(0.0, (clock - quote_ts).total_seconds()) if quote_ts else None
    with_quote = {
        **base,
        "quote_source": source,
        "current_underlying": current,
        "quote_timestamp": quote_ts.isoformat() if quote_ts else None,
        "quote_age_seconds": round(quote_age, 3) if quote_age is not None else None,
    }
    if current is None:
        return _block(NO_UNDERLYING_DATA, with_quote)
    if current <= 0:
        return _block(ZERO_UNDERLYING, with_quote)
    if quote_ts is None or quote_age is None or quote_age > age_limit:
        return _block(STALE_UNDERLYING_QUOTE, with_quote)
    if side not in {"CALL", "PUT"} or trigger is None or trigger <= 0 or stop is None or stop <= 0 or target is None or target <= 0:
        return _block(CANNOT_EVALUATE_DIRECTION, with_quote)
    if (side == "CALL" and not (target > trigger > stop)) or (side == "PUT" and not (target < trigger < stop)):
        return _block(CANNOT_EVALUATE_DIRECTION, with_quote)
    confirmed = current >= trigger if side == "CALL" else current <= trigger
    return UnderlyingConfirmationResult(
        True,
        None,
        {**with_quote, "passed": True, "directional_confirmation_evaluable": True, "directional_confirmed": bool(confirmed)},
    )


def require_underlying_confirmation_available(**kwargs: Any) -> UnderlyingConfirmationResult:
    result = check_underlying_confirmation(**kwargs)
    if not result.passed:
        raise UnderlyingConfirmationUnavailable(result)
    return result


def install_underlying_confirmation_entry_guard() -> None:
    from ap.order_state_machine import APOrderStateMachine, OrderStatus

    if getattr(APOrderStateMachine, "_underlying_confirmation_guard_installed", False):
        return
    original_submit = APOrderStateMachine.submit_entry
    original_submit_existing = APOrderStateMachine.submit_existing_entry

    def mode_for(self, broker: Any, plan: Any = None, order: Any = None) -> str:
        mode = _clean(_first_text(_sources(plan, order), "execution_mode", "mode")).lower()
        if mode in {"paper", "live"}:
            return mode
        try:
            from ap.authorization import execution_mode_for_broker

            return _clean(execution_mode_for_broker(broker)).lower()
        except Exception:
            return mode

    def guarded_submit(self, *args, **kwargs):
        broker = kwargs.get("broker")
        plan = kwargs.get("plan")
        execution_mode = mode_for(self, broker, plan=plan)
        try:
            require_underlying_confirmation_available(
                plan=plan,
                broker=broker,
                client_id=getattr(self, "client_id", ""),
                execution_mode=execution_mode,
            )
        except UnderlyingConfirmationUnavailable as exc:
            return {
                "ok": False,
                "local_order_id": None,
                "broker_order_id": None,
                "status": OrderStatus.ERROR,
                "error": exc.reason,
                "stage": STAGE,
                "reason_code": exc.reason_code,
                "underlying_confirmation": exc.metadata,
                "order_mutated": False,
            }
        return original_submit(self, *args, **kwargs)

    def guarded_submit_existing(self, *args, **kwargs):
        local_order_id = kwargs.get("local_order_id")
        broker = kwargs.get("broker")
        plan = kwargs.get("plan")
        current = None
        if local_order_id:
            try:
                current = self._get_order(local_order_id)
            except Exception:
                current = None
        order = dict(current or {})
        status = _clean(order.get("status")).upper()
        if current and _clean(order.get("kind")).upper() == "ENTRY" and status in (OrderStatus.CREATED, OrderStatus.PENDING_TRIGGER):
            execution_mode = mode_for(self, broker, plan=plan, order=order)
            try:
                require_underlying_confirmation_available(
                    plan=plan or order,
                    payload=order,
                    order=order,
                    broker=broker,
                    client_id=getattr(self, "client_id", ""),
                    execution_mode=execution_mode,
                )
            except UnderlyingConfirmationUnavailable as exc:
                try:
                    self._emit_transition_event(
                        local_order_id=local_order_id,
                        old_status=status,
                        new_status=status,
                        order=order,
                        decision="REJECT",
                        reason_code=exc.reason_code,
                        explanation=exc.reason,
                        extra_inputs={"underlying_confirmation": exc.metadata, "order_mutated": False},
                    )
                except Exception:
                    pass
                return {
                    "ok": False,
                    "local_order_id": local_order_id,
                    "broker_order_id": order.get("broker_order_id"),
                    "status": status,
                    "error": exc.reason,
                    "stage": STAGE,
                    "reason_code": exc.reason_code,
                    "underlying_confirmation": exc.metadata,
                    "order_mutated": False,
                    "decision_event_only": True,
                }
        return original_submit_existing(self, *args, **kwargs)

    APOrderStateMachine.submit_entry = guarded_submit
    APOrderStateMachine.submit_existing_entry = guarded_submit_existing
    APOrderStateMachine._underlying_confirmation_guard_installed = True
