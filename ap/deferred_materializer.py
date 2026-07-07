"""
ap/deferred_materializer.py — Deferred contract materialization lifecycle.

A deferred `PENDING_TRIGGER` order (contract=DEFERRED:<TICKER>, limit_price=0.01)
waits for two events: watcher trigger breach, then real OCC contract selection.
This module stamps the materialization lifecycle into orders.meta.

P0 live hard-hold amendment:
`broker_ready=true` must never be stamped when client_id is missing or
execution_mode is blank/null/unknown.  This closes the production incident shape
where a live row reached materialization logs with execution_mode blank.
"""

from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta
from typing import Optional

from ap.logger import get_logger
from ap.live_submit_safety import normalize_execution_mode, require_live_identity

log = get_logger("ap.deferred_materializer")

WAITING_FOR_TRIGGER = "WAITING_FOR_TRIGGER"
QUEUED = "QUEUED"
RUNNING = "RUNNING"
RETRY_PENDING = "RETRY_PENDING"
SELECTED = "SELECTED"
FAILED_TERMINAL = "FAILED_TERMINAL"

RETRYABLE_MATERIALIZATION_REASONS: frozenset[str] = frozenset({
    "NO_CHAIN_DATA",
    "CHAIN_PROVIDER_ERROR",
    "CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
    "CHAIN_PROVIDER_EMPTY_OPTIONS",
    "CHAIN_PARSE_EMPTY",
    "NO_EXPIRATION_IN_DTE_WINDOW",
    "DIRECT_QUOTE_UNAVAILABLE",
    "CHAIN_ROW_ZERO_BID_ASK",
    "DIRECT_QUOTE_ZERO_BID_ASK",
    "QUOTE_FETCH_FAILED",
    "CHAIN_EMPTY",
})


def _cfg() -> dict:
    def _int(key: str, default: int) -> int:
        try:
            return max(1, int(os.getenv(key, str(default)).strip()))
        except (TypeError, ValueError):
            return default

    def _bool(key: str, default: bool) -> bool:
        return os.getenv(key, "1" if default else "0").strip().lower() in ("1", "true", "yes")

    def _hhmm(key: str, default: str) -> str:
        val = os.getenv(key, default).strip()
        try:
            h, m = val.split(":")
            if 0 <= int(h) <= 23 and 0 <= int(m) <= 59:
                return val
        except Exception:
            pass
        return default

    return {
        "enabled": _bool("DEFERRED_MATERIALIZATION_BUCKET_ENABLED", True),
        "max_attempts": _int("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", 3),
        "retry_base_s": _int("DEFERRED_MATERIALIZATION_RETRY_BASE_SECONDS", 15),
        "retry_max_s": _int("DEFERRED_MATERIALIZATION_RETRY_MAX_SECONDS", 90),
        "lock_ttl_s": _int("DEFERRED_MATERIALIZATION_LOCK_TTL_SECONDS", 120),
        "entry_cutoff_et": _hhmm("DEFERRED_MATERIALIZATION_ENTRY_CUTOFF_ET", "15:30"),
    }


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _retry_delay_seconds(attempt: int, cfg: dict) -> int:
    try:
        base = int(cfg.get("retry_base_s", 15))
        maximum = int(cfg.get("retry_max_s", 90))
        return min(maximum, base * (2 ** max(0, attempt - 1)))
    except Exception:
        return 15


def _is_past_entry_cutoff(cfg: dict) -> bool:
    try:
        from zoneinfo import ZoneInfo
        now_et = datetime.now(ZoneInfo("America/New_York"))
        cutoff = cfg.get("entry_cutoff_et", "15:30")
        ch, cm = cutoff.split(":")
        cutoff_minutes = int(ch) * 60 + int(cm)
        now_minutes = now_et.hour * 60 + now_et.minute
        return now_minutes >= cutoff_minutes
    except Exception:
        return False


def _osm_update_meta(osm, local_order_id: str, patch: dict) -> bool:
    try:
        fn = getattr(osm, "update_order_meta", None)
        if callable(fn):
            return bool(fn(local_order_id, patch))
    except Exception as exc:
        log.debug("materialization meta update failed local_order_id=%s: %s", local_order_id, exc)
    return False


def _safe_materialization_identity(client_id: str, execution_mode: str) -> bool:
    """Return True only when materializer identity is safe enough to continue.

    Paper rows may proceed only with explicit execution_mode=paper.  Live rows may
    proceed only with explicit execution_mode=live and non-empty client_id.  Blank
    or unknown mode is blocked before broker_ready can become true.
    """
    mode = normalize_execution_mode(execution_mode)
    if mode == "paper":
        if str(client_id or "").strip():
            return True
        log.critical(
            "DEFERRED_MATERIALIZATION_IDENTITY_BLOCK order_client_missing execution_mode=paper"
        )
        return False
    decision = require_live_identity(client_id=client_id, execution_mode=execution_mode)
    if not decision.ok:
        log.critical(
            "DEFERRED_MATERIALIZATION_IDENTITY_BLOCK client_id=%s execution_mode=%s reason=%s",
            str(client_id or ""), str(execution_mode or ""), decision.reason,
        )
        return False
    return True


def stamp_trigger_queued(
    osm,
    local_order_id: str,
    *,
    client_id: str,
    execution_mode: str,
    symbol: str,
    direction: str,
    triggered_price: float,
    signal_id: str = "",
) -> bool:
    now = _now_utc().isoformat()
    patch = {
        "materialization_status": QUEUED,
        "broker_ready": False,
        "triggered_at": now,
        "triggered_underlying_price": float(triggered_price or 0),
        "materialization_started_at": now,
        "materialization_attempts": 0,
        "materialization_client_id": str(client_id or ""),
        "materialization_execution_mode": str(execution_mode or ""),
        "materialization_identity_ok": _safe_materialization_identity(client_id, execution_mode),
    }
    ok = _osm_update_meta(osm, local_order_id, patch)
    log.info(
        "DEFERRED_TRIGGER_MOVED_TO_MATERIALIZATION_BUCKET "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
        "direction=%s contract_before=DEFERRED:* limit_before=0.01 "
        "new_status=QUEUED broker_ready=false triggered_price=%.4f",
        local_order_id, client_id, execution_mode, symbol, direction, float(triggered_price or 0),
    )
    return ok


def stamp_running(
    osm,
    local_order_id: str,
    *,
    client_id: str,
    execution_mode: str,
    symbol: str,
    attempt: int,
    lock_ttl_s: int = 120,
) -> bool:
    lock_until = (_now_utc() + timedelta(seconds=lock_ttl_s)).isoformat()
    identity_ok = _safe_materialization_identity(client_id, execution_mode)
    patch = {
        "materialization_status": RUNNING,
        "broker_ready": False,
        "materialization_lock_until": lock_until,
        "materialization_claimed_at": _now_utc().isoformat(),
        "materialization_attempts": int(attempt),
        "materialization_client_id": str(client_id or ""),
        "materialization_execution_mode": str(execution_mode or ""),
        "materialization_identity_ok": identity_ok,
    }
    ok = _osm_update_meta(osm, local_order_id, patch)
    log.info(
        "DEFERRED_MATERIALIZATION_CLAIMED "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s attempt=%d lock_until=%s",
        local_order_id, client_id, execution_mode, symbol, attempt, lock_until,
    )
    log.info(
        "DEFERRED_MATERIALIZATION_STARTED "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
        "direction=unknown retry_count=%d contract_before=DEFERRED:* limit_before=0.01 "
        "materialization_status=RUNNING broker_ready=false identity_ok=%s",
        local_order_id, client_id, execution_mode, symbol, attempt, identity_ok,
    )
    return ok


def stamp_selected(
    osm,
    local_order_id: str,
    *,
    client_id: str,
    execution_mode: str,
    symbol: str,
    direction: str,
    contract: str,
    bid: float,
    ask: float,
    mid: float,
    limit_price: float,
    qty: int,
    reserved_cost: float,
    dte: Optional[int] = None,
    expiration: Optional[str] = None,
    delta: Optional[float] = None,
    open_interest: Optional[int] = None,
    volume: Optional[int] = None,
    attempt: int = 1,
) -> bool:
    _c = str(contract or "")
    if not _c or _c.upper().startswith("DEFERRED:") or float(limit_price or 0) <= 0.01 or int(qty or 0) < 1:
        log.critical(
            "stamp_selected called with invalid contract/limit/qty — refusing to set broker_ready=True: contract=%r limit=%.4f qty=%d",
            _c, float(limit_price or 0), int(qty or 0),
        )
        return False
    if not _safe_materialization_identity(client_id, execution_mode):
        _osm_update_meta(osm, local_order_id, {
            "materialization_status": FAILED_TERMINAL,
            "broker_ready": False,
            "materialization_reason": "LIVE_SUBMIT_BLOCK_BLANK_OR_UNKNOWN_EXECUTION_MODE",
            "materialization_finished_at": _now_utc().isoformat(),
            "materialization_attempts": int(attempt),
            "materialization_client_id": str(client_id or ""),
            "materialization_execution_mode": str(execution_mode or ""),
            "materialization_identity_ok": False,
        })
        log.critical(
            "DEFERRED_MATERIALIZATION_SELECTED_BLOCKED order_id=%s client_id=%s execution_mode=%s symbol=%s contract=%s reason=unsafe_identity",
            local_order_id, client_id, execution_mode, symbol, _c,
        )
        return False

    now = _now_utc().isoformat()
    patch = {
        "materialization_status": SELECTED,
        "broker_ready": True,
        "selected_contract": _c,
        "selected_bid": round(float(bid or 0), 4),
        "selected_ask": round(float(ask or 0), 4),
        "selected_mid": round(float(mid or 0), 4),
        "selected_limit": round(float(limit_price), 4),
        "selected_qty": int(qty),
        "selected_reserved_cost": round(float(reserved_cost or 0), 2),
        "selected_dte": int(dte) if dte is not None else None,
        "selected_expiration": str(expiration) if expiration else None,
        "selected_delta": round(float(delta), 4) if delta is not None else None,
        "selected_open_interest": int(open_interest) if open_interest is not None else None,
        "selected_volume": int(volume) if volume is not None else None,
        "materialization_finished_at": now,
        "materialization_attempts": int(attempt),
        "materialization_client_id": str(client_id or ""),
        "materialization_execution_mode": str(execution_mode or ""),
        "materialization_identity_ok": True,
    }
    ok = _osm_update_meta(osm, local_order_id, patch)
    log.info(
        "DEFERRED_MATERIALIZATION_SELECTED "
        "order_id=%s client_id=%s execution_mode=%s symbol=%s "
        "direction=%s contract_after=%s limit_after=%.4f qty=%d "
        "bid=%.4f ask=%.4f mid=%.4f reserved_cost=%.2f "
        "retry_count=%d materialization_status=SELECTED broker_ready=true",
        local_order_id, client_id, execution_mode, symbol, direction, _c, float(limit_price), int(qty),
        float(bid or 0), float(ask or 0), float(mid or 0), float(reserved_cost or 0), int(attempt),
    )
    log.info(
        "READY_TO_SUBMIT_CREATED order_id=%s client_id=%s execution_mode=%s symbol=%s contract=%s limit=%.4f qty=%d broker_ready=true",
        local_order_id, client_id, execution_mode, symbol, _c, float(limit_price), int(qty),
    )
    return ok


def stamp_retry_pending(
    osm,
    local_order_id: str,
    *,
    client_id: str,
    execution_mode: str,
    symbol: str,
    direction: str,
    reason_code: str,
    attempt: int,
    max_attempts: int,
    retry_delay_s: int,
    selector_failure: "dict | None" = None,
) -> bool:
    next_retry = (_now_utc() + timedelta(seconds=retry_delay_s)).isoformat()
    patch = {
        "materialization_status": RETRY_PENDING,
        "broker_ready": False,
        "materialization_attempts": int(attempt),
        "materialization_next_retry_at": next_retry,
        "materialization_reason": str(reason_code or ""),
        "materialization_last_failure_at": _now_utc().isoformat(),
        "materialization_client_id": str(client_id or ""),
        "materialization_execution_mode": str(execution_mode or ""),
        "materialization_identity_ok": _safe_materialization_identity(client_id, execution_mode),
    }
    if selector_failure:
        patch["materialization_selector_failure"] = selector_failure
    ok = _osm_update_meta(osm, local_order_id, patch)
    log.warning(
        "DEFERRED_MATERIALIZATION_RETRY order_id=%s client_id=%s execution_mode=%s symbol=%s direction=%s failure_reason=%s retry_count=%d next_retry_at=%s contract_before=DEFERRED:* limit_before=0.01 materialization_status=RETRY_PENDING broker_ready=false",
        local_order_id, client_id, execution_mode, symbol, direction, reason_code, attempt, next_retry,
    )
    return ok


def stamp_failed_terminal(
    osm,
    local_order_id: str,
    *,
    client_id: str,
    execution_mode: str,
    symbol: str,
    direction: str,
    reason_code: str,
    attempt: int,
    selector_failure: "dict | None" = None,
) -> bool:
    patch = {
        "materialization_status": FAILED_TERMINAL,
        "broker_ready": False,
        "materialization_attempts": int(attempt),
        "materialization_reason": str(reason_code or ""),
        "materialization_finished_at": _now_utc().isoformat(),
        "materialization_last_failure_at": _now_utc().isoformat(),
        "materialization_client_id": str(client_id or ""),
        "materialization_execution_mode": str(execution_mode or ""),
        "materialization_identity_ok": _safe_materialization_identity(client_id, execution_mode),
    }
    if selector_failure:
        patch["materialization_selector_failure"] = selector_failure
    ok = _osm_update_meta(osm, local_order_id, patch)
    log.critical(
        "DEFERRED_MATERIALIZATION_FAILED order_id=%s client_id=%s execution_mode=%s symbol=%s direction=%s failure_reason=%s retry_count=%d contract_before=DEFERRED:* limit_before=0.01 materialization_status=FAILED_TERMINAL broker_ready=false",
        local_order_id, client_id, execution_mode, symbol, direction, reason_code, attempt,
    )
    return ok


def is_broker_ready_from_meta(plan_metadata: "dict | None") -> bool:
    if not isinstance(plan_metadata, dict):
        return False
    if plan_metadata.get("broker_ready") is not True:
        return False
    if plan_metadata.get("materialization_identity_ok") is False:
        return False
    return True


def should_reprocess_claim(plan_metadata: "dict | None", cfg: dict) -> bool:
    if not isinstance(plan_metadata, dict):
        return True
    if plan_metadata.get("materialization_status") != RUNNING:
        return True
    lock_until_str = plan_metadata.get("materialization_lock_until")
    if not lock_until_str:
        return True
    try:
        lock_until = datetime.fromisoformat(str(lock_until_str))
        return datetime.now(timezone.utc) > lock_until
    except Exception:
        return True


def is_reason_retryable(reason_code: str) -> bool:
    return str(reason_code or "").strip() in RETRYABLE_MATERIALIZATION_REASONS
