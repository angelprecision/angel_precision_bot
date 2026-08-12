from __future__ import annotations

import logging
import os
from datetime import date as _date, datetime, time, timedelta, timezone
from typing import Any, Callable, Mapping, Optional

log = logging.getLogger("ap.counterfactual_tracker")

MAX_RESOLUTION_ATTEMPTS = 3
RESOLUTION_UNAVAILABLE = "unavailable"
RESOLUTION_UNKNOWN = "unknown"
RESOLUTION_TARGET_FIRST = "target_first"
RESOLUTION_STOP_FIRST = "stop_first"
RESOLUTION_NEITHER_EOD = "neither_eod"


def _default_conn_factory() -> Optional[Callable[[], Any]]:
    try:
        from ap.db import conn  # type: ignore
        return conn
    except Exception:
        return None


def _as_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _normalize_direction(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"CALL", "BUY_CALL", "LONG_CALL", "C"}:
        return "CALL"
    if raw in {"PUT", "BUY_PUT", "LONG_PUT", "P"}:
        return "PUT"
    return raw or "UNKNOWN"


def _normalize_mode(value: Any) -> str:
    raw = str(value or "").strip().upper()
    return raw if raw in {"LIVE", "PAPER"} else ""


def _as_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)
        except Exception:
            return None
    return None


def _coerce_signal(signal: Any) -> dict[str, Any]:
    if isinstance(signal, dict):
        return dict(signal)
    if hasattr(signal, "to_signal_dict"):
        try:
            out = signal.to_signal_dict()
            if isinstance(out, dict):
                return dict(out)
        except Exception:
            pass
    out: dict[str, Any] = {}
    for key in (
        "signal_id",
        "canonical_signal_id",
        "ticker",
        "symbol",
        "side",
        "direction",
        "entry_ref",
        "target_ref",
        "stop_ref",
        "entry_price",
        "target_price",
        "stop_price",
        "trigger_price",
        "timeframe",
    ):
        if hasattr(signal, key):
            out[key] = getattr(signal, key)
    return out


def _extract_levels(signal: Mapping[str, Any]) -> tuple[float | None, float | None, float | None, list[str]]:
    trigger = signal.get("trigger")
    trigger_map = trigger if isinstance(trigger, Mapping) else {}

    entry = (
        _as_float(signal.get("entry_ref"))
        or _as_float(signal.get("entry_price"))
        or _as_float(signal.get("trigger_price"))
        or _as_float(signal.get("entry_trigger"))
        or _as_float(trigger_map.get("entry"))
    )
    target = (
        _as_float(signal.get("target_ref"))
        or _as_float(signal.get("target_price"))
        or _as_float(signal.get("target_underlying"))
        or _as_float(trigger_map.get("pt1"))
        or _as_float(trigger_map.get("target"))
    )
    stop = (
        _as_float(signal.get("stop_ref"))
        or _as_float(signal.get("stop_price"))
        or _as_float(signal.get("stop_underlying"))
        or _as_float(trigger_map.get("stop"))
    )

    missing: list[str] = []
    if entry is None or entry <= 0:
        missing.append("entry_ref")
    if target is None or target <= 0:
        missing.append("target_ref")
    if stop is None or stop <= 0:
        missing.append("stop_ref")
    if entry is not None and stop is not None and abs(entry - stop) == 0:
        missing.append("invalid_risk_width")
    return entry, target, stop, missing


def _build_meta(
    *,
    signal: Mapping[str, Any],
    missing_levels: list[str],
    source: str,
) -> dict[str, Any]:
    meta = {
        "tracker_source": source,
        "resolution_attempts": 0,
        "missing_levels": list(missing_levels),
    }
    if signal.get("timeframe") is not None:
        meta["timeframe"] = str(signal.get("timeframe"))
    if signal.get("pattern") is not None:
        meta["pattern"] = str(signal.get("pattern"))
    return meta


def track_counterfactual_signal(
    *,
    signal: Any,
    client_id: str,
    execution_mode: str,
    block_stage: str,
    block_reason: str,
    reason_code: str = "",
    source: str = "block",
    blocked_at: datetime | None = None,
    conn_factory: Optional[Callable[[], Any]] = None,
) -> bool:
    """Best-effort insert. Never raises and never mutates trading state."""
    sig = _coerce_signal(signal)
    signal_id = str(sig.get("signal_id") or "").strip()
    ticker = str(sig.get("ticker") or sig.get("symbol") or "").strip().upper()
    if not signal_id or not ticker or not client_id:
        return False

    direction = _normalize_direction(sig.get("side") or sig.get("direction"))
    supplied_mode = execution_mode if execution_mode not in (None, "") else sig.get("execution_mode")
    mode = _normalize_mode(supplied_mode)
    if not mode:
        log.warning(
            "COUNTERFACTUAL_INSERT_REJECTED reason_code=EXECUTION_MODE_UNPROVEN "
            "client_id=%s signal_id=%s",
            client_id,
            signal_id,
        )
        return False
    canonical_signal_id = str(sig.get("canonical_signal_id") or "").strip() or None
    entry_ref, target_ref, stop_ref, missing_levels = _extract_levels(sig)
    resolution = RESOLUTION_UNAVAILABLE if missing_levels else None
    meta = _build_meta(signal=sig, missing_levels=missing_levels, source=source)
    if resolution == RESOLUTION_UNAVAILABLE:
        meta["unavailable_reason"] = "missing_or_invalid_levels"

    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    if cf is None:
        return False

    blocked_ts = blocked_at or datetime.now(timezone.utc)
    try:
        with cf() as c:
            c.execute(
                """
                INSERT INTO public.blocked_signal_counterfactuals (
                    signal_id,
                    canonical_signal_id,
                    client_id,
                    execution_mode,
                    ticker,
                    direction,
                    block_stage,
                    block_reason,
                    reason_code,
                    blocked_at,
                    entry_ref,
                    target_ref,
                    stop_ref,
                    resolution,
                    meta
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (signal_id, client_id, execution_mode) DO NOTHING
                """,
                (
                    signal_id,
                    canonical_signal_id,
                    str(client_id),
                    mode,
                    ticker,
                    direction,
                    str(block_stage or "blocked_unknown"),
                    str(block_reason or "unknown"),
                    str(reason_code or ""),
                    blocked_ts,
                    entry_ref,
                    target_ref,
                    stop_ref,
                    resolution,
                    _json_dumps(meta),
                ),
            )
        return True
    except Exception as exc:
        log.debug("counterfactual insert skipped: %s", exc)
        return False


def _json_dumps(value: Any) -> str:
    import json

    return json.dumps(value, default=str, sort_keys=True)


def _json_loads(value: Any) -> dict[str, Any]:
    import json

    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
            return loaded if isinstance(loaded, dict) else {}
        except Exception:
            return {}
    return {}


def _candle_number(candle: Mapping[str, Any], *keys: str) -> float | None:
    for key in keys:
        val = _as_float(candle.get(key))
        if val is not None:
            return val
    return None


def _resolve_bar_crossing(
    *,
    direction: str,
    target_ref: float,
    stop_ref: float,
    bars: list[dict[str, Any]],
) -> tuple[str, float | None, dict[str, Any]]:
    for idx, bar in enumerate(bars):
        open_ = _candle_number(bar, "open", "o")
        high = _candle_number(bar, "high", "h")
        low = _candle_number(bar, "low", "l")
        close = _candle_number(bar, "close", "c", "price", "last")

        if direction == "PUT":
            target_hit = low is not None and low <= target_ref
            stop_hit = high is not None and high >= stop_ref
            if open_ is not None and open_ <= target_ref:
                return RESOLUTION_TARGET_FIRST, target_ref, {"gap_bar_index": idx, "gap_side": "target"}
            if open_ is not None and open_ >= stop_ref:
                return RESOLUTION_STOP_FIRST, stop_ref, {"gap_bar_index": idx, "gap_side": "stop"}
        else:
            target_hit = high is not None and high >= target_ref
            stop_hit = low is not None and low <= stop_ref
            if open_ is not None and open_ >= target_ref:
                return RESOLUTION_TARGET_FIRST, target_ref, {"gap_bar_index": idx, "gap_side": "target"}
            if open_ is not None and open_ <= stop_ref:
                return RESOLUTION_STOP_FIRST, stop_ref, {"gap_bar_index": idx, "gap_side": "stop"}

        if target_hit and not stop_hit:
            return RESOLUTION_TARGET_FIRST, target_ref, {"bar_index": idx}
        if stop_hit and not target_hit:
            return RESOLUTION_STOP_FIRST, stop_ref, {"bar_index": idx}
        if target_hit and stop_hit:
            return RESOLUTION_UNKNOWN, None, {"bar_index": idx, "ambiguous_bar": True}

        if idx == len(bars) - 1 and close is not None:
            return RESOLUTION_NEITHER_EOD, close, {"bar_index": idx}

    return RESOLUTION_UNKNOWN, None, {"bars_present": bool(bars)}


def compute_hypothetical_r(*, entry_ref: float, stop_ref: float, outcome: float, direction: str) -> float | None:
    risk = abs(float(entry_ref) - float(stop_ref))
    if risk <= 0:
        return None
    if direction == "PUT":
        return round((float(entry_ref) - float(outcome)) / risk, 6)
    return round((float(outcome) - float(entry_ref)) / risk, 6)


def _session_date_from_row(row: Mapping[str, Any]) -> _date:
    blocked_at = _as_datetime(row.get("blocked_at"))
    if blocked_at is not None:
        return blocked_at.astimezone(timezone.utc).date()
    return datetime.now(timezone.utc).date()


def _market_tz():
    try:
        from zoneinfo import ZoneInfo

        return ZoneInfo("America/New_York")
    except Exception:  # pragma: no cover
        return timezone.utc


def _next_weekday(day: _date) -> _date:
    out = day + timedelta(days=1)
    while out.weekday() >= 5:
        out += timedelta(days=1)
    return out


def _resolution_window(row: Mapping[str, Any]) -> tuple[_date | None, datetime | None, str | None]:
    blocked_at = _as_datetime(row.get("blocked_at"))
    if blocked_at is None:
        return None, None, "blocked_at_unavailable"

    et = _market_tz()
    blocked_et = blocked_at.astimezone(et)
    session_open = datetime.combine(blocked_et.date(), time(9, 30), tzinfo=et)
    session_close = datetime.combine(blocked_et.date(), time(16, 0), tzinfo=et)

    if blocked_et.weekday() >= 5:
        next_session = _next_weekday(blocked_et.date())
        return next_session, datetime.combine(next_session, time(9, 30), tzinfo=et).astimezone(timezone.utc), None

    if session_open <= blocked_et < session_close:
        return blocked_et.date(), blocked_at.astimezone(timezone.utc), None

    if blocked_et >= session_close:
        next_session = _next_weekday(blocked_et.date())
        return next_session, datetime.combine(next_session, time(9, 30), tzinfo=et).astimezone(timezone.utc), None

    return blocked_et.date(), session_open.astimezone(timezone.utc), None


def _filter_bars_for_resolution(
    *,
    bars: list[dict[str, Any]],
    start_dt_utc: datetime,
) -> tuple[list[dict[str, Any]] | None, str | None]:
    filtered: list[dict[str, Any]] = []
    for bar in bars:
        bar_dt = _as_datetime(bar.get("time"))
        if bar_dt is None:
            return None, "bar_timestamp_unavailable"
        if bar_dt.astimezone(timezone.utc) >= start_dt_utc:
            filtered.append(bar)
    return filtered, None


def fetch_underlying_bars_for_session(
    *,
    ticker: str,
    broker: Any,
    session_date: _date,
    interval: str = "1min",
) -> list[dict[str, Any]]:
    if not ticker or broker is None:
        return []

    try:
        from zoneinfo import ZoneInfo

        et = ZoneInfo("America/New_York")
    except Exception:  # pragma: no cover
        et = timezone.utc

    start_dt = datetime.combine(session_date, time(9, 30), tzinfo=et)
    end_dt = datetime.combine(session_date, time(16, 0), tzinfo=et)

    try:
        try:
            from ap.overnight_daily_validator import _resolve_market_data_base_url

            base_url = _resolve_market_data_base_url(broker)
        except Exception:
            base_url = str(
                os.getenv("TRADIER_MARKET_DATA_BASE_URL")
                or os.getenv("TRADIER_DATA_BASE_URL")
                or getattr(getattr(broker, "cfg", None), "base_url", "")
                or "https://api.tradier.com"
            ).rstrip("/")
            if "sandbox.tradier.com" in base_url.lower():
                base_url = "https://api.tradier.com"

        session = getattr(broker, "session", None)
        if session is None or not hasattr(session, "get"):
            return []

        resp = session.get(
            f"{base_url}/v1/markets/timesales",
            params={
                "symbol": ticker,
                "interval": interval,
                "start": start_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "end": end_dt.strftime("%Y-%m-%dT%H:%M:%S"),
                "session_filter": "open",
            },
            headers={"Accept": "application/json"},
            timeout=10,
        )
        if hasattr(resp, "raise_for_status"):
            resp.raise_for_status()
        payload = resp.json() if hasattr(resp, "json") else {}
        series = (payload.get("series") or {}) if isinstance(payload, Mapping) else {}
        data_node = series.get("data") if isinstance(series, Mapping) else None
        items = data_node.get("item") if isinstance(data_node, Mapping) else data_node
        if items is None:
            return []
        if isinstance(items, Mapping):
            items = [items]
        out: list[dict[str, Any]] = []
        for item in items:
            if not isinstance(item, Mapping):
                continue
            out.append(
                {
                    "open": _as_float(item.get("open") or item.get("o")),
                    "high": _as_float(item.get("high") or item.get("h")),
                    "low": _as_float(item.get("low") or item.get("l")),
                    "close": _as_float(item.get("close") or item.get("c") or item.get("price")),
                    "time": item.get("time") or item.get("timestamp") or item.get("datetime"),
                }
            )
        return [row for row in out if _candle_number(row, "high") is not None and _candle_number(row, "low") is not None]
    except Exception as exc:
        log.debug("counterfactual bar fetch failed for %s: %s", ticker, exc)
        return []


def _fetch_pending_rows(
    *,
    conn_factory: Optional[Callable[[], Any]],
    client_id: str | None,
    execution_mode: str | None,
) -> list[dict[str, Any]]:
    if conn_factory is None:
        return []
    where = [
        "(resolution IS NULL OR resolution = '')",
    ]
    params: list[Any] = []
    if client_id:
        where.append("client_id = %s")
        params.append(str(client_id))
    if execution_mode:
        where.append("execution_mode = %s")
        params.append(str(execution_mode))
    try:
        with conn_factory() as c:
            c.execute(
                f"""
                SELECT *
                FROM public.blocked_signal_counterfactuals
                WHERE {' AND '.join(where)}
                ORDER BY blocked_at ASC
                """
                ,
                params,
            )
            return list(c.fetchall() or [])
    except Exception as exc:
        log.debug("counterfactual pending read skipped: %s", exc)
        return []


def _update_resolution(
    *,
    conn_factory: Optional[Callable[[], Any]],
    row_id: Any,
    resolution: str,
    hypothetical_r: float | None,
    meta: dict[str, Any],
    resolved_at: datetime | None = None,
) -> None:
    if conn_factory is None:
        return
    try:
        with conn_factory() as c:
            c.execute(
                """
                UPDATE public.blocked_signal_counterfactuals
                SET resolution=%s,
                    hypothetical_r=%s,
                    resolved_at=%s,
                    meta=%s::jsonb
                WHERE id=%s
                """,
                (
                    resolution,
                    hypothetical_r,
                    resolved_at or datetime.now(timezone.utc),
                    _json_dumps(meta),
                    row_id,
                ),
            )
    except Exception as exc:
        log.debug("counterfactual resolution update skipped: %s", exc)


def _mark_retry_or_unknown(
    *,
    conn_factory: Optional[Callable[[], Any]],
    row: Mapping[str, Any],
    error_code: str,
) -> str:
    meta = _json_loads(row.get("meta"))
    attempts = _as_int(meta.get("resolution_attempts")) + 1
    meta["resolution_attempts"] = attempts
    meta["last_resolution_error"] = error_code
    if attempts >= MAX_RESOLUTION_ATTEMPTS:
        _update_resolution(
            conn_factory=conn_factory,
            row_id=row.get("id"),
            resolution=RESOLUTION_UNKNOWN,
            hypothetical_r=None,
            meta=meta,
        )
        return RESOLUTION_UNKNOWN
    if conn_factory is not None:
        try:
            with conn_factory() as c:
                c.execute(
                    """
                    UPDATE public.blocked_signal_counterfactuals
                    SET meta=%s::jsonb
                    WHERE id=%s
                    """,
                    (_json_dumps(meta), row.get("id")),
                )
        except Exception as exc:
            log.debug("counterfactual retry update skipped: %s", exc)
    return "retry_later"


def resolve_pending_counterfactuals(
    *,
    broker: Any,
    client_id: str | None = None,
    execution_mode: str | None = None,
    conn_factory: Optional[Callable[[], Any]] = None,
) -> dict[str, Any]:
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    rows = _fetch_pending_rows(conn_factory=cf, client_id=client_id, execution_mode=execution_mode)
    summary = {
        "fetched": len(rows),
        "resolved": 0,
        "unknown": 0,
        "retry_later": 0,
        "unavailable": 0,
    }
    for row in rows:
        meta = _json_loads(row.get("meta"))
        if str(row.get("resolution") or "") == RESOLUTION_UNAVAILABLE:
            summary["unavailable"] += 1
            continue

        entry_ref = _as_float(row.get("entry_ref"))
        target_ref = _as_float(row.get("target_ref"))
        stop_ref = _as_float(row.get("stop_ref"))
        direction = _normalize_direction(row.get("direction"))
        if entry_ref is None or target_ref is None or stop_ref is None or abs(entry_ref - stop_ref) == 0:
            meta["unavailable_reason"] = "missing_or_invalid_levels"
            _update_resolution(
                conn_factory=cf,
                row_id=row.get("id"),
                resolution=RESOLUTION_UNAVAILABLE,
                hypothetical_r=None,
                meta=meta,
            )
            summary["unavailable"] += 1
            continue

        session_date, resolution_start, window_error = _resolution_window(row)
        if session_date is None or resolution_start is None:
            meta["resolution_attempts"] = _as_int(meta.get("resolution_attempts"))
            meta["last_resolution_error"] = str(window_error or "blocked_at_unavailable")
            _update_resolution(
                conn_factory=cf,
                row_id=row.get("id"),
                resolution=RESOLUTION_UNKNOWN,
                hypothetical_r=None,
                meta=meta,
            )
            summary["unknown"] += 1
            continue
        bars = fetch_underlying_bars_for_session(
            ticker=str(row.get("ticker") or ""),
            broker=broker,
            session_date=session_date,
        )
        if not bars:
            status = _mark_retry_or_unknown(
                conn_factory=cf,
                row=row,
                error_code="bars_unavailable",
            )
            summary["unknown" if status == RESOLUTION_UNKNOWN else "retry_later"] += 1
            continue

        filtered_bars, timestamp_error = _filter_bars_for_resolution(
            bars=bars,
            start_dt_utc=resolution_start,
        )
        if filtered_bars is None:
            meta["resolution_attempts"] = _as_int(meta.get("resolution_attempts"))
            meta["last_resolution_error"] = str(timestamp_error or "bar_timestamp_unavailable")
            _update_resolution(
                conn_factory=cf,
                row_id=row.get("id"),
                resolution=RESOLUTION_UNKNOWN,
                hypothetical_r=None,
                meta=meta,
            )
            summary["unknown"] += 1
            continue
        if not filtered_bars:
            status = _mark_retry_or_unknown(
                conn_factory=cf,
                row=row,
                error_code="no_bars_after_blocked_at",
            )
            summary["unknown" if status == RESOLUTION_UNKNOWN else "retry_later"] += 1
            continue

        resolution, outcome, details = _resolve_bar_crossing(
            direction=direction,
            target_ref=target_ref,
            stop_ref=stop_ref,
            bars=filtered_bars,
        )
        meta["resolution_attempts"] = _as_int(meta.get("resolution_attempts"))
        meta["resolver_details"] = details
        meta["bars_count"] = len(filtered_bars)
        meta["resolution_session_date"] = session_date.isoformat()
        meta["resolution_start"] = resolution_start.isoformat()
        if resolution == RESOLUTION_UNKNOWN or outcome is None:
            _update_resolution(
                conn_factory=cf,
                row_id=row.get("id"),
                resolution=RESOLUTION_UNKNOWN,
                hypothetical_r=None,
                meta=meta,
            )
            summary["unknown"] += 1
            continue

        hypothetical_r = compute_hypothetical_r(
            entry_ref=entry_ref,
            stop_ref=stop_ref,
            outcome=outcome,
            direction=direction,
        )
        _update_resolution(
            conn_factory=cf,
            row_id=row.get("id"),
            resolution=resolution,
            hypothetical_r=hypothetical_r,
            meta=meta,
        )
        summary["resolved"] += 1
    return summary


def build_weekly_counterfactual_rollup(
    *,
    iso_week: str | None = None,
    date_value: Any = None,
    conn_factory: Optional[Callable[[], Any]] = None,
) -> list[dict[str, Any]]:
    cf = conn_factory if conn_factory is not None else _default_conn_factory()
    if cf is None:
        return []

    if iso_week:
        from ap_operator_daily_folders import iso_week_bounds

        week_start, week_end = iso_week_bounds(str(iso_week))
    else:
        from ap_operator_daily_folders import iso_week_bounds, iso_week_string

        target_date = date_value
        if target_date is None:
            target_date = datetime.now(timezone.utc).date()
        week_start, week_end = iso_week_bounds(iso_week_string(target_date))

    start_dt = datetime.combine(week_start, time(0, 0), tzinfo=timezone.utc)
    end_dt = datetime.combine(week_end + timedelta(days=1), time(0, 0), tzinfo=timezone.utc)

    try:
        with cf() as c:
            c.execute(
                """
                SELECT
                    COALESCE(NULLIF(reason_code, ''), 'unknown') AS reason_code,
                    COUNT(*) AS count,
                    COALESCE(SUM(CASE WHEN hypothetical_r < 0 THEN ABS(hypothetical_r) ELSE 0 END), 0) AS saved_r,
                    COALESCE(SUM(CASE WHEN hypothetical_r > 0 THEN hypothetical_r ELSE 0 END), 0) AS cost_r,
                    COUNT(*) FILTER (WHERE resolution = %s) AS unknown_count
                FROM public.blocked_signal_counterfactuals
                WHERE blocked_at >= %s
                  AND blocked_at < %s
                GROUP BY COALESCE(NULLIF(reason_code, ''), 'unknown')
                ORDER BY count DESC, reason_code ASC
                """,
                (RESOLUTION_UNKNOWN, start_dt, end_dt),
            )
            rows = list(c.fetchall() or [])
    except Exception as exc:
        log.debug("counterfactual weekly rollup skipped: %s", exc)
        return []

    out: list[dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "reason_code": str(row.get("reason_code") or "unknown"),
                "count": _as_int(row.get("count")),
                "saved_R": round(float(row.get("saved_r") or 0.0), 6),
                "cost_R": round(float(row.get("cost_r") or 0.0), 6),
                "unknown_count": _as_int(row.get("unknown_count")),
            }
        )
    return out
