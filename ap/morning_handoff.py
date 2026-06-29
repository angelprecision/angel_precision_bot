from __future__ import annotations

import logging
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo


log = logging.getLogger("ap.morning_handoff")

ET = ZoneInfo("America/New_York")
_TABLE_READY = False
_WATCHER_RESEED_LOOKBACK_HOURS = 48


def _now_et(now: datetime | None = None) -> datetime:
    return (now or datetime.now(ET)).astimezone(ET)


def _trading_date(now: datetime | None = None) -> str:
    return _now_et(now).date().isoformat()


def _is_market_day(now: datetime | None = None) -> bool:
    return _now_et(now).weekday() < 5


def _after_929_et(now: datetime | None = None) -> bool:
    dt = _now_et(now)
    return _is_market_day(dt) and (dt.hour > 9 or (dt.hour == 9 and dt.minute >= 29))


def _normalize_mode(value: str | None) -> str:
    return str(value or "").strip().lower()


# ─────────────────────────────────────────────────────────────────────────────
# PR #183 — ap_signals → trade_queue WATCHING handoff
#
# Problem: paper accounts had ap_signals WATCHING rows but empty trade_queue,
# so watcher_started_at stayed null and no entry watchers were seeded.
# Live (Jason) worked because his signals had trade_queue WATCHING rows.
#
# Fix: enqueue_watching_signals_to_trade_queue() runs for every eligible client
# during morning handoff (post_overnight_reeval stage) and creates the missing
# WATCHING trade_queue rows before _reseed_watchers is called.
# ─────────────────────────────────────────────────────────────────────────────


def _fetch_client_member(client_id: str) -> dict | None:
    """Fetch the clients row for credential resolution and account_id normalisation."""
    from ap.db import conn, run_with_retry

    def _q():
        with conn() as c:
            c.execute(
                """
                SELECT email, execution_mode, active_mode,
                       tradier_account_id, tradier_access_token,
                       tradier_paper_account_id, tradier_paper_access_token,
                       tradier_live_account_id, tradier_live_access_token,
                       tradier_base_url
                FROM clients
                WHERE email = %s
                LIMIT 1
                """,
                (client_id,),
            )
            row = c.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in getattr(c, "description", [])]
            return dict(row) if isinstance(row, dict) else dict(zip(cols, row))

    try:
        return run_with_retry(_q)
    except Exception as exc:
        log.warning("_fetch_client_member failed client=%s: %s", client_id, exc)
        return None


def _resolve_paper_creds(member: dict) -> tuple[str | None, str | None]:
    """
    Return (account_id, access_token) for paper mode.

    Priority:
      1. tradier_paper_account_id / tradier_paper_access_token
      2. tradier_account_id / tradier_access_token
         (only if active_mode='paper' or account_id starts with VA — sandbox shape)

    Returns (None, None) if no usable credentials found.
    Strips whitespace/newlines from account_id (Jose-style corruption fix).
    """
    raw_account_id = (
        member.get("tradier_paper_account_id")
        or member.get("tradier_account_id")
        or ""
    )
    account_id = str(raw_account_id).strip().replace("\n", "").replace("\r", "").replace("\t", "")
    token = (
        member.get("tradier_paper_access_token")
        or member.get("tradier_access_token")
        or ""
    )
    token = str(token).strip()

    # Generic tradier_account_id is only valid as paper fallback if it looks
    # like a sandbox account (starts with VA) or active_mode confirms paper.
    if not member.get("tradier_paper_account_id") and account_id:
        active_mode = str(member.get("active_mode") or "").strip().lower()
        if not account_id.startswith("VA") and active_mode not in ("paper", "sandbox"):
            account_id = ""

    if not account_id or not token:
        return None, None
    return account_id, token


def _hydrate_trigger_from_raw_payload(signal_row: dict) -> dict:
    """
    If entry_trigger / stop_price / target_price / underlying_at_signal are NULL
    on the ap_signals row, try to fill them from raw_payload.

    Checks:
      raw_payload.trigger          (top-level key dict with entry/stop/target)
      raw_payload.entry_trigger    (flat scalar)
      raw_payload.stop_price       (flat scalar)
      raw_payload.target_price     (flat scalar)
      raw_payload.underlying_at_signal
    """
    out = dict(signal_row)
    raw = out.get("raw_payload") or {}
    if isinstance(raw, str):
        import json as _json
        try:
            raw = _json.loads(raw)
        except Exception:
            raw = {}

    # Inner trigger sub-dict (scanner sometimes nests it here)
    trigger_block = raw.get("trigger") or {}

    def _pick(current, *keys) -> float | None:
        if current is not None and float(current) > 0:
            return float(current)
        for k in keys:
            v = trigger_block.get(k) or raw.get(k)
            if v is not None:
                try:
                    f = float(v)
                    if f > 0:
                        return f
                except (TypeError, ValueError):
                    pass
        return None

    out["entry_trigger"] = _pick(
        out.get("entry_trigger"),
        "entry_trigger", "entry_price", "trigger_price", "price",
    )
    out["stop_price"] = _pick(
        out.get("stop_price"),
        "stop_price", "stop_underlying", "stop",
    )
    out["target_price"] = _pick(
        out.get("target_price"),
        "target_price", "target_underlying", "target",
    )
    out["underlying_at_signal"] = _pick(
        out.get("underlying_at_signal"),
        "underlying_at_signal", "underlying_price", "underlying",
    )
    return out


def _validate_trigger_geometry(
    side: str,
    entry_trigger: float | None,
    stop_price: float | None,
    target_price: float | None,
) -> tuple[bool, str]:
    """
    Returns (is_valid, reason).

    CALL: target > entry > stop   (target above entry, stop below entry)
    PUT:  stop > entry > target   (stop above entry, target below entry)

    Signals with no geometry (all None) are passed through without blocking —
    the watcher validates at execution time.
    """
    side = (side or "").upper().strip()
    if entry_trigger is None and stop_price is None and target_price is None:
        return True, "no_geometry_present"

    if entry_trigger is None:
        return False, "entry_trigger_missing"
    if stop_price is None:
        return False, "stop_price_missing"
    if target_price is None:
        return False, "target_price_missing"

    if side == "CALL":
        if not (target_price > entry_trigger > stop_price):
            return False, (
                f"call_geometry_invalid: target={target_price} entry={entry_trigger} "
                f"stop={stop_price} — expected target>entry>stop"
            )
    elif side == "PUT":
        if not (stop_price > entry_trigger > target_price):
            return False, (
                f"put_geometry_invalid: stop={stop_price} entry={entry_trigger} "
                f"target={target_price} — expected stop>entry>target"
            )
    else:
        return False, f"unknown_side:{side}"

    return True, "ok"


def _fetch_watching_signals(client_id: str) -> list[dict]:
    """Fetch ap_signals rows with decision_status=WATCHING and watcher_started_at IS NULL."""
    from ap.db import conn, run_with_retry

    def _q():
        with conn() as c:
            c.execute(
                """
                SELECT signal_id, client_email, ticker, side, score,
                       tier, pattern, timeframe, decision_status,
                       entry_trigger, stop_price, target_price,
                       underlying_at_signal, raw_payload, context_notes,
                       queued_at, watcher_started_at, canonical_signal_id
                FROM ap_signals
                WHERE client_email = %s
                  AND decision_status = 'WATCHING'
                  AND watcher_started_at IS NULL
                ORDER BY queued_at ASC NULLS LAST
                """,
                (client_id,),
            )
            rows = c.fetchall() or []
            cols = [d[0] for d in getattr(c, "description", [])]
            return [
                (dict(r) if isinstance(r, dict) else dict(zip(cols, r)))
                for r in rows
            ]

    try:
        return run_with_retry(_q) or []
    except Exception as exc:
        log.warning("_fetch_watching_signals failed client=%s: %s", client_id, exc)
        return []


def _insert_trade_queue_watching(
    *,
    client_id: str,
    signal_id: str,
    payload: dict,
    idempotency_key: str,
) -> bool:
    """
    Insert a WATCHING row into trade_queue.
    Returns True if inserted, False if duplicate or error.
    ON CONFLICT (idempotency_key) DO NOTHING prevents same-day duplicates.
    Session-scoped key (signal_id:client_id:YYYYMMDD) prevents old rejected
    rows from blocking today's insert.
    """
    import json as _json
    from ap.db import conn, run_with_retry

    def _ins():
        with conn() as c:
            c.execute(
                """
                INSERT INTO trade_queue
                    (client_id, signal_id, created_ts, status, payload, idempotency_key)
                VALUES
                    (%s, %s, NOW(), 'WATCHING', %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING id
                """,
                (
                    client_id,
                    signal_id,
                    _json.dumps(payload),
                    idempotency_key,
                ),
            )
            row = c.fetchone()
            return row is not None  # True = inserted, False = duplicate

    try:
        return bool(run_with_retry(_ins))
    except Exception as exc:
        log.error(
            "_insert_trade_queue_watching failed signal_id=%s client=%s: %s",
            signal_id, client_id, exc,
        )
        return False


def _mark_watcher_started(client_id: str, signal_id: str) -> None:
    """Set ap_signals.watcher_started_at = NOW() only after trade_queue insert succeeded."""
    from ap.db import conn, run_with_retry

    def _upd():
        with conn() as c:
            c.execute(
                """
                UPDATE ap_signals
                SET watcher_started_at = NOW()
                WHERE signal_id = %s
                  AND client_email = %s
                  AND watcher_started_at IS NULL
                """,
                (signal_id, client_id),
            )

    try:
        run_with_retry(_upd)
    except Exception as exc:
        log.warning(
            "_mark_watcher_started failed signal_id=%s client=%s: %s",
            signal_id, client_id, exc,
        )


def enqueue_watching_signals_to_trade_queue(
    client_id: str,
    execution_mode: str,
    *,
    trading_date: str | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> dict:
    """
    PR #183 — Core fix for paper morning handoff.

    Fetch ap_signals WHERE decision_status='WATCHING' AND watcher_started_at IS NULL
    for `client_id`, then insert a production-shaped WATCHING trade_queue row for
    each eligible signal. Update watcher_started_at only after a successful insert.

    Idempotency: uses signal_id:client_id:YYYYMMDD so old rejected trade_queue
    rows with the legacy key signal_id:client_id do NOT block today's inserts,
    while same-day duplicate calls are safely skipped.

    Returns a diagnostics dict with per-signal outcomes.
    """
    mode   = _normalize_mode(execution_mode)
    cid    = str(client_id or "").strip()
    tdate  = trading_date or _trading_date(now)
    tdate_nodash = tdate.replace("-", "")

    result: dict = {
        "client_id":        cid,
        "execution_mode":   mode,
        "trading_date":     tdate,
        "dry_run":          dry_run,
        "signals_found":    0,
        "inserted":         [],
        "skipped_duplicate":[],
        "rejected":         [],
        "errors":           [],
    }

    if not cid:
        result["errors"].append("client_id_required")
        return result

    # Resolve paper credentials up front so we fail loudly before touching signals.
    if mode == "paper":
        member = _fetch_client_member(cid)
        if member is None:
            result["errors"].append("client_member_not_found")
            return result
        account_id, access_token = _resolve_paper_creds(member)
        if not account_id or not access_token:
            result["errors"].append("paper_credentials_missing")
            log.error(
                "PR183 enqueue_watching_signals: paper_credentials_missing client=%s "
                "paper_account_id=%s paper_token=%s",
                cid,
                "set" if member.get("tradier_paper_account_id") else "MISSING",
                "set" if member.get("tradier_paper_access_token") else "MISSING",
            )
            return result
        tradier_meta = {
            "tradier_account_id": account_id,
            "tradier_access_token": access_token,
            "tradier_base_url": "https://sandbox.tradier.com",
        }
    else:
        # Live: credentials already on the runner/broker — don't re-resolve here.
        # The trade_queue payload for live omits raw credentials.
        tradier_meta = {"execution_mode": "live"}

    signals = _fetch_watching_signals(cid)
    result["signals_found"] = len(signals)
    if not signals:
        log.info(
            "PR183 enqueue_watching_signals: no WATCHING signals client=%s date=%s",
            cid, tdate,
        )
        return result

    for sig in signals:
        sig_id = str(sig.get("signal_id") or "").strip()
        ticker = str(sig.get("ticker") or "").strip()
        side   = str(sig.get("side")   or "").upper().strip()

        if not sig_id:
            result["errors"].append("signal_id_empty")
            continue

        # Hydrate missing trigger fields from raw_payload
        sig = _hydrate_trigger_from_raw_payload(sig)

        entry_trigger = sig.get("entry_trigger")
        stop_price    = sig.get("stop_price")
        target_price  = sig.get("target_price")

        # Validate geometry
        valid, geom_reason = _validate_trigger_geometry(
            side, entry_trigger, stop_price, target_price
        )
        if not valid:
            log.warning(
                "PR183 enqueue_watching_signals: geometry_rejected signal_id=%s "
                "client=%s ticker=%s side=%s reason=%s",
                sig_id, cid, ticker, side, geom_reason,
            )
            result["rejected"].append({
                "signal_id": sig_id, "ticker": ticker, "side": side,
                "reason": geom_reason,
            })
            continue

        # Session-scoped idempotency: prevents old rejected rows from blocking today
        idempotency_key = f"{sig_id}:{cid}:{tdate_nodash}"

        # Build the trade_queue payload — mirrors the shape the entry watcher expects
        payload = {
            "signal_id":            sig_id,
            "canonical_signal_id":  str(sig.get("canonical_signal_id") or sig_id),
            "client_id":            cid,
            "execution_mode":       mode,
            "ticker":               ticker,
            "side":                 side,
            "score":                float(sig.get("score") or 0),
            "tier":                 str(sig.get("tier") or "B"),
            "pattern":              str(sig.get("pattern") or ""),
            "timeframe":            str(sig.get("timeframe") or "1d"),
            "entry_trigger":        entry_trigger,
            "stop_price":           stop_price,
            "target_price":         target_price,
            "underlying_at_signal": sig.get("underlying_at_signal"),
            "context_notes":        str(sig.get("context_notes") or ""),
            "queued_at":            str(sig.get("queued_at") or ""),
            "handoff_source":       "pr183_morning_handoff",
            **tradier_meta,
        }

        if dry_run:
            result["inserted"].append({
                "signal_id": sig_id, "ticker": ticker, "side": side,
                "idempotency_key": idempotency_key, "dry_run": True,
                "geometry_reason": geom_reason,
            })
            log.info(
                "PR183 enqueue_watching_signals DRY_RUN signal_id=%s ticker=%s "
                "client=%s key=%s",
                sig_id, ticker, cid, idempotency_key,
            )
            continue

        inserted = _insert_trade_queue_watching(
            client_id=cid,
            signal_id=sig_id,
            payload=payload,
            idempotency_key=idempotency_key,
        )

        if inserted:
            _mark_watcher_started(cid, sig_id)
            result["inserted"].append({
                "signal_id": sig_id, "ticker": ticker, "side": side,
                "idempotency_key": idempotency_key,
            })
            log.info(
                "PR183 enqueue_watching_signals INSERTED signal_id=%s ticker=%s "
                "client=%s execution_mode=%s key=%s",
                sig_id, ticker, cid, mode, idempotency_key,
            )
        else:
            result["skipped_duplicate"].append({
                "signal_id": sig_id, "ticker": ticker, "side": side,
                "idempotency_key": idempotency_key,
            })
            log.info(
                "PR183 enqueue_watching_signals DUPLICATE signal_id=%s client=%s "
                "key=%s",
                sig_id, cid, idempotency_key,
            )

    return result


# ─── end PR #183 ──────────────────────────────────────────────────────────────


def _ensure_handoff_table() -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    from ap.db import conn, run_with_retry

    def _create():
        with conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS handoff_run_locks (
                    client_id TEXT NOT NULL,
                    execution_mode TEXT NOT NULL,
                    trading_date DATE NOT NULL,
                    stage TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'running',
                    last_run_at TIMESTAMPTZ,
                    last_success_at TIMESTAMPTZ,
                    last_error TEXT,
                    details JSONB NOT NULL DEFAULT '{}'::jsonb,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    PRIMARY KEY (client_id, execution_mode, trading_date, stage)
                )
                """
            )
            return True

    run_with_retry(_create)
    _TABLE_READY = True


def _load_handoff_run_lock(
    *,
    client_id: str,
    execution_mode: str,
    trading_date: str,
    stage: str,
) -> dict | None:
    from ap.db import conn, run_with_retry

    _ensure_handoff_table()

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT client_id, execution_mode, trading_date::text AS trading_date,
                       stage, status, last_run_at::text AS last_run_at,
                       last_success_at::text AS last_success_at,
                       last_error, details
                FROM handoff_run_locks
                WHERE client_id = %s
                  AND execution_mode = %s
                  AND trading_date = %s::date
                  AND stage = %s
                """,
                (client_id, execution_mode, trading_date, stage),
            )
            row = c.fetchone()
            if not row:
                return None
            cols = [d[0] for d in getattr(c, "description", [])]
            return dict(row) if isinstance(row, dict) else dict(zip(cols, row))

    return run_with_retry(_load)


def _upsert_handoff_run_lock(
    *,
    client_id: str,
    execution_mode: str,
    trading_date: str,
    stage: str,
    status: str,
    last_error: str | None,
    details: dict | None,
    mark_success: bool,
) -> None:
    from ap.db import conn, run_with_retry
    import json

    _ensure_handoff_table()

    def _write():
        with conn() as c:
            c.execute(
                """
                INSERT INTO handoff_run_locks (
                    client_id, execution_mode, trading_date, stage, status,
                    last_run_at, last_success_at, last_error, details, updated_at
                )
                VALUES (
                    %s, %s, %s::date, %s, %s,
                    NOW(),
                    CASE WHEN %s THEN NOW() ELSE NULL END,
                    %s,
                    %s::jsonb,
                    NOW()
                )
                ON CONFLICT (client_id, execution_mode, trading_date, stage)
                DO UPDATE SET
                    status = EXCLUDED.status,
                    last_run_at = EXCLUDED.last_run_at,
                    last_success_at = CASE
                        WHEN %s THEN EXCLUDED.last_run_at
                        ELSE handoff_run_locks.last_success_at
                    END,
                    last_error = EXCLUDED.last_error,
                    details = EXCLUDED.details,
                    updated_at = NOW()
                """,
                (
                    client_id,
                    execution_mode,
                    trading_date,
                    stage,
                    status,
                    mark_success,
                    last_error,
                    json.dumps(details or {}, default=str),
                    mark_success,
                ),
            )
            return True

    run_with_retry(_write)


def _count_state(client_id: str) -> dict:
    from ap.db import conn, run_with_retry

    def _snapshot():
        with conn() as c:
            c.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN status = 'WATCHING' THEN 1 ELSE 0 END), 0)::int AS watching_rows,
                    COALESCE(SUM(CASE WHEN status = 'NEW' THEN 1 ELSE 0 END), 0)::int AS new_rows
                FROM trade_queue
                WHERE client_id = %s
                """,
                (client_id,),
            )
            tq_row = c.fetchone()
            tq_cols = [d[0] for d in getattr(c, "description", [])]
            c.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN status = 'PENDING_TRIGGER' THEN 1 ELSE 0 END), 0)::int AS pending_trigger_rows
                FROM orders
                WHERE client_id = %s
                  AND kind = 'ENTRY'
                """,
                (client_id,),
            )
            orders_row = c.fetchone()
            orders_cols = [d[0] for d in getattr(c, "description", [])]
            tq = dict(tq_row or {}) if isinstance(tq_row, dict) else dict(zip(tq_cols, tq_row or ()))
            orders = dict(orders_row or {}) if isinstance(orders_row, dict) else dict(zip(orders_cols, orders_row or ()))
            tq.update(orders)
            return tq

    return run_with_retry(_snapshot) or {}


def _resolve_runner(client_id: str):
    from client_runner import _active_runners, _registry_lock

    with _registry_lock:
        return _active_runners.get(client_id)


def _expected_clients_by_mode() -> dict[str, list[str]]:
    from client_runner import _active_runners, _registry_lock

    paper: list[str] = []
    live: list[str] = []
    with _registry_lock:
        items = list(_active_runners.items())
    for email, runner in items:
        mode = _normalize_mode(getattr(runner, "mode", None) or getattr(getattr(runner, "master_control", None), "mode", None))
        if mode == "live":
            live.append(email)
        elif mode == "paper":
            paper.append(email)
    return {"paper": sorted(paper), "live": sorted(live)}


def _latest_handoff_rows(trading_date: str) -> list[dict]:
    from ap.db import conn, run_with_retry

    _ensure_handoff_table()

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT client_id, execution_mode, trading_date::text AS trading_date,
                       stage, status, last_run_at::text AS last_run_at,
                       last_success_at::text AS last_success_at,
                       last_error, details
                FROM handoff_run_locks
                WHERE trading_date = %s::date
                ORDER BY execution_mode, client_id, updated_at DESC
                """,
                (trading_date,),
            )
            rows = c.fetchall() or []
            cols = [d[0] for d in getattr(c, "description", [])]
            out = []
            for row in rows:
                out.append(dict(row) if isinstance(row, dict) else dict(zip(cols, row)))
            return out

    try:
        return run_with_retry(_load) or []
    except Exception:
        return []


def _has_unowned_pending_trigger_orders(client_id: str, entry_watcher, *, now: datetime | None = None) -> bool:
    from ap.db import conn, run_with_retry
    from datetime import timedelta, timezone
    import os

    if entry_watcher is None or not hasattr(entry_watcher, "has_order"):
        return True

    lookback_hours = int(os.getenv("STARTUP_WATCHER_RESEED_LOOKBACK_HOURS", str(_WATCHER_RESEED_LOOKBACK_HOURS)))
    cutoff_utc = (_now_et(now).astimezone(timezone.utc) - timedelta(hours=lookback_hours))

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT local_order_id
                FROM orders
                WHERE client_id = %s
                  AND kind = 'ENTRY'
                  AND status = 'PENDING_TRIGGER'
                  AND created_ts >= %s
                  AND broker_order_id IS NULL
                  AND submitted_ts IS NULL
                  AND filled_ts IS NULL
                ORDER BY created_ts ASC
                """,
                (client_id, cutoff_utc),
            )
            return c.fetchall() or []

    rows = run_with_retry(_load) or []
    for row in rows:
        local_order_id = str((row.get("local_order_id") if isinstance(row, dict) else row[0]) or "").strip()
        if not local_order_id:
            continue
        try:
            if not entry_watcher.has_order(local_order_id):
                return True
        except Exception:
            return True
    return False


def get_morning_handoff_health(now: datetime | None = None) -> dict:
    trading_date = _trading_date(now)
    rows = _latest_handoff_rows(trading_date)
    expected = _expected_clients_by_mode()

    by_mode: dict[str, dict[str, Any]] = {
        "paper": {"clients": {}, "last_run_at": None, "last_success_at": None},
        "live": {"clients": {}, "last_run_at": None, "last_success_at": None},
    }
    seen: set[tuple[str, str]] = set()

    for row in rows:
        mode = _normalize_mode(row.get("execution_mode"))
        client_id = str(row.get("client_id") or "").strip()
        if mode not in by_mode or not client_id:
            continue
        key = (mode, client_id)
        if key in seen:
            continue
        seen.add(key)
        by_mode[mode]["clients"][client_id] = {
            "last_run_at": row.get("last_run_at"),
            "last_success_at": row.get("last_success_at"),
            "last_stage": row.get("stage"),
            "last_error": row.get("last_error"),
            "status": row.get("status"),
        }
        if row.get("last_run_at") and (
            by_mode[mode]["last_run_at"] is None or str(row.get("last_run_at")) > str(by_mode[mode]["last_run_at"])
        ):
            by_mode[mode]["last_run_at"] = row.get("last_run_at")
        if row.get("last_success_at") and (
            by_mode[mode]["last_success_at"] is None or str(row.get("last_success_at")) > str(by_mode[mode]["last_success_at"])
        ):
            by_mode[mode]["last_success_at"] = row.get("last_success_at")

    after_guard = _after_929_et(now)
    live_missing = any(not by_mode["live"]["clients"].get(client_id, {}).get("last_success_at") for client_id in expected["live"])
    paper_missing_clients = [client_id for client_id in expected["paper"] if not by_mode["paper"]["clients"].get(client_id, {}).get("last_success_at")]

    by_mode["live"]["missing_after_929_et"] = bool(after_guard and live_missing)
    by_mode["paper"]["missing_after_929_et"] = bool(after_guard and bool(paper_missing_clients))
    by_mode["paper"]["missing_clients"] = paper_missing_clients
    return {
        "trading_date": trading_date,
        "paper": by_mode["paper"],
        "live": by_mode["live"],
    }


def run_morning_handoff_audit(
    *,
    client_id: str,
    execution_mode: str,
    stage: str,
    dry_run: bool = False,
    runner=None,
    now: datetime | None = None,
) -> dict:
    mode = _normalize_mode(execution_mode)
    client_id = str(client_id or "").strip()
    stage = str(stage or "").strip().lower()
    if not client_id:
        return {"ok": False, "error": "client_id_required", "stage": stage}
    if mode not in {"paper", "live"}:
        return {"ok": False, "error": "invalid_execution_mode", "stage": stage, "client_id": client_id}
    if stage not in {"startup", "post_overnight_reeval", "manual"}:
        return {"ok": False, "error": "invalid_stage", "stage": stage, "client_id": client_id}

    trading_date = _trading_date(now)
    existing = _load_handoff_run_lock(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
    )
    runner = runner or _resolve_runner(client_id)
    core = getattr(runner, "core", None) if runner is not None else None
    entry_watcher = getattr(core, "entry_watcher", None) if core else None

    can_skip_existing = bool(existing and str(existing.get("status") or "").lower() == "success" and existing.get("last_success_at"))
    if can_skip_existing and stage == "startup" and not dry_run:
        if _has_unowned_pending_trigger_orders(client_id, entry_watcher, now=now):
            can_skip_existing = False
    if can_skip_existing:
        return {
            "ok": True,
            "skipped": True,
            "reason": "handoff_already_succeeded_for_stage_today",
            "client_id": client_id,
            "execution_mode": mode,
            "stage": stage,
            "trading_date": trading_date,
            "last_success_at": existing.get("last_success_at"),
        }

    _upsert_handoff_run_lock(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
        status="running",
        last_error=None,
        details={"dry_run": dry_run},
        mark_success=False,
    )

    if runner is None:
        err = "runner_not_found"
        _upsert_handoff_run_lock(
            client_id=client_id,
            execution_mode=mode,
            trading_date=trading_date,
            stage=stage,
            status="failed",
            last_error=err,
            details={"dry_run": dry_run},
            mark_success=False,
        )
        return {"ok": False, "error": err, "client_id": client_id, "execution_mode": mode, "stage": stage}

    osm = getattr(runner, "order_state_machine", None)
    pm = getattr(runner, "position_manager", None)
    mc = getattr(runner, "master_control", None)
    core = getattr(runner, "core", None)
    broker = getattr(core, "broker", None) if core else getattr(runner, "broker", None)
    exit_engine = getattr(core, "exit_eng", None) if core else None
    entry_watcher = getattr(core, "entry_watcher", None) if core else None

    warnings: list[str] = []
    if osm is None:
        warnings.append("order_state_machine_missing")
        log.warning("morning_handoff missing OSM client=%s execution_mode=%s stage=%s", client_id, mode, stage)
    if entry_watcher is None:
        warnings.append("entry_watcher_missing")
        log.warning("morning_handoff missing entry_watcher client=%s execution_mode=%s stage=%s", client_id, mode, stage)
    if mc is None:
        warnings.append("master_control_missing")
        log.warning("morning_handoff missing master_control client=%s execution_mode=%s stage=%s", client_id, mode, stage)

    before = _count_state(client_id)
    recovery_result = {"client_id": client_id, "watchers_requeued": 0, "errors": []}
    enqueue_result: dict = {}
    ok = True
    error = None

    if not dry_run and warnings:
        ok = False
        error = ",".join(warnings)
    elif not dry_run:
        try:
            # PR #183: For post_overnight_reeval and manual stages, enqueue
            # ap_signals WATCHING rows into trade_queue before _reseed_watchers.
            # This is the step that was missing for paper accounts — live worked
            # because Jason's signals already had trade_queue WATCHING rows.
            if stage in ("post_overnight_reeval", "manual"):
                enqueue_result = enqueue_watching_signals_to_trade_queue(
                    client_id=client_id,
                    execution_mode=mode,
                    trading_date=trading_date,
                    dry_run=False,
                    now=now,
                )
                if enqueue_result.get("errors"):
                    # Hard errors (e.g. paper_credentials_missing) abort the handoff
                    # so we don't proceed to _reseed_watchers with no valid signals.
                    crit_errors = [
                        e for e in enqueue_result["errors"]
                        if e in ("paper_credentials_missing", "client_member_not_found")
                    ]
                    if crit_errors:
                        ok = False
                        error = crit_errors[0]
                        log.error(
                            "morning_handoff enqueue CRITICAL error client=%s stage=%s err=%s",
                            client_id, stage, crit_errors,
                        )

            if ok:
                from ap_recovery import APStartupRecovery

                recovery = APStartupRecovery(
                    client_id=client_id,
                    broker=broker,
                    osm=osm,
                    pm=pm,
                    master_control=mc,
                    exit_engine=exit_engine,
                    entry_watcher=entry_watcher,
                )
                recovery._reseed_watchers(recovery_result)
        except Exception as exc:  # noqa: BLE001
            ok = False
            error = str(exc)
            recovery_result.setdefault("errors", []).append(str(exc))
            log.error(
                "morning_handoff failed client=%s execution_mode=%s stage=%s err=%s",
                client_id, mode, stage, exc, exc_info=True,
            )

    after = _count_state(client_id)
    details = {
        "dry_run": dry_run,
        "before": before,
        "after": after,
        "watchers_requeued": int(recovery_result.get("watchers_requeued", 0) or 0),
        "warnings": warnings,
        "errors": list(recovery_result.get("errors") or []),
        "enqueue_result": enqueue_result,  # PR #183
    }
    _upsert_handoff_run_lock(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
        status="success" if ok else "failed",
        last_error=error,
        details=details,
        mark_success=ok,
    )
    return {
        "ok": ok,
        "client_id": client_id,
        "execution_mode": mode,
        "stage": stage,
        "trading_date": trading_date,
        "dry_run": dry_run,
        "watchers_requeued": int(recovery_result.get("watchers_requeued", 0) or 0),
        "before": before,
        "after": after,
        "warnings": warnings,
        "errors": list(recovery_result.get("errors") or []),
        "error": error,
        "enqueue_result": enqueue_result,  # PR #183: signals inserted/skipped/rejected
    }
