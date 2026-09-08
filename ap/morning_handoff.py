from __future__ import annotations

import logging
import os as _os
import uuid
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
    # P0 (monday-trade-flow-readiness): delegate to the canonical NYSE
    # calendar (ap.flatline_alarm.is_trading_day, single source of truth per
    # #282) so the handoff never runs a "trading day" pass on a full-closure
    # holiday. Fail-safe to legacy weekday-only logic on import failure.
    _dt = _now_et(now)
    try:
        from ap.flatline_alarm import is_trading_day as _nyse_is_trading_day
        return _nyse_is_trading_day(_dt.date())
    except Exception:
        return _dt.weekday() < 5


def _after_929_et(now: datetime | None = None) -> bool:
    dt = _now_et(now)
    return _is_market_day(dt) and (dt.hour > 9 or (dt.hour == 9 and dt.minute >= 29))


def _normalize_mode(value: str | None) -> str:
    return str(value or "").strip().lower()


def _autonomy_runtime_context(execution_mode: str | None = None) -> dict:
    commit_sha = (
        _os.getenv("RENDER_GIT_COMMIT")
        or _os.getenv("COMMIT_SHA")
        or _os.getenv("GITHUB_SHA")
        or "unknown"
    )
    mode = _normalize_mode(execution_mode)
    client_count = None
    try:
        expected = _expected_clients_by_mode()
        client_count = len(expected.get(mode, [])) if mode else sum(len(v) for v in expected.values())
    except Exception:
        client_count = None
    return {
        "commit_sha": str(commit_sha)[:12],
        "pod_id": _os.getenv("POD_ID", "").strip() or "unknown",
        "client_count": client_count,
        "execution_mode": mode,
    }


# ─────────────────────────────────────────────────────────────────────────────
# PR #183 amendment — ap_signals → trade_queue WATCHING handoff
# ─────────────────────────────────────────────────────────────────────────────

def _normalize_account_id(raw: str | None) -> str:
    """Strip chr(10)/chr(13)/chr(9) and whitespace from Tradier account IDs."""
    return str(raw or "").replace("\n", "").replace("\r", "").replace("\t", "").strip()


def _fetch_paper_members_for_pod(target_client_id: str | None = None) -> list[dict]:
    """
    Query members table for paper-eligible clients on this pod.
    Mirrors _fetch_active_members from client_runner.py but via psycopg2
    (no supabase client available in this context).

    Filters:
      approved=true, subscription_active=true, tradier_account_mode='paper'
      POD_ID env → execution_pod = POD_ID
      SINGLE_CLIENT_EMAIL / target_client_id → email = X

    Returns member dicts with credential fields.
    Account IDs are normalised (strip whitespace/newlines).

    On schema error (missing execution_pod column): falls back to core columns
    without pod filter, same safe-degraded pattern as client_runner.
    """
    from ap.db import conn, run_with_retry

    pod_id  = _os.getenv("POD_ID", "").strip()
    single  = (
        _os.getenv("SINGLE_CLIENT_EMAIL", "").strip()
        or (target_client_id or "")
    )

    _FULL_COLS = (
        "email, approved, subscription_active, "
        "tradier_account_mode, tradier_active_mode, execution_pod, "
        "tradier_paper_account_id, tradier_paper_access_token, "
        "tradier_account_id, tradier_access_token"
    )
    _CORE_COLS = (
        "email, approved, subscription_active, "
        "tradier_account_mode, tradier_active_mode, "
        "tradier_paper_account_id, tradier_paper_access_token, "
        "tradier_account_id, tradier_access_token"
    )

    def _run(cols: str, include_pod: bool):
        conditions = [
            "approved = TRUE",
            "subscription_active = TRUE",
            "tradier_account_mode = 'paper'",
        ]
        params: list = []
        if single:
            conditions.append("email = %s")
            params.append(single)
        if include_pod and pod_id:
            conditions.append("execution_pod = %s")
            params.append(pod_id)
        where = " AND ".join(conditions)
        with conn() as c:
            c.execute(f"SELECT {cols} FROM members WHERE {where}", params)
            rows = c.fetchall() or []
            col_names = [d[0] for d in getattr(c, "description", [])]
            out = []
            for row in rows:
                m = dict(row) if isinstance(row, dict) else dict(zip(col_names, row))
                m["tradier_paper_account_id"] = _normalize_account_id(
                    m.get("tradier_paper_account_id")
                )
                m["tradier_account_id"] = _normalize_account_id(
                    m.get("tradier_account_id")
                )
                out.append(m)
            return out

    try:
        return run_with_retry(lambda: _run(_FULL_COLS, include_pod=True)) or []
    except Exception as exc:
        if "execution_pod" in str(exc).lower() or "column" in str(exc).lower():
            log.warning(
                "PR183 _fetch_paper_members_for_pod: execution_pod column missing "
                "— falling back to core cols without pod filter: %s", exc
            )
            try:
                return run_with_retry(lambda: _run(_CORE_COLS, include_pod=False)) or []
            except Exception as exc2:
                log.error("PR183 _fetch_paper_members_for_pod core fetch failed: %s", exc2)
                return []
        log.error("PR183 _fetch_paper_members_for_pod failed: %s", exc)
        return []


def _resolve_paper_creds_from_member(member: dict) -> tuple[str | None, str | None]:
    """
    Return (account_id, access_token) for paper, or (None, None) if missing.
    Priority: tradier_paper_* over generic tradier_*.
    Account_id already normalised by _fetch_paper_members_for_pod.
    """
    account_id = member.get("tradier_paper_account_id") or member.get("tradier_account_id")
    token      = member.get("tradier_paper_access_token") or member.get("tradier_access_token")
    account_id = _normalize_account_id(account_id)
    token      = str(token or "").strip()
    if not account_id or not token:
        return None, None
    return account_id, token


def _is_shared_watch_signal_row(row: dict) -> bool:
    """
    Return True only for ap_signals WATCHING rows that match the existing
    shared overnight scanner/deferred source shapes already used by the bot.

    This keeps paper fanout scoped to shared overnight candidates instead of
    treating every same-day WATCHING row as globally shareable.
    """
    payload = row.get("raw_payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    reason_code = str(payload.get("reason_code") or "").strip().lower()
    stage = str(payload.get("stage") or "").strip().lower()
    human_reason = str(payload.get("human_reason") or "").strip().lower()
    context_notes = str(row.get("context_notes") or "").strip().lower()

    if reason_code == "market_closed_deferred":
        return True
    if stage == "contract_selection" and "deferred" in human_reason:
        return True
    if context_notes.startswith("post_market_blocked:"):
        return True
    return False


def _fetch_today_watching_signals(trading_date: str) -> tuple[list[dict] | None, str | None]:
    """
    Fetch shared ap_signals WATCHING rows with queued_at >= trading_date.
    Does NOT filter by client_email, but it DOES fail closed unless the row
    matches the existing shared overnight/deferred signal source shapes.
    Date scope prevents stale Jun-25/Jun-26 rows from being re-armed.

    Returns:
      (rows, None)  — success, rows may be empty list
      (None, error) — schema/query error: the error string must surface
                      in enqueue_result; must NOT be silently treated as
                      "no signals".
    """
    from ap.db import conn, run_with_retry
    import json as _json

    def _q():
        with conn() as c:
            c.execute(
                """
                SELECT signal_id,
                       client_email,
                       ticker,
                       side,
                       score,
                       tier,
                       pattern,
                       timeframe,
                       decision_status,
                       entry_trigger,
                       stop_price,
                       target_price,
                       underlying_at_signal,
                       raw_payload,
                       context_notes,
                       queued_at,
                       watcher_started_at
                FROM ap_signals
                WHERE decision_status = 'WATCHING'
                  AND queued_at IS NOT NULL
                  AND queued_at >= %s::date
                ORDER BY queued_at ASC NULLS LAST
                """,
                (trading_date,),
            )
            rows = c.fetchall() or []
            cols = [d[0] for d in getattr(c, "description", [])]
            out = []
            for row in rows:
                r = dict(row) if isinstance(row, dict) else dict(zip(cols, row))
                if isinstance(r.get("raw_payload"), str):
                    try:
                        r["raw_payload"] = _json.loads(r["raw_payload"])
                    except Exception:
                        r["raw_payload"] = {}
                if _is_shared_watch_signal_row(r):
                    out.append(r)
            return out

    try:
        rows = run_with_retry(_q) or []
        return rows, None
    except Exception as exc:
        msg = str(exc)
        log.error(
            "PR183 _fetch_today_watching_signals SCHEMA ERROR trading_date=%s: %s",
            trading_date, msg,
        )
        return None, f"fetch_watching_signals_failed:{msg}"


def _hydrate_trigger_from_raw_payload(signal_row: dict) -> dict:
    """
    Fill entry_trigger / stop_price / target_price / underlying_at_signal
    from raw_payload when ap_signals columns are NULL.
    Checks flat keys and nested raw_payload.trigger sub-dict.
    """
    out = dict(signal_row)
    raw = out.get("raw_payload") or {}
    trigger_block = raw.get("trigger") or {}

    def _pick(current, *keys) -> float | None:
        try:
            if current is not None and float(current) > 0:
                return float(current)
        except (TypeError, ValueError):
            pass
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
    CALL: target > entry > stop
    PUT:  stop  > entry > target
    Missing geometry (any value None after hydration) is a hard block.
    """
    side = (side or "").upper().strip()
    if entry_trigger is None:
        return False, "entry_trigger_missing_after_hydration"
    if stop_price is None:
        return False, "stop_price_missing_after_hydration"
    if target_price is None:
        return False, "target_price_missing_after_hydration"

    if side == "CALL":
        if not (target_price > entry_trigger > stop_price):
            return False, (
                f"call_geometry_invalid: target={target_price} entry={entry_trigger} "
                f"stop={stop_price} — required target>entry>stop"
            )
    elif side == "PUT":
        if not (stop_price > entry_trigger > target_price):
            return False, (
                f"put_geometry_invalid: stop={stop_price} entry={entry_trigger} "
                f"target={target_price} — required stop>entry>target"
            )
    else:
        return False, f"unknown_side:{side}"

    return True, "ok"


def _update_ap_signal_blocked(signal_id: str, reason: str) -> None:
    """
    Mark ap_signals as blocked_at_breach and append handoff_rejected:<reason>
    to context_notes. Called when geometry validation fails.
    """
    from ap.db import conn, run_with_retry

    def _upd():
        with conn() as c:
            c.execute(
                """
                UPDATE ap_signals
                SET decision_status = 'blocked_at_breach',
                    context_notes   = COALESCE(context_notes, '') ||
                                      %s
                WHERE signal_id = %s
                  AND decision_status = 'WATCHING'
                """,
                (f"\nhandoff_rejected:{reason}", signal_id),
            )

    try:
        run_with_retry(_upd)
    except Exception as exc:
        log.warning(
            "PR183 _update_ap_signal_blocked failed signal_id=%s: %s", signal_id, exc
        )


def _watching_row_exists(client_id: str, signal_id: str) -> bool:
    """
    True if trade_queue already has a WATCHING row for (client_id, signal_id).
    Guards against duplicate rows regardless of idempotency key format.
    Fails closed on error (returns True = skip rather than duplicate).
    """
    from ap.db import conn, run_with_retry

    def _q():
        with conn() as c:
            c.execute(
                "SELECT 1 FROM trade_queue "
                "WHERE client_id=%s AND signal_id=%s AND status='WATCHING' "
                "LIMIT 1",
                (client_id, signal_id),
            )
            return c.fetchone() is not None

    try:
        return bool(run_with_retry(_q))
    except Exception as exc:
        log.warning(
            "PR183 _watching_row_exists failed — failing closed signal_id=%s client=%s: %s",
            signal_id, client_id, exc,
        )
        return True


def _insert_trade_queue_watching(
    *,
    client_id: str,
    signal_id: str,
    payload: dict,
    idempotency_key: str,
) -> str:
    """
    Returns 'inserted' | 'duplicate' | 'error'.
    ON CONFLICT (idempotency_key) DO NOTHING → 'duplicate'.
    Any exception → 'error' (caller must NOT mark watcher_started_at).
    """
    import json as _json
    from ap.db import conn, run_with_retry

    def _ins():
        with conn() as c:
            c.execute(
                """
                INSERT INTO trade_queue
                    (client_id, signal_id, created_ts, status, payload, idempotency_key)
                VALUES (%s, %s, NOW(), 'WATCHING', %s, %s)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING id
                """,
                (client_id, signal_id, _json.dumps(payload), idempotency_key),
            )
            return c.fetchone() is not None

    try:
        return "inserted" if run_with_retry(_ins) else "duplicate"
    except Exception as exc:
        log.error(
            "PR183 _insert_trade_queue_watching error signal_id=%s client=%s: %s",
            signal_id, client_id, exc,
        )
        return "error"


# NOTE:
# ap_signals is shared setup truth.
# trade_queue is per-client execution truth.
# This timestamp only marks that the shared source signal has been handed off.
# Per-client watcher state must be derived from trade_queue/orders/entry_watcher,
# not ap_signals.watcher_started_at.
def _mark_shared_signal_handoff_started(source_client_email: str, signal_id: str) -> None:
    """Mark the shared ap_signals source row as handed off.

    This does NOT mean the target client's entry_watcher is armed.
    Per-client watcher truth lives in trade_queue/orders/entry_watcher.
    """
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
                (signal_id, source_client_email),
            )

    try:
        run_with_retry(_upd)
    except Exception as exc:
        log.warning(
            "PR183 _mark_shared_signal_handoff_started failed signal_id=%s client=%s: %s",
            signal_id, source_client_email, exc,
        )


def enqueue_watching_signals_to_trade_queue(
    target_client_id: str,
    execution_mode: str,
    *,
    trading_date: str | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> dict:
    """
    PR #183 — Core fix for paper morning handoff.

    Fanout: query ALL today's WATCHING ap_signals (no client_email filter),
    create one trade_queue WATCHING row per signal for `target_client_id`.
    A shared scanner signal is thus distributed to every eligible paper client
    on this pod, not just the client it was originally scanned for.

    Safety:
      - Credential existence verified from members table (not clients table).
        Raw tokens never stored in trade_queue.payload — broker resolves by
        client_id + execution_mode at execution time.
      - Date-scoped: queued_at >= trading_date prevents stale row re-arm.
      - Schema errors surface visibly in enqueue_result, never silently [] .
      - Missing geometry after hydration hard-blocks the signal and writes
        blocked_at_breach to ap_signals.
      - Idempotency: source_signal_id:target_client_id:YYYYMMDD so old rows
        with different key formats don't block today.
      - watcher_started_at set after insert confirmed OR on same-day duplicate.
        Never set on insert error.
    """
    mode   = _normalize_mode(execution_mode)
    cid    = str(target_client_id or "").strip()
    tdate  = trading_date or _trading_date(now)
    tdate_nodash = tdate.replace("-", "")

    result: dict = {
        "client_id":         cid,
        "execution_mode":    mode,
        "trading_date":      tdate,
        "dry_run":           dry_run,
        "signals_found":     0,
        "inserted":          [],
        "skipped_duplicate": [],
        "rejected":          [],
        "errors":            [],
    }

    if not cid:
        result["errors"].append("client_id_required")
        return result
    if mode not in {"paper", "live"}:
        result["errors"].append("metadata_invalid:unknown_execution_mode")
        return result

    # ── Step 1: Verify paper credentials from members table ───────────────────
    # We verify credentials exist but DO NOT store them in trade_queue.payload.
    # The broker/watcher resolves credentials at execution time via client_id +
    # execution_mode. Storing raw tokens in the payload/logs is a security risk.
    if mode == "paper":
        members = _fetch_paper_members_for_pod(target_client_id=cid)
        target_member = next(
            (m for m in members if str(m.get("email") or "").strip() == cid),
            None,
        )
        if target_member is None:
            result["errors"].append("member_not_found_in_pod")
            log.error(
                "PR183 enqueue_watching_signals: member_not_found client=%s "
                "pod=%s", cid, _os.getenv("POD_ID", "")
            )
            return result

        account_id, _token = _resolve_paper_creds_from_member(target_member)
        if not account_id or not _token:
            result["errors"].append("paper_credentials_missing")
            log.error(
                "PR183 enqueue_watching_signals: paper_credentials_missing client=%s "
                "paper_acct=%s paper_tok=%s",
                cid,
                "set" if target_member.get("tradier_paper_account_id") else "MISSING",
                "set" if target_member.get("tradier_paper_access_token") else "MISSING",
            )
            return result
        # Credentials verified. Do not persist to payload.

    # ── Step 2: Fetch today's WATCHING signals (all clients, date-scoped) ─────
    signals, fetch_err = _fetch_today_watching_signals(tdate)
    if fetch_err is not None:
        # Schema/query error: surface visibly, fail the handoff
        result["errors"].append(fetch_err)
        log.error(
            "PR183 enqueue_watching_signals: signal fetch failed client=%s err=%s",
            cid, fetch_err,
        )
        return result

    result["signals_found"] = len(signals or [])
    if not signals:
        return result

    # ── Step 3: Process each signal ───────────────────────────────────────────
    for sig in signals:
        src_signal_id        = str(sig.get("signal_id") or "").strip()
        src_client_email     = str(sig.get("client_email") or "").strip()
        ticker               = str(sig.get("ticker") or "").strip()
        side                 = str(sig.get("side") or "").upper().strip()

        if not src_signal_id:
            result["errors"].append("signal_id_empty")
            continue

        # Hydrate missing trigger fields from raw_payload
        sig = _hydrate_trigger_from_raw_payload(sig)

        entry_trigger = sig.get("entry_trigger")
        stop_price    = sig.get("stop_price")
        target_price  = sig.get("target_price")

        # Hard-block on missing / invalid geometry after hydration
        valid, geom_reason = _validate_trigger_geometry(
            side, entry_trigger, stop_price, target_price
        )
        if not valid:
            log.warning(
                "PR183 enqueue_watching_signals: geometry_rejected "
                "signal_id=%s ticker=%s side=%s reason=%s",
                src_signal_id, ticker, side, geom_reason,
            )
            if not dry_run:
                _update_ap_signal_blocked(src_signal_id, geom_reason)
            result["rejected"].append({
                "signal_id": src_signal_id,
                "source_client_email": src_client_email,
                "ticker": ticker,
                "side": side,
                "reason": geom_reason,
            })
            continue

        # Session-scoped idempotency key (fanout-aware: includes target client)
        idempotency_key = f"{src_signal_id}:{cid}:{tdate_nodash}"

        # Guard: existing WATCHING row regardless of key format
        if not dry_run and _watching_row_exists(cid, src_signal_id):
            # Duplicate — still mark the source signal's handoff timestamp so
            # repeated handoff runs don't re-process it (NOT a per-client watcher state).
            _mark_shared_signal_handoff_started(src_client_email, src_signal_id)
            result["skipped_duplicate"].append({
                "signal_id":           src_signal_id,
                "source_client_email": src_client_email,
                "ticker": ticker,
                "side": side,
                "idempotency_key": idempotency_key,
                "reason": "watching_row_already_exists",
            })
            continue

        # Build payload — NO raw credentials stored here
        payload = {
            # canonical_signal_id not on ap_signals — use signal_id as canonical
            "signal_id":                  src_signal_id,
            "client_id":                  cid,
            "execution_mode":             mode,
            "source_signal_client_email": src_client_email,
            "source_signal_id":           src_signal_id,
            "ticker":                     ticker,
            "side":                       side,
            "score":                      float(sig.get("score") or 0),
            "tier":                       str(sig.get("tier") or "B"),
            "pattern":                    str(sig.get("pattern") or ""),
            "timeframe":                  str(sig.get("timeframe") or "1d"),
            "entry_trigger":              entry_trigger,
            "stop_price":                 stop_price,
            "target_price":               target_price,
            "underlying_at_signal":       sig.get("underlying_at_signal"),
            "context_notes":              str(sig.get("context_notes") or ""),
            "queued_at":                  str(sig.get("queued_at") or ""),
            "handoff_source":             "pr183_morning_handoff",
        }

        if dry_run:
            result["inserted"].append({
                "signal_id":           src_signal_id,
                "source_client_email": src_client_email,
                "ticker": ticker, "side": side,
                "idempotency_key": idempotency_key,
                "dry_run": True,
            })
            continue

        insert_outcome = _insert_trade_queue_watching(
            client_id=cid,
            signal_id=src_signal_id,
            payload=payload,
            idempotency_key=idempotency_key,
        )

        if insert_outcome == "inserted":
            # Mark the source ap_signals row's handoff timestamp
            # (NOT a per-client watcher state — see function docstring)
            _mark_shared_signal_handoff_started(src_client_email, src_signal_id)
            result["inserted"].append({
                "signal_id":           src_signal_id,
                "source_client_email": src_client_email,
                "ticker": ticker, "side": side,
                "idempotency_key": idempotency_key,
            })
            log.info(
                "PR183 INSERTED signal_id=%s ticker=%s -> client=%s key=%s",
                src_signal_id, ticker, cid, idempotency_key,
            )
        elif insert_outcome == "duplicate":
            # ON CONFLICT — still mark the source signal's handoff timestamp
            # so the loop is idempotent (NOT a per-client watcher state).
            _mark_shared_signal_handoff_started(src_client_email, src_signal_id)
            result["skipped_duplicate"].append({
                "signal_id":           src_signal_id,
                "source_client_email": src_client_email,
                "ticker": ticker, "side": side,
                "idempotency_key": idempotency_key,
                "reason": "on_conflict_do_nothing",
            })
        else:  # "error"
            # Do NOT mark watcher_started_at on error
            result["errors"].append(
                f"insert_error:signal_id={src_signal_id}:client={cid}"
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
    attempt_generation: int | None = None,
    attempt_id: str | None = None,
) -> bool:
    from ap.db import conn, run_with_retry
    import json

    _ensure_handoff_table()
    if attempt_generation is not None and (
        isinstance(attempt_generation, bool)
        or not isinstance(attempt_generation, int)
        or attempt_generation <= 0
    ):
        return False
    if attempt_id is not None and not str(attempt_id).strip():
        return False
    durable_details = dict(details or {}) if isinstance(details, dict) else {}
    if attempt_generation is not None:
        durable_details["attempt_generation"] = int(attempt_generation)
    if attempt_id is not None:
        durable_details["attempt_id"] = str(attempt_id).strip()

    def _write():
        with conn() as c:
            cur = c.execute(
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
                WHERE
                    (
                        NOT (handoff_run_locks.details ? 'attempt_generation')
                        AND NOT (EXCLUDED.details ? 'attempt_generation')
                    )
                    OR (
                        (EXCLUDED.details->>'attempt_generation') ~ '^[0-9]+$'
                        AND (
                            NOT (handoff_run_locks.details ? 'attempt_generation')
                            OR CASE
                                WHEN handoff_run_locks.details->>'attempt_generation' ~ '^[0-9]+$'
                                THEN (handoff_run_locks.details->>'attempt_generation')::bigint
                                ELSE 0
                            END < (EXCLUDED.details->>'attempt_generation')::bigint
                            OR (
                                CASE
                                    WHEN handoff_run_locks.details->>'attempt_generation' ~ '^[0-9]+$'
                                    THEN (handoff_run_locks.details->>'attempt_generation')::bigint
                                    ELSE 0
                                END = (EXCLUDED.details->>'attempt_generation')::bigint
                                AND (
                                    NOT (handoff_run_locks.details ? 'attempt_id')
                                    OR COALESCE(handoff_run_locks.details->>'attempt_id', '')
                                       = COALESCE(EXCLUDED.details->>'attempt_id', '')
                                )
                            )
                        )
                    )
                """,
                (
                    client_id,
                    execution_mode,
                    trading_date,
                    stage,
                    status,
                    mark_success,
                    last_error,
                    json.dumps(durable_details, default=str),
                    mark_success,
                ),
            )
            return int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0)

    return bool(run_with_retry(_write))


def _claim_overnight_reeval_attempt(
    *,
    client_id: str,
    execution_mode: str,
    trading_date: str,
    session_key: str,
) -> dict:
    """Atomically claim the next durable overnight authority generation.

    The claim is the invalidation boundary: it records a new ``running``
    attempt and clears the prior overnight/post-handoff success before the
    engine can mutate a queue, order, ledger, or watcher.  The row lock plus
    ``ON CONFLICT DO NOTHING`` makes two runners converge on one monotonic
    generation instead of racing a local counter.
    """
    from ap.db import conn, run_with_retry
    import json

    _ensure_handoff_table()
    client_id = str(client_id or "").strip()
    execution_mode = _normalize_mode(execution_mode)
    trading_date = str(trading_date or "").strip()
    session_key = str(session_key or "").strip()
    if not client_id or execution_mode not in {"live", "paper"} or not trading_date or not session_key:
        raise ValueError("overnight_attempt_identity_incomplete")

    def _new_details(attempt_id: str, generation: int, attempt_count: int) -> dict:
        return {
            "client_id": client_id,
            "execution_mode": execution_mode,
            "trading_date": trading_date,
            "overnight_reeval_session_key": session_key,
            "attempt_id": attempt_id,
            "attempt_generation": generation,
            "attempt_count": attempt_count,
            "result_class": "ATTEMPT_IN_PROGRESS",
            "completed": False,
            "retryable": True,
            "retry_reason": "attempt_in_progress",
            "source_lookup_partial": True,
            "source_identity_conflict": False,
        }

    def _int_detail(details: dict, key: str) -> int:
        value = details.get(key) if isinstance(details, dict) else None
        if value is None:
            return 0
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"overnight_attempt_{key}_malformed")
        return max(0, value)

    def _claim():
        with conn() as c:
            # Establish the row if this is the first attempt. A concurrent
            # first writer loses this insert and then takes the row lock below
            # after the winner commits.
            first_id = f"{trading_date}:1:{uuid.uuid4().hex}"
            first_details = _new_details(first_id, 1, 1)
            cur = c.execute(
                """
                INSERT INTO handoff_run_locks (
                    client_id, execution_mode, trading_date, stage, status,
                    last_run_at, last_success_at, last_error, details, updated_at
                )
                VALUES (%s, %s, %s::date, 'overnight_reeval', 'running',
                        NOW(), NULL, %s, %s::jsonb, NOW())
                ON CONFLICT (client_id, execution_mode, trading_date, stage)
                DO NOTHING
                """,
                (
                    client_id,
                    execution_mode,
                    trading_date,
                    "OVERNIGHT_REEVAL_IN_PROGRESS",
                    json.dumps(first_details, default=str),
                ),
            )
            inserted = int(getattr(cur, "rowcount", getattr(c, "rowcount", 0)) or 0) > 0
            if inserted:
                generation = 1
                attempt_count = 1
                attempt_id = first_id
            else:
                c.execute(
                    """
                    SELECT details
                    FROM handoff_run_locks
                    WHERE client_id = %s
                      AND execution_mode = %s
                      AND trading_date = %s::date
                      AND stage = 'overnight_reeval'
                    FOR UPDATE
                    """,
                    (client_id, execution_mode, trading_date),
                )
                row = c.fetchone()
                raw_details = row.get("details") if isinstance(row, dict) else (row[0] if row else {})
                if isinstance(raw_details, str):
                    try:
                        raw_details = json.loads(raw_details)
                    except Exception:
                        raw_details = {}
                prior_details = raw_details if isinstance(raw_details, dict) else {}
                generation = _int_detail(prior_details, "attempt_generation") + 1
                attempt_count = _int_detail(prior_details, "attempt_count") + 1
                attempt_id = f"{trading_date}:{generation}:{uuid.uuid4().hex}"
                details = _new_details(attempt_id, generation, attempt_count)
                c.execute(
                    """
                    UPDATE handoff_run_locks
                    SET status = 'running',
                        last_run_at = NOW(),
                        last_success_at = NULL,
                        last_error = %s,
                        details = %s::jsonb,
                        updated_at = NOW()
                    WHERE client_id = %s
                      AND execution_mode = %s
                      AND trading_date = %s::date
                      AND stage = 'overnight_reeval'
                    """,
                    (
                        "OVERNIGHT_REEVAL_IN_PROGRESS",
                        json.dumps(details, default=str),
                        client_id,
                        execution_mode,
                        trading_date,
                    ),
                )

            # A new overnight attempt supersedes any post-overnight success
            # from an older generation. Readiness must not discover that stale
            # stage and re-authorize LIVE while this attempt is unresolved.
            c.execute(
                """
                UPDATE handoff_run_locks
                SET status = 'superseded',
                    last_success_at = NULL,
                    last_error = 'OVERNIGHT_ATTEMPT_SUPERSEDED',
                    details = COALESCE(details, '{}'::jsonb)
                              || jsonb_build_object(
                                   'superseded_by_attempt_id', %s,
                                   'superseded_by_attempt_generation', %s
                                 ),
                    updated_at = NOW()
                WHERE client_id = %s
                  AND execution_mode = %s
                  AND trading_date = %s::date
                  AND stage = 'post_overnight_reeval'
                  AND LOWER(COALESCE(status, '')) = 'success'
                  AND (
                      NOT (COALESCE(details, '{}'::jsonb) ? 'attempt_generation')
                      OR CASE
                          WHEN details->>'attempt_generation' ~ '^[0-9]+$'
                          THEN (details->>'attempt_generation')::bigint
                          ELSE 0
                      END < %s
                  )
                """,
                (
                    attempt_id,
                    generation,
                    client_id,
                    execution_mode,
                    trading_date,
                    generation,
                ),
            )
            return {
                "client_id": client_id,
                "execution_mode": execution_mode,
                "trading_date": trading_date,
                "session_key": session_key,
                "attempt_id": attempt_id,
                "attempt_generation": generation,
                "attempt_count": attempt_count,
            }

    return run_with_retry(_claim)


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
    overnight_attempt: dict | None = None,
) -> dict:
    mode = _normalize_mode(execution_mode)
    client_id = str(client_id or "").strip()
    stage = str(stage or "").strip().lower()
    overnight_attempt = dict(overnight_attempt or {})
    _attempt_id = str(overnight_attempt.get("attempt_id") or "").strip() or None
    _raw_attempt_generation = overnight_attempt.get("attempt_generation")
    _attempt_generation = (
        _raw_attempt_generation
        if isinstance(_raw_attempt_generation, int)
        and not isinstance(_raw_attempt_generation, bool)
        and _raw_attempt_generation > 0
        else None
    )
    _attempt_session = (
        str(
            overnight_attempt.get("overnight_reeval_session_key")
            or overnight_attempt.get("session_key")
            or ""
        ).strip()
        or None
    )
    _attempt_details = {}
    if _attempt_id is not None:
        _attempt_details["attempt_id"] = _attempt_id
    if _attempt_generation is not None:
        _attempt_details["attempt_generation"] = _attempt_generation
    if _attempt_session is not None:
        _attempt_details["overnight_reeval_session_key"] = _attempt_session
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

    autonomy_ctx = _autonomy_runtime_context(mode)
    log.info(
        "CLIENT_HANDOFF_STARTED client_id=%s execution_mode=%s stage=%s trading_date=%s "
        "commit_sha=%s pod_id=%s client_count=%s",
        client_id,
        mode,
        stage,
        trading_date,
        autonomy_ctx["commit_sha"],
        autonomy_ctx["pod_id"],
        autonomy_ctx["client_count"],
    )

    can_skip_existing = bool(existing and str(existing.get("status") or "").lower() == "success" and existing.get("last_success_at"))
    if can_skip_existing and stage == "startup" and not dry_run:
        # Startup is process ownership, not only a daily job event. A prior
        # success may predate a restart, deployment, or partial handoff crash,
        # so it cannot prove this runtime owns watchers or retry consumers.
        can_skip_existing = False
    if can_skip_existing:
        summary = {
            "client_id": client_id,
            "execution_mode": mode,
            "overnight_rows_found": 0,
            "morning_rows_accepted": 0,
            "watchers_restored": 0,
            "deferred_retries_restored": 0,
            "preexisting_pending_trigger_rows": 0,
            "pending_trigger_watchers_rearmed": 0,
            "deferred_lifecycles_recovered": 0,
            "already_verified_owner_rows": 0,
            "orders_with_verified_owner": 0,
            "orders_missing_runtime_owner": 0,
            "watching_rows_reset": 0,
            "errors": [],
            "skipped": True,
        }
        log.info(
            "CLIENT_HANDOFF_COMPLETE client_id=%s execution_mode=%s stage=%s status=skipped "
            "commit_sha=%s pod_id=%s client_count=%s summary=%s",
            client_id,
            mode,
            stage,
            autonomy_ctx["commit_sha"],
            autonomy_ctx["pod_id"],
            autonomy_ctx["client_count"],
            summary,
        )
        return {
            "ok": True,
            "skipped": True,
            "reason": "handoff_already_succeeded_for_stage_today",
            "client_id": client_id,
            "execution_mode": mode,
            "stage": stage,
            "trading_date": trading_date,
            "last_success_at": existing.get("last_success_at"),
            "summary": summary,
        }

    _upsert_handoff_run_lock(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
        status="running",
        last_error=None,
        details={"dry_run": dry_run, **_attempt_details},
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
            details={"dry_run": dry_run, **_attempt_details},
            mark_success=False,
        )
        summary = {
            "client_id": client_id,
            "execution_mode": mode,
            "overnight_rows_found": 0,
            "morning_rows_accepted": 0,
            "watchers_restored": 0,
            "deferred_retries_restored": 0,
            "preexisting_pending_trigger_rows": 0,
            "pending_trigger_watchers_rearmed": 0,
            "deferred_lifecycles_recovered": 0,
            "already_verified_owner_rows": 0,
            "orders_with_verified_owner": 0,
            "orders_missing_runtime_owner": 0,
            "watching_rows_reset": 0,
            "errors": [err],
        }
        log.info(
            "CLIENT_HANDOFF_COMPLETE client_id=%s execution_mode=%s stage=%s status=failed "
            "commit_sha=%s pod_id=%s client_count=%s summary=%s",
            client_id,
            mode,
            stage,
            autonomy_ctx["commit_sha"],
            autonomy_ctx["pod_id"],
            autonomy_ctx["client_count"],
            summary,
        )
        return {
            "ok": False,
            "error": err,
            "client_id": client_id,
            "execution_mode": mode,
            "stage": stage,
            "summary": summary,
        }

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

    # ── P0 (monday-trade-flow-readiness): pre-reseed WATCHING readiness pass ──
    # Runs BEFORE fanout/reseed so stale/holiday/orphaned WATCHING rows are
    # classified out of the pipe before anything is re-armed. Classification
    # only — never resets to NEW, never re-triggers; live_no_replay_policy is
    # untouched. Gated on AP_WATCHING_READINESS_PASS=1 (no-op when unset).
    # dry_run mirrors the handoff's own dry_run so the audit endpoint shows a
    # full preview with zero writes. A readiness failure is recorded in
    # details but does NOT abort the handoff: a cleanup error must never
    # block watcher reseed on a live trading morning.
    # NOTE: readiness issues go into readiness_warnings (details-only), NOT
    # into `warnings` — that list hard-fails the handoff below, and a cleanup
    # error must never block watcher reseed on a live trading morning.
    readiness_result: dict = {}
    readiness_warnings: list[str] = []
    try:
        from ap.watching_readiness import run_watching_readiness_pass
        readiness_result = run_watching_readiness_pass(
            client_id,
            execution_mode=mode,
            dry_run=dry_run,
            now=now,
        )
        if not readiness_result.get("ok", True):
            readiness_warnings.append(
                f"watching_readiness_failed:{readiness_result.get('error')}"
            )
            log.error(
                "morning_handoff watching_readiness failed client=%s stage=%s err=%s",
                client_id, stage, readiness_result.get("error"),
            )
    except Exception as _readiness_exc:  # noqa: BLE001
        readiness_warnings.append(f"watching_readiness_exception:{_readiness_exc}")
        log.error(
            "morning_handoff watching_readiness exception client=%s stage=%s err=%s",
            client_id, stage, _readiness_exc, exc_info=True,
        )

    if not dry_run and warnings:
        ok = False
        error = ",".join(warnings)
    elif not dry_run:
        # Fan-out and durable watcher recovery are independent. A temporary
        # failure while discovering new paper signals must remain visible, but
        # it must never strand PENDING_TRIGGER work that already exists.
        if mode == "paper" and stage in ("startup", "post_overnight_reeval", "manual"):
            try:
                enqueue_result = enqueue_watching_signals_to_trade_queue(
                    target_client_id=client_id,
                    execution_mode=mode,
                    trading_date=trading_date,
                    dry_run=False,
                    now=now,
                )
                # Enqueue errors fail the audit visibly without suppressing
                # recovery of previously durable watcher work.
                if enqueue_result.get("errors"):
                    ok = False
                    error = str(enqueue_result["errors"][0])
                    log.error(
                        "morning_handoff enqueue failed client=%s stage=%s errors=%s",
                        client_id, stage, enqueue_result["errors"],
                    )
            except Exception as exc:  # noqa: BLE001
                ok = False
                error = str(exc)
                enqueue_result = {"errors": [f"enqueue_exception:{exc}"]}
                log.error(
                    "morning_handoff enqueue exception client=%s execution_mode=%s stage=%s err=%s",
                    client_id, mode, stage, exc, exc_info=True,
                )

        try:
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
            if error is None:
                error = str(exc)
            recovery_result.setdefault("errors", []).append(str(exc))
            log.error(
                "morning_handoff recovery failed client=%s execution_mode=%s stage=%s err=%s",
                client_id, mode, stage, exc, exc_info=True,
            )

    after = _count_state(client_id)
    accepted_rows = 0
    if enqueue_result:
        accepted_rows = len(enqueue_result.get("inserted") or []) + len(
            enqueue_result.get("skipped_duplicate") or []
        )
    summary_errors = list(warnings)
    if error and error not in summary_errors:
        summary_errors.append(error)
    summary_errors.extend(str(item) for item in (enqueue_result.get("errors") or []))
    summary_errors.extend(str(item) for item in (recovery_result.get("errors") or []))
    preexisting_pending_trigger_rows = int(before.get("pending_trigger_rows") or 0)
    pending_trigger_watchers_rearmed = int(
        recovery_result.get("pending_trigger_watchers_rearmed", 0) or 0
    )
    deferred_lifecycles_recovered = int(
        recovery_result.get("deferred_lifecycles_recovered", 0) or 0
    )
    already_verified_owner_rows = int(
        recovery_result.get("already_verified_owner_rows", 0) or 0
    )
    watching_rows_reset = int(recovery_result.get("watching_rows_reset", 0) or 0)
    orders_with_verified_owner = min(
        pending_trigger_watchers_rearmed
        + deferred_lifecycles_recovered
        + already_verified_owner_rows,
        preexisting_pending_trigger_rows,
    )
    orders_missing_runtime_owner = max(
        preexisting_pending_trigger_rows - orders_with_verified_owner,
        0,
    )
    summary = {
        "client_id": client_id,
        "execution_mode": mode,
        "overnight_rows_found": int(enqueue_result.get("signals_found") or 0) if enqueue_result else 0,
        "morning_rows_accepted": int(accepted_rows),
        "watchers_restored": int(recovery_result.get("watchers_requeued", 0) or 0),
        "deferred_retries_restored": deferred_lifecycles_recovered,
        "preexisting_pending_trigger_rows": preexisting_pending_trigger_rows,
        "pending_trigger_watchers_rearmed": pending_trigger_watchers_rearmed,
        "deferred_lifecycles_recovered": deferred_lifecycles_recovered,
        "already_verified_owner_rows": already_verified_owner_rows,
        "orders_with_verified_owner": orders_with_verified_owner,
        "orders_missing_runtime_owner": orders_missing_runtime_owner,
        "watching_rows_reset": watching_rows_reset,
        "errors": summary_errors,
    }
    details = {
        "dry_run": dry_run,
        **_attempt_details,
        "before": before,
        "after": after,
        "watchers_requeued": int(recovery_result.get("watchers_requeued", 0) or 0),
        "warnings": warnings,
        "errors": list(recovery_result.get("errors") or []),
        "enqueue_result": enqueue_result,  # PR #183
        # P0 (monday-trade-flow-readiness): full readiness pass result +
        # non-fatal readiness warnings, persisted with the handoff lock row
        # so the audit endpoint shows exactly what was archived/terminalized.
        "watching_readiness": readiness_result,
        "readiness_warnings": readiness_warnings,
        "summary": summary,
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
        attempt_generation=_attempt_generation,
        attempt_id=_attempt_id,
    )
    result = {
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
        "summary": summary,
        "overnight_rows_found": summary["overnight_rows_found"],
        "morning_rows_accepted": summary["morning_rows_accepted"],
        "watchers_restored": summary["watchers_restored"],
        "deferred_retries_restored": summary["deferred_retries_restored"],
        "preexisting_pending_trigger_rows": summary["preexisting_pending_trigger_rows"],
        "pending_trigger_watchers_rearmed": summary["pending_trigger_watchers_rearmed"],
        "deferred_lifecycles_recovered": summary["deferred_lifecycles_recovered"],
        "already_verified_owner_rows": summary["already_verified_owner_rows"],
        "orders_with_verified_owner": summary["orders_with_verified_owner"],
        "orders_missing_runtime_owner": summary["orders_missing_runtime_owner"],
        "watching_rows_reset": summary["watching_rows_reset"],
        "enqueue_result": enqueue_result,  # PR #183: signals inserted/skipped/rejected
    }
    log.info(
        "CLIENT_HANDOFF_COMPLETE client_id=%s execution_mode=%s stage=%s status=%s "
        "commit_sha=%s pod_id=%s client_count=%s summary=%s",
        client_id,
        mode,
        stage,
        "success" if ok else "failed",
        autonomy_ctx["commit_sha"],
        autonomy_ctx["pod_id"],
        autonomy_ctx["client_count"],
        summary,
    )
    return result
