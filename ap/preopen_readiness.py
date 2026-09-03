from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any
from zoneinfo import ZoneInfo


log = logging.getLogger("ap.preopen_readiness")

ET = ZoneInfo("America/New_York")
_TABLE_READY = False

PROCESSING_STALE_MINUTES = int(os.getenv("PREOPEN_PROCESSING_STALE_MINUTES", "10"))
WATCHING_ORPHAN_GRACE_MINUTES = int(os.getenv("PREOPEN_WATCHING_ORPHAN_GRACE_MINUTES", "5"))
PENDING_TRIGGER_LOOKBACK_HOURS = int(os.getenv("STARTUP_WATCHER_RESEED_LOOKBACK_HOURS", "48"))
READINESS_ENFORCEMENT_START_HOUR_ET = int(os.getenv("PREOPEN_READINESS_START_HOUR_ET", "9"))
READINESS_ENFORCEMENT_START_MINUTE_ET = int(os.getenv("PREOPEN_READINESS_START_MINUTE_ET", "0"))
READINESS_ENFORCEMENT_END_HOUR_ET = int(os.getenv("PREOPEN_READINESS_END_HOUR_ET", "10"))
READINESS_ENFORCEMENT_END_MINUTE_ET = int(os.getenv("PREOPEN_READINESS_END_MINUTE_ET", "0"))
OVERNIGHT_REEVAL_DUE_HOUR_ET = int(os.getenv("OVERNIGHT_REEVAL_DUE_HOUR_ET", "9"))
OVERNIGHT_REEVAL_DUE_MINUTE_ET = int(os.getenv("OVERNIGHT_REEVAL_DUE_MINUTE_ET", "18"))

# ── PR #573 P0: after-hours WATCHING readiness boundary ──────────────────────
# Exact durable marker written by ap/queue.py::_persist_watching_deferral
# (constant `_PAPER_OVERNIGHT_REEVAL_ONLY_ERROR`) when a signal is intentionally
# parked for the next overnight/pre-open reevaluation. This is the ONLY
# `last_error` string that qualifies a WATCHING row for the expected-deferred
# exemption below. Substring / case / alias matches are not permitted.
AFTER_HOURS_DEFERRED_MARKER = "after_hours_deferred:awaiting_overnight_reeval"

# Upper bound on how far back a legitimate after-hours defer window can begin.
# A row older than this in calendar days is stale by construction: it survived
# at least one required morning consumer boundary without honest downstream
# disposition, so it cannot still qualify as fresh expected-deferred inventory.
# The deadline comparison is the primary authority; this is a defense-in-depth
# bound.
AFTER_HOURS_DEFERRED_MAX_LOOKBACK_DAYS = int(
    os.getenv("PREOPEN_AFTER_HOURS_DEFERRED_MAX_LOOKBACK_DAYS", "10")
)


def _now_et(now: datetime | None = None) -> datetime:
    return (now or datetime.now(ET)).astimezone(ET)


def _trading_date(now: datetime | None = None) -> str:
    return _now_et(now).date().isoformat()


def _normalize_mode(value: str | None) -> str:
    return str(value or "").strip().lower()


def _nyse_is_trading_day(dt: datetime) -> bool:
    """Route through the canonical NYSE calendar in ap.flatline_alarm.

    Fail-safe: on calendar import failure, fall back to weekday-only. This
    preserves prior behavior for environments where the calendar module is
    unavailable, while every deployment that carries flatline_alarm (all
    production pods) uses the authoritative holiday-aware truth.
    """
    try:
        from ap.flatline_alarm import is_trading_day as _nyse
        return bool(_nyse(dt.date()))
    except Exception:
        return dt.weekday() < 5  # legacy fallback


def _after_929_et(now: datetime | None = None) -> bool:
    dt = _now_et(now)
    if not _nyse_is_trading_day(dt):
        return False
    return dt.hour > 9 or (dt.hour == 9 and dt.minute >= 29)


# ── PR #573: authoritative next-trading-day resolution ───────────────────────
# The expected-after-hours-deferred exemption ends at the configured overnight
# reeval due time (OVERNIGHT_REEVAL_DUE_HOUR_ET:MINUTE, default 09:18 ET) of
# the NEXT NYSE trading session after the row was created. This helper
# resolves that deadline authoritatively and returns None on calendar
# ambiguity (per binding spec: calendar ambiguity => fail closed).
def _next_trading_session_deadline_et(
    created_ts_utc: datetime,
) -> datetime | None:
    """Return the authoritative next-trading-session overnight-reeval deadline
    in ET, or None if the NYSE calendar cannot resolve it authoritatively.

    Fail-closed semantics:
      - naive/malformed inputs return None (caller must fail closed);
      - the flatline_alarm NYSE calendar must have an authoritative year map
        for both the source date and every candidate next day scanned;
      - Friday → Monday and holiday-eve → next trading day are handled by
        iterating with the canonical `is_trading_day`.
    """
    try:
        from ap.flatline_alarm import is_trading_day as _is_trading_day
        from ap.flatline_alarm import NYSE_HOLIDAYS as _NYSE_HOLIDAYS
    except Exception:
        # Calendar authority unavailable — never manufacture a deadline.
        return None

    if not isinstance(created_ts_utc, datetime) or created_ts_utc.tzinfo is None:
        return None

    created_et = created_ts_utc.astimezone(ET)

    # Walk forward from the day AFTER the source date until we find a trading
    # day. If any calendar-year lookup falls into the "unknown year" branch
    # (which fail-opens to weekday-only), refuse to guess — return None.
    candidate = (created_et + timedelta(days=1)).date()
    # Bound the walk so a broken calendar cannot loop indefinitely.
    for _ in range(14):
        if _NYSE_HOLIDAYS.get(candidate.year) is None:
            # Unknown year in the authoritative map — fail closed.
            return None
        if _is_trading_day(candidate):
            return datetime(
                candidate.year, candidate.month, candidate.day,
                OVERNIGHT_REEVAL_DUE_HOUR_ET,
                OVERNIGHT_REEVAL_DUE_MINUTE_ET,
                tzinfo=ET,
            )
        candidate = candidate + timedelta(days=1)
    return None


def _parse_created_ts_to_utc(raw: Any) -> datetime | None:
    """Parse a trade_queue.created_ts value to a UTC-aware datetime, or
    return None on any parse failure. Naive datetimes are rejected: the
    binding spec forbids machine-local timezone assumptions.
    """
    if raw is None:
        return None
    if isinstance(raw, datetime):
        if raw.tzinfo is None:
            return None
        return raw.astimezone(timezone.utc)
    s = str(raw).strip()
    if not s:
        return None
    # psycopg2 typically returns tz-aware datetimes; string arrival happens
    # only in test fixtures or when payloads are re-serialized.
    try:
        # datetime.fromisoformat accepts trailing "Z" only on 3.11+.
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
    except (TypeError, ValueError):
        return None
    if dt.tzinfo is None:
        return None
    return dt.astimezone(timezone.utc)


# ── PR #573 audit follow-up (2026-09-03): source-session classification ──
# The queue producer at ap/queue.py::_persist_watching_deferral legitimately
# writes AFTER_HOURS_DEFERRED_MARKER in TWO windows relative to the ET market
# clock (matching `ap/queue.py::_is_regular_session_et`, which is exactly
# 09:30–16:00 ET Mon-Fri):
#
#   post_close       - after 16:00 ET on a trading day (Sep 2 incident shape),
#                      OR any time on a non-trading day (Sat/Sun/holiday);
#   pre_market       - before 09:30 ET on a trading day;
#   regular_session  - 09:30–16:00 ET on a trading day; the producer does not
#                      legitimately mint this marker inside regular session,
#                      so seeing it here means the row lifecycle is
#                      contradictory and MUST fail closed.
#
# The applicable overnight-reeval deadline differs per window:
#
#   post_close (trading-day close)  → next trading session 09:18 ET
#   post_close (non-trading day)    → next trading session 09:18 ET
#   pre_market                       → SAME trading day 09:18 ET
#   regular_session                  → NO deadline; fail closed
#
# Returns (session_kind, deadline_et) or (session_kind, None) on calendar
# ambiguity / regular-session shape. Callers treat None deadline the same as
# a regular-session shape: no exemption, fall back to ordinary orphan.
_REGULAR_SESSION_OPEN_MIN = 9 * 60 + 30   # 09:30 ET
_REGULAR_SESSION_CLOSE_MIN = 16 * 60      # 16:00 ET


def _resolve_source_session_and_deadline_et(
    created_ts_utc: datetime,
) -> tuple[str, datetime | None]:
    try:
        from ap.flatline_alarm import is_trading_day as _is_trading_day
        from ap.flatline_alarm import NYSE_HOLIDAYS as _NYSE_HOLIDAYS
    except Exception:
        return "unknown", None

    if not isinstance(created_ts_utc, datetime) or created_ts_utc.tzinfo is None:
        return "unknown", None

    created_et = created_ts_utc.astimezone(ET)
    src_date = created_et.date()

    # If the source date's year isn't in the authoritative NYSE map, we
    # cannot reason about session windows safely. Fail closed.
    if _NYSE_HOLIDAYS.get(src_date.year) is None:
        return "unknown", None

    src_is_trading_day = _is_trading_day(src_date)
    created_minute_of_day = created_et.hour * 60 + created_et.minute

    if not src_is_trading_day:
        # Weekend / holiday origin — deadline is the next trading session.
        deadline = _next_trading_session_deadline_et(created_ts_utc)
        return "non_trading_day", deadline

    if created_minute_of_day < _REGULAR_SESSION_OPEN_MIN:
        # Pre-market on a trading day. Deadline is SAME day at 09:18 ET.
        deadline = datetime(
            src_date.year, src_date.month, src_date.day,
            OVERNIGHT_REEVAL_DUE_HOUR_ET,
            OVERNIGHT_REEVAL_DUE_MINUTE_ET,
            tzinfo=ET,
        )
        return "pre_market", deadline

    if created_minute_of_day >= _REGULAR_SESSION_CLOSE_MIN:
        # Post-close on a trading day — deadline is the next trading session.
        deadline = _next_trading_session_deadline_et(created_ts_utc)
        return "post_close", deadline

    # 09:30 ≤ minute < 16:00 on a trading day — contradictory shape.
    # The queue producer does not legitimately mint the after-hours marker
    # here, so treat it as ambiguous / fail closed. No deadline is returned.
    return "regular_session", None


def _classify_watching_row(
    row: dict,
    *,
    client_id: str,
    execution_mode: str,  # noqa: ARG001 — retained for signature stability; see (4) note below
    now_utc: datetime,
) -> tuple[str, dict]:
    """Classify one trade_queue WATCHING row (that already has no matching
    ENTRY order) into one of:

      - "expected_after_hours_deferred": legitimate parked inventory before
        its next authoritative processing deadline. Non-blocking.
      - "after_hours_deferred_overdue": parked with the exact marker but at
        or past its next authoritative processing deadline. Blocking.
      - "ordinary_orphan": lacks the exact marker or fails any evidence gate
        below. Blocking under the existing orphan rule.

    ALL of the following must be true to reach a deferred classification.
    Any ambiguity returns "ordinary_orphan" (existing fail-closed behavior).

      1. client_id on the row equals the readiness client_id;
      2. status is exactly "WATCHING";
      3. last_error is exactly AFTER_HOURS_DEFERRED_MARKER;
      4. created_ts parseable, tz-aware, non-future, within lookback window;
      5. source-session window is post_close on a trading day (created_ts
         in ET is >= 16:00 on an authoritative NYSE trading day). All
         other origins — pre-market, regular session, non-trading day,
         unknown-year calendar — return "ordinary_orphan".
      6. next-trading-session deadline resolvable via NYSE calendar
         authority (walks forward to the next actual trading session,
         honoring weekends and holidays: e.g. a Friday post-close row
         with Monday-holiday resolves to Tuesday 09:18 ET).

    NOTE (PR #573 amendment 2026-09-03): payload.execution_mode is NOT
    a gate. The 244 real production rows from the September 2 incident
    do not carry that field; requiring it silently rejected legitimate
    inventory. Mode safety is provided by client_id scoping in the SQL
    and by the fact that trade_queue.client_id is unique per runtime
    client. Same-client mixed-mode ambiguity (one client_id running
    both PAPER and LIVE simultaneously) is not currently produced and
    will be handled in a separate PR if it becomes a real shape.

    The returned dict carries diagnostic fields (row_id, signal_id,
    source session, resolved deadline) suitable for readiness
    diagnostics — never for lifecycle mutation.
    """
    diag: dict = {
        "id": row.get("id") if isinstance(row, dict) else None,
        "signal_id": row.get("signal_id") if isinstance(row, dict) else None,
    }
    if not isinstance(row, dict):
        return "ordinary_orphan", diag

    # (1) client identity — must match exactly. A blank/mismatched client
    # never earns the exemption on someone else's readiness call.
    row_client = str(row.get("client_id") or "").strip()
    if not row_client or row_client != str(client_id or "").strip():
        return "ordinary_orphan", diag

    # (2) status — exact WATCHING only.
    if str(row.get("status") or "").strip() != "WATCHING":
        return "ordinary_orphan", diag

    # (3) exact marker equality — never substring, never case-fold. Any other
    # last_error (including None) falls to ordinary_orphan.
    if row.get("last_error") != AFTER_HOURS_DEFERRED_MARKER:
        return "ordinary_orphan", diag

    # (4) [removed] payload.execution_mode gate.
    # PR #573 amendment (2026-09-03): the durable trade_queue.payload for
    # the real 244 after-hours WATCHING rows produced by the September 2
    # incident does NOT contain an execution_mode field. Requiring it here
    # would silently reject legitimate production inventory.
    #
    # Mode safety is provided by the SQL scoping on trade_queue.client_id
    # (the readiness client identity gate above) combined with the
    # post-close-on-trading-day source-session gate below. If same-client
    # mixed-mode (PAPER runner + LIVE runner sharing one client_id)
    # becomes a proven production shape, it will be addressed in a
    # separate PR against evidence — not by fabricating producer mode
    # provenance the queue never persisted.

    # (5) created_ts — must be tz-aware, non-future, within lookback bound.
    created_utc = _parse_created_ts_to_utc(row.get("created_ts"))
    if created_utc is None:
        return "ordinary_orphan", diag
    if created_utc > now_utc:
        # Clock skew or malformed payload — never trust a future creation.
        return "ordinary_orphan", diag
    if (now_utc - created_utc).days > AFTER_HOURS_DEFERRED_MAX_LOOKBACK_DAYS:
        return "ordinary_orphan", diag
    diag["created_ts"] = created_utc.isoformat()

    # (6) source-session classification + deadline resolution.
    #
    # PR #573 amendment (2026-09-03): only rows created POST-CLOSE on an
    # authoritative NYSE trading day (created_ts_ET >= 16:00 on a trading
    # day) may earn the deferred exemption. Every other source-session
    # shape falls through to ordinary_orphan and follows the existing
    # 5-minute grace + orphan block behavior.
    #
    #   post_close_trading_day   → eligible; deadline = next trading
    #                              session at OVERNIGHT_REEVAL_DUE ET
    #   pre_market_trading_day   → ordinary_orphan (contradictory shape;
    #                              producer legitimately mints marker
    #                              here but the amendment does not
    #                              extend the exemption to pre-market)
    #   regular_session          → ordinary_orphan (contradictory)
    #   non_trading_day          → ordinary_orphan (amendment does not
    #                              extend exemption to weekend/holiday-
    #                              created rows; only Friday->Monday
    #                              deadline resolution is honored for
    #                              a post-close Friday row)
    #   unknown / ambiguous      → ordinary_orphan (calendar fail-closed)
    session_kind, deadline_et = _resolve_source_session_and_deadline_et(created_utc)
    if session_kind != "post_close" or deadline_et is None:
        diag["source_session"] = session_kind
        return "ordinary_orphan", diag
    diag["source_session"] = session_kind
    deadline_utc = deadline_et.astimezone(timezone.utc)
    diag["next_deadline_utc"] = deadline_utc.isoformat()
    diag["next_deadline_et"] = deadline_et.isoformat()

    if now_utc < deadline_utc:
        return "expected_after_hours_deferred", diag
    return "after_hours_deferred_overdue", diag


def _overnight_reeval_due(now: datetime | None = None) -> bool:
    dt = _now_et(now)
    if not _nyse_is_trading_day(dt):
        return False
    current = dt.hour * 60 + dt.minute
    due = OVERNIGHT_REEVAL_DUE_HOUR_ET * 60 + OVERNIGHT_REEVAL_DUE_MINUTE_ET
    return current >= due


def _is_market_day(now: datetime | None = None) -> bool:
    return _nyse_is_trading_day(_now_et(now))


def _readiness_enforcement_active(now: datetime | None = None) -> bool:
    dt = _now_et(now)
    if not _is_market_day(dt):
        return False
    current = dt.hour * 60 + dt.minute
    start = READINESS_ENFORCEMENT_START_HOUR_ET * 60 + READINESS_ENFORCEMENT_START_MINUTE_ET
    end = READINESS_ENFORCEMENT_END_HOUR_ET * 60 + READINESS_ENFORCEMENT_END_MINUTE_ET
    return start <= current <= end


def _pod_mode() -> str:
    return _normalize_mode(os.getenv("BOT_MODE", os.getenv("MODE", "paper")))


def _ensure_preopen_table() -> None:
    global _TABLE_READY
    if _TABLE_READY:
        return
    from ap.db import conn, run_with_retry

    def _create():
        with conn() as c:
            c.execute(
                """
                CREATE TABLE IF NOT EXISTS preopen_readiness_runs (
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


def _load_preopen_row(*, client_id: str, execution_mode: str, trading_date: str, stage: str) -> dict | None:
    from ap.db import conn, run_with_retry

    _ensure_preopen_table()

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT client_id, execution_mode, trading_date::text AS trading_date,
                       stage, status, last_run_at::text AS last_run_at,
                       last_success_at::text AS last_success_at,
                       last_error, details
                FROM preopen_readiness_runs
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


def _upsert_preopen_row(
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

    _ensure_preopen_table()

    def _write():
        with conn() as c:
            c.execute(
                """
                INSERT INTO preopen_readiness_runs (
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
                        ELSE preopen_readiness_runs.last_success_at
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


def _latest_preopen_rows(trading_date: str) -> list[dict]:
    from ap.db import conn, run_with_retry

    _ensure_preopen_table()

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT client_id, execution_mode, trading_date::text AS trading_date,
                       stage, status, last_run_at::text AS last_run_at,
                       last_success_at::text AS last_success_at,
                       last_error, details
                FROM preopen_readiness_runs
                WHERE trading_date = %s::date
                ORDER BY execution_mode, client_id, updated_at DESC
                """,
                (trading_date,),
            )
            rows = c.fetchall() or []
            cols = [d[0] for d in getattr(c, "description", [])]
            return [dict(r) if isinstance(r, dict) else dict(zip(cols, r)) for r in rows]

    try:
        return run_with_retry(_load) or []
    except Exception:
        return []


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


def _broker_credentials_present(runner, execution_mode: str) -> tuple[bool, dict]:
    member = getattr(runner, "member", None) or {}
    broker = getattr(getattr(runner, "core", None), "broker", None)
    broker_cfg = getattr(broker, "cfg", None)
    base_url = str(
        getattr(runner, "base_url", None)
        or getattr(broker, "base_url", None)
        or getattr(broker_cfg, "base_url", None)
        or ""
    )
    account_id = str(
        getattr(runner, "account_id", None)
        or getattr(broker, "account_id", None)
        or getattr(broker_cfg, "account_id", None)
        or ""
    )

    token_candidates: list[tuple[str, Any]] = [
        ("runner._resolved_tradier_token", getattr(runner, "_resolved_tradier_token", None)),
        ("broker.access_token", getattr(broker, "access_token", None)),
        ("broker._access_token", getattr(broker, "_access_token", None)),
        ("broker.cfg.access_token", getattr(broker_cfg, "access_token", None)),
    ]
    if execution_mode == "live":
        token_candidates.extend([
            ("member.tradier_live_access_token", member.get("tradier_live_access_token")),
            ("env.TRADIER_ACCESS_TOKEN", os.getenv("TRADIER_ACCESS_TOKEN")),
        ])
    else:
        token_candidates.extend([
            ("member.tradier_paper_access_token", member.get("tradier_paper_access_token")),
            ("member.tradier_access_token", member.get("tradier_access_token")),
            ("env.TRADIER_ACCESS_TOKEN", os.getenv("TRADIER_ACCESS_TOKEN")),
        ])

    token_sources = [
        name for name, value in token_candidates
        if str(value or "").strip()
    ]
    token_state = "configured" if token_sources else "missing"
    status = "configured" if (base_url and account_id and token_sources) else "missing"

    return status == "configured", {
        "base_url": base_url,
        "account_id_present": bool(account_id),
        "token_state": token_state,
        "token_present": bool(token_sources),
        "token_sources": token_sources,
        "credential_status": status,
        "execution_mode": execution_mode,
    }


def _selector_identity(runner) -> dict:
    selector = getattr(runner, "contract_selector", None)
    brk = getattr(selector, "data_broker", None) or getattr(selector, "broker", None) or getattr(getattr(runner, "core", None), "broker", None)
    base_url = (
        getattr(getattr(brk, "cfg", None), "base_url", None)
        or getattr(brk, "base_url", None)
        or ""
    )
    base_url = str(base_url)
    sandbox = "sandbox" in base_url.lower()
    if not base_url:
        source = "unknown"
    elif sandbox:
        source = "tradier_sandbox"
    else:
        source = "tradier_live"
    return {
        "quote_source": source,
        "chain_source": "tradier_options_chain" if base_url else "unknown",
        "tradier_base_url": base_url,
        "sandbox_mode": bool(sandbox),
    }


def _morning_handoff_success_exists(client_id: str, execution_mode: str, trading_date: str) -> bool:
    from ap.morning_handoff import _latest_handoff_rows

    rows = _latest_handoff_rows(trading_date)
    for row in rows:
        if (
            str(row.get("client_id") or "").strip() == client_id
            and _normalize_mode(row.get("execution_mode")) == execution_mode
            and str(row.get("status") or "").lower() == "success"
            and row.get("last_success_at")
        ):
            return True
    return False


def _post_overnight_reeval_success_exists(client_id: str, execution_mode: str, trading_date: str) -> bool:
    from ap.morning_handoff import _latest_handoff_rows

    rows = _latest_handoff_rows(trading_date)
    for row in rows:
        if (
            str(row.get("client_id") or "").strip() == client_id
            and _normalize_mode(row.get("execution_mode")) == execution_mode
            and str(row.get("stage") or "").strip().lower() == "post_overnight_reeval"
            and str(row.get("status") or "").strip().lower() == "success"
            and row.get("last_success_at")
        ):
            return True
    return False


def _query_client_state(
    client_id: str,
    *,
    execution_mode: str | None = None,
    now: datetime | None = None,
) -> dict:
    """Read the WATCHING/PROCESSING/PENDING_TRIGGER lifecycle facts for one
    client and bucket the WATCHING rows-without-entry-orders into three
    disjoint categories.

    PR #573 additions to the returned dict:
      - `expected_after_hours_deferred` (list): rows carrying the exact
        canonical defer marker whose next-trading-session processing deadline
        has NOT yet elapsed. These are diagnostic-only and MUST NOT be
        counted as orphan blockers.
      - `after_hours_deferred_overdue` (list): rows carrying the exact defer
        marker whose next-trading-session deadline HAS elapsed while still
        parked. These are blocking.

    Backward-compat: `watching_orphans` (list) remains and now contains ONLY
    ordinary orphans — rows older than WATCHING_ORPHAN_GRACE_MINUTES that
    did not qualify for either deferred bucket (e.g. no marker, missing mode,
    calendar ambiguity, marker mismatch, etc). This preserves the existing
    downstream error "watching_rows_missing_orders_recommend_new_rescue"
    without weakening any prior orphan protection.

    The `execution_mode` and `now` parameters are keyword-only with defaults
    so pre-#573 monkeypatches in tests continue to receive the correct
    single-positional call shape while production code passes both.
    """
    from ap.db import conn, run_with_retry

    now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc) \
        if now is not None else datetime.now(timezone.utc)
    processing_cutoff = now_utc - timedelta(minutes=PROCESSING_STALE_MINUTES)
    watching_cutoff = now_utc - timedelta(minutes=WATCHING_ORPHAN_GRACE_MINUTES)
    pending_cutoff = now_utc - timedelta(hours=PENDING_TRIGGER_LOOKBACK_HOURS)
    readiness_mode = _normalize_mode(execution_mode)

    def _load():
        with conn() as c:
            c.execute(
                """
                SELECT id
                FROM trade_queue
                WHERE client_id = %s
                  AND status = 'PROCESSING'
                  AND COALESCE(started_ts, created_ts) < %s
                ORDER BY id
                """,
                (client_id, processing_cutoff),
            )
            stale_processing = [r[0] if not isinstance(r, dict) else r.get("id") for r in (c.fetchall() or [])]

            # PR #573: project the fields the classifier needs — last_error,
            # created_ts, payload — and drop the age-only filter so the
            # classifier can bucket every candidate. The age filter still
            # applies to the ordinary_orphan bucket below.
            c.execute(
                """
                SELECT q.id, q.signal_id, q.client_id, q.status,
                       q.last_error, q.created_ts, q.payload
                FROM trade_queue q
                WHERE q.client_id = %s
                  AND q.status = 'WATCHING'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM orders o
                      WHERE o.client_id = q.client_id
                        AND COALESCE(o.signal_id, '') = COALESCE(q.signal_id, '')
                        AND o.kind = 'ENTRY'
                  )
                ORDER BY q.id
                """,
                (client_id,),
            )
            watching_orphans: list[dict] = []
            expected_deferred: list[dict] = []
            overdue_deferred: list[dict] = []
            for row in (c.fetchall() or []):
                if not isinstance(row, dict):
                    # psycopg2 tuple cursor — hydrate a dict in the same
                    # projected column order.
                    row = {
                        "id": row[0], "signal_id": row[1], "client_id": row[2],
                        "status": row[3], "last_error": row[4],
                        "created_ts": row[5], "payload": row[6],
                    }
                kind, diag = _classify_watching_row(
                    row,
                    client_id=client_id,
                    execution_mode=readiness_mode,
                    now_utc=now_utc,
                )
                if kind == "expected_after_hours_deferred":
                    expected_deferred.append(diag)
                elif kind == "after_hours_deferred_overdue":
                    overdue_deferred.append(diag)
                else:
                    # Ordinary orphan bucket — preserve the pre-#573 age
                    # threshold so we do not create new blocking noise for
                    # rows still inside the legacy 5-minute grace.
                    #
                    # PR #573 amendment (2026-09-03 review 5101059272):
                    # `created_utc > now_utc` (future timestamp) must NOT
                    # earn the grace-window skip. Without the upper bound
                    # a corrupt future ts satisfies `>= watching_cutoff`
                    # trivially, causing the row to silently vanish from
                    # readiness — neither deferred nor orphan-reported.
                    # Only a proven non-future fresh row receives grace.
                    created_utc = _parse_created_ts_to_utc(row.get("created_ts"))
                    if (
                        created_utc is not None
                        and created_utc <= now_utc
                        and created_utc >= watching_cutoff
                    ):
                        continue
                    watching_orphans.append({
                        "id": row.get("id"),
                        "signal_id": row.get("signal_id"),
                    })

            c.execute(
                """
                SELECT local_order_id, signal_id
                FROM orders
                WHERE client_id = %s
                  AND kind = 'ENTRY'
                  AND status = 'PENDING_TRIGGER'
                  AND created_ts >= %s
                  AND broker_order_id IS NULL
                  AND submitted_ts IS NULL
                  AND filled_ts IS NULL
                ORDER BY created_ts
                """,
                (client_id, pending_cutoff),
            )
            pending_trigger = []
            for row in (c.fetchall() or []):
                if isinstance(row, dict):
                    pending_trigger.append({"local_order_id": row.get("local_order_id"), "signal_id": row.get("signal_id")})
                else:
                    pending_trigger.append({"local_order_id": row[0], "signal_id": row[1]})

            c.execute(
                """
                SELECT COUNT(*)::int AS n
                FROM trade_queue
                WHERE client_id = %s
                  AND status = 'WATCHING'
                """,
                (client_id,),
            )
            watch_count_row = c.fetchone()
            watching_count = watch_count_row["n"] if isinstance(watch_count_row, dict) else (watch_count_row[0] if watch_count_row else 0)

            return {
                "stale_processing_ids": stale_processing,
                "watching_orphans": watching_orphans,
                "expected_after_hours_deferred": expected_deferred,
                "after_hours_deferred_overdue": overdue_deferred,
                "pending_trigger_rows": pending_trigger,
                "watching_count": int(watching_count or 0),
            }

    return run_with_retry(_load) or {
        "stale_processing_ids": [],
        "watching_orphans": [],
        "expected_after_hours_deferred": [],
        "after_hours_deferred_overdue": [],
        "pending_trigger_rows": [],
        "watching_count": 0,
    }


def _pending_trigger_without_watcher(runner, pending_rows: list[dict]) -> list[dict]:
    entry_watcher = getattr(getattr(runner, "core", None), "entry_watcher", None)
    if entry_watcher is None or not hasattr(entry_watcher, "has_order"):
        return list(pending_rows or [])
    out = []
    for row in pending_rows or []:
        local_order_id = str(row.get("local_order_id") or "").strip()
        if not local_order_id:
            out.append(row)
            continue
        try:
            if not entry_watcher.has_order(local_order_id):
                out.append(row)
        except Exception:
            out.append(row)
    return out


def _overnight_status(
    runner,
    client_state: dict,
    trading_date: str,
    *,
    client_id: str,
    execution_mode: str,
    stage: str = "",
    now: datetime | None = None,
) -> tuple[str, dict]:
    if _post_overnight_reeval_success_exists(client_id, execution_mode, trading_date):
        return "success", {"source": "handoff_run_locks.post_overnight_reeval"}
    success_date = getattr(runner, "_overnight_reeval_success_date", None)
    if str(success_date or "") == trading_date:
        return "success", {"source": "runner_overnight_reeval_success_date"}
    if int(client_state.get("watching_count", 0) or 0) == 0 and not client_state.get("pending_trigger_rows"):
        return "explicit_noop", {"source": "no_watching_or_pending_trigger_rows"}
    if str(stage or "").strip().lower() == "startup" and not _overnight_reeval_due(now):
        return "pending", {"source": "startup_before_overnight_reeval_due"}
    return "missing", {"source": "watching_or_pending_trigger_present_without_overnight_success"}


def _stage_success_recent(existing: dict | None) -> bool:
    return bool(existing and str(existing.get("status") or "").lower() == "success" and existing.get("last_success_at"))


def run_preopen_autonomous_readiness(
    client_id: str,
    execution_mode: str,
    *,
    dry_run: bool = True,
    repair: bool = False,
    stage: str = "manual",
    runner=None,
    now: datetime | None = None,
) -> dict:
    mode = _normalize_mode(execution_mode)
    client_id = str(client_id or "").strip()
    stage = str(stage or "manual").strip().lower()
    trading_date = _trading_date(now)

    if not client_id:
        return {"ok": False, "status": "BLOCKED", "error": "client_id_required"}
    if mode not in {"paper", "live"}:
        return {"ok": False, "status": "BLOCKED", "error": "invalid_execution_mode", "client_id": client_id}

    runner = runner or _resolve_runner(client_id)
    _upsert_preopen_row(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
        status="running",
        last_error=None,
        details={"dry_run": dry_run, "repair": repair},
        mark_success=False,
    )

    errors: list[str] = []
    warnings: list[str] = []
    details: dict[str, Any] = {"dry_run": dry_run, "repair": repair}

    if runner is None:
        errors.append("runner_not_found")
        result = {
            "ok": False,
            "status": "BLOCKED",
            "client_id": client_id,
            "execution_mode": mode,
            "stage": stage,
            "trading_date": trading_date,
            "errors": errors,
            "warnings": warnings,
            "details": details,
        }
        _upsert_preopen_row(
            client_id=client_id,
            execution_mode=mode,
            trading_date=trading_date,
            stage=stage,
            status="blocked",
            last_error="runner_not_found",
            details=result,
            mark_success=False,
        )
        return result

    initialized = bool(getattr(runner, "initialized", None) and runner.initialized.is_set())
    worker_alive = bool(getattr(runner, "worker_thread", None) and runner.worker_thread.is_alive())
    runner_alive = bool(hasattr(runner, "is_alive") and runner.is_alive())
    osm_present = getattr(runner, "order_state_machine", None) is not None
    entry_watcher_present = getattr(getattr(runner, "core", None), "entry_watcher", None) is not None
    mc_present = getattr(runner, "master_control", None) is not None

    details.update({
        "runner_alive": runner_alive,
        "runner_initialized": initialized,
        "queue_worker_alive": worker_alive,
        "order_state_machine_present": osm_present,
        "entry_watcher_present": entry_watcher_present,
        "master_control_present": mc_present,
    })

    if not runner_alive:
        errors.append("runner_not_alive")
    if not initialized:
        errors.append("runner_not_initialized")
    if not worker_alive:
        errors.append("queue_worker_not_alive")
    if not osm_present:
        errors.append("order_state_machine_missing")
    if not entry_watcher_present:
        errors.append("entry_watcher_missing")
    if not mc_present:
        errors.append("master_control_missing")

    pod_mode = _pod_mode()
    actual_mode = _normalize_mode(getattr(runner, "mode", None) or getattr(getattr(runner, "master_control", None), "mode", None))
    details["pod_mode"] = pod_mode
    details["runner_mode"] = actual_mode
    if actual_mode != mode:
        errors.append("requested_execution_mode_mismatch")
    if pod_mode and actual_mode and pod_mode != actual_mode:
        errors.append("pod_mode_client_mode_mismatch")

    broker_ok, broker_details = _broker_credentials_present(runner, mode)
    details["broker_credentials"] = broker_details
    broker_state = str(broker_details.get("credential_status") or "missing").lower()
    if stage != "startup":
        if not broker_ok and broker_state == "missing":
            errors.append("broker_credentials_missing")
        elif not broker_ok:
            errors.append("broker_credentials_unverified")
    elif not broker_ok:
        warnings.append("broker_credentials_unverified")

    selector_identity = _selector_identity(runner)
    details["selector_identity"] = selector_identity
    if selector_identity["quote_source"] == "unknown" or not selector_identity["tradier_base_url"]:
        errors.append("selector_quote_identity_unresolved")

    client_state = _query_client_state(client_id, execution_mode=mode, now=now)
    details["client_state"] = client_state

    if client_state.get("stale_processing_ids"):
        errors.append("stale_processing_rows")

    if client_state.get("watching_orphans"):
        errors.append("watching_rows_missing_orders_recommend_new_rescue")

    # PR #573: after-hours defer classification.
    # `expected_after_hours_deferred` rows are legitimate parked inventory
    # awaiting the next authoritative overnight/pre-open consumer; they are
    # visible in diagnostics but MUST NOT block LIVE entries by themselves
    # before their next-trading-session deadline. `after_hours_deferred_overdue`
    # rows carry the exact marker but have passed that deadline still parked —
    # they are blocking with a distinct reason so operators can tell the two
    # apart in incident review.
    expected_after_hours = client_state.get("expected_after_hours_deferred") or []
    overdue_after_hours = client_state.get("after_hours_deferred_overdue") or []
    details["expected_after_hours_deferred_count"] = len(expected_after_hours)
    details["after_hours_deferred_overdue_count"] = len(overdue_after_hours)
    if overdue_after_hours:
        errors.append("after_hours_deferred_overdue")

    unowned_pending = _pending_trigger_without_watcher(runner, client_state.get("pending_trigger_rows") or [])
    details["pending_trigger_without_watcher"] = unowned_pending
    if unowned_pending:
        errors.append("pending_trigger_without_watcher_ownership")

    handoff_ok = _morning_handoff_success_exists(client_id, mode, trading_date)
    details["morning_handoff_success"] = handoff_ok
    if stage != "startup":
        if not handoff_ok:
            if mode == "live" or _after_929_et(now):
                errors.append("morning_handoff_missing")
            else:
                warnings.append("morning_handoff_missing")
    elif not handoff_ok:
        warnings.append("morning_handoff_pending_startup")

    overnight_state, overnight_details = _overnight_status(
        runner,
        client_state,
        trading_date,
        client_id=client_id,
        execution_mode=mode,
        stage=stage,
        now=now,
    )
    if overnight_state == "pending":
        warnings.append("overnight_reeval_pending_startup")
    elif overnight_state == "missing":
        if mode == "live" or _after_929_et(now):
            errors.append("overnight_reeval_missing")
        else:
            warnings.append("overnight_reeval_missing")
    details["overnight_reeval"] = {"status": overnight_state, **overnight_details}

    # PR #388 LIVE blocked_keys hardening
    # ────────────────────────────────────
    # The prior set omitted the two conditions that most directly violate
    # this PR's core promise of verified pre-open watcher ownership:
    #   • overnight_reeval_missing — no overnight watchers armed for this
    #     trading day. Live entries must not be authorized without them;
    #     the whole PR is about restoring that guarantee.
    #   • pending_trigger_without_watcher_ownership — DB rows in
    #     PENDING_TRIGGER with no live in-memory watcher owner. A breach
    #     would never fire; a broker-side fill (if any) would be untracked.
    # Both are BLOCKED for live mode. Paper continues to DEGRADE so paper
    # sessions can still surface diagnostics without freezing.
    blocked_keys = {
        "pod_mode_client_mode_mismatch",
        "requested_execution_mode_mismatch",
        "entry_watcher_missing",
        "morning_handoff_missing",
        "selector_quote_identity_unresolved",
        "runner_not_alive",
        "overnight_reeval_missing",
        "pending_trigger_without_watcher_ownership",
        # PR #573: an overdue after-hours defer row is a row still parked past
        # its authoritative next-trading-session processing deadline. Global
        # overnight_reeval success does not erase this — the row itself is the
        # unresolved-lifecycle evidence. LIVE must fail closed on this.
        "after_hours_deferred_overdue",
    }
    if mode == "live" and any(err in blocked_keys for err in errors):
        status = "BLOCKED"
    elif errors:
        status = "DEGRADED"
    else:
        status = "OK"

    ok = status == "OK"
    result = {
        "ok": ok,
        "status": status,
        "client_id": client_id,
        "execution_mode": mode,
        "stage": stage,
        "trading_date": trading_date,
        "errors": errors,
        "warnings": warnings,
        "details": details,
    }
    _upsert_preopen_row(
        client_id=client_id,
        execution_mode=mode,
        trading_date=trading_date,
        stage=stage,
        status=status.lower(),
        last_error=";".join(errors) if errors else None,
        details=result,
        mark_success=ok,
    )
    return result


def get_preopen_readiness_health(now: datetime | None = None) -> dict:
    trading_date = _trading_date(now)
    enforcement_active = _readiness_enforcement_active(now)
    rows = _latest_preopen_rows(trading_date)
    expected = _expected_clients_by_mode()
    by_mode: dict[str, dict[str, Any]] = {
        "paper": {"clients": {}, "last_run_at": None, "last_success_at": None, "errors": []},
        "live": {"clients": {}, "last_run_at": None, "last_success_at": None, "errors": []},
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
        details = row.get("details") or {}
        if not isinstance(details, dict):
            details = {}
        by_mode[mode]["clients"][client_id] = {
            "status": row.get("status"),
            "last_run_at": row.get("last_run_at"),
            "last_success_at": row.get("last_success_at"),
            "last_stage": row.get("stage"),
            "last_error": row.get("last_error"),
            "errors": details.get("errors") or [],
        }
        if row.get("last_run_at") and (
            by_mode[mode]["last_run_at"] is None or str(row.get("last_run_at")) > str(by_mode[mode]["last_run_at"])
        ):
            by_mode[mode]["last_run_at"] = row.get("last_run_at")
        if row.get("last_success_at") and (
            by_mode[mode]["last_success_at"] is None or str(row.get("last_success_at")) > str(by_mode[mode]["last_success_at"])
        ):
            by_mode[mode]["last_success_at"] = row.get("last_success_at")
        if row.get("last_error"):
            by_mode[mode]["errors"].append(row.get("last_error"))

    observed_statuses = []
    missing_expected_clients: dict[str, list[str]] = {"paper": [], "live": []}
    for mode, client_ids in expected.items():
        for client_id in client_ids:
            status = str(by_mode[mode]["clients"].get(client_id, {}).get("status") or "missing").upper()
            if status == "MISSING":
                missing_expected_clients[mode].append(client_id)
            else:
                observed_statuses.append(status)

    if any(s == "BLOCKED" for s in observed_statuses):
        overall = "BLOCKED"
    elif any(s == "DEGRADED" for s in observed_statuses):
        overall = "DEGRADED"
    elif enforcement_active and any(missing_expected_clients.values()):
        overall = "DEGRADED"
    else:
        overall = "OK"

    return {
        "status": overall,
        "enforcement_active": enforcement_active,
        "trading_date": trading_date,
        "paper": by_mode["paper"],
        "live": by_mode["live"],
        "missing_expected_clients": missing_expected_clients,
        "last_run_at": max(filter(None, [by_mode["paper"]["last_run_at"], by_mode["live"]["last_run_at"]]), default=None),
        "errors": by_mode["paper"]["errors"] + by_mode["live"]["errors"],
    }
