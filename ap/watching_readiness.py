# ap/watching_readiness.py — P0: pre-open WATCHING readiness pass
# =============================================================================
# WHY THIS MODULE EXISTS
# ──────────────────────
# Production forensics (2026-07-04) found the WATCHING backlog is a structural
# clog, not a transient one:
#   • Jason (LIVE) carried 350 WATCHING trade_queue rows into the July 4
#     weekend; 166 were created before 2026-07-02 ET and 184 across the
#     Jul-2/Jul-3 window (Jul-3 was an NYSE full-closure holiday — those rows
#     were generated against dead quotes). Paper clients carried WATCHING rows
#     back to June 22.
#   • ap_recovery enforces live_no_replay_policy (post 2026-06-16 incident):
#     LIVE never resets WATCHING → NEW; it only reattaches orphaned
#     PENDING_TRIGGER orders. A WATCHING row whose paired ENTRY order already
#     EXPIRED therefore has NO rearm path and NO expiry path — it sits in
#     WATCHING forever.
#   • master_control._has_durable_duplicate_signal treats WATCHING as an
#     ACTIVE path, so every permanently-stuck WATCHING row blocks any future
#     dispatch of the same signal for that client. The clog is self-sealing.
#
# WHAT THIS DOES
# ──────────────
# A CLASSIFICATION-ONLY pass over a client's WATCHING rows. It never replays,
# never re-triggers, never resets anything to NEW — live_no_replay_policy is
# preserved byte-for-byte. Each WATCHING row is classified exactly once, in
# priority order:
#
#   1. ORPHANED_ORDER_TERMINAL → status EXPIRED
#      The paired ENTRY order (client_id, signal_id) is already terminal
#      (EXPIRED/CANCELLED/CANCELED/REJECTED/ERROR) with no broker submission.
#      The queue row can never produce a trade; leaving it WATCHING only
#      poisons dedup. last_error: READINESS_ORPHANED_ORDER_<order_status>.
#
#   2. STALE_BEFORE_CUTOFF → status ARCHIVED
#      created_ts (ET) is before the archive cutoff. Cutoff resolution:
#        a. AP_WATCHING_ARCHIVE_BEFORE_ET (ISO date, e.g. "2026-07-02")
#           — explicit operator cutoff, archives rows created before that
#           date's 00:00 ET. Used for the 2026-07-06 acceptance run.
#        b. else rolling: start-of-session N trading days back, where
#           N = AP_WATCHING_STALE_SESSIONS (default 2), computed against the
#           canonical NYSE calendar (ap.flatline_alarm.is_trading_day).
#      last_error: READINESS_ARCHIVED_STALE:<created-date-ET>.
#
#   3. NON_TRADING_DAY_ORIGIN → status ARCHIVED
#      created_ts falls on a weekend or NYSE full-closure holiday (e.g. the
#      2026-07-03 batch generated against dead quotes).
#      last_error: READINESS_ARCHIVED_NON_TRADING_DAY:<created-date-ET>.
#
#   4. NOT_IN_ALLOWLIST → status ARCHIVED
#      AP_ENTRY_TICKER_ALLOWLIST is set and the row's ticker is outside it
#      (same env + index-alias normalization as the queue dispatch gate, so
#      pre-open cleanup and dispatch-time gating can never disagree).
#      last_error: READINESS_ARCHIVED_NOT_IN_ALLOWLIST:<ticker>.
#
#   5. ELIGIBLE → untouched. Fresh, trading-day-origin, allowlisted rows with
#      a live (or absent-but-fresh) paired order remain WATCHING for the
#      normal handoff/reseed machinery.
#
# DESIGN / SAFETY
# ───────────────
# • Master switch: AP_WATCHING_READINESS_PASS=1 (default OFF). When unset the
#   pass is a no-op and system behavior is byte-for-byte unchanged.
# • Every UPDATE carries `AND status = 'WATCHING'` (CAS): a concurrent
#   transition (trigger, expiry, operator action) always wins; we never
#   clobber a row that moved.
# • dry_run=True performs the full classification and returns counts + row
#   samples but writes NOTHING. The morning handoff invokes dry-run first in
#   its own dry-run mode, mirroring the handoff_run_locks discipline.
# • Statuses used (ARCHIVED, EXPIRED) already exist in the queue vocabulary
#   and are already excluded from the durable duplicate guard's active set
#   ('NEW','PROCESSING','WATCHING','PENDING_TRIGGER','DEFERRED') and from
#   write_breach_last_error's writable set. No new status values introduced.
# • Fail-loud: DB errors abort the pass with ok=False and the error string;
#   partial progress counts are still reported.
# =============================================================================

from __future__ import annotations

import os
from datetime import date, datetime, timedelta, timezone
from typing import Any, Optional
from zoneinfo import ZoneInfo

from ap.logger import get_logger

log = get_logger("ap.watching_readiness")

ET = ZoneInfo("America/New_York")

# Terminal ENTRY-order statuses that prove the paired WATCHING queue row can
# never materialize a trade. SUBMITTED/FILLED are deliberately absent: a
# WATCHING row whose order actually submitted is a bookkeeping inconsistency
# we surface (counted as `inconsistent_submitted`) but never auto-mutate.
_ORDER_TERMINAL_STATUSES = (
    "EXPIRED", "CANCELLED", "CANCELED", "REJECTED", "ERROR",
)

# Same alias map as the dispatch gate in ap/queue.py — kept identical so the
# pre-open pass and dispatch-time gate can never disagree on a ticker.
_INDEX_ALIAS = {"^GSPC": "SPY", "^NDX": "QQQ", "^RUT": "IWM"}


def _enabled() -> bool:
    return os.getenv("AP_WATCHING_READINESS_PASS", "0").strip() in ("1", "true", "yes")


def _allowlist() -> Optional[set[str]]:
    raw = os.getenv("AP_ENTRY_TICKER_ALLOWLIST", "").strip()
    if not raw:
        return None
    vals = {t.strip().upper() for t in raw.split(",") if t.strip()}
    return vals or None


def _is_trading_day(d: date) -> bool:
    """Canonical NYSE calendar; fail-safe to weekday-only on import failure."""
    try:
        from ap.flatline_alarm import is_trading_day as _nyse
        return _nyse(d)
    except Exception:
        return d.weekday() < 5


def _resolve_archive_cutoff_et(now_et: datetime) -> tuple[datetime, str]:
    """Return (cutoff ET datetime, source_label). Rows created strictly before
    the cutoff are archived as STALE_BEFORE_CUTOFF."""
    explicit = os.getenv("AP_WATCHING_ARCHIVE_BEFORE_ET", "").strip()
    if explicit:
        try:
            d = date.fromisoformat(explicit)
            return datetime(d.year, d.month, d.day, tzinfo=ET), f"env:{explicit}"
        except ValueError:
            log.error(
                "watching_readiness: AP_WATCHING_ARCHIVE_BEFORE_ET=%r is not "
                "ISO YYYY-MM-DD — falling back to rolling cutoff", explicit,
            )
    sessions = 2
    raw_sessions = os.getenv("AP_WATCHING_STALE_SESSIONS", "").strip()
    if raw_sessions:
        try:
            sessions = max(1, int(raw_sessions))
        except ValueError:
            log.error(
                "watching_readiness: AP_WATCHING_STALE_SESSIONS=%r invalid — "
                "using default 2", raw_sessions,
            )
    d = now_et.date()
    remaining = sessions
    guard = 0
    while remaining > 0 and guard < 30:
        d = d - timedelta(days=1)
        guard += 1
        if _is_trading_day(d):
            remaining -= 1
    return datetime(d.year, d.month, d.day, tzinfo=ET), f"rolling:{sessions}_sessions"


def _row_ticker(payload: Any) -> str:
    if isinstance(payload, dict):
        t = str(payload.get("ticker") or payload.get("symbol") or "").upper()
    else:
        t = ""
    return _INDEX_ALIAS.get(t, t)


def run_watching_readiness_pass(
    client_id: str,
    *,
    execution_mode: str = "",
    dry_run: bool = False,
    now: Optional[datetime] = None,
) -> dict:
    """Classify and (unless dry_run) transition a client's WATCHING rows.

    Returns a result dict:
      {ok, enabled, dry_run, client_id, execution_mode, cutoff_et,
       cutoff_source, scanned, orphaned_terminalized, archived_stale,
       archived_non_trading_day, archived_not_in_allowlist, eligible,
       inconsistent_submitted, samples, error}
    """
    result: dict = {
        "ok": True,
        "enabled": _enabled(),
        "dry_run": bool(dry_run),
        "client_id": client_id,
        "execution_mode": execution_mode,
        "cutoff_et": None,
        "cutoff_source": None,
        "scanned": 0,
        "orphaned_terminalized": 0,
        "archived_stale": 0,
        "archived_non_trading_day": 0,
        "archived_not_in_allowlist": 0,
        "eligible": 0,
        "inconsistent_submitted": 0,
        "samples": {},
        "error": None,
    }
    if not result["enabled"]:
        log.info(
            "watching_readiness SKIPPED client=%s — AP_WATCHING_READINESS_PASS "
            "not enabled", client_id,
        )
        return result

    now_et = (now.astimezone(ET) if now else datetime.now(ET))
    cutoff_et, cutoff_source = _resolve_archive_cutoff_et(now_et)
    result["cutoff_et"] = cutoff_et.isoformat()
    result["cutoff_source"] = cutoff_source
    allowlist = _allowlist()

    try:
        from ap.db import conn, run_with_retry
    except Exception as exc:  # pragma: no cover — import environment failure
        result["ok"] = False
        result["error"] = f"db_import_failed:{exc}"
        log.error("watching_readiness db import failed: %s", exc)
        return result

    def _load_rows():
        with conn() as c:
            c.execute(
                """
                SELECT tq.id,
                       tq.signal_id,
                       tq.created_ts,
                       tq.payload,
                       o.status  AS order_status,
                       o.broker_order_id,
                       o.submitted_ts
                FROM   trade_queue tq
                LEFT JOIN LATERAL (
                        SELECT status, broker_order_id, submitted_ts
                        FROM   orders
                        WHERE  orders.client_id = tq.client_id
                          AND  orders.signal_id = tq.signal_id
                          AND  orders.kind      = 'ENTRY'
                        ORDER BY orders.created_ts DESC
                        LIMIT 1
                ) o ON TRUE
                WHERE  tq.client_id = %s
                  AND  tq.status    = 'WATCHING'
                ORDER BY tq.created_ts ASC
                """,
                (client_id,),
            )
            return c.fetchall()

    def _transition(queue_id: int, new_status: str, last_error: str) -> int:
        """CAS transition WATCHING → new_status. Returns rowcount (0 or 1)."""
        with conn() as c:
            c.execute(
                """
                UPDATE trade_queue
                   SET status = %s,
                       last_error = %s
                 WHERE id = %s
                   AND status = 'WATCHING'
                """,
                (new_status, last_error[:400], int(queue_id)),
            )
            return c.rowcount

    try:
        rows = run_with_retry(_load_rows) or []
    except Exception as exc:
        result["ok"] = False
        result["error"] = f"load_failed:{exc}"
        log.error("watching_readiness load failed client=%s: %s", client_id, exc)
        return result

    result["scanned"] = len(rows)

    def _sample(bucket: str, row_desc: dict) -> None:
        result["samples"].setdefault(bucket, [])
        if len(result["samples"][bucket]) < 5:
            result["samples"][bucket].append(row_desc)

    for row in rows:
        r = dict(row or {})
        qid = r.get("id")
        signal_id = str(r.get("signal_id") or "")
        created_ts = r.get("created_ts")
        payload = r.get("payload")
        order_status = str(r.get("order_status") or "").upper()
        has_broker_proof = bool(r.get("broker_order_id")) or bool(r.get("submitted_ts"))

        created_et: Optional[datetime] = None
        if isinstance(created_ts, datetime):
            created_et = (
                created_ts.astimezone(ET)
                if created_ts.tzinfo
                else created_ts.replace(tzinfo=timezone.utc).astimezone(ET)
            )
        created_date_str = created_et.date().isoformat() if created_et else "unknown"
        ticker = _row_ticker(payload)
        desc = {
            "queue_id": qid,
            "signal_id": signal_id,
            "ticker": ticker,
            "created_et": created_date_str,
            "order_status": order_status or None,
        }

        # Bookkeeping inconsistency: WATCHING queue row but the order actually
        # reached the broker. Surface it; never auto-mutate.
        if has_broker_proof:
            result["inconsistent_submitted"] += 1
            _sample("inconsistent_submitted", desc)
            log.warning(
                "watching_readiness INCONSISTENT_SUBMITTED client=%s queue_id=%s "
                "signal_id=%s order_status=%s — WATCHING row with broker proof; "
                "left untouched for manual review",
                client_id, qid, signal_id, order_status,
            )
            continue

        new_status: Optional[str] = None
        last_error = ""
        bucket = ""

        if order_status in _ORDER_TERMINAL_STATUSES:
            new_status = "EXPIRED"
            last_error = f"READINESS_ORPHANED_ORDER_{order_status}"
            bucket = "orphaned_terminalized"
        elif created_et is not None and created_et < cutoff_et:
            new_status = "ARCHIVED"
            last_error = f"READINESS_ARCHIVED_STALE:{created_date_str}"
            bucket = "archived_stale"
        elif created_et is not None and not _is_trading_day(created_et.date()):
            new_status = "ARCHIVED"
            last_error = f"READINESS_ARCHIVED_NON_TRADING_DAY:{created_date_str}"
            bucket = "archived_non_trading_day"
        elif allowlist is not None and ticker and ticker not in allowlist:
            new_status = "ARCHIVED"
            last_error = f"READINESS_ARCHIVED_NOT_IN_ALLOWLIST:{ticker}"
            bucket = "archived_not_in_allowlist"

        if new_status is None:
            result["eligible"] += 1
            _sample("eligible", desc)
            continue

        _sample(bucket, desc)
        if dry_run:
            result[bucket] += 1
            continue
        try:
            changed = run_with_retry(lambda: _transition(int(qid), new_status, last_error))
        except Exception as exc:
            result["ok"] = False
            result["error"] = f"transition_failed:queue_id={qid}:{exc}"
            log.error(
                "watching_readiness transition failed client=%s queue_id=%s: %s",
                client_id, qid, exc,
            )
            return result
        if changed:
            result[bucket] += 1
            log.info(
                "watching_readiness %s client=%s queue_id=%s signal_id=%s "
                "ticker=%s created_et=%s -> %s (%s)",
                bucket.upper(), client_id, qid, signal_id, ticker,
                created_date_str, new_status, last_error,
            )
        else:
            # CAS lost: row moved concurrently (trigger/expiry/operator).
            # The concurrent transition wins by design.
            log.info(
                "watching_readiness CAS_SKIP client=%s queue_id=%s — row left "
                "WATCHING concurrently transitioned", client_id, qid,
            )

    log.info(
        "watching_readiness COMPLETE client=%s mode=%s dry_run=%s scanned=%d "
        "orphaned=%d stale=%d non_trading=%d not_allowlisted=%d eligible=%d "
        "inconsistent=%d cutoff=%s(%s)",
        client_id, execution_mode, dry_run, result["scanned"],
        result["orphaned_terminalized"], result["archived_stale"],
        result["archived_non_trading_day"], result["archived_not_in_allowlist"],
        result["eligible"], result["inconsistent_submitted"],
        result["cutoff_et"], result["cutoff_source"],
    )
    return result
