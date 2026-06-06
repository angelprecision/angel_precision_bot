# ap/authorization.py
# =============================================================================
# Live Authorization Gate — bot-side validation helpers.
#
# Mirrors the dashboard backend's client_authorization.py validation logic so
# the bot and dashboard agree on what counts as a valid authorization.
#
# Two authorizations are required before a LIVE client may open a NEW entry:
#   1. A one-time LIVE_TRADING authorization (accepted, not revoked, and whose
#      disclosure_version matches the CURRENT disclosure version). A material
#      disclosure change therefore invalidates the prior live authorization.
#   2. A WEEKLY_TRADING authorization (accepted, not revoked, not yet expired).
#
# Paper / sandbox clients are EXEMPT — they never require weekly authorization.
# The caller (entry gate) is responsible for only invoking these for LIVE-mode
# clients; see is_live_broker().
#
# All reads go through ap/db.py (psycopg2) against the shared Supabase Postgres
# client_authorizations table, keyed by client_email (== client_id here).
# =============================================================================

from __future__ import annotations

import os
import logging
from datetime import datetime, timezone

from ap.db import conn, run_with_retry

log = logging.getLogger("ap.authorization")

# Reason codes raised by the gate — kept stable for the operator dashboard.
LIVE_AUTHORIZATION_REQUIRED = "LIVE_AUTHORIZATION_REQUIRED"
WEEKLY_AUTHORIZATION_REQUIRED = "WEEKLY_AUTHORIZATION_REQUIRED"
# Raised when we cannot positively determine whether a broker is live or paper.
# An unknown/unverifiable broker mode is treated as a BLOCK on new entries
# (never silently assumed to be paper) so we never open an unauthorized live
# trade on a misconfigured/transiently-unreadable broker.
LIVE_AUTHORIZATION_GATE_UNAVAILABLE = "LIVE_AUTHORIZATION_GATE_UNAVAILABLE"


def authorization_gate_enforced() -> bool:
    """Whether the live authorization gate REJECTS unauthorized live entries
    (True) or only OBSERVES and logs them while allowing the entry (False).

    Controlled by env LIVE_AUTHORIZATION_GATE_ENFORCE. Defaults to FALSE
    (observe-only) so the gate can be rolled out and watched in the ledger
    before it begins hard-blocking live entries. Truthy values: 1/true/yes/on.
    """
    return (os.getenv("LIVE_AUTHORIZATION_GATE_ENFORCE", "false").strip().lower()
            in ("1", "true", "yes", "on"))


def broker_live_mode_known(broker) -> bool:
    """True when we can POSITIVELY classify the broker as live OR paper.

    A broker is classifiable when it exposes a non-empty base_url (live = not
    sandbox, paper = sandbox). If we cannot read a base_url at all, the mode is
    UNKNOWN/unverifiable and the caller must BLOCK the new entry (fail closed),
    never assume paper.
    """
    if broker is None:
        return False
    try:
        base_url = (
            getattr(broker, "base_url", None)
            or getattr(getattr(broker, "cfg", None), "base_url", None)
            or ""
        )
    except Exception:
        return False
    return bool(str(base_url))


def current_disclosure_version() -> str:
    """Current disclosure version, read from env so it stays in sync with the
    backend's disclosure.py DISCLOSURE_VERSION. Fallback matches the backend
    constant at the time of writing."""
    return os.getenv("DISCLOSURE_VERSION", "2026.06.1").strip() or "2026.06.1"


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def is_live_broker(broker) -> bool:
    """True when the broker routes to a LIVE (non-sandbox) base_url.

    Reuses ap.execution._is_sandbox_base_url so live-vs-paper detection is
    identical to the execution path. A client is LIVE only when the broker has
    a non-empty base_url that is NOT the Tradier sandbox. Paper/sim brokers
    (empty or sandbox base_url) are treated as NOT live, so they are exempt
    from authorization checks.
    """
    if broker is None:
        return False
    from ap.execution import _is_sandbox_base_url

    base_url = (
        getattr(broker, "base_url", None)
        or getattr(getattr(broker, "cfg", None), "base_url", None)
        or ""
    )
    base_url = str(base_url)
    if not base_url:
        return False
    return not _is_sandbox_base_url(base_url)


def execution_mode_for_broker(broker) -> str:
    """Return the execution mode label to stamp on an entry order at creation:
    'live' when the broker routes to a live (non-sandbox) base_url, else 'paper'.

    This is the SOURCE OF TRUTH for proof_trades on close — never recomputed at
    close time, since the client may switch modes while a position is open.
    """
    return "live" if is_live_broker(broker) else "paper"


def _fetch_authorizations(client_email: str, authorization_type: str) -> list[dict]:
    """Return all rows for (client_email, authorization_type), newest first.

    Read-only. Returns [] on any DB error so the caller can fail closed without
    crashing the worker loop.
    """
    def _fn():
        with conn() as c:
            c.execute(
                """
                SELECT accepted, disclosure_version, revoked_at,
                       expires_at, trading_week_start
                FROM client_authorizations
                WHERE client_email = %s
                  AND authorization_type = %s
                ORDER BY created_at DESC
                """,
                (client_email, authorization_type),
            )
            return c.fetchall()

    try:
        return run_with_retry(_fn) or []
    except Exception as e:
        log.error(
            "authorization read failed for %s/%s: %s",
            client_email, authorization_type, e,
        )
        return []


def _as_datetime(val) -> datetime | None:
    """Coerce a DB timestamptz value (datetime or ISO string) to an aware UTC
    datetime, or None if absent/unparseable."""
    if val is None:
        return None
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    try:
        dt = datetime.fromisoformat(str(val).replace("Z", "+00:00"))
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def valid_live_authorization(client_email: str) -> bool:
    """True if the client has a valid one-time LIVE_TRADING authorization:
    accepted, not revoked, and disclosure_version == current version.

    A material disclosure version change invalidates a prior live authorization.

    Material RISK-PROFILE reauthorization (a change to max positions, max trades/
    day, daily-loss %, score/context floor) is enforced at the BACKEND
    authorization layer: the dashboard recomputes the effective risk profile and
    invalidates the live authorization (and blocks re-activation / weekly
    acceptance) when its material hash no longer matches. We intentionally do NOT
    recompute that hash on the bot's hot entry path to avoid any drift from the
    backend's canonical _compute_client_risk_profile(): a divergent computation
    here could wrongly block a legitimate live entry. The bot enforces the
    accepted/not-revoked/disclosure-version invariants every entry; the backend
    owns material-risk invalidation by revoking/expiring the row the bot reads.
    """
    current = current_disclosure_version()
    for row in _fetch_authorizations(client_email, "LIVE_TRADING"):
        if not row.get("accepted"):
            continue
        if row.get("revoked_at") is not None:
            continue
        if str(row.get("disclosure_version") or "") != current:
            continue
        return True
    return False


def valid_weekly_authorization(client_email: str) -> bool:
    """True if the client has a valid WEEKLY_TRADING authorization:
    accepted, not revoked, and now is INSIDE the window
    trading_week_start <= now <= expires_at.

    The trading_week_start lower bound mirrors the dashboard backend: a weekend
    submission (whose window is the upcoming Mon–Fri) must NOT be honored until
    that trading week has actually begun.
    """
    now = _now_utc()
    for row in _fetch_authorizations(client_email, "WEEKLY_TRADING"):
        if not row.get("accepted"):
            continue
        if row.get("revoked_at") is not None:
            continue
        expires_at = _as_datetime(row.get("expires_at"))
        if expires_at is None or now > expires_at:
            continue
        start_at = _as_datetime(row.get("trading_week_start"))
        if start_at is not None and now < start_at:
            # Window has not started yet (e.g. weekend submission).
            continue
        return True
    return False


def check_live_authorization(client_email: str) -> str | None:
    """Return a reason_code if the LIVE client is NOT authorized to open a new
    entry, else None.

    Precedence: one-time LIVE_TRADING first, then WEEKLY_TRADING. Callers must
    only invoke this for LIVE-mode clients (see is_live_broker); paper/sandbox
    clients are exempt and must never reach this function.
    """
    if not valid_live_authorization(client_email):
        return LIVE_AUTHORIZATION_REQUIRED
    if not valid_weekly_authorization(client_email):
        return WEEKLY_AUTHORIZATION_REQUIRED
    return None
