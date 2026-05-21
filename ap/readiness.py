"""
ap/readiness.py — Single canonical readiness contract.

Background
==========
The dashboard backend has its own readiness endpoint. The bot itself
has implicit readiness checks scattered across client_runner, master_control,
and the runner's startup guard. They drift. A client could appear "ready" in
the UI but actually have a degraded organ that blocks trading.

This module is the SINGLE SOURCE OF TRUTH for readiness. Both the dashboard
backend AND the bot import compute_readiness() from here. Same inputs, same
output, no drift.

Inputs
======
A client row from members + optional organ health snapshot. No DB access
inside this module — callers fetch the data, this module computes the result.
This keeps the module DB-agnostic and testable.

Output
======
ReadinessReport — a dataclass with:
  email, mode, ready (bool), checks (per-check dict), blockers (list),
  critical_checks (list), allow_paper (bool), allow_live (bool),
  active_account_id, computed_at

The semantics:
  - ready=True   → client passes ALL critical checks for its mode
  - allow_paper=True → can trade in paper RIGHT NOW
  - allow_live=True  → can trade in live RIGHT NOW (stricter)
  - blockers list   → exact reason(s) for ready=False

Invariants (these must hold for live-client money):
  1. ready=True can never produce a False if all checks pass — no quiet
     downgrade.
  2. Missing organ data → not ready. Never "assume healthy".
  3. allow_live=True implies live_operator_approved=True.
  4. kill_switch_on disables both allow_paper and allow_live.
"""
from __future__ import annotations
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone, timedelta
from typing import Any, Optional


# How fresh must an organ heartbeat be to count as "online"?
DEFAULT_ORGAN_FRESHNESS_SEC = 300  # 5 minutes


# Critical organs the bot must have running for LIVE trading.
# Paper trading requires the same organs but the mode flag is relaxed.
REQUIRED_ORGANS_LIVE = (
    "client_runner",
    "fill_monitor",
    "reconciler",
    "exit_engine",
)
REQUIRED_ORGANS_PAPER = REQUIRED_ORGANS_LIVE  # same — degraded organs are unsafe in both modes


@dataclass
class ReadinessReport:
    email: str
    mode: str                          # 'PAPER' or 'LIVE'
    ready: bool                        # all critical checks pass
    checks: dict[str, bool]            # per-check truth
    blockers: list[str]                # specific check names that failed
    critical_checks: list[str]         # which checks were considered critical
    allow_paper: bool                  # can trade in paper RIGHT NOW
    allow_live: bool                   # can trade in live RIGHT NOW
    active_account_id: Optional[str] = None
    organ_status: dict[str, str] = field(default_factory=dict)  # organ -> status
    organ_last_heartbeat: dict[str, str] = field(default_factory=dict)  # organ -> iso ts
    computed_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _as_bool(v) -> bool:
    """Defensive truthy coercion. Strings like 'false', 'False', '0', 'no'
    must NOT count as True even though Python's bool() would treat them as
    truthy (non-empty string). DB columns are usually proper booleans but
    test data and partial migrations can leak strings in. Fail-closed here."""
    if v is None or v is False:
        return False
    if v is True:
        return True
    if isinstance(v, str):
        return v.strip().lower() in ("true", "t", "1", "yes", "y")
    if isinstance(v, (int, float)):
        return v != 0
    return bool(v)


def _organ_is_online(
    organ_status: dict[str, str],
    organ_last_heartbeat: dict[str, datetime],
    organ_name: str,
    now: datetime,
    freshness_sec: int = DEFAULT_ORGAN_FRESHNESS_SEC,
) -> bool:
    """An organ is online only if status=HEALTHY AND heartbeat is fresh."""
    status = (organ_status.get(organ_name) or "").upper()
    if status != "HEALTHY":
        return False
    last_hb = organ_last_heartbeat.get(organ_name)
    if last_hb is None:
        return False
    age = (now - last_hb).total_seconds()
    return age <= freshness_sec


def compute_readiness(
    member_row: dict[str, Any],
    organ_status: Optional[dict[str, str]] = None,
    organ_last_heartbeat: Optional[dict[str, datetime]] = None,
    organ_freshness_sec: int = DEFAULT_ORGAN_FRESHNESS_SEC,
    now: Optional[datetime] = None,
) -> ReadinessReport:
    """Compute readiness for a single client.

    Arguments:
        member_row: dict from the members table (email, subscription_active,
            tradier_active_mode, tradier_account_id, tradier_live_account_id,
            tradier_access_token, tradier_live_access_token, allow_live_trading,
            kill_switch).
        organ_status: {organ_name: status_str} from ap_organ_health, filtered
            to this client. Status values: HEALTHY / DEGRADED / DOWN / ''
        organ_last_heartbeat: {organ_name: datetime} last heartbeat ts.
        organ_freshness_sec: organ is offline if heartbeat older than this.
        now: time reference (for tests). Defaults to datetime.now(UTC).

    Returns:
        ReadinessReport with everything the dashboard or bot needs to gate.

    This function does ZERO I/O. Pure compute over its inputs.
    """
    if now is None:
        now = datetime.now(timezone.utc)
    if organ_status is None:
        organ_status = {}
    if organ_last_heartbeat is None:
        organ_last_heartbeat = {}

    email = str(member_row.get("email") or "")
    mode_raw = (member_row.get("tradier_active_mode") or "paper").lower()
    mode = "LIVE" if mode_raw == "live" else "PAPER"

    # ── Compute each individual check (no short-circuiting — all visible) ───
    has_paper_creds = bool(
        member_row.get("tradier_account_id")
        and member_row.get("tradier_access_token")
    )
    has_live_creds = bool(
        member_row.get("tradier_live_account_id")
        and member_row.get("tradier_live_access_token")
    )
    # Defensive bool coercion: DB columns are bool but partial migrations or
    # legacy data may leak strings like 'false'. _as_bool fails closed.
    operator_approved_live = _as_bool(member_row.get("allow_live_trading"))
    subscription_active    = _as_bool(member_row.get("subscription_active"))
    kill_switch_on         = _as_bool(member_row.get("kill_switch"))

    # Mode consistency:
    #   - mode=LIVE requires both live_connected AND live_operator_approved
    #   - mode=PAPER requires paper_connected
    mode_consistent = True
    if mode == "LIVE" and not has_live_creds:
        mode_consistent = False
    if mode == "LIVE" and not operator_approved_live:
        mode_consistent = False
    if mode == "PAPER" and not has_paper_creds:
        mode_consistent = False

    # Per-organ status
    runner_online      = _organ_is_online(organ_status, organ_last_heartbeat, "client_runner",  now, organ_freshness_sec)
    fill_monitor_online= _organ_is_online(organ_status, organ_last_heartbeat, "fill_monitor",   now, organ_freshness_sec)
    reconciler_online  = _organ_is_online(organ_status, organ_last_heartbeat, "reconciler",     now, organ_freshness_sec)
    exit_engine_online = _organ_is_online(organ_status, organ_last_heartbeat, "exit_engine",    now, organ_freshness_sec)

    checks: dict[str, bool] = {
        "subscription_active":    subscription_active,
        "paper_connected":        has_paper_creds,
        "live_connected":         has_live_creds,
        "live_operator_approved": operator_approved_live,
        "mode_consistent":        mode_consistent,
        "kill_switch_off":        not kill_switch_on,
        "runner_online":          runner_online,
        "fill_monitor_online":    fill_monitor_online,
        "reconciler_online":      reconciler_online,
        "exit_engine_online":     exit_engine_online,
    }

    # ── Which checks are CRITICAL depends on mode ──────────────────────────
    critical_checks: list[str] = [
        "subscription_active",
        "mode_consistent",
        "kill_switch_off",
        "runner_online",
        "fill_monitor_online",
        "reconciler_online",
        "exit_engine_online",
    ]
    if mode == "LIVE":
        critical_checks += [
            "live_connected",
            "live_operator_approved",
        ]
    else:  # PAPER
        critical_checks += ["paper_connected"]

    blockers = [c for c in critical_checks if not checks.get(c, False)]
    ready = (len(blockers) == 0)

    # ── allow_paper and allow_live are STRICTER than `ready` ───────────────
    # ready means "can trade in CURRENT mode right now".
    # allow_paper means "can trade in paper specifically right now".
    # allow_live means "can trade in live specifically right now".
    # A LIVE-approved client with kill_switch=on has ready=False, allow_live=False, allow_paper=False.
    organs_all_online = (
        runner_online and fill_monitor_online
        and reconciler_online and exit_engine_online
    )
    common_ok = (
        subscription_active
        and (not kill_switch_on)
        and organs_all_online
    )
    allow_paper = bool(common_ok and has_paper_creds)
    allow_live = bool(
        common_ok
        and has_live_creds
        and operator_approved_live
    )

    active_account_id = (
        member_row.get("tradier_live_account_id")
        if mode == "LIVE"
        else member_row.get("tradier_account_id")
    )

    # Serialize organ heartbeats for output
    hb_str = {k: v.isoformat() for k, v in organ_last_heartbeat.items()}

    return ReadinessReport(
        email=email,
        mode=mode,
        ready=ready,
        checks=checks,
        blockers=blockers,
        critical_checks=critical_checks,
        allow_paper=allow_paper,
        allow_live=allow_live,
        active_account_id=active_account_id,
        organ_status=dict(organ_status),
        organ_last_heartbeat=hb_str,
        computed_at=now.isoformat(),
    )
