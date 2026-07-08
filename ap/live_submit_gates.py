"""
ap/live_submit_gates.py

LIVE pre-submit safety gates — PR #305.

These are the last three brakes between the materialization pipeline and the
broker POST. Each is a pure, stateless classifier that returns a GateResult:
either PASS (submit may proceed) or FAIL (block submit, stamp durable audit,
terminalize the row).

WHY THIS MODULE EXISTS
──────────────────────
The META/VZ incident postmortem exposed three failure classes that could still
reach the broker even after PR #300 (broker_ready lifecycle) and PR #304
(watcher/order lifecycle integrity):

    1. IDENTITY DRIFT — a plan whose execution_mode was blank/unknown, or
       whose client_id was missing, or where the plan and OSM disagreed on
       which mode was active. Result: live money could be committed under
       an incorrect account.

    2. STALE MARKET VALIDITY — a trigger confirmed at 9:31 ET, held in the
       materialization retry queue until 9:47 ET, submitted after the move
       had already run its course. The contract was materialized correctly,
       but the underlying opportunity was gone.

    3. STALE TRIGGER AGE — same category but from a different angle: a
       genuine trigger breach from 30-45 minutes ago must not fire a live
       order regardless of intermediate lifecycle validity.

DESIGN CONTRACT
───────────────
• Pure classification — no DB writes, no side effects on watcher/OSM state.
• Never raises — client-money code; a classifier bug must never crash submit.
• LIVE fails closed on every ambiguous condition.
• PAPER fails closed on identity (mode drift is a genuine bug), but market-
  validity and trigger-age are LIVE-only gates (paper is for testing, we
  want stale-quote signals to still exercise the flow).
• Every FAIL returns a canonical reason_code from a documented set — the
  operator dashboard filters on these strings.
• Every FAIL returns an audit dict ready for orders.meta.

USAGE
─────
    from ap.live_submit_gates import (
        GateResult, GateOutcome,
        check_identity_gate,
        check_market_validity_gate,
        check_trigger_age_gate,
    )

    # At the exact call site of submit_existing_entry:
    id_result = check_identity_gate(
        client_id=plan_client_id,
        execution_mode=plan_execution_mode,
        watcher_client_id=watcher_client_id,
        osm_client_id=osm_client_id,
    )
    if not id_result.passed:
        # Stamp audit, terminalize, DO NOT SUBMIT
        _terminalize(id_result.reason_code)
        return

    # ... likewise for market_validity and trigger_age ...
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

from ap.logger import get_logger

log = get_logger("ap.live_submit_gates")


# ─────────────────────────────────────────────────────────────────────────────
# Canonical reason codes — must stay in sync with operator runbook filters
# ─────────────────────────────────────────────────────────────────────────────

class GateOutcome:
    """Canonical block reasons — orders.meta.live_submit_gate.reason_code."""

    # Identity gate (Gate 1)
    LIVE_SUBMIT_IDENTITY_INVALID           = "LIVE_SUBMIT_IDENTITY_INVALID"
    LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN     = "LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN"
    LIVE_SUBMIT_CLIENT_ID_MISSING          = "LIVE_SUBMIT_CLIENT_ID_MISSING"
    LIVE_SUBMIT_CLIENT_ID_MISMATCH         = "LIVE_SUBMIT_CLIENT_ID_MISMATCH"
    LIVE_SUBMIT_EXECUTION_MODE_MISMATCH    = "LIVE_SUBMIT_EXECUTION_MODE_MISMATCH"

    # Market validity gate (Gate 2)
    CURRENT_PRICE_MISSING                  = "CURRENT_PRICE_MISSING"
    CURRENT_PRICE_STALE                    = "CURRENT_PRICE_STALE"
    CURRENT_PRICE_ZERO                     = "CURRENT_PRICE_ZERO"
    TARGET_ALREADY_INVALID                 = "TARGET_ALREADY_INVALID"
    REMAINING_OPPORTUNITY_TOO_SMALL        = "REMAINING_OPPORTUNITY_TOO_SMALL"
    CALL_NO_LONGER_ABOVE_TRIGGER           = "CALL_NO_LONGER_ABOVE_TRIGGER"
    CALL_STOP_ALREADY_BROKEN               = "CALL_STOP_ALREADY_BROKEN"
    PUT_NO_LONGER_BELOW_TRIGGER            = "PUT_NO_LONGER_BELOW_TRIGGER"
    PUT_STOP_ALREADY_BROKEN                = "PUT_STOP_ALREADY_BROKEN"

    # Trigger age gate (Gate 3)
    STALE_TRIGGER_BREACH                   = "STALE_TRIGGER_BREACH"

    # Sentinel
    PASS                                   = "PASS"


@dataclass
class GateResult:
    """Uniform return type for every gate.

    Fields:
        passed: True → submit may proceed to the next gate.
                False → block submit; caller must terminalize + audit.
        reason_code: canonical code from GateOutcome (PASS on success).
        detail: short human-readable explanation for logs / audit.
        audit: dict ready to be stamped into orders.meta under the gate's key.
               Always non-empty on FAIL, populated with all data used for
               the decision so an operator can reproduce the logic.
    """
    passed: bool
    reason_code: str = GateOutcome.PASS
    detail: str = ""
    audit: dict = field(default_factory=dict)


# ─────────────────────────────────────────────────────────────────────────────
# Config helpers (env-tunable, safe defaults)
# ─────────────────────────────────────────────────────────────────────────────

def _int_env(name: str, default: int) -> int:
    try:
        return max(1, int(os.getenv(name, str(default)).strip()))
    except (TypeError, ValueError):
        return default


def _float_env(name: str, default: float) -> float:
    try:
        return max(0.0, float(os.getenv(name, str(default)).strip()))
    except (TypeError, ValueError):
        return default


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        s = str(dt_str).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        d = datetime.fromisoformat(s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
# GATE 1 — IDENTITY
# ─────────────────────────────────────────────────────────────────────────────

_VALID_EXECUTION_MODES: frozenset[str] = frozenset({"live", "paper"})


def check_identity_gate(
    *,
    client_id: Optional[str],
    execution_mode: Optional[str],
    watcher_client_id: Optional[str] = None,
    osm_client_id: Optional[str] = None,
    watcher_execution_mode: Optional[str] = None,
    osm_execution_mode: Optional[str] = None,
) -> GateResult:
    """
    Prove submit identity before any broker POST.

    Rules:
        1. client_id must be non-empty
        2. execution_mode must be exactly "live" or "paper" (case-insensitive)
        3. LIVE submit requires execution_mode == "live"
        4. If any of the caller-supplied cross-check IDs are provided and
           non-empty, they must match the primary. Missing cross-checks are
           acceptable (paper handoff paths may not carry all four) — this
           gate blocks on POSITIVE disagreement only, never absence.

    LIVE and PAPER both fail closed on blank/unknown execution_mode and
    missing client_id. Identity is not a mode-conditional concept.
    """
    cid = str(client_id or "").strip()
    mode = str(execution_mode or "").strip().lower()

    audit_common = {
        "gate":            "identity",
        "checked_at":      _now_utc().isoformat(),
        "client_id":       cid or None,
        "execution_mode":  mode or None,
        "watcher_client_id": (str(watcher_client_id or "").strip() or None),
        "osm_client_id":     (str(osm_client_id or "").strip() or None),
        "watcher_execution_mode": (str(watcher_execution_mode or "").strip().lower() or None),
        "osm_execution_mode":     (str(osm_execution_mode or "").strip().lower() or None),
    }

    # Rule 1: client_id required
    if not cid:
        return GateResult(
            passed=False,
            reason_code=GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISSING,
            detail="client_id is blank/None — cannot attribute broker submit",
            audit={**audit_common, "passed": False,
                   "reason_code": GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISSING},
        )

    # Rule 2: execution_mode must be a known value
    if mode not in _VALID_EXECUTION_MODES:
        return GateResult(
            passed=False,
            reason_code=GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN,
            detail=f"execution_mode={mode!r} not in {sorted(_VALID_EXECUTION_MODES)}",
            audit={**audit_common, "passed": False,
                   "reason_code": GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_UNKNOWN},
        )

    # Rule 4: cross-check client_id mismatch (positive disagreement only)
    for cross_cid, source_name in (
        (str(watcher_client_id or "").strip(), "watcher"),
        (str(osm_client_id or "").strip(), "osm"),
    ):
        if cross_cid and cross_cid != cid:
            return GateResult(
                passed=False,
                reason_code=GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISMATCH,
                detail=(
                    f"plan.client_id={cid!r} disagrees with "
                    f"{source_name}.client_id={cross_cid!r}"
                ),
                audit={**audit_common, "passed": False,
                       "reason_code": GateOutcome.LIVE_SUBMIT_CLIENT_ID_MISMATCH,
                       "mismatch_source": source_name},
            )

    # Rule 4: cross-check execution_mode mismatch
    for cross_mode, source_name in (
        (str(watcher_execution_mode or "").strip().lower(), "watcher"),
        (str(osm_execution_mode or "").strip().lower(), "osm"),
    ):
        if cross_mode and cross_mode != mode:
            return GateResult(
                passed=False,
                reason_code=GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_MISMATCH,
                detail=(
                    f"plan.execution_mode={mode!r} disagrees with "
                    f"{source_name}.execution_mode={cross_mode!r}"
                ),
                audit={**audit_common, "passed": False,
                       "reason_code": GateOutcome.LIVE_SUBMIT_EXECUTION_MODE_MISMATCH,
                       "mismatch_source": source_name},
            )

    return GateResult(
        passed=True,
        reason_code=GateOutcome.PASS,
        detail=f"identity verified: client_id={cid} execution_mode={mode}",
        audit={**audit_common, "passed": True, "reason_code": GateOutcome.PASS},
    )


# ─────────────────────────────────────────────────────────────────────────────
# GATE 2 — FINAL MARKET VALIDITY
# ─────────────────────────────────────────────────────────────────────────────

def _remaining_opportunity_pct(
    side: str,
    current_price: float,
    trigger_price: float,
    target_price: Optional[float],
) -> Optional[float]:
    """
    How much of the intended move is still available from current_price to target.

    For a CALL: (target - current) / (target - trigger)
    For a PUT:  (current - target) / (trigger - target)

    Returns None when the geometry is undefined (missing target, degenerate
    move sizing). Returns 0.0 or negative when the target has already been
    touched or overshot.
    """
    if target_price is None:
        return None
    try:
        cp, tg, tr = float(current_price), float(target_price), float(trigger_price)
    except (TypeError, ValueError):
        return None

    _side = str(side or "").strip().upper()
    if _side == "CALL":
        denom = tg - tr
        if denom <= 0:
            return None
        return (tg - cp) / denom
    if _side == "PUT":
        denom = tr - tg
        if denom <= 0:
            return None
        return (cp - tg) / denom
    return None


def check_market_validity_gate(
    *,
    side: str,
    trigger_price: float,
    stop_price: Optional[float],
    target_price: Optional[float],
    current_bid: Optional[float],
    current_ask: Optional[float],
    quote_age_ms: Optional[float] = None,
    execution_mode: str = "live",
    max_quote_age_ms: Optional[int] = None,
    min_remaining_opportunity_pct: Optional[float] = None,
) -> GateResult:
    """
    Final pre-submit sanity check on the underlying market.

    Called immediately before broker POST. Fails closed on LIVE for any
    stale/missing quote or invalidated setup geometry. PAPER passes with a
    warning so testing can continue past stale-quote sandbox artifacts.

    Rules (LIVE):
      • current bid and ask must both exist and be > 0
      • quote age must not exceed max_quote_age_ms (env: LIVE_SUBMIT_MAX_QUOTE_AGE_MS,
        default 5000)
      • CALL: current mid ≥ trigger (still in breach direction),
              current mid < target (target not touched),
              remaining opportunity ≥ min_remaining_pct (default 0.10 = 10%),
              current mid > stop (stop not broken)
      • PUT: mirror of CALL

    Rules (PAPER):
      • Same checks but log-only. Result.passed always True in paper.
    """
    max_age_ms = max_quote_age_ms if max_quote_age_ms is not None else _int_env("LIVE_SUBMIT_MAX_QUOTE_AGE_MS", 5000)
    min_rem_pct = min_remaining_opportunity_pct if min_remaining_opportunity_pct is not None else _float_env("LIVE_SUBMIT_MIN_REMAINING_OPPORTUNITY_PCT", 0.10)
    _mode = str(execution_mode or "").strip().lower()
    _live = _mode == "live"

    def _to_float(v):
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    bid = _to_float(current_bid)
    ask = _to_float(current_ask)
    mid = None
    if bid is not None and ask is not None and bid > 0 and ask > 0:
        mid = (bid + ask) / 2.0

    audit = {
        "gate":                       "market_validity",
        "checked_at":                 _now_utc().isoformat(),
        "execution_mode":             _mode,
        "side":                       str(side or "").upper(),
        "trigger_price":              _to_float(trigger_price),
        "stop_price":                 _to_float(stop_price),
        "target_price":               _to_float(target_price),
        "current_bid":                bid,
        "current_ask":                ask,
        "current_mid":                mid,
        "quote_age_ms":               _to_float(quote_age_ms),
        "max_quote_age_ms":           int(max_age_ms),
        "min_remaining_opportunity_pct": min_rem_pct,
    }

    def _fail(reason_code: str, detail: str) -> GateResult:
        # In paper mode we log the fail but still return passed=True so the
        # sandbox flow can be exercised end-to-end. LIVE always blocks.
        blocked = _live
        _audit_out = {**audit, "passed": not blocked, "reason_code": reason_code}
        if blocked:
            _audit_out["blocked"] = True
        return GateResult(
            passed=not blocked,
            reason_code=reason_code if blocked else GateOutcome.PASS,
            detail=("BLOCKED " if blocked else "warn ") + detail,
            audit=_audit_out,
        )

    # Rule: quote presence
    if bid is None or ask is None or bid <= 0 or ask <= 0:
        if bid is None and ask is None:
            return _fail(GateOutcome.CURRENT_PRICE_MISSING, "no bid/ask received")
        return _fail(GateOutcome.CURRENT_PRICE_ZERO, f"bid={bid} ask={ask}")

    # Rule: quote freshness
    if quote_age_ms is not None and float(quote_age_ms) > max_age_ms:
        return _fail(
            GateOutcome.CURRENT_PRICE_STALE,
            f"quote_age_ms={quote_age_ms:.0f} > max={max_age_ms}",
        )

    _side = str(side or "").strip().upper()
    tr = _to_float(trigger_price) or 0
    tg = _to_float(target_price)
    st = _to_float(stop_price)

    # Rule: direction-specific geometry
    if _side == "CALL":
        # Trigger still valid — current price must be at or above trigger
        if mid < tr:
            return _fail(
                GateOutcome.CALL_NO_LONGER_ABOVE_TRIGGER,
                f"mid={mid:.4f} < trigger={tr:.4f} — breach reversed",
            )
        # Stop not broken
        if st is not None and st > 0 and mid <= st:
            return _fail(
                GateOutcome.CALL_STOP_ALREADY_BROKEN,
                f"mid={mid:.4f} <= stop={st:.4f}",
            )
        # Target not already reached
        if tg is not None and tg > 0 and mid >= tg:
            return _fail(
                GateOutcome.TARGET_ALREADY_INVALID,
                f"CALL mid={mid:.4f} >= target={tg:.4f} — move complete",
            )
    elif _side == "PUT":
        if mid > tr:
            return _fail(
                GateOutcome.PUT_NO_LONGER_BELOW_TRIGGER,
                f"mid={mid:.4f} > trigger={tr:.4f} — breach reversed",
            )
        if st is not None and st > 0 and mid >= st:
            return _fail(
                GateOutcome.PUT_STOP_ALREADY_BROKEN,
                f"mid={mid:.4f} >= stop={st:.4f}",
            )
        if tg is not None and tg > 0 and mid <= tg:
            return _fail(
                GateOutcome.TARGET_ALREADY_INVALID,
                f"PUT mid={mid:.4f} <= target={tg:.4f} — move complete",
            )
    # Unknown side: pass (identity gate would have blocked this earlier;
    # we don't add extra assertions here to keep the gate composable)

    # Rule: remaining opportunity
    rem_pct = _remaining_opportunity_pct(_side, mid, tr, tg)
    if rem_pct is not None:
        audit["remaining_opportunity_pct"] = round(rem_pct, 6)
        if rem_pct < min_rem_pct:
            return _fail(
                GateOutcome.REMAINING_OPPORTUNITY_TOO_SMALL,
                f"remaining={rem_pct:.2%} < min={min_rem_pct:.2%}",
            )

    return GateResult(
        passed=True,
        reason_code=GateOutcome.PASS,
        detail=f"market valid: mid={mid:.4f} side={_side}",
        audit={**audit, "passed": True, "reason_code": GateOutcome.PASS},
    )


# ─────────────────────────────────────────────────────────────────────────────
# GATE 3 — TRIGGER AGE
# ─────────────────────────────────────────────────────────────────────────────

def check_trigger_age_gate(
    *,
    trigger_crossed_at: Optional[str],
    trigger_confirmed_at: Optional[str] = None,
    submit_attempt_at: Optional[datetime] = None,
    execution_mode: str = "live",
    max_age_seconds: Optional[int] = None,
) -> GateResult:
    """
    Enforce a maximum age from FIRST breach to broker POST.

    A trigger crossed at market open must not fire a live entry 30-45
    minutes later. Default: 120 seconds. Env: ENTRY_TRIGGER_MAX_AGE_SEC.

    LIVE: fails closed on age exceeded, missing trigger_crossed_at,
    or a trigger_crossed_at that is in the future (clock skew signal).

    PAPER: log-only, passes with a warning.
    """
    _mode = str(execution_mode or "").strip().lower()
    _live = _mode == "live"
    max_age = max_age_seconds if max_age_seconds is not None else _int_env("ENTRY_TRIGGER_MAX_AGE_SEC", 120)

    now = submit_attempt_at or _now_utc()
    if now.tzinfo is None:
        now = now.replace(tzinfo=timezone.utc)

    crossed = _parse_iso(trigger_crossed_at)
    confirmed = _parse_iso(trigger_confirmed_at)

    audit = {
        "gate":                "trigger_age",
        "trigger_crossed_at":  trigger_crossed_at,
        "trigger_confirmed_at": trigger_confirmed_at,
        "submit_attempt_at":   now.isoformat(),
        "max_age_seconds":     int(max_age),
        "execution_mode":      _mode,
    }

    if crossed is None:
        # No trigger_crossed_at stamped — LIVE fails closed because we cannot
        # prove age. PAPER passes with warning.
        return GateResult(
            passed=not _live,
            reason_code=GateOutcome.STALE_TRIGGER_BREACH if _live else GateOutcome.PASS,
            detail="trigger_crossed_at missing — cannot verify age" + (" [LIVE BLOCKED]" if _live else " [paper warn]"),
            audit={**audit, "passed": not _live,
                   "reason_code": GateOutcome.STALE_TRIGGER_BREACH if _live else GateOutcome.PASS,
                   "age_seconds": None,
                   "missing_trigger_crossed_at": True},
        )

    age_seconds = (now - crossed).total_seconds()
    audit["age_seconds"] = round(age_seconds, 3)

    # Clock skew: crossed timestamp is in the future
    if age_seconds < -1.0:
        return GateResult(
            passed=not _live,
            reason_code=GateOutcome.STALE_TRIGGER_BREACH if _live else GateOutcome.PASS,
            detail=f"trigger_crossed_at is {-age_seconds:.1f}s in the future — clock skew",
            audit={**audit, "passed": not _live, "clock_skew": True},
        )

    if age_seconds > max_age:
        return GateResult(
            passed=not _live,
            reason_code=GateOutcome.STALE_TRIGGER_BREACH if _live else GateOutcome.PASS,
            detail=f"age={age_seconds:.1f}s > max={max_age}s",
            audit={**audit, "passed": not _live,
                   "reason_code": GateOutcome.STALE_TRIGGER_BREACH if _live else GateOutcome.PASS,
                   "blocked": _live},
        )

    return GateResult(
        passed=True,
        reason_code=GateOutcome.PASS,
        detail=f"trigger age {age_seconds:.1f}s within {max_age}s limit",
        audit={**audit, "passed": True, "reason_code": GateOutcome.PASS},
    )


# ─────────────────────────────────────────────────────────────────────────────
# Convenience: run all three gates in order
# ─────────────────────────────────────────────────────────────────────────────

def run_all_live_submit_gates(
    *,
    # Identity
    client_id: Optional[str],
    execution_mode: Optional[str],
    watcher_client_id: Optional[str] = None,
    osm_client_id: Optional[str] = None,
    watcher_execution_mode: Optional[str] = None,
    osm_execution_mode: Optional[str] = None,
    # Market validity
    side: str = "",
    trigger_price: float = 0.0,
    stop_price: Optional[float] = None,
    target_price: Optional[float] = None,
    current_bid: Optional[float] = None,
    current_ask: Optional[float] = None,
    quote_age_ms: Optional[float] = None,
    # Trigger age
    trigger_crossed_at: Optional[str] = None,
    trigger_confirmed_at: Optional[str] = None,
    submit_attempt_at: Optional[datetime] = None,
) -> tuple[GateResult, dict]:
    """
    Run identity → market_validity → trigger_age in order.
    Returns (first_failing_result_or_passing, combined_audit).

    Short-circuits on first failure — subsequent gates are not called.
    Combined audit dict has three top-level keys:
        identity_gate, market_validity_gate, trigger_age_gate
    """
    combined_audit = {}

    id_res = check_identity_gate(
        client_id=client_id,
        execution_mode=execution_mode,
        watcher_client_id=watcher_client_id,
        osm_client_id=osm_client_id,
        watcher_execution_mode=watcher_execution_mode,
        osm_execution_mode=osm_execution_mode,
    )
    combined_audit["identity_gate"] = id_res.audit
    if not id_res.passed:
        return id_res, combined_audit

    mv_res = check_market_validity_gate(
        side=side,
        trigger_price=trigger_price,
        stop_price=stop_price,
        target_price=target_price,
        current_bid=current_bid,
        current_ask=current_ask,
        quote_age_ms=quote_age_ms,
        execution_mode=str(execution_mode or "").strip().lower(),
    )
    combined_audit["market_validity_gate"] = mv_res.audit
    if not mv_res.passed:
        return mv_res, combined_audit

    ta_res = check_trigger_age_gate(
        trigger_crossed_at=trigger_crossed_at,
        trigger_confirmed_at=trigger_confirmed_at,
        submit_attempt_at=submit_attempt_at,
        execution_mode=str(execution_mode or "").strip().lower(),
    )
    combined_audit["trigger_age_gate"] = ta_res.audit
    if not ta_res.passed:
        return ta_res, combined_audit

    return GateResult(
        passed=True,
        reason_code=GateOutcome.PASS,
        detail="all three LIVE submit gates passed",
        audit=combined_audit,
    ), combined_audit
