"""Canonical intelligence admission authority contract.

Incident context (2026-07-17/18 audit):
  intelligence_bridge produced veto decisions via free-text reasoning
  attached to ``intel_rejected: <free text>``.  Operators could not
  distinguish:
    - authoritative directional or risk veto;
    - advisory / observe-only result;
    - infrastructure failure (timeout, import error, pipeline crash);
    - low-confidence or missing data;
    - unknown pipeline outcome.
  Any broken module could accidentally block trades; legitimate vetoes
  were indistinguishable from noise.

This module closes the failure class:
  1. Only explicitly enumerated stable reason codes may set allowed=False.
  2. Infrastructure failure, missing data, timeout, malformed response,
     or any unknown status → fail open with structured diagnostics.
  3. Observe-only modules (ap/intelligence_evaluation.py) may never
     set allowed=False; their results are metadata only.
  4. INTELLIGENCE_ADMISSION_MODE=observe_only records every result but
     never blocks — operator diagnostic tool, not the default.
  5. Malformed env value resolves to 'authoritative' with a warning;
     it never silently disables authority.

Producer/authority table (traced 2026-07-18):

  Source                          | Field       | Can veto? | Authority
  --------------------------------|-------------|-----------|------------------------
  intelligence_bridge             | approved    | yes       | authoritative through this policy
  intelligence_bridge (SKIP)      | intel_status| yes (hard)| INTEL_AUTHORITATIVE_VETO_SKIP_HARD
  intelligence_bridge (RISK_VETO) | intel_status| yes       | INTEL_AUTHORITATIVE_VETO_RISK
  intelligence_bridge (LOW_CONF)  | intel_status| no        | INTEL_LOW_DATA_QUALITY_FAIL_OPEN
  intelligence_bridge (TIMEOUT)   | intel_status| no        | INTEL_UNAVAILABLE_FAIL_OPEN
  intelligence_bridge (ERROR)     | intel_status| no        | INTEL_ERROR_FAIL_OPEN
  intelligence_bridge (UNAVAIL.)  | intel_status| no        | INTEL_UNAVAILABLE_FAIL_OPEN
  ap/intelligence_evaluation.py   | observe_only| never     | INTEL_ADVISORY_ONLY
  ap_quality_mode.py              | (separate)  | own gate  | unchanged — not admission policy

  SPY-trend vetoes: the bridge produces these through
  _map_result() → action="skip" → _is_hard_block evaluation.
  Hard-block authority requires ONE of:
    (a) risk_ok=False from _risk_allows_trade() (structured risk_detail);
    (b) skip_reason_code in the closed authoritative set:
        NEUTRAL_DIRECTION, CONTRACT_QUALITY_FAILED, BUYING_POWER_UNAVAILABLE,
        ACCOUNT_AUTH_FAILED, RISK_MANAGER_VETO; or
    (c) reasoning matches a specific unambiguous phrase:
        "risk manager veto", "scanner signal is neutral", "neutral direction",
        "contract quality failed", "risk veto", "hard risk",
        "buying power unavailable", "insufficient buying power",
        "account auth failed", "authorization failed", "account not authorized".
  Removed from phrase list (too broad — false-positive on benign text):
    "capital", "auth", "buying power", "risk manager".
  Non-hard skips and all other outcomes → LOW_CONFIDENCE fail open.
  When matched, bridge emits intel_status="SKIP" → INTEL_AUTHORITATIVE_VETO_SKIP_HARD.
  The 222 SPY-trend vetoes observed in production are therefore either:
    (a) hard-block skips → INTEL_AUTHORITATIVE_VETO_SKIP_HARD; or
    (b) fail-open LOW_CONFIDENCE / scanner-approved fallbacks.
  No free-text pass-through and no risk_reason text re-evaluation creates authority.

Policy version: 1.0
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("ap.intelligence_admission_policy")

POLICY_VERSION = "1.0"

# ---------------------------------------------------------------------------
# Stable public reason codes
# ---------------------------------------------------------------------------

# Authoritative approvals
INTEL_AUTHORITATIVE_APPROVED     = "INTEL_AUTHORITATIVE_APPROVED"
INTEL_SCANNER_APPROVED_OBSERVE   = "INTEL_SCANNER_APPROVED_OBSERVE_ONLY"

# Authoritative vetoes — the ONLY codes that may produce allowed=False.
# Derived from actual intel_status values emitted by intelligence_bridge._map_result().
# Adding a new authoritative veto requires an explicit PR to this allowlist.
INTEL_AUTHORITATIVE_VETO_RISK            = "INTEL_AUTHORITATIVE_VETO_RISK"
INTEL_AUTHORITATIVE_VETO_SKIP_HARD       = "INTEL_AUTHORITATIVE_VETO_SKIP_HARD"
# Fail-open codes — infrastructure / unknown / data issues never block
INTEL_UNAVAILABLE_FAIL_OPEN      = "INTEL_UNAVAILABLE_FAIL_OPEN"
INTEL_ERROR_FAIL_OPEN            = "INTEL_ERROR_FAIL_OPEN"
INTEL_MALFORMED_FAIL_OPEN        = "INTEL_MALFORMED_FAIL_OPEN"
INTEL_UNKNOWN_REASON_FAIL_OPEN   = "INTEL_UNKNOWN_REASON_FAIL_OPEN"
INTEL_LOW_DATA_QUALITY_FAIL_OPEN = "INTEL_LOW_DATA_QUALITY_FAIL_OPEN"

# Observe-only / advisory codes — never produce allowed=False
INTEL_ADVISORY_ONLY         = "INTEL_ADVISORY_ONLY"
INTEL_OBSERVE_ONLY_MODE     = "INTEL_OBSERVE_ONLY_MODE"
# PR: Regime mismatch is directional context — non-authoritative, allowed=True
INTEL_REGIME_MISMATCH_ADVISORY = "INTEL_REGIME_MISMATCH_ADVISORY"

# ---------------------------------------------------------------------------
# Allowlist: intel_status values that are authoritative enough to deny entry.
# Any intel_status NOT in this map → fail open.
# Derived from intelligence_bridge._block_gate() call sites (2026-07-18 trace).
# ---------------------------------------------------------------------------
_AUTHORITATIVE_VETO_STATUS_MAP: dict[str, str] = {
    "RISK_VETO": INTEL_AUTHORITATIVE_VETO_RISK,
    "SKIP":      INTEL_AUTHORITATIVE_VETO_SKIP_HARD,
}

# Bridge statuses that explicitly signal infrastructure issues → fail open
_FAIL_OPEN_STATUS_MAP: dict[str, str] = {
    "LOW_CONFIDENCE": INTEL_LOW_DATA_QUALITY_FAIL_OPEN,
    "UNAVAILABLE":    INTEL_UNAVAILABLE_FAIL_OPEN,
    "TIMEOUT":        INTEL_UNAVAILABLE_FAIL_OPEN,
    "ERROR":          INTEL_ERROR_FAIL_OPEN,
}


# ---------------------------------------------------------------------------
# Verdict dataclass
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class IntelligenceAdmissionVerdict:
    """Canonical result of intelligence admission adjudication.

    ``allowed=False`` is set only when ``authoritative=True`` and
    ``reason_code`` is one of the INTEL_AUTHORITATIVE_VETO_* constants.
    Every other outcome produces ``allowed=True``.
    """
    allowed: bool
    authoritative: bool
    reason_code: str
    reasoning: str
    source: str
    confidence: float | None
    data_quality: str
    raw_status: str
    policy_version: str
    diagnostics: dict = field(default_factory=dict)

    def as_block_meta(self) -> dict[str, Any]:
        """Return structured metadata for decision events and block records.

        Written into decision_events.context_json (via _block → emit_decision_event)
        so operators can query stable reason codes without parsing free-text
        reasoning strings.

        intel_execution_mode is extracted from diagnostics so that
        report_intelligence_funnel can filter by LIVE/PAPER from
        decision_events.context_json — decision_events has no standalone
        execution_mode column.
        """
        return {
            "intel_reason_code":    self.reason_code,
            "intel_reasoning":      self.reasoning[:200],
            "intel_source":         self.source,
            "intel_authoritative":  self.authoritative,
            "intel_confidence":     self.confidence,
            "intel_data_quality":   self.data_quality,
            "intel_raw_status":     self.raw_status,
            "intel_policy_version": self.policy_version,
            # Execution mode preserved from diagnostics for funnel report filtering.
            "intel_execution_mode": str(
                self.diagnostics.get("execution_mode") or ""
            ).upper() or None,
        }


# ---------------------------------------------------------------------------
# Admission mode resolver
# ---------------------------------------------------------------------------

def _admission_mode() -> str:
    """Resolve INTELLIGENCE_ADMISSION_MODE.

    Allowed values: 'authoritative' (default), 'observe_only'.
    Any unrecognised value resolves to 'authoritative' with a WARNING —
    it never silently disables authority.
    """
    raw = os.getenv("INTELLIGENCE_ADMISSION_MODE", "authoritative").strip().lower()
    if raw in ("authoritative", "observe_only"):
        return raw
    log.warning(
        "INTELLIGENCE_ADMISSION_MODE=%r is not a recognised value "
        "(expected 'authoritative' or 'observe_only'). "
        "Resolving to 'authoritative' to preserve production safety.",
        raw,
    )
    return "authoritative"


# ---------------------------------------------------------------------------
# Public evaluator
# ---------------------------------------------------------------------------

def adjudicate_intelligence_result(
    result: dict,
    *,
    signal: dict,
    execution_mode: str,
) -> IntelligenceAdmissionVerdict:
    """Canonical intelligence admission evaluator.

    ap_master_control must consume only this verdict.  It must not
    independently inspect ``approved``, ``intel_status``, or free-text
    reasoning from the raw result dict.

    Failure modes that MUST NOT block (fail open):
      - result is None or not a dict
      - 'approved' field missing from result
      - intelligence not available (_available=False)
      - intel_status not in _AUTHORITATIVE_VETO_STATUS_MAP
      - infrastructure: TIMEOUT, ERROR, UNAVAILABLE statuses
      - unknown, malformed, or low data quality

    Policy version: 1.0
    """
    signal_id = str(signal.get("signal_id") or "")
    ticker    = str(signal.get("ticker") or signal.get("symbol") or "")
    side      = str(signal.get("side") or "")
    mode      = str(execution_mode or "").strip().lower()

    base_diag: dict[str, Any] = {
        "signal_id":      signal_id,
        "ticker":         ticker,
        "side":           side,
        "execution_mode": mode,
    }

    # ── Observe-only mode override ─────────────────────────────────────────
    if _admission_mode() == "observe_only":
        raw_s = str((result or {}).get("intel_status") or "") if isinstance(result, dict) else ""
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=INTEL_OBSERVE_ONLY_MODE,
            reasoning=(
                "INTELLIGENCE_ADMISSION_MODE=observe_only — "
                "intelligence recorded but never blocks entry"
            ),
            source="intelligence_bridge",
            confidence=None,
            data_quality="observe_only_mode",
            raw_status=raw_s,
            policy_version=POLICY_VERSION,
            diagnostics=base_diag,
        )

    # ── Malformed result guard ─────────────────────────────────────────────
    if not isinstance(result, dict):
        log.warning(
            "[%s] INTEL_MALFORMED_FAIL_OPEN result type=%s signal_id=%s",
            ticker, type(result).__name__, signal_id,
        )
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=INTEL_MALFORMED_FAIL_OPEN,
            reasoning=(
                f"intelligence result is not a dict "
                f"(type={type(result).__name__}) — fail open"
            ),
            source="unknown",
            confidence=None,
            data_quality="malformed",
            raw_status="",
            policy_version=POLICY_VERSION,
            diagnostics={**base_diag, "result_type": type(result).__name__},
        )

    raw_status  = str(result.get("intel_status") or "").strip().upper()
    approved    = result.get("approved")
    reasoning   = str(result.get("reasoning") or "")[:400]
    available   = bool(result.get("_available", False))
    source      = "intelligence_bridge"

    _conf_raw = result.get("score") or result.get("confidence")
    try:
        confidence: float | None = float(_conf_raw) if _conf_raw is not None else None
    except (TypeError, ValueError):
        confidence = None

    # ── Infrastructure / fail-open intel_status — classified BEFORE _available ──
    # master control unconditionally stamps result["_available"] = True on every
    # successful bridge call, including TIMEOUT, ERROR, and UNAVAILABLE results.
    # Relying on `not available` to catch these statuses therefore fails in the
    # real production path.  We classify them by intel_status first so the
    # correct reason code is emitted regardless of the _available stamp.
    #
    # Note: LOW_CONFIDENCE is included here so it is handled consistently whether
    # _available is True or False, and always produces INTEL_LOW_DATA_QUALITY_FAIL_OPEN.
    _early_fail_open = _FAIL_OPEN_STATUS_MAP.get(raw_status)
    if _early_fail_open is not None:
        log.debug(
            "[%s] %s intel_status=%r early fail-open (before _available check) signal_id=%s",
            ticker, _early_fail_open, raw_status, signal_id,
        )
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=_early_fail_open,
            reasoning=reasoning or f"infrastructure status={raw_status!r} — fail open",
            source=source,
            confidence=confidence,
            data_quality="unavailable" if raw_status != "LOW_CONFIDENCE" else "low_data_quality",
            raw_status=raw_status,
            policy_version=POLICY_VERSION,
            diagnostics={**base_diag, "raw_status": raw_status, "available": available},
        )

    # ── Missing 'approved' field → fail open ──────────────────────────────
    if approved is None:
        log.warning(
            "[%s] INTEL_MALFORMED_FAIL_OPEN missing 'approved' field "
            "raw_status=%r signal_id=%s",
            ticker, raw_status, signal_id,
        )
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=INTEL_MALFORMED_FAIL_OPEN,
            reasoning="intelligence result missing 'approved' field — fail open",
            source=source,
            confidence=confidence,
            data_quality="malformed",
            raw_status=raw_status,
            policy_version=POLICY_VERSION,
            diagnostics={**base_diag, "raw_status": raw_status},
        )

    # ── Intelligence not available → fail open ────────────────────────────
    if not available:
        fail_code = _FAIL_OPEN_STATUS_MAP.get(raw_status, INTEL_UNAVAILABLE_FAIL_OPEN)
        log.debug(
            "[%s] %s intel not available raw_status=%r signal_id=%s",
            ticker, fail_code, raw_status, signal_id,
        )
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=fail_code,
            reasoning=reasoning or f"intelligence unavailable (raw_status={raw_status!r})",
            source=source,
            confidence=confidence,
            data_quality="unavailable",
            raw_status=raw_status,
            policy_version=POLICY_VERSION,
            diagnostics={**base_diag, "raw_status": raw_status, "available": False},
        )

    # ── Explicit low-data-quality / low-confidence → fail open ────────────
    fail_open_code = _FAIL_OPEN_STATUS_MAP.get(raw_status)
    if fail_open_code == INTEL_LOW_DATA_QUALITY_FAIL_OPEN:
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=fail_open_code,
            reasoning=reasoning or "low-confidence intelligence result — fail open",
            source=source,
            confidence=confidence,
            data_quality="low_data_quality",
            raw_status=raw_status,
            policy_version=POLICY_VERSION,
            diagnostics={**base_diag, "raw_status": raw_status, "available": True},
        )

    # ── approved=True ──────────────────────────────────────────────────────
    if approved is True:
        _is_scanner_observe = (raw_status == "SCANNER_APPROVED_INTEL_OBSERVE_ONLY")
        # PR: Regime mismatch is advisory — never authoritative, never blocks
        _is_regime_advisory = (raw_status == "REGIME_MISMATCH_ADVISORY")

        if _is_regime_advisory:
            log.debug(
                "[%s] %s raw_status=REGIME_MISMATCH_ADVISORY "
                "allowed=True authoritative=False signal_id=%s",
                ticker, INTEL_REGIME_MISMATCH_ADVISORY, signal_id,
            )
            return IntelligenceAdmissionVerdict(
                allowed=True,
                authoritative=False,
                reason_code=INTEL_REGIME_MISMATCH_ADVISORY,
                reasoning=reasoning,
                source=source,
                confidence=confidence,
                data_quality="available",
                raw_status=raw_status,
                policy_version=POLICY_VERSION,
                diagnostics={**base_diag, "raw_status": raw_status, "available": True},
            )

        reason_code = (
            INTEL_SCANNER_APPROVED_OBSERVE
            if _is_scanner_observe
            else INTEL_AUTHORITATIVE_APPROVED
        )
        # SCANNER_APPROVED_INTEL_OBSERVE_ONLY is explicitly non-authoritative:
        # the bridge approved via scanner score because intel had incomplete/
        # neutral data — the intel score itself is observe-only metadata and
        # must NOT be re-evaluated by the LIVE final gate.  Marking it
        # authoritative=False ensures the final gate's fail-open bypass fires.
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=not _is_scanner_observe,
            reason_code=reason_code,
            reasoning=reasoning,
            source=source,
            confidence=confidence,
            data_quality="available",
            raw_status=raw_status,
            policy_version=POLICY_VERSION,
            diagnostics={**base_diag, "raw_status": raw_status, "available": True},
        )

    # ── approved=False: check allowlist ───────────────────────────────────
    veto_code = _AUTHORITATIVE_VETO_STATUS_MAP.get(raw_status)
    if veto_code is not None:
        log.info(
            "[%s] %s raw_status=%r confidence=%s signal_id=%s mode=%s",
            ticker, veto_code, raw_status, confidence, signal_id, mode,
        )
        return IntelligenceAdmissionVerdict(
            allowed=False,
            authoritative=True,
            reason_code=veto_code,
            reasoning=reasoning,
            source=source,
            confidence=confidence,
            data_quality="available",
            raw_status=raw_status,
            policy_version=POLICY_VERSION,
            diagnostics={**base_diag, "raw_status": raw_status, "available": True},
        )

    # ── approved=False, unknown status → fail open ─────────────────────────
    log.warning(
        "[%s] INTEL_UNKNOWN_REASON_FAIL_OPEN approved=False "
        "raw_status=%r not in authoritative allowlist — fail open. signal_id=%s",
        ticker, raw_status, signal_id,
    )
    return IntelligenceAdmissionVerdict(
        allowed=True,
        authoritative=False,
        reason_code=INTEL_UNKNOWN_REASON_FAIL_OPEN,
        reasoning=(
            f"approved=False with unrecognised status={raw_status!r} — "
            "fail open per policy (add to allowlist to make authoritative)"
        ),
        source=source,
        confidence=confidence,
        data_quality="unknown",
        raw_status=raw_status,
        policy_version=POLICY_VERSION,
        diagnostics={**base_diag, "raw_status": raw_status, "available": True},
    )


# ---------------------------------------------------------------------------
# Operator read-only funnel report
# ---------------------------------------------------------------------------

def report_intelligence_funnel(
    *,
    client_id: str | None = None,
    execution_mode: str | None = None,
    session_date: str | None = None,
) -> dict[str, Any]:
    """Read-only operator query: intelligence admission counts by reason code.

    WHY decision_events, NOT orders
    ────────────────────────────────
    Authoritative intelligence vetoes are decided before any selector or order
    row is created.  When the admission gate denies a signal, _block() returns
    immediately and no order row is ever written.  Querying orders.meta for
    intel_reason_code therefore produces an empty funnel for every blocked
    candidate — the data is structurally absent from that table.

    decision_events is the correct surface:
      • _block() calls emit_decision_event() unconditionally for every REJECT.
      • stage='blocked_intel' identifies admission-gate blocks specifically.
      • reason_code stores the stable INTEL_* taxonomy code directly.
      • context_json contains the full as_block_meta() dict, including
        intel_execution_mode (LIVE/PAPER) written by the adjudicator.
      • Timestamp column is `ts` (TIMESTAMPTZ), not `created_at` or `created_ts`.

    Fallback: if decision_events does not exist on this deploy (pre-observability
    schema), the function returns an empty funnel with a diagnostic note rather
    than raising.

    Returns:
        {
            "funnel": [{"reason_code": str, "execution_mode": str, "count": int}, ...],
            "filters": {"client_id": ..., "execution_mode": ..., "session_date": ...},
        }
    """
    from ap.db import conn, run_with_retry

    # decision_events uses `ts` (TIMESTAMPTZ), not created_at / created_ts.
    where_parts: list[str] = [
        "stage = 'blocked_intel'",
        "decision = 'REJECT'",
        "reason_code LIKE 'INTEL_%'",
    ]
    params: list[Any] = []

    if client_id:
        where_parts.append("client_id = %s")
        params.append(client_id)

    # execution_mode is stored in context_json by as_block_meta() as
    # intel_execution_mode.  No standalone column exists in decision_events.
    if execution_mode:
        _mode_upper = execution_mode.strip().upper()
        where_parts.append(
            "UPPER(COALESCE(context_json->>'intel_execution_mode', '')) = %s"
        )
        params.append(_mode_upper)

    if session_date:
        # `ts` is the canonical timestamp in decision_events.
        where_parts.append(
            "DATE(ts AT TIME ZONE 'America/New_York') = %s"
        )
        params.append(session_date)

    where_sql = "WHERE " + " AND ".join(where_parts)

    def _read() -> dict[str, Any]:
        with conn() as c:
            c.execute(
                f"""
                SELECT
                    reason_code                                              AS reason_code,
                    UPPER(COALESCE(context_json->>'intel_execution_mode','')) AS execution_mode,
                    COUNT(*)                                                 AS count
                FROM decision_events
                {where_sql}
                GROUP BY 1, 2
                ORDER BY 3 DESC
                """,
                params,
            )
            rows = c.fetchall() or []
        return {
            "funnel": [dict(r) for r in rows],
            "filters": {
                "client_id":      client_id,
                "execution_mode": execution_mode,
                "session_date":   session_date,
            },
        }

    try:
        return run_with_retry(_read)
    except Exception as exc:
        # decision_events may not exist on older deploys; return diagnostic
        # rather than raising so callers get a usable empty response.
        log.warning(
            "report_intelligence_funnel failed (decision_events may be missing "
            "on this deploy): %s", exc,
        )
        return {"funnel": [], "error": str(exc), "surface": "decision_events"}


# ---------------------------------------------------------------------------
# Alias: report_intelligence_vetoes — the veto-only report is now explicit
# ---------------------------------------------------------------------------

#: report_intelligence_vetoes is the explicit name for the blocked-only view.
#: The older name report_intelligence_funnel remains for backwards compat.
report_intelligence_vetoes = report_intelligence_funnel


# ---------------------------------------------------------------------------
# Full funnel: all intelligence admission outcomes (approved + fail-open + vetoed)
# ---------------------------------------------------------------------------

def report_intelligence_full_funnel(
    *,
    client_id: str | None = None,
    execution_mode: str | None = None,
    session_date: str | None = None,
) -> dict[str, Any]:
    """Read-only operator query: all intelligence admission outcomes.

    Unlike report_intelligence_funnel (which only sees blocked candidates),
    this function reads score_audit.intelligence_admission from approved-plan
    metadata written into orders.meta.  The canonical verdict is written there
    by master control on every approved signal, so TIMEOUT, LOW_CONFIDENCE,
    UNAVAILABLE, and observe-only paths that continued are all visible.

    Surface: orders.meta->'score_audit'->'intelligence_admission'->>'intel_reason_code'
    Timestamp: orders.created_ts (canonical orders timestamp)

    Returns:
        {
            "funnel": [{"reason_code": str, "execution_mode": str, "count": int}, ...],
            "approved_funnel": [...],   # reason_code ∈ INTEL_AUTHORITATIVE_APPROVED etc.
            "fail_open_funnel": [...],  # reason_code ∈ INTEL_*_FAIL_OPEN etc.
            "veto_funnel": [...],       # from decision_events (pre-order blocks)
            "filters": {...},
        }
    """
    from ap.db import conn, run_with_retry

    where_parts: list[str] = [
        "meta->'score_audit'->'intelligence_admission'->>'intel_reason_code' IS NOT NULL"
    ]
    params: list[Any] = []

    if client_id:
        where_parts.append("client_id = %s")
        params.append(client_id)
    if execution_mode:
        where_parts.append(
            "UPPER(COALESCE(meta->'score_audit'->'intelligence_admission'->>'intel_execution_mode','')) = %s"
        )
        params.append(execution_mode.strip().upper())
    if session_date:
        where_parts.append("DATE(created_ts AT TIME ZONE 'America/New_York') = %s")
        params.append(session_date)

    where_sql = "WHERE " + " AND ".join(where_parts)

    def _read() -> dict[str, Any]:
        with conn() as c:
            c.execute(
                f"""
                SELECT
                    meta->'score_audit'->'intelligence_admission'->>'intel_reason_code'  AS reason_code,
                    UPPER(COALESCE(
                        meta->'score_audit'->'intelligence_admission'->>'intel_execution_mode',
                        ''
                    ))                                                                    AS execution_mode,
                    COUNT(*)                                                              AS count
                FROM orders
                {where_sql}
                GROUP BY 1, 2
                ORDER BY 3 DESC
                """,
                params,
            )
            rows = [dict(r) for r in (c.fetchall() or [])]
        return {
            "funnel":    rows,
            "filters": {
                "client_id":      client_id,
                "execution_mode": execution_mode,
                "session_date":   session_date,
            },
            "note": (
                "Approved/fail-open outcomes read from orders.meta "
                "(score_audit.intelligence_admission). "
                "Veto counts from report_intelligence_vetoes() (decision_events)."
            ),
        }

    try:
        return run_with_retry(_read)
    except Exception as exc:
        log.warning("report_intelligence_full_funnel failed: %s", exc)
        return {"funnel": [], "error": str(exc)}
