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
  Hard-block phrases include "risk manager", "risk veto", "hard risk",
  "capital", "buying power", "auth", "scanner signal is neutral",
  "neutral direction", "contract quality failed".  When matched, the
  bridge emits intel_status="SKIP" only when _is_hard_block is true,
  which maps to INTEL_AUTHORITATIVE_VETO_SKIP_HARD. Non-hard skip,
  low-confidence, malformed-confidence, and incomplete-data outcomes
  route through LOW_CONFIDENCE → INTEL_LOW_DATA_QUALITY_FAIL_OPEN or
  the scanner-approved fallback (approved=True).
  The 222 SPY-trend vetoes observed in production are therefore either:
    (a) hard-block skips → INTEL_AUTHORITATIVE_VETO_SKIP_HARD; or
    (b) fail-open LOW_CONFIDENCE / scanner-approved fallbacks.
  No free-text pass-through creates authority.

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
INTEL_ADVISORY_ONLY    = "INTEL_ADVISORY_ONLY"
INTEL_OBSERVE_ONLY_MODE = "INTEL_OBSERVE_ONLY_MODE"

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
        """Return structured metadata for queue/plan/order block records.

        Stored in orders.meta and decision events so operators can query
        stable reason codes without parsing free-text reasoning strings.
        """
        return {
            "intel_reason_code":   self.reason_code,
            "intel_reasoning":     self.reasoning[:200],
            "intel_source":        self.source,
            "intel_authoritative": self.authoritative,
            "intel_confidence":    self.confidence,
            "intel_data_quality":  self.data_quality,
            "intel_raw_status":    self.raw_status,
            "intel_policy_version": self.policy_version,
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
        reason_code = (
            INTEL_SCANNER_APPROVED_OBSERVE
            if raw_status == "SCANNER_APPROVED_INTEL_OBSERVE_ONLY"
            else INTEL_AUTHORITATIVE_APPROVED
        )
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=True,
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

    Reads stable intel_reason_code values stored in orders.meta JSONB by
    master control.  No new analytics table is required.

    Returns:
        {
            "funnel": [{"reason_code": str, "execution_mode": str, "count": int}, ...],
            "filters": {"client_id": ..., "execution_mode": ..., "session_date": ...},
        }
    """
    from ap.db import conn, run_with_retry

    where_parts: list[str] = ["meta->>'intel_reason_code' IS NOT NULL"]
    params: list[Any] = []

    if client_id:
        where_parts.append("client_id = %s")
        params.append(client_id)
    if execution_mode:
        where_parts.append("LOWER(COALESCE(execution_mode,'')) = %s")
        params.append(execution_mode.strip().lower())
    if session_date:
        where_parts.append(
            "DATE(created_at AT TIME ZONE 'America/New_York') = %s"
        )
        params.append(session_date)

    where_sql = "WHERE " + " AND ".join(where_parts)

    def _read() -> dict[str, Any]:
        with conn() as c:
            c.execute(
                f"""
                SELECT
                    meta->>'intel_reason_code'         AS reason_code,
                    UPPER(COALESCE(execution_mode,'')) AS execution_mode,
                    COUNT(*)                           AS count
                FROM orders
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
        log.warning("report_intelligence_funnel failed: %s", exc)
        return {"funnel": [], "error": str(exc)}
