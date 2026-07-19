"""
ap.intelligence_admission_policy
─────────────────────────────────
Canonical contract for intelligence admission authority.

Gate G in APMasterControl delegates all intelligence result interpretation to
``adjudicate_intelligence_result()``. No other code should independently
inspect ``approved``, ``intel_status``, or free-text ``reasoning`` fields
from the intelligence bridge.

Authority classes
─────────────────
1. Authoritative veto   — stable allowlisted reason code → blocks entry
2. Advisory / observe   — enriches decision; never blocks
3. Fail-open            — infrastructure failure, missing data, unknown code
4. Authoritative approval — explicit valid approval

All authoritative vetoes include: stable reason_code, source, policy_version,
confidence/data-quality evidence, execution_mode, signal_id, ticker, side.
Free text explains but never determines authority.

INTELLIGENCE_ADMISSION_MODE
───────────────────────────
  authoritative  (default) — allowlisted veto codes block; preserves current
                             production behavior
  observe_only             — every result recorded; nothing blocked; diagnostic
                             tool for operators only

Malformed values → authoritative + warning. Disabling authority by misconfiguring
the mode variable is not allowed.

Policy version: 1
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger("ap.intelligence_admission_policy")

POLICY_VERSION = "1"

# ── Stable reason codes ───────────────────────────────────────────────────────

# Authoritative vetoes (block entry)
INTEL_AUTHORITATIVE_VETO_RISK            = "INTEL_AUTHORITATIVE_VETO_RISK"
INTEL_AUTHORITATIVE_VETO_PORTFOLIO_SKIP  = "INTEL_AUTHORITATIVE_VETO_PORTFOLIO_SKIP"

# Authoritative approval
INTEL_AUTHORITATIVE_APPROVED             = "INTEL_AUTHORITATIVE_APPROVED"

# Advisory / observe-only (never blocks)
INTEL_ADVISORY_ONLY                      = "INTEL_ADVISORY_ONLY"

# Fail-open codes (infrastructure/data issues — never block)
INTEL_UNAVAILABLE_FAIL_OPEN              = "INTEL_UNAVAILABLE_FAIL_OPEN"
INTEL_ERROR_FAIL_OPEN                    = "INTEL_ERROR_FAIL_OPEN"
INTEL_MALFORMED_FAIL_OPEN                = "INTEL_MALFORMED_FAIL_OPEN"
INTEL_UNKNOWN_REASON_FAIL_OPEN           = "INTEL_UNKNOWN_REASON_FAIL_OPEN"
INTEL_LOW_DATA_QUALITY_FAIL_OPEN         = "INTEL_LOW_DATA_QUALITY_FAIL_OPEN"

# Observe-only mode diagnostic code
INTEL_OBSERVE_ONLY_MODE                  = "INTEL_OBSERVE_ONLY_MODE"

# ── Allowlist of bridge intel_status values that map to authoritative vetoes ──
# Derived by tracing every producer of approved=False in intelligence_bridge.py.
# Only explicit, intentional veto outcomes are in this set.
#
#   RISK_VETO   → hard risk finding (capital, auth, contract quality, "hard risk")
#   SKIP        → portfolio manager skip with hard-block phrasing
#
# All other statuses that can produce approved=False (UNAVAILABLE, TIMEOUT,
# ERROR, LOW_CONFIDENCE) are infrastructure/quality issues and FAIL OPEN.
_AUTHORITATIVE_VETO_STATUS_MAP: dict[str, str] = {
    "RISK_VETO":    INTEL_AUTHORITATIVE_VETO_RISK,
    "SKIP":         INTEL_AUTHORITATIVE_VETO_PORTFOLIO_SKIP,
}

# Bridge statuses that represent advisory / observe-only outcomes
_ADVISORY_STATUS_SET: frozenset[str] = frozenset({
    "SCANNER_APPROVED_INTEL_OBSERVE_ONLY",
    "RISK_VETO_OVERRIDE",
    "SKIP_OVERRIDE",
    "LOW_CONF_OVERRIDE",
})

# Bridge statuses that approved=True with full authority
_APPROVED_STATUS_SET: frozenset[str] = frozenset({
    "APPROVED",
})

# Bridge statuses that must fail open (infrastructure / data quality)
_FAIL_OPEN_STATUS_MAP: dict[str, str] = {
    "UNAVAILABLE":      INTEL_UNAVAILABLE_FAIL_OPEN,
    "TIMEOUT":          INTEL_UNAVAILABLE_FAIL_OPEN,
    "ERROR":            INTEL_ERROR_FAIL_OPEN,
    "LOW_CONFIDENCE":   INTEL_LOW_DATA_QUALITY_FAIL_OPEN,
    "UNKNOWN":          INTEL_UNKNOWN_REASON_FAIL_OPEN,
}

# ── Verdict dataclass ─────────────────────────────────────────────────────────

@dataclass(frozen=True)
class IntelligenceAdmissionVerdict:
    """Immutable result of intelligence admission adjudication.

    ``allowed=True``  → continue to selector.
    ``allowed=False`` → block entry; use ``reason_code`` in the queue record.
    ``authoritative`` → True only for explicit allowlisted veto or approval;
                        False for advisory and fail-open (which never block).
    """
    allowed:         bool
    authoritative:   bool
    reason_code:     str
    reasoning:       str         # bounded human-readable explanation
    source:          str         # originating module
    confidence:      float | None
    data_quality:    str         # "high" | "low" | "unknown"
    raw_status:      str         # bridge intel_status before mapping
    policy_version:  str
    diagnostics:     dict = field(default_factory=dict)

    @property
    def blocks_entry(self) -> bool:
        return not self.allowed

    def to_block_reason(self) -> str:
        """Stable queue reason string; never raw free text."""
        return self.reason_code

    def to_metadata(self, *, signal_id: str = "", ticker: str = "", side: str = "",
                    execution_mode: str = "") -> dict[str, Any]:
        """Structured verdict for plan/decision-event metadata."""
        return {
            "intelligence_verdict": {
                "allowed":        self.allowed,
                "authoritative":  self.authoritative,
                "reason_code":    self.reason_code,
                "reasoning":      self.reasoning[:300],
                "source":         self.source,
                "confidence":     self.confidence,
                "data_quality":   self.data_quality,
                "raw_status":     self.raw_status,
                "policy_version": self.policy_version,
                "signal_id":      signal_id,
                "ticker":         ticker,
                "side":           side,
                "execution_mode": execution_mode,
            }
        }


# ── Mode resolution ───────────────────────────────────────────────────────────

def _admission_mode() -> str:
    """Return 'authoritative' or 'observe_only'; default authoritative.

    Malformed values → authoritative + warning. Disabling authority by
    misconfiguring this variable is not allowed.
    """
    raw = str(os.getenv("INTELLIGENCE_ADMISSION_MODE", "authoritative")).strip().lower()
    if raw == "observe_only":
        return "observe_only"
    if raw != "authoritative":
        log.warning(
            "INTELLIGENCE_ADMISSION_MODE=%r is not a recognised value; "
            "resolving to 'authoritative' to preserve production safety. "
            "Allowed values: authoritative, observe_only",
            raw,
        )
    return "authoritative"


# ── Core adjudicator ─────────────────────────────────────────────────────────

def adjudicate_intelligence_result(
    result: dict[str, Any],
    *,
    signal: dict[str, Any],
    execution_mode: str,
) -> IntelligenceAdmissionVerdict:
    """Map a raw intelligence bridge result to a canonical admission verdict.

    This is the single, authoritative translation point. APMasterControl must
    call this function and consume only the returned verdict. It must not
    independently inspect ``approved``, ``intel_status``, or ``reasoning``.

    Parameters
    ----------
    result:
        The dict returned by ``_run_intelligence()`` / the bridge.
    signal:
        The triggering signal dict; used for identity metadata only.
    execution_mode:
        'live' or 'paper'; sourced from the order/position context.
    """
    mode = _admission_mode()
    ticker    = str(signal.get("ticker") or signal.get("symbol") or "")
    signal_id = str(signal.get("signal_id") or signal.get("id") or "")
    side      = str(signal.get("side") or signal.get("direction") or "")
    exec_mode = str(execution_mode or "").strip().lower()

    # ── Malformed result: fail open ────────────────────────────────────────
    if not isinstance(result, dict):
        log.warning(
            "[%s] INTEL_MALFORMED_RESULT type=%r signal_id=%s — failing open",
            ticker, type(result).__name__, signal_id,
        )
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=INTEL_MALFORMED_FAIL_OPEN,
            reasoning="Intelligence result was not a dict; failing open",
            source="intelligence_admission_policy",
            confidence=None,
            data_quality="unknown",
            raw_status="",
            policy_version=POLICY_VERSION,
            diagnostics={"type": str(type(result).__name__)},
        )

    available   = bool(result.get("_available", False))
    approved    = result.get("approved")      # bridge sets this
    intel_status = str(result.get("intel_status") or "").strip().upper()
    reasoning   = str(result.get("reasoning") or "")[:300]
    confidence  = result.get("score") or result.get("intel_score") or result.get("confidence")
    risk_detail = result.get("risk_detail") or {}
    contracts   = result.get("contracts")

    try:
        confidence = float(confidence) if confidence is not None else None
    except (TypeError, ValueError):
        confidence = None

    # ── observe_only mode: record but never block ──────────────────────────
    if mode == "observe_only":
        log.info(
            "[%s] INTEL_OBSERVE_ONLY_MODE signal_id=%s raw_status=%s approved=%s",
            ticker, signal_id, intel_status, approved,
        )
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=INTEL_OBSERVE_ONLY_MODE,
            reasoning=f"observe_only mode: {reasoning}",
            source="intelligence_bridge",
            confidence=confidence,
            data_quality="unknown",
            raw_status=intel_status,
            policy_version=POLICY_VERSION,
            diagnostics={"raw_approved": approved, "available": available,
                         "risk_detail": risk_detail, "contracts": contracts},
        )

    # ── Intelligence not available: fail open ─────────────────────────────
    if not available:
        code = INTEL_UNAVAILABLE_FAIL_OPEN
        dq   = "unknown"
        log.info(
            "[%s] INTEL_UNAVAILABLE_FAIL_OPEN signal_id=%s reason=%s",
            ticker, signal_id, reasoning or intel_status,
        )
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=code,
            reasoning=reasoning or "intelligence unavailable",
            source="intelligence_bridge",
            confidence=None,
            data_quality=dq,
            raw_status=intel_status or "UNAVAILABLE",
            policy_version=POLICY_VERSION,
            diagnostics={"available": False, "contracts": contracts},
        )

    # ── Map intel_status to stable taxonomy ────────────────────────────────

    # 1. Authoritative approval — must exclude advisory/fail-open/veto statuses
    if (intel_status in _APPROVED_STATUS_SET
            or (approved is True
                and intel_status not in _FAIL_OPEN_STATUS_MAP
                and intel_status not in _AUTHORITATIVE_VETO_STATUS_MAP
                and intel_status not in _ADVISORY_STATUS_SET)):
        dq = "high" if confidence and confidence >= 35.0 else "low"
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=True,
            reason_code=INTEL_AUTHORITATIVE_APPROVED,
            reasoning=reasoning,
            source="intelligence_bridge",
            confidence=confidence,
            data_quality=dq,
            raw_status=intel_status,
            policy_version=POLICY_VERSION,
            diagnostics={"contracts": contracts, "risk_detail": risk_detail},
        )

    # 2. Advisory / observe-only sources (approved=True but from non-authoritative path)
    if intel_status in _ADVISORY_STATUS_SET:
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=INTEL_ADVISORY_ONLY,
            reasoning=reasoning,
            source="intelligence_bridge",
            confidence=confidence,
            data_quality="low",
            raw_status=intel_status,
            policy_version=POLICY_VERSION,
            diagnostics={"contracts": contracts, "risk_detail": risk_detail,
                         "advisory_note": "scanner-approved or data-collection path"},
        )

    # 3. Fail-open infrastructure/quality codes
    if intel_status in _FAIL_OPEN_STATUS_MAP:
        mapped_code = _FAIL_OPEN_STATUS_MAP[intel_status]
        dq = "low" if intel_status == "LOW_CONFIDENCE" else "unknown"
        log.info(
            "[%s] %s signal_id=%s raw_status=%s confidence=%s — failing open",
            ticker, mapped_code, signal_id, intel_status, confidence,
        )
        return IntelligenceAdmissionVerdict(
            allowed=True,
            authoritative=False,
            reason_code=mapped_code,
            reasoning=reasoning,
            source="intelligence_bridge",
            confidence=confidence,
            data_quality=dq,
            raw_status=intel_status,
            policy_version=POLICY_VERSION,
            diagnostics={"risk_detail": risk_detail},
        )

    # 4. Authoritative veto — only explicitly allowlisted codes
    if intel_status in _AUTHORITATIVE_VETO_STATUS_MAP:
        stable_code = _AUTHORITATIVE_VETO_STATUS_MAP[intel_status]
        dq = "high" if risk_detail else "low"
        log.warning(
            "[%s] %s signal_id=%s raw_status=%s ticker=%s side=%s exec_mode=%s "
            "confidence=%s reasoning=%.100s",
            ticker, stable_code, signal_id, intel_status, ticker, side,
            exec_mode, confidence, reasoning,
        )
        return IntelligenceAdmissionVerdict(
            allowed=False,
            authoritative=True,
            reason_code=stable_code,
            reasoning=reasoning,
            source="intelligence_bridge",
            confidence=confidence,
            data_quality=dq,
            raw_status=intel_status,
            policy_version=POLICY_VERSION,
            diagnostics={
                "risk_detail": risk_detail,
                "ticker": ticker,
                "side": side,
                "execution_mode": exec_mode,
                "signal_id": signal_id,
            },
        )

    # 5. Unknown status: fail open — unknown reason is not an authoritative veto
    log.info(
        "[%s] INTEL_UNKNOWN_REASON_FAIL_OPEN signal_id=%s raw_status=%r approved=%s "
        "— unknown status is not authoritative; failing open",
        ticker, signal_id, intel_status, approved,
    )
    return IntelligenceAdmissionVerdict(
        allowed=True,
        authoritative=False,
        reason_code=INTEL_UNKNOWN_REASON_FAIL_OPEN,
        reasoning=f"unknown intel_status={intel_status!r}; {reasoning}",
        source="intelligence_bridge",
        confidence=confidence,
        data_quality="unknown",
        raw_status=intel_status,
        policy_version=POLICY_VERSION,
        diagnostics={"raw_approved": approved, "risk_detail": risk_detail},
    )


# ── Reporting helper ──────────────────────────────────────────────────────────

def summarise_verdicts(verdicts: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate a list of verdict metadata dicts for operator reporting.

    Each element should be the dict returned by
    ``IntelligenceAdmissionVerdict.to_metadata()``.

    Returns counts by reason_code, client, execution_mode, ticker, side.
    No new DB table required — use existing decision-event / queue-result fields.
    """
    from collections import defaultdict
    by_code:   dict[str, int] = defaultdict(int)
    by_client: dict[str, int] = defaultdict(int)
    by_mode:   dict[str, int] = defaultdict(int)
    by_ticker: dict[str, int] = defaultdict(int)
    by_side:   dict[str, int] = defaultdict(int)

    for v in verdicts:
        iv = v.get("intelligence_verdict") or v
        code   = str(iv.get("reason_code") or "unknown")
        client = str(iv.get("client_id")   or iv.get("source") or "unknown")
        mode   = str(iv.get("execution_mode") or "unknown")
        ticker = str(iv.get("ticker")      or "unknown")
        side   = str(iv.get("side")        or "unknown")
        by_code[code]     += 1
        by_client[client] += 1
        by_mode[mode]     += 1
        by_ticker[ticker] += 1
        by_side[side]     += 1

    authoritative_vetoes = sum(
        v for k, v in by_code.items()
        if k.startswith("INTEL_AUTHORITATIVE_VETO_")
    )
    advisory_count = by_code.get(INTEL_ADVISORY_ONLY, 0)
    fail_open_count = sum(
        v for k, v in by_code.items()
        if k.endswith("_FAIL_OPEN")
    )
    approval_count = by_code.get(INTEL_AUTHORITATIVE_APPROVED, 0)

    return {
        "authoritative_veto_count":    authoritative_vetoes,
        "advisory_count":              advisory_count,
        "unavailable_error_fail_open": fail_open_count,
        "approval_count":              approval_count,
        "by_reason_code":              dict(by_code),
        "by_client":                   dict(by_client),
        "by_execution_mode":           dict(by_mode),
        "by_ticker":                   dict(by_ticker),
        "by_side":                     dict(by_side),
        "total":                       len(verdicts),
    }
