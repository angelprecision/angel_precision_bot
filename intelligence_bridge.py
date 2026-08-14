# intelligence_bridge.py  v2
# ============================================================================
# Connects APMasterControl Gate G to the Angel Precision Intelligence Stack.
#
# v2 changes:
#   1. intel_status field on every return path
#   2. APAuditLog wired in — decisions persisted to Supabase ap_audit_log
#   3. Sizing hierarchy documented — intel caps, Kelly sizes, MC gates
#   4. Retry logic — transient failures don't permanently disable intel
# ============================================================================

from __future__ import annotations

import importlib.util
import logging
import os
import sys
import threading
import time
import concurrent.futures
import atexit
from typing import Optional

log = logging.getLogger("intelligence_bridge")

_BOT_ROOT  = os.path.dirname(os.path.abspath(__file__))
_INTEL_DIR = os.path.join(_BOT_ROOT, "ap_intelligence")

if _BOT_ROOT not in sys.path:
    sys.path.insert(0, _BOT_ROOT)


def _register_intel_package() -> bool:
    if "ap_intelligence" in sys.modules:
        return True
    if not os.path.isdir(_INTEL_DIR):
        log.warning(f"intelligence_bridge: ap_intelligence-3 not found at {_INTEL_DIR}")
        return False
    try:
        init_path = os.path.join(_INTEL_DIR, "__init__.py")
        spec = importlib.util.spec_from_file_location(
            "ap_intelligence", init_path,
            submodule_search_locations=[_INTEL_DIR],
        )
        mod = importlib.util.module_from_spec(spec)
        sys.modules["ap_intelligence"] = mod
        spec.loader.exec_module(mod)

        for subpkg in ("agents", "tools", "backtesting"):
            subpkg_dir = os.path.join(_INTEL_DIR, subpkg)
            full_name  = f"ap_intelligence.{subpkg}"
            if os.path.isdir(subpkg_dir) and full_name not in sys.modules:
                init_file = os.path.join(subpkg_dir, "__init__.py")
                if os.path.exists(init_file):
                    sub_spec = importlib.util.spec_from_file_location(
                        full_name, init_file,
                        submodule_search_locations=[subpkg_dir],
                    )
                    sub_mod = importlib.util.module_from_spec(sub_spec)
                    sys.modules[full_name] = sub_mod
                    sub_spec.loader.exec_module(sub_mod)
                else:
                    import types
                    stub = types.ModuleType(full_name)
                    stub.__path__ = [subpkg_dir]
                    stub.__package__ = full_name
                    sys.modules[full_name] = stub

        log.info("intelligence_bridge: ap_intelligence package registered")
        return True
    except Exception as e:
        log.warning(f"intelligence_bridge: package registration failed ({e})")
        for key in list(sys.modules.keys()):
            if key.startswith("ap_intelligence"):
                del sys.modules[key]
        return False


_package_ready = _register_intel_package()

INTELLIGENCE_AVAILABLE: bool = _package_ready  # MED-008: derive from actual registration result

INTEL_TIMEOUT_SECONDS:   float = float(os.getenv("INTEL_TIMEOUT_SECONDS",  "8.0"))
INTEL_APPROVE_THRESHOLD: float = float(os.getenv("INTEL_APPROVE_THRESHOLD", "35.0"))

# Minimum scanner score for the data-gap fallback path.
# Must be the live-eligible floor (70), not the generic intel threshold (35).
# A scanner score of 35 with missing fundamentals is not a confirmed setup.
GATE_G_SCANNER_MIN_ELIGIBLE: float = float(os.getenv("GATE_G_SCANNER_MIN_ELIGIBLE", "70.0"))

# Gate G policy:
# - LIVE defaults fail-closed: intel unavailable/timeout/error/skip/low-score/risk-veto blocks.
# - Paper/research may opt into 1-contract data-collection overrides.
# - To intentionally collect data in LIVE, set INTEL_DATA_COLLECTION_OVERRIDE=1.
_AP_MODE = (os.getenv("AP_MODE") or os.getenv("BOT_MODE") or "paper").lower()
_INTEL_DATA_COLLECTION_OVERRIDE = os.getenv("INTEL_DATA_COLLECTION_OVERRIDE", "0") == "1"
_INTEL_FAIL_OPEN_UNAVAILABLE = os.getenv("INTEL_FAIL_OPEN_UNAVAILABLE", "0") == "1"
_INTEL_ENFORCE_RISK_VETO = os.getenv("INTEL_ENFORCE_RISK_VETO", "1") != "0"
_INTEL_IS_LIVE = _AP_MODE == "live"

# One shared executor for non-blocking audit writes. Do not spin up a new
# ThreadPoolExecutor for every signal.
_AUDIT_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=int(os.getenv("INTEL_AUDIT_WORKERS", "1")),
    thread_name_prefix="intel-audit",
)
atexit.register(lambda: _AUDIT_EXECUTOR.shutdown(wait=False, cancel_futures=True))

# FIX 19: Module-level executor for intel checks — never spawn per-signal
_INTEL_EXECUTOR = concurrent.futures.ThreadPoolExecutor(
    max_workers=int(os.getenv("INTEL_WORKERS", "2")),
    thread_name_prefix="intel-run",
)
atexit.register(lambda: _INTEL_EXECUTOR.shutdown(wait=False, cancel_futures=True))

def _data_collection_allowed() -> bool:
    return _INTEL_DATA_COLLECTION_OVERRIDE or not _INTEL_IS_LIVE

def _block_gate(*, status: str, score: float, reasoning: str, risk_detail=None, price_data_stub: bool = False) -> dict:
    return {
        "approved": False,
        "score": round(float(score or 0), 1),
        "contracts": 0,
        "reasoning": reasoning,
        "intel_status": status,
        "intel_score": round(float(score or 0), 1) if score is not None else None,
        "risk_detail": risk_detail or {},
        "price_data_stub": price_data_stub,
    }

def _collect_gate(*, status: str, score: float, reasoning: str, risk_detail=None, price_data_stub: bool = False) -> dict:
    return {
        "approved": True,
        "score": round(float(score or 0), 1),  # honest raw score — 1-contract cap is the real guard
        "contracts": 1,
        "reasoning": reasoning,
        "intel_status": status,
        "intel_score": round(float(score or 0), 1) if score is not None else None,
        "risk_detail": risk_detail or {},
        "price_data_stub": price_data_stub,
    }

# FIX 4: Retry logic — transient errors don't permanently disable intel
# HIGH-002: Per-client pipeline state — prevents one client's failures from
# disabling intelligence for all clients sharing the process.
_PIPELINE_RETRY_SECONDS: float = float(os.getenv("INTEL_RETRY_SECONDS", "120.0"))
_PIPELINE_MAX_FAILS:     int   = int(os.getenv("INTEL_MAX_FAILS", "5"))

_pipelines:              dict[str, object] = {}   # client_id → pipeline
_pipeline_fail_counts:   dict[str, int]    = {}   # client_id → fail count
_pipeline_last_attempts: dict[str, float]  = {}   # client_id → monotonic time
_pipeline_lock = threading.Lock()


def _get_pipeline(client_id: str = "default"):
    with _pipeline_lock:
        pipeline = _pipelines.get(client_id)
        if pipeline is not None:
            return pipeline
        if _pipeline_fail_counts.get(client_id, 0) >= _PIPELINE_MAX_FAILS:
            return None

        now = time.monotonic()
        last = _pipeline_last_attempts.get(client_id, 0)
        if last > 0 and (now - last) < _PIPELINE_RETRY_SECONDS:
            return None

        _pipeline_last_attempts[client_id] = now

    # Build pipeline outside lock (may be slow)
    try:
        from ap_intelligence.ap_signal_pipeline import APSignalPipeline
        from ap_intelligence.ap_mode_config     import APModeConfig

        equity     = float(os.getenv("ACCOUNT_EQUITY", "25000"))
        openai_key = os.getenv("OPENAI_API_KEY", "")
        ap_mode    = (os.getenv("AP_MODE") or os.getenv("BOT_MODE") or "paper").lower()
        intel_mode = "production" if ap_mode == "live" else "research"

        pipeline = APSignalPipeline(
            portfolio_value  = equity,
            openai_api_key   = openai_key,
            use_llm          = bool(openai_key),
            use_sentiment    = bool(openai_key),
            use_fundamentals = True,
        )

        if not hasattr(pipeline, "mode_cfg"):
            try:
                pipeline.mode_cfg = APModeConfig(mode=intel_mode)
            except Exception:
                class _Stub:
                    mode = intel_mode
                pipeline.mode_cfg = _Stub()

        with _pipeline_lock:
            _pipelines[client_id] = pipeline
            _pipeline_fail_counts[client_id] = 0
        log.info(
            f"intelligence_bridge: pipeline ready for {client_id} | mode={intel_mode} "
            f"equity=${equity:,.0f} llm={'yes' if openai_key else 'rule-based'}"
        )
        return pipeline

    except Exception as e:
        with _pipeline_lock:
            _pipeline_fail_counts[client_id] = _pipeline_fail_counts.get(client_id, 0) + 1
            count = _pipeline_fail_counts[client_id]
        remaining = _PIPELINE_MAX_FAILS - count
        log.warning(
            f"intelligence_bridge: init failed for {client_id} "
            f"(attempt {count}/{_PIPELINE_MAX_FAILS}): {e} "
            f"— {'permanently disabled' if remaining <= 0 else f'retry in {_PIPELINE_RETRY_SECONDS:.0f}s ({remaining} left)'}"
        )
        return None


_audit_log = None
_audit_log_lock = threading.Lock()

def _get_audit_log():
    global _audit_log
    if _audit_log is not None:
        return _audit_log
    with _audit_log_lock:
        if _audit_log is not None:
            return _audit_log
        try:
            from ap_intelligence.ap_audit_log import APAuditLog
            _audit_log = APAuditLog()
        except Exception as e:
            log.debug(f"intelligence_bridge: audit log unavailable ({e})")
        return _audit_log


def _signal_to_direction(signal: dict) -> str:
    raw = (signal.get("side") or signal.get("direction") or "CALL").upper()
    if raw in ("CALL", "BUY", "LONG", "BULLISH", "CALLS"):
        return "bullish"
    if raw in ("PUT", "SELL", "SHORT", "BEARISH", "PUTS"):
        return "bearish"
    return "neutral"


_STRUCTURED_RISK_APPROVAL_CODES = frozenset({
    "APPROVED",
    "APPROVED_WITH_REGIME_MISMATCH",
})


def _risk_allows_trade(risk: dict) -> tuple[bool, str]:
    """Resolve structured risk authority before legacy compatibility fields.

    Structured payload contract:

    hard_veto=True
        Always blocks. approved must be False and reason_code must be present.

    hard_veto=False
        Allows only an explicitly approved result whose reason_code belongs to
        _STRUCTURED_RISK_APPROVAL_CODES and whose durable sizing is positive.

    hard_veto absent
        Uses the legacy compatibility parser below.

    A malformed structured payload fails closed. Legacy payload behavior remains
    unchanged because the strict contract is activated only when `hard_veto`
    exists in the payload.
    """
    if not isinstance(risk, dict):
        return True, ""

    # New structured contract. Presence of the key activates strict validation.
    if "hard_veto" in risk:
        hard_veto   = risk.get("hard_veto")
        approved    = risk.get("approved")
        reason_code = str(risk.get("reason_code") or "").strip().upper()
        reason      = str(
            risk.get("reason")
            or reason_code
            or "structured risk result"
        )

        # Literal bool required. Values such as 0, 1, "false" and None are not
        # accepted as structured authority.
        if type(hard_veto) is not bool:
            return False, (
                "malformed structured risk result: "
                f"hard_veto must be bool, got {hard_veto!r}"
            )

        if hard_veto is True:
            if approved is not False:
                return False, (
                    "contradictory structured hard veto: "
                    f"approved={approved!r} hard_veto=True "
                    f"reason_code={reason_code!r}"
                )
            if not reason_code:
                return False, (
                    "malformed structured hard veto: reason_code is required"
                )
            return False, reason

        # hard_veto is exactly False from here.
        if approved is not True:
            return False, (
                "contradictory structured approval: "
                f"approved={approved!r} hard_veto=False "
                f"reason_code={reason_code!r}"
            )

        if reason_code not in _STRUCTURED_RISK_APPROVAL_CODES:
            return False, (
                "unknown structured approval reason: "
                f"reason_code={reason_code!r} hard_veto=False"
            )

        try:
            max_contracts   = int(risk.get("max_contracts") or 0)
            max_position_usd = float(risk.get("max_position_usd") or 0.0)
        except (TypeError, ValueError):
            return False, (
                "malformed structured approval sizing: "
                f"max_contracts={risk.get('max_contracts')!r} "
                f"max_position_usd={risk.get('max_position_usd')!r}"
            )

        if max_contracts < 1 or max_position_usd <= 0:
            return False, (
                "structured approval has no executable size: "
                f"max_contracts={max_contracts} "
                f"max_position_usd={max_position_usd}"
            )

        return True, reason

    # Legacy compatibility parser. Do not change this behavior in this PR.
    for key in (
        "risk_ok",
        "ok",
        "approved",
        "pass",
        "passed",
        "allow",
        "allowed",
    ):
        if key in risk:
            val = risk.get(key)

            if isinstance(val, str):
                val_norm = val.strip().lower()

                if val_norm in {
                    "false",
                    "no",
                    "0",
                    "fail",
                    "failed",
                    "reject",
                    "veto",
                    "blocked",
                }:
                    return False, str(
                        risk.get("reason") or f"{key}={val}"
                    )

                if val_norm in {
                    "true",
                    "yes",
                    "1",
                    "pass",
                    "passed",
                    "allow",
                    "allowed",
                    "ok",
                }:
                    return True, str(risk.get("reason") or "")

            elif val is False:
                return False, str(
                    risk.get("reason") or f"{key}=False"
                )

            elif val is True:
                return True, str(risk.get("reason") or "")

    for key in ("veto", "blocked", "rejected", "hard_block"):
        if bool(risk.get(key)):
            return False, str(
                risk.get("reason") or f"{key}=True"
            )

    status = str(
        risk.get("status")
        or risk.get("decision")
        or ""
    ).strip().lower()

    if status in {
        "reject",
        "rejected",
        "block",
        "blocked",
        "veto",
        "fail",
        "failed",
    }:
        return False, str(
            risk.get("reason") or f"status={status}"
        )

    return True, str(risk.get("reason") or "")


def _count_missing_fundamentals(result: dict) -> int:
    """
    Count/estimate missing fundamental fields for audit logging only.
    Checks explicit fields first; falls back to neutral+confidence==50.
    Not required for scanner fallback — used for audit/logging only.
    """
    sb   = result.get("signal_breakdown") or {}
    fund = sb.get("fundamentals") or {}
    if fund.get("missing_fundamental_count") is not None:
        return int(fund.get("missing_fundamental_count", 0))
    if fund.get("unavailable_fields"):
        return len(fund.get("unavailable_fields") or [])
    if fund.get("available") is False or fund.get("fundamentals_available") is False:
        return 7
    if result.get("yfinance_unavailable") or result.get("data_unavailable"):
        return 7
    conf = float(fund.get("confidence", 50))
    sig  = str(fund.get("signal", "neutral")).lower()
    if sig == "neutral" and conf == 50.0:
        return 7
    return 0


def _map_result(result: dict, fallback_score: float) -> dict:
    """
    Map pipeline result → Gate G decision.

    fallback_score is the scanner-approved score.  It is the primary
    eligibility gate and must win over a low intel score caused by
    missing/neutral fundamentals — not over explicit hard-risk findings.
    """
    action     = str(result.get("action", "execute")).lower()
    confidence = float(result.get("confidence") or 0)
    intel_score = float(result.get("score") or confidence or fallback_score)
    score      = intel_score          # may be overridden below
    contracts  = int(result.get("contracts") or 1)
    reasoning  = str(result.get("reasoning") or "")
    risk       = result.get("risk_detail") or {}
    ticker     = str(result.get("ticker") or "?")

    risk_ok, risk_reason = _risk_allows_trade(risk)
    allow_collect = _data_collection_allowed()

    structured_risk_contract = (
        isinstance(risk, dict)
        and "hard_veto" in risk
    )

    # ── Hard risk veto (explicit risk finding — always block) ─────────────────
    # Structured risk authority is identical in LIVE and PAPER. PAPER may retain
    # the historical data-collection override only for legacy payloads that do
    # not carry the new hard_veto contract.
    if _INTEL_ENFORCE_RISK_VETO and not risk_ok:
        log.info(
            "[%s] GATE_G_HARD_RISK_BLOCK scanner_score=%.1f intel_score=%.1f "
            "effective_score=%.1f hard_risk_reason=%s structured=%s",
            ticker, fallback_score, intel_score, intel_score, risk_reason,
            structured_risk_contract,
        )

        if allow_collect and not structured_risk_contract:
            return _collect_gate(
                status="RISK_VETO_OVERRIDE",
                score=score,
                reasoning=(
                    f"risk_veto_override: {risk_reason} "
                    "(legacy 1-contract data collection)"
                ),
                risk_detail=risk,
            )

        return _block_gate(
            status="RISK_VETO",
            score=score,
            reasoning=f"risk_veto: {risk_reason}",
            risk_detail=risk,
        )

    # ── action == "skip" (portfolio manager decision) ─────────────────────────
    if action == "skip":
        # Hard-block phrases — always reject regardless of scanner score.
        # Matched case-insensitively against reasoning.
        #
        # IMPORTANT: every phrase must be specific enough that it cannot
        # accidentally match a positive/benign statement.  Broad terms such as
        # "capital" or "auth" were removed because they match "capital efficient
        # setup" or "authentication successful" and falsely create authoritative
        # authority from benign free text.
        #
        # Structured evidence is preferred: RISK_VETO already handles
        # risk_detail["approved"] is False (see above).  For skip, structured
        # skip_reason_code from the producer is checked first (closed set);
        # phrase matching is a secondary fallback for legacy producer text.
        #
        # Phrase-only skips that do NOT match a specific phrase are treated as
        # LOW_CONFIDENCE and fail open — they must not create authority.
        _HARD_PHRASES = (
            "risk manager veto",
            "scanner signal is neutral",
            "neutral direction",
            "contract quality failed",
            "risk veto",
            "hard risk",
            "buying power unavailable",
            "insufficient buying power",
            "account auth failed",
            "authorization failed",
            "account not authorized",
        )

        # Closed-set structured skip reason codes that the producer may set
        # explicitly (preferred over phrase matching).
        _AUTHORITATIVE_SKIP_CODES = frozenset({
            "NEUTRAL_DIRECTION",
            "CONTRACT_QUALITY_FAILED",
            "BUYING_POWER_UNAVAILABLE",
            "ACCOUNT_AUTH_FAILED",
            "RISK_MANAGER_VETO",
        })

        _producer_skip_code = str(result.get("skip_reason_code") or "").upper().strip()
        _reasoning_lower   = reasoning.lower()
        _is_data_skip  = "insufficient edge" in _reasoning_lower
        # Use the already-computed risk_ok boolean from _risk_allows_trade().
        # Do NOT re-derive authority from the risk_reason text: a reason like
        # "position remains within account risk limits" is explanatory prose for
        # an approved decision; it must not flip a passing risk_ok into a block.
        # risk_ok=True when risk_detail["approved"]=True (or equivalent).
        _is_hard_block = (
            not risk_ok or
            (_producer_skip_code in _AUTHORITATIVE_SKIP_CODES) or
            any(p in _reasoning_lower for p in _HARD_PHRASES)
        )
        _missing_count = _count_missing_fundamentals(result)

        log.info(
            "[%s] GATE_G_SKIP scanner_score=%.1f intel_score=%.1f "
            "effective_score=%.1f missing_fundamental_count=%d "
            "hard_risk_reason=%s is_data_skip=%s is_hard_block=%s",
            ticker, fallback_score, intel_score, intel_score,
            _missing_count, risk_reason or "none",
            _is_data_skip, _is_hard_block,
        )

        # Scanner-approved fallback: when scanner_score >= GATE_G_SCANNER_MIN_ELIGIBLE
        # and skip is "insufficient edge" and no hard block, approve with scanner score.
        # missing_fundamental_count is audit only — not a gate condition.
        # Intel score is stored as observe-only metadata.
        if (_is_data_skip
                and not _is_hard_block
                and fallback_score >= GATE_G_SCANNER_MIN_ELIGIBLE):
            log.info(
                "[%s] GATE_G_SCANNER_APPROVED scanner_score=%.1f intel_score=%.1f "
                "effective_score=%.1f second_score_mode=observe_only "
                "missing_fundamental_count=%d hard_risk_reason=none decision=ALLOW",
                ticker, fallback_score, intel_score, fallback_score, _missing_count,
            )
            return {
                "approved":          True,
                "score":             round(fallback_score, 1),
                "contracts":         max(1, contracts),
                "reasoning":         (
                    f"scanner_approved_score={fallback_score:.1f} "
                    f"intel_score={intel_score:.1f} "
                    f"missing_fundamental_count={_missing_count} "
                    f"second_score_mode=observe_only "
                    f"gate=SCANNER_APPROVED_INTEL_OBSERVE_ONLY"
                ),
                "intel_status":      "SCANNER_APPROVED_INTEL_OBSERVE_ONLY",
                "intel_score":       round(intel_score, 1),
                "second_score_mode": "observe_only",
                "risk_detail":       risk,
            }

        # Only bridge-proven hard-risk skips may emit status="SKIP".
        # Any non-hard skip must fail open through LOW_CONFIDENCE so the
        # canonical admission policy never treats ambiguous skip text as
        # execution-authoritative.
        if not _is_hard_block:
            return _block_gate(
                status="LOW_CONFIDENCE",
                score=score,
                reasoning=f"intel_skip_non_authoritative: {reasoning[:160]}",
                risk_detail=risk,
            )

        # Hard block or explicit risk skip — block as authoritative SKIP
        if allow_collect:
            return _collect_gate(
                status="SKIP_OVERRIDE",
                score=score,
                reasoning=f"intel_skip_override: {reasoning[:100]} (1 contract)",
                risk_detail=risk,
            )
        return _block_gate(
            status="SKIP",
            score=score,
            reasoning=f"intel_skip: {reasoning[:160]}",
            risk_detail=risk,
        )
    # ── Low confidence (intel score below approve threshold) ──────────────────
    if score < INTEL_APPROVE_THRESHOLD:
        log.info(
            "[%s] GATE_G_LOW_CONFIDENCE scanner_score=%.1f intel_score=%.1f "
            "effective_score=%.1f hard_risk_reason=none",
            ticker, fallback_score, intel_score, intel_score,
        )
        if allow_collect:
            return _collect_gate(
                status="LOW_CONF_OVERRIDE",
                score=score,
                reasoning=f"intel_low_conf_override: {score:.1f} (1 contract data collection)",
                risk_detail=risk,
            )
        return _block_gate(
            status="LOW_CONFIDENCE",
            score=score,
            reasoning=f"intel_score {score:.1f} below threshold {INTEL_APPROVE_THRESHOLD:.1f}",
            risk_detail=risk,
        )

    # ── Approved ──────────────────────────────────────────────────────────────
    # Intel contracts is a CAP — MC still applies min(kelly, risk cap, intel cap).

    # Regime mismatch is an approved, non-authoritative market-context result.
    # Preserve the exact Portfolio Manager contract count. Never manufacture one
    # through max(1, contracts).
    if (
        str(risk.get("reason_code") or "").strip().upper()
        == "APPROVED_WITH_REGIME_MISMATCH"
        and risk.get("hard_veto") is False
        and risk.get("approved") is True
    ):
        try:
            advisory_contracts = int(result.get("contracts") or 0)
        except (TypeError, ValueError):
            advisory_contracts = 0

        advisory_contracts = max(0, advisory_contracts)

        log.info(
            "[%s] GATE_G_REGIME_MISMATCH_ADVISORY "
            "scanner_score=%.1f intel_score=%.1f "
            "risk_max_contracts=%s pm_contracts=%d "
            "veto_category=%s",
            ticker,
            fallback_score,
            intel_score,
            risk.get("max_contracts"),
            advisory_contracts,
            risk.get("veto_category") or "MARKET_CONTEXT",
        )

        return {
            "approved": True,
            "score": round(score, 1),
            "contracts": advisory_contracts,
            "reasoning": (
                "regime_mismatch_advisory: "
                f"{risk.get('reason') or ''} "
                "(reason_code=APPROVED_WITH_REGIME_MISMATCH "
                "hard_veto=False)"
            ),
            "intel_status": "REGIME_MISMATCH_ADVISORY",
            "intel_score": round(score, 1),
            "risk_detail": risk,
        }

    log.info(
        "[%s] GATE_G_APPROVED scanner_score=%.1f intel_score=%.1f "
        "effective_score=%.1f hard_risk_reason=none",
        ticker, fallback_score, intel_score, intel_score,
    )
    return {
        "approved":     True,
        "score":        round(score, 1),
        "contracts":    max(1, contracts),
        "reasoning":    reasoning[:200],
        "intel_status": "APPROVED",
        "intel_score":  round(score, 1),
        "risk_detail":  risk,
    }

def _persist_audit(result: dict, gate: dict, signal: dict) -> None:
    audit = _get_audit_log()
    if not audit:
        return
    try:
        import datetime as _dt
        audit.record({
            "ticker":       signal.get("ticker") or signal.get("symbol", ""),
            "action":       "execute" if gate["approved"] else "skip",
            "direction":    _signal_to_direction(signal),
            "score":        gate.get("intel_score", 0),
            "intel_score":  gate.get("intel_score"),              # DATA-001
            "intel_status": gate.get("intel_status", "UNKNOWN"),
            "reasoning":    gate.get("reasoning", ""),
            "contracts":    gate.get("contracts", 0),
            "signal_id":    signal.get("signal_id", ""),
            "scanner_score":float(signal.get("score") or signal.get("ev_score") or 0),
            "pattern":      signal.get("pattern") or signal.get("pattern_id", ""),  # DATA-001
            "timeframe":    signal.get("timeframe", ""),          # DATA-001
            "timestamp":    _dt.datetime.now(_dt.timezone.utc).isoformat(),  # DATA-001
            "signal_breakdown": result.get("signal_breakdown", {}),
            "risk_detail":      result.get("risk_detail", {}),
        })
    except Exception as e:
        log.debug(f"intelligence_bridge: audit write failed ({e})")


def record_trade_outcome(ticker: str, signal_id: str, pnl_pct: float) -> None:
    """
    Call when a position closes to log real P&L back to intel audit.
    Wire into exit engine to build (decision, outcome) learning pairs.
    """
    audit = _get_audit_log()
    if not audit:
        return
    try:
        from datetime import datetime, timezone
        audit.record_outcome(
            ticker=ticker,
            timestamp=datetime.now(timezone.utc).isoformat(),
            pnl_pct=pnl_pct,
        )
        log.info(f"[{ticker}] Intel outcome recorded: pnl={pnl_pct:+.2%}")
    except Exception as e:
        log.debug(f"intelligence_bridge: outcome record failed ({e})")


def run_intelligence_check(signal: dict, underlying_price: float,
                           client_id: str = "default") -> dict:
    """Called by APMasterControl._run_intelligence().

    LIVE default: hard-veto on unavailable/timeout/error/skip/low-score/risk-veto.
    Paper/research default: 1-contract data-collection override.
    """
    ticker    = signal.get("ticker") or signal.get("symbol", "UNKNOWN")
    direction = _signal_to_direction(signal)
    score_in  = float(signal.get("score") or signal.get("ev_score") or 65.0)

    def _unavailable_gate(status: str, reason: str) -> dict:
        if _INTEL_FAIL_OPEN_UNAVAILABLE or _data_collection_allowed():
            return _collect_gate(
                status=status,
                score=score_in,
                reasoning=reason,
                price_data_stub=True,
            )
        return _block_gate(
            status=status,
            score=score_in,
            reasoning=reason,
            price_data_stub=True,
        )

    pipeline = _get_pipeline(client_id=client_id)
    if pipeline is None:
        return _unavailable_gate("UNAVAILABLE", "intel_unavailable")

    def _run():
        # Pass live chain data from signal if available.
        # If None, pipeline.run() uses estimates — same as before.
        _atr       = signal.get("atr_value")
        _delta     = signal.get("option_delta")
        _spread    = signal.get("spread_pct")
        _oi        = signal.get("open_interest") or signal.get("option_open_interest")
        _vol_opts  = signal.get("option_volume")
        _prem      = signal.get("option_bid") or signal.get("option_ask")

        if any(x is not None for x in [_atr, _delta, _spread]):
            # Full call with live data — risk manager gets real inputs
            return pipeline.run(
                ticker               = ticker,
                scanner_signal       = direction,
                scanner_confidence   = score_in,
                underlying_price     = float(underlying_price),
                dte                  = int(signal.get("dte") or 1),
                allow_0dte           = True,
                atr_value            = float(_atr) if _atr else None,
                option_delta         = float(_delta) if _delta else None,
                bid_ask_spread_pct   = float(_spread) if _spread else None,
                open_interest        = int(_oi) if _oi else None,
                daily_volume_options = int(_vol_opts) if _vol_opts else None,
                option_premium       = float(_prem) if _prem else None,
            )
        else:
            # Fallback: estimates only (chain data not available)
            return pipeline.run_quick(
                ticker             = ticker,
                scanner_signal     = direction,
                scanner_confidence = score_in,
                underlying_price   = float(underlying_price),
                dte                = int(signal.get("dte") or 1),
                allow_0dte         = True,
            )

    try:
        future = _INTEL_EXECUTOR.submit(_run)
        result = future.result(timeout=INTEL_TIMEOUT_SECONDS)

        gate = _map_result(result, score_in)

        # Async audit persist — never blocks execution. Uses one shared executor.
        try:
            _AUDIT_EXECUTOR.submit(_persist_audit, result, gate, signal)
        except Exception:
            pass

        log.info(
            f"[{ticker}] Gate G | status={gate['intel_status']} "
            f"approved={gate['approved']} score={gate['score']} "
            f"contracts={gate['contracts']} dir={direction}"
        )
        return gate

    except concurrent.futures.TimeoutError:
        log.warning(
            f"[{ticker}] Intel timeout after {INTEL_TIMEOUT_SECONDS}s — "
            f"{'data-collection override' if (_INTEL_FAIL_OPEN_UNAVAILABLE or _data_collection_allowed()) else 'blocking'}"
        )
        return _unavailable_gate("TIMEOUT", f"intel_timeout_{INTEL_TIMEOUT_SECONDS}s")

    except Exception as e:
        log.warning(
            f"[{ticker}] Intel error ({e}) — "
            f"{'data-collection override' if (_INTEL_FAIL_OPEN_UNAVAILABLE or _data_collection_allowed()) else 'blocking'}"
        )
        return _unavailable_gate("ERROR", f"intel_error:{str(e)[:60]}")
