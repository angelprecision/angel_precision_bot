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
import time
import concurrent.futures
from typing import Optional

log = logging.getLogger("intelligence_bridge")

_BOT_ROOT  = os.path.dirname(os.path.abspath(__file__))
_INTEL_DIR = os.path.join(_BOT_ROOT, "ap_intelligence-3")

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

INTELLIGENCE_AVAILABLE: bool = True

INTEL_TIMEOUT_SECONDS:   float = float(os.getenv("INTEL_TIMEOUT_SECONDS",  "8.0"))
INTEL_APPROVE_THRESHOLD: float = float(os.getenv("INTEL_APPROVE_THRESHOLD", "35.0"))

# FIX 4: Retry logic — transient errors don't permanently disable intel
_pipeline:               Optional[object] = None
_pipeline_last_attempt:  float = 0.0
_pipeline_fail_count:    int   = 0
_PIPELINE_RETRY_SECONDS: float = float(os.getenv("INTEL_RETRY_SECONDS", "120.0"))
_PIPELINE_MAX_FAILS:     int   = int(os.getenv("INTEL_MAX_FAILS", "5"))


def _get_pipeline():
    global _pipeline, _pipeline_last_attempt, _pipeline_fail_count

    if _pipeline is not None:
        return _pipeline
    if _pipeline_fail_count >= _PIPELINE_MAX_FAILS:
        return None

    now = time.monotonic()
    if _pipeline_last_attempt > 0 and (now - _pipeline_last_attempt) < _PIPELINE_RETRY_SECONDS:
        return None

    _pipeline_last_attempt = now

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

        _pipeline = pipeline
        _pipeline_fail_count = 0
        log.info(
            f"intelligence_bridge: pipeline ready | mode={intel_mode} "
            f"equity=${equity:,.0f} llm={'yes' if openai_key else 'rule-based'}"
        )
        return _pipeline

    except Exception as e:
        _pipeline_fail_count += 1
        remaining = _PIPELINE_MAX_FAILS - _pipeline_fail_count
        log.warning(
            f"intelligence_bridge: init failed (attempt {_pipeline_fail_count}/{_PIPELINE_MAX_FAILS}): {e} "
            f"— {'permanently disabled' if remaining <= 0 else f'retry in {_PIPELINE_RETRY_SECONDS:.0f}s ({remaining} left)'}"
        )
        return None


_audit_log = None

def _get_audit_log():
    global _audit_log
    if _audit_log is not None:
        return _audit_log
    try:
        from ap_intelligence.ap_audit_log import APAuditLog
        _audit_log = APAuditLog()
        return _audit_log
    except Exception as e:
        log.debug(f"intelligence_bridge: audit log unavailable ({e})")
        return None


def _signal_to_direction(signal: dict) -> str:
    raw = (signal.get("side") or signal.get("direction") or "CALL").upper()
    if raw in ("CALL", "BUY", "LONG", "BULLISH", "CALLS"):
        return "bullish"
    if raw in ("PUT", "SELL", "SHORT", "BEARISH", "PUTS"):
        return "bearish"
    return "neutral"


def _map_result(result: dict, fallback_score: float) -> dict:
    action    = str(result.get("action", "execute")).lower()
    confidence= float(result.get("confidence") or 0)
    score     = float(result.get("score") or confidence or fallback_score)
    contracts = int(result.get("contracts") or 1)
    reasoning = str(result.get("reasoning") or "")
    risk      = result.get("risk_detail") or {}
    risk_ok   = risk.get("approved", True)
    risk_reason=risk.get("reason", "")

    if not risk_ok:
        return {"approved": False, "score": score, "contracts": 0,
                "reasoning": f"risk_veto: {risk_reason}",
                "intel_status": "RISK_VETO", "intel_score": score, "risk_detail": risk}

    if action == "skip":
        return {"approved": False, "score": score, "contracts": 0,
                "reasoning": f"intel_skip: {reasoning[:120]}",
                "intel_status": "SKIP", "intel_score": score, "risk_detail": risk}

    if score < INTEL_APPROVE_THRESHOLD:
        return {"approved": False, "score": score, "contracts": 0,
                "reasoning": f"intel_low_conf: {score:.1f}<{INTEL_APPROVE_THRESHOLD}",
                "intel_status": "LOW_CONFIDENCE", "intel_score": score, "risk_detail": risk}

    # FIX 3: intel contracts is a CAP — MC does min(kelly, intel_cap) at Gate I
    return {"approved": True, "score": round(score, 1), "contracts": max(1, contracts),
            "reasoning": reasoning[:200],
            "intel_status": "APPROVED", "intel_score": round(score, 1), "risk_detail": risk}


def _persist_audit(result: dict, gate: dict, signal: dict) -> None:
    audit = _get_audit_log()
    if not audit:
        return
    try:
        audit.record({
            "ticker":       signal.get("ticker") or signal.get("symbol", ""),
            "action":       "execute" if gate["approved"] else "skip",
            "direction":    _signal_to_direction(signal),
            "score":        gate.get("intel_score", 0),
            "intel_status": gate.get("intel_status", "UNKNOWN"),
            "reasoning":    gate.get("reasoning", ""),
            "contracts":    gate.get("contracts", 0),
            "signal_id":    signal.get("signal_id", ""),
            "scanner_score":float(signal.get("score") or signal.get("ev_score") or 0),
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


def run_intelligence_check(signal: dict, underlying_price: float) -> dict:
    """Called by APMasterControl._run_intelligence(). Always fails open."""
    ticker    = signal.get("ticker") or signal.get("symbol", "UNKNOWN")
    direction = _signal_to_direction(signal)
    score_in  = float(signal.get("score") or signal.get("ev_score") or 65.0)

    _fail_open = {
        "approved": True, "score": score_in, "contracts": 1,
        "reasoning": "intel_unavailable",
        "intel_status": "UNAVAILABLE", "intel_score": None,
    }

    pipeline = _get_pipeline()
    if pipeline is None:
        return _fail_open

    def _run():
        return pipeline.run_quick(
            ticker             = ticker,
            scanner_signal     = direction,
            scanner_confidence = score_in,
            underlying_price   = float(underlying_price),
            dte                = int(signal.get("dte") or 1),
            allow_0dte         = True,
        )

    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as ex:
            future = ex.submit(_run)
            result = future.result(timeout=INTEL_TIMEOUT_SECONDS)

        gate = _map_result(result, score_in)

        # FIX 2: Async audit persist — never blocks execution
        try:
            _t = concurrent.futures.ThreadPoolExecutor(max_workers=1)
            _t.submit(_persist_audit, result, gate, signal)
            _t.shutdown(wait=False)
        except Exception:
            pass

        log.info(
            f"[{ticker}] Gate G | status={gate['intel_status']} "
            f"approved={gate['approved']} score={gate['score']} "
            f"contracts={gate['contracts']} dir={direction}"
        )
        return gate

    except concurrent.futures.TimeoutError:
        log.warning(f"[{ticker}] Intel timeout after {INTEL_TIMEOUT_SECONDS}s — fail open")
        return {**_fail_open, "reasoning": f"intel_timeout_{INTEL_TIMEOUT_SECONDS}s",
                "intel_status": "TIMEOUT"}

    except Exception as e:
        log.warning(f"[{ticker}] Intel error ({e}) — fail open")
        return {**_fail_open, "reasoning": f"intel_error:{str(e)[:60]}",
                "intel_status": "ERROR"}
