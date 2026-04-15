# intelligence_bridge.py
# ============================================================================
# Connects APMasterControl Gate G to the Angel Precision Intelligence Stack.
#
# APMasterControl._run_intelligence() imports this file and calls:
#   run_intelligence_check(signal, underlying_price) -> dict
#
# Required return shape:
#   {
#       "approved":  bool,
#       "score":     float,   # 0-100 intel confidence score
#       "contracts": int,     # max contracts intel recommends (0 = block)
#       "reasoning": str,
#       "_available": True    # set by master control after return
#   }
#
# FAIL OPEN by design — if the intelligence pipeline errors, times out,
# or is misconfigured, execution is NOT blocked. The signal passes through
# with a warning log. The other 8 gates in master control still protect you.
#
# Package fix: folder is named "ap_intelligence-3" on disk but the code
# imports as "ap_intelligence". This bridge handles the sys.path injection
# so imports resolve correctly.
# ============================================================================

from __future__ import annotations

import importlib.util
import logging
import os
import sys
import concurrent.futures

log = logging.getLogger("intelligence_bridge")

# ── Package path injection ────────────────────────────────────────────────────
_BOT_ROOT  = os.path.dirname(os.path.abspath(__file__))
_INTEL_DIR = os.path.join(_BOT_ROOT, "ap_intelligence-3")

if _BOT_ROOT not in sys.path:
    sys.path.insert(0, _BOT_ROOT)


def _register_intel_package():
    """
    Register ap_intelligence-3 as the 'ap_intelligence' Python package.
    Called once at module load. Safe to call multiple times.
    """
    if "ap_intelligence" in sys.modules:
        return True

    if not os.path.isdir(_INTEL_DIR):
        log.warning(f"intelligence_bridge: ap_intelligence-3 not found at {_INTEL_DIR}")
        return False

    try:
        init_path = os.path.join(_INTEL_DIR, "__init__.py")
        spec = importlib.util.spec_from_file_location(
            "ap_intelligence",
            init_path,
            submodule_search_locations=[_INTEL_DIR],
        )
        mod = importlib.util.module_from_spec(spec)
        sys.modules["ap_intelligence"] = mod
        spec.loader.exec_module(mod)

        # Register subpackages so nested imports resolve
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

        log.info("intelligence_bridge: ap_intelligence package registered successfully")
        return True

    except Exception as e:
        log.warning(f"intelligence_bridge: package registration failed ({e})")
        for key in list(sys.modules.keys()):
            if key.startswith("ap_intelligence"):
                del sys.modules[key]
        return False


_package_ready = _register_intel_package()

# ── Public flag checked by APMasterControl ───────────────────────────────────
INTELLIGENCE_AVAILABLE: bool = True

# ── Config ───────────────────────────────────────────────────────────────────
INTEL_TIMEOUT_SECONDS:   float = float(os.getenv("INTEL_TIMEOUT_SECONDS",  "8.0"))
INTEL_APPROVE_THRESHOLD: float = float(os.getenv("INTEL_APPROVE_THRESHOLD", "35.0"))

# ── Singleton pipeline ───────────────────────────────────────────────────────
_pipeline             = None
_pipeline_init_failed = False


def _get_pipeline():
    """Lazy singleton — import and init once, reuse forever."""
    global _pipeline, _pipeline_init_failed

    if _pipeline is not None:
        return _pipeline
    if _pipeline_init_failed:
        return None

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

        # Patch missing mode_cfg — pipeline references self.mode_cfg but
        # APSignalPipeline.__init__ never initializes it.
        if not hasattr(pipeline, "mode_cfg"):
            try:
                pipeline.mode_cfg = APModeConfig(mode=intel_mode)
            except Exception:
                class _Stub:
                    mode = intel_mode
                pipeline.mode_cfg = _Stub()

        _pipeline = pipeline
        log.info(
            f"intelligence_bridge: pipeline initialized | "
            f"mode={intel_mode} equity=${equity:,.0f} "
            f"llm={'yes' if openai_key else 'no — rule-based only'}"
        )
        return _pipeline

    except Exception as e:
        _pipeline_init_failed = True
        log.warning(f"intelligence_bridge: init failed ({e}) — Gate G will fail open")
        return None


# ── Helpers ──────────────────────────────────────────────────────────────────

def _signal_to_direction(signal: dict) -> str:
    raw = (signal.get("side") or signal.get("direction") or "CALL").upper()
    if raw in ("CALL", "BUY", "LONG", "BULLISH", "CALLS"):
        return "bullish"
    if raw in ("PUT", "SELL", "SHORT", "BEARISH", "PUTS"):
        return "bearish"
    return "neutral"


def _map_result(result: dict, fallback_score: float) -> dict:
    action      = str(result.get("action", "execute")).lower()
    confidence  = float(result.get("confidence") or 0)
    score       = float(result.get("score") or confidence or fallback_score)
    contracts   = int(result.get("contracts") or 1)
    reasoning   = str(result.get("reasoning") or "")
    risk        = result.get("risk_detail") or {}
    risk_ok     = risk.get("approved", True)
    risk_reason = risk.get("reason", "")

    if not risk_ok:
        return {"approved": False, "score": score, "contracts": 0,
                "reasoning": f"risk_veto: {risk_reason}"}

    if action == "skip":
        return {"approved": False, "score": score, "contracts": 0,
                "reasoning": f"intel_skip: {reasoning[:120]}"}

    if score < INTEL_APPROVE_THRESHOLD:
        return {"approved": False, "score": score, "contracts": 0,
                "reasoning": f"intel_low_conf: {score:.1f}<{INTEL_APPROVE_THRESHOLD}"}

    return {
        "approved":  True,
        "score":     round(score, 1),
        "contracts": max(1, contracts),
        "reasoning": reasoning[:200],
    }


# ── Main entry point ─────────────────────────────────────────────────────────

def run_intelligence_check(signal: dict, underlying_price: float) -> dict:
    """
    Called by APMasterControl._run_intelligence().
    Always fails open — never blocks execution due to intel system issues.
    """
    ticker    = signal.get("ticker") or signal.get("symbol", "UNKNOWN")
    direction = _signal_to_direction(signal)
    score_in  = float(signal.get("score") or signal.get("ev_score") or 65.0)

    _fail_open = {
        "approved": True, "score": score_in,
        "contracts": 1,   "reasoning": "intel_unavailable",
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
        log.info(
            f"[{ticker}] Gate G intel | approved={gate['approved']} "
            f"score={gate['score']} contracts={gate['contracts']} "
            f"dir={direction} | {gate['reasoning'][:80]}"
        )
        return gate

    except concurrent.futures.TimeoutError:
        log.warning(f"[{ticker}] Intel timeout after {INTEL_TIMEOUT_SECONDS}s — fail open")
        return {**_fail_open, "reasoning": f"intel_timeout_{INTEL_TIMEOUT_SECONDS}s"}

    except Exception as e:
        log.warning(f"[{ticker}] Intel pipeline error ({e}) — fail open")
        return {**_fail_open, "reasoning": f"intel_error:{str(e)[:60]}"}
