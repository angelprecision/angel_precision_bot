"""
ap/intelligence_evaluation.py

Intelligence PR 1 — production intelligence evidence wiring.

Runs every configured intelligence module on every eligible production-shaped
opportunity and persists one canonical ``intelligence_evaluation`` payload.
No trade eligibility is changed. No broker submit, cancel, sizing, or position
mutations. observe_only=True enforced at every layer.

Wiring contract:
  Call ``evaluate_and_persist_intelligence()`` after signal/plan normalization
  and before the final admission / submit decision. The call must not raise —
  all errors are logged and recorded inside the payload, never propagated.

  The same call path fires on rejection and block paths so that:
    - rejected trades have an intelligence_evaluation in rejected_diagnostics
    - blocked deferred materialization rows have it in orders.meta
    - completed trades have it in the dossier and score_audit

Canonical payload shape:
  intelligence_evaluation = {
    client_id, execution_mode, signal_id, canonical_signal_id,
    ticker, side, timeframe, pattern,
    evaluated_at, data_as_of, module_version, config_hash, git_commit,
    available, overall_score,
    module_scores:   {module_name: score|None, ...},
    module_statuses: {module_name: "available"|"unavailable"|"error", ...},
    missing_inputs:  [module_name, ...],
    stale_inputs:    [module_name, ...],
    errors:          ["module_name:reason", ...],
    observe_only:    true,
    module_results:  {module_name: {available, score, missing_reason, error, ...}, ...},
  }

Module availability contract:
  Each module result has:
    available:         true / false   — never omit, never infer from score
    score:             float | None   — None when unavailable; real 0.0 is NOT None
    missing_reason:    str | None     — set when available=false due to missing data
    error:             str | None     — set when available=false due to an exception
    source:            str | None     — data source identifier
    source_timestamp:  str | None     — ISO timestamp of data used
    freshness:         str | None     — "fresh" / "stale" / None
    affected_eligibility: false       — always false; observe_only enforced
"""
from __future__ import annotations

import concurrent.futures
import copy
import hashlib
import json
import logging
import os
import subprocess
import threading
import time as _time_module
from collections import OrderedDict
from datetime import datetime, timezone
from typing import Any, Optional

log = logging.getLogger("ap.intelligence_evaluation")

INTELLIGENCE_EVAL_VERSION = "intelligence_evaluation_v1_observe_only"
_OBSERVE_ONLY = True  # compile-time constant; no branch can flip this


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ── Module-load constants — computed once, never per evaluation ──────────────
def _git_commit() -> str:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL, timeout=2,
        ).decode().strip()
    except Exception:
        return os.getenv("GIT_COMMIT", "unknown")

# Cached at import time — subprocess git rev-parse runs ONCE, not per signal.
_CACHED_GIT_COMMIT: str = _git_commit()

# Budget for background intelligence evaluation (ms). Enforced via Future.result(timeout).
_INTELLIGENCE_BUDGET_MS: int = int(os.getenv("INTELLIGENCE_BUDGET_MS", "500"))
# Max workers in the bounded ThreadPoolExecutor. Prevents unlimited thread creation.
_INTEL_MAX_WORKERS: int = max(1, int(os.getenv("INTELLIGENCE_MAX_WORKERS", "2")))
# Max additional tasks permitted in the pending queue (beyond active workers).
# Total capacity = max_workers + max_pending. When full: queue_saturated.
_INTEL_MAX_PENDING: int = max(0, int(os.getenv("INTELLIGENCE_MAX_PENDING", "4")))

# Req 6: Feature flag — disabled by default for controlled rollout.
# Set INTELLIGENCE_EVIDENCE_ENABLED=1 to activate on PAPER before LIVE.
# When disabled: no executor dispatch, no metadata mutation, no semaphore use.
_INTELLIGENCE_ENABLED: bool = (
    os.getenv("INTELLIGENCE_EVIDENCE_ENABLED", "0").strip() not in ("", "0", "false", "no")
)

# Req 8: Bounded completed/state cache
_INTEL_MAX_COMPLETED: int = max(100, int(os.getenv("INTELLIGENCE_MAX_COMPLETED", "2000")))
_INTEL_STATE_TTL_S:   float = max(60.0, float(os.getenv("INTELLIGENCE_STATE_TTL_S", "7200")))


def _config_hash(cfg: dict) -> str:
    try:
        return hashlib.sha1(
            json.dumps(cfg, sort_keys=True, default=str).encode()
        ).hexdigest()[:12]
    except Exception:
        return "unknown"


# ─────────────────────────────────────────────────────────────────────────────
# Per-module result shaping
# ─────────────────────────────────────────────────────────────────────────────

def _module_result(
    *,
    available: bool,
    score: Optional[float],
    missing_reason: Optional[str] = None,
    error: Optional[str] = None,
    source: Optional[str] = None,
    source_timestamp: Optional[str] = None,
    freshness: Optional[str] = None,
    raw: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Canonical per-module result. Enforces the availability contract:
      - available=False must always have either missing_reason or error set
      - score=None is the correct representation for unavailable data
      - score=0.0 is a real scored zero, distinct from None
    """
    if not available and not missing_reason and not error:
        missing_reason = "module_did_not_return_data"
    return {
        "available":           available,
        "score":               score,
        "missing_reason":      missing_reason,
        "error":               error,
        "source":              source,
        "source_timestamp":    source_timestamp,
        "freshness":           freshness,
        "affected_eligibility": False,  # NEVER true in this module
        "_raw":                raw,
    }


def _wrap_module(name: str, fn, *args, **kwargs) -> dict[str, Any]:
    """
    Run a single intelligence module, normalize its output, and return a
    canonical module result. Never raises.
    """
    try:
        raw = fn(*args, **kwargs)
        if not isinstance(raw, dict):
            return _module_result(
                available=False,
                score=None,
                error=f"non_dict_return:{type(raw).__name__}",
                raw=None,
            )
        # ── Normalization order (enforced strictly) ──────────────────────────────
        # Step 1: error — any nonblank error string means module failed.
        #         raw available=True must NOT override a nonblank error.
        raw_error = str(raw.get("error") or "").strip() or None

        # Step 2: missing — list or explicit string.
        missing_list = raw.get("missing_data") or []
        if isinstance(missing_list, list) and missing_list:
            missing_reason: Optional[str] = "; ".join(str(m) for m in missing_list[:10])
        else:
            missing_reason = str(raw.get("missing_reason") or "").strip() or None

        # Step 3: score value (before availability decision).
        score_val = (
            raw.get("score")
            if "score" in raw
            else raw.get("total_score")
            if "total_score" in raw
            else raw.get("value")
        )

        # Step 4: availability — explicit False is decisive; any error or missing
        # forces unavailable regardless of what the raw dict claims for available.
        explicit_avail = raw.get("available")
        available = (
            explicit_avail is not False  # not explicitly marked unavailable
            and not raw_error            # no error (overrides available=True)
            and not missing_reason       # no missing data (overrides available=True)
        )

        # Step 5: unavailable must have score=None.
        # Real 0.0 is preserved only when available=True.
        # Missing must be distinguishable from a real zero score.
        scored_value = (
            (float(score_val) if score_val is not None else None)
            if available
            else None
        )
        return _module_result(
            available=available,
            score=scored_value,
            missing_reason=missing_reason,
            error=raw_error,
            source=raw.get("source") or raw.get("score_source"),
            source_timestamp=raw.get("source_timestamp") or raw.get("data_as_of"),
            freshness=raw.get("freshness"),
            raw=raw,
        )
    except Exception as exc:
        log.debug("[intelligence] module=%s error=%s", name, exc)
        return _module_result(
            available=False,
            score=None,
            error=str(exc)[:200],
            missing_reason=None,
        )


# ─────────────────────────────────────────────────────────────────────────────
# Individual module runners — thin adapters over existing module interfaces
# ─────────────────────────────────────────────────────────────────────────────

def _run_market_context(sig: dict, data_sources: Optional[dict] = None) -> dict:
    from ap.market_context_builder import build_market_context_for_signal
    return build_market_context_for_signal(sig, data_sources=data_sources or {})


def _run_sector_context(sig: dict, ctx: dict) -> dict:
    from ap.sector_context import score_sector_context
    return score_sector_context(sig, ctx)


def _run_volume_confirmation(sig: dict, ctx: dict) -> dict:
    from ap.volume_confirmation import score_volume_confirmation
    return score_volume_confirmation(_build_volume_inputs(sig, ctx), ctx)


def _run_vwap_context(sig: dict, ctx: dict) -> dict:
    from ap.vwap_context import score_vwap_context
    return score_vwap_context(sig, ctx)


def _run_fair_value_gap(sig: dict, ctx: dict) -> dict:
    from ap.fair_value_gap import evaluate_fvg_context  # type: ignore[attr-defined]
    return evaluate_fvg_context(sig, ctx)


def _run_the_strat_confluence(sig: dict, ctx: dict) -> dict:
    from ap.the_strat_confluence import evaluate_higher_timeframe_confluence
    return evaluate_higher_timeframe_confluence(sig, ctx)


def _run_trigger_geometry(sig: dict) -> dict:
    from ap.trigger_geometry import score_trigger_geometry
    adapted = _build_geometry_inputs(sig)
    side = adapted.get("side") or adapted.get("direction")
    return score_trigger_geometry(adapted, side)


def _run_expected_move(sig: dict, ctx: dict) -> dict:
    """
    expected_move: requires underlying_price + atm_iv + dte from the signal
    or market context. Reports available=False with specific missing_reason
    when IV or price data is absent.
    """
    from ap.expected_move import expected_move_1d, atm_iv_from_chain

    underlying_price = (
        _f(sig.get("underlying_price"))
        or _f(sig.get("entry_price"))
        or _f((ctx.get("levels") or {}).get("current_price"))
    )
    atm_iv = (
        _f(sig.get("atm_iv"))
        or _f(sig.get("iv"))
        or _f((ctx.get("trend") or {}).get("atm_iv"))
    )

    if underlying_price is None:
        return {"available": False, "missing_reason": "underlying_price_missing",
                "score": None, "missing_data": ["underlying_price"]}
    if atm_iv is None:
        return {"available": False, "missing_reason": "atm_iv_missing",
                "score": None, "missing_data": ["atm_iv"],
                "note": "atm_iv required from chain or signal; not inferred"}

    result = expected_move_1d(underlying_price, atm_iv)
    em_val = getattr(result, "value", None)
    return {
        "available": em_val is not None,
        "score": float(em_val) if em_val is not None else None,
        "underlying_price": underlying_price,
        "atm_iv": atm_iv,
        "expected_move_1d": em_val,
        "source": "signal_dict",
        "missing_data": [] if em_val is not None else ["expected_move_compute_failed"],
    }


def _run_position_score_profile(
    sig: dict,
    ctx: dict,
    upstream_results: Optional[dict] = None,
) -> dict:
    """
    Build position score profile, passing upstream module results so the profile
    knows which inputs were available, scored zero, or unavailable.

    profile_context structure:
      - market_context (the base ctx dict)
      - per-module canonical results from upstream evaluation
      Each upstream entry has: available, score, missing_reason, error.
      The profile must distinguish available/zero/unavailable/error/stale.
      Unavailable module scores appear as None, not 0.0.
    """
    from ap.position_score_profile import build_position_score_profile

    # Build profile_context containing all upstream canonical results.
    profile_context = dict(ctx) if isinstance(ctx, dict) else {}

    if upstream_results:
        _upstream_canonical: dict = {}
        for mod_name in (
            "sector_context", "volume_confirmation", "vwap_context",
            "fair_value_gap", "the_strat_confluence", "trigger_geometry",
            "expected_move",
        ):
            mod_res = upstream_results.get(mod_name)
            if mod_res and isinstance(mod_res, dict):
                # Pass canonical status plus an internal copy of the raw
                # evidence so downstream scoring does not rerun the module.
                _upstream_canonical[mod_name] = {
                    "provided":     True,
                    "available":    mod_res.get("available"),
                    "score":        mod_res.get("score"),     # None = unavailable
                    "missing_reason": mod_res.get("missing_reason"),
                    "error":        mod_res.get("error"),
                    "freshness":    mod_res.get("freshness"),
                    # Internal-only raw evidence lets the profile consume the
                    # exact module result rather than recomputing it.
                    "raw":          copy.deepcopy(mod_res.get("_raw")),
                }
            else:
                _upstream_canonical[mod_name] = {
                    "provided": True,
                    "available": False,
                    "score":     None,
                    "missing_reason": "upstream_result_missing",
                    "error":     None,
                    "raw":       None,
                }
        # Attach upstream canonical results to profile_context under a namespaced key.
        # position_score_profile can read these for comparison or override.
        profile_context["_intelligence_upstream"] = _upstream_canonical

    result = build_position_score_profile(sig, profile_context)
    # Stamp that upstream context was provided
    if isinstance(result, dict) and upstream_results:
        result.setdefault("diagnostics", {})["upstream_modules_provided"] = (
            list(upstream_results.keys())
        )
    return result


def _f(v: Any) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Canonical evaluation runner
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_intelligence(
    signal: dict[str, Any],
    *,
    client_id: str,
    execution_mode: str,
    market_context: Optional[dict[str, Any]] = None,
    data_sources: Optional[dict[str, Any]] = None,
    observe_only: bool = True,
) -> dict[str, Any]:
    """
    Run all configured intelligence modules on the signal and return one
    canonical intelligence_evaluation payload. Never raises — all module
    errors are captured in the payload.

    observe_only is always True. The parameter exists only so callers can
    verify the contract without needing to trust internals.
    """
    observe_only = True  # enforce regardless of argument

    sig: dict = dict(signal or {})
    now = _now_iso()

    # ── Build market context ──────────────────────────────────────────────────
    ctx: dict = {}
    ctx_result: dict = {}
    if market_context is not None:
        ctx = dict(market_context)
        ctx_result = _module_result(
            available=bool(ctx),
            score=None,
            source="caller_provided",
            missing_reason=None if ctx else "empty_market_context",
        )
    else:
        try:
            ctx = _run_market_context(sig, data_sources) or {}
            ctx_result = _module_result(
                available=bool(ctx),
                score=None,
                source="market_context_builder",
                missing_reason=None if ctx else "market_context_builder_returned_empty",
                raw=ctx,
            )
        except Exception as ctx_exc:
            log.debug("[intelligence] market_context_builder failed: %s", ctx_exc)
            ctx_result = _module_result(
                available=False,
                score=None,
                error=str(ctx_exc)[:200],
            )

    # ── Run per-module scorers (upstream first, profile last) ────────────────────
    import time as _time
    _eval_start = _time.monotonic()
    module_results: dict[str, dict] = {"market_context": ctx_result}

    def _timed_wrap(name, fn, *args, **kw):
        _t0 = _time.monotonic()
        result = _wrap_module(name, fn, *args, **kw)
        result["duration_ms"] = round((_time.monotonic() - _t0) * 1000, 1)
        return result

    module_results["sector_context"]       = _timed_wrap("sector_context",       _run_sector_context,       sig, ctx)
    module_results["volume_confirmation"]  = _timed_wrap("volume_confirmation",  _run_volume_confirmation,  sig, ctx)
    module_results["vwap_context"]         = _timed_wrap("vwap_context",         _run_vwap_context,         sig, ctx)
    module_results["fair_value_gap"]       = _timed_wrap("fair_value_gap",       _run_fair_value_gap,       sig, ctx)
    module_results["the_strat_confluence"] = _timed_wrap("the_strat_confluence", _run_the_strat_confluence, sig, ctx)
    module_results["trigger_geometry"]     = _timed_wrap("trigger_geometry",     _run_trigger_geometry,     sig)
    module_results["expected_move"]        = _timed_wrap("expected_move",        _run_expected_move,        sig, ctx)

    # position_score_profile runs last and receives all upstream results
    _t0_psp = _time.monotonic()
    _psp_raw = _wrap_module(
        "position_score_profile",
        _run_position_score_profile,
        sig, ctx,
        {k: v for k, v in module_results.items() if k != "market_context"},
    )
    _psp_raw["duration_ms"] = round((_time.monotonic() - _t0_psp) * 1000, 1)
    module_results["position_score_profile"] = _psp_raw

    # ── Aggregate status ───────────────────────────────────────────────────────
    module_statuses: dict[str, str] = {}
    module_scores:   dict[str, Any] = {}
    missing_inputs:  list[str] = []
    stale_inputs:    list[str] = []
    errors:          list[str] = []

    for mod_name, mod_res in module_results.items():
        avail = bool(mod_res.get("available"))
        err   = mod_res.get("error")
        miss  = mod_res.get("missing_reason")
        score = mod_res.get("score")

        # Fix 5: populate stale_inputs when module reports freshness=stale.
        # A stale module is still counted as available but surfaced explicitly.
        if avail and mod_res.get("freshness") == "stale":
            stale_inputs.append(mod_name)

        if avail and not err:
            module_statuses[mod_name] = "available"
        elif err:
            module_statuses[mod_name] = "error"
            errors.append(f"{mod_name}:{err[:100]}")
            missing_inputs.append(mod_name)
        else:
            module_statuses[mod_name] = "unavailable"
            missing_inputs.append(mod_name)

        if mod_name != "market_context":
            module_scores[mod_name] = score

    # One canonical 0-100 score is derived from the detailed raw profile.
    # The raw profile remains available for diagnostics and compatibility.
    psp_raw = (module_results.get("position_score_profile") or {}).get("_raw") or {}
    from ap.intelligence_score import build_intelligence_score
    intelligence_score = build_intelligence_score(
        psp_raw,
        signal=sig,
        client_id=str(client_id or ""),
        execution_mode=str(execution_mode or ""),
        scored_at=now,
        data_as_of=ctx.get("data_as_of") or now,
        source="intelligence_evaluation",
        git_commit=_CACHED_GIT_COMMIT,
    )
    raw_profile_score = (
        psp_raw.get("total_score")
        if isinstance(psp_raw.get("total_score"), (int, float))
        else None
    )

    # ── Canonical payload ──────────────────────────────────────────────────────
    payload: dict[str, Any] = {
        "intelligence_evaluation_version": INTELLIGENCE_EVAL_VERSION,
        "client_id":           str(client_id or ""),
        "execution_mode":      str(execution_mode or ""),
        "signal_id":           str(sig.get("signal_id") or ""),
        "canonical_signal_id": str(sig.get("canonical_signal_id") or sig.get("signal_id") or ""),
        "ticker":              str(sig.get("ticker") or sig.get("symbol") or ""),
        "side":                str(sig.get("side") or sig.get("direction") or ""),
        "timeframe":           str(sig.get("timeframe") or ""),
        "pattern":             str(sig.get("pattern") or ""),
        "evaluated_at":        now,
        "data_as_of":          ctx.get("data_as_of") or now,
        "module_version":      INTELLIGENCE_EVAL_VERSION,
        "config_hash":         _config_hash({}),
        "git_commit":          _CACHED_GIT_COMMIT,  # cached at module load, not per evaluation
        "available":           len(missing_inputs) < len(module_results),
        # Backward-compatible diagnostic field (0-120 raw profile scale).
        "overall_score":       raw_profile_score,
        # Canonical research-ranking field (strict 0-100, or None if invalid).
        "policy_score":        intelligence_score.get("policy_score"),
        "intelligence_score":  intelligence_score,
        "module_scores":       module_scores,
        "module_statuses":     module_statuses,
        "missing_inputs":      sorted(set(missing_inputs)),
        "stale_inputs":        sorted(set(stale_inputs)),
        "errors":              errors,
        "observe_only":        True,   # hard-coded; never client-controlled
        "evaluation_duration_ms": round((_time.monotonic() - _eval_start) * 1000, 1),
        "module_results": {
            k: {kk: vv for kk, vv in v.items() if kk != "_raw"}
            for k, v in module_results.items()
        },
    }
    return payload


# ─────────────────────────────────────────────────────────────────────────────
# Persistence helpers — write to every required destination
# ─────────────────────────────────────────────────────────────────────────────

def persist_intelligence_evaluation(
    payload: dict[str, Any],
    *,
    plan: Any = None,
    order_meta_writer: Any = None,
    local_order_id: Optional[str] = None,
    dossier: Optional[dict] = None,
    rejected_diag: Optional[dict] = None,
) -> None:
    """
    Write the canonical intelligence_evaluation payload to every required
    destination. Never raises — each destination is written independently.

    Destinations:
      1. approved_plan.metadata["intelligence_evaluation"]
      2. approved_plan.metadata["score_audit"]["intelligence_evaluation"]
      3. orders.meta via order_meta_writer(local_order_id, patch) when available
      4. trade dossier["dossier"]["intelligence_evaluation"]
      5. rejected_diag["intelligence_evaluation"]
    """
    key = "intelligence_evaluation"

    # 1. Approved plan metadata
    try:
        if plan is not None:
            meta = getattr(plan, "metadata", None)
            if meta is None:
                try:
                    plan.metadata = {}
                    meta = plan.metadata
                except Exception:
                    meta = None
            if isinstance(meta, dict):
                meta[key] = payload
                # 2. score_audit sub-key
                if isinstance(meta.get("score_audit"), dict):
                    meta["score_audit"][key] = payload
                else:
                    meta.setdefault("score_audit", {})[key] = payload
    except Exception as exc:
        log.debug("intelligence persist to plan failed: %s", exc)

    # 3. orders.meta
    try:
        if callable(order_meta_writer) and local_order_id:
            order_meta_writer(str(local_order_id), {key: payload})
    except Exception as exc:
        log.debug("intelligence persist to orders.meta failed: %s", exc)

    # 4. Trade dossier
    try:
        if isinstance(dossier, dict):
            inner = dossier.get("dossier")
            if isinstance(inner, dict):
                inner[key] = payload
            else:
                dossier[key] = payload
    except Exception as exc:
        log.debug("intelligence persist to dossier failed: %s", exc)

    # 5. Rejected/blocked diagnostics
    try:
        if isinstance(rejected_diag, dict):
            rejected_diag[key] = payload
    except Exception as exc:
        log.debug("intelligence persist to rejected_diag failed: %s", exc)


# ─────────────────────────────────────────────────────────────────────────────
# Combined convenience entry point used by production seam
# ─────────────────────────────────────────────────────────────────────────────

def evaluate_and_persist_intelligence(
    signal: dict[str, Any],
    *,
    client_id: str,
    execution_mode: str,
    plan: Any = None,
    order_meta_writer: Any = None,
    local_order_id: Optional[str] = None,
    dossier: Optional[dict] = None,
    rejected_diag: Optional[dict] = None,
    market_context: Optional[dict] = None,
    data_sources: Optional[dict] = None,
) -> dict[str, Any]:
    """
    Evaluate all modules and persist to all destinations in one call.
    Returns the payload for caller inspection. Never raises.
    """
    try:
        payload = evaluate_intelligence(
            signal,
            client_id=client_id,
            execution_mode=execution_mode,
            market_context=market_context,
            data_sources=data_sources,
        )
    except Exception as exc:
        log.warning("[intelligence] evaluate_intelligence raised unexpectedly: %s", exc)
        payload = {
            "intelligence_evaluation_version": INTELLIGENCE_EVAL_VERSION,
            "observe_only": True,
            "available": False,
            "error": str(exc)[:200],
            "client_id": str(client_id or ""),
            "execution_mode": str(execution_mode or ""),
            "signal_id": str((signal or {}).get("signal_id") or ""),
        }

    persist_intelligence_evaluation(
        payload,
        plan=plan,
        order_meta_writer=order_meta_writer,
        local_order_id=local_order_id,
        dossier=dossier,
        rejected_diag=rejected_diag,
    )
    return payload


# ─────────────────────────────────────────────────────────────────────────────
# Bounded, idempotent intelligence executor
# ─────────────────────────────────────────────────────────────────────────────

def _make_eval_key(
    *,
    client_id: str,
    execution_mode: str,
    signal_id: str,
    local_order_id: str,
    evaluation_version: str,
) -> str:
    """
    Stable evaluation key for idempotency tracking.
    Same inputs always produce the same key within a process lifetime.
    """
    raw = f"{client_id}:{execution_mode}:{signal_id}:{local_order_id}:{evaluation_version}"
    return hashlib.sha1(raw.encode()).hexdigest()[:20]


def make_phase_intelligence_key(
    *,
    client_id: str,
    execution_mode: str,
    canonical_signal_id: str,
    local_order_id: str = "",
    phase: str,
    context_revision: int = 1,
    profile_version: str = INTELLIGENCE_EVAL_VERSION,
) -> str:
    """
    Stable phase-aware key for durable intelligence context snapshots.
    Preserves the legacy intelligence_evaluation metadata key while giving
    PRETRIGGER/PREOPEN/BREACH/CONTRACT_SELECTED independent identities.
    """
    raw = "|".join(
        (
            str(client_id or ""),
            str(execution_mode or "").upper(),
            str(canonical_signal_id or ""),
            str(local_order_id or "").strip() or "__none__",
            str(phase or "").upper(),
            str(int(context_revision or 1)),
            str(profile_version or ""),
        )
    )
    return hashlib.sha1(raw.encode()).hexdigest()[:24]


# ─────────────────────────────────────────────────────────────────────────────
# Req 2: Safe plan accessor — handles both object and dict approved plans
# ─────────────────────────────────────────────────────────────────────────────

def _plan_attr(plan: Any, *keys: str) -> Any:
    """
    Safe read-only accessor for both object and dict approved plans.
    Tries each key in order; returns first nonblank value or None.
    Never mutates the plan.
    """
    for key in keys:
        try:
            v = plan.get(key) if isinstance(plan, dict) else getattr(plan, key, None)
            if v is not None and v != "":
                return v
        except Exception:
            pass
    return None


def _resolve_execution_mode(sig: dict, plan: Any) -> "tuple[str, str]":
    """
    Resolve and validate execution_mode from both signal and plan.

    Returns (normalized_mode, status):
      normalized_mode: "live" | "paper" | ""
      status: "ok" | "blank_mode" | "identity_mismatch" | "invalid_execution_mode"

    Precedence: sig wins; plan fills gap.
    If both nonblank and disagree → "identity_mismatch".
    If result is not "live" or "paper" → "invalid_execution_mode".
    Blank → "blank_mode". Do not dispatch under blank or ambiguous identity.
    """
    def _norm(raw: Any) -> str:
        m = str(raw or "").strip().lower()
        if m in ("live",):
            return "live"
        if m in ("paper", "sandbox", "test", "paper_sim"):
            return "paper"
        return ""

    sig_mode  = _norm(sig.get("execution_mode") or sig.get("mode") or "")
    plan_mode = _norm(_plan_attr(plan, "execution_mode", "mode") or "")

    nonblank = [m for m in (sig_mode, plan_mode) if m]
    if not nonblank:
        return "", "blank_mode"
    unique = set(nonblank)
    if len(unique) > 1:
        return "", "identity_mismatch"
    return unique.pop(), "ok"


# ─────────────────────────────────────────────────────────────────────────────
# Req 1: Canonical evaluation snapshot from signal + approved_plan
# ─────────────────────────────────────────────────────────────────────────────

def build_intelligence_signal(
    sig: Any,
    approved_plan: Any = None,
) -> dict:
    """
    Build one canonical evaluation input snapshot from both signal and approved_plan.
    Neither source is mutated. The result is a deep-copied, merged dict.

    Explicit precedence (documented):
      - Identity fields (client_id, execution_mode, ticker, signal_id):
          sig wins; plan fills gap.
      - Market-data fields (underlying_price, entry_price, current volume):
          sig wins; plan fills gap (sig has fresher live data).
      - Trade-geometry fields (stop, target/PT1, PT2, trigger_price):
          sig wins; plan fills gap (plan owns the approved geometry).
      - Volume / sizing context:
          sig.volume_context wins; plan.metadata fills gaps.
      - Metadata:
          plan.metadata merged under the snapshot; sig fields win on conflict.

    Do not rely on sig["plan"] already containing the approved plan.
    """
    base = copy.deepcopy(dict(sig or {}))

    if approved_plan is None:
        return base

    def _pa(*keys):
        return _plan_attr(approved_plan, *keys)

    def _fill(field: str, *plan_keys: str) -> None:
        """Fill base[field] from plan if absent or blank in base."""
        if not base.get(field):
            v = _pa(*plan_keys)
            if v is not None:
                base[field] = v

    # Identity
    _fill("client_id",      "client_id", "email")
    _fill("execution_mode", "execution_mode", "mode")
    _fill("signal_id",      "signal_id")
    _fill("ticker",         "ticker", "symbol")
    _fill("side",           "side", "direction")
    _fill("timeframe",      "timeframe")
    _fill("pattern",        "pattern")

    # Trade geometry
    _fill("trigger",        "trigger_price", "trigger")
    _fill("trigger_price",  "trigger_price", "trigger")
    _fill("underlying_price", "underlying_price", "current_price")
    _fill("stop",           "stop_price", "stop")
    _fill("stop_price",     "stop_price", "stop")
    _fill("target",         "target_underlying", "pt1", "target", "target_price")
    _fill("target_price",   "target_underlying", "pt1", "target", "target_price")
    _fill("pt1",            "pt1", "target_underlying")
    _fill("pt2",            "pt2")
    _fill("entry_price",    "entry_price")

    # Volume / sizing from plan.metadata when absent in sig
    plan_meta = _pa("metadata") or {}
    if isinstance(plan_meta, dict):
        if not base.get("volume_context") and plan_meta.get("volume_context"):
            base["volume_context"] = copy.deepcopy(plan_meta["volume_context"])
        if not base.get("sizing_context") and plan_meta.get("sizing_context"):
            base["sizing_context"] = copy.deepcopy(plan_meta["sizing_context"])
        # Merge any intelligence keys the plan carries
        for mk in ("atm_iv", "market_breadth", "sector", "sector_etf"):
            if not base.get(mk) and plan_meta.get(mk):
                base[mk] = plan_meta[mk]
        # Attach normalized metadata for geometry adapters to read
        base.setdefault("metadata", {})
        for mk, mv in plan_meta.items():
            base["metadata"].setdefault(mk, copy.deepcopy(mv))

    return base


# ─────────────────────────────────────────────────────────────────────────────
# Req 3: In-memory durable rejection diagnostic store
# Used when orders.meta is unavailable (no local_order_id at some paths).
# Keyed by client_id:exec_mode:signal_id:eval_key.
# ─────────────────────────────────────────────────────────────────────────────

_REJECTION_DIAG_STORE: dict[str, dict] = {}
_REJECTION_DIAG_LOCK  = threading.Lock()


def _write_rejection_diag(
    eval_key: str,
    payload: dict,
    *,
    client_id: str,
    execution_mode: str,
    signal_id: str,
) -> None:
    """
    Persist intelligence diagnostic to the in-memory rejection store.
    Used when no local_order_id / orders.meta is available.
    Key: client_id:exec_mode:signal_id:eval_key
    """
    key = f"{client_id}:{execution_mode}:{signal_id}:{eval_key}"
    with _REJECTION_DIAG_LOCK:
        _REJECTION_DIAG_STORE[key] = payload


def get_rejection_diag(
    *,
    client_id: str,
    execution_mode: str,
    signal_id: str,
    eval_key: str,
) -> Optional[dict]:
    """Read back a rejection diagnostic by identity key."""
    key = f"{client_id}:{execution_mode}:{signal_id}:{eval_key}"
    with _REJECTION_DIAG_LOCK:
        return _REJECTION_DIAG_STORE.get(key)


def _take_signal_snapshot(signal: Any) -> dict:
    """
    Immutable deep copy of the signal dict.
    Background thread must not read the live signal object — the caller
    may mutate it after dispatch (e.g. adding broker order fields).
    """
    try:
        return copy.deepcopy(dict(signal or {}))
    except Exception:
        try:
            return dict(signal or {})
        except Exception:
            return {}


def _take_plan_metadata_snapshot(plan: Any) -> dict:
    """
    Shallow copy of the plan metadata fields needed for persistence.
    Does not hold a reference to the full plan object.
    """
    try:
        meta = getattr(plan, "metadata", None) or {}
        return {
            "local_order_id": meta.get("local_order_id"),
            "signal_id":      meta.get("signal_id"),
            "score_audit":    meta.get("score_audit"),
        }
    except Exception:
        return {}


# ─────────────────────────────────────────────────────────────────────────────
# Req 8: Bounded LRU state cache
# ─────────────────────────────────────────────────────────────────────────────

class _BoundedStateCache:
    """
    Bounded LRU state cache for eval_key lifecycle tracking.
    - Max size enforced by evicting the oldest terminal (non-pending) entry.
    - TTL enforced: terminal entries older than ttl_s are treated as absent.
    - In-flight (pending) entries are never evicted.
    Thread-safe: all mutations hold self._lock.
    """
    def __init__(self, maxsize: int = _INTEL_MAX_COMPLETED,
                 ttl_s: float = _INTEL_STATE_TTL_S) -> None:
        self._maxsize = maxsize
        self._ttl_s   = ttl_s
        self._lock    = threading.Lock()
        # (state, monotonic_timestamp)
        self._data: OrderedDict[str, tuple[str, float]] = OrderedDict()

    def get(self, key: str) -> Optional[str]:
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            state, ts = entry
            if state != "pending" and (_time_module.monotonic() - ts) > self._ttl_s:
                del self._data[key]
                return None
            self._data.move_to_end(key)
            return state

    def set(self, key: str, state: str) -> None:
        with self._lock:
            now = _time_module.monotonic()
            if key in self._data:
                self._data.move_to_end(key)
                self._data[key] = (state, now)
            else:
                self._data[key] = (state, now)
                if len(self._data) > self._maxsize:
                    for k in list(self._data.keys()):
                        if self._data[k][0] != "pending":
                            del self._data[k]
                            break

    def transition(self, key: str, from_state: str, to_state: str) -> bool:
        """Atomic CAS transition. Returns True if applied."""
        with self._lock:
            entry = self._data.get(key)
            current = entry[0] if entry else "pending"
            if current != from_state:
                return False
            self._data[key] = (to_state, _time_module.monotonic())
            if key in self._data:
                self._data.move_to_end(key)
            return True

    def __contains__(self, key: str) -> bool:
        return self.get(key) is not None

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


# ─────────────────────────────────────────────────────────────────────────────
# Req 2: Canonical volume input adapter
# ─────────────────────────────────────────────────────────────────────────────

def _build_volume_inputs(sig: dict, ctx: dict) -> dict:
    """
    Build canonical volume inputs from production signal/plan/context shapes.
    Explicit precedence (first nonblank wins per field):
      current_volume:  sig.volume_context.current_volume → ctx.volume.current → sig.volume
      avg_volume:      sig.volume_context.avg_volume → ctx.volume.average → sig.avg_volume
      relative_volume: sig.volume_context.relative_volume → ctx.volume.relative → computed
      confirmation:    sig.volume_context.breakout → ctx.volume.confirmation
      freshness:       sig.volume_context.freshness → "unknown"
    Returns augmented signal dict for score_volume_confirmation().
    """
    def _pf(v) -> Optional[float]:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    vc  = sig.get("volume_context") or {}
    cvc = ctx.get("volume") or {}
    meta_vc = (sig.get("metadata") or {}).get("volume_context") or {}

    cur = (_pf(vc.get("current_volume"))
           or _pf(cvc.get("current"))
           or _pf(sig.get("volume"))
           or _pf(meta_vc.get("current_volume")))
    avg = (_pf(vc.get("avg_volume"))
           or _pf(cvc.get("average"))
           or _pf(sig.get("avg_volume"))
           or _pf(meta_vc.get("avg_volume")))
    rel = (_pf(vc.get("relative_volume"))
           or _pf(cvc.get("relative"))
           or _pf(sig.get("relative_volume"))
           or _pf(meta_vc.get("relative_volume")))
    if rel is None and cur and avg and avg > 0:
        rel = round(cur / avg, 4)

    conf = (vc.get("breakout")
            if "breakout" in vc
            else cvc.get("confirmation")
            if "confirmation" in cvc
            else meta_vc.get("breakout"))
    fresh = vc.get("freshness") or cvc.get("freshness") or meta_vc.get("freshness")

    adapted = dict(sig)
    adapted["_vol_canonical"] = {
        "current_volume":    cur,
        "avg_volume":        avg,
        "relative_volume":   rel,
        "confirmation":      conf,
        "freshness":         fresh,
        "source":            "volume_adapter",
        "source_paths": {
            "current": "sig.volume_context.current_volume" if _pf(vc.get("current_volume")) else "ctx/meta",
            "avg":     "sig.volume_context.avg_volume"     if _pf(vc.get("avg_volume"))     else "ctx/meta",
        },
    }
    # Merge into top-level keys the scorer reads
    if cur is not None:
        adapted.setdefault("volume", cur)
    if avg is not None:
        adapted.setdefault("avg_volume", avg)
    if rel is not None:
        adapted.setdefault("relative_volume", rel)
    return adapted


# ─────────────────────────────────────────────────────────────────────────────
# Req 3: Canonical trigger geometry input adapter
# ─────────────────────────────────────────────────────────────────────────────

def _build_geometry_inputs(sig: dict) -> dict:
    """
    Build canonical trigger geometry inputs from production plan/signal shapes.
    Resolves: side, trigger, underlying_price, stop, target/PT1, entry_price.
    Explicit deterministic precedence — never reports unavailable when production
    metadata already has the value under a supported nested key.
    """
    def _pf(v) -> Optional[float]:
        try:
            return float(v) if v is not None else None
        except (TypeError, ValueError):
            return None

    meta = sig.get("metadata") or {}
    plan = sig.get("plan") or {}

    # Side / direction
    side = (str(sig.get("side") or sig.get("direction") or
                plan.get("side") or meta.get("side") or "").upper() or None)

    # Trigger
    trigger = (_pf(sig.get("trigger"))
               or _pf(sig.get("trigger_price"))
               or _pf(plan.get("trigger_price"))
               or _pf(meta.get("trigger_price")))

    # Current underlying price
    underlying = (_pf(sig.get("underlying_price"))
                  or _pf(sig.get("entry_price"))
                  or _pf(plan.get("underlying_price"))
                  or _pf(meta.get("underlying_price")))

    # Stop
    stop = (_pf(sig.get("stop"))
            or _pf(sig.get("stop_price"))
            or _pf(plan.get("stop_price"))
            or _pf(meta.get("stop_price"))
            or _pf(plan.get("stop")))

    # Target / PT1
    target = (_pf(sig.get("target"))
              or _pf(sig.get("target_price"))
              or _pf(sig.get("pt1"))
              or _pf(plan.get("target_price"))
              or _pf(plan.get("pt1"))
              or _pf(meta.get("target_price"))
              or _pf(meta.get("pt1")))

    entry = (_pf(sig.get("entry_price"))
             or _pf(sig.get("underlying_price"))
             or _pf(plan.get("entry_price")))

    adapted = dict(sig)
    adapted["_geom_canonical"] = {
        "side":             side,
        "trigger":          trigger,
        "underlying_price": underlying,
        "stop":             stop,
        "target":           target,
        "entry_price":      entry,
    }
    # Normalize into top-level keys the scorer reads
    if side:
        adapted["side"]             = side
    if trigger is not None:
        adapted["trigger"]          = trigger
    if underlying is not None:
        adapted["underlying_price"] = underlying
    if stop is not None:
        adapted["stop"]             = stop
    if target is not None:
        adapted["target"]           = target
    return adapted


def _make_plan_writer(plan: Any) -> Optional[callable]:
    """
    Create a safe closure that writes intelligence_evaluation to plan.metadata
    at dispatch time, capturing the reference to plan.metadata — not plan itself.
    The worker calls this closure; it does not read plan directly.
    Returns None when plan has no writable metadata.
    """
    try:
        meta = getattr(plan, "metadata", None)
        if meta is None:
            try:
                plan.metadata = {}
                meta = plan.metadata
            except Exception:
                return None
        if not isinstance(meta, dict):
            return None
        # Capture meta reference — the closure holds this dict, not the plan object.
        _captured_meta = meta
        def _writer(payload: dict) -> None:
            try:
                _captured_meta["intelligence_evaluation"] = payload
                score_audit = _captured_meta.get("score_audit")
                if not isinstance(score_audit, dict):
                    _captured_meta["score_audit"] = {}
                    score_audit = _captured_meta["score_audit"]
                score_audit["intelligence_evaluation"] = payload
            except Exception:
                pass
        return _writer
    except Exception:
        return None


class _IntelligenceExecutor:
    """
    Bounded, idempotent intelligence evaluation executor.

    Lifecycle guarantees:
      - ThreadPoolExecutor with _INTEL_MAX_WORKERS threads.
      - Real bounded queue: Semaphore(max_workers + max_pending) limits total
        tasks. Acquired before executor.submit(); released in done callback.
        Queue saturation detected without shutting down the executor.
      - Atomic state machine per eval_key (pending→completed|timed_out|error).
        Only one transition may win; late writers check state before persisting.
      - plan_writer closure captured at dispatch time, called by worker on win.
      - Immutable signal snapshots; worker never reads live plan/sig objects.
      - Timeout bounds observer waiting, not worker runtime. A timed-out Python
        thread cannot be cancelled; it occupies a worker slot until it exits.

    Thread-safety: all state mutations hold _lock.
    """

    def __init__(self, max_workers: Optional[int] = None, max_pending: Optional[int] = None) -> None:
        _mw = max_workers if max_workers is not None else _INTEL_MAX_WORKERS
        _mp = max_pending if max_pending is not None else _INTEL_MAX_PENDING
        self._lock             = threading.Lock()
        # Fix 1: real bounded queue via Semaphore(max_workers + max_pending).
        # Acquired before submit; released in done callback. No executor shutdown needed.
        self._capacity         = threading.Semaphore(_mw + _mp)
        self._executor         = concurrent.futures.ThreadPoolExecutor(
            max_workers=_mw,
            thread_name_prefix="intelligence-worker",
        )
        self._monitor_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=_mw,
            thread_name_prefix="intelligence-monitor",
        )
        # Req 8: atomic state machine via bounded LRU cache.
        self._state_cache = _BoundedStateCache()
        self._completed_cache = _BoundedStateCache()  # sentinel for completed evals
        self._inflight:  dict[str, concurrent.futures.Future] = {}

    def _transition_state(self, eval_key: str, from_state: str, to_state: str) -> bool:
        """
        Attempt an atomic state transition.
        Returns True if the transition was applied, False if state already moved on.
        """
        return self._state_cache.transition(eval_key, from_state, to_state)

    def _get_state(self, eval_key: str) -> str:
        return self._state_cache.get(eval_key) or "pending"

    def submit(
        self,
        *,
        eval_key:              str,
        signal_snapshot:       dict,
        client_id:             str,
        execution_mode:        str,
        local_order_id:        str,
        plan_metadata_snapshot: dict,
        plan_writer:           Optional[callable],
        order_meta_writer:     Any,
        rejected_diag:         Optional[dict],
        budget_s:              float,
        ticker:                str,
    ) -> str:
        """
        Attempt to submit an intelligence evaluation.

        Returns one of:
          "submitted"          — new Future accepted into executor
          "in_flight"          — duplicate key already running; skipped
          "completed_cached"   — key already evaluated; result available
          "queue_saturated"    — real bounded queue full; diagnostic written
          "executor_error"     — internal error; diagnostic written
        """
        with self._lock:
            # (a) Completed — reuse, no recompute (bounded LRU)
            if self._completed_cache.get(eval_key) == "yes" or self._state_cache.get(eval_key) == "completed":
                return "completed_cached"

            # (b) In-flight — guard against duplicate
            if eval_key in self._inflight:
                fut = self._inflight[eval_key]
                if not fut.done():
                    return "in_flight"
                del self._inflight[eval_key]
                return "completed_cached"

            # Initialize state to pending in bounded cache
            self._state_cache.set(eval_key, "pending")

        # Fix 1: acquire bounded semaphore before submitting to executor.
        # Non-blocking: if no slot available, return queue_saturated immediately.
        acquired = self._capacity.acquire(blocking=False)
        if not acquired:
            self._state_cache.set(eval_key, "queue_saturated")
            self._write_status_diag(
                eval_key=eval_key, status="queue_saturated",
                error="max_workers+max_pending capacity reached",
                client_id=client_id, local_order_id=local_order_id,
                order_meta_writer=order_meta_writer, rejected_diag=rejected_diag,
            )
            return "queue_saturated"

        # (c) New submission
        try:
            future = self._executor.submit(
                self._run_evaluation,
                eval_key=eval_key,
                signal_snapshot=signal_snapshot,
                client_id=client_id,
                execution_mode=execution_mode,
                local_order_id=local_order_id,
                plan_metadata_snapshot=plan_metadata_snapshot,
                plan_writer=plan_writer,
                order_meta_writer=order_meta_writer,
                rejected_diag=rejected_diag,
            )
            # Release semaphore slot when future completes (success, error, or cancel)
            future.add_done_callback(lambda _f: self._capacity.release())
            with self._lock:
                self._inflight[eval_key] = future
        except Exception as exc:
            self._capacity.release()  # release the slot we acquired
            self._state_cache.set(eval_key, "queue_saturated")
            self._write_status_diag(
                eval_key=eval_key, status="queue_saturated", error=str(exc)[:200],
                client_id=client_id, local_order_id=local_order_id,
                order_meta_writer=order_meta_writer, rejected_diag=rejected_diag,
            )
            return "queue_saturated"

        # Launch monitor outside lock — blocks for budget_s in monitor thread
        try:
            self._monitor_executor.submit(
                self._monitor_future,
                eval_key=eval_key,
                future=future,
                budget_s=budget_s,
                order_meta_writer=order_meta_writer,
                local_order_id=local_order_id,
                rejected_diag=rejected_diag,
                ticker=ticker,
            )
        except Exception:
            pass   # monitor failure does not affect trade flow

        return "submitted"

    def _run_evaluation(
        self,
        *,
        eval_key:              str,
        signal_snapshot:       dict,
        client_id:             str,
        execution_mode:        str,
        local_order_id:        str,
        plan_metadata_snapshot: dict,
        plan_writer:           Optional[callable],
        order_meta_writer:     Any,
        rejected_diag:         Optional[dict],
    ) -> dict:
        """
        Worker function: runs evaluate_intelligence and persists result.

        Fix 2 (atomic state): only persists the canonical intelligence_evaluation
        when it wins the pending → completed transition. If the monitor already
        transitioned to timed_out, the worker persists a late_result_discarded
        entry instead of overwriting the canonical status.

        Fix 3 (plan persistence): calls plan_writer (captured at dispatch time)
        on successful completion to write to plan.metadata and score_audit.
        """
        try:
            payload = evaluate_intelligence(
                signal_snapshot,
                client_id=client_id,
                execution_mode=execution_mode,
            )
            # Stamp stale-write guards
            payload["evaluation_key"]      = eval_key
            payload["evaluation_version"]  = INTELLIGENCE_EVAL_VERSION
            payload["intelligence_status"] = "completed"
            payload["intelligence_completed_at"] = _now_iso()

            # Fix 2: atomic transition pending → completed.
            # If monitor already wrote timed_out, we must not overwrite.
            won = self._transition_state(eval_key, "pending", "completed")
            if won:
                # We won — persist canonical payload to all destinations.
                persist_intelligence_evaluation(
                    payload,
                    order_meta_writer=order_meta_writer,
                    local_order_id=local_order_id,
                    rejected_diag=rejected_diag,
                )
                # Fix 3: persist to plan.metadata and score_audit via captured writer.
                if callable(plan_writer):
                    try:
                        plan_writer(payload)
                    except Exception:
                        pass
                with self._lock:
                    self._completed_cache.set(eval_key, "yes")
                    self._inflight.pop(eval_key, None)
            else:
                # Lost race — monitor already set timed_out (or error).
                # Write a late_result_discarded diagnostic only (do NOT overwrite canonical).
                _late_diag = {
                    "evaluation_key":          eval_key,
                    "intelligence_status":     "late_result_discarded",
                    "observe_only":            True,
                    "won_state":               self._get_state(eval_key),
                    "late_completed_at":       _now_iso(),
                }
                try:
                    if callable(order_meta_writer) and local_order_id:
                        order_meta_writer(str(local_order_id),
                                          {"intelligence_late_result": _late_diag})
                except Exception:
                    pass
                with self._lock:
                    self._inflight.pop(eval_key, None)
            return payload
        except Exception as exc:
            self._transition_state(eval_key, "pending", "error")
            self._write_status_diag(
                eval_key=eval_key, status="error", error=str(exc)[:200],
                client_id=client_id, local_order_id=local_order_id,
                order_meta_writer=order_meta_writer, rejected_diag=rejected_diag,
            )
            with self._lock:
                self._inflight.pop(eval_key, None)
            raise   # re-raise so Future.exception() is set correctly

    def _monitor_future(
        self,
        *,
        eval_key:          str,
        future:            concurrent.futures.Future,
        budget_s:          float,
        order_meta_writer: Any,
        local_order_id:    str,
        rejected_diag:     Optional[dict],
        ticker:            str,
    ) -> None:
        """
        Monitor the future for budget_s seconds.

        Fix 2 (atomic): uses _transition_state(pending → timed_out) so only
        one path (monitor or worker) wins the canonical write.

        Fix 4 (documentation): timeout bounds observer waiting, not worker
        runtime. A running Python thread cannot be forcibly cancelled.
        A timed-out worker continues occupying its slot until it exits.
        The semaphore prevents unbounded backlog; saturation is persisted.
        """
        done, _not_done = concurrent.futures.wait([future], timeout=budget_s)
        if future not in done:
            # Attempt atomic transition pending → timed_out
            won = self._transition_state(eval_key, "pending", "timed_out")
            if won:
                log.warning(
                    "[%s] INTELLIGENCE_EVALUATION_TIMED_OUT "
                    "eval_key=%s budget_ms=%d "
                    "(worker still running; semaphore slot held until thread exits)",
                    ticker, eval_key, int(budget_s * 1000),
                )
                self._write_status_diag(
                    eval_key=eval_key,
                    status="timed_out",
                    error=f"exceeded_budget_ms={int(budget_s * 1000)}",
                    client_id="", local_order_id=local_order_id,
                    order_meta_writer=order_meta_writer,
                    rejected_diag=rejected_diag,
                )
            # else: worker already transitioned to completed/error — don't overwrite
        else:
            # Completed (success or exception) — the worker already persisted
            avail_count = 0
            try:
                result = future.result()
                avail_count = sum(
                    1 for v in (result or {}).get("module_statuses", {}).values()
                    if v == "available"
                )
            except Exception:
                pass
            log.info(
                "[%s] INTELLIGENCE_EVALUATION_COMPLETE eval_key=%s "
                "modules_available=%d observe_only=true",
                ticker, eval_key, avail_count,
            )

    def _write_status_diag(
        self,
        *,
        eval_key:          str,
        status:            str,
        error:             str,
        client_id:         str,
        local_order_id:    str,
        order_meta_writer: Any,
        rejected_diag:     Optional[dict],
    ) -> None:
        """Write a minimal intelligence_status diagnostic to all destinations."""
        diag = {
            "intelligence_evaluation_version": INTELLIGENCE_EVAL_VERSION,
            "evaluation_key":                  eval_key,
            "evaluated_at":                    _now_iso(),
            "evaluation_version":              INTELLIGENCE_EVAL_VERSION,
            "observe_only":                    True,
            "available":                       False,
            "intelligence_status":             status,
            "error":                           error,
        }
        try:
            if callable(order_meta_writer) and local_order_id:
                order_meta_writer(str(local_order_id), {"intelligence_evaluation": diag})
        except Exception:
            pass
        try:
            if isinstance(rejected_diag, dict):
                rejected_diag["intelligence_evaluation"] = diag
        except Exception:
            pass

    def get_completed(self, eval_key: str) -> Optional[str]:
        return self._completed_cache.get(eval_key)

    def is_in_flight(self, eval_key: str) -> bool:
        with self._lock:
            fut = self._inflight.get(eval_key)
            return fut is not None and not fut.done()

    def state_cache_size(self) -> int:
        return len(self._state_cache)


# Module-level singleton — one bounded executor per process
_EXECUTOR = _IntelligenceExecutor()


def _ensure_intelligence_dispatched(
    signal: Any,
    *,
    client_id:         str = "",
    execution_mode:    str = "",
    local_order_id:    str = "",
    plan:              Any = None,
    order_meta_writer: Any = None,
    ticker:            str = "",
) -> str:
    """
    Idempotent intelligence dispatch for use at the post-plan seam.
    Call once immediately after plan is finalized and identity is confirmed.
    All later post-plan paths (rejections, submit, expiry) share the same
    evaluation_key so dispatch never fires twice per lifecycle.

    Req 1: builds a canonical snapshot from both signal and approved_plan.
    Req 2: validates execution_mode from both sources before forming an eval_key.
          Blank or mismatched mode → persists diagnostic, no dispatch.
    Req 3: uses orders.meta when local_order_id present; rejection diag store otherwise.

    Returns the eval_key string, or "" if disabled/invalid.
    """
    if not _INTELLIGENCE_ENABLED:
        return ""
    try:
        # Req 1: build canonical merged snapshot from sig + approved_plan
        sig_snap = build_intelligence_signal(signal, plan)

        # Req 2: validate execution_mode from both sources
        xmode, mode_status = _resolve_execution_mode(sig_snap, plan)
        if mode_status != "ok":
            # Persist identity failure diagnostic without dispatching
            _diag = {
                "intelligence_evaluation_version": INTELLIGENCE_EVAL_VERSION,
                "observe_only": True,
                "available": False,
                "intelligence_status": mode_status,
                "execution_mode_attempted": str(execution_mode or ""),
                "evaluated_at": _now_iso(),
            }
            sig_id = str(sig_snap.get("signal_id") or "")
            cid    = str(sig_snap.get("client_id") or client_id or "")
            try:
                if callable(order_meta_writer) and local_order_id:
                    order_meta_writer(str(local_order_id), {"intelligence_evaluation": _diag})
                elif sig_id:
                    _write_rejection_diag(
                        "no_eval_key", _diag,
                        client_id=cid, execution_mode="", signal_id=sig_id,
                    )
            except Exception:
                pass
            log.debug("[%s] intelligence dispatch skipped: mode_status=%s", ticker, mode_status)
            return ""

        sig_id = str(sig_snap.get("signal_id") or "")
        cid    = str(sig_snap.get("client_id") or client_id or "")
        loid   = str(local_order_id or "")

        eval_key = _make_eval_key(
            client_id=cid, execution_mode=xmode,
            signal_id=sig_id, local_order_id=loid,
            evaluation_version=INTELLIGENCE_EVAL_VERSION,
        )

        # Idempotency: check executor state before dispatching
        if _EXECUTOR.get_completed(eval_key) or _EXECUTOR.is_in_flight(eval_key):
            return eval_key

        # Req 3: choose durable destination
        # local_order_id is guaranteed nonblank at the _on_entry_trigger seam
        # (blank-loid guard fires before plan recovery). For completeness, fall
        # back to rejection diag store when loid is absent (other call sites).
        def _meta_writer_with_fallback(loid_inner: str, patch: dict) -> None:
            if callable(order_meta_writer) and loid_inner:
                try:
                    order_meta_writer(loid_inner, patch)
                except Exception:
                    pass
            if not loid_inner:
                # No order exists — write to per-signal rejection diag store
                ie = patch.get("intelligence_evaluation") or patch
                try:
                    _write_rejection_diag(
                        eval_key, ie,
                        client_id=cid, execution_mode=xmode, signal_id=sig_id,
                    )
                except Exception:
                    pass

        submit_bounded_intelligence(
            sig_snap,               # canonical merged snapshot, not raw sig
            client_id=cid,
            execution_mode=xmode,
            local_order_id=loid,
            plan=plan,
            order_meta_writer=_meta_writer_with_fallback,
            ticker=ticker,
        )
        return eval_key
    except Exception as exc:
        log.debug("[intelligence] _ensure_intelligence_dispatched non-critical: %s", exc)
        return ""


def submit_bounded_intelligence(
    signal: Any,
    *,
    client_id:         str,
    execution_mode:    str,
    local_order_id:    str,
    plan:              Any = None,
    order_meta_writer: Any = None,
    rejected_diag:     Optional[dict] = None,
    ticker:            str = "",
) -> str:
    """
    Primary entry point for intelligence evaluation from ap_execution_core.

    1. Snapshots all mutable inputs (signal deep-copy, plan metadata snapshot).
    2. Creates plan_writer closure at dispatch time (Fix 3).
    3. Writes intelligence_status=pending to plan.metadata immediately, before
       the worker starts, so the plan always reflects current intelligence state.
    4. Submits to the bounded _EXECUTOR with the plan_writer.
    Returns submission status. Never raises.
    """
    # Req 6: Feature flag — disabled by default for controlled rollout.
    if not _INTELLIGENCE_ENABLED:
        return "disabled"
    try:
        signal_snapshot        = _take_signal_snapshot(signal)
        plan_metadata_snapshot = _take_plan_metadata_snapshot(plan)
        sig_id   = str(signal_snapshot.get("signal_id") or "")
        cid      = str(client_id or "")
        xmode    = str(execution_mode or "")
        loid     = str(local_order_id or "")
        tkr      = str(ticker or "")

        eval_key = _make_eval_key(
            client_id=cid, execution_mode=xmode,
            signal_id=sig_id, local_order_id=loid,
            evaluation_version=INTELLIGENCE_EVAL_VERSION,
        )
        budget_s = _INTELLIGENCE_BUDGET_MS / 1000.0

        # Fix 3: create plan_writer closure at dispatch time.
        plan_writer = _make_plan_writer(plan)

        # Write pending status to plan.metadata immediately (before worker starts).
        _pending_diag = {
            "intelligence_evaluation_version": INTELLIGENCE_EVAL_VERSION,
            "evaluation_key":     eval_key,
            "evaluated_at":       _now_iso(),
            "evaluation_version": INTELLIGENCE_EVAL_VERSION,
            "observe_only":       True,
            "intelligence_status": "pending",
            "intelligence_started_at": _now_iso(),
        }
        if callable(plan_writer):
            try:
                plan_writer(_pending_diag)
            except Exception:
                pass

        return _EXECUTOR.submit(
            eval_key=eval_key,
            signal_snapshot=signal_snapshot,
            client_id=cid,
            execution_mode=xmode,
            local_order_id=loid,
            plan_metadata_snapshot=plan_metadata_snapshot,
            plan_writer=plan_writer,
            order_meta_writer=order_meta_writer,
            rejected_diag=rejected_diag,
            budget_s=budget_s,
            ticker=tkr,
        )
    except Exception as exc:
        log.debug("[intelligence] submit_bounded_intelligence non-critical: %s", exc)
        return "executor_error"
