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
import math
import os
import re
import sys
import threading
import time
import concurrent.futures
import atexit
from datetime import datetime, timezone
from typing import Any, Optional

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
# - Gate G runs before selector/materialization, so it cannot treat a generic
#   intelligence-pipeline contract estimate as selected-contract truth.
# - Legacy pipeline data collection remains available to the compatibility
#   mapper below, but the active run_intelligence_check path is advisory until
#   production-shaped selected OCC evidence exists.
# - No mode or client identity is inferred from process environment here.
_AP_MODE = (os.getenv("AP_MODE") or os.getenv("BOT_MODE") or "paper").lower()
_INTEL_DATA_COLLECTION_OVERRIDE = os.getenv("INTEL_DATA_COLLECTION_OVERRIDE", "0") == "1"
_INTEL_FAIL_OPEN_UNAVAILABLE = os.getenv("INTEL_FAIL_OPEN_UNAVAILABLE", "0") == "1"
_INTEL_ENFORCE_RISK_VETO = os.getenv("INTEL_ENFORCE_RISK_VETO", "1") != "0"
_INTEL_IS_LIVE = _AP_MODE == "live"

_OCC_RE = re.compile(r"^[A-Z0-9.\-]{1,6}\d{6}[CP]\d{8}$")
_CONTRACT_ALIAS_KEYS = (
    "selected_occ",
    "occ_symbol",
    "occ",
    "contract_symbol",
    "selected_contract_symbol",
    "selected_option_symbol",
    "option_contract_symbol",
    "option_symbol",
)
_IDENTITY_NESTED_KEYS = (
    "selected_contract",
    "selected_option",
    "contract_evidence",
    "option_contract",
)
_INTEL_MAX_QUOTE_AGE_SECONDS = 120.0

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

def _data_collection_allowed(execution_mode: Optional[str] = None) -> bool:
    """Return the legacy compatibility collection policy.

    The active Gate G path does not use this function before selected-contract
    evidence exists.  Keeping the old no-argument behavior preserves the
    reviewed compatibility mapper and its tests without letting AP_MODE
    become production identity authority.
    """
    if execution_mode is not None:
        mode = str(execution_mode).strip().upper()
        if mode == "LIVE":
            return _INTEL_DATA_COLLECTION_OVERRIDE
        if mode == "PAPER":
            return True
        return False
    return _INTEL_DATA_COLLECTION_OVERRIDE or not _INTEL_IS_LIVE

def _safe_finite_float(value: Any, *, default: Optional[float] = None) -> Optional[float]:
    """Parse a finite numeric scalar without treating bool as a number."""
    if isinstance(value, bool) or value is None:
        return default
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return parsed if math.isfinite(parsed) else default


def _strict_integer(value: Any, *, minimum: int = 0) -> Optional[int]:
    """Parse integer fields while preserving explicit zero and rejecting junk."""
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str):
        raw = value.strip()
        if not re.fullmatch(r"[+-]?\d+", raw):
            return None
        try:
            parsed = int(raw)
        except ValueError:
            return None
    else:
        return None
    return parsed if parsed >= minimum else None


def _rounded_score(value: Any) -> float:
    parsed = _safe_finite_float(value, default=0.0)
    return round(parsed if parsed is not None else 0.0, 1)


def _block_gate(*, status: str, score: float, reasoning: str, risk_detail=None, price_data_stub: bool = False) -> dict:
    return {
        "approved": False,
        "score": _rounded_score(score),
        "contracts": 0,
        "reasoning": reasoning,
        "intel_status": status,
        "intel_score": _rounded_score(score) if score is not None else None,
        "risk_detail": risk_detail or {},
        "price_data_stub": price_data_stub,
    }

def _collect_gate(*, status: str, score: float, reasoning: str, risk_detail=None, price_data_stub: bool = False) -> dict:
    return {
        "approved": True,
        "score": _rounded_score(score),  # compatibility path only
        "contracts": 1,
        "reasoning": reasoning,
        "intel_status": status,
        "intel_score": _rounded_score(score) if score is not None else None,
        "risk_detail": risk_detail or {},
        "price_data_stub": price_data_stub,
    }

# FIX 4: Retry logic — transient errors don't permanently disable intel
# HIGH-002: Per-client pipeline state — prevents one client's failures from
# disabling intelligence for all clients sharing the process.
_PIPELINE_RETRY_SECONDS: float = float(os.getenv("INTEL_RETRY_SECONDS", "120.0"))
_PIPELINE_MAX_FAILS:     int   = int(os.getenv("INTEL_MAX_FAILS", "5"))

_pipelines:              dict[str, object] = {}   # client_id:mode → pipeline
_pipeline_fail_counts:   dict[str, int]    = {}   # client_id:mode → fail count
_pipeline_last_attempts: dict[str, float]  = {}   # client_id:mode → monotonic time
_pipeline_lock = threading.Lock()


def _get_pipeline(client_id: str = "", execution_mode: str = ""):
    """Legacy compatibility pipeline factory, not active Gate G authority.

    It requires explicit identity and a validated account value. The active
    Phase 1 path never calls it; this guard prevents a future caller from
    silently inheriting a process-wide mode or a hardcoded economic default.
    """
    client_key = str(client_id or "").strip()
    mode = _normalise_execution_mode(execution_mode)
    equity = _safe_finite_float(os.getenv("ACCOUNT_EQUITY"), default=None)
    if not client_key or not mode or equity is None or equity <= 0:
        log.warning(
            "intelligence_bridge: legacy pipeline unavailable without explicit "
            "client/mode and valid ACCOUNT_EQUITY"
        )
        return None
    pipeline_key = f"{client_key}:{mode}"
    with _pipeline_lock:
        pipeline = _pipelines.get(pipeline_key)
        if pipeline is not None:
            return pipeline
        if _pipeline_fail_counts.get(pipeline_key, 0) >= _PIPELINE_MAX_FAILS:
            return None

        now = time.monotonic()
        last = _pipeline_last_attempts.get(pipeline_key, 0)
        if last > 0 and (now - last) < _PIPELINE_RETRY_SECONDS:
            return None

        _pipeline_last_attempts[pipeline_key] = now

    # Build pipeline outside lock (may be slow)
    try:
        from ap_intelligence.ap_signal_pipeline import APSignalPipeline
        from ap_intelligence.ap_mode_config     import APModeConfig

        openai_key = os.getenv("OPENAI_API_KEY", "")
        intel_mode = "production" if mode == "LIVE" else "research"

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
            _pipelines[pipeline_key] = pipeline
            _pipeline_fail_counts[pipeline_key] = 0
        # Compatibility metadata is not production account authority.
        try:
            pipeline.account_equity_authority = "DIAGNOSTIC_ONLY"
            pipeline.contract_authority = False
        except Exception:
            pass
        log.info(
            f"intelligence_bridge: legacy pipeline ready for {client_key} | mode={intel_mode} "
            f"equity_diagnostic_state=PRESENT llm={'yes' if openai_key else 'rule-based'}"
        )
        return pipeline

    except Exception as e:
        with _pipeline_lock:
            _pipeline_fail_counts[pipeline_key] = _pipeline_fail_counts.get(pipeline_key, 0) + 1
            count = _pipeline_fail_counts[pipeline_key]
        remaining = _PIPELINE_MAX_FAILS - count
        log.warning(
            f"intelligence_bridge: init failed for {client_key} mode={mode} "
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


def _normalise_execution_mode(value: Any) -> str:
    """Accept only an explicit runtime mode; never infer PAPER/LIVE."""
    if not isinstance(value, str):
        return ""
    mode = value.strip().upper()
    return mode if mode in {"LIVE", "PAPER"} else ""


def _identity_sources(signal: dict) -> list[dict]:
    """Return bounded top-level/nested sources used for duplicate detection."""
    sources: list[dict] = [signal]
    for key in _IDENTITY_NESTED_KEYS:
        nested = signal.get(key)
        if isinstance(nested, dict):
            sources.append(nested)
            for child_key in ("identity", "quote", "market_data"):
                child = nested.get(child_key)
                if isinstance(child, dict):
                    sources.append(child)
    return sources


def _alias_values(signal: dict, aliases: tuple[str, ...]) -> list[Any]:
    values: list[Any] = []
    for source in _identity_sources(signal):
        for alias in aliases:
            if alias in source and source.get(alias) is not None:
                value = source.get(alias)
                if isinstance(value, str) and not value.strip():
                    continue
                values.append(value)
    return values


def _values_conflict(values: list[Any], *, normalise=None) -> bool:
    if len(values) < 2:
        return False
    normalise = normalise or (lambda value: str(value).strip())
    try:
        first = normalise(values[0])
        return any(normalise(value) != first for value in values[1:])
    except Exception:
        return True


def _one_alias(signal: dict, aliases: tuple[str, ...], *, normalise=None) -> tuple[Any, bool]:
    values = _alias_values(signal, aliases)
    return (values[0] if values else None, _values_conflict(values, normalise=normalise))


def _normalise_occ(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return re.sub(r"\s+", "", value.strip().upper())


def _parse_quote_timestamp(value: Any) -> Optional[datetime]:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        parsed_epoch = _safe_finite_float(value)
        if parsed_epoch is None:
            return None
        try:
            return datetime.fromtimestamp(parsed_epoch, tz=timezone.utc)
        except (OverflowError, OSError, ValueError):
            return None
    if not isinstance(value, str):
        return None
    raw = value.strip()
    if not raw:
        return None
    if raw.endswith("Z"):
        raw = raw[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(raw)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        # Treat a legacy naive timestamp as UTC for compatibility, while still
        # applying the same freshness bound. No authority is granted by this
        # compatibility interpretation.
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _account_equity_diagnostic() -> dict[str, Any]:
    """Describe environment equity without making it economic authority."""
    raw = os.getenv("ACCOUNT_EQUITY")
    if raw is None or not str(raw).strip():
        return {
            "state": "MISSING",
            "authority": "DIAGNOSTIC_ONLY",
        }
    parsed = _safe_finite_float(raw)
    if parsed is None or parsed <= 0:
        return {
            "state": "MALFORMED",
            "authority": "DIAGNOSTIC_ONLY",
        }
    return {
        "state": "PRESENT",
        "authority": "DIAGNOSTIC_ONLY",
    }


def _resolve_selected_contract_evidence(
    signal: dict,
    *,
    client_id: Any,
    execution_mode: Any,
) -> tuple[str, dict[str, Any], str]:
    """Validate selected-contract evidence without calling a legacy pipeline.

    Returns ``(state, evidence, reason)``. The state is one of:
    ``NOT_AVAILABLE_YET`` (no selected OCC), ``UNAVAILABLE`` (an attempted
    identity/quote was malformed or conflicting), or ``AVAILABLE_ADVISORY``.
    Even the last state is advisory in Phase 1; selected-contract authority is
    intentionally reserved for the later exact-contract intelligence phase.
    """
    if not isinstance(signal, dict):
        return "UNAVAILABLE", {}, "signal_not_a_dict"

    explicit_client = str(client_id).strip() if isinstance(client_id, str) else ""
    mode = _normalise_execution_mode(execution_mode)
    if not explicit_client:
        return "UNAVAILABLE", {"execution_mode": mode}, "client_id_missing"
    if not mode:
        return "UNAVAILABLE", {"client_id": explicit_client}, "execution_mode_missing_or_invalid"

    client_value, client_conflict = _one_alias(
        signal, ("client_id", "intel_client_id", "owner_client_id")
    )
    if client_conflict or (client_value is not None and str(client_value).strip() != explicit_client):
        return "UNAVAILABLE", {
            "client_id": explicit_client,
            "execution_mode": mode,
        }, "client_id_conflict"

    mode_value, mode_conflict = _one_alias(
        signal, ("execution_mode", "mode", "intel_mode")
    )
    if mode_conflict or (mode_value is not None and _normalise_execution_mode(mode_value) != mode):
        return "UNAVAILABLE", {
            "client_id": explicit_client,
            "execution_mode": mode,
        }, "execution_mode_conflict"

    signal_id, signal_id_conflict = _one_alias(
        signal, ("signal_id", "intel_signal_id")
    )
    canonical_signal_id, canonical_conflict = _one_alias(
        signal, ("canonical_signal_id", "canonical_id", "intel_canonical_signal_id")
    )
    local_order_id, local_order_conflict = _one_alias(
        signal, ("local_order_id", "entry_local_order_id", "intel_local_order_id")
    )
    if signal_id_conflict or canonical_conflict or local_order_conflict:
        return "UNAVAILABLE", {
            "client_id": explicit_client,
            "execution_mode": mode,
        }, "identity_alias_conflict"

    occ_value, occ_conflict = _one_alias(
        signal, _CONTRACT_ALIAS_KEYS, normalise=_normalise_occ
    )
    if occ_value is None:
        return "NOT_AVAILABLE_YET", {
            "client_id": explicit_client,
            "execution_mode": mode,
            "signal_id": str(signal_id or ""),
            "canonical_signal_id": str(canonical_signal_id or ""),
            "local_order_id": str(local_order_id or ""),
        }, "selected_occ_missing"
    if occ_conflict:
        return "UNAVAILABLE", {
            "client_id": explicit_client,
            "execution_mode": mode,
        }, "occ_alias_conflict"

    occ = _normalise_occ(occ_value)
    if not _OCC_RE.fullmatch(occ):
        return "UNAVAILABLE", {
            "client_id": explicit_client,
            "execution_mode": mode,
            "occ_symbol": occ,
        }, "occ_invalid"
    if not signal_id or not canonical_signal_id:
        return "UNAVAILABLE", {
            "client_id": explicit_client,
            "execution_mode": mode,
            "occ_symbol": occ,
        }, "signal_identity_missing"

    quote_ts, quote_ts_conflict = _one_alias(
        signal,
        (
            "quote_timestamp",
            "quote_ts",
            "quote_time",
            "observed_at",
            "quote_observed_at",
            "quote_observed_ts",
            "as_of",
        ),
    )
    source, source_conflict = _one_alias(
        signal,
        (
            "quote_source",
            "source",
            "data_source",
            "quote_provenance",
            "provenance",
            "provenance_source",
        ),
        normalise=lambda value: str(value).strip().casefold(),
    )
    if quote_ts_conflict:
        return "UNAVAILABLE", {"client_id": explicit_client, "execution_mode": mode, "occ_symbol": occ}, "quote_timestamp_conflict"
    if source_conflict:
        return "UNAVAILABLE", {"client_id": explicit_client, "execution_mode": mode, "occ_symbol": occ}, "source_conflict"
    parsed_ts = _parse_quote_timestamp(quote_ts)
    if parsed_ts is None:
        return "UNAVAILABLE", {"client_id": explicit_client, "execution_mode": mode, "occ_symbol": occ}, "quote_timestamp_missing_or_malformed"
    quote_age = (datetime.now(timezone.utc) - parsed_ts).total_seconds()
    if quote_age < -5.0 or quote_age > _INTEL_MAX_QUOTE_AGE_SECONDS:
        return "UNAVAILABLE", {
            "client_id": explicit_client,
            "execution_mode": mode,
            "occ_symbol": occ,
            "quote_age_seconds": round(quote_age, 3),
        }, "quote_stale_or_future"
    if source is None or not str(source).strip():
        return "UNAVAILABLE", {"client_id": explicit_client, "execution_mode": mode, "occ_symbol": occ}, "quote_source_missing"

    field_specs: dict[str, tuple[tuple[str, ...], int | None, float | None, float | None]] = {
        "bid": (("bid", "option_bid", "quote_bid", "selected_bid"), None, 0.0, None),
        "ask": (("ask", "option_ask", "quote_ask", "selected_ask"), None, 0.0, None),
        "spread": (("spread", "spread_pct", "bid_ask_spread_pct", "quote_spread_pct", "selected_spread"), None, 0.0, None),
        "delta": (("delta", "option_delta", "quote_delta", "selected_delta"), None, -1.0, 1.0),
        "open_interest": (("open_interest", "option_open_interest", "oi", "quote_open_interest"), 0, 0.0, None),
        "volume": (("volume", "option_volume", "daily_volume_options", "quote_volume"), 0, 0.0, None),
        "dte": (("dte", "days_to_expiry", "option_dte"), 0, 0.0, None),
    }
    parsed_fields: dict[str, Any] = {}
    for field_name, (aliases, integer_minimum, minimum, maximum) in field_specs.items():
        raw_value, conflict = _one_alias(signal, aliases)
        if conflict:
            return "UNAVAILABLE", {
                "client_id": explicit_client,
                "execution_mode": mode,
                "occ_symbol": occ,
            }, f"{field_name}_conflict"
        if integer_minimum is not None:
            parsed_value = _strict_integer(raw_value, minimum=integer_minimum)
        else:
            parsed_value = _safe_finite_float(raw_value)
            if parsed_value is not None:
                if minimum is not None and parsed_value < minimum:
                    parsed_value = None
                if maximum is not None and parsed_value > maximum:
                    parsed_value = None
        if parsed_value is None:
            return "UNAVAILABLE", {
                "client_id": explicit_client,
                "execution_mode": mode,
                "occ_symbol": occ,
            }, f"{field_name}_missing_or_malformed"
        parsed_fields[field_name] = parsed_value

    if parsed_fields["ask"] < parsed_fields["bid"]:
        return "UNAVAILABLE", {
            "client_id": explicit_client,
            "execution_mode": mode,
            "occ_symbol": occ,
        }, "quote_bid_ask_unordered"
    if parsed_fields["ask"] <= 0:
        return "UNAVAILABLE", {
            "client_id": explicit_client,
            "execution_mode": mode,
            "occ_symbol": occ,
        }, "quote_ask_non_positive"

    evidence = {
        "occ_symbol": occ,
        "client_id": explicit_client,
        "execution_mode": mode,
        "signal_id": str(signal_id).strip(),
        "canonical_signal_id": str(canonical_signal_id).strip(),
        "local_order_id": str(local_order_id or "").strip(),
        "quote_timestamp": quote_ts,
        "quote_age_seconds": round(quote_age, 3),
        "source": str(source).strip(),
        **parsed_fields,
    }
    return "AVAILABLE_ADVISORY", evidence, "selected_occ_evidence_valid_but_advisory"


def _advisory_gate(
    *,
    signal: dict,
    scanner_score: float,
    client_id: Any,
    execution_mode: Any,
    state: str,
    status: str,
    reason: str,
    evidence: Optional[dict[str, Any]] = None,
    available: bool = False,
) -> dict:
    """Return scanner-preserving metadata with no intelligence authority."""
    mode = _normalise_execution_mode(execution_mode)
    client = str(client_id).strip() if isinstance(client_id, str) else ""
    evidence = dict(evidence or {})
    return {
        "approved": True,
        "score": _rounded_score(scanner_score),
        "scanner_score": _rounded_score(scanner_score),
        "intel_score": None,
        "contracts": 0,
        "reasoning": f"{reason} | second_score_mode=observe_only",
        "intel_status": status,
        "_available": bool(available),
        "contract_quality_state": state,
        "contract_authority": False,
        "second_score_mode": "observe_only",
        "risk_detail": {},
        "client_id": client,
        "execution_mode": mode,
        "signal_id": str(signal.get("signal_id") or "").strip() if isinstance(signal, dict) else "",
        "canonical_signal_id": str(signal.get("canonical_signal_id") or "").strip() if isinstance(signal, dict) else "",
        "local_order_id": str(signal.get("local_order_id") or "").strip() if isinstance(signal, dict) else "",
        "contract_evidence": evidence,
        "account_equity_diagnostic": _account_equity_diagnostic(),
    }


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
    conf = _safe_finite_float(fund.get("confidence", 50), default=None)
    if conf is None:
        return 7
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
    action = str(result.get("action", "execute")).lower()
    _score_raw = result.get("score") if "score" in result else result.get("confidence")
    intel_score = _safe_finite_float(_score_raw, default=None)
    intel_score_valid = intel_score is not None
    # Missing/malformed intelligence is never converted into the scanner score
    # or a passing second score. Explicit numeric zero remains zero.
    intel_score = intel_score if intel_score is not None else 0.0
    score      = intel_score          # may be overridden below
    contracts  = _strict_integer(result.get("contracts"), minimum=0)
    contracts  = contracts if contracts is not None else 0
    reasoning  = str(result.get("reasoning") or "")
    risk       = result.get("risk_detail") or {}
    ticker     = str(result.get("ticker") or "?")

    fallback_parsed = _safe_finite_float(fallback_score, default=0.0)
    fallback_score = fallback_parsed if fallback_parsed is not None else 0.0

    # A producer that explicitly declares non-authoritative contract evidence
    # cannot use legacy action/risk text to veto or size an entry. Keep this
    # compatibility guard separate from old raw fixtures that predate the
    # explicit authority field.
    if "contract_authority" in result and result.get("contract_authority") is not True:
        state = str(result.get("contract_quality_state") or "UNAVAILABLE").upper()
        if state not in {"NOT_AVAILABLE_YET", "UNAVAILABLE", "AVAILABLE_ADVISORY"}:
            state = "UNAVAILABLE"
        status = (
            "CONTRACT_EVIDENCE_AVAILABLE_ADVISORY"
            if state == "AVAILABLE_ADVISORY"
            else "CONTRACT_EVIDENCE_UNAVAILABLE"
            if state == "NOT_AVAILABLE_YET"
            else "CONTRACT_EVIDENCE_INVALID"
        )
        return _advisory_gate(
            signal=result,
            scanner_score=fallback_score,
            client_id=result.get("client_id") or "",
            execution_mode=result.get("execution_mode") or "",
            state=state,
            status=status,
            reason="explicit_contract_authority_false",
            evidence=result.get("contract_evidence") if isinstance(result.get("contract_evidence"), dict) else {},
            available=state == "AVAILABLE_ADVISORY",
        )

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
        if (intel_score_valid
                and _is_data_skip
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
                "contracts":         contracts,
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
        "contracts":    contracts,
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


def run_intelligence_check(
    signal: dict,
    underlying_price: Optional[float] = None,
    client_id: str = "",
    execution_mode: str = "",
) -> dict:
    """Evaluate only production-shaped Gate G context on the active path.

    Gate G executes before contract selection. Without a selected OCC and an
    exact identity-bound, fresh quote, the legacy pipeline's estimated delta,
    spread, OI, volume, DTE, account equity, and ``run_quick`` score are not
    allowed to become admission authority. The scanner score remains the
    distinct effective score; intelligence is explicit observe-only metadata.

    Phase 1 deliberately does not run the legacy pipeline even when a selected
    OCC is present. It records valid selected-contract evidence as
    ``AVAILABLE_ADVISORY`` for the later exact-contract intelligence phase.
    """
    if not isinstance(signal, dict):
        return _advisory_gate(
            signal={},
            scanner_score=0.0,
            client_id=client_id,
            execution_mode=execution_mode,
            state="UNAVAILABLE",
            status="CONTRACT_EVIDENCE_INVALID",
            reason="signal_not_a_dict",
        )

    scanner_raw = signal.get("score") if "score" in signal else signal.get("ev_score")
    scanner_score = _safe_finite_float(scanner_raw, default=0.0)
    scanner_score = scanner_score if scanner_score is not None else 0.0

    state, evidence, reason = _resolve_selected_contract_evidence(
        signal,
        client_id=client_id,
        execution_mode=execution_mode,
    )
    if state == "NOT_AVAILABLE_YET":
        return _advisory_gate(
            signal=signal,
            scanner_score=scanner_score,
            client_id=client_id,
            execution_mode=execution_mode,
            state=state,
            status="CONTRACT_EVIDENCE_UNAVAILABLE",
            reason=reason,
            evidence=evidence,
            available=False,
        )

    if state != "AVAILABLE_ADVISORY":
        return _advisory_gate(
            signal=signal,
            scanner_score=scanner_score,
            client_id=client_id,
            execution_mode=execution_mode,
            state=state,
            status="CONTRACT_EVIDENCE_INVALID",
            reason=reason,
            evidence=evidence,
            available=False,
        )

    return _advisory_gate(
        signal=signal,
        scanner_score=scanner_score,
        client_id=client_id,
        execution_mode=execution_mode,
        state=state,
        status="CONTRACT_EVIDENCE_AVAILABLE_ADVISORY",
        reason=reason,
        evidence=evidence,
        available=True,
    )
