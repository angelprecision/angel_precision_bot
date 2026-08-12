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
import datetime as _dt
import math
import re
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
# - LIVE defaults fail-closed: intel unavailable/timeout/error/skip/low-score/risk-veto blocks.
# - Paper/research may opt into 1-contract data-collection overrides.
# - To intentionally collect data in LIVE, set INTEL_DATA_COLLECTION_OVERRIDE=1.
_AP_MODE = (os.getenv("AP_MODE") or os.getenv("BOT_MODE") or "paper").lower()
_INTEL_DATA_COLLECTION_OVERRIDE = os.getenv("INTEL_DATA_COLLECTION_OVERRIDE", "0") == "1"
_INTEL_FAIL_OPEN_UNAVAILABLE = os.getenv("INTEL_FAIL_OPEN_UNAVAILABLE", "0") == "1"
_INTEL_ENFORCE_RISK_VETO = os.getenv("INTEL_ENFORCE_RISK_VETO", "1") != "0"
_INTEL_IS_LIVE = _AP_MODE == "live"

# Gate G input provenance.  A value can be useful for diagnostics without
# being eligible to create execution authority.
PRODUCTION_EXACT = "PRODUCTION_EXACT"
PRODUCTION_DERIVED = "PRODUCTION_DERIVED"
ESTIMATED_ADVISORY = "ESTIMATED_ADVISORY"
DEFAULT_ADVISORY = "DEFAULT_ADVISORY"
UNAVAILABLE = "UNAVAILABLE"
NOT_AVAILABLE_YET = "NOT_AVAILABLE_YET"

_OCC_RE = re.compile(r"^[A-Z0-9.]{1,10}\d{6}[CP]\d{8}$")


def _positive_finite_env_float(name: str, default: float) -> Optional[float]:
    """Read a positive finite authority bound without trusting bad config."""
    raw = os.getenv(name, str(default))
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return value


_MAX_SELECTED_QUOTE_AGE_SECONDS = _positive_finite_env_float(
    "GATE_G_MAX_SELECTED_QUOTE_AGE_SECONDS", 120.0
)
# Gate G does not infer selected-contract authority from a generic broker label,
# a signal timestamp, or a caller-provided fallback.  This token is reserved
# for the future selector/revalidator contract; it is not itself a producer
# attestation.  The current deferred materializer does not emit a
# producer-bound Gate G attestation, so selected-contract metadata remains
# advisory until that later contract is frozen.
SELECTED_CONTRACT_TRUSTED_SOURCE = "selector_revalidated_production"
# Keep this closed set empty until a real selector/revalidator integration
# supplies producer-bound evidence.  A raw string in a signal or metadata
# payload must never be sufficient to activate PRODUCTION_EXACT authority.
TRUSTED_SELECTED_CONTRACT_QUOTE_SOURCES = frozenset()


def _safe_float(value: Any, *, field: str = "value") -> tuple[Optional[float], str]:
    """Parse an external scalar without treating numeric zero as missing."""
    if value is None:
        return None, "missing"
    if isinstance(value, bool):
        return None, f"{field}_bool_not_numeric"
    if isinstance(value, str) and not value.strip():
        return None, "blank"
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None, f"{field}_unparseable"
    if not math.isfinite(parsed):
        return None, f"{field}_non_finite"
    return parsed, "ok"


def _safe_int(value: Any, *, field: str = "value") -> tuple[Optional[int], str]:
    """Parse an external integer while preserving explicit zero."""
    if value is None:
        return None, "missing"
    if isinstance(value, bool):
        return None, f"{field}_bool_not_integer"
    if isinstance(value, str) and not value.strip():
        return None, "blank"
    try:
        parsed_float = float(value)
        if not math.isfinite(parsed_float) or not parsed_float.is_integer():
            return None, f"{field}_not_integral"
        return int(parsed_float), "ok"
    except (TypeError, ValueError):
        return None, f"{field}_unparseable"


def _first_present(mapping: Any, *keys: str) -> tuple[Any, Optional[str]]:
    if not isinstance(mapping, dict):
        return None, None
    for key in keys:
        if key in mapping and mapping[key] is not None:
            return mapping[key], key
    return None, None


def _normalize_mode(value: Any) -> str:
    raw = str(value or "").strip().upper()
    if raw in {"LIVE", "PRODUCTION"}:
        return "LIVE"
    if raw in {"PAPER", "RESEARCH", "SIM", "SIMULATION"}:
        return "PAPER"
    return ""


def _normalize_occ(value: Any) -> str:
    contract = "".join(str(value or "").upper().split())
    return contract if _OCC_RE.fullmatch(contract) else ""


def _parse_timestamp(value: Any) -> Optional[_dt.datetime]:
    if value is None:
        return None
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            return _dt.datetime.fromtimestamp(float(value), tz=_dt.timezone.utc)
        except (TypeError, ValueError, OSError, OverflowError):
            return None
    raw = str(value).strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = _dt.datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_dt.timezone.utc)
        return parsed.astimezone(_dt.timezone.utc)
    except ValueError:
        return None


def _parse_authoritative_quote_timestamp(value: Any) -> Optional[_dt.datetime]:
    """Parse selected-quote time only when its timezone is explicit."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        try:
            numeric = float(value)
            if not math.isfinite(numeric):
                return None
            return _dt.datetime.fromtimestamp(numeric, tz=_dt.timezone.utc)
        except (TypeError, ValueError, OSError, OverflowError):
            return None

    raw = str(value).strip()
    if not raw:
        return None
    try:
        if raw.endswith("Z"):
            raw = raw[:-1] + "+00:00"
        parsed = _dt.datetime.fromisoformat(raw)
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(_dt.timezone.utc)
    except (TypeError, ValueError):
        return None


def _signal_client_id(signal: dict, client_id: str) -> str:
    runtime_client = str(client_id or "").strip()
    gate_client = signal.get("_gate_client_id")
    if gate_client is not None and str(gate_client).strip():
        return str(gate_client).strip()
    # A non-default argument is the caller's canonical runtime identity.  Only
    # legacy direct callers that omit it may fall back to payload metadata.
    if runtime_client and runtime_client.lower() != "default":
        return runtime_client
    for key in ("client_id", "client_email"):
        raw = signal.get(key)
        if raw is not None and str(raw).strip():
            return str(raw).strip()
    return runtime_client


def _signal_execution_mode(signal: dict) -> str:
    for key in ("_gate_execution_mode", "execution_mode", "mode"):
        mode = _normalize_mode(signal.get(key))
        if mode:
            return mode
    return "LIVE" if _INTEL_IS_LIVE else "PAPER"


def _iter_candidate_contract_containers(signal: dict):
    """Yield source containers and their nested selected-contract payloads."""
    if not isinstance(signal, dict):
        return

    plan_metadata = []
    for plan_key in ("_approved_plan", "approved_plan"):
        plan = signal.get(plan_key)
        if isinstance(plan, dict):
            plan_metadata.append(plan.get("metadata"))
        else:
            plan_metadata.append(getattr(plan, "metadata", None))

    for container in (
        signal,
        signal.get("metadata"),
        signal.get("selector_metadata"),
        *plan_metadata,
    ):
        if not isinstance(container, dict):
            continue
        nested = container.get("selected_contract")
        if isinstance(nested, dict) or any(
            key in container
            for key in (
                "selected_contract", "contract_symbol", "occ_symbol", "option_symbol",
                "selected_dte", "selected_bid", "selected_ask",
            )
        ):
            yield container, nested if isinstance(nested, dict) else None


def _candidate_contract_records(signal: dict):
    records = []
    for container, nested in _iter_candidate_contract_containers(signal):
        if isinstance(nested, dict):
            merged = dict(container)
            merged.update(nested)
        else:
            merged = dict(container)
        records.append((merged, container, nested))
    return records


def _candidate_contract_sources(signal: dict) -> list[dict]:
    """Return merged candidate payloads for callers that need a flat view."""
    return [candidate for candidate, _, _ in _candidate_contract_records(signal)]


def _identity_values(parts, keys, normalizer):
    """Collect normalized immutable-identity values across duplicated fields."""
    values: set[str] = set()
    invalid = False
    for mapping in parts:
        if not isinstance(mapping, dict):
            continue
        for key in keys:
            if key not in mapping:
                continue
            raw = mapping.get(key)
            if raw is None or (key == "selected_contract" and isinstance(raw, dict)):
                continue
            normalized = normalizer(raw)
            if normalized:
                values.add(normalized)
            else:
                invalid = True
    return values, invalid


def _candidate_contract_identity_claim(container: dict, nested: dict | None):
    """Build one complete immutable identity claim for a source container.

    The outer wrapper and nested selected-contract object are one source
    claim, but every duplicated identity field is retained for conflict
    detection.  This prevents ``dict.update`` precedence from deciding money
    authority when both layers disagree.
    """
    parts = [container]
    if isinstance(nested, dict):
        parts.append(nested)

    contract_values, contract_invalid = _identity_values(
        parts,
        ("contract_symbol", "occ_symbol", "option_symbol", "selected_contract"),
        _normalize_occ,
    )
    has_contract_claim = any(
        isinstance(mapping, dict)
        and any(key in mapping and mapping.get(key) is not None for key in (
            "selected_contract", "contract_symbol", "occ_symbol", "option_symbol",
        ))
        for mapping in parts
    )
    if not has_contract_claim:
        return None

    client_values, client_invalid = _identity_values(
        parts,
        ("selected_client_id", "client_id", "client_email"),
        lambda value: str(value).strip(),
    )
    mode_values, mode_invalid = _identity_values(
        parts,
        ("selected_execution_mode", "execution_mode", "mode"),
        _normalize_mode,
    )
    signal_values, signal_invalid = _identity_values(
        parts,
        ("canonical_signal_id", "originating_signal_id", "signal_id"),
        lambda value: str(value).strip(),
    )

    complete = (
        len(contract_values) == 1
        and len(client_values) == 1
        and len(mode_values) == 1
        and len(signal_values) == 1
        and not any((contract_invalid, client_invalid, mode_invalid, signal_invalid))
    )
    return {
        "contract": next(iter(contract_values), ""),
        "client": next(iter(client_values), ""),
        "mode": next(iter(mode_values), ""),
        "signal": next(iter(signal_values), ""),
        "complete": complete,
        "identity_conflict": any(
            len(values) > 1
            for values in (contract_values, client_values, mode_values, signal_values)
        ),
        "contract_values": sorted(contract_values),
    }


def _candidate_contract_identity_claims(signal: dict) -> list[dict]:
    claims: list[dict] = []
    for container, nested in _iter_candidate_contract_containers(signal):
        claim = _candidate_contract_identity_claim(container, nested)
        if claim is not None:
            claims.append(claim)
    return claims


def _selected_contract_source_values(
    container: dict,
    nested: Optional[dict],
) -> set[str]:
    """Collect every explicit source alias from one contract claim."""
    values: set[str] = set()
    for mapping in (container, nested):
        if not isinstance(mapping, dict):
            continue
        for key in ("source", "quote_source", "provider"):
            if key not in mapping or mapping[key] is None:
                continue
            values.add(str(mapping[key]).strip().lower())
    return values


def _extract_selected_contract_evidence(
    signal: dict,
    *,
    client_id: str,
    execution_mode: str,
) -> dict:
    """Resolve a current, identity-bound, fresh selected-contract snapshot.

    Gate G never treats a loose ``contract``/quote field as selected evidence.
    A candidate must carry the current client, mode, signal identity, a
    timestamped quote, and all contract-quality dimensions.  Any mismatch
    quarantines the candidate and leaves contract quality NOT_AVAILABLE_YET.
    """
    # The runtime client argument is the authority.  A payload-level client
    # field is useful for diagnostics, but must not silently re-bind a quote to
    # another account when the caller supplied the canonical runtime identity.
    expected_client = str(client_id or "").strip() or _signal_client_id(signal, "")
    expected_mode = _normalize_mode(execution_mode)
    expected_signal = str(signal.get("signal_id") or "").strip()
    expected_canonical = str(signal.get("canonical_signal_id") or "").strip()
    records = _candidate_contract_records(signal)
    if not records:
        return {
            "exists": False,
            "authoritative": False,
            "classification": NOT_AVAILABLE_YET,
            "reason": "no_selected_occ_contract",
        }

    # A retry/queue payload can contain multiple claims for the same OCC.  The
    # entire immutable identity tuple must agree before any one candidate can
    # become authority; a valid Jason/LIVE candidate must not hide a Jose/PAPER
    # duplicate merely because the OCC symbol matches.
    identity_claims = _candidate_contract_identity_claims(signal)
    claimed_contracts = sorted({
        contract
        for claim in identity_claims
        for contract in claim.get("contract_values", [])
        if contract
    })
    identity_tuples = {
        (
            claim.get("contract"),
            claim.get("client"),
            claim.get("mode"),
            claim.get("signal"),
        )
        for claim in identity_claims
    }
    if (
        not identity_claims
        or any(claim.get("identity_conflict") for claim in identity_claims)
        or len(claimed_contracts) > 1
        or len(identity_tuples) > 1
        or (
            len(identity_claims) > 1
            and any(not claim.get("complete") for claim in identity_claims)
        )
    ):
        return {
            "exists": False,
            "authoritative": False,
            "classification": UNAVAILABLE,
            "reason": "selected_contract_identity_conflict",
            "claimed_contracts": claimed_contracts,
        }

    # Preserve the concrete identity diagnostic before checking provenance.
    # A source problem must not hide a client, mode, signal, or OCC mismatch.
    for claim in identity_claims:
        if not claim.get("contract"):
            return {
                "exists": False,
                "authoritative": False,
                "classification": UNAVAILABLE,
                "reason": "selected_contract_not_occ",
                "claimed_contracts": claimed_contracts,
            }
        if claim.get("client") != expected_client or not expected_client:
            return {
                "exists": False,
                "authoritative": False,
                "classification": UNAVAILABLE,
                "reason": "selected_contract_client_mismatch",
                "claimed_contracts": claimed_contracts,
            }
        if claim.get("mode") != expected_mode or not expected_mode:
            return {
                "exists": False,
                "authoritative": False,
                "classification": UNAVAILABLE,
                "reason": "selected_contract_execution_mode_mismatch",
                "claimed_contracts": claimed_contracts,
            }
        candidate_signal = claim.get("signal") or ""
        if (
            not candidate_signal
            or (expected_canonical and candidate_signal != expected_canonical)
            or (not expected_canonical and expected_signal and candidate_signal != expected_signal)
            or (not expected_canonical and not expected_signal)
        ):
            return {
                "exists": False,
                "authoritative": False,
                "classification": UNAVAILABLE,
                "reason": "selected_contract_signal_identity_mismatch",
                "claimed_contracts": claimed_contracts,
            }

    # A valid alias cannot launder a malformed duplicate identity field from
    # the same source container.  The claim must be complete, not merely
    # usable after dictionary precedence has selected one value.
    if any(claim.get("complete") is not True for claim in identity_claims):
        return {
            "exists": False,
            "authoritative": False,
            "classification": UNAVAILABLE,
            "reason": "selected_contract_identity_conflict",
            "claimed_contracts": claimed_contracts,
        }

    # Source provenance is part of the authority contract, not a diagnostic
    # preference.  Every explicit alias and every duplicate candidate must
    # agree on the same closed trusted producer token.  Otherwise insertion
    # order could select a trusted-looking dictionary while silently ignoring
    # contradictory source metadata.
    source_claims = [
        _selected_contract_source_values(container, nested)
        for _, container, nested in records
    ]
    claimed_quote_sources = sorted({
        source
        for values in source_claims
        for source in values
    })
    provenance_conflict = (
        any(len(values) > 1 for values in source_claims)
        or len(claimed_quote_sources) > 1
    )
    if provenance_conflict:
        return {
            "exists": False,
            "authoritative": False,
            "classification": UNAVAILABLE,
            "reason": "selected_contract_quote_source_conflict",
            "claimed_contracts": claimed_contracts,
            "claimed_quote_sources": claimed_quote_sources,
        }
    quote_source = claimed_quote_sources[0] if len(claimed_quote_sources) == 1 else None
    # Source aliases are still resolved for diagnostics and conflict detection,
    # but they cannot establish authority.  Only a future producer-bound
    # attestation may make this true; no current Gate G caller supplies one.
    source_is_trusted = bool(
        quote_source
        and quote_source in TRUSTED_SELECTED_CONTRACT_QUOTE_SOURCES
        and all(values == {quote_source} for values in source_claims)
    )

    last_reason = "selected_contract_unavailable"
    for candidate, source_container, nested_contract in records:
        # ``candidate`` carries the selected-contract fields from either a
        # flat producer container or its nested payload.  It intentionally
        # excludes generic ``timestamp`` by key selection below, so an outer
        # signal timestamp cannot attest to an option quote.
        quote_payload = candidate
        if isinstance(nested_contract, dict):
            contract_raw, contract_key = _first_present(
                nested_contract, "contract_symbol", "occ_symbol", "option_symbol"
            )
            contract_key = f"selected_contract.{contract_key}" if contract_key else None
        else:
            contract_raw, contract_key = _first_present(
                candidate, "contract_symbol", "occ_symbol", "option_symbol", "selected_contract"
            )
        contract = "".join(str(contract_raw or "").upper().split())
        if not contract or not _OCC_RE.fullmatch(contract):
            last_reason = "selected_contract_not_occ"
            continue

        candidate_client, _ = _first_present(
            candidate, "selected_client_id", "client_id", "client_email"
        )
        if str(candidate_client or "").strip() != expected_client or not expected_client:
            last_reason = "selected_contract_client_mismatch"
            continue

        candidate_mode, _ = _first_present(
            candidate, "selected_execution_mode", "execution_mode", "mode"
        )
        if _normalize_mode(candidate_mode) != expected_mode or not expected_mode:
            last_reason = "selected_contract_execution_mode_mismatch"
            continue

        candidate_signal, _ = _first_present(
            candidate, "canonical_signal_id", "originating_signal_id", "signal_id"
        )
        candidate_signal = str(candidate_signal or "").strip()
        if (
            not candidate_signal
            or (expected_canonical and candidate_signal != expected_canonical)
            or (not expected_canonical and expected_signal and candidate_signal != expected_signal)
            or (not expected_canonical and not expected_signal)
        ):
            last_reason = "selected_contract_signal_identity_mismatch"
            continue

        if _selected_contract_source_values(
            source_container, nested_contract
        ) != {quote_source}:
            last_reason = "selected_contract_quote_source_untrusted"
            continue

        # Only an explicit quote observation timestamp from the selected
        # contract payload is eligible.  A generic outer signal timestamp is
        # not quote provenance and must not launder stale option data.
        timestamp_payload = (
            nested_contract if isinstance(nested_contract, dict) else source_container
        )
        quote_ts_raw, _ = _first_present(
            timestamp_payload,
            "quote_ts", "quote_timestamp", "quote_observed_at", "observed_at",
        )
        quote_ts = _parse_authoritative_quote_timestamp(quote_ts_raw)
        if quote_ts is None:
            last_reason = "selected_contract_quote_timestamp_missing_or_invalid"
            continue
        max_quote_age_seconds = _MAX_SELECTED_QUOTE_AGE_SECONDS
        if (
            max_quote_age_seconds is None
            or not math.isfinite(float(max_quote_age_seconds))
            or float(max_quote_age_seconds) <= 0
        ):
            last_reason = "selected_contract_quote_freshness_config_invalid"
            continue
        age_seconds = (_dt.datetime.now(_dt.timezone.utc) - quote_ts).total_seconds()
        if age_seconds < -5 or age_seconds > float(max_quote_age_seconds):
            last_reason = "selected_contract_quote_stale"
            continue

        bid_raw, _ = _first_present(quote_payload, "selected_bid", "bid", "option_bid")
        ask_raw, _ = _first_present(quote_payload, "selected_ask", "ask", "option_ask")
        bid, bid_status = _safe_float(bid_raw, field="bid")
        ask, ask_status = _safe_float(ask_raw, field="ask")
        if bid is None or ask is None or bid < 0 or ask <= 0 or ask < bid:
            last_reason = f"selected_contract_quote_invalid:{bid_status}:{ask_status}"
            continue
        mid = (bid + ask) / 2.0
        if mid <= 0:
            last_reason = "selected_contract_quote_mid_invalid"
            continue

        spread_raw, _ = _first_present(quote_payload, "selected_spread_pct", "spread_pct")
        spread, spread_status = _safe_float(spread_raw, field="spread_pct")
        if spread is None:
            spread = (ask - bid) / mid
        if spread < 0:
            last_reason = f"selected_contract_spread_invalid:{spread_status}"
            continue

        delta_raw, _ = _first_present(quote_payload, "selected_delta", "delta", "option_delta")
        oi_raw, _ = _first_present(
            quote_payload, "selected_open_interest", "open_interest", "oi"
        )
        volume_raw, _ = _first_present(
            quote_payload, "selected_volume", "volume", "option_volume"
        )
        dte_raw, _ = _first_present(quote_payload, "selected_dte", "dte")
        delta, delta_status = _safe_float(delta_raw, field="delta")
        open_interest, oi_status = _safe_int(oi_raw, field="open_interest")
        volume, volume_status = _safe_int(volume_raw, field="volume")
        dte, dte_status = _safe_int(dte_raw, field="dte")

        if dte is None:
            expiration_raw, _ = _first_present(
                quote_payload, "selected_expiration", "expiration", "expiration_date"
            )
            try:
                expiration = _dt.date.fromisoformat(str(expiration_raw)[:10])
                dte = (expiration - _dt.date.today()).days
                dte_status = "derived_from_expiration"
            except (TypeError, ValueError):
                dte = None
        if any(value is None for value in (delta, open_interest, volume, dte)):
            last_reason = (
                "selected_contract_quality_field_missing_or_invalid:"
                f"{delta_status}:{oi_status}:{volume_status}:{dte_status}"
            )
            continue

        if not source_is_trusted:
            last_reason = "selected_contract_quote_source_untrusted"
            continue

        return {
            "exists": True,
            "authoritative": True,
            "classification": PRODUCTION_EXACT,
            "source": quote_source,
            "source_ts": quote_ts.isoformat(),
            "quote_age_seconds": round(max(0.0, age_seconds), 3),
            "contract_symbol": contract,
            "contract_key": contract_key,
            "client_id": expected_client,
            "execution_mode": expected_mode,
            "signal_id": candidate_signal,
            "values": {
                "bid": bid,
                "ask": ask,
                "option_premium": ask,
                "spread_pct": spread,
                "option_delta": delta,
                "open_interest": open_interest,
                "option_volume": volume,
                "dte": dte,
            },
        }

    return {
        "exists": False,
        "authoritative": False,
        "classification": UNAVAILABLE,
        "reason": last_reason,
    }


def _provenance_entry(*, value: Any, source: str, classification: str,
                      authoritative: bool, source_ts: Any = None,
                      reason: str = "") -> dict:
    return {
        "value": value,
        "source": source,
        "source_ts": source_ts,
        "classification": classification,
        "authoritative": bool(authoritative),
        "reason": reason,
    }


def _build_gate_inputs(
    signal: dict,
    *,
    score: float,
    underlying_price: float,
    client_id: str,
    execution_mode: str,
    contract_evidence: dict,
) -> dict:
    """Normalize Gate G inputs and retain the authority decision for each."""
    evidence_exists = bool(contract_evidence.get("exists") is True)
    evidence_values = contract_evidence.get("values") if evidence_exists else {}
    evidence_values = evidence_values if isinstance(evidence_values, dict) else {}
    provenance: dict[str, dict] = {}
    raw_source_ts, _ = _first_present(
        signal, "source_ts", "observed_at", "timestamp", "signal_timestamp"
    )
    parsed_source_ts = _parse_timestamp(raw_source_ts)
    source_ts = parsed_source_ts.isoformat() if parsed_source_ts else raw_source_ts

    raw_dte, dte_key = _first_present(signal, "dte")
    parsed_dte, dte_status = _safe_int(raw_dte, field="dte")
    if evidence_exists:
        dte = evidence_values.get("dte")
        provenance["dte"] = _provenance_entry(
            value=dte,
            source="selected_contract",
            source_ts=contract_evidence.get("source_ts"),
            classification=PRODUCTION_EXACT,
            authoritative=True,
            reason="identity-bound selected contract and fresh quote",
        )
    elif parsed_dte is not None:
        # A signal-level DTE is useful for setup diagnostics, but without a
        # selected OCC contract it cannot authorize contract quality.
        dte = parsed_dte
        provenance["dte"] = _provenance_entry(
            value=dte,
            source=f"signal.{dte_key}",
            source_ts=source_ts,
            classification=PRODUCTION_DERIVED,
            authoritative=False,
            reason="signal DTE is not selected-contract authority",
        )
    else:
        dte = 1
        provenance["dte"] = _provenance_entry(
            value=dte,
            source="gate_g_default",
            classification=DEFAULT_ADVISORY,
            authoritative=False,
            reason=(
                "DTE missing" if raw_dte is None
                else f"DTE malformed ({dte_status})"
            ),
        )

    raw_atr, atr_key = _first_present(signal, "atr_value", "atr")
    atr_value, atr_status = _safe_float(raw_atr, field="atr_value")
    if atr_value is None:
        provenance["atr_value"] = _provenance_entry(
            value=None,
            source="pipeline_default",
            source_ts=source_ts,
            classification=DEFAULT_ADVISORY,
            authoritative=False,
            reason=("ATR missing" if raw_atr is None else f"ATR malformed ({atr_status})"),
        )
    else:
        provenance["atr_value"] = _provenance_entry(
            value=atr_value,
            source=f"signal.{atr_key}",
            source_ts=source_ts,
            classification=PRODUCTION_DERIVED,
            authoritative=False,
            reason="underlying setup context; not contract authority",
        )

    if evidence_exists:
        contract_values = {
            "option_premium": evidence_values.get("option_premium"),
            "option_delta": evidence_values.get("option_delta"),
            "spread_pct": evidence_values.get("spread_pct"),
            "open_interest": evidence_values.get("open_interest"),
            "option_volume": evidence_values.get("option_volume"),
        }
        for name, value in contract_values.items():
            provenance[name] = _provenance_entry(
                value=value,
                source="selected_contract",
                source_ts=contract_evidence.get("source_ts"),
                classification=PRODUCTION_EXACT,
                authoritative=True,
                reason="identity-bound selected contract and fresh quote",
            )
        option_premium = contract_values["option_premium"]
        option_delta = contract_values["option_delta"]
        spread_pct = contract_values["spread_pct"]
        open_interest = contract_values["open_interest"]
        option_volume = contract_values["option_volume"]
    else:
        # Do not consume loose/stale contract-like fields before selector.  The
        # pipeline receives only documented defaults, all explicitly advisory.
        option_premium = None
        option_delta = 0.45
        spread_pct = 0.06
        open_interest = 500
        option_volume = 100
        for name, value in (
            ("option_premium", None),
            ("option_delta", option_delta),
            ("spread_pct", spread_pct),
            ("open_interest", open_interest),
            ("option_volume", option_volume),
        ):
            provenance[name] = _provenance_entry(
                value=value,
                source="gate_g_default",
                classification=DEFAULT_ADVISORY,
                authoritative=False,
                reason=(
                    "selected OCC contract and fresh quote are not available"
                ),
            )

    env_equity_raw = os.getenv("ACCOUNT_EQUITY")
    env_equity, env_equity_status = _safe_float(env_equity_raw, field="ACCOUNT_EQUITY")
    provenance["account_equity"] = _provenance_entry(
        value=env_equity,
        source="environment.ACCOUNT_EQUITY",
        classification=DEFAULT_ADVISORY,
        authoritative=False,
        reason=(
            "environment equity is never canonical account authority"
            if env_equity is not None
            else f"environment equity unavailable ({env_equity_status})"
        ),
    )
    provenance["account_state"] = _provenance_entry(
        value=None,
        source="MasterControl.snapshot",
        classification=UNAVAILABLE,
        authoritative=False,
        reason="Gate G did not receive a canonical account snapshot",
    )
    provenance["scanner_score"] = _provenance_entry(
        value=score,
        source="scanner_signal",
        classification=PRODUCTION_EXACT,
        authoritative=True,
        reason="scanner score remains separately identified admission input",
    )

    gate_context = {
        "client_id": str(client_id or "").strip() or None,
        "execution_mode": _normalize_mode(execution_mode) or None,
        "canonical_signal_id": str(
            signal.get("canonical_signal_id") or signal.get("signal_id") or ""
        ).strip() or None,
        "source_ts": source_ts,
        "dte_raw": raw_dte,
        "dte_resolved": dte,
    }
    return {
        "dte": dte,
        "atr_value": atr_value,
        "option_premium": option_premium,
        "option_delta": option_delta,
        "spread_pct": spread_pct,
        "open_interest": open_interest,
        "option_volume": option_volume,
        "contract_quality_authoritative": evidence_exists,
        "account_state_authoritative": False,
        "input_provenance": provenance,
        "contract_evidence": contract_evidence,
        "gate_context": gate_context,
    }

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

_STRUCTURED_RISK_APPROVAL_CODES = frozenset({
    "APPROVED",
    "APPROVED_WITH_REGIME_MISMATCH",
    "ADVISORY_DATA_UNAVAILABLE",
    "VIX_UNAVAILABLE",
})


def _diagnostics_from_risk(risk_detail: Any) -> dict:
    if not isinstance(risk_detail, dict):
        return {}
    diagnostics = risk_detail.get("gate_diagnostics")
    if isinstance(diagnostics, dict):
        return dict(diagnostics)
    return {}


def _block_gate(*, status: str, score: float, reasoning: str, risk_detail=None,
                price_data_stub: bool = False, canonical_reason_code: str = "") -> dict:
    parsed_score, _ = _safe_float(score, field="score")
    effective_score = parsed_score if parsed_score is not None else 0.0
    diagnostics = _diagnostics_from_risk(risk_detail)
    diagnostics.update({
        "intel_status": status,
        "canonical_admission_reason_code": canonical_reason_code or status,
        "legacy_intel_score": round(effective_score, 1),
    })
    return {
        "approved": False,
        "score": round(effective_score, 1),
        "contracts": 0,
        "reasoning": reasoning,
        "intel_status": status,
        "intel_score": round(effective_score, 1) if score is not None else None,
        "risk_detail": risk_detail or {},
        "price_data_stub": price_data_stub,
        "gate_diagnostics": diagnostics,
    }

def _collect_gate(*, status: str, score: float, reasoning: str, risk_detail=None,
                  price_data_stub: bool = False, canonical_reason_code: str = "") -> dict:
    parsed_score, _ = _safe_float(score, field="score")
    effective_score = parsed_score if parsed_score is not None else 0.0
    diagnostics = _diagnostics_from_risk(risk_detail)
    diagnostics.update({
        "intel_status": status,
        "canonical_admission_reason_code": canonical_reason_code or status,
        "legacy_intel_score": round(effective_score, 1),
    })
    return {
        "approved": True,
        "score": round(effective_score, 1),  # honest raw score — 1-contract cap is the real guard
        "contracts": 1,
        "reasoning": reasoning,
        "intel_status": status,
        "intel_score": round(effective_score, 1) if score is not None else None,
        "risk_detail": risk_detail or {},
        "price_data_stub": price_data_stub,
        "gate_diagnostics": diagnostics,
    }

# FIX 4: Retry logic — transient errors don't permanently disable intel
# HIGH-002: Per-client pipeline state — prevents one client's failures from
# disabling intelligence for all clients sharing the process.
_PIPELINE_RETRY_SECONDS: float = float(os.getenv("INTEL_RETRY_SECONDS", "120.0"))
_PIPELINE_MAX_FAILS:     int   = int(os.getenv("INTEL_MAX_FAILS", "5"))

_pipelines:              dict[str, object] = {}   # client_id + mode → pipeline
_pipeline_fail_counts:   dict[str, int]    = {}   # client_id + mode → fail count
_pipeline_last_attempts: dict[str, float]  = {}   # client_id + mode → monotonic time
_pipeline_lock = threading.Lock()


def _pipeline_key(client_id: str, execution_mode: str) -> str:
    return f"{str(client_id or 'default').strip()}:{_normalize_mode(execution_mode) or 'UNKNOWN'}"


def _get_pipeline(client_id: str = "default", execution_mode: str = "PAPER"):
    pipeline_key = _pipeline_key(client_id, execution_mode)
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

        equity_raw = os.getenv("ACCOUNT_EQUITY")
        equity, _ = _safe_float(equity_raw, field="ACCOUNT_EQUITY")
        if equity is None:
            equity = 25000.0
        openai_key = os.getenv("OPENAI_API_KEY", "")
        ap_mode    = "live" if _normalize_mode(execution_mode) == "LIVE" else "paper"
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
            _pipelines[pipeline_key] = pipeline
            _pipeline_fail_counts[pipeline_key] = 0
        log.info(
            f"intelligence_bridge: pipeline ready for {client_id} | execution_mode={_normalize_mode(execution_mode) or 'UNKNOWN'} "
            f"intel_mode={intel_mode} account_equity_source=environment_advisory "
            f"equity=${equity:,.0f} llm={'yes' if openai_key else 'rule-based'}"
        )
        return pipeline

    except Exception as e:
        with _pipeline_lock:
            _pipeline_fail_counts[pipeline_key] = _pipeline_fail_counts.get(pipeline_key, 0) + 1
            count = _pipeline_fail_counts[pipeline_key]
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

        max_contracts, contracts_status = _safe_int(
            risk.get("max_contracts"), field="max_contracts"
        )
        max_position_usd, position_status = _safe_float(
            risk.get("max_position_usd"), field="max_position_usd"
        )
        if max_contracts is None or max_position_usd is None:
            return False, (
                "malformed structured approval sizing: "
                f"max_contracts={risk.get('max_contracts')!r} "
                f"max_position_usd={risk.get('max_position_usd')!r} "
                f"({contracts_status}, {position_status})"
            )

        if max_contracts < 1:
            return False, (
                "structured approval has no executable size: "
                f"max_contracts={max_contracts} "
                f"max_position_usd={max_position_usd}"
            )

        # ADVISORY_DATA_UNAVAILABLE is deliberately not an executable sizing
        # approval.  Gate G only needs a structurally valid non-veto result so
        # Master Control can apply canonical account/selector sizing later.
        # Gate G may carry a structurally approved advisory result while the
        # canonical account snapshot is still owned by Master Control.  A zero
        # local dollar cap is not favorable sizing authority in that path; it
        # is an explicit signal to let the canonical final gate size later.
        account_state_advisory = risk.get("account_state_authoritative") is False
        if (
            max_position_usd <= 0
            and reason_code != "ADVISORY_DATA_UNAVAILABLE"
            and not account_state_advisory
        ):
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
    action = str(result.get("action", "execute")).lower()
    fallback_parsed, _ = _safe_float(fallback_score, field="fallback_score")
    fallback_score = fallback_parsed if fallback_parsed is not None else 0.0
    confidence_parsed, _ = _safe_float(result.get("confidence"), field="confidence")
    confidence = confidence_parsed if confidence_parsed is not None else 0.0
    raw_score = result.get("score")
    score_parsed, _ = _safe_float(raw_score, field="score")
    intel_score = (
        score_parsed
        if score_parsed is not None
        else (confidence if raw_score is None and confidence_parsed is not None else fallback_score)
    )
    score = intel_score          # may be overridden below
    raw_contracts = result.get("contracts")
    contracts_parsed, _ = _safe_int(raw_contracts, field="contracts")
    contracts = contracts_parsed if contracts_parsed is not None else 1
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
                canonical_reason_code="INTEL_AUTHORITATIVE_VETO_RISK",
            )

        return _block_gate(
            status="RISK_VETO",
            score=score,
            reasoning=f"risk_veto: {risk_reason}",
            risk_detail=risk,
            canonical_reason_code="INTEL_AUTHORITATIVE_VETO_RISK",
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
                "gate_diagnostics": {
                    **_diagnostics_from_risk(risk),
                    "intel_status": "SCANNER_APPROVED_INTEL_OBSERVE_ONLY",
                    "canonical_admission_reason_code": "INTEL_SCANNER_APPROVED_OBSERVE_ONLY",
                    "scanner_score": round(fallback_score, 1),
                    "legacy_intel_score": round(intel_score, 1),
                    "second_score_mode": "observe_only",
                },
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
                canonical_reason_code="INTEL_LOW_DATA_QUALITY_FAIL_OPEN",
            )

        # Hard block or explicit risk skip — block as authoritative SKIP
        if allow_collect:
            return _collect_gate(
                status="SKIP_OVERRIDE",
                score=score,
                reasoning=f"intel_skip_override: {reasoning[:100]} (1 contract)",
                risk_detail=risk,
                canonical_reason_code="INTEL_AUTHORITATIVE_VETO_SKIP_HARD",
            )
        return _block_gate(
            status="SKIP",
            score=score,
            reasoning=f"intel_skip: {reasoning[:160]}",
            risk_detail=risk,
            canonical_reason_code="INTEL_AUTHORITATIVE_VETO_SKIP_HARD",
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
                canonical_reason_code="INTEL_LOW_DATA_QUALITY_FAIL_OPEN",
            )
        return _block_gate(
            status="LOW_CONFIDENCE",
            score=score,
            reasoning=f"intel_score {score:.1f} below threshold {INTEL_APPROVE_THRESHOLD:.1f}",
            risk_detail=risk,
            canonical_reason_code="INTEL_LOW_DATA_QUALITY_FAIL_OPEN",
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
            "gate_diagnostics": {
                **_diagnostics_from_risk(risk),
                "intel_status": "REGIME_MISMATCH_ADVISORY",
                "canonical_admission_reason_code": "INTEL_REGIME_MISMATCH_ADVISORY",
                "scanner_score": round(fallback_score, 1),
                "legacy_intel_score": round(score, 1),
            },
        }

    # VIX data authority and the approval policy outcome are separate facts.
    # An unavailable/stale/unproven VIX observation may continue Gate G, but it
    # must remain non-authoritative so the LIVE final gate cannot reinterpret a
    # downstream score as an authoritative intelligence approval.
    if (
        str(risk.get("reason_code") or "").strip().upper() == "VIX_UNAVAILABLE"
        and risk.get("hard_veto") is False
        and risk.get("approved") is True
    ):
        try:
            advisory_contracts = int(contracts or 0)
        except (TypeError, ValueError):
            advisory_contracts = 0
        advisory_contracts = max(0, advisory_contracts)
        return {
            "approved": True,
            "score": round(score, 1),
            "contracts": advisory_contracts,
            "reasoning": (
                f"vix_unavailable_advisory: {reasoning[:160]} "
                "(hard_veto=False)"
            ),
            "intel_status": "VIX_ADVISORY",
            "intel_score": round(score, 1),
            "risk_detail": risk,
            "gate_diagnostics": {
                **_diagnostics_from_risk(risk),
                "intel_status": "VIX_ADVISORY",
                "canonical_admission_reason_code": "INTEL_VIX_ADVISORY",
                "scanner_score": round(fallback_score, 1),
                "legacy_intel_score": round(score, 1),
            },
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
        "gate_diagnostics": {
            **_diagnostics_from_risk(risk),
            "intel_status": "APPROVED",
            "canonical_admission_reason_code": "INTEL_AUTHORITATIVE_APPROVED",
            "scanner_score": round(fallback_score, 1),
            "legacy_intel_score": round(score, 1),
        },
    }

def _persist_audit(result: dict, gate: dict, signal: dict) -> None:
    audit = _get_audit_log()
    if not audit:
        return
    try:
        import datetime as _dt
        raw_scanner_score, _ = _first_present(signal, "score", "ev_score")
        scanner_score, _ = _safe_float(raw_scanner_score, field="scanner_score")
        diagnostics = gate.get("gate_diagnostics") or {}
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
            "scanner_score": scanner_score if scanner_score is not None else 0.0,
            "legacy_intel_score": gate.get("intel_score"),
            "canonical_admission_reason_code": diagnostics.get(
                "canonical_admission_reason_code"
            ),
            "input_provenance": diagnostics.get("input_provenance", {}),
            "selected_contract_evidence": diagnostics.get(
                "selected_contract_evidence", {}
            ),
            "account_state": diagnostics.get("account_state", {}),
            "client_id": diagnostics.get("client_id"),
            "execution_mode": diagnostics.get("execution_mode"),
            "canonical_signal_id": diagnostics.get("canonical_signal_id"),
            "dte_raw": diagnostics.get("dte_raw"),
            "dte_resolved": diagnostics.get("dte_resolved"),
            "pattern":      signal.get("pattern") or signal.get("pattern_id", ""),  # DATA-001
            "timeframe":    signal.get("timeframe", ""),          # DATA-001
            "timestamp":    _dt.datetime.now(_dt.timezone.utc).isoformat(),  # DATA-001
            "signal_breakdown": result.get("signal_breakdown", {}),
            "risk_detail":      result.get("risk_detail", {}),
        })
    except Exception as e:
        log.debug(f"intelligence_bridge: audit write failed ({e})")


def record_trade_outcome(ticker: str, signal_id: str, pnl_pct: float) -> dict:
    """Reject the legacy fuzzy outcome interface without mutating training.

    The positional arguments remain for compatibility with older callers, but
    ticker/time/signal/P&L evidence is not a canonical economic identity.  The
    only official outcome path is exact proof binding in the background
    evidence plane.
    """
    log.warning(
        "LEGACY_FUZZY_OUTCOME_BINDING_DISABLED ticker=%s signal_id=%s "
        "training_mutated=false",
        ticker,
        signal_id,
    )
    return {
        "ok": True,
        "disposition": "LEGACY_FUZZY_OUTCOME_BINDING_DISABLED",
        "training_mutated": False,
        "official_binding": False,
    }


def run_intelligence_check(signal: dict, underlying_price: float,
                           client_id: str = "default") -> dict:
    """Called by APMasterControl._run_intelligence().

    LIVE default: hard-veto on unavailable/timeout/error/skip/low-score/risk-veto.
    Paper/research default: 1-contract data-collection override.
    """
    signal = signal if isinstance(signal, dict) else {}
    ticker = str(signal.get("ticker") or signal.get("symbol") or "UNKNOWN")
    direction = _signal_to_direction(signal)

    # Preserve explicit zero and keep scanner admission separate from the
    # legacy pipeline score.  Missing or malformed scanner input gets a
    # documented fallback, but that fallback is recorded as advisory below.
    raw_score, score_key = _first_present(signal, "score", "ev_score")
    parsed_score, score_status = _safe_float(raw_score, field="scanner_score")
    score_in = parsed_score if parsed_score is not None else 65.0

    resolved_client = _signal_client_id(signal, client_id)
    execution_mode = _signal_execution_mode(signal)
    _underlying_price, _underlying_status = _safe_float(
        underlying_price, field="underlying_price"
    )
    if _underlying_price is None or _underlying_price <= 0:
        _underlying_price = 0.0
    contract_evidence = _extract_selected_contract_evidence(
        signal,
        client_id=resolved_client,
        execution_mode=execution_mode,
    )
    gate_inputs = _build_gate_inputs(
        signal,
        score=score_in,
        underlying_price=_underlying_price,
        client_id=resolved_client,
        execution_mode=execution_mode,
        contract_evidence=contract_evidence,
    )
    gate_inputs["input_provenance"]["scanner_score"] = _provenance_entry(
        value=score_in,
        source=f"scanner_signal.{score_key}" if score_key else "gate_g_default",
        classification=PRODUCTION_EXACT if score_key and score_status == "ok" else DEFAULT_ADVISORY,
        authoritative=bool(score_key and score_status == "ok"),
        reason=(
            "scanner score remains separately identified admission input"
            if score_key and score_status == "ok"
            else f"scanner score missing or malformed ({score_status})"
        ),
    )

    def _unavailable_gate(status: str, reason: str) -> dict:
        canonical_reason = {
            "UNAVAILABLE": "INTEL_UNAVAILABLE_FAIL_OPEN",
            "TIMEOUT": "INTEL_UNAVAILABLE_FAIL_OPEN",
            "ERROR": "INTEL_ERROR_FAIL_OPEN",
        }.get(status, status)
        diagnostics = {
            "scanner_score": round(score_in, 1),
            "legacy_intel_score": None,
            "intel_status": status,
            "canonical_admission_reason_code": canonical_reason,
            "client_id": gate_inputs["gate_context"].get("client_id"),
            "execution_mode": gate_inputs["gate_context"].get("execution_mode"),
            "canonical_signal_id": gate_inputs["gate_context"].get("canonical_signal_id"),
            "dte_raw": gate_inputs["gate_context"].get("dte_raw"),
            "dte_resolved": gate_inputs["gate_context"].get("dte_resolved"),
            "selected_contract_evidence": contract_evidence,
            "input_provenance": gate_inputs["input_provenance"],
            "account_state": {
                "source": "MasterControl.snapshot",
                "classification": UNAVAILABLE,
                "authoritative": False,
                "reason": "Gate G did not receive a canonical account snapshot",
            },
        }
        risk_detail = {"gate_diagnostics": diagnostics}
        if _INTEL_FAIL_OPEN_UNAVAILABLE or _data_collection_allowed():
            return _collect_gate(
                status=status,
                score=score_in,
                reasoning=reason,
                risk_detail=risk_detail,
                price_data_stub=True,
                canonical_reason_code=canonical_reason,
            )
        return _block_gate(
            status=status,
            score=score_in,
            reasoning=reason,
            risk_detail=risk_detail,
            price_data_stub=True,
            canonical_reason_code=canonical_reason,
        )

    if _underlying_price <= 0:
        return _unavailable_gate(
            "ERROR",
            f"underlying_price_invalid:{_underlying_status}",
        )

    pipeline = _get_pipeline(
        client_id=resolved_client,
        execution_mode=execution_mode,
    )
    if pipeline is None:
        return _unavailable_gate("UNAVAILABLE", "intel_unavailable")

    def _run():
        if contract_evidence.get("exists") is True:
            # Only an identity-bound, fresh selected contract may activate the
            # contract-quality authority path.  Account authority remains with
            # Master Control even when this evidence is exact.
            return pipeline.run(
                ticker               = ticker,
                scanner_signal       = direction,
                scanner_confidence   = score_in,
                underlying_price     = _underlying_price,
                dte                  = gate_inputs["dte"],
                allow_0dte           = True,
                atr_value            = gate_inputs["atr_value"],
                option_delta         = gate_inputs["option_delta"],
                bid_ask_spread_pct   = gate_inputs["spread_pct"],
                open_interest        = gate_inputs["open_interest"],
                daily_volume_options = gate_inputs["option_volume"],
                option_premium       = gate_inputs["option_premium"],
                contract_quality_authoritative=True,
                account_state_authoritative=False,
                input_provenance=gate_inputs["input_provenance"],
                contract_evidence=gate_inputs["contract_evidence"],
                gate_context=gate_inputs["gate_context"],
            )

        # Pre-selector path: preserve setup-level context, but force every
        # contract-quality dimension to remain advisory/defaulted in the
        # pipeline.  In particular, loose option_bid/ask/OI/volume fields are
        # not silently promoted to selected-contract truth.
        return pipeline.run_quick(
            ticker             = ticker,
            scanner_signal     = direction,
            scanner_confidence = score_in,
            underlying_price   = _underlying_price,
            dte                = gate_inputs["dte"],
            allow_0dte         = True,
            gate_context       = gate_inputs["gate_context"],
            input_provenance   = gate_inputs["input_provenance"],
            contract_evidence  = gate_inputs["contract_evidence"],
        )

    try:
        future = _INTEL_EXECUTOR.submit(_run)
        result = future.result(timeout=INTEL_TIMEOUT_SECONDS)

        gate = _map_result(result, score_in)

        # Keep the production-shape context even if a legacy pipeline
        # implementation returns no diagnostics of its own.
        gate_diagnostics = {
            "scanner_score": round(score_in, 1),
            "legacy_intel_score": gate.get("intel_score"),
            "intel_status": gate.get("intel_status"),
            "canonical_admission_reason_code": gate.get("gate_diagnostics", {}).get(
                "canonical_admission_reason_code", gate.get("intel_status")
            ),
            "client_id": gate_inputs["gate_context"].get("client_id"),
            "execution_mode": gate_inputs["gate_context"].get("execution_mode"),
            "canonical_signal_id": gate_inputs["gate_context"].get("canonical_signal_id"),
            "dte_raw": gate_inputs["gate_context"].get("dte_raw"),
            "dte_resolved": gate_inputs["gate_context"].get("dte_resolved"),
            "selected_contract_evidence": contract_evidence,
            "input_provenance": gate_inputs["input_provenance"],
            "account_state": {
                "source": "MasterControl.snapshot",
                "classification": UNAVAILABLE,
                "authoritative": False,
                "reason": "Gate G did not receive a canonical account snapshot",
            },
        }
        gate_diagnostics.update(gate.get("gate_diagnostics") or {})
        gate["gate_diagnostics"] = gate_diagnostics
        if isinstance(gate.get("risk_detail"), dict):
            risk_detail = gate["risk_detail"]
            risk_detail.setdefault("gate_diagnostics", gate_diagnostics)

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
