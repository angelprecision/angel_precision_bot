"""
ap/deferred_breach_underlying_repair.py

P0 PR #301 — resolve_positive_underlying_for_breach

Before submit_existing_entry() on a deferred breach, execution core must
resolve a positive, finite underlying price from every available source.

Returns tuple[float | None, str | None, dict]:
  - resolved_underlying: first positive finite float found, or None
  - resolved_underlying_source: label of the winning source, or None
  - audit: full candidate map including every checked source and raw value

If positive found:
  - Caller patches approved_plan.metadata (flat keys + nested trigger)
  - Caller persists orders.meta patch via update_order_meta

If no positive source exists on a deferred breach:
  - Caller MUST persist the full candidate audit to orders.meta BEFORE
    calling _terminalize_breach_failure
  - Terminal reason: metadata_invalid:zero_underlying:no_positive_source

Paper data-domain (Fix C):
  - paper_selector_data_domain:   "live" | "sandbox" | "unknown"
  - paper_selector_quote_source:  "tradier_live" | "tradier_sandbox" | ... (raw selector field)
  - paper_selector_base_url:      actual URL string
  - paper_order_broker_domain:    "live" | "sandbox" | "unknown"
  - paper_order_broker_base_url:  actual broker base URL
  - paper_data_order_domain_mismatch: bool — domain-level comparison, not quote_source string

PAPER_SELECTOR_REQUIRE_LIVE_DATA=1:
  When set and paper selector data source is sandbox, fail closed before submit
  with PAPER_SELECTOR_DATA_DOMAIN_BLOCKED. Persists audit before terminalizing.

Resolution priority (first positive finite float wins):
  1.  selector_audit["underlying_price"]                    — live Tradier, chain-fetch
  2.  selector_result.candidate_audit["underlying_price"]   — SelectedContract path
  3.  plan.metadata["underlying_price"]
  4.  plan.metadata["current_underlying_price"]
  5.  plan.metadata["trigger_current_price"]
  6.  plan.metadata["underlying_entry"]
  7.  plan.metadata["trigger_price"]
  8.  plan.metadata["trigger"]["current_price"]             — nested trigger sub-dict
  9.  watched.trigger_price                                 — live price at trigger fire
  10. watched.entry_trigger                                  — configured trigger level
  11. plan.trigger_price                                    — approved_plan attribute
  12. plan.underlying_price
  13. sig["underlying_price"]
  14. sig["underlying_entry"]
  15. sig["current_underlying_price"]
  16. sig["underlying_at_signal"]
  17. sig["entry_price"]
  18. sig["trigger_price"]
  19. order_row_meta["underlying_entry"]
  20. order_row_meta["underlying_price"]
  21. order_row_meta["current_underlying_price"]
"""
from __future__ import annotations

import math
import os
from typing import Any, Optional

from ap.logger import get_logger

log = get_logger("ap.deferred_breach_underlying_repair")

# ── Terminal reason strings ───────────────────────────────────────────────────
ZERO_UNDERLYING_TERMINAL_REASON = "metadata_invalid:zero_underlying:no_positive_source"
PAPER_SELECTOR_DATA_DOMAIN_BLOCKED = "paper_selector_data_domain_blocked:sandbox_data_live_data_required"

# ── Log markers (literal strings — grep-able in Render logs) ─────────────────
LOG_MARKER_REPAIRED   = "ZERO_UNDERLYING_REPAIRED"
LOG_MARKER_NO_SOURCE  = "ZERO_UNDERLYING_NO_POSITIVE_SOURCE"
LOG_MARKER_PAPER_BLOCKED = "PAPER_SELECTOR_DATA_DOMAIN_BLOCKED"


def _to_positive_finite(v: Any) -> Optional[float]:
    """Return float(v) if > 0 and finite, else None. Never raises."""
    try:
        f = float(v)
        if f > 0 and math.isfinite(f):
            return f
    except Exception:
        pass
    return None


def _classify_domain(base_url: str) -> str:
    """
    Return 'live', 'sandbox', or 'unknown' from a Tradier base URL.
    Used for both paper_selector_data_domain and paper_order_broker_domain.
    """
    url = str(base_url or "").lower()
    if "api.tradier.com" in url:
        return "live"
    if "sandbox" in url:
        return "sandbox"
    if url:
        return "unknown"
    return "unknown"


def resolve_positive_underlying_for_breach(
    *,
    approved_plan: Any,
    watched: Any,
    sig: dict,
    selector_audit: Optional[dict] = None,
    selector_result: Any = None,
    order_row_meta: Optional[dict] = None,
) -> tuple[Optional[float], Optional[str], dict]:
    """
    Resolve the first positive finite underlying price from all available
    deferred breach context.

    Returns:
        (underlying_price, source_label, audit_dict)
        underlying_price is None if no positive source found.
        audit_dict always includes full candidate map for persistence.
    """
    _sources: list[tuple[str, Any]] = []

    # 1. selector_audit live underlying (highest confidence)
    _sa = selector_audit or {}
    _sources.append(("selector_audit.underlying_price", _sa.get("underlying_price")))

    # 2. selector_result candidate_audit
    try:
        _ca = getattr(selector_result, "candidate_audit", None) or {}
        _sources.append(("selector_result.candidate_audit.underlying_price", _ca.get("underlying_price")))
    except Exception:
        _sources.append(("selector_result.candidate_audit.underlying_price", None))

    # 3–8. approved_plan.metadata keys (including nested trigger)
    _plan_meta: dict = {}
    try:
        _plan_meta = dict(getattr(approved_plan, "metadata", None) or {})
    except Exception:
        pass
    for _k in (
        "underlying_price",
        "current_underlying_price",
        "trigger_current_price",
        "underlying_entry",
        "trigger_price",
    ):
        _sources.append((f"plan.metadata.{_k}", _plan_meta.get(_k)))

    # 8. nested trigger sub-dict
    try:
        _trigger_dict = _plan_meta.get("trigger") or {}
        if isinstance(_trigger_dict, dict):
            _sources.append(("plan.metadata.trigger.current_price", _trigger_dict.get("current_price")))
    except Exception:
        _sources.append(("plan.metadata.trigger.current_price", None))

    # 9. watched.trigger_price — live price at trigger fire
    try:
        _sources.append(("watched.trigger_price", getattr(watched, "trigger_price", None)))
    except Exception:
        _sources.append(("watched.trigger_price", None))

    # 10. watched.entry_trigger — configured trigger level
    try:
        _sources.append(("watched.entry_trigger", getattr(watched, "entry_trigger", None)))
    except Exception:
        _sources.append(("watched.entry_trigger", None))

    # 11–12. approved_plan attributes
    for _attr in ("trigger_price", "underlying_price"):
        try:
            _sources.append((f"plan.{_attr}", getattr(approved_plan, _attr, None)))
        except Exception:
            _sources.append((f"plan.{_attr}", None))

    # 13–18. sig dict
    _sig = sig or {}
    for _k in (
        "underlying_price",
        "underlying_entry",
        "current_underlying_price",
        "underlying_at_signal",
        "entry_price",
        "trigger_price",
    ):
        _sources.append((f"sig.{_k}", _sig.get(_k)))

    # 19–21. order_row_meta
    _orm = order_row_meta or {}
    for _k in ("underlying_entry", "underlying_price", "current_underlying_price"):
        _sources.append((f"order_row_meta.{_k}", _orm.get(_k)))

    # ── Walk sources, build full candidate map ────────────────────────────────
    _candidates: list[dict] = []
    _result_price: Optional[float] = None
    _result_source: Optional[str] = None

    for _label, _raw in _sources:
        _v = _to_positive_finite(_raw)
        _candidates.append({
            "source":   _label,
            "raw":      _raw,
            "resolved": _v is not None,
            "value":    _v,
        })
        if _v is not None and _result_price is None:
            _result_price  = _v
            _result_source = _label
            log.debug("%s underlying resolved from %s: %.4f", LOG_MARKER_REPAIRED, _label, _v)

    _audit: dict = {
        "resolved_underlying":        _result_price,
        "resolved_underlying_source": _result_source,
        "zero_underlying_repaired":   _result_price is not None,
        "underlying_candidates":      _candidates,
    }

    return _result_price, _result_source, _audit


def build_underlying_patch(
    underlying: float,
    source: str,
    audit: dict,
    *,
    order_id: str = "",
    ticker: str = "",
    existing_trigger: Optional[dict] = None,
) -> dict:
    """
    Build the metadata patch dict for a resolved underlying.
    Written into both approved_plan.metadata and orders.meta.

    Writes both flat canonical keys AND nested trigger dict.
    existing_trigger: if the plan already has a trigger sub-dict, merge into it.
    """
    _repair_audit = {
        "repaired":   True,
        "source":     source,
        "value":      underlying,
        "order_id":   order_id,
        "ticker":     ticker,
        "candidates": audit.get("underlying_candidates", []),
    }
    # Merge into existing trigger dict or create fresh
    _trigger = dict(existing_trigger or {})
    _trigger["current_price"] = underlying

    return {
        "underlying_entry":           underlying,
        "underlying_price":           underlying,
        "current_underlying_price":   underlying,
        "trigger_current_price":      underlying,
        "trigger":                    _trigger,   # nested — not flat dotted key
        "zero_underlying_repair":     _repair_audit,
    }


def build_no_source_meta(
    audit: dict,
    *,
    order_id: str = "",
    ticker: str = "",
) -> dict:
    """
    Build the orders.meta patch for when no positive underlying is found.
    Persisted to the DB BEFORE _terminalize_breach_failure is called.
    """
    return {
        "zero_underlying_repair": {
            "repaired":   False,
            "reason":     "no_positive_source",
            "order_id":   order_id,
            "ticker":     ticker,
            "candidates": audit.get("underlying_candidates", []),
        },
        "zero_underlying_repair_failed":   True,
        "zero_underlying_failure_reason":  ZERO_UNDERLYING_TERMINAL_REASON,
    }


def check_paper_selector_data_domain(
    *,
    is_paper: bool,
    selector_audit: dict,
    broker_base_url: str = "",
) -> tuple[bool, Optional[str]]:
    """
    Check whether paper data-domain enforcement should block submit.

    Returns (should_block, block_reason).
    should_block is True only when:
      - is_paper=True
      - PAPER_SELECTOR_REQUIRE_LIVE_DATA=1
      - selector data domain is 'sandbox'

    Never raises.
    """
    if not is_paper:
        return False, None
    try:
        _require_live = os.getenv("PAPER_SELECTOR_REQUIRE_LIVE_DATA", "0").strip() == "1"
        if not _require_live:
            return False, None
        _sel_base = str(selector_audit.get("tradier_base_url") or broker_base_url or "")
        _sel_domain = _classify_domain(_sel_base)
        if _sel_domain == "sandbox":
            return True, PAPER_SELECTOR_DATA_DOMAIN_BLOCKED
    except Exception:
        pass
    return False, None


def build_paper_domain_fields(
    *,
    selector_audit: dict,
    broker_base_url: str = "",
) -> dict:
    """
    Build the paper data-domain audit fields.
    Separates domain classification ("live"/"sandbox"/"unknown") from
    raw quote_source string ("tradier_live" etc.) — they are different things.
    """
    _sa = selector_audit or {}
    _sel_base    = str(_sa.get("tradier_base_url") or broker_base_url or "")
    _sel_domain  = _classify_domain(_sel_base)
    _quote_src   = str(_sa.get("quote_source") or "") or "unknown"
    _broker_base = str(broker_base_url or "")
    # Paper broker is always sandbox — but classify from actual URL if available
    _broker_domain = _classify_domain(_broker_base) if _broker_base else "sandbox"
    _mismatch = (_sel_domain not in ("sandbox", "unknown")) and (_broker_domain == "sandbox")

    return {
        "paper_selector_data_domain":       _sel_domain,    # "live" | "sandbox" | "unknown"
        "paper_selector_quote_source":      _quote_src,     # "tradier_live" | "tradier_sandbox" | ...
        "paper_selector_base_url":          _sel_base,
        "paper_order_broker_domain":        _broker_domain, # "live" | "sandbox" | "unknown"
        "paper_order_broker_base_url":      _broker_base,
        "paper_data_order_domain_mismatch": _mismatch,
    }
