# ap/selector_feature_capture.py — Angel Precision feature-at-signal capture
# =============================================================================
# CAPTURE-ONLY. Persists the option-chain features the live selector already
# computed for the WINNING contract, keyed by signal_id, into
# `ap_signal_option_outcomes`. This is the feature half of the edge dataset.
#
# WHY THIS EXISTS: the historical writer lived in
# `ap_options_intelligence.evaluate_contract()`, which is dead code (see
# ap/contract_selector.py lines 11-18). The live gate
# `APContractSelectionEngine.select()` returns a `SelectedContract` carrying
# spread_pct / delta / open_interest / volume / mid / dte but never persisted
# them. This helper closes that gap at the call site, where both the signal_id
# and the signal_store are already in scope.
#
# INVARIANTS (docs/pr_specs/intelligence_outcome_capture_20260903.md):
#   * NEVER raises into the trading path — one guarded entrypoint.
#   * NEVER alters the selection result. Pure side-effect after the decision.
#   * Idempotent upsert on signal_id (safe on selector retries).
#   * iv_at_signal is written ONLY if already present on the selected contract;
#     never fetched. Absent → omitted (NULL), never guessed.
# =============================================================================
from __future__ import annotations

import logging
from typing import Any, Optional

log = logging.getLogger("ap.selector_feature_capture")


def _get(obj: Any, name: str) -> Any:
    """Read an attribute or dict key without raising."""
    try:
        if isinstance(obj, dict):
            return obj.get(name)
        return getattr(obj, name, None)
    except Exception:
        return None


def _num(value: Any) -> Optional[float]:
    try:
        f = float(value)
        return f if f == f else None  # drop NaN
    except (TypeError, ValueError):
        return None


def _int(value: Any) -> Optional[int]:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def capture_selected_contract_features(
    signal_store: Any,
    signal_id: Any,
    selected: Any,
) -> bool:
    """
    Best-effort persistence of feature-at-signal for a winning contract.

    Returns True if a write was enqueued, False otherwise. Never raises.
    """
    try:
        sid = str(signal_id or "").strip()
        if not sid or signal_store is None or selected is None:
            return False

        contract_symbol = _get(selected, "contract_symbol")
        if not contract_symbol:
            return False  # no identity → nothing worth capturing

        row: dict[str, Any] = {
            "contract_symbol": str(contract_symbol),
            "expiration": _get(selected, "expiration"),
            "strike": _num(_get(selected, "strike")),
            "option_type": (str(_get(selected, "option_type") or "").lower() or None),
            "spread_pct_at_signal": _num(_get(selected, "spread_pct")),
            "delta_at_signal": _num(_get(selected, "delta")),
            "volume_at_signal": _int(_get(selected, "volume")),
            "oi_at_signal": _int(_get(selected, "open_interest")),
            "mark_at_signal": _num(_get(selected, "mid")),
        }

        # chain_grade: selector may expose it under a few names; capture if present.
        grade = _get(selected, "chain_grade") or _get(selected, "grade")
        if grade:
            row["chain_grade"] = str(grade)

        # iv_at_signal: ONLY if already present. Never fetch.
        iv = _num(_get(selected, "iv")) or _num(_get(selected, "iv_at_signal"))
        if iv is not None:
            row["iv_at_signal"] = iv

        # drop keys with no value so we never overwrite a good prior with NULL
        row = {k: v for k, v in row.items() if v is not None}
        if len(row) <= 1:  # only contract_symbol → not worth a write
            return False

        signal_store.insert_option_outcome(sid, row)
        return True
    except Exception as exc:  # pragma: no cover - defensive
        log.debug("selector feature capture failed (ignored): %s", exc)
        return False
