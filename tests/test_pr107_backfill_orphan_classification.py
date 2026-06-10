"""
tests/test_pr107_backfill_orphan_classification.py
PR#107: reconciler backfill classifies FILLED orphan orders before acting.
No false P0 noise for historical/expired/null-mode rows.
"""
import datetime
from pathlib import Path
import re, textwrap, pytest

_REPO = Path(__file__).resolve().parents[1]
SRC   = (_REPO / "ap_reconciler.py").read_text()


# ── Load classifiers directly from source ────────────────────────────────────

def _extract(name):
    m = re.search(rf"    (def {name}\b.*?)(?=\n    (?:def |@)|\Z)", SRC, re.DOTALL)
    assert m, f"{name} not found in ap_reconciler.py"
    return textwrap.dedent(m.group(1))

import logging
_log = logging.getLogger("test_pr107")

_g = {"__name__": "ap_reconciler", "datetime": datetime,
      "re": re, "log": _log}

for _mname in ["_occ_expiry", "_classify_orphan"]:
    _code = _extract(_mname)
    _local = {}
    exec(compile(_code, f"ap_reconciler::{_mname}", "exec"), _g, _local)
    _g[_mname] = _local[_mname]

class _C:
    _occ_expiry      = staticmethod(_g["_occ_expiry"])
    _classify_orphan = _g["_classify_orphan"]

_eng   = _C()
_TODAY = datetime.date(2026, 6, 10)
_PAST  = "TSLA260212C00200000"   # Feb 12 2026 — expired
_LIVE  = "RIVN260612P00016500"   # Jun 12 2026 — same day, not past


# ── _occ_expiry ───────────────────────────────────────────────────────────────

def test_occ_expiry_rivn():
    assert _eng._occ_expiry(_LIVE) == datetime.date(2026, 6, 12)

def test_occ_expiry_c_call():
    assert _eng._occ_expiry("C260612C00136000") == datetime.date(2026, 6, 12)

def test_occ_expiry_historical():
    assert _eng._occ_expiry(_PAST) == datetime.date(2026, 2, 12)

def test_occ_expiry_returns_none_for_garbage():
    assert _eng._occ_expiry("NOTANOCC") is None


# ── _classify_orphan ──────────────────────────────────────────────────────────

def _o(contract, mode):
    return {"contract": contract, "execution_mode": mode,
            "filled_qty": 1, "fill_price": 1.0}


# Test 1: expired null-mode → MUST NOT become P0
def test_classify_expired_null_mode_is_historical():
    cls = _eng._classify_orphan(_o(_PAST, None), _TODAY)
    assert cls == "historical_null_mode", (
        f"Expired null-mode order must be historical_null_mode, got {cls}"
    )

def test_historical_null_mode_not_p0():
    """historical_null_mode must NOT appear as filled_order_missing_position_p0."""
    cls = _eng._classify_orphan(_o(_PAST, None), _TODAY)
    assert cls != "current_live", (
        "Expired null-mode order must never be classified as current_live (P0)"
    )


# Test 2: unexpired live → P0 and attempt create+link
def test_classify_unexpired_live_is_current_live():
    cls = _eng._classify_orphan(_o(_LIVE, "live"), _TODAY)
    assert cls == "current_live", (
        f"Unexpired live order must be current_live (P0), got {cls}"
    )

def test_classify_unexpired_live_different_contracts():
    for contract in ("AAPL260612C00190000", "C260612C00136000",
                     "RIVN260612P00016500"):
        cls = _eng._classify_orphan(_o(contract, "live"), _TODAY)
        assert cls == "current_live", f"{contract}: expected current_live, got {cls}"


# Test 3: unexpired null-mode → manual_review (not P0)
def test_classify_unexpired_null_mode_is_manual_review():
    cls = _eng._classify_orphan(_o(_LIVE, None), _TODAY)
    assert cls == "manual_review", (
        f"Unexpired null-mode order must be manual_review, got {cls}"
    )

def test_manual_review_not_automatically_p0():
    cls = _eng._classify_orphan(_o(_LIVE, None), _TODAY)
    assert cls != "current_live", (
        "Unexpired null-mode must NOT be auto-classified as current_live P0"
    )


# Test 4: expired live → historical_live (not P0)
def test_classify_expired_live_is_historical_live():
    cls = _eng._classify_orphan(_o(_PAST, "live"), _TODAY)
    assert cls == "historical_live"

def test_historical_live_not_p0():
    cls = _eng._classify_orphan(_o(_PAST, "live"), _TODAY)
    assert cls != "current_live"


# ── Source structural checks ──────────────────────────────────────────────────

def test_required_heartbeat_counters_present():
    for counter in [
        "live_orphan_filled_orders",
        "historical_expired_orphan_filled_orders",
        "historical_null_mode_orphan_filled_orders",
        "historical_orphan_backfill_skipped",
        "manual_review_required_orphan_filled_orders",
        "orphan_backfill_failed_current_live",
    ]:
        assert counter in SRC, f"Heartbeat counter missing from source: {counter}"

def test_exception_messages_include_type():
    """Improved exception logging must include type(e).__name__."""
    assert "type(_be).__name__" in SRC, (
        "Exception logging must include type(_be).__name__ to avoid 'failed: 0' noise"
    )

def test_fetch_includes_execution_mode():
    """The _fetch query must select execution_mode column."""
    idx = SRC.find("def _backfill_missing_position_links")
    end = SRC.find("\n    def ", idx + 1)
    body = SRC[idx:end]
    assert "execution_mode" in body, (
        "Backfill query must fetch execution_mode to enable classification"
    )

def test_null_mode_expired_does_not_create_active_position():
    """historical_null_mode bucket must NOT call _create_imported_position."""
    idx = SRC.find("# ── 1. historical_null_mode")
    end = SRC.find("# ── 2.", idx)
    null_mode_section = SRC[idx:end]
    assert "_create_imported_position" not in null_mode_section, (
        "historical_null_mode section must NOT attempt active-position creation"
    )

def test_current_live_still_logs_p0():
    """current_live bucket must still log filled_order_missing_position_p0."""
    idx = SRC.find("# ── 4. current_live")
    end = SRC.find("\n    def ", idx + 1)
    live_section = SRC[idx:end]
    assert "filled_order_missing_position_p0" in live_section

def test_classify_null_contract_but_valid_option_symbol_expired():
    """
    Regression: execution_mode IS NULL, contract NULL/malformed,
    option_symbol contains expired OCC contract.
    Must classify as historical_null_mode (not manual_review).
    Without the option_symbol fix, _occ_expiry would receive None or a
    non-OCC string and return None, causing expired rows to be misclassified
    as manual_review.
    """
    order = {
        "execution_mode": None,
        "contract":        None,                         # missing/malformed
        "option_symbol":   "TSLA260212C00200000",        # Feb 2026 — expired
        "symbol":          None,
        "filled_qty":      1,
        "fill_price":      2.50,
    }
    cls = _eng._classify_orphan(order, _TODAY)
    assert cls == "historical_null_mode", (
        f"NULL contract + expired option_symbol must be historical_null_mode, got {cls}. "
        "Regression: _classify_orphan must use COALESCE(option_symbol, contract, symbol)."
    )
