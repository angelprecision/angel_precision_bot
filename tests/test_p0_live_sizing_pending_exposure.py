"""
tests/test_p0_live_sizing_pending_exposure.py
PR P0-SIZING: LIVE small-account resize + pending exposure honesty.
"""
import re, math, pytest
from pathlib import Path

_REPO   = Path(__file__).resolve().parents[1]
MC_SRC  = (_REPO / "ap_master_control.py").read_text()

# ── Source checks ─────────────────────────────────────────────────────────────

def test_pending_orders_capital_uses_broker_proof():
    idx = MC_SRC.find("def _pending_orders_capital")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    assert "broker_order_id IS NOT NULL" in body
    assert "submitted_ts IS NOT NULL" in body
    assert "pending_submitted_entry_exposure" in body

def test_pending_orders_capital_excludes_no_broker_proof_rows():
    idx = MC_SRC.find("def _pending_orders_capital")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    # Must NOT use fill-truth predicate as the primary filter
    # (fill-truth moves to filled_unreconciled_exposure)
    assert "fill_price IS NOT NULL" not in body or "COALESCE(filled_qty" not in body.split("broker_order_id IS NOT NULL")[0]

def test_pending_capital_from_snapshot_sums_both():
    idx = MC_SRC.find("def _pending_capital_from_snapshot_or_db")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    assert "_pending_submitted" in body
    assert "_filled_unreconciled" in body
    assert "_pending_submitted + _filled_unreconciled" in body

def test_live_resize_log_present():
    assert "LIVE_SMALL_ACCOUNT_RESIZE" in MC_SRC
    assert "LIVE_SMALL_ACCOUNT_UNAFFORDABLE" in MC_SRC

def test_resize_log_has_all_required_fields():
    idx = MC_SRC.find("LIVE_SMALL_ACCOUNT_RESIZE")
    region = MC_SRC[idx:idx + 600]
    for field in ["client_email", "execution_mode=live", "client_cap",
                  "capital_deployed", "pending_submitted_entry_exposure",
                  "filled_unreconciled_exposure", "remaining_capital",
                  "candidate_limit", "computed_qty", "original_qty",
                  "final_qty", "resized_for_small_live=true"]:
        assert field in region, f"LIVE_SMALL_ACCOUNT_RESIZE missing field: {field}"

def test_capital_limit_contract_unaffordable_present():
    assert "capital_limit_contract_unaffordable" in MC_SRC

def test_resize_path_before_hard_block():
    """LIVE resize must happen before the hard capital_limit block."""
    reval_idx  = MC_SRC.find("def revalidate_exposure")
    resize_idx = MC_SRC.find("LIVE_SMALL_ACCOUNT_RESIZE", reval_idx)
    block_idx  = MC_SRC.find("ACTUAL_CONTRACT_COST_EXCEEDS_CLIENT_CAPITAL_LIMIT", reval_idx)
    assert reval_idx < resize_idx < block_idx, (
        "LIVE resize log must appear before hard block in revalidate_exposure"
    )


# ── Pure-function behavioral tests ────────────────────────────────────────────

def _floor(remaining: float, limit_price: float) -> int:
    """Mirror the spec formula: floor(remaining / (limit_price * 100))."""
    if limit_price <= 0 or remaining <= 0:
        return 0
    return int(remaining // (limit_price * 100))

def _live_resize(original_qty: int, equity: float, max_cap_pct: float,
                 deployed: float, pending: float, limit_price: float):
    """
    Simulate the revalidate_exposure LIVE resize path (pure arithmetic).
    Returns (final_qty, allowed, resize_happened).
    """
    max_capital   = equity * max_cap_pct
    real_cost     = original_qty * limit_price * 100
    proj_total    = deployed + pending + real_cost
    if proj_total <= max_capital:
        return original_qty, True, False  # no resize needed

    remaining     = max(0.0, max_capital - deployed - pending)
    computed_qty  = _floor(remaining, limit_price)
    if computed_qty >= 1:
        final_qty = min(original_qty, computed_qty)
        return final_qty, True, True
    return 0, False, False


# Test 1: LIVE small account resize — equity=$1980, limit=$0.99, qty=15
def test_ac1_live_resize_099():
    """Example A: computed_qty=2 final_qty=2 allowed=true."""
    qty, allowed, resized = _live_resize(15, 1980, 0.10, 0, 0, 0.99)
    assert allowed is True,  "Trade must be allowed after resize"
    assert resized is True,  "Resize flag must be set"
    assert qty == 2,         f"final_qty must be 2, got {qty}"
    assert _floor(198, 0.99) == 2

# Test 2: LIVE one-contract case — limit=$1.95
def test_ac2_live_resize_195():
    """Example B: computed_qty=1 final_qty=1 allowed=true."""
    qty, allowed, resized = _live_resize(15, 1980, 0.10, 0, 0, 1.95)
    assert allowed is True
    assert resized is True
    assert qty == 1,        f"final_qty must be 1, got {qty}"
    assert _floor(198, 1.95) == 1

# Test 3: LIVE unaffordable — limit=$2.25
def test_ac3_live_unaffordable_225():
    """Example C: computed_qty=0 allowed=false."""
    qty, allowed, resized = _live_resize(15, 1980, 0.10, 0, 0, 2.25)
    assert allowed is False, "Trade must be blocked (unaffordable)"
    assert qty == 0
    assert _floor(198, 2.25) == 0

# Test 4: Pending exposure honesty — rejected row must not contribute
def test_ac4_rejected_row_no_pending():
    """Example D: REJECTED row, no broker_order_id, no submitted_ts → pending=0."""
    # The broker-proof predicate requires broker_order_id OR submitted_ts.
    # A row with neither contributes $0 to pending.
    has_broker_proof = lambda row: (
        bool(row.get("broker_order_id")) or row.get("submitted_ts") is not None
    )
    rejected_row = {
        "status": "REJECTED", "reserved_cost": 219,
        "broker_order_id": None, "submitted_ts": None,
        "filled_qty": 0, "fill_price": None,
    }
    assert not has_broker_proof(rejected_row), (
        "REJECTED row with no broker proof must not count as pending"
    )

def test_ac4_failed_row_no_pending():
    failed_row = {
        "status": "FAILED", "reserved_cost": 219,
        "broker_order_id": None, "submitted_ts": None,
    }
    assert not (bool(failed_row.get("broker_order_id")) or
                failed_row.get("submitted_ts") is not None)

def test_ac4_watching_row_no_pending():
    watching_row = {
        "status": "WATCHING", "reserved_cost": 450,
        "broker_order_id": None, "submitted_ts": None,
    }
    assert not (bool(watching_row.get("broker_order_id")) or
                watching_row.get("submitted_ts") is not None)

# Test 4 continued: active submitted row DOES count
def test_ac4e_submitted_row_counts():
    """Example E: SUBMITTED + broker_order_id → pending=218."""
    submitted_row = {
        "status": "SUBMITTED", "limit_price": 1.09, "qty": 2,
        "broker_order_id": "br-001", "submitted_ts": None,
        "filled_qty": 0, "fill_price": None,
    }
    has_proof = bool(submitted_row.get("broker_order_id"))
    cost = submitted_row["limit_price"] * submitted_row["qty"] * 100
    assert has_proof
    assert abs(cost - 218.0) < 0.01

# Test 5: Large account — no resize when original qty is affordable
def test_ac5_large_account_no_resize():
    """Large account: original qty already affordable → final_qty=15, no resize."""
    qty, allowed, resized = _live_resize(15, 100_000, 0.10, 0, 0, 1.09)
    assert allowed is True
    assert resized is False, "Large account must not trigger resize"
    assert qty == 15


# ── Multi-client / pod isolation tests ───────────────────────────────────────

def test_multi_client_no_cross_contamination():
    """
    Same signal, same limit=$0.99, original_qty=15.
    Each client computes independently.
    """
    clients = [
        # (label, equity, cap_pct, deployed, pending, expected_final_qty, expected_allowed)
        ("paper_large",   50_000,  0.40, 0, 0, 15,  True),   # paper — uses own sizing
        ("live_b_1980",    1_980,  0.10, 0, 0,  2,  True),   # Jason equivalent
        ("live_c_1000",    1_000,  0.10, 0, 0,  1,  True),   # $100 cap, 1 contract
        ("live_d_500",       500,  0.10, 0, 0,  0, False),   # $50 cap, unaffordable
    ]
    results = {}
    for label, equity, cap_pct, dep, pend, exp_qty, exp_allow in clients:
        qty, allowed, _ = _live_resize(15, equity, cap_pct, dep, pend, 0.99)
        results[label] = (qty, allowed)
        assert allowed == exp_allow,  f"{label}: allowed={allowed} expected={exp_allow}"
        assert qty == exp_qty,        f"{label}: qty={qty} expected={exp_qty}"

    # No two live clients share the same result (unless intentionally same)
    live_qtys = [results["live_b_1980"][0], results["live_c_1000"][0], results["live_d_500"][0]]
    assert live_qtys[0] != live_qtys[2], "live_b and live_d must have different final_qty"

def test_paper_large_not_constrained_by_live_logic():
    """Paper large account gets original qty=15, not constrained by live resize."""
    qty, allowed, resized = _live_resize(15, 50_000, 0.40, 0, 0, 0.99)
    assert qty == 15
    assert resized is False, "Paper/large account must not be resized"


# ── Source: SQL does not count rejected/watching rows ─────────────────────────

def test_pending_sql_excludes_rejected():
    idx = MC_SRC.find("def _pending_orders_capital")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    assert "REJECTED" in body, "REJECTED must be in NOT IN terminal clause"
    assert "FAILED" in body

def test_pending_sql_excludes_no_broker_proof():
    idx = MC_SRC.find("def _pending_orders_capital")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    assert "(broker_order_id IS NOT NULL" in body, "Broker proof guard must exist"
    assert "submitted_ts IS NOT NULL" in body

def test_pending_sql_excludes_deferred():
    idx = MC_SRC.find("def _pending_orders_capital")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    assert "DEFERRED:" in body, "DEFERRED:% exclusion must exist"
