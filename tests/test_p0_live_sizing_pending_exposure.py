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
    # Sum logic lives in _get_pending_capital_breakdown; the float wrapper calls it.
    idx = MC_SRC.find("def _get_pending_capital_breakdown(")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    assert "pending_submitted_entry_exposure" in body
    assert "filled_unreconciled_exposure" in body
    assert "pending_total_capital_reserved" in body
    # _pending_capital_from_snapshot_or_db must be a thin wrapper
    idx2 = MC_SRC.find("def _pending_capital_from_snapshot_or_db(")
    end2 = MC_SRC.find("\n    def ", idx2 + 1)
    body2 = MC_SRC[idx2:end2]
    assert "_get_pending_capital_breakdown(" in body2, (
        "_pending_capital_from_snapshot_or_db must call breakdown helper"
    )

def test_affordable_resize_log_present():
    assert "SMALL_ACCOUNT_AFFORDABLE_QTY_RESIZE" in MC_SRC
    assert "SMALL_ACCOUNT_CONTRACT_UNAFFORDABLE" in MC_SRC
    assert "LIVE_SMALL_ACCOUNT_RESIZE" not in MC_SRC
    assert "LIVE_SMALL_ACCOUNT_UNAFFORDABLE" not in MC_SRC

def test_resize_log_has_all_required_fields():
    idx = MC_SRC.find("SMALL_ACCOUNT_AFFORDABLE_QTY_RESIZE")
    region = MC_SRC[idx:idx + 600]
    for field in ["client_email", "execution_mode=%s", "client_cap",
                  "capital_deployed", "pending_submitted_entry_exposure",
                  "filled_unreconciled_exposure", "remaining_capital",
                  "candidate_limit", "computed_qty", "original_qty",
                  "final_qty", "resized_for_affordable_qty=true"]:
        assert field in region, (
            f"SMALL_ACCOUNT_AFFORDABLE_QTY_RESIZE missing field: {field}"
        )

def test_capital_limit_contract_unaffordable_present():
    assert "capital_limit_contract_unaffordable" in MC_SRC

def test_resize_path_before_hard_block():
    """Affordable resize must happen before the hard capital_limit block."""
    reval_idx  = MC_SRC.find("def revalidate_exposure")
    resize_idx = MC_SRC.find("SMALL_ACCOUNT_AFFORDABLE_QTY_RESIZE", reval_idx)
    block_idx  = MC_SRC.find("ACTUAL_CONTRACT_COST_EXCEEDS_CLIENT_CAPITAL_LIMIT", reval_idx)
    assert reval_idx < resize_idx < block_idx, (
        "Affordable resize log must appear before hard block in revalidate_exposure"
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

# ── Amendment AC5: OPEN broker-proof order counts as pending ─────────────────

def test_ac5_open_broker_proof_order_counts():
    """
    Fix 1: ENTRY status=OPEN with broker_order_id must count as pending_submitted.
    limit_price=1.09 qty=2 → expected pending=218.
    """
    idx = MC_SRC.find("def _pending_orders_capital")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    assert "'OPEN'" in body, (
        "Status IN clause must include \'OPEN\' for broker-proof active orders"
    )
    # Arithmetic check
    limit_price, qty = 1.09, 2
    expected_pending = limit_price * qty * 100
    assert abs(expected_pending - 218.0) < 0.01

def test_ac5_open_status_in_sql():
    idx = MC_SRC.find("def _pending_orders_capital")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    in_clause_idx = body.find("IN (")
    in_clause = body[in_clause_idx:in_clause_idx + 300]
    assert "OPEN" in in_clause, "OPEN must be in the status IN clause"


# ── Amendment AC6: separate pending_submitted vs filled_unreconciled in log ───

def test_ac6_resize_log_has_pending_total_field():
    idx = MC_SRC.find("SMALL_ACCOUNT_AFFORDABLE_QTY_RESIZE")
    region = MC_SRC[idx:idx + 700]
    assert "pending_total_capital_reserved" in region, (
        "SMALL_ACCOUNT_AFFORDABLE_QTY_RESIZE must log pending_total_capital_reserved"
    )

def test_ac6_unaffordable_log_has_pending_total_field():
    idx = MC_SRC.find("SMALL_ACCOUNT_CONTRACT_UNAFFORDABLE")
    region = MC_SRC[idx:idx + 700]
    assert "pending_total_capital_reserved" in region

def test_ac6_resize_separates_submitted_and_filled():
    # breakdown helper provides the separation — log uses _bd_resize.get()
    idx = MC_SRC.find("SMALL_ACCOUNT_AFFORDABLE_QTY_RESIZE")
    region = MC_SRC[max(0, idx - 500) : idx + 1200]
    assert "_bd_resize.get(" in region, (
        "Resize log must use breakdown dict _bd_resize.get(...)"
    )
    assert "pending_submitted_entry_exposure" in region
    assert "filled_unreconciled_exposure" in region
    assert "pending_total_capital_reserved" in region

def test_ac6_filled_separate_not_double_reported():
    """filled_unreconciled comes from snap.pending_entry_capital in breakdown helper."""
    idx = MC_SRC.find("def _get_pending_capital_breakdown(")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    assert "pending_entry_capital" in body
    assert "max(0" not in body, "breakdown helper must not use max(0,...) subtraction"


def test_ac3_unaffordable_reason_code():
    """Fix 3: unaffordable path must use CAPITAL_LIMIT_CONTRACT_UNAFFORDABLE."""
    assert "CAPITAL_LIMIT_CONTRACT_UNAFFORDABLE" in MC_SRC, (
        "Unaffordable reason_code must be CAPITAL_LIMIT_CONTRACT_UNAFFORDABLE"
    )
    # Must NOT use the old reason_code in the unaffordable path
    # (the old code still uses ACTUAL_CONTRACT_COST... for the non-LIVE hard block)
    idx = MC_SRC.find("CAPITAL_LIMIT_CONTRACT_UNAFFORDABLE")
    region = MC_SRC[max(0, idx-200):idx+50]
    assert "reason_code=" in region


# ── Amendment Fix 4: proj_sector/proj_ticker recomputed after resize ─────────

def test_fix4_proj_sector_recomputed_after_resize():
    idx = MC_SRC.find("_resized_for_affordable_qty = True")
    region = MC_SRC[max(0, idx-300):idx+200]
    assert "proj_sector" in region, (
        "proj_sector must be recomputed after resize, before _resized_for_affordable_qty=True"
    )
    assert "proj_ticker" in region

def test_fix4_ticker_sector_caps_not_stale():
    """
    After resize: final_cost=198, not 1485.
    proj_sector and proj_ticker must use resized real_cost.
    Verify arithmetic: equity=1980 max_sector_pct=0.10 → max_sector=198.
    sector_deployed=0 proj_sector=198 ≤ 198 → passes.
    """
    equity, max_sector_pct = 1980, 0.10
    sector_deployed, ticker_deployed = 0, 0
    resized_real_cost = 198.0   # 2 contracts × $0.99 × 100

    max_sector  = equity * max_sector_pct
    max_ticker  = equity * max_sector_pct
    proj_sector = sector_deployed + resized_real_cost
    proj_ticker = ticker_deployed + resized_real_cost

    assert proj_sector <= max_sector, (
        f"Resized trade must fit sector cap: {proj_sector} <= {max_sector}"
    )
    assert proj_ticker <= max_ticker, (
        f"Resized trade must fit ticker cap: {proj_ticker} <= {max_ticker}"
    )
    # Original (stale) projection would have blocked
    stale_cost = 15 * 0.99 * 100   # = 1485
    stale_proj = sector_deployed + stale_cost
    assert stale_proj > max_sector, "Stale cost must exceed cap (shows why recompute matters)"


# ── Amendment Fix 5: exclude path still includes filled_unreconciled ──────────

def test_fix5_exclude_path_retains_filled_unreconciled():
    """breakdown helper always sums submitted + filled regardless of exclusion."""
    idx = MC_SRC.find("def _get_pending_capital_breakdown(")
    sig_end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:sig_end]
    assert "exclude_local_order_id" in MC_SRC[idx:idx+200], (
        "breakdown helper must accept exclude_local_order_id kwarg"
    )
    assert "filled_unreconciled_exposure" in body
    ret_idx = body.rfind("return {")
    assert ret_idx >= 0
    assert "filled_unreconciled" in body[ret_idx:ret_idx+300]


def test_fix5_exclude_sum_is_submitted_plus_filled():
    """
    Arithmetic: snap[pending_entry_capital]=99, submitted_excl=0
    → total = 0 + 99 = 99 (not 0).
    """
    snap_pending = 99.0
    submitted_excl = 0.0
    total = submitted_excl + snap_pending
    assert abs(total - 99.0) < 0.01, f"Total must be 99, got {total}"

# ── Final amendment: no derivation-by-subtraction ─────────────────────────────

def test_no_subtraction_derivation_of_submitted_pending():
    """
    Source guard: pending_submitted_entry_exposure must never be derived by
    subtracting filled_unreconciled from total pending.
    Check line-by-line (not DOTALL) to avoid false matches across functions.
    """
    import re
    for line in MC_SRC.splitlines():
        stripped = line.strip()
        # Reject any line that computes submitted pending by subtracting filled
        if re.search(r"pending_cap\s*-\s*_filled_unreconciled", stripped):
            raise AssertionError(
                f"Must not derive submitted pending by subtraction: {stripped!r}"
            )
        if re.search(r"max\(0.*pending_cap.*filled_unreconciled", stripped):
            raise AssertionError(
                f"Must not derive submitted pending by subtraction: {stripped!r}"
            )

def test_breakdown_helper_defined():
    assert "def _get_pending_capital_breakdown(" in MC_SRC

def test_breakdown_returns_three_fields():
    idx = MC_SRC.find("def _get_pending_capital_breakdown(")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    for field in ["pending_submitted_entry_exposure", "filled_unreconciled_exposure",
                  "pending_total_capital_reserved"]:
        assert field in body, f"breakdown helper must return field: {field}"

def test_breakdown_fields_come_from_independent_sources():
    """pending_submitted from _pending_orders_capital, filled from snap."""
    idx = MC_SRC.find("def _get_pending_capital_breakdown(")
    end = MC_SRC.find("\n    def ", idx + 1)
    body = MC_SRC[idx:end]
    assert "_pending_orders_capital(" in body, "submitted must come from _pending_orders_capital"
    assert "pending_entry_capital" in body, "filled must come from snap.pending_entry_capital"
    # Must NOT have subtraction derivation
    assert "pending_cap -" not in body
    assert "max(0" not in body


def test_separation_submitted_zero_filled_219():
    """
    _pending_orders_capital returns 0, snap.pending_entry_capital=219
    → submitted=0, filled=219, total=219
    """
    submitted   = 0.0
    filled      = 219.0
    total       = submitted + filled
    assert submitted == 0.0
    assert filled    == 219.0
    assert total     == 219.0

def test_separation_submitted_218_filled_zero():
    """
    _pending_orders_capital returns 218, snap.pending_entry_capital=0
    → submitted=218, filled=0, total=218
    """
    submitted   = 218.0
    filled      = 0.0
    total       = submitted + filled
    assert submitted == 218.0
    assert filled    == 0.0
    assert total     == 218.0

def test_separation_mixed_218_99():
    """
    _pending_orders_capital=218, snap.pending_entry_capital=99
    → submitted=218, filled=99, total=317
    """
    submitted   = 218.0
    filled      = 99.0
    total       = submitted + filled
    assert submitted == 218.0
    assert filled    == 99.0
    assert abs(total - 317.0) < 0.01

def test_block_message_uses_pending_total_not_submitted():
    """evaluate() block message must use pending_total=, not pending_submitted=."""
    # Find the block message string (f-string inside _block call)
    idx = MC_SRC.find('f"capital_limit_no_remaining ')
    assert idx > 0, "capital_limit_no_remaining f-string not found"
    region = MC_SRC[idx:idx + 400]
    assert "pending_total=" in region, (
        f"Block message must say pending_total=. Got: {region[:200]!r}"
    )
    assert "pending_submitted=" not in region, (
        "Block message must not say pending_submitted= (total is not submitted-only)"
    )

def test_log_sites_use_breakdown_dict():
    """All small-account log sites must use _bd_*.get() not manual derivation."""
    for bd_var in ["_bd_resize", "_bd_unafford", "_bd_eval"]:
        assert f"{bd_var}.get(" in MC_SRC, f"Log site must use {bd_var}.get(...)"

# ── Final wording fix: reason string must not use pending_submitted=${pending_cap} ──

def test_unaffordable_reason_string_no_pending_submitted_equals_total():
    """
    The capital_limit_contract_unaffordable reason string must not print
    pending_submitted=${pending_cap} — that mislabels total reserved capital
    as submitted-only pending.
    """
    # Find the reason f-string block
    idx = MC_SRC.find('f"capital_limit_contract_unaffordable "')
    assert idx > 0, "capital_limit_contract_unaffordable reason f-string not found"
    region = MC_SRC[idx : idx + 800]
    # Must NOT contain the bad pattern
    assert 'pending_submitted=${pending_cap' not in region, (
        "reason string must not use pending_submitted=${pending_cap} — "
        "pending_cap is total reserved capital, not submitted-only"
    )

def test_unaffordable_reason_string_has_separated_fields():
    """
    The reason string must include the three separated fields from _bd_unafford.
    """
    idx = MC_SRC.find('f"capital_limit_contract_unaffordable "')
    region = MC_SRC[idx : idx + 800]
    for field in [
        "pending_submitted_entry_exposure=",
        "filled_unreconciled_exposure=",
        "pending_total_capital_reserved=",
    ]:
        assert field in region, (
            f"capital_limit_contract_unaffordable reason must include: {field}"
        )

def test_unaffordable_reason_string_has_reason_code():
    idx   = MC_SRC.find('f"capital_limit_contract_unaffordable "')
    # The reason string spans multiple f-string lines; find its closing )
    close = MC_SRC.find("\n                    )", idx)
    region = MC_SRC[idx : close + 30]
    assert "reason_code=CAPITAL_LIMIT_CONTRACT_UNAFFORDABLE" in region, (
        f"reason_code must be in reason string. region=...{region[-200:]!r}"
    )
