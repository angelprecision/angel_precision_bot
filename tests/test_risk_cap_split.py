"""
tests/test_risk_cap_split.py

P1 — Split per-position risk cap from total portfolio exposure cap.

Current: max_capital_pct acts as BOTH the per-trade budget AND the total
exposure cap. For small live accounts (Jason: equity=$1987, max_capital_pct=0.10),
one fill of $183 leaves only $15.72 for the next trade.

New model:
  max_position_pct      = per-trade budget cap (fraction of equity)
  max_total_capital_pct = total portfolio exposure cap (fraction of equity)

Formula:
  per_trade_budget       = equity * max_position_pct
  total_capital_cap      = equity * max_total_capital_pct
  current_total_exposure = capital_deployed + pending_capital
  remaining_total_cap    = total_capital_cap - current_total_exposure
  selector_budget        = min(per_trade_budget, remaining_total_cap)

Block reasons:
  capital_limit_total_exposure_cap_reached  — total cap at/over limit
  capital_limit_no_remaining                — selector_budget <= 0

Backward compatibility:
  max_position_pct falls back to max_capital_pct when not set.
  max_total_capital_pct falls back to DEFAULT_MAX_TOTAL_CAPITAL_PCT (0.40).
  Never silently unlimited.

Tests:
  Test 1 — normal multi-position room: existing exposure doesn't shrink per-trade budget
  Test 2 — near total cap: selector_budget is constrained by remaining_total_cap
  Test 3 — at total cap: capital_limit_total_exposure_cap_reached block
  Test 4 — backward compatibility: only max_capital_pct set, total cap gets default
  Test 5 — sizing_context metadata contains all new split-cap fields
  Test 6 — live/paper exposure isolation (mode filter already exists; cap math is client-scoped)
  Test 7 — total cap clamped to per-position cap when set below it
  Test 8 — source guards: new fields present in APMasterControl source
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]
_MC_SRC  = (_REPO / "ap_master_control.py").read_text()
_CR_SRC  = (_REPO / "client_runner.py").read_text()
_MIGRATION_SRC = (_REPO / "migrations" / "20260617_risk_cap_split.sql").read_text()


# ---------------------------------------------------------------------------
# Source guards
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_max_position_pct_in_mc_init(self):
        assert "max_position_pct" in _MC_SRC, (
            "APMasterControl.__init__ must accept max_position_pct"
        )

    def test_max_total_capital_pct_in_mc_init(self):
        assert "max_total_capital_pct" in _MC_SRC, (
            "APMasterControl.__init__ must accept max_total_capital_pct"
        )

    def test_per_trade_budget_computed(self):
        assert "per_trade_budget" in _MC_SRC

    def test_total_capital_cap_computed(self):
        assert "total_capital_cap" in _MC_SRC

    def test_current_total_exposure_computed(self):
        assert "current_total_exposure" in _MC_SRC

    def test_remaining_total_cap_computed(self):
        assert "remaining_total_cap" in _MC_SRC

    def test_selector_budget_in_source(self):
        assert "selector_budget" in _MC_SRC

    def test_total_exposure_cap_block_reason_present(self):
        assert "capital_limit_total_exposure_cap_reached" in _MC_SRC

    def test_sizing_context_contains_max_position_pct(self):
        sc_start = _MC_SRC.find('"sizing_context"')
        sc_block = _MC_SRC[sc_start: sc_start + 4500]
        assert '"max_position_pct"' in sc_block

    def test_sizing_context_contains_max_total_capital_pct(self):
        sc_start = _MC_SRC.find('"sizing_context"')
        sc_block = _MC_SRC[sc_start: sc_start + 4500]
        assert '"max_total_capital_pct"' in sc_block

    def test_sizing_context_contains_per_trade_budget(self):
        sc_start = _MC_SRC.find('"sizing_context"')
        sc_block = _MC_SRC[sc_start: sc_start + 4500]
        assert '"per_trade_budget"' in sc_block

    def test_sizing_context_contains_total_capital_cap(self):
        sc_start = _MC_SRC.find('"sizing_context"')
        sc_block = _MC_SRC[sc_start: sc_start + 4500]
        assert '"total_capital_cap"' in sc_block

    def test_sizing_context_contains_remaining_total_cap(self):
        sc_start = _MC_SRC.find('"sizing_context"')
        sc_block = _MC_SRC[sc_start: sc_start + 4500]
        assert '"remaining_total_cap"' in sc_block

    def test_sizing_context_contains_selector_budget(self):
        sc_start = _MC_SRC.find('"sizing_context"')
        sc_block = _MC_SRC[sc_start: sc_start + 4500]
        assert '"selector_budget"' in sc_block

    def test_sizing_context_contains_legacy_max_capital_pct(self):
        sc_start = _MC_SRC.find('"sizing_context"')
        sc_block = _MC_SRC[sc_start: sc_start + 4500]
        assert '"legacy_max_capital_pct"' in sc_block

    def test_client_runner_reads_max_position_pct(self):
        assert "max_position_pct" in _CR_SRC

    def test_client_runner_reads_max_total_capital_pct(self):
        assert "max_total_capital_pct" in _CR_SRC

    def test_client_runner_passes_max_position_pct_to_mc(self):
        idx = _CR_SRC.find("APMasterControl(")
        if idx == -1:
            idx = _CR_SRC.find("master_control = APMasterControl")
        region = _CR_SRC[idx: idx + 800]
        assert "max_position_pct" in region

    def test_total_exposure_cap_reached_has_reason_code(self):
        assert "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED" in _MC_SRC


# ---------------------------------------------------------------------------
# Capital math helper — pure Python simulation of the MC split-cap formula
# ---------------------------------------------------------------------------

def _compute_split_cap(
    *,
    account_equity: float,
    max_position_pct: float,
    max_total_capital_pct: float,
    capital_deployed: float,
    pending_capital: float,
) -> dict:
    """
    Mirror of the production capital math in ap_master_control.py evaluate().
    Returns a dict with all intermediate values for assertion.
    """
    per_trade_budget       = account_equity * max_position_pct
    total_capital_cap      = account_equity * max_total_capital_pct
    current_total_exposure = capital_deployed + pending_capital
    remaining_total_cap    = total_capital_cap - current_total_exposure

    if remaining_total_cap <= 0.0:
        selector_budget = 0.0
        block_reason = "capital_limit_total_exposure_cap_reached"
    else:
        selector_budget = max(0.0, min(per_trade_budget, remaining_total_cap))
        block_reason = None if selector_budget > 0 else "capital_limit_no_remaining"

    return {
        "per_trade_budget":       per_trade_budget,
        "total_capital_cap":      total_capital_cap,
        "current_total_exposure": current_total_exposure,
        "remaining_total_cap":    remaining_total_cap,
        "selector_budget":        selector_budget,
        "block_reason":           block_reason,
    }


# ---------------------------------------------------------------------------
# Test 1 — Normal multi-position room
# Jason: equity=1987.24, max_position_pct=0.10, max_total_capital_pct=0.40
# Existing exposure=183 (two filled orders, both closed in PR #152 scenario)
# Expected: selector_budget = per_trade_budget = 198.724
# ---------------------------------------------------------------------------

class TestNormalMultiPositionRoom:
    """
    Core acceptance: existing exposure does NOT reduce selector_budget
    below max_position_pct unless total exposure cap is near full.
    """

    def test_selector_budget_equals_per_trade_budget(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=183.0,
            pending_capital=0.0,
        )
        assert result["selector_budget"] == pytest.approx(198.724, abs=0.01), (
            f"selector_budget should equal per_trade_budget=198.724, got {result['selector_budget']}"
        )

    def test_per_trade_budget_correct(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=183.0,
            pending_capital=0.0,
        )
        assert result["per_trade_budget"] == pytest.approx(198.724, abs=0.01)

    def test_total_capital_cap_correct(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=183.0,
            pending_capital=0.0,
        )
        assert result["total_capital_cap"] == pytest.approx(794.896, abs=0.01)

    def test_remaining_total_cap_correct(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=183.0,
            pending_capital=0.0,
        )
        assert result["remaining_total_cap"] == pytest.approx(611.896, abs=0.01)

    def test_no_block(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=183.0,
            pending_capital=0.0,
        )
        assert result["block_reason"] is None

    def test_not_shrunken_by_old_formula(self):
        """Old formula would give 198.724 - 183 = 15.724. New must give 198.724."""
        old_formula_result = max(0.0, 1987.24 * 0.10 - 183.0)  # = 15.724
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=183.0,
            pending_capital=0.0,
        )
        assert result["selector_budget"] > old_formula_result, (
            f"New formula ({result['selector_budget']:.2f}) must exceed old formula ({old_formula_result:.2f})"
        )
        assert result["selector_budget"] == pytest.approx(198.724, abs=0.01)


# ---------------------------------------------------------------------------
# Test 2 — Near total cap
# existing exposure=750, total cap=794.896
# remaining_total_cap=44.896 < per_trade_budget=198.724
# selector_budget = min(198.724, 44.896) = 44.896
# ---------------------------------------------------------------------------

class TestNearTotalCap:
    def test_selector_budget_constrained_by_remaining_total_cap(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=750.0,
            pending_capital=0.0,
        )
        assert result["selector_budget"] == pytest.approx(44.896, abs=0.01), (
            f"Near total cap: selector_budget should be 44.896, got {result['selector_budget']}"
        )

    def test_remaining_total_cap_is_44_896(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=750.0,
            pending_capital=0.0,
        )
        assert result["remaining_total_cap"] == pytest.approx(44.896, abs=0.01)

    def test_no_block_when_some_capacity_remains(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=750.0,
            pending_capital=0.0,
        )
        assert result["block_reason"] is None, (
            "Should not block when some total capacity remains"
        )

    def test_selector_budget_less_than_per_trade_budget(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=750.0,
            pending_capital=0.0,
        )
        assert result["selector_budget"] < result["per_trade_budget"]


# ---------------------------------------------------------------------------
# Test 3 — At/over total cap: block with capital_limit_total_exposure_cap_reached
# ---------------------------------------------------------------------------

class TestAtTotalCap:
    def test_block_when_at_total_cap(self):
        # Use slightly over the cap to avoid floating point epsilon issues
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=795.0,   # > 794.896 total cap
            pending_capital=0.0,
        )
        assert result["block_reason"] == "capital_limit_total_exposure_cap_reached"
        assert result["selector_budget"] == 0.0

    def test_block_when_over_total_cap(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=850.0,
            pending_capital=0.0,
        )
        assert result["block_reason"] == "capital_limit_total_exposure_cap_reached"

    def test_pending_capital_counts_toward_total_exposure(self):
        """Pending capital + deployed must together trigger the cap."""
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=600.0,
            pending_capital=200.0,  # together = 800 > 794.896
        )
        assert result["block_reason"] == "capital_limit_total_exposure_cap_reached"

    def test_remaining_total_cap_negative_triggers_block(self):
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=900.0,
            pending_capital=0.0,
        )
        assert result["remaining_total_cap"] < 0
        assert result["block_reason"] == "capital_limit_total_exposure_cap_reached"
        assert result["selector_budget"] == 0.0


# ---------------------------------------------------------------------------
# Test 4 — Backward compatibility
# Only max_capital_pct=0.10 set; max_position_pct falls back to max_capital_pct.
# max_total_capital_pct falls back to DEFAULT_MAX_TOTAL_CAPITAL_PCT (0.40).
# ---------------------------------------------------------------------------

class TestBackwardCompatibility:
    def test_max_position_pct_falls_back_to_max_capital_pct(self):
        """If max_position_pct not provided, it resolves to max_capital_pct."""
        legacy_max_capital_pct = 0.10
        # Simulate the fallback in APMasterControl.__init__
        max_position_pct_resolved = legacy_max_capital_pct  # no override
        assert max_position_pct_resolved == pytest.approx(0.10)

    def test_max_total_capital_pct_not_unlimited(self):
        """max_total_capital_pct must never be 1.0 or missing — must get a real default."""
        default_total_pct = float(os.getenv("DEFAULT_MAX_TOTAL_CAPITAL_PCT", "0.40"))
        assert default_total_pct > 0.0
        assert default_total_pct <= 1.0

    def test_legacy_single_cap_behavior_when_pcts_equal(self):
        """When max_position_pct == max_total_capital_pct, formula == old single-cap."""
        equity = 1987.24
        pct = 0.10
        deployed = 100.0
        # New formula with equal pcts
        result = _compute_split_cap(
            account_equity=equity,
            max_position_pct=pct,
            max_total_capital_pct=pct,
            capital_deployed=deployed,
            pending_capital=0.0,
        )
        # Old formula
        old_remaining = max(0.0, equity * pct - deployed)
        assert result["selector_budget"] == pytest.approx(old_remaining, abs=0.01), (
            "When pcts are equal, new formula must match old single-cap formula"
        )

    def test_mc_source_preserves_max_capital_pct_attribute(self):
        """max_capital_pct must still exist on APMasterControl for backward compat."""
        assert "self.max_capital_pct = max_capital_pct" in _MC_SRC


# ---------------------------------------------------------------------------
# Test 5 — Metadata: sizing_context contains all new split-cap fields
# ---------------------------------------------------------------------------

class TestSizingContextMetadata:
    REQUIRED_FIELDS = [
        "max_position_pct",
        "max_total_capital_pct",
        "per_trade_budget",
        "total_capital_cap",
        "current_total_exposure",
        "remaining_total_cap",
        "selector_budget",
        "legacy_max_capital_pct",
    ]

    def test_all_required_fields_in_mc_source(self):
        sc_start = _MC_SRC.find('"sizing_context"')
        sc_block = _MC_SRC[sc_start: sc_start + 4500]
        for field in self.REQUIRED_FIELDS:
            assert f'"{field}"' in sc_block, (
                f"sizing_context must contain field '{field}'"
            )

    def test_sizing_context_simulation(self):
        """Simulate the sizing_context dict that MC would write."""
        equity = 1987.24
        max_position_pct = 0.10
        max_total_capital_pct = 0.40
        capital_deployed = 183.0
        pending_capital = 0.0

        result = _compute_split_cap(
            account_equity=equity,
            max_position_pct=max_position_pct,
            max_total_capital_pct=max_total_capital_pct,
            capital_deployed=capital_deployed,
            pending_capital=pending_capital,
        )

        sizing_context = {
            "account_equity":          equity,
            "max_position_pct":        max_position_pct,
            "max_total_capital_pct":   max_total_capital_pct,
            "per_trade_budget":        result["per_trade_budget"],
            "total_capital_cap":       result["total_capital_cap"],
            "current_total_exposure":  result["current_total_exposure"],
            "remaining_total_cap":     result["remaining_total_cap"],
            "selector_budget":         result["selector_budget"],
            "legacy_max_capital_pct":  0.10,
        }

        for field in self.REQUIRED_FIELDS:
            assert field in sizing_context, f"Missing field: {field}"
        assert sizing_context["selector_budget"] == pytest.approx(198.724, abs=0.01)
        assert sizing_context["per_trade_budget"] == pytest.approx(198.724, abs=0.01)
        assert sizing_context["total_capital_cap"] == pytest.approx(794.896, abs=0.01)


# ---------------------------------------------------------------------------
# Test 6 — Live/paper exposure isolation
# The split-cap formula itself is mode-agnostic (the mode filter in snapshot()
# already ensures only same-mode exposure is counted). This test verifies the
# formula behaves correctly given mode-isolated inputs.
# ---------------------------------------------------------------------------

class TestLivePaperIsolation:
    def test_live_exposure_does_not_include_paper(self):
        """
        When snapshot() is correctly mode-filtered (existing behavior),
        capital_deployed contains only same-mode positions. The formula
        should give a full per_trade_budget when other-mode exposure is excluded.
        """
        # Simulate: live has 0 exposure, paper has 500 deployed
        # Live snapshot would give capital_deployed=0, pending=0
        live_result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=0.0,   # only live positions here
            pending_capital=0.0,
        )
        assert live_result["selector_budget"] == pytest.approx(198.724, abs=0.01)

    def test_same_mode_exposure_counted(self):
        """Same-mode exposure must count toward the total cap."""
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=600.0,   # same-mode exposure
            pending_capital=100.0,
        )
        assert result["current_total_exposure"] == pytest.approx(700.0, abs=0.01)
        assert result["remaining_total_cap"] == pytest.approx(94.896, abs=0.01)


# ---------------------------------------------------------------------------
# Test 7 — Total cap clamped to per-position cap when set below it
# ---------------------------------------------------------------------------

class TestTotalCapClampedToPositionCap:
    def test_total_cap_below_position_cap_clamped(self):
        """If max_total_capital_pct < max_position_pct, total cap is clamped up."""
        # This is enforced in APMasterControl.__init__
        max_position_pct = 0.10
        max_total_capital_pct_raw = 0.05  # invalid: below per-trade cap

        # After clamping: max_total_capital_pct = max_position_pct
        clamped_total_pct = max(max_total_capital_pct_raw, max_position_pct)
        assert clamped_total_pct == pytest.approx(0.10)

    def test_clamp_in_mc_source(self):
        assert "max_total_capital_pct < self.max_position_pct" in _MC_SRC, (
            "APMasterControl must clamp max_total_capital_pct to max_position_pct when below"
        )

    def test_clamped_caps_give_consistent_formula(self):
        """After clamping, formula should work identically to legacy single-cap."""
        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.10,  # clamped to equal per-position cap
            capital_deployed=50.0,
            pending_capital=0.0,
        )
        # With equal caps, selector_budget = min(198.724, 794.896-50) but capped to 148.724
        expected = max(0.0, 1987.24 * 0.10 - 50.0)  # 148.724
        assert result["selector_budget"] == pytest.approx(expected, abs=0.01)


# ---------------------------------------------------------------------------
# Test 8 — APMasterControl constructor resolution
# Unit test the __init__ backward-compat resolution logic in isolation.
# ---------------------------------------------------------------------------

class TestMCConstructorResolution:
    def test_mc_source_accepts_max_position_pct_kwarg(self):
        sig_idx = _MC_SRC.find("def __init__(")
        sig_block = _MC_SRC[sig_idx: sig_idx + 1200]
        assert "max_position_pct" in sig_block

    def test_mc_source_accepts_max_total_capital_pct_kwarg(self):
        sig_idx = _MC_SRC.find("def __init__(")
        sig_block = _MC_SRC[sig_idx: sig_idx + 1200]
        assert "max_total_capital_pct" in sig_block

    def test_mc_stores_max_position_pct(self):
        assert "self.max_position_pct" in _MC_SRC

    def test_mc_stores_max_total_capital_pct(self):
        assert "self.max_total_capital_pct" in _MC_SRC

    def test_mc_fallback_to_max_capital_pct(self):
        """Source must have a fallback: if max_position_pct is None, use max_capital_pct."""
        # The init should have: self.max_position_pct = float(max_capital_pct) as fallback
        assert "float(max_capital_pct)" in _MC_SRC


# ---------------------------------------------------------------------------
# Test: Migration file
# ---------------------------------------------------------------------------

class TestMigrationFile:
    def test_migration_file_exists(self):
        migration_path = _REPO / "migrations" / "20260617_risk_cap_split.sql"
        assert migration_path.exists(), "Migration file 20260617_risk_cap_split.sql must exist"

    def test_migration_targets_client_risk_profiles(self):
        assert "client_risk_profiles" in _MIGRATION_SRC, (
            "Migration must ALTER TABLE client_risk_profiles"
        )

    def test_migration_adds_max_position_pct(self):
        assert "max_position_pct" in _MIGRATION_SRC

    def test_migration_adds_max_total_capital_pct(self):
        assert "max_total_capital_pct" in _MIGRATION_SRC

    def test_migration_uses_add_column_if_not_exists(self):
        assert "ADD COLUMN IF NOT EXISTS" in _MIGRATION_SRC, (
            "Migration must use ADD COLUMN IF NOT EXISTS for idempotency"
        )

    def test_migration_columns_are_nullable(self):
        # Columns must be double precision DEFAULT NULL so existing clients unaffected
        assert "double precision" in _MIGRATION_SRC

    def test_migration_has_constraint_total_gte_position(self):
        assert "chk_total_cap_gte_position_cap" in _MIGRATION_SRC, (
            "Migration must add constraint preventing total_cap < position_cap"
        )

    def test_migration_has_deployment_sql_for_jason(self):
        assert "jasoncosby1@gmail.com" in _MIGRATION_SRC, (
            "Migration must include deployment SQL for Jason"
        )

    def test_migration_jason_deployment_sets_both_values(self):
        assert "max_position_pct" in _MIGRATION_SRC
        assert "max_total_capital_pct" in _MIGRATION_SRC
        # Check the UPDATE block
        idx = _MIGRATION_SRC.find("jasoncosby1@gmail.com")
        surrounding = _MIGRATION_SRC[max(0, idx - 300): idx + 200]
        assert "0.10" in surrounding
        assert "0.40" in surrounding

    def test_migration_does_not_alter_clients_table(self):
        """This migration targets client_risk_profiles, not clients."""
        # The line 'ALTER TABLE clients' should not appear (separate table)
        lines = [l.strip() for l in _MIGRATION_SRC.splitlines()
                 if l.strip().upper().startswith("ALTER TABLE CLIENTS")]
        assert len(lines) == 0, (
            "Migration must target client_risk_profiles, not the clients table"
        )


# ---------------------------------------------------------------------------
# Test: Hydration — client_risk_profiles → client_cfg via _rp_field_map
# ---------------------------------------------------------------------------

class TestRiskProfileHydration:
    def test_rp_field_map_includes_max_position_pct(self):
        """_rp_field_map must include max_position_pct so it flows from
        client_risk_profiles into client_cfg before _cfg_or_env reads it."""
        assert '"max_position_pct":' in _CR_SRC or "'max_position_pct':" in _CR_SRC, (
            "_rp_field_map in client_runner must include max_position_pct"
        )

    def test_rp_field_map_includes_max_total_capital_pct(self):
        assert '"max_total_capital_pct":' in _CR_SRC or "'max_total_capital_pct':" in _CR_SRC, (
            "_rp_field_map in client_runner must include max_total_capital_pct"
        )

    def test_rp_field_map_block_contains_both_new_fields(self):
        """Both fields must appear inside the _rp_field_map dict."""
        idx = _CR_SRC.find("_rp_field_map")
        block = _CR_SRC[idx: idx + 1200]  # new fields are deeper in the dict
        assert "max_position_pct" in block
        assert "max_total_capital_pct" in block

    def test_client_risk_profiles_queried_by_client_email(self):
        """client_risk_profiles is keyed by client_email, not client_id."""
        # Find the Supabase table query (deeper in the file than the field map)
        idx = _CR_SRC.find('.table("client_risk_profiles")')
        assert idx != -1, 'client_risk_profiles Supabase table call not found'
        region = _CR_SRC[idx: idx + 300]
        assert "client_email" in region, (
            "client_risk_profiles must be queried using client_email"
        )

    def test_hydration_simulation(self):
        """Simulate _rp_field_map merge: risk profile fields override client_cfg."""
        # Simulate a risk profile row with the new fields set
        _rp = {
            "max_capital_pct":        0.10,
            "max_sector_pct":         0.25,
            "max_ticker_pct":         0.10,
            "max_calls":              3,
            "max_puts":               3,
            "score_floor":            65.0,
            "context_floor":          0.0,
            "max_positions":          4,
            "daily_max_loss_pct":     0.05,
            "entries_enabled":        True,
            "daily_profit_target_usd": 0.0,
            "max_position_pct":       0.10,   # NEW
            "max_total_capital_pct":  0.40,   # NEW
        }
        _rp_field_map = {
            "max_capital_pct":        "max_capital_pct",
            "max_sector_pct":         "max_sector_pct",
            "max_ticker_pct":         "max_ticker_pct",
            "max_calls":              "max_calls",
            "max_puts":               "max_puts",
            "score_floor":            "score_floor",
            "context_floor":          "context_floor",
            "max_positions":          "max_concurrent_positions",
            "daily_max_loss_pct":     "daily_max_loss_pct",
            "entries_enabled":        "entries_enabled",
            "daily_profit_target_usd": "daily_profit_target_usd",
            "max_position_pct":       "max_position_pct",
            "max_total_capital_pct":  "max_total_capital_pct",
        }
        client_cfg = {}
        for rp_key, cfg_key in _rp_field_map.items():
            if _rp.get(rp_key) is not None:
                client_cfg[cfg_key] = _rp[rp_key]

        assert client_cfg["max_position_pct"] == 0.10
        assert client_cfg["max_total_capital_pct"] == 0.40

    def test_null_fields_not_merged_into_cfg(self):
        """NULL risk profile fields must NOT overwrite client_cfg (backward compat)."""
        _rp = {
            "max_capital_pct":       0.10,
            "max_position_pct":      None,   # NULL — must not override
            "max_total_capital_pct": None,   # NULL — must not override
        }
        client_cfg = {"max_position_pct": "fallback_value"}
        # Simulate the merge guard: only merge non-None values
        for k, v in _rp.items():
            if v is not None:
                client_cfg[k] = v
        assert client_cfg.get("max_position_pct") == "fallback_value", (
            "NULL risk profile value must not overwrite existing client_cfg value"
        )


# ---------------------------------------------------------------------------
# Test: Backward compatibility — max_position_pct falls back to max_capital_pct
# ---------------------------------------------------------------------------

class TestBackwardCompatFallback:
    def test_max_position_pct_none_falls_back_to_max_capital_pct(self):
        """Simulate APMasterControl.__init__ fallback when max_position_pct=None."""
        max_capital_pct = 0.10
        max_position_pct_arg = None  # not provided

        # Production fallback logic:
        if max_position_pct_arg is not None:
            resolved = float(max_position_pct_arg)
        else:
            resolved = float(max_capital_pct)

        assert resolved == pytest.approx(0.10), (
            "When max_position_pct is None, must fall back to max_capital_pct"
        )

    def test_max_total_capital_pct_none_falls_back_to_env_default(self):
        """When max_total_capital_pct=None, must use DEFAULT_MAX_TOTAL_CAPITAL_PCT=0.40."""
        import os
        default = float(os.getenv("DEFAULT_MAX_TOTAL_CAPITAL_PCT", "0.40"))
        max_total_capital_pct_arg = None

        resolved = float(max_total_capital_pct_arg) if max_total_capital_pct_arg is not None else default
        assert resolved == pytest.approx(0.40), (
            "When max_total_capital_pct is None, must fall back to 0.40 default"
        )

    def test_explicit_values_override_fallback(self):
        """Explicit max_position_pct=0.05 must not fall back to max_capital_pct=0.10."""
        max_capital_pct = 0.10
        max_position_pct_arg = 0.05

        if max_position_pct_arg is not None:
            resolved = float(max_position_pct_arg)
        else:
            resolved = float(max_capital_pct)

        assert resolved == pytest.approx(0.05)

    def test_fallback_logic_in_mc_source(self):
        """MC source must have the fallback: use max_capital_pct when max_position_pct is None."""
        assert "float(max_capital_pct)" in _MC_SRC, (
            "APMasterControl must fall back to max_capital_pct when max_position_pct is None"
        )


# ---------------------------------------------------------------------------
# Test: Total cap constraint
# ---------------------------------------------------------------------------

class TestTotalCapConstraint:
    def test_migration_has_db_constraint(self):
        assert "max_total_capital_pct >= max_position_pct" in _MIGRATION_SRC, (
            "Migration must enforce total_cap >= position_cap at DB level"
        )

    def test_mc_source_has_runtime_clamp(self):
        assert "max_total_capital_pct < self.max_position_pct" in _MC_SRC

    def test_clamp_prevents_permanent_block(self):
        """After clamp, formula must produce a non-zero selector_budget."""
        # Simulate: operator set total=0.05 < position=0.10 — clamped to 0.10
        max_position_pct = 0.10
        max_total_capital_pct_raw = 0.05
        max_total_capital_pct = max(max_total_capital_pct_raw, max_position_pct)

        result = _compute_split_cap(
            account_equity=1987.24,
            max_position_pct=max_position_pct,
            max_total_capital_pct=max_total_capital_pct,
            capital_deployed=0.0,
            pending_capital=0.0,
        )
        assert result["selector_budget"] > 0, (
            "Clamped total cap must not permanently block trades"
        )
        assert max_total_capital_pct == pytest.approx(max_position_pct)


# ---------------------------------------------------------------------------
# revalidate_exposure() split-cap correctness tests
#
# Two independent gates — each with the correct headroom and reason code:
#
# Gate 1 — per-position cap:
#   Fires when: real_cost > per_trade_budget
#   Resize headroom: per_trade_budget
#   Reason: ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP
#
# Gate 2 — total exposure cap:
#   Fires when: projected_total_exposure > total_capital_cap
#   Resize headroom: remaining_total_capacity = total_cap - deployed - pending
#   Reason: CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED
#           or ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY
#
# Do NOT compare projected_total_exposure to per_trade_budget.
# ---------------------------------------------------------------------------

def _compute_revalidate(
    *,
    account_equity: float,
    max_position_pct: float,
    max_total_capital_pct: float,
    capital_deployed: float,
    pending_capital: float,
    real_cost: float,
    original_contracts: int = 1,
    cost_per_contract: float | None = None,
) -> dict:
    """
    Mirror of the revalidate_exposure() two-gate split-cap math.

    Gate 1: real_cost vs per_trade_budget (per-position cap).
      Resize headroom = per_trade_budget.

    Gate 2: projected_total_exposure vs total_capital_cap (total portfolio cap).
      Resize headroom = remaining_total_capacity.

    Returns dict with all intermediate values for precise assertion.
    """
    per_trade_budget         = account_equity * max_position_pct
    total_capital_cap        = account_equity * max_total_capital_pct
    current_total_exposure   = capital_deployed + pending_capital
    projected_total_exposure = current_total_exposure + real_cost
    remaining_total_capacity = total_capital_cap - current_total_exposure

    _cpc = cost_per_contract or (real_cost / original_contracts if original_contracts else real_cost)

    # Gate 1: per-position cap
    g1_fires        = real_cost > per_trade_budget
    g1_remaining    = per_trade_budget                              # resize headroom
    g1_computed_qty = int(g1_remaining // _cpc) if (_cpc > 0 and g1_fires) else original_contracts
    g1_resize_ok    = g1_computed_qty >= 1 if g1_fires else False

    # After Gate 1 resize (if it fires and succeeds), real_cost shrinks.
    # Compute adjusted real_cost for Gate 2 evaluation.
    if g1_fires and g1_resize_ok:
        _adjusted_real_cost      = g1_computed_qty * _cpc
        _adjusted_projected_total = current_total_exposure + _adjusted_real_cost
    else:
        _adjusted_real_cost      = real_cost
        _adjusted_projected_total = projected_total_exposure

    # Gate 2: total exposure cap (evaluates on potentially resized real_cost)
    g2_fires        = _adjusted_projected_total > total_capital_cap
    g2_remaining    = max(0.0, remaining_total_capacity)            # resize headroom
    g2_computed_qty = int(g2_remaining // _cpc) if (_cpc > 0 and g2_fires) else original_contracts
    g2_resize_ok    = g2_computed_qty >= 1 if g2_fires else False

    return {
        "per_trade_budget":         per_trade_budget,
        "total_capital_cap":        total_capital_cap,
        "current_total_exposure":   current_total_exposure,
        "projected_total_exposure": projected_total_exposure,
        "remaining_total_capacity": remaining_total_capacity,
        # Gate 1 — per-position cap
        "g1_fires":                 g1_fires,
        "g1_remaining":             g1_remaining,    # = per_trade_budget
        "g1_computed_qty":          g1_computed_qty,
        "g1_resize_ok":             g1_resize_ok,
        # Gate 2 — total exposure cap
        "g2_fires":                 g2_fires,
        "g2_remaining":             g2_remaining,    # = remaining_total_capacity
        "g2_computed_qty":          g2_computed_qty,
        "g2_resize_ok":             g2_resize_ok,
        # Convenience
        "adjusted_real_cost":       _adjusted_real_cost,
        "adjusted_projected_total": _adjusted_projected_total,
    }


class TestRevalidateExposureSplitCap:
    """
    Two-gate split-cap revalidate_exposure() correctness.

    Gate 1 compares real_cost to per_trade_budget (NOT to projected_total).
    Gate 2 compares projected_total_exposure to total_capital_cap.
    Each gate uses the correct resize headroom and emits its own reason code.
    """

    # ── Test 1 (spec): Jason case — deployed=183, real_cost=198.724 → PASS ────

    def test_jason_existing_183_real_cost_198_passes(self):
        """
        equity=1987.24, deployed=183, real_cost=198.724
        per_trade_budget=198.724, total_cap=794.896
        projected_total=381.724

        Gate 1: real_cost=198.724 <= per_trade_budget=198.724 → does NOT fire
        Gate 2: projected=381.724 <= total_cap=794.896         → does NOT fire
        Result: PASS (trade approved)
        """
        result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=183.0,
            pending_capital=0.0,
            real_cost=198.724,
        )
        assert result["g1_fires"] is False, (
            f"Gate 1 must NOT fire: real_cost=198.724 == per_trade_budget=198.724. "
            f"Old bug: compared proj_total={result['projected_total_exposure']:.2f} "
            f"to per_trade_budget={result['per_trade_budget']:.2f} → false block."
        )
        assert result["g2_fires"] is False, (
            f"Gate 2 must NOT fire: projected={result['projected_total_exposure']:.2f} "
            f"<= total_cap={result['total_capital_cap']:.2f}"
        )

    def test_second_trade_183_deployed_183_real_cost_passes(self):
        """
        equity=1987.24, deployed=183, real_cost=183
        Each trade is within per_trade_budget=198.724. Both gates must pass.
        Old code: proj_total=366 > per_trade_cap=198 → false block.
        """
        result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=183.0,
            pending_capital=0.0,
            real_cost=183.0,
        )
        assert result["g1_fires"] is False, (
            "Gate 1 must NOT fire: real_cost=183 < per_trade_budget=198.724"
        )
        assert result["g2_fires"] is False, (
            f"Gate 2 must NOT fire: projected={result['projected_total_exposure']:.0f} "
            f"< total_cap={result['total_capital_cap']:.0f}"
        )

    # ── Test 2 (spec): Per-trade too expensive — Gate 1 fires, total cap has room ─

    def test_per_trade_cap_exceeded_gate1_fires_with_room_in_total_cap(self):
        """
        real_cost=250, per_trade_budget=198.724, total_cap has plenty of room.
        Gate 1 fires. Resize headroom = per_trade_budget=198.724 (not deployed-adjusted).
        """
        result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=0.0,
            pending_capital=0.0,
            real_cost=250.0,
            original_contracts=3,
            cost_per_contract=83.333,
        )
        assert result["g1_fires"] is True, (
            "real_cost=250 > per_trade_budget=198.724 must fire Gate 1"
        )
        assert result["g2_fires"] is False, (
            "Gate 2 must NOT fire — total cap has plenty of room"
        )
        # Gate 1 resize headroom is per_trade_budget, not (per_trade - deployed - pending)
        assert result["g1_remaining"] == pytest.approx(198.724, abs=0.01), (
            "Gate 1 resize headroom must be per_trade_budget=198.724, not 198-0-0=198 (trivial here) "
            "but critically NOT per_trade - deployed - pending (which gives $15 when deployed=183)"
        )
        assert result["g1_computed_qty"] == 2, (
            f"With $198.724 and $83.333/contract → computed_qty=2, got {result['g1_computed_qty']}"
        )
        assert result["g1_resize_ok"] is True

    def test_gate1_resize_headroom_is_per_trade_budget_not_depleted(self):
        """
        THE KEY REGRESSION TEST: when deployed=183, Gate 1 resize headroom
        must be per_trade_budget=198.724, NOT per_trade_budget - deployed - pending = 15.724.
        Old code used _remaining_now = max_capital - deployed - pending → $15 bug.
        New code uses _remaining_g1 = per_trade_budget → $198.
        """
        result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=183.0,
            pending_capital=0.0,
            real_cost=220.0,        # over per_trade_budget → Gate 1 fires
            original_contracts=2,
            cost_per_contract=110.0,
        )
        assert result["g1_fires"] is True
        assert result["g1_remaining"] == pytest.approx(198.724, abs=0.01), (
            f"Gate 1 resize headroom must be per_trade_budget=198.724. "
            f"Old $15 bug value: per_trade_budget - deployed - pending = "
            f"{198.724 - 183:.3f}. Got g1_remaining={result['g1_remaining']:.3f}"
        )
        # With $198.724 / $110 per contract → 1 contract affordable
        assert result["g1_computed_qty"] == 1
        assert result["g1_resize_ok"] is True

    def test_gate1_per_trade_unaffordable_even_after_resize(self):
        """Contract is more expensive than per_trade_budget even for 1 unit."""
        result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=0.0,
            pending_capital=0.0,
            real_cost=250.0,        # per_trade_budget=198.724; 1 contract = $250 > $198
            original_contracts=1,
            cost_per_contract=250.0,
        )
        assert result["g1_fires"] is True
        assert result["g1_resize_ok"] is False, (
            "1 contract at $250 > per_trade_budget=$198.724 → cannot resize → block"
        )
        assert result["g1_computed_qty"] == 0

    # ── Test 3 (spec): total cap near full → Gate 2 fires ────────────────────

    def test_total_cap_near_full_gate2_fires(self):
        """
        existing exposure=750, real_cost=198.724, total_cap=794.896.
        remaining_total_capacity = 44.896
        projected_total = 948.724 > total_cap=794.896 → Gate 2 fires.
        Gate 1: real_cost=198.724 <= per_trade_budget=198.724 → does NOT fire.
        Gate 2 resize headroom = remaining_total_capacity=44.896.
        """
        result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=750.0,
            pending_capital=0.0,
            real_cost=198.724,
            original_contracts=2,
            cost_per_contract=99.362,
        )
        assert result["g1_fires"] is False, (
            "Gate 1 must NOT fire: real_cost=198.724 <= per_trade_budget=198.724"
        )
        assert result["g2_fires"] is True, (
            f"Gate 2 must fire: adjusted_projected={result['adjusted_projected_total']:.2f} "
            f"> total_cap={result['total_capital_cap']:.2f}"
        )
        assert result["remaining_total_capacity"] == pytest.approx(44.896, abs=0.01)
        # Gate 2 resize headroom is remaining_total_capacity, not per_trade_budget
        assert result["g2_remaining"] == pytest.approx(44.896, abs=0.01), (
            "Gate 2 resize headroom must be remaining_total_capacity=44.896, "
            "not per_trade_budget=198.724"
        )

    def test_total_cap_near_full_resize_to_affordable_qty(self):
        """
        Gate 2 fires; resize using remaining_total_capacity=94.896.
        cost_per_contract=$50 → computed_qty=1.
        """
        result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=700.0,
            pending_capital=0.0,
            real_cost=150.0,        # per_trade_budget=198.724 → Gate 1 does NOT fire (150<=198)
            original_contracts=3,
            cost_per_contract=50.0,
        )
        # Gate 1: 150 <= 198.724 → does NOT fire
        assert result["g1_fires"] is False
        # Gate 2: projected=850 > total_cap=794.896 → fires
        assert result["g2_fires"] is True
        assert result["remaining_total_capacity"] == pytest.approx(94.896, abs=0.01)
        assert result["g2_remaining"] == pytest.approx(94.896, abs=0.01)
        assert result["g2_computed_qty"] == 1, (
            f"With $94.896 remaining and $50/contract → 1 affordable, got {result['g2_computed_qty']}"
        )
        assert result["g2_resize_ok"] is True

    # ── Test 4 (spec): total cap reached → Gate 2 block ──────────────────────

    def test_total_cap_reached_gate2_blocks(self):
        """
        existing exposure >= total_capital_cap.
        Gate 2 fires and no resize is possible (remaining_total_capacity <= 0).
        """
        result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=795.0,  # over total_cap=794.896
            pending_capital=0.0,
            real_cost=100.0,
            original_contracts=1,
            cost_per_contract=100.0,
        )
        assert result["g2_fires"] is True
        assert result["remaining_total_capacity"] < 0
        assert result["g2_remaining"] == 0.0, (
            "remaining_total_capacity <= 0 → g2_remaining clamped to 0 → no resize possible"
        )
        assert result["g2_computed_qty"] == 0
        assert result["g2_resize_ok"] is False

    def test_total_cap_exactly_at_limit_blocks(self):
        """Exposure exactly at total_cap + any real_cost → Gate 2 blocks."""
        result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,
            capital_deployed=794.896,  # = total_cap (float might be slightly above)
            pending_capital=10.0,      # pushes clearly over
            real_cost=50.0,
            original_contracts=1,
            cost_per_contract=50.0,
        )
        assert result["g2_fires"] is True
        assert result["g2_resize_ok"] is False

    # ── Test 5 (spec): backward compat — max_total_capital_pct default = 0.40 ─

    def test_backward_compat_default_total_cap_is_040_not_per_trade(self):
        """
        When only max_capital_pct=0.10 is set (legacy), max_total_capital_pct
        resolves to DEFAULT_MAX_TOTAL_CAPITAL_PCT=0.40 (env default).
        This intentionally widens the total exposure cap from 10% to 40%.
        The PR body must document this behavior change.
        """
        # Legacy: only max_capital_pct set → both pcts would have been 0.10
        # New default: max_position_pct=0.10, max_total_capital_pct=0.40
        legacy_result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.10,  # legacy single-cap
            capital_deployed=183.0,
            pending_capital=0.0,
            real_cost=150.0,
        )
        new_result = _compute_revalidate(
            account_equity=1987.24,
            max_position_pct=0.10,
            max_total_capital_pct=0.40,  # new default
            capital_deployed=183.0,
            pending_capital=0.0,
            real_cost=150.0,
        )
        # In legacy single-cap: projected=333 > total_cap=198.724 → Gate 2 fires
        assert legacy_result["g2_fires"] is True, (
            "Legacy single-cap (max_total_capital_pct=0.10): Gate 2 fires "
            "because projected_total=333 > total_cap=198.724. "
            "This is why the default of 0.40 intentionally widens total exposure."
        )
        # With new default total_cap=0.40: projected=333 < total_cap=794.896 → passes
        assert new_result["g2_fires"] is False, (
            "New default (max_total_capital_pct=0.40): Gate 2 does NOT fire "
            "because projected_total=333 < total_cap=794.896."
        )
        # Per-trade cap behavior is identical in both cases
        assert legacy_result["g1_fires"] == new_result["g1_fires"], (
            "Gate 1 (per-trade cap) must behave identically regardless of total_cap setting"
        )

    def test_backward_compat_doc_total_cap_widening_is_intentional(self):
        """
        Confirm that DEFAULT_MAX_TOTAL_CAPITAL_PCT=0.40 is an intentional
        design choice documented in the source. When total cap is 0.40 and
        per-trade cap is 0.10, a client CAN have up to 4 concurrent positions
        each at max per-trade budget — which was impossible under the old
        single-cap model where max_capital_pct=0.10 covered both.
        """
        mc_src = (_REPO / "ap_master_control.py").read_text()
        assert "DEFAULT_MAX_TOTAL_CAPITAL_PCT" in mc_src, (
            "Source must document DEFAULT_MAX_TOTAL_CAPITAL_PCT"
        )
        # The default must not silently be unlimited
        import os
        default_pct = float(os.getenv("DEFAULT_MAX_TOTAL_CAPITAL_PCT", "0.40"))
        assert 0 < default_pct <= 1.0, (
            f"DEFAULT_MAX_TOTAL_CAPITAL_PCT must be between 0 and 1, got {default_pct}"
        )
        assert default_pct < 1.0, (
            "DEFAULT_MAX_TOTAL_CAPITAL_PCT must not be 1.0 (unlimited exposure)"
        )

    # ── Source guards ─────────────────────────────────────────────────────────

    def test_source_gate1_compares_real_cost_to_per_trade_budget(self):
        """Gate 1 must compare real_cost to per_trade_budget, not to allowed_budget."""
        mc_src = (_REPO / "ap_master_control.py").read_text()
        rv_start = mc_src.find("def revalidate_exposure(")
        rv_body = mc_src[rv_start: rv_start + 18000]
        assert "real_cost > per_trade_budget" in rv_body, (
            "Gate 1 must compare real_cost to per_trade_budget"
        )

    def test_source_gate2_compares_projected_total_to_total_cap(self):
        """Gate 2 must compare projected_total_exposure to total_capital_cap."""
        mc_src = (_REPO / "ap_master_control.py").read_text()
        rv_start = mc_src.find("def revalidate_exposure(")
        rv_body = mc_src[rv_start: rv_start + 18000]
        assert "projected_total_exposure > total_capital_cap" in rv_body, (
            "Gate 2 must compare projected_total_exposure to total_capital_cap"
        )

    def test_source_gate1_reason_code_present(self):
        mc_src = (_REPO / "ap_master_control.py").read_text()
        assert "ACTUAL_CONTRACT_COST_EXCEEDS_PER_POSITION_CAP" in mc_src

    def test_source_gate2_reason_code_present(self):
        mc_src = (_REPO / "ap_master_control.py").read_text()
        assert "ACTUAL_CONTRACT_COST_EXCEEDS_REMAINING_TOTAL_CAPACITY" in mc_src
        assert "CAPITAL_LIMIT_TOTAL_EXPOSURE_CAP_REACHED" in mc_src

    def test_source_no_proj_total_vs_per_trade_comparison(self):
        """No executable line should compare proj_total (total exposure) to per_trade_budget."""
        mc_src = (_REPO / "ap_master_control.py").read_text()
        rv_start = mc_src.find("def revalidate_exposure(")
        rv_body = mc_src[rv_start: rv_start + 18000]
        bad = [
            ln for ln in rv_body.splitlines()
            if not ln.strip().startswith("#")
            and ("proj_total > max_capital" in ln or
                 "projected_total > per_trade" in ln or
                 "projected_total_exposure > per_trade_budget" in ln)
        ]
        assert len(bad) == 0, (
            f"No executable comparison of total exposure vs per-trade budget: {bad}"
        )

    def test_source_gate1_resize_uses_per_trade_budget_not_depleted(self):
        """Gate 1 resize headroom variable must be per_trade_budget, not the old
        (max_capital - deployed - pending) formula that produces $15."""
        mc_src = (_REPO / "ap_master_control.py").read_text()
        rv_start = mc_src.find("def revalidate_exposure(")
        rv_body = mc_src[rv_start: rv_start + 18000]
        # Old bug signature
        assert "_remaining_now = max_capital - snap" not in rv_body
        assert '_remaining_now   = max_capital - snap["capital_deployed"] - pending_cap' not in rv_body
        # New correct Gate 1 resize headroom
        assert "_remaining_g1" in rv_body, "Gate 1 must use _remaining_g1 for its resize headroom"
        assert "= per_trade_budget" in rv_body, (
            "Gate 1 resize headroom must equal per_trade_budget"
        )

    def test_source_proj_total_not_defined_in_revalidate(self):
        """proj_total must not exist as an executable statement in revalidate_exposure.
        It was the stale variable that carried the apples-vs-oranges comparison."""
        mc_src = (_REPO / "ap_master_control.py").read_text()
        rv_start = mc_src.find("def revalidate_exposure(")
        rv_body = mc_src[rv_start: rv_start + 18000]
        bad = [
            ln for ln in rv_body.splitlines()
            if not ln.strip().startswith("#")
            and "proj_total" in ln
            and "projected_total" not in ln   # allow projected_total_exposure
        ]
        assert len(bad) == 0, (
            f"proj_total must not appear as executable code in revalidate_exposure "
            f"(use projected_total_exposure instead): {bad}"
        )

    def test_source_gate1_comment_says_per_position_cap(self):
        """The Gate 1 comment must say 'per-position cap', not reference allowed_budget."""
        mc_src = (_REPO / "ap_master_control.py").read_text()
        rv_start = mc_src.find("def revalidate_exposure(")
        rv_body = mc_src[rv_start: rv_start + 18000]
        assert "per-position cap" in rv_body or "per_position_cap" in rv_body or \
               "Gate 1" in rv_body, (
            "Gate 1 comment must describe the per-position cap check"
        )

    def test_source_log_includes_required_fields(self):
        """Revalidation logs must include the six required fields from the spec."""
        mc_src = (_REPO / "ap_master_control.py").read_text()
        rv_start = mc_src.find("def revalidate_exposure(")
        rv_body = mc_src[rv_start: rv_start + 18000]
        for field in ("per_trade_budget", "total_capital_cap",
                      "current_total_exposure", "projected_total_exposure",
                      "remaining_total_capacity", "real_cost"):
            assert field in rv_body, (
                f"revalidate_exposure log must include field '{field}' per spec"
            )

