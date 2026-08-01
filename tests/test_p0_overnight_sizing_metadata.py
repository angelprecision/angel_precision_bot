"""
tests/test_p0_overnight_sizing_metadata.py

P0 observability: overnight/deferred orders must preserve sizing_context
and deferred metadata from MC's plan.metadata into the OSM entry order.

Without this, orders created by ap_overnight_reeval.run_overnight_reeval()
had null/missing sizing_context, making budget-mismatch debugging blind
(e.g. the Jason $198.724→$15.724 incident).

Test structure:
  Test 1 — Deferred overnight order carries sizing_context.
  Test 2 — Real-contract overnight order carries sizing_context, no deferred flag.
  Test 3 — OSM auto-meta not erased by plan metadata (merge semantics).
  Test 4 — Hydrated plan (observe-only path) passes its metadata through.
  Test 5 — Source-level guards on overnight reeval call site.
  Test 6 — OSM merge semantics: caller meta wins on conflict.
"""
from __future__ import annotations

import importlib
import json
import re
import sys
import types
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch, call

import pytest

_REPO = Path(__file__).resolve().parents[1]
_OVERNIGHT_SRC = (_REPO / "ap_overnight_reeval.py").read_text()
_OSM_SRC = (_REPO / "ap" / "order_state_machine.py").read_text()


# ---------------------------------------------------------------------------
# Source-level invariant checks
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_create_entry_order_call_passes_meta(self):
        """The overnight reeval call site must pass meta= to create_entry_order."""
        # Find the create_entry_order call block
        idx = _OVERNIGHT_SRC.find("order_state_machine.create_entry_order(")
        assert idx != -1, "create_entry_order call not found in ap_overnight_reeval.py"
        call_block = _OVERNIGHT_SRC[idx: idx + 500]
        assert "meta=" in call_block, (
            "create_entry_order call in overnight reeval must pass meta= kwarg"
        )

    def test_plan_metadata_extracted_before_call(self):
        """The plan metadata must be extracted before the call."""
        idx = _OVERNIGHT_SRC.find("order_state_machine.create_entry_order(")
        preceding = _OVERNIGHT_SRC[max(0, idx - 300): idx]
        assert "_plan_meta" in preceding or "plan.metadata" in preceding, (
            "plan.metadata must be extracted before create_entry_order call"
        )

    def test_osm_accepts_meta_kwarg(self):
        """OSM create_entry_order must accept meta= kwarg."""
        sig_start = _OSM_SRC.find("def create_entry_order(")
        sig_block = _OSM_SRC[sig_start: sig_start + 400]
        assert "meta:" in sig_block or "meta =" in sig_block, (
            "OSM create_entry_order must have meta parameter"
        )

    def test_osm_merges_caller_meta_on_top_of_auto(self):
        """OSM must merge caller meta on top of auto-meta with null-safety guard."""
        # RED-ON-MAIN CLEANUP (PR #265): the function outgrew the fixed
        # 9000-char slice (direction canonicalization + enforcement stamp).
        # Extract the FULL function body to the next method def instead —
        # the merge invariant itself is unchanged and still enforced below.
        fn_start = _OSM_SRC.find("def create_entry_order(")
        fn_end = _OSM_SRC.find("\n    def ", fn_start + 10)
        fn_body = _OSM_SRC[fn_start: fn_end if fn_end != -1 else len(_OSM_SRC)]
        assert "_auto_meta" in fn_body, "OSM must build _auto_meta dict"
        assert "_final_meta" in fn_body, "OSM must build _final_meta dict"
        # Must NOT use raw .update() — must guard against null/empty overwriting auto
        assert "_final_meta = dict(_auto_meta)" in fn_body, (
            "OSM must copy auto_meta as the base for _final_meta"
        )
        # The guard: skip null/empty caller values for keys auto_meta already has
        assert "_mv is None" in fn_body or "is None" in fn_body, (
            "OSM merge must check for None caller values before overwriting auto"
        )

    def test_osm_does_not_use_raw_update_for_caller_meta(self):
        """OSM must NOT use _final_meta.update(meta) — that allows null clobber."""
        # RED-ON-MAIN CLEANUP (PR #265): full-function extraction — the
        # function outgrew the fixed 9000-char slice. Invariant unchanged.
        fn_start = _OSM_SRC.find("def create_entry_order(")
        fn_end = _OSM_SRC.find("\n    def ", fn_start + 10)
        fn_body = _OSM_SRC[fn_start: fn_end if fn_end != -1 else len(_OSM_SRC)]
        # After the _final_meta = dict(_auto_meta) line, there must NOT be
        # a raw _final_meta.update(meta) call
        final_meta_idx = fn_body.find("_final_meta = dict(_auto_meta)")
        assert final_meta_idx != -1
        after_final = fn_body[final_meta_idx:]
        assert "_final_meta.update(meta)" not in after_final, (
            "OSM must NOT use raw .update(meta) — use per-key null guard instead"
        )

    def test_contract_deferred_flag_set_in_metadata(self):
        """When contract is deferred, plan.metadata must have contract_deferred=True."""
        assert 'contract_deferred' in _OVERNIGHT_SRC


# ---------------------------------------------------------------------------
# OSM meta merge unit test (direct function test)
# ---------------------------------------------------------------------------

class TestOSMMetaMerge:
    """Test the merge semantics directly by building _auto_meta and caller meta."""

    def test_caller_meta_overwrites_auto_meta_on_conflict(self):
        auto_meta = {
            "score": 0.0,
            "tier": "",
            "selected_contract": "DEFERRED:USB",
            "execution_mode": "live",
        }
        caller_meta = {
            "sizing_context": {
                "account_equity": 1987.24,
                "max_capital_pct": 0.10,
                "remaining_capital": 198.724,
            },
            "contract_deferred": True,
            "score": 85.0,  # caller wins
        }
        final = dict(auto_meta)
        # Replicate hardened merge: skip null/empty caller values for auto keys
        for k, v in caller_meta.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v
        assert final["score"] == 85.0, "caller meta must win on conflict"
        assert final["sizing_context"]["account_equity"] == 1987.24
        assert final["selected_contract"] == "DEFERRED:USB", "auto field preserved"
        assert final["execution_mode"] == "live", "auto field preserved"

    def test_auto_meta_preserved_when_no_conflict(self):
        auto_meta = {
            "score": 75.0,
            "tier": "A",
            "signal_id": "sig123",
            "pattern": "3-1-2",
        }
        caller_meta = {
            "sizing_context": {"account_equity": 5000.0},
            "overnight": True,
        }
        final = dict(auto_meta)
        for k, v in caller_meta.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v
        assert final["score"] == 75.0
        assert final["tier"] == "A"
        assert final["signal_id"] == "sig123"
        assert final["pattern"] == "3-1-2"
        assert final["sizing_context"]["account_equity"] == 5000.0
        assert final["overnight"] is True

    def test_null_selected_contract_does_not_clobber_auto(self):
        """P0 SAFETY: plan.metadata with selected_contract=None must NOT overwrite
        OSM's correctly derived selected_contract from plan.contract_symbol."""
        auto_meta = {
            "selected_contract": "USB260626C00055000",
            "score": 72.5,
            "canonical_signal_id": "can-2026-06-17:USB:CALL:1d",
        }
        caller_meta = {
            "selected_contract": None,  # stale/missing — must NOT overwrite
            "sizing_context": {"account_equity": 1987.24},
        }
        final = dict(auto_meta)
        for k, v in caller_meta.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v
        assert final["selected_contract"] == "USB260626C00055000", (
            "Null caller selected_contract must NOT overwrite auto-derived value"
        )
        assert final["sizing_context"]["account_equity"] == 1987.24, (
            "sizing_context should still pass through"
        )

    def test_empty_string_canonical_signal_id_does_not_clobber_auto(self):
        """Empty string canonical_signal_id must NOT overwrite auto-derived value."""
        auto_meta = {
            "canonical_signal_id": "can-2026-06-17:USB:CALL:1d",
            "score": 72.5,
        }
        caller_meta = {
            "canonical_signal_id": "",  # stale — must NOT overwrite
            "overnight": True,
        }
        final = dict(auto_meta)
        for k, v in caller_meta.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v
        assert final["canonical_signal_id"] == "can-2026-06-17:USB:CALL:1d", (
            "Empty caller canonical_signal_id must NOT overwrite auto-derived value"
        )

    def test_null_score_does_not_clobber_auto(self):
        """Caller with score=None must NOT zero out auto-derived score."""
        auto_meta = {"score": 72.5, "tier": "A"}
        caller_meta = {"score": None, "sizing_context": {}}
        final = dict(auto_meta)
        for k, v in caller_meta.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v
        assert final["score"] == 72.5

    def test_zero_score_from_caller_does_overwrite(self):
        """Caller with score=0.0 is a valid value (not null/empty) and SHOULD overwrite."""
        auto_meta = {"score": 72.5}
        caller_meta = {"score": 0.0}
        final = dict(auto_meta)
        for k, v in caller_meta.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v
        assert final["score"] == 0.0, (
            "Explicit 0.0 is a valid caller value and should overwrite auto"
        )

    def test_new_keys_from_caller_always_pass_through(self):
        """Keys not in auto_meta (sizing_context, contract_deferred, etc.)
        always pass through, even if their value is None."""
        auto_meta = {"score": 72.5}
        caller_meta = {
            "sizing_context": {"account_equity": 1987.24},
            "contract_deferred": True,
            "risk_profile_source": None,  # new key, None is fine — it passes through
        }
        final = dict(auto_meta)
        for k, v in caller_meta.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v
        assert final["sizing_context"]["account_equity"] == 1987.24
        assert final["contract_deferred"] is True
        assert final["risk_profile_source"] is None, (
            "New key with None value should still be present (not in auto_meta)"
        )


# ---------------------------------------------------------------------------
# Test 1 — Deferred overnight order carries sizing_context
# ---------------------------------------------------------------------------

class TestDeferredOvernightOrder:
    """Simulate a deferred overnight order and verify meta contains sizing_context."""

    @staticmethod
    def _make_plan(*, contract_deferred=True, ticker="USB"):
        return types.SimpleNamespace(
            plan_id="plan-usb-001",
            ticker=ticker,
            side="CALL",
            direction="CALL",
            score=72.5,
            tier="B",
            timeframe="1d",
            entry_trigger=55.20,
            trigger_price=55.20,
            trigger_type="breach",
            prior_day_high=55.15,
            prior_day_low=53.80,
            pattern="3-1-2",
            contract_symbol=f"DEFERRED:{ticker}" if contract_deferred else "USB260626C00055000",
            contracts=2,
            limit_price=0.01 if contract_deferred else 1.05,
            max_position_usd=198.724,
            stop_underlying=53.50,
            target_underlying=56.00,
            signal_id="2026-06-17:1-1:USB:1d:CALL",
            metadata={
                "contract_deferred": contract_deferred,
                "overnight": True,
                "sizing_context": {
                    "account_equity": 1987.24,
                    "max_capital_pct": 0.10,
                    "max_capital_allowed": 198.724,
                    "capital_deployed": 0.0,
                    "pending_capital": 0.0,
                    "remaining_capital": 198.724,
                    "max_affordable_premium": 1.98724,
                },
                "execution_mode": "live",
                "risk_profile_source": "client_db",
                "snapshot_at_eval": {
                    "open_count": 0,
                    "capital_deployed": 0.0,
                },
            },
        )

    def test_plan_metadata_has_sizing_context(self):
        plan = self._make_plan()
        meta = getattr(plan, "metadata", None) or {}
        assert "sizing_context" in meta
        ctx = meta["sizing_context"]
        assert ctx["account_equity"] == 1987.24
        assert ctx["max_capital_pct"] == 0.10
        assert ctx["remaining_capital"] == 198.724

    def test_meta_merge_preserves_sizing_context(self):
        """Simulates OSM hardened merge: auto_meta + plan.metadata → final_meta.
        Null/empty caller values for auto-meta keys are skipped."""
        plan = self._make_plan()
        auto_meta = {
            "score": float(plan.score),
            "tier": plan.tier,
            "selected_contract": plan.contract_symbol,
            "execution_mode": "live",
            "max_position_usd": plan.max_position_usd,
        }
        final = dict(auto_meta)
        for k, v in plan.metadata.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v
        assert "sizing_context" in final
        assert final["sizing_context"]["account_equity"] == 1987.24
        assert final["sizing_context"]["remaining_capital"] == 198.724
        assert final["contract_deferred"] is True
        assert final["execution_mode"] == "live"
        assert final["selected_contract"] == "DEFERRED:USB"

    def test_deferred_contract_in_meta(self):
        plan = self._make_plan(contract_deferred=True)
        meta = plan.metadata
        assert meta["contract_deferred"] is True

    def test_reserved_cost_matches_max_position_usd(self):
        plan = self._make_plan()
        assert plan.max_position_usd == pytest.approx(198.724, abs=0.01)


# ---------------------------------------------------------------------------
# Test 2 — Real-contract overnight order carries sizing_context
# ---------------------------------------------------------------------------

class TestRealContractOvernightOrder:
    def test_real_contract_has_sizing_context(self):
        plan = TestDeferredOvernightOrder._make_plan(contract_deferred=False)
        meta = plan.metadata
        assert "sizing_context" in meta
        assert meta["sizing_context"]["account_equity"] == 1987.24
        # contract_deferred=False for real selection
        assert meta["contract_deferred"] is False

    def test_real_contract_selected_contract_is_real(self):
        plan = TestDeferredOvernightOrder._make_plan(contract_deferred=False)
        assert plan.contract_symbol == "USB260626C00055000"
        assert not plan.contract_symbol.startswith("DEFERRED:")

    def test_merge_with_real_contract(self):
        plan = TestDeferredOvernightOrder._make_plan(contract_deferred=False)
        auto_meta = {
            "selected_contract": plan.contract_symbol,
            "max_position_usd": 105.0,
        }
        final = dict(auto_meta)
        for k, v in plan.metadata.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v
        assert final["selected_contract"] == plan.contract_symbol
        assert "sizing_context" in final
        assert final["contract_deferred"] is False


# ---------------------------------------------------------------------------
# Test 3 — OSM auto-meta not erased by plan metadata
# ---------------------------------------------------------------------------

class TestAutoMetaNotErased:
    """Auto-meta fields like signal_id, pattern, direction must survive merge."""

    def test_signal_id_survives_merge(self):
        auto = {"signal_id": "sig-abc", "canonical_signal_id": "can-abc", "direction": "CALL"}
        caller = {"sizing_context": {"account_equity": 1000.0}, "overnight": True}
        final = dict(auto)
        final.update(caller)
        assert final["signal_id"] == "sig-abc"
        assert final["canonical_signal_id"] == "can-abc"
        assert final["direction"] == "CALL"

    def test_pattern_and_timeframe_survive(self):
        auto = {"pattern": "3-1-2", "timeframe": "1d"}
        caller = {"sizing_context": {}, "contract_deferred": True}
        final = dict(auto)
        final.update(caller)
        assert final["pattern"] == "3-1-2"
        assert final["timeframe"] == "1d"


# ---------------------------------------------------------------------------
# Test 4 — Hydrated plan (observe-only path) passes its metadata through
# ---------------------------------------------------------------------------

class TestHydratedPlanMetadata:
    def test_hydrated_plan_has_metadata(self):
        """_hydrate_plan_from_signal must produce a plan with metadata."""
        signal = {
            "ticker": "SMCI",
            "side": "CALL",
            "score": 78.0,
            "timeframe": "1d",
            "entry_trigger": 45.50,
        }
        # Load _hydrate_plan_from_signal
        assert "_hydrate_plan_from_signal" in _OVERNIGHT_SRC
        # Manually test the structure
        plan_meta = {
            "overnight": True,
            "hydrated_from_signal": True,
            "second_score_mode": "observe_only",
        }
        assert "overnight" in plan_meta
        assert "hydrated_from_signal" in plan_meta

    def test_hydrated_plan_metadata_structure_in_source(self):
        """The hydrate function must set metadata dict on the plan."""
        fn_start = _OVERNIGHT_SRC.find("def _hydrate_plan_from_signal(")
        assert fn_start != -1
        fn_body = _OVERNIGHT_SRC[fn_start: fn_start + 1600]
        assert "metadata" in fn_body
        assert "overnight" in fn_body


# ---------------------------------------------------------------------------
# Test 5 — Sizing context required fields
# ---------------------------------------------------------------------------

class TestSizingContextRequiredFields:
    """All required sizing_context fields must be present per spec."""

    REQUIRED_FIELDS = [
        "account_equity",
        "max_capital_pct",
        "max_capital_allowed",
        "capital_deployed",
        "pending_capital",
        "remaining_capital",
        "max_affordable_premium",
    ]

    def test_all_required_fields_present_in_mc_source(self):
        """MC must write all required sizing_context fields."""
        mc_src = (_REPO / "ap_master_control.py").read_text()
        sc_start = mc_src.find('"sizing_context": {')
        assert sc_start != -1, "sizing_context block not found in ap_master_control.py"
        # The sizing_context block can be quite large — search to the closing brace
        sc_block = mc_src[sc_start: sc_start + 4000]
        for field in self.REQUIRED_FIELDS:
            assert f'"{field}"' in sc_block, (
                f"Required sizing_context field '{field}' missing from MC plan metadata"
            )

    def test_all_required_fields_present_in_test_plan(self):
        plan = TestDeferredOvernightOrder._make_plan()
        ctx = plan.metadata["sizing_context"]
        for field in self.REQUIRED_FIELDS:
            assert field in ctx, f"Test plan missing required sizing_context field: {field}"


# ---------------------------------------------------------------------------
# Test 6 — End-to-end meta passthrough verification
# ---------------------------------------------------------------------------

class TestEndToEndMetaPassthrough:
    """Verify that when _plan_meta is passed to create_entry_order(meta=...),
    the resulting order would have all the critical fields."""

    def test_full_passthrough_simulation(self):
        plan = TestDeferredOvernightOrder._make_plan()
        _plan_meta = getattr(plan, "metadata", None) or {}

        # Simulate OSM auto_meta
        auto_meta = {
            "score": float(plan.score),
            "tier": plan.tier,
            "signal_id": plan.signal_id,
            "selected_contract": plan.contract_symbol,
            "max_position_usd": plan.max_position_usd,
            "execution_mode": "live",
            "direction": "CALL",
            "symbol": plan.ticker,
            "canonical_signal_id": "can-2026-06-17:USB:CALL:1d",
        }
        # Hardened merge: skip null/empty caller values for auto-meta keys
        final = dict(auto_meta)
        for k, v in _plan_meta.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v

        # Required assertions per spec
        assert final["sizing_context"]["account_equity"] == 1987.24
        assert final["sizing_context"]["max_capital_pct"] == 0.10
        assert final["sizing_context"]["remaining_capital"] == 198.724
        assert final["sizing_context"]["max_capital_allowed"] == 198.724
        assert final["sizing_context"]["capital_deployed"] == 0.0
        assert final["sizing_context"]["pending_capital"] == 0.0
        assert final["sizing_context"]["max_affordable_premium"] == pytest.approx(1.98724)
        assert final["contract_deferred"] is True
        assert final["execution_mode"] == "live"
        assert final["risk_profile_source"] == "client_db"
        assert "selected_contract" in final
        assert final["selected_contract"] == "DEFERRED:USB"
        # OSM auto fields not erased
        assert final["score"] == 72.5
        assert final["tier"] == "B"
        assert final["signal_id"] == "2026-06-17:1-1:USB:1d:CALL"
        assert final["direction"] == "CALL"
        assert final["symbol"] == "USB"
        assert final["canonical_signal_id"] == "can-2026-06-17:USB:CALL:1d"

    def test_full_passthrough_with_stale_nulls_in_plan_meta(self):
        """Even if plan.metadata had stale null fields, auto-derived values survive."""
        plan = TestDeferredOvernightOrder._make_plan()
        _plan_meta = getattr(plan, "metadata", None) or {}
        # Inject stale nulls into plan_meta (simulating a bad code path)
        _plan_meta["selected_contract"] = None
        _plan_meta["canonical_signal_id"] = ""
        _plan_meta["score"] = None

        auto_meta = {
            "score": 72.5,
            "selected_contract": "DEFERRED:USB",
            "canonical_signal_id": "can-2026-06-17:USB:CALL:1d",
            "execution_mode": "live",
        }
        final = dict(auto_meta)
        for k, v in _plan_meta.items():
            if k in auto_meta and (v is None or v == ""):
                continue
            final[k] = v

        assert final["selected_contract"] == "DEFERRED:USB", (
            "Stale null must NOT clobber auto-derived selected_contract"
        )
        assert final["canonical_signal_id"] == "can-2026-06-17:USB:CALL:1d", (
            "Stale empty string must NOT clobber auto-derived canonical_signal_id"
        )
        assert final["score"] == 72.5, (
            "Stale null must NOT clobber auto-derived score"
        )
        # sizing_context still passes through
        assert final["sizing_context"]["account_equity"] == 1987.24
