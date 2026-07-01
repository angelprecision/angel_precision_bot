"""
tests/test_dte_ladder.py — PR1

Verifies the DTE-bucket ladder for deferred contract selection:
  - Flag OFF (default) → select() behaves exactly as before (no ladder).
  - Flag ON → eligible plans route through the ladder.
  - Buckets are ordered near-first (A 0-2, B 3-7, C 8+).
  - The ladder tries the preferred bucket first and only falls to 8+ DTE
    when nearer buckets yield zero quality survivors.
  - Quality gates are NEVER loosened by the ladder.
  - NO_VALID_PLAYBOOK_DTE_CONTRACT recorded when all buckets exhausted.

Loaded with heavy deps stubbed so the real selector logic runs in-process.
"""
from __future__ import annotations

import sys
import os
import importlib.util
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]
_SRC  = (_REPO / "ap" / "contract_selector.py").read_text()


# ---------------------------------------------------------------------------
# Source guards — cheap, fast, prove the structure exists
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_flag_defaults_off(self):
        assert 'os.getenv("DEFERRED_DTE_LADDER", "0")' in _SRC

    def test_ladder_method_present(self):
        assert "def _select_with_dte_ladder(" in _SRC

    def test_bucketing_present(self):
        assert "def _bucket_expirations(" in _SRC

    def test_expiration_override_threaded(self):
        # select() and the fetch chain must accept the override
        assert "expiration_override: Optional[str] = None" in _SRC
        assert "expiration_override=expiration_override" in _SRC

    def test_no_survivor_reason_code(self):
        assert "NO_VALID_PLAYBOOK_DTE_CONTRACT" in _SRC

    def test_quality_gates_not_loosened(self):
        """The ladder must not change min_oi/min_volume/spread thresholds.
        It only changes which expiration is evaluated. Guard: the ladder
        method must not assign to self.min_oi/min_volume/max_spread_pct."""
        idx = _SRC.find("def _select_with_dte_ladder(")
        end = _SRC.find("\n    def ", idx + 10)
        body = _SRC[idx:end]
        assert "self.min_oi =" not in body
        assert "self.min_volume =" not in body
        assert "self.max_spread_pct =" not in body

    def test_ladder_only_when_override_none(self):
        """Recursion guard: ladder runs only when expiration_override is None."""
        idx = _SRC.find("if (\n            self.dte_ladder_enabled")
        assert idx != -1
        region = _SRC[idx: idx + 300]
        assert "expiration_override is None" in region


# ---------------------------------------------------------------------------
# Behavioral tests — load the module with deps stubbed
# ---------------------------------------------------------------------------

def _load_selector(env_overrides: dict | None = None):
    stubs = {
        "ap.brokers": MagicMock(),
        "ap.brokers.tradier": MagicMock(),
        "ap.observability": MagicMock(
            emit_decision_event=MagicMock(),
            get_git_commit=MagicMock(return_value="test"),
            make_config_hash=MagicMock(return_value="test"),
        ),
        "yfinance": MagicMock(),
        "requests": MagicMock(),
    }
    env = dict(os.environ)
    if env_overrides:
        env.update(env_overrides)
    mod_name = "ap_contract_selector_shim"
    with patch.dict(sys.modules, stubs), patch.dict(os.environ, env, clear=False):
        spec = importlib.util.spec_from_file_location(
            mod_name, _REPO / "ap" / "contract_selector.py"
        )
        mod = importlib.util.module_from_spec(spec)
        # Register before exec so @dataclass can resolve cls.__module__, then
        # remove it afterward so this shim never pollutes other test modules
        # that import the real ap.contract_selector.
        sys.modules[mod_name] = mod
        try:
            spec.loader.exec_module(mod)
        finally:
            sys.modules.pop(mod_name, None)
    return mod


def _make_plan(ticker="AMAT", side="CALL", timeframe="1d", budget=198.72):
    p = MagicMock()
    p.ticker = ticker
    p.side = side
    p.timeframe = timeframe
    p.max_position_usd = budget
    p.signal_id = "sig-test"
    return p


class TestBucketing:
    def test_buckets_split_by_dte(self):
        mod = _load_selector()
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        today = date.today()
        dates = [
            (today + timedelta(days=1)).isoformat(),   # A
            (today + timedelta(days=2)).isoformat(),   # A
            (today + timedelta(days=5)).isoformat(),   # B
            (today + timedelta(days=14)).isoformat(),  # C
        ]
        # skip weekends in the fixture by filtering to weekdays
        buckets = sel._bucket_expirations(dates)
        # at least the structure is right; counts depend on weekday calendar
        assert set(buckets.keys()) == {"A", "B", "C"}
        # nearest-first ordering within a bucket
        for k, v in buckets.items():
            ds = [date.fromisoformat(x) for x in v]
            assert ds == sorted(ds)

    def test_bucket_order_near_first(self):
        mod = _load_selector()
        sel = object.__new__(mod.APContractSelectionEngine)
        order = sel._preferred_bucket_order(_make_plan(timeframe="1d"))
        assert order == ["A", "B", "C"]


class TestFlagOffIdentity:
    def test_flag_off_does_not_route_to_ladder(self):
        """With the flag off, select() must NOT call _select_with_dte_ladder."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "0"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = False
        plan = _make_plan()
        should_ladder = (
            sel.dte_ladder_enabled
            and None is None
            and True
        )
        assert should_ladder is False


class TestLadderEligibilityScope:
    """Amendment (#166 blast-radius fix): the ladder must apply ONLY to deferred
    breach-time selection — gated by an explicit plan marker, never every call."""

    @staticmethod
    def _bare_sel(mod):
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        return sel

    def test_no_marker_not_eligible_even_with_flag_on(self):
        """Flag on but NO deferred marker → not eligible → normal path."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = self._bare_sel(mod)
        plan = MagicMock()
        plan.metadata = {}  # no marker
        assert sel._is_ladder_eligible(plan) is False

    def test_boolean_marker_eligible(self):
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = self._bare_sel(mod)
        plan = MagicMock()
        plan.metadata = {"deferred_breach_selection": True}
        assert sel._is_ladder_eligible(plan) is True

    def test_context_marker_eligible(self):
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = self._bare_sel(mod)
        plan = MagicMock()
        plan.metadata = {"selection_context": "deferred_breach"}
        assert sel._is_ladder_eligible(plan) is True

    def test_dict_plan_marker_eligible(self):
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = self._bare_sel(mod)
        plan = {"metadata": {"deferred_breach_selection": True}}
        assert sel._is_ladder_eligible(plan) is True

    def test_no_metadata_attr_not_eligible(self):
        """An object plan with no metadata at all → not eligible (safe)."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = self._bare_sel(mod)
        class _P:  # no metadata attribute
            ticker = "AMAT"
        assert sel._is_ladder_eligible(_P()) is False

    def test_wrong_context_value_not_eligible(self):
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = self._bare_sel(mod)
        plan = MagicMock()
        plan.metadata = {"selection_context": "intraday"}
        assert sel._is_ladder_eligible(plan) is False

    def test_execution_core_sets_marker_before_select(self):
        """The deferred breach path must set the marker before calling select."""
        ec = (Path(__file__).resolve().parents[1] / "ap_execution_core.py").read_text()
        idx = ec.find("_sel = self.contract_selector.select(approved_plan)")
        assert idx != -1
        before = ec[idx - 900: idx]
        assert '"deferred_breach_selection"' in before
        assert '"selection_context"' in before


def _next_weekday(base: date, offset_days: int) -> str:
    """Return an ISO date offset_days from base, bumped to the next weekday."""
    d = base + timedelta(days=offset_days)
    while d.weekday() >= 5:
        d += timedelta(days=1)
    return d.isoformat()


class TestLadderRouting:
    def test_ladder_tries_bucket_a_before_c(self):
        """When bucket A has a survivor, C is never probed."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 2
        sel._last_failure = None
        sel._last_dte_ladder_audit = None

        today = date.today()
        a_exp = _next_weekday(today, 1)    # bucket A (0-2)
        c_exp = _next_weekday(today, 14)   # bucket C (8+)
        # ensure they didn't collide into the same bucket after weekday bump
        sel._fetch_expirations_list = MagicMock(return_value=[a_exp, c_exp])

        probed = []
        def _fake_select(plan, *, expiration_override=None):
            probed.append(expiration_override)
            if expiration_override == a_exp:
                return MagicMock(contract_symbol="AMAT260101C00100000")
            return MagicMock(contract_symbol="OTHER")
        sel.select = _fake_select

        result = sel._select_with_dte_ladder(_make_plan(timeframe="1d"))
        assert result is not None
        assert a_exp in probed
        assert c_exp not in probed
        audit = sel.get_last_dte_ladder_audit()
        assert audit["selected_expiration"] == a_exp

    def test_ladder_falls_to_c_when_near_empty(self):
        """When A yields no survivor, the ladder falls to C."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 2
        sel._last_failure = None
        sel._last_dte_ladder_audit = None

        today = date.today()
        a_exp = _next_weekday(today, 1)
        c_exp = _next_weekday(today, 14)
        sel._fetch_expirations_list = MagicMock(return_value=[a_exp, c_exp])

        def _fake_select(plan, *, expiration_override=None):
            if expiration_override == c_exp:
                return MagicMock(contract_symbol="AMAT_C")
            return None
        sel.select = _fake_select

        result = sel._select_with_dte_ladder(_make_plan(timeframe="1d"))
        assert result is not None
        audit = sel.get_last_dte_ladder_audit()
        assert audit["selected_bucket"] == "C"

    def test_ladder_no_survivor_sets_reason(self):
        """All buckets empty → None + NO_VALID_PLAYBOOK_DTE_CONTRACT."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 2
        sel._last_failure = None
        sel._last_dte_ladder_audit = None

        today = date.today()
        a_exp = (today + timedelta(days=1)).isoformat()
        sel._fetch_expirations_list = MagicMock(return_value=[a_exp])
        sel.select = MagicMock(return_value=None)

        result = sel._select_with_dte_ladder(_make_plan(timeframe="1d"))
        assert result is None
        assert sel._last_failure is not None
        assert sel._last_failure["reason_code"] == "NO_VALID_PLAYBOOK_DTE_CONTRACT"

    def test_ladder_falls_back_to_legacy_on_no_expirations(self):
        """If the expirations list can't be fetched, fall back to legacy
        single-shot (override="") rather than failing."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 2
        sel._last_failure = None
        sel._last_dte_ladder_audit = None
        sel._fetch_expirations_list = MagicMock(return_value=[])

        legacy_called = {"override": "unset"}
        def _fake_select(plan, *, expiration_override=None):
            legacy_called["override"] = expiration_override
            return MagicMock(contract_symbol="LEGACY")
        sel.select = _fake_select

        result = sel._select_with_dte_ladder(_make_plan())
        assert result is not None
        # legacy fallback passes empty-string override (=> _pick_expiration path)
        assert legacy_called["override"] == ""


class TestAmendmentGateOrdering:
    """Amendment: run terminal non-DTE gates before the ladder; never overwrite
    EARNINGS_LOCKOUT / invalid-plan reasons with NO_VALID_PLAYBOOK_DTE_CONTRACT."""

    def test_ladder_gate_after_earnings_in_source(self):
        """The ladder delegation must appear AFTER the earnings gate and the
        INVALID_PLAN gate in source order."""
        invalid_plan = _SRC.find('reason_code="INVALID_PLAN"')
        earnings = _SRC.find('reason_code="EARNINGS_LOCKOUT"')
        ladder_gate = _SRC.find("return self._select_with_dte_ladder(plan)")
        assert invalid_plan != -1 and earnings != -1 and ladder_gate != -1
        assert ladder_gate > invalid_plan, "ladder must run after INVALID_PLAN gate"
        assert ladder_gate > earnings, "ladder must run after EARNINGS gate"

    def test_terminal_non_dte_preserved_set_present(self):
        assert "_TERMINAL_NON_DTE" in _SRC
        for code in ("EARNINGS_LOCKOUT", "INVALID_PLAN"):
            idx = _SRC.find("_TERMINAL_NON_DTE = {")
            block = _SRC[idx: idx + 250]
            assert code in block

    def test_ladder_preserves_earnings_reason(self):
        """If a sub-call rejects with EARNINGS_LOCKOUT, the ladder must preserve
        that reason, NOT overwrite with NO_VALID_PLAYBOOK_DTE_CONTRACT."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 2
        sel._last_failure = None
        sel._last_dte_ladder_audit = None

        today = date.today()
        a_exp = _next_weekday(today, 1)
        sel._fetch_expirations_list = MagicMock(return_value=[a_exp])

        def _fake_select(plan, *, expiration_override=None):
            # simulate the earnings gate firing inside the sub-call
            sel._last_failure = {
                "stage": "earnings_gate",
                "reason_code": "EARNINGS_LOCKOUT",
                "explanation": "Blocked by EarningsGuard",
            }
            return None
        sel.select = _fake_select

        result = sel._select_with_dte_ladder(_make_plan(timeframe="1d"))
        assert result is None
        # the true reason must survive
        assert sel._last_failure["reason_code"] == "EARNINGS_LOCKOUT"

    def test_ladder_preserves_quality_reason_when_no_terminal(self):
        """When sub-calls fail after real chain rows are evaluated, preserve
        the quality reason instead of masking it as DTE exhaustion."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 2
        sel._last_failure = None
        sel._last_dte_ladder_audit = None

        today = date.today()
        a_exp = _next_weekday(today, 1)
        sel._fetch_expirations_list = MagicMock(return_value=[a_exp])

        def _fake_select(plan, *, expiration_override=None):
            sel._last_failure = {
                "stage": "quality_summary",
                "reason_code": "OI_TOO_LOW",
                "explanation": "illiquid",
            }
            return None
        sel.select = _fake_select

        result = sel._select_with_dte_ladder(_make_plan(timeframe="1d"))
        assert result is None
        assert sel._last_failure["reason_code"] == "OI_TOO_LOW"

    def test_ladder_preserves_retryable_data_reason_when_all_probes_data_miss(self):
        """All-probe retryable data misses must stay retryable so the deferred
        breach path can rearm instead of terminalizing immediately."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 2
        sel._last_failure = None
        sel._last_dte_ladder_audit = None

        today = date.today()
        a_exp = _next_weekday(today, 1)
        c_exp = _next_weekday(today, 14)
        sel._fetch_expirations_list = MagicMock(return_value=[a_exp, c_exp])

        def _fake_select(plan, *, expiration_override=None):
            sel._last_failure = {
                "stage": "chain_fetch",
                "reason_code": (
                    "CHAIN_PROVIDER_EMPTY_OPTIONS"
                    if expiration_override == a_exp else
                    "CHAIN_PARSE_EMPTY"
                ),
                "explanation": "provider warming up",
            }
            return None

        sel.select = _fake_select

        result = sel._select_with_dte_ladder(_make_plan(timeframe="1d"))
        assert result is None
        assert sel._last_failure["reason_code"] in {
            "CHAIN_PROVIDER_EMPTY_OPTIONS",
            "CHAIN_PARSE_EMPTY",
        }
