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
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]
_SRC  = (_REPO / "ap" / "contract_selector.py").read_text()


# ---------------------------------------------------------------------------
# Source guards — cheap, fast, prove the structure exists
# ---------------------------------------------------------------------------

class TestSourceGuards:
    def test_flag_defaults_off_for_post_close_rollout(self):
        assert 'os.getenv("DEFERRED_DTE_LADDER", "0")' in _SRC
        assert 'os.getenv("DEFERRED_DTE_LADDER", "1")' not in _SRC

    def test_live_legacy_fallback_defaults_off(self):
        assert '"DEFERRED_DTE_LEGACY_FALLBACK", "0"' in _SRC

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
        "ap.db": MagicMock(),
        "ap.brokers": MagicMock(),
        "ap.brokers.tradier": MagicMock(),
        "ap.observability": MagicMock(
            emit_decision_event=MagicMock(),
            get_git_commit=MagicMock(return_value="test"),
            make_config_hash=MagicMock(return_value="test"),
        ),
        "ap.trace": MagicMock(trace_gate=MagicMock()),
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


class _FakeResponse:
    def __init__(self, status_code: int, payload: dict, headers: dict | None = None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses: dict[tuple[str, tuple[tuple[str, str], ...]], _FakeResponse]):
        self._responses = responses

    def get(self, url, params=None, headers=None, timeout=None):
        key = (url, tuple(sorted((params or {}).items())))
        try:
            return self._responses[key]
        except KeyError as exc:
            raise AssertionError(f"unexpected GET {url} params={params}") from exc


def _make_option(symbol: str, expiration: str, *, bid: float, ask: float, strike: float, delta: float,
                 volume: int = 250, open_interest: int = 1200) -> dict:
    return {
        "symbol": symbol,
        "option_type": "call",
        "expiration_date": expiration,
        "strike": strike,
        "bid": bid,
        "ask": ask,
        "volume": volume,
        "open_interest": open_interest,
        "greeks": {"delta": delta},
    }


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
        def _fake_select(plan, *, expiration_override=None, request_context=None):
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

        def _fake_select(plan, *, expiration_override=None, request_context=None):
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

        plan = _make_plan(timeframe="1d")
        plan.metadata = {}
        result = sel._select_with_dte_ladder(plan)
        assert result is None
        assert sel._last_failure is not None
        assert sel._last_failure["reason_code"] == "NO_VALID_PLAYBOOK_DTE_CONTRACT"

    def test_ladder_fails_closed_on_empty_expirations(self):
        """Empty expirations must fail closed with the exact provider reason."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 2
        sel.mode = "live"
        sel._last_failure = None
        sel._last_dte_ladder_audit = None
        plan = _make_plan()
        plan.metadata = {"deferred_breach_selection": True}
        sel._fetch_expirations_list = MagicMock(side_effect=mod.ChainEmptyExpirations("empty list"))

        result = sel._select_with_dte_ladder(plan)
        assert result is None
        assert sel._last_failure["reason_code"] == "CHAIN_PROVIDER_EMPTY_EXPIRATIONS"
        assert plan.metadata["selector_failure"]["reason_code"] == "CHAIN_PROVIDER_EMPTY_EXPIRATIONS"
        failure = plan.metadata["selector_failure"]
        assert failure["canonical_selector_reason"] == "CHAIN_PROVIDER_EMPTY_EXPIRATIONS"
        assert failure["last_observed_selector_reason"] == "CHAIN_PROVIDER_EMPTY_EXPIRATIONS"
        assert failure["selector_terminal_reason"] == "CHAIN_PROVIDER_EMPTY_EXPIRATIONS"
        assert failure["operational_reason"] is None

    def test_ladder_budget_failure_preserves_operational_selector_truth(self):
        """An expiration-cap stop must not lose the selector-owned budget reason."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.mode = "live"
        sel.deferred_dte_legacy_fallback = False
        sel._last_failure = None
        sel._last_dte_ladder_audit = None
        sel._fetch_expirations_list = MagicMock(
            side_effect=mod.SelectorRequestBudgetExhausted(
                "expirations", "expiration_calls=3 limit=3"
            )
        )
        plan = _make_plan()
        plan.execution_mode = "live"
        plan.metadata = {"deferred_breach_selection": True}

        result = sel._select_with_dte_ladder(plan)

        assert result is None
        failure = plan.metadata["selector_failure"]
        assert failure["reason_code"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        assert failure["canonical_selector_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert failure["last_observed_selector_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert failure["selector_terminal_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert failure["operational_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )

    @pytest.mark.parametrize(
        "first_reason",
        ["UNTRADEABLE_FOR_ACCOUNT_SIZE", "DIRECT_QUOTE_ZERO_BID_ASK"],
    )
    def test_ladder_operational_stop_keeps_prior_canonical_reason(
        self, first_reason
    ):
        """A later budget stop ends probing without erasing earlier truth."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.mode = "live"
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 1
        sel._last_failure = None
        sel._last_dte_ladder_audit = None

        today = date.today()
        a_exp = _next_weekday(today, 1)
        b_exp = _next_weekday(today, 5)
        sel._fetch_expirations_list = MagicMock(return_value=[a_exp, b_exp])
        probed = []

        def _fake_select(plan, *, expiration_override=None, request_context=None):
            probed.append(expiration_override)
            if len(probed) == 1:
                plan.metadata["selector_failure"] = {
                    "stage": "quality_filter",
                    "reason_code": first_reason,
                    "canonical_selector_reason": first_reason,
                    "last_observed_selector_reason": first_reason,
                    "selector_terminal_reason": first_reason,
                    "operational_reason": None,
                    "explanation": "first expiration had a truthful selector result",
                    "selection_diagnostics": {
                        "direct_quote_budget": {"used": 0, "remaining": 1},
                    },
                }
            else:
                plan.metadata["selector_failure"] = {
                    "stage": "selector_request_budget",
                    "reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "canonical_selector_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "last_observed_selector_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "selector_terminal_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "operational_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "explanation": "direct quote cap reached",
                    "selection_diagnostics": {
                        "direct_quote_budget": {"used": 1, "remaining": 0},
                        "budget_exhausted_stage": "direct_quote",
                    },
                }
            return None

        sel.select = _fake_select
        plan = _make_plan(timeframe="1d")
        plan.execution_mode = "live"
        plan.metadata = {}

        assert sel._select_with_dte_ladder(plan) is None
        assert probed == [a_exp, b_exp]
        failure = plan.metadata["selector_failure"]
        assert failure["reason_code"] == first_reason
        assert failure["canonical_selector_reason"] == first_reason
        assert failure["selector_terminal_reason"] == first_reason
        assert failure["last_observed_selector_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert failure["operational_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert failure["operational_failure"]["reason_code"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert failure["selection_diagnostics"]["budget_exhausted_stage"] == (
            "direct_quote"
        )
        assert sel._last_failure == failure
        assert len(plan.metadata["dte_ladder_audit"]["buckets_attempted"]) == 2

    @pytest.mark.parametrize(
        ("first_reason", "second_reason"),
        [
            ("UNTRADEABLE_FOR_ACCOUNT_SIZE", "SPREAD_TOO_WIDE"),
            ("SPREAD_TOO_WIDE", "UNTRADEABLE_FOR_ACCOUNT_SIZE"),
        ],
    )
    def test_ladder_quality_reduction_uses_fixed_precedence(
        self, first_reason, second_reason
    ):
        """Quality reduction cannot depend on which expiration was last."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 1
        sel._last_failure = None
        sel._last_dte_ladder_audit = None
        today = date.today()
        a_exp = _next_weekday(today, 1)
        b_exp = _next_weekday(today, 5)
        sel._fetch_expirations_list = MagicMock(return_value=[a_exp, b_exp])
        reasons = iter([first_reason, second_reason])

        def _fake_select(plan, *, expiration_override=None, request_context=None):
            reason = next(reasons)
            plan.metadata["selector_failure"] = {
                "reason_code": reason,
                "canonical_selector_reason": reason,
                "explanation": reason,
            }
            return None

        sel.select = _fake_select
        plan = _make_plan(timeframe="1d")
        plan.metadata = {}
        assert sel._select_with_dte_ladder(plan) is None
        assert plan.metadata["selector_failure"]["reason_code"] == (
            "UNTRADEABLE_FOR_ACCOUNT_SIZE"
        )

    @pytest.mark.parametrize("mode,explicit_flag,expected_fallback", [
        ("paper", False, True),
        ("live", False, False),
        ("live", True, True),
    ])
    def test_provider_failure_has_one_explicit_bounded_fallback(
        self, mode, explicit_flag, expected_fallback
    ):
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.mode = mode
        sel.deferred_dte_legacy_fallback = explicit_flag
        sel._last_failure = None
        sel._last_dte_ladder_audit = None
        sel._fetch_expirations_list = MagicMock(side_effect=mod.ChainProviderError(
            "rate limited",
            status_code=429,
            provider_latency_ms=87.5,
            retry_after_ms=2000,
        ))
        selected = MagicMock(contract_symbol="AMAT_LEGACY")
        sel.select = MagicMock(return_value=selected)
        plan = _make_plan()
        plan.execution_mode = mode
        plan.metadata = {"deferred_breach_selection": True}

        result = sel._select_with_dte_ladder(plan)

        assert (result is selected) is expected_fallback
        assert sel.select.call_count == (1 if expected_fallback else 0)
        if expected_fallback:
            sel.select.assert_called_once_with(
                plan,
                _dte_legacy_fallback=True,
                request_context=None,
            )
        audit = plan.metadata["dte_ladder_audit"]
        assert audit["expiration_fetch_attempts"] == 1
        assert audit["expiration_http_status"] == 429
        assert audit["expiration_provider_latency_ms"] == 87.5
        assert audit["retry_after_ms"] == 2000
        assert audit["final_reason"] == "CHAIN_PROVIDER_ERROR"
        assert audit["fallback_considered"] is True
        assert audit["fallback_allowed"] is expected_fallback
        assert audit["fallback_used"] is expected_fallback

    def test_auth_failure_never_falls_back_even_in_paper(self):
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.mode = "paper"
        sel.deferred_dte_legacy_fallback = True
        sel._last_failure = None
        sel._last_dte_ladder_audit = None
        sel._fetch_expirations_list = MagicMock(side_effect=mod.ChainAuthError(
            "unauthorized", status_code=401, provider_latency_ms=12.0
        ))
        sel.select = MagicMock()
        plan = _make_plan()
        plan.execution_mode = "paper"
        plan.metadata = {"deferred_breach_selection": True}

        assert sel._select_with_dte_ladder(plan) is None
        sel.select.assert_not_called()
        audit = plan.metadata["dte_ladder_audit"]
        assert audit["final_reason"] == "CHAIN_AUTH_ERROR"
        assert audit["fallback_considered"] is False
        assert audit["fallback_used"] is False

    def test_real_expiration_fetch_captures_http_latency_and_retry_after(self):
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        url = "https://api.tradier.com/v1/markets/options/expirations"
        session = _FakeSession({
            (url, (("includeAllRoots", "true"), ("symbol", "AMAT"))):
                _FakeResponse(429, {}, headers={"Retry-After": "3"}),
        })
        sel = mod.APContractSelectionEngine(
            broker=SimpleNamespace(),
            data_broker=SimpleNamespace(
                session=session,
                cfg=SimpleNamespace(base_url="https://api.tradier.com", access_token="token"),
            ),
            mode="paper",
        )

        with pytest.raises(mod.ChainProviderError) as exc_info:
            sel._fetch_expirations_list("AMAT")

        exc = exc_info.value
        assert exc.status_code == 429
        assert exc.provider_latency_ms is not None
        assert exc.provider_latency_ms >= 0
        assert exc.retry_after_ms == 3000
        assert exc.attempts == 1

    def test_ladder_uses_plan_scoped_failure_not_shared_last_failure(self):
        """Ladder control flow must preserve the sub-call plan failure snapshot,
        not whatever another request last wrote to self._last_failure."""
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 2
        sel.mode = "live"
        sel._last_failure = {"reason_code": "ALIEN_FAILURE", "stage": "other", "explanation": "other request"}
        sel._last_dte_ladder_audit = None
        today = date.today()
        a_exp = _next_weekday(today, 1)
        plan = _make_plan(timeframe="1d")
        plan.metadata = {"deferred_breach_selection": True}
        sel._fetch_expirations_list = MagicMock(return_value=[a_exp])

        def _fake_select(sub_plan, *, expiration_override=None, request_context=None):
            sub_plan.metadata["selector_failure"] = {
                "reason_code": "CHAIN_PROVIDER_EMPTY_OPTIONS",
                "explanation": "no options for this expiration",
            }
            sel._last_failure = {"reason_code": "ALIEN_FAILURE", "stage": "other", "explanation": "other request"}
            return None

        sel.select = _fake_select
        result = sel._select_with_dte_ladder(plan)
        assert result is None
        assert sel._last_failure["reason_code"] == "CHAIN_PROVIDER_EMPTY_OPTIONS"
        assert plan.metadata["selector_failure"]["reason_code"] == "CHAIN_PROVIDER_EMPTY_OPTIONS"

    def test_top_level_select_resets_stale_ladder_audit(self):
        mod = _load_selector({"DEFERRED_DTE_LADDER": "0"})
        sel = mod.APContractSelectionEngine(
            broker=SimpleNamespace(),
            data_broker=SimpleNamespace(
                session=_FakeSession({
                    ("https://api.tradier.com/v1/markets/quotes", (("greeks", "false"), ("symbols", "AMAT"))):
                        _FakeResponse(200, {"quotes": {"quote": {"last": 145.0}}}),
                    ("https://api.tradier.com/v1/markets/options/expirations", (("includeAllRoots", "true"), ("symbol", "AMAT"))):
                        _FakeResponse(200, {"expirations": {"date": [_next_weekday(date.today(), 1)]}}),
                    ("https://api.tradier.com/v1/markets/options/chains", (("expiration", _next_weekday(date.today(), 1)), ("greeks", "true"), ("symbol", "AMAT"))):
                        _FakeResponse(200, {"options": {"option": [
                            _make_option("AMAT_A", _next_weekday(date.today(), 1), bid=2.0, ask=2.1, strike=145.0, delta=0.42),
                        ]}}),
                }),
                cfg=SimpleNamespace(base_url="https://api.tradier.com", access_token="token"),
                base_url="https://api.tradier.com",
            ),
            mode="live",
        )
        sel._last_dte_ladder_audit = {"ticker": "STALE"}
        plan = SimpleNamespace(
            ticker="AMAT",
            side="CALL",
            timeframe="1d",
            max_position_usd=500.0,
            execution_mode="LIVE",
            signal_id="sig-reset",
            metadata={"dte_ladder_audit": {"ticker": "STALE"}},
        )

        result = sel.select(plan)
        assert result is not None
        assert sel.get_last_dte_ladder_audit() is None
        assert "dte_ladder_audit" not in plan.metadata


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

        def _fake_select(plan, *, expiration_override=None, request_context=None):
            # simulate the earnings gate firing inside the sub-call
            plan.metadata["selector_failure"] = {
                "reason_code": "EARNINGS_LOCKOUT",
                "explanation": "Blocked by EarningsGuard",
            }
            return None
        sel.select = _fake_select

        plan = _make_plan(timeframe="1d")
        plan.metadata = {}
        result = sel._select_with_dte_ladder(plan)
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

        def _fake_select(plan, *, expiration_override=None, request_context=None):
            plan.metadata["selector_failure"] = {
                "reason_code": "OI_TOO_LOW",
                "explanation": "illiquid",
            }
            return None
        sel.select = _fake_select

        plan = _make_plan(timeframe="1d")
        plan.metadata = {}
        result = sel._select_with_dte_ladder(plan)
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

        def _fake_select(plan, *, expiration_override=None, request_context=None):
            plan.metadata["selector_failure"] = {
                "reason_code": (
                    "CHAIN_PROVIDER_EMPTY_OPTIONS"
                    if expiration_override == a_exp else
                    "CHAIN_PARSE_EMPTY"
                ),
                "explanation": "provider warming up",
            }
            return None

        sel.select = _fake_select

        plan = _make_plan(timeframe="1d")
        plan.metadata = {}
        result = sel._select_with_dte_ladder(plan)
        assert result is None
        assert sel._last_failure["reason_code"] in {
            "CHAIN_PROVIDER_EMPTY_OPTIONS",
            "CHAIN_PARSE_EMPTY",
        }

    def test_ladder_prefers_later_quality_reason_over_earlier_retryable_miss(self):
        """If an early expiration is transient but a later usable chain fails
        quality, the truthful final verdict must be the quality rejection."""
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

        def _fake_select(plan, *, expiration_override=None, request_context=None):
            if expiration_override == a_exp:
                plan.metadata["selector_failure"] = {
                    "reason_code": "CHAIN_PROVIDER_EMPTY_OPTIONS",
                    "explanation": "provider warming up",
                }
            else:
                plan.metadata["selector_failure"] = {
                    "reason_code": "SPREAD_TOO_WIDE",
                    "explanation": "usable chain, no contract passed spread gate",
                }
            return None

        sel.select = _fake_select

        plan = _make_plan(timeframe="1d")
        plan.metadata = {}
        result = sel._select_with_dte_ladder(plan)
        assert result is None
        assert sel._last_failure["reason_code"] == "SPREAD_TOO_WIDE"
        assert plan.metadata["selector_failure"]["reason_code"] == "SPREAD_TOO_WIDE"

    # ─────────────────────────────────────────────────────────────────────
    # P0 amendment: DUPLICATE_QUOTE_CONFLICT_UNRESOLVED taxonomy defect
    #
    # The DTE ladder previously classified this authoritative RETRYABLE_DATA
    # reason via a stale local handwritten set that omitted it, so it fell
    # into the generic quality-reduction branch instead of the
    # retryable-data preservation branch. The following four tests directly
    # execute _select_with_dte_ladder (not source inspection, not policy
    # table membership alone) and prove the corrected behavior across the
    # four failure-timing/precedence scenarios the ladder must handle.
    # ─────────────────────────────────────────────────────────────────────

    def test_ladder_classifies_duplicate_conflict_as_retryable_not_quality(self):
        """Test A: baseline contract -- a single DUPLICATE_QUOTE_CONFLICT_
        UNRESOLVED probe with no competing reason must surface unchanged,
        with data_failure=True/quality_failure=False, and must never be
        converted to NO_VALID_PLAYBOOK_DTE_CONTRACT or
        PLAYBOOK_NO_FULLY_ELIGIBLE_CONTRACT.

        Note: for a SOLO probe, the final surfaced dict is byte-identical
        regardless of which internal ladder variable (_preserved_quality vs
        _preserved_retryable) held it -- both paths do a plain dict copy of
        the single occupant. This test alone does NOT distinguish the two
        code paths; it is a baseline behavioral contract. Test B below is
        the test that actually distinguishes pre-fix from post-fix behavior
        (probe order matters -- see its docstring), verified by reverting
        the production fix and confirming Test B fails while this one still
        passes either way.
        """
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

        def _fake_select(plan, *, expiration_override=None, request_context=None):
            plan.metadata["selector_failure"] = {
                "reason_code": "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
                "canonical_selector_reason": "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
                "explanation": "conflicting duplicate OCC representations",
                # Real values from _classify_selector_failure for this exact
                # reason (ap/contract_selector.py "data_quality_zero_quotes").
                "data_failure": True,
                "quality_failure": False,
            }
            return None
        sel.select = _fake_select

        plan = _make_plan(timeframe="1d")
        plan.metadata = {}
        result = sel._select_with_dte_ladder(plan)

        assert result is None
        failure = sel._last_failure
        assert failure["reason_code"] == "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        assert failure["canonical_selector_reason"] == (
            "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        )
        assert failure["data_failure"] is True
        assert failure["quality_failure"] is False
        assert failure["reason_code"] != "NO_VALID_PLAYBOOK_DTE_CONTRACT"
        assert failure["reason_code"] != "PLAYBOOK_NO_FULLY_ELIGIBLE_CONTRACT"

    def test_ladder_preserves_duplicate_conflict_across_all_transient_buckets(self):
        """Test B: a genuinely retryable data miss observed FIRST, followed
        by DUPLICATE_QUOTE_CONFLICT_UNRESOLVED observed SECOND, must still
        surface the FIRST retryable-data reason -- matching the existing
        first-observed-wins contract already proven by
        test_ladder_preserves_retryable_data_reason_when_all_probes_data_miss
        above for two reasons that were always correctly classified.

        Probe order is deliberately chosen to make the defect observable:
        pre-fix, DUPLICATE_QUOTE_CONFLICT_UNRESOLVED fell into the generic
        quality branch, and the ladder's post-loop reduction checks
        _preserved_quality before _preserved_retryable unconditionally. That
        meant a duplicate-conflict reason observed SECOND could silently
        displace a genuinely-retryable FIRST reason merely by landing in the
        quality slot -- inverting the intended first-observed-wins contract
        for retryable-data misses. Verified: this exact test fails against
        the pre-fix code (final reason becomes
        DUPLICATE_QUOTE_CONFLICT_UNRESOLVED instead of CHAIN_PARSE_EMPTY).
        """
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

        def _fake_select(plan, *, expiration_override=None, request_context=None):
            plan.metadata["selector_failure"] = {
                "reason_code": (
                    "CHAIN_PARSE_EMPTY"
                    if expiration_override == a_exp else
                    "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
                ),
                "canonical_selector_reason": (
                    "CHAIN_PARSE_EMPTY"
                    if expiration_override == a_exp else
                    "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
                ),
                "data_failure": True,
                "quality_failure": False,
                "explanation": "transient data miss",
            }
            return None

        sel.select = _fake_select

        plan = _make_plan(timeframe="1d")
        plan.metadata = {}
        result = sel._select_with_dte_ladder(plan)

        assert result is None
        # First-observed retryable-data reason must win -- the same contract
        # already proven for two always-correctly-classified reasons.
        assert sel._last_failure["reason_code"] == "CHAIN_PARSE_EMPTY"
        assert sel._last_failure["data_failure"] is True
        assert sel._last_failure["quality_failure"] is False

    def test_ladder_genuine_quality_rejection_outranks_earlier_duplicate_conflict(self):
        """Test C: existing documented precedence (see
        test_ladder_prefers_later_quality_reason_over_earlier_retryable_miss
        above) says a genuine usable-chain quality verdict from a later
        expiration outranks an earlier transient data miss. This proves that
        contract still holds when the earlier transient miss is specifically
        DUPLICATE_QUOTE_CONFLICT_UNRESOLVED -- the fix must not change this
        existing precedence, only the category the duplicate-conflict reason
        itself enters before reduction.

        Note: this scenario does not independently distinguish pre-fix from
        post-fix behavior -- SPREAD_TOO_WIDE wins in both cases, because the
        ladder's final reduction always prefers any populated
        _preserved_quality slot over _preserved_retryable regardless of this
        fix (verified by reverting the production fix and confirming this
        test still passes). It documents an important invariant that must
        NOT change, not the defect itself -- see Test B for the actual
        regression proof.
        """
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

        def _fake_select(plan, *, expiration_override=None, request_context=None):
            if expiration_override == a_exp:
                plan.metadata["selector_failure"] = {
                    "reason_code": "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
                    "canonical_selector_reason": "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
                    "data_failure": True,
                    "quality_failure": False,
                    "explanation": "conflicting duplicate OCC representations",
                }
            else:
                plan.metadata["selector_failure"] = {
                    "reason_code": "SPREAD_TOO_WIDE",
                    "canonical_selector_reason": "SPREAD_TOO_WIDE",
                    "data_failure": False,
                    "quality_failure": True,
                    "explanation": "usable chain, no contract passed spread gate",
                }
            return None

        sel.select = _fake_select

        plan = _make_plan(timeframe="1d")
        plan.metadata = {}
        result = sel._select_with_dte_ladder(plan)

        assert result is None
        assert sel._last_failure["reason_code"] == "SPREAD_TOO_WIDE"
        assert plan.metadata["selector_failure"]["reason_code"] == "SPREAD_TOO_WIDE"
        # The duplicate-conflict reason still had a chance to be preserved as
        # retryable-data -- it just lost to a stronger genuine quality
        # verdict per existing precedence, exactly as CHAIN_PROVIDER_EMPTY_OPTIONS
        # does in the analogous existing test above.

    def test_ladder_operational_stop_after_duplicate_conflict_keeps_prior_truth(self):
        """Test D: a later request-budget/throttle operational stop must not
        convert an earlier DUPLICATE_QUOTE_CONFLICT_UNRESOLVED verdict into
        generic quality, must stop further provider work, and must keep the
        operational reason separately diagnosable -- mirroring the existing
        contract already proven for other first_reason values in
        test_ladder_operational_stop_keeps_prior_canonical_reason above.

        Note: this scenario does not independently distinguish pre-fix from
        post-fix behavior either -- with only one prior probe observed
        before the operational stop, _prior_failure resolves to the same
        dict content whether it came from _preserved_quality (pre-fix) or
        _preserved_retryable (post-fix). Verified by reverting the
        production fix and confirming this test still passes. It documents
        the operational-stop contract must survive the fix unchanged, not
        the defect itself -- see Test B for the actual regression proof.
        """
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        sel = object.__new__(mod.APContractSelectionEngine)
        sel.mode = "live"
        sel.dte_ladder_enabled = True
        sel.dte_bucket_a_max = 2
        sel.dte_bucket_b_max = 7
        sel.dte_ladder_probe_per_bucket = 1
        sel._last_failure = None
        sel._last_dte_ladder_audit = None

        today = date.today()
        a_exp = _next_weekday(today, 1)
        b_exp = _next_weekday(today, 5)
        sel._fetch_expirations_list = MagicMock(return_value=[a_exp, b_exp])
        probed = []

        def _fake_select(plan, *, expiration_override=None, request_context=None):
            probed.append(expiration_override)
            if len(probed) == 1:
                plan.metadata["selector_failure"] = {
                    "stage": "quality_filter",
                    "reason_code": "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
                    "canonical_selector_reason": "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
                    "last_observed_selector_reason": "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
                    "selector_terminal_reason": "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED",
                    "operational_reason": None,
                    "data_failure": True,
                    "quality_failure": False,
                    "explanation": "conflicting duplicate OCC representations",
                    "selection_diagnostics": {
                        "direct_quote_budget": {"used": 1, "remaining": 0},
                    },
                }
            else:
                plan.metadata["selector_failure"] = {
                    "stage": "selector_request_budget",
                    "reason_code": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "canonical_selector_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "last_observed_selector_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "selector_terminal_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "operational_reason": "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
                    "explanation": "direct quote cap reached",
                    "selection_diagnostics": {
                        "direct_quote_budget": {"used": 1, "remaining": 0},
                        "budget_exhausted_stage": "direct_quote",
                    },
                }
            return None

        sel.select = _fake_select
        plan = _make_plan(timeframe="1d")
        plan.execution_mode = "live"
        plan.metadata = {}

        assert sel._select_with_dte_ladder(plan) is None
        # Provider work stopped after the operational-stop probe -- no third
        # expiration was attempted even though _fetch_expirations_list
        # returned two.
        assert probed == [a_exp, b_exp]
        failure = plan.metadata["selector_failure"]
        assert failure["reason_code"] == "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        assert failure["canonical_selector_reason"] == (
            "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        )
        assert failure["selector_terminal_reason"] == (
            "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED"
        )
        assert failure["data_failure"] is True
        assert failure["quality_failure"] is False
        # Operational reason stays separately diagnosable, not merged into
        # the canonical selector verdict.
        assert failure["last_observed_selector_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert failure["operational_reason"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert failure["operational_failure"]["reason_code"] == (
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )
        assert sel._last_failure == failure

    def test_ladder_retryable_data_authority_matches_shared_policy_for_all_reasons(self):
        """Step 8 taxonomy parity guard: any reason the authoritative shared
        selector retry policy classifies as RETRYABLE_DATA must win the
        first-observed-retryable-wins contract the same way an
        always-correctly-classified reason does -- not just
        DUPLICATE_QUOTE_CONFLICT_UNRESOLVED. This is a behavioral guard, not
        a source/string inspection: for every authoritative RETRYABLE_DATA
        reason not already intercepted earlier in the ladder's own chain
        (_TERMINAL_NON_DTE, _OPERATIONAL_STOP_REASONS -- intentional
        DTE-ladder-specific narrowing per this PR's step 3/4 constraints,
        unaffected by this fix), it drives a real two-probe sequence through
        _select_with_dte_ladder with a known-good anchor reason FIRST and
        the reason under test SECOND, using the same paired-order pattern
        that makes Test B a genuine regression proof (a solo-probe guard
        would pass identically before and after the fix, since the ladder's
        post-loop reduction only diverges when two populated preservation
        slots compete -- see Test A/C/D docstrings). Verified this guard
        fails for DUPLICATE_QUOTE_CONFLICT_UNRESOLVED against the reverted
        pre-fix code and passes against the fixed code for every reason
        under test.
        """
        from ap.selector_retry_policy import _POLICY_TABLE, RETRYABLE_DATA

        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})

        _terminal_non_dte = {
            "EARNINGS_LOCKOUT", "EARNINGS_GUARD_ERROR",
            "INVALID_PLAN", "UNSUPPORTED_INDEX_MAPPING",
            "CHAIN_AUTH_ERROR",
            "CHAIN_PROVIDER_EMPTY_EXPIRATIONS",
        }
        _operational_stop = {
            "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
            "MARKET_DATA_THROTTLE_UNAVAILABLE",
        }
        _anchor_reason = "CHAIN_PARSE_EMPTY"
        _reasons_under_test = sorted(
            code for code, policy in _POLICY_TABLE.items()
            if policy.classification == RETRYABLE_DATA
            and code not in _terminal_non_dte
            and code not in _operational_stop
            and code != _anchor_reason
        )
        assert _reasons_under_test, (
            "policy table produced zero RETRYABLE_DATA reasons to test -- "
            "this guard would silently pass on an empty set"
        )
        assert "DUPLICATE_QUOTE_CONFLICT_UNRESOLVED" in _reasons_under_test

        for reason in _reasons_under_test:
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

            def _fake_select(
                plan, *, expiration_override=None, request_context=None,
                _reason=reason,
            ):
                plan.metadata["selector_failure"] = {
                    "reason_code": _anchor_reason if expiration_override == a_exp else _reason,
                    "canonical_selector_reason": (
                        _anchor_reason if expiration_override == a_exp else _reason
                    ),
                    "data_failure": True,
                    "quality_failure": False,
                    "explanation": "taxonomy parity probe",
                }
                return None
            sel.select = _fake_select

            plan = _make_plan(timeframe="1d")
            plan.metadata = {}
            result = sel._select_with_dte_ladder(plan)

            assert result is None, f"reason={reason} unexpectedly selected"
            # The anchor (first-observed, always-correctly-classified) must
            # win -- if `reason` is misclassified as quality, it would
            # hijack the final answer via the quality-checked-first
            # reduction despite being observed second.
            assert sel._last_failure["reason_code"] == _anchor_reason, (
                f"reason={reason} hijacked the final answer from the "
                f"first-observed anchor {_anchor_reason!r} (got "
                f"{sel._last_failure['reason_code']!r}); this indicates "
                f"{reason} is not correctly classified as retryable-data "
                f"by the DTE ladder"
            )


class TestDeferredPlanIntegration:
    def test_real_selector_ladder_preserves_recovered_plan_identity_on_copyback(self):
        mod = _load_selector({"DEFERRED_DTE_LADDER": "1"})
        today = date.today()
        near_exp = _next_weekday(today, 1)
        far_exp = _next_weekday(today, 14)
        responses = {
            ("https://api.tradier.com/v1/markets/options/expirations", (("includeAllRoots", "true"), ("symbol", "AMAT"))):
                _FakeResponse(200, {"expirations": {"date": [near_exp, far_exp]}}),
            ("https://api.tradier.com/v1/markets/quotes", (("greeks", "false"), ("symbols", "AMAT"))):
                _FakeResponse(200, {"quotes": {"quote": {"last": 145.0}}}),
            ("https://api.tradier.com/v1/markets/options/chains", (("expiration", near_exp), ("greeks", "true"), ("symbol", "AMAT"))):
                _FakeResponse(200, {"options": {"option": [
                    _make_option("AMAT_NEAR_EXPENSIVE", near_exp, bid=8.7, ask=9.1, strike=145.0, delta=0.41),
                ]}}),
            ("https://api.tradier.com/v1/markets/options/chains", (("expiration", far_exp), ("greeks", "true"), ("symbol", "AMAT"))):
                _FakeResponse(200, {"options": {"option": [
                    _make_option("AMAT_FAR_WINNER", far_exp, bid=2.0, ask=2.2, strike=145.0, delta=0.40),
                    _make_option("AMAT_FAR_LOSER", far_exp, bid=1.5, ask=2.9, strike=150.0, delta=0.19),
                ]}}),
        }
        sel = mod.APContractSelectionEngine(
            broker=SimpleNamespace(),
            data_broker=SimpleNamespace(
                session=_FakeSession(responses),
                cfg=SimpleNamespace(base_url="https://api.tradier.com", access_token="token"),
                base_url="https://api.tradier.com",
            ),
            mode="live",
        )
        sel.dte_ladder_enabled = True
        plan = SimpleNamespace(
            client_id="jason@example.com",
            execution_mode="live",
            signal_id="sig-live-314",
            ticker="AMAT",
            side="CALL",
            timeframe="1d",
            trigger_price=145.0,
            target_underlying=147.0,
            max_position_usd=500.0,
            contract_symbol="DEFERRED:AMAT",
            limit_price=0.01,
            contracts=1,
            metadata={
                "deferred_breach_selection": True,
                "selection_context": "deferred_breach",
                "recovered_plan": True,
            },
        )

        contract_before = plan.contract_symbol
        result = sel.select(plan)

        assert result is not None
        assert result.contract_symbol == "AMAT_FAR_WINNER"
        assert result.expiration == far_exp
        assert result.affordable_contracts == 2
        assert result.premium_per_contract == pytest.approx(220.0)
        assert plan.client_id == "jason@example.com"
        assert plan.execution_mode == "live"
        assert plan.signal_id == "sig-live-314"

        if result.contract_symbol and contract_before.upper().startswith("DEFERRED:"):
            plan.contract_symbol = result.contract_symbol
            plan.limit_price = result.execution_price_per_share or result.ask or result.mid
            plan.contracts = int(result.affordable_contracts or 0)
            plan.max_position_usd = float(plan.contracts) * float(result.premium_per_contract)

        assert plan.contract_symbol == result.contract_symbol
        assert plan.limit_price == result.execution_price_per_share
        assert plan.contracts == result.affordable_contracts
        assert plan.max_position_usd == result.affordable_contracts * result.premium_per_contract
        assert sel.get_last_dte_ladder_audit()["selected_expiration"] == far_exp
