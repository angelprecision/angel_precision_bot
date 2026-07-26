"""
Trigger-anchored contract intent tests (PR #389 amendment).

Covers the surgical amendments made on top of PR #389:

  * The primary strike is anchored to the scanner trigger price, not
    whichever underlying price happens to exist several seconds later.
  * Tie-break on distance prefers the OTM side for the trade direction
    (higher for CALL, lower for PUT).
  * The prepared ``ContractIntent`` structure carries all identity and
    preference evidence required by the breach-time fast path, and never
    becomes submit-time price authority.

These tests do not touch, relax, or duplicate the existing hard quality
gates or the ``SELECTOR_MAX_DIRECT_QUOTE_CALLS`` budget authority.
"""

from __future__ import annotations

from datetime import date

import pytest

from ap.contract_playbook import (
    ContractPlaybookSpec,
    build_playbook_candidate_context,
    resolve_contract_playbook,
)
from ap.contract_intent import (
    CONTRACT_INTENT_POLICY_VERSION,
    build_contract_intent,
    build_contract_intent_from_playbook,
)


def _spec(side: str, *, trigger: float | None, underlying: float, target: float | None = None) -> ContractPlaybookSpec:
    return ContractPlaybookSpec(
        instrument_class="ORDINARY_EQUITY",
        timeframe="DAILY",
        side=side,
        preferred_expirations=["2026-07-31"],
        permitted_expirations=["2026-07-31"],
        preferred_dte_order=[6],
        strike_policy="LEXICOGRAPHIC_ATM_OR_ONE_STEP_OTM_TOWARD_TARGET",
        preferred_strikes=[],
        strike_band_low=None,
        strike_band_high=None,
        target_underlying=target,
        trigger_price=trigger,
        underlying_price=float(underlying),
        fallback_policy="NEXT_APPROVED_EXPIRATION",
        policy_reason="TEST",
        diagnostics={},
    )


def _candidates(strikes):
    return [{"symbol": f"OPT_{s}", "strike": float(s)} for s in strikes]


# BAC production replay: trigger 61.17, strikes 61 and 62, PUT --------------

def test_bac_trigger_6117_selects_61_put_when_underlying_has_drifted():
    """Underlying has drifted above 61.5 by the time the chain is sampled.
    Distance ranking against underlying would pick 62; anchoring to the
    trigger 61.17 must still select 61 (|61-61.17|=0.17 < |62-61.17|=0.83)."""
    spec = _spec("PUT", trigger=61.17, underlying=61.60)
    ctx = build_playbook_candidate_context(spec, _candidates([60, 61, 62]))
    assert ctx["atm_strike"] == 61.0
    assert ctx["one_step_otm_strike"] == 60.0
    assert ctx["preferred_strikes"] == [61.0, 60.0]


def test_bac_trigger_6117_selects_61_put_when_underlying_missing():
    spec = _spec("PUT", trigger=61.17, underlying=0.0)
    ctx = build_playbook_candidate_context(spec, _candidates([60, 61, 62]))
    assert ctx["atm_strike"] == 61.0
    assert ctx["one_step_otm_strike"] == 60.0


# Tie-break: equidistant strikes prefer OTM side for the trade direction ----

def test_tie_break_put_prefers_lower_strike_on_equidistant():
    spec = _spec("PUT", trigger=61.5, underlying=61.5)
    ctx = build_playbook_candidate_context(spec, _candidates([61, 62]))
    assert ctx["atm_strike"] == 61.0


def test_tie_break_call_prefers_higher_strike_on_equidistant():
    spec = _spec("CALL", trigger=61.5, underlying=61.5)
    ctx = build_playbook_candidate_context(spec, _candidates([61, 62]))
    assert ctx["atm_strike"] == 62.0


# ContractIntent construction & identity fencing ----------------------------

def test_contract_intent_captures_all_required_identity_fields():
    intent = build_contract_intent(
        ticker="BAC",
        side="PUT",
        timeframe="DAILY",
        trigger_price=61.17,
        target=60.0,
        instrument_class="ORDINARY_EQUITY",
        preferred_expiration="2026-07-31",
        fallback_expiration="2026-08-07",
        preferred_strike=61.0,
        fallback_strike=60.0,
        preferred_contract_symbol="BAC260731P00061000",
        fallback_contract_symbol="BAC260731P00060000",
        source_chain_timestamp="2026-07-25T13:29:55+00:00",
        source_chain_domain="paper",
        client_id="client-abc",
        execution_mode="paper",
        signal_id="sig-123",
        setup_generation=4,
    )
    assert intent.ticker == "BAC"
    assert intent.side == "PUT"
    assert intent.trigger_price == 61.17
    assert intent.preferred_strike == 61.0
    assert intent.fallback_strike == 60.0
    assert intent.preferred_expiration == "2026-07-31"
    assert intent.fallback_expiration == "2026-08-07"
    assert intent.preferred_contract_symbol == "BAC260731P00061000"
    assert intent.fallback_contract_symbol == "BAC260731P00060000"
    assert intent.client_id == "client-abc"
    assert intent.execution_mode == "paper"
    assert intent.signal_id == "sig-123"
    assert intent.setup_generation == 4
    assert intent.policy_version == CONTRACT_INTENT_POLICY_VERSION
    # Provenance is preserved, but it is not a price authority.
    assert intent.source_chain_timestamp == "2026-07-25T13:29:55+00:00"
    assert intent.source_chain_domain == "paper"
    assert intent.prepared_at
    payload = intent.to_dict()
    assert payload["preferred_strike"] == 61.0
    assert payload["diagnostics"] == {}


def test_contract_intent_from_playbook_uses_trigger_anchored_strikes():
    spec = _spec("PUT", trigger=61.17, underlying=61.60)
    intent = build_contract_intent_from_playbook(
        spec,
        ticker="BAC",
        timeframe="DAILY",
        trigger_price=61.17,
        target=60.0,
        candidates=_candidates([60, 61, 62]),
        client_id="client-abc",
        execution_mode="paper",
        signal_id="sig-1",
        setup_generation=1,
    )
    assert intent.preferred_strike == 61.0
    assert intent.fallback_strike == 60.0
    assert intent.preferred_expiration == "2026-07-31"
    assert intent.diagnostics.get("atm_strike") == 61.0
    assert intent.diagnostics.get("one_step_otm_strike") == 60.0


def test_contract_intent_rejects_nan_target():
    intent = build_contract_intent(
        ticker="BAC",
        side="PUT",
        timeframe="DAILY",
        trigger_price=61.17,
        target=float("nan"),
        instrument_class="ORDINARY_EQUITY",
        preferred_expiration="2026-07-31",
        fallback_expiration=None,
        preferred_strike=61.0,
        fallback_strike=60.0,
        client_id="client-abc",
        execution_mode="live",
    )
    assert intent.target is None


def test_amendment_does_not_broaden_supported_index_list():
    spec = resolve_contract_playbook(
        ticker="AAPL",
        side="CALL",
        timeframe="daily",
        pattern=None,
        underlying_price=200.0,
        trigger_price=200.5,
        target_underlying=210.0,
        wick_targets=[],
        available_expirations=["2026-07-31"],
        today=date(2026, 7, 25),
        min_dte=0,
        max_dte=7,
        metadata={},
    )
    assert spec.instrument_class == "ORDINARY_EQUITY"
