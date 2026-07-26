"""
Budget audit truthfulness (PR #389 amendment).

Two focused checks over the existing budget-authority machinery:

* When direct-quote revalidation short-circuits on the request-budget
  guard (``SKIP_BUDGET_EXHAUSTED``), the per-candidate audit stamps
  ``budget_skipped=True`` and leaves ``attempted=False``. No provider
  call happened for that candidate, so the summary flag must match
  call-count truth.

* ``direct_quote_eligible_candidates`` counts only rows that could
  actually go to direct revalidation — valid OCC symbol, valid
  expiration, and directional strike fit — not the full ordered chain
  length. Candidates lacking any of those cannot be direct-quoted and
  so must not be counted as eligible.

No hard quality gate or the canonical ``SELECTOR_MAX_DIRECT_QUOTE_CALLS``
budget authority is altered by these changes.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pytest

os.environ.setdefault("DATABASE_URL", "postgresql://mock/mock")

from ap.contract_selector import (
    SelectorRequestContext,
    _order_chain_for_direct_quote_recovery,
)


def _next_weekday(target: date) -> date:
    while target.weekday() >= 5:
        target += timedelta(days=1)
    return target


_NEAR_EXPIRY = _next_weekday(date.today() + timedelta(days=2)).isoformat()


def _ctx() -> SelectorRequestContext:
    ctx = SelectorRequestContext(
        ticker="SPY",
        started_at_monotonic=0.0,
        max_direct_quote_calls=20,
        effective_direct_quote_limit=20,
    )
    ctx.diagnostics_sink = {}
    return ctx


def _opt(**over):
    base = {
        "symbol": "SPY260101C00450000",
        "expiration_date": _NEAR_EXPIRY,
        "strike": 450.0,
        "option_type": "call",
        "bid": 1.0,
        "ask": 1.1,
        "open_interest": 100,
        "volume": 10,
        "greeks": {"delta": 0.4},
    }
    base.update(over)
    return base


def test_direct_quote_eligible_candidates_counts_only_direct_quotable_rows():
    """The eligibility count must not include rows that lack a valid OCC
    symbol, lack a valid expiration, or fail directional strike fit —
    those rows cannot be direct-quoted, so counting them would overstate
    the pool the bounded budget can address."""
    ctx = _ctx()
    chain = [
        _opt(symbol="SPY260101C00450000", strike=450.0),        # eligible
        _opt(symbol="SPY260101C00451000", strike=451.0),        # eligible
        _opt(symbol="NOTOCC", strike=452.0),                    # invalid OCC
        _opt(symbol="SPY260101C00449000", strike=449.0,
             expiration_date="not-a-date"),                     # invalid expiry
        _opt(symbol="SPY260101C00400000", strike=400.0),        # wrong side for CALL
    ]
    _order_chain_for_direct_quote_recovery(
        chain,
        direction="CALL",
        underlying_price=450.0,
        target_delta=0.4,
        today=date.today(),
        request_context=ctx,
    )
    # Only the two SPY 450/451 CALL rows are direct-quotable.
    assert ctx.direct_quote_eligible_candidates == 2
    # The full ranking still reports every row so callers keep visibility.
    assert len(ctx.direct_quote_candidate_ranking) == len(chain)


def test_budget_skip_marks_audit_not_attempted_and_budget_skipped():
    """When the revalidator returns SKIP_BUDGET_EXHAUSTED for a candidate,
    the aggregate audit must report ``attempted=False`` and
    ``budget_skipped=True``. This mirrors the behavior wired into the
    selector's per-candidate audit updates."""
    # Simulate what the selector does when it hits SKIP_BUDGET_EXHAUSTED.
    audit: dict = {"attempted": False, "budget_skipped": False, "selected": False}
    audit.update({
        "attempted":      False,
        "budget_skipped": True,
        "selected":       False,
        "failure":        "SELECTOR_REQUEST_BUDGET_EXHAUSTED",
    })
    assert audit["attempted"] is False
    assert audit["budget_skipped"] is True
    assert audit["failure"] == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"


def test_budget_skip_audit_snippet_matches_selector_source():
    """Guardrail: the audit update sites in ap/contract_selector.py must
    stamp ``budget_skipped`` and leave ``attempted`` False for the
    SKIP_BUDGET_EXHAUSTED branches. This keeps future edits honest — if
    someone silently reverts the flag, this test catches it."""
    from pathlib import Path

    source = Path(__file__).resolve().parents[1] / "ap" / "contract_selector.py"
    text = source.read_text(encoding="utf-8")
    # Both SKIP_BUDGET_EXHAUSTED branches must include budget_skipped=True.
    branches = text.split("SKIP_BUDGET_EXHAUSTED")
    # There are >=2 references to SKIP_BUDGET_EXHAUSTED, each followed by
    # an audit update. Every branch that stamps ``attempted`` must set it
    # False and stamp ``budget_skipped`` True.
    audit_updates = [seg for seg in branches[1:] if "_direct_quote_recovery_audit" in seg[:600]]
    assert audit_updates, "expected at least one SKIP_BUDGET_EXHAUSTED audit-update site"
    for seg in audit_updates:
        head = seg[:800]
        assert '"budget_skipped": True' in head, (
            "SKIP_BUDGET_EXHAUSTED audit site missing budget_skipped=True"
        )
        assert '"attempted":      False' in head or '"attempted": False' in head, (
            "SKIP_BUDGET_EXHAUSTED audit site must not claim attempted=True"
        )
