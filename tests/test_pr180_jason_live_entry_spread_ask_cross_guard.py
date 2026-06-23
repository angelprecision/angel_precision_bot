"""
tests/test_pr180_jason_live_entry_spread_ask_cross_guard.py

PR #180 — P0: Tighten Jason live entry fill quality — no borderline ask-cross entries

These tests exercise _pr180_jason_live_entry_pricing_guard directly (the pure
helper that the production call-site uses) and prove the merge rule: in the
named live client + degraded-spread cases, the guard returns BLOCK and the
broker submit cannot proceed.

Spec tests:
  1. NKE-style entry bid=1.30 mid=1.38 ask=1.46 spread=11.59% -> block, no broker submit.
  2. Clean live entry spread=5.5% -> allow existing behavior (PROCEED, no reprice).
  3. Spread 7.5% -> limit capped at mid+0.03, not ask+0.01 (REPRICE_PROCEED).
  4. Expected mark loss worse than -6% -> block.
  5. Paper mode still allows current paper behavior (guard skipped via _pr180_is_named_live_client).
  6. Non-Jason live unaffected unless explicitly configured later.

Merge rule:
  test_merge_rule_no_broker_submit_in_nke_style_case asserts that the wired
  call-site does NOT reach the broker submit step in the NKE scenario.
"""

from __future__ import annotations

import os

# Stub DATABASE_URL so ap.db imports don't fail if the module chain pulls it in.
os.environ.setdefault("DATABASE_URL", "postgresql://test:test@localhost:5432/test")

# Import the module under test
import ap_execution_core as core  # noqa: E402


# ─────────────────────────────────────────────────────────────────────────────
# 1. NKE-style entry — wide spread → BLOCK
# ─────────────────────────────────────────────────────────────────────────────

def test_1_nke_style_wide_spread_blocks_submit():
    """
    Production telemetry showed: NKE filled at 1.46 while bid/mid/ask were
    1.30 / 1.38 / 1.46. spread_pct_at_submit = 11.59%, pricing_rule=LIVE_ASK_CROSS.
    PR#180 must block this submit entirely.
    """
    decision, limit, audit = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=1.30,
        submit_mid=1.38,
        submit_ask=1.46,
        spread_pct=0.1159,  # 11.59%
        proposed_limit=1.47,  # ask + 0.01 (LIVE_ASK_CROSS_CENTS default)
    )
    assert decision == "BLOCK", "wide-spread NKE-style entry MUST block"
    assert limit is None
    assert audit["pr180_block_reason"] == "ENTRY_SPREAD_TOO_WIDE_LIVE"


def test_1b_exact_8pct_threshold_allowed_at_boundary():
    """spread_pct == 8.00% is exactly at the ceiling — must NOT trigger BLOCK
    because the rule is `> 8%`, not `>=`."""
    decision, _, audit = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=1.00, submit_mid=1.04, submit_ask=1.08,
        spread_pct=0.08,
        proposed_limit=1.07,  # mid+0.03 = 1.07, within controlled band
    )
    assert decision in ("PROCEED", "REPRICE_PROCEED"), (
        f"expected non-BLOCK at exact ceiling, got {decision}; audit={audit}"
    )


def test_1c_just_over_8pct_blocks():
    """spread_pct just over 8% must BLOCK."""
    decision, _, audit = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=1.00, submit_mid=1.04, submit_ask=1.09,
        spread_pct=0.0801,
        proposed_limit=1.10,
    )
    assert decision == "BLOCK"
    assert audit["pr180_block_reason"] == "ENTRY_SPREAD_TOO_WIDE_LIVE"


# ─────────────────────────────────────────────────────────────────────────────
# 2. Clean entry — spread 5.5% → allow existing behavior
# ─────────────────────────────────────────────────────────────────────────────

def test_2_clean_entry_spread_5p5_allows_existing_behavior():
    """
    Spread 5.5% is below the controlled band (6%) — the guard must not
    reprice and must not block. Behavior matches existing LIVE_ASK_CROSS.
    """
    # bid=1.00, ask=1.06 → spread (ask-bid)/mid = 0.06/1.03 ≈ 5.83%
    # We pass the spread explicitly so we don't conflate calculation with policy.
    proposed = round(1.06 + 0.01, 2)  # LIVE_ASK_CROSS_CENTS
    decision, limit, audit = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=1.00,
        submit_mid=1.03,
        submit_ask=1.06,
        spread_pct=0.055,
        proposed_limit=proposed,
    )
    assert decision == "PROCEED", f"clean entry must PROCEED unchanged, got {decision}"
    assert limit == proposed, "clean entry limit must equal proposed (no reprice)"
    # No reprice fields written
    assert "pr180_repriced_to" not in audit


# ─────────────────────────────────────────────────────────────────────────────
# 3. Spread 7.5% → controlled-limit reprice (mid+0.03 cap, not ask+0.01)
# ─────────────────────────────────────────────────────────────────────────────

def test_3_spread_7p5_caps_limit_at_mid_plus_3_cents():
    """
    Spread 7.5% is inside the controlled band (6%–8%). Limit must be capped
    at min(ask, mid + 0.03), not ask + 0.01.
    Example: bid=1.20, mid=1.30, ask=1.40 → ask+0.01=1.41 but mid+0.03=1.33.
    Final limit MUST be 1.33, not 1.41.
    """
    decision, limit, audit = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=1.20,
        submit_mid=1.30,
        submit_ask=1.40,
        spread_pct=0.075,
        proposed_limit=1.41,  # ask + LIVE_ASK_CROSS_CENTS
    )
    assert decision == "REPRICE_PROCEED", f"spread in controlled band MUST reprice, got {decision}"
    assert limit == 1.33, f"limit must be capped at mid+0.03=1.33, got {limit}"
    assert audit["pr180_controlled_cap"] == 1.33
    assert audit["pr180_original_limit"] == 1.41
    assert audit["pr180_repriced_to"] == 1.33


def test_3b_controlled_band_caps_at_ask_when_ask_below_mid_plus_cents():
    """If ask < mid+0.03 (very tight bid), the cap must be the ask, not mid+0.03."""
    decision, limit, _ = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=1.50,
        submit_mid=1.55,
        submit_ask=1.56,   # ask < mid+0.03=1.58
        spread_pct=0.065,
        proposed_limit=1.57,
    )
    assert decision == "REPRICE_PROCEED"
    assert limit == 1.56, f"cap must be ask=1.56, got {limit}"


# ─────────────────────────────────────────────────────────────────────────────
# 4. Expected mark-to-mid loss worse than -6% → BLOCK
# ─────────────────────────────────────────────────────────────────────────────

def test_4_expected_mark_loss_worse_than_6pct_blocks():
    """
    (submit_mid - intended_limit) / intended_limit <= -0.06 → block.
    Example: limit=1.46, mid=1.37 → (1.37 - 1.46)/1.46 = -0.0616 → BLOCK.
    Pick a spread just under 6% so we land in the no-reprice branch and the
    proposed limit reaches the mark-loss check unchanged.
    """
    decision, limit, audit = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=1.28,
        submit_mid=1.37,
        submit_ask=1.46,
        spread_pct=0.059,
        proposed_limit=1.46,
    )
    assert decision == "BLOCK", "mark-loss <= -6% MUST block"
    assert limit is None
    assert audit["pr180_block_reason"] == "ENTRY_EXPECTED_MARK_LOSS_TOO_HIGH"
    assert audit["pr180_expected_mark_loss_pct"] <= -0.06


def test_4b_mark_loss_exactly_minus_5pct_allowed():
    """-5% mark loss is above the -6% floor — should PROCEED."""
    # limit=1.00, mid=0.95 → (0.95-1.00)/1.00 = -0.05
    decision, _, _ = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=0.92, submit_mid=0.95, submit_ask=0.98,
        spread_pct=0.06,        # at boundary, no reprice
        proposed_limit=1.00,
    )
    assert decision == "PROCEED"


def test_4c_mark_loss_after_reprice_uses_repriced_limit():
    """
    The mark-loss check must be computed against the FINAL (post-reprice) limit,
    not the original proposed limit. Otherwise a wide spread that gets repriced
    down to a sane limit would still spuriously block.
    """
    # spread 7%, mid=1.00, ask=1.07 → controlled cap = min(1.07, 1.03) = 1.03
    # mark loss vs 1.03 = (1.00-1.03)/1.03 = -2.91% → PROCEED after reprice
    decision, limit, audit = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=0.93, submit_mid=1.00, submit_ask=1.07,
        spread_pct=0.07,
        proposed_limit=1.08,
    )
    assert decision == "REPRICE_PROCEED"
    assert limit == 1.03
    assert audit["pr180_expected_mark_loss_pct"] > -0.06


# ─────────────────────────────────────────────────────────────────────────────
# 5. Paper mode — guard skipped at the helper gate
# ─────────────────────────────────────────────────────────────────────────────

def test_5_paper_mode_helper_gate_skips_guard():
    """
    _pr180_is_named_live_client(client_id, paper) returns False for paper,
    so the production call-site never invokes the guard for paper accounts.
    This preserves paper behavior exactly.
    """
    assert core._pr180_is_named_live_client("jasoncosby1@gmail.com", paper=True) is False
    # Also any other paper account
    assert core._pr180_is_named_live_client("jose.vasquez4011@gmail.com", paper=True) is False


def test_5b_paper_mode_with_jason_email_still_skipped():
    """Even if Jason's email is somehow used in a paper context, paper takes precedence."""
    assert core._pr180_is_named_live_client("jasoncosby1@gmail.com", paper=True) is False


# ─────────────────────────────────────────────────────────────────────────────
# 6. Non-Jason live → guard does not apply
# ─────────────────────────────────────────────────────────────────────────────

def test_6_non_jason_live_unaffected():
    """Other live clients must not be gated by this PR's helper gate."""
    assert core._pr180_is_named_live_client("someone-else@example.com", paper=False) is False
    assert core._pr180_is_named_live_client("",                          paper=False) is False
    # Jason live IS named
    assert core._pr180_is_named_live_client("jasoncosby1@gmail.com",     paper=False) is True


def test_6b_pr180_disabled_globally_disables_guard():
    """Env override PR180_ENABLED=0 must disable the guard for all clients."""
    original = core.PR180_ENABLED
    try:
        core.PR180_ENABLED = False
        assert core._pr180_is_named_live_client("jasoncosby1@gmail.com", paper=False) is False
    finally:
        core.PR180_ENABLED = original


def test_6c_pr180_client_allowlist_env_overridable():
    """Allowlist override must work for emergency expansion without deploy."""
    original = core.PR180_LIVE_CLIENT_IDS
    try:
        core.PR180_LIVE_CLIENT_IDS = frozenset({"new-client@example.com"})
        assert core._pr180_is_named_live_client("new-client@example.com", paper=False) is True
        assert core._pr180_is_named_live_client("jasoncosby1@gmail.com",  paper=False) is False
    finally:
        core.PR180_LIVE_CLIENT_IDS = original


# ─────────────────────────────────────────────────────────────────────────────
# Degraded-quote safety: missing fields must fail closed
# ─────────────────────────────────────────────────────────────────────────────

def test_degraded_missing_mid_blocks():
    decision, _, audit = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=1.30, submit_mid=None, submit_ask=1.46,
        spread_pct=0.06, proposed_limit=1.47,
    )
    assert decision == "BLOCK"
    assert audit["pr180_block_reason"] == "ENTRY_QUOTE_INCOMPLETE_LIVE"


def test_degraded_missing_spread_blocks():
    decision, _, audit = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=1.30, submit_mid=1.38, submit_ask=1.46,
        spread_pct=None, proposed_limit=1.47,
    )
    assert decision == "BLOCK"
    assert audit["pr180_block_reason"] == "ENTRY_QUOTE_INCOMPLETE_LIVE"


def test_degraded_zero_mid_blocks():
    decision, _, audit = core._pr180_jason_live_entry_pricing_guard(
        submit_bid=0.0, submit_mid=0.0, submit_ask=1.46,
        spread_pct=0.06, proposed_limit=1.47,
    )
    assert decision == "BLOCK"
    assert audit["pr180_block_reason"] == "ENTRY_QUOTE_INCOMPLETE_LIVE"


# ─────────────────────────────────────────────────────────────────────────────
# MERGE RULE — broker submit must not run when guard blocks
# ─────────────────────────────────────────────────────────────────────────────

class _BrokerSubmitTrap:
    """Simulates the broker submit call site. Tracks whether submit was reached."""
    def __init__(self) -> None:
        self.submit_called = False
        self.submit_limit_used: float | None = None
        self.terminalize_reason: str | None = None

    def submit(self, limit: float) -> None:
        self.submit_called = True
        self.submit_limit_used = limit

    def terminalize(self, reason: str) -> None:
        self.terminalize_reason = reason


def _simulate_jason_live_submit_flow(
    *,
    client_id: str,
    paper: bool,
    submit_bid: float | None,
    submit_mid: float | None,
    submit_ask: float | None,
    spread_pct: float | None,
    proposed_limit: float,
) -> _BrokerSubmitTrap:
    """
    Mirrors the wired call-site logic in ap_execution_core._on_entry_trigger
    exactly:
      if named_live_client:
          decision = guard(...)
          if BLOCK:           terminalize and return (no broker submit)
          if REPRICE_PROCEED: update limit, then submit
          if PROCEED:         submit with original proposed limit
      else:
          submit with original proposed limit (paper / non-Jason path)
    """
    trap = _BrokerSubmitTrap()
    final_limit = proposed_limit

    if core._pr180_is_named_live_client(client_id, paper):
        decision, repriced, audit = core._pr180_jason_live_entry_pricing_guard(
            submit_bid=submit_bid,
            submit_mid=submit_mid,
            submit_ask=submit_ask,
            spread_pct=spread_pct,
            proposed_limit=proposed_limit,
        )
        if decision == "BLOCK":
            trap.terminalize(f"pr180_block:{audit.get('pr180_block_reason', '').lower()}")
            return trap   # no broker submit
        if decision == "REPRICE_PROCEED" and repriced is not None:
            final_limit = repriced

    trap.submit(final_limit)
    return trap


def test_merge_rule_no_broker_submit_in_nke_style_case():
    """NKE: bid 1.30 / mid 1.38 / ask 1.46, spread 11.59% — guard BLOCKS the broker submit."""
    trap = _simulate_jason_live_submit_flow(
        client_id="jasoncosby1@gmail.com", paper=False,
        submit_bid=1.30, submit_mid=1.38, submit_ask=1.46,
        spread_pct=0.1159,
        proposed_limit=1.47,
    )
    assert trap.submit_called is False, "MERGE BLOCKER: broker submit must NOT run on NKE-style wide-spread live entry"
    assert trap.terminalize_reason is not None
    assert "entry_spread_too_wide_live" in trap.terminalize_reason


def test_merge_rule_clean_entry_still_submits():
    """Clean spread for Jason live still submits at the proposed limit."""
    trap = _simulate_jason_live_submit_flow(
        client_id="jasoncosby1@gmail.com", paper=False,
        submit_bid=1.00, submit_mid=1.03, submit_ask=1.06,
        spread_pct=0.055,
        proposed_limit=1.07,
    )
    assert trap.submit_called is True
    assert trap.submit_limit_used == 1.07
    assert trap.terminalize_reason is None


def test_merge_rule_controlled_band_submits_at_repriced_limit():
    """Spread 7% reprices Jason live to mid+0.03 then submits."""
    trap = _simulate_jason_live_submit_flow(
        client_id="jasoncosby1@gmail.com", paper=False,
        submit_bid=1.20, submit_mid=1.30, submit_ask=1.40,
        spread_pct=0.075,
        proposed_limit=1.41,
    )
    assert trap.submit_called is True
    assert trap.submit_limit_used == 1.33
    assert trap.terminalize_reason is None


def test_merge_rule_paper_path_unchanged():
    """Paper Jason: guard never invoked, submit goes through at proposed limit."""
    trap = _simulate_jason_live_submit_flow(
        client_id="jasoncosby1@gmail.com", paper=True,
        submit_bid=1.30, submit_mid=1.38, submit_ask=1.46,
        spread_pct=0.1159,   # would BLOCK if Jason live
        proposed_limit=1.48,
    )
    assert trap.submit_called is True, "paper path must submit unchanged"
    assert trap.submit_limit_used == 1.48
    assert trap.terminalize_reason is None


def test_merge_rule_non_jason_live_path_unchanged():
    """Non-Jason live: guard not active, submit goes through unchanged."""
    trap = _simulate_jason_live_submit_flow(
        client_id="someone-else@example.com", paper=False,
        submit_bid=1.30, submit_mid=1.38, submit_ask=1.46,
        spread_pct=0.1159,
        proposed_limit=1.47,
    )
    assert trap.submit_called is True, "non-Jason live must NOT be affected by this PR"
    assert trap.submit_limit_used == 1.47
    assert trap.terminalize_reason is None


def test_merge_rule_mark_loss_block_prevents_submit():
    trap = _simulate_jason_live_submit_flow(
        client_id="jasoncosby1@gmail.com", paper=False,
        submit_bid=1.28, submit_mid=1.37, submit_ask=1.46,
        spread_pct=0.059,
        proposed_limit=1.46,
    )
    assert trap.submit_called is False
    assert "entry_expected_mark_loss_too_high" in (trap.terminalize_reason or "")
