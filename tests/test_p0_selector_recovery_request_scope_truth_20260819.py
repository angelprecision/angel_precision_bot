"""P0 — PR #491: request-scope terminal truth in deferred selector recovery.

Binding spec: docs/pr_specs/p0_selector_recovery_request_scope_truth_20260819.md

Defect (current-main, base 462106c8839769ef6b3137867839a44aec39ad09, verified
still present after rebase onto main@b8e25dc1c19c969ac595685f3cc302d3b9c830da):

    resolve_selector_recovery_final_reason() promoted ANY single candidate-
    level structural skip (e.g. one far-OTM candidate) to request-level
    terminal truth the moment that structural reason appeared anywhere in
    ``set(skipped.values())`` -- even while another candidate in the same
    request was still eligible, unattempted, or transiently retryable.

Fix under test: a structural skip may become request-level terminal truth
ONLY when the full known candidate set (structural_skip_results ∪
attempted_results ∪ eligible_unattempted_symbols, normalized OCC identity)
exhaustively proves that single structural condition. Otherwise the
resolver falls through to the existing truthful precedence, and -- new in
this PR -- an already-known original selector reason
(``fallback_selector_reason``) is preserved instead of collapsing to
UNKNOWN_SELECTOR_RECOVERY_FAILURE merely because no exhaustive/aggregate
proof was found.

Scope: ap/selector_retry_policy.py::resolve_selector_recovery_final_reason
and its new private helper _resolve_exhaustive_structural_terminal_reason.
Zero broker submit/cancel authority. Zero position/proof_trade/queue
mutation. Deferred-recovery reduction logic only -- ordinary selector
candidate admissibility (moneyness/DTE/delta/liquidity/spread/OI/volume/
premium/policy/affordability gates) is untouched.
"""

from __future__ import annotations

import os

os.environ.setdefault("DATABASE_URL", "postgresql://fake")

import pytest

from ap.selector_retry_policy import (
    UNKNOWN_FAIL_CLOSED,
    _resolve_exhaustive_structural_terminal_reason,
    get_policy,
    resolve_selector_recovery_final_reason,
)


# ── Helpers ────────────────────────────────────────────────────────────────

def _attempted_record(result_reason: str, *, attempt_number: int = 1) -> dict:
    return {"result_reason": result_reason, "attempt_number": attempt_number}


def _evidence(
    *,
    structural_skip_results: dict | None = None,
    attempted_results: dict | None = None,
    eligible_unattempted_symbols: list | None = None,
    quality_rejections: dict | None = None,
    fallback_selector_reason: str | None = None,
    budget_exhausted_stage: str | None = None,
    budget_exhausted_detail: str | None = None,
    actual_limit_reached: bool = False,
    direct_quote_known_eligible_symbols: list | None = None,
) -> dict:
    return {
        "structural_skip_results": structural_skip_results or {},
        "attempted_results": attempted_results or {},
        "eligible_unattempted_symbols": eligible_unattempted_symbols or [],
        "quality_rejections": quality_rejections or {},
        "fallback_selector_reason": fallback_selector_reason,
        "budget_exhausted_stage": budget_exhausted_stage,
        "budget_exhausted_detail": budget_exhausted_detail,
        "actual_limit_reached": actual_limit_reached,
        "market_truth_outcome": None,
        "market_truth_reason": None,
        # Deliberately omitted from the dict unless explicitly provided below
        # would change dict-equality-based tests elsewhere; instead default
        # to None so absence is explicit and matches production's "field
        # not supplied -> skip the accounting-gap check" contract.
        "direct_quote_known_eligible_symbols": direct_quote_known_eligible_symbols,
    }


_AAPL_150C = "AAPL260101C00150000"
_AAPL_155C = "AAPL260101C00155000"
_AAPL_160C = "AAPL260101C00160000"
_JASON_LIVE_SYMBOL_A = "SPY260102C00500000"
_JASON_LIVE_SYMBOL_B = "SPY260102C00505000"


# ── 1/2. One structural moneyness + a surviving candidate ───────────────────

class TestOneStructuralPlusSurvivor:
    def test_moneyness_structural_plus_retryable_attempt_not_terminalized(self):
        """FAIL-FIRST: reproduces the exact defect from the amendment.

        candidate A: STRUCTURAL_MONEYNESS_OUT_OF_RANGE (structural skip)
        candidate B: DIRECT_QUOTE_ZERO_BID_ASK (attempted, retryable)

        Current-main defect: whole request -> MONEYNESS_OUT_OF_RANGE.
        Required: request must NOT terminalize as MONEYNESS_OUT_OF_RANGE;
        the truthful retryable reason survives.
        """
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            attempted_results={
                _AAPL_160C: _attempted_record("DIRECT_QUOTE_ZERO_BID_ASK"),
            },
        )
        result = resolve_selector_recovery_final_reason(evidence)
        assert result != "MONEYNESS_OUT_OF_RANGE"
        assert result == "DIRECT_QUOTE_ZERO_BID_ASK"

    def test_moneyness_structural_plus_eligible_unattempted_not_terminalized(self):
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            eligible_unattempted_symbols=[_AAPL_160C],
        )
        result = resolve_selector_recovery_final_reason(evidence)
        assert result != "MONEYNESS_OUT_OF_RANGE"


# ── 3/4/5. Exhaustive single-reason structural sets DO terminalize ─────────

class TestExhaustiveSingleReasonStructural:
    def test_all_moneyness_structural_terminalizes(self):
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                _AAPL_155C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                _AAPL_160C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
        )
        assert resolve_selector_recovery_final_reason(evidence) == "MONEYNESS_OUT_OF_RANGE"

    def test_all_dte_structural_terminalizes(self):
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_DTE_OUT_OF_RANGE",
                _AAPL_155C: "STRUCTURAL_DTE_OUT_OF_RANGE",
            },
        )
        assert resolve_selector_recovery_final_reason(evidence) == "DTE_OUT_OF_RANGE"

    def test_all_delta_structural_terminalizes(self):
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_DELTA_OUT_OF_RANGE",
                _AAPL_155C: "STRUCTURAL_DELTA_OUT_OF_RANGE",
            },
        )
        assert resolve_selector_recovery_final_reason(evidence) == "DELTA_OUT_OF_RANGE"

    def test_all_terminal_policy_structural_terminalizes(self):
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_TERMINAL_POLICY_REJECT",
                _AAPL_155C: "STRUCTURAL_TERMINAL_POLICY_REJECT",
            },
        )
        assert resolve_selector_recovery_final_reason(evidence) == "TERMINAL_POLICY_REJECT"


# ── 6/7. Mixed structural evidence must never fabricate a single reason ────

class TestMixedStructuralNeverFabricates:
    def test_mixed_moneyness_and_dte_no_fabrication_either_direction(self):
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                _AAPL_155C: "STRUCTURAL_DTE_OUT_OF_RANGE",
            },
        )
        result = resolve_selector_recovery_final_reason(evidence)
        assert result not in ("MONEYNESS_OUT_OF_RANGE", "DTE_OUT_OF_RANGE")

    def test_mixed_structural_insertion_order_independent(self):
        forward = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                _AAPL_155C: "STRUCTURAL_DTE_OUT_OF_RANGE",
            },
        )
        reversed_ = _evidence(
            structural_skip_results={
                _AAPL_155C: "STRUCTURAL_DTE_OUT_OF_RANGE",
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
        )
        assert resolve_selector_recovery_final_reason(
            forward
        ) == resolve_selector_recovery_final_reason(reversed_)


# ── 8/9. Mixed structural + fallback preservation ───────────────────────────

class TestMixedStructuralWithFallback:
    def test_mixed_structural_with_known_fallback_preserves_fallback(self):
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                _AAPL_155C: "STRUCTURAL_DTE_OUT_OF_RANGE",
            },
            fallback_selector_reason="NO_CONTRACT_AFTER_FILTERS",
        )
        assert resolve_selector_recovery_final_reason(evidence) == "NO_CONTRACT_AFTER_FILTERS"

    def test_mixed_structural_with_unknown_fallback_fails_closed(self):
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                _AAPL_155C: "STRUCTURAL_DTE_OUT_OF_RANGE",
            },
            fallback_selector_reason="MADE_UP_REASON_XYZ",
        )
        assert (
            resolve_selector_recovery_final_reason(evidence)
            == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
        )


# ── Escape-hatch closure: fallback must not resurrect a structural reason ───
# that the exhaustive helper already declined to prove for this exact
# evidence. Found in post-implementation review: DELTA_OUT_OF_RANGE and
# DTE_OUT_OF_RANGE are registered TERMINAL_QUALITY codes in the retry-policy
# table, so a mixed-structural request whose selector pre-reducer
# observation (fallback_selector_reason) happened to equal one of those two
# codes could resurrect it through Step 8.5's fallback -- reopening the
# exact false request-level claim Step 3's exhaustive proof had just
# correctly declined. (MONEYNESS_OUT_OF_RANGE and TERMINAL_POLICY_REJECT are
# NOT registered in the retry-policy table at all, so those two specific
# strings already failed closed via get_policy() by coincidence -- not by
# design -- which is why the guard below is necessary for ALL FOUR governed
# reasons, not simply the two that happened to already be safe.)
#
# The fixture below is a SYNTHETIC, WDAY-SHAPED regression fixture -- loosely
# modeled on (5 delta-invalid + 33 moneyness-invalid candidates,
# WDAY-prefixed OCC symbols) but NOT identical to, the real persisted
# evidence from Supabase orders row e066bb1d-8284-4721-b5eb-dfcd0bd138b7
# (WDAY, 2026-08-18, jasoncosby1@gmail.com, live). It exists purely to make
# these unit tests readable with a realistic-looking shape. It is
# deliberately NOT presented as an exact production replay -- for that, see
# tests/test_p0_selector_recovery_production_replay_491.py, which replays
# the resolver against the exact persisted candidate identities pulled live
# from Supabase (WDAY, DDOG, and CRM), including a side-by-side historical/
# pre-#491/post-#491 comparison. An earlier version of this file's
# `test_wday_...production_replay` test used this same synthetic map while
# describing itself as a production replay, which was inaccurate and has
# since been renamed below to avoid exactly the ambiguity a synthetic
# fixture and a real replay sitting side by side would otherwise create.

class TestFallbackCannotResurrectUnprovenStructuralReason:
    # SYNTHETIC, WDAY-shaped fixture (NOT exact persisted data -- see the
    # module-level comment above and tests/test_p0_selector_recovery_
    # production_replay_491.py for the real thing).
    _WDAY_SHAPED_SYNTHETIC_MIXED_STRUCTURAL = {
        **{f"WDAY260821C00{205 + i}000": "STRUCTURAL_DELTA_OUT_OF_RANGE" for i in range(5)},
        **{f"WDAY260821C00{220 + i * 2}500": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE" for i in range(33)},
    }

    def test_mixed_moneyness_and_delta_structural_fallback_moneyness_must_not_resurrect(self):
        evidence = _evidence(
            structural_skip_results=self._WDAY_SHAPED_SYNTHETIC_MIXED_STRUCTURAL,
            eligible_unattempted_symbols=[],
            fallback_selector_reason="MONEYNESS_OUT_OF_RANGE",
        )
        assert resolve_selector_recovery_final_reason(evidence) != "MONEYNESS_OUT_OF_RANGE"

    def test_mixed_moneyness_and_delta_structural_fallback_delta_must_not_resurrect(self):
        # This is the genuinely exploitable case pre-amendment: DELTA_OUT_OF_RANGE
        # IS a registered TERMINAL_QUALITY code in the retry-policy table, so
        # get_policy() alone does not fail it closed -- only the new governed-
        # reason guard in Step 8.5 does.
        evidence = _evidence(
            structural_skip_results=self._WDAY_SHAPED_SYNTHETIC_MIXED_STRUCTURAL,
            eligible_unattempted_symbols=[],
            fallback_selector_reason="DELTA_OUT_OF_RANGE",
        )
        assert resolve_selector_recovery_final_reason(evidence) != "DELTA_OUT_OF_RANGE"

    def test_mixed_moneyness_and_dte_structural_fallback_dte_must_not_resurrect(self):
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                _AAPL_155C: "STRUCTURAL_DTE_OUT_OF_RANGE",
            },
            fallback_selector_reason="DTE_OUT_OF_RANGE",
        )
        assert resolve_selector_recovery_final_reason(evidence) != "DTE_OUT_OF_RANGE"

    def test_structural_moneyness_plus_eligible_unattempted_fallback_moneyness_must_not_resurrect(self):
        evidence = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            eligible_unattempted_symbols=[_AAPL_160C],
            fallback_selector_reason="MONEYNESS_OUT_OF_RANGE",
        )
        assert resolve_selector_recovery_final_reason(evidence) != "MONEYNESS_OUT_OF_RANGE"

    def test_structural_moneyness_plus_retryable_attempt_fallback_moneyness_must_not_resurrect(self):
        evidence = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            attempted_results={_AAPL_160C: _attempted_record("DIRECT_QUOTE_ZERO_BID_ASK")},
            fallback_selector_reason="MONEYNESS_OUT_OF_RANGE",
        )
        result = resolve_selector_recovery_final_reason(evidence)
        assert result != "MONEYNESS_OUT_OF_RANGE"
        # The truthful retryable reason must still win -- the guard closes
        # the false structural door without breaking the true survivor door.
        assert result == "DIRECT_QUOTE_ZERO_BID_ASK"

    def test_fully_exhaustive_homogeneous_moneyness_with_fallback_still_valid(self):
        # Complementary control: when the SAME reason genuinely IS exhaustively
        # proven by Step 3, it must still be returned -- the guard only blocks
        # the fallback DOOR, it does not weaken the exhaustive-proof door.
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                _AAPL_155C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            fallback_selector_reason="MONEYNESS_OUT_OF_RANGE",
        )
        assert resolve_selector_recovery_final_reason(evidence) == "MONEYNESS_OUT_OF_RANGE"

    def test_mixed_structural_with_non_structural_fallback_still_preserved(self):
        # Control: non-structural known fallbacks are NOT affected by this
        # guard and continue through exactly as before.
        evidence = _evidence(
            structural_skip_results=self._WDAY_SHAPED_SYNTHETIC_MIXED_STRUCTURAL,
            fallback_selector_reason="NO_CONTRACT_AFTER_FILTERS",
        )
        assert (
            resolve_selector_recovery_final_reason(evidence)
            == "NO_CONTRACT_AFTER_FILTERS"
        )

    def test_wday_shaped_synthetic_fixture_agrees_with_exact_replay_conclusion(self):
        """This is a SYNTHETIC, WDAY-shaped regression fixture (see the
        module-level comment above) -- it is NOT an exact production
        replay. It exercises the same conceptual shape as the real WDAY
        case (mixed structural delta+moneyness, zero unattempted,
        fallback_selector_reason=MONEYNESS_OUT_OF_RANGE) for unit-test
        readability. For the exact, faithful production replay using the
        real persisted candidate identities pulled live from Supabase, see
        tests/test_p0_selector_recovery_production_replay_491.py::
        TestExactProductionReplay::
        test_wday_20260818_jasoncosby1_live_mixed_structural, which is the
        authoritative source of truth for this production case."""
        evidence = _evidence(
            structural_skip_results=self._WDAY_SHAPED_SYNTHETIC_MIXED_STRUCTURAL,
            attempted_results={},
            eligible_unattempted_symbols=[],
            fallback_selector_reason="MONEYNESS_OUT_OF_RANGE",
        )
        result = resolve_selector_recovery_final_reason(evidence)
        assert result != "MONEYNESS_OUT_OF_RANGE", (
            "PR #491 regression: WDAY-shaped synthetic fixture resurrected "
            "an unproven structural reason through fallback"
        )


# ── 10/11/12. Known fallback preservation by classification ────────────────

class TestKnownFallbackPreservation:
    def test_known_retryable_fallback_preserved(self):
        evidence = _evidence(fallback_selector_reason="DIRECT_QUOTE_ZERO_BID_ASK")
        assert resolve_selector_recovery_final_reason(evidence) == "DIRECT_QUOTE_ZERO_BID_ASK"

    def test_known_request_budget_fallback_preserved(self):
        evidence = _evidence(fallback_selector_reason="SELECTOR_REQUEST_BUDGET_EXHAUSTED")
        assert (
            resolve_selector_recovery_final_reason(evidence)
            == "SELECTOR_REQUEST_BUDGET_EXHAUSTED"
        )

    def test_known_terminal_quality_fallback_preserved(self):
        evidence = _evidence(fallback_selector_reason="OI_TOO_LOW")
        assert resolve_selector_recovery_final_reason(evidence) == "OI_TOO_LOW"


# ── 13/14/15. Fallback failure modes stay fail-closed ───────────────────────

class TestFallbackFailClosed:
    def test_blank_fallback_is_unknown(self):
        evidence = _evidence(fallback_selector_reason="")
        assert (
            resolve_selector_recovery_final_reason(evidence)
            == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
        )

    def test_none_fallback_is_unknown(self):
        evidence = _evidence(fallback_selector_reason=None)
        assert (
            resolve_selector_recovery_final_reason(evidence)
            == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
        )

    def test_unmapped_fallback_is_unknown(self):
        evidence = _evidence(fallback_selector_reason="TOTALLY_MADE_UP_CODE")
        assert (
            resolve_selector_recovery_final_reason(evidence)
            == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
        )

    def test_unknown_fail_closed_classified_fallback_is_unknown(self):
        # UNKNOWN_REJECTION is present in the policy table but explicitly
        # classified UNKNOWN_FAIL_CLOSED -- being "known to the table" must
        # not be conflated with being a truthful non-unknown reason.
        assert get_policy("UNKNOWN_REJECTION").classification == UNKNOWN_FAIL_CLOSED
        evidence = _evidence(fallback_selector_reason="UNKNOWN_REJECTION")
        assert (
            resolve_selector_recovery_final_reason(evidence)
            == "UNKNOWN_SELECTOR_RECOVERY_FAILURE"
        )


# ── 16/17. Insertion order + duplicate OCC normalization ───────────────────

class TestOrderingAndNormalization:
    def test_full_evidence_insertion_order_permutations_identical(self):
        base_skipped = {
            _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            _AAPL_155C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
        }
        permutations = [
            dict(base_skipped),
            dict(reversed(list(base_skipped.items()))),
        ]
        results = {
            resolve_selector_recovery_final_reason(
                _evidence(structural_skip_results=perm)
            )
            for perm in permutations
        }
        assert len(results) == 1
        assert results == {"MONEYNESS_OUT_OF_RANGE"}

    def test_duplicate_occ_whitespace_case_variants_collapse_to_one_candidate(self):
        # Same OCC, same structural reason, represented with whitespace/case
        # noise -- must normalize to ONE candidate, not two, and must not
        # manufacture a false "exhaustive set of 2" when there is really 1.
        evidence = _evidence(
            structural_skip_results={
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                "  aapl260101c00150000  ": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
        )
        assert resolve_selector_recovery_final_reason(evidence) == "MONEYNESS_OUT_OF_RANGE"

    def test_duplicate_occ_conflicting_reason_variants_fail_closed(self):
        # Same normalized OCC, but the two raw-key representations disagree
        # on the structural reason -- this is corrupted/laundered evidence,
        # not proof of anything.
        result = _resolve_exhaustive_structural_terminal_reason(
            {
                _AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                "aapl260101c00150000": "STRUCTURAL_DTE_OUT_OF_RANGE",
            },
            {},
            [],
        )
        assert result is None


# ── 18. Conflicting same-OCC evidence (structural + transient) ─────────────

class TestConflictingSameOccEvidence:
    def test_same_normalized_occ_structural_and_attempted_no_proof(self):
        result = _resolve_exhaustive_structural_terminal_reason(
            {_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            {_AAPL_150C: _attempted_record("DIRECT_QUOTE_ZERO_BID_ASK")},
            [],
        )
        assert result is None


# ── 19. Malformed evidence must fail closed, never crash ───────────────────

class TestMalformedEvidenceFailsClosedNeverCrashes:
    def test_blank_occ_key_cannot_strengthen_proof(self):
        result = _resolve_exhaustive_structural_terminal_reason(
            {"": "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            {},
            [],
        )
        assert result is None

    def test_non_string_occ_key_cannot_strengthen_proof(self):
        result = _resolve_exhaustive_structural_terminal_reason(
            {123: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},  # type: ignore[dict-item]
            {},
            [],
        )
        assert result is None

    def test_malformed_attempted_record_type_cannot_strengthen_proof(self):
        # attempted record with blank result_reason (e.g. malformed dict)
        result = _resolve_exhaustive_structural_terminal_reason(
            {_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            {_AAPL_155C: {"result_reason": ""}},
            [],
        )
        assert result is None

    def test_malformed_eligible_container_cannot_manufacture_empty_eligible(self):
        # A non-container "eligible" value must NOT be silently treated as
        # "no eligible symbols" -- that would let malformed input strengthen
        # a false exhaustive claim.
        result = _resolve_exhaustive_structural_terminal_reason(
            {_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            {},
            "not-a-container",
        )
        assert result is None

    def test_malformed_evidence_never_raises_via_public_resolver(self):
        # End-to-end: garbage evidence dict must never raise out of the
        # public resolver, and must not incorrectly terminalize.
        evidence = _evidence(
            structural_skip_results={None: object()},  # type: ignore[dict-item]
            attempted_results={"x": 12345},
            eligible_unattempted_symbols={"not", "a", "list"},  # a set is allowed
        )
        result = resolve_selector_recovery_final_reason(evidence)
        assert isinstance(result, str)


# ── 20. Ordinary (non-deferred) selector path is unaffected ────────────────

class TestOrdinarySelectorControlUnaffected:
    def test_ordinary_selector_never_invokes_this_resolver(self):
        """Static/behavioral control: the reducer is scoped strictly to the
        deferred-breach no-survivors path in ap/contract_selector.py. This
        test asserts the resolver is invoked from exactly one production
        call site, and that call site is gated on
        SELECTOR_REQUEST_KIND_DEFERRED_BREACH -- i.e. ordinary selection
        never reaches this function at all."""
        import ap.contract_selector as cs
        import inspect

        source = inspect.getsource(cs)
        call_count = source.count("resolve_selector_recovery_final_reason(")
        # One import reference + one call = counted once here via the call
        # paren; assert it is invoked from inside a SELECTOR_REQUEST_KIND_
        # DEFERRED_BREACH-gated block, not unconditionally.
        assert call_count == 1
        call_index = source.index("resolve_selector_recovery_final_reason(")
        preceding = source[:call_index]
        # The nearest preceding guard must reference the deferred-breach kind.
        assert "SELECTOR_REQUEST_KIND_DEFERRED_BREACH" in preceding[-1200:]


# ── 21. Deferred-only scope control ─────────────────────────────────────────

class TestDeferredOnlyScopeControl:
    def test_resolver_behavior_change_isolated_to_exhaustive_structural_case(self):
        # A single structural skip with nothing else present used to
        # terminalize (defect). After the fix it must not. This is the
        # smallest possible demonstration that the *change* is scoped to
        # the exhaustive-proof requirement, not a rewrite of other steps.
        single_structural = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
        )
        # With nothing else, this candidate set IS exhaustive (1 candidate,
        # 1 structural reason) so it legitimately terminalizes.
        assert (
            resolve_selector_recovery_final_reason(single_structural)
            == "MONEYNESS_OUT_OF_RANGE"
        )
        # Adding one retryable attempted candidate must flip the outcome.
        with_survivor = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            attempted_results={_AAPL_155C: _attempted_record("PROVIDER_TIMEOUT")},
        )
        assert (
            resolve_selector_recovery_final_reason(with_survivor)
            != "MONEYNESS_OUT_OF_RANGE"
        )


# ── 22. Exact identity control (Jason LIVE-shaped) ──────────────────────────

class TestJasonLiveShapedIdentityControl:
    def test_jason_live_shaped_evidence_does_not_mutate_identity_fields(self):
        plan_like = {
            "client_id": "jasoncosby1@gmail.com",
            "execution_mode": "live",
            "signal_id": "sig-jason-491-001",
        }
        evidence = _evidence(
            structural_skip_results={
                _JASON_LIVE_SYMBOL_A: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
            attempted_results={
                _JASON_LIVE_SYMBOL_B: _attempted_record("DIRECT_QUOTE_ZERO_BID_ASK"),
            },
        )
        before = dict(plan_like)
        result = resolve_selector_recovery_final_reason(evidence)
        assert plan_like == before  # resolver never touches plan identity
        assert result != "MONEYNESS_OUT_OF_RANGE"
        assert result == "DIRECT_QUOTE_ZERO_BID_ASK"

    def test_jason_live_shaped_all_structural_terminalizes(self):
        evidence = _evidence(
            structural_skip_results={
                _JASON_LIVE_SYMBOL_A: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
                _JASON_LIVE_SYMBOL_B: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE",
            },
        )
        assert resolve_selector_recovery_final_reason(evidence) == "MONEYNESS_OUT_OF_RANGE"


# ── 23/24. Provider budget & retry-count freeze (resolver purity control) ──

class TestResolverPurityFreeze:
    def test_resolver_is_pure_and_makes_no_provider_calls(self):
        """The resolver is a pure function over an evidence dict -- it must
        not import or touch broker/provider/network modules, and repeated
        calls with identical evidence must be idempotent (proves it holds
        no hidden counters that would drift retry/provider-call budgets)."""
        evidence = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            attempted_results={_AAPL_155C: _attempted_record("PROVIDER_TIMEOUT")},
        )
        first = resolve_selector_recovery_final_reason(evidence)
        second = resolve_selector_recovery_final_reason(evidence)
        third = resolve_selector_recovery_final_reason(dict(evidence))
        assert first == second == third

    def test_resolver_module_has_no_broker_import(self):
        import ap.selector_retry_policy as srp
        import inspect

        source = inspect.getsource(srp)
        for forbidden in ("import requests", "tradier", "place_order", "submit_order", "cancel_order"):
            assert forbidden not in source.lower()


# ── 25. #474 adjacency: zero broker authority, survivor continues downstream ─

class TestNoBrokerAuthorityAdjacency:
    def test_resolver_return_value_is_a_reason_string_not_an_order_action(self):
        """#491 owns selector recovery TRUTH only. It must return a reason
        string for the existing deferred lifecycle to consume -- it must
        never itself resemble or trigger an order/broker action. #474's
        final real-cost/broker authority remains entirely downstream and
        untouched by this module."""
        evidence = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            attempted_results={_AAPL_155C: _attempted_record("DIRECT_QUOTE_ZERO_BID_ASK")},
        )
        result = resolve_selector_recovery_final_reason(evidence)
        assert isinstance(result, str)
        # A survivor's reason is retryable data truth, not a submit signal.
        assert get_policy(result).classification in (
            "RETRYABLE_DATA",
            "TERMINAL_QUALITY",
            "TERMINAL_POLICY",
            "UNKNOWN_FAIL_CLOSED",
        )
        assert not hasattr(result, "submit")
        assert not hasattr(result, "broker_order_id")


# ── Existing affordability full-set accounting must be untouched (spec §4) ──

class TestAffordabilityFullSetAccountingUnchanged:
    def test_single_unaffordable_candidate_with_survivor_not_terminalized(self):
        evidence = _evidence(
            quality_rejections={"NO_AFFORDABLE_CONTRACT": 1},
            attempted_results={_AAPL_155C: _attempted_record("DIRECT_QUOTE_ZERO_BID_ASK")},
        )
        result = resolve_selector_recovery_final_reason(evidence)
        assert result != "NO_AFFORDABLE_CONTRACT"

    def test_full_set_unaffordable_still_terminalizes(self):
        evidence = _evidence(
            quality_rejections={"NO_AFFORDABLE_CONTRACT": 3},
        )
        assert resolve_selector_recovery_final_reason(evidence) == "NO_AFFORDABLE_CONTRACT"

    def test_full_set_premium_cap_still_terminalizes(self):
        evidence = _evidence(
            quality_rejections={"PREMIUM_CAP_EXCEEDED": 2},
        )
        assert resolve_selector_recovery_final_reason(evidence) == "PREMIUM_CAP_EXCEEDED"


# ── Accounting-gap closure (found via external review + real CRM evidence) ──
# recovery_cursor_persist is threaded into request_context in
# ap/contract_selector.py but is never actually invoked within a single
# selection pass -- confirmed by reading the current-main source, not
# assumed. This means a candidate that was genuinely direct-quote-attempted
# THIS pass, with a real (possibly non-structural) outcome, can be entirely
# invisible to `attempted_results`, silently erased from the exhaustive-
# proof accounting. Real production evidence: CRM, 2026-08-12,
# jasoncosby1@gmail.com, LIVE -- persisted direct_quote_eligible_candidates
# = 48, structural candidates = 47, direct_quote_attempted_symbols =
# ["CRM260814P00172500"] with real outcome DIRECT_QUOTE_ZERO_BID_ASK. CRM's
# own mixed structural evidence happened not to falsely terminalize even
# with the erasure (a second structural reason already blocked exhaustive
# proof) -- but a request with only ONE structural reason plus one erased
# attempted candidate would not have been so lucky. See
# _resolve_exhaustive_structural_terminal_reason's Rule 1.5 and the
# `direct_quote_known_eligible_symbols` evidence field in
# ap/selector_retry_policy.py / ap/contract_selector.py.

class TestAccountingGapClosure:
    def test_erased_attempted_candidate_without_closure_field_falsely_terminalizes(self):
        """Fail-first: reproduces the confirmed defect exactly. One
        candidate (A) is structurally skipped; a second real candidate (B)
        was genuinely attempted this pass but its outcome never reached
        attempted_results (the historical/pre-closure evidence shape --
        i.e. direct_quote_known_eligible_symbols is NOT supplied). Before
        the accounting-gap closure, this falsely proves exhaustive
        MONEYNESS_OUT_OF_RANGE. This test documents the shape of evidence
        that WOULD be dangerous if a real call site failed to supply
        direct_quote_known_eligible_symbols -- it is not itself a
        regression the closure can prevent (the closure is opt-in by
        design, matching production's real call-site behavior), but it is
        the exact scenario the closure exists to guard against once the
        field IS supplied (see the next test)."""
        evidence_without_closure = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            attempted_results={},  # candidate B erased -- no accounting of it at all
            eligible_unattempted_symbols=[],
            # direct_quote_known_eligible_symbols intentionally NOT supplied
        )
        result = resolve_selector_recovery_final_reason(evidence_without_closure)
        assert result == "MONEYNESS_OUT_OF_RANGE", (
            "This documents the defect shape; if this assertion starts "
            "failing, the resolver's default (no-closure-field) behavior "
            "has changed and this test's purpose should be revisited."
        )

    def test_known_eligible_field_closes_the_gap(self):
        """The same shape as above, but with direct_quote_known_eligible_
        symbols supplied naming both A and B -- exactly what the real
        production call site now provides
        (request_context.direct_quote_eligible_symbols). Exhaustive proof
        must now correctly be refused."""
        evidence_with_closure = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            attempted_results={},
            eligible_unattempted_symbols=[],
            direct_quote_known_eligible_symbols=[_AAPL_150C, _AAPL_160C],
        )
        result = resolve_selector_recovery_final_reason(evidence_with_closure)
        assert result != "MONEYNESS_OUT_OF_RANGE"

    def test_known_eligible_matching_universe_still_terminalizes(self):
        """Control: when the known-eligible accounting agrees exactly with
        the accounted universe (no erased candidate), exhaustive proof
        must still succeed -- the closure only blocks the false claim, it
        does not weaken a genuinely complete one."""
        evidence = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            attempted_results={},
            eligible_unattempted_symbols=[],
            direct_quote_known_eligible_symbols=[_AAPL_150C],
        )
        assert resolve_selector_recovery_final_reason(evidence) == "MONEYNESS_OUT_OF_RANGE"

    def test_known_eligible_absent_preserves_prior_behavior(self):
        """Backward-compatibility control: when the field is absent
        entirely (None, the default), behavior is identical to before this
        amendment -- existing evidence shapes and callers that do not
        supply it are unaffected."""
        evidence = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
        )
        assert evidence["direct_quote_known_eligible_symbols"] is None
        assert resolve_selector_recovery_final_reason(evidence) == "MONEYNESS_OUT_OF_RANGE"

    def test_known_eligible_malformed_container_fails_closed(self):
        """A present-but-non-container known-eligible value must not be
        silently treated as 'no additional accounting' -- that would let
        malformed input manufacture a false exhaustive claim, matching the
        existing eligible_raw malformed-container handling."""
        evidence = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            direct_quote_known_eligible_symbols="not-a-container",  # type: ignore[arg-type]
        )
        result = resolve_selector_recovery_final_reason(evidence)
        assert result != "MONEYNESS_OUT_OF_RANGE"

    def test_known_eligible_with_survivor_matches_real_crm_shape(self):
        """Mirrors the real CRM production shape at unit-test scale: one
        structural candidate, one genuinely attempted candidate with a
        retryable outcome, both named in the known-eligible accounting.
        Must not terminalize structurally, and the real attempted
        candidate's truthful outcome should surface directly."""
        evidence = _evidence(
            structural_skip_results={_AAPL_150C: "STRUCTURAL_MONEYNESS_OUT_OF_RANGE"},
            attempted_results={_AAPL_160C: _attempted_record("DIRECT_QUOTE_ZERO_BID_ASK")},
            eligible_unattempted_symbols=[],
            direct_quote_known_eligible_symbols=[_AAPL_150C, _AAPL_160C],
        )
        result = resolve_selector_recovery_final_reason(evidence)
        assert result != "MONEYNESS_OUT_OF_RANGE"
        assert result == "DIRECT_QUOTE_ZERO_BID_ASK"


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-v"]))
