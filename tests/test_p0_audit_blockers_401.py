"""Audit-blocker regressions for PR #401.

Blocker 1: conflicting retry counters can bypass chart revalidation.

Covers _resolve_selector_attempt_number() -- the replacement for the naive
first-truthy `retry_attempt or materialization_attempts or
_recovery_pre_claimed_attempt or 1` chain that could silently mask a higher
attempt count behind a lower, merely-truthy one (e.g. retry_attempt=1
masking materialization_attempts=2), skipping the attempt-2+ chart
revalidation fence.
"""

from __future__ import annotations

import pytest

from ap_execution_core import _resolve_selector_attempt_number


class TestAttemptCounterConflictResolution:
    def test_conflicting_counters_fail_closed(self):
        """The exact failure scenario from the audit: retry_attempt=1 must
        not silently mask materialization_attempts=2 / preclaimed=2."""
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=1,
            breach_attempt_count=None,
            materialization_attempts=2,
            recovery_pre_claimed_attempt=2,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_reverse_conflict_also_fails_closed(self):
        """The mirror: a higher retry_attempt against a lower durable
        materialization_attempts must also be rejected, not silently
        accepted just because it's the larger of the two."""
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=3,
            breach_attempt_count=3,
            materialization_attempts=1,
            recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_all_three_durable_counters_disagree(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=1,
            breach_attempt_count=2,
            materialization_attempts=3,
            recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_missing_one_field_uses_the_agreeing_remainder(self):
        """A single absent field (None -- never written by this row's
        history) is not itself a conflict; the counters that ARE present
        must still agree, and here they do."""
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=2,
            breach_attempt_count=None,
            materialization_attempts=2,
            recovery_pre_claimed_attempt=None,
        )
        assert resolved == 2
        assert reason is None

    def test_only_one_field_present_is_trusted_alone(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=None,
            breach_attempt_count=None,
            materialization_attempts=4,
            recovery_pre_claimed_attempt=None,
        )
        assert resolved == 4
        assert reason is None

    def test_all_fields_absent_defaults_to_attempt_one(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=None,
            breach_attempt_count=None,
            materialization_attempts=None,
            recovery_pre_claimed_attempt=None,
        )
        assert resolved == 1
        assert reason is None

    @pytest.mark.parametrize(
        "bad_value",
        ["not-a-number", -1, -5, float("nan"), object(), [1], {"x": 1}],
    )
    def test_malformed_field_fails_closed(self, bad_value):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=bad_value,
            breach_attempt_count=None,
            materialization_attempts=None,
            recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    @pytest.mark.parametrize(
        "field_name",
        ["breach_attempt_count", "materialization_attempts", "recovery_pre_claimed_attempt"],
    )
    def test_malformed_field_fails_closed_regardless_of_which_field(self, field_name):
        kwargs = {
            "retry_attempt": 1,
            "breach_attempt_count": 1,
            "materialization_attempts": 1,
            "recovery_pre_claimed_attempt": None,
        }
        kwargs[field_name] = "garbage"
        resolved, reason = _resolve_selector_attempt_number(**kwargs)
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_negative_value_fails_closed(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=-1,
            breach_attempt_count=None,
            materialization_attempts=None,
            recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_conflict_following_restart_shape(self):
        """Mirrors the exact restart scenario: durable counters written by
        an older code path (materialization_attempts only) disagree with a
        pre-claim made during this restart's recovery -- must fail closed,
        not silently trust whichever value happens to be non-None first."""
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=None,
            breach_attempt_count=None,
            materialization_attempts=2,
            recovery_pre_claimed_attempt=1,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_pre_claim_forward_progress_over_durable_is_accepted(self):
        """A pre-claim strictly ahead of the agreed durable value represents
        legitimate mid-CAS-transition forward progress (the durable row
        hasn't been updated yet), not a conflict -- this is the one case
        where disagreement is expected and must resolve to the higher
        value, exactly as the audit's 'use the highest proven attempt only
        after validating ownership and lineage' requirement describes."""
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=1,
            breach_attempt_count=1,
            materialization_attempts=1,
            recovery_pre_claimed_attempt=2,
        )
        assert resolved == 2
        assert reason is None

    def test_pre_claim_behind_durable_is_a_conflict_not_forward_progress(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=3,
            breach_attempt_count=3,
            materialization_attempts=3,
            recovery_pre_claimed_attempt=1,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_pre_claim_equal_to_durable_is_not_a_conflict(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=2,
            breach_attempt_count=2,
            materialization_attempts=2,
            recovery_pre_claimed_attempt=2,
        )
        assert resolved == 2
        assert reason is None


class TestSecondAttemptChartGateStillRuns:
    """Proof that the fix doesn't disable the attempt-2+ chart revalidation
    fence for the ordinary, non-conflicting case -- only conflicting/
    malformed metadata is rejected; agreeing counters resolve normally and
    flow into the existing gate exactly as before.

    The chart-truth gate itself (validate_retry_market_quote_authority /
    check_market_validity_gate / classify_market_truth) and its zero-
    selector-call proof on all three fail branches is already covered
    behaviorally by tests/test_p0_selector_recovery_july27_replay.py
    (test_market_truth_unavailable_holds_without_selector_or_broker,
    test_direction_reversal_rearms_with_zero_selector_work,
    test_terminal_geometry_precedes_reversal) and
    tests/test_p0_item11_behavioral_regressions.py's PAPER/LIVE transport
    proofs -- this class specifically proves the resolver hands those gates
    a correct, non-conflicting attempt number rather than short-circuiting
    them.
    """

    def test_agreeing_counters_resolve_to_attempt_two_not_one(self):
        """This is the exact shape that would have been silently
        misresolved to attempt 1 (skipping the gate) before the fix, if the
        counters disagreed -- here they agree, and must resolve to the true
        attempt 2, which is what the caller then passes into
        load_selector_recovery_cursor(selector_attempt_count=...) and the
        `if _selector_attempt_number > 1:` chart-gate condition."""
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=2,
            breach_attempt_count=2,
            materialization_attempts=2,
            recovery_pre_claimed_attempt=None,
        )
        assert reason is None
        assert resolved == 2
        assert resolved > 1, (
            "attempt 2 must resolve as > 1 so the caller's "
            "`if _selector_attempt_number > 1:` chart-gate condition "
            "actually activates"
        )

    def test_attempt_one_correctly_does_not_trigger_the_gate(self):
        """Symmetry check: a genuine first attempt (all counters absent or
        agreeing at 1) must resolve to 1, correctly leaving the gate
        inactive -- the fix must not become overzealous and gate ordinary
        first attempts too."""
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=None,
            breach_attempt_count=None,
            materialization_attempts=None,
            recovery_pre_claimed_attempt=None,
        )
        assert reason is None
        assert resolved == 1


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 2: retry maximum authority split between incompatible defaults.
# ─────────────────────────────────────────────────────────────────────────────

class TestRetryMaximumAuthorityConsistency:
    """Proves the three consumers of the deferred-materialization retry
    ceiling agree with each other, both with the environment unset and
    under the documented rollback configuration -- closing the gap where
    ap/deferred_materializer.py defaulted to 3 while
    ap_execution_core.py's MAX_BREACH_SELECTOR_RETRIES and
    ap/pending_trigger_restart_recovery.py's DEFERRED_MATERIALIZATION_MAX_ATTEMPTS
    both defaulted to 5.
    """

    def _resolved_maxima(self, monkeypatch, *, mat_env=None, breach_env=None):
        if mat_env is None:
            monkeypatch.delenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", raising=False)
        else:
            monkeypatch.setenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", mat_env)
        if breach_env is None:
            monkeypatch.delenv("MAX_BREACH_SELECTOR_RETRIES", raising=False)
        else:
            monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", breach_env)

        from ap_execution_core import _positive_int_env_config
        from ap.pending_trigger_restart_recovery import _env_int, _MAT_MAX_ATTEMPTS_ENV
        import ap.deferred_materializer as deferred_materializer_mod

        execution_core_max = _positive_int_env_config("MAX_BREACH_SELECTOR_RETRIES", 5)
        restart_recovery_max = _env_int(_MAT_MAX_ATTEMPTS_ENV, 5)
        deferred_materializer_max = deferred_materializer_mod._cfg()["max_attempts"]
        return execution_core_max, restart_recovery_max, deferred_materializer_max

    def test_env_unset_all_three_consumers_agree_at_five(self, monkeypatch):
        execution_core, restart_recovery, materializer = self._resolved_maxima(
            monkeypatch,
        )
        assert execution_core == 5
        assert restart_recovery == 5
        assert materializer == 5
        assert execution_core == restart_recovery == materializer

    def test_documented_rollback_configuration_restores_three_everywhere(
        self, monkeypatch,
    ):
        """The documented rollback sets both env vars back to 3 -- proving
        that MAX_BREACH_SELECTOR_RETRIES=3 alone is NOT sufficient (it only
        governs the execution-core selector loop); DEFERRED_MATERIALIZATION_
        MAX_ATTEMPTS=3 must also be set to roll back restart recovery and
        the deferred materializer, since those two share that env var name
        independently of MAX_BREACH_SELECTOR_RETRIES."""
        execution_core, restart_recovery, materializer = self._resolved_maxima(
            monkeypatch, mat_env="3", breach_env="3",
        )
        assert execution_core == 3
        assert restart_recovery == 3
        assert materializer == 3
        assert execution_core == restart_recovery == materializer

    def test_breach_env_alone_does_not_roll_back_the_other_two(self, monkeypatch):
        """Documents the real, non-unified-authority shape rather than
        hiding it: MAX_BREACH_SELECTOR_RETRIES is a genuinely separate env
        var from DEFERRED_MATERIALIZATION_MAX_ATTEMPTS. Setting only the
        former rolls back the execution-core selector loop but leaves
        restart recovery and the deferred materializer at their own
        (now-aligned) default of 5."""
        execution_core, restart_recovery, materializer = self._resolved_maxima(
            monkeypatch, mat_env=None, breach_env="3",
        )
        assert execution_core == 3
        assert restart_recovery == 5
        assert materializer == 5


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 3: contradictory quote provenance is not actually rejected.
# ─────────────────────────────────────────────────────────────────────────────

from datetime import datetime, timezone
from ap.live_submit_gates import validate_retry_market_quote_authority


class _FakeTransport:
    def __init__(self, base_url: str = "https://api.tradier.com"):
        self.cfg = type("Cfg", (), {"base_url": base_url})()


def _fresh_ts() -> str:
    return datetime.now(timezone.utc).isoformat()


class TestContradictoryQuoteProvenanceRejected:
    def test_two_simultaneous_source_fields_one_unapproved_is_rejected(self):
        """The exact audit example: source is approved, provider is not --
        previously only 'source' was ever inspected via `or`-chaining."""
        quote = {
            "bid": 449.9, "ask": 450.1,
            "provider_timestamp": _fresh_ts(),
            "source": "tradier_live",
            "provider": "simulator",
        }
        result = validate_retry_market_quote_authority(quote, transport=_FakeTransport())
        assert result["valid"] is False
        assert result["reason"] == "MARKET_QUOTE_SOURCE_UNPROVEN"

    def test_source_and_quote_source_disagree_is_rejected(self):
        """The second audit example: source approved, quote_source is not."""
        quote = {
            "bid": 449.9, "ask": 450.1,
            "provider_timestamp": _fresh_ts(),
            "source": "tradier_live",
            "quote_source": "polygon",
        }
        result = validate_retry_market_quote_authority(quote, transport=_FakeTransport())
        assert result["valid"] is False
        assert result["reason"] == "MARKET_QUOTE_SOURCE_UNPROVEN"

    def test_three_simultaneous_source_fields_one_unapproved_is_rejected(self):
        quote = {
            "bid": 449.9, "ask": 450.1,
            "provider_timestamp": _fresh_ts(),
            "source": "tradier_live",
            "quote_source": "tradier",
            "provider": "not_approved_at_all",
        }
        result = validate_retry_market_quote_authority(quote, transport=_FakeTransport())
        assert result["valid"] is False
        assert result["reason"] == "MARKET_QUOTE_SOURCE_UNPROVEN"

    def test_all_three_fields_approved_and_consistent_passes(self):
        """Positive control: multiple explicit fields, all individually
        approved, none contradictory -- must still pass."""
        quote = {
            "bid": 449.9, "ask": 450.1,
            "provider_timestamp": _fresh_ts(),
            "source": "tradier_live",
            "quote_source": "tradier_live",
            "provider": "tradier",
        }
        result = validate_retry_market_quote_authority(quote, transport=_FakeTransport())
        assert result["valid"] is True

    def test_source_less_payload_still_accepted_on_approved_transport(self):
        """Positive control: real Tradier production payloads omit source
        metadata entirely -- must still pass on the exact approved
        transport, unaffected by the multi-field validation change."""
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(quote, transport=_FakeTransport())
        assert result["valid"] is True
        assert result["quote_source"] == "tradier_live"

    def test_explicit_falsy_bid_date_is_treated_as_invalid_not_absent(self):
        """bid_date=0, explicitly present, must not silently fall back to a
        fresh common provider_timestamp -- it must be evaluated on its own
        (as an extremely stale/invalid epoch-0 timestamp) and rejected."""
        quote = {
            "bid": 449.9, "ask": 450.1,
            "bid_date": 0,
            "provider_timestamp": _fresh_ts(),
        }
        result = validate_retry_market_quote_authority(quote, transport=_FakeTransport())
        assert result["valid"] is False
        assert result["reason"] in ("MARKET_QUOTE_STALE", "MARKET_QUOTE_TIMESTAMP_UNPROVEN")

    def test_explicit_empty_string_ask_date_is_treated_as_invalid_not_absent(self):
        quote = {
            "bid": 449.9, "ask": 450.1,
            "ask_date": "",
            "provider_timestamp": _fresh_ts(),
        }
        result = validate_retry_market_quote_authority(quote, transport=_FakeTransport())
        assert result["valid"] is False
        assert result["reason"] in ("MARKET_QUOTE_STALE", "MARKET_QUOTE_TIMESTAMP_UNPROVEN")

    def test_genuinely_absent_leg_timestamp_still_falls_back_to_common(self):
        """Positive control: when bid_date is genuinely never supplied (key
        absent entirely, not present-but-falsy), falling back to a fresh
        common provider_timestamp remains correct behavior."""
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        assert "bid_date" not in quote
        result = validate_retry_market_quote_authority(quote, transport=_FakeTransport())
        assert result["valid"] is True

    def test_custom_port_rejected_even_with_exact_hostname(self):
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(
            quote, transport=_FakeTransport("https://api.tradier.com:8443")
        )
        assert result["valid"] is False
        assert result["reason"] == "MARKET_QUOTE_UNAPPROVED_TRANSPORT"

    def test_default_https_port_443_explicit_still_accepted(self):
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(
            quote, transport=_FakeTransport("https://api.tradier.com:443")
        )
        assert result["valid"] is True
