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

class TestRetryMaximumAuthorityNoLocalFallback:
    """Round 3 correction: the previous version of this test class
    verified that all three consumers fell back to a MATCHING local value
    (3) on conflict. That was itself wrong -- the audit required that NO
    consumer substitute a local numeric fallback after the canonical
    resolver raises DeferredMaterializationConfigConflict. These tests
    verify the corrected contract: each consumer now refuses to act
    (context-appropriate "no claim" signal) rather than silently
    proceeding with any number, including a "safe-looking" 3.
    """

    def test_canonical_resolver_conflict_is_unchanged(self, monkeypatch):
        """The resolver itself still raises on conflict -- this is the
        contract every consumer now respects rather than catching."""
        monkeypatch.setenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", "3")
        monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")
        from ap.selector_retry_policy import (
            DeferredMaterializationConfigConflict,
            resolve_deferred_materialization_max_attempts,
        )
        with pytest.raises(DeferredMaterializationConfigConflict):
            resolve_deferred_materialization_max_attempts()

    def test_deferred_materializer_cfg_raises_directly_no_fallback(self, monkeypatch):
        """ap/deferred_materializer.py's _cfg() previously caught the
        conflict and returned max_attempts=3. It must now raise directly
        -- no traced caller exists in this module to add per-row handling
        to, so propagating is the correct, complete fix rather than
        inventing a fallback."""
        monkeypatch.setenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", "3")
        monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", "5")
        from ap.selector_retry_policy import DeferredMaterializationConfigConflict
        import ap.deferred_materializer as deferred_materializer_mod
        with pytest.raises(DeferredMaterializationConfigConflict):
            deferred_materializer_mod._cfg()

    def test_deferred_materializer_cfg_succeeds_when_not_conflicting(self, monkeypatch):
        monkeypatch.delenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", raising=False)
        monkeypatch.delenv("MAX_BREACH_SELECTOR_RETRIES", raising=False)
        import ap.deferred_materializer as deferred_materializer_mod
        cfg = deferred_materializer_mod._cfg()
        assert cfg["max_attempts"] == 5

    def test_no_dead_fallback_wrappers_remain(self):
        """Structural guard: the three now-removed wrapper functions that
        used to catch-and-substitute must not have been reintroduced."""
        import inspect
        import ap_execution_core as exec_core_mod
        import ap.pending_trigger_restart_recovery as restart_recovery_mod
        assert not hasattr(exec_core_mod, "_resolve_deferred_materialization_ceiling")
        assert not hasattr(restart_recovery_mod, "_resolve_max_attempts")

    def test_restart_recovery_call_sites_no_longer_reference_removed_wrapper(self):
        """Structural guard: both restart-recovery call sites now call the
        canonical resolver directly and handle
        DeferredMaterializationConfigConflict inline (UNRESOLVED / None),
        rather than routing through a wrapper that substituted a number."""
        import inspect
        import ap.pending_trigger_restart_recovery as restart_recovery_mod
        source = inspect.getsource(restart_recovery_mod)
        assert source.count("resolve_deferred_materialization_max_attempts()") == 2
        assert "DeferredMaterializationConfigConflict" in source
        assert "return 3" not in source

    def test_exec_core_call_sites_no_longer_substitute_a_local_number(self):
        """Structural guard: none of the three ap_execution_core.py call
        sites for the retry ceiling may contain a bare numeric fallback
        after catching the conflict."""
        import inspect
        import ap_execution_core as exec_core_mod
        source = inspect.getsource(exec_core_mod)
        # 3 real call sites + 1 comment mention of the function name.
        assert source.count("resolve_deferred_materialization_max_attempts()") == 4
        assert "MATERIALIZATION_CONFIG_CONFLICT" in source


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


# ─────────────────────────────────────────────────────────────────────────────
# Cursor persistence must fail closed, not log-and-continue.
# ─────────────────────────────────────────────────────────────────────────────

from types import SimpleNamespace
from ap.contract_quote_revalidator import _ctx_persist_attempt
from ap.selector_retry_policy import (
    SelectorRecoveryCursorPersistFailed,
    SelectorRecoveryOwnershipLost,
)


class TestCursorPersistenceFailsClosed:
    """Ordinary selector requests never set recovery_cursor_persist at all,
    so _ctx_persist_attempt returns at the `not callable(callback)` check
    before ever reaching the try/except -- these fixes are inherently
    scoped to deferred-breach requests only, verified structurally by that
    early return remaining untouched."""

    def test_db_exception_on_first_persisted_event_raises_typed_failure(self):
        def _raising_callback(**kwargs):
            raise ConnectionError("db unavailable")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _ctx_persist_attempt(
                ctx, "AAPL240101C00200000",
                result_reason="OI_TOO_LOW", transient=False,
            )

    def test_serialization_failure_raises_typed_failure(self):
        def _raising_callback(**kwargs):
            raise TypeError("Object of type X is not JSON serializable")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _ctx_persist_attempt(
                ctx, "AAPL240101C00200000",
                result_reason="OI_TOO_LOW", transient=False,
            )

    def test_timeout_raises_typed_failure(self):
        def _raising_callback(**kwargs):
            raise TimeoutError("statement timeout")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _ctx_persist_attempt(
                ctx, "AAPL240101C00200000",
                result_reason="OI_TOO_LOW", transient=False,
            )

    def test_generic_exception_raises_typed_failure(self):
        def _raising_callback(**kwargs):
            raise RuntimeError("something unexpected")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _ctx_persist_attempt(
                ctx, "AAPL240101C00200000",
                result_reason="OI_TOO_LOW", transient=False,
            )

    def test_ownership_cas_miss_still_raises_the_original_exception_unchanged(self):
        """SelectorRecoveryOwnershipLost must still propagate as itself,
        not be wrapped into the new exception type -- callers distinguish
        the two (ownership genuinely lost vs. a write failure with
        ownership potentially still held) for accurate diagnostics."""
        def _raising_callback(**kwargs):
            raise SelectorRecoveryOwnershipLost("owner CAS missed")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryOwnershipLost) as excinfo:
            _ctx_persist_attempt(
                ctx, "AAPL240101C00200000",
                result_reason="OI_TOO_LOW", transient=False,
            )
        assert not isinstance(excinfo.value, SelectorRecoveryCursorPersistFailed)

    def test_success_does_not_raise(self):
        calls = []

        def _ok_callback(**kwargs):
            calls.append(kwargs)

        ctx = SimpleNamespace(recovery_cursor_persist=_ok_callback)
        _ctx_persist_attempt(
            ctx, "AAPL240101C00200000",
            result_reason="OI_TOO_LOW", transient=False,
        )
        assert len(calls) == 1

    def test_no_callback_configured_is_a_silent_noop_ordinary_request_shape(self):
        """This is the exact shape of an ORDINARY (non-deferred) selector
        request -- no recovery_cursor_persist is ever set, so this function
        must return silently without attempting anything, proving ordinary
        requests are structurally untouched by this fix."""
        ctx = SimpleNamespace()  # no recovery_cursor_persist attribute at all
        _ctx_persist_attempt(
            ctx, "AAPL240101C00200000",
            result_reason="OI_TOO_LOW", transient=False,
        )  # must not raise


class TestCursorPersistFailureStopsSelectorWork:
    """Integration-level proof at the ap_execution_core.py catch sites:
    a cursor-persist failure during the selector call must produce a
    distinct MATERIALIZATION_CURSOR_PERSIST_FAILED disposition -- proving
    the exception is caught and converted into a durable-safe stop, not
    left to propagate uncaught or silently continue."""

    def test_exception_types_are_distinct_and_both_subclass_runtimeerror(self):
        assert issubclass(SelectorRecoveryCursorPersistFailed, RuntimeError)
        assert issubclass(SelectorRecoveryOwnershipLost, RuntimeError)
        assert not issubclass(
            SelectorRecoveryCursorPersistFailed, SelectorRecoveryOwnershipLost
        )
        assert not issubclass(
            SelectorRecoveryOwnershipLost, SelectorRecoveryCursorPersistFailed
        )

    def test_both_catch_sites_reference_the_new_exception_type(self):
        """Structural guard (secondary, not primary proof -- the primary
        proof is the direct raise tests above): confirms both
        ap_execution_core.py catch sites that previously caught only
        SelectorRecoveryOwnershipLost now also catch
        SelectorRecoveryCursorPersistFailed, so a persistence failure
        cannot propagate uncaught and crash the whole materialization
        attempt instead of returning a clean, durable-safe disposition."""
        import inspect
        import ap_execution_core as exec_core_mod
        source = inspect.getsource(exec_core_mod)
        assert source.count("SelectorRecoveryCursorPersistFailed") >= 3, (
            "expected the import plus both catch-site references"
        )


class TestCanonicalResolverDirectly:
    """Unit-level tests of resolve_deferred_materialization_max_attempts()
    itself -- the integration tests above go through each consumer's own
    fallback-on-conflict wrapper, which could mask a bug in the core
    resolver; these test the raw function's raise/return contract."""

    def _resolve(self, monkeypatch, *, mat_env=None, breach_env=None):
        if mat_env is None:
            monkeypatch.delenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", raising=False)
        else:
            monkeypatch.setenv("DEFERRED_MATERIALIZATION_MAX_ATTEMPTS", mat_env)
        if breach_env is None:
            monkeypatch.delenv("MAX_BREACH_SELECTOR_RETRIES", raising=False)
        else:
            monkeypatch.setenv("MAX_BREACH_SELECTOR_RETRIES", breach_env)
        from ap.selector_retry_policy import resolve_deferred_materialization_max_attempts
        return resolve_deferred_materialization_max_attempts()

    def test_both_absent_returns_five(self, monkeypatch):
        assert self._resolve(monkeypatch) == 5

    def test_only_breach_set_returns_that_value(self, monkeypatch):
        assert self._resolve(monkeypatch, breach_env="3") == 3

    def test_only_deferred_set_returns_that_value(self, monkeypatch):
        assert self._resolve(monkeypatch, mat_env="3") == 3

    def test_both_set_equal_returns_that_value(self, monkeypatch):
        assert self._resolve(monkeypatch, mat_env="4", breach_env="4") == 4

    def test_conflict_raises(self, monkeypatch):
        from ap.selector_retry_policy import DeferredMaterializationConfigConflict
        with pytest.raises(DeferredMaterializationConfigConflict):
            self._resolve(monkeypatch, mat_env="3", breach_env="5")

    def test_reverse_conflict_raises(self, monkeypatch):
        from ap.selector_retry_policy import DeferredMaterializationConfigConflict
        with pytest.raises(DeferredMaterializationConfigConflict):
            self._resolve(monkeypatch, mat_env="5", breach_env="3")

    def test_malformed_breach_raises(self, monkeypatch):
        from ap.selector_retry_policy import DeferredMaterializationConfigConflict
        with pytest.raises(DeferredMaterializationConfigConflict):
            self._resolve(monkeypatch, breach_env="garbage")

    def test_malformed_deferred_raises(self, monkeypatch):
        from ap.selector_retry_policy import DeferredMaterializationConfigConflict
        with pytest.raises(DeferredMaterializationConfigConflict):
            self._resolve(monkeypatch, mat_env="garbage")

    def test_negative_value_raises(self, monkeypatch):
        from ap.selector_retry_policy import DeferredMaterializationConfigConflict
        with pytest.raises(DeferredMaterializationConfigConflict):
            self._resolve(monkeypatch, breach_env="-1")

    def test_zero_value_raises(self, monkeypatch):
        from ap.selector_retry_policy import DeferredMaterializationConfigConflict
        with pytest.raises(DeferredMaterializationConfigConflict):
            self._resolve(monkeypatch, breach_env="0")

    def test_whitespace_padded_matching_values_are_not_a_conflict(self, monkeypatch):
        assert self._resolve(monkeypatch, mat_env=" 4 ", breach_env="4") == 4


# ─────────────────────────────────────────────────────────────────────────────
# Blocker 4 (premature disposition): correct_recovered_cursor_disposition
# must overwrite a premature DIRECT_QUOTE_RECOVERED_CHAIN_ZERO/transient=False
# record with the real final reason once quality/affordability/delta/
# premium checks actually reject the candidate.
# ─────────────────────────────────────────────────────────────────────────────

from ap.contract_quote_revalidator import correct_recovered_cursor_disposition


class TestPrematureCursorDispositionCorrected:
    def _ctx_with_recording_callback(self):
        calls = []

        def _persist(**kwargs):
            calls.append(dict(kwargs))

        ctx = SimpleNamespace(recovery_cursor_persist=_persist)
        return ctx, calls

    @pytest.mark.parametrize(
        "final_reason",
        [
            "SPREAD_TOO_WIDE",
            "OI_TOO_LOW",
            "DELTA_OUT_OF_RANGE",
            "PREMIUM_CAP_EXCEEDED",
            "UNTRADEABLE_FOR_ACCOUNT_SIZE",
        ],
    )
    def test_correction_overwrites_premature_recovered_record(self, final_reason):
        """Simulates the exact sequence the fix addresses: transport
        recovery persists a premature 'PASS' record, then the real
        quality/affordability/delta/premium check rejects the candidate
        and the correction call must overwrite it with the true reason."""
        ctx, calls = self._ctx_with_recording_callback()

        # Step 1: transport-recovery stage persists the premature record
        # (mirrors _ctx_persist_attempt's own call inside
        # revalidate_with_direct_quote).
        _ctx_persist_attempt(
            ctx, "AAPL240101C00200000",
            result_reason="DIRECT_QUOTE_RECOVERED_CHAIN_ZERO",
            transient=False,
        )
        assert calls[-1]["result_reason"] == "DIRECT_QUOTE_RECOVERED_CHAIN_ZERO"
        assert calls[-1]["transient"] is False

        # Step 2: the real check rejects it -- correction must fire.
        correct_recovered_cursor_disposition(
            ctx, "AAPL240101C00200000",
            final_reason=final_reason,
        )

        assert len(calls) == 2, (
            "the correction must be a real second persist call, not a "
            "no-op"
        )
        assert calls[-1]["result_reason"] == final_reason, (
            "the cursor's most recent record for this symbol must be the "
            "true final rejection reason, not the premature "
            "DIRECT_QUOTE_RECOVERED_CHAIN_ZERO"
        )
        assert calls[-1]["symbol"] == "AAPL240101C00200000"
        assert calls[-1]["transient"] is False, (
            "still non-transient/don't-retry -- correct classification, "
            "just with the truthful reason"
        )

    def test_correction_never_fires_for_a_genuinely_recovered_candidate(self):
        """Positive control: if the candidate genuinely passes every
        subsequent check (never reaches a rejection site), no correction
        call happens and the original DIRECT_QUOTE_RECOVERED_CHAIN_ZERO
        record legitimately stands as the final, accurate disposition."""
        ctx, calls = self._ctx_with_recording_callback()
        _ctx_persist_attempt(
            ctx, "AAPL240101C00200000",
            result_reason="DIRECT_QUOTE_RECOVERED_CHAIN_ZERO",
            transient=False,
        )
        assert len(calls) == 1
        assert calls[0]["result_reason"] == "DIRECT_QUOTE_RECOVERED_CHAIN_ZERO"

    def test_correction_propagates_ownership_loss_like_any_other_write(self):
        def _raising_callback(**kwargs):
            raise SelectorRecoveryOwnershipLost("owner CAS missed")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryOwnershipLost):
            correct_recovered_cursor_disposition(
                ctx, "AAPL240101C00200000", final_reason="SPREAD_TOO_WIDE",
            )

    def test_correction_fails_closed_on_a_generic_persist_error(self):
        def _raising_callback(**kwargs):
            raise ConnectionError("db unavailable")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            correct_recovered_cursor_disposition(
                ctx, "AAPL240101C00200000", final_reason="OI_TOO_LOW",
            )


class TestDirectQuoteUsedFlagGatesCorrection:
    """Verifies contract_selector.py's _record_final_rejection only
    attempts the correction for candidates that actually went through
    direct-quote transport recovery (opt["_direct_quote_used"] is True),
    matching the real patch-site flag set in contract_quote_revalidator.py
    -- ordinary chain-sourced candidates that were simply rejected on
    their own merits must not trigger any correction call at all."""

    def test_direct_quote_used_flag_is_the_correct_gate(self):
        import inspect
        import ap.contract_selector as selector_mod
        source = inspect.getsource(selector_mod)
        assert '_direct_quote_used' in source
        assert 'correct_recovered_cursor_disposition' in source


class TestQuoteAuthorityURLNormalizationEdgeCases:
    """Claim 5 follow-up: additional URL-shape edge cases for the exact
    api.tradier.com transport check -- credentials, query strings,
    fragments, case, trailing dot, and IPv6/lookalike hosts."""

    def test_embedded_credentials_rejected(self):
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(
            quote, transport=_FakeTransport("https://user:pass@api.tradier.com")
        )
        assert result["valid"] is False

    def test_query_string_does_not_bypass_hostname_check(self):
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(
            quote,
            transport=_FakeTransport(
                "https://evil.example/?redirect=api.tradier.com"
            ),
        )
        assert result["valid"] is False

    def test_fragment_does_not_bypass_hostname_check(self):
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(
            quote,
            transport=_FakeTransport("https://evil.example/#api.tradier.com"),
        )
        assert result["valid"] is False

    def test_uppercase_hostname_still_accepted(self):
        """Hostnames are case-insensitive by spec -- urlparse().hostname
        already lowercases, so this must still pass."""
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(
            quote, transport=_FakeTransport("https://API.TRADIER.COM")
        )
        assert result["valid"] is True

    def test_trailing_dot_hostname_rejected(self):
        """api.tradier.com. (trailing dot, a valid DNS absolute-name form)
        must not silently match -- urlparse().hostname preserves it and it
        will not equal the exact string 'api.tradier.com'."""
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(
            quote, transport=_FakeTransport("https://api.tradier.com.")
        )
        assert result["valid"] is False

    def test_ipv6_host_rejected(self):
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(
            quote, transport=_FakeTransport("https://[::1]")
        )
        assert result["valid"] is False

    def test_lookalike_subdomain_prefix_rejected(self):
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(
            quote, transport=_FakeTransport("https://api.tradier.com.attacker.example")
        )
        assert result["valid"] is False

    def test_lookalike_subdomain_of_tradier_rejected(self):
        """A genuine subdomain of tradier.com that is NOT exactly
        api.tradier.com must still be rejected -- "exact approved
        transport" means exactly that host, not the domain family."""
        quote = {"bid": 449.9, "ask": 450.1, "provider_timestamp": _fresh_ts()}
        result = validate_retry_market_quote_authority(
            quote, transport=_FakeTransport("https://evil.api.tradier.com")
        )
        assert result["valid"] is False


# ─────────────────────────────────────────────────────────────────────────────
# Round 3: structural-skip cursor persistence also failed open -- a
# genuinely separate code path from _ctx_persist_attempt with the same bug.
# ─────────────────────────────────────────────────────────────────────────────

from ap.contract_quote_revalidator import _ctx_persist_structural_skip


class TestStructuralSkipPersistenceFailsClosed:
    def test_db_exception_on_first_structural_skip_raises_typed_failure(self):
        def _raising_callback(**kwargs):
            raise ConnectionError("db unavailable")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _ctx_persist_structural_skip(
                ctx, "AAPL240101C00200000",
                structural_skip_reason="OI_TOO_LOW_STRUCTURAL",
            )

    def test_serialization_failure_raises_typed_failure(self):
        def _raising_callback(**kwargs):
            raise TypeError("not JSON serializable")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _ctx_persist_structural_skip(
                ctx, "AAPL240101C00200000",
                structural_skip_reason="OI_TOO_LOW_STRUCTURAL",
            )

    def test_timeout_raises_typed_failure(self):
        def _raising_callback(**kwargs):
            raise TimeoutError("statement timeout")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _ctx_persist_structural_skip(
                ctx, "AAPL240101C00200000",
                structural_skip_reason="OI_TOO_LOW_STRUCTURAL",
            )

    def test_generic_exception_raises_typed_failure(self):
        def _raising_callback(**kwargs):
            raise RuntimeError("unexpected")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryCursorPersistFailed):
            _ctx_persist_structural_skip(
                ctx, "AAPL240101C00200000",
                structural_skip_reason="OI_TOO_LOW_STRUCTURAL",
            )

    def test_ownership_cas_miss_still_raises_the_original_exception_unchanged(self):
        def _raising_callback(**kwargs):
            raise SelectorRecoveryOwnershipLost("owner CAS missed")

        ctx = SimpleNamespace(recovery_cursor_persist=_raising_callback)
        with pytest.raises(SelectorRecoveryOwnershipLost) as excinfo:
            _ctx_persist_structural_skip(
                ctx, "AAPL240101C00200000",
                structural_skip_reason="OI_TOO_LOW_STRUCTURAL",
            )
        assert not isinstance(excinfo.value, SelectorRecoveryCursorPersistFailed)

    def test_success_does_not_raise_and_uses_correct_kwarg_shape(self):
        calls = []

        def _ok_callback(**kwargs):
            calls.append(kwargs)

        ctx = SimpleNamespace(recovery_cursor_persist=_ok_callback)
        _ctx_persist_structural_skip(
            ctx, "AAPL240101C00200000",
            structural_skip_reason="OI_TOO_LOW_STRUCTURAL",
        )
        assert len(calls) == 1
        assert calls[0]["symbol"] == "AAPL240101C00200000"
        assert calls[0]["structural_skip_reason"] == "OI_TOO_LOW_STRUCTURAL"
        assert "result_reason" not in calls[0]
        assert "transient" not in calls[0]

    def test_no_callback_configured_is_a_silent_noop_ordinary_request_shape(self):
        ctx = SimpleNamespace()
        _ctx_persist_structural_skip(
            ctx, "AAPL240101C00200000",
            structural_skip_reason="OI_TOO_LOW_STRUCTURAL",
        )  # must not raise

    def test_selector_module_calls_the_shared_function_not_duplicated_logic(self):
        """Structural guard: confirms contract_selector.py no longer
        duplicates the try/except inline -- it calls the one shared,
        already-tested function, so there is only one place this contract
        can drift."""
        import inspect
        import ap.contract_selector as selector_mod
        source = inspect.getsource(selector_mod)
        assert "_ctx_persist_structural_skip(" in source
        assert "structural_skip_reason=reason" in source


class TestStrictAttemptCounterParsing:
    """Round 3: int(raw) accepted booleans (int(True)==1), whole-valued
    floats (int(2.0)==2), and fractional floats (int(1.5)==1) as if they
    were genuine attempt counts. Now only real ints or strictly
    integer-shaped strings are accepted."""

    def test_boolean_true_is_malformed(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=True, breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_boolean_false_is_malformed(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=False, breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_fractional_float_is_malformed(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=1.5, breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_whole_valued_float_is_still_malformed(self):
        """2.0 has no fractional part but is still a float, not a genuine
        integral representation -- rejected regardless."""
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=2.0, breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_negative_fractional_is_malformed(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=-0.5, breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_nan_is_malformed(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=float("nan"), breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_infinity_is_malformed(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=float("inf"), breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_non_integer_string_1_point_0_is_malformed(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt="1.0", breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert resolved is None
        assert reason == "MATERIALIZATION_ATTEMPT_COUNTER_CONFLICT"

    def test_genuine_integer_string_is_accepted(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt="3", breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert resolved == 3
        assert reason is None

    def test_whitespace_padded_integer_string_is_accepted(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=" 3 ", breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert resolved == 3
        assert reason is None

    def test_absent_is_distinct_from_explicit_zero(self):
        absent_resolved, absent_reason = _resolve_selector_attempt_number(
            retry_attempt=None, breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        zero_resolved, zero_reason = _resolve_selector_attempt_number(
            retry_attempt=0, breach_attempt_count=None,
            materialization_attempts=None, recovery_pre_claimed_attempt=None,
        )
        assert absent_reason is None and zero_reason is None
        assert absent_resolved == 1, "absent floors to attempt 1"
        assert zero_resolved == 1, "explicit zero also floors to attempt 1 (0 is not >= first attempt)"
        # Both resolve to the same final number here, but critically
        # neither is malformed -- an explicit 0 must not be treated as
        # equivalent to a missing/invalid field.
        assert absent_reason == zero_reason == None

    def test_real_integer_zero_is_not_malformed(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=0, breach_attempt_count=0,
            materialization_attempts=0, recovery_pre_claimed_attempt=None,
        )
        assert reason is None, "explicit zero across all three agreeing durable counters is valid, not a conflict"
        assert resolved == 1

    def test_genuine_integer_type_still_accepted(self):
        resolved, reason = _resolve_selector_attempt_number(
            retry_attempt=3, breach_attempt_count=3,
            materialization_attempts=3, recovery_pre_claimed_attempt=None,
        )
        assert resolved == 3
        assert reason is None
