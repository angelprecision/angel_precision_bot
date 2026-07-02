"""Integration tests for PR P0 multi-expiration + transient-retry.

Two layers:
1. Contract selector: ladder default-on when the deferred_breach_selection
   marker is on the plan; non-deferred plans get single-shot behavior
   (byte-for-byte unchanged).
2. Retry-loop policy: the retry helper's classification correctly gates
   which reasons retry vs. terminalize.

The full end-to-end (execution_core → selector → retry) is expensive to
mock realistically; those tests would double-cover what the two layers
already lock. So we keep the layers isolated and validate the boundaries.
"""
import os
os.environ.setdefault("DATABASE_URL", "postgresql://t:t@127.0.0.1:5432/t")
os.environ.setdefault("ENCRYPTION_KEY", "test-key")

from types import SimpleNamespace
from unittest.mock import patch

import pytest

from ap import deferred_breach_retry as dbr


# ─────────────────────────────────────────────────────────────────────────────
# Layer 1: ladder default-on for deferred, off for non-deferred
# ─────────────────────────────────────────────────────────────────────────────

def test_contract_selector_ladder_default_on_env_var():
    """PR P0 flipped the DEFERRED_DTE_LADDER default from '0' to '1'.

    Any operator can still turn it OFF with DEFERRED_DTE_LADDER=0 as a
    kill switch, but the default is ON so Render doesn't need a config
    change to get the fix.
    """
    from ap.contract_selector import APContractSelectionEngine as _CS
    with patch.dict(os.environ, {}, clear=False):
        # Ensure the env var is unset for this test
        os.environ.pop("DEFERRED_DTE_LADDER", None)
        # Only exercise __init__ up to the flag read — we don't need broker.
        sel = object.__new__(_CS)
        _CS.__init__(sel, broker=SimpleNamespace(), data_broker=SimpleNamespace())
        assert sel.dte_ladder_enabled is True


def test_contract_selector_ladder_kill_switch_still_works():
    from ap.contract_selector import APContractSelectionEngine as _CS
    with patch.dict(os.environ, {"DEFERRED_DTE_LADDER": "0"}, clear=False):
        sel = object.__new__(_CS)
        _CS.__init__(sel, broker=SimpleNamespace(), data_broker=SimpleNamespace())
        assert sel.dte_ladder_enabled is False


def test_contract_selector_ladder_eligibility_requires_deferred_marker():
    """The ladder is only eligible when the plan is explicitly tagged as a
    deferred breach selection. Non-deferred plans continue single-shot."""
    from ap.contract_selector import APContractSelectionEngine as _CS
    sel = object.__new__(_CS)
    _CS.__init__(sel, broker=SimpleNamespace(), data_broker=SimpleNamespace())

    # No metadata → not eligible
    assert sel._is_ladder_eligible(SimpleNamespace()) is False
    # Empty metadata → not eligible
    assert sel._is_ladder_eligible(SimpleNamespace(metadata={})) is False
    # Random marker → not eligible
    assert sel._is_ladder_eligible(SimpleNamespace(metadata={"foo": "bar"})) is False
    # Deferred boolean marker → eligible
    assert sel._is_ladder_eligible(SimpleNamespace(metadata={"deferred_breach_selection": True})) is True
    # Deferred string context marker → eligible
    assert sel._is_ladder_eligible(SimpleNamespace(metadata={"selection_context": "deferred_breach"})) is True
    # Dict-shaped plan works too
    assert sel._is_ladder_eligible({"metadata": {"deferred_breach_selection": True}}) is True


# ─────────────────────────────────────────────────────────────────────────────
# Layer 2: retry-loop policy — one call per attempt-shape scenario
# ─────────────────────────────────────────────────────────────────────────────

class _StubSelector:
    """Records select() calls; returns from a scripted sequence."""

    def __init__(self, script):
        # `script` is a list of (return_value, last_failure_dict) tuples.
        self._script = list(script)
        self.calls = 0
        self._last_failure = None

    def select(self, plan, *, expiration_override=None):
        self.calls += 1
        try:
            ret, fail = self._script.pop(0)
        except IndexError:
            ret, fail = None, {"reason_code": "OUT_OF_SCRIPT"}
        self._last_failure = fail
        return ret

    def get_last_failure(self):
        return self._last_failure

    def get_last_dte_ladder_audit(self):
        return None


def _drive_retry_loop(selector, plan, *, max_attempts, sleep_calls=None):
    """Local mini-loop mirroring the shape of the loop in ap_execution_core.

    This exists because faithfully driving the real execution_core path would
    require standing up the whole breach pipeline (OSM / queue / broker / PM).
    The retry-loop shape here is the same one committed in ap_execution_core,
    so this test locks its behavior end-to-end at the policy boundary.
    """
    sel = None
    reason = None
    attempts = []
    attempt = 0
    while attempt < max_attempts:
        attempt += 1
        if attempt > 1:
            secs = dbr.sleep_seconds_for_attempt(attempt)
            if sleep_calls is not None:
                sleep_calls.append(secs)
        sel = selector.select(plan)
        fail = selector.get_last_failure()
        reason = (fail or {}).get("reason_code")
        attempts.append({"attempt": attempt, "reason_code": reason, "sel": sel})
        if sel is not None:
            return sel, reason, attempts
        if not dbr.is_retryable(reason):
            return sel, reason, attempts
    return sel, reason, attempts


def test_retry_recovers_on_second_attempt_after_transient_zero_quote(monkeypatch):
    monkeypatch.setattr(dbr, "max_attempts", lambda env=None: 3)
    selector = _StubSelector(script=[
        (None, {"reason_code": "CHAIN_ROW_ZERO_BID_ASK"}),
        (SimpleNamespace(contract_symbol="SBUX260710P00102000"), None),
    ])
    sleeps = []
    sel, reason, attempts = _drive_retry_loop(selector, SimpleNamespace(), max_attempts=3, sleep_calls=sleeps)
    assert sel is not None
    assert selector.calls == 2
    assert len(attempts) == 2
    assert attempts[0]["reason_code"] == "CHAIN_ROW_ZERO_BID_ASK"
    assert attempts[1]["sel"] is not None
    assert sleeps == [5.0]  # one sleep between attempts 1 and 2


def test_retry_gives_up_on_terminal_reason_immediately():
    selector = _StubSelector(script=[
        (None, {"reason_code": "UNTRADEABLE_FOR_ACCOUNT_SIZE"}),
    ])
    sleeps = []
    sel, reason, attempts = _drive_retry_loop(selector, SimpleNamespace(), max_attempts=3, sleep_calls=sleeps)
    assert sel is None
    assert selector.calls == 1  # NO retry
    assert reason == "UNTRADEABLE_FOR_ACCOUNT_SIZE"
    assert sleeps == []


def test_retry_gives_up_on_unknown_reason_without_looping():
    """Unknown reasons are NOT retryable — we don't want to burn API calls on
    a code we can't reason about, and we don't want to mask the true blocker."""
    selector = _StubSelector(script=[
        (None, {"reason_code": "SOME_UNCLASSIFIED_FUTURE_CODE"}),
    ])
    sleeps = []
    sel, reason, attempts = _drive_retry_loop(selector, SimpleNamespace(), max_attempts=3, sleep_calls=sleeps)
    assert sel is None
    assert selector.calls == 1
    assert reason == "SOME_UNCLASSIFIED_FUTURE_CODE"
    assert sleeps == []


def test_retry_caps_at_max_attempts_when_all_transient(monkeypatch):
    monkeypatch.setattr(dbr, "max_attempts", lambda env=None: 3)
    selector = _StubSelector(script=[
        (None, {"reason_code": "CHAIN_ROW_ZERO_BID_ASK"}),
        (None, {"reason_code": "DIRECT_QUOTE_ZERO_BID_ASK"}),
        (None, {"reason_code": "NO_VALID_PLAYBOOK_DTE_CONTRACT"}),
    ])
    sleeps = []
    sel, reason, attempts = _drive_retry_loop(selector, SimpleNamespace(), max_attempts=3, sleep_calls=sleeps)
    assert sel is None
    assert selector.calls == 3   # exactly max, no more
    assert reason == "NO_VALID_PLAYBOOK_DTE_CONTRACT"
    assert sleeps == [5.0, 10.0]  # 2 sleeps between the 3 attempts


def test_retry_terminates_early_on_terminal_after_transient(monkeypatch):
    """If a subsequent attempt returns a terminal reason (e.g. all buckets now
    return UNTRADEABLE), stop retrying immediately — the verdict is now
    structural and further retries can't help."""
    monkeypatch.setattr(dbr, "max_attempts", lambda env=None: 5)
    selector = _StubSelector(script=[
        (None, {"reason_code": "CHAIN_ROW_ZERO_BID_ASK"}),
        (None, {"reason_code": "UNTRADEABLE_FOR_ACCOUNT_SIZE"}),  # terminal
        (SimpleNamespace(contract_symbol="unused"), None),         # never reached
    ])
    sleeps = []
    sel, reason, attempts = _drive_retry_loop(selector, SimpleNamespace(), max_attempts=5, sleep_calls=sleeps)
    assert sel is None
    assert selector.calls == 2  # stopped after terminal
    assert reason == "UNTRADEABLE_FOR_ACCOUNT_SIZE"


# ─────────────────────────────────────────────────────────────────────────────
# Layer 3: execution_core wiring lives in a review-only check
# ─────────────────────────────────────────────────────────────────────────────

def test_execution_core_imports_deferred_breach_retry():
    """The execution_core retry loop must actually reference the retry policy
    module. If a refactor drops the import, tests must fail loudly."""
    src = open("ap_execution_core.py").read()
    assert "from ap import deferred_breach_retry" in src
    assert "DEFERRED_BREACH_SELECTOR_RETRY" in src
    assert "deferred_breach_selector_attempts" in src


def test_execution_core_captures_ladder_audit_for_meta_propagation():
    src = open("ap_execution_core.py").read()
    assert "get_last_dte_ladder_audit" in src
    assert "deferred_breach_dte_ladder_audit" in src
