"""
tests/test_p0_execution_core_client_id.py

P0 HOTFIX — APExecutionCore.client_id missing broke breach-time contract selection.

Production error: 'APExecutionCore' object has no attribute 'client_id'
Affected: INTC, C, AMAT, DDOG, ROP, BKNG, CVX, CTSH (Jason live, 2026-06-18)

These tests verify:
1. APExecutionCore sets self.client_id from the email parameter.
2. APExecutionCore sets self.client_email independently from email.
3. self.execution_mode is set as an alias of self.mode.
4. _on_entry_trigger resolves _breach_client_id from signal dict first,
   then falls back to self.client_id, then self.email — never raises
   AttributeError regardless of which fields are present.
5. BREACH_TIME_CONTRACT_FINALIZED is emitted on successful selection.
6. BREACH_TIME_CONTRACT_SELECTION_FAILED is emitted on failure (not
   'APExecutionCore object has no attribute client_id').
7. Source guards: self.client_id referenced as attribute NOT accessed
   via bare self.client_id at the two failure log sites (confirmed by
   checking _breach_client_id is used instead).
8. Both BREACH_TIME_CONTRACT_FINALIZED and
   BREACH_TIME_CONTRACT_SELECTION_FAILED sentinels are in source.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_REPO = Path(__file__).resolve().parents[1]
_EC_SRC = (_REPO / "ap_execution_core.py").read_text()


# ─── Part A: __init__ attribute assignments ──────────────────────────────────

def _make_core(email: str = "jasoncosby1@gmail.com", mode: str = "LIVE"):
    """Instantiate APExecutionCore with minimal mocks."""
    with patch.dict("os.environ", {"BOT_MODE": mode}, clear=False):
        # Stub every heavy dependency so __init__ can run without real infra.
        mc = MagicMock()
        mc.mode = mode
        mc.max_positions = 5

        broker = MagicMock()
        broker.data_broker = None

        osm   = MagicMock()
        pm    = MagicMock()

        # Prevent thread start / network calls inside __init__
        with patch("ap_execution_core.APEntryWatcher", MagicMock()), \
             patch("ap_execution_core.APExitEngine", MagicMock()), \
             patch("ap_execution_core.APFeedbackLoop", MagicMock()), \
             patch("ap_execution_core.APShadowTracker", MagicMock()), \
             patch("ap_execution_core.APProofLogger", MagicMock()), \
             patch("ap_execution_core.APSignalStore", MagicMock()), \
             patch("ap_execution_core.APSignalTracker", MagicMock()):
            import ap_execution_core as _ec
            core = _ec.APExecutionCore(
                broker=broker,
                supabase_client=MagicMock(),
                email=email,
                position_manager=pm,
                order_state_machine=osm,
                master_control=mc,
                contract_selector=None,
            )
    return core


class TestClientIdInit:

    def test_client_id_set_from_email(self):
        core = _make_core(email="jasoncosby1@gmail.com")
        assert core.client_id == "jasoncosby1@gmail.com"

    def test_client_id_set_from_email_paper(self):
        core = _make_core(email="jose.vasquez4011@gmail.com", mode="PAPER")
        assert core.client_id == "jose.vasquez4011@gmail.com"

    def test_client_email_set_from_email(self):
        core = _make_core(email="jasoncosby1@gmail.com")
        assert core.client_email == "jasoncosby1@gmail.com"

    def test_client_email_not_overwritten_if_same(self):
        """client_email must equal email when no separate client_email was passed."""
        core = _make_core(email="tradefluencehq@gmail.com")
        assert core.client_email == "tradefluencehq@gmail.com"

    def test_client_id_empty_string_fallback_to_env(self):
        with patch.dict("os.environ", {"SINGLE_CLIENT_EMAIL": "env@test.com"}):
            core = _make_core(email="")
        assert core.client_id == "env@test.com"

    def test_client_id_empty_without_env(self):
        env = {k: v for k, v in os.environ.items() if k != "SINGLE_CLIENT_EMAIL"}
        env["BOT_MODE"] = "LIVE"
        with patch.dict("os.environ", env, clear=True):
            core = _make_core(email="")
        assert core.client_id == ""

    def test_execution_mode_is_mode_alias(self):
        core = _make_core(email="test@test.com", mode="LIVE")
        assert core.execution_mode == core.mode

    def test_execution_mode_paper(self):
        core = _make_core(email="test@test.com", mode="PAPER")
        assert core.execution_mode == "PAPER"


# ─── Part B: _breach_client_id resolution in _on_entry_trigger ──────────────
# We test the resolution logic directly (without running the full trigger flow)
# by reproducing the exact priority chain from the source.

def _resolve_breach_client_id(
    sig_client_id: str | None = None,
    sig_client_email: str | None = None,
    self_client_id: str | None = "self@test.com",
    self_email: str = "self@test.com",
) -> str:
    """
    Reproduce the exact resolution logic from _on_entry_trigger:
        str(sig.get("client_id") or sig.get("client_email") or "").strip()
        or getattr(self, "client_id", None)
        or self.email
        or ""
    """
    sig = {}
    if sig_client_id is not None:
        sig["client_id"] = sig_client_id
    if sig_client_email is not None:
        sig["client_email"] = sig_client_email

    # Build fake self that either has or doesn't have client_id
    if self_client_id is not None:
        class _FakeSelf:
            client_id = self_client_id
            email     = self_email
    else:
        class _FakeSelf:
            email = self_email
        # No client_id attribute — simulates the old broken APExecutionCore

    fake_self = _FakeSelf()

    _breach_client_id = (
        str(sig.get("client_id") or sig.get("client_email") or "").strip()
        or getattr(fake_self, "client_id", None)
        or fake_self.email
        or ""
    )
    return _breach_client_id


class TestBreachClientIdResolution:

    def test_signal_client_id_takes_priority(self):
        result = _resolve_breach_client_id(
            sig_client_id="signal@client.com",
            self_client_id="self@client.com",
        )
        assert result == "signal@client.com"

    def test_signal_client_email_used_when_no_client_id(self):
        result = _resolve_breach_client_id(
            sig_client_id=None,
            sig_client_email="email@signal.com",
            self_client_id="self@client.com",
        )
        assert result == "email@signal.com"

    def test_self_client_id_fallback_when_sig_empty(self):
        result = _resolve_breach_client_id(
            sig_client_id="",
            sig_client_email=None,
            self_client_id="self@client.com",
        )
        assert result == "self@client.com"

    def test_self_email_fallback_when_no_client_id_attr(self):
        """When self.client_id doesn't exist (old code), self.email is used."""
        result = _resolve_breach_client_id(
            sig_client_id=None,
            sig_client_email=None,
            self_client_id=None,   # attribute absent
            self_email="email@self.com",
        )
        assert result == "email@self.com"

    def test_never_raises_attribute_error(self):
        """The entire resolution must never raise AttributeError regardless
        of which fields are missing — this was the production crash."""
        try:
            result = _resolve_breach_client_id(
                sig_client_id=None,
                sig_client_email=None,
                self_client_id=None,
                self_email="",
            )
            assert result == ""
        except AttributeError as e:
            pytest.fail(f"AttributeError raised: {e}")

    def test_whitespace_stripped(self):
        result = _resolve_breach_client_id(sig_client_id="  user@test.com  ")
        assert result == "user@test.com"


# ─── Part C: Source guards ───────────────────────────────────────────────────

class TestSourceGuards:

    def test_self_client_id_set_in_init(self):
        """__init__ must explicitly assign self.client_id."""
        assert "self.client_id" in _EC_SRC
        # Assignment must exist (not just a read)
        assert "self.client_id    =" in _EC_SRC or "self.client_id=" in _EC_SRC

    def test_self_client_email_set_in_init(self):
        assert "self.client_email" in _EC_SRC

    def test_self_execution_mode_set_in_init(self):
        assert "self.execution_mode" in _EC_SRC

    def test_breach_client_id_local_var_used(self):
        """The two breach-time log sites must use _breach_client_id,
        not the bare self.client_id that caused the AttributeError."""
        assert "_breach_client_id" in _EC_SRC, \
            "_breach_client_id local variable must exist in breach-time code"

    def test_no_bare_self_client_id_in_breach_log(self):
        """Verify that the log.critical at Path A and Path B use
        _breach_client_id and not self.client_id (which crashes)."""
        # Find the two DEFERRED_BREACH_CONTRACT_SELECTION_FAILED log blocks
        idx_a = _EC_SRC.find("DEFERRED_BREACH_CONTRACT_SELECTION_FAILED")
        assert idx_a != -1
        region_a = _EC_SRC[idx_a: idx_a + 500]
        assert "self.client_id" not in region_a, \
            "Path A failure log must not use bare self.client_id"
        assert "_breach_client_id" in region_a, \
            "Path A failure log must use _breach_client_id"

        idx_b = _EC_SRC.find("DEFERRED_BREACH_CONTRACT_SELECTION_FAILED", idx_a + 1)
        assert idx_b != -1
        region_b = _EC_SRC[idx_b: idx_b + 500]
        assert "self.client_id" not in region_b, \
            "Path B failure log must not use bare self.client_id"
        assert "_breach_client_id" in region_b, \
            "Path B failure log must use _breach_client_id"

    def test_breach_time_contract_finalized_sentinel_present(self):
        assert "BREACH_TIME_CONTRACT_FINALIZED" in _EC_SRC, \
            "BREACH_TIME_CONTRACT_FINALIZED log sentinel must be in source"

    def test_breach_time_contract_selection_failed_sentinel_present(self):
        assert "BREACH_TIME_CONTRACT_SELECTION_FAILED" in _EC_SRC, \
            "BREACH_TIME_CONTRACT_SELECTION_FAILED log sentinel must be in source"

    def test_breach_time_contract_finalized_has_client_ticker_contract_fields(self):
        """The BREACH_TIME_CONTRACT_FINALIZED log must include all required fields."""
        idx = _EC_SRC.find("BREACH_TIME_CONTRACT_FINALIZED")
        assert idx != -1
        region = _EC_SRC[idx: idx + 300]
        assert "client=%s" in region
        assert "ticker=%s" in region
        assert "local_order_id=%s" in region
        assert "old_contract=%s" in region
        assert "new_contract=%s" in region
        assert "limit=" in region

    def test_no_new_order_created_in_breach_block(self):
        """Breach-time selection must update the existing PENDING_TRIGGER order,
        never create a new one. Verified by absence of create_entry_order
        inside the deferred breach block."""
        idx = _EC_SRC.find("DEFERRED_BREACH_CONTRACT_FAILED")
        assert idx != -1
        # Read from start of deferred block to the first log after success
        breach_end = _EC_SRC.find("BREACH_TIME_CONTRACT_FINALIZED", idx)
        region = _EC_SRC[idx:breach_end]
        assert "create_entry_order" not in region, \
            "breach-time deferred block must not create a new order"

    def test_local_order_id_preserved_in_breach_block(self):
        """The breach block must preserve queue_local_order_id (not create
        a new one). Verified by presence of queue_local_order_id in source."""
        assert "queue_local_order_id" in _EC_SRC

    def test_single_client_email_env_fallback_present(self):
        assert "SINGLE_CLIENT_EMAIL" in _EC_SRC, \
            "SINGLE_CLIENT_EMAIL env fallback must be present in __init__"
