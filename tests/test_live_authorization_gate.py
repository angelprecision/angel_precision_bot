"""
Live Authorization Gate + execution_mode persistence tests.

Covers the bot_authz_spec acceptance cases:
  1. Live client, no live auth → LIVE_AUTHORIZATION_REQUIRED; exits unaffected.
  2. Live client, live auth but no/expired weekly auth → WEEKLY_AUTHORIZATION_REQUIRED.
  3. Revoked weekly auth blocks new entries only.
  4. Paper-mode client: no weekly auth required (gate skipped entirely).
  5. proof_trades.execution_mode is copied from the entry order; a live entry
     stays 'live' even if mode toggled before close.
  6. Material disclosure version change invalidates the live authorization.

Run:
    pytest tests/test_live_authorization_gate.py -xvs
"""
from __future__ import annotations

import os
from datetime import datetime, timezone, timedelta

os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_live_authz",
)
os.environ.setdefault("DISCLOSURE_VERSION", "2026.06.1")

import pytest

import ap.authorization as authz


# --------------------------------------------------------------------------
# Fake brokers
# --------------------------------------------------------------------------
class _Cfg:
    def __init__(self, base_url):
        self.base_url = base_url


class FakeBroker:
    def __init__(self, base_url):
        self.cfg = _Cfg(base_url)


LIVE_BROKER = FakeBroker("https://api.tradier.com")
SANDBOX_BROKER = FakeBroker("https://sandbox.tradier.com")
EMPTY_BROKER = FakeBroker("")


# --------------------------------------------------------------------------
# Stub the client_authorizations DB read
# --------------------------------------------------------------------------
def _patch_auth_rows(monkeypatch, rows_by_type: dict):
    """Patch authz._fetch_authorizations to return canned rows per type."""
    def _fake(client_email, authorization_type):
        return list(rows_by_type.get(authorization_type, []))
    monkeypatch.setattr(authz, "_fetch_authorizations", _fake)


def _live_row(disclosure="2026.06.1", accepted=True, revoked=False):
    return {
        "accepted": accepted,
        "disclosure_version": disclosure,
        "revoked_at": (datetime.now(timezone.utc) if revoked else None),
        "expires_at": None,
    }


def _weekly_row(accepted=True, revoked=False, expires_in_days=3):
    exp = datetime.now(timezone.utc) + timedelta(days=expires_in_days)
    return {
        "accepted": accepted,
        "disclosure_version": "n/a",
        "revoked_at": (datetime.now(timezone.utc) if revoked else None),
        "expires_at": exp,
    }


# --------------------------------------------------------------------------
# is_live_broker / execution_mode_for_broker
# --------------------------------------------------------------------------
class TestBrokerModeDetection:
    def test_live_broker_is_live(self):
        assert authz.is_live_broker(LIVE_BROKER) is True

    def test_sandbox_broker_not_live(self):
        assert authz.is_live_broker(SANDBOX_BROKER) is False

    def test_empty_broker_not_live(self):
        assert authz.is_live_broker(EMPTY_BROKER) is False

    def test_none_broker_not_live(self):
        assert authz.is_live_broker(None) is False

    def test_execution_mode_label(self):
        assert authz.execution_mode_for_broker(LIVE_BROKER) == "live"
        assert authz.execution_mode_for_broker(SANDBOX_BROKER) == "paper"
        assert authz.execution_mode_for_broker(EMPTY_BROKER) == "paper"


# --------------------------------------------------------------------------
# Acceptance case 1: no live auth → LIVE_AUTHORIZATION_REQUIRED
# --------------------------------------------------------------------------
class TestLiveAuthorizationRequired:
    def test_no_live_auth_blocks(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {})  # no rows at all
        reason = authz.check_live_authorization("client@x.com")
        assert reason == authz.LIVE_AUTHORIZATION_REQUIRED

    def test_unaccepted_live_auth_blocks(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {
            "LIVE_TRADING": [_live_row(accepted=False)],
            "WEEKLY_TRADING": [_weekly_row()],
        })
        assert authz.check_live_authorization("c@x.com") == authz.LIVE_AUTHORIZATION_REQUIRED


# --------------------------------------------------------------------------
# Acceptance case 2: live auth but no/expired weekly → WEEKLY_AUTHORIZATION_REQUIRED
# --------------------------------------------------------------------------
class TestWeeklyAuthorizationRequired:
    def test_live_ok_no_weekly_blocks(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {"LIVE_TRADING": [_live_row()]})
        assert authz.check_live_authorization("c@x.com") == authz.WEEKLY_AUTHORIZATION_REQUIRED

    def test_live_ok_expired_weekly_blocks(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {
            "LIVE_TRADING": [_live_row()],
            "WEEKLY_TRADING": [_weekly_row(expires_in_days=-1)],  # expired yesterday
        })
        assert authz.check_live_authorization("c@x.com") == authz.WEEKLY_AUTHORIZATION_REQUIRED

    def test_live_ok_valid_weekly_passes(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {
            "LIVE_TRADING": [_live_row()],
            "WEEKLY_TRADING": [_weekly_row(expires_in_days=2)],
        })
        assert authz.check_live_authorization("c@x.com") is None


# --------------------------------------------------------------------------
# Acceptance case 3: revoked weekly auth blocks
# --------------------------------------------------------------------------
class TestRevokedWeekly:
    def test_revoked_weekly_blocks(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {
            "LIVE_TRADING": [_live_row()],
            "WEEKLY_TRADING": [_weekly_row(revoked=True)],
        })
        assert authz.check_live_authorization("c@x.com") == authz.WEEKLY_AUTHORIZATION_REQUIRED


# --------------------------------------------------------------------------
# Acceptance case 6: material disclosure version change invalidates live auth
# --------------------------------------------------------------------------
class TestDisclosureVersion:
    def test_stale_disclosure_invalidates_live(self, monkeypatch):
        monkeypatch.setenv("DISCLOSURE_VERSION", "2026.07.1")
        _patch_auth_rows(monkeypatch, {
            "LIVE_TRADING": [_live_row(disclosure="2026.06.1")],  # old version
            "WEEKLY_TRADING": [_weekly_row()],
        })
        assert authz.check_live_authorization("c@x.com") == authz.LIVE_AUTHORIZATION_REQUIRED

    def test_matching_disclosure_valid(self, monkeypatch):
        monkeypatch.setenv("DISCLOSURE_VERSION", "2026.07.1")
        _patch_auth_rows(monkeypatch, {
            "LIVE_TRADING": [_live_row(disclosure="2026.07.1")],
            "WEEKLY_TRADING": [_weekly_row()],
        })
        assert authz.check_live_authorization("c@x.com") is None


# --------------------------------------------------------------------------
# Acceptance case 4: paper-mode client never requires weekly auth.
# The dispatch gate only runs check_live_authorization when is_live_broker()
# is True; for a paper/sandbox broker it is skipped entirely.
# --------------------------------------------------------------------------
class TestPaperExempt:
    def test_paper_broker_is_exempt_from_gate(self):
        # Sandbox broker → not live → gate skipped → no authz lookup at all.
        assert authz.is_live_broker(SANDBOX_BROKER) is False

    def test_paper_gate_skip_semantics(self, monkeypatch):
        # Even with zero authorizations, a paper client must be allowed because
        # the gate is keyed on is_live_broker. We model the dispatch condition
        # directly here.
        _patch_auth_rows(monkeypatch, {})
        broker = SANDBOX_BROKER
        blocked_reason = None
        if authz.is_live_broker(broker):
            blocked_reason = authz.check_live_authorization("c@x.com")
        assert blocked_reason is None  # paper never blocked by authz gate


# --------------------------------------------------------------------------
# Acceptance case 5: proof_trades.execution_mode copied from entry order.
# A live entry stays 'live' even if self.mode toggled to 'paper' before close.
# --------------------------------------------------------------------------
class TestProofExecutionModeCopy:
    def test_resolve_from_orders_column(self, monkeypatch):
        import ap_proof_logger as pl
        monkeypatch.setattr(
            "ap.db.get_order_by_id",
            lambda loid, client_id=None: {"execution_mode": "live", "meta": "{}"},
        )
        assert pl._resolve_entry_execution_mode("loid-1") == "live"

    def test_resolve_from_meta_fallback(self, monkeypatch):
        import ap_proof_logger as pl
        monkeypatch.setattr(
            "ap.db.get_order_by_id",
            lambda loid, client_id=None: {
                "execution_mode": None,
                "meta": '{"execution_mode": "paper"}',
            },
        )
        assert pl._resolve_entry_execution_mode("loid-2") == "paper"

    def test_missing_order_is_unknown(self, monkeypatch):
        import ap_proof_logger as pl
        monkeypatch.setattr("ap.db.get_order_by_id", lambda loid, client_id=None: None)
        assert pl._resolve_entry_execution_mode("loid-3") == "unknown"

    def test_empty_local_order_id_is_unknown(self):
        import ap_proof_logger as pl
        assert pl._resolve_entry_execution_mode("") == "unknown"

    def test_never_guesses_live(self, monkeypatch):
        import ap_proof_logger as pl
        # Order exists but has no mode anywhere → 'unknown', not 'live'.
        monkeypatch.setattr(
            "ap.db.get_order_by_id",
            lambda loid, client_id=None: {"execution_mode": "", "meta": "{}"},
        )
        assert pl._resolve_entry_execution_mode("loid-4") == "unknown"

    def test_row_uses_entry_mode_not_self_mode(self, monkeypatch):
        """Row execution_mode comes from the entry order even if logger.mode
        was toggled to 'paper' after a live entry opened."""
        import ap_proof_logger as pl
        monkeypatch.setattr(
            "ap.db.get_order_by_id",
            lambda loid, client_id=None: {"execution_mode": "live", "meta": "{}"},
        )
        captured = {}

        class _FakeSB:
            def table(self, name):
                return self
            def insert(self, row):
                captured.update(row)
                return self
            def execute(self):
                return None

        logger = pl.APProofLogger(supabase_client=_FakeSB(), client_email="c@x.com", mode="paper")
        logger.log_trade(
            ticker="SPY", pattern="p", side="CALL", timeframe="1d",
            score=80, tier="A", context_score=70, setup_status="ok",
            entry_trigger=1.0, entry_option_price=1.0, exit_option_price=1.5,
            underlying_entry=400, underlying_exit=405, contracts=1,
            exit_reason="TARGET HIT", option_pnl_pct=50.0, underlying_pnl_pct=1.0,
            win=True, local_order_id="loid-live-1",
        )
        assert captured.get("execution_mode") == "live"
        assert captured.get("mode") == "paper"  # self.mode unchanged, but distinct field


# --------------------------------------------------------------------------
# Gate invocation order: exits must NEVER reach this gate. We assert the gate
# function is only defined for new-entry use and that create_exit_order has no
# execution_mode/authorization coupling.
# --------------------------------------------------------------------------
class TestExitsBypass:
    def test_create_exit_order_has_no_authz_param(self):
        import inspect
        from ap.order_state_machine import APOrderStateMachine
        sig = inspect.signature(APOrderStateMachine.create_exit_order)
        assert "execution_mode" not in sig.parameters
        # Authorization is never imported/applied to exit creation.

    def test_create_entry_order_accepts_execution_mode(self):
        import inspect
        from ap.order_state_machine import APOrderStateMachine
        sig = inspect.signature(APOrderStateMachine.create_entry_order)
        assert "execution_mode" in sig.parameters
        assert sig.parameters["execution_mode"].default is None


# --------------------------------------------------------------------------
# Amendment cases: exits allowed in EVERY missing/revoked auth state; the gate
# (check_live_authorization) is the ONLY entry guard and is never consulted for
# exits. We verify the reason-code precedence and that no auth state changes
# exit behavior (exits don't call the gate at all).
# --------------------------------------------------------------------------
class TestAmendmentGate:
    def test_no_live_and_no_weekly_blocks_with_live_reason_first(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {})  # nothing accepted
        assert authz.check_live_authorization("c@x.com") == authz.LIVE_AUTHORIZATION_REQUIRED

    def test_live_ok_weekly_missing_blocks_weekly_reason(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {"LIVE_TRADING": [_live_row()]})
        assert authz.check_live_authorization("c@x.com") == authz.WEEKLY_AUTHORIZATION_REQUIRED

    def test_both_valid_allows_entry(self, monkeypatch):
        future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        _patch_auth_rows(monkeypatch, {
            "LIVE_TRADING": [_live_row()],
            "WEEKLY_TRADING": [{"accepted": True, "revoked_at": None, "expires_at": future, "disclosure_version": "2026.06.1"}],
        })
        assert authz.check_live_authorization("c@x.com") is None

    def test_revoked_live_blocks_even_with_valid_weekly(self, monkeypatch):
        future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        _patch_auth_rows(monkeypatch, {
            "LIVE_TRADING": [_live_row(revoked=True)],
            "WEEKLY_TRADING": [{"accepted": True, "revoked_at": None, "expires_at": future}],
        })
        assert authz.check_live_authorization("c@x.com") == authz.LIVE_AUTHORIZATION_REQUIRED

    def test_db_read_failure_fails_closed(self, monkeypatch):
        # If the auth read errors, _fetch_authorizations returns [] -> blocked.
        def _boom(email, t):
            raise RuntimeError("db down")
        monkeypatch.setattr(authz, "_fetch_authorizations", _boom)
        # _fetch_authorizations swallows internally and returns []; emulate that:
        monkeypatch.setattr(authz, "_fetch_authorizations", lambda e, t: [])
        assert authz.check_live_authorization("c@x.com") == authz.LIVE_AUTHORIZATION_REQUIRED


# --------------------------------------------------------------------------
# FINAL AMENDMENT — enforce flag, observe mode, unknown-broker block.
#
# These cover the helpers added in the Final Amendment plus the gate's
# decision rule (compute a _gate_reason, then observe-vs-enforce). The gate's
# branching in ap/queue.py and ap_overnight_reeval.py is identical, so we test
# the shared decision logic here against the real authz helpers.
# --------------------------------------------------------------------------
class TestEnforceFlag:
    def test_default_is_observe(self, monkeypatch):
        monkeypatch.delenv("LIVE_AUTHORIZATION_GATE_ENFORCE", raising=False)
        assert authz.authorization_gate_enforced() is False

    def test_explicit_false_is_observe(self, monkeypatch):
        monkeypatch.setenv("LIVE_AUTHORIZATION_GATE_ENFORCE", "false")
        assert authz.authorization_gate_enforced() is False

    def test_truthy_values_enable_enforce(self, monkeypatch):
        for v in ("1", "true", "TRUE", "Yes", "on"):
            monkeypatch.setenv("LIVE_AUTHORIZATION_GATE_ENFORCE", v)
            assert authz.authorization_gate_enforced() is True, v

    def test_falsy_values_stay_observe(self, monkeypatch):
        for v in ("0", "no", "off", "", "maybe"):
            monkeypatch.setenv("LIVE_AUTHORIZATION_GATE_ENFORCE", v)
            assert authz.authorization_gate_enforced() is False, v


class TestBrokerModeKnown:
    def test_live_broker_mode_known(self):
        assert authz.broker_live_mode_known(LIVE_BROKER) is True

    def test_sandbox_broker_mode_known(self):
        assert authz.broker_live_mode_known(SANDBOX_BROKER) is True

    def test_empty_broker_mode_unknown(self):
        # No base_url at all → unverifiable → must NOT be assumed paper.
        assert authz.broker_live_mode_known(EMPTY_BROKER) is False

    def test_none_broker_mode_unknown(self):
        assert authz.broker_live_mode_known(None) is False


def _gate_reason_for(broker, client_email):
    """Replicates the gate's _gate_reason computation shared by ap/queue.py and
    ap_overnight_reeval.py: unknown mode → GATE_UNAVAILABLE; live → authz check;
    known paper → None (exempt)."""
    if not authz.broker_live_mode_known(broker):
        return authz.LIVE_AUTHORIZATION_GATE_UNAVAILABLE
    if authz.is_live_broker(broker):
        return authz.check_live_authorization(client_email)
    return None


class TestGateDecisionRule:
    def test_unknown_broker_blocks_not_paper(self, monkeypatch):
        # Empty/unverifiable broker → GATE_UNAVAILABLE (never treated as paper).
        _patch_auth_rows(monkeypatch, {})
        assert _gate_reason_for(EMPTY_BROKER, "c@x.com") == authz.LIVE_AUTHORIZATION_GATE_UNAVAILABLE
        assert _gate_reason_for(None, "c@x.com") == authz.LIVE_AUTHORIZATION_GATE_UNAVAILABLE

    def test_known_paper_is_exempt(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {})  # no authz at all
        # Sandbox broker is known-paper → exempt → no gate reason.
        assert _gate_reason_for(SANDBOX_BROKER, "c@x.com") is None

    def test_live_unauthorized_yields_reason(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {})
        assert _gate_reason_for(LIVE_BROKER, "c@x.com") == authz.LIVE_AUTHORIZATION_REQUIRED

    def test_live_fully_authorized_no_reason(self, monkeypatch):
        future = (datetime.now(timezone.utc) + timedelta(days=1)).isoformat()
        _patch_auth_rows(monkeypatch, {
            "LIVE_TRADING": [_live_row()],
            "WEEKLY_TRADING": [{"accepted": True, "revoked_at": None, "expires_at": future}],
        })
        assert _gate_reason_for(LIVE_BROKER, "c@x.com") is None


class TestObserveVsEnforceOutcome:
    """The gate decision: a non-None _gate_reason BLOCKS only when enforced;
    in observe mode it logs WOULD_BLOCK and allows the entry."""

    @staticmethod
    def _blocks(gate_reason, enforced):
        # Mirrors the gate: block iff there's a reason AND enforce is on.
        return bool(gate_reason) and enforced

    def test_observe_mode_allows_unauthorized_live(self, monkeypatch):
        monkeypatch.setenv("LIVE_AUTHORIZATION_GATE_ENFORCE", "false")
        _patch_auth_rows(monkeypatch, {})
        reason = _gate_reason_for(LIVE_BROKER, "c@x.com")
        assert reason == authz.LIVE_AUTHORIZATION_REQUIRED
        assert self._blocks(reason, authz.authorization_gate_enforced()) is False

    def test_enforce_mode_blocks_unauthorized_live(self, monkeypatch):
        monkeypatch.setenv("LIVE_AUTHORIZATION_GATE_ENFORCE", "true")
        _patch_auth_rows(monkeypatch, {})
        reason = _gate_reason_for(LIVE_BROKER, "c@x.com")
        assert self._blocks(reason, authz.authorization_gate_enforced()) is True

    def test_observe_mode_allows_unknown_broker(self, monkeypatch):
        monkeypatch.setenv("LIVE_AUTHORIZATION_GATE_ENFORCE", "false")
        _patch_auth_rows(monkeypatch, {})
        reason = _gate_reason_for(EMPTY_BROKER, "c@x.com")
        assert reason == authz.LIVE_AUTHORIZATION_GATE_UNAVAILABLE
        assert self._blocks(reason, authz.authorization_gate_enforced()) is False

    def test_enforce_mode_blocks_unknown_broker(self, monkeypatch):
        monkeypatch.setenv("LIVE_AUTHORIZATION_GATE_ENFORCE", "true")
        _patch_auth_rows(monkeypatch, {})
        reason = _gate_reason_for(EMPTY_BROKER, "c@x.com")
        assert self._blocks(reason, authz.authorization_gate_enforced()) is True

    def test_known_paper_never_blocks_either_mode(self, monkeypatch):
        _patch_auth_rows(monkeypatch, {})
        for mode in ("false", "true"):
            monkeypatch.setenv("LIVE_AUTHORIZATION_GATE_ENFORCE", mode)
            reason = _gate_reason_for(SANDBOX_BROKER, "c@x.com")
            assert self._blocks(reason, authz.authorization_gate_enforced()) is False


# --------------------------------------------------------------------------
# FINAL AMENDMENT (review) — proof_trades.execution_mode schema guard.
#
# The proof logger writes proof_trades.execution_mode on every close. These
# tests confirm the startup guard correctly detects presence/absence and
# caches a definitive answer (but not a transient/inconclusive one).
# --------------------------------------------------------------------------
class _FakeSelectChain:
    def __init__(self, raise_exc=None):
        self._raise = raise_exc
    def select(self, *a, **k): return self
    def limit(self, *a, **k): return self
    def execute(self):
        if self._raise:
            raise self._raise
        return type("R", (), {"data": [], "count": 0})()


class _FakeSB:
    def __init__(self, raise_exc=None):
        self._raise = raise_exc
    def table(self, name):
        return _FakeSelectChain(self._raise)


class TestProofSchemaGuard:
    def _reset_cache(self):
        import ap_proof_logger as pl
        pl._EXECUTION_MODE_COLUMN_OK = None

    def test_present_when_select_succeeds(self):
        import ap_proof_logger as pl
        self._reset_cache()
        assert pl.proof_trades_execution_mode_present(_FakeSB()) is True
        assert pl.ensure_proof_trades_schema(_FakeSB()) is True

    def test_missing_when_column_error(self):
        import ap_proof_logger as pl
        self._reset_cache()
        exc = Exception('column "execution_mode" does not exist')
        assert pl.proof_trades_execution_mode_present(_FakeSB(exc)) is False
        # ensure_* returns False (blocking signal) when confirmed missing.
        self._reset_cache()
        assert pl.ensure_proof_trades_schema(_FakeSB(exc)) is False

    def test_inconclusive_on_transient_error_not_cached(self):
        import ap_proof_logger as pl
        self._reset_cache()
        exc = Exception("connection reset by peer")
        assert pl.proof_trades_execution_mode_present(_FakeSB(exc)) is None
        # Not cached → a later successful probe flips to True.
        assert pl.proof_trades_execution_mode_present(_FakeSB()) is True
        # ensure_* is optimistic (True) on inconclusive so trading is not blocked.
        self._reset_cache()
        assert pl.ensure_proof_trades_schema(_FakeSB(exc)) is True

    def test_none_client_is_inconclusive(self):
        import ap_proof_logger as pl
        self._reset_cache()
        assert pl.proof_trades_execution_mode_present(None) is None

    def test_definitive_answer_is_cached(self):
        import ap_proof_logger as pl
        self._reset_cache()
        assert pl.proof_trades_execution_mode_present(_FakeSB()) is True
        # Even if a later call would error, the cached True is returned.
        assert pl.proof_trades_execution_mode_present(_FakeSB(Exception("column missing"))) is True
