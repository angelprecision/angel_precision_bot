"""
tests/test_paper_selector_market_data_transport.py

PR #150 — P0 Paper Selector Market-Data Transport Split.

Verifies the resolver `client_runner.resolve_market_data_transport()`:

  PAPER mode:
    1. With TRADIER_MARKET_DATA_TOKEN set → dedicated live data broker on
       https://api.tradier.com, token_source=TRADIER_MARKET_DATA_TOKEN.
    2. With only TRADIER_DATA_TOKEN set → dedicated live data broker on
       https://api.tradier.com, token_source=TRADIER_DATA_TOKEN.
    3. With NEITHER set → raises PaperSelectorNoMarketDataTokenError.
       The selector is NEVER pointed at the sandbox execution broker.

  LIVE mode:
    4. With no MD token → falls back to live execution broker (unchanged).
    5. With MD token → dedicated MD broker, execution broker untouched.

  Cross-cutting:
    6. TRADIER_MARKET_DATA_TOKEN beats TRADIER_DATA_TOKEN when both set.
    7. Base URL priority: MARKET_DATA_BASE_URL > DATA_BASE_URL > default.
    8. Sandbox-URL guard rewrites any sandbox base URL to live + warns.
    9. Execution broker base_url is preserved on the result for audit logs.

  Regression guard:
   10. Paper execution credentials still resolve to sandbox.tradier.com
       (resolve_tradier_credentials is untouched by this PR).

All tests are pure unit tests against the resolver — no real DB, no real
network, no real broker classes. We inject fake broker_cls / broker_config_cls
seams so the resolver is exercised against a deterministic in-memory stub.

Heavy client_runner imports (supabase, psycopg2, ap.db, ap.queue) are mocked
at the module boundary BEFORE `import client_runner`. We do this once per
test session to keep import time low.
"""
from __future__ import annotations

import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from ap.live_submit_gates import MarketDataTransportConfigurationError


# ─── Mock heavy imports BEFORE importing client_runner ──────────────────────
def _stub(name: str):
    sys.modules.setdefault(name, MagicMock())

_stub("psycopg2")
_stub("psycopg2.extras")
_stub("psycopg2.pool")
_stub("supabase")
_stub("cryptography")
_stub("cryptography.fernet")
_stub("ap.db")
_stub("ap.queue")
_stub("ap.order_monitor")
_stub("ap.position_sizer")
_stub("ap.market_intelligence")
_stub("ap.worker_health")
_stub("ap_reconciler")
_stub("ap_recovery")
_stub("ap.self_healing")

# Ensure stubbed modules expose the attributes client_runner reads at import time.
sys.modules["supabase"].create_client = MagicMock()
sys.modules["supabase"].Client = MagicMock()
sys.modules["cryptography.fernet"].Fernet = MagicMock()
sys.modules["ap.db"].run_with_retry = MagicMock()
sys.modules["ap.queue"].enqueue_signal = MagicMock()
sys.modules["ap.queue"].worker_loop = MagicMock()
sys.modules["ap.order_monitor"].APOrderMonitor = MagicMock()
sys.modules["ap.position_sizer"].APPositionSizer = MagicMock()
sys.modules["ap.market_intelligence"].APEarningsGuard = MagicMock()
sys.modules["ap.market_intelligence"].APIVRankFilter = MagicMock()
sys.modules["ap.worker_health"].get_monitor = MagicMock()
sys.modules["ap.worker_health"].init_monitor = MagicMock()
sys.modules["ap_reconciler"].APBrokerReconciler = MagicMock()
sys.modules["ap_recovery"].APStartupRecovery = MagicMock()
sys.modules["ap.self_healing"].get_healer = MagicMock()
sys.modules["ap.self_healing"].init_self_healing = MagicMock()

import client_runner as _cr  # noqa: E402

resolve_market_data_transport = _cr.resolve_market_data_transport
PaperSelectorNoMarketDataTokenError = _cr.PaperSelectorNoMarketDataTokenError


# ─── Test doubles ───────────────────────────────────────────────────────────
class _FakeCfg:
    def __init__(self, base_url: str, access_token: str, account_id: str):
        self.base_url     = base_url
        self.access_token = access_token
        self.account_id   = account_id


class _FakeBroker:
    def __init__(self, cfg: _FakeCfg):
        self.cfg = cfg


def _sandbox_execution_broker(account_id: str = "VA00000001") -> _FakeBroker:
    """A stand-in for a paper-runner execution broker (sandbox URL)."""
    return _FakeBroker(_FakeCfg(
        base_url="https://sandbox.tradier.com",
        access_token="sandbox-execution-token",
        account_id=account_id,
    ))


def _live_execution_broker(account_id: str = "LIVE0000001") -> _FakeBroker:
    """A stand-in for a live-runner execution broker (live URL)."""
    return _FakeBroker(_FakeCfg(
        base_url="https://api.tradier.com",
        access_token="live-execution-token",
        account_id=account_id,
    ))


# ─── PAPER mode ─────────────────────────────────────────────────────────────
class TestPaperWithMarketDataToken:
    """Requirement 1 of the PR spec."""

    def test_paper_with_market_data_token_uses_live_url(self):
        env = {"TRADIER_MARKET_DATA_TOKEN": "mdt-secret-1"}
        exec_broker = _sandbox_execution_broker()
        out = resolve_market_data_transport(
            mode="PAPER", account_id="VA1", execution_broker=exec_broker,
            env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
        )
        # Selector data broker points at LIVE Tradier
        assert out["data_broker"] is not exec_broker
        assert out["data_broker"].cfg.base_url == "https://api.tradier.com"
        assert out["data_broker"].cfg.access_token == "mdt-secret-1"
        assert out["base_url"]     == "https://api.tradier.com"
        assert out["token_source"] == "TRADIER_MARKET_DATA_TOKEN"
        assert out["is_dedicated"] is True
        # Execution broker remains sandbox — REGRESSION GUARD
        assert exec_broker.cfg.base_url == "https://sandbox.tradier.com"
        assert out["execution_base_url"] == "https://sandbox.tradier.com"


class TestPaperWithOnlyDataToken:
    """Requirement 2 of the PR spec — TRADIER_DATA_TOKEN fallback works."""

    def test_paper_with_only_data_token_uses_live_url(self):
        env = {"TRADIER_DATA_TOKEN": "dt-secret-1"}
        exec_broker = _sandbox_execution_broker()
        out = resolve_market_data_transport(
            mode="PAPER", account_id="VA2", execution_broker=exec_broker,
            env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
        )
        assert out["data_broker"].cfg.base_url     == "https://api.tradier.com"
        assert out["data_broker"].cfg.access_token == "dt-secret-1"
        assert out["token_source"] == "TRADIER_DATA_TOKEN"
        assert out["is_dedicated"] is True
        assert exec_broker.cfg.base_url == "https://sandbox.tradier.com"


class TestPaperWithoutAnyMarketDataToken:
    """Requirement 3 of the PR spec — fail closed, NEVER use sandbox."""

    def test_paper_without_md_token_raises(self):
        env = {}  # no MD token, no DATA token
        exec_broker = _sandbox_execution_broker()
        with pytest.raises(PaperSelectorNoMarketDataTokenError) as exc_info:
            resolve_market_data_transport(
                mode="PAPER", account_id="VA3", execution_broker=exec_broker,
                env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
            )
        msg = str(exc_info.value)
        assert "PAPER_SELECTOR_NO_MARKET_DATA_TOKEN" in msg
        assert "TRADIER_MARKET_DATA_TOKEN" in msg

    def test_paper_with_empty_string_tokens_raises(self):
        """Whitespace-only / empty env values must be treated as unset."""
        env = {"TRADIER_MARKET_DATA_TOKEN": "  ", "TRADIER_DATA_TOKEN": ""}
        exec_broker = _sandbox_execution_broker()
        with pytest.raises(PaperSelectorNoMarketDataTokenError):
            resolve_market_data_transport(
                mode="PAPER", account_id="VA3b", execution_broker=exec_broker,
                env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
            )

    def test_paper_failure_never_returns_sandbox_data_broker(self):
        """Even if the caller swallows the exception, the resolver must not
        have configured `data_broker = execution_broker` for paper."""
        env = {}
        exec_broker = _sandbox_execution_broker()
        try:
            resolve_market_data_transport(
                mode="PAPER", account_id="VA3c", execution_broker=exec_broker,
                env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
            )
            assert False, "expected fail-closed exception"
        except PaperSelectorNoMarketDataTokenError:
            # exec_broker still has its sandbox identity untouched
            assert exec_broker.cfg.base_url == "https://sandbox.tradier.com"


# ─── LIVE mode ──────────────────────────────────────────────────────────────
class TestLiveWithoutMarketDataToken:
    """Requirement 4 of the PR spec — LIVE behavior preserved."""

    def test_live_no_md_token_falls_back_to_execution_broker(self):
        env = {}
        exec_broker = _live_execution_broker()
        out = resolve_market_data_transport(
            mode="LIVE", account_id="LIVE1", execution_broker=exec_broker,
            env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
        )
        # data_broker IS the execution broker (the safe live fallback)
        assert out["data_broker"] is exec_broker
        assert out["is_dedicated"] is False
        assert out["token_source"] == "execution_broker_fallback_live"
        assert out["base_url"]     == "https://api.tradier.com"
        # Execution broker untouched
        assert exec_broker.cfg.base_url     == "https://api.tradier.com"
        assert exec_broker.cfg.access_token == "live-execution-token"


class TestLiveWithMarketDataToken:
    """Requirement 5 of the PR spec — LIVE with MD token works."""

    def test_live_with_md_token_uses_dedicated_data_broker(self):
        env = {"TRADIER_MARKET_DATA_TOKEN": "live-md-token-1"}
        exec_broker = _live_execution_broker()
        out = resolve_market_data_transport(
            mode="LIVE", account_id="LIVE2", execution_broker=exec_broker,
            env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
        )
        assert out["data_broker"] is not exec_broker  # dedicated
        assert out["data_broker"].cfg.access_token == "live-md-token-1"
        assert out["data_broker"].cfg.base_url     == "https://api.tradier.com"
        assert out["is_dedicated"] is True
        assert out["token_source"] == "TRADIER_MARKET_DATA_TOKEN"
        # Execution broker still live, untouched
        assert exec_broker.cfg.access_token == "live-execution-token"


# ─── Cross-cutting behavior ─────────────────────────────────────────────────
class TestTokenAndBaseUrlPriority:
    def test_market_data_token_beats_data_token(self):
        env = {
            "TRADIER_MARKET_DATA_TOKEN": "winner",
            "TRADIER_DATA_TOKEN":        "loser",
        }
        out = resolve_market_data_transport(
            mode="PAPER", account_id="X", execution_broker=_sandbox_execution_broker(),
            env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
        )
        assert out["data_broker"].cfg.access_token == "winner"
        assert out["token_source"] == "TRADIER_MARKET_DATA_TOKEN"

    def test_market_data_base_url_beats_data_base_url(self):
        # PR #391 amendment: env var priority is proven by showing that
        # TRADIER_MARKET_DATA_BASE_URL is the one validated when both are
        # set. Setting MARKET_DATA to an invalid URL and DATA to the
        # canonical live URL must raise — proving MARKET_DATA is what the
        # resolver actually consulted (if it had used DATA, this would
        # succeed).
        env = {
            "TRADIER_MARKET_DATA_TOKEN":    "t",
            "TRADIER_MARKET_DATA_BASE_URL": "https://md.example.com",
            "TRADIER_DATA_BASE_URL":        "https://api.tradier.com",
        }
        with pytest.raises(MarketDataTransportConfigurationError):
            resolve_market_data_transport(
                mode="PAPER", account_id="X", execution_broker=_sandbox_execution_broker(),
                env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
            )

    def test_data_base_url_used_when_no_md_base_url(self):
        # PR #391 amendment: TRADIER_DATA_BASE_URL is consulted only when
        # TRADIER_MARKET_DATA_BASE_URL is absent, and it is subject to the
        # same strict allowlist. Canonical value → dedicated broker on live.
        env = {
            "TRADIER_MARKET_DATA_TOKEN": "t",
            "TRADIER_DATA_BASE_URL":     "https://api.tradier.com",
        }
        out = resolve_market_data_transport(
            mode="PAPER", account_id="X", execution_broker=_sandbox_execution_broker(),
            env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
        )
        assert out["base_url"] == "https://api.tradier.com"

    def test_default_base_url_is_live_tradier(self):
        env = {"TRADIER_MARKET_DATA_TOKEN": "t"}
        out = resolve_market_data_transport(
            mode="PAPER", account_id="X", execution_broker=_sandbox_execution_broker(),
            env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
        )
        assert out["base_url"] == "https://api.tradier.com"


class TestSandboxGuard:
    """PR #391 amendment: the previous behavior was to silently rewrite any
    sandbox URL to https://api.tradier.com. That masked real configuration
    bugs. The new contract is fail-closed: any non-canonical URL — sandbox
    included — must raise MarketDataTransportConfigurationError BEFORE the
    token-bearing TradierConfig is constructed.
    """

    def test_sandbox_in_md_base_url_raises_not_rewrites(self):
        env = {
            "TRADIER_MARKET_DATA_TOKEN":    "t",
            "TRADIER_MARKET_DATA_BASE_URL": "https://sandbox.tradier.com",
        }
        with pytest.raises(MarketDataTransportConfigurationError):
            resolve_market_data_transport(
                mode="PAPER", account_id="X", execution_broker=_sandbox_execution_broker(),
                env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
            )

    def test_sandbox_in_data_base_url_raises_not_rewrites(self):
        env = {
            "TRADIER_DATA_TOKEN":    "t",
            "TRADIER_DATA_BASE_URL": "https://sandbox.tradier.com",
        }
        with pytest.raises(MarketDataTransportConfigurationError):
            resolve_market_data_transport(
                mode="PAPER", account_id="X", execution_broker=_sandbox_execution_broker(),
                env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
            )


class TestExecutionBaseUrlPreserved:
    """The result includes execution_base_url for the PAPER_EXECUTION_TRANSPORT_SELECTED log."""

    def test_paper_execution_base_url_in_result(self):
        env = {"TRADIER_MARKET_DATA_TOKEN": "t"}
        exec_broker = _sandbox_execution_broker()
        out = resolve_market_data_transport(
            mode="PAPER", account_id="X", execution_broker=exec_broker,
            env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
        )
        assert out["execution_base_url"] == "https://sandbox.tradier.com"

    def test_live_execution_base_url_in_result(self):
        env = {"TRADIER_MARKET_DATA_TOKEN": "t"}
        exec_broker = _live_execution_broker()
        out = resolve_market_data_transport(
            mode="LIVE", account_id="X", execution_broker=exec_broker,
            env=env, broker_cls=_FakeBroker, broker_config_cls=_FakeCfg,
        )
        assert out["execution_base_url"] == "https://api.tradier.com"


class TestRegressionPaperExecutionStillSandbox:
    """Requirement: PR #150 must NOT change submit / cancel / order-status
    routing for paper. Execution credential resolution is the single source
    of truth for that — verify it still pins paper to sandbox.tradier.com."""

    def test_paper_execution_credentials_still_sandbox(self):
        member = {
            "email": "test@example.com",
            "tradier_paper_account_id":   "VAPAPER1",
            "tradier_paper_access_token": "paper-token",
        }
        import os
        prior_mode = os.environ.get("BOT_MODE")
        os.environ["BOT_MODE"] = "PAPER"
        try:
            creds = _cr.resolve_tradier_credentials(member)
        finally:
            if prior_mode is None:
                os.environ.pop("BOT_MODE", None)
            else:
                os.environ["BOT_MODE"] = prior_mode
        assert creds["mode"]     == "PAPER"
        assert creds["base_url"] == "https://sandbox.tradier.com"


# ─────────────────────────────────────────────────────────────────────────────
# PR #391 amendment — strict market-data transport allowlist.
#
# The previous "sandbox substring" denylist over-approved: any nonblank host
# not matching sandbox/paper/sim/mock/test was treated as proven live. That
# opened a path for client_runner to construct TradierConfig with the live
# market-data bearer token pointed at an arbitrary environment-supplied URL.
#
# The corrected policy is a strict allowlist: scheme=https, host exactly
# api.tradier.com, port omitted or 443, no embedded credentials, no query/
# fragment. Invalid explicit URLs must fail BEFORE the token-bearing
# TradierConfig is constructed.
# ─────────────────────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "bad_url",
    [
        "https://evil.example",
        "https://api.tradier.com.evil.example",
        "ftp://api.tradier.com",
        "http://api.tradier.com",
        "https://api.tradier.com:8443",
        "https://sandbox.tradier.com",
        "https://user:pass@api.tradier.com",
        "https://api.tradier.com?redirect=https://evil.example",
    ],
)
def test_invalid_market_data_url_fails_before_token_enters_config(bad_url):
    """A misconfigured TRADIER_MARKET_DATA_BASE_URL must raise before the
    token-bearing TradierConfig is constructed. This is the critical safety
    invariant: the live market-data bearer token must never touch an
    untrusted transport, and the resolver must not silently rewrite the URL.
    """
    config_calls = []

    class RecordingConfig:
        def __init__(self, *, base_url, access_token, account_id):
            config_calls.append({
                "base_url": base_url,
                "access_token": access_token,
                "account_id": account_id,
            })
            self.base_url = base_url
            self.access_token = access_token
            self.account_id = account_id

    class RecordingBroker:
        def __init__(self, cfg):
            self.cfg = cfg

    execution_broker = SimpleNamespace(
        cfg=SimpleNamespace(base_url="https://sandbox.tradier.com")
    )

    with pytest.raises(MarketDataTransportConfigurationError):
        resolve_market_data_transport(
            mode="PAPER",
            account_id="paper-account",
            execution_broker=execution_broker,
            env={
                "TRADIER_MARKET_DATA_TOKEN": "secret-market-data-token",
                "TRADIER_MARKET_DATA_BASE_URL": bad_url,
            },
            broker_cls=RecordingBroker,
            broker_config_cls=RecordingConfig,
        )

    # Critical assertion: invalid URL was rejected before the token-bearing
    # config object was constructed.
    assert config_calls == []


@pytest.mark.parametrize(
    "valid_url",
    [
        "https://api.tradier.com",
        "https://api.tradier.com/",
        "https://api.tradier.com/v1",
        "https://api.tradier.com:443",
    ],
)
def test_exact_tradier_https_transport_is_allowed(valid_url):
    """Positive proof: the exact canonical Tradier live HTTPS transport is
    accepted (with or without trailing slash, with a path, with the default
    port explicit), and the token-bearing TradierConfig is built exactly once
    with the expected inputs.
    """
    config_calls = []

    class RecordingConfig:
        def __init__(self, *, base_url, access_token, account_id):
            config_calls.append({
                "base_url": base_url,
                "access_token": access_token,
                "account_id": account_id,
            })
            self.base_url = base_url
            self.access_token = access_token
            self.account_id = account_id

    class RecordingBroker:
        def __init__(self, cfg):
            self.cfg = cfg

    result = resolve_market_data_transport(
        mode="PAPER",
        account_id="paper-account",
        execution_broker=SimpleNamespace(
            cfg=SimpleNamespace(base_url="https://sandbox.tradier.com")
        ),
        env={
            "TRADIER_MARKET_DATA_TOKEN": "secret-market-data-token",
            "TRADIER_MARKET_DATA_BASE_URL": valid_url,
        },
        broker_cls=RecordingBroker,
        broker_config_cls=RecordingConfig,
    )

    assert result["data_broker"] is not None
    assert result["base_url"] == valid_url
    assert len(config_calls) == 1
    assert config_calls[0]["base_url"] == valid_url
    assert config_calls[0]["access_token"] == "secret-market-data-token"
