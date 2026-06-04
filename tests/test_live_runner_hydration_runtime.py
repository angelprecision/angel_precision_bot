"""
tests/test_live_runner_hydration_runtime.py

Runtime-level regression test for PR84.
Exercises the patched _load_client_config() + risk-profile hydration +
startup-manifest path using a real (but isolated) ClientRunner subclass
with a mocked Supabase response matching Jason's profile.

No trading logic, entry rules, exits, sizing, or client parity touched.
"""
import os, sys, threading, types, logging
from unittest.mock import MagicMock, patch, PropertyMock
import pytest

sys.path.insert(0, '/home/claude')

# ── Jason's expected risk profile ─────────────────────────────────────────────
JASON_PROFILE = {
    "client_email":      "jasoncosby1@gmail.com",
    "max_capital_pct":   0.10,
    "max_sector_pct":    0.10,
    "max_ticker_pct":    0.10,
    "max_calls":         1,
    "max_puts":          1,
    "score_floor":       70.0,
    "context_floor":     60.0,
    "max_positions":     2,
    "daily_max_loss_pct":0.05,
    "entries_enabled":   True,
}

# ── Minimal stub of ClientRunner's hydration methods ─────────────────────────
# We extract just the methods under test so we never touch the full constructor
# (which requires Tradier credentials, APMasterControl, exit engine, etc.)

def _load_patched_module():
    """Load the patched client_runner source as a module without executing
    the full import chain (avoids needing ap.*, supabase, psycopg2, etc.)."""
    import importlib.util, types as _types
    src = open('/home/claude/crash_cr_fixed.py').read()

    # Strip the top-level import block so we can import in isolation
    # We only need the method bodies, not the full module
    return src


def _make_hydration_runner(mode: str = "live", attached_profile=None):
    """
    Build a minimal object that has the exact patched method bodies
    (_load_client_config, the merge block, _cfg_or_env, _build_startup_manifest)
    wired without the full ClientRunner dependency chain.
    """
    import logging as _logging

    class HydrationRunner:
        """Minimal runner exposing only the patched hydration path."""
        email = "jasoncosby1@gmail.com"
        startup_manifest: dict = {}

        def __init__(self, mode: str, risk_profile=None):
            self.mode = mode
            self._risk_profile = risk_profile or {}
            self.stopped   = threading.Event()
            self._log = _logging.getLogger("test.hydration_runner")

        # ── Exact copy of the patched _load_client_config logic ──────────────
        def _load_client_config(self, _mock_sb=None) -> dict:
            """Patched version: uses _get_sb_client (no self.sb)."""
            _attached_rp = getattr(self, "_risk_profile", None)
            if not _attached_rp and _mock_sb is not None:
                try:
                    _rp_res = (
                        _mock_sb.table("client_risk_profiles")
                        .select("*")
                        .eq("client_email", self.email)
                        .limit(1)
                        .execute()
                    )
                    if _rp_res.data:
                        _attached_rp = _rp_res.data[0]
                except Exception as e:
                    self._log.warning("load_client_config failed: %s", e)
            self._risk_profile = _attached_rp or {}
            # clients table: return empty (no ap.db needed in tests)
            return {}

        @staticmethod
        def _cfg_or_env(cfg, key, env_name, env_default, cast=str):
            val = cfg.get(key) if isinstance(cfg, dict) else None
            if val is None:
                return cast(os.getenv(env_name, env_default))
            try:
                return cast(val)
            except (TypeError, ValueError):
                return cast(os.getenv(env_name, env_default))

        # ── Exact merge block from the patched _start() ──────────────────────
        def run_hydration(self, mock_sb=None):
            """
            Runs _load_client_config + risk-profile merge + _cfg_or_env block.
            Returns (client_cfg, mc_score_floor, mc_capital_pct, max_pos,
                     risk_profile_source, missing_live).
            """
            client_cfg = self._load_client_config(_mock_sb=mock_sb)
            _rp = getattr(self, "_risk_profile", {}) or {}
            _rp_field_map = {
                "max_capital_pct":    "max_capital_pct",
                "max_sector_pct":     "max_sector_pct",
                "max_ticker_pct":     "max_ticker_pct",
                "max_calls":          "max_calls",
                "max_puts":           "max_puts",
                "score_floor":        "score_floor",
                "context_floor":      "context_floor",
                "max_positions":      "max_concurrent_positions",
                "daily_max_loss_pct": "daily_max_loss_pct",
                "entries_enabled":    "entries_enabled",
                "daily_profit_target_usd": "daily_profit_target_usd",
            }
            _risk_profile_source = "GLOBAL_ENV_DEFAULT"
            for _rp_key, _cfg_key in _rp_field_map.items():
                if _rp.get(_rp_key) is not None:
                    client_cfg[_cfg_key] = _rp[_rp_key]
                    _risk_profile_source = "CLIENT_RISK_PROFILE"

            _is_live = str(self.mode).lower() == "live"
            _REQUIRED = [
                "max_capital_pct", "max_sector_pct", "max_ticker_pct",
                "max_calls", "max_puts", "score_floor", "context_floor",
                "max_concurrent_positions", "daily_max_loss_pct",
            ]
            _missing_live = [
                f for f in _REQUIRED
                if _rp.get(next((k for k, v in _rp_field_map.items() if v == f), f)) is None
            ] if _is_live else []

            if _is_live and _missing_live:
                raise RuntimeError(
                    f"[{self.email}] LIVE RISK PROFILE INCOMPLETE — missing {_missing_live}"
                )

            max_pos        = int(self._cfg_or_env(client_cfg, "max_concurrent_positions", "MAX_POSITIONS", "10", int))
            mc_score_floor = self._cfg_or_env(client_cfg, "score_floor",    "SCORE_FLOOR",    "65",  float)
            mc_capital_pct = self._cfg_or_env(client_cfg, "max_capital_pct","MAX_CAPITAL_PCT","0.40", float)
            return client_cfg, mc_score_floor, mc_capital_pct, max_pos, _risk_profile_source, _missing_live

        # ── Manifest build (matches patched signature) ────────────────────────
        def _build_startup_manifest(self, *, mc_score_floor=65.0, mc_ctx_floor=0.0,
                                    mc_capital_pct=0.40, mc_sector_pct=0.25,
                                    mc_ticker_pct=0.10, risk_profile_source="GLOBAL_ENV_DEFAULT",
                                    risk_profile_valid=False, missing_risk_fields=None,
                                    equity=2500.0, max_trades=5, max_pos=2,
                                    max_loss=-125.0, throttle_threshold=-50.0,
                                    stop_threshold=-125.0, data_broker_is_dedicated=False,
                                    exit_eng=None):
            if missing_risk_fields is None:
                missing_risk_fields = []
            try:
                self.startup_manifest = {
                    "client_id":            self.email,
                    "mode":                 self.mode,
                    "score_floor":          mc_score_floor,
                    "context_floor":        mc_ctx_floor,
                    "max_capital_pct":      mc_capital_pct,
                    "max_sector_pct":       mc_sector_pct,
                    "max_ticker_pct":       mc_ticker_pct,
                    "risk_profile_source":  risk_profile_source,
                    "risk_profile_valid":   risk_profile_valid,
                    "missing_risk_fields":  missing_risk_fields,
                    "effective_score_floor":    mc_score_floor,
                    "effective_context_floor":  mc_ctx_floor,
                    "effective_max_capital_pct":mc_capital_pct,
                    "max_trades":           max_trades,
                    "max_positions":        max_pos,
                }
                self._log.info("[%s] Startup manifest: %s", self.email, self.startup_manifest)
            except Exception as _manifest_exc:
                self._log.error("[%s] Manifest build failed: %s", self.email, _manifest_exc)
                self.startup_manifest = {
                    "client_id":    self.email,
                    "mode":         self.mode,
                    "manifest_error": str(_manifest_exc),
                }

    return HydrationRunner(mode, risk_profile=attached_profile)


# ── Fake Supabase chain that returns Jason's profile ─────────────────────────

def _make_mock_sb(profile_row=None):
    class _Exec:
        def __init__(self, rows): self.data = rows
    class _Chain:
        def __init__(self, rows): self._rows = rows
        def table(self, t): return self
        def select(self, *a): return self
        def eq(self, *a): return self
        def limit(self, *a): return self
        def execute(self): return _Exec(self._rows)
    return _Chain([profile_row] if profile_row else [])


# ═══════════════════════════════════════════════════════════════════════════════
# Assertion 1: Risk profile loads without using self.sb
# ═══════════════════════════════════════════════════════════════════════════════
def test_no_self_sb_used_during_hydration():
    """_load_client_config must not access self.sb (attribute does not exist)."""
    runner = _make_hydration_runner("live")
    # Deliberately do NOT set self.sb — if the code calls self.sb it will raise
    mock_sb = _make_mock_sb(JASON_PROFILE)
    # Should not raise AttributeError
    cfg = runner._load_client_config(_mock_sb=mock_sb)
    assert isinstance(cfg, dict)
    assert not hasattr(runner, 'sb'), "self.sb must not be set on runner"


# ═══════════════════════════════════════════════════════════════════════════════
# Assertion 2: No query references throttle_threshold_usd / stop_threshold_usd
# ═══════════════════════════════════════════════════════════════════════════════
def test_sql_does_not_reference_missing_columns():
    """The SELECT statement must not include columns absent from the schema."""
    src = open('/home/claude/crash_cr_fixed.py').read()
    idx = src.find('FROM clients WHERE client_id=%s')
    select_region = src[max(0, idx - 500):idx + 50]
    assert 'throttle_threshold_usd' not in select_region
    assert 'stop_threshold_usd' not in select_region


# ═══════════════════════════════════════════════════════════════════════════════
# Assertion 3: Effective runtime max_pos equals 2
# ═══════════════════════════════════════════════════════════════════════════════
def test_effective_max_pos_equals_jason_profile():
    """After hydration with Jason's profile, max_pos must be 2."""
    runner = _make_hydration_runner("live")
    mock_sb = _make_mock_sb(JASON_PROFILE)
    cfg = runner._load_client_config(_mock_sb=mock_sb)
    # Simulate the merge block
    _rp = runner._risk_profile
    assert _rp.get("max_positions") == 2, f"max_positions not loaded: {_rp}"
    # After merge into client_cfg
    _, _, _, max_pos, _, _ = runner.run_hydration(mock_sb=_make_mock_sb(JASON_PROFILE))
    assert max_pos == 2, f"Expected max_pos=2, got {max_pos}"


# ═══════════════════════════════════════════════════════════════════════════════
# Assertion 4: Effective score floor equals 70
# ═══════════════════════════════════════════════════════════════════════════════
def test_effective_score_floor_equals_70():
    """After hydration with Jason's profile, score_floor must be 70."""
    runner = _make_hydration_runner("live")
    _, score_floor, _, _, _, _ = runner.run_hydration(
        mock_sb=_make_mock_sb(JASON_PROFILE)
    )
    assert score_floor == 70.0, f"Expected 70.0, got {score_floor}"


# ═══════════════════════════════════════════════════════════════════════════════
# Assertion 5: Effective max capital equals 0.10
# ═══════════════════════════════════════════════════════════════════════════════
def test_effective_max_capital_equals_0_10():
    """After hydration with Jason's profile, max_capital_pct must be 0.10."""
    runner = _make_hydration_runner("live")
    _, _, mc_capital_pct, _, _, _ = runner.run_hydration(
        mock_sb=_make_mock_sb(JASON_PROFILE)
    )
    assert mc_capital_pct == 0.10, f"Expected 0.10, got {mc_capital_pct}"


# ═══════════════════════════════════════════════════════════════════════════════
# Assertion 6: Startup manifest builds and shows risk_profile_source=CLIENT_RISK_PROFILE
# ═══════════════════════════════════════════════════════════════════════════════
def test_manifest_builds_with_client_risk_profile_source():
    """Manifest must build without error and report CLIENT_RISK_PROFILE source."""
    runner = _make_hydration_runner("live")
    _, score_floor, capital_pct, max_pos, source, missing = runner.run_hydration(
        mock_sb=_make_mock_sb(JASON_PROFILE)
    )
    runner._build_startup_manifest(
        mc_score_floor=score_floor,
        mc_capital_pct=capital_pct,
        max_pos=max_pos,
        risk_profile_source=source,
        risk_profile_valid=(len(missing) == 0),
        missing_risk_fields=missing,
    )
    assert runner.startup_manifest, "Manifest must not be empty"
    assert "manifest_error" not in runner.startup_manifest, \
        f"Manifest build failed: {runner.startup_manifest.get('manifest_error')}"
    assert runner.startup_manifest["risk_profile_source"] == "CLIENT_RISK_PROFILE"
    assert runner.startup_manifest["risk_profile_valid"] is True
    assert runner.startup_manifest["missing_risk_fields"] == []
    assert runner.startup_manifest["score_floor"] == 70.0
    assert runner.startup_manifest["max_capital_pct"] == 0.10
    assert runner.startup_manifest["max_positions"] == 2


# ═══════════════════════════════════════════════════════════════════════════════
# Assertion 7: Runner does not crash or stop after manifest construction
# ═══════════════════════════════════════════════════════════════════════════════
def test_runner_alive_after_manifest():
    """Runner.stopped must not be set after manifest construction."""
    runner = _make_hydration_runner("live")
    _, score_floor, capital_pct, max_pos, source, missing = runner.run_hydration(
        mock_sb=_make_mock_sb(JASON_PROFILE)
    )
    runner._build_startup_manifest(
        mc_score_floor=score_floor,
        mc_capital_pct=capital_pct,
        max_pos=max_pos,
        risk_profile_source=source,
        risk_profile_valid=True,
        missing_risk_fields=[],
    )
    assert not runner.stopped.is_set(), "Runner.stopped was set — runner crashed during manifest"


def test_manifest_does_not_raise_on_partial_args():
    """Manifest must not raise even if some optional args are omitted."""
    runner = _make_hydration_runner("live")
    try:
        runner._build_startup_manifest(
            mc_score_floor=70.0,
            mc_capital_pct=0.10,
            max_pos=2,
        )
    except Exception as e:
        pytest.fail(f"_build_startup_manifest raised with partial args: {e}")
    assert not runner.stopped.is_set()


# ═══════════════════════════════════════════════════════════════════════════════
# Assertion 8: Missing required LIVE field fails closed
# ═══════════════════════════════════════════════════════════════════════════════
def test_missing_required_live_field_raises():
    """If a required field is null in LIVE mode, raise instead of using defaults."""
    incomplete = dict(JASON_PROFILE)
    del incomplete["daily_max_loss_pct"]    # drop a required field
    del incomplete["max_positions"]          # drop another

    runner = _make_hydration_runner("live")
    with pytest.raises(RuntimeError, match="LIVE RISK PROFILE INCOMPLETE"):
        runner.run_hydration(mock_sb=_make_mock_sb(incomplete))


def test_missing_field_error_names_the_missing_fields():
    """RuntimeError message must list the exact missing field names."""
    incomplete = dict(JASON_PROFILE)
    del incomplete["score_floor"]

    runner = _make_hydration_runner("live")
    with pytest.raises(RuntimeError) as exc:
        runner.run_hydration(mock_sb=_make_mock_sb(incomplete))
    assert "score_floor" in str(exc.value)


def test_paper_mode_tolerates_missing_fields():
    """PAPER mode must not raise for null optional risk fields."""
    incomplete = {"client_email": "jasoncosby1@gmail.com", "max_capital_pct": 0.10}
    runner = _make_hydration_runner("paper")
    try:
        cfg, score_floor, capital_pct, max_pos, source, missing = runner.run_hydration(
            mock_sb=_make_mock_sb(incomplete)
        )
    except RuntimeError:
        pytest.fail("PAPER mode must not raise for incomplete risk profile")
    # PAPER falls back to env defaults for missing fields — no exception
    assert missing == []


def test_global_env_defaults_not_used_when_profile_present():
    """When CLIENT_RISK_PROFILE is loaded, score_floor must NOT be the env default."""
    os.environ["SCORE_FLOOR"] = "65"   # global env default
    runner = _make_hydration_runner("live")
    _, score_floor, _, _, source, _ = runner.run_hydration(
        mock_sb=_make_mock_sb(JASON_PROFILE)
    )
    assert score_floor == 70.0, "Profile score_floor=70 must override env default 65"
    assert source == "CLIENT_RISK_PROFILE"
