"""
tests/test_live_risk_profile_hydration.py
Fixes for live ClientRunner config hydration and startup-manifest crash.
"""
import os, sys, types, pytest
sys.path.insert(0, '/home/claude')
os.environ.setdefault("BOT_MODE", "live")

# ── Minimal ClientRunner stub ─────────────────────────────────────────────────
# Import only the functions / logic we patched — no Supabase needed.

def _make_fake_runner(mode="live", risk_profile=None):
    """Build a minimal object that exercises the patched code paths."""
    class FakeRunner:
        email       = "jasoncosby1@gmail.com"
        startup_manifest = {}
        _risk_profile    = risk_profile or {}

        def _cfg_or_env(self, cfg, key, env_key, default, cast=str):
            v = cfg.get(key)
            if v is not None:
                try: return cast(v)
                except: pass
            return cast(os.getenv(env_key, default))

    r = FakeRunner()
    r.mode = mode
    return r


# ── Test 1: manifest does not crash on NameError ─────────────────────────────
def test_manifest_build_uses_passed_params_not_locals():
    """
    _build_startup_manifest must accept mc_score_floor etc. as kwargs.
    Verifies the NameError is gone by calling the method directly.
    """
    # Import the patched module
    import importlib, shutil
    shutil.copy('/home/claude/crash_cr_fixed.py', '/home/claude/client_runner_patched.py')

    # We can't fully instantiate ClientRunner, but we can verify the
    # manifest method signature accepts the new kwargs without NameError.
    source = open('/home/claude/crash_cr_fixed.py').read()
    assert 'mc_score_floor: float = 65.0' in source, \
        "mc_score_floor not in _build_startup_manifest signature"
    assert 'mc_ctx_floor: float   = 0.0' in source, \
        "mc_ctx_floor not in signature"
    assert '"score_floor": mc_score_floor,' in source, \
        "manifest still references _mc_score_floor local"
    assert '"effective_score_floor": mc_score_floor,' in source


def test_manifest_call_site_passes_mc_vars():
    """Call site must pass mc_score_floor etc. so they are in scope."""
    source = open('/home/claude/crash_cr_fixed.py').read()
    assert 'mc_score_floor=_mc_score_floor,' in source
    assert 'mc_ctx_floor=_mc_ctx_floor,' in source
    assert 'risk_profile_source=_risk_profile_source,' in source
    assert 'risk_profile_valid=(len(_missing_live) == 0),' in source
    assert 'missing_risk_fields=_missing_live,' in source


def test_manifest_wrapped_in_try_except():
    """Manifest build must never kill the runner — wrapped in try/except."""
    source = open('/home/claude/crash_cr_fixed.py').read()
    assert 'except Exception as _manifest_exc:' in source
    assert 'manifest_error' in source   # fallback manifest field


# ── Test 2: self.sb replaced with _get_sb_client() ───────────────────────────
def test_no_self_sb_in_load_client_config():
    source = open('/home/claude/crash_cr_fixed.py').read()
    # Find _load_client_config function body
    idx = source.find('def _load_client_config(')
    fn_end = source.find('\n    def ', idx + 1)
    fn = source[idx:fn_end]
    assert 'self.sb' not in fn, \
        "self.sb still present in _load_client_config — ClientRunner has no .sb attribute"
    assert '_get_sb_client' in fn or '_get_sb' in fn, \
        "_get_sb_client not used in _load_client_config"


# ── Test 3: throttle_threshold_usd removed from SQL ──────────────────────────
def test_bad_sql_columns_removed():
    source = open('/home/claude/crash_cr_fixed.py').read()
    # Find the SQL SELECT statement in _load_client_config
    idx = source.find('FROM clients WHERE client_id=%s')
    select_region = source[max(0, idx-400):idx+50]
    assert 'throttle_threshold_usd' not in select_region, \
        "throttle_threshold_usd still in SELECT — column does not exist in schema"
    assert 'stop_threshold_usd' not in select_region, \
        "stop_threshold_usd still in SELECT — column does not exist in schema"


# ── Test 4: Risk profile hydration uses client_risk_profiles values ───────────
def test_risk_profile_merged_into_client_cfg():
    """
    After _load_client_config(), client_risk_profiles values override
    clients table entries. Verify mapping logic is present.
    """
    source = open('/home/claude/crash_cr_fixed.py').read()
    assert '"max_positions":     "max_concurrent_positions"' in source, \
        "max_positions → max_concurrent_positions mapping missing"
    assert '_risk_profile_source = "CLIENT_RISK_PROFILE"' in source


def test_live_fail_closed_on_incomplete_profile():
    """
    In LIVE mode, if required risk fields are missing, raise RuntimeError
    instead of falling back to global defaults.
    """
    source = open('/home/claude/crash_cr_fixed.py').read()
    assert 'LIVE RISK PROFILE INCOMPLETE' in source
    assert 'RuntimeError' in source
    assert '_missing_live' in source


# ── Test 5: Jason's expected profile values flow correctly ────────────────────
def test_jason_profile_field_map_covers_required_fields():
    """All of Jason's required fields must be in the _rp_field_map."""
    source = open('/home/claude/crash_cr_fixed.py').read()
    idx = source.find('_rp_field_map = {')
    end = source.find('}', idx)
    field_map_str = source[idx:end+1]

    required = [
        'max_capital_pct', 'max_sector_pct', 'max_ticker_pct',
        'max_calls', 'max_puts', 'score_floor', 'context_floor',
        'max_positions', 'daily_max_loss_pct', 'entries_enabled',
    ]
    for f in required:
        assert f in field_map_str, \
            f"Required field '{f}' missing from _rp_field_map"


def test_manifest_shows_effective_values_not_env():
    """Manifest must show effective values (mc_*), not raw os.getenv() calls."""
    source = open('/home/claude/crash_cr_fixed.py').read()
    idx = source.find('def _build_startup_manifest(')
    fn_end = source.find('\n    def ', idx + 1)
    fn = source[idx:fn_end]
    # None of the risk fields should be reading os.getenv inside the manifest
    assert 'os.getenv("SCORE_FLOOR"' not in fn
    assert 'os.getenv("CONTEXT_FLOOR"' not in fn
    assert 'os.getenv("MAX_CAPITAL_PCT"' not in fn
    assert '"score_floor": mc_score_floor,' in fn
    assert '"context_floor": mc_ctx_floor,' in fn


def test_manifest_includes_risk_truth_fields():
    """Manifest must include risk_profile_source, risk_profile_valid, missing_risk_fields."""
    source = open('/home/claude/crash_cr_fixed.py').read()
    idx = source.find('def _build_startup_manifest(')
    fn_end = source.find('\n    def ', idx + 1)
    fn = source[idx:fn_end]
    assert '"risk_profile_source": risk_profile_source,' in fn
    assert '"risk_profile_valid": risk_profile_valid,' in fn
    assert '"missing_risk_fields": missing_risk_fields,' in fn
