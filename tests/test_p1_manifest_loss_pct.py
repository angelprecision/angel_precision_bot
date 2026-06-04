"""
tests/test_p1_manifest_loss_pct.py
Regression: _build_startup_manifest must not reference caller-local variables.
"""
import os, sys, threading
sys.path.insert(0, '/home/claude')

import pytest

# ── Minimal manifest-only stub ───────────────────────────────────────────────
class _ManifestRunner:
    """Exposes only the patched _build_startup_manifest body for isolation."""
    email        = "jasoncosby1@gmail.com"
    account_id   = "VA12345"
    mode         = "live"
    base_url     = "https://sandbox.tradier.com/v1"
    startup_manifest: dict = {}
    stopped      = threading.Event()
    core         = None
    position_manager     = None
    order_state_machine  = None
    contract_selector    = None
    order_monitor        = None
    quotemonitor         = None
    quote_monitor        = None
    fill_monitor_thread  = None
    worker_thread        = None
    equity_thread        = None
    reconciler           = None

    def _build_startup_manifest(
        self, *, equity, max_trades, max_pos, max_loss,
        throttle_threshold, stop_threshold, data_broker_is_dedicated, exit_eng,
        mc_score_floor=65.0, mc_ctx_floor=0.0, mc_capital_pct=0.40,
        mc_sector_pct=0.25, mc_ticker_pct=0.10,
        risk_profile_source="GLOBAL_ENV_DEFAULT",
        risk_profile_valid=False, missing_risk_fields=None,
        daily_max_loss_pct=0.06,
    ):
        if missing_risk_fields is None:
            missing_risk_fields = []
        try:
            self.startup_manifest = {
                "client_id":              self.email,
                "mode":                   self.mode,
                "equity":                 equity,
                "max_trades":             max_trades,
                "max_positions":          max_pos,
                "max_daily_loss":         max_loss,
                "score_floor":            mc_score_floor,
                "context_floor":          mc_ctx_floor,
                "max_capital_pct":        mc_capital_pct,
                "max_sector_pct":         mc_sector_pct,
                "max_ticker_pct":         mc_ticker_pct,
                "risk_profile_source":    risk_profile_source,
                "risk_profile_valid":     risk_profile_valid,
                "missing_risk_fields":    missing_risk_fields,
                "effective_score_floor":      mc_score_floor,
                "effective_context_floor":    mc_ctx_floor,
                "effective_max_capital_pct":  mc_capital_pct,
                "effective_daily_max_loss_pct": daily_max_loss_pct,
            }
        except Exception as _exc:
            self.startup_manifest = {"manifest_error": str(_exc)}


def _runner(**overrides):
    r = _ManifestRunner()
    defaults = dict(
        equity=2500.0, max_trades=5, max_pos=2, max_loss=-125.0,
        throttle_threshold=-50.0, stop_threshold=-125.0,
        data_broker_is_dedicated=False, exit_eng=None,
        mc_score_floor=70.0, mc_ctx_floor=60.0, mc_capital_pct=0.10,
        mc_sector_pct=0.10, mc_ticker_pct=0.10,
        risk_profile_source="CLIENT_RISK_PROFILE",
        risk_profile_valid=True, missing_risk_fields=[],
        daily_max_loss_pct=0.05,
    )
    defaults.update(overrides)
    r._build_startup_manifest(**defaults)
    return r


# ── Tests ─────────────────────────────────────────────────────────────────────

def test_no_loss_pct_name_error():
    """Manifest must build without NameError for loss_pct."""
    r = _runner()
    assert "manifest_error" not in r.startup_manifest, \
        f"Manifest failed: {r.startup_manifest.get('manifest_error')}"


def test_effective_daily_max_loss_pct_present():
    r = _runner(daily_max_loss_pct=0.05)
    assert r.startup_manifest["effective_daily_max_loss_pct"] == 0.05


def test_risk_profile_source_present():
    r = _runner(risk_profile_source="CLIENT_RISK_PROFILE")
    assert r.startup_manifest["risk_profile_source"] == "CLIENT_RISK_PROFILE"


def test_risk_profile_valid_present():
    r = _runner(risk_profile_valid=True)
    assert r.startup_manifest["risk_profile_valid"] is True


def test_missing_risk_fields_present():
    r = _runner(missing_risk_fields=[])
    assert r.startup_manifest["missing_risk_fields"] == []


def test_effective_score_floor_present():
    r = _runner(mc_score_floor=70.0)
    assert r.startup_manifest["effective_score_floor"] == 70.0


def test_effective_context_floor_present():
    r = _runner(mc_ctx_floor=60.0)
    assert r.startup_manifest["effective_context_floor"] == 60.0


def test_effective_max_capital_pct_present():
    r = _runner(mc_capital_pct=0.10)
    assert r.startup_manifest["effective_max_capital_pct"] == 0.10


def test_max_positions_present():
    r = _runner(max_pos=2)
    assert r.startup_manifest["max_positions"] == 2


def test_max_daily_loss_present():
    r = _runner(max_loss=-125.0)
    assert r.startup_manifest["max_daily_loss"] == -125.0


def test_runner_not_stopped_after_manifest():
    """Manifest build must never set runner.stopped."""
    r = _runner()
    assert not r.stopped.is_set()


def test_source_code_has_no_bare_loss_pct_in_manifest():
    """Source-level: no bare 'loss_pct' reference inside _build_startup_manifest."""
    src = open('/home/claude/p1_cr_fixed.py').read()
    idx = src.find('    def _build_startup_manifest(')
    fn_end = src.find('\n    def ', idx + 1)
    fn = src[idx:fn_end]
    bare = [l.strip() for l in fn.splitlines()
            if 'loss_pct' in l and 'daily_max_loss_pct' not in l]
    assert not bare, f"Bare loss_pct found in manifest fn: {bare}"


def test_source_code_call_site_passes_daily_max_loss_pct():
    """Call site must pass daily_max_loss_pct=loss_pct."""
    src = open('/home/claude/p1_cr_fixed.py').read()
    assert 'daily_max_loss_pct=loss_pct,' in src
