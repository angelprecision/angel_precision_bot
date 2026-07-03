"""
P0 (PR #264): LIVE enforcement proof.

A LIVE runner must durably prove it is running under the enforced live
policy — not stale code, not paper/default policy, not a bypassed PR180
pricing guard. On success it emits LIVE_ENFORCEMENT_OK with the full
enforcement snapshot, computes runtime_config_hash (sha256 of the
canonical snapshot), exposes both in the startup manifest, and pins the
hash on the OSM so EVERY order row carries enforcement_config_hash.

Fail-closed startup conditions:
  - PR180 constants unimportable (enforcement module missing/stale)
  - named live client (Jason) with PR180_ENABLED off
  - live client not in the enforcement allowlist while
    LIVE_REQUIRE_NAMED_ENFORCEMENT=1 (default) — a live runner can never
    start under paper/default policy
"""

import os
import pathlib
import re
import sys
from unittest.mock import MagicMock

import pytest

_REPO = pathlib.Path(__file__).resolve().parents[1]
SRC = (_REPO / "client_runner.py").read_text()
OSM_SRC = (_REPO / "ap" / "order_state_machine.py").read_text()

# ─── Mock heavy imports BEFORE importing client_runner (repo pattern) ────────
os.environ.setdefault("DATABASE_URL", "postgresql://stub:stub@localhost/stub")
for _m in (
    "psycopg2", "psycopg2.extras", "psycopg2.pool", "supabase",
    "cryptography", "cryptography.fernet", "ap.db", "ap.queue",
    "ap.order_monitor", "ap.position_sizer", "ap.market_intelligence",
):
    sys.modules.setdefault(_m, MagicMock())


_RISK = dict(
    score_floor=65.0, capital_pct=0.40, sector_pct=0.25, ticker_pct=0.10,
    max_calls=10, max_puts=10, daily_max_loss_pct=0.06,
)


def _make_runner(email="jasoncosby1@gmail.com", mode="LIVE"):
    import client_runner as cr
    r = cr.ClientRunner.__new__(cr.ClientRunner)
    r.email = email
    r.mode = mode
    r.account_id = "ACC1"
    return r


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    for k in (
        "LIVE_ENFORCEMENT_PROOF_ENABLED", "LIVE_REQUIRE_NAMED_ENFORCEMENT",
        "PR180_ENABLED", "PR180_MODE", "LIVE_CONFIRMATION_REQUIRED",
        "ALLOW_FAILED_DIR_CLIENT", "RENDER_GIT_COMMIT", "RENDER_INSTANCE_ID",
    ):
        monkeypatch.delenv(k, raising=False)
    yield


def _pr180_ids():
    from ap_execution_core import PR180_LIVE_CLIENT_IDS
    return PR180_LIVE_CLIENT_IDS


# ── Success path: named live client under enforced policy ───────────────────

def test_named_live_client_passes_and_builds_snapshot(monkeypatch):
    assert "jasoncosby1@gmail.com" in _pr180_ids(), (
        "test premise: Jason must be a PR180 named client"
    )
    monkeypatch.setenv("RENDER_GIT_COMMIT", "abc1234deadbeef")
    monkeypatch.setenv("RENDER_INSTANCE_ID", "srv-live-1")
    r = _make_runner()
    ok, reason = r._verify_live_enforcement(**_RISK)
    assert ok and reason == "ok"
    enf = r.live_enforcement
    assert enf["status"] == "ok"
    assert enf["client_id"] == "jasoncosby1@gmail.com"
    assert enf["execution_mode"] == "LIVE"
    assert enf["commit_sha"] == "abc1234deadbeef"
    assert enf["pod_id"] == "srv-live-1"
    assert enf["pr180_enabled"] is True
    assert enf["pr180_named_client"] is True
    assert enf["confirmation_required"] is True
    assert enf["intraday_failed_dir_allowed"] is False
    assert enf["risk"]["score_floor"] == 65.0
    assert enf["risk"]["daily_max_loss_pct"] == 0.06
    assert re.fullmatch(r"[0-9a-f]{16}", r.runtime_config_hash)
    assert enf["runtime_config_hash"] == r.runtime_config_hash


def test_hash_is_deterministic_and_config_sensitive(monkeypatch):
    monkeypatch.setenv("RENDER_GIT_COMMIT", "abc1234")
    monkeypatch.setenv("RENDER_INSTANCE_ID", "srv-1")
    a = _make_runner(); a._verify_live_enforcement(**_RISK)
    b = _make_runner(); b._verify_live_enforcement(**_RISK)
    assert a.runtime_config_hash == b.runtime_config_hash
    c = _make_runner()
    c._verify_live_enforcement(**{**_RISK, "score_floor": 70.0})
    assert c.runtime_config_hash != a.runtime_config_hash


# ── Fail-closed matrix ───────────────────────────────────────────────────────

def test_named_client_with_pr180_disabled_fails(monkeypatch):
    monkeypatch.setenv("PR180_ENABLED", "0")
    # PR180 constants are module-level; reload to honor the env pin.
    import importlib
    import ap_execution_core
    importlib.reload(ap_execution_core)
    try:
        r = _make_runner()
        ok, reason = r._verify_live_enforcement(**_RISK)
        assert not ok and reason == "pr180_disabled_for_named_live_client"
        assert r.live_enforcement["status"].startswith("failed:")
    finally:
        monkeypatch.delenv("PR180_ENABLED", raising=False)
        importlib.reload(ap_execution_core)


def test_unlisted_live_client_fails_under_default_policy():
    r = _make_runner(email="brand-new-client@x.com")
    ok, reason = r._verify_live_enforcement(**_RISK)
    assert not ok and reason == "live_client_not_in_enforcement_allowlist"


def test_unlisted_live_client_allowed_only_with_explicit_override(monkeypatch):
    monkeypatch.setenv("LIVE_REQUIRE_NAMED_ENFORCEMENT", "0")
    r = _make_runner(email="brand-new-client@x.com")
    ok, reason = r._verify_live_enforcement(**_RISK)
    assert ok and reason == "ok"
    assert r.live_enforcement["pr180_named_client"] is False


def test_kill_switch_is_loud(monkeypatch):
    monkeypatch.setenv("LIVE_ENFORCEMENT_PROOF_ENABLED", "0")
    r = _make_runner()
    ok, reason = r._verify_live_enforcement(**_RISK)
    assert ok and reason == "disabled_by_env"
    assert "LIVE_ENFORCEMENT_PROOF_DISABLED_BY_ENV" in SRC


# ── Wiring fences ────────────────────────────────────────────────────────────

def test_startup_wiring_live_only_after_both_preflights():
    block = re.search(
        r"self\.databroker = data_broker(.*?)_verify_live_enforcement\(", SRC, re.S
    )
    assert block and 'strip().upper() == "LIVE"' in block.group(1)
    assert '_mark_failed(f"LIVE_ENFORCEMENT_FAILED:{_enf_reason}")' in SRC


def test_hash_is_pinned_on_osm_at_startup():
    assert "self.order_state_machine.runtime_config_hash = self.runtime_config_hash" in SRC


def test_manifest_exposes_enforcement_and_hash():
    assert SRC.count('"live_enforcement": getattr(self, "live_enforcement"') == 2
    assert SRC.count('"runtime_config_hash": getattr(self, "runtime_config_hash", None)') == 2


def test_markers_present():
    assert "LIVE_ENFORCEMENT_OK" in SRC
    assert "LIVE_ENFORCEMENT_FAILED reason=" in SRC


# ── OSM stamps every order with the hash ─────────────────────────────────────

def test_osm_stamps_enforcement_hash_on_create():
    m = re.search(
        r'meta\["side"\] = _direction(.*?)_rt_hash = getattr\(self, "runtime_config_hash", None\)',
        OSM_SRC, re.S,
    )
    assert m, "hash stamp must live in create_entry_order after direction canonicalization"
    assert 'meta["enforcement_config_hash"] = str(_rt_hash)' in OSM_SRC
    # Caller-supplied value wins; absent hash writes nothing.
    assert 'if _rt_hash and "enforcement_config_hash" not in meta:' in OSM_SRC
