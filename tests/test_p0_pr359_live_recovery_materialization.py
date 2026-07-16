from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import ap.order_state_machine as osm_mod
from ap.order_state_machine import APOrderStateMachine


REPO = Path(__file__).resolve().parents[1]


class _Cursor:
    def __init__(self, sink, rowcount=1):
        self.sink = sink
        self.rowcount = rowcount

    def execute(self, sql, params=()):
        self.sink.append((" ".join(str(sql).split()), tuple(params)))
        return self


class _Conn:
    def __init__(self, sink, rowcount=1):
        self.cursor = _Cursor(sink, rowcount=rowcount)

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_):
        return False


def _patch_osm_db(monkeypatch, *, rowcount=1):
    sink = []
    monkeypatch.setattr(osm_mod, "conn", lambda: _Conn(sink, rowcount=rowcount))
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **k: fn())
    return sink


def _claim_kwargs(**overrides):
    base = {
        "owner": "watcher:jason:nke",
        "generation": 1,
        "lease_until": (datetime.now(timezone.utc) + timedelta(seconds=120)).isoformat(),
        "trigger_crossed_at": datetime.now(timezone.utc).isoformat(),
        "trigger_price": 43.58,
        "observed_underlying_price": 43.59,
        "signal_id": "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72:f4dc44",
        "execution_mode": "live",
        "canonical_signal_id": "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72",
        "expected_order_status": "PENDING_TRIGGER",
        "expected_materialization_status": "WAITING_FOR_TRIGGER",
        "expected_lifecycle_state": "",
    }
    base.update(overrides)
    return base


def test_reeval_suffixed_source_claim_uses_canonical_order_identity(monkeypatch):
    sink = _patch_osm_db(monkeypatch, rowcount=1)
    osm = APOrderStateMachine("jasoncosby1@gmail.com")

    assert osm.claim_deferred_materialization("nke-oid", **_claim_kwargs()) is True

    sql, params = sink[-1]
    patch = json.loads(params[0])
    assert "canonical_signal_id = %s" in sql
    assert "COALESCE(meta->>'submit_intent_at','') = ''" in sql
    assert patch["signal_id"].endswith(":f4dc44")
    assert "REEVAL:8d9338d0-5dde-4b7b-81ea-208039999b72" in params


def test_genuinely_different_opportunity_cannot_claim(monkeypatch):
    sink = _patch_osm_db(monkeypatch, rowcount=0)
    osm = APOrderStateMachine("jasoncosby1@gmail.com")

    assert osm.claim_deferred_materialization(
        "nke-oid",
        **_claim_kwargs(canonical_signal_id="REEVAL:11111111-1111-4111-8111-111111111111"),
    ) is False
    sql, params = sink[-1]
    assert "canonical_signal_id = %s" in sql
    assert "REEVAL:11111111-1111-4111-8111-111111111111" in params


def test_live_recovery_classifier_replaces_broad_live_watching_reset():
    src = (REPO / "ap_recovery.py").read_text()

    assert "def _recover_unowned_live_watching_signals" in src
    assert "LOWER(COALESCE(payload->>'execution_mode','')) = 'live'" in src
    assert "LIVE_RECOVERY_MISSED_TRIGGER" in src
    assert "ownership_absent_at_trigger" in src
    assert "recovery_rescue" in src

    live_branch = src.split("if _is_live:", 1)[1].split("else:", 1)[0]
    assert "_recover_unowned_live_watching_signals" in live_branch
    assert "_reset" not in live_branch
    assert "recovery_rescue" not in live_branch


def test_materialization_cas_loser_is_already_claimed_not_state_write_failed():
    src = (REPO / "ap_execution_core.py").read_text()

    assert "MATERIALIZATION_ALREADY_CLAIMED" in src
    assert "materialization_in_flight" in src
    assert "materialization_lease_until" in src
    assert "and _claim_canonical == str(_canonical_signal_id or \"\").strip()" in src
    already = src.index("MATERIALIZATION_ALREADY_CLAIMED")
    state_failed = src.index("MATERIALIZATION_STATE_WRITE_FAILED", already)
    assert already < state_failed


def test_materialization_identity_never_falls_back_to_raw_plan_signal_as_canonical():
    src = (REPO / "ap_execution_core.py").read_text()

    identity_block = src.split("_canonical_signal_id = str(", 1)[1].split(").strip()", 1)[0]
    assert "_durable_canonical_id" in identity_block
    assert "getattr(approved_plan, \"canonical_signal_id\"" in identity_block
    assert "_plan_meta_for_identity.get(\"canonical_signal_id\"" in identity_block
    assert "_signal_payload_for_identity.get(\"canonical_signal_id\"" in identity_block
    assert "build_canonical_signal_id(" in identity_block
    assert "or getattr(approved_plan, \"signal_id\"" not in identity_block


def test_nke_affordability_budget_ordering_source_contract():
    src = (REPO / "ap" / "contract_selector.py").read_text()

    assert "_qty_for_affordability" in src
    assert "_max_affordable_premium" in src
    assert "_chain_with_order = list(enumerate(chain))" in src
    assert "if _ask <= 0:" in src
    assert "chain = [opt for _, opt in sorted(_chain_with_order, key=_affordability_order)]" in src
    assert "SELECTOR_REQUEST_BUDGET_EXHAUSTED" in src
    assert "NO_AFFORDABLE_CONTRACT" in src


def test_incident_policy_and_workflow_coverage_are_preserved():
    workflow = (REPO / ".github" / "workflows" / "p0_regression.yml").read_text()
    core = (REPO / "ap_execution_core.py").read_text()
    recovery = (REPO / "ap_recovery.py").read_text()

    assert "tests/test_p0_pr143_live_recovery_replay_regression.py" in workflow
    assert "tests/test_p0_pr359_live_recovery_materialization.py" in workflow
    assert "POLICY_BLOCKED" in core or "POLICY_BLOCKED" in recovery
    assert "WMT" not in recovery or "LIVE_RECOVERY_MISSED_TRIGGER" in recovery
