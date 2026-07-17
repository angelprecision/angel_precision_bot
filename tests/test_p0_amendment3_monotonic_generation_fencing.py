"""P0 tests for Amendment §3: strictly monotonic generation fencing.

Proves the mandatory invariants from PR #323 amendment §3:

  * claim_deferred_materialization advances the generation by exactly
    one: the SQL predicate binds ``new_generation - 1`` as the required
    persisted (expected-previous) generation, and the patch writes
    ``new_generation``
  * the old permissive ``<= %s`` / ``generation = ''`` predicate is
    gone — a stale worker at an already-consumed generation can no
    longer reclaim the row
  * ``new_generation < 1`` is rejected without any DB write
  * the legacy ``generation=N`` keyword is a semantic alias for
    ``new_generation=N``
  * ``new_generation`` takes precedence when both are supplied
  * a CAS miss (real DB rowcount 0 because the generation already
    advanced) makes the claim return False
  * the downstream fenced writes — persist_deferred_broker_ready,
    schedule_deferred_materialization_retry, terminalize_deferred_breach —
    all bind the caller's exact owner + generation, so a stale prior
    owner is locked out once a newer generation has claimed the row

The db spy captures the exact SQL + params for every write, letting us
assert the fencing contract deterministically without a live database.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

import ap.order_state_machine as osm_mod
from ap.order_state_machine import APOrderStateMachine


# ─────────────────────────── DB spy scaffold ────────────────────────────


class _Cursor:
    def __init__(self, sink, rowcount=1):
        self.sink = sink
        self.rowcount = rowcount

    def execute(self, sql, params=()):
        # Normalise whitespace so SQL substring assertions are stable.
        self.sink.append((" ".join(str(sql).split()), tuple(params)))
        return self


class _Conn:
    def __init__(self, sink, rowcount=1):
        self.cursor = _Cursor(sink, rowcount=rowcount)
        self.rowcount = rowcount

    def __enter__(self):
        return self.cursor

    def __exit__(self, *_):
        return False


@pytest.fixture
def db_spy(monkeypatch):
    sink = []
    state = {"rowcount": 1}
    monkeypatch.setattr(
        osm_mod, "conn", lambda: _Conn(sink, rowcount=state["rowcount"]),
    )
    monkeypatch.setattr(osm_mod, "run_with_retry", lambda fn, *a, **k: fn())
    return sink, state


def _claim_kwargs(**overrides):
    base = {
        "owner": "materializer:worker-A",
        "lease_until": (datetime.now(timezone.utc) + timedelta(seconds=120)).isoformat(),
        "trigger_crossed_at": datetime.now(timezone.utc).isoformat(),
        "trigger_price": 100.0,
        "observed_underlying_price": 100.2,
        "signal_id": "sig-1",
        "execution_mode": "live",
        "canonical_signal_id": "sig-1",
    }
    base.update(overrides)
    return base


# ═══════════════════════════════════════════════════════════════════════
# Expected-previous generation binding (the core of §3)
# ═══════════════════════════════════════════════════════════════════════


def test_claim_binds_expected_previous_generation(db_spy):
    """new_generation=5 must bind 4 as the required persisted generation
    and write 5 into the patch."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    assert osm.claim_deferred_materialization(
        "oid-1", new_generation=5, **_claim_kwargs()
    ) is True
    sql, params = sink[-1]
    # Last bound param is the expected-previous generation.
    assert params[-1] == 4, "must bind new_generation - 1 as expected previous"
    patch = json.loads(params[0])
    assert patch["materialization_generation"] == 5


def test_claim_legacy_generation_alias_binds_expected_previous(db_spy):
    """generation=3 (legacy alias) is equivalent to new_generation=3:
    binds 2 as expected-previous, writes 3."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    assert osm.claim_deferred_materialization(
        "oid-1", generation=3, **_claim_kwargs()
    ) is True
    _, params = sink[-1]
    assert params[-1] == 2
    patch = json.loads(params[0])
    assert patch["materialization_generation"] == 3


def test_new_generation_takes_precedence_over_legacy(db_spy):
    """When both are supplied, new_generation wins."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    assert osm.claim_deferred_materialization(
        "oid-1", generation=9, new_generation=4, **_claim_kwargs()
    ) is True
    _, params = sink[-1]
    assert params[-1] == 3          # 4 - 1
    patch = json.loads(params[0])
    assert patch["materialization_generation"] == 4


def test_generation_one_binds_zero_expected_previous(db_spy):
    """The first claim (new_generation=1) requires persisted generation 0
    (the COALESCE default for a never-claimed row)."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    assert osm.claim_deferred_materialization(
        "oid-1", new_generation=1, **_claim_kwargs()
    ) is True
    _, params = sink[-1]
    assert params[-1] == 0


def test_monotonic_sequence_each_step_binds_prior(db_spy):
    """Advancing 1 → 2 → 3 must bind 0 → 1 → 2 respectively."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    for gen, expected_prev in [(1, 0), (2, 1), (3, 2)]:
        sink.clear()
        assert osm.claim_deferred_materialization(
            "oid-1", new_generation=gen, **_claim_kwargs()
        ) is True
        _, params = sink[-1]
        assert params[-1] == expected_prev


# ═══════════════════════════════════════════════════════════════════════
# Rejections and SQL contract
# ═══════════════════════════════════════════════════════════════════════


def test_claim_rejects_generation_below_one(db_spy):
    """new_generation < 1 is invalid — return False with no DB write."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    assert osm.claim_deferred_materialization(
        "oid-1", new_generation=0, **_claim_kwargs()
    ) is False
    assert sink == [], "no SQL may be issued for an invalid generation"


def test_claim_rejects_negative_generation(db_spy):
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    assert osm.claim_deferred_materialization(
        "oid-1", new_generation=-5, **_claim_kwargs()
    ) is False
    assert sink == []


def test_claim_sql_uses_exact_generation_equality_not_lte(db_spy):
    """The permissive predicate must be gone. The generation match must
    be exact equality; there must be no '<= ' comparison and no
    'generation','') = ''' escape hatch."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    osm.claim_deferred_materialization("oid-1", new_generation=2, **_claim_kwargs())
    sql, _ = sink[-1]
    assert "COALESCE((meta->>'materialization_generation')::int, 0) = %s" in sql
    assert "<=" not in sql, "stale-reclaim '<=' predicate must be removed"
    assert "materialization_generation','') = ''" not in sql, (
        "empty-generation escape hatch must be removed"
    )


# ═══════════════════════════════════════════════════════════════════════
# CAS miss = stale worker rejected
# ═══════════════════════════════════════════════════════════════════════


def test_claim_cas_miss_returns_false(db_spy):
    """When the DB reports rowcount 0 (the persisted generation did not
    equal new_generation - 1, i.e. another worker already advanced it),
    the claim fails."""
    _, state = db_spy
    state["rowcount"] = 0
    osm = APOrderStateMachine("client@example.com")
    assert osm.claim_deferred_materialization(
        "oid-1", new_generation=2, **_claim_kwargs()
    ) is False


def test_claim_cas_hit_returns_true(db_spy):
    _, state = db_spy
    state["rowcount"] = 1
    osm = APOrderStateMachine("client@example.com")
    assert osm.claim_deferred_materialization(
        "oid-1", new_generation=2, **_claim_kwargs()
    ) is True


def test_two_claims_same_prior_generation_bind_identical_expected_previous(db_spy):
    """Two workers racing from the same observed prior generation both
    bind the same expected-previous value; the DB's atomic UPDATE ...
    WHERE generation = expected_previous is what arbitrates (exactly one
    commits). This proves arbitration is delegated to the durable CAS,
    not to in-process state."""
    sink, _ = db_spy
    osm = APOrderStateMachine("client@example.com")
    # Worker A and worker B both computed new_generation=2 from persisted 1.
    osm.claim_deferred_materialization("oid-1", new_generation=2, **_claim_kwargs(owner="worker-A"))
    a_sql, a_params = sink[-1]
    osm.claim_deferred_materialization("oid-1", new_generation=2, **_claim_kwargs(owner="worker-B"))
    b_sql, b_params = sink[-1]
    assert a_params[-1] == b_params[-1] == 1
    # Both are single atomic UPDATEs carrying the generation predicate.
    assert a_sql == b_sql
    assert "materialization_generation')::int, 0) = %s" in a_sql


# ═══════════════════════════════════════════════════════════════════════
# Downstream fenced writes lock out a stale prior owner/generation
# ═══════════════════════════════════════════════════════════════════════


def test_stale_owner_copyback_blocked_by_generation_cas(db_spy):
    """persist_deferred_broker_ready binds the caller's exact
    owner+generation. A stale worker (generation 1) whose row has since
    advanced to generation 2 gets a CAS miss (rowcount 0) → False."""
    sink, state = db_spy
    state["rowcount"] = 0        # row already advanced beyond this owner
    osm = APOrderStateMachine("client@example.com")
    assert osm.persist_deferred_broker_ready(
        "oid-1",
        owner="materializer:worker-A",
        generation=1,
        signal_id="sig-1",
        execution_mode="live",
        contract="SPY260717C00600000",
        limit_price=1.25,
        qty=1,
        reserved_cost=125.0,
        selector_meta={},
    ) is False
    # Req 2 (P0/osm-broker-submit-proof-and-identity): on CAS miss the helper
    # now issues a diagnostic re-read SELECT after the UPDATE. The UPDATE is
    # therefore at sink[-2]; the SELECT is at sink[-1].
    sql, params = sink[-2]   # the fenced UPDATE (was sink[-1] before re-read)
    assert "COALESCE((meta->>'materialization_generation')::int, 0) = %s" in sql
    assert params[-1] == 1        # binds the stale caller's generation
    assert params[-2] == "materializer:worker-A"
    # Re-read SELECT must also have been issued
    reread_sql, _ = sink[-1]
    assert "SELECT" in reread_sql.upper() or "select" in reread_sql


def test_stale_owner_retry_schedule_blocked_by_generation_cas(db_spy):
    """schedule_deferred_materialization_retry is owner+generation fenced."""
    sink, state = db_spy
    state["rowcount"] = 0
    osm = APOrderStateMachine("client@example.com")
    assert osm.schedule_deferred_materialization_retry(
        "oid-1",
        owner="materializer:worker-A",
        generation=1,
        reason_code="SELECTOR_TRANSIENT",
        attempt=1,
        max_attempts=3,
        next_retry_at=(datetime.now(timezone.utc) + timedelta(seconds=20)).isoformat(),
        selector_failure={},
    ) is False
    sql, params = sink[-1]
    assert "COALESCE((meta->>'materialization_generation')::int, 0) = %s" in sql
    assert params[-1] == 1
    assert params[-2] == "materializer:worker-A"


def test_stale_owner_terminalize_blocked_by_generation_cas(db_spy):
    """terminalize_deferred_breach, when given owner+generation, is
    fenced identically."""
    sink, state = db_spy
    state["rowcount"] = 0
    osm = APOrderStateMachine("client@example.com")
    assert osm.terminalize_deferred_breach(
        "oid-1",
        reason_code="RECOVERY_TRIGGER_TOO_OLD",
        terminal_status="EXPIRED",
        owner="materializer:worker-A",
        generation=1,
        diagnostics={"src": "test"},
    ) is False
    sql, params = sink[-1]
    assert "COALESCE((meta->>'materialization_generation')::int, 0) = %s" in sql
    assert params[-1] == 1
    assert params[-2] == "materializer:worker-A"


def test_current_owner_write_succeeds_when_cas_matches(db_spy):
    """Sanity: the same downstream write succeeds (rowcount 1) when the
    generation matches — proving the block above is the CAS, not a
    structural rejection."""
    _, state = db_spy
    state["rowcount"] = 1
    osm = APOrderStateMachine("client@example.com")
    assert osm.persist_deferred_broker_ready(
        "oid-1",
        owner="materializer:worker-B",
        generation=2,
        signal_id="sig-1",
        execution_mode="live",
        contract="SPY260717C00600000",
        limit_price=1.25,
        qty=1,
        reserved_cost=125.0,
        selector_meta={},
    ) is True
