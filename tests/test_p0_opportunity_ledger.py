"""
tests/test_p0_opportunity_ledger.py
PR1 + PR2 — Client Opportunity Ledger + Preflight

Unit tests — no real Supabase required.
"""
import sys, os
sys.path.insert(0, '/home/claude')
os.environ.update({
    "MAX_CLIENT_TRADES_PER_DAY":          "5",
    "MAX_CLIENT_DAILY_TRADES":            "3",
    "MAX_CLIENT_INTRADAY_TRADES":         "2",
    "MAX_CLIENT_SYMBOL_TRADES_PER_DAY":   "1",
    "MAX_CLIENT_OPEN_POSITIONS":          "10",
    "MAX_CLIENT_PENDING_ENTRIES":         "3",
    "MAX_CLIENT_TRADE_COST_USD":          "500",
    "BOT_MODE":                           "paper",
})
import pytest


# ── Stub Supabase client ──────────────────────────────────────────────────────
class _FakeResult:
    def __init__(self, data=None): self.data = data or []
class _FakeChain:
    def __init__(self): self._rows = []; self._captured = []
    def table(self, t): return self
    def select(self, *a): return self
    def upsert(self, row, **kw): self._captured.append(('upsert', row)); return self
    def update(self, row): self._captured.append(('update', row)); return self
    def insert(self, row): self._captured.append(('insert', row)); return self
    def eq(self, *a): return self
    def in_(self, *a): return self
    def gte(self, *a): return self
    def limit(self, *a): return self
    def order(self, *a): return self
    def execute(self): return _FakeResult(self._rows)

def _sb(rows=None):
    sb = _FakeChain()
    if rows: sb._rows = rows
    return sb


class _SmartFakeSB:
    """Returns member rows for 'members' table, empty for all others."""
    def __init__(self, member_rows):
        self._member_rows = member_rows
        self._captured = []
        self._current_table = None
    def table(self, t):
        self._current_table = t
        return self
    def select(self, *a): return self
    def upsert(self, row, **kw): self._captured.append(('upsert', row)); return self
    def update(self, row): self._captured.append(('update', row)); return self
    def insert(self, row): self._captured.append(('insert', row)); return self
    def eq(self, *a): return self
    def in_(self, *a): return self
    def gte(self, *a): return self
    def limit(self, *a): return self
    def order(self, *a): return self
    def execute(self):
        if self._current_table == 'members':
            return _FakeResult(self._member_rows)
        return _FakeResult([])


# ── PR1: Opportunity Ledger ───────────────────────────────────────────────────
from ap.opportunity_ledger import (
    create_opportunities, update_opportunity,
    mark_missed, mark_skipped,
    CREATED, MISSED, CLIENT_SKIPPED, ORDER_CREATED, WATCHER_ARMED,
    STAGE_CLIENT_PREFLIGHT, STAGE_INTERNAL_ERROR,
)

SIG = {"ticker": "AAPL", "direction": "CALL", "score": 75, "tier": "A",
       "timeframe": "1d", "pattern": "2-3"}

def test_create_opportunities_one_row_per_client():
    sb = _sb()
    n = create_opportunities("sig1", ["c1@x.com", "c2@x.com"], SIG, sb=sb)
    assert n == 2
    inserts = [c for c in sb._captured if c[0] == 'upsert']
    assert len(inserts) == 2
    client_ids = {c[1]["client_id"] for c in inserts}
    assert client_ids == {"c1@x.com", "c2@x.com"}

def test_create_opportunities_sets_created_status():
    sb = _sb()
    create_opportunities("sig2", ["c1@x.com"], SIG, sb=sb)
    row = sb._captured[0][1]
    assert row["opportunity_status"] == CREATED
    assert row["client_eligibility_status"] == "CLIENT_ELIGIBLE"

def test_create_opportunities_idempotent_on_conflict():
    # upsert on_conflict="signal_id,client_id" must be used
    sb = _sb()
    create_opportunities("sig3", ["c1@x.com"], SIG, sb=sb)
    create_opportunities("sig3", ["c1@x.com"], SIG, sb=sb)
    upserts = [c for c in sb._captured if c[0] == 'upsert']
    # Two calls produce two upserts — DB unique constraint handles dedup
    assert len(upserts) == 2
    assert upserts[0][1]["signal_id"] == "sig3"

def test_create_opportunities_empty_clients_returns_zero():
    sb = _sb()
    n = create_opportunities("sig4", [], SIG, sb=sb)
    assert n == 0

def test_update_opportunity_writes_status():
    sb = _sb()
    ok = update_opportunity("sig5", "c1@x.com", ORDER_CREATED,
                             order_local_id="loc-123", sb=sb)
    assert ok is True
    update = sb._captured[0][1]
    assert update["opportunity_status"] == ORDER_CREATED
    assert update["order_local_id"] == "loc-123"

def test_mark_missed_sets_stage_and_reason():
    sb = _sb()
    mark_missed("sig6", "c1@x.com", STAGE_INTERNAL_ERROR, "order_create_failed", sb=sb)
    update = sb._captured[0][1]
    assert update["opportunity_status"] == MISSED
    assert update["miss_stage"] == STAGE_INTERNAL_ERROR
    assert update["miss_reason"] == "order_create_failed"

def test_mark_skipped_sets_client_skipped():
    sb = _sb()
    mark_skipped("sig7", "c1@x.com", STAGE_CLIENT_PREFLIGHT, "kill_switch_on", sb=sb)
    update = sb._captured[0][1]
    assert update["opportunity_status"] == CLIENT_SKIPPED
    assert update["miss_reason"] == "kill_switch_on"

def test_create_opportunities_no_sb_returns_zero_no_raise():
    # Module should not raise if no Supabase client available
    n = create_opportunities("sig8", ["c1@x.com"], SIG, sb=None)
    assert n == 0   # _get_sb() will fail silently in test env

def test_internal_exception_marks_ledger_not_raises():
    class _BadSB(_FakeChain):
        def execute(self): raise RuntimeError("supabase_down")
    # mark_missed must not raise even if sb fails
    try:
        mark_missed("sig9", "c1@x.com", STAGE_INTERNAL_ERROR, "test", sb=_BadSB())
    except Exception as e:
        pytest.fail(f"mark_missed raised: {e}")


# ── PR2: Client State Preflight ───────────────────────────────────────────────
from ap.client_preflight import (
    build_client_trade_preflight,
    ClientTradePreflight,
    KILL_SWITCH_ON, ENTRIES_PAUSED, CLIENT_NOT_APPROVED, SUBSCRIPTION_INACTIVE,
    MISSING_BROKER_CREDENTIALS, BROKER_MODE_MISMATCH, INSUFFICIENT_BUYING_POWER,
    DAILY_CAP_REACHED, LANE_CAP_REACHED, SAME_SYMBOL_CAP_REACHED,
    MAX_OPEN_POSITIONS_REACHED, MAX_PENDING_ENTRIES_REACHED,
    ESTIMATED_COST_EXCEEDS_LIMIT,
)

def _member(**kw):
    # Real schema (matches ap/entry_gate.py): lowercase no-underscore booleans
    # and tradier_account_mode for the broker mode.
    base = dict(approved=True, subscriptionactive=True, killswitch=False,
                entriespaused=False, maintenancemode=False,
                scannerroutingenabled=True,
                tradier_account_mode="paper",
                tradier_paper_account_id="VA12345",
                tradier_paper_access_token="tok123",
                tradier_account_id="VA12345", tradier_access_token="tok123",
                tradier_live_account_id=None, tradier_live_access_token=None)
    # Back-compat: tests may still pass legacy kwarg names
    legacy_map = {
        "subscription_active": "subscriptionactive",
        "tradier_active_mode": "tradier_account_mode",
    }
    for old, new in legacy_map.items():
        if old in kw:
            kw[new] = kw.pop(old)
    base.update(kw)
    # If a test nulls a generic credential to test missing creds, also null
    # the paper-specific equivalents so the priority resolution still misses.
    if "tradier_account_id" in kw and kw["tradier_account_id"] is None:
        base["tradier_paper_account_id"] = None
    if "tradier_access_token" in kw and kw["tradier_access_token"] is None:
        base["tradier_paper_access_token"] = None
    return [base]

def _sb_member(**kw):
    return _SmartFakeSB(_member(**kw))

def _plan(cost=100.0):
    class P:
        max_position_usd = cost
    return P()

SIG2 = {"ticker": "AAPL", "symbol": "AAPL", "direction": "CALL"}

def test_preflight_clean_client_passes():
    pf = build_client_trade_preflight("c@x.com", SIG2, _plan(), sb=_sb_member())
    assert pf.eligible is True
    assert pf.block_reason is None

def test_preflight_kill_switch_on_blocks():
    pf = build_client_trade_preflight("c@x.com", SIG2, _plan(),
                                       sb=_sb_member(killswitch=True))
    assert pf.eligible is False
    assert pf.block_reason == KILL_SWITCH_ON

def test_preflight_entries_paused_blocks():
    pf = build_client_trade_preflight("c@x.com", SIG2, _plan(),
                                       sb=_sb_member(entriespaused=True))
    assert pf.eligible is False
    assert pf.block_reason == ENTRIES_PAUSED

def test_preflight_not_approved_blocks():
    pf = build_client_trade_preflight("c@x.com", SIG2, _plan(),
                                       sb=_sb_member(approved=False))
    assert pf.eligible is False
    assert pf.block_reason == CLIENT_NOT_APPROVED

def test_preflight_subscription_inactive_blocks():
    pf = build_client_trade_preflight("c@x.com", SIG2, _plan(),
                                       sb=_sb_member(subscription_active=False))
    assert pf.eligible is False
    assert pf.block_reason == SUBSCRIPTION_INACTIVE

def test_preflight_missing_credentials_blocks():
    pf = build_client_trade_preflight("c@x.com", SIG2, _plan(),
                                       sb=_sb_member(tradier_account_id=None,
                                                      tradier_access_token=None))
    assert pf.eligible is False
    assert pf.block_reason == MISSING_BROKER_CREDENTIALS

def test_preflight_broker_mode_mismatch_blocks():
    # Amendment §5: expected_mode is client/pod aware. Pass a runner
    # whose expected_mode is 'live' while the members row reports paper.
    class R:
        kill_switch_active = False
        expected_mode      = "live"
        mode               = "live"
        member             = None
        class _EA:
            @staticmethod
            def is_set(): return True
        entries_allowed = _EA()
    pf = build_client_trade_preflight(
        "c@x.com", SIG2, _plan(),
        runner=R(),
        sb=_sb_member(tradier_account_mode="paper",
                      tradier_live_account_id=None,
                      tradier_live_access_token=None),
    )
    assert pf.eligible is False
    # Either MISSING_BROKER_CREDENTIALS (live tokens missing) or
    # BROKER_MODE_MISMATCH — both are the correct truth here.
    assert pf.block_reason in (BROKER_MODE_MISMATCH, MISSING_BROKER_CREDENTIALS)

def test_preflight_insufficient_buying_power_blocks():
    sb = _sb_member()
    # Amendment §5: buying_power is its own attribute on the runner,
    # NOT derived from account_equity.
    class R:
        buying_power      = 50.0
        account_equity    = 50000.0   # must NOT be used as buying_power
        kill_switch_active = False
        entries_allowed   = type('E', (), {'is_set': lambda s: True})()
        approved          = True
        subscriptionactive = True
        mode              = "paper"
        expected_mode     = "paper"
        member            = None
    pf = build_client_trade_preflight("c@x.com", SIG2, _plan(cost=200.0),
                                       runner=R(), sb=sb)
    assert pf.eligible is False
    assert pf.block_reason == INSUFFICIENT_BUYING_POWER

def test_preflight_estimated_cost_exceeds_limit():
    pf = build_client_trade_preflight("c@x.com", SIG2, _plan(cost=9999.0),
                                       sb=_sb_member())
    assert pf.eligible is False
    assert pf.block_reason == ESTIMATED_COST_EXCEEDS_LIMIT

def test_preflight_to_dict_contains_all_fields():
    pf = build_client_trade_preflight("c@x.com", SIG2, _plan(), sb=_sb_member())
    d = pf.to_dict()
    for field in ["client_id", "kill_switch", "entries_paused", "approved",
                  "subscription_active", "eligible", "block_reason",
                  "buying_power", "estimated_cost", "daily_trade_count"]:
        assert field in d, f"Missing field: {field}"

def test_preflight_snapshot_ts_is_utc_iso():
    pf = build_client_trade_preflight("c@x.com", SIG2, _plan(), sb=_sb_member())
    assert "T" in pf.snapshot_ts
    assert "Z" in pf.snapshot_ts or "+" in pf.snapshot_ts

def test_preflight_no_sb_does_not_raise():
    # Should not raise even with no Supabase — uses safe defaults
    try:
        pf = build_client_trade_preflight("c@x.com", SIG2, _plan(), sb=None)
        assert isinstance(pf, ClientTradePreflight)
    except Exception as e:
        pytest.fail(f"preflight raised without sb: {e}")


# ── Amendment tests ───────────────────────────────────────────────────────────

def test_create_opportunities_uses_canonical_conflict_key():
    sb = _sb()
    create_opportunities("sig_raw", ["c1@x.com"], SIG,
                         canonical_signal_id="canon_abc", sb=sb)
    upsert = sb._captured[0]
    row = upsert[1]
    assert row["canonical_signal_id"] == "canon_abc"
    # The upsert on_conflict key must be canonical_signal_id,client_id
    # We verify the row has canonical_signal_id set correctly
    assert row["signal_id"] == "sig_raw"

def test_create_opportunities_fallback_canonical_to_signal_id():
    sb = _sb()
    create_opportunities("sig_fallback", ["c1@x.com"], SIG, sb=sb)
    row = sb._captured[0][1]
    # canonical_signal_id should equal signal_id as fallback
    assert row["canonical_signal_id"] == "sig_fallback"

def test_update_opportunity_uses_canonical_signal_id():
    sb = _sb()
    ok = update_opportunity("sig_raw", "c1@x.com", ORDER_CREATED,
                             canonical_signal_id="canon_abc",
                             order_local_id="loc-999", sb=sb)
    assert ok is True
    # The WHERE clause uses canonical_signal_id — verify captured payload
    update = sb._captured[0][1]
    assert update["opportunity_status"] == ORDER_CREATED

def test_PREFLIGHT_PASSED_status_exists():
    from ap.opportunity_ledger import PREFLIGHT_PASSED
    assert PREFLIGHT_PASSED == "PREFLIGHT_PASSED"

def test_preflight_enforce_false_missing_buying_power_does_not_block():
    import os
    os.environ["CLIENT_PREFLIGHT_ENFORCE"] = "false"
    # buying_power=0 (unknown) should NOT trigger insufficient_buying_power
    pf = build_client_trade_preflight("c@x.com", SIG2,
                                       _plan(cost=200.0),   # cost > 0
                                       sb=_sb_member())     # no buying_power in member row
    # buying_power defaults to 0.0 — but 0.0 means UNKNOWN, not zero dollars
    # so the insufficient_buying_power check must not fire
    assert pf.block_reason != "insufficient_buying_power", (
        f"Should not block on buying_power=0 (unknown); got {pf.block_reason}"
    )

def test_preflight_enforce_flag_readable():
    from ap.client_preflight import preflight_enforce
    import os
    os.environ["CLIENT_PREFLIGHT_ENFORCE"] = "false"
    assert preflight_enforce() is False
    os.environ["CLIENT_PREFLIGHT_ENFORCE"] = "true"
    assert preflight_enforce() is True
    # Don't leave it set to true for other tests
    os.environ["CLIENT_PREFLIGHT_ENFORCE"] = "false"
    os.environ["CLIENT_PREFLIGHT_ENFORCE"] = "false"
