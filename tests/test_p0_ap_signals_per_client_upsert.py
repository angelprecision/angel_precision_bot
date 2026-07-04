"""
P0 (PR #260): ap_signals per-client identity.

ap_signals was keyed by signal_id ALONE: multi-client fanout produced
last-writer-wins row theft (client B's upsert overwrote client A's
client_email) and cross-client suppression (one client's decision_status
update pulled the shared row out of WATCHING for everyone's reeval).

These tests drive SignalStore against an in-memory fake Supabase table that
enforces the NEW composite key (signal_id, client_email), proving:
  - same signal_id for two clients → two rows
  - updating one client's row never mutates the other's
  - duplicate upsert from the same client is idempotent (updates own row)
  - lifecycle updates are client-scoped
  - ownerless stores use the '__shared__' sentinel
  - legacy fallback path exists for pre-migration databases
"""

import pathlib
import re
import time

from ap_signal_store import APSignalStore as SignalStore

_REPO = pathlib.Path(__file__).resolve().parents[1]
SRC = (_REPO / "ap_signal_store.py").read_text()


# ── In-memory Supabase fake enforcing the composite key ─────────────────────

class _FakeTable:
    def __init__(self, store):
        self.store = store          # dict[(signal_id, client_email)] = row
        self._pending = None
        self._filters = []

    # upsert / update builders
    def upsert(self, payload, on_conflict=None):
        if on_conflict is not None and on_conflict != "signal_id,client_email":
            raise Exception(f"42P10 no unique constraint matching {on_conflict}")
        self._pending = ("upsert", dict(payload), on_conflict)
        return self

    def update(self, patch):
        self._pending = ("update", dict(patch), None)
        return self

    def eq(self, col, val):
        self._filters.append((col, val))
        return self

    def execute(self):
        op, payload, on_conflict = self._pending
        if op == "upsert":
            if on_conflict == "signal_id,client_email":
                key = (payload["signal_id"], payload["client_email"])
            else:
                # legacy single-key semantics: signal_id collides across clients
                key = (payload["signal_id"], "__LEGACY__")
            self.store[key] = {**self.store.get(key, {}), **payload}
        else:
            for key, row in self.store.items():
                if all(row.get(c) == v or key[0] == v and c == "signal_id"
                       for c, v in self._filters):
                    matched = True
                    for c, v in self._filters:
                        if c == "signal_id" and key[0] != v:
                            matched = False
                        elif c == "client_email" and key[1] != v:
                            matched = False
                    if matched:
                        row.update(payload)
        self._pending, self._filters = None, []
        return type("R", (), {"data": []})()


class _FakeSB:
    def __init__(self):
        self.rows = {}

    def table(self, name):
        assert name == "ap_signals"
        return _FakeTable(self.rows)


def _store(email):
    st = SignalStore.__new__(SignalStore)
    st.client_email = email
    st.system_version = "test"
    st.sb = _FakeSB()
    st._enqueue = lambda sid, label, fn: fn()   # synchronous for tests
    return st


def _shared_sb(*stores):
    sb = _FakeSB()
    for st in stores:
        st.sb = sb
    return sb


SIG = "11111111-1111-1111-1111-111111111111"


# ── Spec 3: shared scanner signal creates separate client rows ──────────────

def test_same_signal_id_two_clients_two_rows():
    jason, jose = _store("jason@x.com"), _store("jose@x.com")
    sb = _shared_sb(jason, jose)
    jason.insert_signal(SIG, {"ticker": "SPY", "side": "CALL"})
    jose.insert_signal(SIG, {"ticker": "SPY", "side": "CALL"})
    assert (SIG, "jason@x.com") in sb.rows
    assert (SIG, "jose@x.com") in sb.rows
    assert len(sb.rows) == 2


# ── Spec 4: status updates only affect the correct client row ───────────────

def test_update_one_client_never_mutates_the_other():
    jason, jose = _store("jason@x.com"), _store("jose@x.com")
    sb = _shared_sb(jason, jose)
    jason.insert_signal(SIG, {"ticker": "SPY"})
    jose.insert_signal(SIG, {"ticker": "SPY"})
    jason.update_status(SIG, "executed", timestamp_flag="executed_at")
    assert sb.rows[(SIG, "jason@x.com")]["decision_status"] == "executed"
    assert sb.rows[(SIG, "jose@x.com")].get("decision_status") == "received"
    assert "executed_at" not in sb.rows[(SIG, "jose@x.com")]


def test_update_signal_fields_is_client_scoped():
    jason, jose = _store("jason@x.com"), _store("jose@x.com")
    sb = _shared_sb(jason, jose)
    jason.insert_signal(SIG, {"ticker": "SPY"})
    jose.insert_signal(SIG, {"ticker": "SPY"})
    jose.update_signal_fields(SIG, {"context_notes": "jose-only"})
    assert sb.rows[(SIG, "jose@x.com")]["context_notes"] == "jose-only"
    assert sb.rows[(SIG, "jason@x.com")].get("context_notes") != "jose-only"


# ── Spec 5: idempotency within the same client ───────────────────────────────

def test_duplicate_same_client_upsert_updates_own_row_only():
    jason, jose = _store("jason@x.com"), _store("jose@x.com")
    sb = _shared_sb(jason, jose)
    jason.insert_signal(SIG, {"ticker": "SPY", "score": 60})
    jose.insert_signal(SIG, {"ticker": "SPY", "score": 60})
    jason.insert_signal(SIG, {"ticker": "SPY", "score": 71})
    assert len(sb.rows) == 2
    assert sb.rows[(SIG, "jason@x.com")]["score"] == 71
    assert sb.rows[(SIG, "jose@x.com")]["score"] == 60


# ── Sentinel + fallback ──────────────────────────────────────────────────────

def test_ownerless_store_uses_shared_sentinel():
    anon = _store(None)
    sb = _shared_sb(anon)
    anon.insert_signal(SIG, {"ticker": "SPY"})
    assert (SIG, "__shared__") in sb.rows


def test_ownerless_updates_only_touch_shared_row():
    anon, jason = _store(None), _store("jason@x.com")
    sb = _shared_sb(anon, jason)
    anon.insert_signal(SIG, {"ticker": "SPY"})
    jason.insert_signal(SIG, {"ticker": "SPY"})
    anon.update_status(SIG, "expired", timestamp_flag="expired_at")
    assert sb.rows[(SIG, "__shared__")]["decision_status"] == "expired"
    assert sb.rows[(SIG, "jason@x.com")]["decision_status"] == "received"


def test_legacy_fallback_exists_and_logs_missing_migration():
    assert 'on_conflict="signal_id,client_email"' in SRC
    assert "AP_SIGNALS_PER_CLIENT_KEY_MISSING" in SRC
    m = re.search(r"def upsert_ap_signal_row_with_fallback.*?sb\.table\(\"ap_signals\"\)\.upsert\(p\)\.execute\(\)", SRC, re.S)
    assert m, "legacy single-key fallback must remain inside the exception path"


def test_repo_wide_ap_signals_writers_use_composite_key_path():
    queue_src = (_REPO / "ap" / "queue.py").read_text()
    overnight_src = (_REPO / "ap_overnight_reeval.py").read_text()

    assert 'on_conflict="signal_id"' not in queue_src
    assert 'on_conflict="signal_id"' not in overnight_src
    assert 'upsert_ap_signal_row_with_fallback' in queue_src
    assert 'upsert_ap_signal_row_with_fallback' in overnight_src
    assert 'canonical_client_email(client_id)' in queue_src
    assert 'canonical_client_email(client_id)' in overnight_src


# ── Migration file contract ──────────────────────────────────────────────────

def test_migration_is_idempotent_and_fk_aware():
    mig = (_REPO / "migrations" / "2026_07_02_ap_signals_per_client_key.sql").read_text()
    assert "PRIMARY KEY (signal_id, client_email)" in mig
    assert mig.count("DROP CONSTRAINT IF EXISTS") == 3
    assert "CREATE INDEX IF NOT EXISTS" in mig
    assert "'__shared__'" in mig
    assert "already present — no-op" in mig       # idempotent guard
    assert "manual review required" in mig         # unexpected-PK guard
    assert "ROLLBACK PLAN" in mig
