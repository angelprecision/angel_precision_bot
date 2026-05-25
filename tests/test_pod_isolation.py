"""
Patch 6 — Pod-isolation tests for client_runner._fetch_active_members.

These tests are STRICTLY READ-ONLY against the source. Zero runtime
changes ship in this PR. If a test reveals unsafe behavior in
client_runner.py, the failing test is reported as a finding and a
SEPARATE PR fixes the runtime; we do NOT patch runtime inside this PR.

Coverage (≥18 distinct test IDs):

  SINGLE_CLIENT_EMAIL behavior (4)
    - canonical SINGLE_CLIENT_EMAIL filters to exactly one client
    - generic CLIENT_ID is honored ONLY when it looks like an email
    - generic CLIENT_ID=default is NOT treated as single-client mode
    - SINGLE_CLIENT_EMAIL with zero matching members hard-exits boot (code 5)

  POD_ID filtering (4)
    - POD_ID=POD_A returns only execution_pod=POD_A members
    - POD_A / POD_B / POD_C see ZERO cross-pod members (no contamination)
    - POD mode with zero eligible members hard-exits boot (code 5)
    - POD mode + missing execution_pod column hard-exits boot (code 6)
      when no runners are already active

  Shared / live guardrails (3)
    - BOT_MODE=live + no SINGLE_CLIENT_EMAIL + no POD_ID + no
      ALLOW_SHARED_LIVE hard-exits boot (code 4)
    - BOT_MODE=live + ALLOW_SHARED_LIVE=1 permits shared mode
    - BOT_MODE=paper shared mode is allowed without ALLOW_SHARED_LIVE

  allow_live_trading guardrail (2)
    - LIVE drops a member with allow_live_trading=false
    - LIVE keeps a member with allow_live_trading=true
      (provided a client_risk_profiles row exists)

  MAX_POD_CLIENTS overflow (2)
    - With no already-active runners, _max_pod=2 keeps first 2 admitted
    - Already-active runners are NEVER displaced; new clients only fill
      remaining slots

  Schema fallback safety (2)
    - Missing execution_pod column in SHARED PAPER falls back safely
      (no hard-exit, members loaded)
    - Missing allow_live_trading column in LIVE = SAFE-default
      (allow_live_trading=False → all members dropped) but does NOT crash

  Runtime / transient empty fetch (1)
    - SINGLE or POD mode with zero members BUT active runners present
      does NOT hard-exit (mid-day transient empty fetch protection)

Total: 18 test IDs across 6 test classes.

Run:
    pytest tests/test_pod_isolation.py -v
"""
from __future__ import annotations

import os
import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# Set env BEFORE importing client_runner so module-level reads succeed.
os.environ.setdefault(
    "DATABASE_URL",
    "postgresql://test:test@127.0.0.1:5432/test_pod_isolation",
)
os.environ.setdefault("ENCRYPTION_KEY", "angel-test-key-for-pod-isolation-2026")

# Stub supabase so client_runner imports cleanly.
if "supabase" not in sys.modules:
    _supa_stub = types.ModuleType("supabase")
    _supa_stub.create_client = lambda *a, **kw: None
    _supa_stub.Client = type("Client", (), {})
    sys.modules["supabase"] = _supa_stub

# NOTE on module pollution:
# An earlier test in the full suite (test_repeg_resubmit.py) installs a
# partial ap.db stub in sys.modules that lacks `run_with_retry`. If our
# `import client_runner` runs after that, it fails with ImportError.
# We do NOT mutate sys.modules at module-load here because that would
# also affect tests that run AFTER us (we observed that doing so
# converts pre-existing pollution-driven ERRORs into FAILUREs in
# downstream tests, which is a side effect we refuse to ship).
# Instead, client_runner is imported lazily inside a fixture that
# patches the missing names on whatever ap.db happens to be installed,
# then RESTORES the original state after each test session via
# pytest's monkeypatch teardown.


# ────────────────────────────────────────────────────────────────────
# Fake Supabase chain
# ────────────────────────────────────────────────────────────────────

class _FakeResult:
    def __init__(self, data):
        self.data = data


class _FakeQuery:
    """Mimics the supabase-py chainable query builder."""

    def __init__(self, rows, *, raise_on_select_cols=None):
        self._rows = list(rows)
        self._raise_on_select_cols = raise_on_select_cols  # tuple of substrings
        self._selected = None

    def select(self, cols):
        self._selected = cols
        # If select() was called with a column that the schema rejects,
        # raise when execute() is invoked.
        return self

    def eq(self, col, val):
        if col == "email":
            self._rows = [r for r in self._rows if r.get("email") == val]
        elif col == "execution_pod":
            self._rows = [r for r in self._rows if r.get("execution_pod") == val]
        elif col == "approved":
            self._rows = [r for r in self._rows if bool(r.get("approved")) == bool(val)]
        elif col == "subscription_active":
            self._rows = [
                r for r in self._rows if bool(r.get("subscription_active")) == bool(val)
            ]
        elif col == "tradier_account_mode":
            self._rows = [r for r in self._rows if r.get("tradier_account_mode") == val]
        return self

    def in_(self, col, vals):
        self._rows = [r for r in self._rows if r.get(col) in set(vals)]
        return self

    def execute(self):
        if self._raise_on_select_cols and self._selected:
            for needle in self._raise_on_select_cols:
                if needle in self._selected:
                    # Mimic the kind of Postgres error message
                    # client_runner._fetch_active_members keys off ("column").
                    raise RuntimeError(
                        f"column \"{needle}\" does not exist on table members"
                    )
        # Strip columns the caller did not request, to mimic supabase select().
        if self._selected:
            wanted = [c.strip() for c in self._selected.split(",")]
            rows_out = [{k: r.get(k) for k in wanted} for r in self._rows]
        else:
            rows_out = list(self._rows)
        return _FakeResult(rows_out)


class FakeSupabase:
    """Drop-in replacement for the supabase Client.

    Tests build one with a {table_name: rows} mapping and optional
    raise_on_select_cols to simulate a missing column.
    """

    def __init__(self, tables, *, raise_on_select_cols=None):
        # tables: dict[str, list[dict]]
        self._tables = tables
        self._raise = raise_on_select_cols

    def table(self, name):
        rows = self._tables.get(name, [])
        # Only simulate the missing-column raise on the members table —
        # client_risk_profiles is a separate query.
        raise_on = self._raise if name == "members" else None
        return _FakeQuery(rows, raise_on_select_cols=raise_on)


# ────────────────────────────────────────────────────────────────────
# Fixtures
# ────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def client_runner_module():
    # Ensure ap.db carries the names client_runner needs to import.
    # Use module-scope so the patch is applied once and torn down after
    # all tests in THIS file are done — never leaking into later files.
    _apdb = sys.modules.get("ap.db")
    _added = []
    if _apdb is not None:
        if not hasattr(_apdb, "run_with_retry"):
            def _stub_run_with_retry(fn, *args, **kwargs):  # pragma: no cover
                return fn(*args, **kwargs)
            _apdb.run_with_retry = _stub_run_with_retry  # type: ignore[attr-defined]
            _added.append("run_with_retry")
        if not hasattr(_apdb, "conn"):
            _apdb.conn = lambda *a, **kw: None  # type: ignore[attr-defined]
            _added.append("conn")
    import client_runner as _cr  # noqa: E402
    yield _cr
    # Teardown: remove only the attributes we added; never touch ones
    # the polluting stub or the real module already provided.
    if _apdb is not None:
        for name in _added:
            try:
                delattr(_apdb, name)
            except AttributeError:
                pass


@pytest.fixture
def clean_runtime(monkeypatch, client_runner_module):
    """Patch os._exit on the client_runner module so guardrails raise
    SystemExit(code) instead of killing the test process. Also reset
    _active_runners to empty between tests.
    """
    def _raising_exit(code):
        raise SystemExit(code)

    # client_runner does `import os` then calls `os._exit(...)` — patching
    # the os module's _exit attribute reaches client_runner's reference too.
    monkeypatch.setattr(client_runner_module.os, "_exit", _raising_exit)
    monkeypatch.setattr(client_runner_module, "_active_runners", {}, raising=False)

    # Clear all pod-related env each test so a leak doesn't contaminate.
    for k in (
        "SINGLE_CLIENT_EMAIL",
        "CLIENT_ID",
        "POD_ID",
        "MAX_POD_CLIENTS",
        "ALLOW_SHARED_LIVE",
        "BOT_MODE",
        "MODE",
    ):
        monkeypatch.delenv(k, raising=False)
    yield


# ────────────────────────────────────────────────────────────────────
# Sample data builders
# ────────────────────────────────────────────────────────────────────

def _member(email, *, pod=None, allow_live=True, mode="paper"):
    return {
        "id": email.split("@")[0],
        "email": email,
        "name": email.split("@")[0].title(),
        "tier": "B",
        "tradier_account_mode": mode,
        "tradier_account_id": "TEST123",
        "tradier_access_token": "tok",
        "tradier_base_url": "https://sandbox.tradier.com",
        "tradier_paper_account_id": "P123",
        "tradier_paper_access_token": "ptok",
        "tradier_live_account_id": "L123",
        "tradier_live_access_token": "ltok",
        "subscription_active": True,
        "approved": True,
        "allow_live_trading": allow_live,
        "execution_pod": pod,
    }


def _risk_profiles_for(*emails):
    return [{"client_email": e} for e in emails]


# ════════════════════════════════════════════════════════════════════
# 1) SINGLE_CLIENT_EMAIL behavior
# ════════════════════════════════════════════════════════════════════

class TestSingleClientEmail:
    def test_single_client_email_filters_exactly_one(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        monkeypatch.setenv("BOT_MODE", "paper")
        monkeypatch.setenv("SINGLE_CLIENT_EMAIL", "alice@x.com")
        sb = FakeSupabase({
            "members": [
                _member("alice@x.com"),
                _member("bob@x.com"),
                _member("carol@x.com"),
            ]
        })
        result = client_runner_module._fetch_active_members(sb)
        emails = sorted(m["email"] for m in result)
        assert emails == ["alice@x.com"], (
            f"SINGLE_CLIENT_EMAIL must isolate to exactly one client, got {emails}"
        )

    def test_legacy_client_id_email_promoted_to_single(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        monkeypatch.setenv("BOT_MODE", "paper")
        monkeypatch.setenv("CLIENT_ID", "alice@x.com")
        sb = FakeSupabase({
            "members": [_member("alice@x.com"), _member("bob@x.com")]
        })
        result = client_runner_module._fetch_active_members(sb)
        assert [m["email"] for m in result] == ["alice@x.com"]

    def test_legacy_client_id_non_email_is_not_single_mode(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """CLIENT_ID=default must NOT collapse to single-client mode.
        BOT_MODE=paper + no SINGLE_CLIENT_EMAIL + no POD_ID = shared
        mode (allowed in paper); all approved members should load.
        """
        monkeypatch.setenv("BOT_MODE", "paper")
        monkeypatch.setenv("CLIENT_ID", "default")
        sb = FakeSupabase({
            "members": [_member("alice@x.com"), _member("bob@x.com")]
        })
        result = client_runner_module._fetch_active_members(sb)
        emails = sorted(m["email"] for m in result)
        assert emails == ["alice@x.com", "bob@x.com"], (
            "CLIENT_ID=default must not be treated as single-client; "
            "both members must load in paper shared mode"
        )

    def test_single_with_zero_matches_hard_exits(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        monkeypatch.setenv("BOT_MODE", "paper")
        monkeypatch.setenv("SINGLE_CLIENT_EMAIL", "ghost@x.com")
        sb = FakeSupabase({"members": [_member("alice@x.com")]})
        with pytest.raises(SystemExit) as exc:
            client_runner_module._fetch_active_members(sb)
        assert exc.value.code == 5, (
            f"SINGLE_CLIENT_EMAIL with zero matches must os._exit(5), got {exc.value.code}"
        )


# ════════════════════════════════════════════════════════════════════
# 2) POD_ID filtering & cross-contamination
# ════════════════════════════════════════════════════════════════════

class TestPodFiltering:
    def test_pod_id_filters_to_pod_members_only(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        monkeypatch.setenv("BOT_MODE", "paper")
        monkeypatch.setenv("POD_ID", "POD_A")
        monkeypatch.setenv("MAX_POD_CLIENTS", "5")
        sb = FakeSupabase({
            "members": [
                _member("a1@x.com", pod="POD_A"),
                _member("a2@x.com", pod="POD_A"),
                _member("b1@x.com", pod="POD_B"),
                _member("c1@x.com", pod="POD_C"),
            ]
        })
        result = client_runner_module._fetch_active_members(sb)
        emails = sorted(m["email"] for m in result)
        assert emails == ["a1@x.com", "a2@x.com"], (
            f"POD_A must load only POD_A members, got {emails}"
        )

    def test_pods_a_b_c_have_no_cross_contamination(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """Each pod sees ONLY its own clients; no overlap across pods."""
        members = [
            _member("a1@x.com", pod="POD_A"),
            _member("a2@x.com", pod="POD_A"),
            _member("b1@x.com", pod="POD_B"),
            _member("b2@x.com", pod="POD_B"),
            _member("c1@x.com", pod="POD_C"),
        ]
        seen_by_pod = {}
        for pod in ("POD_A", "POD_B", "POD_C"):
            # New monkeypatch context per pod by re-setting env
            monkeypatch.setenv("BOT_MODE", "paper")
            monkeypatch.setenv("POD_ID", pod)
            monkeypatch.setenv("MAX_POD_CLIENTS", "5")
            monkeypatch.delenv("SINGLE_CLIENT_EMAIL", raising=False)
            # _active_runners must be reset per call so empty-result
            # branches go through the boot path consistently.
            monkeypatch.setattr(
                client_runner_module, "_active_runners", {}, raising=False
            )
            sb = FakeSupabase({"members": members})
            res = client_runner_module._fetch_active_members(sb)
            seen_by_pod[pod] = sorted(m["email"] for m in res)

        assert seen_by_pod["POD_A"] == ["a1@x.com", "a2@x.com"]
        assert seen_by_pod["POD_B"] == ["b1@x.com", "b2@x.com"]
        assert seen_by_pod["POD_C"] == ["c1@x.com"]
        # No overlap
        assert set(seen_by_pod["POD_A"]).isdisjoint(seen_by_pod["POD_B"])
        assert set(seen_by_pod["POD_B"]).isdisjoint(seen_by_pod["POD_C"])
        assert set(seen_by_pod["POD_A"]).isdisjoint(seen_by_pod["POD_C"])

    def test_pod_mode_zero_eligible_hard_exits(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        monkeypatch.setenv("BOT_MODE", "paper")
        monkeypatch.setenv("POD_ID", "POD_EMPTY")
        sb = FakeSupabase({
            "members": [_member("a1@x.com", pod="POD_A")]
        })
        with pytest.raises(SystemExit) as exc:
            client_runner_module._fetch_active_members(sb)
        assert exc.value.code == 5

    def test_pod_mode_missing_column_hard_exits_at_boot(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """POD_ID set + execution_pod column missing + no active runners
        must hard-exit(6) — pod service cannot run without pod-filtering.
        """
        monkeypatch.setenv("BOT_MODE", "paper")
        monkeypatch.setenv("POD_ID", "POD_A")
        # Members exist but the schema-degraded path triggers because the
        # FULL_COLS select includes execution_pod and we raise on it.
        sb = FakeSupabase(
            {"members": [_member("a1@x.com", pod="POD_A")]},
            raise_on_select_cols=("execution_pod",),
        )
        with pytest.raises(SystemExit) as exc:
            client_runner_module._fetch_active_members(sb)
        assert exc.value.code == 6, (
            f"POD mode + missing execution_pod must os._exit(6), got {exc.value.code}"
        )


# ════════════════════════════════════════════════════════════════════
# 3) Shared / live guardrails
# ════════════════════════════════════════════════════════════════════

class TestSharedLiveGuardrails:
    def test_live_shared_without_override_hard_exits(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """BOT_MODE=live + no SINGLE_CLIENT_EMAIL + no POD_ID +
        no ALLOW_SHARED_LIVE=1 must os._exit(4)."""
        monkeypatch.setenv("BOT_MODE", "live")
        sb = FakeSupabase({"members": [_member("a@x.com", mode="live")]})
        with pytest.raises(SystemExit) as exc:
            client_runner_module._fetch_active_members(sb)
        assert exc.value.code == 4

    def test_live_shared_with_allow_shared_live_permitted(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """ALLOW_SHARED_LIVE=1 must bypass the live-shared hard-exit."""
        monkeypatch.setenv("BOT_MODE", "live")
        monkeypatch.setenv("ALLOW_SHARED_LIVE", "1")
        sb = FakeSupabase({
            "members": [_member("a@x.com", mode="live", allow_live=True)],
            "client_risk_profiles": _risk_profiles_for("a@x.com"),
        })
        result = client_runner_module._fetch_active_members(sb)
        assert [m["email"] for m in result] == ["a@x.com"]

    def test_paper_shared_mode_allowed_without_override(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """BOT_MODE=paper with no SINGLE_CLIENT_EMAIL and no POD_ID
        must NOT hard-exit — paper shared mode is allowed."""
        monkeypatch.setenv("BOT_MODE", "paper")
        sb = FakeSupabase({
            "members": [_member("a@x.com"), _member("b@x.com")]
        })
        result = client_runner_module._fetch_active_members(sb)
        emails = sorted(m["email"] for m in result)
        assert emails == ["a@x.com", "b@x.com"]


# ════════════════════════════════════════════════════════════════════
# 4) allow_live_trading guardrail
# ════════════════════════════════════════════════════════════════════

class TestAllowLiveTradingGuardrail:
    def test_live_drops_member_with_allow_live_false(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        monkeypatch.setenv("BOT_MODE", "live")
        monkeypatch.setenv("SINGLE_CLIENT_EMAIL", "blocked@x.com")
        sb = FakeSupabase({
            "members": [_member("blocked@x.com", mode="live", allow_live=False)],
            "client_risk_profiles": _risk_profiles_for("blocked@x.com"),
        })
        with pytest.raises(SystemExit) as exc:
            client_runner_module._fetch_active_members(sb)
        # After the allow_live=False drop, members becomes empty —
        # single mode + zero members => hard-exit(5).
        assert exc.value.code == 5

    def test_live_keeps_member_with_allow_live_true_and_risk_profile(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        monkeypatch.setenv("BOT_MODE", "live")
        monkeypatch.setenv("SINGLE_CLIENT_EMAIL", "ok@x.com")
        sb = FakeSupabase({
            "members": [_member("ok@x.com", mode="live", allow_live=True)],
            "client_risk_profiles": _risk_profiles_for("ok@x.com"),
        })
        result = client_runner_module._fetch_active_members(sb)
        assert [m["email"] for m in result] == ["ok@x.com"]


# ════════════════════════════════════════════════════════════════════
# 5) MAX_POD_CLIENTS overflow
# ════════════════════════════════════════════════════════════════════

class TestPodOverflow:
    def test_overflow_with_no_active_runners_admits_first_n(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """With no active runners, more members than MAX_POD_CLIENTS
        results in the first _max_pod members admitted (the rest are
        explicitly rejected with a CRITICAL log)."""
        monkeypatch.setenv("BOT_MODE", "paper")
        monkeypatch.setenv("POD_ID", "POD_A")
        monkeypatch.setenv("MAX_POD_CLIENTS", "2")
        sb = FakeSupabase({
            "members": [
                _member("a1@x.com", pod="POD_A"),
                _member("a2@x.com", pod="POD_A"),
                _member("a3@x.com", pod="POD_A"),
            ]
        })
        result = client_runner_module._fetch_active_members(sb)
        assert len(result) == 2, (
            f"MAX_POD_CLIENTS=2 must cap admitted to 2, got {len(result)}"
        )

    def test_overflow_preserves_already_active_runners(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """If a1 is already actively running and pod fetch returns
        a1, a2, a3 with MAX_POD_CLIENTS=2, a1 MUST be kept and one of
        {a2,a3} admitted. A new client must never displace a running one.
        """
        monkeypatch.setenv("BOT_MODE", "paper")
        monkeypatch.setenv("POD_ID", "POD_A")
        monkeypatch.setenv("MAX_POD_CLIENTS", "2")
        # Mark a1 as already active
        monkeypatch.setattr(
            client_runner_module,
            "_active_runners",
            {"a1@x.com": object()},
            raising=False,
        )
        sb = FakeSupabase({
            "members": [
                _member("a1@x.com", pod="POD_A"),
                _member("a2@x.com", pod="POD_A"),
                _member("a3@x.com", pod="POD_A"),
            ]
        })
        result = client_runner_module._fetch_active_members(sb)
        emails = {m["email"] for m in result}
        assert "a1@x.com" in emails, (
            f"already-active a1@x.com must NEVER be evicted, got {emails}"
        )
        assert len(result) == 2, (
            f"MAX_POD_CLIENTS=2 must cap total to 2 even with active runner present"
        )


# ════════════════════════════════════════════════════════════════════
# 6) Schema fallback safety
# ════════════════════════════════════════════════════════════════════

class TestSchemaFallback:
    def test_shared_paper_missing_pod_column_falls_back_safely(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """Missing execution_pod column in SHARED PAPER must NOT crash,
        NOT hard-exit, and must load members (core columns)."""
        monkeypatch.setenv("BOT_MODE", "paper")
        # No SINGLE_CLIENT_EMAIL, no POD_ID → shared paper
        sb = FakeSupabase(
            {"members": [_member("a@x.com"), _member("b@x.com")]},
            raise_on_select_cols=("execution_pod",),
        )
        result = client_runner_module._fetch_active_members(sb)
        emails = sorted(m["email"] for m in result)
        assert emails == ["a@x.com", "b@x.com"], (
            f"Shared paper + missing execution_pod must keep loading; got {emails}"
        )

    def test_live_missing_allow_live_column_safe_default_drops_all(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """Missing allow_live_trading column in LIVE shared mode (with
        ALLOW_SHARED_LIVE=1 so we get past the shared-live guard) must
        treat allow_live_trading as False — every member dropped — but
        MUST NOT crash. Result is empty list; no hard-exit because
        single/pod mode flags are not set.
        """
        monkeypatch.setenv("BOT_MODE", "live")
        monkeypatch.setenv("ALLOW_SHARED_LIVE", "1")
        sb = FakeSupabase(
            {
                "members": [_member("a@x.com", mode="live", allow_live=True)],
                "client_risk_profiles": _risk_profiles_for("a@x.com"),
            },
            raise_on_select_cols=("allow_live_trading",),
        )
        result = client_runner_module._fetch_active_members(sb)
        # On the safe-default path, allow_live_trading is forced to False
        # by setdefault — so the LIVE guardrail drops this member.
        assert result == [], (
            f"Missing allow_live_trading must default to False (safe) and "
            f"drop all live members; got {result}"
        )


# ════════════════════════════════════════════════════════════════════
# 7) Runtime / transient empty fetch
# ════════════════════════════════════════════════════════════════════

class TestRuntimeEmptyFetch:
    def test_zero_members_with_active_runners_does_not_hard_exit(
        self, monkeypatch, client_runner_module, clean_runtime
    ):
        """SINGLE or POD mode with zero members from this fetch BUT
        runners already alive must NOT hard-exit — that would kill a
        live position on a transient empty fetch. The function returns
        an empty list and the caller continues.
        """
        monkeypatch.setenv("BOT_MODE", "paper")
        monkeypatch.setenv("SINGLE_CLIENT_EMAIL", "ghost@x.com")
        # Mark a runner as already active so the empty-fetch path
        # takes the runtime branch instead of the boot branch.
        monkeypatch.setattr(
            client_runner_module,
            "_active_runners",
            {"ghost@x.com": object()},
            raising=False,
        )
        sb = FakeSupabase({"members": []})
        # Must NOT raise SystemExit — must return []
        result = client_runner_module._fetch_active_members(sb)
        assert result == [], (
            f"Mid-day transient empty fetch with active runners must return "
            f"[] (not hard-exit); got {result}"
        )
