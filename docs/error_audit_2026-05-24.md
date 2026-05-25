# Angel Precision — Test Suite Error Audit

**Generated:** 2026-05-24
**HEAD:** `2c5c5ce` (`main`) — PR #30-credentials merged
**Scope:** Every `FAILED` and `ERROR` line emitted by `python3 -m pytest tests/ -p no:cacheprovider` against the bot repo.

---

## Headline

**There are no runtime bugs in the bot that the test suite has caught.**

Of **118 FAILED/ERROR lines** in the full-suite run:

| Class | Count | What it means | Production impact |
| --- | --- | --- | --- |
| Test-suite pollution (mostly `test_repeg_resubmit.py` stubbing `ap.db`) | **104** | Tests pass cleanly in isolation; fail only because a prior test left a partial `ap.db` stub in `sys.modules` | **None** |
| Environmental (sandbox has no Postgres) | **3** | `test_claim_jsonb_regression.py` requires a live PG; skips cleanly in isolation | **None** |
| Stale tests (production code renamed, test not updated) | **2** | `_check_daily_loss` → `check_daily_loss_breach`; `_is_protective` → `_is_protective_exit` | **None** — runtime code is correct |
| Stale tests (string-marker drift) | **1** | Log marker `LOST_HANDOFF` → `LOST_HANDOFF_30S`; test still searches old name | **None** — runtime emits the marker, just under the new name |
| Broken test harness (stub missing new attribute) | **1** | `ap/exit_replay_harness.py::_StubPosition` missing `is_trend_day` attr; `evaluate_exit` raises and the harness swallows the exception | **LOW** — runtime positions all have `is_trend_day` defaulted in `ap_exit_engine.ManagedPosition`. **MEDIUM for confidence** — the harness can no longer prove hard-stop fires. |
| Test bug (forgot `os.environ.setdefault("DATABASE_URL", ...)`) | **7** | `test_simulation.py` tries to import modules that read `DATABASE_URL` at import time | **None** — runtime sets it in Render env |

**Total real bugs in production code revealed by these failures: ZERO.**

Total real bugs in TEST code: **11** (the last 4 rows above).

---

## Evidence — isolation vs. full-suite

Every "noisy" test file was re-run in isolation. The pattern is unambiguous:

| File | Full sweep | Isolation | Verdict |
| --- | --- | --- | --- |
| `test_auth_api_key.py` | 14 failed | **17 passed** | Pollution |
| `test_client_scoped_db_lookups.py` | 8 failed | **11 passed** | Pollution |
| `test_claim_jsonb_regression.py` | 3 errors | **3 skipped** (auto) | Environmental — needs real Postgres |
| `test_credential_decryption.py` | 12 errors | **12 passed** | Pollution |
| `test_exit_replay_harness.py` | 1 failed | 6 passed + **1 failed** | **REAL — harness stub bug** |
| `test_funnel_fixes.py` | 1 failed | 20 passed + **1 failed** | **REAL — stale string marker** |
| `test_osm_retry_idempotency.py` | 15 errors | **15 passed** | Pollution |
| `test_phase10_telemetry_endpoint.py` | 3 failed + 13 errors | **21 passed** | Pollution |
| `test_phase11_entry_fill_conversion.py` | 4 errors | **31 passed** | Pollution |
| `test_phase12_live_safety_hardening.py` | 2 failed | **48 passed** | Pollution |
| `test_phase3_submit_time_refresh.py` | 6 errors | **17 passed** | Pollution |
| `test_phase4_account_equity_sizing.py` | 17 errors | **26 passed** | Pollution |
| `test_phase7_exit_verification.py` | 4 failed | **20 passed** | Pollution |
| `test_phase9_retry_wire_in.py` | 6 errors | **25 passed** | Pollution |
| `test_simulation.py` | 9 failed | 4 passed + **9 failed** | **REAL — test bugs (DATABASE_URL + renames)** |

**~107 of 118 failures vanish under isolation.** Only 11 are genuine test issues (and none are runtime bugs).

---

## Root cause #1 — `tests/test_repeg_resubmit.py:38` pollutes `sys.modules`

```python
# tests/test_repeg_resubmit.py, line 35-38
# Stub ap.db before importing retry_engine so update_order is observable.
_db_stub = types.ModuleType("ap.db")
_db_stub.update_order = MagicMock()
sys.modules["ap.db"] = _db_stub  # <-- installed at module top, never torn down
```

The stub only defines `update_order`. After this file is collected (alphabetically very late in the run, since `r` > `q`), every subsequent test that does:

```python
from ap.db import conn         # used by ap/auth.py line 7
from ap.db import run_with_retry  # used by client_runner.py line 54
```

…fails with `ImportError: cannot import name 'conn' from 'ap.db'`.

**Fix (NOT shipped in this audit — needs its own PR):**

Replace the module-level stub with a fixture-scoped stub:

```python
# tests/test_repeg_resubmit.py
@pytest.fixture(autouse=True, scope="module")
def stub_ap_db(monkeypatch_session):
    stub = types.ModuleType("ap.db")
    stub.update_order = MagicMock()
    # Preserve other names if real ap.db is loaded, so downstream tests
    # that need `conn` / `run_with_retry` still work.
    real = sys.modules.get("ap.db")
    if real is not None:
        for name in ("conn", "run_with_retry"):
            if hasattr(real, name):
                setattr(stub, name, getattr(real, name))
    monkeypatch_session.setitem(sys.modules, "ap.db", stub)
    yield stub
```

Or simpler: don't stub the whole module — just patch `ap.db.update_order` with `unittest.mock.patch`.

**Estimated impact of fixing this:** at minimum **76 of the 76 ERRORs** disappear. ~26 of the 42 FAILEDs disappear. Combined with the test renames below, the suite returns to a clean baseline.

---

## Root cause #2 — Stale test assertions (production code moved on)

### #2a — `test_simulation.py::TestKillSwitch::test_entries_blocked_after_daily_loss`

```
E   AttributeError: 'APMasterControl' object has no attribute '_check_daily_loss'
```

The method `_check_daily_loss` was renamed to `check_daily_loss_breach` in `ap_master_control.py:727`. Production code is correct. Update the test to call the new name.

### #2b — `test_simulation.py::TestKillSwitch::test_exit_not_blocked_by_kill_switch`

```
E   AttributeError: 'APExitEngine' object has no attribute '_is_protective'
```

`_is_protective` was extracted to a module-level function `_is_protective_exit` in `ap_exit_engine.py:1172`. Production code is correct. Update the test.

### #2c — `test_funnel_fixes.py::TestLostHandoffForensics::test_warning_log_emitted_for_lost_handoff`

```
E   AssertionError: A WARN-level log must fire on every LOST_HANDOFF cancel
E   assert 'LOST_HANDOFF | local=' in '<order_monitor.py source>'
```

The log marker was renamed from `LOST_HANDOFF` to `LOST_HANDOFF_30S` when the timeout dropped from 120s to 30s (`order_monitor.py:419, 425`). Production code emits the marker correctly. Update the test grep pattern.

### #2d — 7 `test_simulation.py` tests failing on `DATABASE_URL env var is required`

These tests forgot the `os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/test_X")` pattern at module top. Production code is correct. Add the setdefault.

---

## Root cause #3 — `_StubPosition` in `exit_replay_harness.py` missing `is_trend_day`

```
WARNING  ap.exit_replay_harness:exit_replay_harness.py:184
  evaluate_exit raised at step 0: '_StubPosition' object has no attribute 'is_trend_day'
```

`ap_exit_engine._eval_exit()` (at line 956) reads `pos.is_trend_day`. The harness's `_StubPosition` was written before this attribute was added; it doesn't set it.

The harness wraps `evaluate_exit` in `try/except` and swallows the AttributeError silently — so the test (`test_hard_stop_fires`) reports `exit_fired=False`, fooling itself.

**Production impact:** `ap_exit_engine.ManagedPosition.is_trend_day` has `bool = False` as its dataclass default (`ap_exit_engine.py:270`). Every live position has this attribute set. Production is safe.

**Test confidence impact:** the harness is currently a no-op for replay-based hard-stop validation. **Fix the stub** — add `self.is_trend_day = False` to `_StubPosition.__init__`. Two lines, zero risk.

---

## Environmental — `test_claim_jsonb_regression.py` (3 ERRORs)

These tests require a real Postgres at `127.0.0.1:5432`. In the dev sandbox they correctly auto-skip when collected alone:

```
$ pytest tests/test_claim_jsonb_regression.py
3 skipped in 0.01s
```

In the full sweep they ERROR instead of SKIP because a prior `os.environ.setdefault("DATABASE_URL", "postgresql://test:test@127.0.0.1:5432/...")` from an earlier test makes psycopg2 attempt a real connection and fail. **Not a runtime issue.** They will pass on Render where the real PG is reachable.

---

## Severity / production blocker assessment

| Issue | Production blocker? | Pre-proof-week action |
| --- | --- | --- |
| `test_repeg_resubmit` pollution | **No** | Should fix soon — currently masks ~10 stale tests as ERROR instead of FAILED |
| 4 stale test assertions in `test_simulation` / `test_funnel_fixes` | **No** | Update test code; production is correct |
| 7 missing `DATABASE_URL` in `test_simulation` | **No** | Add the setdefault; trivial |
| `_StubPosition` missing `is_trend_day` | **No** | One-line stub fix; restores hard-stop replay confidence |
| `test_claim_jsonb_regression` env-dependent | **No** | Will pass on Render automatically |

**Bot is clear to enter Proof Week.** None of the failures correspond to a runtime defect that could surface live.

---

## Recommended cleanup PR (separate, post-Proof-Week safe)

Suggested title: `test: clean up suite pollution and stale assertions surfaced by audit`

Diff scope (≤80 lines):

1. `tests/test_repeg_resubmit.py` — replace module-level stub with fixture-scoped stub that preserves `conn` and `run_with_retry`.
2. `tests/test_simulation.py` — add `os.environ.setdefault("DATABASE_URL", ...)` at module top; update 2 method names (`_check_daily_loss` → `check_daily_loss_breach`, `_is_protective` → `_is_protective_exit`).
3. `tests/test_funnel_fixes.py` — change `"LOST_HANDOFF | local="` to `"LOST_HANDOFF_30S | local="`.
4. `ap/exit_replay_harness.py` — add `self.is_trend_day = False` (and any other attributes `evaluate_exit` reads but `_StubPosition` is missing — audit `_eval_exit` for all `pos.*` accesses).

After this PR, expected baseline: **0 failures, 0 errors** (excepting the 3 `test_claim_jsonb_regression` that need real PG and skip cleanly in isolation).

---

## What I did NOT do in this audit

- Did **not** modify any production code. Audit is read-only.
- Did **not** modify any test files. The fix is recommended as a separate PR you approve explicitly.
- Did **not** declare any of the 118 failures a runtime bug without checking isolation, source code, and the renamed-symbol mapping.

Audit performed against `main` HEAD `2c5c5ce` on 2026-05-24.
