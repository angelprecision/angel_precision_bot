# PR #435 Remaining Evidence Plan

**Repo:** `angelprecision/angel_precision_bot`  
**PR:** [#435](https://github.com/angelprecision/angel_precision_bot/pull/435) (draft / HARD HOLD)  
**Branch tip (API at investigation):** `8cf53f3738640631105592669ef6b595ad4a2139`  
**PR body still cites tip:** `eed1da092af1de0fa18c30678332a2c66971516c` (one commit behind tip)  
**Base:** `main@d404df34e00522ba2cce995b6a0129ba39b83944`  
**Constraint for this investigation:** GitHub API only — no clone / push / merge.

Harness copies for parent editing: `/workspace/pr435/harness/`

---

## 1) Full exact-head P0 suite — how to run / how P0 is defined

### Definition

P0 is **not** a pytest marker. It is an **explicit inventory** env var `P0_TEST_INVENTORY` in:

- `.github/workflows/p0_regression.yml`

The workflow job `p0-tests` checks out the **exact PR head SHA** (`github.event.pull_request.head.sha`), asserts `git rev-parse HEAD` matches, then runs:

```bash
python -m pytest ${P0_TEST_INVENTORY} -v --tb=short --color=yes
```

There is **no** `-m p0`. Inclusion = listed path. Related cohesion / P1 files are interleaved in the same inventory (e.g. `tests/test_p1_intelligence_admission_policy.py`).

### Exact-head vs merge-ref

| Job | Checkout ref | Purpose |
|-----|--------------|---------|
| `p0-tests` | `${{ github.event.pull_request.head.sha }}` | **Exact-head P0** |
| `p0-merge-ref-tests` | `refs/pull/${{ number }}/merge` | Synthetic merge of PR into base (PR events only) |

Both jobs share the same `P0_TEST_INVENTORY`, Postgres 17 service, migrations, and env.

### Env vars (CI + local Postgres-dependent tests)

```text
PGSSLMODE=disable
DATABASE_URL=postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable
INTELLIGENCE_POSTGRES_TEST_URL=postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable
MANUAL_CLOSE_POSTGRES_TEST_URL=postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable
```

Postgres tests also skip unless `INTELLIGENCE_POSTGRES_TEST_URL` is set (`pytest.mark.skipif`).

### Local exact-head command (conceptually)

```bash
# After checkout of tip 8cf53f37 (or eed1da09) — do NOT use merge ref
export P0_TEST_INVENTORY="$(yq -r '.env.P0_TEST_INVENTORY' .github/workflows/p0_regression.yml | tr '\n' ' ')"
# or paste the inventory from the workflow file
export PGSSLMODE=disable
export DATABASE_URL='postgresql://postgres:postgres@localhost:5432/intelligence_test?sslmode=disable'
export INTELLIGENCE_POSTGRES_TEST_URL="$DATABASE_URL"
export MANUAL_CLOSE_POSTGRES_TEST_URL="$DATABASE_URL"
# apply migrations/20260717_exit_decision_generation_claims.sql
# apply migrations/20260809_exit_decision_generation_requested_qty.sql
python -m pytest $P0_TEST_INVENTORY -v --tb=short --color=yes
```

### #435-relevant inventory entries already in P0

- `tests/test_p0_intelligence_postgres.py` ✅ (in inventory; tip adds BREACH claim/idempotency cases)
- `tests/test_p0_intelligence_context_snapshots.py` ✅ (in inventory via… **verify**: snapshots file is **NOT** listed in `P0_TEST_INVENTORY` in the workflow text fetched from tip — only `test_p0_intelligence_postgres.py` among intelligence files)
- `tests/test_p0_watcher_recovery_execution_ownership.py` ✅ listed

**Gap:** new `tests/test_p0_435_*.py` files are **not** in `P0_TEST_INVENTORY` yet. Fixture pack is local/CI-optional until inventory is extended.

### Key #435 fixture modules (tip)

| Path | Role |
|------|------|
| `tests/test_p0_435_canonical_identity.py` | pattern / lineage / lifecycle / readiness flags |
| `tests/test_p0_435_fvg_as_of_cache.py` | 15m as-of cache completeness |
| `tests/test_p0_435_market_structure_freeze.py` | FVG freeze / opposing wall / PIT |
| `tests/test_p0_435_behavioral_matrix.py` | table-driven readiness matrix |
| `tests/test_p0_435_aapl_opening_replay_shape.py` | AAPL-shaped opening PUT |
| `tests/test_p0_435_money_path_non_authority.py` | handoff disabled/capacity/error shapes |
| `tests/test_p0_435_september_live_cohort.py` | Sept cohort scaffold (+ JSON fixture) |

Coverage map: `docs/pr_specs/p1_435_behavioral_matrix_coverage_20260910.md`

---

## 2) Merge-ref P0

There is **no separate comparison script**. Merge-ref is a **second GitHub Actions job** in the same workflow:

- Job: `p0-merge-ref-tests`
- `if: github.event_name == 'pull_request'`
- Checkout: `ref: refs/pull/${{ github.event.pull_request.number }}/merge`
- Asserts HEAD == `github.sha` (merge commit)
- Same pytest inventory / Postgres / env as exact-head

**How to extend:** keep inventory identical across both jobs; never diverge commands. Adding a file to `P0_TEST_INVENTORY` automatically covers both exact-head and merge-ref.

---

## 3) Real PostgreSQL intelligence / BREACH job tests

### File + symbols

**`tests/test_p0_intelligence_postgres.py`** (tip SHA content; harness copy)

| Symbol | What it proves |
|--------|----------------|
| `_database` (autouse fixture) | Applies `migrations/20260712_intelligence_context_snapshots.sql`, truncates `ap_intelligence_jobs` / `ap_intelligence_snapshots`, patches `ap.intelligence_snapshot_store._db_conn` + `_run_with_retry` |
| `_enqueue(...)` | Wrapper → `enqueue_intelligence_job` |
| `test_real_postgres_revision_concurrency_claim_scope_and_atomic_completion` | Concurrent hash change → one next revision; claim scoped by client/mode; `complete_job_with_snapshot` atomic |
| `test_real_postgres_expired_owner_cannot_complete` | Expired lease → `JOB_CLAIM_OWNERSHIP_LOST`, no snapshot |
| `test_real_postgres_breach_phase_uses_existing_identity_and_claim_fence` | `phase="BREACH"` + `local_order_id` claimable |
| `test_real_postgres_breach_duplicate_enqueue_is_idempotent` | Same hash/order → one job row |
| `test_real_postgres_breach_identity_keys_do_not_cross_clients_or_modes` | Different client / LIVE vs PAPER insert separately |
| `test_real_postgres_breach_claim_is_scoped_and_single_owner` | Second claim owner gets `[]` |

### Related in-memory / store fencing (no real PG required if memory backend)

**`tests/test_p0_intelligence_context_snapshots.py`** — key symbols:

- `test_breach_enqueue_is_idempotent_frozen_and_observe_only`
- `test_breach_handoff_disabled_returns_without_executor_or_database`
- `test_breach_handoff_forwards_signal_id_once_to_async_enqueue`
- `test_worker_claims_are_exactly_scoped_by_client_and_mode`
- `test_two_workers_for_same_scope_claim_each_job_once`
- `test_identical_input_is_idempotent_but_changed_input_gets_next_revision`
- `test_expired_owner_cannot_complete_reclaimed_job`
- `test_recovery_scan_backfills_missing_phase_jobs_from_durable_truth`
- `test_recovery_scan_rebuilds_breach_from_order_meta_without_execution_side_effect`
- `test_recovery_conflicting_timestamp_authorities_fail_closed`
- `test_breach_materializer_rejects_job_column_payload_identity_disagreement`
- `test_breach_materializer_rejection_terminalizes_without_retry`

### Production claim/idempotency path symbols

| Path | Symbols |
|------|---------|
| `ap/intelligence_snapshot_store.py` | `enqueue_intelligence_job`, `claim_due_intelligence_jobs`, `complete_job_with_snapshot` |
| `ap/intelligence_context_materializer.py` | `enqueue_breach_context`, `build_snapshot_kwargs`, `recover_missing_intelligence_jobs`, `BreachMaterializationRejected` |
| `ap/intelligence_context_handoff.py` | `enqueue_breach_context_best_effort`, `submit_intelligence_enqueue`, `intelligence_context_enabled` |
| `migrations/20260712_intelligence_context_snapshots.sql` | schema for jobs/snapshots |

### Env / fixtures

- **Requires Postgres:** any test in `test_p0_intelligence_postgres.py` (`INTELLIGENCE_POSTGRES_TEST_URL`)
- **Pure fixtures / monkeypatch:** snapshots suite (memory store), money-path non-authority, behavioral matrix, AAPL shape, market-structure freeze, September cohort scaffold (classification only)

---

## 4) September 2026 LIVE incident fixtures / dossier / replay

### Present on tip (`8cf53f37`)

| Artifact | Contents |
|----------|----------|
| `tests/fixtures/september_live_cohort_202609.json` | Schema `september_live_cohort.v1` |
| `tests/test_p0_435_september_live_cohort.py` | Parametrized family A/B/C/D observe-only proofs + honesty gates |

**Cases present:**

| case_id | ticker | durable IDs | notes |
|---------|--------|-------------|-------|
| `QQQ_PUT_2026-09-09` | QQQ | mostly `UNKNOWN` | scaffold; geometry partially filled |
| `HOOD_PUT_2026-09-09` | HOOD | `UNKNOWN` | scaffold |
| `LULU_CALL_2026-09-08` | LULU | `UNKNOWN` | scaffold |
| `NKE_CALL_2026-09-08` | NKE | `UNKNOWN` | geometry `null` → **pytest.skip** until filled |
| `AAPL_PUT_2026-08-12_OPENING` | AAPL | documented `signal_id=83503891-…`, client `jasoncosby1@gmail.com` | strongest identity |

### Absent (requested but not on tip)

- **GOOGL**, **C**, **IWM** — no cases in JSON / no dossier / no replay module
- No trade-dossier binder specific to this cohort (generic `tests/test_trade_dossier.py` exists elsewhere, unrelated)
- No full candle/quote PIT replay packs for QQQ/HOOD/LULU/NKE (only narrative + sparse `breach_shape`)
- Amendment required replays emphasize QQQ/HOOD/LULU with exact production identities — **not yet filled**

### Related but not September cohort

- `tests/test_p0_435_aapl_opening_replay_shape.py` — generic AAPL-shaped opening PUT (fixture date `2026-09-09` for timing math; durable cohort AAPL is `2026-08-12`)

---

## 5) Runtime vs restart vs materializer parity — already present

### Spec contract (docs)

`docs/pr_specs/p1_breach_setup_intelligence_20260811.md` § “Runtime / restart / deferred-materializer parity” defines the matrix (worker unset / valid PAPER|LIVE / conflicting identity / ownership loss / persistence failure) across:

1. **Execution core** (`ap_execution_core._on_entry_trigger` → `enqueue_breach_context_best_effort`)
2. **Restart recovery** (`recover_missing_intelligence_jobs`)
3. **Deferred materializer** (`build_snapshot_kwargs` / worker claim+complete)

Amendment scenario **#18** (`docs/pr_specs/p1_435_behavioral_matrix_coverage_20260910.md`): **PARTIAL** — identical frozen market_structure hash covered; full restart/materializer parity deferred.

### What exists today

| Coverage | Where |
|----------|-------|
| Runtime freeze before submit | `test_execution_core_freezes_breach_before_existing_submit_path` in `tests/test_p0_watcher_recovery_execution_ownership.py` |
| Handoff disabled / no DB | `test_breach_handoff_disabled_*`, `test_disabled_feature_does_not_submit_enqueue` |
| Restart backfill from durable truth | `test_recovery_scan_backfills_missing_phase_jobs_from_durable_truth` |
| Restart BREACH rebuild w/o execution side effects | `test_recovery_scan_rebuilds_breach_from_order_meta_without_execution_side_effect` |
| Restart conflict fail-closed | `test_recovery_conflicting_timestamp_authorities_fail_closed` |
| Materializer identity disagreement | `test_breach_materializer_rejects_job_column_payload_identity_disagreement` |
| Materializer no broker fabrication | `test_materializer_does_not_call_broker_or_fabricate_missing_inputs` |
| Frozen structure determinism | `test_identical_frozen_market_structure_yields_identical_zone_ids_and_hash` |

### Still missing for “complete parity matrix with actual values”

- Single table-driven test that feeds **one frozen BREACH envelope** through:
  1. runtime handoff enqueue,
  2. `recover_missing_intelligence_jobs` rebuild,
  3. `build_snapshot_kwargs` materialization,
  and asserts **byte-identical** `breach_evidence` / readiness / market_structure hashes.
- Explicit env-unset vs enabled matrix rows with asserted job counts.
- No `test_p0_435_*parity*` module yet (unlike selector taxonomy’s `test_*_runtime_restart_materializer_parity` in `tests/test_p0_selector_retry_taxonomy.py` — different domain, useful pattern to copy).

---

## 6) Money-path exactly-one-submit around `enqueue_breach_context_best_effort`

### Production seam

`ap_execution_core.py` inside `_on_entry_trigger` (added in commit `1dcb290c`):

- Builds frozen input via `_build_breach_intelligence_signal`
- Calls `enqueue_breach_context_best_effort(...)` inside try/except
- Handoff failure → warning only; **does not return early**
- Existing selector / `submit_existing_entry` path continues

Patch snippet saved: `/workspace/pr435/harness/ap_execution_core_breach_handoff.patch`

### Tests present

| Test | File | Assertion |
|------|------|-----------|
| `test_execution_core_freezes_breach_before_existing_submit_path` | `test_p0_watcher_recovery_execution_ownership.py` | Captures handoff kwargs; **`osm.submit_existing_entry.call_count == 1`** even when submit returns reconcile disposition |
| Money-path non-authority pack | `test_p0_435_money_path_non_authority.py` | disabled / capacity / pool error shapes never imply submit/cancel/eligibility |
| Snapshots handoff | `test_breach_handoff_*`, `test_handoff_never_waits_for_delayed_persistence` | async non-blocking |

### Gaps

- No matrix proving **exactly-one-submit** when handoff raises / returns `ok:false` / capacity exhausted / worker disabled — all while a successful broker submit path is otherwise taken.
- No proof that **duplicate** breach callback cannot yield two submits (may already be owned by other watcher P0s; needs cross-link).
- `test_best_effort_wrappers_never_raise_when_enabled_enqueue_explodes` currently documents that wrappers may not catch submit exceptions — incomplete fence.

---

## Pure fixtures vs needs Postgres

### Pure fixtures / unit (no Postgres)

- All `tests/test_p0_435_*.py` except none of them need PG today
- `september_live_cohort_202609.json` classification scaffold
- Most of `test_p0_intelligence_context_snapshots.py` (memory backend / monkeypatch)
- Money-path non-authority
- Market-structure / FVG as-of / canonical identity / AAPL shape / behavioral matrix

### Needs disposable Postgres (`INTELLIGENCE_POSTGRES_TEST_URL`)

- Entire `tests/test_p0_intelligence_postgres.py`
- Full exact-head / merge-ref P0 CI jobs (service container)
- Any future parity test that asserts durable job row counts / claim leases / `complete_job_with_snapshot` transactions
- Production DB fill for September durable IDs (read-only export → fixture; not inventable)

### Needs production DB export (not inventable fixtures)

- Real `signal_id` / `local_order_id` / `trigger_crossed_at` / option quotes for QQQ HOOD LULU NKE (+ optional GOOGL C IWM if confirmed incidents)
- Exact candle as-of packs for dossier replay

---

## Recommended next 3 commits

### Commit 1 — `test(#435): exact-one-submit money-path matrix + inventory hook`

**Add:** `tests/test_p0_435_money_path_exactly_one_submit.py`  
**Extend:** `.github/workflows/p0_regression.yml` `P0_TEST_INVENTORY` with:

- `tests/test_p0_435_money_path_non_authority.py`
- `tests/test_p0_435_money_path_exactly_one_submit.py`
- (optionally the rest of the `test_p0_435_*` pack)

**Prove:** for each of `{worker_disabled, capacity_exhausted, handoff_exception, enqueue_ok:false}`, `_on_entry_trigger` still calls `submit_existing_entry` **exactly once** and never `cancel_pending_entry` due to intelligence.

**Needs:** pure fixtures / mocks only.

### Commit 2 — `test(#435): runtime/restart/materializer frozen-envelope parity`

**Add:** `tests/test_p0_435_runtime_restart_materializer_parity.py`  
**Symbols to drive:** `enqueue_breach_context` → store → `recover_missing_intelligence_jobs` path with pre-seeded order/meta → `build_snapshot_kwargs`  
**Assert:** identical `input_hash`, `breach_lineage`, `entry_readiness_observe_only`, `market_structure` zone ids / hash.

**Needs:** memory store OK for first cut; add 1–2 real-Postgres twins in `test_p0_intelligence_postgres.py` for claim+complete of the same envelope.

### Commit 3 — `test(#435): fill September LIVE cohort durable identities + PIT candles`

**Update:** `tests/fixtures/september_live_cohort_202609.json` + dossier notes under `docs/pr_specs/`  
**Fill from production (Jason LIVE):** QQQ/HOOD/LULU/NKE durable IDs + timestamps; add GOOGL/C/IWM **only if** durable incidents exist (else document `ABSENT_IN_PRODUCTION`).  
**Add:** optional `tests/fixtures/september_live_cohort_candles_202609/*.json` completed-bars-as-of packs.  
**Gate:** remove `pytest.skip` for NKE once geometry present; keep `UNKNOWN` honesty where truth missing — never fabricate option BID/ASK.

**Needs:** production DB read; pure fixture thereafter.

---

## Existing harness snippets (20–40 lines)

### A) Exact-head / merge-ref P0 workflow (inventory + dual jobs)

```yaml
# .github/workflows/p0_regression.yml (excerpt)
env:
  P0_TEST_INVENTORY: >-
    tests/test_osm_retry_idempotency.py
    # ... full inventory ...
    tests/test_p0_intelligence_postgres.py
    tests/test_p0_pending_trigger_restart_recovery.py
    tests/test_p0_watcher_recovery_execution_ownership.py
    # ...

jobs:
  p0-tests:
    # checkout ref: github.event.pull_request.head.sha  → exact-head
    # pytest ${P0_TEST_INVENTORY}

  p0-merge-ref-tests:
    if: github.event_name == 'pull_request'
    # checkout ref: refs/pull/${{ number }}/merge
    # same pytest inventory
```

### B) Real Postgres BREACH claim / idempotency

```python
# tests/test_p0_intelligence_postgres.py
DATABASE_URL = os.getenv("INTELLIGENCE_POSTGRES_TEST_URL", "")
pytestmark = pytest.mark.skipif(not DATABASE_URL, reason="disposable PostgreSQL URL not configured")

def test_real_postgres_breach_duplicate_enqueue_is_idempotent():
    first = _enqueue("breach-dup-hash", phase="BREACH", local_order_id="order-dup")
    second = _enqueue("breach-dup-hash", phase="BREACH", local_order_id="order-dup")
    assert first["ok"] and first.get("inserted")
    assert second["ok"] and (second.get("duplicate") or not second.get("inserted"))
    with _conn() as c:
        c.execute(
            "SELECT COUNT(*)::int AS count FROM ap_intelligence_jobs "
            "WHERE phase='BREACH' AND local_order_id=%s AND input_hash=%s",
            ("order-dup", "breach-dup-hash"),
        )
        assert c.fetchone()["count"] == 1

def test_real_postgres_breach_claim_is_scoped_and_single_owner():
    _enqueue("breach-claim", phase="BREACH", local_order_id="order-claim")
    from ap.intelligence_snapshot_store import claim_due_intelligence_jobs
    first = claim_due_intelligence_jobs(
        claim_owner="owner-a", client_id="client@example.com",
        execution_mode="PAPER", limit=10,
    )
    second = claim_due_intelligence_jobs(
        claim_owner="owner-b", client_id="client@example.com",
        execution_mode="PAPER", limit=10,
    )
    assert first["ok"] and len(first["jobs"]) == 1
    assert second["ok"] and second["jobs"] == []
```

### C) Exactly-one-submit after BREACH freeze (watcher/execution harness)

```python
# tests/test_p0_watcher_recovery_execution_ownership.py
def test_execution_core_freezes_breach_before_existing_submit_path(monkeypatch):
    core, osm = _execution_core(monkeypatch, {
        "ok": False,
        "local_order_id": "oid-1",
        "reconciliation_required": True,
        "error": "BROKER_AMBIGUOUS_READ_TIMEOUT_RECONCILIATION_REQUIRED",
    })
    captured = {}
    def capture(signal, **kwargs):
        captured["signal"] = signal
        captured["kwargs"] = kwargs
        return {"ok": True, "accepted": True}
    monkeypatch.setattr(
        "ap.intelligence_context_handoff.enqueue_breach_context_best_effort", capture
    )
    watched = _submit_ready_watched_signal()
    # ... stamp trigger / breach quote fields ...
    result = core_mod.APExecutionCore._on_entry_trigger(core, watched)
    assert result["disposition"] == "RECONCILE_BROKER_INTENT"
    assert captured["kwargs"]["local_order_id"] == "oid-1"
    assert frozen["trigger_source"] == "watcher_confirmed_breach"
    assert osm.submit_existing_entry.call_count == 1
```

### D) Handoff non-authority shapes

```python
# tests/test_p0_435_money_path_non_authority.py
def test_handoff_disabled_returns_observe_safe_shape(monkeypatch):
    monkeypatch.delenv("INTELLIGENCE_CONTEXT_WORKER_ENABLED", raising=False)
    assert handoff.intelligence_context_enabled() is False
    for fn in (
        handoff.enqueue_pretrigger_context_best_effort,
        handoff.enqueue_preopen_context_best_effort,
        handoff.enqueue_breach_context_best_effort,
    ):
        result = fn({"signal_id": "s1", "side": "CALL"})
        assert result == {"ok": True, "accepted": False, "disabled": True}
        _assert_non_authority(result)
```

### E) September cohort honesty gate

```python
# tests/test_p0_435_september_live_cohort.py
def test_fixture_marks_missing_durable_ids_explicitly():
    unknown_cases = [
        c for c in FIXTURES["cases"] if c["ticker"] in {"QQQ", "HOOD", "LULU", "NKE"}
    ]
    assert unknown_cases
    for case in unknown_cases:
        assert case["durable"]["signal_id"] == "UNKNOWN"
        assert case["durable"]["option_bid_ask_at_breach"] == "UNKNOWN"
```

### F) Runtime handoff in execution core (production)

```python
# ap_execution_core._on_entry_trigger (excerpt from commit 1dcb290c)
from ap.intelligence_context_handoff import enqueue_breach_context_best_effort
_breach_input = _build_breach_intelligence_signal(...)
_breach_handoff = enqueue_breach_context_best_effort(
    _breach_input,
    client_id=_breach_client_id,
    execution_mode=_breach_mode_for_intelligence,
    canonical_signal_id=str(_breach_input.get("canonical_signal_id") or ""),
    local_order_id=str(queue_local_order_id or ""),
    signal_id=signal_id,
)
# failure → log only; selector/submit path continues unchanged
```

---

## Harness directory (downloaded for parent)

```text
/workspace/pr435/harness/
  test_p0_intelligence_postgres.py
  test_p0_intelligence_context_snapshots.py
  test_p0_watcher_recovery_execution_ownership.py
  test_p0_435_money_path_non_authority.py
  test_p0_435_behavioral_matrix.py
  test_p0_435_aapl_opening_replay_shape.py
  test_p0_435_market_structure_freeze.py
  test_p0_435_canonical_identity.py
  test_p0_435_fvg_as_of_cache.py
  test_p0_435_september_live_cohort.py
  september_live_cohort_202609.json
  ap_execution_core_breach_handoff.patch
```

## Success criteria for this investigation

- [x] Plan written to `/workspace/pr435/REMAINING_EVIDENCE_PLAN.md`
- [x] Harness copies on disk under `/workspace/pr435/harness/`
- [x] No push / no clone / no merge
