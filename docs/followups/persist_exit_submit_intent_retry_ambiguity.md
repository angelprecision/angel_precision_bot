# P2 follow-up: `persist_exit_submit_intent` idempotent retry on commit-ack loss

> **Filed:** 2026-08-09 from PR #425 fifth-round independent audit  
> **Head at filing:** `66d1072c1b0e4ef6161a7dae8b38a08ebc33f75c`  
> **Priority:** P2 (real defect, low frequency, existing safety net)  
> **Do not** bundle this fix into any other PR. See "Scope discipline" below.

---

## Summary

`OrderStateMachine.persist_exit_submit_intent` (`ap/order_state_machine.py:2180`) is not idempotent under `run_with_retry` retries. On a narrow network-partition window between Postgres commit and ACK, a successful CAS is retried, the second attempt sees its own committed evidence, refuses, and the method returns `False` — even though we already own the fence.

The caller in `submit_exit` (`ap/order_state_machine.py:6178`) treats `False` as `EXIT_SUBMIT_INTENT_FENCE_LOST` and aborts the exit. No broker POST happens on the false-negative path, so **this is not a correctness bug**. It is a liveness defect: a legitimate exit gets abandoned and requires reconciliation.

Discovered during the fifth independent audit round of PR #425 (head `66d1072`). Not blocking that PR — filed as a follow-up per scope discipline.

## Impact

- **Not a money-path defect.** No broker POST, no position mutation, no proof-trade write happens on the false-negative branch.
- **Liveness only.** A valid exit returns `EXIT_SUBMIT_INTENT_FENCE_LOST` with `reconciliation_required: True`. The exit-decision generation claim goes to `AMBIGUOUS` or `STALE_CLAIM_RECONCILING`.
- **Existing safety net catches it.** `reconcile_stale_exit_generation_claim` in `ap/exit_decision_idempotency_guard.py` is designed exactly for this: it re-reads the durable row, matches the exact identity, and either promotes to `BROKER_OWNED` (if evidence exists) or `RELEASED_NO_SUBMIT` (if retirement CAS succeeds).
- **Real-world frequency:** rare. Requires the specific network-partition-mid-commit scenario. Never observed in production. Would matter on a portfolio scale, over a long time horizon, on a bad connectivity day.

## Root cause

`persist_exit_submit_intent` runs its CAS inside `run_with_retry`:

```python
# ap/order_state_machine.py:2244
try:
    return bool(run_with_retry(_persist) > 0)
except Exception as exc:
    ...
    return False
```

The CAS `WHERE` clause requires `COALESCE(meta->>'submit_intent_at','')=''`, `COALESCE(meta->>'broker_submit_key','')=''`, empty `broker_order_id`, null `submitted_ts`, and no quarantine flags. Once we successfully commit the CAS in iteration 1, those preconditions no longer hold — because **we** just wrote them.

`run_with_retry` (`ap/db.py:95`) retries on `psycopg2.OperationalError`, `InterfaceError`, `PoolError`, `DeadlockDetected`, `SerializationFailure`, and `LockNotAvailable`. The failure mode:

1. Iteration 1: `_persist()` runs, CAS executes successfully (rowcount=1), `with conn()` block exits.
2. `_ConnWrapper.__exit__` calls `db_conn.commit()` at `ap/db.py:175`.
3. Postgres commits the transaction server-side. Network partition drops the ACK.
4. psycopg2 raises `OperationalError`. `run_with_retry` catches, sleeps, retries.
5. Iteration 2: `_persist()` runs, CAS refuses because `meta.submit_intent_at` is now populated (by our own iteration 1 that actually committed).
6. `rowcount=0`. `bool(0 > 0)` returns `False`.
7. `submit_exit` returns `EXIT_SUBMIT_INTENT_FENCE_LOST`. Legitimate exit aborted.

The rowcount=0 result is genuinely ambiguous at that point: either we lost a race to another worker, OR we already own the fence from a previous iteration whose commit-ack we never received. The current code cannot distinguish.

## Proposed fix shape

Make the operation idempotent by adding a read-back on CAS refusal. If the row's current `meta.broker_submit_key` and `meta.broker_submit_payload_hash` match the values **we would have written**, we already own it — return `True`.

**Sketch** (do not implement from this alone — audit and write tests first):

```python
def _persist():
    with conn() as c:
        cur = c.execute("UPDATE orders SET ... WHERE ...", (params...))
        rowcount = int(getattr(cur, "rowcount", 0) or 0)
        if rowcount > 0:
            return True
        # rowcount == 0 is ambiguous: race lost OR we already own it.
        # Read the row and check whether OUR exact submit intent is present.
        row = c.execute(
            "SELECT meta FROM orders WHERE local_order_id=%s AND client_id=%s",
            (local_order_id, self.client_id),
        ).fetchone()
        if not row:
            return False
        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                return False
        if not isinstance(meta, dict):
            return False
        return (
            meta.get("broker_submit_key") == submit_key
            and meta.get("broker_submit_payload_hash") == payload_hash
            and meta.get("current_owner") == f"broker_submit:{submit_key}"
        )
```

**Critical care points when implementing:**

1. The read-back must happen **inside the same `_persist()` call** so it's inside `run_with_retry`'s exception boundary and re-runs cleanly if the read itself fails.
2. Do NOT compare only `broker_submit_key` — the payload hash and current_owner disambiguate against a different worker that legitimately won for a different payload.
3. The `_exit_submit_intent_proven()` re-read at `submit_exit:6196` is a separate proof and should NOT be relied on to catch this — by the time we get there, we've already returned `EXIT_SUBMIT_INTENT_FENCE_LOST`. The fix must live inside `persist_exit_submit_intent` itself.
4. `retire_unsubmitted_exit_intent` (`ap/order_state_machine.py:2142`) is theoretically vulnerable to the same class of bug (successful retire → commit-ack lost → retry sees status already ERROR → refuses). **Do not fix both in the same PR.** Retirement CAS ambiguity has different semantics (retirement is destructive; wrong resolution is more dangerous). Handle it in a separate follow-up if it turns out to matter.

## Test to write

Simulate commit-ack loss deterministically. Sketch:

```python
def test_persist_exit_submit_intent_survives_commit_ack_loss(monkeypatch):
    """A retry after commit-ack loss must recognize our own prior commit."""
    # Real Postgres schema, real CAS.
    call_count = {"n": 0}
    real_conn = osm_module.conn

    def _flaky_conn():
        cm = real_conn()
        original_exit = cm.__exit__
        def _exit(exc_type, exc_val, exc_tb):
            call_count["n"] += 1
            result = original_exit(exc_type, exc_val, exc_tb)
            if call_count["n"] == 1:
                # Simulate commit succeeding server-side but ACK loss.
                raise psycopg2.OperationalError("simulated commit ack loss")
            return result
        cm.__exit__ = _exit
        return cm

    monkeypatch.setattr(osm_module, "conn", _flaky_conn)

    ok = osm.persist_exit_submit_intent(...)
    assert ok is True  # not False — we own the fence from iteration 1
    # Prove exactly one durable submit_intent_at write happened.
```

Also verify the negative case: a different worker's `broker_submit_key` in the row must correctly return `False`.

## Why P2, not P0 or P1

- **P0** would mean money-path or safety. This is neither.
- **P1** would mean live production impact today. The reconciler catches every case. No observed occurrence.
- **P2** is correct: real defect, low frequency, existing safety net, worth fixing when convenient.

## Scope discipline

Fix this in **its own PR**. Do not bundle it with:

- Any #425 amendment (that scope contract is closed).
- Any other exit-lifecycle change.
- Any retirement CAS work.

The PR should touch only `ap/order_state_machine.py` (the `_persist` closure) and add the one dedicated test. Everything else remains untouched.

## Related

- PR #425 (broker-owned EXIT_REQUESTED recovery) — the code path this defect lives in
- `ap/db.py:95` `run_with_retry` — retry semantics
- `ap/exit_decision_idempotency_guard.py` `reconcile_stale_exit_generation_claim` — current safety net
- `ap/schema_attestation.py` `REQUIRED_SCHEMA["exit_decision_generation_claims"]` — no schema change needed for the fix

## Acceptance criteria

- [ ] `persist_exit_submit_intent` returns `True` when its own prior commit succeeded server-side but ACK was lost
- [ ] `persist_exit_submit_intent` returns `False` when a different worker's evidence occupies the row
- [ ] Dedicated test simulates commit-ack loss deterministically and passes
- [ ] No other production file touched
- [ ] Exact-head CI run green on the final SHA
- [ ] Independent audit at exact head signs off before merge
