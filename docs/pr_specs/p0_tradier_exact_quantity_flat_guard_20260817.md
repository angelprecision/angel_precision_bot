# P0 SPEC — Exact-contract quantity must never be coerced into broker-flat truth

## Status

**IMPLEMENTED / PENDING INDEPENDENT REVIEW AND MERGE AUTHORIZATION.**

This is a forward-fix on top of #478 (merged, `main@37f61d0` via commit
`9a4287a`). #478 remains correct and is not reverted or rewritten by this
spec or its implementing PR — see "Relationship to #478" below.

Base: `main@37f61d004b44bfebd6708680b3261f90e528e0d5`.

Implementing branch: `spec/p0-tradier-exact-quantity-flat-guard-20260817`.

Independent review and Angel's explicit merge authorization are required
before merge. This status reflects that the implementation, fail-first
proof, and regression coverage are complete — it does not itself authorize
merge or deployment.

## Relationship to #478

#478 (spec: `p0_broker_position_unavailable_not_flat_20260815.md`) fixed
the case where the broker is **unavailable or the payload is structurally
malformed** (transport failure, HTTP error, missing keys, wrong types,
non-finite quantity) — those cases must never be swallowed into `[]` and
must never resolve as fresh authoritative flat.

This spec fixes a narrower, distinct case that #478 did not cover: the
broker responds **successfully** and the payload is **structurally
well-formed**, but the **quantity value itself** for the exact-OCC-matched
row is untrustworthy — boolean, fractional, negative, or an explicit zero.
#478's adapter accepted any finite `float()` value; this spec tightens that
to reject bool/fractional/negative at the adapter boundary and to treat an
explicit zero on an exact-match row as a resolver-level conflict rather
than authoritative flat.

All of #478's original protections remain intact and are covered by its
own still-green, unmodified test suite
(`tests/test_p0_broker_position_unavailable_not_flat.py`, 69/69 passing
throughout this work):

- broker transport failures (401/403/429/5xx, timeouts, connection errors);
- malformed positions containers (missing `positions` key, missing
  `position` key, wrong types);
- missing `quantity` key;
- unparseable quantity;
- NaN/inf quantity;
- successful snapshot with the exact OCC contract absent = authoritative
  flat (required for stale synthetic-position cleanup).

## Proven production defect

Current production path (unchanged from #478):

```text
ap/brokers/tradier.py::TradierBroker.list_positions()
  -> ap/exit_safety.py::resolve_exit_broker_truth()
  -> ap/order_state_machine.py::APOrderStateMachine.submit_exit()
```

Before this fix, `TradierBroker.list_positions()` accepted any finite
`float()` as a valid quantity:

```python
quantity = float(p["quantity"])
if not math.isfinite(quantity):
    raise ...
```

That accepted all of the following as believable broker position truth:

```text
quantity = False       # bool is numeric in Python: float(False) == 0.0
quantity = 0.5          # fractional — not a valid whole-contract count
quantity = -0.5         # fractional AND negative
quantity = -1           # negative — broker exposure, not flatness
```

Downstream, `resolve_exit_broker_truth()` routed the normalized quantity
through `_extract_long_position_qty()`, which collapsed `None` (missing),
negative, and short-side quantity to `0`:

```python
if qty is None:
    return 0
if qty < 0:
    return 0
if "short" in side_text:
    return 0
return int(qty)
```

Because that `0` return value is indistinguishable from a genuine
zero-contribution, and because the row still **matched the exact target
contract**, `resolve_exit_broker_truth()` treated it as a confirmed
exact-match snapshot:

```text
broker_truth_open_qty = 0
is_fresh_exact = True
```

`APOrderStateMachine.submit_exit()` could then mark the local position
`CLOSED` under `SYNTHETIC_POSITION_STALE_BROKER_FLAT` while the broker
still held (or held in a conflicting direction/shape) the contract. A
malformed, negative, boolean, fractional, or suspiciously-explicit-zero
quantity value could manufacture false broker-flat truth exactly the way
#478 was written to prevent for transport/structural failures — just via a
different mechanism (value-level, not shape-level).

A fifth case matters even without any malformation: an exact-contract row
**explicitly asserting `quantity == 0`** is itself suspicious. Real
Tradier broker-flat behavior for a closed/expired option is normally row
**absence** from a successful positions snapshot — not an explicit row
with `quantity: 0`. Treating an explicit zero-quantity exact-match row as
equivalent to contract absence conflates two different broker signals and
was never proven correct against actual Tradier semantics.

## Binding invariant

**BROKER QUANTITY UNCERTAINTY OR CONFLICT MUST NEVER BECOME ZERO EXPOSURE.**

For an exact OCC contract, quantity truth must preserve these distinct
states:

```text
VALID_LONG_OPEN        positive integral quantity
VALID_FLAT              explicit broker semantics that authoritatively
                         establish zero/no position (in practice: the
                         contract is ABSENT from a successful snapshot —
                         see "contract_absent_open_qty_zero" below)
CONFLICT_NEGATIVE        negative quantity / short exposure
MALFORMED_FRACTIONAL     non-integral option quantity
MALFORMED_BOOLEAN        bool masquerading as numeric
MALFORMED_NONFINITE      NaN / inf (already covered by #478)
MALFORMED_UNPARSEABLE    cannot parse (already covered by #478)
UNKNOWN                  broker unavailable / malformed response
                         (already covered by #478)
```

Only authoritative flat truth (contract absence from a successful
snapshot) may produce:

```text
broker_truth_open_qty = 0
is_fresh_exact = True
```

Any malformed, contradictory, unsupported, or ambiguous exact-contract
quantity — including an explicit `quantity == 0` on an exact-match row —
must produce:

```text
broker_truth_open_qty = None
is_fresh_exact = False
```

or raise at the adapter boundary so the resolver maps it to unknown.

## Implementation

### 1. Adapter boundary — `ap/brokers/tradier.py::TradierBroker.list_positions()`

After reading the raw quantity and before any numeric coercion:

- reject `bool` explicitly (`isinstance(raw_quantity, bool)`) — this must
  run before `float()`, since `float(False) == 0.0` would otherwise pass
  every subsequent check silently;
- parse via `float()`, raising `TRADIER_POSITIONS_PAYLOAD_MALFORMED` on
  failure (unchanged from #478);
- require `math.isfinite` (unchanged from #478);
- **new:** require `quantity.is_integer()`, raising
  `TRADIER_POSITIONS_PAYLOAD_MALFORMED: fractional option quantity`;
- **new:** require `quantity >= 0`, raising
  `TRADIER_POSITIONS_PAYLOAD_CONFLICT: negative option quantity` — a
  distinct error family from `MALFORMED`, since this is a conflict
  (exposure exists, in an unexpected direction) rather than an inability
  to parse.

`quantity == 0` is **not** rejected at the adapter boundary. The adapter
has no concept of "the exact contract currently being resolved against" —
it returns every position row in the account. The explicit-zero policy is
enforced downstream, in the resolver, which does have that context.

### 2. Resolver — `ap/exit_safety.py`

`_extract_long_position_qty()` rewritten to return `Optional[int]` with
three distinct outcomes instead of collapsing everything to `int`:

- a **positive int** — confirmed, valid, open long quantity;
- **`0`** — an explicit, well-formed zero. Returned distinctly from `None`
  so the caller can apply the "explicit zero on an exact match is not
  authoritative flat" policy;
- **`None`** — malformed / negative / non-integral / boolean / short-side
  / unparseable / missing quantity. The caller must never treat this as 0
  and must never sum it toward an open-quantity total.

This is defense-in-depth: for `TradierBroker`-sourced rows specifically,
`list_positions()` already rejects bool/fractional/negative/non-finite at
the adapter boundary, so those cases never reach this function for a
Tradier-sourced row. This function protects any other broker/raw-dict
source that does not enforce the same adapter-level contract.

`resolve_exit_broker_truth()`'s matching loop now separates matched rows
into `matched_rows` (positive long quantity) and `conflict_rows`
(`_extract_long_position_qty` returned `None`, **or** returned `0`). If
`conflict_rows` is non-empty for the exact-match contract, the **entire**
resolution becomes unknown — fail closed, consistent with the existing
per-row philosophy in `TradierBroker.list_positions()` (one bad row
invalidates the whole snapshot's truth for this contract rather than being
averaged away or silently dropped). This also covers the multi-row case:
if a contract has two matched rows (e.g. a lot split) and one is a
conflict, the valid row's quantity is not silently summed in isolation —
the whole resolution is unknown.

## Final quantity semantics (post-fix)

```text
CALL/PUT exact-match row, quantity = positive integer  -> VALID_LONG_OPEN
                                                            broker_truth_open_qty = sum
                                                            is_fresh_exact = True
CALL/PUT exact-match row, quantity = 0                 -> CONFLICT (explicit zero)
                                                            broker_truth_open_qty = None
                                                            is_fresh_exact = False
CALL/PUT exact-match row, quantity = negative           -> raises at adapter
                                                            (TradierBroker source)
                                                            broker_truth_open_qty = None
                                                            is_fresh_exact = False
CALL/PUT exact-match row, quantity = fractional          -> raises at adapter
                                                            (TradierBroker source)
                                                            broker_truth_open_qty = None
                                                            is_fresh_exact = False
CALL/PUT exact-match row, quantity = bool                -> raises at adapter
                                                            (TradierBroker source)
                                                            broker_truth_open_qty = None
                                                            is_fresh_exact = False
CALL/PUT contract absent from successful snapshot        -> VALID_FLAT
                                                            broker_truth_open_qty = 0
                                                            is_fresh_exact = True
                                                            (unchanged from #478 — required
                                                            for stale synthetic-position cleanup)
Broker unavailable / malformed payload / missing method   -> UNKNOWN
                                                            broker_truth_open_qty = None
                                                            is_fresh_exact = False
                                                            (unchanged from #478)
```

## Explicit non-goals

Do not change in this fix:

- broker transport/auth/HTTP failure handling (already correct per #478);
- malformed-container handling (already correct per #478);
- contract-absence-equals-flat semantics (already correct per #478,
  required for stale synthetic-position cleanup — preserved here);
- new ENTRY submit authority;
- new EXIT submit authority;
- new broker cancel authority;
- selector behavior;
- retry timing;
- `client_id` / `execution_mode` fencing;
- `proof_trades` mutation semantics;
- queue semantics;
- PAPER/LIVE taxonomy.

## Production file budget

- `ap/brokers/tradier.py` (adapter boundary hardening)
- `ap/exit_safety.py` (resolver + defense-in-depth helper hardening)

No other production file required a change. Every direct
`broker.list_positions()` caller was re-audited (see "Caller audit" below)
and confirmed to already fail closed on any exception — the increase in
raised-exception cases from the adapter fix does not change any caller's
flat-truth authority.

## Caller audit

All direct `list_positions()` callers, confirmed safe against the new
raised-exception cases (bool/fractional/negative quantity anywhere in the
account payload now raises where it previously did not):

1. **`ap/exit_safety.py::resolve_exit_broker_truth`** — exception ->
   `broker_truth_open_qty=None`, `is_fresh_exact=False`. No flat authority.
   (Primary consumer; this spec's resolver-side fix lives here.)
2. **`ap_exit_engine.py` broker position precheck** (`~line 7214`) —
   exception caught, logs `EXIT_UNSAFE_BROKER_TRUTH_UNAVAILABLE`, returns
   `False` (unavailable), continues with local engine state, never assumes
   flat. No change needed.
3. **`ap_reconciler.py` filled-order backfill** (`~line 2416`) — exception
   caught, `broker_truth_ok=False`, exposure stays `OPEN`/unmanaged. No
   change needed.
4. **`ap_reconciler.py::_safe_get_broker_positions`** (`~line 2893`) —
   swallows any exception to `[]`. Its sole consumer
   (`_handle_db_position_missing_at_broker`) is confirmed HOLD-only: it
   alerts (`RECONCILER_BROKER_FLAT_EXIT_FILL_UNRESOLVED`) and requires a
   three-pass ghost-tracker confirmation before even alerting further — it
   never mutates position status directly. Documented here as
   observation-only, consistent with the audit's P1/P2 allowance.
5. **`ap_reconciler.py` partial-close repair** (`~line 4298`) — exception
   caught, `broker_truth_available=False`, rows flagged for manual review
   "without changing status" (explicit in-code comment). No status
   mutation on broker-unavailable/conflict. No change needed.
6. **`app.py::nightly_reconcile`** (`~line 3957`) — exception caught
   per-runner, `{"ok": False, "error": ...}`. Pure reporting/alerting
   endpoint; no position mutation occurs in this function at all. No
   change needed.
7. **`ap/exit_autonomous_recovery.py`** (`~line 369`) — exception caught by
   a `try/except Exception: log.debug(...)` that swallows and falls
   through to `_mark_replacement_safe(...)`, **not**
   `exit_engine.mark_position_closed(...)`. Confirmed: the exception path
   never reaches the close call — it only reaches `mark_position_closed`
   inside the `try` block's success path, when `_contract_held` is
   explicitly proven `False` from a successfully-parsed broker snapshot.
   No change needed.

No caller converts a newly-propagated quantity-conflict exception into
authoritative flatness.

## Diagnostics

New/tightened deterministic reason codes (adapter boundary,
`ap/brokers/tradier.py`):

```text
TRADIER_POSITIONS_PAYLOAD_MALFORMED: boolean quantity
TRADIER_POSITIONS_PAYLOAD_MALFORMED: fractional option quantity
TRADIER_POSITIONS_PAYLOAD_CONFLICT: negative option quantity
```

Resolver-level audit status (`ap/exit_safety.py`,
`resolve_exit_broker_truth`'s `audit["snapshot_status"]`):

```text
exact_match_conflict_unknown_quantity   # new: conflict_rows non-empty
                                         # (explicit zero and/or malformed
                                         # quantity on an exact-match row)
```

Existing statuses (`exact_match`, `contract_absent_open_qty_zero`,
`broker_positions_unavailable`, `broker_positions_error`,
`broker_positions_malformed`) are unchanged.

## Money-path audit

- Live behavior: **YES, protective exit-truth correction** — this changes
  what counts as trustworthy broker exposure evidence for exit decisions.
- Flag-off or active: active resolver/adapter path; correction does not
  rely on a new flag.
- Direct broker submit/cancel: **zero.** Neither `ap/brokers/tradier.py`
  nor `ap/exit_safety.py` calls broker submit or cancel directly; both
  only read/interpret positions. Verified explicitly in tests via
  `broker.submit_order.call_count == 0` / `broker.cancel_order.call_count
  == 0` and, in `submit_exit` integration tests, that the exit still
  reaches broker (fail-open on unknown truth) rather than blocking or
  fabricating a close.
- Orders/positions/proof_trades/queue: no new direct mutation architecture.
- `client_id`/`execution_mode`: unchanged and exact — preserved through
  every `submit_exit` integration test.
- PAPER/LIVE pollution: none.
- Could this make Jason trade junk? The defect could cause a still-open
  LIVE position to be marked `CLOSED` under
  `SYNTHETIC_POSITION_STALE_BROKER_FLAT` on a malformed, negative, or
  suspiciously-explicit-zero quantity value. This fix makes that
  impossible — every such case now resolves as unknown broker truth
  instead of manufactured flat.

## Required tests — status

All required fail-first and regression coverage implemented in
`tests/test_p0_tradier_exact_quantity_flat_guard.py` (33 tests):

- Section A — adapter boundary rejects bool/fractional/negative, accepts
  explicit zero and valid integers (including numeric-string `"4"`).
- Section B — resolver never manufactures `broker_truth_open_qty=0` /
  `is_fresh_exact=True` from a conflicting exact-match row; preserves
  valid-quantity and contract-absent-flat resolution; multi-row
  conflict-poisons-the-whole-match case.
- Section C — `_extract_long_position_qty` defense-in-depth, direct.
- Section D — full `submit_exit()` integration: zero `CLOSED` writes, zero
  `SYNTHETIC_POSITION_STALE_BROKER_FLAT` transitions, broker submission
  still proceeds (fail-open on unknown truth) for every malformed/
  conflicting case, and proceeds identically to before for valid
  quantities.
- Section E — zero direct broker submit/cancel authority proof.

Two pre-existing tests in `tests/test_p0_exit_closed_guard_and_circuit_
breaker.py` directly asserted the pre-fix (buggy) behavior — that an
exact-match `quantity=0` row produces `SYNTHETIC_POSITION_STALE_BROKER_
FLAT` and marks the position `CLOSED`. Those two tests were rewritten
(`test_broker_exact_match_zero_quantity_no_longer_flat_hits_circuit_
breaker`, `test_broker_exact_match_zero_quantity_no_longer_flat_proceeds_
fail_open`) to assert the corrected invariant: broker truth resolves as
unknown/conflicting, never flat, for that row shape. All other tests in
that file were unaffected and required no changes.

## Validation results

- `tests/test_p0_broker_position_unavailable_not_flat.py` (#478's own
  suite): 69/69 passed, unmodified.
- `tests/test_p0_tradier_exact_quantity_flat_guard.py` (this fix's
  dedicated suite): 33/33 passed.
- `tests/test_p0_exit_closed_guard_and_circuit_breaker.py`: 25/25 passed
  (2 tests updated per above).
- Broader validation sweep (exit safety, synthetic broker-flat/circuit-
  breaker, reconciler broker-truth, autonomous exit recovery, broker-open
  protective monitoring — `test_p0_broker_open_protective_monitoring.py`,
  `test_p0_exit_circuit_breaker_repair.py`,
  `test_p0_exit_closed_guard_and_circuit_breaker.py`,
  `test_p0_exit_engine_broker_truth.py`,
  `test_p0_reconciler_external_close_broker_truth.py`,
  `test_pr81_broker_truth_wins.py`, `test_p0_308_amendments.py`,
  `test_p0_broker_owned_exit_requested_recovery.py`,
  `test_p0_underlying_authoritative_exit_geometry.py`,
  `test_p1_reconciler_broker_reject_reason.py`): 450 passed, 1 skipped, 8
  failed — all 8 failures confirmed identical (same test, same failure
  reason) on a clean, unmodified `main@37f61d0` checkout with this fix's
  changes stashed out; they are pre-existing and unrelated to this work.
- Full CI-exact P0 Regression Suite (116 files, including this fix's new
  dedicated test file added to `.github/workflows/p0_regression.yml`)
  against a fresh local Postgres 16 instance: 3910 passed, 28 skipped, 0
  failed.

## Delivery

New forward-fix branch/PR from current main. #478 is not reverted or
rewritten. Do not merge, deploy, or mark ready for review without Angel's
explicit authorization.
