# P0 SPEC — Exact-contract quantity must never be coerced into broker-flat truth

## Status

**IMPLEMENTED / PENDING INDEPENDENT REVIEW AND MERGE AUTHORIZATION.**

This is a forward-fix on top of #478 (merged, `main@37f61d0` via commit
`9a4287a`). #478 remains correct and is not reverted or rewritten by this
spec or its implementing PR — see "Relationship to #478" below.

Branch was subsequently rebased onto post-#479 `main` with zero conflicts.
Current base: `main@d0d37e79ae698e604eb8080065d2314b161de351` (merge of
PR #479, `spec/p0-watcher-zero-quote-breach-guard-20260815` — watcher
zero-quote-breach guard work, unrelated territory to this spec; the
rebase touched no shared files).

Implementing branch: `spec/p0-tradier-exact-quantity-flat-guard-20260817`.

This spec now covers three layers of work, described separately below:

1. **Original fix** — adapter/resolver quantity-conflict hardening
   (`ap/brokers/tradier.py`, `ap/exit_safety.py`).
2. **Amendment 1** — autonomous-recovery false-flat seam. An independent
   caller audit found that `ap/exit_autonomous_recovery.py::recover_exit_
   position()` re-implemented broker-position quantity semantics locally
   instead of consuming the resolver above, reopening the same class of
   defect through a second code path. See "Amendment 1" below.
3. **Amendment 2** — missing-contract-identity seam. A further audit found
   that an empty/unproven contract identity could be normalized to `""`
   and then treated as "absent from snapshot" = authoritative flat, even
   though no exact contract was ever established. See "Amendment 2" below.

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
Missing/empty/unproven contract identity                  -> UNKNOWN
                                                            broker_truth_open_qty = None
                                                            is_fresh_exact = False
                                                            (Amendment 2 — never authoritative
                                                            flat regardless of snapshot state)
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

- `ap/brokers/tradier.py` (adapter boundary hardening — original fix)
- `ap/exit_safety.py` (resolver + defense-in-depth helper hardening —
  original fix; missing-contract-identity guard — Amendment 2)
- `ap/exit_autonomous_recovery.py` (negative-proof block now consumes the
  canonical resolver instead of re-implementing quantity semantics —
  Amendment 1; defense-in-depth contract-identity check — Amendment 2)

Every direct `broker.list_positions()` caller was re-audited (see "Caller
audit" below). The original audit incorrectly cleared
`ap/exit_autonomous_recovery.py` as needing no change — that error is
corrected in the caller-audit entry above and fixed by Amendment 1. All
other callers remain confirmed to fail closed on any exception.

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
7. **`ap/exit_autonomous_recovery.py`** (`~line 369`, original audit) —
   **STALE / CORRECTED BY AMENDMENT 1.** The original caller audit stated
   this call site required no change, on the theory that its
   `try/except Exception: log.debug(...)` swallowed exceptions and fell
   through to `_mark_replacement_safe(...)` rather than
   `mark_position_closed(...)`. That analysis missed a second, independent
   defect in the **success path** of the same `try` block: it called raw
   `broker.list_positions()` and evaluated
   `int(p.get("quantity") or 0) != 0` directly, which — for an exact-match
   row with `quantity=0`/`False`/fractional/negative — collapsed to
   `_contract_held=False` and called `mark_position_closed()`, exactly the
   false-flat defect this spec exists to prevent, via a second
   independent code path that bypassed the resolver entirely. The
   exception path was also wrong: it fell through to
   `_mark_replacement_safe()`, letting a failed broker query independently
   authorize replacement. **This call site required a change and received
   one — see "Amendment 1" below.**

No caller converts a newly-propagated quantity-conflict exception into
authoritative flatness (true of the adapter/resolver layer as originally
audited; Amendment 1 extends the same guarantee to the autonomous-recovery
caller that the original audit incorrectly cleared).

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

## Amendment 1 — autonomous-recovery false-flat seam

### Defect

`ap/exit_autonomous_recovery.py::recover_exit_position()`'s negative-proof
block (reached when no pending broker order id and no matching open
sell-to-close order exists) independently re-implemented broker-position
quantity semantics instead of consuming `resolve_exit_broker_truth()`:

```python
_broker_positions = broker.list_positions()
_contract_held = any(
    str(p.get("symbol") or "").upper() == str(contract or "").upper()
    for p in (_broker_positions or [])
    if int(p.get("quantity") or 0) != 0
)
if not _contract_held and contract:
    exit_engine.mark_position_closed(...)
```

For an exact-match row with `quantity=0`/`False`/`0.5`/`-1`/`-0.5`,
`int(p.get("quantity") or 0) != 0` evaluates falsy, `_contract_held`
becomes `False`, and `mark_position_closed()` is called — the same
false-flat defect this spec fixes at the adapter/resolver layer,
reintroduced via a second independent code path that bypassed the
resolver entirely.

The exception path was also unsafe:

```python
except Exception as _bp_exc:
    log.debug(...)
# falls through to _mark_replacement_safe(...)
```

A failed `broker.list_positions()` call let execution fall through to
`_mark_replacement_safe()`, letting unknown broker truth independently
authorize replacement.

### Fix

The negative-proof block now calls `resolve_exit_broker_truth()` and
applies a three-branch decision table:

```text
broker_truth_open_qty is None                -> NOOP / HOLD
broker_truth_open_qty == 0, is_fresh_exact   -> MARKED_CLOSED (authoritative flat)
broker_truth_open_qty > 0                    -> replacement-safe (position held)
```

Only one canonical definition of "broker flat" now exists in the
codebase; autonomous recovery consumes it rather than re-deriving it.

### Tests

`tests/test_p0_exit_autonomous_recovery_quantity_guard.py` — 15 tests,
covering: exact-OCC quantity=0/False/0.5/-1/-0.5, broker exception
(RuntimeError plus ConnectionError/TimeoutError/ValueError/OSError
variants), valid positive quantity (position remains held), authoritative
flat on genuine absence (both non-empty-snapshot and empty-snapshot
variants), and a static-analysis guard against new broker submit/cancel
authority. Fail-first: 11/15 failed against pre-fix code, confirmed before
any production line was touched.

## Amendment 2 — missing-contract-identity seam

### Defect

An independent audit found that Amendment 1's centralization through
`resolve_exit_broker_truth()` removed an older implicit safety property:
authoritative broker-flat cleanup required a usable contract identity.
`_position_contract(pos)` can return an empty string when a position's
`option_symbol`/`contract`/`symbol` fields are all unset. Before this
amendment, `resolve_exit_broker_truth()` would normalize that to
`normalized_contract = ""`, no row in any snapshot would match the empty
string, and the "no matched rows" branch would manufacture authoritative
flat truth (`broker_truth_open_qty=0`, `is_fresh_exact=True`) for a
contract that was never actually identified — even against a non-empty,
otherwise-valid broker snapshot.

Binding invariant: **absence can only prove flatness when we know exactly
which broker contract we were trying to find.** Missing/unproven contract
identity is UNKNOWN truth, never broker-flat truth.

### Fix

Two layers, per the amendment's explicit "do not rely solely on the
resolver guard" instruction:

1. **`ap/exit_safety.py::resolve_exit_broker_truth()`** — immediately
   after contract normalization, before any broker call: if
   `normalized_contract` is empty, return
   `{"broker_truth_open_qty": None, "is_fresh_exact": False, "audit": {..., "snapshot_status": "contract_identity_unavailable"}}`
   without ever calling `broker.list_positions()`.
2. **`ap/exit_autonomous_recovery.py::recover_exit_position()`** —
   defense-in-depth: before calling the resolver at all, if
   `contract` (from `_position_contract(pos)`) is falsy, return
   `RecoveryAction("NOOP", "broker_contract_identity_unknown_hold", ...)`.

Both guards are intentionally redundant — the amendment spec requires the
caller not depend solely on the resolver-level fix.

### Tests

`tests/test_p0_missing_contract_identity_never_flat.py` — 15 tests:

- **Case A** (3 parametrized + 1 empty-snapshot variant) — resolver with
  empty/`None`/whitespace-only contract, against both a non-empty and an
  empty broker snapshot: `broker_truth_open_qty is None`,
  `is_fresh_exact is False`,
  `audit["snapshot_status"] == "contract_identity_unavailable"`.
- **Case B** (2 variants) — `recover_exit_position()` with an unestablished
  contract identity, against both a non-empty and an empty broker
  snapshot: action is `NOOP`, zero `mark_position_closed()` calls, zero
  replacement authorization.
- **Case C** — valid known OCC contract genuinely absent from a successful
  non-empty snapshot: still resolves authoritative flat and autonomous
  recovery still marks it closed (regression guard on existing working
  behavior).
- **Case D** — valid known OCC contract with positive quantity: still
  resolves open, never falsely closed (regression guard).
- **Case E** (5 parametrized quantity variants + 1 broker-exception case)
  — all existing Amendment-1/original malformed-quantity protections
  re-verified intact after the contract-identity hardening.
- **Normal-path preservation** — valid contract + positive quantity: exact
  resolver truth, position stays open, and a static-analysis check
  confirms zero new broker submit/cancel authority in either file touched
  by this amendment.

Fail-first: 6/15 failed against pre-Amendment-2 code (the Case A and
Case B tests specifically; Cases C/D/E and the normal-path test were
written to already pass, proving they were unaffected by the fix).

## Amendment 3 — missing contract identity must never become wildcard broker-order identity

### Defect

An independent review of Amendment 2 found that its HOLD guard, while
correctly preventing missing contract identity from becoming authoritative
broker-**flat** truth, executed too late to prevent a second, more severe
consequence: missing contract identity becoming a **wildcard match against
unrelated broker orders**.

`ap/exit_autonomous_recovery.py::recover_exit_position()` calls
`_matching_open_exit_orders(broker, contract, ...)` at two call sites
**before** any contract-identity guard runs:

1. the terminal-pending-broker-id path, scanning for a "different live
   exit on the same contract" before authorizing replacement;
2. the generic/missing-broker-id path, scanning for any matching live
   exit order — also reached when a pending broker id was set but its
   exact-order-id lookup failed/was unavailable.

`_matching_open_exit_orders()` filtered rows with:

```python
if contract and _contract(raw) != contract:
    continue
```

When `contract == ""`, this condition is always `False` — **no row is ever
filtered out by contract**. Every exit-like open order in the account,
belonging to any position or any client, became a "match" regardless of
which position was actually being recovered.

Consequences, all reachable with an unproven contract identity:

- a single unrelated live sell-to-close order could be **adopted** for
  the wrong position via `set_pending_exit_order()`;
- multiple unrelated live sell-to-close orders could be **canceled** via
  `_cancel_order_with_proof()` — broker cancel authority exercised
  against orders never proven to belong to this position;
- a terminal-pending-broker-id recovery could adopt an unrelated broker
  order as the position's "different live exit still open".

Amendment 2's HOLD guard (in the negative-proof block) only covers the
case where the scan already ran and returned zero matches — it could not
prevent a wildcard scan from finding and acting on unrelated real orders
in the first place.

### Binding invariant

**UNPROVEN CONTRACT IDENTITY MUST NEVER AUTHORIZE:**
wildcard broker-order matching, broker-order adoption, broker cancel,
replacement-safe, or broker-flat close.

Exact broker-order-id truth (the `pending_broker_id` path, when the exact
order lookup succeeds) remains independently authoritative and is
unaffected — see cases G/H below.

### Fix

One production file, additive only (`ap/exit_autonomous_recovery.py`,
+61/-0 lines across three changes):

1. **Helper-level defense in depth** — `_matching_open_exit_orders()` now
   returns `[]` immediately if `contract` is empty/falsy, before scanning
   any broker order. This alone is *not* sufficient at every call site
   (see below), so it is explicitly documented as one layer among several.
2. **Terminal-pending-broker-id path** — before calling
   `_matching_open_exit_orders(broker, contract, exclude_broker_id=...)`,
   an explicit `if not contract:` check returns `NOOP` /
   `broker_contract_identity_unknown_hold`. This is necessary in addition
   to the helper-level guard: without it, an empty scan result would fall
   through to `len(other_matches) == 0`, which authorizes
   `_mark_replacement_safe()` from terminal status alone — exactly the
   "duplicate scan cannot be performed, so assume it's safe" failure mode
   this amendment forbids.
3. **Generic/missing-broker-id path** — the same explicit `if not
   contract:` HOLD check runs before `_matching_open_exit_orders(broker,
   contract)`, for the same reason: an empty result must not be
   reinterpreted as "the scan ran and found nothing" when the scan never
   meaningfully executed. This call site is reached both when no pending
   broker id exists at all, and when a pending broker id existed but its
   exact-order-id lookup failed (`raw` falsy) — both scenarios needed the
   same guard.

Amendment 2's original negative-proof HOLD guard remains in place as a
fourth, now-redundant-but-harmless layer, consistent with the
defense-in-depth philosophy both amendments were written under.

### Normal-path preservation

Explicitly re-verified unaffected:

- valid contract + one matching live exit → existing `RECOVERED_BROKER_ID`
  broker-id recovery unchanged (case E);
- valid contract + multiple matching live exits → existing bounded
  cancel-then-replacement-safe behavior unchanged (case F);
- missing contract + exact pending broker id confirmed `OPEN` → exact
  broker-id truth remains valid, unaffected by contract-based scanning
  being unavailable (case G);
- missing contract + exact pending broker id confirmed `FILLED` → exact
  broker fill truth remains valid and the position still closes (case H).

### Tests

`tests/test_p0_missing_contract_identity_no_wildcard_match.py` — 9 tests:

- **Case A** — missing contract, no pending broker id, one unrelated live
  STC order → `NOOP`, zero adoption, zero replacement authorization.
- **Case B** — missing contract, no pending broker id, two unrelated live
  STC orders → `NOOP`, `cancel_order` call count `0`, zero replacement.
- **Case C** — missing contract, terminal pending broker id, one
  unrelated live STC → zero adoption, zero replacement-safe, `NOOP`.
- **Case D** — missing contract, pending-broker-id lookup unavailable
  (broker.get_order raises), unrelated live STC → zero adoption, `NOOP`.
- **Case E** — valid contract, one matching exit → unchanged
  `RECOVERED_BROKER_ID` behavior.
- **Case F** — valid contract, two matching exits → unchanged bounded
  cancel-then-replacement-safe behavior.
- **Case G** — missing contract, exact pending id confirmed `OPEN` →
  unchanged `CONFIRMED_OPEN` behavior.
- **Case H** — missing contract, exact pending id confirmed `FILLED` →
  unchanged `MARKED_CLOSED` behavior.
- **Defense-in-depth** — direct unit test of `_matching_open_exit_orders()`
  confirming `[]` for both `""` and `None` contract.

Fail-first: 5/9 failed against pre-Amendment-3 code — Cases A, B, C, D, and
the defense-in-depth helper test. Cases E/F/G/H were written to already
pass, proving normal-path behavior was unaffected before the fix was even
applied. Confirmed:

- Case A pre-fix: `action=RECOVERED_BROKER_ID`, unrelated order adopted.
- Case B pre-fix: `action=REPLACEMENT_SAFE`, unrelated orders canceled via
  `_cancel_order_with_proof`.
- Case C pre-fix: unrelated order adopted as "different live exit still
  open".
- Case D pre-fix: unrelated order adopted despite the pending-id lookup
  having failed.

Post-fix: 9/9 pass.

## Amendment 4 — exact-contract authority hardening + real Tradier order shape + signed-quantity isolation

Independent audit against the exact head of Amendment 3 found three
further merge blockers, all confirmed against the live code before any
change was made. All three amendments 1-3 fixes were explicitly preserved
and re-verified; none were reverted or weakened.

### Blocker 1 — non-empty invalid contract identity could still become authoritative broker flat

**Defect.** `_normalize_contract()` only strips/uppercases/removes spaces
— it never proved the value was a complete, exact OCC option symbol.
Amendment 2's `if not normalized_contract` guard only caught the *empty*
case. A non-empty but malformed/incomplete token — `"UNKNOWN"`, a bare
underlying ticker like `"SMCI"`, a placeholder like `"DEFERRED:SMCI"`, a
truncated or malformed OCC shape — passed that guard, then matched zero
rows in any successful broker snapshot, manufacturing authoritative
broker-flat truth for a contract that was never actually proven to exist.

**Fix.** One canonical exact-OCC validator, `ap/exit_safety.py::
_normalize_exact_occ_contract()`, built on the regex
`^[A-Z0-9.]{1,6}\d{6}[CP]\d{8}$` (root symbol, 6-digit YYMMDD expiration,
C/P side, 8-digit strike). `resolve_exit_broker_truth()`'s identity gate
now uses this instead of the bare normalizer, and distinguishes
`contract_identity_unavailable` (empty/whitespace) from
`contract_identity_invalid` (non-empty but not a proven exact OCC) in the
audit. `ap/exit_autonomous_recovery.py::_position_contract()` was
rewritten to use the same validator — this single change propagates the
protection through every existing `if not contract:` HOLD guard already
in place from Amendments 2 and 3, with no new guard code required.

### Blocker 2 — autonomous recovery didn't parse real Tradier option-order identity

**Defect.** `ap/exit_autonomous_recovery.py::_contract(raw)` checked
`raw.get("contract") or raw.get("symbol") or raw.get("option_symbol") or
raw.get("instrument")`. Real Tradier option orders carry `symbol` =
underlying ticker (e.g. `"SMCI"`) and `option_symbol` = the exact OCC
contract (e.g. `"SMCI260626P00032500"`) as two *different* fields. Because
`symbol` was checked first and is truthy, `_contract(raw)` returned the
underlying instead of the OCC contract for any production-shaped order
row — an already-live same-contract exit order could be missed entirely,
risking a duplicate exit submission.

**Fix.** `_contract(raw)` now iterates `("contract", "option_symbol",
"symbol", "instrument")` but only *accepts* a candidate field's value if
it proves out as a complete exact OCC symbol via the same validator from
Blocker 1 — a bare underlying ticker fails that proof and is skipped
rather than blindly accepted via short-circuit `or`.

### Blocker 3 — valid signed quantity on an unrelated position poisoned the whole account snapshot

**Defect.** `ap/brokers/tradier.py::TradierBroker.list_positions()`
raised `TRADIER_POSITIONS_PAYLOAD_CONFLICT` for the *entire* account
snapshot the moment any row anywhere carried a negative quantity. Tradier
position quantity is signed broker data — negative legitimately
represents a short position — and this method reads the whole brokerage
account in one call. One completely unrelated legitimate short position
(a different underlying, or a short option AP never opened) made broker
truth unavailable for the actual AP target contract being resolved, even
though the target's own row was perfectly valid.

**Fix.** Removed the blanket negative-quantity raise. Genuine structural
payload garbage — boolean, missing quantity, unparseable, non-finite,
fractional — is still rejected globally, since those represent real
malformed data regardless of which row carries them (and the fractional
check runs *before* the sign would ever be examined, so a
negative-and-fractional quantity like `-0.5` still raises exactly as
before). A negative quantity now simply passes through as valid signed
data for whichever row carries it. Rejection of a negative quantity as
UNKNOWN/never-flat for AP's own long-only target contract already
happened at the resolver boundary
(`ap/exit_safety.py::_extract_long_position_qty`, unchanged) — that
function only examines the row that exact-matches the contract actually
being resolved, so it was never the source of the poisoning; the adapter
was.

All four external non-test callers of `list_positions()` were re-audited
against this change: `ap_exit_engine.py` and `app.py::nightly_reconcile`
already filter to `qty > 0` before using the result; `ap_reconciler.py`'s
`_safe_get_broker_positions()` → `_handle_db_position_missing_at_broker`
path is observation-only (already covered by the original spec's caller
audit); `app.py::nightly_reconcile`'s remaining path is pure
reporting/alerting with zero position mutation. No caller needed a
change.

**Known second-order consequence, flagged for awareness (not fixed —
outside this amendment's bounded file scope, no failing regression
proves a dependency):** `ap_reconciler.py::_broker_position_qty()`
predates this amendment and already applies `abs()` to the raw quantity,
apparently anticipating signed data. Before this fix, a negative quantity
on *any* row — including a hypothetical anomaly where AP's own tracked
contract itself reports negative at the broker — would have caused
`list_positions()` to raise, and the reconciler's existing exception
handling would flag the affected rows for manual review without changing
status. After this fix, such a row now flows through and gets `abs()`'d
into a positive magnitude by the reconciler's pre-existing logic. This
only matters for the narrow case of AP's *own* target contract itself
carrying negative quantity (a broker-side data anomaly distinct from the
"unrelated position" case this blocker targets) — genuinely unrelated
short positions on other contracts remain harmless in the reconciler's
by-contract/by-underlying dictionaries, keyed by their own (non-colliding)
contract symbol. No test in this amendment's scope currently exercises
this specific edge case; it is documented here rather than silently
left unaddressed.

### Tests

Three new files, 48 tests total:

- `tests/test_p0_invalid_nonempty_contract_never_flat.py` (29 tests) —
  8 invalid non-empty contract values × 3 coverage angles (resolver
  against empty snapshot, resolver against non-empty snapshot, full
  `recover_exit_position()` end-to-end), plus empty-like regression
  coverage and two valid-contract regression guards.
- `tests/test_p0_tradier_order_shape_option_symbol.py` (8 tests) —
  cases A-F per the amendment spec (same-OCC adopted, terminal-id
  different-live-exit recognized, bounded duplicate-cancel preserved,
  same-underlying-different-OCC never matches, unrelated never matches,
  missing/malformed order identity never wildcards) plus a direct
  extractor unit test.
- `tests/test_p0_unrelated_short_position_no_poison.py` (11 tests) —
  required tests 1-6 per the amendment spec, plus direct adapter-level
  proof that the real `TradierBroker.list_positions()` no longer raises
  for an unrelated negative row, and a regression proof that fractional
  quantity still raises regardless of sign.

One pre-existing test was rewritten:
`test_negative_integer_quantity_raises_at_adapter` in
`tests/test_p0_tradier_exact_quantity_flat_guard.py` asserted the
pre-Blocker-3 (now-incorrect) adapter behavior; renamed to
`test_negative_integer_quantity_does_not_raise_at_adapter` and rewritten
to assert the corrected invariant. `test_negative_fractional_quantity_
raises_at_adapter` (testing `-0.5`) required no change — the fractional
check runs before the sign check in the adapter's row-parsing order, so
it continues to raise exactly as before.

Fail-first: 30/48 new tests failed against pre-Amendment-4 code — 24 for
Blocker 1 (8 values × 3 angles), 4 for Blocker 2 (extractor + cases A/B/C;
cases D/E/F were confirmed analytically and empirically to already pass
pre-fix, since a mismatched-but-still-wrong extracted value coincidentally
still failed to match the target — they serve as regression guards, not
fail-first evidence), 2 for Blocker 3 (the two direct adapter-level
tests; all 11 resolver-level Blocker-3 tests already passed pre-fix,
confirming the resolver's per-row exact-match filtering already correctly
isolated unrelated rows — the defect was purely adapter-scoped). Post-fix:
48/48 pass.

## Amendment 5 — account-wide fractional poisoning + broker order query tri-state + SCALE_OUT fill routing

Independent audit against the exact head of Amendment 4 found three
further P0 defects, all confirmed against the live code before any
change was made, and all confirmed with proper fail-first evidence
(including, for Blocker 3, temporarily reverting just the affected code
block to capture failure evidence without losing Blocker 2's already-
verified fix in the same file). Amendments 1-4 were explicitly preserved
and re-verified; none were reverted or weakened.

### Blocker 1 — account-wide fractional quantity poisoned exact-OCC broker truth

**Defect.** The same class of defect Amendment 4 fixed for negative
quantity also existed for fractional quantity:
`ap/brokers/tradier.py::list_positions()` raised
`TRADIER_POSITIONS_PAYLOAD_MALFORMED: fractional option quantity`
globally the moment any row anywhere in the account carried a
non-integral quantity. Tradier accounts may legitimately hold fractional
EQUITY positions (fractional-share programs); one unrelated fractional
row made the actual AP target OCC contract's broker truth unavailable,
even though the target's own row could be a perfectly valid whole-integer
quantity.

**Fix.** Removed the global `quantity.is_integer()` raise. The resolver's
`_extract_long_position_qty()` already correctly returned `None` (never a
truncated int) for a fractional quantity on the row that exact-matches
the AP target contract — that protection was already in place and
required no change. Boolean and non-finite (NaN/inf) quantities remain
globally rejected, since those represent genuine structural garbage
regardless of which row carries them.

**Tests.** Added to the existing `tests/test_p0_unrelated_short_position_
no_poison.py` file (per the amendment's explicit instruction to extend
the Blocker-3 suite rather than create a new file for this closely
related invariant): 10 new tests covering unrelated fractional equity
alongside a valid target, target absence with an unrelated fractional
row present, the real adapter not raising for an unrelated fractional
row, the target's own fractional and negative-fractional quantity
resolving UNKNOWN, and bool/NaN/±inf regression coverage. Two
pre-existing tests in `tests/test_p0_tradier_exact_quantity_flat_guard.py`
asserted the now-superseded behavior and were rewritten:
`test_fractional_quantity_raises_at_adapter` →
`test_fractional_quantity_does_not_raise_at_adapter`, and
`test_negative_fractional_quantity_raises_at_adapter` →
`test_negative_fractional_quantity_does_not_raise_at_adapter` (both
values now pass through as valid signed/fractional broker data).

Fail-first: 2/11 new tests failed pre-fix (the two direct adapter-level
tests — all resolver-level tests already passed, confirming the defect
was purely adapter-scoped, exactly mirroring Blocker 3's shape).
Post-fix: 21/21 pass in the combined file (11 Blocker-3 + 10 Blocker-1).

### Blocker 2 — broker open-order query failure could become false "no exit exists" truth

**Defect.** `ap/exit_autonomous_recovery.py::_list_open_orders()` caught
every broker-order query failure mode — exceptions, `None` returns,
unusable/malformed payloads, no supported query method available — and
returned `[]` in every case. That result was indistinguishable from an
authoritative successful broker response confirming zero open orders
exist. If a real live exit order for the position's exact contract was
still OPEN/WORKING at the broker but the query happened to fail during a
recovery pass, the failure was silently reinterpreted as "no live exit
exists," and autonomous recovery could authorize a duplicate replacement
exit while the real one was still working — violating the module's own
stated rule: "If broker truth is ambiguous, alert/no-op."

**Fix.** `_list_open_orders()` now returns `Optional[list[dict]]`: a list
(possibly empty) for `AVAILABLE_NONEMPTY`/`AVAILABLE_EMPTY`, `None` for
`UNKNOWN`/`UNAVAILABLE`. `_matching_open_exit_orders()` propagates `None`
(distinct from its pre-existing Amendment-3 empty-contract guard, which
still returns confirmed `[]` by construction since it never even attempts
a scan). Both call sites in `recover_exit_position()` — the
terminal-pending-broker-id duplicate-exit scan, and the generic
missing-broker-id scan — now explicitly check `is None` and return
`NOOP`/`broker_order_truth_unknown_hold` rather than letting `None` fall
through toward `_mark_replacement_safe()`.

**Tests.** `tests/test_p0_broker_order_query_unknown_never_empty.py` — 14
tests: direct unit coverage of `_list_open_orders()`'s tri-state contract
(raises, returns `None`, returns an unusable shape, no method available,
all methods fail — each → `None`; genuine empty/nonempty → the list
itself), `_matching_open_exit_orders()` propagation, and full
`recover_exit_position()` end-to-end coverage of both call sites holding
on `UNKNOWN`, plus normal-path preservation for genuinely successful
empty and nonempty scans.

Fail-first: 9/14 failed pre-fix, with the two end-to-end cases showing
the exact money-path consequence: `action=REPLACEMENT_SAFE` where `NOOP`
was required, for both the generic-path and terminal-status-duplicate-
scan call sites.

### Blocker 3 — broker-confirmed FILLED SCALE_OUT could full-close remaining exposure

**Defect.** `recover_exit_position()`'s `st == "filled"` branch
unconditionally called `exit_engine.mark_position_closed()` for any
broker-confirmed fill on the pending exit order — which hard-sets
`quantity_remaining = 0` and `closed = True`. A `SCALE_OUT` tranche
independently reports `"filled"` for just that tranche's quantity while
genuine broker exposure remains open on the rest of the position (e.g. a
4-contract position with a 1-contract `SCALE_OUT` order filling would
silently drop the other 3 contracts from management).

**Fix.** The `st == "filled"` branch now routes through the existing
canonical partial-fill/full-close classification,
`ap_exit_engine.py::APExitEngine.note_partial_exit_fill()`, rather than
duplicating a second scale-out algorithm in autonomous recovery. That
helper already correctly reduces `quantity_remaining` by the actual fill
delta and only marks the position closed when `quantity_remaining`
reaches zero. The fill quantity is passed as `cumulative_filled` (not
`qty_filled`) keyed by `broker_order_id`, so a duplicate `FILLED`
callback or duplicate recovery pass for the same order computes
`delta = 0` and is correctly ignored rather than double-decrementing —
this was a deliberate choice after tracing `note_partial_exit_fill()`'s
own cumulative-vs-incremental branching, since passing a plain
incremental `qty_filled` would NOT have been idempotent against a
repeated call. A bounded fallback preserves exact pre-amendment-5
behavior for exit-engine implementations that don't provide
`note_partial_exit_fill()`: if `quantity_remaining` is tracked on the
position and proves the fill is partial, hold (`NOOP`) rather than
fabricate a close; if `quantity_remaining` isn't tracked at all (as in
several pre-existing test doubles from earlier amendments), fall through
to the original `mark_position_closed()` call unchanged, since there is
no information available to determine partial-vs-full in that case.

**Tests.** `tests/test_p0_scale_out_fill_recovery_no_false_close.py` — 9
tests covering cases A-G from the amendment spec: partial fills of 1 and
2 out of 4 contracts remain open with correct `quantity_remaining`
(cases A/B); a second scale-out tranche under a different
`broker_order_id` after a prior partial fill applies correctly with no
double subtraction (case C); a fill that exactly consumes all remaining
exposure closes (case D); a duplicate recovery pass for the same order is
idempotent (case E); an ambiguous/missing fill quantity falls back to
`pending_exit_qty` and is still routed through the canonical partial-fill
path rather than fabricating a close (case F); and two normal-path
preservation variants for exit-engine doubles without the canonical
helper — one where `quantity_remaining` proves the fill partial and
recovery holds, one where it's genuinely a full fill and the legacy path
still closes, and one full regression guard for position objects that
don't track `quantity_remaining` at all (preserving exact prior behavior
for every pre-existing amendment 1-4 test).

Fail-first: 6/9 failed pre-fix. Verified via a targeted temporary revert
of just the `st == "filled"` code block (rather than a full-file
`git stash`, since Blocker 2's already-verified fix lives in the same
file and needed to remain active) — pre-fix, a 1-of-4 `SCALE_OUT` fill
resulted in `quantity_remaining == 0` and `closed == True`, confirming
the exact defect: 3 real contracts silently dropped from management.
Post-fix: 9/9 pass.

### Combined validation

- Full PR-#481-targeted suite (12 files: original fix + Amendments 1-5 +
  `test_p0_broker_owned_exit_requested_recovery.py` +
  `test_p0_broker_owned_exit_recovery_preflight.py`): 387 passed, 1
  skipped, 0 failed.
- `tests/test_p0_broker_position_unavailable_not_flat.py` (#478)
  re-verified in isolation, post all six amendments plus the merge-gate
  correction: 69/69 passed,
  unmodified.
- Exit-fill-adjacent blast-radius check (`test_p0_canonical_exit_fill_
  truth.py` + `test_p0_partial_exit_ownership_guard.py` +
  `test_p0_exit_decision_idempotency_guard.py`, run specifically because
  Blocker 3 touches fill-routing logic): 172 passed, 2 skipped, 0 failed.
- Scope discipline: `git diff --stat` for this amendment shows exactly
  the two production files the amendment specified —
  `ap/brokers/tradier.py`, `ap/exit_autonomous_recovery.py` — plus the
  required pre-existing test rewrites. `ap_reconciler.py` was explicitly
  NOT touched, per the amendment's own non-scope finding.

## Amendment 6 — Tradier list_orders() UNKNOWN-vs-EMPTY boundary + order-query dict-handling correction

Independent audit against the exact head of Amendment 5 found one
remaining adapter-boundary blocker, and a follow-up correction found one
further bounded defect in the same broker-order-truth territory. Both
are documented together since they share root cause and were fixed in
the same session against the same underlying invariant.

### Amendment 6 proper — Tradier `list_orders()` boundary

**Defect.** Amendment 5 correctly made
`ap/exit_autonomous_recovery.py::_list_open_orders()` tri-state, but the
production Tradier adapter could still collapse an unusable broker order
response into `[]` *before* autonomous recovery ever saw it — the same
class of defect Amendment 4 fixed for `list_positions()`. Two concrete
paths: a malformed top-level payload (scalar/string/non-dict JSON)
collapsed to `[]` via `node = None -> orders = None -> return []`; and
malformed order rows within an `orders.order` list were silently filtered
out via a list comprehension (`[order for order in orders if
isinstance(order, dict)]`), producing a false confirmed-empty result even
though the broker reported rows whose identity couldn't be established.

**Fix.** `TradierBroker.list_orders()` rewritten to mirror
`list_positions()`'s exact shape-validation discipline: propagate
transport/auth exceptions unchanged; raise
`TRADIER_ORDERS_PAYLOAD_MALFORMED` for a non-dict top-level response, a
missing `orders`/`order` key on an otherwise non-empty dict, a
non-dict/non-list order container, or any individual order row that
isn't a dict (fail closed, never silently filter a malformed row out of
an otherwise-returned list); return `[]` only for explicit
authoritative-empty shapes (top-level `{}`; `orders`/`order` ==
`null`/`"null"`/`""`/`{}` or an empty list).

No other production file required a change: `TradierBroker` only exposes
`list_orders` (not `list_open_orders`/`get_open_orders`), so
`_list_open_orders()`'s tri-state helper already finds and calls it as a
fallback candidate — confirmed via direct inspection before writing any
test. `ap/manual_close_reconciliation.py`'s separate `list_orders()`
fallback path is unreachable for the real production `TradierBroker` (it
always has `_get`/`cfg.account_id`, so it takes the primary pagination
branch instead) and has zero existing test coverage — verified safe to
leave untouched rather than widening scope without a failing regression.

**Tests.** `tests/test_p0_tradier_orders_unknown_never_empty.py` — 25
tests covering cases A-F from the spec: 4 malformed-top-level variants, 3
malformed-order-row variants, 2 mixed-valid-and-malformed variants, 9
legitimate-empty-shape variants (all correctly returning `[]`), valid
single-order and list-of-orders shapes, an end-to-end same-OCC live EXIT
recognition proof through the real adapter, and three end-to-end
malformed-payload-holds proofs wiring a real `TradierBroker` into
`recover_exit_position()` (generic path, malformed-row variant,
terminal-pending-id path).

Fail-first: verified via a targeted temporary revert of just the
`list_orders()` method body — 15/25 failed pre-fix, including all 3
end-to-end proofs showing `action=REPLACEMENT_SAFE` where `NOOP` was
required. The revert also surfaced that the old implementation was
internally inconsistent across plausible empty-shape variants (over-
raising on some string-typed empty markers while under-raising on scalar
top-level payloads and malformed rows) — the new implementation is
uniform. Post-fix: 25/25 pass.

### Merge-gate correction — order-query dict-handling

**Defect.** A follow-up independent review found that
`_list_open_orders()`'s dict-handling branch still treated *every*
unrecognized dict as a successful singleton order result:
`return [result]` unconditionally, once the recognized-container checks
failed. Broker responses such as `{"error": "rate_limited"}`,
`{"errors": [...]}`, `{"message": "broker unavailable"}`,
`{"status": "ERROR", ...}`, or `{}` were silently wrapped as `[result]`
and treated as `AVAILABLE` broker-order truth.
`_matching_open_exit_orders()` then filtered those fake rows out (they
match no real contract and have no real status) and obtained zero
matching exits — a confirmed-but-wrong empty result that could still
reach replacement-safe logic as though an authoritative scan had proven
no live exit exists, violating Amendment 5's own binding invariant.

**Fix.** New helper `_looks_like_broker_order_row(d)`: a dict is only
accepted as a genuine single order row if it carries actual
broker-order identity (via the existing `_broker_order_id()` extractor)
*and* carries no explicit `error`/`errors`/`message` key *and* carries no
`status` of `error`/`fail`/`failed`/`failure`. Wired into
`_list_open_orders()`'s dict-handling branch: a dict that is neither a
recognized container nor a genuine order row now logs and falls through
to the next candidate method rather than being accepted.

**Bounded Fix 2** (the correction's own name for the `list_orders()`
missing-container case) required *zero* additional changes: fail-first
testing confirmed Amendment 6 (already implemented earlier in this same
session) already raises `TRADIER_ORDERS_PAYLOAD_MALFORMED` for exactly
this shape. Narrow confirmatory tests were added anyway per the explicit
instruction to add them to `tests/test_p0_broker_order_query_unknown_
never_empty.py`.

**Tests.** Appended to the existing Amendment-5 test file per the
correction's explicit file instruction: 11 new tests — 5 parametrized
error-like/unrecognized dict shapes each resolving to `None`; a
regression guard confirming a genuine single order dict without any
error shape is still correctly accepted; two end-to-end tests (generic
path and terminal-pending-id duplicate-scan path) proving zero
replacement-safe/clear-in-flight/adoption/cancel/close; and three
real-`TradierBroker` confirmatory tests for the missing-container/
authoritative-empty/valid-nonempty cases.

Fail-first: verified via a targeted temporary revert of just the
dict-handling branch — 7/11 relevant tests failed pre-fix, including both
end-to-end cases showing `action=REPLACEMENT_SAFE` where `NOOP` was
required. Post-fix: 25/25 pass in the full file (14 original + 11 new).

### Combined validation

- Full PR-#481-targeted suite (14 files: original fix + Amendments 1-6 +
  merge-gate correction + `test_p0_broker_owned_exit_requested_
  recovery.py` + `test_p0_broker_owned_exit_recovery_preflight.py` +
  `test_p0_exit_closed_guard_and_circuit_breaker.py`): 448 passed, 1
  skipped, 0 failed.
- `tests/test_p0_broker_position_unavailable_not_flat.py` (#478)
  re-verified in isolation, post Amendment 6 and the merge-gate
  correction: 69/69 passed, unmodified.
- Scope discipline: Amendment 6 touched exactly `ap/brokers/tradier.py`
  (production) as required; the merge-gate correction touched exactly
  `ap/exit_autonomous_recovery.py` (production). `ap_reconciler.py`
  untouched, scale-out routing untouched, no ENTRY/selector/watcher/
  reporting/queue/execution_mode/client_id changes in either.

## Required tests — status

All required fail-first and regression coverage across the original fix
and all six amendments (plus the Amendment-5 merge-gate correction) is
implemented in ten dedicated files:

- `tests/test_p0_tradier_exact_quantity_flat_guard.py` (33 tests) —
  original adapter/resolver fix (two tests rewritten under Amendment 5;
  see above).
- `tests/test_p0_exit_autonomous_recovery_quantity_guard.py` (15 tests) —
  Amendment 1.
- `tests/test_p0_missing_contract_identity_never_flat.py` (15 tests) —
  Amendment 2.
- `tests/test_p0_missing_contract_identity_no_wildcard_match.py` (9 tests)
  — Amendment 3.
- `tests/test_p0_invalid_nonempty_contract_never_flat.py` (29 tests),
  `tests/test_p0_tradier_order_shape_option_symbol.py` (8 tests),
  `tests/test_p0_unrelated_short_position_no_poison.py` (21 tests,
  includes Amendment 5's Blocker 1 coverage) — Amendment 4 (Blockers 1,
  2, 3 respectively).
- `tests/test_p0_broker_order_query_unknown_never_empty.py` (25 tests:
  14 original Amendment-5 Blocker-2 tests + 11 merge-gate correction
  tests), `tests/test_p0_scale_out_fill_recovery_no_false_close.py`
  (9 tests) — Amendment 5 (Blockers 2, 3 respectively; Blocker 1 folded
  into the existing Amendment-4 file above).
- `tests/test_p0_tradier_orders_unknown_never_empty.py` (25 tests) —
  Amendment 6.

All ten files are included in `.github/workflows/p0_regression.yml` and
run as part of the exact-head CI P0 Regression Suite.

Original fix's dedicated suite breakdown:

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

Original fix:

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

Amendment 1:

- Fail-first (pre-fix): 11/15 new tests failed, confirmed.
- Post-fix: `tests/test_p0_exit_autonomous_recovery_quantity_guard.py`
  15/15 passed.
- Combined PR-#481-targeted suite (`test_p0_broker_position_unavailable_
  not_flat.py` + `test_p0_tradier_exact_quantity_flat_guard.py` +
  `test_p0_exit_autonomous_recovery_quantity_guard.py` +
  `test_p0_broker_owned_exit_requested_recovery.py` +
  `test_p0_broker_owned_exit_recovery_preflight.py`): 282 passed, 1
  skipped, 0 failed.
- `tests/test_p0_broker_position_unavailable_not_flat.py` (#478)
  re-verified in isolation: 69/69 passed, unmodified.

Amendment 2:

- Fail-first (pre-fix): 6/15 new tests failed (Case A + Case B), 9/15
  passed (Cases C/D/E + normal-path, proving they were unaffected by the
  fix before it was even applied) — confirmed.
- Post-fix: `tests/test_p0_missing_contract_identity_never_flat.py` 15/15
  passed.
- Combined PR-#481-targeted suite (all five files above plus
  `test_p0_missing_contract_identity_never_flat.py`): 297 passed, 1
  skipped, 0 failed.
- `tests/test_p0_broker_position_unavailable_not_flat.py` (#478)
  re-verified in isolation, post both amendments: 69/69 passed,
  unmodified.

Amendment 3:

- Fail-first (pre-fix): 5/9 new tests failed (Cases A, B, C, D, and the
  defense-in-depth helper test), confirmed with exact captured
  action/reason values showing unrelated broker orders adopted or
  canceled. Cases E/F/G/H passed pre-fix, proving normal-path behavior
  was unaffected before the fix was even applied.
- Post-fix: `tests/test_p0_missing_contract_identity_no_wildcard_match.py`
  9/9 passed.
- Combined PR-#481-targeted suite (all seven files: original +
  Amendment 1 + Amendment 2 + Amendment 3 + `test_p0_broker_owned_exit_
  requested_recovery.py` + `test_p0_broker_owned_exit_recovery_
  preflight.py`): 306 passed, 1 skipped, 0 failed.
- `tests/test_p0_broker_position_unavailable_not_flat.py` (#478)
  re-verified in isolation, post all three amendments: 69/69 passed,
  unmodified.
- Scope discipline: `git diff --stat` for this amendment shows exactly
  one production file changed, additive only (+61/-0 lines,
  `ap/exit_autonomous_recovery.py`).

Amendment 4:

- Fail-first (pre-fix): 30/48 new tests failed — 24 for Blocker 1, 4 for
  Blocker 2 (cases A/B/C + extractor), 2 for Blocker 3 (adapter-level
  only; all resolver-level Blocker-3 tests already passed pre-fix,
  confirming the defect was purely adapter-scoped). Full detail above
  under "Amendment 4".
- Post-fix: all three new files 48/48 passed
  (`test_p0_invalid_nonempty_contract_never_flat.py` 29/29,
  `test_p0_tradier_order_shape_option_symbol.py` 8/8,
  `test_p0_unrelated_short_position_no_poison.py` 11/11).
- `tests/test_p0_tradier_exact_quantity_flat_guard.py` re-verified after
  the one required rewrite: still 33/33 passed.
- Combined PR-#481-targeted suite (all ten files: original + Amendments
  1-4 + `test_p0_broker_owned_exit_requested_recovery.py` +
  `test_p0_broker_owned_exit_recovery_preflight.py`): 354 passed, 1
  skipped, 0 failed.
- `tests/test_p0_broker_position_unavailable_not_flat.py` (#478)
  re-verified in isolation, post all four amendments: 69/69 passed,
  unmodified.
- `tests/test_p0_exit_closed_guard_and_circuit_breaker.py` (explicitly
  required by the amendment): 25/25 passed.
- Broader blast-radius check: `test_p0_exit_engine_broker_truth.py` +
  `test_p0_reconciler_external_close_broker_truth.py` +
  `test_p0_broker_open_protective_monitoring.py`: 122 passed, 15
  skipped, 1 failed — the 1 failure
  (`test_postgres_fixture_wrapper_returns_dict_rows`) requires a live
  local Postgres connection unavailable in the validation sandbox; it is
  an environment gap, not a regression, and is unrelated to any code this
  amendment touched.
- Scope discipline: `git diff --stat` for this amendment shows exactly
  the three production files the amendment specified —
  `ap/brokers/tradier.py`, `ap/exit_safety.py`,
  `ap/exit_autonomous_recovery.py` — plus the one required pre-existing
  test rewrite. No ENTRY, selector, watcher, execution_mode, client_id,
  proof_trades, queue, reporting, position sizing, or exit
  strategy/intelligence file was touched.

Full CI-exact P0 Regression Suite: see PR #481 body / latest CI run for
the exact-head result at the current head SHA — all four amendments'
test files are included in `.github/workflows/p0_regression.yml` and
execute as part of that workflow.

## Delivery

New forward-fix branch/PR from current main. #478 is not reverted or
rewritten. Branch was rebased onto post-#479 main
(`d0d37e79ae698e604eb8080065d2314b161de351`) with zero conflicts; all
six amendments (plus the Amendment-5 merge-gate correction) were
implemented and validated against the rebased
branch. Do not merge, deploy, or mark ready for review without Angel's
explicit authorization.
