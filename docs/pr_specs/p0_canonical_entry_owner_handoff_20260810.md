# P0 SPEC — Bind canonical ENTRY position before exit-engine ownership handoff

**Status: DRAFT / HARD HOLD. Implementation contract only. Do not merge or deploy until code, production-shaped tests, and exact-head audit are complete.**

Base at branch creation: `main` / `570a56933615cbac82e3256c0e350119eca80c0d`.

## Incident that created this PR

2026-08-10 Jason LIVE INTC:

- exact contract: `INTC260810P00098000`
- broker ENTRY order: `141002256`
- ENTRY fill: `$1.46`
- filled around `13:34:26Z`
- broker EXIT order: `141003153`
- EXIT fill: `$1.44`
- filled around `13:36:01Z`
- realized database result approximately `-1.37%`

Production runtime evidence showed exit evaluation occurring under synthetic identity:

```text
broker-repair-jasoncosby1@gmail.com-INTC260810P00098000
```

The synthetic owner observed positive peak state (approximately +11% in the runtime trace) and later authorized the touched-profit exit. The canonical position persisted with zeroed peak/touched state, and the canonical filled ENTRY order remained with `position_id = NULL` despite a canonical position existing.

This is an ownership/state-coordination failure. Do **not** tune touched-profit thresholds in this PR. Exit policy cannot be evaluated reliably while two identities hold different memory for one LIVE economic position.

## Verification already completed

Current `main` was read directly before opening this spec.

### Existing intended canonicalization seam

`ap/fill_monitor.py:1835+`, `_seed_exit_engine()` explicitly says that if the engine already has a `broker-repair-*` position for the same contract, it should upgrade that object to canonical identity instead of creating a second in-memory position.

The method already:

- resolves exact execution mode;
- calls `exit_engine.adopt_canonical_position_identity(...)`;
- handles structured dispositions `ADOPTED`, `ALREADY_CANONICAL_REPAIR_REMOVED`, `NO_REPAIR_FOUND`, and `RETRY_*`;
- does not intentionally create a second owner on a `RETRY_*` adoption result.

Therefore this PR should strengthen the handoff/postcondition, not replace the subsystem.

### Confirmed ordering gap in fill processing

`ap/fill_monitor.py:2380-2465` currently handles a confirmed ENTRY fill in this order:

1. OSM ENTRY becomes `FILLED`.
2. `_open_position_safe(...)` creates/finds the canonical position and returns `position_id`.
3. optional standing stop is attempted.
4. `_seed_exit_engine(exit_engine, position_id, ...)` performs ownership adoption/seeding.
5. **only after exit-engine seeding**, an inline SQL write attempts to bind `orders.position_id`:

```python
UPDATE orders
SET position_id=%s, updated_ts=NOW()
WHERE client_id=%s AND local_order_id=%s
AND (position_id IS NULL OR position_id='')
```

The production incident ended with this filled ENTRY order still carrying `position_id = NULL`. The write is best-effort: failure is logged, but no durable reread/postcondition prevents the rest of the process from continuing.

### Confirmed exit-engine adoption seam

`ap_exit_engine.py:3558+`, `adopt_canonical_position_identity()` is already the canonical collapse authority. It holds the engine lock, checks exact client/mode/contract identity, can merge/remove matching `broker-repair-*` objects, and has explicit `RETRY_*` dispositions for unproven identity.

The right repair is therefore:

```text
broker fill -> canonical DB position -> durable ENTRY order/position binding -> canonical exit-engine adoption -> verify exactly one behavior-active owner
```

not a new exit owner or new reconciliation subsystem.

## Root cause / failure class

The current ENTRY fill side effects are individually best-effort but the system lacks one enforced postcondition joining them:

```text
FILLED order.position_id
    == canonical positions.id
    == one behavior-active exit-engine position_id
```

A broker-repair object can exist before fill monitor finishes canonical ENTRY processing. If durable order binding or canonical adoption does not converge, the synthetic object can continue making soft-exit decisions while the canonical DB row stores different peak/profit state.

## Required production changes

### File 1 — `ap/fill_monitor.py`

Keep changes local to ENTRY fill side effects.

#### Change A — extract and harden the filled-order position bind

Near the existing inline block at current `ap/fill_monitor.py:2415+`, create a helper:

```python
def _bind_filled_entry_position_id(
    *,
    order: dict,
    position_id: str,
) -> tuple[bool, str]:
```

Required identity inputs:

- nonblank `client_id`
- nonblank `local_order_id`
- nonblank `position_id`
- exact normalized `execution_mode` in `{live,paper}`

Required write/read contract:

1. UPDATE only the exact `client_id + local_order_id + kind=ENTRY` row.
2. Fence on exact execution mode.
3. Allow mutation only when `position_id IS NULL/blank` OR already equals the requested canonical id.
4. If UPDATE returns one row -> success.
5. If UPDATE returns zero -> reread exact row.
6. If reread already has the same position id -> idempotent success.
7. If reread has a different nonblank position id -> `POSITION_IDENTITY_CONFLICT`, fail closed.
8. Missing/malformed row/mode -> fail closed.

Suggested SQL shape:

```sql
UPDATE orders
SET position_id=%s, updated_ts=NOW()
WHERE client_id=%s
  AND local_order_id=%s
  AND kind='ENTRY'
  AND LOWER(TRIM(COALESCE(execution_mode,'')))=%s
  AND (position_id IS NULL OR position_id::text='' OR position_id::text=%s)
RETURNING position_id, client_id, local_order_id, execution_mode, contract
```

No overwrite of a different existing position id.

#### Change B — change ENTRY fill ordering

Current order is:

```text
open canonical position
standing stop
seed/adopt exit engine
bind orders.position_id
```

Required order:

```text
open/find canonical position
bind + verify orders.position_id
seed/adopt + verify canonical exit owner
standing stop remains best-effort after canonical ownership is proven
```

If the team must keep the standing stop earlier for protection, it may remain immediately after position creation, but **exit-engine soft-decision ownership must not be considered canonical until the order-position bind is proven**.

On bind failure:

- emit `FILLED_ENTRY_POSITION_IDENTITY_BIND_FAILED` at CRITICAL;
- include client, mode, local order, broker order, exact contract, requested position id and reread result;
- do not create an additional exit-engine owner;
- do not erase any already-existing broker-repair protection;
- request/retain reconciliation.

Do not mark the broker fill undone. The fill is real. This is a downstream ownership hold.

#### Change C — make `_seed_exit_engine()` return its outcome

Current function mostly returns `None`, so the caller cannot prove the postcondition.

Use a small structured result; avoid a framework rewrite. Example:

```python
@dataclass(frozen=True)
class ExitEngineSeedResult:
    ok: bool
    disposition: str
    canonical_position_id: str
    owner_count: int = 0
    reason: str = ""
```

Required dispositions can be small:

- `ADOPTED`
- `ALREADY_CANONICAL_REPAIR_REMOVED`
- `SEEDED_CANONICAL`
- `ALREADY_CANONICAL`
- `OWNER_UNPROVEN`
- `MODE_UNPROVEN`
- `SEED_FAILED`

Reuse existing `CanonicalAdoptionResult`; do not duplicate its identity rules.

#### Change D — verify one behavior-active owner after adoption/seed

After successful adoption or normal canonical seed, inspect `exit_engine.active_positions()` and require exactly one behavior-active position matching:

- exact `client_id`
- exact `execution_mode`
- exact OCC contract
- exact canonical `position_id`

Required result:

```text
matching canonical owners = 1
same-client/mode/contract broker-repair active owners = 0
```

If not true:

- emit `FILLED_ENTRY_CANONICAL_OWNER_UNPROVEN`;
- include all matching ids/modes/clients in diagnostics;
- do not silently return success.

If a wrong-client/wrong-mode/blank-mode repair exists, preserve the existing quarantine semantics. It must not donate economic state or become a behavior-active soft-exit owner.

### File 2 — `ap_exit_engine.py`

Do **not** rewrite `adopt_canonical_position_identity()`.

Only make surgical changes required for the postcondition above.

Likely changes:

1. expose/reuse a helper that classifies behavior-active positions by exact client/mode/contract;
2. ensure a quarantined repair cannot remain in `active_positions()` / soft-exit evaluation;
3. if successful canonical adoption removes a repair, transfer only already-approved safe quote/high-water fields under the current implementation's provenance rules;
4. preserve hard-stop/EOD protection.

Do not change:

- `IMMEDIATE_TP_PCT`;
- scale thresholds;
- profit floors;
- runner trail;
- hard stop thresholds;
- technical-stop policy.

### File 3 — focused tests

Create:

`tests/test_p0_filled_entry_canonical_owner_handoff.py`

Required production-shaped cases:

1. **Jason INTC replay**: synthetic `broker-repair-*` exists first, then canonical ENTRY fill arrives. After processing: one behavior-active owner, canonical id only.
2. Filled ENTRY order is durably bound to canonical `position_id` **before** successful seed result is returned.
3. Link-back SQL failure -> CRITICAL + seed does not claim canonical success.
4. Link-back CAS zero because row already carries same canonical id -> idempotent success.
5. Link-back row carries different position id -> conflict; no overwrite.
6. Same contract, wrong client repair -> never collapsed/donated.
7. Same client/contract, wrong execution mode -> never collapsed/donated.
8. Blank/unknown repair mode -> quarantined/unproven, never behavior-active soft owner.
9. Existing exact canonical + exact repair -> repair removed, canonical retained.
10. Normal case with no repair -> canonical position seeded exactly once.
11. Duplicate fill-monitor tick -> no second canonical owner.
12. Repair peak/current BID transfer follows existing provenance and canonical-entry rebase rules; test must not bless midpoint contamination.
13. Canonical DB peak/touched state can subsequently persist under canonical id.
14. No new broker ENTRY POST.
15. No new broker EXIT POST.
16. No new broker cancel.

## Explicit non-goals

Do not change in this PR:

- exit strategy thresholds;
- whether +11%, +16%, +23% is the desired take-profit behavior;
- broker submit/cancel policy;
- manual-close reconciliation;
- entry retries;
- scanner/selector/risk/sizing;
- queue;
- proof taxonomy;
- database schema.

The incident tells us state ownership was wrong. It does **not** yet prove a threshold should be loosened.

## Production file budget

Expected maximum:

1. `ap/fill_monitor.py`
2. `ap_exit_engine.py`
3. `tests/test_p0_filled_entry_canonical_owner_handoff.py`
4. `.github/workflows/p0_regression.yml` only for focused CI wiring

If more production files are required, stop and justify before widening.

## Money-path audit answers

- Changes live behavior: **YES**, ENTRY-fill downstream ownership/handoff.
- Flag-off or active: **ACTIVE**.
- Broker submit/cancel touched: **NO new call sites**.
- Orders mutation: **YES**, existing `position_id` link becomes exact/fail-closed.
- Positions mutation: existing position open path unchanged; no new quantity/P&L writer.
- proof_trades mutation: **NO**.
- Queue mutation: **NO**.
- `client_id`: exact, nonblank.
- `execution_mode`: exact `live|paper`, no inference.
- Production metadata shape: use current orders/positions fields and existing engine position attributes.
- Diagnostics: must add durable/visible bind/owner failure reasons.
- PAPER/LIVE pollution: must be reduced, never widened.
- Could make Jason trade junk: **NO entry admission or retry gate changes**.
- Could make Jason exit from the wrong owner: this PR exists to make that impossible.

## Required validation before merge consideration

```bash
python -m pytest -q \
  tests/test_p0_filled_entry_canonical_owner_handoff.py \
  tests/test_p0_live_executable_bid_pnl.py \
  tests/test_soft_exit_executable_truth.py \
  tests/test_fill_monitor_mvp_hardening.py
python -m py_compile ap/fill_monitor.py ap_exit_engine.py

git diff --check
```

Required source/audit checks:

```bash
grep -R "SET position_id" -n ap/fill_monitor.py ap/order_state_machine.py
grep -R "broker-repair-" -n ap_exit_engine.py ap/fill_monitor.py
```

Acceptance requires a direct replay proving:

```text
confirmed broker ENTRY fill
-> canonical position id exists
-> orders.position_id exact
-> one exact behavior-active engine owner
-> zero active synthetic owner for same client/mode/contract
```

Then run exact-head P0 CI and perform the permanent PR audit: description, cumulative diff, comments, changed files, exact code path, production metadata shape, broker call exposure, order/position/proof/queue mutations, client/mode preservation, diagnostics, taxonomy, Jason-safety, final **MERGE / HOLD / HARD HOLD**.