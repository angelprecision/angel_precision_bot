# P0 SPEC — Prior-session broker-submit ambiguity must not poison current-session readiness

Date: 2026-09-02

Status: **SPEC ONLY / HARD HOLD UNTIL IMPLEMENTED AND INDEPENDENTLY RE-AUDITED**

Base: `main@0c794c26109b9b5e0a288c1d57c237694c0df418`

Depends on merged PR #572. Do not revert, weaken, or redesign #572.

## Why this exists

PR #572 correctly hardened the LIVE broker-submit crash window:

- durable `broker_submit:*` ownership is retained when broker truth is unavailable;
- ambiguous rows are not rearmed as ordinary watchers;
- stale age alone no longer expires a row carrying broker-submit authority;
- reconciliation has zero new broker POST authority and zero cancel authority.

Post-merge production logs prove those safety fences are working, but they expose one remaining liveness/readiness defect.

Jason LIVE still has two historical ENTRY rows with durable ambiguous-submit ownership:

- CSCO local order `386a608d-6282-4a08-a97f-2a6413f93064`
- AAPL local order `33121850-ce16-43c0-a1e0-964c8bc608f1`

Observed after #572 merged:

```text
BROKER_SUBMIT_OWNER_RETAINED_PENDING_RECONCILIATION
reason=RECONCILE_BROKER_QUERY_FAILED:ValueError:TRADIER_ORDERS_PAYLOAD_MALFORMED:orders

PENDING_TRIGGER_BROKER_HANDOFF_RECONCILED
broker_truth=UNKNOWN
watcher_rearm=NOT_ATTEMPTED

entries_allowed BLOCKED:
preopen_readiness_enforcement_failed:startup:status_degraded
```

This means #572 is failing safe, but the same old ambiguity can keep current-session entries blocked forever.

## Confirmed production facts

### 1. Tradier account-order enumeration is current-session only

`GET /v1/accounts/{account_id}/orders` returns orders for the current market session. It is not a historical tag lookup.

Therefore a Wednesday restart cannot authoritatively discover a Monday ambiguous-submit order by repeatedly calling the current-session order list.

### 2. Angel Precision ENTRY orders are DAY orders

The canonical LIVE ENTRY broker payload contains:

```text
class=option
type=limit
duration=day
side=buy_to_open
```

`duration=day` is part of `build_entry_submit_payload(...)`, and the durable `broker_submit_payload_hash` proves that exact canonical payload.

The Tradier adapter also sends `duration=day` on the actual ENTRY POST.

A prior-session DAY order cannot remain a working broker order after that market session has ended. It may, however, have filled before expiration. Therefore historical ambiguity cannot be cleared merely because the order-list endpoint no longer shows the order. Current position truth is still required.

### 3. Existing `list_positions()` is not sufficient authority for absence

Current `TradierBroker.list_positions()` returns `[]` both when the account truly has no positions and when the broker query/parsing fails.

That is acceptable for legacy best-effort callers, but it MUST NOT be used as proof that a historical ambiguous ENTRY did not fill.

Do not globally change this existing method in this PR. Preserve working callers.

---

# Binding invariant

A prior-session LIVE ENTRY carrying exact durable `broker_submit:*` authority may stop blocking **current-session entry readiness** only when all of the following are proven:

1. exact `client_id` matches the running client;
2. exact `execution_mode=live` is proven from durable row/meta provenance;
3. row is the canonical ENTRY row and still has no adopted `broker_order_id` / no authoritative submitted identity;
4. `submit_intent_at` is present, parseable, timezone-aware, nonfuture, and belongs to a **completed prior applicable market session**;
5. `broker_submit_key` is canonical and exact for the local order;
6. `broker_submit_payload_hash` recomputes exactly from the persisted row identity and the canonical ENTRY payload;
7. that canonical payload proves `duration=day`;
8. `current_owner` is the exact `broker_submit:<canonical key>` authority and lifecycle is consistent with the submit handoff;
9. an **authoritative, fail-closed current broker-position query** succeeds;
10. that broker-position result proves there is **no open position for the exact OCC contract** owned by the ambiguous ENTRY;
11. there is no contradictory current durable order/position evidence locally;
12. no broker query failure, parser ambiguity, calendar ambiguity, mode ambiguity, or identity mismatch exists.

If any required fact is UNKNOWN, malformed, contradictory, or unavailable, the row remains blocking and retains exact `broker_submit:*` authority.

This is a liveness exception only after current exposure is independently disproved. It is NOT permission to forget an ambiguous broker handoff.

---

# Required behavior

## Same-session ambiguous submit

Preserve #572 exactly.

```text
same-session broker_submit:* ambiguity
→ current-session Tradier order enumeration
→ exact tag/OCC/side/qty match may adopt broker id at SUBMITTED
→ malformed/query failure/no authoritative match remains RECONCILE_PENDING
→ readiness may remain degraded
→ zero new POST
→ zero cancel
```

Do not weaken this path.

## Prior-session DAY ambiguous submit

Do NOT keep querying the current-session order list as if it could discover historical broker identity.

Instead:

```text
prior completed session
+ exact durable broker_submit:* identity
+ canonical payload hash proves duration=day
→ obtain authoritative current broker-position truth

if exact OCC position exists:
    retain broker_submit authority
    remain blocking / degraded
    route only through existing position/recovery ownership paths
    no new position reconstruction in this PR unless an existing canonical helper already owns it

if authoritative position truth proves exact OCC absent:
    retain broker_submit authority durably
    classify row as historical broker-submit ambiguity with no current exposure
    do NOT terminalize as EXPIRED/FILLED/REJECTED
    do NOT clear submit_intent/key/hash/current_owner
    do NOT create replacement submit authority
    do NOT call watcher.watch
    do NOT broker POST
    do NOT broker cancel
    do NOT let this historical row alone make current-session entries_allowed false
```

The row may remain historically unresolved for audit/provenance. The purpose of this PR is to quarantine old ambiguity safely, not fabricate a historical broker outcome.

## Durable quarantine / restart behavior

The nonblocking historical classification must survive restart.

Use the existing JSON `meta` patch/CAS mechanism. Do not add a database column or new table.

A minimal durable marker is acceptable, for example:

```text
broker_submit_resolution_state=PRIOR_SESSION_DAY_NO_CURRENT_POSITION
broker_submit_resolution_at=<UTC timestamp>
broker_submit_resolution_contract=<exact OCC>
```

Names may differ, but the durable state MUST:

- be written only after all binding proof gates pass;
- preserve the original `broker_submit:*` owner and submit evidence;
- be client/mode/order fenced;
- fail closed if the write is not proven;
- prevent pointless current-session historical tag polling on every loop/restart;
- remain reversible by future authoritative broker evidence.

Do not mark the row `EXPIRED` merely to make readiness green.

---

# Authoritative position query requirement

Because legacy `TradierBroker.list_positions()` collapses failures to `[]`, this PR needs a **new read-only strict position-truth path** for this recovery proof.

Preferred narrow shape:

```text
TradierBroker.list_positions_authoritative(...)
```

or an equivalently named helper used only by this recovery path.

Requirements:

- GET the existing account positions endpoint;
- zero broker mutation;
- propagate transport/auth/server failures instead of returning empty;
- fail closed on malformed root/position shapes;
- accept only explicitly supported empty shapes already observed/documented by the adapter (`null`/`"null"`/empty collection as appropriate);
- return normalized positions only after successful parsing;
- exact OCC comparison, no loose underlying-symbol match;
- PAPER must never borrow LIVE broker truth.

Do **not** change the semantics of existing `list_positions()` globally in this PR. Existing working paths rely on its best-effort behavior.

---

# Current Tradier `orders` payload error

Production is currently logging:

```text
TRADIER_ORDERS_PAYLOAD_MALFORMED:orders
```

Do not solve this by broadly accepting arbitrary scalar values from the broker.

For this PR:

- same-session broker order parsing remains fail closed;
- prior-session DAY rows should no longer depend on current-session order enumeration once prior-session proof is established;
- if diagnostics are touched, log only sanitized shape/type information, never tokens or sensitive account payloads;
- a separate parser fix is allowed later only after the actual safe payload shape is proven.

This prevents us from converting one safe failure into a permissive broker parser because a third-party API decided JSON is performance art.

---

# Production scope budget

Expected production files: **2–3 maximum**.

Preferred ownership:

1. `ap/brokers/tradier.py`
   - add strict read-only authoritative positions enumeration only;

2. `ap_execution_core.py` **or** `ap_recovery.py`
   - classify prior-session DAY broker-submit ambiguity and persist quarantine;

3. only if required by existing caller contract: `ap_recovery.py` / `ap/order_monitor.py`
   - recognize the exact nonblocking historical disposition without changing ordinary recovery.

Do not create a new recovery subsystem.

If implementation requires more than 3 production modules or ~250 production LOC, stop and re-audit the design before continuing.

---

# Explicit non-scope

Do NOT change:

- scanner logic or schedules;
- signal creation/approval;
- Master Control;
- selector gates;
- delta/spread/OI/volume/premium/DTE policy;
- risk or sizing;
- watcher trigger/breach policy;
- #569 first-time watcher ownership deadline;
- #573 after-hours WATCHING readiness work;
- exit policy or thresholds;
- protective exits;
- proof_trades taxonomy;
- queue semantics;
- broker submit/cancel behavior;
- #551 mode-scoped snapshot behavior;
- global Tradier `list_positions()` semantics;
- same-session #572 fail-closed broker reconciliation.

This PR hardens one recovery/readiness edge. The bot is already working and must stay recognizable.

---

# Required tests

At minimum, add deterministic behavioral coverage for:

### A. Exact production-shaped historical row

Use a row shaped like the Sep 2 Jason incident:

- LIVE;
- ENTRY/PENDING_TRIGGER;
- no broker_order_id;
- exact OCC;
- valid qty and executable limit;
- materialization generation present;
- `submit_intent_at` > one completed market session old;
- canonical key;
- recomputed payload hash;
- `current_owner=broker_submit:<exact key>`;
- lifecycle `SUBMITTING`;
- canonical payload proves `duration=day`.

Authoritative broker position result: empty for exact OCC.

Prove:

- no call to current-session `list_orders()` is required for historical discovery;
- strict position query called exactly as intended;
- row is not terminalized;
- owner retained exactly;
- submit evidence retained;
- historical nonblocking marker persisted with CAS/fence;
- startup recovery does not become DEGRADED solely because of this row;
- `entries_allowed` is not blocked solely by this row;
- zero `place_order` / broker POST;
- zero cancel;
- zero watcher registration.

### B. Historical row with exact broker position present

Prove:

- remains blocking/degraded;
- no quarantine-to-nonblocking classification;
- owner retained;
- no new submit/cancel;
- no fabricated terminal status.

### C. Position query failure

Timeout / network / auth / malformed payload must:

- remain UNKNOWN/blocking;
- not treat `[]` as broker-flat;
- not persist nonblocking quarantine;
- retain owner;
- zero POST/cancel.

### D. Same-session row

Prove existing #572 behavior is unchanged:

- still uses exact current-session order reconciliation;
- malformed order payload remains UNKNOWN/fail closed;
- no new submit authority.

### E. Identity/mode/calendar fail-closed cases

Include:

- PAPER row;
- missing/mismatched client_id;
- bad execution-mode provenance;
- malformed/future submit_intent_at;
- same-session timestamp;
- weekend/holiday ambiguity if calendar helper cannot prove prior completed session;
- wrong/blank broker submit key;
- wrong payload hash;
- noncanonical owner;
- non-SUBMITTING contradictory lifecycle;
- malformed OCC contract.

None may become nonblocking through this exception.

### F. Restart durability

Once exact historical quarantine is durably proven:

- restart reuses it only after revalidating the durable identity/fence required by the implementation;
- it does not repeatedly perform useless historical current-session order enumeration;
- later contradictory broker/current-position evidence invalidates the nonblocking assumption and fails closed.

---

# Evidence required before merge

1. Exact-head P0 Regression Suite green on the final SHA.
2. Focused tests for all cases above.
3. At least one production-shaped test using the exact Jason-style metadata contract.
4. `git diff --check` clean.
5. No unresolved review threads.
6. Independent re-audit tracing:

```text
startup/restart
→ historical broker-submit classification
→ canonical DAY proof
→ authoritative position truth
→ durable quarantine CAS
→ readiness result
→ order-monitor behavior
```

7. Explicit proof of zero broker POST, zero cancel, and zero watcher registration from the historical quarantine path.

---

# Merge criterion

This PR is merge-ready only when all of the following are true:

- #572 same-session safety remains intact;
- historical DAY ambiguity can no longer block the account forever after current exposure is authoritatively disproved;
- historical broker outcome is never fabricated;
- `broker_submit:*` ownership/evidence is never silently cleared;
- an unavailable/malformed broker-position query fails closed;
- no trade-policy behavior changes;
- exact-head P0 is green;
- independent audit returns `MERGE`.

Until then: **HARD HOLD**.
