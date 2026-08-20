# P0 — Exactly-once proof closure + execution_mode preservation

**Severity:** P0 ledger integrity / performance taxonomy

**Target repository:** `angelprecision/angel_precision_bot`

**Related stale work:** PR #390 (`fix: exactly-once proof-trade terminal binding`) addresses an older version of this class of problem. Do **not** merge #390 blindly. Re-audit against current `main`, current production metadata shape, reconciler/recovery paths, and the evidence below. This PR is intended to supersede or replace that stale implementation if its assumptions no longer hold.

---

## Production incident

The Proof Vault win rate and total-trade metrics are not only affected by a dashboard 200-row cap. The underlying `proof_trades` ledger also currently contains:

1. duplicate terminal proof observations for the same economic position, and
2. real LIVE positions whose proof row loses `execution_mode='live'` and is written as `execution_mode='unknown'`.

Those defects can corrupt total trades, wins/losses, averages, and LIVE-vs-PAPER performance taxonomy even after the dashboard aggregation cap is repaired.

---

## Concrete production evidence captured 2026-08-20

### A. Jason LIVE NVDA winner lost execution_mode in proof_trades

The real `positions` row for Jason's closed NVDA PUT showed:

- client: `jasoncosby1@gmail.com`
- status: `CLOSED`
- position `execution_mode = 'live'`
- entry fill: `1.24`
- exit: `1.37`
- `realized_pnl_pct = 10.4839`
- `realized_pnl = 13.0`

The corresponding `proof_trades` row (ID 467 at the incident snapshot) showed:

- ticker: `NVDA`
- side: `PUT`
- `mode = 'live'`
- **`execution_mode = 'unknown'`**
- `option_pnl_pct = 10.48`
- `win = true`
- exit reason: runner trail exit
- generated `position_id = 'broker-repair-jasoncosby1@gmail.com-NVDA260821P00215000'`
- no broker entry/exit IDs on that proof row

This is a taxonomy failure. The underlying production position already knew the execution mode; the proof/recovery writer discarded it.

Current dashboard backend LIVE scoping explicitly relies on `proof_trades.execution_mode='live'`. Therefore a legitimate LIVE winner can exist in Supabase while being omitted from the official LIVE performance population.

### B. One PAPER NVDA economic trade produced two proof rows

Tradefluence's PAPER NVDA PUT closed +30.5% and produced two qualifying proof rows approximately **0.899 seconds apart**:

**ID 468**

- client: `tradefluencehq@gmail.com`
- NVDA PUT
- entry `2.00`
- exit `2.61`
- P&L `+30.5%`
- `mode='paper'`
- `execution_mode='unknown'`
- broker-repair style `position_id`
- no broker IDs
- runner trail exit reason

**ID 469**

- same client
- same NVDA PUT
- same entry `2.00`
- same exit `2.61`
- same P&L `+30.5%`
- `mode='paper'`
- `execution_mode='paper'`
- real position ID
- broker entry/exit IDs present
- reconciler auto-close reason

That is one economic position represented as two performance observations.

### C. This is not isolated

A production scan found multiple same-client/ticker/side/entry/exit/P&L terminal proof rows seconds apart across historical symbols including CRWD, AMZN, ORCL, AVGO, DIS, PG, VZ and others.

At one incident snapshot, among 255 rows passing the current `system_version='v2' AND synthetic_entry=false` filter:

- 31 rows had no `position_id`
- only 212 distinct non-null `position_id` values existed
- 87 rows had `execution_mode='unknown'`

These numbers are evidence snapshots, not static expected values.

---

## Root contract to enforce

> **One economic CLOSED position may contribute at most one canonical proof-trade observation.**

Normal exit, soft exit, runner trail, reconciler recovery, restart recovery, broker-repair, manual close repair, and any later enrichment must all converge on the same canonical proof record.

A later source with stronger broker truth should **enrich/update** the canonical proof row, not insert a second performance observation.

---

## Exact code paths to audit before implementation

At minimum inspect current-main behavior in:

- `ap_proof_logger.py`
- `ap_reconciler.py`
- `ap_recovery.py`
- `ap/position_manager.py`
- any helper that writes or repairs `proof_trades`
- any exit/restart path that calls the proof logger
- schema/attestation code that defines required proof metadata

Do not assume the old PR #390 covers current call paths. Trace every terminal writer from actual closure to Supabase.

---

## Canonical identity requirements

Use immutable production identity, not mutable outcome fields.

Identity priority should be explicit and deterministic:

1. **real `position_id`** when available
2. stable local/broker order/position identity that uniquely binds the same economic position
3. stable trade/signal identity where it is demonstrably one-to-one with the executed position
4. a deterministic recovery key only as a last resort when legacy/recovery data lacks the stronger identities

A fallback key must include enough client + contract + lifecycle identity to avoid cross-client or repeat-trade collisions.

### Identity MUST NOT depend on

- final P&L
- `win`
- `exit_reason`
- `closed_at` alone
- an executable quote that can move
- mutable diagnostics

Those values can change during reconciliation and must not create a new trade identity.

---

## Required write behavior

### 1. Normal terminal close

When a canonical position closes, create or upsert exactly one proof row with all known terminal metadata.

### 2. Recovery/reconciler finds an existing proof row

Update/enrich the existing canonical row.

Do not create a second row merely because:

- the earlier row came from broker repair,
- the earlier row lacked broker IDs,
- the later source has a different exit reason string,
- the later source has a more authoritative close timestamp,
- the later source now knows `execution_mode`, or
- restart generated a temporary/recovery position identity.

### 3. Stronger truth wins, nulls do not erase truth

A reconciliation update may promote fields from weaker to stronger evidence, but must not overwrite known values with null/unknown values.

Examples:

- known `broker_entry_order_id` must not become null
- known `broker_exit_order_id` must not become null
- known `execution_mode='live'` must not become `'unknown'`
- broker-confirmed fill price must not be replaced by a weaker estimated quote
- existing downstream diagnostics must be preserved unless deliberately superseded by stronger diagnostics

---

## execution_mode contract

`execution_mode` is performance taxonomy and must survive the entire lifecycle.

### Required source priority

Use actual execution metadata from the executed object chain, preferably:

1. `positions.execution_mode`
2. associated `orders.execution_mode`
3. explicit execution metadata propagated from the order/position lifecycle

Do **not** infer a historical trade's execution mode solely from the client's current account mode.

A client may switch PAPER/LIVE after a trade. Current account state is not historical execution truth.

### LIVE invariant

If the authoritative position/order for a closed economic trade says LIVE, the canonical proof row must say:

```text
execution_mode = 'live'
```

### PAPER invariant

If authoritative execution metadata says PAPER, the canonical proof row must remain PAPER.

### Ambiguous legacy/recovery row

If no trustworthy execution metadata can be resolved, preserve `unknown` and fail closed for official LIVE eligibility. Never guess LIVE merely to improve a metric.

---

## Metadata that must be preserved downstream

When canonicalizing/upserting proof rows, preserve or correctly enrich all available fields including:

- `client_email`
- `position_id`
- `local_order_id`
- `exit_local_order_id`
- `broker_entry_order_id`
- `broker_exit_order_id`
- `trade_id`
- `signal_id`
- `mode`
- `execution_mode`
- `broker_reconciled`
- `entry_price_source`
- `exit_price_source`
- broker entry/exit fill timestamps and quantities
- `performance_taxonomy`
- `training_eligible`
- `taxonomy_reason`
- `quote_domain_consistent`
- trigger/realized P&L diagnostics
- exit pricing/slippage diagnostics
- exit reason / close diagnostics

Do not “fix duplicates” by collapsing the row to a tiny shape that destroys forensic evidence.

---

## Required tests

Tests must exercise the actual production writers/call seams, not only a standalone dedupe function.

### A. Normal exit then reconciler

1. position closes via normal/runner exit
2. proof row is written
3. reconciler later observes broker terminal state

Assert exactly **one** canonical proof record remains and later broker metadata enriches that row.

### B. Broker repair first, canonical reconciler later

Reproduce the 2026-08-20 Tradefluence NVDA shape:

1. broker-repair path writes weak/partial proof
2. reconciler later has real position ID + broker IDs + execution mode

Assert:

- one economic proof record, not two
- real position identity replaces/aliases recovery identity safely
- broker IDs retained
- execution mode becomes PAPER
- P&L counted once

### C. Jason-style LIVE position

Create a CLOSED position with `execution_mode='live'` and run the exact proof/recovery path that previously generated `execution_mode='unknown'`.

Assert canonical proof row is LIVE.

### D. PAPER stays PAPER

A PAPER position must never become LIVE because the member/client later switched modes.

### E. Restart/recovery idempotency

Replay the same terminal event across process restart/recovery multiple times.

Assert row count remains one and metadata only becomes more authoritative.

### F. Null metadata cannot downgrade truth

Create a proof row with broker IDs/live mode, then run a weaker recovery update containing null broker IDs / unknown mode.

Assert stronger fields survive unchanged.

### G. Different real trades do not collide

Two legitimate separate trades in the same ticker, same side, potentially same contract, must remain two rows when their actual lifecycle identity differs.

This is mandatory. A dedupe implementation that merely compares ticker/side/P&L/timestamp proximity is unacceptable.

### H. No broker behavior changes

Tests/diff must prove this P0 does **not** alter:

- broker entry submit
- broker exit submit
- broker cancel
- fill decisions
- scanner decisions
- entry watcher decisions
- sizing
- stops/targets
- queue admission

---

## Historical repair policy

Do **not** ship a blind production DELETE/UPDATE that merges old rows based on ticker, timestamp proximity, or matching P&L.

Historical canonicalization requires a separate dry-run report first:

- candidate duplicate groups
- proposed canonical identity
- winning/source row
- metadata that would be promoted
- rows that remain ambiguous

Only after review should a reversible migration/backfill be considered.

The writer fix must prevent **new** duplicates immediately; historical cleanup is a separate reviewed operation.

---

## Explicit mutation boundary

This PR may mutate **`proof_trades` only as part of its intended exactly-once/upsert behavior**.

It must not mutate:

- `orders`
- `positions`
- signal/entry queue
- broker state

Reading positions/orders to recover identity and execution mode is allowed and expected.

---

## Review checklist before merge

Reviewer must perform in this order:

1. Read this PR description/spec.
2. Read the actual diff.
3. Read all review comments.
4. Enumerate changed files.
5. Trace every proof writer from signal/position closure through recovery/reconciler to `proof_trades`.
6. Test real production-shaped metadata, including missing broker IDs, generated broker-repair IDs, LIVE position metadata, PAPER metadata, restart replay, and later stronger reconciliation.
7. Give one verdict: **MERGE / HOLD / HARD HOLD**.

Mandatory questions:

- Does this change live trading behavior? Expected: **NO** decision/execution behavior change.
- Is it flag-off or active? Exactly-once proof writes should be active once merged; no fake safety through an unused flag.
- Does it touch broker submit/cancel? Expected: **NO**.
- Does it mutate orders/positions/queue? Expected: **NO**.
- Does it mutate proof_trades? **YES**, only to make terminal proof writes idempotent/enriching.
- Does it preserve client identity / execution_mode? Expected: **YES**.
- Does it use real production metadata shape? Must be demonstrated by tests modeled on the evidence above.
- Does it preserve diagnostics downstream? Expected: **YES**.
- Could it pollute PAPER/LIVE taxonomy? Must be **NO**.
- Could it make Jason trade junk? Must be **NO**; this code must remain downstream of trade decisions and broker execution.

---

## Merge gate

**HARD HOLD** until the implementation proves exactly-once economic-trade identity across normal exit + reconciler + recovery paths and proves LIVE/PAPER execution-mode preservation.

Do not merge stale PR #390 as a shortcut without comparing its actual diff against current `main` and this production evidence.
