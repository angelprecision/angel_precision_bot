# angel_precision_bot
Execution Bot
elease candidate: hardened order lifecycle, broker-confirmed fill handling, idempotent exit accounting, degraded-mode runner protections, equity-aware sizingThis release candidate hardens the core Angel Precision execution stack around a single order-truth contract.

Included changes:
- APOrderStateMachine is the canonical order lifecycle authority.
- All fill quantities are treated as broker cumulative fill quantity, never incremental deltas.
- Fill monitor now acts as broker reconciliation only and routes lifecycle updates through the OSM.
- Non-broker watch-plan orders (PENDING_TRIGGER) are excluded from broker polling.
- Exit engine no longer mutates scale-out or closure state on submit; state changes occur only after broker-confirmed fills.
- Partial exit fills are idempotent and tied to local/broker order identity plus cumulative fill tracking.
- Exit rejection/cancel/expire clears in-flight state safely for retry.
- Client runner now enforces startup manifest validation, degraded-mode protections, and fatal handling for missing/dead control-stack components.
- Broker reconciler remains disabled by default because rich fill monitor is the primary reconciliation path.
- Position sizing now enforces contract-dollar premium units, daily stop blocking, throttle behavior, equity-scaled thresholds, and safe tier fallback.

Operational invariants:
- Submit is not fill.
- OSM owns order truth.
- Fill monitor reconciles broker truth; it does not directly own lifecycle persistence.
- Exit accounting is broker-confirmed only.
- Legacy fill monitor direct-write path must remain disabled in production unless explicitly enabled for recovery.
- Secondary broker reconciler must remain disabled by default unless separately proven safe.

Known follow-up items:
- Full restart/reseed proof under active partial exits.
- Final decision on local handling of partial entry fills.
- Audit that all exit-order creation paths always populate position_id.
- Audit that all sizer callers pass premium_per_contract as total contract dollars (option price * 100).
- `ap_strategy_evolver.py` is frozen by default. It only runs when `ENABLE_STRATEGY_EVOLVER=true`, and any emitted config is research-only and not runtime-eligible.
- ## Execution invariants

The live execution stack follows a strict contract:

1. **APOrderStateMachine owns order lifecycle truth.**  
   All legal status transitions are validated and persisted by the OSM, not by monitors or strategy helpers. [file:124]

2. **Broker fills are cumulative.**  
   Any `filled_qty` passed into lifecycle processing must be the broker cumulative filled quantity for that order, never an incremental fill delta. [file:124][file:172]

3. **Fill monitor is reconciliation-only.**  
   The fill monitor polls broker-backed active orders, maps broker statuses into canonical OSM states, and triggers side effects only after OSM confirmation. It must not directly own production lifecycle state. [file:172]

4. **Non-broker plans are not broker orders.**  
   `PENDING_TRIGGER` is a watcher plan state and must not be polled as a live broker order. [file:172]

5. **Submit is not fill.**  
   Exit submission does not change realized exit state, scale-out counts, or close status. Only broker-confirmed fills may do that. [file:199]

6. **Exit accounting must be idempotent.**  
   Partial and full exit events must be tied to order identity and cumulative fill progression so duplicate or stale callbacks cannot double-apply. [file:199]

7. **Runner health gates entries.**  
   Missing exit engine, missing entry watcher, dead worker thread, or dead fill monitor must prevent healthy entry operation or force degraded mode. [file:191]

8. **Position sizing uses contract-dollar units.**  
   `premium_per_contract` must be the full dollar cost of one options contract, e.g. `2.35 -> 235.0`. [file:230]

9. **Secondary reconciliation stays off by default.**  
   The separate broker reconciler remains disabled unless it is explicitly tested to coexist safely with the rich fill monitor. [file:191]

10. **Legacy direct-write fill behavior stays off in production.**  
    The legacy fill-monitor path must remain disabled unless intentionally enabled for controlled recovery or migration work. [file:172]
