# P0: Coherent unfilled-entry repricing and retry continuity

## Status

Implementation contract only. No production behavior is implemented by this commit. Keep the pull request in draft until production code and exact-head tests are added.

Base SHA: `ffd22ca37ab0dcf874c2b463e61bee16337772fa`

## Production incident

Tradefluence attempted `BAC260724P00062000` for 19 contracts.

- initial submitted PAPER limit: `0.66`
- later observed option price: approximately `0.94`
- status: unfilled, then canceled
- cancel reason: `STALE_ENTRY_CANCEL MISSED_MOVE — limit=$0.66 current=$0.94 (42% above) status=SUBMITTED age=23s`
- post-cancel retry status: `ARMED`
- retry delay: approximately `21.17s`
- retry result: `FAILED`
- retry submit error: `time_gate`

The retry system existed and ran. It did not disappear. It failed because two separate entry-recovery systems now disagree:

1. the in-order re-peg path uses a short six-second cadence, two-attempt cap, and roughly 25-second total lifetime;
2. the post-cancel retry path waits 15–30 seconds, rebuilds a signal payload, and routes it through the full fresh-signal `process_signal()` admission path.

The original BAC order was allowed to submit before 10:00 ET. Its replacement was rejected by the first-30-minute `time_gate`. The same approved setup therefore received contradictory policy depending on whether it was the original order or its retry.

## Design objective

Restore one bounded lifecycle for a good setup that is valid but unfilled:

`approved setup -> initial submit -> bounded reprice -> cancel confirmed -> at most one replacement -> fill or terminal outcome`

The bot must not blindly chase. It also must not abandon a valid opportunity because a limit became stale or because a retry re-ran an unrelated static gate that the original order had already passed.

## Required behavior

### 1. One retry owner

Select one canonical owner for every unfilled ENTRY order. The order monitor, re-peg engine, post-cancel retry engine, watcher, and OSM must not independently schedule competing retries.

Persist:

- `entry_lifecycle_id`
- `original_local_order_id`
- `current_local_order_id`
- `retry_generation`
- `retry_owner`
- `retry_state`
- `retry_not_before`
- `retry_deadline`
- `reprice_attempts`
- `replacement_attempts`
- exact `client_id`
- exact `execution_mode`
- `signal_id`
- trigger generation and setup owner

A restart must recover the same owner and generation without creating a second broker order.

### 2. Bounded 75-second opportunity window

Default total unfilled-entry opportunity window: approximately 75 seconds from the first broker acknowledgment. Keep it configurable.

Within that window:

- poll fresh option and underlying truth on the existing monitor cadence;
- reprice only after broker status proves the prior limit remains open and unfilled;
- allow at most two in-place reprice/cancel-replace actions;
- preserve an explicit maximum chase bound from the latest fresh executable quote and original setup economics;
- stop immediately if trigger direction reverses, stop is broken, target is complete, spread becomes unacceptable, quote truth is unavailable, or account risk no longer permits the trade.

The 75-second window is not permission to pay any price. It is time to keep managing an approved opportunity while its thesis remains valid.

### 3. Reprice from current contract truth

Before each reprice or replacement:

- fetch the exact current contract quote from the canonical data broker;
- fetch fresh underlying quote truth;
- validate ticker-specific trigger direction;
- validate spread and quote provenance;
- recompute final limit from the current executable market;
- recompute affordable quantity if necessary, allowing small configured rounding tolerance but never increasing past client and intelligence hard caps;
- preserve the same contract only while it remains liquid, affordable, and aligned;
- if the exact contract is no longer valid, allow one fresh selector pass under PR #389's single direct-quote budget authority.

Never compare a sandbox-canned quote to a live selector quote and call the resulting gap a market move.

### 4. Policy continuity

A retry must inherit the original setup's static admission decision and rerun only facts that can legitimately change.

Inherited and immutable unless explicitly invalidated:

- scanner eligibility and score floor result;
- original session/setup approval;
- client identity and execution mode;
- whether the setup was allowed during the first 30 minutes;
- original signal validity window.

Revalidated on every attempt:

- current trigger direction;
- current option/underlying quote freshness and provenance;
- spread and liquidity;
- available equity and open-position exposure;
- kill switch and daily loss stop;
- duplicate/same-symbol exposure;
- contract affordability;
- target/stop geometry.

The retry may not call a generic path that rejects solely because current clock time is before 10:00 when the original approved order was already permitted in that same window. Persist an explicit `time_gate_policy_id` or equivalent approval proof rather than silently bypassing all time safety.

### 5. Broker sequencing

- Never submit a replacement until the prior broker order is confirmed canceled or terminal.
- A cancel timeout must not create a replacement.
- A late fill after cancel request must adopt the fill and suppress replacement.
- Partial fill must reduce or eliminate replacement quantity.
- Broker submit/cancel identities must remain unique and traceable to one entry lifecycle.
- No retry may mutate positions or proof trades before a broker fill is confirmed.

### 6. Terminal outcomes

Canonical terminal reason codes:

- `ENTRY_RETRY_FILLED`
- `ENTRY_RETRY_THESIS_REVERSED`
- `ENTRY_RETRY_QUOTE_UNAVAILABLE`
- `ENTRY_RETRY_SPREAD_INVALID`
- `ENTRY_RETRY_CHASE_LIMIT_EXCEEDED`
- `ENTRY_RETRY_CONTRACT_UNAVAILABLE`
- `ENTRY_RETRY_RISK_CHANGED`
- `ENTRY_RETRY_DEADLINE_EXPIRED`
- `ENTRY_RETRY_CANCEL_UNCONFIRMED`
- `ENTRY_RETRY_LATE_FILL_ADOPTED`

A retryable data failure must not be mislabeled as a dead thesis. A dead thesis must not remain armed indefinitely.

## Expected production files

- `ap/order_monitor.py`
- `ap/retry_engine.py`
- `ap/post_cancel_retry.py`
- `ap/execution.py` only for a dedicated approved-retry submit seam
- `ap/order_state_machine.py` only for canonical lifecycle transitions
- `ap_entry_watcher.py` only if existing owner recovery is required
- `.github/workflows/p0_regression.yml`
- `tests/test_p0_entry_retry_continuity.py`

Do not edit exit logic, intelligence scoring, proof taxonomy, scanner thresholds, or queue fanout.

## Required tests

1. BAC production replay: initial order unfilled at `0.66`, current contract `0.94`, still-valid underlying. The lifecycle manages the opportunity for the bounded window and does not fail solely on `time_gate`.
2. Original pre-10AM permission is inherited by its exact retry; a brand-new unrelated signal remains subject to the normal time gate.
3. Direction reversal during the window blocks reprice and returns the setup to the PR #391 re-arm lifecycle.
4. Valid rebreach performs exactly one fresh submit.
5. Broker cancel unconfirmed: zero replacement submits.
6. Late fill after cancel request: fill adopted, replacement suppressed.
7. Partial fill: replacement quantity equals only the unfilled remainder and stays within limits.
8. Restart between cancel and replacement: one durable owner and no duplicate broker POST.
9. Contract no longer liquid: one bounded fresh selection under PR #389 authority.
10. Quote domain unknown/sandbox-only for PAPER market truth: no reprice submit.
11. Exact `client_id` and `execution_mode` survive every new order row and decision event.
12. No position, proof trade, or daily filled-trade count is created before fill confirmation.

## Ordering

- Rebase after PR #389 if it merges first.
- Reuse PR #391's final market-validity result and re-arm states.
- Do not recreate direct-quote budget logic.

## Merge gates

- Production implementation added on the branch.
- Focused exact-head tests green.
- Exact-head P0 workflow green.
- Controlled trace proving one original order, confirmed cancel, one replacement, and no duplicate exposure.
- No merge without explicit approval from Angel.
