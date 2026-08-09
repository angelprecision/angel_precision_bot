# Exact implementation map — broker-owned EXIT_REQUESTED recovery

This amendment is binding on the implementation. It narrows the repair to the exact handoff exposed by the 2026-08-07 ORCL and 2026-07-29 IBM incidents.

## Baseline

Start from current main:

`188f2338de3ca4b3e687aa07fd6b2c5ea4b2ab0b`

Before implementation, confirm main has not moved. If main has moved, re-read the six production files and re-resolve this map before editing. Do not transplant stale hunks by line number.

## File budget

Production, expected exactly:

- `ap/order_state_machine.py`
- `ap/exit_decision_idempotency_guard.py`
- `ap/fill_monitor.py`
- `ap/order_monitor.py`
- `ap/self_healing.py`
- `client_runner.py`

`ap/self_healing.py` is permitted only for narrow runtime-mode provenance wiring into recovery. It does not become a lifecycle owner, add broker behavior, or authorize a generalized self-healing redesign. Missing or unproven execution mode must HOLD; recovery must never default it to LIVE.

`client_runner.py` is permitted only to pass the runner's exact, URL-proven operational mode into `fill_monitor_loop` as canonical lowercase runtime provenance. The boundary maps only exact `LIVE` and `PAPER`; it does not trim, case-fold, or otherwise launder malformed taxonomy. This wiring adds no lifecycle owner and no broker behavior.

Tests:

- one focused `tests/test_p0_broker_owned_exit_requested_recovery.py`
- the adjacent `tests/test_fill_monitor_mvp_hardening.py`
- `.github/workflows/p0_regression.yml` only to add that test if the workflow does not already collect it

No other production file without a documented HARD HOLD dependency finding.

## Patch A — OSM evidence-fenced adoption

Do not alter `OrderStatus.TRANSITIONS` to make `EXIT_REQUESTED -> EXIT_FILLED` legal.

Add one method that atomically adopts proven broker ownership by changing only:

`EXIT_REQUESTED -> EXIT_SUBMITTED`

Required predicate:

```text
client_id == exact OSM client
local_order_id == exact supplied local id
kind == EXIT
status == EXIT_REQUESTED
execution_mode == exact supplied live/paper
broker_order_id == exact supplied broker id OR broker_order_id is null/blank and is set by this same CAS
position_id == exact supplied position when supplied
qty > 0
```

Required update:

```text
status = EXIT_SUBMITTED
broker_order_id = exact broker id
submitted_ts = preserve an existing authoritative value; do not synthesize one
broker_ownership_adopted_at = recovery timestamp/provenance
updated_ts = now()
meta = non-destructive merge of recovery diagnostic
```

The original broker-acceptance time is not necessarily available during recovery. Preserve an authoritative `submitted_ts` when one already exists or is supplied as proven broker evidence; otherwise leave it null. Do not present adoption wall-clock time as broker submission time. `broker_ownership_adopted_at` is the implemented recovery-age authority when original submission time cannot be proven. Broker ownership is established by the exact nonblank `broker_order_id` plus the full recovery identity, not by manufacturing a submission timestamp.

Return a structured disposition or boolean that distinguishes:

- adopted exactly once;
- already adopted with the same exact broker identity;
- identity/CAS miss;
- database failure.

An already `EXIT_SUBMITTED`/`EXIT_ACKNOWLEDGED`/`EXIT_PARTIAL_FILL` row with the exact same broker id is idempotent success. A different broker id is not.

## Patch B — exit-decision durable broker handoff

At the callback seam, distinguish three facts:

1. callback never reached broker / conclusively no submit;
2. callback returned broker acceptance with exact broker id;
3. callback outcome ambiguous.

For case 2, invoke OSM adoption immediately. Do not mark the durable decision generation as ordinary broker-owned until either:

- OSM adoption succeeded; or
- the claim is explicitly persisted as broker-owned durability-gap with exact broker id so restart recovery cannot release it as no-submit.

On adoption failure after broker acceptance:

- zero second broker submits;
- preserve `pending_exit_local_order_id`;
- preserve `pending_exit_broker_order_id`;
- preserve durable generation claim;
- set classified error `BROKER_OWNED_DURABILITY_GAP`;
- alert/recovery owns resolution.

Do not clear in-flight ownership based on callback exception alone when a broker id was returned or independently observed.

## Patch C — fill-monitor recovery query

Keep existing `get_pending_orders()` semantics for normal rows.

Add a separate query/helper such as:

`get_broker_owned_exit_requests(client_id)`

Exact SQL semantics:

```text
WHERE client_id = exact client
  AND kind = 'EXIT'
  AND status = 'EXIT_REQUESTED'
  AND execution_mode IN ('live','paper')
  AND broker_order_id IS NOT NULL
  AND BTRIM(broker_order_id) NOT IN ('', 'N/A')
  AND qty > 0
```

Do not select by contract alone. Do not select broker-less EXIT_REQUESTED intents.

For each candidate:

1. call OSM adoption;
2. reload/normalize the row if necessary;
3. only after adoption, call the existing broker check and existing canonical fill processor;
4. never write position quantity directly from this helper.

## Patch D — order-monitor broker truth advancement

Before `_advance_from_broker_status` or equivalent attempts ACK/PARTIAL/FILLED on a local EXIT_REQUESTED row with exact broker id:

1. adopt broker ownership through OSM;
2. if adoption succeeds/already exact, continue normal broker-status advancement;
3. if adoption fails, emit `BROKER_OWNED_EXIT_REQUEST_RECOVERY_HOLD` and return without clearing ownership or creating a replacement.

This must remove the production loop:

`broker fill seen -> EXIT_REQUESTED->EXIT_FILLED illegal transition -> repeat forever`

Do not bypass OSM with direct SQL in order monitor.

## Required ORCL replay sequence

```text
local EXIT intent created: EXIT_REQUESTED qty=4
broker callback returns id=36661364
simulated durable submit transition failure leaves row EXIT_REQUESTED + broker id
next recovery tick
-> exact broker ownership adopted to EXIT_SUBMITTED
-> broker reports FILLED qty=4
-> normal OSM EXIT_FILLED
-> normal fill reducer applies cumulative qty exactly once
-> no ILLEGAL_TRANSITION
-> no duplicate exit broker POST
```

## Required IBM restart replay sequence

```text
process starts with historical row:
EXIT_REQUESTED + broker id + submitted_ts null
-> startup monitor discovers broker-owned request
-> adopts state
-> broker truth determines active/filled/terminal state
-> no new exit submission simply because process restarted
```

## Prohibited shortcuts

- no generic `EXIT_REQUESTED -> EXIT_FILLED` transition;
- no `EXIT_REQUESTED` polling without broker-id proof;
- no elapsed-time adoption;
- no contract-only matching in LIVE;
- no mode inference from client name/environment;
- no direct position decrement from broker reservation or adoption;
- no direct proof writer;
- no queue mutation;
- no #423 stale-cancel logic;
- no #424 reservation quantity logic;
- no scanner/intelligence/entry changes.

## Audit verdict

HARD HOLD until runtime implementation, focused real-method tests, changed-file audit, comments review, exact-head P0 CI, and a fresh independent release verdict are complete.
