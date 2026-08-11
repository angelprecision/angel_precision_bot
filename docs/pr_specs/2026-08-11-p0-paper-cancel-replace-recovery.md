# P0 Work Order — PAPER entry cancel/replace recovery correctness

This branch intentionally contains only the work-order specification. Production code changes are not authorized by this commit. Codex should implement the PR against this branch and preserve the scope below.

## Incident

On 2026-08-11, PAPER accounts selected and submitted contracts that did not fill, then failed during cancel/replace recovery. Production rows included:

- José PAPER AAPL: original order submitted, canceled, replacement path failed with HTTP 400 from Tradier sandbox
- José PAPER TSLA: same failure shape
- Tradefluence PAPER AAPL: same failure shape
- Tradefluence PAPER AVGO: same failure shape
- Tradefluence PAPER TSLA: stale-entry cancel after underlying option price moved ~14% above the original limit; this is a separate valid missed-move terminal path and must remain protected

Current durable error examples are too weak:

`replace_bad_response_after_cancel:400 Client Error: for url: https://sandbox.tradier.com/.../orders`

The response body/provider reason is not preserved in the order diagnostics, making it impossible to distinguish malformed replacement request, invalid order state, pricing rejection, unsupported replace semantics, or broker-side validation failure.

## Required implementation

1. Trace the exact PAPER entry lifecycle from initial submit through order monitor, cancel confirmation, replacement construction, replacement submit, and terminal/retry persistence.
2. Reproduce the 2026-08-11 AAPL/TSLA/AVGO sandbox shapes through the real broker adapter and order-monitor/retry seam.
3. Preserve the Tradier HTTP status and parsed response payload/body for failed replacement submits in durable order metadata/diagnostics. Do not store secrets/auth headers.
4. Distinguish these states explicitly:
   - original order still active/unconfirmed
   - original cancel requested but not broker-confirmed
   - original cancel broker-confirmed
   - replacement submit accepted
   - replacement submit rejected by broker
   - replacement state ambiguous/provider unavailable
   - missed-move terminal
5. A replacement may be submitted only after broker truth proves the original order can no longer fill. Never create overlapping live entry orders for the same logical attempt.
6. Preserve original broker order ID, replacement broker order ID if one exists, local order ID, client ID, execution mode, signal ID, contract, quantity, original limit, replacement limit, timestamps, and exact recovery attempt number.
7. Fail closed on ambiguous broker state. Do not blindly resubmit after transport timeout/unknown response.
8. Preserve `STALE_ENTRY_CANCEL MISSED_MOVE` behavior when price has materially escaped the allowed chase envelope. Do not turn missed moves into aggressive market chasing.
9. Keep PAPER and LIVE execution semantics isolated. This PR is driven by PAPER sandbox evidence. Any shared helper touched must have explicit LIVE regression proof that no LIVE submit/cancel authority expands.
10. Do not alter contract-selection thresholds, trigger logic, sizing, exits, positions, proof_trades, or selector recovery policy.

## Investigation targets

Inspect the real production paths responsible for:

- stale entry monitoring
- cancel request and cancel confirmation
- replacement order request construction
- broker response parsing
- replacement retry/terminal disposition
- order metadata persistence

Likely files may include `ap/order_monitor.py`, Tradier broker adapter/order submission code, `ap/order_state_machine.py`, and any entry retry helper. Codex must determine actual call ownership before editing; do not scatter fixes across unrelated modules.

## Required tests

### Broker-rejected replacement

Initial PAPER order is accepted, remains unfilled, cancel is confirmed, replacement submit returns HTTP 400 with a Tradier payload.

Expected:

- no overlapping active order remains
- response status and sanitized provider payload are durably preserved
- deterministic classified reason is stored
- original and replacement identities remain linked
- behavior follows the chosen explicit retry/terminal policy
- no position/proof-trade is created

### Ambiguous cancel

Cancel request times out or broker status cannot prove cancellation.

Expected: no replacement submit; fail/hold closed with broker-state ambiguity diagnostics.

### Replacement accepted

Cancel is confirmed and replacement is accepted.

Expected: exactly one replacement order, correct identity/qty/contract/mode, no duplicate submit.

### Missed move

Price moves beyond configured chase tolerance before replacement.

Expected: preserve terminal missed-move cancellation; no replacement/market chase.

### LIVE fence

Run equivalent shared-helper tests under LIVE mode and prove no new LIVE retry/replace authority is introduced accidentally. Existing LIVE broker submit/cancel semantics must remain unchanged unless separately authorized.

### Restart/idempotency

Restart between cancel confirmation and replacement submit, and restart after replacement acceptance but before DB persistence.

Expected: broker truth reconciliation prevents duplicate replacement submissions and preserves one logical entry attempt.

## Observability requirements

Durable diagnostics should include, where safe:

- original broker order id/status
- cancel request/confirmation timestamps
- replacement attempt number
- replacement request price/qty/contract
- HTTP status
- sanitized Tradier response payload/error code/message
- broker base URL/domain class (sandbox/live)
- execution mode
- final monitor action/reason
- ambiguity classification when broker truth cannot be established

## Production safety gate

Before merge, audit exact head for broker submit/cancel exposure, duplicate-order risk, restart idempotency, PAPER/LIVE taxonomy isolation, order-state mutation correctness, and zero unintended position/proof-trade mutation.

## Merge policy

Draft only until exact-head tests and production-shape audit pass. Do not merge or deploy without explicit approval.