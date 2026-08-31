# P0 Stale EXIT Liveness Under Supported Defaults

Base audit: `main@26d9d81c87c7c97fd1e0d33f83f9730e2e245c47`

## One defect

Current `ap/order_monitor.py` defaults to `ORDER_MONITOR_MODE=watchdog`, and stale EXIT handling can return without cancel/clear/re-arm when the monitor is not allowed to act. A real canonical EXIT can therefore become inert under supported defaults.

## Required invariant

A stale/unfilled canonical EXIT always retains one durable owner and a path to another safe canonical evaluation without requiring a broad operator actor-mode flip.

- exact OPEN/WORKING broker EXIT -> keep/adopt one owner or bounded canonical handling;
- UNKNOWN/error/malformed broker truth -> HOLD, zero duplicate sell;
- PARTIAL/FILLED -> consume exact fill truth, retry only exact remainder if still required;
- exact terminal CANCELED/REJECTED/EXPIRED -> durable re-arm to the existing canonical EXIT owner;
- restart with old broker EXIT open -> adopt, zero duplicate sell;
- restart after proven terminal -> at most one replacement generation;
- supported default config cannot leave position permanently CLOSING with no replacement path.

## Stack position

Implement only after the current exit-ownership/protective-order stack (#516/#522/#532/#546 as applicable on latest main) is settled. Consume its final ownership model. Do not create another EXIT submitter.

## Non-goals

No exit threshold/policy change, scanner, selector, entry, sizing, intelligence, proof taxonomy, queue redesign, broad broker-cancel authority, or second canonical EXIT path.

## Required proof

Default-watchdog stale EXIT; UNKNOWN broker truth; partial fill; late fill during cancel; restart before/after terminal proof; exact remaining quantity; exactly one broker sell maximum per generation.