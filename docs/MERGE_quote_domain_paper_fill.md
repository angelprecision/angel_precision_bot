# MERGE FILE — Quote-Domain Audit + Paper/Live Execution Separation

**Status:** AWAITING REVIEW — committed to branch only, NOT pushed, NOT merged to main.
**Branch:** `feat/quote-domain-audit-paper-fill`
**Files touched:** `ap/execution.py`, `ap_entry_watcher.py`, `ap/brokers/tradier.py` (none),
new: `tests/test_quote_domain_paper_fill.py`, `sql/validation/quote_domain_audit.sql`.

**Did NOT touch (per scope):** exits, sizing, strategy scoring, client_runner, proof
logger, dashboard rewrite, rearm logic, retry engine.

---

## The root-cause finding (read this first)

The quote-domain mismatch the spec suspected is REAL and visible in code:

| Component | Broker used | Quote domain |
|---|---|---|
| Contract selector | `data_broker` | LIVE (`api.tradier.com`) when `TRADIER_DATA_TOKEN` set |
| Entry watcher | execution `broker` | SANDBOX (`sandbox.tradier.com`) for paper |
| Submit / fill | execution `broker` + Tradier sandbox | SANDBOX (delayed) for paper |

`client_runner.py:1603-1607`: `TRADIER_DATA_BASE_URL` defaults to `https://api.tradier.com`.
If `TRADIER_DATA_TOKEN` is set, the **selector picks contracts on LIVE quotes** while the
**watcher and paper fill judge against delayed SANDBOX quotes**. Tight live-like limits then
don't fill in paper even when the contract later goes green. That is exactly the distortion
described. This PR makes it provable per-order and gives paper a way to fill again WITHOUT
weakening live.

---

## What this PR does

### 1. Quote-source evidence on every ENTRY order (`ap/execution.py`)
New fields persisted into `orders.meta`:
- `selector_quote_source`, `selector_quote_base_url`
- `submit_quote_source`, `submit_quote_base_url`
- `tradier_sandbox_mode`, `mode`
- `quote_domain_mismatch_possible`
- `paper_fill_mode`, `paper_submitted_type`, `paper_cushion_applied`, `live_submit_limit`

`quote_domain_mismatch_possible = true` when: mode=PAPER AND submit broker is sandbox AND
selector source is non-sandbox (live) AND selector source is known. Read off the live broker
objects — never assumed.

Existing submit-quote evidence (selector_ask, submit_bid/ask/mid/last, submit_limit,
quote_age_ms, spread_pct, gap_pct, submit_refresh_ok/reason) was already wired by PR-A/PR-H
and is preserved.

### 2. Watcher quote source (`ap_entry_watcher.py`)
`_fetch_quotes` uses `self.broker.session` + `self.broker.cfg.base_url` (confirmed by reading
the code, not assumed). Added `_watcher_quote_identity()` which surfaces the ACTUAL base_url
into every `watcher_audit` payload:
- `watcher_quote_source`, `watcher_quote_base_url`, `watcher_sandbox_mode`

### 3. Paper-only fill mode (`ap/execution.py`)
New env `PAPER_ENTRY_FILL_MODE = marketable_limit | market` (default `marketable_limit`).

- **marketable_limit** (default): paper submits `submit_ask + cushion`, where
  `cushion = min(submit_ask * PAPER_ENTRY_SLIPPAGE_CUSHION_PCT, PAPER_ENTRY_MAX_CUSHION_DOLLARS)`,
  floored at $0.01. Defaults: 10% / $0.20. The original live-equivalent limit is preserved in
  `live_submit_limit` for evidence.
- **market** (paper only): submits a true market order (`limit_price=None`). HARD-BLOCKED in
  LIVE by two independent guards.
- **LIVE**: ignores `PAPER_ENTRY_FILL_MODE` entirely. Always normal marketable limit.

### HARD INVARIANTS (tested)
- Market orders are IMPOSSIBLE in LIVE from this patch. Two guards:
  1. The paper-fill branch only runs when `_is_paper` is True.
  2. The call site re-asserts `paper_market_order AND _is_paper AND mode == "PAPER"` before
     passing `order_type="market"`.
- LIVE limit logic is byte-for-byte unchanged (the paper branch is skipped entirely).
- Default `PAPER_ENTRY_FILL_MODE=marketable_limit` — conservative; market is opt-in.

### 4. Verification SQL (`sql/validation/quote_domain_audit.sql`)
Four read-only queries:
- A. Paper no-fill but contract went green
- B. Watcher reject source (which Tradier env)
- C. Quote mismatch evidence (selector vs submit vs broker env)
- D. Paper fill-mode effectiveness (fill % by mode)

---

## What I did NOT implement, and why (honest gaps)

The spec's item 4 asked for after-trigger tracking fields: `MFE_after_trigger`,
`MAE_after_trigger`, `max_contract_price_after_submit`, `min_contract_price_after_submit`,
`time_to_green`, `time_to_max_MFE`, `live_simulated_entry_price`, `went_green_after_trigger`.

These require a **new post-trigger price-tracking loop** that polls the contract for some
window after submit and records the extremes. That is a meaningfully larger change than
"add evidence at decision time" and would touch the exit/monitor loop, which the scope
says NOT to touch. The verification SQL references these as `meta` fields so they slot in
cleanly when that tracker is built as its own PR. I flagged this rather than half-build a
tracker that silently records nothing.

The dashboard display (spec item 5) is a separate frontend change in the website repo. The
backend evidence is now all in `orders.meta`, so the dashboard work is purely additive
display — recommend it as the next small PR after this merges.

`selector_bid/mid/last` remain `None` (the PR-F markers). The selector currently returns
only the accepted ask. Wiring full bid/mid/last from the selector is the "selector candidate
audit" hardening item listed as #2 after this PR — it belongs there, not here.

---

## Tests
`tests/test_quote_domain_paper_fill.py` — 16 tests, all pass in isolation. Covers sandbox
detection, broker quote identity (sandbox/live/unknown), paper cushion math (dollar cap,
pct, penny floor, zero-ask, custom env), the market-order limit_price=None guard, and the
watcher quote identity helper.

**Regression:** P0 regression suite (29) and entry watcher audit (26) both pass in isolation.
The 2 failures seen in a combined run are PRE-EXISTING `importlib.reload()` cross-file
pollution (documented in docs/error_audit_2026-05-24.md) — confirmed identical on a clean
tree with my changes stashed. Not caused by this PR.

---

## Rollout
1. Review + merge this branch.
2. Deploy. Default `PAPER_ENTRY_FILL_MODE=marketable_limit` — paper limits become slightly
   more marketable; LIVE unchanged.
3. Run one monitored paper session.
4. Run `sql/validation/quote_domain_audit.sql` queries A-D.
5. If `quote_domain_mismatch_possible=true` dominates the no-fills, the distortion is
   confirmed and (optionally) try `PAPER_ENTRY_FILL_MODE=market` in PAPER for one session.
6. Judge the bot by signal direction + contract behavior after trigger, NOT paper fill rate.
7. Then live beta, 1 contract per valid signal.

## Env vars added (all optional, safe defaults)
| Var | Default | Effect |
|-----|---------|--------|
| PAPER_ENTRY_FILL_MODE | marketable_limit | paper-only submission policy |
| PAPER_ENTRY_SLIPPAGE_CUSHION_PCT | 0.10 | paper cushion as % of ask |
| PAPER_ENTRY_MAX_CUSHION_DOLLARS | 0.20 | paper cushion hard cap ($/share) |

None affect LIVE.
