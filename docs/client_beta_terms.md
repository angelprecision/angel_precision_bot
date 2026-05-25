# Angel Precision — Client Beta Terms

**Status:** Beta access agreement for Proof-Week and the LIVE canary period.
**Owner:** Angel Precision Intelligence (the "Operator").
**Last updated:** 2026-05-24.

These terms govern access to the Angel Precision autonomous options
trading system (the "Bot") during the closed beta. They are written to
be plain-language and protective for both sides. They are NOT investment
advice, NOT a guarantee of returns, and NOT a registered advisory
relationship. Each client trades their own brokerage account under their
own name. The Operator never takes custody of client funds.

---

## 1. Who this covers

This document applies to clients who have placed a beta access deposit
with Angel Precision Intelligence prior to LIVE activation:

- **Client A** — beta access deposit: **$250 USD** (paid).
- **Client B** — beta access deposit: **$350 USD** (paid).

The deposit amount difference reflects onboarding timing and is not a
performance tier. Both clients receive identical execution behaviour,
signal access, risk gates, and support.

---

## 2. What the deposit buys

The deposit grants the client:

1. **Proof-Week access (Week 1)** — the Bot runs in PAPER mode against
   the client's signal stream and broker credentials. The client
   receives the daily proof report (per Section 5) every trading day.
2. **LIVE canary onboarding (Week 2)** — subject to the criteria in
   Section 4, the Bot is flipped to LIVE for the client's broker
   account with hard caps and an explicit kill switch.
3. **Direct operator support** during Proof-Week and the LIVE canary —
   bug reports answered within one trading day, kill-switch requests
   honoured within 15 minutes during US market hours.

The deposit is **non-refundable after the first PAPER trading day
completes** because it covers the Operator's per-client setup,
provisioning, and proof telemetry. If the Bot fails to produce a daily
proof report on Day 1 of Proof-Week, the deposit is refunded in full.

---

## 3. Monthly subscription (post-Proof-Week)

The **$2,000 USD per month** subscription **starts only when the
client's account is flipped to LIVE** and stays LIVE for at least one
full trading day. It does **not** start during Proof-Week, and it does
**not** start during PAPER replay periods.

- Billing cadence: monthly, invoiced on the LIVE-flip anniversary.
- Pro-ration: if LIVE is paused for more than two consecutive trading
  days for operator-caused reasons (Bot bugs, broker integration
  outages on Operator's side, missing signals), that period is credited
  back on the next invoice.
- Pro-ration does **not** apply to:
  - Broker outages caused by the client's broker (Tradier API down,
    client-side account restrictions, margin calls, etc.).
  - Client-requested pauses (kill switch toggled by client).
  - Market-wide halts.
- Cancellation: 7 days' written notice via email. Cancellation flips
  the account to PAPER immediately on receipt; billing stops at the end
  of the current paid month.

---

## 4. LIVE canary rules (Week 2)

LIVE is **not** automatic at the end of Proof-Week. The Bot only flips
to LIVE for a client when **all** of the following are true:

1. Proof-Week generated **at least 4 of 5 daily proof reports** with no
   missing or corrupt entries.
2. **No `BROKER_ERROR_CIRCUIT_OPEN`** events during normal operation
   that day (a brief test trip is OK if it cleared within
   `BROKER_ERROR_CLEAR_AFTER_SECS`).
3. **No `RECONCILER_STALE` warnings** for that client.
4. **No cross-tenant signal leakage** observed in the audit log.
5. **Fill rate \u2265 40%** and median `seconds_to_fill` \u2264 30s on
   broker-confirmed entries (per `scripts/daily_proof_report.py`).
6. Client acknowledges via written message: "Approve LIVE flip for
   account ending in XXXX, max daily loss $200 or 2%, max 1 to 2 open
   positions."

LIVE canary caps for **each** client during the first calendar month
LIVE:

| Cap | Value | Enforced by |
| --- | --- | --- |
| Account capital exposed | $10,000 to $25,000 USD (client chooses) | Tradier account funding |
| Max contracts per order | `MAX_CONTRACTS=15` (system-wide hard cap) | `ap.position_sizing` |
| Max open positions per symbol | 1 | `MAX_OPEN_PER_SYMBOL=1` |
| Max open positions per sector | 2 | `MAX_OPEN_PER_SECTOR=2` |
| Max daily loss | $200 or 2% of account, whichever lower | Kill-switch trigger |
| Max concurrent open positions | 1 to 2 | Risk gate |

Client A goes LIVE first. Client B remains PAPER for at least one
additional clean trading day after Client A flips. If Client A trips a
kill switch in the first 48h LIVE, Client B's LIVE flip is delayed
until the cause is identified, fixed, and re-proven in PAPER for one
trading day.

---

## 5. Daily proof report

Every trading day, the Operator delivers (via email or dashboard) the
output of `scripts/daily_proof_report.py` for the client, containing:

- Total signals routed to the client and the count actually attempted.
- Per-trade summary: ticker, side, contracts, **broker-confirmed**
  entry and exit fill prices, P/L in dollars and percent, exit reason.
- Fill statistics: fill rate, median seconds-to-fill, count of
  broker-confirmed vs. estimated fills.
- Any `RECONCILER_STALE`, `BROKER_ERROR_CIRCUIT_OPEN`,
  `EXIT_PROOF_FINALIZE_SKIPPED_ALREADY_LOGGED`, or
  `[STALE_ENTRY]` events that occurred that day.
- Net account P/L reported by Tradier and the Bot's calculated P/L for
  cross-check (target delta: within $1).

The proof report is the source of truth. If the proof report and the
client's Tradier statement disagree by more than $1 on closed trades,
the Operator pauses LIVE for that client and reconciles before resuming.

---

## 6. What the Operator guarantees

- The Bot will respect the configured risk gates: `MAX_CONTRACTS`,
  `MAX_OPEN_PER_SYMBOL`, `MAX_OPEN_PER_SECTOR`, kill-switch state, and
  `allow_live_trading` per-client flag.
- The Bot will never trade outside the client's broker account.
- The Bot will never share, sell, or transmit client credentials.
  Broker credentials are stored encrypted at rest with a per-process
  `ENCRYPTION_KEY`; plaintext is never logged.
- Pod isolation: when running in shared infrastructure, the Bot
  enforces `SINGLE_CLIENT_EMAIL` or `POD_ID` filtering so a client's
  signals only route to that client's account.
- The exit engine's broker-confirmed fill price (not the limit-order
  estimate) is what gets written to the proof log for closed trades.
- The kill switch, when toggled, halts new entries within 15 seconds
  and force-closes open positions on the next valid quote.

---

## 7. What the Operator does NOT guarantee

- **No performance guarantee.** Options trading involves substantial
  risk of loss including total loss of capital deployed. Past PAPER
  performance does not predict LIVE results.
- **No uptime SLA.** The Bot is a beta product. Reasonable best-effort
  availability during US equity market hours (9:30am to 4:00pm ET) is
  the target; outside those hours the system may be in maintenance.
- **No tax, legal, or registered investment advice.** The Operator is
  not a registered investment advisor. The client is solely responsible
  for tax reporting, suitability, and compliance with their broker's
  terms.
- **No protection against broker-side issues** — exchange halts,
  Tradier outages, order rejections at the broker, settlement delays,
  margin calls, or account-level restrictions are the client's
  responsibility.

---

## 8. Client responsibilities

- Maintain the brokerage account with sufficient buying power and
  options approval level (minimum: Tradier Tier 3 / Level 3 equivalent).
- Provide and keep current the API credentials needed for the Bot to
  trade. Notify the Operator within one business day of any credential
  rotation or account change.
- Acknowledge daily proof reports within one trading day during the
  beta. Discrepancies must be reported within 24 hours.
- Use the kill switch (provided via dashboard) at any time to stop
  trading.
- Do **not** trade the same account manually while the Bot is LIVE on
  that account. Concurrent manual trades will void the daily proof
  reconciliation for that day and may force the Operator to flip the
  account back to PAPER pending audit.

---

## 9. Termination

Either side may terminate at any time with 7 days' written notice.
Immediate termination by the Operator is permitted if:

- The client trades the LIVE account manually while the Bot is LIVE
  (per Section 8) more than twice in a calendar month.
- The client requests behaviour that violates broker terms, securities
  regulations, or these terms.
- Credential or account access is revoked without notice and not
  restored within 5 business days.

Immediate termination by the client is permitted if:

- The Bot causes a confirmed financial loss attributable to a software
  defect (not a market outcome). In this case, the Operator will
  refund any unconsumed portion of the current month's subscription
  and the original beta deposit, and assist with reconciliation.

On termination, the Operator will:

- Flip the client's account to PAPER mode immediately.
- Force-close any open positions opened by the Bot (client may opt to
  keep them open and manage manually instead, in writing).
- Delete encrypted client credentials within 7 days.
- Deliver a final proof report covering all trades up to termination.

---

## 10. Acceptance

By placing the beta deposit, the client agrees to these terms.

The client's countersignature (digital, by email reply containing the
exact phrase **"I accept the Angel Precision beta terms dated
2026-05-24"**) is required before the Bot will be flipped to LIVE for
that client. PAPER Proof-Week runs without a countersignature so the
client can evaluate the Bot first.

---

### Appendix A — Operator contact

- Operator: Angel Precision Intelligence
- Primary contact: founder@angelprecision (or the email the client was
  onboarded with)
- Kill switch: dashboard \u2192 client controls \u2192 "Halt trading"
- Emergency: reply to the most recent daily proof report email with
  subject prefix `[URGENT KILL]` \u2014 the Operator monitors this
  alias during market hours.

### Appendix B — System telemetry referenced in this document

| Marker | Meaning | Source |
| --- | --- | --- |
| `BROKER_ERROR_CIRCUIT_OPEN` | Broker error threshold breached; new entries paused | `ap.broker_circuit` |
| `RECONCILER_STALE` | Order ACK age exceeded `RECONCILER_SLA_SECONDS` (120s) | `scripts/stale_entry_audit.py` |
| `EXIT_PROOF_FINALIZE_SKIPPED_ALREADY_LOGGED` | Idempotency guard fired; proof was already written | `_finalize_proof` in `ap_execution_core.py` |
| `[STALE_ENTRY]` | Entry order older than SLA without terminal status | `scripts/stale_entry_audit.py` |
| `DAILY_PROOF_REPORT_GENERATED` | Daily proof report finished writing | `scripts/daily_proof_report.py` |
| `READY_FOR_PROOF_WEEK` | Render readiness verification passed | `scripts/verify_render_readiness.py` |

---

*This document supersedes any verbal understanding between the
Operator and beta clients regarding the items it covers. It does not
modify the client's separate agreement with their broker. Where these
terms conflict with applicable law, applicable law controls and the
remaining terms remain in effect.*
