# P0 #573 — Bound after-hours WATCHING readiness by post-close deadline

Status: HARD HOLD — awaiting independent audit
Base:   `main@e9f04bd409696a36ad2d933ced5b1dee4b81ee0d` (post-#572 revert)

## Defect

Wed 2026-09-02 after close, scanners produced 244 intentional after-hours
WATCHING rows for `jasoncosby1@gmail.com` between 20:54:05Z and 22:50:38Z.
Each carried:

    status     = 'WATCHING'
    last_error = 'after_hours_deferred:awaiting_overnight_reeval'
    (no matching ENTRY order — by design; awaiting next overnight/preopen)

The legacy 5-minute orphan rule in `ap/preopen_readiness.py` classified
all 244 as `watching_rows_missing_orders_recommend_new_rescue`, turning
startup preopen readiness DEGRADED and blocking LIVE entries.

## Fix — readiness interpretation only

The classifier `_classify_watching_row` and its caller
`_query_client_state` in `ap/preopen_readiness.py` recognise the exact
durable deferred lifecycle and bucket each WATCHING-row-without-matching-
ENTRY into one of three outcomes consumed by
`run_preopen_autonomous_readiness()`:

    expected_after_hours_deferred   - legitimate parked inventory before
                                      the next overnight-reeval deadline.
                                      NONBLOCKING (diagnostic only).
    after_hours_deferred_overdue    - same evidence, deadline elapsed
                                      with the row still parked.
                                      BLOCKING via new LIVE blocked_key.
    ordinary_orphan                 - fails any gate; existing 5-minute
                                      grace + block behaviour preserved.

A row earns the deferred exemption iff ALL of the following:

  1. `client_id` matches the readiness client (SQL-scoped);
  2. `status` is exactly `'WATCHING'`;
  3. `last_error` is exactly the marker
     `after_hours_deferred:awaiting_overnight_reeval`
     (no substring, no case-fold, no whitespace bypass);
  4. `created_ts` is tz-aware, non-future, within
     `PREOPEN_AFTER_HOURS_DEFERRED_MAX_LOOKBACK_DAYS` (default 10);
  5. source session is POST-CLOSE on an NYSE trading day
     (`created_ts` in ET is >= 16:00 on an authoritative trading day);
  6. no matching ENTRY order exists (SQL `WHERE NOT EXISTS` gate);
  7. the next-trading-session `OVERNIGHT_REEVAL_DUE` (default 09:18 ET)
     deadline has not passed.

Deadline is walked forward through `ap.flatline_alarm.is_trading_day`.
Friday post-close correctly resolves to Tuesday when Monday is a
market holiday.

## Amendment (2026-09-03) — payload.execution_mode is NOT a gate

An earlier iteration required `trade_queue.payload['execution_mode']`
to exist and match the readiness mode. The 244 real production rows do
NOT carry that field — the queue producer never wrote it for this
path. Requiring it silently rejected legitimate inventory and would
still block Jason.

Mode safety is provided by:

  * `client_id` scoping in the SQL (each runtime client has one
    `client_id`);
  * the POST-CLOSE-on-trading-day source-session gate; and
  * the SQL `WHERE NOT EXISTS` on `orders` (any downstream ENTRY, of
    any mode, removes the row from every bucket).

Same-client mixed-mode (one `client_id` running PAPER and LIVE
simultaneously) is not currently a produced shape. If it becomes one,
it will be handled in a separate PR against evidence — not by
fabricating producer mode provenance the queue never persisted.

## Amendment (2026-09-03 review 5101059272) — future-timestamp fail-closed

`_query_client_state`'s ordinary-orphan grace-window skip was:

    if created_utc is not None and created_utc >= watching_cutoff:
        continue

A future `created_ts` is trivially `>= watching_cutoff`, so a
corrupt future-dated row was skipped by the grace window and
silently vanished from readiness — neither counted as deferred nor
reported as orphan. Corrected to:

    if (created_utc is not None
        and created_utc <= now_utc
        and created_utc >= watching_cutoff):
        continue

Only a proven non-future fresh row receives the existing 5-minute
grace. A real-Postgres regression asserts a future-ts WATCHING row
lands in `watching_orphans`.

## Fail-closed conditions (any → ordinary_orphan)

  * client_id mismatch or blank
  * status != 'WATCHING'
  * last_error != exact marker
  * created_ts naive / malformed / null / future
  * row older than `PREOPEN_AFTER_HOURS_DEFERRED_MAX_LOOKBACK_DAYS`
  * source session = pre_market / regular_session / non_trading_day
  * NYSE calendar cannot resolve source session or deadline

Global `post_overnight_reeval=success` does NOT excuse an overdue row.

## Scope — intentionally UNCHANGED

Scanner, `ap/queue.py` producer (the marker written by the producer is
CONSUMED as-is), master control, `ap_overnight_reeval`, selector,
contract selection, risk, sizing, exits, `proof_trades`, broker
submit/cancel, order reconciliation, order state machine, positions,
PR #569 first-time watcher-ownership deadline. Zero mutation added —
classifier reads only. Zero broker call, zero watcher registration.

## Test evidence

`tests/test_p0_after_hours_watching_readiness.py` — one file, three
tiers:

  * **Classifier unit (24)** — 244-row post-close shape without
    `payload.execution_mode`; empty/null payload; deadline
    boundaries; marker substring / case / whitespace; pre-market
    and regular-session contradictions; timestamp corruption;
    lookback bound; Fri→Tue weekend + Labor Day; client/status
    gates.
  * **Session helper unit (7)** — each session kind + calendar
    fail-closed.
  * **End-to-end `run_preopen_autonomous_readiness` (7)** — 244
    rows do not block LIVE; overdue blocks; ordinary orphan still
    blocks; global overnight success does not excuse overdue;
    unowned pending trigger still blocks; stale processing still
    blocks; zero broker/watcher across three timestamps spanning
    09:29:30.
  * **Module-source invariants (2)** — no mutation SQL added;
    marker constant matches producer.
  * **Real Postgres 17 integration (4)** — 244-row production-shape
    replay through the real production SELECT; mixed state including
    CONTRADICTORY row (marker AND matching FILLED ENTRY — SQL
    `WHERE NOT EXISTS` gates it out); pre-market and regular-session
    contradictions rejected end-to-end; future-timestamp WATCHING row
    lands in `watching_orphans`.

CI provisions Postgres 17 via `INTELLIGENCE_POSTGRES_TEST_URL`.
Skipped locally.

## Files changed

  ap/preopen_readiness.py                            (only production file)
  tests/test_p0_after_hours_watching_readiness.py    (new)
  tests/test_p0_live_readiness_blocked_keys.py       (lambda kwargs)
  tests/test_p0_preopen_autonomous_readiness.py      (lambda kwargs)
  .github/workflows/p0_regression.yml                (register new test)
  docs/pr_specs/p0_after_hours_watching_readiness_20260902.md
