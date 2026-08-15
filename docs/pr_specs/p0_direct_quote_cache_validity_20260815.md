# P0: Reject invalid direct option quotes before the cache write

**Date:** 2026-08-15
**Base:** `main@b43ce9c53433bd0479baa87e9757b50740adaa01`
**Scope:** Surgical. One production file: `ap/contract_quote_revalidator.py`.

## Phase 1 — reproduced on current main (evidence)

Traced: `broker.get_quote` -> `_normalize_quote` scalar parse -> `_QUOTE_CACHE`
write -> cache-hit read.

Both direct-fetch paths (`fetch_direct_option_quote_with_meta` and
`fetch_direct_option_quote`) wrote `_normalize_quote()` output to `_QUOTE_CACHE`
**unconditionally**. A standalone reproduction against a stub broker showed all
of the following observations were cached on current main:

None, boolean, malformed string, zero bid, zero ask, negative, NaN,
+Infinity, -Infinity, overflow (1e400 -> inf), inverted (ask < bid).

Poisoning confirmed: after an invalid observation, a second fetch returned the
cached invalid quote and **did not** re-read the provider (`get_quote` call
count = 1 across two fetches). Both paths share the defect.

Two observations additionally passed the downstream `direct_quote_is_valid`
gate on current main: **boolean True** (normalized to bid=1.0) and **NaN**
(because `nan <= 0` and `nan < bid` are both False). Those would have entered
selector authority as "valid".

`#471` (feae216) touched only the direct-quote *budget* surface, not
normalization, the cache writes, or validity. It did not change these
semantics. Note: commits `45e060b` / `ecfc40b` had previously fixed
invalid-quote caching but were removed by the Aug 10 rollback (`cb45860`); the
prior fix also relied on `direct_quote_is_valid`, which still admitted NaN/bool.

## Phase 2 — fix

1. `_normalize_quote._f` / `_i`: reject `bool` outright and reject non-finite
   (`math.isfinite`) so an unusable scalar normalizes to `None` rather than a
   poisonous numeric value.
2. New `quote_is_cache_eligible(quote)`: bid and ask must both be finite,
   strictly positive, and uncrossed (`ask >= bid`). New `_strict_finite_positive`
   helper enforces the bool/NaN/Inf/positivity rules.
3. Both cache writes are gated: `if quote_is_cache_eligible(...): _QUOTE_CACHE[...] = ...`.
   Invalid observations are returned to the caller but never cached, so the next
   eligible attempt performs a fresh provider read.
4. `direct_quote_is_valid` now delegates to the strict eligibility check,
   closing the read-time NaN/bool hole for selector authority.

Valid quotes retain existing TTL/cache behavior. No change to selector
thresholds, spread/OI/volume/delta/DTE logic, client or PAPER/LIVE identity, or
the #471 direct-quote budget authority. No broker submit/cancel. No
orders/positions/proof/trade_queue mutations. No retry-count or
selection-policy expansion.

## Tests (production-path)

`tests/test_p0_direct_quote_cache_validity.py` (16 tests, all pass), covering
the 12 required cases: valid caches + cache-hit avoidance; zero bid; zero ask;
negative; NaN; +/-Infinity; boolean; malformed; overflow; inverted;
invalid-then-valid fresh-read recovery; invalid shapes rejected from selector
authority; #471 budget behavior unchanged.

## Regression

- `test_contract_quote_revalidator.py` + `_hardening.py` — 71 passed.
- Adjacent selector/direct-quote/budget/revalidation suites — 170 passed.
- `test_p0_selector_recovery_capacity_and_validity` — 30 passed;
  `test_selector_reason_honesty` — 19 passed.
- One pre-existing unrelated failure (`test_selector_candidate_audit::test_meta_size_bound`,
  `17441 < 12000`) fails identically with and without this change; not caused here.
- `git diff --check` clean; module compiles.

## Overlap

- **#472** and **#473** do not touch `ap/contract_quote_revalidator.py` — no collision.
- **#464** is the prior implementation branch for this same defect; this PR is a
  fresh current-main implementation and supersedes it.

## Recommendation

Fix is proven and surgical (one file, both paths, strict validity, zero
regressions). Recommend **MERGE** once exact-head P0 + DB hot-path CI are green.
**#464 should be closed as superseded.**

Do not merge without explicit operator instruction.
