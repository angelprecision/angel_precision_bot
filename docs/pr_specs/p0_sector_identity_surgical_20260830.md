# P0 #548 — Surgical Master Control sector identity

## Status

**HARD HOLD / DRAFT. Do not merge.** This PR must be based on the exact main that contains dependency #589. #589 owns executable-underlying map coverage; #548 owns only Master Control consumption and fail-closed eligibility.

Dependency #589 is currently present on main at merge commit `3e3bd4d70891bc19c6175e9b732c6e590ee7a7f5`. Rebase #548 onto that exact main before any release decision.

## Scope

Production behavior is limited to `ap_master_control.py`:

- resolve sectors only through `ap.exposure_gate.get_sector`;
- remove the duplicate Master Control map and all synthetic shared-sector authority;
- preserve existing cap percentages, ticker/total/broker capital gates, selector, sizing, position count, side limits, score, and execution mode behavior;
- introduce no broker submit/cancel/replace, order, position, proof-trade, queue, scanner, selector, sizing, or portfolio mutation.

Supporting tests, this binding spec, and the existing P0 workflow registration may change. `ap/exposure_gate.py` must not remain in the final #548 diff unless an independently proven non-#589 requirement is documented.

## Canonical identity invariant

After symbol normalization, `APMasterControl._resolve_sector()` calls only `ap.exposure_gate.get_sector()` and returns the canonical lowercase diagnostic value or `None`. It never returns `OTHER`, `UNKNOWN`, `MISC`, `UNMAPPED`, or a synthetic shared bucket.

Known identity → evaluate sector exposure normally.

Missing, blank, malformed, or unresolved candidate identity → `SECTOR_IDENTITY_UNPROVEN` with `ok=False` at both `evaluate()` and `revalidate_exposure()`.

Every `OPEN`/`CLOSING` position row included in the sector-cap snapshot must also resolve canonically before sector exposure is authoritative. An unresolved active row is not treated as zero exposure, `OTHER`, `UNKNOWN`, or any other shared bucket: the gate returns `SECTOR_IDENTITY_UNPROVEN` with symbol/row diagnostics and performs no sector-cap arithmetic or executable approval.

`evaluate()` resolves the candidate and blocks before durable duplicate/capital work can produce an executable plan. After the required snapshot is read, it checks active-position identity before pending-capital, selector, or broker-facing approval work. `revalidate_exposure()` resolves the candidate before bootstrap clamp or snapshot work; after the snapshot is read, it checks active-position identity before bootstrap clamp, pending-capital reads, resize, or any broker-bound handoff. No unknown candidate or incomplete active-position snapshot reaches selector approval, broker-ready handoff, or broker submission.

Reporting may expose symbol-qualified identity diagnostics, but reporting is not sector-cap authority and unresolved names are never aggregated.

## Required behavioral proof

Use synthetic absent symbols `ZZUNKNOWN1` and `ZZUNKNOWN2`:

- `get_sector()` returns `None`;
- initial evaluation and revalidation both return `ok=False`, `reason=SECTOR_IDENTITY_UNPROVEN`, `reason_code=SECTOR_IDENTITY_UNPROVEN`;
- ticker, total-capital, and available broker capital cannot override the identity block;
- two unknown symbols never aggregate as a risk sector;
- LIVE and PAPER produce the same identity result;
- known same-sector pairs aggregate and saturated caps block;
- required cross-sector pairs do not aggregate;
- unknown approval produces no broker/order/position/proof/queue mutation.

Required known controls include UNH+BMY, WMT+PEP, AAPL+QCOM, T+VZ, COIN+MSTR, AMT+SPG, LIN+DOW, and SPY+SPX. Required cross-sector controls include QQQ+BMY, QQQ+NEE, CSCO+KO, T+AAPL, AMT+JPM, and COIN+NVDA.

## Release gates

- exact base SHA and exact head SHA recorded;
- complete changed-file list reviewed;
- #589 map additions absent from the #548 production diff;
- focused Master Control tests green, including unresolved OPEN/CLOSING position fences;
- neighboring exposure/risk tests green;
- exact-head blocking P0 green;
- independent final audit confirms no new money-path authority.

Final status remains **HARD HOLD / DRAFT — DO NOT MERGE** until every gate is independently verified.
