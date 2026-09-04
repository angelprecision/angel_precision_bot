# P0 — Validate sector-cap fraction authority across LIVE/PAPER

## STATUS

**SPEC ONLY / HARD HOLD. DO NOT MERGE OR DEPLOY.**

Base: `main@d3c61850df709fe4c399196b9c509f28c9af2a8a`

Incident date: 2026-09-04

This work order owns one configuration-authority defect only:

> `max_sector_pct` is consumed by Master Control as a fractional multiplier of account equity, but persisted client risk profiles can currently supply values outside the fractional domain without rejection.

Do not use this PR to alter the intended sector cap, increase trade count, align all PAPER policy with LIVE, or change sector identity. #548 owns sector identity.

---

## Production evidence

During the September 4 Jason LIVE vs PAPER comparison:

```text
Jason LIVE max_sector_pct = 0.10
Jose PAPER max_sector_pct = 10
Tradefluence PAPER max_sector_pct = 10
```

Current main exposes the semantic contract in `APMasterControl` defaults:

```text
max_sector_pct = 0.25
```

and computes sector capital authority as:

```text
max_sector = equity * max_sector_pct
```

Current runner hydration casts the configured value directly with `float(...)` before passing it into Master Control.

Therefore:

```text
0.10 -> 10% of equity
0.25 -> 25% of equity
1.00 -> 100% of equity
10.0 -> 1000% of equity
```

A persisted value of `10` is not equivalent to ten percent under current production semantics. It effectively makes the sector cap nonbinding for ordinary account exposure.

That materially distorts PAPER/LIVE parity and, if the same malformed value ever reaches LIVE, can silently disable an intended client-money risk control.

---

## Binding invariant

There must be one explicit unit contract for `max_sector_pct`:

```text
finite decimal fraction
strictly > 0
<= 1.0
```

Examples:

```text
0.10 -> valid 10%
0.25 -> valid 25%
1.00 -> valid 100%
10    -> invalid, never interpreted as 10%
10%   -> invalid unless one existing canonical parser explicitly owns percent-string syntax
0     -> invalid configuration
-0.1  -> invalid configuration
NaN   -> invalid configuration
+inf  -> invalid configuration
blank -> missing authority, use only the existing explicit missing-value policy
```

**Do not auto-divide values greater than 1 by 100.** `10` is ambiguous data, not proof of `0.10` intent.

**Do not silently clamp to 1.0.** That hides corruption and changes policy without authority.

---

## Required authority trace

Before editing production code, trace current main exactly:

```text
client_risk_profiles.max_sector_pct
-> ClientRunner risk-profile read
-> config/env fallback resolution
-> numeric normalization
-> APMasterControl construction
-> evaluate()
-> revalidate_exposure()
-> max_sector = equity * max_sector_pct
-> ALLOW/BLOCK
```

For each changed production function report:

```text
caller
-> validation
-> state read
-> authority resolution
-> mutation
-> return classification
-> downstream consumer
```

Runtime and restart must hydrate the same resolved value from the same source.

---

## Required behavior

### Valid explicit value

A finite value in `(0, 1]` passes unchanged.

No unit conversion. No rounding other than existing diagnostic formatting.

### Missing value

Preserve current explicit missing-value precedence only after tracing it. If current policy uses a named env/default for PAPER and/or LIVE, retain that exact source and persist/log the source.

Missing is not the same as malformed.

### Malformed/out-of-range value

An explicit persisted value outside the fractional domain must never silently fall through to Master Control.

For LIVE:

```text
invalid max_sector_pct
-> startup/readiness/config HOLD
-> entries fail closed
-> zero broker submit/cancel
```

For PAPER:

The invalid profile must be visible as a configuration error and must not be treated as a 1000% sector cap. Do not silently make PAPER more permissive. The implementation may fail PAPER entry readiness or use an existing explicitly documented invalid-profile policy, but it may not guess the intended value.

If current PAPER behavior has no safe invalid-profile policy, fail closed and require operator correction.

### Diagnostics

Expose enough durable/startup diagnostics to answer:

```text
client_id
execution_mode
raw max_sector_pct
resolved max_sector_pct
resolution source
validation status
failure reason
```

Do not log secrets or unrelated account credentials.

---

## Deployment/preflight requirement

Because current PAPER profiles have been observed with `max_sector_pct=10`, implementation must include or provide a **read-only preflight/report** before activation.

The report must identify every active client profile whose explicit value is:

- null/missing;
- malformed;
- non-finite;
- <= 0;
- > 1.

Do not write corrected values in the migration/PR. Operator policy intent must be supplied separately.

The code change must not be deployed until intentionally configured profiles are corrected or an explicitly reviewed fallback policy is documented.

---

## Relationship to other September 4 work

### #548

Owns canonical sector identity and removal of the fake shared `other` bucket.

This PR does not change sector mappings or how positions are assigned to sectors.

### #579

Owns stale canonical position exposure after exact EXIT fill.

This PR does not mutate positions or exposure snapshots.

### #580

Owns post-outage watcher lifecycle/retry convergence.

This PR does not touch watcher/retry state.

The three defects are independent. Correct sector identity with an invalid 1000% cap is still unsafe; a valid 10% cap with stale closed exposure can still false-block; neither is permission to weaken the other.

---

## Failure-class audit

### Authority

One authority for:

- raw persisted value;
- fallback source when genuinely missing;
- normalized fractional value;
- LIVE/PAPER execution mode;
- diagnostics source.

Do not allow DB/env/manifest values to disagree silently.

If two explicit nonblank configuration authorities conflict and current precedence is not already binding, HOLD rather than inventing precedence.

### State/restart

Test:

- clean startup valid value;
- restart same valid value;
- DB value changes between restart cycles;
- malformed value at startup;
- malformed value introduced before runner rehydrate;
- config cache versus fresh DB disagreement;
- LIVE/PAPER runners for the same raw value.

### Data corruption

Test:

- integer 10;
- float 10.0;
- string `"10"`;
- string `"10%"`;
- zero;
- negative;
- whitespace;
- NaN;
- +inf / -inf;
- boolean values;
- lists/dicts if JSON/config surfaces can supply them;
- valid boundary `1.0`;
- valid `0.10`;
- very small positive finite fraction.

### Money-path safety

Every invalid-value test must prove:

- zero broker ENTRY POST;
- zero broker EXIT POST;
- zero broker cancel;
- zero order/position/proof/queue mutation caused by the validation path;
- no fallback to an effectively unlimited sector cap;
- exact client/mode preserved in diagnostics.

---

## Mandatory behavioral tests

1. `0.10` hydrates exactly `0.10` and computes 10% sector cap.
2. `0.25` hydrates exactly `0.25`.
3. `1.0` hydrates exactly `1.0`.
4. explicit `10` does not become `10.0` inside Master Control.
5. explicit `10` does not auto-convert to `0.10`.
6. LIVE explicit `10` blocks readiness/startup before broker submit.
7. PAPER explicit `10` is visible invalid configuration and does not run with a 1000% cap.
8. explicit zero fails validation.
9. negative fails validation.
10. NaN/inf fail validation.
11. malformed string fails validation.
12. genuinely missing field follows existing documented missing-value fallback only.
13. conflicting DB and other explicit authority follows existing binding precedence or HOLDs; no silent choice added by this PR.
14. restart resolves the same valid DB value as initial runtime.
15. configuration correction from invalid to valid becomes effective through the existing safe restart/rehydration boundary.
16. same candidate/exposure snapshot with `0.10` resolves identically before and after this validation PR.
17. no sector-map behavior changes (#548 independence).
18. no broker submit/cancel calls on invalid paths.
19. no orders/positions/proof_trades/queue mutations added.
20. read-only preflight identifies the observed PAPER `10` shape.

Tests must exercise the real runner hydration -> Master Control constructor boundary, not only a standalone numeric helper.

---

## Preferred production scope

First attempt:

```text
client_runner.py
```

A small shared configuration validator is acceptable only if a real second caller exists and the same unit contract would otherwise be duplicated.

Focused tests and P0 workflow registration as required.

Do not modify:

- `ap_master_control.py` risk arithmetic unless fail-first proves validation cannot be enforced at the configuration boundary;
- sector maps;
- broker code;
- order state machine;
- watchers;
- positions;
- proof_trades;
- queue;
- scanner/selector;
- sizing values;
- exit policy.

No DB mutation/migration should be required for the validation itself.

---

## Merge gate

**HARD HOLD** until:

1. fail-first proves explicit `10` reaches current Master Control as `10.0` under current main;
2. implementation validates at the real hydration boundary;
3. read-only production-shaped preflight identifies invalid active profiles;
4. no automatic unit guessing or clamping exists;
5. LIVE invalid config fails closed before any broker action;
6. PAPER invalid config cannot silently become permissive;
7. existing valid fractional profiles behave byte-for-byte/numerically the same downstream;
8. exact-head P0 CI is green;
9. complete final diff/review audit returns MERGE;
10. deployment waits for explicit operator correction of known invalid profiles.

This PR is a configuration integrity guard, not permission to change Jason's 10% cap or to increase trade flow by weakening risk.
