# P1 Work Order — Selector direct-quote budget configuration parity

This branch intentionally contains only the work-order specification. Production code/deployment changes are not authorized by this commit. Codex should implement the PR against this branch and preserve the scope below.

## Incident evidence

2026-08-11 production metadata shows selector recovery budget configuration drift between LIVE and PAPER pods.

Representative LIVE diagnostics:

- `SELECTOR_MAX_DIRECT_QUOTE_CALLS=40`
- direct recovery alias = 40
- contract revalidate alias = 40
- effective limit = 40
- `conflict=false`

Representative PAPER diagnostics:

- `SELECTOR_MAX_DIRECT_QUOTE_CALLS=20`
- direct recovery alias = 8
- contract revalidate alias = 20
- effective limit = 20
- `conflict=true`

PAPER is intended to be a meaningful behavioral validation environment for LIVE while preserving PAPER execution identity. Configuration drift makes parity analysis unreliable and may change how many eligible contracts can be directly revalidated in deferred breach materialization.

This is not authorization to simply increase provider calls. The canonical authority and request-kind scoping must be audited first.

## Required implementation

1. Identify every runtime/deployment source for:
   - `SELECTOR_MAX_DIRECT_QUOTE_CALLS`
   - `DIRECT_QUOTE_RECOVERY_TOP_N`
   - `CONTRACT_REVALIDATE_TOP_N`
   - any request-kind-specific selector budget defaults/overrides
2. Identify all Render/service/pod/environment manifests in-repo that can produce the current LIVE/PAPER divergence.
3. Confirm the canonical authority used by current main for ORDINARY and `DEFERRED_BREACH_MATERIALIZATION` request kinds.
4. Eliminate conflicting aliases or make them derive from one canonical authority so diagnostics report `conflict=false` on every supported deployment.
5. Preserve intended request-kind scoping. Deferred recovery may have a larger bounded budget; ordinary selector behavior must not silently inherit that envelope.
6. Preserve PAPER execution identity and live market-data-domain behavior. Do not switch PAPER selection to sandbox market data merely to make configs match.
7. Add startup/preflight attestation that records resolved selector request budgets and fails closed or marks deployment unhealthy when explicitly conflicting env values would produce ambiguous authority. Follow existing fail-closed conventions; do not invent silent precedence between contradictory explicit operator settings unless the canonical contract already defines it.
8. Ensure LIVE and PAPER pods resolve the same canonical deferred recovery budget unless an explicitly documented, tested mode-specific difference is intentional.
9. Do not change selector quality gates, broker submit/cancel semantics, sizing, triggers, exits, positions, or proof_trades.
10. Do not merge deployment changes until PR #439 final-reason behavior is independently fixed/proven; larger capacity must not mask incorrect disposition logic.

## Required tests

### Resolver/authority tests

- no env values -> documented defaults
- canonical env only -> same resolved value across LIVE/PAPER
- aliases only if still supported -> deterministic documented resolution
- explicit matching aliases -> `conflict=false`
- explicit contradictory values -> deterministic fail-closed/config-conflict behavior
- malformed/non-positive values -> fail closed

### Request-kind scope

Prove ordinary requests retain their intended bounded envelope while deferred breach requests receive only the documented deferred envelope.

### Mode parity

Instantiate production-shaped LIVE and PAPER selector/request contexts with identical canonical config. Assert equal deferred budget authority and no identity pollution.

### Startup/preflight

Production-shaped startup manifest/preflight must expose resolved values, source, request kind, conflict state, and deployment identity. Conflicting explicit configuration must be visible before market execution.

## Deployment evidence required before merge

For each active pod/service, capture sanitized resolved startup diagnostics proving:

- client/mode
- canonical selector budget source
- ordinary request envelope
- deferred request envelope
- alias values if retained
- `conflict=false`
- correct market-data base URL/domain class
- PAPER remains PAPER; LIVE remains LIVE

## Non-goals

- no threshold relaxation
- no automatic provider-call increase without canonical authority
- no PAPER-to-LIVE execution crossover
- no broker order mutation
- no database lifecycle mutation except optional observability/config-attestation fields already supported by startup diagnostics

## Merge policy

P1 draft only until exact-head tests and deployment-shape audit pass. No merge/deploy without explicit approval.