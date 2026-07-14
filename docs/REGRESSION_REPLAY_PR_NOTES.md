# Regression replay PR notes

This branch intentionally stops at the deterministic, read-only foundation.

It does not claim that current-main production stages are already wired into replay. Current-main adapters must be added as pure, dependency-injected evaluators in a follow-up so the harness never discovers broker credentials, database connections, or live services.

Local verification before opening the pull request:

```text
15 passed
```

Fixture summary:

```json
{"fixtures":10,"wins":4,"losses":6,"fixtures_with_unknown_execution_mode":10,"fixtures_with_missing_identity":10}
```
