# Deterministic regression replay

`ap/regression_replay.py` is a read-only harness for comparing recorded production behavior with explicitly supplied candidate stage adapters.

## Safety boundary

The harness imports no broker, database, queue, order-state, HTTP, or deployment client. It cannot submit, cancel, reserve capital, create positions, or mutate production rows. Missing stage adapters become `NOT_RUN / NO_STAGE_RUNNER`; missing evidence must be written as `UNKNOWN`.

## Evidence policy

The bundled June/July fixtures are redacted exports from `proof_trades` and `decision_events`. Numeric P&L is authoritative. Historical `win` flags are retained only as evidence and never determine the derived outcome. Raw emails, broker order IDs, API keys, tokens, and credentials are forbidden by fixture validation.

Deployment SHAs come from `decision_events.git_commit` at the recorded trade window. A window that crosses a deployment transition stores both observed SHAs and marks confidence accordingly.

## Canonical stages

1. scanner
2. queue normalization
3. master control
4. Gate G
5. watcher
6. breach
7. contract selector
8. submit gate
9. fill
10. exit decision
11. protective exit submission
12. reconciliation

## Usage

```bash
python -m ap.regression_replay --json
python -m pytest tests/test_p0_regression_replay_harness.py -v
```

The next integration step is to add pure adapters for current-main stage evaluators. Adapter wiring must remain dependency-injected so the harness itself never discovers production credentials or services.
