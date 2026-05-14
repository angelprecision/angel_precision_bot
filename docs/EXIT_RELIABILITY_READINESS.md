# Angel Precision Exit Reliability Readiness

This is the next-level production checklist before scaling live client seats.

## 1. Database migration

Run:

```sql
migrations/20260513_exit_decision_ledger.sql
```

This creates `exit_decision_ledger`, the permanent audit trail for quote state, P&L, exit decisions, broker order status, fills, and errors.

## 2. Install exit engine hooks

After pulling the branch locally or on Render shell, run:

```bash
python scripts/install_exit_ledger_hooks.py
```

The installer is intentionally marker-based and refuses to patch if the expected anchors are not found. It does not blindly overwrite `ap_exit_engine.py`.

## 3. Run replay tests

```bash
pytest tests/test_exit_replay_harness.py -q
```

These tests prove the exit engine fires on core price paths:

- single contract +25% peak then below +10% floor
- single contract trail after +12%+ peak and giveback
- multi-contract scale-out at +15%
- never-green stop

## 4. Dashboard / health-center integration

Use:

```python
from ap.exit_reliability_monitor import exit_reliability_snapshot
snapshot = exit_reliability_snapshot()
```

Expose these fields in the admin health center:

- `ok`
- `critical_count`
- `high_count`
- `issues`

Critical issue types:

- `green_peak_without_exit_decision`
- `exit_decision_without_order_event`
- `stale_exit_order_without_terminal_event`

## 5. Production rule

No client scaling until every paper/live-beta trade can answer:

1. Did quote monitor see the current option price?
2. Did peak P&L update?
3. Did `evaluate_exit()` return HOLD, SCALE_OUT, CLOSE_ALL, or STOP?
4. If exit decision fired, did an exit order get submitted?
5. Did broker ack, reject, or fill?
6. Did OSM and position manager close or scale the position correctly?
7. Did the dashboard reflect broker truth?

## 6. Next repairs after this branch

After the ledger proves where failures occur, patch the exact bottleneck:

- If decisions are missing: fix quote freshness / peak tracking / exit loop wakeups.
- If orders are missing: fix `_submit_exit_decision()` / OSM exit creation.
- If orders are stale: enable/test cancel-replace protection.
- If fills are missing: audit `ap/fill_monitor.py` and reconciler fill heal.
- If dashboard is wrong: fix position manager/admin health API read path.
