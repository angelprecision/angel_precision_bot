# Admission Thresholds

This document describes the visible admission thresholds used across the scanner fallback, interrogation layer, and master-control admission funnel.

No values are changed by this document or by the paired code change. The implementation is read-path consolidation only.

## Threshold Table

| Threshold | Default / Current Source | Purpose | Enforcement Location |
| --- | --- | --- | --- |
| `scanner_floor` | `GATE_G_SCANNER_MIN_ELIGIBLE` default `70.0` | Minimum scanner score that can qualify for the Gate-G scanner fallback path when interrogation/intel data is unavailable. | `intelligence_bridge.py` |
| `interrogation_floor` | `INTERROGATION_MIN_SCORE` default `62.0` | Minimum interrogation quality score before a fully-cleared packet can move from `WATCH` to `EXECUTE`. | `trade_interrogation_engine.py` |
| `mc_score_floor` | Runtime current value. Commonly `SCORE_FLOOR` default `65.0` via `client_runner`, but `APMasterControl` also has its own constructor fallback. | Minimum score floor for non-priority tickers inside master control. | `ap_master_control.py` |
| `mc_priority_floor` | Hardcoded current value `40.0` | Lower priority-ticker score floor used after score normalization and post-target bump logic. | `ap_master_control.py` |
| `context_floor` | Runtime current value. Commonly `CONTEXT_FLOOR` default `0.0` via `client_runner`, but `APMasterControl` also has its own constructor fallback. | Minimum real-time context score when `score_breakdown.real_time_ctx` is present. | `ap_master_control.py` |

## Funnel Order

1. Scanner score can matter before master control through the Gate-G scanner fallback path.
2. Interrogation computes a packet quality score and compares it with the interrogation floor.
3. Master control enforces its score-band logic, then the priority or non-priority floor, then the context floor when real-time context is available.

## Visibility

Startup logs now emit:

- The resolved threshold table
- The source used for each threshold
- A threshold-only configuration hash

Decision diagnostics now include a threshold trace with:

- The score value inspected
- The floor value inspected
- Pass or fail status
- The source used for the threshold

If an interrogation packet is missing, the trace records it as unavailable instead of coercing the score to zero.
