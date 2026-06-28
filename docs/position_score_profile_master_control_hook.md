# Position score profile Master Control metadata hook

This PR is the planned final hook for the observe-only position score profile book.

## Goal

Attach the position score profile to approved plan metadata without making it active.

Expected metadata keys:

```python
plan.metadata["position_score_profile"]
plan.metadata["score_audit"]["position_score_profile"]
```

## Safety boundary

This PR must not replace scanner score, replace plan score, submit orders, cancel orders, mutate queue rows, mutate order rows, mutate position rows, mutate proof records, change sizing, change client id, change execution mode, or change live/paper taxonomy.

## Failure behavior

If context/profile building fails, attach an error diagnostic only. The original Master Control decision must remain unchanged.

## Merge condition

Merge only after the actual `ap_master_control.py` hook and tests are added, CI passes, and audit proves metadata-only behavior.
