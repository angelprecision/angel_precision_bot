# PR 214 HOLD review checklist

This PR is open so the scoring-book process stays inside PRs 209 through 214.

Before merge, amend and verify:

- Add the actual `ap_master_control.py` metadata-only hook.
- Attach `position_score_profile` under plan metadata.
- Attach the same payload under `score_audit.position_score_profile` when score audit exists.
- Preserve plan score and signal score.
- Preserve client id and execution mode.
- Do not change approval, rejection, sizing, routing, execution, queue state, order state, position state, or proof records.
- If profile building fails, attach error diagnostics only and preserve the original decision.
- Add real tests for unchanged decision behavior and JSON-safe metadata.

Verdict until amended: HOLD.
