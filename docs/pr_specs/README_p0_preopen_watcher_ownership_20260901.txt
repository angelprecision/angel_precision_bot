CLAUDE IMPLEMENTATION HANDOFF

Read docs/pr_specs/p0_preopen_watcher_ownership_20260901.md in full before editing production code.

This branch is spec-only. Do not merge or deploy.

Primary job: guarantee eligible overnight Daily LIVE setups have exact durable order + runtime watcher ownership before the 09:30 ET market open, with fail-closed readiness when ownership cannot be proven.

Do not loosen late-attachment, selector, spread, score, risk, earnings, sizing, broker-submit, or exit safety to increase throughput.

Required first step: reproduce the 2026-09-01 Jason timing defect fail-first on current main.
