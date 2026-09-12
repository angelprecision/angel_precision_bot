# FVG Bounce Candidate Promotion Rollout Gate

This branch is spec-only.

Do not implement production promotion until #621, #622, and the FVG hold/bounce classifier are merged and independently cleared.

Promotion must reuse the existing Angel Precision candidate/watcher/selector/broker pipeline and remain fail-soft for unrelated tradeflow.
