# P0 SPEC: LIVE pending-trigger liveness audit

This branch is documentation-only. It records the requirement to perform an observe-only audit of pending-trigger lifecycle state before any behavioral repair is proposed.

Safety constraints: no broker actions, no queue replay, no lifecycle mutation, no strategy changes, no PAPER/LIVE crossover, and no position or proof-trade creation.

Any later behavioral repair remains HARD HOLD pending separate review.