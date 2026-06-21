#!/usr/bin/env python3
"""
scripts/verify_deferred_ladder_wiring.py

Post-merge sanity check for the deferred-DTE-ladder stack (#165 + #166 + #167).
Run this AFTER merging the PRs, BEFORE enabling any flag, to confirm the wiring
survived the merge. Pure static checks — no DB, no broker, no side effects.

Exit 0 = all wiring intact. Exit 1 = a problem to investigate.

Catches the specific risks flagged in review:
  - #166 marker not set before select() (merge could have moved it)
  - #166 ladder eligibility not gated to the marker (blast-radius)
  - #165 terminal/progress split intact (SELECTED not terminal)
  - #167 fail-closed-on-mismatch intact
"""
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
EC = (REPO / "ap_execution_core.py").read_text()
CS = (REPO / "ap" / "contract_selector.py").read_text()
OR = (REPO / "ap_overnight_reeval.py").read_text()

checks = []
def check(name, ok, detail=""):
    checks.append((name, ok, detail))

# ── #166: marker set before the deferred select() call ──────────────────────
marker = EC.find('"deferred_breach_selection"')
select = EC.find("_sel = self.contract_selector.select(approved_plan)")
check("#166 marker present", marker != -1)
check("#166 select() call present", select != -1)
check("#166 marker set BEFORE select()", marker != -1 and select != -1 and marker < select,
      f"marker@{marker} select@{select}")

# ── #166: ladder eligibility gated to the marker, not unconditional ─────────
elig_idx = CS.find("def _is_ladder_eligible(")
elig_body = CS[elig_idx: CS.find("\n    def ", elig_idx + 10)] if elig_idx != -1 else ""
check("#166 eligibility gated to marker",
      'meta.get("deferred_breach_selection")' in elig_body
      and "return False" in elig_body
      and "return True\n" not in elig_body.split("deferred_breach")[0][-40:] if elig_body else False)
check("#166 eligibility not unconditional True",
      elig_body.strip().split("\n")[-1].strip() == "return False" if elig_body else False)

# ── #165: terminal/progress split intact ────────────────────────────────────
check("#165 terminal set present", "_TERMINAL_DEFERRED_OUTCOMES" in EC)
check("#165 progress emitter present", "_emit_deferred_progress" in EC)
check("#165 SELECTED is progress not terminal",
      '_emit_deferred_progress(\n                    "BREACH_CONTRACT_SELECTED"' in EC)
tset_start = EC.find("_TERMINAL_DEFERRED_OUTCOMES = frozenset({")
tset = EC[tset_start: EC.find("})", tset_start)] if tset_start != -1 else ""
check("#165 SELECTED excluded from terminal set", "BREACH_CONTRACT_SELECTED" not in tset)
check("#165 SUBMITTED in terminal set", "BREACH_BROKER_SUBMITTED" in tset)

# ── #167: fail-closed on session mismatch intact ────────────────────────────
check("#167 session mismatch handling", "PRIOR_LEVEL_SESSION_MISMATCH" in OR)
mm = OR.find("PRIOR_LEVEL_SESSION_MISMATCH")
mm_block = OR[mm: mm + 600] if mm != -1 else ""
check("#167 clears fresh high on mismatch", "prior_day_high = None" in mm_block)
check("#167 clears fresh low on mismatch", "prior_day_low = None" in mm_block)
check("#167 uses fresh only when session_ok", "if _have_fresh and _session_ok:" in OR)

# ── report ──────────────────────────────────────────────────────────────────
print("=== Deferred ladder wiring verification ===\n")
all_ok = True
for name, ok, detail in checks:
    flag = "PASS" if ok else "FAIL"
    if not ok:
        all_ok = False
    line = f"[{flag}] {name}"
    if detail:
        line += f"  ({detail})"
    print(line)

print()
if all_ok:
    print("ALL WIRING INTACT — safe to proceed to paper enablement.")
    sys.exit(0)
else:
    print("WIRING PROBLEM — investigate before enabling any flag.")
    sys.exit(1)
