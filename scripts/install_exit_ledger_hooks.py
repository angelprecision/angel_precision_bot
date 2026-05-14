#!/usr/bin/env python3
"""
scripts/install_exit_ledger_hooks.py
===============================================================================
Safely installs best-effort exit ledger hooks into ap_exit_engine.py.

Why a script instead of a blind rewrite?
---------------------------------------
ap_exit_engine.py is money-critical. This installer makes small, idempotent,
marker-based changes and refuses to run if the expected anchor points are not
found. That is safer than pasting a full 1,000+ line file over production code.

What it adds
------------
1. Import helpers from ap.exit_decision_ledger.
2. A helper function _ledger_exit_decision(...).
3. A call immediately after evaluate_exit(pos, ...) in the exit loop when found.

If the evaluate_exit call pattern changes, the script refuses to patch and tells
us to patch manually.
===============================================================================
"""

from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
TARGET = ROOT / "ap_exit_engine.py"

IMPORT_BLOCK = '''
# EXIT-LEDGER-HOOK: best-effort audit logging; must never block live exits.
try:
    from ap.exit_decision_ledger import record_exit_decision, record_exit_order_event
except Exception:  # pragma: no cover - production safety
    record_exit_decision = None
    record_exit_order_event = None
# /EXIT-LEDGER-HOOK
'''

HELPER_BLOCK = r'''

# EXIT-LEDGER-HOOK: central non-fatal decision logger.
def _ledger_exit_decision(pos, decision, *, event_context: str = "exit_loop") -> None:
    try:
        if record_exit_decision is None:
            return
        record_exit_decision(
            pos,
            decision,
            client_id=str(getattr(pos, "client_id", "") or ""),
            metadata={"event_context": event_context},
        )
    except Exception:
        # Trading safety: ledger failure must never block an exit.
        pass
# /EXIT-LEDGER-HOOK
'''


def main() -> int:
    if not TARGET.exists():
        raise SystemExit(f"Target not found: {TARGET}")

    text = TARGET.read_text()
    original = text

    if "# EXIT-LEDGER-HOOK" in text:
        print("exit ledger hooks already installed; no changes made")
        return 0

    # 1. Install import block after observability import fallback section.
    import_anchor = 'log = logging.getLogger("ap.exit_engine")\nET  = ZoneInfo("America/New_York")\n'
    if import_anchor not in text:
        raise SystemExit("Import anchor not found; refusing to patch ap_exit_engine.py")
    text = text.replace(import_anchor, IMPORT_BLOCK + "\n" + import_anchor, 1)

    # 2. Install helper before evaluate_exit.
    helper_anchor = "# ── EXIT LOGIC ────────────────────────────────────────────────────────────────\n\ndef evaluate_exit"
    if helper_anchor not in text:
        raise SystemExit("evaluate_exit anchor not found; refusing to patch ap_exit_engine.py")
    text = text.replace(
        helper_anchor,
        "# ── EXIT LOGIC ────────────────────────────────────────────────────────────────" + HELPER_BLOCK + "\n\ndef evaluate_exit",
        1,
    )

    # 3. Install call after common decision assignment patterns.
    # The preferred target is inside APExitEngine's loop where decision = evaluate_exit(...).
    candidates = [
        "decision = evaluate_exit(pos, now_et=now_et)",
        "decision = evaluate_exit(pos)",
    ]
    patched_decision = False
    for candidate in candidates:
        if candidate in text:
            replacement = candidate + "\n                    _ledger_exit_decision(pos, decision, event_context=\"exit_loop\")"
            text = text.replace(candidate, replacement, 1)
            patched_decision = True
            break

    if not patched_decision:
        raise SystemExit(
            "Could not find an evaluate_exit assignment to hook. "
            "Add _ledger_exit_decision(pos, decision, event_context='exit_loop') "
            "manually immediately after evaluate_exit() in the exit loop."
        )

    if text == original:
        raise SystemExit("No changes produced; refusing to write")

    TARGET.write_text(text)
    print(f"installed exit ledger hooks into {TARGET}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
