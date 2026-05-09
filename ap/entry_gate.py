# bot/entry_gate.py
from datetime import datetime, timezone

class EntryBlocked(Exception):
    def __init__(self, reason_code: str, detail: str = ""):
        self.reason_code = reason_code
        self.detail = detail
        super().__init__(f"{reason_code}: {detail}")

def check_entry_allowed(member: dict, system_control: dict) -> None:
    """
    Raises EntryBlocked if any gate is active. Precedence: killswitch > global > entriespaused > maintenance > scanner.
    Exits are NEVER gated by this function — exit engine and admin force-close bypass entirely.
    """
    # Global killswitch — highest precedence
    if system_control and system_control.get("global_killswitch"):
        raise EntryBlocked("global_killswitch", system_control.get("reason") or "global emergency")

    # Per-client killswitch — second highest
    if member.get("killswitch"):
        raise EntryBlocked("client_killswitch", member.get("killswitchreason") or "manual clear required")

    # Global entries paused
    if system_control and system_control.get("global_entriespaused"):
        raise EntryBlocked("global_entriespaused", system_control.get("reason") or "")

    # Per-client entries paused — existing positions still managed by exit engine
    if member.get("entriespaused"):
        raise EntryBlocked("entries_paused", "client entries paused by operator")

    # Maintenance mode — soft block during degraded/resync states
    if member.get("maintenancemode"):
        raise EntryBlocked("maintenance_mode", "client in maintenance mode")

    # Scanner routing disabled
    if member.get("scannerroutingenabled") is False:
        raise EntryBlocked("scanner_disabled", "scanner routing disabled for client")

    # Subscription / approval guards
    if not member.get("approved"):
        raise EntryBlocked("not_approved", "client not approved")
    if not member.get("subscriptionactive"):
        raise EntryBlocked("subscription_inactive", "subscription not active")


# USAGE in your entry flow:
#
# from bot.entry_gate import check_entry_allowed, EntryBlocked
#
# def attempt_entry(member, signal, system_control):
#     try:
#         check_entry_allowed(member, system_control)
#     except EntryBlocked as e:
#         log_rejection(member, signal, e.reason_code, e.detail)
#         return {"status": "REJECTED", "reason": e.reason_code, "detail": e.detail}
#
#     # proceed with entry order submission
#     return submit_entry_order(member, signal)
