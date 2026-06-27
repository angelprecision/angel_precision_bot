# package marker

# P0 entry safety: fail closed when fresh underlying confirmation data is unavailable.
# Direct submit blocks before local order creation; existing queued ENTRY rows block
# before broker submit without mutating the order row.
from .underlying_confirmation_entry_guard import install_underlying_confirmation_entry_guard

install_underlying_confirmation_entry_guard()
