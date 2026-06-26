# package marker

# P0 entry safety: fail closed when fresh underlying confirmation data is unavailable.
# Runs before ENTRY order creation and before broker submit through OSM wrappers.
from .underlying_confirmation_entry_guard import install_underlying_confirmation_entry_guard

install_underlying_confirmation_entry_guard()
