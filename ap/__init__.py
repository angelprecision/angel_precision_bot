# package marker

# P0 entry safety: install a read-only metadata guard around ENTRY order
# creation and ENTRY broker submit. The guard rejects missing/zero thesis
# metadata before contract/order/broker side effects can create junk trades.
#
# Deliberately do not swallow import/install errors here. If the guard cannot
# install, startup should fail closed instead of running live/paper entries with
# unknown execution_mode, zero score, zero trigger, or zero underlying metadata.
from .entry_metadata_guard import install_entry_metadata_guard

install_entry_metadata_guard()
