# package marker

# P0 entry safety: install read-only metadata guards around the ENTRY path.
# Master Control blocks malformed signals before contract selection. OSM blocks
# malformed plans/orders before entry order creation and broker submit.
#
# Deliberately do not swallow install errors. If the guards cannot install,
# startup should fail closed instead of running entries with unknown/zero thesis
# metadata.
from .entry_metadata_guard import install_entry_metadata_guard
from .master_control_metadata_guard import install_master_control_metadata_guard

install_master_control_metadata_guard()
install_entry_metadata_guard()
