# package marker

# P0 entry safety: install a read-only metadata guard around ENTRY order
# creation and ENTRY broker submit. The guard rejects missing/zero thesis
# metadata before contract/order/broker side effects can create junk trades.
try:
    from .entry_metadata_guard import install_entry_metadata_guard

    install_entry_metadata_guard()
except Exception:
    # Package imports must stay resilient for tooling/tests that import ap.* before
    # runtime dependencies are configured. Production entry paths still import the
    # guard module directly in tests and fail closed once OSM is available.
    pass
