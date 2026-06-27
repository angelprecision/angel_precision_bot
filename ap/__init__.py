# package marker

def install_entry_metadata_safety_guards() -> None:
    from .master_control_metadata_guard import install_master_control_metadata_guard
    from .entry_metadata_guard import install_entry_metadata_guard

    install_master_control_metadata_guard()
    install_entry_metadata_guard()


try:
    install_entry_metadata_safety_guards()
except (ImportError, AttributeError):
    pass
