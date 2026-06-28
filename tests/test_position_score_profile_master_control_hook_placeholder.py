from pathlib import Path


def test_mc_metadata_hook_handoff_document_exists():
    assert Path("docs/position_score_profile_master_control_hook.md").exists()
