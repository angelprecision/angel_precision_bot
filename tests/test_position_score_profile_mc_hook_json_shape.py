from dataclasses import dataclass, field

from ap.position_score_profile_mc_hook import attach_observe_only_position_profile


@dataclass
class Plan:
    metadata: dict = field(default_factory=dict)


def test_mc_hook_profile_shape_keys():
    profile = attach_observe_only_position_profile(Plan(), {"score": 70}, {})
    for key in ("profile_version", "observe_only", "components", "missing_data", "block_recommendations", "diagnostics"):
        assert key in profile
