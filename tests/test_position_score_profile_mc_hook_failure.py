class FrozenPlan:
    @property
    def metadata(self):
        return None


from ap.position_score_profile_mc_hook import attach_observe_only_position_profile


def test_attach_helper_returns_profile_even_if_metadata_unwritable():
    profile = attach_observe_only_position_profile(FrozenPlan(), {"score": 70}, {})
    assert profile["observe_only"] is True
