from dataclasses import dataclass, field

from ap.position_score_profile_mc_hook import attach_observe_only_position_profile


@dataclass
class DummyPlan:
    score: float = 78.0
    metadata: dict = field(default_factory=lambda: {"score_audit": {}})


def test_attach_observe_only_position_profile_metadata_only():
    plan = DummyPlan()
    signal = {"side": "CALL", "score": 78, "entry_price": 10, "stop_price": 9, "target_price": 12}
    original_score = plan.score
    profile = attach_observe_only_position_profile(plan, signal, {})
    assert plan.score == original_score
    assert plan.metadata["position_score_profile"] == profile
    assert plan.metadata["score_audit"]["position_score_profile"] == profile
    assert profile["diagnostics"]["observe_only"] is True
