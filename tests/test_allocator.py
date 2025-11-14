from libs.portfolio.allocator import Idea, propose_target_weights


def test_caps_and_budget():
    ideas = [
        Idea(symbol=f"S{i}", sector="Tech" if i < 3 else "Fin", score=1.0 + i,
             llm_allow=True, price=100.0, cur_weight=0.0, cur_shares=0.0, nav=100000.0)
        for i in range(6)
    ]
    weights = propose_target_weights(
        ideas,
        max_pos=0.10,
        sector_cap=0.25,
        gross_cap=1.0,
        cash_floor=0.05,
        top_n=5,
        score_exp=1.0,
    )
    assert all(w <= 0.10 + 1e-9 for w in weights.values())
    tech_sum = sum(w for sym, w in weights.items() if sym in {"S0", "S1", "S2"})
    assert tech_sum <= 0.25 + 1e-9
    assert sum(weights.values()) <= 1.0 + 1e-9
