from libs.portfolio.allocator import Idea, propose_target_weights_long_short


def _idea(symbol: str, sector: str, score: float) -> Idea:
    return Idea(
        symbol=symbol,
        sector=sector,
        score=score,
        llm_allow=True,
        price=100.0,
        cur_weight=0.0,
        cur_shares=0.0,
        nav=100000.0,
    )


def test_long_short_caps_and_bands():
    ideas = [
        _idea("LONG1", "Tech", 2.0),
        _idea("LONG2", "Fin", 1.5),
        _idea("LONG3", "Health", 0.8),
        _idea("SHORT1", "Tech", -3.0),
        _idea("SHORT2", "Fin", -1.2),
        _idea("SHORT3", "Energy", -0.5),
    ]

    weights = propose_target_weights_long_short(
        ideas,
        allow_shorts=True,
        max_long=0.10,
        max_short=0.10,
        sector_cap_long=0.25,
        sector_cap_short=0.25,
        gross_cap=1.0,
        net_min=-0.30,
        net_max=0.70,
        cash_floor=0.05,
        top_n=3,
        score_exp=1.0,
    )

    assert weights
    sector_map = {idea.symbol: idea.sector for idea in ideas}
    longs = {sym: w for sym, w in weights.items() if w > 0}
    shorts = {sym: w for sym, w in weights.items() if w < 0}
    assert longs and shorts
    assert all(w <= 0.10 + 1e-9 for w in longs.values())
    assert all(abs(w) <= 0.10 + 1e-9 for w in shorts.values())
    tech_long = sum(w for sym, w in longs.items() if sector_map[sym] == "Tech")
    tech_short = sum(abs(w) for sym, w in shorts.items() if sector_map[sym] == "Tech")
    assert tech_long <= 0.25 + 1e-9
    assert tech_short <= 0.25 + 1e-9
    gross = sum(abs(w) for w in weights.values())
    assert gross <= 1.0 + 1e-9
    net = sum(weights.values())
    assert -0.30 - 1e-9 <= net <= 0.70 + 1e-9


def test_long_only_when_shorts_disabled():
    ideas = [
        _idea("LONG1", "Tech", 2.0),
        _idea("SHORT1", "Tech", -1.0),
    ]
    weights = propose_target_weights_long_short(
        ideas,
        allow_shorts=False,
        max_long=0.10,
        max_short=0.10,
        sector_cap_long=0.25,
        sector_cap_short=0.25,
        gross_cap=1.0,
        net_min=-0.30,
        net_max=0.70,
        cash_floor=0.05,
        top_n=2,
        score_exp=1.0,
    )
    assert all(w >= 0 for w in weights.values())
