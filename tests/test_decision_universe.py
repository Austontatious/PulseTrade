from app.decision.universe import get_tradable


def test_trading_universe_top100():
    symbols = get_tradable(view="trading_universe_100")
    assert symbols, "expected at least one symbol in trading universe"
    assert len(symbols) <= 100
    assert all(isinstance(sym, str) for sym in symbols)
