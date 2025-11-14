from types import SimpleNamespace

from libs.portfolio.sizer import to_orders


def test_sizer_bands():
    idea = SimpleNamespace(cur_weight=0.05, cur_shares=50)
    ideas = {"AAPL": idea}
    price_map = {"AAPL": 100.0}
    orders = to_orders({"AAPL": 0.051}, ideas, nav=100000.0, min_order_notional=200.0, no_trade_band=0.0025, price_map=price_map)
    assert orders == []

    orders = to_orders({"AAPL": 0.10}, ideas, nav=100000.0, min_order_notional=200.0, no_trade_band=0.0025, price_map=price_map)
    assert orders and orders[0][0] == "AAPL"
