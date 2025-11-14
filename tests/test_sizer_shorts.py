from libs.portfolio.allocator import Idea
from libs.portfolio.sizer import split_buys_and_sells, to_orders


def test_negative_weight_produces_sell_order():
    idea = Idea(
        symbol="XYZ",
        sector="Tech",
        score=-1.0,
        llm_allow=True,
        price=100.0,
        cur_weight=0.0,
        cur_shares=0.0,
        nav=100000.0,
    )
    target_w = {"XYZ": -0.05}
    ideas = {"XYZ": idea}
    price_map = {"XYZ": 100.0}

    orders = to_orders(
        target_w,
        ideas,
        nav=idea.nav,
        min_order_notional=200,
        no_trade_band=0.0,
        price_map=price_map,
    )

    assert orders
    _, sells = split_buys_and_sells(orders)
    assert sells and sells[0][0] == "XYZ" and sells[0][1] < 0
