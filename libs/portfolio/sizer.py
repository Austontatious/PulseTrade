from __future__ import annotations

from typing import Dict, List, Tuple


def to_orders(
    target_w: Dict[str, float],
    ideas_by_symbol,
    *,
    nav: float,
    min_order_notional: float,
    no_trade_band: float,
    price_map: Dict[str, float],
) -> List[Tuple[str, int]]:
    orders: List[Tuple[str, int]] = []
    for sym, tw in target_w.items():
        idea = ideas_by_symbol.get(sym)
        if idea is None:
            continue
        price = price_map.get(sym)
        if not price:
            continue
        current_weight = float(getattr(idea, "cur_weight", 0.0))
        delta_weight = tw - current_weight
        if abs(delta_weight) < no_trade_band:
            continue
        notional = delta_weight * nav
        if abs(notional) < min_order_notional:
            continue
        delta_shares = int(round(notional / price))
        if delta_shares != 0:
            orders.append((sym, delta_shares))

    for sym, idea in ideas_by_symbol.items():
        if sym in target_w:
            continue
        price = price_map.get(sym)
        if not price:
            continue
        current_weight = float(getattr(idea, "cur_weight", 0.0))
        if abs(current_weight) <= no_trade_band:
            continue
        notional = -current_weight * nav
        if abs(notional) < min_order_notional:
            continue
        delta = int(round(notional / price))
        if delta != 0:
            orders.append((sym, delta))

    return orders


def cash_after_orders(cash: float, orders: List[Tuple[str, int]], price_map: Dict[str, float]) -> float:
    net = 0.0
    for sym, delta_shares in orders:
        price = price_map.get(sym)
        if not price:
            continue
        net -= delta_shares * price
    return cash + net


def split_buys_and_sells(orders: List[Tuple[str, int]]) -> Tuple[List[Tuple[str, int]], List[Tuple[str, int]]]:
    buys = [(symbol, qty) for symbol, qty in orders if qty > 0]
    sells = [(symbol, qty) for symbol, qty in orders if qty < 0]
    return buys, sells
