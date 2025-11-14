from __future__ import annotations

from typing import Dict, List, Tuple


def replacement_plan(
    deficit: float,
    buy_list: List[Tuple[str, int]],
    *,
    ideas_by_symbol,
    price_map: Dict[str, float],
    fric_bps: float,
) -> List[Tuple[str, int]]:
    sells: List[Tuple[str, int]] = []
    if deficit <= 0:
        return sells

    keep = []
    for sym, idea in ideas_by_symbol.items():
        shares = float(getattr(idea, "cur_shares", 0.0))
        if shares <= 0:
            continue
        weight = float(getattr(idea, "cur_weight", 0.0))
        fric = (fric_bps / 10000.0) * weight
        keep_score = max(float(getattr(idea, "score", 0.0)), 0.0) - fric
        keep.append((keep_score, sym))

    keep.sort(key=lambda x: x[0])

    need = deficit
    for _, sym in keep:
        if need <= 1e-6:
            break
        idea = ideas_by_symbol[sym]
        price = price_map.get(sym)
        if not price:
            continue
        shares = float(getattr(idea, "cur_shares", 0.0))
        if shares <= 0:
            continue
        max_notional = shares * price
        sell_notional = min(max_notional, need)
        delta_shares = int(round(-sell_notional / price))
        if delta_shares == 0:
            continue
        sells.append((sym, delta_shares))
        need -= (-delta_shares * price)

    return sells
