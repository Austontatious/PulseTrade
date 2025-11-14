from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
from collections import defaultdict


@dataclass
class Idea:
    symbol: str
    sector: str
    score: float
    llm_allow: bool
    price: float
    cur_weight: float
    cur_shares: float
    nav: float


def _softmax_norm(vals: List[float], p: float = 1.0) -> List[float]:
    mod = [(max(v, 0.0)) ** p for v in vals]
    s = sum(mod)
    if s <= 0:
        return [0.0 for _ in mod]
    return [v / s for v in mod]


def _abs_softmax_norm(vals: List[float], p: float = 1.0) -> List[float]:
    mod = [abs(v) ** p for v in vals]
    s = sum(mod)
    if s <= 0:
        return [0.0 for _ in mod]
    return [v / s for v in mod]


def propose_target_weights(
    ideas: List[Idea],
    *,
    max_pos: float,
    sector_cap: float,
    gross_cap: float,
    cash_floor: float,
    top_n: int,
    score_exp: float,
) -> Dict[str, float]:
    good = [x for x in ideas if x.llm_allow and x.score > 0]
    good.sort(key=lambda x: x.score, reverse=True)
    basket = good[:top_n]

    weights: Dict[str, float] = {}
    norm = _softmax_norm([x.score for x in basket], p=score_exp)
    budget = max(gross_cap - cash_floor, 0.0)

    for idea, w in zip(basket, norm):
        weights[idea.symbol] = w * budget

    for sym in list(weights.keys()):
        if weights[sym] > max_pos:
            weights[sym] = max_pos

    def sector_of(symbol: str) -> str:
        for item in basket:
            if item.symbol == symbol:
                return item.sector
        for item in ideas:
            if item.symbol == symbol:
                return item.sector
        return "UNK"

    changed = True
    while changed:
        changed = False
        sector_sum: Dict[str, float] = defaultdict(float)
        for sym, w in weights.items():
            sector_sum[sector_of(sym)] += w
        for sec, tot in sector_sum.items():
            if tot > sector_cap:
                ratio = sector_cap / max(tot, 1e-9)
                for sym in list(weights.keys()):
                    if sector_of(sym) == sec:
                        new_w = weights[sym] * ratio
                        if abs(new_w - weights[sym]) > 1e-9:
                            weights[sym] = new_w
                            changed = True

    gross = sum(weights.values())
    if gross > gross_cap and gross > 0:
        ratio = gross_cap / gross
        for sym in list(weights.keys()):
            weights[sym] *= ratio

    return weights


def propose_target_weights_long_short(
    ideas: List[Idea],
    *,
    allow_shorts: bool,
    max_long: float,
    max_short: float,
    sector_cap_long: float,
    sector_cap_short: float,
    gross_cap: float,
    net_min: float,
    net_max: float,
    cash_floor: float,
    top_n: int,
    score_exp: float,
) -> Dict[str, float]:
    if not allow_shorts:
        return propose_target_weights(
            ideas,
            max_pos=max_long,
            sector_cap=sector_cap_long,
            gross_cap=gross_cap,
            cash_floor=cash_floor,
            top_n=top_n,
            score_exp=score_exp,
        )

    eligible = [idea for idea in ideas if idea.llm_allow]
    long_bucket = sorted(
        [idea for idea in eligible if idea.score > 0],
        key=lambda x: x.score,
        reverse=True,
    )[:top_n]
    short_bucket = sorted(
        [idea for idea in eligible if idea.score < 0],
        key=lambda x: x.score,
    )[:top_n]

    if not long_bucket and not short_bucket:
        return {}

    sector_map = {idea.symbol: idea.sector for idea in ideas}

    budget = max(gross_cap - cash_floor, 0.0)
    weights: Dict[str, float] = {}

    long_norm = _softmax_norm([idea.score for idea in long_bucket], p=score_exp)
    short_norm = _abs_softmax_norm([idea.score for idea in short_bucket], p=score_exp)

    long_mass = sum((max(idea.score, 0.0)) ** score_exp for idea in long_bucket)
    short_mass = sum((abs(idea.score)) ** score_exp for idea in short_bucket)
    total_mass = long_mass + short_mass

    if total_mass <= 0:
        total_mass = 1.0

    long_budget = budget * (long_mass / total_mass) if long_mass > 0 else 0.0
    short_budget = min(budget - long_budget, budget) if short_mass > 0 else 0.0
    if short_budget < 0:
        short_budget = 0.0

    for idea, weight_frac in zip(long_bucket, long_norm):
        alloc = weight_frac * long_budget
        weights[idea.symbol] = min(alloc, max_long)

    for idea, weight_frac in zip(short_bucket, short_norm):
        alloc = weight_frac * short_budget
        weights[idea.symbol] = -min(alloc, max_short)

    def sector_of(symbol: str) -> str:
        return sector_map.get(symbol, "UNK")

    def _enforce_sector_cap(cap: float, short: bool = False) -> None:
        if cap <= 0:
            return
        changed = True
        while changed:
            changed = False
            sector_sum: Dict[str, float] = defaultdict(float)
            for sym, w in weights.items():
                if short and w < 0:
                    sector_sum[sector_of(sym)] += abs(w)
                elif not short and w > 0:
                    sector_sum[sector_of(sym)] += w
            for sec, total in sector_sum.items():
                if total > cap:
                    ratio = cap / max(total, 1e-9)
                    for sym in list(weights.keys()):
                        w = weights[sym]
                        if sector_of(sym) != sec:
                            continue
                        if short and w < 0:
                            new_w = w * ratio
                        elif not short and w > 0:
                            new_w = w * ratio
                        else:
                            continue
                        if abs(new_w - w) > 1e-9:
                            weights[sym] = new_w
                            changed = True

    _enforce_sector_cap(sector_cap_long, short=False)
    _enforce_sector_cap(sector_cap_short, short=True)

    gross = sum(abs(w) for w in weights.values())
    if gross > gross_cap and gross > 0:
        ratio = gross_cap / gross
        for sym in list(weights.keys()):
            weights[sym] *= ratio

    long_gross = sum(w for w in weights.values() if w > 0)
    short_gross = sum(-w for w in weights.values() if w < 0)
    net = long_gross - short_gross

    if net > net_max and long_gross > 0:
        scale = (net_max + short_gross) / max(long_gross, 1e-9)
        scale = max(min(scale, 1.0), 0.0)
        for sym in list(weights.keys()):
            if weights[sym] > 0:
                weights[sym] *= scale
        long_gross = sum(w for w in weights.values() if w > 0)
        short_gross = sum(-w for w in weights.values() if w < 0)
        net = long_gross - short_gross

    if net < net_min and short_gross > 0:
        scale = (long_gross - net_min) / max(short_gross, 1e-9)
        scale = max(min(scale, 1.0), 0.0)
        for sym in list(weights.keys()):
            if weights[sym] < 0:
                weights[sym] *= scale

    return {sym: w for sym, w in weights.items() if abs(w) > 1e-9}
