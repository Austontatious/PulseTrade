from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from typing import Dict, List, Literal

import numpy as np

Z_VALUE = 1.959963984540054  # 95% confidence


@dataclass
class PlanAction:
    name: str
    kind: Literal["entry", "exit", "adjust"]
    window: str
    direction: Literal["buy", "sell"]
    sizing: str


@dataclass
class DailyPlan:
    symbol: str
    side: Literal["long", "short"]
    actions: List[PlanAction]
    reason: str

    def to_dict(self) -> Dict:
        return {
            "symbol": self.symbol,
            "side": self.side,
            "reason": self.reason,
            "actions": [asdict(a) for a in self.actions],
        }


@dataclass
class PlanEvaluation:
    plan: DailyPlan
    mu: float
    sigma: float
    p_success: float
    p_loss_upper: float
    mu_lower: float
    score: float
    ok_to_trade: bool
    sims: int

    def serialize(self) -> Dict:
        data = {
            "mu": self.mu,
            "sigma": self.sigma,
            "p_success": self.p_success,
            "p_loss_upper": self.p_loss_upper,
            "mu_lower": self.mu_lower,
            "score": self.score,
            "ok_to_trade": self.ok_to_trade,
            "sims": self.sims,
        }
        data.update(self.plan.to_dict())
        return data


def simulate_plan_returns(
    mean_daily_return: float,
    vol_daily: float,
    side: Literal["long", "short"],
    n_paths: int,
) -> np.ndarray:
    draws = np.random.normal(loc=mean_daily_return, scale=max(vol_daily, 1e-5), size=n_paths)
    if side == "short":
        draws *= -1.0
    return draws


def evaluate_plan(
    plan: DailyPlan,
    mean_daily_return: float,
    vol_daily: float,
    eps_mu: float,
    eps_p: float,
    p_loss_max: float,
    mu_min: float,
    lambda_risk: float,
    epsilon_success: float = 0.0,
    batch_size: int = 512,
    max_paths: int = 10000,
    *,
    mu_min_short: float | None = None,
) -> PlanEvaluation:
    all_draws: List[float] = []
    n_paths = 0
    margin_mu = float("inf")
    margin_p = float("inf")
    p_success = 0.0
    mu_hat = 0.0
    sigma_hat = float(abs(vol_daily))

    while (margin_mu > eps_mu or margin_p > eps_p) and n_paths < max_paths:
        batch = simulate_plan_returns(mean_daily_return, vol_daily, plan.side, batch_size)
        all_draws.extend(batch.tolist())
        n_paths += batch_size
        arr = np.array(all_draws)
        mu_hat = float(arr.mean())
        sigma_hat = float(arr.std(ddof=1)) if n_paths > 1 else float(abs(vol_daily))
        wins = np.greater_equal(arr, epsilon_success)
        p_success = float(wins.mean())
        margin_mu = Z_VALUE * (sigma_hat / math.sqrt(max(n_paths, 1))) if n_paths > 1 else float("inf")
        p_var = max(p_success * (1 - p_success), 1e-6)
        margin_p = Z_VALUE * math.sqrt(p_var / max(n_paths, 1)) if n_paths > 1 else float("inf")

    adj_margin = margin_mu if math.isfinite(margin_mu) else 0.0
    mu_lower = mu_hat - adj_margin
    mu_upper = mu_hat + adj_margin
    p_loss = 1.0 - p_success
    p_loss_upper = min(1.0, p_loss + (margin_p if math.isfinite(margin_p) else 0.0))
    mu_threshold_short = mu_min_short if mu_min_short is not None else mu_min
    if plan.side == "short":
        meets_mu = (
            (mu_lower >= mu_threshold_short)
            or ((-mu_lower) >= mu_threshold_short)
            or ((-mu_upper) >= mu_threshold_short)
        )
        score_mu = abs(mu_hat)
    else:
        meets_mu = (mu_lower >= mu_min)
        score_mu = mu_hat
    score = score_mu - lambda_risk * sigma_hat
    ok_to_trade = (p_loss_upper <= p_loss_max) and meets_mu

    return PlanEvaluation(
        plan=plan,
        mu=mu_hat,
        sigma=sigma_hat,
        p_success=p_success,
        p_loss_upper=p_loss_upper,
        mu_lower=mu_lower,
        score=score,
        ok_to_trade=ok_to_trade,
        sims=n_paths,
    )
