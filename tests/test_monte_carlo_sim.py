from __future__ import annotations

import numpy as np

from tools.universe import monte_carlo_sim as mc


def test_simulate_symbol_generates_positive_score() -> None:
    rng = np.random.default_rng(0)
    returns = [0.001] * 200
    config = mc.MonteCarloConfig(
        num_simulations=100,
        days=5,
        lookback_days=200,
        min_history=50,
        seed=None,
    )
    result = mc._simulate_symbol("TEST", 100.0, returns, config, rng)  # type: ignore[attr-defined]
    assert result is not None
    assert result.mean_profit > 0
    assert result.best_score > 0
    assert result.sim_count >= config.batch_size


def test_simulate_symbol_requires_min_history() -> None:
    rng = np.random.default_rng(0)
    returns = [0.001] * 5
    config = mc.MonteCarloConfig(min_history=10)
    result = mc._simulate_symbol("TEST", 100.0, returns, config, rng)  # type: ignore[attr-defined]
    assert result is None
