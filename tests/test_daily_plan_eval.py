from libs.planner.daily_plan_eval import (
    DailyPlan,
    PlanAction,
    evaluate_plan,
)


def _plan(side: str) -> DailyPlan:
    return DailyPlan(
        symbol="TEST",
        side=side,  # type: ignore[arg-type]
        actions=[PlanAction(name="entry", kind="entry", window="09:30-10:00", direction="buy", sizing="notional")],
        reason="test",
    )


def test_long_plan_requires_positive_mu_lower() -> None:
    plan = _plan("long")
    result = evaluate_plan(
        plan,
        mean_daily_return=0.02,
        vol_daily=0.0,
        eps_mu=1e-6,
        eps_p=1e-6,
        p_loss_max=0.45,
        mu_min=0.001,
        lambda_risk=0.0,
    )
    assert result.mu > 0
    assert result.ok_to_trade


def test_short_plan_allows_negative_mu_when_certainty_high() -> None:
    plan = _plan("short")
    result = evaluate_plan(
        plan,
        mean_daily_return=0.02,
        vol_daily=0.0,
        eps_mu=1e-6,
        eps_p=1e-6,
        p_loss_max=0.45,
        mu_min=0.001,
        lambda_risk=0.0,
        mu_min_short=0.01,
        epsilon_success=-0.05,
    )
    assert result.mu < 0  # short losing expectation when treated as long
    assert result.ok_to_trade
