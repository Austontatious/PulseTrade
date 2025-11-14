import pandas as pd

from tools.factors.export_factors import _compute_quiver_factors


def test_compute_quiver_factors_columns_present():
    dates = pd.date_range("2024-01-01", periods=60, freq="D")
    records = []
    for idx, ds in enumerate(dates):
        records.append({
            "symbol": "AAPL",
            "as_of": ds,
            "metric": "quiver_congress_net_usd",
            "value": 1000 * ((-1) ** idx),
        })
        records.append({
            "symbol": "AAPL",
            "as_of": ds,
            "metric": "quiver_house_net_usd",
            "value": 600 * ((-1) ** (idx + 1)),
        })
        records.append({
            "symbol": "AAPL",
            "as_of": ds,
            "metric": "quiver_senate_net_usd",
            "value": 400 * ((-1) ** idx),
        })
        records.append({
            "symbol": "AAPL",
            "as_of": ds,
            "metric": "quiver_lobbying_spend_usd",
            "value": 5000.0,
        })
        records.append({
            "symbol": "AAPL",
            "as_of": ds,
            "metric": "quiver_gov_award_usd",
            "value": 100000.0,
        })
        records.append({
            "symbol": "AAPL",
            "as_of": ds,
            "metric": "quiver_offex_shortvol_ratio",
            "value": 0.2 + idx * 0.001,
        })

    df = pd.DataFrame(records)
    factors = _compute_quiver_factors(df)
    expected = {
        "congress_net_usd_30d_z",
        "house_net_usd_30d_z",
        "senate_net_usd_30d_z",
        "lobbying_spend_12m_z",
        "govcontracts_awarded_12m_usd_z",
        "shortvol_ratio_5d_z",
        "shortvol_ratio_chg_5d",
    }
    assert expected.issubset(factors.columns)
