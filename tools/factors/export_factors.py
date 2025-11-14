from __future__ import annotations

import os
from typing import Iterable, List

import numpy as np
import pandas as pd
import psycopg2


OUT_PATH = "/mnt/data/kronos_data/interim/fundamentals_factors_v1.parquet"
QUIVER_PUBLIC_EXTRAS = os.getenv("QUIVER_ENABLE_PUBLIC_EXTRAS", "false").lower() == "true"


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "pulse")
    password = os.getenv("POSTGRES_PASSWORD", "pulsepass")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    name = os.getenv("POSTGRES_DB", "pulse")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


def _rolling_z(series: pd.Series, window: int, min_periods: int) -> pd.Series:
    rolling_mean = series.rolling(window, min_periods=min_periods).mean()
    rolling_std = series.rolling(window, min_periods=min_periods).std(ddof=0)
    z = (series - rolling_mean) / rolling_std
    return z.replace([np.inf, -np.inf], np.nan)


def _compute_quiver_factors(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["symbol", "ds"])

    df["as_of"] = pd.to_datetime(df["as_of"])
    pivot = (
        df.pivot_table(index=["symbol", "as_of"], columns="metric", values="value", aggfunc="first")
        .sort_index()
        .fillna(np.nan)
    )

    records: List[pd.DataFrame] = []
    for symbol, frame in pivot.groupby(level=0):
        frame = frame.droplevel(0)
        frame = frame.sort_index()
        out = pd.DataFrame(index=frame.index)

        congress_net = None
        if "quiver_congress_net_usd" in frame:
            congress_net = frame["quiver_congress_net_usd"].fillna(0).rolling(30, min_periods=5).sum()

        if "quiver_house_net_usd" in frame:
            house_roll = frame["quiver_house_net_usd"].fillna(0).rolling(30, min_periods=5).sum()
            out["house_net_usd_30d_z"] = _rolling_z(house_roll, 252, 30)
            congress_net = house_roll if congress_net is None else congress_net.add(house_roll, fill_value=0)

        if "quiver_senate_net_usd" in frame:
            senate_roll = frame["quiver_senate_net_usd"].fillna(0).rolling(30, min_periods=5).sum()
            out["senate_net_usd_30d_z"] = _rolling_z(senate_roll, 252, 30)
            congress_net = senate_roll if congress_net is None else congress_net.add(senate_roll, fill_value=0)

        if congress_net is not None:
            out["congress_net_usd_30d_z"] = _rolling_z(congress_net, 252, 30)

        if "quiver_insider_net_usd" in frame:
            insider_net = frame["quiver_insider_net_usd"].fillna(0).rolling(30, min_periods=5).sum()
            out["insider_net_usd_30d_z"] = _rolling_z(insider_net, 252, 30)

        if "quiver_lobbying_spend_usd" in frame:
            spend_365 = frame["quiver_lobbying_spend_usd"].fillna(0).rolling(365, min_periods=30).sum()
            out["lobbying_spend_12m_z"] = _rolling_z(spend_365, 365, 60)

        if "quiver_gov_award_usd" in frame:
            awards_365 = frame["quiver_gov_award_usd"].fillna(0).rolling(365, min_periods=30).sum()
            out["govcontracts_awarded_12m_usd_z"] = _rolling_z(awards_365, 365, 60)

        if "quiver_offex_shortvol_ratio" in frame:
            ratio = frame["quiver_offex_shortvol_ratio"].astype(float)
            avg5 = ratio.rolling(5, min_periods=3).mean()
            out["shortvol_ratio_5d_z"] = _rolling_z(avg5, 90, 20)
            out["shortvol_ratio_chg_5d"] = avg5 - avg5.shift(5)

        if QUIVER_PUBLIC_EXTRAS:
            if "quiver_app_rating" in frame:
                out["app_rating_30d_avg"] = frame["quiver_app_rating"].rolling(30, min_periods=5).mean()
            if "quiver_etf_weight_pct" in frame:
                out["etf_weight_pct_7d_avg"] = frame["quiver_etf_weight_pct"].rolling(7, min_periods=3).mean()
            if "quiver_political_beta_today" in frame:
                out["political_beta_today"] = frame["quiver_political_beta_today"].copy()

        out = out.dropna(how="all")
        if not out.empty:
            out["symbol"] = symbol
            out = out.reset_index().rename(columns={"as_of": "ds"})
            records.append(out)

    if not records:
        return pd.DataFrame(columns=["symbol", "ds"])
    factors = pd.concat(records, ignore_index=True)
    return factors


def main() -> None:
    conn = psycopg2.connect(database_url())
    fundamentals_query = """
        SELECT *
        FROM fundamentals_factors_v1
        WHERE ds >= CURRENT_DATE - INTERVAL '5 years'
        ORDER BY ds, symbol
    """
    base_df = pd.read_sql(fundamentals_query, conn)

    metrics = [
        "quiver_congress_net_usd",
        "quiver_house_net_usd",
        "quiver_senate_net_usd",
        "quiver_insider_net_usd",
        "quiver_lobbying_spend_usd",
        "quiver_gov_award_usd",
        "quiver_offex_shortvol_ratio",
    ]
    if QUIVER_PUBLIC_EXTRAS:
        metrics.extend(
            [
                "quiver_app_rating",
                "quiver_app_reviews",
                "quiver_etf_weight_pct",
                "quiver_political_beta_today",
                "quiver_political_beta_bulk",
            ]
        )

    placeholders = ",".join(["%s"] * len(metrics))
    metrics_query = f"""
        SELECT symbol, as_of, metric, value
        FROM ingest_metrics
        WHERE metric IN ({placeholders})
          AND as_of >= CURRENT_DATE - INTERVAL '730 days'
    """
    quiver_df = pd.read_sql(metrics_query, conn, params=metrics)
    conn.close()

    quiver_factors = _compute_quiver_factors(quiver_df)

    if "ds" not in base_df.columns:
        base_df.rename(columns={"as_of": "ds"}, inplace=True)

    if not quiver_factors.empty:
        merged = pd.merge(base_df, quiver_factors, on=["symbol", "ds"], how="outer")
    else:
        merged = base_df

    merged.sort_values(["ds", "symbol"], inplace=True)
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    merged.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {len(merged)} rows → {OUT_PATH}")


if __name__ == "__main__":
    main()
