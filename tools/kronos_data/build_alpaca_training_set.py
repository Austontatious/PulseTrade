"""Build enriched NBEATS training set for the Alpaca universe."""

from __future__ import annotations

import argparse
import datetime as dt
import os
from pathlib import Path
from typing import Iterable, List

import numpy as np
import pandas as pd
import psycopg2


MIN_LOOKBACK_DAYS = 420
DEFAULT_LOOKBACK_DAYS = int(os.getenv("ALPACA_TRAIN_LOOKBACK_DAYS", str(MIN_LOOKBACK_DAYS)))
UNIVERSE_PATH = Path("/mnt/data/PulseTrade/db/alpaca_universe.symbols.txt")
OUT_PATH = Path("/mnt/data/kronos_data/processed/nbeats_alpaca_daily.parquet")


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "pulse")
    password = os.getenv("POSTGRES_PASSWORD", "pulsepass")
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    name = os.getenv("POSTGRES_DB", "pulse")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


def load_symbols(path: Path) -> List[str]:
    if not path.exists():
        raise SystemExit(f"Universe file missing: {path}")
    with path.open("r", encoding="utf-8") as handle:
        symbols = [line.strip().upper() for line in handle if line.strip() and not line.startswith("#")]
    if not symbols:
        raise SystemExit("Universe symbol list is empty.")
    return sorted(set(symbols))


def fetch_prices(conn, symbols: List[str], start_date: dt.date) -> pd.DataFrame:
    query = """
        SELECT ds::date AS ds, symbol, y, dollar_vol
        FROM daily_returns
        WHERE symbol = ANY(%s)
    """
    frame = pd.read_sql(query, conn, params=(symbols,))
    if frame.empty:
        return frame
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    frame = frame.sort_values(["symbol", "ds"])
    frame.rename(columns={"symbol": "unique_id"}, inplace=True)
    frame = frame.dropna(subset=["y"])  # ensure target present
    return frame


def fetch_sentiment(conn, symbols: List[str], start_date: dt.date) -> pd.DataFrame:
    query = """
        SELECT symbol, as_of::date AS ds, sentiment_score, positive_count, negative_count, neutral_count
        FROM fmp_social_sentiment
        WHERE as_of >= %s AND symbol = ANY(%s)
    """
    df = pd.read_sql(query, conn, params=(start_date, symbols))
    if df.empty:
        return df
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df.rename(columns={"symbol": "unique_id"}, inplace=True)
    df = df.sort_values(["unique_id", "ds"])
    return df


def fetch_targets(conn, symbols: List[str], start_date: dt.date) -> pd.DataFrame:
    query = """
        SELECT symbol, published_date::date AS ds,
               target_avg, target_high, target_low, target_current, analyst_count
        FROM fmp_price_targets_v4
        WHERE published_date >= %s AND symbol = ANY(%s)
    """
    df = pd.read_sql(query, conn, params=(start_date, symbols))
    if df.empty:
        return df
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df.rename(columns={"symbol": "unique_id"}, inplace=True)
    df = df.sort_values(["unique_id", "ds"])
    return df


def engineer_sentiment_features(sent_df: pd.DataFrame) -> pd.DataFrame:
    if sent_df.empty:
        return pd.DataFrame(columns=["unique_id", "ds"])
    sent_df = sent_df.sort_values(["unique_id", "ds"])
    out = sent_df.copy()
    out["ds"] = pd.to_datetime(out["ds"])
    windows = (7, 30)
    for col in ("sentiment_score", "positive_count", "negative_count", "neutral_count"):
        grouped = out.groupby("unique_id")[["ds", col]].apply(
            lambda grp: grp.set_index("ds").rolling(window=windows[0], min_periods=1).mean()
        )
        grouped = grouped.reset_index().rename(columns={col: f"{col}_mean_{windows[0]}d"})
        out = out.merge(grouped, on=["unique_id", "ds"], how="left")
        grouped = out.groupby("unique_id")[["ds", col]].apply(
            lambda grp: grp.set_index("ds").rolling(window=windows[1], min_periods=1).mean()
        )
        grouped = grouped.reset_index().rename(columns={col: f"{col}_mean_{windows[1]}d"})
        out = out.merge(grouped, on=["unique_id", "ds"], how="left")
    deltas = out.groupby("unique_id").apply(
        lambda grp: grp.set_index("ds")["sentiment_score"].diff().rename("sentiment_score_delta")
    )
    deltas = deltas.reset_index()
    out = out.merge(deltas, on=["unique_id", "ds"], how="left")
    keep_cols = [
        "unique_id",
        "ds",
        "sentiment_score",
        "sentiment_score_mean_7d",
        "sentiment_score_mean_30d",
        "sentiment_score_delta",
        "positive_count_mean_7d",
        "negative_count_mean_7d",
    ]
    out = out[keep_cols].drop_duplicates(["unique_id", "ds"])
    out.sort_values(["unique_id", "ds"], inplace=True)
    out = out.groupby("unique_id").apply(lambda grp: grp.ffill().bfill()).reset_index(drop=True)
    return out


def engineer_target_features(target_df: pd.DataFrame) -> pd.DataFrame:
    if target_df.empty:
        return pd.DataFrame(columns=["unique_id", "ds"])
    target_df = target_df.sort_values(["unique_id", "ds"])
    target_df["ds"] = pd.to_datetime(target_df["ds"])
    target_df["target_dispersion"] = target_df["target_high"] - target_df["target_low"]
    features = target_df.groupby("unique_id").apply(
        lambda grp: grp.set_index("ds").ffill().reset_index()
    )
    features = features.reset_index(drop=True)
    keep = [
        "unique_id",
        "ds",
        "target_avg",
        "target_high",
        "target_low",
        "target_current",
        "target_dispersion",
        "analyst_count",
    ]
    features = features[keep].drop_duplicates(["unique_id", "ds"])
    features.sort_values(["unique_id", "ds"], inplace=True)
    features = features.groupby("unique_id").apply(lambda grp: grp.ffill().bfill()).reset_index(drop=True)
    return features


def merge_features(price_df: pd.DataFrame, sent_df: pd.DataFrame, target_df: pd.DataFrame, start_date: dt.date) -> pd.DataFrame:
    df = price_df.copy()
    df["ds"] = pd.to_datetime(df["ds"])
    for extra in (sent_df, target_df):
        if extra is None or extra.empty:
            continue
        df = df.merge(extra, on=["unique_id", "ds"], how="left")
    df = df[df["ds"].dt.date >= start_date]
    fill_cols = [
        "sentiment_score",
        "sentiment_score_mean_7d",
        "sentiment_score_mean_30d",
        "sentiment_score_delta",
        "positive_count_mean_7d",
        "negative_count_mean_7d",
        "target_avg",
        "target_high",
        "target_low",
        "target_current",
        "target_dispersion",
        "analyst_count",
    ]
    for col in fill_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0.0)
    df = df.sort_values(["unique_id", "ds"])
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["y"]).reset_index(drop=True)
    return df


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Alpaca universe training dataset for NBEATS")
    parser.add_argument("--symbols", type=Path, default=UNIVERSE_PATH, help="Path to universe symbols list")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS, help="Number of calendar days to include")
    parser.add_argument("--out", type=Path, default=OUT_PATH, help="Destination parquet path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    symbols = load_symbols(args.symbols)
    lookback_days = max(args.lookback_days, MIN_LOOKBACK_DAYS)
    if lookback_days != args.lookback_days:
        print(
            f"[build-alpaca-training-set] requested lookback {args.lookback_days}d "
            f"is shorter than minimum {MIN_LOOKBACK_DAYS}d; using {lookback_days}d instead."
        )
    cutoff = dt.date.today() - dt.timedelta(days=lookback_days)
    conn = psycopg2.connect(database_url())
    try:
        price_df = fetch_prices(conn, symbols, cutoff)
        if price_df.empty:
            raise SystemExit("No price rows returned; aborting.")
        sentiment_raw = fetch_sentiment(conn, symbols, cutoff)
        sentiment_feat = engineer_sentiment_features(sentiment_raw) if not sentiment_raw.empty else pd.DataFrame()
        target_raw = fetch_targets(conn, symbols, cutoff)
        target_feat = engineer_target_features(target_raw) if not target_raw.empty else pd.DataFrame()
    finally:
        conn.close()

    combined = merge_features(price_df, sentiment_feat, target_feat, cutoff)
    args.out.parent.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(args.out, index=False)
    print(f"[build-alpaca-training-set] Wrote {len(combined):,} rows for {combined['unique_id'].nunique()} symbols -> {args.out}")


if __name__ == "__main__":
    main()
