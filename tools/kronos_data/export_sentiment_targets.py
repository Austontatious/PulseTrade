"""Export daily sentiment + price target features for NBEATS training."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd
import psycopg2

PROCESSED = Path("/mnt/data/kronos_data/processed")
PROCESSED.mkdir(parents=True, exist_ok=True)
OUT_FILE = PROCESSED / "sentiment_price_targets_daily.parquet"


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


def _fetch_frame(query: str) -> pd.DataFrame:
    conn = psycopg2.connect(database_url())
    try:
        df = pd.read_sql(query, conn, parse_dates=["ds"])
    finally:
        conn.close()
    if df.empty:
        return df
    df["unique_id"] = df["symbol"].str.upper()
    df = df.drop(columns=["symbol"])
    df = df.sort_values(["unique_id", "ds"])
    return df


def load_sentiment() -> pd.DataFrame:
    query = """
        SELECT
          symbol,
          as_of::date AS ds,
          sentiment_score,
          positive_count,
          negative_count,
          neutral_count
        FROM fmp_social_sentiment
    """
    return _fetch_frame(query)


def load_price_targets() -> pd.DataFrame:
    query = """
        SELECT
          symbol,
          published_date AS ds,
          target_avg,
          target_high,
          target_low,
          target_current,
          analyst_count
        FROM fmp_price_targets_v4
    """
    return _fetch_frame(query)


def _ffill_events(df: pd.DataFrame, columns: Iterable[str]) -> pd.DataFrame:
    frames = []
    for sym, grp in df.groupby("unique_id", sort=False):
        grp = grp.sort_values("ds")
        grp = grp.set_index("ds")
        all_days = (
            pd.date_range(grp.index.min(), grp.index.max(), freq="D")
            if len(grp.index) > 0
            else pd.DatetimeIndex([])
        )
        if all_days.empty:
            continue
        expanded = grp.reindex(all_days).ffill()
        expanded["unique_id"] = sym
        expanded = expanded.reset_index().rename(columns={"index": "ds"})
        frames.append(expanded[["unique_id", "ds", *columns]])
    if not frames:
        return pd.DataFrame(columns=["unique_id", "ds", *columns])
    merged = pd.concat(frames, ignore_index=True)
    merged = merged.dropna(subset=["ds"]).sort_values(["unique_id", "ds"])
    merged[columns] = merged[columns].replace([np.inf, -np.inf], np.nan)
    return merged


def main() -> None:
    sent = load_sentiment()
    targets = load_price_targets()
    if sent.empty and targets.empty:
        print("[export-sentiment-targets] No rows available; aborting.")
        return
    frames = []
    if not sent.empty:
        sent = sent.rename(
            columns={
                "sentiment_score": "sentiment_score",
                "positive_count": "sentiment_pos",
                "negative_count": "sentiment_neg",
                "neutral_count": "sentiment_neu",
            }
        )
        sent = sent[["unique_id", "ds", "sentiment_score", "sentiment_pos", "sentiment_neg", "sentiment_neu"]]
        frames.append(sent)
    if not targets.empty:
        targets = _ffill_events(
            targets,
            columns=["target_avg", "target_high", "target_low", "target_current", "analyst_count"],
        )
        frames.append(targets)
    if not frames:
        print("[export-sentiment-targets] No usable frames after processing.")
        return
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.merge(frame, on=["unique_id", "ds"], how="outer")
    combined = combined.sort_values(["unique_id", "ds"])
    combined.to_parquet(OUT_FILE, index=False)
    print(f"[export-sentiment-targets] Wrote {len(combined)} rows to {OUT_FILE}")


if __name__ == "__main__":
    main()
