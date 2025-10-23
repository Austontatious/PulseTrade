from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

RAW = Path("/mnt/data/kronos_data/raw")
OUT_INTERIM = Path("/mnt/data/kronos_data/interim")
OUT_PROCESSED = Path("/mnt/data/kronos_data/processed")
LOGS = Path("/mnt/data/kronos_data/logs")
OUT_INTERIM.mkdir(parents=True, exist_ok=True)
OUT_PROCESSED.mkdir(parents=True, exist_ok=True)
LOGS.mkdir(parents=True, exist_ok=True)


def safe_log(values: pd.Series) -> pd.Series:
    with np.errstate(divide="ignore", invalid="ignore"):
        return np.log(values)


def load_fnspid_prices() -> pd.DataFrame:
    parquet_path = RAW / "FNSPID_prices.parquet"
    if not parquet_path.exists():
        raise SystemExit("FNSPID price parquet not found; run fetch first.")
    df = pd.read_parquet(parquet_path)
    return df


def detect_columns(df: pd.DataFrame) -> tuple[str, str, str]:
    sym_col = next((c for c in df.columns if c.lower() in ("ticker", "symbol", "sym")), None)
    date_col = next(
        (c for c in df.columns if c.lower() in ("date", "ds", "datetime", "time", "timestamp")), None
    )
    adj_col = next(
        (
            c
            for c in df.columns
            if "adj" in c.lower()
            and ("close" in c.lower() or "price" in c.lower() or "last" in c.lower())
        ),
        None,
    )
    if adj_col is None:
        adj_col = next(
            (c for c in df.columns if c.lower() in ("adj_close", "adjusted_close", "close", "price")),
            None,
        )
    if not all([sym_col, date_col, adj_col]):
        raise SystemExit(f"Cannot detect symbol/date/price columns in FNSPID data. Columns: {df.columns.tolist()}")
    return sym_col, date_col, adj_col


def detect_split_ratio(prev: float, curr: float) -> Optional[float]:
    if prev is None or curr is None or prev <= 0 or curr <= 0:
        return None
    ratio = curr / prev
    candidates = [0.25, 0.3333, 0.5, 0.6667, 1.5, 2.0, 3.0, 4.0]
    for cand in candidates:
        if abs(ratio - cand) <= 0.02 * cand:
            return cand
    return None


def build_returns(df: pd.DataFrame, sym_col: str, date_col: str, price_col: str) -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce", utc=True)
    df = df.dropna(subset=[date_col]).copy()
    df["trade_date"] = df[date_col].dt.tz_convert("America/New_York").dt.date

    parts = []
    bad_edges = 0

    for sym, group in df.groupby(sym_col, sort=False):
        daily = (
            group.groupby("trade_date")
            .tail(1)
            .sort_values("trade_date")
            .dropna(subset=[price_col])
            .copy()
        )
        daily["p"] = daily[price_col].astype(float)
        daily["ds"] = pd.to_datetime(daily["trade_date"])
        daily = daily[["ds", "p"]].drop_duplicates("ds").sort_values("ds")

        daily["y"] = np.log(daily["p"]).diff()

        mask_big = daily["y"].abs() > 0.25
        if mask_big.any():
            prev_price = daily["p"].shift(1)
            candidate_idx = daily.index[mask_big & prev_price.notna()]
            for idx in candidate_idx:
                prev_val = float(prev_price.loc[idx])
                curr_val = float(daily.at[idx, "p"])
                if detect_split_ratio(prev_val, curr_val) is not None:
                    daily.at[idx, "y"] = np.nan
                    bad_edges += 1

        valid_y = daily["y"].dropna()
        if not valid_y.empty:
            limit = valid_y.abs().quantile(0.999)
            if pd.notna(limit) and limit > 0:
                daily["y"] = daily["y"].clip(lower=-limit, upper=limit)

        daily = daily.dropna(subset=["y"])
        if daily.empty:
            continue
        daily["unique_id"] = sym
        parts.append(daily[["unique_id", "ds", "y"]])

    if not parts:
        raise SystemExit("No price series remained after preprocessing.")

    combined = pd.concat(parts, ignore_index=True)
    combined = combined[~combined.duplicated(["unique_id", "ds"])]
    combined = combined.sort_values(["unique_id", "ds"])
    print(f"[compile] split-like edges nulled: {bad_edges}")
    return combined


def build_news_features(sym_col_hint: str) -> pd.DataFrame:
    news_path = RAW / "FNSPID_All_external.parquet"
    if not news_path.exists():
        raise SystemExit("FNSPID news parquet not found; run fetch first.")
    news = pd.read_parquet(news_path)
    symbol_col = sym_col_hint
    if symbol_col not in news.columns:
        symbol_col = next(
            (c for c in news.columns if c.lower() in ("ticker", "symbol") or "symbol" in c.lower()),
            None,
        )
    if symbol_col is None or symbol_col not in news.columns:
        raise SystemExit("Could not find symbol column in FNSPID news data.")
    news = news.rename(columns={symbol_col: "symbol_raw"})
    news = news.rename(columns={"Date": "trade_date"} if "Date" in news.columns else {})
    if "trade_date" not in news.columns:
        date_col = next((c for c in news.columns if "date" in c.lower()), None)
        if date_col is None:
            raise SystemExit("Could not find date column in FNSPID news data.")
        news = news.rename(columns={date_col: "trade_date"})
    news["trade_date"] = pd.to_datetime(news["trade_date"], errors="coerce")
    news = news.dropna(subset=["trade_date", "symbol_raw"])
    news["trade_date"] = news["trade_date"].dt.date
    grouped = (
        news.assign(news_count=1)
        .groupby(["symbol_raw", "trade_date"], as_index=False)["news_count"]
        .sum()
    )
    grouped = grouped.rename(columns={"symbol_raw": "unique_id", "trade_date": "ds"})
    grouped["ds"] = pd.to_datetime(grouped["ds"])
    grouped["news_count"] = grouped["news_count"].astype(int)
    return grouped


def build_tweet_features() -> pd.DataFrame:
    tweets_path = RAW / "financial_tweets.parquet"
    if not tweets_path.exists():
        return pd.DataFrame(columns=["unique_id", "ds", "tweet_count"])

    tweets = pd.read_parquet(tweets_path)
    time_col = next(
        (c for c in ["created_at", "timestamp", "time", "date", "datetime"] if c in tweets.columns),
        None,
    )
    sym_col = next((c for c in ["tickers", "symbols", "cashtags", "ticker"] if c in tweets.columns), None)
    if time_col is None or sym_col is None:
        return pd.DataFrame(columns=["unique_id", "ds", "tweet_count"])

    tweets[time_col] = pd.to_datetime(tweets[time_col], errors="coerce", utc=True)
    tweets = tweets.dropna(subset=[time_col])

    def normalise_symbols(value) -> list[str]:
        if isinstance(value, (list, tuple)):
            return [str(item).replace("$", "").upper() for item in value if isinstance(item, str)]
        if isinstance(value, str):
            return [value.replace("$", "").upper()]
        return []

    exploded = tweets[[time_col, sym_col]].copy()
    exploded[sym_col] = exploded[sym_col].apply(normalise_symbols)
    exploded = exploded.explode(sym_col).dropna(subset=[sym_col])
    if exploded.empty:
        return pd.DataFrame(columns=["unique_id", "ds", "tweet_count"])

    exploded["unique_id"] = exploded[sym_col]
    exploded["ds"] = exploded[time_col].dt.tz_convert("America/New_York").dt.date
    exploded["ds"] = pd.to_datetime(exploded["ds"])
    grouped = exploded.groupby(["unique_id", "ds"], as_index=False).size()
    grouped = grouped.rename(columns={"size": "tweet_count"})
    grouped["tweet_count"] = grouped["tweet_count"].astype(int)
    return grouped


def main() -> None:
    price_df = load_fnspid_prices()
    sym_col, date_col, price_col = detect_columns(price_df)

    vol_candidates = [
        c for c in price_df.columns if c.lower() in ("dollar_volume", "dollarvol", "turnover", "volume")
    ]
    if vol_candidates:
        lower_map = {c.lower(): c for c in price_df.columns}
        med_vol = None
        if "dollar_volume" in lower_map:
            dv_col = lower_map["dollar_volume"]
            med_vol = price_df.groupby(sym_col)[dv_col].median()
        else:
            volume_col = lower_map.get("volume")
            if volume_col:
                price_df["dollar_volume_proxy"] = (
                    price_df[price_col].astype(float) * price_df[volume_col].astype(float)
                )
                med_vol = price_df.groupby(sym_col)["dollar_volume_proxy"].median()
        if med_vol is not None:
            keep_symbols = set(med_vol[med_vol > 1_000_000].index)
            price_df = price_df[price_df[sym_col].isin(keep_symbols)]

    returns = build_returns(price_df, sym_col, date_col, price_col)

    news_feat = build_news_features(sym_col)
    tweet_feat = build_tweet_features()

    aux = news_feat.merge(tweet_feat, on=["unique_id", "ds"], how="outer").fillna(0.0)
    if not aux.empty:
        aux["news_count"] = aux["news_count"].astype(int)
        aux["tweet_count"] = aux["tweet_count"].astype(int)

    returns.to_parquet(OUT_PROCESSED / "nbeats_global_daily.parquet", index=False)
    aux.to_parquet(OUT_INTERIM / "aux_features_daily.parquet", index=False)

    qa = {
        "rows": int(len(returns)),
        "symbols": int(returns["unique_id"].nunique()),
        "span_days": int((returns["ds"].max() - returns["ds"].min()).days) if len(returns) else 0,
        "null_y": int(returns["y"].isna().sum()),
        "dupes": int(returns.duplicated(["unique_id", "ds"]).sum()),
        "pct_abs_y_gt_25pct": float((returns["y"].abs() > 0.25).mean()) if len(returns) else 0.0,
    }
    with (LOGS / "qa_compile.json").open("w", encoding="utf-8") as handle:
        json.dump(qa, handle, indent=2)
    with (LOGS / "qa_compile.jsonl").open("a", encoding="utf-8") as handle:
        record = qa.copy()
        record["timestamp"] = pd.Timestamp.utcnow().isoformat()
        handle.write(json.dumps(record) + "\n")

    print(
        "Wrote:",
        OUT_PROCESSED / "nbeats_global_daily.parquet",
        "and",
        OUT_INTERIM / "aux_features_daily.parquet",
    )
    print("QA:", LOGS / "qa_compile.json")


if __name__ == "__main__":
    main()
