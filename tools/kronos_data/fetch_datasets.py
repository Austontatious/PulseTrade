from __future__ import annotations

import datetime as dt
import json
import os
import shutil
import zipfile
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2
import psycopg2.extras
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download

BASE = Path("/mnt/data/kronos_data/raw")
BASE.mkdir(parents=True, exist_ok=True)
INSERT_BATCH_SIZE = 2000
UNIVERSE_SYMS = Path("/mnt/data/PulseTrade/db/alpaca_universe.symbols.txt")


def write_meta(name: str, payload: dict) -> None:
    path = BASE / f"{name}.dataset_meta.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


def load_universe_symbols(path: Path = UNIVERSE_SYMS) -> set[str]:
    if not path.exists():
        raise FileNotFoundError(f"Alpaca universe symbols not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        syms = {line.strip().upper() for line in f if line.strip()}
    if not syms:
        raise RuntimeError("Alpaca universe symbol list is empty.")
    return syms


def fetch_fnspid() -> None:
    repo = "Zihan1004/FNSPID"
    files = {
        "Stock_price/full_history.zip": "FNSPID_full_history.zip",
        "Stock_news/All_external.csv": "FNSPID_All_external.csv",
    }
    local_paths = {}
    for remote, local_name in files.items():
        downloaded = hf_hub_download(
            repo_id=repo,
            repo_type="dataset",
            filename=remote,
            local_dir=str(BASE),
            local_dir_use_symlinks=False,
        )
        dest = BASE / local_name
        if Path(downloaded) != dest:
            shutil.move(downloaded, dest)
        local_paths[remote] = dest

    # Convert zipped price history to parquet
    price_zip = local_paths["Stock_price/full_history.zip"]
    dest_parquet = BASE / "FNSPID_prices.parquet"
    # Load Alpaca universe and prune at source
    try:
        universe = load_universe_symbols()
    except FileNotFoundError as exc:
        print(f"[fetch-datasets] {exc}. Run build_alpaca_universe.py first.")
        raise
    with zipfile.ZipFile(price_zip) as zf:
        csv_members = [
            name
            for name in zf.namelist()
            if name.lower().endswith(".csv") and not name.startswith("__MACOSX")
        ]
        if not csv_members:
            raise RuntimeError("full_history.zip contains no CSV files.")

        # Only process files whose stem (ticker) is in Alpaca universe
        def _sym_ok(member_name: str) -> bool:
            try:
                return Path(member_name).stem.upper() in universe
            except Exception:
                return False
        csv_members = [m for m in csv_members if _sym_ok(m)]
        print(
            f"[fetch-datasets] FNSPID members pruned to {len(csv_members)} files (universe={len(universe)})"
        )

        writer: pq.ParquetWriter | None = None
        for name in csv_members:
            with zf.open(name) as fh:
                frame = pd.read_csv(fh)
            frame.columns = [c.strip().lower() for c in frame.columns]
            expected_order = ["date", "open", "high", "low", "close", "adj close", "volume"]
            for col in expected_order:
                if col not in frame.columns:
                    raise RuntimeError(f"Expected column '{col}' missing in {name}")
            frame = frame[expected_order]
            numeric_cols = ["open", "high", "low", "close", "adj close"]
            for col in numeric_cols:
                frame[col] = pd.to_numeric(frame[col], errors="coerce").astype("float64")
            frame["volume"] = pd.to_numeric(frame["volume"], errors="coerce").astype("float64")
            frame[numeric_cols] = frame[numeric_cols].ffill().bfill()
            frame["volume"] = frame["volume"].fillna(0.0)
            frame = frame.dropna(subset=["date"])
            symbol = Path(name).stem.upper()
            frame["symbol"] = symbol
            table = pa.Table.from_pandas(frame, preserve_index=False)
            if writer is None:
                writer = pq.ParquetWriter(dest_parquet, table.schema)
            writer.write_table(table)
        if writer is not None:
            writer.close()

    # Convert news CSVs to parquet
    for key in ("Stock_news/All_external.csv",):
        csv_path = local_paths[key]
        df = pd.read_csv(csv_path)
        parquet_name = Path(csv_path).with_suffix(".parquet").name
        df.to_parquet(BASE / parquet_name, index=False)

    write_meta(
        "FNSPID",
        {
            "info": "Fetched FNSPID repo assets (prices + news) from HF, pruned to Alpaca universe",
            "universe_count": len(universe),
        },
    )


def fetch_financial_tweets() -> None:
    repo = "StephanAkkerman/financial-tweets"
    csv_path = hf_hub_download(
        repo_id=repo,
        repo_type="dataset",
        filename="financial_tweets.csv",
        local_dir=str(BASE),
        local_dir_use_symlinks=False,
    )
    dest_csv = BASE / "financial_tweets.csv"
    if Path(csv_path) != dest_csv:
        shutil.move(csv_path, dest_csv)
    df = pd.read_csv(dest_csv)
    df.to_parquet(BASE / "financial_tweets.parquet", index=False)
    write_meta("financial_tweets", {"info": "Fetched financial-tweets CSV snapshot from HF"})


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


def _prepare_price_frame(path: Path) -> pd.DataFrame:
    if not path.exists():
        print(f"[fetch-datasets] price parquet not found at {path}")
        return pd.DataFrame()
    df = pd.read_parquet(path)
    if df.empty:
        return pd.DataFrame()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    required = {"date", "adj_close", "volume", "symbol"}
    missing = required - set(df.columns)
    if missing:
        raise RuntimeError(f"Missing expected columns in price parquet: {sorted(missing)}")
    df["symbol"] = df["symbol"].astype(str).str.upper()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date", "adj_close"]).copy()
    df["adj_close"] = pd.to_numeric(df["adj_close"], errors="coerce")
    df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0.0)
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["adj_close"]).copy()
    df = df[df["adj_close"] > 0]
    df = df.sort_values(["symbol", "date"])
    df["log_price"] = np.log(df["adj_close"])
    df["y"] = df.groupby("symbol")["log_price"].diff()
    df["dollar_vol"] = df["adj_close"] * df["volume"]
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.dropna(subset=["y", "dollar_vol"])
    df["ds"] = df["date"].dt.date
    return df[["ds", "symbol", "y", "dollar_vol"]]


def _upsert_symbols(conn, symbols: list[str]) -> None:
    if not symbols:
        return
    meta_payload = {"source": "FNSPID→ALPACA_PRUNED", "fetched_at": dt.datetime.utcnow().isoformat()}
    rows = [(sym, json.dumps(meta_payload)) for sym in symbols]
    sql = """
        INSERT INTO symbols(ticker, class, venue, meta)
        VALUES (%s, 'equity', 'ALPACA', %s::jsonb)
        ON CONFLICT (ticker, class)
        DO UPDATE SET
            venue = EXCLUDED.venue,
            meta = COALESCE(symbols.meta, '{}'::jsonb) || EXCLUDED.meta
    """
    with conn.cursor() as cur:
        psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)


def _upsert_daily_returns(conn, frame: pd.DataFrame) -> int:
    if frame.empty:
        return 0
    sql = """
        INSERT INTO daily_returns (ds, symbol, y, dollar_vol)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (symbol, ds)
        DO UPDATE SET
            y = EXCLUDED.y,
            dollar_vol = EXCLUDED.dollar_vol
    """
    total = 0
    data_iter = (
        (
            row.ds,
            row.symbol,
            float(row.y),
            float(row.dollar_vol),
        )
        for row in frame.itertuples(index=False)
    )
    with conn.cursor() as cur:
        batch: list[tuple] = []
        for record in data_iter:
            batch.append(record)
            if len(batch) >= INSERT_BATCH_SIZE:
                psycopg2.extras.execute_batch(cur, sql, batch, page_size=INSERT_BATCH_SIZE)
                total += len(batch)
                batch = []
        if batch:
            psycopg2.extras.execute_batch(cur, sql, batch, page_size=INSERT_BATCH_SIZE)
            total += len(batch)
    return total


def _refresh_liquidity_view(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("REFRESH MATERIALIZED VIEW mv_liquidity_60d")


def load_into_database() -> None:
    skip = os.getenv("FETCH_DATASETS_SKIP_DB_LOAD") == "1"
    if skip:
        print("[fetch-datasets] DB load skipped via FETCH_DATASETS_SKIP_DB_LOAD=1")
        return
    prices_path = BASE / "FNSPID_prices.parquet"
    frame = _prepare_price_frame(prices_path)
    if frame.empty:
        print("[fetch-datasets] No price rows to ingest; skipping DB load")
        return
    # Extra safety: ensure only Alpaca universe symbols make it to DB
    try:
        universe = load_universe_symbols()
        frame = frame[frame["symbol"].isin(universe)]
    except Exception:
        pass
    try:
        conn = psycopg2.connect(database_url())
    except psycopg2.Error as exc:
        print(f"[fetch-datasets] Could not connect to database ({exc}); skipping DB load")
        return

    try:
        symbols = sorted(frame["symbol"].unique())
        _upsert_symbols(conn, symbols)
        conn.commit()
        rows = _upsert_daily_returns(conn, frame)
        conn.commit()
        if rows:
            _refresh_liquidity_view(conn)
            conn.commit()
        print(
            f"[fetch-datasets] Loaded {rows} daily_return rows for {len(symbols)} Alpaca-tradable symbols into PostgreSQL"
        )
    finally:
        conn.close()


def main() -> None:
    fetch_fnspid()
    fetch_financial_tweets()
    print("Raw snapshots written to", BASE)
    load_into_database()


if __name__ == "__main__":
    main()
