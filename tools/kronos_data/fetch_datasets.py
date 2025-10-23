from __future__ import annotations

import json
import os
import shutil
import zipfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from huggingface_hub import hf_hub_download

BASE = Path("/mnt/data/kronos_data/raw")
BASE.mkdir(parents=True, exist_ok=True)


def write_meta(name: str, payload: dict) -> None:
    path = BASE / f"{name}.dataset_meta.json"
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)


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
    with zipfile.ZipFile(price_zip) as zf:
        csv_members = [
            name
            for name in zf.namelist()
            if name.lower().endswith(".csv") and not name.startswith("__MACOSX")
        ]
        if not csv_members:
            raise RuntimeError("full_history.zip contains no CSV files.")

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

    write_meta("FNSPID", {"info": "Fetched FNSPID repo assets (prices + news) from HF"})


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


def main() -> None:
    fetch_fnspid()
    fetch_financial_tweets()
    print("Raw snapshots written to", BASE)


if __name__ == "__main__":
    main()
