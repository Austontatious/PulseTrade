"""Rank symbols by NBEATS residual performance and emit top universe."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Rank symbols by residual metrics and select top-K universe.")
    parser.add_argument("--residuals", type=Path, required=True, help="Path to residuals parquet from train_nbeats.")
    parser.add_argument("--top-k", type=int, default=100, help="Number of symbols to retain.")
    parser.add_argument("--report", type=Path, default=Path("reports/top100_best_fit.json"), help="Where to write JSON report.")
    parser.add_argument("--update-file", type=Path, default=None, help="Optional symbols.txt to overwrite with Top-K universe.")
    return parser.parse_args()


def _window_order(series: pd.Series) -> List[str]:
    windows = sorted({int(str(name).rstrip("d")) for name in series.unique() if isinstance(name, str) and name.endswith("d")})
    return [f"{w}d" for w in windows]


def compute_symbol_metrics(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame()
    windows = _window_order(df["window"])
    rows: List[Dict[str, float]] = []
    for sym, grp in df.groupby("unique_id", sort=False):
        entry: Dict[str, float] = {"unique_id": sym}
        entry["mae_all"] = float(grp["abs_error"].mean()) if not grp["abs_error"].empty else np.nan
        entry["dir_all"] = float(grp["direction_ok"].mean()) if not grp["direction_ok"].empty else np.nan
        entry["latest_ds"] = str(grp["ds"].max().date())
        for window in windows:
            subset = grp[grp["window"] == window]
            if subset.empty:
                entry[f"mae_{window}"] = np.nan
                entry[f"dir_{window}"] = np.nan
                continue
            entry[f"mae_{window}"] = float(subset["abs_error"].mean())
            entry[f"dir_{window}"] = float(subset["direction_ok"].mean())
        rows.append(entry)
    metrics_df = pd.DataFrame(rows)
    return metrics_df, windows


def rank_symbols(metrics_df: pd.DataFrame, windows: List[str], top_k: int) -> pd.DataFrame:
    if metrics_df.empty:
        return metrics_df
    sort_keys = [f"mae_{windows[-1]}" ] if windows else ["mae_all"]
    if len(windows) > 1:
        sort_keys.extend(f"mae_{w}" for w in reversed(windows[:-1]))
    sort_keys.append("mae_all")
    sort_keys = [key for key in sort_keys if key in metrics_df.columns]
    ranked = metrics_df.sort_values(sort_keys, ascending=True, na_position="last").head(top_k).reset_index(drop=True)
    ranked["rank"] = ranked.index + 1
    return ranked


def main() -> None:
    args = parse_args()
    df = pd.read_parquet(args.residuals)
    metrics_df, windows = compute_symbol_metrics(df)
    ranked = rank_symbols(metrics_df, windows, args.top_k)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "residuals_path": str(args.residuals),
        "windows": windows,
        "top_k": args.top_k,
        "symbols": ranked.to_dict(orient="records"),
    }
    with args.report.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2)
    print(f"[rank-best-fit] Wrote report to {args.report}")
    if args.update_file:
        args.update_file.parent.mkdir(parents=True, exist_ok=True)
        with args.update_file.open("w", encoding="utf-8") as handle:
            for sym in ranked["unique_id"]:
                handle.write(f"{sym}\n")
        print(f"[rank-best-fit] Updated universe file at {args.update_file}")


if __name__ == "__main__":
    main()
