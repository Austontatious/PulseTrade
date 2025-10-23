from __future__ import annotations

import os
from pathlib import Path

import pandas as pd

PROC = Path("/mnt/data/kronos_data/processed/nbeats_global_daily.parquet")
AUX = Path("/mnt/data/kronos_data/interim/aux_features_daily.parquet")
LOGS = Path("/mnt/data/kronos_data/logs")
LOGS.mkdir(parents=True, exist_ok=True)


def main() -> None:
    issues: list[str] = []
    if not PROC.exists():
        raise SystemExit(f"Processed file missing: {PROC}")
    df = pd.read_parquet(PROC)
    if df["y"].isna().any():
        issues.append("Null y found.")
    dupes = int(df.duplicated(["unique_id", "ds"]).sum())
    if dupes:
        issues.append(f"Duplicates in (unique_id, ds): {dupes}")
    if (df["y"].abs() > 0.25).mean() > 0.001:
        issues.append(">0.1% daily moves >25% — likely split not adjusted in slice.")
    non_monotonic = sum(
        (group["ds"].diff().dt.days.fillna(1) < 0).any()
        for _, group in df.groupby("unique_id")
    )
    if non_monotonic:
        issues.append(f"{non_monotonic} symbols with non-monotonic dates")

    aux = pd.read_parquet(AUX) if AUX.exists() else pd.DataFrame()
    if not aux.empty and aux["news_count"].lt(0).any():
        issues.append("Negative news_count values detected.")

    log_path = LOGS / "validate_surface.txt"
    with log_path.open("w", encoding="utf-8") as handle:
        if issues:
            handle.write("PROBLEMS:\n- " + "\n- ".join(issues))
        else:
            handle.write("OK")
    print("Validation written to", log_path)


if __name__ == "__main__":
    main()
