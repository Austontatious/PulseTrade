from __future__ import annotations

import os
import sys
import time
from typing import Dict

import numpy as np
import pandas as pd
import psycopg2

from libs.calibration.online_calibrator import OnlineCalibrator

DATABASE_URL = os.getenv("DATABASE_URL")
STATE_DIR = "/mnt/data/kronos_state"
os.makedirs(STATE_DIR, exist_ok=True)
OUT_PATH = os.path.join(STATE_DIR, "calibration.parquet")

HORIZON = "1"
LAM = float(os.getenv("CALIB_LAMBDA", "0.02"))
TARGET_COV = float(os.getenv("CALIB_TARGET_COV", "0.90"))
OUTLIER_SIGMA = float(os.getenv("CALIB_OUTLIER_SIGMA", "4.0"))

if not DATABASE_URL:
    print("DATABASE_URL not set; aborting.")
    sys.exit(1)

con = psycopg2.connect(DATABASE_URL)
cur = con.cursor()

cur.execute(
    """
    SELECT date_trunc('day', ts)::date AS ds, symbol,
           (yhat->>%s)::float AS mu,
           ((q->%s)->>'p05')::float AS p05,
           ((q->%s)->>'p95')::float AS p95
    FROM kronos_forecasts
    WHERE ts >= now() - interval '90 days'
    """,
    (HORIZON, HORIZON, HORIZON),
)
rows = cur.fetchall()
fc = pd.DataFrame(rows, columns=["ds", "symbol", "mu", "p05", "p95"])

cur.execute(
    """
    SELECT ds, symbol, y FROM daily_returns
    WHERE ds >= current_date - interval '90 days'
    """
)
rr = pd.DataFrame(cur.fetchall(), columns=["ds", "symbol", "y"])

con.close()

if fc.empty or rr.empty:
    print("No data to calibrate.")
    sys.exit(0)

df = fc.merge(rr, on=["ds", "symbol"], how="inner").dropna()
if df.empty:
    print("No matching realized vs forecast rows.")
    sys.exit(0)

sector_map: Dict[str, str] = {}

cal = OnlineCalibrator(lam=LAM, target_cov=TARGET_COV, outlier_sigma=OUTLIER_SIGMA)

try:
    prev = pd.read_parquet(OUT_PATH)
    for _, row in prev[prev["level"] == "symbol"].iterrows():
        state = cal.states.get(row["key"]) or cal._get(row["key"])
        state.bias = row["bias"]
        state.var = row["var"]
        state.cov = row["cov"]
        state.scale = row["scale"]
        state.n = int(row["n"])
        state.updated_ts = row["updated_ts"]
    for _, row in prev[prev["level"] == "sector"].iterrows():
        state = cal.sector_states.get(row["key"]) or cal._get(row["key"], "sector")
        state.bias = row["bias"]
        state.var = row["var"]
        state.cov = row["cov"]
        state.scale = row["scale"]
        state.n = int(row["n"])
        state.updated_ts = row["updated_ts"]
    global_row = prev[prev["level"] == "global"]
    if not global_row.empty:
        g = cal.global_state
        row = global_row.iloc[0]
        g.bias = row["bias"]
        g.var = row["var"]
        g.cov = row["cov"]
        g.scale = row["scale"]
        g.n = int(row["n"])
        g.updated_ts = row["updated_ts"]
except Exception:
    pass

for _, row in df.iterrows():
    residual = float(row["y"] - row["mu"])
    inside = (row["y"] >= row["p05"]) and (row["y"] <= row["p95"])
    symbol = row["symbol"]
    sector = sector_map.get(symbol)
    skip = abs(float(row["y"])) > 0.10
    cal.update_symbol(symbol, residual, inside_interval=inside, skip=skip, sector=sector)

out = pd.DataFrame(cal.to_records())
out.to_parquet(OUT_PATH, index=False)
print("Calibration written to", OUT_PATH, "rows:", len(out))
