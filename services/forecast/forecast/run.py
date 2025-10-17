import asyncpg, os, asyncio, pandas as pd, json
from typing import Any, Dict, List
from .model_swap import Model

DB_DSN = f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}@" \
         f"{os.getenv('POSTGRES_HOST','db')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}"

TICKERS = ["BTCUSD", "ETHUSD"]  # extend from symbols table later
HORIZON = "1m"
MODEL = Model()
MODEL_NAME = MODEL.__class__.__name__

async def load_covariates(conn: asyncpg.Connection, ticker: str) -> Dict[str, Any]:
    social_rows = await conn.fetch(
        """SELECT ts, window_minutes, msg_rate, senti_mean, senti_std, top_handles
           FROM social_features
           WHERE ticker=$1
           ORDER BY ts DESC, window_minutes ASC
           LIMIT 12""",
        ticker,
    )
    social_features: List[Dict[str, Any]] = [
        {
            "ts": row["ts"],
            "window": row["window_minutes"],
            "rate": float(row["msg_rate"]) if row["msg_rate"] is not None else None,
            "senti_mean": float(row["senti_mean"]) if row["senti_mean"] is not None else None,
            "senti_std": float(row["senti_std"]) if row["senti_std"] is not None else None,
            "top_handles": row["top_handles"],
        }
        for row in social_rows
    ]
    return {"social": social_features}

async def forecast_once(conn, ticker: str):
    q = """SELECT ts, price FROM trades WHERE ticker=$1 AND ts > NOW() - INTERVAL '10 minutes' ORDER BY ts ASC"""
    rows = await conn.fetch(q, ticker)
    if len(rows) < 5:
        return
    s = pd.Series([r["price"] for r in rows], index=[r["ts"] for r in rows])
    covariates = await load_covariates(conn, ticker)
    feature_snapshot = {
        "social": [
            {
                "window": item["window"],
                "rate": item["rate"],
                "senti_mean": item["senti_mean"],
                "senti_std": item["senti_std"],
            }
            for item in covariates.get("social", [])
        ]
    }
    mean, lower, upper = MODEL.predict(s, covariates=covariates)
    await conn.execute("""INSERT INTO forecasts(ts,ticker,horizon,model,mean,lower,upper,features)
                          VALUES(NOW(), $1, $2, $3, $4, $5, $6, $7)""",
                       ticker, HORIZON, MODEL_NAME, mean, lower, upper, json.dumps(feature_snapshot))

async def main():
    while True:
        conn = await asyncpg.connect(dsn=DB_DSN)
        try:
            for t in TICKERS:
                await forecast_once(conn, t)
        finally:
            await conn.close()
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
