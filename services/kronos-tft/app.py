from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel, Field
import os
import json
import pandas as pd
from neuralforecast import NeuralForecast

API_KEY = os.getenv("API_KEY")
MODEL_DIR = os.getenv("TFT_MODEL_DIR", "/models/kronos-tft/latest")

with open(os.path.join(MODEL_DIR, "config.json"), encoding="utf-8") as handle:
    CFG = json.load(handle)

NF = NeuralForecast.load(MODEL_DIR, verbose=False, weights_only=False)
MODEL_COLUMN = "TFT"
SIGMA = float(CFG.get("sigma", 0.02))

app = FastAPI(title="kronos-tft", version="0.1.0")


def require_key(x_api_key: str | None) -> None:
    if API_KEY and x_api_key != API_KEY:
        raise HTTPException(status_code=401, detail="bad api key")


class ForecastReq(BaseModel):
    symbol: str
    ds: list[str] = Field(..., description="ISO8601 dates for the history window")
    y: list[float]
    covariates: dict[str, list[float]]
    horizons: list[int] = Field(default_factory=lambda: list(range(1, int(CFG["h"]) + 1)))


class ForecastResp(BaseModel):
    symbol: str
    yhat: dict[str, float]
    q: dict[str, dict[str, float]]
    meta: dict[str, str]


@app.get("/health")
def health():
    return {"ok": True, "covariates": CFG.get("covariates", [])}


@app.post("/forecast", response_model=ForecastResp)
def forecast(req: ForecastReq, x_api_key: str | None = Header(default=None)):
    require_key(x_api_key)

    if len(req.ds) != len(req.y):
        raise HTTPException(status_code=400, detail="ds and y length mismatch")

    expected = CFG.get("covariates", [])
    for name in expected:
        series = req.covariates.get(name)
        if series is None or len(series) != len(req.ds):
            raise HTTPException(status_code=400, detail=f"covariate {name} missing or wrong length")

    if len(req.ds) < int(CFG.get("input_size", 1)):
        raise HTTPException(status_code=400, detail="history shorter than input_size")

    frame = pd.DataFrame({"unique_id": req.symbol, "ds": pd.to_datetime(req.ds), "y": req.y})
    for name in expected:
        frame[name] = req.covariates[name]

    predictions = NF.predict(df=frame)
    predictions = predictions.reset_index()
    predictions = predictions[predictions["unique_id"] == req.symbol]
    predictions = predictions.sort_values("ds").tail(int(CFG["h"]))
    if MODEL_COLUMN not in predictions.columns or predictions.empty:
        raise HTTPException(status_code=422, detail="prediction failed")

    horizons = sorted(set(req.horizons))
    yhat: dict[str, float] = {}
    q: dict[str, dict[str, float]] = {}
    for idx, (_, row) in enumerate(predictions.iterrows(), start=1):
        horizon = str(idx)
        mu = float(row.get(MODEL_COLUMN, 0.0))
        yhat[horizon] = mu
        q[horizon] = {
            "p05": mu - 1.645 * SIGMA,
            "p50": mu,
            "p95": mu + 1.645 * SIGMA,
        }

    yhat = {k: yhat[k] for k in yhat if int(k) in horizons}
    q = {k: q[k] for k in q if int(k) in horizons}

    return ForecastResp(symbol=req.symbol, yhat=yhat, q=q, meta={"model": "tft", "ver": CFG.get("version", "unknown")})
