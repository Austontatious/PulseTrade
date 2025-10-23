from __future__ import annotations

from typing import Dict, List, Optional

import numpy as np
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator

app = FastAPI(title="kronos-graph", version="0.1.0")


class ForecastReq(BaseModel):
    symbol: str
    horizon: List[int] = Field(default_factory=list)
    series: List[float] = Field(default_factory=list)
    features: Optional[Dict] = None

    @field_validator("horizon")
    @classmethod
    def _ensure_horizon(cls, value: List[int]) -> List[int]:
        if not value:
            raise ValueError("horizon must include at least one step")
        return value

    @field_validator("series")
    @classmethod
    def _ensure_series(cls, value: List[float]) -> List[float]:
        if not value:
            raise ValueError("series must include at least one value")
        return value


class ForecastResp(BaseModel):
    symbol: str
    yhat: Dict[str, float]
    q: Dict[str, Dict[str, float]]
    meta: Dict[str, str]


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


@app.post("/forecast", response_model=ForecastResp)
def forecast(req: ForecastReq) -> ForecastResp:
    try:
        series = np.asarray(req.series, dtype=float)
        last_value = float(series[-1])
    except Exception as exc:  # pragma: no cover - defensive guard
        raise HTTPException(status_code=400, detail=f"invalid series data: {exc}") from exc

    yhat: Dict[str, float] = {}
    quantiles: Dict[str, Dict[str, float]] = {}
    for horizon in req.horizon:
        key = str(horizon)
        mu = last_value
        yhat[key] = mu
        quantiles[key] = {"p05": mu - 0.01, "p50": mu, "p95": mu + 0.01}
    return ForecastResp(
        symbol=req.symbol,
        yhat=yhat,
        q=quantiles,
        meta={"model": "graph-stub", "ver": "0.1.0"},
    )

