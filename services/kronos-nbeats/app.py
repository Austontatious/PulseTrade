from __future__ import annotations

from typing import Dict, List, Optional

import json
import numpy as np
import os
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field, field_validator
import torch
from pathlib import Path
import pandas as pd

PORT = int(os.getenv("PORT", "8080"))
MODELS_DIR = os.getenv("MODELS_DIR", "/models")

app = FastAPI(title="kronos-nbeats", version="0.1.0")


class ForecastReq(BaseModel):
    symbol: str = Field(..., examples=["AAPL"])
    horizon: List[int] = Field(..., examples=[[1, 5, 20]])
    series: List[float] = Field(..., description="Recent window of the target series.")
    freq: str = "1min"
    features: Optional[Dict] = None

    @field_validator("horizon")
    @classmethod
    def _ensure_horizon(cls, value: List[int]) -> List[int]:
        if not value:
            raise ValueError("horizon must include at least one step")
        if any(h <= 0 for h in value):
            raise ValueError("horizon steps must be positive")
        return value


class ForecastResp(BaseModel):
    symbol: str
    yhat: Dict[str, float]
    q: Dict[str, Dict[str, float]]
    meta: Dict[str, str]


class _LoadedModel:
    def __init__(self, artifact_dir: Path):
        config_path = artifact_dir / "config.json"
        state_path = artifact_dir / "state.pt"
        version_path = artifact_dir / "VERSION"
        if not (config_path.exists() and state_path.exists()):
            raise FileNotFoundError("config.json/state.pt missing from artifact.")

        with config_path.open("r", encoding="utf-8") as handle:
            cfg = json.load(handle)
        state = torch.load(state_path, map_location="cpu")
        hparams = state["model_hparams"]
        self.h = int(hparams["h"])
        self.input_size = int(hparams["input_size"])
        self.quantiles = cfg.get("quantiles") or state.get("quantiles") or [0.05, 0.5, 0.95]
        self.feature_pipe = cfg.get("feature_pipe_ver", "unknown")

        from neuralforecast.models import NBEATS

        self.model = NBEATS(h=self.h, input_size=self.input_size)
        self.model.load_state_dict(state["state_dict"])
        self.model.eval()
        self.version = version_path.read_text().strip() if version_path.exists() else "unknown"

    def predict(self, series: np.ndarray) -> np.ndarray:
        window = series[-self.input_size :]
        insample = torch.from_numpy(window.astype(np.float32)).unsqueeze(0)
        mask = torch.ones_like(insample)
        windows_batch = {"insample_y": insample, "insample_mask": mask}
        with torch.no_grad():
            out = self.model(windows_batch)
        if isinstance(out, torch.Tensor):
            values = out.detach().cpu().numpy()
        else:
            values = out[0].detach().cpu().numpy()
        values = np.squeeze(values, axis=0)
        if values.ndim == 1:
            values = values[:, None]
        return values[: self.h]


def _load_artifact() -> Optional[_LoadedModel]:
    model_dir = os.getenv("MODEL_DIR")
    if not model_dir:
        return None
    path = Path(model_dir)
    if not path.exists():
        return None
    try:
        return _LoadedModel(path)
    except Exception as exc:  # pragma: no cover - defensive
        print(f"Artifact load failed: {exc}")
        return None


MODEL_ARTIFACT = _load_artifact()

CALIB_FILE = os.getenv("CALIBRATION_FILE", "/state/calibration.parquet")
CALIB_ENABLED = os.getenv("CALIBRATION_ENABLED", "1") == "1"
_calib_df: Optional[pd.DataFrame] = None
_calib_mtime = 0.0


def _load_calibration() -> None:
    global _calib_df, _calib_mtime
    if not CALIB_ENABLED:
        return
    try:
        mtime = os.path.getmtime(CALIB_FILE)
    except FileNotFoundError:
        _calib_df = None
        _calib_mtime = 0.0
        return
    if mtime > _calib_mtime:
        _calib_df = pd.read_parquet(CALIB_FILE)
        _calib_mtime = mtime


def _get_calibration(symbol: str) -> tuple[float, float]:
    if not CALIB_ENABLED or _calib_df is None:
        return 0.0, 1.0
    row = _calib_df.query("level == 'symbol' and key == @symbol")
    if row.empty:
        global_row = _calib_df[_calib_df["level"] == "global"]
        if global_row.empty:
            return 0.0, 1.0
        global_state = global_row.iloc[0]
        return float(global_state["bias"]), float(global_state["scale"])
    state = row.iloc[0]
    return float(state["bias"]), float(state["scale"])


@app.get("/health")
def health() -> Dict[str, bool]:
    return {"ok": True}


def _lazy_model_fit(y: np.ndarray, horizon_steps: List[int]):
    """Fit an N-BEATS model on the fly for demo purposes."""
    # Lazy import keeps container boot quick; replaced with artifact load later.
    from neuralforecast import NeuralForecast
    from neuralforecast.losses.pytorch import QuantileLoss
    from neuralforecast.models import NBEATS
    import pandas as pd

    df = pd.DataFrame(
        {
            "unique_id": "series",
            "ds": pd.RangeIndex(start=0, stop=len(y), step=1),
            "y": y.astype(float),
        }
    )

    horizon_size = max(horizon_steps)
    lookback = min(64, max(8, len(y) // 2))
    model = NBEATS(
        input_size=lookback,
        h=horizon_size,
        loss=QuantileLoss(),
        valid_loss=QuantileLoss(),
    )
    nf = NeuralForecast(models=[model], freq="H")
    nf.fit(df=df, verbose=False)

    forecast_df = nf.predict().reset_index(drop=True)
    yhat: Dict[str, float] = {}
    quantiles: Dict[str, Dict[str, float]] = {}
    for step in horizon_steps:
        idx = step - 1
        mu = float(forecast_df["y"].iloc[idx])
        p05 = float(forecast_df.get("y_q05", forecast_df["y"]).iloc[idx])
        p50 = float(forecast_df.get("y_q50", forecast_df["y"]).iloc[idx])
        p95 = float(forecast_df.get("y_q95", forecast_df["y"]).iloc[idx])
        key = str(step)
        yhat[key] = mu
        quantiles[key] = {"p05": p05, "p50": p50, "p95": p95}
    return yhat, quantiles


@app.post("/forecast", response_model=ForecastResp)
def forecast(req: ForecastReq) -> ForecastResp:
    if len(req.series) < 32:
        raise HTTPException(status_code=400, detail="Need at least 32 points in 'series'.")

    _load_calibration()

    series = np.asarray(req.series, dtype=float)
    if MODEL_ARTIFACT:
        if len(series) < MODEL_ARTIFACT.input_size:
            raise HTTPException(
                status_code=400,
                detail=f"Need at least {MODEL_ARTIFACT.input_size} points for pretrained model context.",
            )
        predictions = MODEL_ARTIFACT.predict(series)
        max_h = MODEL_ARTIFACT.h
        if any(h > max_h for h in req.horizon):
            raise HTTPException(
                status_code=400,
                detail=f"Horizon exceeds model capacity (max {max_h}).",
            )
        yhat: Dict[str, float] = {}
        quantiles: Dict[str, Dict[str, float]] = {}
        quantile_index = {}
        for idx, q in enumerate(MODEL_ARTIFACT.quantiles):
            quantile_index[round(float(q), 4)] = idx
        p50_idx = None
        for candidate in (0.5, 0.50, 0.4999, 0.5001):
            rounded = round(candidate, 4)
            if rounded in quantile_index:
                p50_idx = quantile_index[rounded]
                break
        for h in req.horizon:
            horizon_idx = h - 1
            if horizon_idx >= predictions.shape[0]:
                raise HTTPException(status_code=400, detail=f"Horizon {h} exceeds model output.")
            row = predictions[horizon_idx]
            value = float(row[p50_idx]) if p50_idx is not None else float(row[-1])
            key = str(h)
            bias, scale = _get_calibration(req.symbol)
            mu = value + bias
            quantile_values = {}
            for q_val, idx in quantile_index.items():
                label = f"p{int(q_val * 100):02d}"
                quantile_values[label] = float(row[idx])
            if not quantile_values:
                quantile_values = {"p50": value}
            if "p50" in quantile_values:
                center = quantile_values["p50"] + bias
            else:
                center = mu
            low = quantile_values.get("p05", center) + bias
            high = quantile_values.get("p95", center) + bias
            width_low = center - low
            width_high = high - center
            low_adj = center - width_low * scale
            high_adj = center + width_high * scale
            low_adj, high_adj = min(low_adj, high_adj), max(low_adj, high_adj)
            quantile_values["p05"] = low_adj
            quantile_values["p50"] = center
            quantile_values["p95"] = high_adj
            yhat[key] = mu
            quantiles[key] = quantile_values
        return ForecastResp(
            symbol=req.symbol,
            yhat=yhat,
            q=quantiles,
            meta={
                "model": "nbeats-pretrained",
                "ver": MODEL_ARTIFACT.version,
                "models_dir": MODELS_DIR,
                "feature_pipe": MODEL_ARTIFACT.feature_pipe,
                "quantiles": ",".join(str(q) for q in MODEL_ARTIFACT.quantiles),
                "input_size": str(MODEL_ARTIFACT.input_size),
                "calib": "ewma_v1" if CALIB_ENABLED else "off",
            },
        )

    try:
        mean, quantiles = _lazy_model_fit(series, req.horizon)
    except Exception as exc:  # pragma: no cover - defensive guard
        detail = f"forecast_error: {type(exc).__name__}: {exc}"
        raise HTTPException(status_code=500, detail=detail) from exc

    return ForecastResp(
        symbol=req.symbol,
        yhat=mean,
        q=quantiles,
        meta={
            "model": "nbeats-toy",
            "ver": "0.1.0",
            "models_dir": MODELS_DIR,
            "feature_pipe": "returns_v1",
            "quantiles": "0.05,0.5,0.95",
        },
    )
