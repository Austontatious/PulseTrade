from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List

import numpy as np
import torch
from neuralforecast.models import NBEATS


@dataclass
class LocalNBeatsArtifact:
    path: Path
    version: str
    input_size: int
    horizon: int
    quantiles: List[float]
    model: NBEATS

    def predict(self, series: np.ndarray) -> np.ndarray:
        if series.ndim != 1:
            raise ValueError("series must be a 1-D array of returns/prices")
        if len(series) < self.input_size:
            raise ValueError(f"Need at least {self.input_size} points for inference")
        window = series[-self.input_size :]
        insample = torch.from_numpy(window.astype(np.float32)).unsqueeze(0)
        mask = torch.ones_like(insample)
        batch = {"insample_y": insample, "insample_mask": mask}
        with torch.no_grad():
            output = self.model(batch)
        if isinstance(output, torch.Tensor):
            values = output.detach().cpu().numpy()
        else:
            values = output[0].detach().cpu().numpy()
        values = np.squeeze(values, axis=0)
        if values.ndim == 0:
            values = np.array([float(values)], dtype=np.float32)
        if values.ndim == 1:
            values = values[:, None]
        return values[: self.horizon]


def _load_config(path: Path) -> tuple[int, int, List[float]]:
    config = {}
    config_path = path / "config.json"
    if config_path.exists():
        config = json.loads(config_path.read_text())
    input_size = int(config.get("input_size", 90))
    horizon = int(config.get("h", 5))
    quantiles = config.get("quantiles")
    if not quantiles:
        quantiles = [0.05, 0.5, 0.95]
    quantiles = [float(q) for q in quantiles]
    return input_size, horizon, quantiles


def load_nbeats_artifact(model_dir: str | Path | None = None) -> LocalNBeatsArtifact:
    resolved_dir: Path
    if model_dir:
        resolved_dir = Path(model_dir).expanduser().resolve()
    else:
        env_path = os.getenv("KRONOS_NBEATS_MODEL_DIR")
        if env_path:
            resolved_dir = Path(env_path).expanduser().resolve()
        else:
            resolved_dir = Path("/mnt/data/models/kronos-nbeats/latest").resolve()
    if not resolved_dir.exists():
        raise FileNotFoundError(f"N-BEATS artifact not found at {resolved_dir}")
    input_size, horizon, quantiles = _load_config(resolved_dir)
    state_path = resolved_dir / "state.pt"
    if not state_path.exists():
        raise FileNotFoundError(f"state.pt missing under {resolved_dir}")
    state = torch.load(state_path, map_location="cpu")
    hparams = state.get("model_hparams") or {}
    input_size = int(hparams.get("input_size") or input_size)
    horizon = int(hparams.get("h") or horizon)
    if not quantiles:
        quantiles = state.get("quantiles") or [0.05, 0.5, 0.95]
    quantiles = [float(q) for q in quantiles]
    model = NBEATS(h=horizon, input_size=input_size)
    model.load_state_dict(state["state_dict"])
    model.eval()
    version_file = resolved_dir / "VERSION"
    version = version_file.read_text().strip() if version_file.exists() else resolved_dir.name
    return LocalNBeatsArtifact(
        path=resolved_dir,
        version=version,
        input_size=input_size,
        horizon=horizon,
        quantiles=quantiles,
        model=model,
    )
