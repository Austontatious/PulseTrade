from __future__ import annotations
import json
import pickle
import time
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd
import torch
from neuralforecast import NeuralForecast
from neuralforecast.losses.pytorch import QuantileLoss
from neuralforecast.models import NBEATS

DATA = Path("/mnt/data/kronos_data/processed/nbeats_global_daily.parquet")
OUT_ROOT = Path("/mnt/data/models/kronos-nbeats")


def _compute_validation_metrics(
    preds: pd.DataFrame,
    valid_df: pd.DataFrame,
    quantiles: List[float],
) -> Dict[str, Optional[float]]:
    metrics: Dict[str, Optional[float]] = {
        "mae_p50_h1": None,
        "coverage_p05_p95_h1": None,
    }
    if preds.empty or valid_df.empty:
        return metrics

    q_cols: Dict[float, str] = {}
    for q in quantiles:
        patterns = [
            f"q{q:.2f}".rstrip("0").rstrip("."),
            f"q{q:.2f}",
            f"q{q:.3f}",
        ]
        for col in preds.columns:
            lower = col.lower()
            if any(pattern in lower for pattern in patterns):
                q_cols[q] = col
                break
    if not q_cols:
        return metrics

    horizon_mask = preds["ds"].isin(valid_df["ds"])
    aligned = preds.loc[horizon_mask, ["unique_id", "ds"] + list(q_cols.values())].merge(
        valid_df[["unique_id", "ds", "y"]], on=["unique_id", "ds"], how="inner"
    )
    if aligned.empty:
        return metrics

    q50_col = q_cols.get(0.5) or q_cols.get(0.50)
    if q50_col:
        metrics["mae_p50_h1"] = float((aligned[q50_col] - aligned["y"]).abs().mean())
    if 0.05 in q_cols and 0.95 in q_cols:
        inside = (aligned["y"] >= aligned[q_cols[0.05]]) & (aligned["y"] <= aligned[q_cols[0.95]])
        metrics["coverage_p05_p95_h1"] = float(inside.mean())
    return metrics


def main() -> None:
    if not DATA.exists():
        raise SystemExit(f"Training data not found at {DATA}")

    OUT_ROOT.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(DATA)
    df = df.sort_values(["ds", "unique_id"]).reset_index(drop=True)

    if df.empty:
        raise SystemExit("Training data is empty.")

    df["ds"] = pd.to_datetime(df["ds"])

    cut_off = df["ds"].max() - pd.Timedelta(days=365)
    train_df = df[df["ds"] <= cut_off].copy()
    valid_df = df[df["ds"] > cut_off].copy()

    quantiles = [0.05, 0.5, 0.95]
    horizon = 5
    input_size = 90
    model_hparams = dict(
        input_size=input_size,
        h=horizon,
        loss=QuantileLoss(quantiles=quantiles),
    )

    trainer_kwargs = dict(
        max_epochs=50,
        enable_checkpointing=False,
        gradient_clip_val=1.0,
        accelerator="auto",
        devices="auto",
        log_every_n_steps=50,
    )

    model = NBEATS(**model_hparams)
    nf = NeuralForecast(models=[model], freq="D", trainer_kwargs=trainer_kwargs)

    nf.fit(df=train_df, verbose=False)
    predictions = nf.predict(valid_df) if not valid_df.empty else pd.DataFrame()

    artifact_id = f"nbeats_{int(time.time())}"
    artifact_dir = OUT_ROOT / artifact_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    public_hparams = {"input_size": input_size, "h": horizon}
    config = {
        "model_name": "nbeats",
        "model_hparams": public_hparams,
        "loss": "QuantileLoss",
        "quantiles": quantiles,
        "freq": "D",
        "feature_pipe_ver": "returns_v1",
        "data_training_span": {
            "min": str(train_df["ds"].min()),
            "max": str(train_df["ds"].max()),
        },
        "data_validation_span": {
            "min": str(valid_df["ds"].min()) if not valid_df.empty else None,
            "max": str(valid_df["ds"].max()) if not valid_df.empty else None,
        },
    }
    with (artifact_dir / "config.json").open("w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2)

    metrics = _compute_validation_metrics(predictions, valid_df, quantiles)
    metrics_path = artifact_dir / "METRICS.json"
    with metrics_path.open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2)

    state = {"model_hparams": public_hparams, "state_dict": nf.models[0].state_dict(), "quantiles": quantiles}
    torch.save(state, artifact_dir / "state.pt")

    scaler_stub = {"type": "identity_returns_v1"}
    with (artifact_dir / "scaler.pkl").open("wb") as handle:
        pickle.dump(scaler_stub, handle)

    with (artifact_dir / "VERSION").open("w", encoding="utf-8") as handle:
        handle.write(artifact_id)

    print("Artifact ready at:", artifact_dir)


if __name__ == "__main__":
    main()
