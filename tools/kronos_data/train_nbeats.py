from __future__ import annotations

import argparse
import json
import math
import pickle
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from neuralforecast import NeuralForecast
from neuralforecast.losses.pytorch import MQLoss, sCRPS
from neuralforecast.models import NBEATS
from pytorch_lightning.callbacks import EarlyStopping, LearningRateMonitor
import torch
try:
    import torch.distributed as dist
except ImportError:  # pragma: no cover - distributed not always compiled in.
    dist = None
from torch.optim import AdamW
from torch.optim.lr_scheduler import ReduceLROnPlateau

DATA = Path("/mnt/data/kronos_data/processed/nbeats_global_daily.parquet")
OUT_ROOT = Path("/mnt/data/models/kronos-nbeats")
QUANTILES: Tuple[float, ...] = (0.05, 0.5, 0.95)


class MonitoredNBEATS(NBEATS):
    """Override optimizer config to include ReduceLROnPlateau monitor metadata."""

    def configure_optimizers(self):  # type: ignore[override]
        config = super().configure_optimizers()
        if isinstance(config, dict):
            lr_conf = config.get("lr_scheduler")
            if isinstance(lr_conf, dict):
                lr_conf.setdefault("monitor", "train_loss")
                lr_conf.setdefault("reduce_on_plateau", True)
        return config


@dataclass
class SplitResult:
    train: pd.DataFrame
    valid: pd.DataFrame


def _train_valid_split(df: pd.DataFrame, min_holdout_frac: float = 0.2, min_holdout_days: int = 365) -> SplitResult:
    """Time-aware split per symbol with guardrails on minimum holdout size."""
    if df.empty:
        raise ValueError("Input frame is empty – nothing to split.")

    df = df.sort_values(["unique_id", "ds"]).reset_index(drop=True)
    val_indices: List[int] = []

    for _, grp in df.groupby("unique_id", sort=False):
        grp = grp.sort_values("ds")
        if grp.empty:
            continue
        cutoff = grp["ds"].max() - pd.Timedelta(days=min_holdout_days)
        mask_time = grp["ds"] > cutoff
        holdout_idx = grp.index[mask_time].tolist()

        min_hold = max(int(math.ceil(len(grp) * min_holdout_frac)), 1)
        if len(holdout_idx) < min_hold:
            holdout_idx = grp.index[-min_hold:].tolist()

        val_indices.extend(holdout_idx)

    val_indices = sorted(set(val_indices))
    valid = df.loc[val_indices].copy()
    train = df.drop(index=val_indices).copy()

    if train.empty or valid.empty:
        print("[train_nbeats] primary split failed; falling back to global 80/20 split.")
        ordered = df.sort_values(["unique_id", "ds"]).reset_index(drop=True)
        split_idx = max(int(len(ordered) * (1 - min_holdout_frac)), 1)
        split_idx = min(split_idx, len(ordered) - 1)
        train = ordered.iloc[:split_idx].copy()
        valid = ordered.iloc[split_idx:].copy()
        if train.empty or valid.empty:
            raise ValueError("Split produced an empty partition – check data coverage.")

    return SplitResult(train=train, valid=valid)


def _collect_quantile_columns(columns: Iterable[str], quantiles: Sequence[float]) -> Dict[float, str]:
    mapping: Dict[float, str] = {}
    lowered = {col.lower(): col for col in columns}
    for q in quantiles:
        patterns = [
            f"q{q:.2f}".rstrip("0").rstrip("."),
            f"quantile_{q:.2f}".rstrip("0").rstrip("."),
            f"{int(q*100)}",
        ]
        if abs(q - 0.5) < 1e-6:
            patterns.extend(["median"])
        else:
            if q < 0.5:
                coverage = int(round((1 - 2 * q) * 100))
            else:
                coverage = int(round((2 * q - 1) * 100))
            if q < 0.5:
                patterns.extend([f"lo-{coverage}", f"low-{coverage}"])
            else:
                patterns.extend([f"hi-{coverage}", f"high-{coverage}"])
        for lowered_name, original in lowered.items():
            if any(pattern in lowered_name for pattern in patterns):
                mapping[q] = original
                break
    return mapping


def _compute_metrics(preds: pd.DataFrame, valid: pd.DataFrame, quantiles: Sequence[float], horizons: Sequence[int]) -> Dict[str, Optional[float]]:
    metrics: Dict[str, Optional[float]] = {
        "val_mae_p50_h1": None,
        "val_coverage_p05_p95_h1": None,
        "val_scrps": None,
    }
    if preds.empty or valid.empty:
        return metrics

    if "unique_id" not in preds.columns:
        preds = preds.reset_index()

    q_cols = _collect_quantile_columns(preds.columns, quantiles)
    if not q_cols:
        return metrics

    horizon_mask = preds["ds"].isin(valid["ds"])
    aligned = preds.loc[horizon_mask, ["unique_id", "ds"] + list(q_cols.values())].merge(
        valid[["unique_id", "ds", "y"]], on=["unique_id", "ds"], how="inner"
    )
    if aligned.empty:
        return metrics

    q50 = q_cols.get(0.5) or q_cols.get(0.50)
    if q50:
        metrics["val_mae_p50_h1"] = float((aligned[q50] - aligned["y"]).abs().mean())
    if 0.05 in q_cols and 0.95 in q_cols:
        inside = (aligned["y"] >= aligned[q_cols[0.05]]) & (aligned["y"] <= aligned[q_cols[0.95]])
        metrics["val_coverage_p05_p95_h1"] = float(inside.mean())

    try:
        torch_preds = torch.tensor(aligned[[q_cols[q] for q in quantiles]].to_numpy(dtype=np.float32))
        torch_y = torch.tensor(aligned["y"].to_numpy(dtype=np.float32))
        scrps_metric = sCRPS(quantiles=list(quantiles))
        mask = torch.ones_like(torch_y.unsqueeze(0))
        metrics["val_scrps"] = float(
            scrps_metric(y=torch_y.unsqueeze(0), y_hat=torch_preds.unsqueeze(0), mask=mask).item()
        )
    except Exception:
        metrics["val_scrps"] = None

    # window-specific MAE
    max_ds = valid["ds"].max()
    for horizon in horizons:
        cutoff = max_ds - pd.Timedelta(days=horizon)
        mask = aligned["ds"] >= cutoff
        if not mask.any():
            continue
        mae = float((aligned.loc[mask, q50] - aligned.loc[mask, "y"]).abs().mean()) if q50 else None
        metrics[f"val_mae_{horizon}d"] = mae
    return metrics


def _merge_predictions(preds: pd.DataFrame, valid: pd.DataFrame, quantile_col: str) -> pd.DataFrame:
    renamed = preds.rename(columns={quantile_col: "yhat"})
    merged = renamed.merge(valid[["unique_id", "ds", "y"]], on=["unique_id", "ds"], how="inner")
    merged["residual"] = merged["y"] - merged["yhat"]
    merged["abs_error"] = merged["residual"].abs()
    merged["direction_ok"] = np.sign(merged["y"]) == np.sign(merged["yhat"])
    return merged


def _ensure_unique_id_column(df: pd.DataFrame) -> pd.DataFrame:
    """Make sure prediction frames expose the id as a column, regardless of NF defaults."""
    if "unique_id" in df.columns:
        return df
    reset = df.reset_index()
    if "unique_id" not in reset.columns and reset.columns.size > 0:
        first_col = reset.columns[0]
        reset = reset.rename(columns={first_col: "unique_id"})
    return reset


def _is_distributed_nonzero_rank() -> bool:
    return bool(dist) and dist.is_available() and dist.is_initialized() and dist.get_rank() != 0


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Train the Kronos N-BEATS forecaster.")
    parser.add_argument("--data", type=Path, default=DATA, help="Path to Parquet training dataset.")
    parser.add_argument("--out", type=Path, default=OUT_ROOT, help="Directory to write trained artifacts.")
    parser.add_argument("--horizon", type=int, default=5, help="Prediction horizon (timesteps).")
    parser.add_argument("--input-size", type=int, default=90, help="Encoder lookback window size.")
    parser.add_argument("--dropout", type=float, default=0.0, help="(Unsupported) dropout applied to theta layers.")
    parser.add_argument("--weight-decay", type=float, default=1e-4, help="AdamW weight decay.")
    parser.add_argument("--max-epochs", type=int, default=80, help="Maximum trainer epochs.")
    parser.add_argument("--patience", type=int, default=6, help="Early stopping patience (epochs).")
    parser.add_argument("--scheduler-patience", type=int, default=3, help="ReduceLROnPlateau patience.")
    parser.add_argument("--min-lr", type=float, default=1e-5, help="Lower bound for LR scheduler.")
    parser.add_argument("--feature-cols", type=str, default="", help="Comma-separated covariate columns (default: all extras).")
    parser.add_argument("--metrics-horizons", type=str, default="30,60,90", help="Comma-separated holdout windows in days.")
    parser.add_argument("--residuals-out", type=Path, default=None, help="Optional path to store residuals parquet.")
    parser.add_argument("--min-holdout-days", type=int, default=365, help="Minimum days reserved for validation per symbol.")
    parser.add_argument("--min-holdout-frac", type=float, default=0.2, help="Minimum fraction reserved for validation per symbol.")
    parser.add_argument("--devices", type=str, default="1", help="Devices for Lightning trainer (e.g., '1' or 'auto').")
    parser.add_argument(
        "--start-padding",
        choices=["auto", "on", "off"],
        default="auto",
        help="Control start-of-series zero padding when histories are shorter than the input window (default: auto).",
    )
    return parser


def main() -> None:
    args = _build_arg_parser().parse_args()

    if not args.data.exists():
        raise SystemExit(f"Training data not found at {args.data}")

    args.out.mkdir(parents=True, exist_ok=True)

    df = pd.read_parquet(args.data)
    if df.empty:
        raise SystemExit("Training data is empty.")

    df["ds"] = pd.to_datetime(df["ds"])
    base_cols = {"unique_id", "ds", "y"}
    extra_cols = [col for col in df.columns if col not in base_cols]
    feature_cols = [c.strip() for c in args.feature_cols.split(",") if c.strip()] if args.feature_cols else extra_cols
    if feature_cols:
        print("[train_nbeats] Covariate columns specified but will be ignored with current NeuralForecast version.")
    split = _train_valid_split(df, min_holdout_frac=args.min_holdout_frac, min_holdout_days=args.min_holdout_days)
    train_df, valid_df = split.train, split.valid

    train_counts = train_df.groupby("unique_id").size()
    if train_counts.empty:
        raise SystemExit("Training split produced no rows.")
    min_train_len = int(train_counts.min())
    required_window = args.input_size + args.horizon
    start_padding_enabled = args.start_padding == "on"
    if args.start_padding == "auto":
        if min_train_len < required_window:
            start_padding_enabled = True
            print(
                f"[train_nbeats] Shortest training series has {min_train_len} rows (< {required_window}); "
                "enabling start padding to avoid window errors."
            )
    elif args.start_padding == "off" and min_train_len < required_window:
        print(
            f"[train_nbeats] WARNING: start padding disabled but the shortest training series has only "
            f"{min_train_len} rows (< {required_window}). Training may fail."
        )

    callbacks = [
        EarlyStopping(monitor="train_loss", patience=args.patience, min_delta=1e-4, mode="min"),
        LearningRateMonitor(logging_interval="epoch"),
    ]

    quantiles_list = list(QUANTILES)

    dropout = max(args.dropout, 0.0)
    if dropout > 0:
        print("N-BEATS dropout not supported upstream – forcing to 0.0")
        dropout = 0.0

    model = MonitoredNBEATS(
        input_size=args.input_size,
        h=args.horizon,
        loss=MQLoss(quantiles=quantiles_list),
        dropout_prob_theta=dropout,
        start_padding_enabled=start_padding_enabled,
        optimizer=AdamW,
        optimizer_kwargs={"weight_decay": args.weight_decay},
        lr_scheduler=ReduceLROnPlateau,
        lr_scheduler_kwargs={
            "mode": "min",
            "patience": args.scheduler_patience,
            "factor": 0.5,
            "min_lr": args.min_lr,
        },
    )

    default_root = args.out / "_lightning"
    default_root.mkdir(parents=True, exist_ok=True)

    devices_arg = str(args.devices).strip().lower()
    trainer_kwargs = dict(
        max_epochs=args.max_epochs,
        enable_checkpointing=False,
        accelerator="auto",
        devices=args.devices,
        log_every_n_steps=50,
        gradient_clip_val=1.0,
        callbacks=callbacks,
        default_root_dir=str(default_root),
    )
    predict_trainer_kwargs = dict(trainer_kwargs)
    predict_trainer_kwargs.pop("strategy", None)
    predict_trainer_kwargs["devices"] = 1
    if devices_arg == "cpu" or not torch.cuda.is_available():
        predict_trainer_kwargs["accelerator"] = "cpu"
    else:
        predict_trainer_kwargs["accelerator"] = "gpu"
    multi_device_training = False
    try:
        multi_device_training = devices_arg not in {"auto", "cpu"} and int(devices_arg) > 1
    except ValueError:
        multi_device_training = devices_arg not in {"auto", "cpu", "1"}
    if devices_arg == "auto":
        multi_device_training = torch.cuda.device_count() > 1
    if multi_device_training:
        print("[train_nbeats] Using single-device inference to avoid multi-GPU predict issues.")

    model.trainer_kwargs = trainer_kwargs
    nf = NeuralForecast(models=[model], freq="D")
    nf.fit(df=train_df, verbose=False)
    if _is_distributed_nonzero_rank():
        if dist:
            print(f"[train_nbeats] Rank {dist.get_rank()} finished training; skipping post-processing.")
        return
    for fitted in nf.models:
        fitted.trainer_kwargs = predict_trainer_kwargs
    predictions_raw = nf.predict()
    if hasattr(predictions_raw, "to_pandas"):
        predictions_raw = predictions_raw.to_pandas()
    predictions = _ensure_unique_id_column(pd.DataFrame(predictions_raw))
    eval_df = (
        valid_df.groupby("unique_id", group_keys=False)
        .head(args.horizon)
        .reset_index(drop=True)
    )

    artifact_id = f"nbeats_{int(time.time())}"
    artifact_dir = args.out / artifact_id
    artifact_dir.mkdir(parents=True, exist_ok=True)

    try:
        horizons = [int(h.strip()) for h in args.metrics_horizons.split(",") if h.strip()]
    except ValueError:
        horizons = [30, 60, 90]
    metrics = _compute_metrics(predictions, eval_df, QUANTILES, horizons)
    q_cols = _collect_quantile_columns(predictions.columns, QUANTILES)
    q50_col = q_cols.get(0.5) or q_cols.get(0.50)
    residuals_path: Optional[Path] = None
    if q50_col:
        merged = _merge_predictions(predictions[["unique_id", "ds", q50_col]], valid_df, q50_col)
        if not merged.empty:
            chunks: List[pd.DataFrame] = []
            max_ds = merged["ds"].max()
            for horizon in horizons:
                cutoff = max_ds - pd.Timedelta(days=horizon)
                chunk = merged[merged["ds"] >= cutoff].copy()
                chunk["window"] = f"{horizon}d"
                chunks.append(chunk)
            if chunks:
                residuals = pd.concat(chunks, ignore_index=True)
                residuals.sort_values(["unique_id", "ds", "window"], inplace=True)
                residuals_path = args.residuals_out or (artifact_dir / "residuals.parquet")
                residuals_path.parent.mkdir(parents=True, exist_ok=True)
                residuals.to_parquet(residuals_path, index=False)
                metrics["residuals_path"] = str(residuals_path)
    config = {
        "model_name": "nbeats",
        "loss": "MQLoss",
        "quantiles": list(QUANTILES),
        "freq": "D",
        "input_size": args.input_size,
        "h": args.horizon,
        "dropout": dropout,
        "weight_decay": args.weight_decay,
        "start_padding_mode": args.start_padding,
        "start_padding_enabled": start_padding_enabled,
        "required_train_window": required_window,
        "shortest_train_series": min_train_len,
        "max_epochs": args.max_epochs,
        "patience": args.patience,
        "scheduler_patience": args.scheduler_patience,
        "min_lr": args.min_lr,
        "data_training_span": {
            "min": str(train_df["ds"].min()),
            "max": str(train_df["ds"].max()),
        },
        "data_validation_span": {
            "min": str(valid_df["ds"].min()),
            "max": str(valid_df["ds"].max()),
        },
        "metrics": metrics,
        "features": feature_cols,
    }

    with (artifact_dir / "config.json").open("w", encoding="utf-8") as handle:
        json.dump(config, handle, indent=2)

    with (artifact_dir / "METRICS.json").open("w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2)

    state = {
        "model_hparams": {
            "input_size": args.input_size,
            "h": args.horizon,
            "quantiles": quantiles_list,
            "feature_cols": feature_cols,
        },
        "state_dict": nf.models[0].state_dict(),
    }
    torch.save(state, artifact_dir / "state.pt")

    scaler_stub = {"type": "identity_returns_v1"}
    with (artifact_dir / "scaler.pkl").open("wb") as handle:
        pickle.dump(scaler_stub, handle)

    latest_link = args.out / "latest"
    if latest_link.exists() or latest_link.is_symlink():
        latest_link.unlink()
    latest_link.symlink_to(artifact_dir.name)

    with (artifact_dir / "VERSION").open("w", encoding="utf-8") as handle:
        handle.write(artifact_id)

    if residuals_path:
        print("Residuals written to:", residuals_path)
    print("Artifact ready at:", artifact_dir)


if __name__ == "__main__":
    main()
