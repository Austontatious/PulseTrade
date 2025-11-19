"""Generate next-day N-BEATS predictions for the Top 100 universe."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import timedelta
from pathlib import Path
from typing import Dict, Iterable, List, Sequence

import pandas as pd
import torch
from neuralforecast import NeuralForecast
from neuralforecast.losses.pytorch import MQLoss

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.append(str(REPO_ROOT))

from tools.kronos_data.train_nbeats import (  # noqa: E402
    MonitoredNBEATS,
    _collect_quantile_columns,
    _ensure_unique_id_column,
)

DEFAULT_DATA = Path("/mnt/data/kronos_data/processed/nbeats_alpaca_daily.parquet")
DEFAULT_MODEL_DIR = Path("/mnt/data/models/kronos-nbeats/latest")
DEFAULT_SYMBOLS = Path("services/ingest/universe_symbols.txt")
DEFAULT_OUTPUT = None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Predict next-day closes for the Top 100 universe.")
    parser.add_argument("--data", type=Path, default=DEFAULT_DATA, help="Parquet dataset with training signals.")
    parser.add_argument(
        "--model-dir",
        type=Path,
        default=DEFAULT_MODEL_DIR,
        help="Directory containing N-BEATS artifact (config.json + state.pt).",
    )
    parser.add_argument(
        "--symbols-file",
        type=Path,
        default=DEFAULT_SYMBOLS,
        help="File containing the Top 100 symbols (one per line).",
    )
    parser.add_argument("--as-of", required=True, help="Use history up to and including this date (YYYY-MM-DD).")
    parser.add_argument(
        "--predict-date",
        help="Historical date to backtest (YYYY-MM-DD). Overrides --predict-horizons if provided.",
    )
    parser.add_argument(
        "--predict-horizons",
        type=str,
        default="1",
        help="Comma-separated list of day offsets to export (default: 1).",
    )
    parser.add_argument("--top-k", type=int, default=100, help="Limit loaded symbols to the first K entries.")
    parser.add_argument(
        "--out",
        type=Path,
        default=DEFAULT_OUTPUT,
        help="Destination JSON file. Defaults to reports/top100_predictions_<as_of>.json",
    )
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        help="Run inference on GPU if available (default: CPU).",
    )
    return parser.parse_args()


def load_symbols(path: Path, limit: int | None = None) -> List[str]:
    if not path.exists():
        raise SystemExit(f"Symbols file missing: {path}")
    with path.open("r", encoding="utf-8") as handle:
        symbols = [
            line.strip().upper()
            for line in handle
            if line.strip() and not line.startswith("#")
        ]
    if limit:
        symbols = symbols[:limit]
    if not symbols:
        raise SystemExit("Symbols list is empty.")
    return symbols


def load_model(model_dir: Path, use_gpu: bool) -> Dict:
    config_path = model_dir / "config.json"
    state_path = model_dir / "state.pt"
    if not config_path.exists() or not state_path.exists():
        raise SystemExit(f"Model artifacts missing in {model_dir}")

    with config_path.open("r", encoding="utf-8") as handle:
        config = json.load(handle)
    state = torch.load(state_path, map_location="cpu")
    hparams = state["model_hparams"]
    quantiles = hparams.get("quantiles") or config.get("quantiles", [0.05, 0.5, 0.95])

    model = MonitoredNBEATS(
        input_size=hparams["input_size"],
        h=hparams["h"],
        loss=MQLoss(quantiles=quantiles),
        dropout_prob_theta=config.get("dropout", 0.0),
        start_padding_enabled=config.get("start_padding_enabled", False),
    )
    accelerator = "gpu" if use_gpu and torch.cuda.is_available() else "cpu"
    model.trainer_kwargs = {
        "accelerator": accelerator,
        "devices": 1,
        "enable_checkpointing": False,
        "logger": False,
    }

    nf = NeuralForecast(models=[model], freq=config.get("freq", "D"))
    nf.id_col = "unique_id"
    nf.time_col = "ds"
    nf.target_col = "y"
    nf.models[0].load_state_dict(state["state_dict"])
    nf.models[0].eval()
    nf._fitted = True  # allow predict() with an explicit df
    nf.scalers_ = {}
    return {"nf": nf, "quantiles": tuple(quantiles)}


def prepare_history(df: pd.DataFrame, symbols: Sequence[str], as_of: pd.Timestamp) -> pd.DataFrame:
    history = df[df["ds"] <= as_of].copy()
    if history.empty:
        raise SystemExit(f"No history available on or before {as_of.date()}.")
    history = history[history["unique_id"].isin(symbols)].copy()
    if history.empty:
        raise SystemExit("Filtered history is empty after applying Top 100 symbols.")
    history.sort_values(["unique_id", "ds"], inplace=True)
    return history


def _parse_horizons(raw: str) -> List[int]:
    horizons: List[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            val = int(chunk)
            if val > 0:
                horizons.append(val)
        except ValueError:
            continue
    return sorted(set(horizons)) or [1]


def format_output(
    predictions: pd.DataFrame,
    symbols: Sequence[str],
    quantiles: Iterable[float],
    predict_date: pd.Timestamp,
    actuals: Dict[str, float],
    horizon: int,
) -> List[Dict[str, float]]:
    if predictions.empty:
        return []
    q_cols = _collect_quantile_columns(predictions.columns, quantiles)
    if not q_cols:
        return []

    rows: List[Dict[str, float]] = []
    for symbol in symbols:
        match = predictions[predictions["unique_id"] == symbol]
        if match.empty:
            continue
        record = {
            "unique_id": symbol,
            "ds": predict_date.date().isoformat(),
            "horizon_days": horizon,
        }
        for q, col in q_cols.items():
            record[f"pred_q{q:0.2f}"] = float(match.iloc[0][col])
        actual = actuals.get(symbol)
        if actual is not None:
            record["actual_y"] = float(actual)
            record["error_p50"] = record.get("pred_q0.50", float("nan")) - actual
        rows.append(record)
    return rows


def main() -> None:
    args = parse_args()
    as_of = pd.Timestamp(args.as_of).normalize()
    horizons = _parse_horizons(args.predict_horizons)
    target_map: Dict[str, Dict[str, Any]] = {}
    if args.predict_date:
        target = pd.Timestamp(args.predict_date).normalize()
        horizon = (target - as_of).days
        target_map[target.date().isoformat()] = {"date": target, "horizon": horizon}
    else:
        for horizon in horizons:
            target = as_of + pd.Timedelta(days=horizon)
            target_map[target.date().isoformat()] = {"date": target, "horizon": horizon}

    df = pd.read_parquet(args.data)
    if df.empty:
        raise SystemExit(f"Dataset {args.data} is empty.")
    df["ds"] = pd.to_datetime(df["ds"])

    symbols = load_symbols(args.symbols_file, args.top_k)
    history = prepare_history(df, symbols, as_of)

    model_bundle = load_model(args.model_dir, args.use_gpu)
    nf: NeuralForecast = model_bundle["nf"]
    quantiles = model_bundle["quantiles"]

    preds = nf.predict(df=history)
    preds = _ensure_unique_id_column(pd.DataFrame(preds))
    preds["ds"] = pd.to_datetime(preds["ds"])

    results: Dict[str, List[Dict[str, float]]] = {}
    for label, meta in target_map.items():
        target_df = preds[preds["ds"] == meta["date"]].copy()
        target_df = target_df[target_df["unique_id"].isin(symbols)]
        actual_map = (
            df[df["ds"] == meta["date"]][["unique_id", "y"]]
            .drop_duplicates("unique_id")
            .set_index("unique_id")["y"]
            .to_dict()
        )
        formatted = format_output(target_df, symbols, quantiles, meta["date"], actual_map, meta.get("horizon") or 0)
        if formatted:
            results[label] = formatted

    if not results:
        missing = ", ".join(target_map.keys())
        raise SystemExit(f"No forecasts produced for requested horizons ({missing}).")

    if args.out:
        output_path = Path(args.out)
    else:
        output_path = Path("reports") / f"top100_predictions_{as_of.date().isoformat()}.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "as_of": as_of.date().isoformat(),
        "generated_at": pd.Timestamp.utcnow().isoformat(),
        "symbols": list(symbols),
        "horizons": results,
    }
    output_path.write_text(json.dumps(payload, indent=2))
    total_preds = sum(len(v) for v in results.values())
    print(f"Wrote {total_preds} predictions across {len(results)} horizons -> {output_path}")


if __name__ == "__main__":
    main()
