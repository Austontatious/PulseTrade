import json
import os
import time

import pandas as pd
from neuralforecast import NeuralForecast
from neuralforecast.losses.pytorch import MAE
from neuralforecast.models import TFT

DATA = "/mnt/data/kronos_data/processed/tft_daily.parquet"
OUT = "/mnt/data/models/kronos-tft"


def main() -> None:
    df = pd.read_parquet(DATA)
    df["ds"] = pd.to_datetime(df["ds"])
    df = df.sort_values(["unique_id", "ds"]).reset_index(drop=True)

    cut = df["ds"].max() - pd.Timedelta(days=365)
    train_df = df[df["ds"] <= cut].copy()
    valid_df = df[df["ds"] > cut].copy()

    symbols_subset = train_df["unique_id"].unique()[:200]
    train_df = train_df[train_df["unique_id"].isin(symbols_subset)]
    valid_df = valid_df[valid_df["unique_id"].isin(symbols_subset)]
    train_df = train_df.groupby("unique_id", group_keys=False).tail(200)
    valid_df = valid_df.groupby("unique_id", group_keys=False).tail(50)

    covariates = [c for c in df.columns if c not in ("unique_id", "ds", "y")]

    horizons = 3
    input_size = 60

    model = TFT(
        input_size=input_size,
        h=horizons,
        hidden_size=256,
        attn_dropout=0.1,
        lr=3e-4,
        scaler_type="identity",
        loss=MAE(),
        hist_exog_list=covariates,
        futr_exog_list=[],
        stat_exog_list=[],
    )
    model.trainer_kwargs = dict(
        max_epochs=1,
        accelerator="auto",
        enable_checkpointing=False,
        gradient_clip_val=1.0,
        log_every_n_steps=50,
    )

    nf = NeuralForecast(models=[model], freq="D")
    nf.fit(df=train_df, verbose=False)

    sigma = 0.02
    if not valid_df.empty:
        pred = nf.predict(df=valid_df)
        pred = pred.reset_index()
        joined = valid_df.merge(pred, on=["unique_id", "ds"], how="inner")
        if "TFT" in joined:
            residuals = joined["y"] - joined["TFT"]
            if residuals.dropna().size:
                sigma = float(residuals.dropna().std())

    run_id = f"tft_{int(time.time())}"
    art = os.path.join(OUT, run_id)
    os.makedirs(art, exist_ok=True)

    nf.save(art, overwrite=True)

    with open(os.path.join(art, "config.json"), "w", encoding="utf-8") as f:
        json.dump(
            {
                "version": run_id,
                "freq": "D",
                "h": horizons,
                "input_size": input_size,
                "covariates": covariates,
                "sigma": sigma,
            },
            f,
            indent=2,
        )

    latest = os.path.join(OUT, "latest")
    if os.path.islink(latest) or os.path.exists(latest):
        os.remove(latest)
    os.symlink(os.path.basename(art), latest)
    print("Artifact:", art)


if __name__ == "__main__":
    main()
