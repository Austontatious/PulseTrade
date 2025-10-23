import json
import os
import time
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

from libs.kronos_scenarios.student_diffusion import StudentDiffusionDecoder


DATA = "/mnt/data/kronos_data/processed/nbeats_global_daily.parquet"
AUX = "/mnt/data/kronos_data/interim/aux_features_daily.parquet"
OUT = "/mnt/data/models/kronos-scenarios"


class DailyContextDataset(Dataset):
    def __init__(self, df, aux, horizon=20, ctx_days=60):
        self.h = horizon
        self.ctx = ctx_days
        self.df = df.copy()
        self.aux = aux.copy()
        self.syms = sorted(self.df["unique_id"].unique())[:128]
        wide = (
            self.df.pivot(index="ds", columns="unique_id", values="y")
            .sort_index()
            .reindex(columns=self.syms)
            .fillna(0.0)
        )
        self.R = wide.values
        self.dates = wide.index.to_list()
        auxg = (
            self.aux.groupby("ds")[["news_count", "tweet_count"]]
            .sum()
            .reindex(wide.index)
            .fillna(0)
        )
        self.ctx_feat = np.stack(
            [np.log1p(auxg["news_count"].values), np.log1p(auxg["tweet_count"].values)],
            axis=1,
        )

    def __len__(self):
        return max(0, len(self.dates) - (self.ctx + self.h))

    def __getitem__(self, i):
        ctx_slice = slice(i, i + self.ctx)
        tar_slice = slice(i + self.ctx, i + self.ctx + self.h)
        window = self.R[ctx_slice]
        vol = window.std(axis=0).mean()
        absm = np.abs(window).mean()
        last_aux = self.ctx_feat[i + self.ctx - 1]
        ctx = np.concatenate([[vol, absm], last_aux, np.zeros(64 - 4)]).astype(np.float32)
        target = self.R[tar_slice]
        z = np.random.randn(32).astype(np.float32)
        return torch.from_numpy(z), torch.from_numpy(ctx), torch.from_numpy(target)


def train():
    os.makedirs(OUT, exist_ok=True)
    df = pd.read_parquet(DATA)
    aux = pd.read_parquet(AUX)
    cut = df["ds"].max() - pd.Timedelta(days=365)
    tr_df = df[df["ds"] <= cut]
    va_df = df[df["ds"] > cut]
    ds_tr = DailyContextDataset(tr_df, aux)
    ds_va = DailyContextDataset(va_df, aux)
    dl_tr = DataLoader(ds_tr, batch_size=256, shuffle=True, num_workers=0, pin_memory=True)
    dl_va = DataLoader(ds_va, batch_size=256, shuffle=False, num_workers=0)

    hparams = dict(context_dim=64, hidden=384, layers=4, horizon=20, num_symbols=128)
    model = StudentDiffusionDecoder(**hparams)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    opt = torch.optim.AdamW(model.parameters(), lr=3e-4)
    loss_fn = nn.SmoothL1Loss()

    best = (1e9, None)
    for epoch in range(20):
        model.train()
        tr_loss = 0.0
        for z, ctx, target in dl_tr:
            z, ctx, target = z.to(device), ctx.to(device), target.to(device)
            pred = model(z, ctx)
            loss = loss_fn(pred, target)
            opt.zero_grad()
            loss.backward()
            opt.step()
            tr_loss += loss.item() * len(z)
        tr_loss = tr_loss / max(1, len(ds_tr))

        model.eval()
        va_loss = 0.0
        with torch.no_grad():
            for z, ctx, target in dl_va:
                z, ctx, target = z.to(device), ctx.to(device), target.to(device)
                pred = model(z, ctx)
                va_loss += loss_fn(pred, target).item() * len(z)
        va_loss = va_loss / max(1, len(ds_va))
        print(f"[epoch {epoch}] train {tr_loss:.6f}  valid {va_loss:.6f}")

        if va_loss < best[0]:
            best = (va_loss, time.time())
            run_id = f"scen_{int(time.time())}"
            run_dir = os.path.join(OUT, run_id)
            os.makedirs(run_dir, exist_ok=True)
            torch.save(
                {"hparams": hparams, "state_dict": model.state_dict()},
                os.path.join(run_dir, "state.pt"),
            )
            with open(os.path.join(run_dir, "config.json"), "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "version": run_id,
                        "horizon": hparams["horizon"],
                        "symbols": min(128, len(ds_tr.syms)),
                    },
                    f,
                    indent=2,
                )
            latest = os.path.join(OUT, "latest")
            if os.path.islink(latest) or os.path.exists(latest):
                os.remove(latest)
            os.symlink(run_id, latest)


if __name__ == "__main__":
    train()
