import json
import os
import time
import numpy as np
import pandas as pd
import torch
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset

from libs.kronos_graphx.graph_transformer import GraphAttentionModel


DATA = "/mnt/data/kronos_data/processed/nbeats_global_daily.parquet"
OUT = "/mnt/data/models/kronos-graphx"


class CrossSectionDataset(Dataset):
    def __init__(self, df, ctx_days=60):
        self.ctx = ctx_days
        syms = sorted(df["unique_id"].unique())[:128]
        self.syms = syms
        wide = (
            df.pivot(index="ds", columns="unique_id", values="y")
            .sort_index()
            .reindex(columns=syms)
            .fillna(0.0)
        )
        self.R = wide.values.astype(np.float32)
        self.dates = wide.index.to_list()

    def __len__(self):
        return max(0, len(self.dates) - (self.ctx + 1))

    def __getitem__(self, i):
        window = self.R[i : i + self.ctx]
        target = self.R[i + self.ctx]
        return torch.from_numpy(window), torch.from_numpy(target)


def train():
    os.makedirs(OUT, exist_ok=True)
    df = pd.read_parquet(DATA)
    cut = df["ds"].max() - pd.Timedelta(days=365)
    train_ds = CrossSectionDataset(df[df["ds"] <= cut])
    valid_ds = CrossSectionDataset(df[df["ds"] > cut])
    dl_tr = DataLoader(train_ds, batch_size=64, shuffle=True, num_workers=0)
    dl_va = DataLoader(valid_ds, batch_size=64, shuffle=False, num_workers=0)

    hparams = dict(symbols=len(train_ds.syms), ctx_dim=16, heads=4, layers=2, ff=128)
    model = GraphAttentionModel(**hparams)
    device = "cuda" if torch.cuda.is_available() else "cpu"
    model.to(device)
    head = nn.Linear(1, 1).to(device)
    opt = torch.optim.AdamW(list(model.parameters()) + list(head.parameters()), lr=3e-4)
    loss_fn = nn.SmoothL1Loss()

    best = (1e9, None)
    for epoch in range(20):
        model.train()
        head.train()
        tr_loss = 0.0
        for window, target in dl_tr:
            window = window.to(device)
            target = target.to(device)
            attn = model.context_attention(window)
            last = window[:, -1, :].unsqueeze(-1)
            feat = torch.bmm(attn, last)
            pred = head(feat).squeeze(-1)
            loss = loss_fn(pred, target)
            opt.zero_grad()
            loss.backward()
            opt.step()
            tr_loss += loss.item() * len(window)
        tr_loss = tr_loss / max(1, len(train_ds))

        model.eval()
        head.eval()
        va_loss = 0.0
        with torch.no_grad():
            for window, target in dl_va:
                window = window.to(device)
                target = target.to(device)
                attn = model.context_attention(window)
                last = window[:, -1, :].unsqueeze(-1)
                feat = torch.bmm(attn, last)
                pred = head(feat).squeeze(-1)
                va_loss += loss_fn(pred, target).item() * len(window)
        va_loss = va_loss / max(1, len(valid_ds))
        print(f"[epoch {epoch}] train {tr_loss:.6f} valid {va_loss:.6f}")

        if va_loss < best[0]:
            best = (va_loss, time.time())
            run_id = f"graphx_{int(time.time())}"
            run_dir = os.path.join(OUT, run_id)
            os.makedirs(run_dir, exist_ok=True)
            torch.save(
                {"hparams": hparams, "state_dict": model.state_dict()},
                os.path.join(run_dir, "state.pt"),
            )
            with open(os.path.join(run_dir, "config.json"), "w", encoding="utf-8") as f:
                json.dump({"version": run_id, "symbols": train_ds.syms}, f, indent=2)
            latest = os.path.join(OUT, "latest")
            if os.path.islink(latest) or os.path.exists(latest):
                os.remove(latest)
            os.symlink(run_id, latest)


if __name__ == "__main__":
    train()
