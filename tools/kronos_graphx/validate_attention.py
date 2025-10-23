import json
import os
import numpy as np
import pandas as pd
import torch

from libs.kronos_graphx.graph_transformer import GraphAttentionModel


DATA = "/mnt/data/kronos_data/processed/nbeats_global_daily.parquet"
MODEL_DIR = "/mnt/data/models/kronos-graphx/latest"
CTX_DAYS = 60


def load_model():
    cfg_path = os.path.join(MODEL_DIR, "config.json")
    state_path = os.path.join(MODEL_DIR, "state.pt")
    if not (os.path.exists(cfg_path) and os.path.exists(state_path)):
        raise FileNotFoundError(f"Missing artifact under {MODEL_DIR}")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    ckpt = torch.load(state_path, map_location="cpu")
    model = GraphAttentionModel(**ckpt["hparams"])
    model.load_state_dict(ckpt["state_dict"])
    model.eval()
    symbols = cfg["symbols"]
    return model, symbols


def spearman(x, y):
    x_ranks = np.argsort(np.argsort(x))
    y_ranks = np.argsort(np.argsort(y))
    x_mean = x_ranks.mean()
    y_mean = y_ranks.mean()
    num = np.sum((x_ranks - x_mean) * (y_ranks - y_mean))
    den = np.sqrt(np.sum((x_ranks - x_mean) ** 2) * np.sum((y_ranks - y_mean) ** 2))
    return float(num / den) if den else 0.0


def eigenvector_centrality(matrix, max_iter=100):
    vec = np.ones(matrix.shape[0], dtype=np.float32) / matrix.shape[0]
    for _ in range(max_iter):
        vec = matrix @ vec
        norm = np.linalg.norm(vec)
        if norm == 0:
            break
        vec /= norm
    return vec


def main():
    model, symbols = load_model()
    df = pd.read_parquet(DATA)
    wide = (
        df.pivot(index="ds", columns="unique_id", values="y")
        .sort_index()
        .reindex(columns=symbols)
        .fillna(0.0)
    )
    window = wide.iloc[-CTX_DAYS:].values.astype(np.float32)
    with torch.no_grad():
        attn = model.context_attention(torch.from_numpy(window).unsqueeze(0))
    A = attn[0].cpu().numpy()
    corr = np.corrcoef(window, rowvar=False)
    corr = np.nan_to_num(corr, nan=0.0)

    attn_mass = A.sum(axis=1)
    corr_centrality = eigenvector_centrality(corr)
    rho = spearman(attn_mass, corr_centrality)

    print(f"Attention mass vs corr centrality Spearman={rho:.3f}")
    if rho <= 0:
        print("WARNING: learned attention not aligned with correlation structure.")
    if rho >= 0.99:
        print("WARNING: attention collapses to correlation network (rho ~ 1).")


if __name__ == "__main__":
    main()
