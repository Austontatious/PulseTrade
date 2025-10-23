import json
import os
import sys
import numpy as np
import pandas as pd
import torch

from libs.kronos_scenarios.student_diffusion import StudentDiffusionDecoder


DATA = "/mnt/data/kronos_data/processed/nbeats_global_daily.parquet"
MODEL_DIR = "/mnt/data/models/kronos-scenarios/latest"
NUM_PATHS = 10_000
CONTEXT_LEN = 60


def load_model():
    cfg_path = os.path.join(MODEL_DIR, "config.json")
    state_path = os.path.join(MODEL_DIR, "state.pt")
    if not (os.path.exists(cfg_path) and os.path.exists(state_path)):
        raise FileNotFoundError(f"Missing artifact under {MODEL_DIR}")
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    ckpt = torch.load(state_path, map_location="cpu")
    model = StudentDiffusionDecoder(**ckpt["hparams"])
    model.load_state_dict(ckpt["state_dict"])
    model.eval()
    return model, cfg


def observed_distribution(symbols, num_symbols):
    df = pd.read_parquet(DATA)
    df = df[df["ds"] >= df["ds"].max() - pd.Timedelta(days=365)]
    wide = df.pivot(index="ds", columns="unique_id", values="y").sort_index()
    if symbols:
        wide = wide.reindex(columns=symbols)
    else:
        take = min(num_symbols, wide.shape[1])
        wide = wide.iloc[:, :take]
    wide = wide.fillna(0.0)
    portfolio = wide.mean(axis=1)
    return portfolio.values.astype(np.float32)


def kurtosis(x: np.ndarray) -> float:
    x = x - x.mean()
    m2 = np.mean(x**2)
    m4 = np.mean(x**4)
    if m2 == 0:
        return -3.0
    return m4 / (m2**2) - 3.0


def main():
    model, cfg = load_model()
    symbols = cfg.get("symbols")
    if isinstance(symbols, int):
        num_symbols = symbols
        sym_names = None
    else:
        sym_names = symbols
        num_symbols = len(sym_names)

    obs = observed_distribution(sym_names, num_symbols)
    if obs.size == 0:
        print("No observed data available for the past year; skipping coverage check.")
        return 0

    with torch.no_grad():
        paths = model.sample(
            num_symbols=num_symbols,
            context_len=CONTEXT_LEN,
            horizon=1,
            num_paths=NUM_PATHS,
            conditioning={},
        )
    simulated = paths.mean(dim=2).view(-1).cpu().numpy()

    obs_p05, obs_p95 = np.percentile(obs, [5, 95])
    sim_p05, sim_p95 = np.percentile(simulated, [5, 95])
    tol = 0.01
    coverage_ok = abs(obs_p05 - sim_p05) < tol and abs(obs_p95 - sim_p95) < tol
    tail_kurt = kurtosis(simulated)

    print(f"Observed p05={obs_p05:.4f} p95={obs_p95:.4f}")
    print(f"Simulated p05={sim_p05:.4f} p95={sim_p95:.4f}")
    print(f"Simulated excess kurtosis={tail_kurt:.3f}")

    if not coverage_ok:
        print("WARNING: Simulated percentiles deviate from observed by more than tolerance.", file=sys.stderr)
    if tail_kurt <= 0:
        print("WARNING: Simulated distribution is not heavy-tailed relative to Gaussian.", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
