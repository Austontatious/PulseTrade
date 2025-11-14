# Kronos Forecast Stack

This document captures the Kronos microservice additions: `kronos-nbeats` for NeuralForecast powered time-series models and `kronos-graph` as the graph-model placeholder. Both containers mount `/mnt/data/models` read-only for shared checkpoints and expose a common HTTP contract of `/forecast` for multi-horizon mean and quantile responses. PulseTrade calls `kronos-nbeats` via the lightweight client in `libs/kronos_client` and records the returned bundle inside forecast features for downstream consumers.

Use the commands below (or simply run `make models.fetch`) to mirror a curated collection of upstream repositories under `/mnt/data/models`. Each block reflects the canonical or most widely referenced implementation for the corresponding model family so we always have documentation, examples, and baselines at hand.

## State-Space Models (S4)

- Repo: https://github.com/state-spaces/s4

```bash
mkdir -p /mnt/data/models && cd /mnt/data/models
git clone https://github.com/state-spaces/s4.git s4 || true
```

## N-BEATS / N-HiTS

- ServiceNow official N-BEATS: https://github.com/ServiceNow/N-BEATS
- philipperemy N-BEATS implementation: https://github.com/philipperemy/n-beats
- N-HiTS author code: https://github.com/cchallu/n-hits

```bash
mkdir -p /mnt/data/models && cd /mnt/data/models
mkdir -p nbeats nhits
git clone https://github.com/ServiceNow/N-BEATS.git nbeats/servicenow || true
git clone https://github.com/philipperemy/n-beats.git nbeats/philipperemy || true
git clone https://github.com/cchallu/n-hits.git nhits || true
```

## Temporal Fusion Transformer (TFT)

- PyTorch Forecasting (canonical TFT implementation): https://github.com/sktime/pytorch-forecasting

```bash
mkdir -p /mnt/data/models/tft && cd /mnt/data/models/tft
git clone https://github.com/sktime/pytorch-forecasting.git pytorch-forecasting || true
```

Docs: https://pytorch-forecasting.readthedocs.io/en/latest/

## NeuralForecast Suite

- Nixtla NeuralForecast (N-BEATS, N-HiTS, TFT, PatchTST, iTransformer, etc.): https://github.com/Nixtla/neuralforecast

```bash
mkdir -p /mnt/data/models && cd /mnt/data/models
git clone https://github.com/Nixtla/neuralforecast.git neuralforecast || true
```

## Graph / Cross-Asset Models

- PyTorch Geometric Temporal: https://github.com/benedekrozemberczki/pytorch_geometric_temporal
- StemGNN: https://github.com/microsoft/StemGNN

```bash
mkdir -p /mnt/data/models/gnn && cd /mnt/data/models/gnn
git clone https://github.com/benedekrozemberczki/pytorch_geometric_temporal.git pytorch_geometric_temporal || true
git clone https://github.com/microsoft/StemGNN.git stemgnn || true
```

## Generative / Diffusion (Scenario Stress Testing)

- GluonTS (TimeGrad, probabilistic baselines): https://github.com/awslabs/gluonts
- CSDI (score-based imputation/diffusion): https://github.com/ermongroup/CSDI
- PyTorch-TS (TimeGrad PyTorch port): https://github.com/zalandoresearch/pytorch-ts

```bash
mkdir -p /mnt/data/models/diffusion && cd /mnt/data/models/diffusion
git clone https://github.com/awslabs/gluonts.git gluonts || true
git clone https://github.com/ermongroup/CSDI.git csdi || true
git clone https://github.com/zalandoresearch/pytorch-ts.git pytorch-ts || true
```

## Python Package Quickstart

Install the following wheels in whichever environment you run experiments or services:

```bash
pip install -U torch torchvision torchaudio torchmetrics
pip install -U pytorch-lightning pytorch-forecasting neuralforecast
pip install -U torch-geometric torch-geometric-temporal
pip install -U gluonts numpyro einops tqdm
```

PyTorch Geometric wheels are CUDA-specific; if `pip install torch-geometric` fails, follow the install selector at https://github.com/pyg-team/pytorch_geometric.

## Operational Flow

1. `make models.fetch` – clone/refresh all upstream repos under `/mnt/data/models`.
2. `make build` – build Kronos and PulseTrade service images.
3. `make up` – launch the full stack; `make logs` tails Kronos microservices.
4. Smoke tests: `curl localhost:8080/health` and `curl localhost:8081/health`.
5. Optional: `pytest -q` to run local integration smoke tests (requires `requests` and the services up).

## Future Work

- Replace the on-demand N-BEATS fit with pre-trained artifacts loaded from `/mnt/data/models`.
- Integrate S4 encoders and graph models once checkpoints are ready.
- Surface service metrics via `/metrics` and wire quantile outputs into risk fail-safes.

## Data & Training Workflow

1. `make data.fetch` – pulls FNSPID prices/news and financial-tweets from HuggingFace into `/mnt/data/kronos_data/raw`, writing project-local parquet snapshots.
2. `make data.compile` – generates `processed/nbeats_global_daily.parquet` (daily log-returns) and supporting aux feature parquet; QA metrics land in `logs/qa_compile.json`.
3. `make data.validate` – quick poison checks; inspect `/mnt/data/kronos_data/logs/validate_surface.txt` for warnings.
4. `make train.nbeats` – trains a global daily N-BEATS using the compiled dataset and writes `state.pt`, `config.json`, `scaler.pkl`, and `VERSION` under `/mnt/data/models/kronos-nbeats/<run-id>`.
5. Point `KRONOS_NBEATS_MODEL_DIR` in `.env` to the latest `<run-id>` and restart `kronos-nbeats` (`make up`) to serve the pretrained artifact without per-request fitting.

## Runtime Watchdog (Recommended)

- `docker-compose.yml` defines `restart: unless-stopped` for `kronos-nbeats` and `kronos-graph`, so Docker will automatically respawn the containers if they crash.
- For additional coverage, use `scripts/watchdog/kronos_watchdog.sh` on the host. It checks container health and restarts unhealthy services. Example cron entry (adjust paths if needed):

  ```cron
  */2 * * * * /bin/bash /mnt/data/PulseTrade/scripts/watchdog/kronos_watchdog.sh /mnt/data/PulseTrade >> /var/log/kronos_watchdog.log 2>&1
  ```

  The script only depends on `docker` and logs health status plus restart attempts.

## Online Calibration Loop

- Daily job (`tools/kronos_data/update_calibration.py`) pulls forecasts vs. realized returns, updates EWMA bias/interval states, and writes `/mnt/data/kronos_state/calibration.parquet`.
- Service reads the file (via `/state` mount) and applies a light bias + interval scaling per symbol. Disable by setting `KRONOS_CALIBRATION_ENABLED=0`.
- Suggested scheduler (cron): `0 18 * * 1-5 PYTHONPATH=/mnt/data/PulseTrade DATABASE_URL=<...> python3 /mnt/data/PulseTrade/tools/kronos_data/update_calibration.py`.

## Strategy Evaluation Scorecard

- Nightly job (`tools/kronos_eval/daily_strategy_eval.py`) joins Planner/allocator fills with Kronos forecasts, strategist recommendations, `daily_returns`, SPY/sector ETF baselines, and the current `risk_regime` knob to answer “does the *strategy* have edge?” instead of just “does the model look calibrated?”.
- Outputs:
  - Per-trade detail parquet/CSV under `reports/` with realized r\_hold/r\_1d/r\_3d, edge vs SPY and sector, PT\_SCORE bucket, regime, and forecast hit-rate flags.
  - Bucketed summary CSV (`reports/kronos_strategy_eval_YYYY-MM-DD.csv`) with win rate, mean/median return, edge vs SPY/sector, and hit-rates by signal strength, PT score deciles, regime, and side.
  - Markdown summary (`reports/kronos_strategy_eval_YYYY-MM-DD.md`) suitable for daily check-ins.
- Recommended host-side wrapper: `scripts/nightly_strategy_eval.sh` (runs the job inside the `tools` container and logs to `/mnt/data/logs/strategy_eval/`).
- Example cron entry (adjust time as needed, typically after the nightly tuner/calibration jobs):

  ```cron
  30 2 * * * /mnt/data/PulseTrade/scripts/nightly_strategy_eval.sh >> /mnt/data/logs/strategy_eval/cron.log 2>&1
  ```
