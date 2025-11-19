Kronos Research Stack: Models, Training Regime, and Data Streams
================================================================

Version 1.0 — Research / Prospectus‑Style Overview  
Applies to the PulseTrade `kronos-*` microservices and supporting data pipelines.

> This document describes a **research stack**. It is not an offer, solicitation, or recommendation to buy or sell any security. Historical or simulated performance, if referenced, does **not** guarantee future results.

1. Executive Summary
--------------------

Kronos is the numerical forecasting core of PulseTrade: a set of GPU‑ready model services that transform heterogeneous market, fundamental, and alternative data into calibrated return distributions and portfolio‑ready signals.

At a high level:
- **Data ingestion** streams trades/quotes from crypto and equities venues, fundamentals from Financial Modeling Prep (FMP), and alternative‑data feeds from QuiverQuant, CapitolTrades, and social platforms.
- **Feature engineering** turns this raw history into returns, factor views, sentiment aggregates, liquidity screens, and political/flow metrics.
- **Model services** (N‑BEATS, Temporal Fusion Transformer, graph models, and diffusion scenarios) produce probabilistic forecasts and scenario distributions from these features.
- An **online calibration loop** keeps interval coverage stable as regimes change, without forcing full retrains each day.
- A **strategist layer** ranks ideas with factor‑aware, regime‑sensitive scoring.
- A **policy / allocator layer** converts signals to long/short target weights and routes orders via Alpaca, under strict exposure caps and circuit breakers.
- A **large‑language‑model overlay** (“Finance”) provides rationales and structured allow/deny decisions, so human reviewers and automated policies can interrogate and gate each idea.

The result is a modular stack: data streams, models, calibration, and portfolio logic are all independently deployable, observable services, wired together over Postgres and lightweight HTTP APIs.

2. System Architecture
----------------------

Kronos sits between ingestion and execution:

- **Ingestion services** (`services/ingest`) handle:
  - Real‑time trades/quotes from Coinbase, Kraken, and Alpaca Market Data.
  - Fundamentals, estimates, ratings, profiles, and news from FMP.
  - Social and political flows from Stocktwits, Truth Social, QuiverQuant, and CapitolTrades.
- **Storage** is consolidated in Postgres + TimescaleDB:
  - Tick‑level history in hypertables such as `trades` and `quotes`.
  - Daily returns (`daily_returns`) and liquidity views (`mv_liquidity_60d`).
  - Fundamental payloads and factor views (`fmp_*`, `fundamentals_factors_v1`).
  - Alternative‑data aggregates in `ingest_metrics`.
  - Forecasts, strategist recommendations, fills, positions, and circuit‑breaker state.
- **Model services** of the Kronos family:
  - `kronos-nbeats`: univariate, global N‑BEATS return forecaster with online calibration.
  - `kronos-tft`: multivariate TFT forecaster that conditions on fundamentals and aux covariates.
  - `kronos-graph` / `kronos-graphx`: graph/spatiotemporal forecasters (sector and cross‑asset structure).
  - `kronos-scenarios`: student‑t diffusion sampler for multi‑asset stress scenarios.
- **Forecast orchestration** (`services/forecast`):
  - Pulls price history and covariates from Postgres.
  - Calls Kronos services using `libs.kronos_client`.
  - Enriches each forecast with factor tags, sentiment, Quiver aggregates, and LLM outputs.
  - Persists results into the `forecasts` table for downstream consumers.
- **Strategy, policy, and execution**:
  - Strategist (`services/strategist`) ranks ideas and writes `strategist_recos`.
  - Policy (`services/policy`) transforms recommendations and forecasts into target weights and executable orders, enforcing portfolio‑level constraints.
  - An exit manager and circuit‑breaker system can flatten risk during headline or structural shocks.

3. Data Streams
---------------

3.1 Market Microstructure and Prices
- **Crypto:** Coinbase and Kraken websockets stream trades for pairs such as `BTCUSD` and `ETHUSD`.
- **Equities:** Alpaca Market Data provides consolidated trades/quotes for a configured universe (`ALPACA_SYMBOLS` or `SYMBOLS`).
- **Storage and derivatives:**
  - Raw ticks land in `trades` and `quotes` hypertables; a nightly process computes `daily_returns`.
  - Liquidity statistics (e.g., `avg_dollar_vol_60d`) populate `mv_liquidity_60d` and support universe and risk screens.

3.2 Fundamentals, Estimates, and Ratings
- **Payload tables (`jsonb`):**
  - `fmp_income_statement`, `fmp_balance_sheet`, `fmp_cash_flow`, `fmp_key_metrics`, `fmp_ratios`.
  - These are backfilled via `tools/fmp/backfill_fundamentals.py`.
- **Analyst expectations:**
  - `fmp_analyst_estimates` stores EPS and revenue estimate histories per symbol.
  - `fmp_rating` captures the latest street rating and score.
  - `fmp_revisions_cs` provides a cross‑sectional revisions view (`rev_eps_chg`, `z_rev_eps`) used by both strategist and model covariates.
- **Company profile and borrowability:**
  - `dim_company_profile` merges FMP profiles with Alpaca metadata to expose sectors and shortability flags consumed by the allocator.

3.3 Sentiment, Social, and News
- **Social messages:** Stocktwits, Truth Social, and other feeds write `social_messages`, which are aggregated into `social_features` (message rate, mean sentiment, dispersion, and top handles).
- **News:** FMP news collectors populate `fmp_news`, linking headlines to tickers and timestamps.
- These series are used in:
  - Strategist sentiment factors.
  - LLM prompts that summarise recent headlines and social chatter for each symbol.

3.4 Alternative Data and Political / Flow Metrics
- **Quiver and CapitolTrades collectors** write to `ingest_metrics` with keys such as:
  - `quiver_congress_net_usd`, `quiver_insider_net_usd`, `quiver_house_net_usd`, `quiver_senate_net_usd`.
  - `quiver_lobbying_spend_usd`, `quiver_gov_award_usd`.
  - `quiver_app_rating`, `quiver_app_reviews`, `quiver_etf_weight_pct`.
  - `quiver_political_beta_today`, `quiver_political_beta_bulk`, `quiver_offex_shortvol_ratio`.
- Daily roll‑ups power:
  - LLM bullet‑point summaries of political and insider flows.
  - Optional risk overlays or circuit‑breaker logic when flows cluster around individual names.

3.5 Strategy and Policy State
- **Factor view:** `fundamentals_factors_v1` exposes value/quality/leverage/growth metrics as sector‑neutral z‑scores.
- **Strategist outputs:** `strategist_recos` records buy/sell recommendations, scores, and factor contributions.
- **Policy knobs:** `policy_knobs` encodes regime and threshold settings (e.g., `risk_regime` and minimum expected return in basis points).
- **Execution telemetry:** `fills`, `positions`, and `circuit_breakers` provide an audit trail and control plane for the allocator.

4. Modeling Stack
-----------------

4.1 N‑BEATS Global Return Forecaster (`kronos-nbeats`)
- **Objective:** predict multi‑day ahead log returns for a large equity universe using only numerical price history (no hand‑crafted features).
- **Architecture:**
  - Neural basis expansion model (N‑BEATS) trained in a global fashion across symbols.
  - Input window size defaults to 90 trading days; forecast horizon typically 5 days, with configuration support for horizons such as {1, 5, 20}.
- **Training configuration (implemented in `tools/kronos_data/train_nbeats.py`):**
  - Default dataset: `/mnt/data/kronos_data/processed/nbeats_global_daily.parquet`.
  - Loss: `MQLoss` over quantiles (0.05, 0.5, 0.95), with sCRPS logged as an additional proper scoring rule.
  - Split: per-symbol chronological split with at least ~20 % of observations and ≥365 days held out for validation.
  - Optimisation: AdamW with ReduceLROnPlateau, gradient clipping at 1.0, light dropout and L2 regularisation.
- **Artifacts and serving:**
  - Trained weights, hyperparameters, and metrics are persisted under `/mnt/data/models/kronos-nbeats/<run-id>/` (`state.pt`, `config.json`, `METRICS.json`, `VERSION`, `scaler.pkl`).
  - The service (`services/kronos-nbeats/app.py`) loads the artifact on startup when `MODEL_DIR` is set; otherwise it fits a small “toy” model per request for demo use.
  - At inference, predictions are adjusted by the online calibration state for each symbol.
- **Alpaca-universe enrichment (2025 Q4):**
  - `tools/kronos_data/build_alpaca_training_set.py` merges the Alpaca equity universe (≈3.4k symbols) with ≥420 days of `daily_returns`, FMP social sentiment (`fmp_social_sentiment`), and FMP v4 price targets (`fmp_price_targets_v4`), emitting `/mnt/data/kronos_data/processed/nbeats_alpaca_daily.parquet`. Sentiment/target covariates are forward/back-filled per symbol; remaining gaps are set to `0.0` (interpreted as “neutral/unavailable”).
  - The training script exposes `--feature-cols`, `--residuals-out`, `--min-holdout-days`, `--min-holdout-frac`, and `--devices`. Covariates are logged as “ignored” because the current NeuralForecast build lacks `X_df` support, but the data is preserved; residuals are persisted for the Top-100 “best fit” ranking.
  - `tools/kronos_data/rank_best_fit.py` consumes the residual parquet, computes MAE/directional accuracy for 30/60/90-day windows, and writes a Top-K JSON plus optional `services/ingest/universe_symbols.txt` update.
  - `tools/pipeline/train_nbeats_alpaca.py` orchestrates the entire flow (build dataset → train → evaluate → refresh Top 100) and should be scheduled weekly to keep training and allocator universes aligned. Use `--devices 1` on single-GPU hosts and relax holdouts via CLI flags if the strict per-symbol window is too short.

4.2 Temporal Fusion Transformer (`kronos-tft`)
- **Objective:** extend return forecasts with **covariate‑aware context**, including fundamentals, valuation metrics, and basic news/sentiment counts.
- **Dataset construction (`tools/kronos_tft/build_dataset.py`):**
  - Starts from the same daily returns used by N‑BEATS.
  - Joins factor exports from `fundamentals_factors_v1` and auxiliary features (e.g., news and tweet counts) into `/mnt/data/kronos_data/processed/tft_daily.parquet`.
  - Fills missing covariates with zeroes to preserve a fixed schema.
- **Training (`tools/kronos_tft/train_tft.py`):**
  - Architecture: Temporal Fusion Transformer with configurable input window (`--input-size`), horizon (`--horizon`), hidden width, dropout, and attention dropout.
  - Loss and metrics: same quantile set (0.05, 0.5, 0.95) and metrics as N‑BEATS (MAE, coverage, sCRPS).
  - Split logic mirrors N‑BEATS (time‑ordered, per‑symbol).
  - Artifacts are saved to `/mnt/data/models/kronos-tft/<run-id>/` with `config.json`, `METRICS.json`, and a `latest` symlink.
- **Serving (`services/kronos-tft/app.py`):**
  - Loads the NeuralForecast artifact pointed to by `TFT_MODEL_DIR`.
  - Accepts a history window (`ds`, `y`) plus named covariate series; validates lengths against `config["input_size"]` and expected covariate names.
  - Produces a horizon‑indexed mean and symmetric quantile band around each point, using a configurable residual sigma (`CFG["sigma"]`) when constructing p05/p95.

4.3 Graph, Cross‑Asset, and Scenario Models
- **Graph forecasters (`kronos-graph` / `kronos-graphx`):**
  - Intended to capture sector and cross‑asset structure via PyTorch‑Geometric Temporal and related models (e.g., StemGNN, MTGNN).
  - Consume sector‑normalised returns and graph connectivity to model shock propagation and shared risk factors.
- **Diffusion scenarios (`kronos-scenarios`):**
  - `services/kronos-scenarios/app.py` loads a `StudentDiffusionDecoder` from `libs/kronos_scenarios.student_diffusion`.
  - The `/sample` endpoint returns multi‑asset return paths, percentiles, and expected shortfall estimates for stress testing.
  - Supports optional conditioning on context features (e.g., news count, volatility proxies).
- **Planned state‑space encoders (S4):**
  - Long‑horizon memory for regimes and volatility clustering, feeding regime features into the risk layer and possibly guiding calibration parameters.

5. Training and Evaluation Regime
---------------------------------

Across N‑BEATS and TFT, Kronos follows a consistent training philosophy:

- **Chronological splits:** all datasets are split along the time dimension **per symbol**, preserving order and enforcing a sizable hold‑out window (≥12 months).
- **Unified loss and metrics:**
  - `MQLoss` over (0.05, 0.5, 0.95) quantiles for optimisation.
  - Validation metrics: p50 MAE, p05–p95 empirical coverage, and sCRPS.
  - These metrics are written to `METRICS.json` and baked into `config.json` for post‑hoc analysis.
- **Regularisation and early stopping:**
  - AdamW optimiser with ReduceLROnPlateau (minimum LR on the order of `1e-5`), gradient clipping, and modest dropout/weight decay.
  - Early stopping on validation quantile loss with short patience settings to avoid overfitting noisy tails.
- **Runtime‑aware batching:**
  - Rolling windows are sized so runs complete within a practical budget (tens to hundreds of thousands of gradient steps).
  - For intraday or expanded universes, patience and batch sizes can be tuned without changing the core scripts.
- **Artifact promotion:**
  - Each successful run writes a timestamped artifact directory plus a `latest` symlink.
  - Services use the `latest` pointer by default; promotion is therefore a file‑system operation rather than an image rebuild.

6. Online Calibration and Drift Management
-----------------------------------------

The online calibrator is designed to keep forecast intervals credible as regimes evolve.

- **Inputs and state:**
  - The calibration script (`tools/kronos_data/update_calibration.py`) joins recent Kronos forecasts from `kronos_forecasts` with realised returns from `daily_returns`.
  - For each symbol (and optionally sector and global aggregates) it tracks:
    - Bias (mean residual), variance, interval coverage, and a scaling factor.
    - Observation counts and last‑update timestamps.
- **Update rule (implemented in `libs/calibration/online_calibrator.py`):**
  - Exponentially‑weighted updates with decay `λ≈0.02` (~35‑day half‑life).
  - Outlier rejection when residuals exceed a configurable sigma threshold and when returns are extremely large (e.g., >10 % in a day).
  - Blend of symbol/sector/global states so thinly traded names borrow strength from sector/global baselines until they have sufficient history.
- **Application in serving:**
  - The `kronos-nbeats` service loads `/mnt/data/kronos_state/calibration.parquet` and applies per‑symbol bias/scale to predicted means and p05/p95 bands.
  - Bias is clamped (±30 bps) and scale remains within [0.8, 1.8] to avoid over‑reacting to short bursts of mis‑calibration.
  - Calibration can be disabled via `CALIBRATION_ENABLED=0` if needed.

7. Signal Path: From Forecasts to Portfolio
-------------------------------------------

7.1 Forecast Generation
- The forecast daemon (`services/forecast/forecast/run.py`):
  - Selects a universe from `FORECAST_TICKERS` / `SYMBOLS`.
  - Pulls recent price histories from `trades`, building percentage‑change windows for Kronos.
  - Optionally enriches forecasts with social and factor covariates.
  - Calls `ForecastClient` (libs.kronos_client) against `kronos-nbeats` and other model endpoints.
  - Persists results into `forecasts` with horizon, mean, quantiles, and a structured `features` JSON blob.

7.2 LLM Rationale and Policy Overlay
- **Rationale:**
  - `generate_rationale` in `llm_hooks.py` composes a prompt from signal strength, factor exposure (`_extract_factors`), and recent headlines.
  - The Finance vLLM server returns a short explanation, cached and stored under `features.llm.rationale`.
- **Policy filter:**
  - `policy_filter` encodes liquidity, earnings timing, borrowability, factor dispersion, coverage statistics, macro regime, and Quiver aggregates into `inputs_json`.
  - The response is validated against `POLICY_SCHEMA`, yielding a structured `{allow, reasons, flags}` object recorded as `features.llm.policy`.

7.3 Strategist and Policy
- **Strategist:**
  - The strategist service (`services/strategist`) computes multi‑horizon momentum, merges fundamentals factors, liquidity measures, revisions, ratings, and sentiment into a per‑symbol feature vector.
  - A scoring function blends these signals into a single score, applies regime‑aware sector tilts, and writes the top‑K ideas to `strategist_recos`.
  - A tuner process writes `policy_knobs` entries such as `risk_regime` and target thresholds.
- **Policy / allocator:**
  - The policy service (`services/policy`) reads live positions, account balances, latest forecasts, and strategist outputs.
  - Ideas are wrapped into `Idea` objects and passed to `propose_target_weights_long_short` to generate long/short target weights under per‑name, sector, gross, net, and cash‑floor constraints.
  - Share deltas are constructed, filtered by minimum notional and turnover caps, and routed to Alpaca; fills are logged with metadata (including forecast scores and target weights).
  - LLM allow/deny signals can be enforced via `PT_BLOCK_IF_LLM_DENY`, effectively giving the overlay veto power.

8. Governance, Guardrails, and Limitations
------------------------------------------

- **Data quality:**
  - Ingestion scripts enforce basic sanity checks (monotonic timestamps, missing‑value handling, span requirements).
  - Dedicated validators (`tools/kronos_data/validate_surface.py`, `make data.validate`) and SQL probes (see `docs/data_fmp.md`) catch common failure modes such as half‑filled fundamentals.
- **Risk and circuit breakers:**
  - Circuit‑breaker tables flag per‑symbol, sector, and global halts, which the allocator respects.
  - A dedicated panic‑exit endpoint (`/strategist/panic-exit`) can insert a global breaker and attempt to close all Alpaca positions within a configured TTL.
- **Operational safeguards:**
  - LLM overlay supports shadow mode and a kill‑switch to avoid hard gating on unproven prompts.
  - Alpaca account ID checks guard against accidentally routing orders to the wrong paper/live account.
- **Use‑case limitations:**
  - Models are fit on historical data that may not capture future structural breaks, policy interventions, or novel regimes.
  - Scenario outputs and forecast distributions are **research estimates**, not guarantees.
  - The stack assumes liquid, large‑cap universes by default; running it unchanged on illiquid or thinly traded names may degrade calibration and execution quality.

Kronos is intentionally modular so that each component—data ingestion, model training, calibration, strategist, policy, and LLM overlay—can evolve independently while preserving a transparent, auditable path from raw data to portfolio action.
