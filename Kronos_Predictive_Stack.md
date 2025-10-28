Kronos Predictive Stack: Numerical Modeling and Data Architecture
===============================================================

Prepared by Auston Horras
Technical Whitepaper v1.1 — Kronos Predictive Stack (PulseTrade)

1. System Overview
------------------

Kronos (inside PulseTrade) is a modular forecasting and risk engine that extracts predictive structure from heterogeneous financial data (prices, fundamentals, analyst estimates/ratings, news, and social signals). It runs as containerized model services, each trained on different temporal or structural representations, and routes outputs into a common risk/decision layer.

Current components
- N‑BEATS (Neural Basis Expansion Analysis for Time Series)
  - Purpose: medium‑horizon equity return forecasting (1–5 days)
  - Impl: `services/kronos-nbeats` FastAPI container (GPU‑ready) serving quantile forecasts.
- Graph/Spatiotemporal Models (StemGNN / PyG‑Temporal)
  - Purpose: cross‑asset dependency and sector propagation modeling
  - Impl: `services/kronos-graph` (scaffolded), `services/kronos-graphx` (artifact runner)
- S4 / State‑Space Encoder (planned)
  - Purpose: long‑context memory for regime & volatility clustering
- Online Calibrator
  - Purpose: post‑fit bias/interval correction using EWMA of forecast residuals
  - Impl: integrated into `kronos-nbeats` via bias/scale application at inference; artifacts in `/state`
- Fail‑Safe Controller
  - Purpose: detect statistically unexplainable anomalies and trip circuit breakers
  - Impl: risk layer in PulseTrade policy; includes symbol/sector/global breakers and headline‑risk breaker

2. Data Architecture and Training Sources
----------------------------------------

2.1 Core Market Data
- Live trades/quotes via Alpaca IEX, plus crypto (Coinbase/Kraken) for broader signal context
  - Stored in Postgres hypertables: `trades`, `quotes` (TimescaleDB)
- Derived daily returns for cross‑sectional analysis and liquidity stats
  - `daily_returns` (external loader) and `mv_liquidity_60d` (materialized view)

2.2 Fundamentals, Estimates, Ratings (FMP Starter Plan)
- Payload tables (jsonb, denormalized):
  - `fmp_income_statement`, `fmp_balance_sheet`, `fmp_cash_flow`, `fmp_key_metrics`, `fmp_ratios`
  - Backfill tool: `tools/fmp/backfill_fundamentals.py`
- Analyst Estimates & Ratings:
  - `fmp_analyst_estimates` (new) and `fmp_rating` (new) via async collectors
  - Cross‑sectional revisions view: `fmp_revisions_cs` (new) with `rev_eps_chg` and `z_rev_eps`
- Company profile enrichment:
  - `dim_company_profile` upserted from FMP `profile/{symbol}`

2.3 Textual / Sentiment & News
- Social features: `social_features` (rate/sentiment aggregates)
- FMP News: `fmp_news` (new) ingested periodically; triggers headline‑risk breakers on spikes

2.4 Strategist / Policy Tables
- `fundamentals_factors_v1` view (value/quality/liquidity factors with sector z‑scores)
- `strategist_recos` (ranked buy/sell recommendations with factor contributions)
- `policy_knobs` (dynamic policy tuning and regime switch)
- `fills`, `positions`, `circuit_breakers` for execution telemetry and controls

3. N‑BEATS: Global Forecasting Model
------------------------------------

Rationale
- Learns basis expansions capturing local/global seasonalities without heavy feature engineering; scales across thousands of equities using pure numerical sequences.

Training configuration (reference)
- Input window: 90 days rolling
- Forecast horizon: 5 days
- Loss: Quantile Loss (p05/p50/p95) for uncertainty
- Optimizer/Framework: PyTorch‑Lightning (Adam, early‑stopped, GPU)
- Data: 1999–2023 adjusted daily returns (global model)
- Validation: last 365 days (MAE and coverage tracked)

Artifact layout (example)
```
/mnt/data/models/kronos-nbeats/<run-id>/
 ├─ state.pt         # torch weights
 ├─ config.json      # hyperparameters & feature version
 ├─ VERSION          # run identifier
 └─ METRICS.json     # validation MAE & coverage
```

Service operation
- Container: `services/kronos-nbeats` (FastAPI on 8080)
- Loads pretrained artifact at startup (if present) and applies online calibration bias/scale if enabled
- Fallback: on‑the‑fly lightweight N‑BEATS fit for demo/testing when artifact is absent
- Fixed: SciPy dependency added to image (prevents artifact import failures)

4. Graph and State‑Space Extensions
-----------------------------------

- Graph Layer (StemGNN / MTGNN / PyG‑Temporal)
  - Inputs: sector‑normalized returns and inter‑asset graph
  - Goal: model propagation of shocks and latent sector coupling
- S4 / State‑Space Sequence Model (planned)
  - Long horizon memory for regimes and volatility clustering; outputs feed risk layer as regime features

5. Online Calibration Loop
--------------------------

Objective
- Prevent slow bias drift and maintain realistic predictive intervals without daily retraining.

Mechanism
- Residual EWMA by symbol with sector/global backoffs
- Nightly updates (λ ≈ 0.02 ≈ 35‑day half‑life)
- Guards: skip on |return|>10% or |residual|>4σ; clamp bias ±0.3%; scale ∈ [0.8, 1.8]
- Shrinkage: new symbols borrow sector/global until ≥20 samples

Application
- At inference: `ŷ' = ŷ + b_i`; `[p05', p95'] = p50 ± (p50 − p{05,95}) × s_i`

6. Strategist: Factor‑Aware Ranking & Regime Control
---------------------------------------------------

Features (latest)
- Momentum: short (≈1h configurable) and medium horizons from `trades`
- Quality/Value: `z_roe`, `z_roa`, `z_gm`, `z_dta`, `z_rev` from `fundamentals_factors_v1`
- Liquidity/Size: `avg_dollar_vol_60d`, `market_cap` filters
- Revisions (new): `z_rev_eps` from `fmp_revisions_cs` (fallback to local Δ of estimates)
- Ratings (new): normalized `ratingScore` from `fmp_rating`
- Sentiment: recent `senti_mean` from `social_features`

Scoring
- Blend: 0.5·mom_s + 0.2·mom_m + 0.15·quality + 0.10·sentiment + 0.06·revisions + 0.04·ratings
- Sector/regime tilts: adjusts score for Utilities/REITs in risk‑on and Tech/Industrials in risk‑off

Universe/filters (env‑tunable)
- Market cap ≥ $10–15B, avg dollar vol ≥ $10–15M/day
- Optional turnaround filter: require positive momentum and revisions simultaneously

Regime switch (tuner)
- Computes breadth (share of large caps with positive 60‑day momentum) and median `z_rev`
- Writes `policy_knobs`: `risk_regime` on/off and `threshold_bps` (e.g., 10 vs 20)

Outputs
- Top‑K recommendations written to `strategist_recos` with factor contributions for audit

6.1 Model Retraining Workflow
-----------------------------

Weekly retrains (the “Wednesday night” job) now run through dedicated CLI entrypoints that rebuild the forecasters on the latest Quiver/FMP ingest:

- **Data split:** per-symbol, strictly time-ordered, with the most recent ≥12 months (or ≥20 % of samples) held out as validation; no look-ahead leakage.
- **Objectives & metrics:** both N-BEATS and TFT optimise `MQLoss` across the (p05, p50, p95) quantiles and log p50 MAE, p05–p95 coverage, and CRPS on the forecast horizon. After training, we continue to rely on the online bias/scale calibrator.
- **Regularisation:** AdamW + ReduceLROnPlateau (min LR `1e-5`), gradient clipping at 1.0, early stopping on the validation quantile loss (`patience` defaults 8 for N-BEATS, 2 for TFT), light dropout (0–0.1) and weight decay (≤1e-4).
- **Batching:** rolling window datasets target roughly 50k–150k optimisation steps. If you expand to intraday data, bump patience by ~2–4 epochs to damp noise.
- **Commands:**  
  - `python3 tools/kronos_data/train_nbeats.py --max-epochs 60 --patience 8 --scheduler-patience 4`  
  - `python3 tools/kronos_tft/train_tft.py --max-epochs 3 --patience 2 --scheduler-patience 1` (defaults to top 200 symbols × latest 400 observations; override `--max-symbols/--max-tail` for full runs).
- **Artifacts:** each run produces `/mnt/data/models/kronos-*/<timestamp>/state.pt`, `config.json`, `METRICS.json`, `scaler.pkl`, plus a refreshed `latest` symlink that the services consume.

7. Policy: Portfolio Allocator & Execution Guardrails
----------------------------------------------------

Allocator (`libs/portfolio`)
- Inputs: latest Kronos forecast/LLM outputs, Alpaca positions + account NAV/cash, sector tags (`dim_company_profile`), live prices.
- Weighting: softmax on positive scores (`PT_SCORE_EXP`), budgeted to (`PT_GROSS_MAX_PCT` – `PT_CASH_FLOOR_PCT`).
- Caps: per-name (`PT_MAX_POS_PCT`), per-sector (`PT_SECTOR_MAX_PCT`), turnover (`PT_TURNOVER_CAP_PCT`).
- Replacement: low-utility holdings trimmed first when cash floor (`PT_CASH_FLOOR_PCT`) would be breached; churn penalty `PT_REPLACEMENT_FRIC_BPS`.
- Universe: only LLM-allowed ideas (when `PT_BLOCK_IF_LLM_DENY=true`) with positive score enter the book; others glide to zero weight.

Execution
- Orders expressed as share deltas; dust filtered via `PT_MIN_ORDER_NOTIONAL` and `PT_NO_TRADE_BAND_PCT`.
- Turnover guard scales demand if aggregate notional exceeds budget.
- Cash floor enforced pre-submit; optional replacement sells bridge deficits; residual buys are scaled if cash still insufficient.
- Existing circuit breakers remain in effect (symbol/sector/global/headline) and panic exit is still wired (`ENABLE_PANIC_EXIT`).
- Fills record allocator metadata (`target_weight`, turnover estimate) for audit.

8. Container & Deployment Layout
--------------------------------

Service | Role | GPU | Key Volumes
---|---|---|---
`kronos-nbeats` | Forecast server | 1 | `/models`, `/state`
`kronos-graph` | Cross‑asset forecaster | 1 (optional) | `/models`
`kronos-calib` | Nightly calibration | CPU | `/mnt/data/kronos_state`
`services/api` | Orchestration / routes | CPU | —

Key environment variables (selection)
- Model: `MODEL_DIR`, `CALIBRATION_FILE`, `CALIBRATION_ENABLED`
- Strategist: `STRAT_MIN_MARKET_CAP`, `STRAT_MIN_DOLLAR_VOL`, `STRAT_RECO_TOPK`, `STRAT_ENABLE_TURNAROUND`
- Policy: `POLICY_REQUIRE_RECO`, `POLICY_MIN_ABS_DEV`, `POLICY_MIN_SIGMA_Z`, `POLICY_MIN_NOTIONAL_USD`, `POLICY_MIN_TRADE_INTERVAL_SECS`
- Planner: `PLANNER_Z_DEV_THRESH`, `PLANNER_LIMIT_NOTIONAL`, `PLANNER_BRACKET_PRICE_MAX`, `PLANNER_COOLDOWN_MINS`
- FMP: `FMP_API_KEY`, `ENABLE_FMP`, `ENABLE_FMP_NEWS`

9. Data Quality and Validation
------------------------------

- Surface validation: monotonic dates, no nulls, <0.1% extreme moves, ≥5y span
- QA logs for data prep runs (`/mnt/data/kronos_data/logs`)
- Model validation: maintain ~90% coverage between p05–p95; p50 MAE monitored via calibration
- Fundamentals backfill verification queries available in `docs/data_fmp.md`

10. Future Extensions
---------------------

Area | Plan
---|---
Text–Price Fusion | Add FinBERT (or similar) embeddings for TFT/N‑HiTS side channels
Intraday Models | Use price‑at‑post to train +5m/+15m targets
Generative Scenarios | Diffusion‑based TimeGrad / CSDI for tail‑risk simulations
Regime Recognition | Train S4 on volatility states to drive dynamic λ in calibrator
Graph Attention Fusion | Replace static correlation with learned graph attention (Graph Transformer)

11. Engineering Notes (What’s New in This Revision)
---------------------------------------------------

- Strategist upgraded: fundamentals‑aware ranking (factors, liquidity, revisions, ratings) + regime switch
- Policy tightened: reco‑required, deviation + z‑significance gate, min notional with autosizing, cooldown
- Planner aligned: same gates; bracket only with whole‑share qty
- FMP integrations: analyst estimates, ratings, profile, news; cross‑sectional `fmp_revisions_cs` view
- Headline risk: news spike trips temporary circuit breaker per symbol
- N‑BEATS service hardened: SciPy dependency added; artifact loading errors resolved
- Allocator forecasters: new N-BEATS/TFT retraining CLI with quantile loss, CRPS reporting, and automatic artifact promotion (`tools/kronos_data/train_nbeats.py`, `tools/kronos_tft/train_tft.py`)
