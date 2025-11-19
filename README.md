# PulseTrade

Emotion -> Sentiment -> Fundamentals -> Forecast -> Policy -> Execution

This monorepo ships a minimal, production-lean trading research stack:

- **Ingestion**: crypto price websockets (Coinbase, Kraken) + stubs for Alpaca, Finnhub, FMP, Stocktwits, Truth Social, QuiverQuant/CapitolTrades
- **Storage**: Postgres + TimescaleDB
- **Processing**: Celery workers (Redis broker), feature builders, rate-limit guards, Prometheus metrics
- **Forecast**: Kronos N-BEATS service (via `services/forecast`, falls back to a moving-average only if Kronos is offline)
- **Kronos modeling services**: multivariate research stack (N-BEATS, TFT, graph, diffusion scenarios) with REST endpoints; see `docs/Kronos_Models_Training_and_Datastreams.md` and `Kronos_Predictive_Stack.md` for model, training, and datastream details.
- **Policy**: simple position sizing (baseline) -> slot in FinRL later
- **API**: FastAPI for signals, features, and status
- **Observability**: Prometheus + basic Grafana starter

## Quick start
1) Copy `.env.example` -> `.env` and fill your keys.
2) Build the shared tools image (Python + pandas/psycopg2) so CLI scripts can talk to Postgres:

   ```bash
   docker compose build tools
   ```

3) `docker compose up --build`
4) Open API: http://localhost:8001/docs
5) Prometheus: http://localhost:9090

### Bring the live loop up
1. Apply the latest DB migrations (LLM + signal universe) **once**:
   ```bash
   docker compose exec -T db psql -U pulse -d pulse -f db/migrations/20251027_add_llm_tables.sql
   docker compose exec -T db psql -U pulse -d pulse -f db/migrations/20251114_signal_universe.sql
   ```
2. Set Alpaca + forecast knobs in `.env`:
   ```ini
   ENABLE_ALPACA=1
   ENABLE_ALPACA_REST=1
   ENABLE_UNIVERSE_SEED=1
   ALPACA_API_KEY_ID=...            # paper or live key
   ALPACA_API_SECRET_KEY=...
   FORECAST_HORIZON=5d
   KRONOS_HORIZON_STEPS=5
   FORECAST_MAX_TICKERS=150         # or whatever size you want
   ```
3. Launch the stack (ingest + Kronos + planners). This brings up Postgres/Redis automatically:
   ```bash
   docker compose build
   docker compose up -d ingest kronos-nbeats forecast strategist worker policy api redis db
   ```
   Ingest seeds the `symbols` table with Alpaca’s `/v2/assets`, refreshes shortability flags, and streams trades/quotes. The forecast service now writes **5-day** Kronos signals into `forecasts` for every Alpaca equity (`FORECAST_MAX_TICKERS` per cycle).
4. Confirm data is flowing:
   ```bash
   docker compose logs -f ingest
   docker compose logs -f forecast
   docker compose exec -T db psql -U pulse -d pulse -c "SELECT horizon, COUNT(*) FROM forecasts GROUP BY 1 ORDER BY 1"
   ```

### After allocator fixes (October 2025)
- The allocator now treats tiny long/short weights symmetrically (`libs/portfolio/sizer.py`), so lingering fractional shorts are covered instead of ignored and short books use the same normalization logic as longs.
- Restart the stack after pulling this change so every service reloads the updated sizing logic:

  ```bash
  docker compose down
  docker compose up -d
  docker compose ps
  ```

  (Expect the GPU Kronos services to report `health: starting` for ~30 seconds before they flip to `healthy`.)
- Sanity-check policy with `docker compose logs -f policy | grep -i "sizer"` and confirm fills show net BUYs when flattening shorts.

## Alpaca-Universe N-BEATS Pipeline (Sentiment + Price Targets)

The research loop now ships a full pipeline to train Kronos N-BEATS on the entire Alpaca universe (≈3.4k symbols) with 6 months of price data, FMP sentiment, and v4 price targets:

1. **Seed data** (ongoing ingest already writes the required tables; backfill with the new scripts if needed):
   ```bash
   python tools/fmp/backfill_sentiment.py --since 2023-01-01 --limit 500
   python tools/fmp/backfill_price_targets_v4.py --since 2023-01-01 --limit 500
   ```
2. **Build the enriched training set**:
   ```bash
   python tools/kronos_data/build_alpaca_training_set.py \
     --symbols db/alpaca_universe.symbols.txt \
     --lookback-days 190 \
     --out /mnt/data/kronos_data/processed/nbeats_alpaca_daily.parquet
   ```
3. **Train N-BEATS with covariates** (sentiment + targets):
   ```bash
   python tools/kronos_data/train_nbeats.py \
     --data /mnt/data/kronos_data/processed/nbeats_alpaca_daily.parquet \
     --out /mnt/data/models/kronos-nbeats \
     --input-size 126 --horizon 5 --feature-cols "" \
     --residuals-out /mnt/data/kronos_data/processed/nbeats_alpaca_residuals.parquet
   ```
4. **Rank the best-fit symbols** (Top 100 by validation residuals):
   ```bash
   python tools/kronos_data/rank_best_fit.py \
     --residuals /mnt/data/kronos_data/processed/nbeats_alpaca_residuals.parquet \
     --top-k 100 \
     --report reports/top100_best_fit.json \
     --update-file services/ingest/universe_symbols.txt
   ```
5. **One-shot orchestration** (runs all steps above):
   ```bash
   python tools/pipeline/train_nbeats_alpaca.py \
     --symbols-file db/alpaca_universe.symbols.txt \
     --lookback-days 190 --top-k 100 \
     --update-universe services/ingest/universe_symbols.txt
   ```

The pipeline writes the enriched dataset, stores the trained Kronos artifact (under `/mnt/data/models/kronos-nbeats/<run-id>`), exports residuals for diagnosability, and refreshes `services/ingest/universe_symbols.txt` with the Top 100 best-fit names for allocators to consume. See `docs/Kronos_Models_Training_and_Datastreams.md` for feature engineering details.

### Short book controls (November 2025)

The live allocator + executor now supports symmetrical long/short books with clamps across positions, sectors, gross exposure, and net exposure. Key knobs (all read from `.env`) are:

| Flag | Purpose |
| --- | --- |
| `PT_ALLOW_SHORTS` | Master enable for short routing (set `false` to fall back to long-only behavior). |
| `PT_MAX_SHORT_POS_PCT`, `PT_MAX_POS_PCT` | Per-symbol caps for shorts/longs (fraction of NAV). |
| `PT_SECTOR_MAX_SHORT_PCT`, `PT_SECTOR_MAX_PCT` | Sector aggregates per side. |
| `PT_GROSS_MAX_PCT` | |long| + |short| exposure cap. |
| `PT_NET_EXPO_MIN` / `PT_NET_EXPO_MAX` | Net exposure band enforced after sector/gross scaling. |
| `PT_REQUIRE_SHORTABLE` | Requires Alpaca to mark the symbol `shortable` *and* `easy_to_borrow` before opening a short. |
| `PT_USE_TRAILING_STOPS`, `PT_TRAIL_BUY_TO_COVER_PCT` | Optional trailing buy-to-cover orders after each new short. |

Implementation highlights:
- `libs/portfolio/allocator.propose_target_weights_long_short` normalizes long and short buckets independently, applies sector/gross caps, and then scales into the requested net-exposure band.
- `services/policy/policy/run.py` now consumes the long/short map, converts weight deltas to share deltas, and routes:
  - Positive deltas → buys / buy-to-cover.
  - Negative deltas → close existing longs first, then (if enabled) short sell the remainder.
  - Every short entry can optionally attach a trailing buy-to-cover protective order.
- Short gate uses cached Alpaca asset metadata (`dim_company_profile.shortable`, `.easy_to_borrow`). Run `make ingest-backfill` or the nightly ingest job to refresh the cache.

**Reminder:** run the DB migration `db/migrations/20251105_add_shortability_flags.sql` (already applied in this repo) before starting policy; otherwise `_fetch_fundamentals` will fail.

## Alpaca end-to-end (paper)
- In `.env` set:
  - `ENABLE_ALPACA=1` to ingest equities via Alpaca Market Data WS.
  - `ALPACA_DATA_WS_URL=wss://stream.data.alpaca.markets/v2/iex` for the free feed (or `sip` if you have access).
  - `ALPACA_SYMBOLS=AAPL,MSFT,SPY` to choose equities for ingestion.
  - `ENABLE_ALPACA_EXECUTOR=1` to allow policy to submit paper orders.
  - `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` from your Alpaca account.
  - `ALPACA_ACCOUNT_ID` (12-character code from the Alpaca console, e.g., `PA3JGK6CZHA5`). The policy process validates the connected account matches this ID before sending orders so you don’t accidentally route to an old paper account.
  - Either set `ALPACA_ORDER_NOTIONAL=10` (preferred, uses dollar notional) or ensure your account supports fractional `qty`.
  - Optionally set `FORECAST_TICKERS` (defaults to `SYMBOLS`) so the forecaster targets your equities as they stream in.

- Bring the stack up: `docker compose up --build -d`
- Verify ingestion: check the `trades` table is filling for your symbols.
- Verify forecasts: call `/signals/latest?horizon=1m` and look for your tickers.
- Verify policy: after forecasts appear, fills will be recorded in `fills` (and orders posted to Alpaca when enabled).

## Kronos forecasting services

These GPU-ready microservices live alongside the core API. Each reads its artifact from `/mnt/data/models/.../latest` and exposes a FastAPI app for inference.

| Service | Port | Artifact dir | Endpoint |
|---------|------|--------------|----------|
| `kronos-nbeats` | 8080 | `/mnt/data/models/kronos-nbeats/latest` | `/forecast` (per-symbol horizon forecasts) |
| `kronos-scenarios` | 8082 | `/mnt/data/models/kronos-scenarios/latest` | `/sample` (tail-risk diffusion sampler) |
| `kronos-graphx` | 8083 | `/mnt/data/models/kronos-graphx/latest` | `/graph_weights` (attention matrix + leaders) |
| `kronos-tft` | 8084 | `/mnt/data/models/kronos-tft/latest` | `/forecast` (multivariate TFT forecasts)

Health checks: `docker compose exec -T <service> curl -fsS http://localhost:<port>/health`

## LLM service (“Finance” via shared vLLM)

PulseTrade no longer ships an embedded vLLM container. Instead, all services call the shared “Finance” instance running on the host (port `9009`) via `host.docker.internal`.

- Launch Finance separately (example):

  ```bash
  export CUDA_VISIBLE_DEVICES=0,1,2,3
  /mnt/data/vllm-venv/bin/python -m vllm.entrypoints.openai.api_server \
    --model /mnt/data/models/Qwen/Qwen2.5-32B-AGI \
    --served-model-name Finance \
    --tensor-parallel-size 4 \
    --dtype float16 \
    --max-model-len 4096 \
    --gpu-memory-utilization 0.88 \
    --max-num-seqs 64 \
    --swap-space 12 \
    --distributed-executor-backend mp \
    --trust-remote-code \
    --download-dir /mnt/data/models \
    --disable-custom-all-reduce \
    --host 0.0.0.0 --port 9009
  ```

- Core services expect:
  - `LLM_BASE_URL=http://host.docker.internal:9009/v1`
  - `LLM_MODEL=Finance`
  - `LLM_API_KEY` left blank (OpenAI-compatible but unauthenticated in this setup)
- `docker-compose.yml` sets `extra_hosts` for API + Forecast so `host.docker.internal` resolves inside the containers.
- Health check (proxy through API): `curl -s http://localhost:8001/llm/health`
- Manual chat: `curl -s -X POST http://localhost:8001/llm/chat -H 'Content-Type: application/json' -d '{"system":"You are terse.","user":"Summarize AAPL earnings in 3 bullets."}'`

Configuration still lives in `.env` / `.env.example`. The forecast service records `llm` metadata (rationales + policy filter output) alongside each stored forecast, and the `/analysis/recent` API endpoint exposes a summary generated on demand.

- Prompt registry: `configs/llm_prompts.yaml` tracks versioned templates. Use `LLM_AB_BUCKETS` to run A/B variants.
- Audit trail: every call is persisted to `llm_calls` with latency, prompt hash, and success flag. `make migrate-llm` applies the schema.
- Weekly deep dive & daily go/no-go: the Friday `tools/universe/build_signal_universe.py` run now calls the `weekly_deep_dive` prompt for the universe-100 and writes outputs to `llm_symbol_reviews` plus `reports/llm_weekly_deep_dive_<date>.json`. The nightly `tools/planner/build_daily_plans.py` adds a 48h `daily_go_nogo` sweep for the active-10, writes `reports/daily_llm_check_<date>.json`, and stores verdicts that policy uses as a kill switch at the open.
- Admin endpoints: `GET /llm/admin/call/{id}`, `POST /llm/admin/replay`, `GET /llm/admin/stats`.
- Offline eval: `make eval-llm` replays historical snapshots and writes latency/validity stats under `results/`.
- Kill switch & panic controls: set `ORDER_GATE_DISABLED=true` to bypass LLM gating, and enable `ENABLE_PANIC_EXIT=1` to allow `POST /strategist/panic-exit` to place a global breaker and close open positions.
- Portfolio allocator knobs: `.env` now exposes `PT_MAX_POS_PCT`, `PT_SECTOR_MAX_PCT`, `PT_GROSS_MAX_PCT`, `PT_CASH_FLOOR_PCT`, `PT_MIN_ORDER_NOTIONAL`, `PT_NO_TRADE_BAND_PCT`, `PT_TOP_N`, `PT_SCORE_FIELD`, `PT_SCORE_EXP`, `PT_TURNOVER_CAP_PCT`, and replacement gates (`PT_ENABLE_REPLACEMENT`, `PT_REPLACEMENT_FRIC_BPS`, `PT_REPLACEMENT_LOOKBACK_D`). These power the new sizing engine (see Kronos doc §7).

### Tools runner for scripts

The new `tools` service mounts the repo and `/mnt/data`, includes pandas/pyarrow/psycopg2/torch, and has direct DB access inside the compose network. Use it for all CLI scripts to avoid local networking headaches:

```bash
docker compose run --rm tools bash -lc 'python tools/kronos_tft/build_dataset.py'
```

The service inherits `DATABASE_URL` and any extra vars you pass via `-e`.

### Daily strategy scorecard

The `tools/kronos_eval/daily_strategy_eval.py` job stitches together fills ↔ forecasts ↔ strategist recos and reports whether the *strategy* (not just the model) has edge. Run it inside the `tools` container so it can hit Postgres:

```bash
docker compose run --rm tools \
  bash -lc 'python tools/kronos_eval/daily_strategy_eval.py --date $(date -d "yesterday" +%F)'
```

What it does:

- Rebuilds entry lots per symbol (long and short) so partial exits roll up cleanly and Kronos forecasts are aligned to the exact fill timestamp.
- Joins strategist score / factors, dim_company_profile sectors, and the most recent risk regime knob.
- Pulls `daily_returns` (log returns) for the trade symbols, SPY, and each sector ETF proxy to compute r_hold / r\_1d / r\_3d plus edge vs SPY and edge vs sector.
- Buckets trades by signal strength (|Kronos dev| bps), PT score quintile, regime, and side; emits win rate, mean/median returns, and forecast hit rate per bucket.
- Writes a CSV summary (`reports/kronos_strategy_eval_YYYY-MM-DD.csv`), a Markdown blurb with the headline takeaways, and a `_trades.parquet` (CSV fallback) containing the per-trade analytics so you can slice further in a notebook.

Drop it on a nightly cron after fills land; the Markdown statement is designed to paste directly into daily check-ins (“Top 20% PT\_SCORE ideas carried +0.8% edge vs SPY; longs lagged shorts in risk-off regime”, etc.).

### Trading universes

We maintain layered universes inside Postgres:

1. `symbols` table: ingestion superset from Alpaca + side feeds.
2. `mv_liquidity_60d`: materialized view with rolling liquidity statistics.
3. `signal_universe_100`: **signal-ranked snapshot** written weekly via Kronos forecasts.
4. `trading_universe_100`: view that points at the most recent `signal_universe_100` snapshot.

Toggle the live trading universe in `.env`:

```
TRADING_UNIVERSE_VIEW=trading_universe_100
ALLOW_EXIT_OUTSIDE_UNIVERSE=1
```

The signal universe builder lives in `tools/universe/build_signal_universe.py`. It pulls Alpaca-tradable equities, filters out OTC/inactive/<$3 names, requires a minimum 60d dollar volume, computes the Kronos 5-day excess Sharpe-like signal, runs the **LLM risk screen** (flags litigation/earnings/odd news) on the top candidates, demotes or drops anything risky, and writes the top `SIGNAL_UNIVERSE_SIZE` (default 100) into `signal_universe_100` with Kronos + LLM metadata. The same run refreshes `services/ingest/universe_symbols.txt` for fast ingest warm-start. Schedule it every Friday 11pm CST via:

```bash
docker compose run --rm tools \
  bash -lc 'python tools/universe/build_signal_universe.py --as-of $(date +%F)'
```

If you want a one-stop “Step 1” job that backfills the prior week, invokes the Kronos forecaster, and writes the top 100 purely by signal-to-noise (no LLM/Monte Carlo yet), use:

```bash
docker compose run --rm tools \
  bash -lc 'python tools/pipeline/run_weekly_predictability.py --as-of $(date +%F)'
```

That script now pulls candles from Finnhub first (`FINNHUB_API_KEY` required in `.env`) and only falls back to the Alpaca REST/liquidity backfills when Finnhub lacks coverage. After data prep it calls `forecast_once` for each symbol and upserts the predictability snapshot into `signal_universe_100` so downstream LLM + Monte Carlo stages can pick up a consistent universe.

Once Step 1 is done, kick off the weekly LLM deep dive (Step 3) to attach agreement/neutral/disagreement signals before Monte Carlo:

```bash
docker compose run --rm tools \
  bash -lc 'python tools/pipeline/run_weekly_deep_dive.py --as-of $(date +%F)'
```

This reuses the `run_weekly_deep_dives` helper so every top-100 symbol gets a structured review stored in `llm_symbol_reviews` plus `reports/llm_weekly_deep_dive_<date>.json`.

Next (Step 4, optional), simulate the universe via Monte Carlo to surface the top trades for the coming session (set `ENABLE_MONTE_CARLO=1` or run manually when needed):

```bash
docker compose run --rm tools \
  bash -lc 'python tools/pipeline/run_monte_carlo_top10.py --as-of $(date +%F)'
```

This script reuses `tools/universe/monte_carlo_sim.py`, writes the `universe_monte_carlo` table, and emits `reports/monte_carlo_top_trades_<date>.json` so the planner/policy can consume the certainty-weighted top 10 when the stage is enabled. With `ENABLE_MONTE_CARLO=0` (default), the weekly pipeline skips this step entirely and you can run the simulator later for research.

Finally (Step 5), re-run the intraday go/no-go sweep to incorporate fresh headlines at 08:00 and 12:00 CST:

```bash
docker compose run --rm tools \
  bash -lc 'python tools/pipeline/run_intraday_go_nogo.py --date $(date +%F) --slot 0800'
```

Repeat with `--slot 1200` at midday. This updates `llm_symbol_reviews` (scope `daily_go_nogo`) and writes `reports/daily_llm_check_<date>_<slot>.json`, allowing the policy service to block any symbols that became risky intraday.

`trading_universe_100` automatically points at the most recent snapshot so planner/policy keep trading inside the “top 100 by forward edge” sandbox.

### Monte Carlo ranking

When enabled, `tools/universe/monte_carlo_sim.py` runs after the Kronos + LLM passes to enrich the universe. For each of the 100 ranked symbols we:

- Pull the last `MONTE_CARLO_LOOKBACK_DAYS` worth of log returns from `daily_returns` (default 252) plus the latest `symbols.meta->last_price`.
- Simulate `MONTE_CARLO_SIMS` geometric paths over `MONTE_CARLO_DAYS` (defaults 1,000 sims × 5 days) using the historical mean/std and record the per-symbol mean profit, profit std-dev, probability of finishing green, and the combined **best score** (`mean_profit / profit_std_dev`).
- Persist the raw stats to `universe_monte_carlo` (with ranks) and embed a `"monte_carlo"` block into each `signal_universe_100.meta` row. JSON reports land under `reports/monte_carlo_universe_<as_of>.json` and `reports/monte_carlo_top10_<as_of>.json`.
- Optionally push run-duration metrics to a Prometheus pushgateway (`PROM_PUSHGATEWAY`) so cron monitors can alert if the Monte Carlo stage stalls.
- **TODO:** integrate live borrow-cost data (e.g., Alpaca stock borrow API) so short rankings subtract the actual borrow expense. For now the planner assumes a neutral borrow cost of 1.0.

Planner + policy consume that metadata automatically when present:

- The nightly planner can prioritize stocks by Monte Carlo best score before falling back to signal `delta`, and can use the Monte Carlo mean/std to seed its per-plan simulations when the data is available. Set `PLAN_FORECAST_HORIZON`, `MONTE_CARLO_*`, and `DAILY_CANDIDATE_COUNT` to tune the blend.
- Policy can enforce `PT_REQUIRE_MONTE_TOP=true` (default false) to only open positions in the latest Monte Carlo top list. Set `PT_MONTE_TOP_LIMIT` (default 10) to choose the cutoff. Live metrics expose the overlap rate via the Prometheus gauge `policy_monte_top_overlap` (listens on `POLICY_METRICS_PORT`, default 9109).

New DB migration: `db/migrations/20251120_add_monte_carlo_universe.sql` installs the `universe_monte_carlo` table—apply it before running the updated tooling.

### Nightly daily-plan selection

The nightly selector (`tools/planner/build_daily_plans.py`) consumes the weekly signal universe and picks the next trading day’s candidates:

- Computes the latest vs. previous 5-day excess signal for each of the 100 names (`delta_signal`), blends in the LLM risk/sentiment metadata, and sets the daily actionability score (so nasty news/earnings get down-weighted before they ever reach Monte Carlo). Mandatory overnight holds are auto-included.
- Calls the LLM “plan coach” to suggest an archetype per symbol (long/short intraday vs swing vs no-trade), then builds a single `DailyPlan` (≤2 actions) and runs Monte Carlo to estimate mean P&L, volatility, and probability of loss with 95% confidence bounds (configurable via env: `PLAN_EPS_MU`, `PLAN_EPS_P`, `PLAN_P_LOSS_MAX`, `PLAN_LAMBDA_RISK`).
- Applies the safety gates (`p_loss_upper <= 0.45`, `mu_lower >= 0.0001`), ranks eligible plans by `score = mu - λ·sigma`, and keeps at most `DAILY_MAX_TRADES // MAX_ACTIONS_PER_STOCK_PER_DAY` symbols (defaults: 20 trades / 2 actions → ≤10 symbols).
- Writes JSON + Markdown summaries under `reports/daily_plan_selection_YYYY-MM-DD.*`. The JSON contains every candidate’s plan + evaluation; the Markdown gives a quick human-readable list for tomorrow’s book review.

Run it nightly (11pm CST Sun–Thu) via:

```bash
docker compose run --rm tools \
  bash -lc 'python tools/planner/build_daily_plans.py --date $(date -d "tomorrow" +%F)'
```

Planner/policy can ingest the resulting JSON to know which symbols/plans are authorized for the next trading day, and enforce the global/per-symbol action caps directly.

#### Automation helpers

Invoke the recurring jobs directly via the tools runner (legacy shell wrappers remain under `scripts/scheduler` if you prefer them):

```bash
docker compose run --rm tools python -m tools.run signal-universe -- --as-of "$(TZ=America/Chicago date +%F)"
docker compose run --rm tools python -m tools.run daily-plans -- --date "$(TZ=America/Chicago date +%F)"
scripts/scheduler/run_liquidity_backfill.sh        # populates last_price + avg_dollar_vol_60d
```

Make targets offer the same behavior:

```bash
make liquidity.backfill       # optional env LIMIT / LOOKBACK_DAYS

make signal.universe
DATE=2025-11-15 make signal.universe

make daily.plans
DATE=2025-11-16 make daily.plans
```

Example cron entries (host timezone CST):

```
# Weekly universe snapshot (Friday 11pm CST)
0 23 * * 5 TZ=America/Chicago docker compose -f /mnt/data/PulseTrade/docker-compose.yml run --rm tools \
  python -m tools.run signal-universe >> /mnt/data/logs/signal_universe.log 2>&1

# Nightly plans (Mon–Thu 11pm CST)
0 23 * * 1-4 TZ=America/Chicago docker compose -f /mnt/data/PulseTrade/docker-compose.yml run --rm tools \
  python -m tools.run daily-plans >> /mnt/data/logs/daily_plans.log 2>&1
```

### Trading knobs and troubleshooting no-trade scenarios

If you aren’t seeing trades after bringing the stack up, start by loosening the decision gates in `.env` and then tighten them back once you confirm the loop is working.

Common gates to tune (safer defaults for initial bring-up):

```
# Planner sensitivity and cooldown
PLANNER_Z_DEV_THRESH=1.2          # was 1.8–2.0; lower is more permissive
PLANNER_COOLDOWN_MINS=10          # was 30

# Policy entry guards
POLICY_REQUIRE_RECO=0             # do not require strategist reco to enter
POLICY_MIN_ABS_DEV=0.0007         # minimum absolute deviation (0.07%)
POLICY_MIN_SIGMA_Z=1.5            # z threshold on signal strength
POLICY_MIN_NOTIONAL_USD=50        # reduce if entries are too small to place
POLICY_MIN_TRADE_INTERVAL_SECS=60

# Quote quality filters
MAX_SPREAD_BPS=12
MAX_QUOTE_AGE_SECS=5
```

After editing `.env` run:

```
docker compose up -d --build policy worker strategist
```

Quick sanity checks:

```
# Are forecasts being written?
docker compose exec -T db bash -lc "psql -U pulse -d pulse -c \"SELECT COUNT(*) FROM forecasts WHERE ts > now() - interval '10 minutes'\""

# Any orders/fills recently?
docker compose exec -T db bash -lc "psql -U pulse -d pulse -c \"SELECT COUNT(*) FROM fills WHERE ts > now() - interval '30 minutes'\""
```

If those are still zero, verify data feeds (Alpaca/crypto) are flowing, and ensure your `TRADING_UNIVERSE_VIEW` returns the expected 100 symbols.

## Accounts to set up
- Required
  - Alpaca Paper Trading account (free): create keys for `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY`.
  - Git + Docker + Docker Compose installed locally.
- Optional data providers (enable via `.env` flags)
  - Finnhub API key (`FINNHUB_API_KEY`) for analyst ratings/news.
  - Financial Modeling Prep API key (`FMP_API_KEY`) for screens (actives/gainers/losers) and targets.
    * Fundamentals endpoints (income/balance/cash flow, key metrics, ratios) require a paid FMP tier. Without it, backfills return HTTP 403 and fundamentals-based features stay empty. Use the tools runner to backfill once your plan includes those endpoints:

      ```bash
      docker compose run --rm -e FMP_API_KEY=YOUR_KEY tools bash -lc '
        python tools/fmp/backfill_fundamentals.py --universe services/ingest/universe_symbols.txt --period annual --since 2005-01-01 &&
        python tools/factors/export_factors.py &&
        python tools/kronos_tft/build_dataset.py &&
        python tools/kronos_tft/train_tft.py
      '
      docker compose up -d --build kronos-tft
      ```

    * Until fundamentals are available, TFT will train on zero-filled covariates. Keep it in shadow mode and swap artifacts after the backfill succeeds.
  - Stocktwits token (`STOCKTWITS_TOKEN`) to use their trending API; if absent, a scrape fallback is used for trending symbols.
  - Quiver/CapitolTrades (`QUIVER_API_TOKEN` or `QUIVER_API_KEY`, `QUIVER_API_BASE`, `QUIVER_TIMEOUT`, `QUIVER_ENABLE_PUBLIC_EXTRAS`, `CAPITOLTRADES_BASE`) for congress trading, lobbying, government contracts, off-exchange short volume, and optional public datasets (13F, ETF holdings, app ratings, patents, political beta). Use `make quiver-backfill` for a historical load and schedule `make quiver-update` daily.
  - Truth Social cookie (`TRUTH_SOCIAL_COOKIE`) if you want to pull posts.

## Feature flags (common)
- Ingest
  - `ENABLE_ALPACA=1` to turn on equities ingestion (IEX WS + REST fallback).
  - `ENABLE_ALPACA_IEX_WS=1`, `ENABLE_ALPACA_SIP_WS=0` for WS feeds (SIP requires paid subscription).
  - `ENABLE_ALPACA_REST=1`, `ALPACA_REST_POLL_SECS`, `ALPACA_REST_MAX_PER_CYCLE` for REST fallback.
  - `ENABLE_UNIVERSE_SEED=1` to seed S&P500/Nasdaq-100 tickers.
  - `ENABLE_FMP_SCREENS=1` to ingest actives/gainers/losers via FMP.
  - `ENABLE_STOCKTWITS_TRENDING=1` to ingest trending tickers (API if token present, else scrape fallback).
  - `ENABLE_SYMBOL_DISCOVERY=1` to add tickers based on social activity (`social_features`).
- Forecast
  - `FORECAST_MAX_TICKERS` caps how many tickers are forecast each cycle.
- Policy/Planner
  - `ENABLE_ALPACA_EXECUTOR=1` to submit orders to Alpaca Paper.
  - `POLICY_ALLOW_ALL_ALPACA=1` to allow baseline policy to submit for any non-USD equity (sells use position checks to avoid shorts).
  - Planner quality: `PLANNER_Z_DEV_THRESH` (ATR z-score), `PLANNER_COOLDOWN_MINS`, `PLANNER_TOP_K`, `PLANNER_LIMIT_NOTIONAL`,
    `PLANNER_ENTRY_ATR_FRAC`, `PLANNER_BRACKET_PRICE_MAX`, `PLANNER_USE_SENTIMENT`, `PLANNER_USE_FUNDAMENTALS`.
  - Throughput: `POLICY_MAX_SUBMITS_PER_STEP`, order/position rate limits `ALPACA_ORDERS_PER_SEC`, `ALPACA_POS_PER_SEC`.

## Notes on execution (paper + live)
- IEX feed is available on free plans; SIP requires upgrading the Alpaca Market Data subscription and signing exchange agreements.
- Fractional brackets are not allowed by Alpaca; we place simple limit orders for fractional qty and brackets only for whole-share orders.
- Short entries obey the shortability cache and per-name clamps; if a symbol is not ETB (or currently cooling down due to rejection), the short leg is skipped and logged.
- Shorting fractional qty is not allowed; delta shares are rounded to whole integers before routing.

## What you’ll see
- Ingestion will continuously populate `trades` from crypto (Coinbase/Kraken), equities (Alpaca IEX WS + REST fallback), and optional providers.
- The forecaster writes 1m horizon signals per symbol; the policy emits decisions; the planner places higher-conviction limit orders with risk targets.
- View recent fills via API: `GET /analytics/fills` or query the DB `fills` table. Venue `ALPACA` indicates a posted paper order; `SIM` is a simulated decision only.

## GitHub workflow
This repo is a standard Git project. After editing configs or code:

```
git add .
git commit -m "<summary of changes>"
git push origin main
```


## Execution Quality & Risk Controls (shipped)
- Quote-aware filtering: skips orders when spreads are wide or quotes stale; uses maker-like entry prices.
- Vol-targeted sizing: sizes so a 1×ATR stop ≈ `RISK_DOLLARS_PER_TRADE`.
- Earnings blackout: avoids trading around scheduled earnings windows.
- Fractional exits: places stop/target child orders for fractional entries (brackets not allowed for fractionals).
- Rate limiting + dedupe: caps orders/sec and avoids duplicate open orders per side/ticker.
- Circuit breakers: per-symbol and global anomaly trips to halt new entries in dislocated tapes.

See docs/PLANNING_EXECUTION.md for a deep-dive on planning and execution logic.

## Next steps (ideas)
- Swap in a stronger forecaster (TimesFM/Chronos) or your model.
- Add sector breakers and macro blackout (CPI/FOMC) schedule.
- Improve sentiment via FinBERT small model and handle weighting/anti-bot heuristics.
