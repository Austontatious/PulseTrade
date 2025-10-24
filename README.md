# PulseTrade

Emotion -> Sentiment -> Fundamentals -> Forecast -> Policy -> Execution

This monorepo ships a minimal, production-lean trading research stack:

- **Ingestion**: crypto price websockets (Coinbase, Kraken) + stubs for Alpaca, Finnhub, FMP, Stocktwits, Truth Social, QuiverQuant/CapitolTrades
- **Storage**: Postgres + TimescaleDB
- **Processing**: Celery workers (Redis broker), feature builders, rate-limit guards, Prometheus metrics
- **Forecast**: pluggable model engine (baseline moving-average -> swap in Chronos/TimesFM)
- **Kronos modeling services**: multivariate research stack (N-BEATS, diffusion scenarios, graph attention, TFT) with REST endpoints
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

## Alpaca end-to-end (paper)
- In `.env` set:
  - `ENABLE_ALPACA=1` to ingest equities via Alpaca Market Data WS.
  - `ALPACA_DATA_WS_URL=wss://stream.data.alpaca.markets/v2/iex` for the free feed (or `sip` if you have access).
  - `ALPACA_SYMBOLS=AAPL,MSFT,SPY` to choose equities for ingestion.
  - `ENABLE_ALPACA_EXECUTOR=1` to allow policy to submit paper orders.
  - `ALPACA_API_KEY_ID` and `ALPACA_API_SECRET_KEY` from your Alpaca account.
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

### Tools runner for scripts

The new `tools` service mounts the repo and `/mnt/data`, includes pandas/pyarrow/psycopg2/torch, and has direct DB access inside the compose network. Use it for all CLI scripts to avoid local networking headaches:

```bash
docker compose run --rm tools bash -lc 'python tools/kronos_tft/build_dataset.py'
```

The service inherits `DATABASE_URL` and any extra vars you pass via `-e`.

### Trading universes

We maintain layered universes inside Postgres:

1. `symbols` table: ingestion superset.
2. `daily_returns` / `mv_liquidity_60d` materialized view.
3. `universe_candidates_daily`: ranks by liquidity/spread.
4. `trading_universe_100`: top 100 tradable names used by the decision layer.

Toggle the live trading universe in `.env`:

```
TRADING_UNIVERSE_VIEW=trading_universe_100
ALLOW_EXIT_OUTSIDE_UNIVERSE=1
```

At runtime `policy` logs the resolved view and tradable count. Update the view definitions in `db/migrations/20251023_top100_universe.sql` as your liquidity heuristics evolve.

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
  - Quiver/CapitolTrades (`QUIVER_API_KEY`, `CAPITOLTRADES_BASE`) for politics-related trades.
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

## Notes on execution (Paper Trading)
- IEX feed is available on free plans; SIP requires upgrading the Alpaca Market Data subscription and signing exchange agreements.
- Fractional brackets are not allowed by Alpaca; we place simple limit orders for fractional qty and brackets only for whole-share orders.
- Shorting fractional qty is not allowed; baseline/planner will skip those submits and record a `SIM` decision for audit.

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
