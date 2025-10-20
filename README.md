# PulseTrade

Emotion -> Sentiment -> Fundamentals -> Forecast -> Policy -> Execution

This monorepo ships a minimal, production-lean trading research stack:

- **Ingestion**: crypto price websockets (Coinbase, Kraken) + stubs for Alpaca, Finnhub, FMP, Stocktwits, Truth Social, QuiverQuant/CapitolTrades
- **Storage**: Postgres + TimescaleDB
- **Processing**: Celery workers (Redis broker), feature builders, rate-limit guards, Prometheus metrics
- **Forecast**: pluggable model engine (baseline moving-average -> swap in TimesFM/Chronos)
- **Policy**: simple position sizing (baseline) -> slot in FinRL later
- **API**: FastAPI for signals, features, and status
- **Observability**: Prometheus + basic Grafana starter

## Quick start
1) Copy `.env.example` -> `.env` and fill your keys.
2) `docker compose up --build`
3) Open API: http://localhost:8001/docs
4) Prometheus: http://localhost:9090

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

## Accounts to set up
- Required
  - Alpaca Paper Trading account (free): create keys for `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY`.
  - Git + Docker + Docker Compose installed locally.
- Optional data providers (enable via `.env` flags)
  - Finnhub API key (`FINNHUB_API_KEY`) for analyst ratings/news.
  - Financial Modeling Prep API key (`FMP_API_KEY`) for screens (actives/gainers/losers) and targets.
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
git commit -m "Configure providers, discovery, planner gating, and Alpaca execution"
git push origin main
```


## Next steps (swaps)
- Replace baseline forecaster with TimesFM/Chronos.
- Add real analyst & recommendation feeds (Finnhub/FMP) and weight by historical accuracy.
- Add equities live feed (Alpaca Market Data websockets) or your vendor of choice.
- Add Congress trade events (QuiverQuant/CapitolTrades) -> feature flags.
