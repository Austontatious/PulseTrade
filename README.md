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
3) Open API: http://localhost:8000/docs
4) Prometheus: http://localhost:9090

## Next steps (swaps)
- Replace baseline forecaster with TimesFM/Chronos.
- Add real analyst & recommendation feeds (Finnhub/FMP) and weight by historical accuracy.
- Add equities live feed (Alpaca Market Data websockets) or your vendor of choice.
- Add Congress trade events (QuiverQuant/CapitolTrades) -> feature flags.
