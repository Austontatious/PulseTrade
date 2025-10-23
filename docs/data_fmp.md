# Financial Modeling Prep Integration

## Live Collectors
- `price-target`: ingested via `services/ingest/collectors/fmp.py` into `analyst_ratings` (`provider='fmp'`).
- `stock_market/{actives,gainers,losers}`: stored via `fmp_screens.py`, expanding the `symbols` table for discovery.
- `earning_calendar`: written by `fmp_calendar.py` into `event_windows` and gated by `ENABLE_EARNINGS_BLACKOUT`.

## Fundamentals Payloads
- Tables (jsonb payloads): `fmp_income_statement`, `fmp_balance_sheet`, `fmp_cash_flow`, `fmp_key_metrics`, `fmp_ratios`.
- Env flags: `ENABLE_FMP_FUNDAMENTALS`, `FMP_FUNDAMENTALS_PERIOD` (`annual|quarter`), `FMP_FUNDAMENTALS_SINCE`.
- Backfill: run via the tools container so the scripts can reach Postgres directly. Fundamentals endpoints require an FMP plan that includes `/income-statement`, `/balance-sheet-statement`, `/cash-flow-statement`, `/key-metrics`, and `/ratios`. A 403 response means the key/plan is not entitled.

  ```bash
  docker compose run --rm -e FMP_API_KEY=YOUR_KEY tools bash -lc '
    python tools/fmp/backfill_fundamentals.py --universe services/ingest/universe_symbols.txt --period annual --since 2005-01-01 &&
    python tools/factors/export_factors.py &&
    python tools/kronos_tft/build_dataset.py &&
    python tools/kronos_tft/train_tft.py
  '
  docker compose up -d --build kronos-tft
  ```

  The helper scripts now abort with a clear error if fundamentals are missing so you do not accidentally retrain TFT on empty covariates.

## Derived Factors
- Liquidity screen: `mv_liquidity_60d` (refresh before `make universe.build`).
- Universe builder: `make universe.build` → writes a filtered symbol list to `services/ingest/universe_symbols.txt`.
- Factors view: `fundamentals_factors_v1` (lagged, winsorized, z-scored fundamentals with sector-neutral z's when `dim_company_profile.sector` is populated).
- Export helper: `make factors.export` → `/mnt/data/kronos_data/interim/fundamentals_factors_v1.parquet`.

## Validation Queries
```sql
SELECT 'income' src, COUNT(*) rows, MAX(date) max_date FROM fmp_income_statement
UNION ALL SELECT 'balance', COUNT(*), MAX(date) FROM fmp_balance_sheet
UNION ALL SELECT 'cashflow', COUNT(*), MAX(date) FROM fmp_cash_flow
UNION ALL SELECT 'metrics', COUNT(*), MAX(date) FROM fmp_key_metrics
UNION ALL SELECT 'ratios', COUNT(*), MAX(date) FROM fmp_ratios;
```

## Notes
- API key: `FMP_API_KEY` (set in `.env`).
- Rate limit derived from `RATE_MAX_HTTP_PER_SEC` (default 5 req/s).
- Current model usage:
  - N-BEATS remains return-only; use factors for universe selection, sizing, and risk overlays.
  - Kronos scenarios/GraphX can ingest aggregated factors in their context vectors.
- Running with no FMP fundamentals will zero-fill the TFT covariates. Keep the TFT service in shadow mode until a fundamentals backfill succeeds, then retrain and redeploy.
- Future work: normalize high-value fields (revenue, EBITDA, netIncome, ratios) into analytics fact tables and add a multivariate forecaster (e.g., TFT) that consumes the factor exports directly.
- Export helper: `make factors.export` → `/mnt/data/kronos_data/interim/fundamentals_factors_v1.parquet`.
+ The parquet will be empty until fundamentals are ingested; we warn in the exporter when no rows are available.
