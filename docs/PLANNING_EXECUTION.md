# Planning & Execution Logic (Chronos)

This document explains how the live planning and execution pipeline works in PulseTrade, the data it uses, and the safeguards in place to improve fill quality and control risk.

## Overview
- Forecast loop (every few seconds) writes 1m‐horizon forecasts per symbol to `forecasts`.
- Policy loop (Chronos) consumes the latest price + forecast and produces trade intents (baseline market orders) and calls the Planner for higher‑conviction limit orders with stops/targets.
- Strategist (PLUTUS) can optionally influence sizing/thresholds via recommendations and knobs; this doc focuses on the PLUTUS‑independent path already live.

## Data used
- Prices: `trades` (last 10–30 minutes for ATR/vol, and recency for forecasting).
- Quotes: `quotes` (top‐of‐book bid/ask and timestamp) for spread and quote‑age filters.
- Forecasts: `forecasts` (mean/lower/upper) to compute forecast deviation and uncertainty proxy.
- Sentiment aggregates: `social_features` (message rate, mean/std) as optional gating.
- Analyst flow: `analyst_ratings` for coarse fundamental veto/boost.
- Event windows: `event_windows` (e.g., earnings blackout).
- Circuit breakers: `circuit_breakers` for symbol/global trip status.

## Core signals
- Forecast deviation: `dev = mean/price − 1` (short‐horizon alpha proxy).
- Source: since the November 2025 Kronos refresh, the `forecast` service writes 1‑minute horizons straight from the Kronos N‑BEATS container (log‑return converted back into a price target); the legacy moving‑average baseline only fires if Kronos is down, so every planner decision now reflects Kronos’ calibrated signal.
- ATR proxy: std‐dev of 1‑second returns × price over last N minutes (volatility).
- ATR z‐score: `z = |dev| × price / ATR` (signal normalized by regime vol).
- Spread/age: from `quotes` for entry quality and to skip locked/stale markets.

## Filtering & gating
1) Circuit breakers
   - If a symbol or global breaker is active, new entries are skipped until expiry.

2) Earnings blackout
   - If `ENABLE_EARNINGS_BLACKOUT=1` and `ticker` is within `event_windows`, the planner skips it.

3) Spread/quote guard
   - Computes mid and spread (ask−bid). Skips submits when:
     - `spread_bps > MAX_SPREAD_BPS`, or
     - quote age `> MAX_QUOTE_AGE_SECS`.

4) ATR z‐gate (planner)
   - Requires `z ≥ PLANNER_Z_DEV_THRESH` (e.g., 1.8). This equalizes aggressiveness across low/high volatility names.

5) Sentiment / fundamentals (optional)
   - Sentiment alignment reduces required threshold; fundamentals veto if flow contradicts the side.

6) Top‑K and cool‑down
- Planner considers only the top `PLANNER_TOP_K` symbols per step and enforces a per-symbol cool-down (e.g., 30 min) to reduce churn.

## Monte Carlo universe ranking (optional)
- When `ENABLE_MONTE_CARLO=1`, weekly signal builds call `tools/universe/monte_carlo_sim.py` after LLM screening. Each ranked symbol gets `MONTE_CARLO_SIMS` log-normal paths over `MONTE_CARLO_DAYS` using the most recent `daily_returns` mean/std.
- Results are stored in `universe_monte_carlo` (schema added via `20251120_add_monte_carlo_universe.sql`), embedded inside `signal_universe_100.meta["monte_carlo"]`, and exported to `reports/monte_carlo_*`. With the flag off (default), the weekly build skips this stage entirely and the planner/policy ignore Monte Carlo metadata.
- Metrics: each run optionally pushes `pulse_monte_carlo_duration_seconds`, `pulse_monte_carlo_symbols_processed`, and `pulse_monte_carlo_top_best_score` via `PROM_PUSHGATEWAY`.
- Planner: when Monte Carlo data exists, `build_daily_plans.py` can consume the extra mean/std; otherwise it falls back to the nightly forecast quantiles to estimate mu/σ for `evaluate_plan`.
- Policy: `services/policy/policy/run.py` only enforces `PT_REQUIRE_MONTE_TOP` when Monte Carlo is enabled; the Prometheus gauge `policy_monte_top_overlap` updates accordingly.

## Allocator (long + short)
- Inputs: latest `Idea` objects from forecasts, live prices, sectors, and existing weights.
- Normalization: longs and shorts are split by score sign, raised to `PT_SCORE_EXP`, and normalized independently.
- Caps enforced (per `.env`):
  - Per symbol (`PT_MAX_POS_PCT`, `PT_MAX_SHORT_POS_PCT`).
  - Per sector (`PT_SECTOR_MAX_PCT`, `PT_SECTOR_MAX_SHORT_PCT`).
  - Gross (`PT_GROSS_MAX_PCT`) and net band (`[PT_NET_EXPO_MIN, PT_NET_EXPO_MAX]`).
- Output: dictionary `{symbol: target_weight}`, positive = long, negative = short.
- Cash floor (`PT_CASH_FLOOR_PCT`) carved out before gross allocation, reused by replacement logic.

## Sizing
- Weight delta → shares:
  - `delta_weight = target_weight - cur_weight`
  - `notional = delta_weight × NAV`
  - `shares = round(notional / price)`
  - Skips trades inside `PT_NO_TRADE_BAND_PCT` or below `PT_MIN_ORDER_NOTIONAL`.
- Vol-targeted sizing (planner path):
  - `qty = RISK_DOLLARS_PER_TRADE / ATR`, so a 1×ATR stop size ≈ fixed dollar risk.
- Fallback notional sizing: `qty = LIMIT_NOTIONAL / entry`.

## Entry & exits
- Entry price:
  - With reliable quotes: maker‑style price at `mid − ENTRY_SPREAD_FRAC×spread` for buys (or `+` for sells).
  - Fallback: ATR nudge (`last ± ENTRY_ATR_FRAC×ATR`).
- Brackets (Alpaca):
  - Allowed when whole shares (`qty ≥ 1`) and price ≤ `PLANNER_BRACKET_PRICE_MAX`. Otherwise, use simple limit orders.
- Exit manager for fractionals:
  - If Planner placed a fractional buy (no bracket allowed), posts child exit orders:
    - TP: limit sell at `target`.
    - SL: stop sell at `stop`.
  - Dedupe: checks open orders before posting.

## Baseline policy orders
- Market buys are allowed broadly (subject to spread/age guard and breakers).
- Sells behave differently depending on the allocator output:
  - When `target_weight < cur_weight ≤ 0` (flattening longs), the policy caps the sell qty to the current shares (close-only).
  - When `target_weight < 0` (new short):
    - Close any existing long inventory first.
    - Check shortability cache (`dim_company_profile.shortable`, `.easy_to_borrow`); optionally refresh via Alpaca assets API if cache is stale.
    - Respect per-symbol cooldowns when a short reject occurs (default 10 minutes).
    - If allowed, place a market short-sell order via Alpaca.
    - Optionally attach a trailing buy-to-cover order (`PT_USE_TRAILING_STOPS`, `PT_TRAIL_BUY_TO_COVER_PCT`).
- Buys always attempt to buy-to-cover existing shorts before adding net long exposure (the policy differentiates buy-to-cover vs. fresh long by inspecting current shares).
- Order/position rate limits (`ALPACA_ORDERS_PER_SEC`, `ALPACA_POS_PER_SEC`) enforce API friendliness.

## Circuit breakers
Tables: `circuit_breakers(scope, key, active, expires_at, meta)`.

### Symbol breaker (2‐of‐X heuristic)
Trip when **two** of the following fire within a short window:
- A) Residual shock: z ≤ −4 (forecast‐based), or sustained 2/3 bars with z ≤ −3.
- B) Regime‐scaled move: 15‑min cumulative standardized move ≤ −6 using EWMA vol.
- D) Volume+price concurrence: volume z ≥ 4 **and** price z ≤ −3 in same bar (proxy via trade counts).

Action: Insert an active symbol breaker with TTL (e.g., 60m). Policy/planner skip the symbol while active.

### Global breaker
Trip when fraction of active symbol breakers ≥ `GLOBAL_BREAKER_FRAC` (e.g., 30%).

Action: Insert an active `('global','ALL')` breaker with TTL (e.g., 60m). Policy stops opening new positions while active.

## Throughput controls
- `POLICY_MAX_SUBMITS_PER_STEP` caps new orders per cycle.
- Open‑order dedupe: skips posting if an open order on same side exists for the ticker.
- Order/position rate limiters: `ALPACA_ORDERS_PER_SEC`, `ALPACA_POS_PER_SEC`.

## Environment knobs (high impact)
- Quotes: `MAX_SPREAD_BPS`, `MAX_QUOTE_AGE_SECS`, `ENTRY_SPREAD_FRAC`.
- Sizing: `USE_VOL_TARGETED`, `RISK_DOLLARS_PER_TRADE`, `LIMIT_NOTIONAL`.
- Planner: `PLANNER_Z_DEV_THRESH`, `PLANNER_ENTRY_ATR_FRAC`, `PLANNER_TOP_K`, `PLANNER_COOLDOWN_MINS`, `PLANNER_BRACKET_PRICE_MAX`.
- Exit mgr: `ENABLE_EXIT_MANAGER`.
- Blackout: `ENABLE_EARNINGS_BLACKOUT`, `EARNINGS_BLACKOUT_MIN`.
- Breakers: `ENABLE_CIRCUIT_BREAKERS`, `SYMBOL_BREAKER_TTL_MIN`, `GLOBAL_BREAKER_FRAC`, `GLOBAL_BREAKER_TTL_MIN`, `EWMA_LAMBDA`.

## Automation (scheduler service)
- `docker/scheduler.cron` holds the nightly/weekly cron entries (dataset refresh, Friday N-BEATS retrain + Top‑100 rebuild, nightly forecasts, nightly planner).
- Start the cron daemon with `docker compose up -d scheduler`; edit the cron file and `docker compose restart scheduler` when timings change.

## Control flow (per cycle)
1) Load active knobs/breakers.
2) Baseline policy dev check → spread/age guard → position checks → market order (close‐only sells).
3) Build planner universe → z‑gate, sentiment/fundamental gates → spread/age guard → limit entries with bracket if allowed.
4) Exit manager posts TP/SL for fractional entries without brackets.
5) Symbol breaker evaluation; global breaker escalation if many symbols trip.
