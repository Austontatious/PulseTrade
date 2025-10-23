CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Symbols universe
CREATE TABLE IF NOT EXISTS symbols (
  id SERIAL PRIMARY KEY,
  ticker TEXT NOT NULL,
  class TEXT NOT NULL,           -- equity, crypto
  venue TEXT,                    -- NASDAQ, NYSE, COINBASE, KRAKEN
  meta JSONB DEFAULT '{}'::jsonb,
  UNIQUE (ticker, class)
);

CREATE TABLE IF NOT EXISTS dim_company_profile (
  symbol TEXT PRIMARY KEY,
  name TEXT,
  sector TEXT,
  industry TEXT,
  country TEXT,
  meta JSONB DEFAULT '{}'::jsonb
);

-- Raw market data (crypto starter: trades)
CREATE TABLE IF NOT EXISTS trades (
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  size DOUBLE PRECISION,
  venue TEXT,
  meta JSONB DEFAULT '{}'::jsonb
);
SELECT create_hypertable('trades','ts', if_not_exists => TRUE);

-- Orderbook snapshots (optional)
CREATE TABLE IF NOT EXISTS orderbooks (
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT NOT NULL,
  venue TEXT,
  bids JSONB,  -- [[price, size], ...]
  asks JSONB
);
SELECT create_hypertable('orderbooks','ts', if_not_exists => TRUE);

-- Social/Emotion Streams
CREATE TABLE IF NOT EXISTS social_messages (
  id BIGSERIAL,
  ts TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,          -- stocktwits, twitter, truth
  handle TEXT,
  ticker TEXT,
  text TEXT,
  sentiment REAL,                -- -1..1
  engagement INTEGER,
  meta JSONB DEFAULT '{}'::jsonb,
  PRIMARY KEY (id, ts)
);
SELECT create_hypertable('social_messages','ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS social_features (
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT NOT NULL,
  window_minutes INTEGER NOT NULL,
  msg_rate DOUBLE PRECISION,
  senti_mean DOUBLE PRECISION,
  senti_std DOUBLE PRECISION,
  top_handles JSONB DEFAULT '[]'::jsonb,
  PRIMARY KEY (ts, ticker, window_minutes)
);
SELECT create_hypertable('social_features','ts', if_not_exists => TRUE);

-- Analyst ratings / targets
CREATE TABLE IF NOT EXISTS analyst_ratings (
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT NOT NULL,
  provider TEXT NOT NULL,        -- finnhub, fmp, marketbeat
  firm TEXT,
  analyst TEXT,
  action TEXT,                   -- upgrade/downgrade/maintain
  rating TEXT,                   -- buy/hold/sell or numeric
  target DOUBLE PRECISION,
  meta JSONB DEFAULT '{}'::jsonb
);
SELECT create_hypertable('analyst_ratings','ts', if_not_exists => TRUE);

-- Forecasts
CREATE TABLE IF NOT EXISTS forecasts (
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT NOT NULL,
  horizon TEXT NOT NULL,         -- e.g., "1m", "5m"
  model TEXT NOT NULL,
  mean DOUBLE PRECISION,
  lower DOUBLE PRECISION,
  upper DOUBLE PRECISION,
  features JSONB DEFAULT '{}'::jsonb
);
SELECT create_hypertable('forecasts','ts', if_not_exists => TRUE);

-- Positions & fills (policy/execution)
CREATE TABLE IF NOT EXISTS positions (
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT NOT NULL,
  qty DOUBLE PRECISION NOT NULL,
  avg_price DOUBLE PRECISION,
  meta JSONB DEFAULT '{}'::jsonb
);
SELECT create_hypertable('positions','ts', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS fills (
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT NOT NULL,
  side TEXT NOT NULL,            -- buy/sell
  qty DOUBLE PRECISION NOT NULL,
  price DOUBLE PRECISION NOT NULL,
  venue TEXT,
  meta JSONB DEFAULT '{}'::jsonb
);
SELECT create_hypertable('fills','ts', if_not_exists => TRUE);

-- Simple registry of rate-limit hits
CREATE TABLE IF NOT EXISTS rate_events (
  ts TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL,
  kind TEXT NOT NULL,            -- http, ws, parse, backoff
  meta JSONB DEFAULT '{}'::jsonb
);
SELECT create_hypertable('rate_events','ts', if_not_exists => TRUE);

-- Quotes (for spread/latency filters)
CREATE TABLE IF NOT EXISTS quotes (
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT NOT NULL,
  bid DOUBLE PRECISION,
  ask DOUBLE PRECISION,
  venue TEXT,
  meta JSONB DEFAULT '{}'::jsonb
);
SELECT create_hypertable('quotes','ts', if_not_exists => TRUE);

-- Event windows (earnings/macro blackouts)
CREATE TABLE IF NOT EXISTS event_windows (
  id BIGSERIAL PRIMARY KEY,
  ticker TEXT,
  start_ts TIMESTAMPTZ NOT NULL,
  end_ts TIMESTAMPTZ NOT NULL,
  kind TEXT NOT NULL,            -- earnings, macro
  meta JSONB DEFAULT '{}'::jsonb
);

-- Circuit breakers log
CREATE TABLE IF NOT EXISTS circuit_breakers (
  ts TIMESTAMPTZ NOT NULL,
  scope TEXT NOT NULL,      -- symbol, sector, global
  key TEXT NOT NULL,        -- ticker, sector name, or 'ALL'
  active BOOLEAN NOT NULL,
  reason TEXT,
  expires_at TIMESTAMPTZ,
  meta JSONB DEFAULT '{}'::jsonb
);

-- Strategist: recommendations and policy knobs
CREATE TABLE IF NOT EXISTS strategist_recos (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  ticker TEXT NOT NULL,
  side TEXT NOT NULL,            -- buy/sell
  score DOUBLE PRECISION NOT NULL,
  horizon TEXT,
  reason TEXT,
  meta JSONB DEFAULT '{}'::jsonb
);
SELECT create_hypertable('strategist_recos','ts', if_not_exists => TRUE);

-- FMP fundamentals payload tables (denormalized jsonb for now)
CREATE TABLE IF NOT EXISTS fmp_income_statement (
  symbol TEXT NOT NULL,
  period TEXT NOT NULL,
  date DATE NOT NULL,
  payload JSONB NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, period, date)
);

CREATE TABLE IF NOT EXISTS fmp_balance_sheet (
  symbol TEXT NOT NULL,
  period TEXT NOT NULL,
  date DATE NOT NULL,
  payload JSONB NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, period, date)
);

CREATE TABLE IF NOT EXISTS fmp_cash_flow (
  symbol TEXT NOT NULL,
  period TEXT NOT NULL,
  date DATE NOT NULL,
  payload JSONB NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, period, date)
);

CREATE TABLE IF NOT EXISTS fmp_key_metrics (
  symbol TEXT NOT NULL,
  period TEXT NOT NULL,
  date DATE NOT NULL,
  payload JSONB NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, period, date)
);

CREATE TABLE IF NOT EXISTS fmp_ratios (
  symbol TEXT NOT NULL,
  period TEXT NOT NULL,
  date DATE NOT NULL,
  payload JSONB NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (symbol, period, date)
);

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_liquidity_60d AS
SELECT
  r.symbol,
  AVG(ABS(r.y)) AS avg_abs_ret_60d,
  AVG(r.dollar_vol) AS avg_dollar_vol_60d
FROM daily_returns r
WHERE r.ds >= (CURRENT_DATE - INTERVAL '90 days')
GROUP BY r.symbol;

CREATE INDEX IF NOT EXISTS idx_mv_liquidity_60d_symbol ON mv_liquidity_60d(symbol);

CREATE OR REPLACE FUNCTION winsor(x numeric, p1 numeric, p99 numeric)
RETURNS numeric
LANGUAGE SQL
IMMUTABLE
RETURNS NULL ON NULL INPUT
AS $$
  SELECT LEAST(GREATEST(x, p1), p99)
$$;

CREATE OR REPLACE VIEW fundamentals_factors_v1 AS
WITH calendar_symbols AS (
  SELECT DISTINCT ds, symbol
  FROM daily_returns
),
fundamentals AS (
  SELECT
    i.symbol,
    i.period,
    i.date::date AS rpt_date,
    NULLIF(i.payload->>'revenue', '')::numeric AS revenue,
    NULLIF(i.payload->>'ebit', '')::numeric AS ebit,
    NULLIF(i.payload->>'netIncome', '')::numeric AS net_income,
    NULLIF(i.payload->>'grossProfit', '')::numeric AS gross_profit,
    NULLIF(i.payload->>'interestExpense', '')::numeric AS interest_expense,
    NULLIF(b.payload->>'totalAssets', '')::numeric AS total_assets,
    NULLIF(b.payload->>'totalLiabilities', '')::numeric AS total_liabilities,
    NULLIF(b.payload->>'totalDebt', '')::numeric AS total_debt,
    NULLIF(b.payload->>'cashAndShortTermInvestments', '')::numeric AS cash_sti,
    NULLIF(c.payload->>'operatingCashFlow', '')::numeric AS ocf,
    NULLIF(c.payload->>'capitalExpenditure', '')::numeric AS capex,
    NULLIF(m.payload->>'enterpriseValue', '')::numeric AS enterprise_value,
    NULLIF(m.payload->>'marketCap', '')::numeric AS market_cap,
    NULLIF(r.payload->>'roe', '')::numeric AS roe_ratio,
    NULLIF(r.payload->>'roa', '')::numeric AS roa_ratio
  FROM fmp_income_statement i
  LEFT JOIN fmp_balance_sheet b USING (symbol, period, date)
  LEFT JOIN fmp_cash_flow c USING (symbol, period, date)
  LEFT JOIN fmp_key_metrics m USING (symbol, period, date)
  LEFT JOIN fmp_ratios r USING (symbol, period, date)
),
fundamentals_growth AS (
  SELECT
    f.*,
    CASE
      WHEN f.period = 'annual'
           AND LAG(f.revenue) OVER (PARTITION BY f.symbol, f.period ORDER BY f.rpt_date) IS NOT NULL
           AND LAG(f.revenue) OVER (PARTITION BY f.symbol, f.period ORDER BY f.rpt_date) <> 0
      THEN (f.revenue - LAG(f.revenue) OVER (PARTITION BY f.symbol, f.period ORDER BY f.rpt_date))
           / LAG(f.revenue) OVER (PARTITION BY f.symbol, f.period ORDER BY f.rpt_date)
      ELSE NULL
    END AS revenue_yoy
  FROM fundamentals f
),
lagged AS (
  SELECT
    cs.ds,
    cs.symbol,
    f.*
  FROM calendar_symbols cs
  LEFT JOIN LATERAL (
    SELECT *
    FROM fundamentals_growth f
    WHERE f.symbol = cs.symbol
      AND f.rpt_date <= cs.ds - INTERVAL '2 days'
    ORDER BY f.rpt_date DESC
    LIMIT 1
  ) f ON TRUE
),
with_factors AS (
  SELECT
    l.ds,
    l.symbol,
    l.period,
    dcp.sector,
    l.market_cap,
    CASE WHEN l.market_cap > 0 THEN LN(l.market_cap) END AS log_mktcap,
    CASE
      WHEN l.market_cap > 0 AND (l.total_assets - l.total_liabilities) IS NOT NULL
      THEN (l.total_assets - l.total_liabilities) / l.market_cap
    END AS book_to_market,
    CASE WHEN l.market_cap > 0 AND l.net_income IS NOT NULL THEN l.net_income / l.market_cap END AS earnings_yield,
    CASE WHEN l.enterprise_value > 0 AND l.ebit IS NOT NULL THEN l.ebit / l.enterprise_value END AS ebit_to_ev,
    CASE WHEN l.revenue > 0 AND l.gross_profit IS NOT NULL THEN l.gross_profit / l.revenue END AS gross_margin,
    CASE WHEN l.revenue <> 0 AND l.ebit IS NOT NULL THEN l.ebit / l.revenue END AS operating_margin,
    CASE WHEN l.revenue <> 0 AND l.net_income IS NOT NULL THEN l.net_income / l.revenue END AS net_margin,
    CASE WHEN l.total_assets <> 0 AND l.net_income IS NOT NULL THEN l.net_income / l.total_assets END AS roa,
    l.roe_ratio AS roe,
    CASE WHEN l.total_assets <> 0 AND l.total_debt IS NOT NULL THEN l.total_debt / l.total_assets END AS debt_to_assets,
    CASE
      WHEN l.ebit IS NOT NULL
           AND (l.total_debt - COALESCE(l.cash_sti, 0)) IS NOT NULL
           AND l.ebit <> 0
      THEN (l.total_debt - COALESCE(l.cash_sti, 0)) / l.ebit
    END AS net_debt_to_ebitda,
    CASE
      WHEN l.interest_expense IS NOT NULL
           AND l.interest_expense <> 0
           AND l.ebit IS NOT NULL
      THEN l.ebit / l.interest_expense
    END AS interest_coverage,
    CASE
      WHEN l.total_assets <> 0
           AND l.ocf IS NOT NULL
           AND l.net_income IS NOT NULL
      THEN (l.net_income - l.ocf) / l.total_assets
    END AS accruals,
    CASE WHEN l.total_assets <> 0 AND l.capex IS NOT NULL THEN l.capex / l.total_assets END AS capex_to_assets,
    l.revenue_yoy
  FROM lagged l
  LEFT JOIN dim_company_profile dcp ON dcp.symbol = l.symbol
),
stats AS (
  SELECT
    ds,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY log_mktcap) AS p_log_mktcap,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY book_to_market) AS p_bm,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY earnings_yield) AS p_ey,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY ebit_to_ev) AS p_ebit_ev,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY gross_margin) AS p_gm,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY operating_margin) AS p_om,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY net_margin) AS p_nm,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY roa) AS p_roa,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY roe) AS p_roe,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY debt_to_assets) AS p_dta,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY net_debt_to_ebitda) AS p_nd_ebitda,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY interest_coverage) AS p_ic,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY accruals) AS p_accr,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY capex_to_assets) AS p_cta,
    percentile_cont(ARRAY[0.01, 0.99]) WITHIN GROUP (ORDER BY revenue_yoy) AS p_rev
  FROM with_factors
  GROUP BY ds
),
wins AS (
  SELECT
    f.*,
    winsor(log_mktcap, p.p_log_mktcap[1], p.p_log_mktcap[2]) AS w_log_mktcap,
    winsor(book_to_market, p.p_bm[1], p.p_bm[2]) AS w_bm,
    winsor(earnings_yield, p.p_ey[1], p.p_ey[2]) AS w_ey,
    winsor(ebit_to_ev, p.p_ebit_ev[1], p.p_ebit_ev[2]) AS w_ebit_ev,
    winsor(gross_margin, p.p_gm[1], p.p_gm[2]) AS w_gm,
    winsor(operating_margin, p.p_om[1], p.p_om[2]) AS w_om,
    winsor(net_margin, p.p_nm[1], p.p_nm[2]) AS w_nm,
    winsor(roa, p.p_roa[1], p.p_roa[2]) AS w_roa,
    winsor(roe, p.p_roe[1], p.p_roe[2]) AS w_roe,
    winsor(debt_to_assets, p.p_dta[1], p.p_dta[2]) AS w_dta,
    winsor(net_debt_to_ebitda, p.p_nd_ebitda[1], p.p_nd_ebitda[2]) AS w_nd_ebitda,
    winsor(interest_coverage, p.p_ic[1], p.p_ic[2]) AS w_ic,
    winsor(accruals, p.p_accr[1], p.p_accr[2]) AS w_accr,
    winsor(capex_to_assets, p.p_cta[1], p.p_cta[2]) AS w_cta,
    winsor(revenue_yoy, p.p_rev[1], p.p_rev[2]) AS w_rev
  FROM with_factors f
  JOIN stats p USING (ds)
),
zs AS (
  SELECT
    w.*,
    (w_log_mktcap - AVG(w_log_mktcap) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_log_mktcap) OVER (PARTITION BY ds), 0) AS z_log_mktcap,
    (w_bm - AVG(w_bm) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_bm) OVER (PARTITION BY ds), 0) AS z_bm,
    (w_ey - AVG(w_ey) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_ey) OVER (PARTITION BY ds), 0) AS z_ey,
    (w_ebit_ev - AVG(w_ebit_ev) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_ebit_ev) OVER (PARTITION BY ds), 0) AS z_ebit_ev,
    (w_gm - AVG(w_gm) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_gm) OVER (PARTITION BY ds), 0) AS z_gm,
    (w_om - AVG(w_om) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_om) OVER (PARTITION BY ds), 0) AS z_om,
    (w_nm - AVG(w_nm) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_nm) OVER (PARTITION BY ds), 0) AS z_nm,
    (w_roa - AVG(w_roa) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_roa) OVER (PARTITION BY ds), 0) AS z_roa,
    (w_roe - AVG(w_roe) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_roe) OVER (PARTITION BY ds), 0) AS z_roe,
    (w_dta - AVG(w_dta) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_dta) OVER (PARTITION BY ds), 0) AS z_dta,
    (w_nd_ebitda - AVG(w_nd_ebitda) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_nd_ebitda) OVER (PARTITION BY ds), 0) AS z_nd_ebitda,
    (w_ic - AVG(w_ic) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_ic) OVER (PARTITION BY ds), 0) AS z_ic,
    (w_accr - AVG(w_accr) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_accr) OVER (PARTITION BY ds), 0) AS z_accr,
    (w_cta - AVG(w_cta) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_cta) OVER (PARTITION BY ds), 0) AS z_cta,
    (w_rev - AVG(w_rev) OVER (PARTITION BY ds))
      / NULLIF(STDDEV_SAMP(w_rev) OVER (PARTITION BY ds), 0) AS z_rev
  FROM wins w
),
zs_sector AS (
  SELECT
    z.*,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_log_mktcap - AVG(z_log_mktcap) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_log_mktcap) OVER (PARTITION BY ds, sector), 0)
    END AS zn_log_mktcap,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_bm - AVG(z_bm) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_bm) OVER (PARTITION BY ds, sector), 0)
    END AS zn_bm,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_ey - AVG(z_ey) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_ey) OVER (PARTITION BY ds, sector), 0)
    END AS zn_ey,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_ebit_ev - AVG(z_ebit_ev) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_ebit_ev) OVER (PARTITION BY ds, sector), 0)
    END AS zn_ebit_ev,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_gm - AVG(z_gm) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_gm) OVER (PARTITION BY ds, sector), 0)
    END AS zn_gm,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_om - AVG(z_om) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_om) OVER (PARTITION BY ds, sector), 0)
    END AS zn_om,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_nm - AVG(z_nm) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_nm) OVER (PARTITION BY ds, sector), 0)
    END AS zn_nm,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_roa - AVG(z_roa) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_roa) OVER (PARTITION BY ds, sector), 0)
    END AS zn_roa,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_roe - AVG(z_roe) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_roe) OVER (PARTITION BY ds, sector), 0)
    END AS zn_roe,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_dta - AVG(z_dta) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_dta) OVER (PARTITION BY ds, sector), 0)
    END AS zn_dta,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_nd_ebitda - AVG(z_nd_ebitda) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_nd_ebitda) OVER (PARTITION BY ds, sector), 0)
    END AS zn_nd_ebitda,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_ic - AVG(z_ic) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_ic) OVER (PARTITION BY ds, sector), 0)
    END AS zn_ic,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_accr - AVG(z_accr) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_accr) OVER (PARTITION BY ds, sector), 0)
    END AS zn_accr,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_cta - AVG(z_cta) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_cta) OVER (PARTITION BY ds, sector), 0)
    END AS zn_cta,
    CASE
      WHEN sector IS NULL THEN NULL
      ELSE (z_rev - AVG(z_rev) OVER (PARTITION BY ds, sector))
             / NULLIF(STDDEV_SAMP(z_rev) OVER (PARTITION BY ds, sector), 0)
    END AS zn_rev
  FROM zs z
)
SELECT
  ds,
  symbol,
  sector,
  w_log_mktcap,
  w_bm,
  w_ey,
  w_ebit_ev,
  w_gm,
  w_om,
  w_nm,
  w_roa,
  w_roe,
  w_dta,
  w_nd_ebitda,
  w_ic,
  w_accr,
  w_cta,
  w_rev,
  z_log_mktcap,
  z_bm,
  z_ey,
  z_ebit_ev,
  z_gm,
  z_om,
  z_nm,
  z_roa,
  z_roe,
  z_dta,
  z_nd_ebitda,
  z_ic,
  z_accr,
  z_cta,
  z_rev,
  zn_log_mktcap,
  zn_bm,
  zn_ey,
  zn_ebit_ev,
  zn_gm,
  zn_om,
  zn_nm,
  zn_roa,
  zn_roe,
  zn_dta,
  zn_nd_ebitda,
  zn_ic,
  zn_accr,
  zn_cta,
  zn_rev
FROM zs_sector;

CREATE TABLE IF NOT EXISTS policy_knobs (
  id BIGSERIAL PRIMARY KEY,
  ts TIMESTAMPTZ NOT NULL,
  key TEXT NOT NULL,
  value JSONB NOT NULL,
  expires_at TIMESTAMPTZ,
  meta JSONB DEFAULT '{}'::jsonb
);
