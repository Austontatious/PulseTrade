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
