-- Indexes for hot paths
CREATE INDEX IF NOT EXISTS trades_ticker_ts ON trades (ticker, ts DESC);
CREATE INDEX IF NOT EXISTS forecasts_ticker_ts ON forecasts (ticker, ts DESC);
CREATE INDEX IF NOT EXISTS social_ticker_ts ON social_messages (ticker, ts DESC);
CREATE INDEX IF NOT EXISTS ratings_ticker_ts ON analyst_ratings (ticker, ts DESC);
CREATE INDEX IF NOT EXISTS social_features_ticker_ts ON social_features (ticker, ts DESC);

-- Compression policies
ALTER TABLE trades SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'ts DESC',
    timescaledb.compress_segmentby = 'ticker'
);
SELECT add_compression_policy('trades', INTERVAL '1 day');

ALTER TABLE forecasts SET (
    timescaledb.compress,
    timescaledb.compress_orderby = 'ts DESC',
    timescaledb.compress_segmentby = 'ticker'
);
SELECT add_compression_policy('forecasts', INTERVAL '1 day');

-- Retention policies
SELECT add_retention_policy('trades', INTERVAL '30 days');
SELECT add_retention_policy('forecasts', INTERVAL '30 days');
SELECT add_retention_policy('social_messages', INTERVAL '90 days');
SELECT add_retention_policy('analyst_ratings', INTERVAL '365 days');
SELECT add_retention_policy('social_features', INTERVAL '180 days');
