CREATE TABLE IF NOT EXISTS ingest_metrics (
  id BIGSERIAL PRIMARY KEY,
  symbol TEXT NOT NULL,
  as_of DATE NOT NULL,
  metric TEXT NOT NULL,
  value DOUBLE PRECISION NOT NULL,
  window TEXT NOT NULL,
  src TEXT NOT NULL,
  raw JSONB,
  UNIQUE(symbol, as_of, metric, window, src)
);

CREATE INDEX IF NOT EXISTS ingest_metrics_idx ON ingest_metrics (symbol, as_of, metric);
