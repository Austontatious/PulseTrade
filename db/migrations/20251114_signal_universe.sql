CREATE TABLE IF NOT EXISTS signal_universe_100 (
  as_of date NOT NULL,
  symbol TEXT NOT NULL,
  signal_strength DOUBLE PRECISION NOT NULL,
  rank INTEGER NOT NULL,
  meta JSONB DEFAULT '{}'::jsonb,
  PRIMARY KEY (as_of, symbol)
);

CREATE INDEX IF NOT EXISTS signal_universe_100_rank_idx
  ON signal_universe_100 (as_of, rank);

CREATE OR REPLACE VIEW trading_universe_100 AS
SELECT symbol
FROM signal_universe_100
WHERE as_of = (SELECT MAX(as_of) FROM signal_universe_100)
ORDER BY rank ASC
LIMIT 100;
