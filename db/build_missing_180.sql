BEGIN;

-- Load the Alpaca universe into a temp table
CREATE TEMP TABLE tmp_universe(symbol text PRIMARY KEY) ON COMMIT DROP;
\copy tmp_universe(symbol) FROM '/mnt/data/PulseTrade/db/alpaca_universe.symbols.txt' WITH (FORMAT text);

-- Compute missing-rows (last 180 calendar days, weekdays only, slack=3)
WITH params AS (
  SELECT CURRENT_DATE::date AS today,
         (CURRENT_DATE - INTERVAL '180 days')::date AS window_start,
         3::int AS slack
),
per AS (
  SELECT
    u.symbol,
    COALESCE((
      SELECT COUNT(*) FROM daily_returns dr
      WHERE dr.symbol = u.symbol
        AND dr.ds >= (SELECT window_start FROM params)
    ), 0) AS have_days,
    GREATEST(
      COALESCE((SELECT MIN(dr2.ds)::date FROM daily_returns dr2 WHERE dr2.symbol = u.symbol),
               (SELECT window_start FROM params)),
      (SELECT window_start FROM params)
    ) AS eff_start
  FROM tmp_universe u
),
need AS (
  SELECT
    p.symbol,
    p.have_days,
    (
      SELECT COUNT(*)
      FROM generate_series(p.eff_start, (SELECT today FROM params), INTERVAL '1 day') g(d)
      WHERE EXTRACT(ISODOW FROM g.d) BETWEEN 1 AND 5
    ) AS required_rows
  FROM per p
),
final AS (
  SELECT
    symbol,
    have_days,
    required_rows,
    GREATEST(required_rows - have_days - 1 - (SELECT slack FROM params), 0) AS missing_rows
  FROM need
)

-- Keep a detail temp with counts so we can both export + summarize
SELECT * INTO TEMP tmp_missing_calc FROM final;

-- Symbols needing rows
CREATE TEMP TABLE tmp_missing_symbols(symbol text PRIMARY KEY) ON COMMIT DROP;
INSERT INTO tmp_missing_symbols(symbol)
SELECT symbol FROM tmp_missing_calc
WHERE missing_rows > 0
ORDER BY symbol;

-- Summary to stdout
SELECT
  COUNT(*) FILTER (WHERE missing_rows = 0)  AS full_coverage,
  COUNT(*) FILTER (WHERE missing_rows > 0)  AS remaining_without_full
FROM tmp_missing_calc;

-- Export the missing list for the backfill script
\copy tmp_missing_symbols(symbol) TO '/workspace/tmp_missing.txt' WITH (FORMAT text);

COMMIT;
