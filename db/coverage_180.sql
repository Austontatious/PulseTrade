CREATE OR REPLACE VIEW coverage_180_final AS
WITH params AS (
  SELECT
    (CURRENT_DATE - INTERVAL '180 days')::date AS window_start,
    LEAST(
      CURRENT_DATE::date,
      COALESCE((SELECT MAX(ds) FROM daily_returns), CURRENT_DATE::date)
    ) AS last_bar,
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
    ) AS eff_start,
    COALESCE(
      (SELECT MAX(dr3.ds)::date FROM daily_returns dr3 WHERE dr3.symbol = u.symbol),
      (SELECT last_bar FROM params)
    ) AS last_bar
  FROM alpaca_universe u
),
need AS (
  SELECT
    p.symbol,
    p.have_days,
    (
      SELECT COUNT(*)
      FROM generate_series(p.eff_start, (SELECT last_bar FROM params), INTERVAL '1 day') g(d)
      WHERE EXTRACT(ISODOW FROM g.d) BETWEEN 1 AND 5
    ) AS required_rows,
    p.last_bar
  FROM per p
)
SELECT
  symbol,
  have_days,
  required_rows,
  GREATEST(required_rows - have_days - 1 - (SELECT slack FROM params), 0) AS missing_rows,
  last_bar
FROM need;
