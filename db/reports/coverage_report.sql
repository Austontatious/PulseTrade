\set lookback 120
WITH horizon AS (
  SELECT (CURRENT_DATE - (:lookback || ' days')::interval)::date AS start_date
),
per_symbol AS (
  SELECT symbol, COUNT(*) AS have_days, MIN(ds) AS first_day, MAX(ds) AS last_day
  FROM daily_returns, horizon
  WHERE ds >= horizon.start_date
  GROUP BY symbol
)
SELECT s.ticker,
       COALESCE(p.have_days, 0) AS have_days,
       p.first_day,
       p.last_day
FROM symbols s
LEFT JOIN per_symbol p ON p.symbol = s.ticker
WHERE s.class = 'equity'
ORDER BY have_days ASC, s.ticker
LIMIT 100;
