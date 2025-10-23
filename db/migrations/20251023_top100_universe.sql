-- prerequisite: mv_liquidity_60d must exist. Uncomment to create if missing.
-- CREATE MATERIALIZED VIEW IF NOT EXISTS mv_liquidity_60d AS
-- SELECT r.symbol,
--        AVG(ABS(r.y))      AS avg_abs_ret_60d,
--        AVG(r.dollar_vol)  AS avg_dollar_vol_60d
-- FROM daily_returns r
-- WHERE r.ds >= (CURRENT_DATE - INTERVAL '90 days')
-- GROUP BY r.symbol;
-- CREATE INDEX IF NOT EXISTS idx_mv_liquidity_60d_symbol ON mv_liquidity_60d(symbol);

CREATE OR REPLACE VIEW universe_candidates_daily AS
SELECT
  s.symbol,
  l.avg_dollar_vol_60d,
  COALESCE(s.spread_bps_5d, 25) AS spread_bps_5d,
  ROW_NUMBER() OVER (
    ORDER BY l.avg_dollar_vol_60d DESC,
             COALESCE(s.spread_bps_5d, 25) ASC
  ) AS liquidity_rank
FROM mv_liquidity_60d l
JOIN symbols s ON s.symbol = l.symbol
WHERE COALESCE(s.is_otc, false) = false
  AND COALESCE(s.is_active, true) = true;

CREATE OR REPLACE VIEW trading_universe_100 AS
SELECT symbol
FROM universe_candidates_daily
WHERE liquidity_rank <= 100;
