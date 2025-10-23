DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_proc
    WHERE proname = 'winsor'
      AND pg_function_is_visible(oid)
  ) THEN
    CREATE OR REPLACE FUNCTION winsor(x numeric, p1 numeric, p99 numeric)
    RETURNS numeric LANGUAGE SQL IMMUTABLE AS
    $$ SELECT LEAST(GREATEST(x, p1), p99) $$;
  END IF;
END$$;

DROP VIEW IF EXISTS fundamentals_factors_v1;

WITH base AS (
  SELECT
    i.symbol,
    i.date::date AS rpt_date,
    (i.payload->>'revenue')::numeric            AS revenue,
    (i.payload->>'ebit')::numeric               AS ebit,
    (i.payload->>'netIncome')::numeric          AS net_income,
    (i.payload->>'grossProfit')::numeric        AS gross_profit,
    (i.payload->>'interestExpense')::numeric    AS interest_expense,
    (b.payload->>'totalAssets')::numeric        AS total_assets,
    (b.payload->>'totalLiabilities')::numeric   AS total_liabilities,
    (b.payload->>'totalDebt')::numeric          AS total_debt,
    (b.payload->>'cashAndShortTermInvestments')::numeric AS cash_sti,
    (c.payload->>'operatingCashFlow')::numeric  AS ocf,
    (c.payload->>'capitalExpenditure')::numeric AS capex,
    (m.payload->>'enterpriseValue')::numeric    AS ev,
    (m.payload->>'marketCap')::numeric          AS mktcap,
    (r.payload->>'roe')::numeric                AS roe_ratio,
    (r.payload->>'roa')::numeric                AS roa_ratio,
    i.period
  FROM fmp_income_statement i
  JOIN fmp_balance_sheet b USING (symbol, period, date)
  JOIN fmp_cash_flow c      USING (symbol, period, date)
  LEFT JOIN fmp_key_metrics m USING (symbol, period, date)
  LEFT JOIN fmp_ratios r      USING (symbol, period, date)
),
calendar AS (
  SELECT generate_series(date '1990-01-01', CURRENT_DATE, interval '1 day')::date AS ds
),
latest_for_day AS (
  SELECT c.ds, b.*
  FROM calendar c
  JOIN LATERAL (
    SELECT *
    FROM base x
    WHERE (x.rpt_date + interval '45 days')::date <= c.ds
    ORDER BY (x.rpt_date + interval '45 days') DESC
    LIMIT 1
  ) b ON TRUE
),
raw AS (
  SELECT
    l.ds,
    l.symbol,
    l.mktcap,
    CASE WHEN l.mktcap > 0 THEN LN(l.mktcap) END AS log_mktcap,
    CASE
      WHEN (l.total_assets - l.total_liabilities) > 0 AND l.mktcap > 0
      THEN (l.total_assets - l.total_liabilities) / l.mktcap
    END AS book_to_market,
    CASE WHEN l.mktcap > 0 AND l.net_income IS NOT NULL
      THEN l.net_income / l.mktcap END AS earnings_yield,
    CASE WHEN l.ev > 0 AND l.ebit IS NOT NULL
      THEN l.ebit / l.ev END AS ebit_to_ev,
    CASE WHEN l.revenue > 0 AND l.gross_profit IS NOT NULL
      THEN l.gross_profit / l.revenue END AS gross_margin,
    CASE WHEN l.revenue > 0 AND l.ebit IS NOT NULL
      THEN l.ebit / l.revenue END AS operating_margin,
    CASE WHEN l.revenue > 0 AND l.net_income IS NOT NULL
      THEN l.net_income / l.revenue END AS net_margin,
    CASE WHEN l.total_assets > 0 AND l.net_income IS NOT NULL
      THEN l.net_income / l.total_assets END AS roa,
    l.roe_ratio AS roe,
    CASE WHEN l.total_assets > 0 AND l.total_debt IS NOT NULL
      THEN l.total_debt / l.total_assets END AS debt_to_assets,
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
      WHEN l.total_assets > 0
           AND l.ocf IS NOT NULL
           AND l.net_income IS NOT NULL
      THEN (l.net_income - l.ocf) / l.total_assets
    END AS accruals,
    CASE WHEN l.total_assets > 0 AND l.capex IS NOT NULL
      THEN l.capex / l.total_assets END AS capex_to_assets
  FROM latest_for_day l
),
wins AS (
  SELECT
    r.*,
    r.log_mktcap        AS w_log_mktcap,
    r.book_to_market    AS w_bm,
    r.earnings_yield    AS w_ey,
    r.ebit_to_ev        AS w_ebit_ev,
    r.gross_margin      AS w_gm,
    r.operating_margin  AS w_om,
    r.net_margin        AS w_nm,
    r.roa               AS w_roa,
    r.roe               AS w_roe,
    r.debt_to_assets    AS w_dta,
    r.net_debt_to_ebitda AS w_nd_ebitda,
    r.interest_coverage AS w_ic,
    r.accruals          AS w_accr,
    r.capex_to_assets   AS w_cta
  FROM raw r
),
zs AS (
  SELECT
    ds,
    symbol,
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
      / NULLIF(STDDEV_SAMP(w_cta) OVER (PARTITION BY ds), 0) AS z_cta
  FROM wins
)
CREATE VIEW fundamentals_factors_v1 AS
SELECT
  w.ds,
  w.symbol,
  w.w_log_mktcap,
  w.w_bm,
  w.w_ey,
  w.w_ebit_ev,
  w.w_gm,
  w.w_om,
  w.w_nm,
  w.w_roa,
  w.w_roe,
  w.w_dta,
  w.w_nd_ebitda,
  w.w_ic,
  w.w_accr,
  w.w_cta,
  NULL::numeric AS w_rev,
  z.z_log_mktcap,
  z.z_bm,
  z.z_ey,
  z.z_ebit_ev,
  z.z_gm,
  z.z_om,
  z.z_nm,
  z.z_roa,
  z.z_roe,
  z.z_dta,
  z.z_nd_ebitda,
  z.z_ic,
  z.z_accr,
  z.z_cta,
  NULL::numeric AS z_rev
FROM wins w
JOIN zs z USING (ds, symbol);
