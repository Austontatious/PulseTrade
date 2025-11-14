import os
import psycopg2

SQL = r"""
CREATE OR REPLACE VIEW fmp_revisions_cs AS
WITH latest_two AS (
  SELECT
    symbol,
    date,
    payload,
    ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY date DESC) AS rn
  FROM fmp_analyst_estimates
), pairs AS (
  SELECT
    l.symbol,
    l.payload AS p0,
    p.payload AS p1
  FROM latest_two l
  LEFT JOIN latest_two p
    ON p.symbol = l.symbol AND p.rn = 2
  WHERE l.rn = 1
), revs AS (
  SELECT
    symbol,
    -- compute normalized deltas with simple fallback
    (
      COALESCE((p0->>'estimatedEpsAvg')::numeric,
               (p0->>'estimatedEpsLow')::numeric,
               (p0->>'estimatedEpsHigh')::numeric)
      - COALESCE((p1->>'estimatedEpsAvg')::numeric,
                 (p1->>'estimatedEpsLow')::numeric,
                 (p1->>'estimatedEpsHigh')::numeric)
    ) / NULLIF(ABS(COALESCE((p1->>'estimatedEpsAvg')::numeric,
                            (p1->>'estimatedEpsLow')::numeric,
                            (p1->>'estimatedEpsHigh')::numeric)), 0) AS d_eps,
    (
      COALESCE((p0->>'estimatedRevenueAvg')::numeric,
               (p0->>'estimatedRevenueLow')::numeric,
               (p0->>'estimatedRevenueHigh')::numeric)
      - COALESCE((p1->>'estimatedRevenueAvg')::numeric,
                 (p1->>'estimatedRevenueLow')::numeric,
                 (p1->>'estimatedRevenueHigh')::numeric)
    ) / NULLIF(ABS(COALESCE((p1->>'estimatedRevenueAvg')::numeric,
                            (p1->>'estimatedRevenueLow')::numeric,
                            (p1->>'estimatedRevenueHigh')::numeric)), 0) AS d_rev
  FROM pairs
)
SELECT
  symbol,
  0.5*COALESCE(d_eps,0) + 0.5*COALESCE(d_rev,0) AS rev_eps_chg,
  CASE WHEN STDDEV_SAMP(0.5*COALESCE(d_eps,0) + 0.5*COALESCE(d_rev,0)) OVER () = 0
       THEN NULL
       ELSE (
         (0.5*COALESCE(d_eps,0) + 0.5*COALESCE(d_rev,0))
         - AVG(0.5*COALESCE(d_eps,0) + 0.5*COALESCE(d_rev,0)) OVER ()
       ) / NULLIF(STDDEV_SAMP(0.5*COALESCE(d_eps,0) + 0.5*COALESCE(d_rev,0)) OVER (), 0)
  END AS z_rev_eps
FROM revs;
"""


def main() -> None:
    url = os.getenv("DATABASE_URL")
    if not url:
        user = os.getenv("POSTGRES_USER", "pulse")
        password = os.getenv("POSTGRES_PASSWORD", "pulsepass")
        host = os.getenv("POSTGRES_HOST", "db")
        port = os.getenv("POSTGRES_PORT", "5432")
        name = os.getenv("POSTGRES_DB", "pulse")
        url = f"postgresql://{user}:{password}@{host}:{port}/{name}"
    with psycopg2.connect(url) as conn:
        with conn.cursor() as cur:
            cur.execute(SQL)
        conn.commit()
    print("Created/updated view fmp_revisions_cs")


if __name__ == "__main__":
    main()
