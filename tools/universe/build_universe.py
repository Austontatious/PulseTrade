import os

import pandas as pd
import psycopg2


OUT_PATH = "services/ingest/universe_symbols.txt"

QUERY = """
SELECT s.ticker AS symbol
FROM symbols s
JOIN mv_liquidity_60d l
  ON l.symbol = s.ticker
LEFT JOIN dim_company_profile p
  ON p.symbol = s.ticker
WHERE COALESCE(NULLIF(s.meta->>'is_otc', '')::boolean, false) = false
  AND COALESCE(NULLIF(s.meta->>'is_active', '')::boolean, true) = true
  AND l.avg_dollar_vol_60d >= 2e6
  AND COALESCE(NULLIF(s.meta->>'last_price', '')::numeric, 0) >= 3
ORDER BY l.avg_dollar_vol_60d DESC
LIMIT 5000;
"""


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "pulse")
    password = os.getenv("POSTGRES_PASSWORD", "pulsepass")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    name = os.getenv("POSTGRES_DB", "pulse")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


def main() -> None:
    conn = psycopg2.connect(database_url())
    df = pd.read_sql(QUERY, conn)
    conn.close()

    df["symbol"].to_csv(OUT_PATH, index=False, header=False)
    print(f"Wrote {len(df)} symbols → {OUT_PATH}")


if __name__ == "__main__":
    main()
