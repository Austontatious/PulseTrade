import os

import pandas as pd
import psycopg2


OUT_PATH = "/mnt/data/kronos_data/interim/fundamentals_factors_v1.parquet"


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
    query = """
        SELECT *
        FROM fundamentals_factors_v1
        WHERE ds >= CURRENT_DATE - INTERVAL '5 years'
        ORDER BY ds, symbol
    """
    df = pd.read_sql(query, conn)
    conn.close()
    os.makedirs(os.path.dirname(OUT_PATH), exist_ok=True)
    df.to_parquet(OUT_PATH, index=False)
    print(f"Wrote {len(df)} rows → {OUT_PATH}")


if __name__ == "__main__":
    main()
