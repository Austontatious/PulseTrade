from __future__ import annotations

import json
import os
from typing import Iterable, Mapping

import psycopg2
import psycopg2.extras

from .config import DB_DSN


PG_DSN = os.getenv("PG_DSN", DB_DSN)


def upsert_metrics(rows: Iterable[Mapping[str, object]]) -> int:
    rows = list(rows)
    if not rows:
        return 0

    columns = ["symbol", "as_of", "metric", "value", "window", "src", "raw"]
    records = [
        (
            row.get("symbol"),
            row.get("as_of"),
            row.get("metric"),
            row.get("value"),
            row.get("window"),
            row.get("src"),
            json.dumps(row.get("raw")) if row.get("raw") is not None else None,
        )
        for row in rows
    ]

    query = f"""
        INSERT INTO ingest_metrics ({', '.join(columns)})
        VALUES ({', '.join(['%s'] * len(columns))})
        ON CONFLICT (symbol, as_of, metric, window, src)
        DO UPDATE SET value = EXCLUDED.value, raw = COALESCE(EXCLUDED.raw, ingest_metrics.raw)
    """

    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            psycopg2.extras.execute_batch(cur, query, records, page_size=500)
    return len(records)
