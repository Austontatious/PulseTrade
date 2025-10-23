from __future__ import annotations

import os
from typing import List

import psycopg2
from psycopg2 import sql


def _database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "pulse")
    password = os.getenv("POSTGRES_PASSWORD", "pulsepass")
    host = os.getenv("POSTGRES_HOST", "db")
    port = os.getenv("POSTGRES_PORT", "5432")
    name = os.getenv("POSTGRES_DB", "pulse")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


def get_tradable(view: str | None = None) -> List[str]:
    """
    Return the set of tradable symbols defined by the given view.
    Defaults to TRADING_UNIVERSE_VIEW env or trading_universe_100.
    """
    resolved_view = view or os.getenv("TRADING_UNIVERSE_VIEW", "trading_universe_100")

    query = sql.SQL("SELECT symbol FROM {}").format(sql.Identifier(resolved_view))

    conn = psycopg2.connect(_database_url())
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            rows = cur.fetchall()
    finally:
        conn.close()
    return [row[0] for row in rows]
