from __future__ import annotations

import datetime as dt
from typing import Optional

import numpy as np
import psycopg2


def load_return_window(
    conn: psycopg2.extensions.connection,
    symbol: str,
    as_of: dt.date,
    window_len: int,
) -> Optional[np.ndarray]:
    """Load the last `window_len` daily returns strictly before `as_of`."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT y
            FROM daily_returns
            WHERE symbol=%s
              AND ds < %s
            ORDER BY ds DESC
            LIMIT %s
            """,
            (symbol, as_of, window_len),
        )
        rows = cur.fetchall()
    if len(rows) < window_len:
        return None
    series = np.array([float(row[0]) for row in reversed(rows)], dtype=np.float32)
    return series
