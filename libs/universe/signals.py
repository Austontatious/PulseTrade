from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import psycopg2


@dataclass
class SignalMetrics:
    symbol: str
    horizon: str
    forecast_ts: Optional[str]
    mean_return: float
    spy_mean_return: float
    excess_return: float
    vol_5d: float
    signal_strength: float


def _fetch_latest_forecast(conn, symbol: str, horizon: str, cutoff: Optional[str] = None) -> Optional[tuple]:
    cur = conn.cursor()
    if cutoff:
        cur.execute(
            """
            SELECT ts, mean
            FROM forecasts
            WHERE ticker=%s AND horizon=%s AND ts <= %s
            ORDER BY ts DESC
            LIMIT 1
            """,
            (symbol, horizon, cutoff),
        )
    else:
        cur.execute(
            """
            SELECT ts, mean
            FROM forecasts
            WHERE ticker=%s AND horizon=%s
            ORDER BY ts DESC
            LIMIT 1
            """,
            (symbol, horizon),
        )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    return row[0], float(row[1]) if row[1] is not None else None


def _fetch_vol_5d(conn, symbol: str) -> float:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT sqrt(5.0) * stddev(y)
        FROM daily_returns
        WHERE symbol=%s AND ds >= (CURRENT_DATE - INTERVAL '120 days')
        """,
        (symbol,),
    )
    row = cur.fetchone()
    cur.close()
    if not row or row[0] is None:
        return 0.02
    return max(float(row[0]), 1e-4)


def compute_signal_metrics(
    conn: psycopg2.extensions.connection,
    symbol: str,
    horizon: str,
    spy_symbol: str = "SPY",
    cutoff_ts: Optional[str] = None,
) -> Optional[SignalMetrics]:
    sym_row = _fetch_latest_forecast(conn, symbol, horizon, cutoff_ts)
    spy_row = _fetch_latest_forecast(conn, spy_symbol, horizon, cutoff_ts)
    if not sym_row or not spy_row:
        return None
    forecast_ts, sym_mean = sym_row
    _, spy_mean = spy_row
    if sym_mean is None or spy_mean is None:
        return None
    vol = _fetch_vol_5d(conn, symbol)
    excess = sym_mean - spy_mean
    signal = excess / vol if vol else 0.0
    return SignalMetrics(
        symbol=symbol,
        horizon=horizon,
        forecast_ts=str(forecast_ts),
        mean_return=float(sym_mean),
        spy_mean_return=float(spy_mean),
        excess_return=float(excess),
        vol_5d=vol,
        signal_strength=signal,
    )

