from __future__ import annotations

import datetime as dt
import json
import math
from statistics import pstdev
import os
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.extras

WEEK_MINUTES = 60 * 24 * 7
DEFAULT_LOOKBACK_DAYS = int(os.getenv("LLM_GO_NOGO_LOOKBACK_DAYS", "7"))


def _dict_cursor(conn) -> psycopg2.extensions.cursor:
    return conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)


def resolve_as_of(conn, explicit: Optional[dt.date]) -> dt.date:
    if explicit:
        return explicit
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(as_of) FROM signal_universe_100")
        row = cur.fetchone()
    if not row or not row[0]:
        raise RuntimeError("signal_universe_100 is empty – run Step 1 first.")
    return row[0]


def fetch_signal_universe(conn, as_of: dt.date, limit: int) -> List[Dict[str, Any]]:
    with _dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT symbol, rank, signal_strength, meta
            FROM signal_universe_100
            WHERE as_of=%s
            ORDER BY rank ASC
            LIMIT %s
            """,
            (as_of, limit),
        )
        rows = cur.fetchall()
    results: List[Dict[str, Any]] = []
    for row in rows:
        meta = row.get("meta")
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        results.append(
            {
                "symbol": row["symbol"],
                "rank": row["rank"],
                "signal_strength": float(row.get("signal_strength") or 0.0),
                "meta": meta or {},
            }
        )
    return results


def fetch_technical_metrics(
    conn,
    symbol: str,
    as_of: dt.date,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> Dict[str, Any]:
    start = as_of - dt.timedelta(days=lookback_days)
    with _dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT ds, y, dollar_vol
            FROM daily_returns
            WHERE symbol=%s AND ds BETWEEN %s AND %s
            ORDER BY ds ASC
            """,
            (symbol, start, as_of),
        )
        rows = cur.fetchall()
    if not rows:
        return {"price_change": None, "volume_change": None, "volatility": None}
    log_sum = 0.0
    vols: List[float] = []
    dollar_vols: List[float] = []
    for row in rows:
        val = row.get("y") or 0.0
        log_sum += float(val)
        vols.append(float(val))
        dollar_vols.append(float(row.get("dollar_vol") or 0.0))
    price_change = math.expm1(log_sum)
    volatility = pstdev(vols) if len(vols) > 1 else 0.0
    volume_change = None
    if dollar_vols:
        volume_change = dollar_vols[-1] - dollar_vols[0]
    return {
        "price_change": price_change,
        "volatility": volatility,
        "volume_change": volume_change,
        "window_days": lookback_days,
        "observations": len(rows),
    }


def fetch_sentiment_snapshot(conn, symbol: str) -> Dict[str, Any]:
    with _dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT ts, msg_rate, senti_mean, senti_std, pos_count, neg_count, neu_count
            FROM social_features
            WHERE ticker=%s AND window_minutes=%s
            ORDER BY ts DESC
            LIMIT 1
            """,
            (symbol, WEEK_MINUTES),
        )
        row = cur.fetchone()
    if not row:
        return {}
    total = sum(
        max(int(row.get(key) or 0), 0) for key in ("pos_count", "neg_count", "neu_count")
    )
    breakdown = {}
    for key in ("pos_count", "neg_count", "neu_count"):
        val = max(int(row.get(key) or 0), 0)
        pct = (val / total) if total > 0 else 0.0
        breakdown[key.replace("_count", "")] = {"count": val, "pct": pct}
    return {
        "ts": row["ts"].isoformat() if row.get("ts") else None,
        "msg_rate": row.get("msg_rate"),
        "senti_mean": row.get("senti_mean"),
        "senti_std": row.get("senti_std"),
        "breakdown": breakdown,
        "window_minutes": WEEK_MINUTES,
    }


POLITICAL_METRICS = [
    "quiver_congress_net_usd",
    "quiver_house_net_usd",
    "quiver_senate_net_usd",
    "quiver_insider_net_usd",
    "quiver_lobbying_spend_usd",
    "quiver_political_beta_today",
]


def fetch_political_signals(
    conn,
    symbol: str,
    as_of: dt.date,
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> List[Dict[str, Any]]:
    start = as_of - dt.timedelta(days=lookback_days)
    with _dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT as_of, metric, value, window, src
            FROM ingest_metrics
            WHERE symbol=%s
              AND metric = ANY(%s)
              AND as_of BETWEEN %s AND %s
            ORDER BY as_of DESC
            """,
            (symbol, POLITICAL_METRICS, start, as_of),
        )
        rows = cur.fetchall()
    results: List[Dict[str, Any]] = []
    for row in rows:
        results.append(
            {
                "metric": row["metric"],
                "value": float(row["value"]),
                "as_of": row["as_of"].isoformat() if row.get("as_of") else None,
                "window": row.get("window"),
                "src": row.get("src"),
            }
        )
    return results


def _fetch_latest_payload(cur, table: str, symbol: str) -> Optional[Dict[str, Any]]:
    cur.execute(
        f"SELECT payload FROM {table} WHERE symbol=%s ORDER BY date DESC LIMIT 1",
        (symbol,),
    )
    row = cur.fetchone()
    if not row:
        return None
    payload = row[0]
    if isinstance(payload, str):
        try:
            payload = json.loads(payload)
        except Exception:
            payload = {}
    return payload or {}


def fetch_fundamentals(conn, symbol: str) -> Dict[str, Any]:
    with conn.cursor() as cur:
        ratios = _fetch_latest_payload(cur, "fmp_ratios", symbol) or {}
        metrics = _fetch_latest_payload(cur, "fmp_key_metrics", symbol) or {}
        estimates = _fetch_latest_payload(cur, "fmp_analyst_estimates", symbol) or {}
        cur.execute(
            """
            SELECT ts, provider, firm, action, rating, target
            FROM analyst_ratings
            WHERE ticker=%s
            ORDER BY ts DESC
            LIMIT 3
            """,
            (symbol,),
        )
        ratings = cur.fetchall()
        cur.execute(
            """
            SELECT start_ts
            FROM event_windows
            WHERE ticker=%s AND kind='earnings' AND start_ts >= NOW()
            ORDER BY start_ts ASC
            LIMIT 1
            """,
            (symbol,),
        )
        earnings_row = cur.fetchone()

    fundamentals = {
        "pe_ratio": ratios.get("priceEarningsRatio") or metrics.get("peRatio"),
        "forward_pe": ratios.get("priceEarningsForward"),
        "eps": metrics.get("netIncomePerShare"),
        "ebitda_margin": ratios.get("ebitdaMargin"),
        "analyst_estimates": estimates,
        "analyst_ratings": [
            {
                "ts": row[0].isoformat() if row and row[0] else None,
                "provider": row[1],
                "firm": row[2],
                "action": row[3],
                "rating": row[4],
                "target": float(row[5]) if row[5] is not None else None,
            }
            for row in ratings
        ],
    }
    if earnings_row and earnings_row[0]:
        fundamentals["next_earnings"] = earnings_row[0].isoformat()
    return fundamentals


def fetch_time_series_forecast(
    conn,
    symbol: str,
    horizon: str = "5d",
) -> Optional[Dict[str, Any]]:
    with _dict_cursor(conn) as cur:
        cur.execute(
            """
            SELECT ts, mean, lower, upper, model, features
            FROM forecasts
            WHERE ticker=%s AND horizon=%s
            ORDER BY ts DESC
            LIMIT 1
            """,
            (symbol, horizon),
        )
        row = cur.fetchone()
    if not row:
        return None
    features = row.get("features")
    if isinstance(features, str):
        try:
            features = json.loads(features)
        except Exception:
            features = {}
    return {
        "ts": row["ts"].isoformat() if row.get("ts") else None,
        "model": row.get("model"),
        "horizon": horizon,
        "mean": row.get("mean"),
        "lower": row.get("lower"),
        "upper": row.get("upper"),
        "features": features or {},
    }
