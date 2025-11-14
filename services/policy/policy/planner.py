import os
import math
import json
from typing import Optional, Tuple
import asyncpg
import datetime as dt

THRESH = float(os.getenv("PLANNER_DEV_THRESH", "0.0001"))  # 0.01%
R_MULT = float(os.getenv("PLANNER_R_MULT", "2.0"))        # target reward/risk
ATR_MINUTES = int(os.getenv("PLANNER_ATR_MINUTES", "10"))
LIMIT_NOTIONAL = float(os.getenv("PLANNER_LIMIT_NOTIONAL", "10"))
ENTRY_ATR_FRAC = float(os.getenv("PLANNER_ENTRY_ATR_FRAC", "0.1"))
BRACKET_PRICE_MAX = float(os.getenv("PLANNER_BRACKET_PRICE_MAX", "300"))
COOLDOWN_MINS = int(os.getenv("PLANNER_COOLDOWN_MINS", "30"))
USE_SENTIMENT = os.getenv("PLANNER_USE_SENTIMENT", "1") == "1"
USE_FUNDAMENTALS = os.getenv("PLANNER_USE_FUNDAMENTALS", "1") == "1"

async def latest_price(conn: asyncpg.Connection, ticker: str) -> Optional[float]:
    row = await conn.fetchrow("SELECT price FROM trades WHERE ticker=$1 ORDER BY ts DESC LIMIT 1", ticker)
    return float(row["price"]) if row else None

async def recent_volatility(conn: asyncpg.Connection, ticker: str) -> Optional[float]:
    # proxy ATR: stddev of 1s returns over last N minutes * price
    rows = await conn.fetch(
        """
        SELECT ts, price
        FROM trades
        WHERE ticker=$1 AND ts > NOW() - make_interval(mins => $2::int)
        ORDER BY ts ASC
        LIMIT 1000
        """,
        ticker,
        ATR_MINUTES,
    )
    if len(rows) < 5:
        return None
    prices = [float(r["price"]) for r in rows]
    rets = []
    for i in range(1, len(prices)):
        if prices[i-1] > 0:
            rets.append(prices[i]/prices[i-1] - 1.0)
    if not rets:
        return None
    mean = sum(rets)/len(rets)
    var = sum((x-mean)**2 for x in rets)/len(rets)
    sigma = math.sqrt(var)
    # approximate ATR as sigma * last_price
    last = prices[-1]
    return sigma * last

async def latest_quote(conn: asyncpg.Connection, ticker: str) -> Optional[Tuple[float, float, object]]:
    row = await conn.fetchrow(
        """
        SELECT ts, bid, ask FROM quotes
        WHERE ticker=$1 ORDER BY ts DESC LIMIT 1
        """,
        ticker,
    )
    if not row:
        return None
    return float(row["bid"] or 0.0), float(row["ask"] or 0.0), row["ts"]

async def in_event_blackout(conn: asyncpg.Connection, ticker: str) -> bool:
    row = await conn.fetchrow(
        """
        SELECT 1 FROM event_windows
        WHERE (ticker IS NULL OR ticker=$1)
          AND NOW() BETWEEN start_ts AND end_ts
        LIMIT 1
        """,
        ticker,
    )
    return bool(row)

async def latest_forecast(conn: asyncpg.Connection, ticker: str) -> Optional[Tuple[float,float,float]]:
    row = await conn.fetchrow(
        """
        SELECT mean, lower, upper
        FROM forecasts
        WHERE ticker=$1 AND horizon='1m'
        ORDER BY ts DESC
        LIMIT 1
        """,
        ticker,
    )
    if not row:
        return None
    return float(row["mean"]), float(row["lower"]), float(row["upper"])

async def latest_sentiment(conn: asyncpg.Connection, ticker: str) -> Tuple[Optional[float], Optional[float]]:
    row = await conn.fetchrow(
        """
        SELECT msg_rate, senti_mean
        FROM social_features
        WHERE ticker=$1
        ORDER BY ts DESC, window_minutes ASC
        LIMIT 1
        """,
        ticker,
    )
    if not row:
        return None, None
    rate = float(row["msg_rate"]) if row["msg_rate"] is not None else None
    mean = float(row["senti_mean"]) if row["senti_mean"] is not None else None
    return rate, mean

async def fundamentals_score(conn: asyncpg.Connection, ticker: str) -> float:
    # Very simple proxy using analyst ratings/targets
    rows = await conn.fetch(
        """
        SELECT action, rating, target
        FROM analyst_ratings
        WHERE ticker=$1 AND ts > NOW() - INTERVAL '30 days'
        ORDER BY ts DESC
        LIMIT 50
        """,
        ticker,
    )
    if not rows:
        return 0.0
    score = 0.0
    for r in rows:
        action = (r["action"] or "").lower()
        rating = (r["rating"] or "").lower()
        tgt = r["target"]
        if "upgrade" in action or rating in ("buy","strong buy","overweight","outperform"):
            score += 1
        if "downgrade" in action or rating in ("sell","underweight","underperform"):
            score -= 1
        if tgt is not None:
            try:
                score += 0.1
            except Exception:
                pass
    return score

async def _recent_planner_fill(conn: asyncpg.Connection, ticker: str) -> bool:
    row = await conn.fetchrow(
        """
        SELECT 1 FROM fills
        WHERE ticker=$1 AND ts > NOW() - make_interval(mins => $2::int)
          AND meta->>'source' = 'planner'
        LIMIT 1
        """,
        ticker,
        COOLDOWN_MINS,
    )
    return bool(row)

async def plan_limit_order(conn: asyncpg.Connection, ticker: str) -> Optional[dict]:
    # Cooldown to avoid spamming
    if await _recent_planner_fill(conn, ticker):
        return None
    # Event blackout
    if os.getenv("ENABLE_EARNINGS_BLACKOUT", "1") == "1":
        if await in_event_blackout(conn, ticker):
            return None
    price = await latest_price(conn, ticker)
    fc = await latest_forecast(conn, ticker)
    atr = await recent_volatility(conn, ticker)
    if price is None or fc is None or atr is None:
        return None
    mean, lower, upper = fc
    dev = (mean/price) - 1.0
    # ATR z-score gating
    z_thresh = float(os.getenv("PLANNER_Z_DEV_THRESH", "1.5"))
    if atr > 0:
        z = abs(dev) * price / atr
        if z < z_thresh:
            return None
    else:
        return None

    # Sentiment weighting: strong aligned sentiment reduces required threshold
    if USE_SENTIMENT:
        rate, senti = await latest_sentiment(conn, ticker)
        align = 1.0
        if senti is not None:
            if dev > 0 and senti > 0:
                align = 1.0 + min(abs(senti), 0.5)
            elif dev < 0 and senti < 0:
                align = 1.0 + min(abs(senti), 0.5)
            else:
                align = 1.0 - min(abs(senti), 0.5)
        eff_thresh = THRESH / align
    else:
        eff_thresh = THRESH

    if abs(dev) < eff_thresh:
        return None
    side = "buy" if dev > 0 else "sell"
    # Fundamentals veto/boost
    if USE_FUNDAMENTALS:
        fscore = await fundamentals_score(conn, ticker)
        if side == "buy" and fscore < 0:
            return None
        if side == "sell" and fscore > 0:
            return None

    # Simple R:R planning around ATR with quote/spread guard
    # Quote/Spread filter
    max_spread_bps = float(os.getenv("MAX_SPREAD_BPS", "8"))
    max_quote_age = int(os.getenv("MAX_QUOTE_AGE_SECS", "5"))
    spread_frac = float(os.getenv("ENTRY_SPREAD_FRAC", "0.25"))
    bid_ask = await latest_quote(conn, ticker)
    if bid_ask:
        bid, ask, qts = bid_ask
        if bid > 0 and ask > 0 and ask >= bid:
            mid = 0.5*(bid+ask)
            spread_bps = (ask-bid)/mid*1e4 if mid > 0 else 1e9
            age_ok = (dt.datetime.now(dt.timezone.utc) - qts).total_seconds() <= max_quote_age
            if (spread_bps > max_spread_bps) or (not age_ok):
                return None
            # Maker-like nudge at fraction of spread
            entry = (mid - spread_frac*(ask-bid)) if side == "buy" else (mid + spread_frac*(ask-bid))
        else:
            entry = price - ENTRY_ATR_FRAC*atr if side == "buy" else price + ENTRY_ATR_FRAC*atr
    else:
        entry = price - ENTRY_ATR_FRAC*atr if side == "buy" else price + ENTRY_ATR_FRAC*atr
    stop = entry - atr if side == "buy" else entry + atr
    target = entry + R_MULT*atr if side == "buy" else entry - R_MULT*atr
    # Sizing: vol-targeted (risk per trade) or notional
    use_vol_target = os.getenv("USE_VOL_TARGETED", "1") == "1"
    if use_vol_target and atr > 0:
        risk_dollars = float(os.getenv("RISK_DOLLARS_PER_TRADE", "10"))
        qty = max(risk_dollars/max(atr, 1e-6), 0.0001)
    else:
        qty = max(LIMIT_NOTIONAL/max(entry, 1e-6), 0.0001)
    # Bracket only allowed for whole-share quantities and reasonable price
    is_whole_qty = abs(qty - round(qty)) < 1e-6 and round(qty) >= 1
    plan = {
        "ticker": ticker,
        "side": side,
        "qty": round(qty, 6),
        "limit_price": round(entry, 2),
        "stop": round(stop, 2),
        "target": round(target, 2),
        "atr": round(atr, 6),
        "dev": dev,
        "bracket_ok": (is_whole_qty and entry <= BRACKET_PRICE_MAX),
    }
    return plan
