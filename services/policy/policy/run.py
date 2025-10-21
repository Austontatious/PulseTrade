import asyncio
import asyncpg
import json
import os
from .executor import maybe_submit_order, get_position_qty
from .planner import plan_limit_order
from .exit_manager import run_exit_manager_once
import math
import datetime as dt

DB_DSN = f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}@" \
         f"{os.getenv('POSTGRES_HOST','db')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}"

THRESH = 0.0001  # trade when forecast deviates >0.01% from last price (more active)

# Only attempt Alpaca execution for these tickers (usually equities/ETFs)
PLANNER_MAX_TRADE_TICKERS = int(os.getenv("PLANNER_MAX_TRADE_TICKERS", "30"))
# Baseline executor gating: allow policy submissions only for these (from env)
ALPACA_TRADE_TICKERS = set(
    t.strip().upper() for t in os.getenv("ALPACA_SYMBOLS", "").split(",") if t.strip()
)

async def _price_at(conn: asyncpg.Connection, ticker: str, ago_secs: int) -> float | None:
    row = await conn.fetchrow(
        """
        SELECT price FROM trades
        WHERE ticker=$1 AND ts <= NOW() - make_interval(secs => $2::int)
        ORDER BY ts DESC LIMIT 1
        """,
        ticker,
        ago_secs,
    )
    return float(row["price"]) if row else None

async def _sigma_ewma_1m(conn: asyncpg.Connection, ticker: str, minutes: int = 30) -> float:
    # Approximate 1m returns via sampling every ~minute
    rows = await conn.fetch(
        """
        SELECT ts, price FROM trades
        WHERE ticker=$1 AND ts > NOW() - make_interval(mins => $2::int)
        ORDER BY ts ASC
        LIMIT 600
        """,
        ticker,
        minutes,
    )
    if len(rows) < 5:
        return 0.0
    # Downsample ~every 60s
    prices = []
    last_ts = None
    for row in rows:
        ts = row["ts"]
        if (last_ts is None) or ( (ts - last_ts).total_seconds() >= 50):
            prices.append(float(row["price"]))
            last_ts = ts
    if len(prices) < 3:
        return 0.0
    rets = []
    for i in range(1, len(prices)):
        if prices[i-1] > 0:
            rets.append(prices[i]/prices[i-1] - 1.0)
    if not rets:
        return 0.0
    # EWMA with lambda=0.94
    lam = float(os.getenv("EWMA_LAMBDA", "0.94"))
    var = 0.0
    for r in rets:
        var = lam*var + (1-lam)*(r*r)
    return math.sqrt(var)

async def _maybe_trip_symbol_breaker(conn: asyncpg.Connection, ticker: str) -> None:
    # Compute tests A (z residual), B (cum standardized), D (vol+price concurrence)
    # Returns based on 1m and 15m windows
    p_now_row = await conn.fetchrow("SELECT price FROM trades WHERE ticker=$1 ORDER BY ts DESC LIMIT 1", ticker)
    if not p_now_row:
        return
    p_now = float(p_now_row["price"]) or 0.0
    p_1m = await _price_at(conn, ticker, 60) or p_now
    p_15m = await _price_at(conn, ticker, 900) or p_now
    r1 = (p_now/p_1m - 1.0) if p_1m > 0 else 0.0
    r15 = (p_now/p_15m - 1.0) if p_15m > 0 else 0.0
    # Forecast-derived mu and sigma (approx)
    f = await conn.fetchrow(
        """
        SELECT mean, lower, upper FROM forecasts
        WHERE ticker=$1 AND horizon='1m'
        ORDER BY ts DESC LIMIT 1
        """,
        ticker,
    )
    mu_hat = 0.0; sig_ret = 0.0
    if f and p_1m > 0:
        mu_hat = (float(f["mean"]) / p_1m) - 1.0
        if f["upper"] is not None and f["lower"] is not None:
            sig_px = (float(f["upper"]) - float(f["lower"])) / (2*1.96)
            sig_ret = sig_px / p_1m if p_1m > 0 else 0.0
    if sig_ret <= 0:
        sig_ret = await _sigma_ewma_1m(conn, ticker) or 1e-6
    z = (r1 - mu_hat) / max(sig_ret, 1e-6)
    test_A = (z <= -4.0)
    # B: 15m cum standardized move
    sig1 = await _sigma_ewma_1m(conn, ticker) or 1e-6
    s_cum = r15 / (sig1*math.sqrt(15)) if sig1 > 0 else 0.0
    test_B = (s_cum <= -6.0)
    # D: volume + price concurrence (approx volume by trade count z)
    vol_row = await conn.fetchrow(
        """
        WITH t AS (
          SELECT ts FROM trades WHERE ticker=$1 AND ts > NOW() - INTERVAL '60 minutes'
        )
        SELECT
          SUM(CASE WHEN ts > NOW() - INTERVAL '1 minutes' THEN 1 ELSE 0 END)::int AS n1,
          COUNT(*)::int AS n60
        FROM t
        """,
        ticker,
    )
    n1 = int(vol_row["n1"]) if vol_row else 0
    n60 = int(vol_row["n60"]) if vol_row else 1
    avg = n60/60.0
    std = math.sqrt(max(avg, 1e-3))
    z_vol = (n1 - avg) / max(std, 1e-6)
    test_D = (z_vol >= 4.0 and z <= -3.0)
    fired = sum(1 for t in (test_A, test_B, test_D) if t)
    if fired >= 2:
        ttl_min = int(os.getenv("SYMBOL_BREAKER_TTL_MIN", "60"))
        reason = []
        if test_A: reason.append("A")
        if test_B: reason.append("B")
        if test_D: reason.append("D")
        await conn.execute(
            """
            INSERT INTO circuit_breakers(ts, scope, key, active, reason, expires_at, meta)
            VALUES(NOW(), 'symbol', $1, TRUE, $2, NOW() + make_interval(mins => $3::int), $4)
            """,
            ticker,
            "symbol_anomaly:" + "+".join(reason),
            ttl_min,
            json.dumps({"z": z, "s_cum": s_cum, "z_vol": z_vol}),
        )

async def step(conn):
    # Global circuit breaker check
    try:
        glb = await conn.fetchrow(
            """
            SELECT 1 FROM circuit_breakers
            WHERE scope='global' AND key='ALL' AND active=TRUE
              AND (expires_at IS NULL OR expires_at > NOW())
            ORDER BY ts DESC LIMIT 1
            """
        )
        global_block = bool(glb)
    except Exception:
        global_block = False
    # Load active knobs (optional)
    knobs = {}
    try:
        rows_knobs = await conn.fetch(
            """
            SELECT key, value FROM policy_knobs
            WHERE (expires_at IS NULL OR expires_at > NOW())
            ORDER BY ts DESC
            LIMIT 200
            """
        )
        for rk in rows_knobs:
            k = rk["key"]; v = rk["value"]
            if isinstance(v, dict) and "value" in v:
                knobs[k] = v["value"]
            else:
                knobs[k] = v
    except Exception:
        knobs = {}
    q = """WITH latest AS (
             SELECT DISTINCT ON (ticker) ts,ticker,mean,lower,upper
             FROM forecasts WHERE horizon='1m' ORDER BY ticker, ts DESC
           ),
           px AS (
             SELECT DISTINCT ON (ticker) ts,ticker,price FROM trades ORDER BY ticker, ts DESC
           )
           SELECT l.ticker, l.mean, p.price
             FROM latest l JOIN px p USING (ticker);"""
    rows = await conn.fetch(q)
    for r in rows:
        ticker = r["ticker"]; mean = float(r["mean"]); price = float(r["price"])
        dev = (mean/price) - 1.0
        # Dynamic threshold override (bps)
        dyn_thresh = THRESH
        try:
            bps = float(knobs.get("threshold_bps") or knobs.get("THRESHOLD_BPS") or 0.0)
            if bps > 0:
                dyn_thresh = max(dyn_thresh, bps/10000.0)
        except Exception:
            pass
        qty = 0.0; side = None
        if dev > dyn_thresh:
            side = "buy"; qty = 0.001  # toy units
        elif dev < -dyn_thresh:
            side = "sell"; qty = 0.001
        # Symbol anomaly detector (trip and blacklist intraday if needed)
        try:
            if os.getenv("ENABLE_CIRCUIT_BREAKERS", "1") == "1":
                await _maybe_trip_symbol_breaker(conn, ticker)
        except Exception:
            pass
        if side:
            meta = {
                "source": "policy",
                "signal_dev": dev,
            }
            # Skip if symbol circuit breaker active or global block
            try:
                blk = await conn.fetchrow(
                    """
                    SELECT 1 FROM circuit_breakers
                    WHERE scope='symbol' AND key=$1 AND active=TRUE
                      AND (expires_at IS NULL OR expires_at > NOW())
                    ORDER BY ts DESC LIMIT 1
                    """,
                    ticker,
                )
                if blk or global_block:
                    side = None
            except Exception:
                pass
            # Quote/Spread guard for baseline policy
            try:
                from .executor import latest_quote as _latest_quote  # type: ignore
                q = await _latest_quote(ticker)
                if q and q.get("bid") and q.get("ask") and q["ask"] >= q["bid"]:
                    mid = 0.5*(q["bid"]+q["ask"]) if (q["bid"] and q["ask"]) else None
                    if mid and mid > 0:
                        spread_bps = (q["ask"]-q["bid"]) / mid * 1e4
                        max_spread_bps = float(os.getenv("MAX_SPREAD_BPS", "8"))
                        # age check
                        import datetime as dt
                        age = (dt.datetime.now(dt.timezone.utc) - q["ts"]).total_seconds()
                        max_quote_age = int(os.getenv("MAX_QUOTE_AGE_SECS", "5"))
                        if spread_bps > max_spread_bps or age > max_quote_age:
                            side = None
            except Exception:
                pass
            # Strategist reco influence (scale or skip)
            try:
                rrec = await conn.fetchrow(
                    """
                    SELECT side, score FROM strategist_recos
                    WHERE ticker=$1 AND ts > NOW() - INTERVAL '10 minutes'
                    ORDER BY ts DESC LIMIT 1
                    """,
                    ticker,
                )
                if rrec:
                    reco_side = rrec["side"]; score = float(rrec["score"])
                    alpha = float(os.getenv("POLICY_RECO_ALPHA", "0.6"))
                    beta = float(os.getenv("POLICY_RECO_BETA", "0.8"))
                    if side == reco_side:
                        qty *= max(0.0, 1.0 + alpha*abs(score))
                    else:
                        shrink = max(0.0, 1.0 - beta*abs(score))
                        qty *= shrink
                        if shrink < 0.2:
                            side = None
            except Exception:
                pass
            ack = None
            allow_all = os.getenv("POLICY_ALLOW_ALL_ALPACA", "0") == "1"
            eligible = ticker in ALPACA_TRADE_TICKERS or (allow_all and not ticker.endswith("USD"))
            if eligible:
                # Baseline: market orders. For sells, avoid shorts by capping to position qty and removing notional.
                if side == "sell":
                    pos_qty = float(await get_position_qty(ticker))
                    if pos_qty > 0:
                        use_qty = min(qty, max(0.0, pos_qty - 0.0001))
                        if use_qty > 0:
                            ack = await maybe_submit_order(ticker, side, use_qty, None, order_type="market")
                        else:
                            ack = None
                    else:
                        ack = None
                else:
                    # buy: allow notional
                    ack = await maybe_submit_order(ticker, side, qty, None, order_type="market")
            if ack:
                meta["alpaca"] = {
                    "id": ack.get("id"),
                    "status": ack.get("status"),
                    "submitted_at": ack.get("submitted_at"),
                }
            venue = "ALPACA" if ack else "SIM"
            await conn.execute(
                """INSERT INTO fills(ts,ticker,side,qty,price,venue,meta)
                   VALUES (NOW(), $1,$2,$3,$4,$5,$6)""",
                ticker,
                side,
                qty,
                price,
                venue,
                json.dumps(meta),
            )

    # Build dynamic trade universe: equities with recent trades and/or in symbols table
    dyn_rows = await conn.fetch(
        """
        WITH recent AS (
          SELECT ticker, ts, price
          FROM trades
          WHERE ts > NOW() - INTERVAL '15 minutes' AND ticker NOT LIKE '%USD'
        ), agg AS (
          SELECT ticker,
                 COUNT(*) AS n,
                 MIN(price) FILTER (WHERE ts >= NOW() - INTERVAL '10 minutes') AS p0,
                 MAX(price) AS p1
          FROM recent
          GROUP BY ticker
        )
        SELECT ticker
        FROM agg
        ORDER BY GREATEST(ABS(COALESCE(p1/p0 - 1.0, 0)), 0) DESC, n DESC
        LIMIT 300
        """
    )
    dyn = [r["ticker"] for r in dyn_rows]
    # merge with env ALPACA symbols
    env_syms = [t.strip() for t in os.getenv("ALPACA_SYMBOLS", "").split(",") if t.strip()]
    trade_universe = list(dict.fromkeys([*env_syms, *dyn]))[:PLANNER_MAX_TRADE_TICKERS]

    # Planner phase: place limit orders with R:R using ATR proxy
    max_submits = int(os.getenv("POLICY_MAX_SUBMITS_PER_STEP", "8"))
    submits = 0
    for t in trade_universe:
        plan = await plan_limit_order(conn, t)
        if not plan:
            continue
        # Use bracket only when planner computed bracket_ok
        use_bracket = bool(plan.get("bracket_ok"))
        # Avoid fractional short-sell submissions (Alpaca disallows)
        if plan["qty"] < 1.0 and plan["side"] == "sell":
            ack = None
        else:
            # Dedupe: skip if open order exists on same side
            ack = None
            try:
                from .executor import get_open_orders  # type: ignore
                open_orders = await get_open_orders(plan["ticker"])
                if not any(o.get("side") == plan["side"] for o in open_orders):
                    ack = await maybe_submit_order(
                        plan["ticker"],
                        plan["side"],
                        plan["qty"],
                        plan["limit_price"],
                        order_type="limit",
                        order_class=("bracket" if use_bracket else None),
                        take_profit_limit=(plan["target"] if use_bracket else None),
                        stop_loss_stop=(plan["stop"] if use_bracket else None),
                    )
            except Exception:
                ack = None
        meta = {
            "source": "planner",
            "plan": plan,
        }
        if ack:
            meta["alpaca"] = {
                "id": ack.get("id"),
                "status": ack.get("status"),
                "submitted_at": ack.get("submitted_at"),
            }
        await conn.execute(
            """INSERT INTO fills(ts,ticker,side,qty,price,venue,meta)
               VALUES (NOW(), $1,$2,$3,$4,$5,$6)""",
            plan["ticker"],
            plan["side"],
            plan["qty"],
            plan["limit_price"],
            "ALPACA" if ack else "SIM",
            json.dumps(meta),
        )
        submits += 1
        if submits >= max_submits:
            break

async def main():
    while True:
        conn = await asyncpg.connect(dsn=DB_DSN)
        try:
            await step(conn)
            # Exit manager pass
            if os.getenv("ENABLE_EXIT_MANAGER", "1") == "1":
                try:
                    placed = await run_exit_manager_once()
                    if placed:
                        print("exit_manager placed orders:", placed)
                except Exception as e:
                    print("exit_manager error:", e)
            # Global breaker escalation
            if os.getenv("ENABLE_CIRCUIT_BREAKERS", "1") == "1":
                await _maybe_trip_global_breaker(conn)
        finally:
            await conn.close()
        await asyncio.sleep(5)

async def _maybe_trip_global_breaker(conn: asyncpg.Connection) -> None:
    try:
        # Compute fraction of active symbol breakers
        num_row = await conn.fetchrow(
            """
            SELECT COUNT(DISTINCT key) AS n
            FROM circuit_breakers
            WHERE scope='symbol' AND active=TRUE
              AND (expires_at IS NULL OR expires_at > NOW())
            """
        )
        num = int(num_row["n"]) if num_row else 0
        den_row = await conn.fetchrow(
            """
            SELECT COUNT(DISTINCT ticker) AS n
            FROM trades
            WHERE ts > NOW() - INTERVAL '30 minutes' AND ticker NOT LIKE '%USD'
            """
        )
        den = int(den_row["n"]) if den_row else 0
        if den <= 0:
            return
        frac = num/den
        thr = float(os.getenv("GLOBAL_BREAKER_FRAC", "0.3"))
        if frac >= thr:
            # If no active global, set one
            g = await conn.fetchrow(
                """
                SELECT 1 FROM circuit_breakers
                WHERE scope='global' AND key='ALL' AND active=TRUE
                  AND (expires_at IS NULL OR expires_at > NOW())
                ORDER BY ts DESC LIMIT 1
                """
            )
            if not g:
                ttl = int(os.getenv("GLOBAL_BREAKER_TTL_MIN", "60"))
                await conn.execute(
                    """
                    INSERT INTO circuit_breakers(ts, scope, key, active, reason, expires_at, meta)
                    VALUES(NOW(), 'global', 'ALL', TRUE, 'global_anomaly:blacklist_frac', NOW() + make_interval(mins => $1::int), $2)
                    """,
                    ttl,
                    json.dumps({"frac": frac, "num": num, "den": den}),
                )
    except Exception:
        return

if __name__ == "__main__":
    asyncio.run(main())
