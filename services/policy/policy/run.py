import asyncio
import asyncpg
import json
import os
try:
    from .executor import maybe_submit_order, get_position_qty
    from .planner import plan_limit_order
except ImportError:  # pragma: no cover
    async def maybe_submit_order(*args, **kwargs):  # type: ignore
        return None
    async def get_position_qty(*args, **kwargs):  # type: ignore
        return 0.0
    async def plan_limit_order(*args, **kwargs):  # type: ignore
        return None

DB_DSN = f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}@" \
         f"{os.getenv('POSTGRES_HOST','db')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}"

THRESH = 0.0001  # trade when forecast deviates >0.01% from last price (more active)

# Only attempt Alpaca execution for these tickers (usually equities/ETFs)
PLANNER_MAX_TRADE_TICKERS = int(os.getenv("PLANNER_MAX_TRADE_TICKERS", "30"))
# Baseline executor gating: allow policy submissions only for these (from env)
ALPACA_TRADE_TICKERS = set(
    t.strip().upper() for t in os.getenv("ALPACA_SYMBOLS", "").split(",") if t.strip()
)

async def step(conn):
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
        qty = 0.0; side = None
        if dev > THRESH:
            side = "buy"; qty = 0.001  # toy units
        elif dev < -THRESH:
            side = "sell"; qty = 0.001
        if side:
            meta = {
                "source": "policy",
                "signal_dev": dev,
            }
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
        finally:
            await conn.close()
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
