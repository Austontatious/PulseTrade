import os
import asyncpg
from .executor import maybe_submit_order, get_open_orders

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

async def place_fractional_exits(conn: asyncpg.Connection) -> int:
    # Find recent planner fills with Alpaca ack and no bracket (qty < 1)
    rows = await conn.fetch(
        """
        SELECT ts, ticker, side, qty, (meta->'plan'->>'target')::double precision AS target,
               (meta->'plan'->>'stop')::double precision AS stop,
               meta
        FROM fills
        WHERE ts > NOW() - INTERVAL '10 minutes'
          AND meta->>'source' = 'planner'
          AND (meta->'alpaca') IS NOT NULL
          AND qty < 1.0
        ORDER BY ts DESC
        LIMIT 100
        """
    )
    placed = 0
    for r in rows:
        tkr = r["ticker"]; side = r["side"]; qty = float(r["qty"]) or 0.0
        if side != 'buy' or qty <= 0:
            continue
        try:
            opens = await get_open_orders(tkr)
            has_tp = any(o.get('side') == 'sell' and o.get('type') == 'limit' for o in opens)
            has_sl = any(o.get('side') == 'sell' and o.get('type') in ('stop','stop_limit') for o in opens)
            if not has_tp and r["target"]:
                await maybe_submit_order(tkr, 'sell', qty, limit_price=float(r['target']), order_type='limit')
                placed += 1
            if not has_sl and r["stop"]:
                await maybe_submit_order(tkr, 'sell', qty, order_type='stop', stop_price=float(r['stop']))
                placed += 1
        except Exception:
            continue
    return placed

async def run_exit_manager_once() -> int:
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        return await place_fractional_exits(conn)
    finally:
        await conn.close()

