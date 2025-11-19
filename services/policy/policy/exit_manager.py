import logging
import os
import re

import asyncpg

from .executor import (
    cancel_orders_for_symbol,
    close_position,
    get_all_positions,
    get_open_orders,
    maybe_submit_order,
)

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)
USE_TRAILING_STOPS = os.getenv("PT_USE_TRAILING_STOPS", "true").lower() == "true"
logger = logging.getLogger(__name__)


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
        tkr = r["ticker"]
        side = r["side"]
        qty = float(r["qty"]) or 0.0
        if side != "buy" or qty <= 0:
            continue
        try:
            opens = await get_open_orders(tkr)
            has_tp = any(o.get("side") == "sell" and o.get("type") == "limit" for o in opens)
            has_sl = any(o.get("side") == "sell" and o.get("type") in ("stop", "stop_limit") for o in opens)
            if not has_tp and r["target"]:
                await maybe_submit_order(tkr, "sell", qty, limit_price=float(r["target"]), order_type="limit")
                placed += 1
            if not has_sl and r["stop"]:
                await maybe_submit_order(tkr, "sell", qty, order_type="stop", stop_price=float(r["stop"]))
                placed += 1
        except Exception as exc:
            logger.warning("fractional exit placement failed for %s: %s", tkr, exc)
            continue
    return placed


async def run_exit_manager_once() -> int:
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        placed = 0
        if not USE_TRAILING_STOPS:
            placed += await place_fractional_exits(conn)
        placed += await place_universe_exits(conn)
        return placed
    finally:
        await conn.close()


async def place_universe_exits(conn: asyncpg.Connection) -> int:
    """
    Exit positions that are currently outside of the configured trading universe.

    Controlled by ALLOW_EXIT_OUTSIDE_UNIVERSE=1 and TRADING_UNIVERSE_VIEW (default trading_universe_100).
    - Fetch current tradable symbols from the DB view
    - Fetch all open positions from Alpaca
    - For positions whose symbol is not in the tradable set (and not a crypto pair like *USD),
      submit a market order to flatten the position (sell for longs, buy for shorts).
    """
    if os.getenv("ALLOW_EXIT_OUTSIDE_UNIVERSE", "0") != "1":
        return 0

    view = os.getenv("TRADING_UNIVERSE_VIEW", "trading_universe_100")
    if not re.match(r"^[A-Za-z_][A-Za-z0-9_]*$", view):
        view = "trading_universe_100"

    try:
        rows = await conn.fetch(f"SELECT symbol FROM {view}")
        tradable = {r["symbol"] for r in rows}
    except Exception:
        tradable = set()

    placed = 0
    positions = await get_all_positions()
    for p in positions:
        try:
            tkr = (p.get("symbol") or "").upper()
            if not tkr or tkr.endswith("USD") or tkr in tradable:
                continue
            cancelled = await cancel_orders_for_symbol(tkr)
            if cancelled:
                logger.info("exit_manager: cancelled %s open orders for %s", cancelled, tkr)
            resp = await close_position(tkr, cancel_orders=False)
            if resp is not None:
                placed += 1
                logger.info("exit_manager: flattened %s outside of universe", tkr)
        except Exception as exc:
            logger.warning("exit_manager: failed to flatten %s: %s", p.get("symbol"), exc)
            continue
    return placed
