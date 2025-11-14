import asyncio
import asyncpg
import json
import logging
import math
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

from libs.broker import alpaca_client as alp
from libs.portfolio.allocator import Idea, propose_target_weights_long_short
from libs.portfolio.replacement import replacement_plan
from libs.portfolio.sizer import cash_after_orders, split_buys_and_sells, to_orders

from .executor import ALPACA_TIF, get_account, get_all_positions, maybe_submit_order
from .exit_manager import run_exit_manager_once

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST','db')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}"
)

CFG = {
    "max_pos": float(os.getenv("PT_MAX_POS_PCT", "0.10")),
    "sector_cap": float(os.getenv("PT_SECTOR_MAX_PCT", "0.25")),
    "gross_cap": float(os.getenv("PT_GROSS_MAX_PCT", "1.00")),
    "cash_floor": float(os.getenv("PT_CASH_FLOOR_PCT", "0.05")),
    "min_order": float(os.getenv("PT_MIN_ORDER_NOTIONAL", "200")),
    "nt_band": float(os.getenv("PT_NO_TRADE_BAND_PCT", "0.0025")),
    "top_n": int(os.getenv("PT_TOP_N", "25")),
    "score_exp": float(os.getenv("PT_SCORE_EXP", "1.0")),
    "turn_cap": float(os.getenv("PT_TURNOVER_CAP_PCT", "0.40")),
    "replacement": os.getenv("PT_ENABLE_REPLACEMENT", "true").lower() == "true",
    "fric_bps": float(os.getenv("PT_REPLACEMENT_FRIC_BPS", "5")),
    "allow_shorts": os.getenv("PT_ALLOW_SHORTS", "true").lower() == "true",
    "max_short": float(os.getenv("PT_MAX_SHORT_POS_PCT", "0.10")),
    "sector_cap_short": float(os.getenv("PT_SECTOR_MAX_SHORT_PCT", "0.25")),
    "net_min": float(os.getenv("PT_NET_EXPO_MIN", "-0.30")),
    "net_max": float(os.getenv("PT_NET_EXPO_MAX", "0.70")),
    "require_shortable": os.getenv("PT_REQUIRE_SHORTABLE", "true").lower() == "true",
    "use_trailing_stops": os.getenv("PT_USE_TRAILING_STOPS", "true").lower() == "true",
    "trail_pct": float(os.getenv("PT_TRAIL_BUY_TO_COVER_PCT", "0.02")),
}

logger = logging.getLogger(__name__)
SHORT_REJECT_COOLDOWN_SECS = 600
_SHORT_COOLDOWNS: Dict[str, float] = {}
_SHORTABLE_CACHE: Dict[str, Tuple[bool, bool]] = {}

PT_BLOCK_IF_LLM_DENY = os.getenv("PT_BLOCK_IF_LLM_DENY", "true").lower() == "true"
PT_SCORE_FIELD = os.getenv("PT_SCORE_FIELD", "signal_value_z")

ALPACA_EXPECT_ACCOUNT_ID = os.getenv("ALPACA_ACCOUNT_ID", "").strip()

ALPACA_TRADE_TICKERS = set(
    t.strip().upper() for t in os.getenv("ALPACA_SYMBOLS", "").split(",") if t.strip()
)
ALLOW_ALL = os.getenv("POLICY_ALLOW_ALL_ALPACA", "0") == "1"


@dataclass
class PortfolioState:
    nav: float
    cash: float
    prices: Dict[str, float]
    fundamentals: Dict[str, Dict[str, Any]]
    signals: List[Dict[str, Any]]
    positions: Dict[str, Dict[str, float]]
    account_id: str | None = None


def _sector_for(state: PortfolioState, symbol: str) -> str:
    entry = state.fundamentals.get(symbol)
    if isinstance(entry, dict):
        return entry.get("sector", "UNK")
    if isinstance(entry, str):
        return entry or "UNK"
    return "UNK"


def _fund_shortable(state: PortfolioState, symbol: str) -> Tuple[bool, bool]:
    entry = state.fundamentals.get(symbol)
    if isinstance(entry, dict):
        return bool(entry.get("shortable")), bool(entry.get("easy_to_borrow"))
    return _SHORTABLE_CACHE.get(symbol, (False, False))


def _update_shortable_cache(symbol: str, shortable: bool, easy_to_borrow: bool) -> None:
    _SHORTABLE_CACHE[symbol] = (shortable, easy_to_borrow)


def _on_short_cooldown(symbol: str) -> bool:
    expiry = _SHORT_COOLDOWNS.get(symbol)
    return bool(expiry and expiry > time.time())


def _mark_short_reject(symbol: str) -> None:
    _SHORT_COOLDOWNS[symbol] = time.time() + SHORT_REJECT_COOLDOWN_SECS


async def _call_blocking(func, *args, **kwargs):
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


async def _fetch_price_map(conn: asyncpg.Connection) -> Dict[str, float]:
    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (ticker) ticker, price
        FROM trades
        ORDER BY ticker, ts DESC
        """
    )
    return {
        row["ticker"].upper(): float(row["price"])
        for row in rows
        if row["price"] is not None
    }


async def _fetch_fundamentals(conn: asyncpg.Connection) -> Dict[str, Dict[str, Any]]:
    rows = await conn.fetch("SELECT symbol, sector, shortable, easy_to_borrow, meta FROM dim_company_profile")
    fundamentals: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        sym = (row["symbol"] or "").upper()
        if not sym:
            continue
        sector = row["sector"] or "UNK"
        shortable = row["shortable"]
        easy_to_borrow = row["easy_to_borrow"]
        meta = row["meta"]
        meta_obj: Dict[str, Any] = {}
        if isinstance(meta, str):
            try:
                meta_obj = json.loads(meta)
            except Exception:
                meta_obj = {}
        elif isinstance(meta, dict):
            meta_obj = meta
        alpaca_meta = meta_obj.get("alpaca") if isinstance(meta_obj, dict) else {}
        if shortable is None and isinstance(alpaca_meta, dict):
            shortable = alpaca_meta.get("shortable")
        if easy_to_borrow is None and isinstance(alpaca_meta, dict):
            easy_to_borrow = alpaca_meta.get("easy_to_borrow")
        info = {
            "sector": sector,
            "shortable": bool(shortable),
            "easy_to_borrow": bool(easy_to_borrow),
        }
        fundamentals[sym] = info
        _SHORTABLE_CACHE[sym] = (info["shortable"], info["easy_to_borrow"])
    return fundamentals


def _llm_allow_from_features(features: Dict[str, Any]) -> bool:
    llm = features.get("llm") if isinstance(features, dict) else {}
    if isinstance(llm, dict):
        policy = llm.get("policy") or {}
        if isinstance(policy, dict):
            decision = policy.get("decision") or {}
            if isinstance(decision, dict) and "allow" in decision:
                return bool(decision.get("allow"))
            if "allow" in policy:
                return bool(policy.get("allow"))
    return True


def _score_from_features(features: Dict[str, Any], score_field: str) -> float | None:
    if not isinstance(features, dict):
        return None
    val = features.get(score_field)
    if val is None and "llm" in features:
        llm = features.get("llm")
        if isinstance(llm, dict):
            val = llm.get(score_field)
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


async def build_state(conn: asyncpg.Connection) -> PortfolioState | None:
    account = await get_account()
    account_id = (account.get("id") or "").strip()
    if ALPACA_EXPECT_ACCOUNT_ID:
        if not account_id:
            logger.warning("Policy: Alpaca account id missing in response; expected %s", ALPACA_EXPECT_ACCOUNT_ID)
        elif account_id != ALPACA_EXPECT_ACCOUNT_ID:
            raise RuntimeError(
                f"Alpaca account mismatch (expected {ALPACA_EXPECT_ACCOUNT_ID}, got {account_id})"
            )
    positions_raw = await get_all_positions()

    price_map = await _fetch_price_map(conn)
    fundamentals = await _fetch_fundamentals(conn)

    nav = float(account.get("portfolio_value", 0.0) or 0.0)
    cash = float(account.get("cash", 0.0) or 0.0)

    positions: Dict[str, Dict[str, float]] = {}
    total_market_value = 0.0
    for entry in positions_raw:
        sym = (entry.get("symbol") or "").upper()
        if not sym:
            continue
        shares = float(entry.get("qty") or entry.get("quantity") or 0.0)
        market_val = float(entry.get("market_value") or 0.0)
        weight = 0.0
        if nav > 0:
            weight = market_val / nav
        positions[sym] = {
            "shares": shares,
            "weight": weight,
            "market_value": market_val,
        }
        total_market_value += abs(market_val)
        px = price_map.get(sym) or float(entry.get("current_price") or entry.get("avg_entry_price") or 0.0)
        if px:
            price_map.setdefault(sym, px)

    if nav <= 0 and total_market_value > 0:
        nav = total_market_value + cash
        for sym, info in positions.items():
            info["weight"] = info["market_value"] / nav if nav > 0 else 0.0

    rows = await conn.fetch(
        """
        SELECT DISTINCT ON (ticker) ticker, mean, lower, upper, features
        FROM forecasts
        WHERE horizon='1m'
        ORDER BY ticker, ts DESC
        """
    )

    signals: List[Dict[str, Any]] = []
    for row in rows:
        sym = (row["ticker"] or "").upper()
        if not sym:
            continue
        price = price_map.get(sym)
        if not price or price <= 0:
            continue
        features = row["features"]
        if isinstance(features, str):
            try:
                features = json.loads(features)
            except Exception:
                features = {}
        elif features is None:
            features = {}
        allow = _llm_allow_from_features(features)
        score = _score_from_features(features, PT_SCORE_FIELD)
        if score is None:
            mean = float(row["mean"] or 0.0)
            score = (mean / price) - 1.0
        signals.append(
            {
                "symbol": sym,
                "price": price,
                "score": float(score),
                "llm_policy_allow": allow,
                "features": features,
            }
        )

    if not price_map:
        return None

    return PortfolioState(
        nav=nav,
        cash=cash,
        prices=price_map,
        fundamentals=fundamentals,
        signals=signals,
        positions=positions,
        account_id=account_id,
    )


def build_ideas(state: PortfolioState) -> List[Idea]:
    ideas: List[Idea] = []
    signal_map = {item["symbol"]: item for item in state.signals}

    for sym, signal in signal_map.items():
        price = state.prices.get(sym)
        if not price:
            continue
        pos = state.positions.get(sym, {"weight": 0.0, "shares": 0.0})
        allow = bool(signal.get("llm_policy_allow", True))
        if not PT_BLOCK_IF_LLM_DENY:
            allow = True
        score = float(signal.get("score", 0.0))
        ideas.append(
            Idea(
                symbol=sym,
                sector=_sector_for(state, sym),
                score=score,
                llm_allow=allow,
                price=price,
                cur_weight=float(pos.get("weight", 0.0)),
                cur_shares=float(pos.get("shares", 0.0)),
                nav=state.nav,
            )
        )

    for sym, pos in state.positions.items():
        if sym in signal_map:
            continue
        price = state.prices.get(sym)
        if not price:
            continue
        ideas.append(
            Idea(
                symbol=sym,
                sector=_sector_for(state, sym),
                score=0.0,
                llm_allow=True,
                price=price,
                cur_weight=float(pos.get("weight", 0.0)),
                cur_shares=float(pos.get("shares", 0.0)),
                nav=state.nav,
            )
        )

    return ideas


def _combine_orders(orders: List[Tuple[str, int]]) -> List[Tuple[str, int]]:
    merged: Dict[str, int] = {}
    for sym, qty in orders:
        if qty == 0:
            continue
        merged[sym] = merged.get(sym, 0) + qty
    return [(sym, qty) for sym, qty in merged.items() if qty != 0]


async def rebalance_once(conn: asyncpg.Connection) -> None:
    state = await build_state(conn)
    if not state or state.nav <= 0:
        return

    ideas = build_ideas(state)
    if not ideas:
        return

    target_w = propose_target_weights_long_short(
        ideas,
        allow_shorts=CFG["allow_shorts"],
        max_long=CFG["max_pos"],
        max_short=CFG["max_short"],
        sector_cap_long=CFG["sector_cap"],
        sector_cap_short=CFG["sector_cap_short"],
        gross_cap=CFG["gross_cap"],
        net_min=CFG["net_min"],
        net_max=CFG["net_max"],
        cash_floor=CFG["cash_floor"],
        top_n=CFG["top_n"],
        score_exp=CFG["score_exp"],
    )

    ideas_by_symbol = {idea.symbol: idea for idea in ideas}

    orders = to_orders(
        target_w,
        ideas_by_symbol,
        nav=state.nav,
        min_order_notional=CFG["min_order"],
        no_trade_band=CFG["nt_band"],
        price_map=state.prices,
    )

    filtered: List[Tuple[str, int]] = []
    for sym, delta in orders:
        if delta > 0:
            if ALLOW_ALL or sym in ALPACA_TRADE_TICKERS or not sym.endswith("USD"):
                filtered.append((sym, delta))
        else:
            filtered.append((sym, delta))
    orders = filtered

    orders = _combine_orders(orders)

    # turnover cap
    turnover_notional = sum(abs(qty) * state.prices.get(sym, 0.0) for sym, qty in orders)
    est_turnover = turnover_notional / state.nav if state.nav > 0 else 0.0
    if est_turnover > CFG["turn_cap"] and est_turnover > 0:
        ratio = CFG["turn_cap"] / est_turnover
        scaled = []
        for sym, qty in orders:
            new_qty = int(round(qty * ratio))
            if new_qty != 0:
                scaled.append((sym, new_qty))
        orders = _combine_orders(scaled)
        turnover_notional = sum(abs(qty) * state.prices.get(sym, 0.0) for sym, qty in orders)
        est_turnover = turnover_notional / state.nav if state.nav > 0 else 0.0

    post_cash = cash_after_orders(state.cash, orders, state.prices)
    min_cash = CFG["cash_floor"] * state.nav

    if post_cash < min_cash and CFG["replacement"]:
        buys = [(sym, qty) for sym, qty in orders if qty > 0]
        deficit = min_cash - post_cash
        repl = replacement_plan(
            deficit,
            buys,
            ideas_by_symbol=ideas_by_symbol,
            price_map=state.prices,
            fric_bps=CFG["fric_bps"],
        )
        if repl:
            orders.extend(repl)
            orders = _combine_orders(orders)
            post_cash = cash_after_orders(state.cash, orders, state.prices)

    if post_cash < min_cash:
        shortfall = min_cash - post_cash
        buy_notional = sum(qty * state.prices.get(sym, 0.0) for sym, qty in orders if qty > 0)
        if buy_notional > 0:
            ratio = max(0.0, 1.0 - shortfall / buy_notional)
            scaled: List[Tuple[str, int]] = []
            for sym, qty in orders:
                if qty > 0:
                    new_qty = int(round(qty * ratio))
                    if new_qty != 0:
                        scaled.append((sym, new_qty))
                else:
                    scaled.append((sym, qty))
            orders = _combine_orders(scaled)

    final_orders: List[Tuple[str, int]] = []
    for sym, qty in orders:
        idea = ideas_by_symbol.get(sym)
        if idea is None:
            continue
        price = state.prices.get(sym)
        if not price:
            continue
        if qty < 0 and not CFG["allow_shorts"]:
            current = max(idea.cur_shares, 0.0)
            max_sell = int(math.floor(current))
            qty = max(qty, -max_sell)
            if qty == 0:
                continue
        final_orders.append((sym, qty))

    if not final_orders:
        return

    final_orders = _combine_orders(final_orders)

    async def _persist_fill(symbol: str, side: str, qty: int, venue: str, ack: Dict[str, Any] | None = None) -> None:
        idea = ideas_by_symbol.get(symbol)
        price = state.prices.get(symbol)
        if idea is None or not price or qty <= 0:
            return
        meta = {
            "source": "pt_allocator",
            "target_weight": target_w.get(symbol, 0.0),
            "turnover_cap": CFG["turn_cap"],
            "score": float(getattr(idea, "score", 0.0)),
        }
        if state.account_id:
            meta["account_id"] = state.account_id
        if ack:
            meta["alpaca"] = {
                "id": ack.get("id"),
                "status": ack.get("status"),
                "submitted_at": ack.get("submitted_at"),
            }
        await conn.execute(
            """INSERT INTO fills(ts,ticker,side,qty,price,venue,meta)
                   VALUES (NOW(), $1,$2,$3,$4,$5,$6)""",
            symbol,
            side,
            qty,
            price,
            venue,
            json.dumps(meta),
        )

    async def _submit_market(symbol: str, side: str, qty: int) -> None:
        if qty <= 0:
            return
        ack = await maybe_submit_order(symbol, side, qty, order_type="market", use_notional_for_market=False)
        venue = "ALPACA" if ack else "SIM"
        await _persist_fill(symbol, side, qty, venue, ack)

    buys, sells = split_buys_and_sells(final_orders)

    for sym, qty in sells:
        idea = ideas_by_symbol.get(sym)
        price = state.prices.get(sym)
        if idea is None or not price:
            continue
        sell_qty = abs(qty)
        if sell_qty == 0:
            continue
        current_long = max(float(idea.cur_shares), 0.0)
        closable = int(math.floor(current_long))
        close_qty = min(sell_qty, closable)
        short_extra = sell_qty - close_qty

        if close_qty > 0:
            await _submit_market(sym, "sell", close_qty)

        if short_extra <= 0:
            continue
        if not CFG["allow_shorts"]:
            continue
        if _on_short_cooldown(sym):
            logger.info("skip short %s (cooldown)", sym)
            continue

        shortable_ok = True
        if CFG["require_shortable"]:
            short_flag, etb_flag = _fund_shortable(state, sym)
            shortable_ok = short_flag and etb_flag
            if not shortable_ok:
                try:
                    live_ok = await _call_blocking(alp.is_shortable, sym)
                    shortable_ok = live_ok
                    _update_shortable_cache(sym, live_ok, live_ok)
                except Exception as exc:
                    logger.warning("shortability check failed for %s: %s", sym, exc)
                    shortable_ok = False
        if not shortable_ok:
            logger.info("skip short %s (not shortable/ETB)", sym)
            continue

        try:
            ack = await _call_blocking(alp.place_short_sell, sym, short_extra, ALPACA_TIF)
        except Exception as exc:
            logger.warning("short sell rejected for %s: %s", sym, exc)
            _mark_short_reject(sym)
            continue

        await _persist_fill(sym, "sell", short_extra, "ALPACA", ack)

        if CFG["use_trailing_stops"]:
            try:
                await _call_blocking(
                    alp.trailing_buy_to_cover,
                    sym,
                    CFG["trail_pct"],
                    short_extra,
                    ALPACA_TIF,
                )
            except Exception as exc:
                logger.warning("trailing stop placement failed for %s: %s", sym, exc)

    for sym, qty in buys:
        idea = ideas_by_symbol.get(sym)
        price = state.prices.get(sym)
        if idea is None or not price or qty <= 0:
            continue
        current_short = float(idea.cur_shares)
        cover_qty = 0
        if current_short < 0:
            coverable = int(abs(math.floor(current_short)))
            cover_qty = min(qty, coverable)
        residual = qty - cover_qty

        if cover_qty > 0:
            try:
                ack = await _call_blocking(alp.buy_to_cover, sym, cover_qty, ALPACA_TIF)
            except Exception as exc:
                logger.warning("buy-to-cover failed for %s: %s", sym, exc)
                await _submit_market(sym, "buy", cover_qty)
                _SHORT_COOLDOWNS.pop(sym, None)
            else:
                _SHORT_COOLDOWNS.pop(sym, None)
                await _persist_fill(sym, "buy", cover_qty, "ALPACA", ack)

        if residual > 0:
            await _submit_market(sym, "buy", residual)


async def main() -> None:
    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            conn = await asyncpg.connect(dsn=DB_DSN)
        except Exception as exc:
            logging.warning("Policy: DB connection unavailable (%s), retrying", exc)
            await asyncio.sleep(5)
            continue
        try:
            await rebalance_once(conn)
            if os.getenv("ENABLE_EXIT_MANAGER", "1") == "1":
                try:
                    placed = await run_exit_manager_once()
                    if placed:
                        logging.info("exit_manager placed orders: %s", placed)
                except Exception as exc:
                    logging.error("exit_manager error: %s", exc)
        finally:
            await conn.close()
        await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())
