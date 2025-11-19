import asyncio
import asyncpg
import datetime as dt
import json
import logging
import math
import os
import time
from httpx import HTTPStatusError
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from zoneinfo import ZoneInfo

try:
    from prometheus_client import Gauge, start_http_server  # type: ignore
except Exception:  # pragma: no cover - fallback when prometheus_client missing
    Gauge = None  # type: ignore

    def start_http_server(*_args, **_kwargs):  # type: ignore
        return None

from libs.broker import alpaca_client as alp
from libs.portfolio.allocator import Idea, propose_target_weights_long_short
from libs.portfolio.replacement import replacement_plan
from libs.portfolio.sizer import cash_after_orders, split_buys_and_sells, to_orders
from libs.planner.daily_plan_loader import LoadedDailyPlan, load_daily_plans

from .executor import (
    ALPACA_TIF,
    get_account,
    get_all_positions,
    get_open_orders,
    maybe_submit_order,
)
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

MAX_ACTIONS_PER_STOCK_PER_DAY = int(os.getenv("MAX_ACTIONS_PER_STOCK_PER_DAY", "2"))
DAILY_MAX_TRADES = int(os.getenv("DAILY_MAX_TRADES", "20"))
DAILY_PLAN_DIR = Path(os.getenv("DAILY_PLAN_DIR", "reports"))
DAILY_PLAN_MODE = os.getenv("DAILY_PLAN_ENFORCEMENT", "off").strip().lower()
NY_TZ = ZoneInfo("America/New_York")
PT_REQUIRE_MONTE_TOP = os.getenv("PT_REQUIRE_MONTE_TOP", "false").lower() == "true"
PT_MONTE_TOP_LIMIT = int(os.getenv("PT_MONTE_TOP_LIMIT", "10"))
MONTE_ENABLED = os.getenv("ENABLE_MONTE_CARLO", "0").strip().lower() not in {"0", "false", "off", "no"}
EFFECTIVE_REQUIRE_MONTE_TOP = PT_REQUIRE_MONTE_TOP and MONTE_ENABLED
POLICY_METRICS_PORT = int(os.getenv("POLICY_METRICS_PORT", "9109"))
LLM_GO_NOGO_SCOPE = "daily_go_nogo"

_ACTIVE_DAILY_PLANS: Dict[str, LoadedDailyPlan] = {}
_PLAN_ACTIONS_USED: Dict[str, int] = {}
_PLAN_DATE: Optional[dt.date] = None
_TRADE_COUNT_DATE: Optional[dt.date] = None
_TOTAL_TRADES_TODAY = 0
_MONTE_TOP_SYMBOLS: set[str] = set()
_MONTE_TRADE_TOTAL = 0
_MONTE_TRADE_TOP = 0

logger = logging.getLogger(__name__)
SHORT_REJECT_COOLDOWN_SECS = 600
_SHORT_COOLDOWNS: Dict[str, float] = {}
_SHORTABLE_CACHE: Dict[str, Tuple[bool, bool]] = {}
_ASSET_STATUS_CACHE: Dict[str, Tuple[bool, float]] = {}
_ASSET_CACHE_TTL_SECS = 900.0

PT_BLOCK_IF_LLM_DENY = os.getenv("PT_BLOCK_IF_LLM_DENY", "true").lower() == "true"
PT_SCORE_FIELD = os.getenv("PT_SCORE_FIELD", "signal_value_z")

ALPACA_EXPECT_ACCOUNT_ID = os.getenv("ALPACA_ACCOUNT_ID", "").strip()

ALPACA_TRADE_TICKERS = set(
    t.strip().upper() for t in os.getenv("ALPACA_SYMBOLS", "").split(",") if t.strip()
)
ALLOW_ALL = os.getenv("POLICY_ALLOW_ALL_ALPACA", "0") == "1"
if PT_REQUIRE_MONTE_TOP and not MONTE_ENABLED:
    logger.warning("PT_REQUIRE_MONTE_TOP is enabled but ENABLE_MONTE_CARLO=0; ignoring Monte gate.")
if Gauge is not None and MONTE_ENABLED:
    try:
        start_http_server(POLICY_METRICS_PORT)
    except Exception:
        logger.warning("Policy metrics server failed to start on port %s", POLICY_METRICS_PORT)
        MONTE_OVERLAP_GAUGE = None
    else:
        MONTE_OVERLAP_GAUGE = Gauge(
            "policy_monte_top_overlap",
            "Share of trades that overlap the Monte Carlo top list",
        )
else:
    MONTE_OVERLAP_GAUGE = None


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


def _parse_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


async def _fetch_monte_carlo_map(conn: asyncpg.Connection) -> Dict[str, Dict[str, Any]]:
    rows = await conn.fetch(
        """
        SELECT symbol, mean_profit, profit_std_dev, best_score, rank,
               mean_return, return_std_dev
        FROM universe_monte_carlo
        WHERE as_of = (SELECT MAX(as_of) FROM universe_monte_carlo)
        """
    )
    data: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        sym = (row["symbol"] or "").upper()
        if not sym:
            continue
        data[sym] = {
            "mean_profit": float(row["mean_profit"] or 0.0),
            "profit_std_dev": float(row["profit_std_dev"] or 0.0),
            "best_score": float(row["best_score"] or 0.0),
            "rank": int(row["rank"] or 0) if row["rank"] is not None else None,
            "mean_return": float(row["mean_return"] or 0.0),
            "return_std_dev": float(row["return_std_dev"] or 0.0),
        }
    return data


def _update_monte_top_symbols(monte_map: Dict[str, Dict[str, Any]]) -> None:
    global _MONTE_TOP_SYMBOLS
    if not MONTE_ENABLED or not monte_map:
        _MONTE_TOP_SYMBOLS = set()
        return
    limit = max(PT_MONTE_TOP_LIMIT, 1)
    _MONTE_TOP_SYMBOLS = {
        sym for sym, info in monte_map.items() if info.get("rank") and info["rank"] <= limit
    }


async def _is_symbol_active(symbol: str) -> bool:
    now = time.time()
    cached = _ASSET_STATUS_CACHE.get(symbol)
    if cached and (now - cached[1]) < _ASSET_CACHE_TTL_SECS:
        return cached[0]
    active = False
    try:
        info = await _call_blocking(alp.get_asset, symbol)
    except HTTPStatusError as exc:
        logger.warning(
            "alpaca asset lookup failed for %s: %s %s",
            symbol,
            exc.response.status_code if exc.response else "n/a",
            exc.response.text if exc.response else "",
        )
    except Exception as exc:
        logger.warning("alpaca asset lookup failed for %s: %s", symbol, exc)
    else:
        status = str(info.get("status") or "").lower()
        tradable = bool(info.get("tradable", True))
        active = (status == "active") and tradable
    _ASSET_STATUS_CACHE[symbol] = (active, now)
    return active


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


async def _fetch_daily_go_nogo(conn: asyncpg.Connection, trading_date: dt.date) -> Dict[str, Dict[str, Any]]:
    try:
        rows = await conn.fetch(
            """
            SELECT symbol, output_json
            FROM llm_symbol_reviews
            WHERE scope=$1 AND as_of=$2
            """,
            LLM_GO_NOGO_SCOPE,
            trading_date,
        )
    except Exception:
        return {}
    mapping: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        sym = (row["symbol"] or "").upper()
        if not sym:
            continue
        payload = row["output_json"] or {}
        mapping[sym] = payload
    return mapping


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


def _position_available_qty(pos: Dict[str, float] | None) -> float:
    if not pos:
        return 0.0
    held = abs(float(pos.get("qty_held_for_orders", 0.0)))
    raw_available = pos.get("qty_available")
    if raw_available is not None:
        try:
            available = abs(float(raw_available))
        except (TypeError, ValueError):
            available = abs(float(pos.get("shares", 0.0)))
    else:
        available = abs(float(pos.get("shares", 0.0)))
    return max(available - held, 0.0)


def _nyc_now() -> dt.datetime:
    return dt.datetime.now(NY_TZ)


def _current_trading_date() -> dt.date:
    return _nyc_now().date()


def _reset_trade_counters_if_needed(trading_date: dt.date) -> None:
    global _TRADE_COUNT_DATE, _TOTAL_TRADES_TODAY, _MONTE_TRADE_TOTAL, _MONTE_TRADE_TOP
    if _TRADE_COUNT_DATE != trading_date:
        _TRADE_COUNT_DATE = trading_date
        _TOTAL_TRADES_TODAY = 0
        if MONTE_ENABLED:
            _MONTE_TRADE_TOTAL = 0
            _MONTE_TRADE_TOP = 0
        if MONTE_OVERLAP_GAUGE is not None:
            MONTE_OVERLAP_GAUGE.set(0.0)
        if DAILY_PLAN_MODE != "off" and _PLAN_ACTIONS_USED:
            for key in list(_PLAN_ACTIONS_USED.keys()):
                _PLAN_ACTIONS_USED[key] = 0


def _refresh_daily_plan_state() -> None:
    trading_date = _current_trading_date()
    _reset_trade_counters_if_needed(trading_date)
    if DAILY_PLAN_MODE == "off":
        return
    global _PLAN_DATE, _ACTIVE_DAILY_PLANS, _PLAN_ACTIONS_USED
    if _PLAN_DATE == trading_date and _ACTIVE_DAILY_PLANS:
        return
    plans = load_daily_plans(DAILY_PLAN_DIR, trading_date)
    if not plans:
        if _ACTIVE_DAILY_PLANS:
            logger.warning("Daily plans missing for %s; disabling new entries", trading_date)
        _ACTIVE_DAILY_PLANS = {}
        _PLAN_ACTIONS_USED = {}
        _PLAN_DATE = trading_date
        return
    _ACTIVE_DAILY_PLANS = {sym.upper(): plan for sym, plan in plans.items()}
    _PLAN_ACTIONS_USED = {sym: 0 for sym in _ACTIVE_DAILY_PLANS}
    _PLAN_DATE = trading_date
    logger.info("Loaded %s daily plans for %s", len(_ACTIVE_DAILY_PLANS), trading_date)


def _within_window(action, now: dt.datetime) -> bool:
    t = now.time()
    if action.start <= action.end:
        return action.start <= t <= action.end
    return t >= action.start or t <= action.end


def _has_trade_capacity(symbol: str, is_exit: bool) -> bool:
    if DAILY_MAX_TRADES <= 0:
        return True
    if _TOTAL_TRADES_TODAY < DAILY_MAX_TRADES:
        return True
    msg = f"daily trade cap reached ({_TOTAL_TRADES_TODAY}/{DAILY_MAX_TRADES}) for {symbol}"
    if is_exit:
        logger.info("%s; allowing exit", msg)
        return True
    if DAILY_PLAN_MODE == "shadow":
        logger.info("daily plan shadow allow: %s", msg)
        return True
    logger.info("daily plan block: %s", msg)
    return False


def _allow_daily_plan(symbol: str, direction: str, *, is_entry: bool, is_exit: bool, reason: str) -> bool:
    if DAILY_PLAN_MODE == "off":
        return True
    plan = _ACTIVE_DAILY_PLANS.get(symbol.upper())
    if not plan:
        if is_exit:
            return True
        msg = f"no active plan for {symbol} ({reason})"
        if DAILY_PLAN_MODE == "shadow":
            logger.info("daily plan shadow allow: %s", msg)
            return True
        logger.info("daily plan block: %s", msg)
        return False
    idx = _PLAN_ACTIONS_USED.get(symbol.upper(), 0)
    if idx >= MAX_ACTIONS_PER_STOCK_PER_DAY or idx >= len(plan.actions):
        msg = f"plan action limit reached for {symbol} ({idx} used)"
        if DAILY_PLAN_MODE == "shadow":
            logger.info("daily plan shadow allow: %s", msg)
            return True
        logger.info("daily plan block: %s", msg)
        return False
    action = plan.actions[idx]
    now_et = _nyc_now()
    if action.direction.lower() != direction.lower():
        msg = f"next plan action for {symbol} expects {action.direction} (got {direction})"
        if DAILY_PLAN_MODE == "shadow":
            logger.info("daily plan shadow allow: %s", msg)
            return True
        logger.info("daily plan block: %s", msg)
        return False
    if not _within_window(action, now_et):
        msg = (
            f"outside plan window {action.window_label} for {symbol} "
            f"(time {now_et.timetz().isoformat(timespec='minutes')})"
        )
        if DAILY_PLAN_MODE == "shadow":
            logger.info("daily plan shadow allow: %s", msg)
            return True
        logger.info("daily plan block: %s", msg)
        return False
    return True


def _should_execute_order(symbol: str, direction: str, *, is_entry: bool, is_exit: bool, reason: str) -> bool:
    if not _has_trade_capacity(symbol, is_exit):
        return False
    if not _allow_daily_plan(symbol, direction, is_entry=is_entry, is_exit=is_exit, reason=reason):
        return False
    return True


def _record_trade(symbol: str) -> None:
    global _TOTAL_TRADES_TODAY, _MONTE_TRADE_TOTAL, _MONTE_TRADE_TOP
    _TOTAL_TRADES_TODAY += 1
    if MONTE_ENABLED:
        _MONTE_TRADE_TOTAL += 1
        if symbol.upper() in _MONTE_TOP_SYMBOLS:
            _MONTE_TRADE_TOP += 1
        if MONTE_OVERLAP_GAUGE is not None and _MONTE_TRADE_TOTAL > 0:
            MONTE_OVERLAP_GAUGE.set(_MONTE_TRADE_TOP / max(_MONTE_TRADE_TOTAL, 1))
    if DAILY_PLAN_MODE == "off":
        return
    sym = symbol.upper()
    if sym in _PLAN_ACTIONS_USED:
        _PLAN_ACTIONS_USED[sym] = min(
            MAX_ACTIONS_PER_STOCK_PER_DAY,
            _PLAN_ACTIONS_USED.get(sym, 0) + 1,
        )


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

    if MONTE_ENABLED:
        price_map, fundamentals, monte_map = await asyncio.gather(
            _fetch_price_map(conn),
            _fetch_fundamentals(conn),
            _fetch_monte_carlo_map(conn),
        )
        _update_monte_top_symbols(monte_map)
    else:
        price_map, fundamentals = await asyncio.gather(
            _fetch_price_map(conn),
            _fetch_fundamentals(conn),
        )
        monte_map = {}
        _update_monte_top_symbols(monte_map)

    nav = float(account.get("portfolio_value", 0.0) or 0.0)
    cash = float(account.get("cash", 0.0) or 0.0)

    positions: Dict[str, Dict[str, float]] = {}
    total_market_value = 0.0
    for entry in positions_raw:
        sym = (entry.get("symbol") or "").upper()
        if not sym:
            continue
        shares = _parse_float(entry.get("qty") or entry.get("quantity") or 0.0)
        market_val = _parse_float(entry.get("market_value") or 0.0)
        weight = 0.0
        if nav > 0:
            weight = market_val / nav
        qty_available = _parse_float(entry.get("qty_available") or entry.get("qty") or 0.0)
        qty_held = _parse_float(entry.get("qty_held_for_orders") or 0.0)
        positions[sym] = {
            "shares": shares,
            "weight": weight,
            "market_value": market_val,
            "qty_available": qty_available,
            "qty_held_for_orders": qty_held,
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
    go_nogo_map = await _fetch_daily_go_nogo(conn, _current_trading_date())

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
        go_decision = go_nogo_map.get(sym)
        if go_decision:
            allow = allow and bool(go_decision.get("go", True))
        signal_entry = {
            "symbol": sym,
            "price": price,
            "score": float(score),
            "llm_policy_allow": allow,
            "features": features,
        }
        if go_decision:
            signal_entry["llm_go_nogo"] = go_decision
        if MONTE_ENABLED:
            monte_info = monte_map.get(sym)
            if monte_info:
                features["monte_carlo_score"] = monte_info.get("best_score")
                signal_entry["monte_carlo"] = monte_info
        signals.append(signal_entry)

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
        monte_info = signal.get("monte_carlo") or {}
        if EFFECTIVE_REQUIRE_MONTE_TOP:
            rank = monte_info.get("rank")
            if not rank or rank > PT_MONTE_TOP_LIMIT:
                continue
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
    _refresh_daily_plan_state()
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
        cur_pos = state.positions.get(sym, {})
        cur_shares = float(cur_pos.get("shares", 0.0))
        is_short = cur_shares < -1e-6
        is_long = cur_shares > 1e-6
        opens_long = delta > 0 and not is_short
        opens_short = delta < 0 and not is_long

        if opens_long and not (ALLOW_ALL or sym in ALPACA_TRADE_TICKERS or not sym.endswith("USD")):
            continue

        if (opens_long or opens_short):
            active = await _is_symbol_active(sym)
            if not active:
                logger.info("skip %s order %s (alpaca asset inactive)", sym, delta)
                continue

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
            if not _should_execute_order(sym, "sell", is_entry=False, is_exit=True, reason="close_long"):
                continue
            await _submit_market(sym, "sell", close_qty)
            _record_trade(sym)

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

        if not _should_execute_order(sym, "sell", is_entry=True, is_exit=False, reason="open_short"):
            continue
        try:
            ack = await _call_blocking(alp.place_short_sell, sym, short_extra, ALPACA_TIF)
        except Exception as exc:
            logger.warning("short sell rejected for %s: %s", sym, exc)
            _mark_short_reject(sym)
            continue

        await _persist_fill(sym, "sell", short_extra, "ALPACA", ack)
        _record_trade(sym)

        if CFG["use_trailing_stops"]:
            try:
                pos_meta = state.positions.get(sym)
                available = _position_available_qty(pos_meta) if pos_meta else short_extra
                if pos_meta and available <= 0:
                    logger.info(
                        "skip trailing stop for %s (no available qty; held_for_orders=%s)",
                        sym,
                        pos_meta.get("qty_held_for_orders"),
                    )
                else:
                    opens = await get_open_orders(sym)
                    has_exit_order = any(o.get("side") == "buy" for o in opens)
                    if has_exit_order:
                        logger.info(
                            "skip trailing stop for %s (existing buy-side exits=%s)",
                            sym,
                            len(opens),
                        )
                    else:
                        await _call_blocking(
                            alp.trailing_buy_to_cover,
                            sym,
                            CFG["trail_pct"],
                            short_extra,
                            ALPACA_TIF,
                        )
            except HTTPStatusError as exc:
                body = exc.response.text if exc.response else ""
                status = exc.response.status_code if exc.response else "HTTP"
                logger.warning("trailing stop placement failed for %s: %s %s", sym, status, body)
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
            if not _should_execute_order(sym, "buy", is_entry=False, is_exit=True, reason="cover_short"):
                continue
            try:
                ack = await _call_blocking(alp.buy_to_cover, sym, cover_qty, ALPACA_TIF)
            except Exception as exc:
                logger.warning("buy-to-cover failed for %s: %s", sym, exc)
                await _submit_market(sym, "buy", cover_qty)
                _SHORT_COOLDOWNS.pop(sym, None)
                _record_trade(sym)
            else:
                _SHORT_COOLDOWNS.pop(sym, None)
                await _persist_fill(sym, "buy", cover_qty, "ALPACA", ack)
                _record_trade(sym)

        if residual > 0:
            if not _should_execute_order(sym, "buy", is_entry=True, is_exit=False, reason="open_long"):
                continue
            await _submit_market(sym, "buy", residual)
            _record_trade(sym)


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
