import asyncio
import json
import logging
import math
import os
import random
from datetime import datetime, timezone
from functools import partial
from pathlib import Path
from typing import Any, Dict, List

import asyncpg
import pandas as pd

from libs.kronos_client.client import ForecastClient
from .llm_hooks import generate_rationale, policy_filter, _fetch_quiver_summary
from libs.llm.settings import is_llm_enabled

from .model_swap import Model

def _parse_horizons(raw: str) -> List[int]:
    values: List[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            values.append(int(chunk))
        except ValueError:
            continue
    return values or [5]


NBEATS_URL = os.getenv("FORECAST_URL_NBEATS", "http://kronos-nbeats:8080")
_KRONOS_TIMEOUT = float(os.getenv("FORECAST_CLIENT_TIMEOUT", "2.0"))
KRONOS_HORIZON = _parse_horizons(os.getenv("KRONOS_HORIZON_STEPS", "5"))

kronos_client = ForecastClient(NBEATS_URL, timeout=_KRONOS_TIMEOUT)

DB_DSN = f"postgresql://{os.getenv('POSTGRES_USER','pulse')}:{os.getenv('POSTGRES_PASSWORD','pulsepass')}@" \
         f"{os.getenv('POSTGRES_HOST','db')}:{os.getenv('POSTGRES_PORT','5432')}/{os.getenv('POSTGRES_DB','pulse')}"

# Preferred order of sources for forecast universe:
# 1) FORECAST_TICKERS (explicit override)
# 2) SYMBOLS (shared env across services)
# 3) Fallback crypto only
ROOT_DIR = Path(__file__).resolve().parents[2]
UNIVERSE_FILE = Path(os.getenv("FORECAST_UNIVERSE_FILE", "services/ingest/universe_symbols.txt"))
if not UNIVERSE_FILE.is_absolute():
    UNIVERSE_FILE = ROOT_DIR / UNIVERSE_FILE

_tickers_env = os.getenv("FORECAST_TICKERS") or os.getenv("SYMBOLS", "")
ENV_TICKERS = [t.strip() for t in _tickers_env.split(",") if t and t.strip()] or ["BTCUSD", "ETHUSD"]
FORECAST_MAX_TICKERS = int(os.getenv("FORECAST_MAX_TICKERS", "50"))
HORIZON = os.getenv("FORECAST_HORIZON", "5d")
MODEL = Model()
MODEL_NAME = MODEL.__class__.__name__
logger = logging.getLogger(__name__)
POLICY_MIN_LIQUIDITY = float(os.getenv("LLM_POLICY_MIN_LIQUIDITY", "2000000"))
MACRO_REGIME = os.getenv("MACRO_REGIME", "neutral")


def _build_series_for_kronos(series: pd.Series) -> List[float]:
    pct = series.pct_change().dropna()
    window = pct.tail(256).tolist()
    if len(window) < 32:
        return []
    return window


async def _fetch_kronos_bundle(ticker: str, price_series: pd.Series) -> Dict[str, Any]:
    prepared = _build_series_for_kronos(price_series)
    if not prepared:
        return {"status": "skipped", "reason": "insufficient_series"}
    loop = asyncio.get_running_loop()
    call = partial(kronos_client.forecast, ticker, KRONOS_HORIZON, prepared)
    try:
        response = await loop.run_in_executor(None, call)
    except Exception as exc:  # pragma: no cover - network errors observed at runtime
        return {"status": "error", "detail": f"{type(exc).__name__}: {exc}"}
    return {
        "status": "ok",
        "yhat": response.yhat,
        "q": response.q,
        "meta": response.meta,
    }

async def load_covariates(conn: asyncpg.Connection, ticker: str) -> Dict[str, Any]:
    social_rows = await conn.fetch(
        """SELECT ts, window_minutes, msg_rate, senti_mean, senti_std, top_handles
           FROM social_features
           WHERE ticker=$1
           ORDER BY ts DESC, window_minutes ASC
           LIMIT 12""",
        ticker,
    )
    social_features: List[Dict[str, Any]] = [
        {
            "ts": row["ts"],
            "window": row["window_minutes"],
            "rate": float(row["msg_rate"]) if row["msg_rate"] is not None else None,
            "senti_mean": float(row["senti_mean"]) if row["senti_mean"] is not None else None,
            "senti_std": float(row["senti_std"]) if row["senti_std"] is not None else None,
            "top_handles": row["top_handles"],
        }
        for row in social_rows
    ]
    return {"social": social_features}


async def _recent_headlines(conn: asyncpg.Connection, ticker: str, limit: int = 5) -> List[str]:
    rows = await conn.fetch(
        """SELECT text FROM social_messages
            WHERE ticker=$1 AND text IS NOT NULL
            ORDER BY ts DESC
            LIMIT $2""",
        ticker,
        limit,
    )
    return [r["text"] for r in rows]


async def _next_earnings(conn: asyncpg.Connection, ticker: str) -> str:
    row = await conn.fetchrow(
        """SELECT start_ts FROM event_windows
            WHERE ticker=$1 AND kind='earnings' AND start_ts > NOW()
            ORDER BY start_ts ASC
            LIMIT 1""",
        ticker,
    )
    if not row:
        return "none"
    return row["start_ts"].date().isoformat()


async def _estimate_liquidity(conn: asyncpg.Connection, ticker: str) -> float:
    row = await conn.fetchrow(
        """SELECT SUM(price * COALESCE(size, 0)) AS notional
            FROM trades
            WHERE ticker=$1 AND ts > NOW() - INTERVAL '1 day'""",
        ticker,
    )
    return float(row["notional"] or 0.0)


def _extract_factors(covariates: Dict[str, Any]) -> Dict[str, float]:
    social = covariates.get("social") or []
    if not social:
        return {}
    latest = social[0]
    factors: Dict[str, float] = {}
    for key in ("rate", "senti_mean", "senti_std"):
        value = latest.get(key)
        if value is not None:
            factors[key] = float(value)
    return factors


def _coverage_pct(covariates: Dict[str, Any]) -> float:
    social = covariates.get("social") or []
    if not social:
        return 0.0
    latest = social[0]
    total = len(latest)
    filled = sum(1 for v in latest.values() if v not in (None, ""))
    if total == 0:
        return 0.0
    return filled / total


async def _attach_llm_context(
    conn: asyncpg.Connection,
    ticker: str,
    signal_value: float,
    signal_label: str,
    covariates: Dict[str, Any],
) -> Dict[str, Any]:
    if not is_llm_enabled():
        return {"rationale": None, "policy": None}
    as_of = datetime.now(timezone.utc).isoformat()
    factors = _extract_factors(covariates)
    factor_dispersion = max((abs(v) for v in factors.values()), default=0.0)
    coverage_pct = _coverage_pct(covariates)

    headlines = await _recent_headlines(conn, ticker)
    earnings_date = await _next_earnings(conn, ticker)
    liquidity = await _estimate_liquidity(conn, ticker)
    quiver_summary = await _fetch_quiver_summary(conn, ticker)
    quiver_bullets = quiver_summary.get("bullets", [])
    headline_items = list(headlines) + quiver_bullets

    llm_meta: Dict[str, Any] = {"rationale": None, "policy": None}
    try:
        rationale_task = asyncio.create_task(
            generate_rationale(
                ticker=ticker,
                as_of=as_of,
                signal_value=signal_value,
                signal_label=signal_label,
                factors=factors,
                headlines=headline_items,
            )
        )
        policy_task = asyncio.create_task(
            policy_filter(
                ticker=ticker,
                as_of=as_of,
                earnings_date=earnings_date,
                liquidity_usd=liquidity,
                borrow_ok=True,
                factor_dispersion=factor_dispersion,
                coverage_pct=coverage_pct,
                macro_regime=MACRO_REGIME,
                signal_label=signal_label,
                signal_value=signal_value,
                min_liquidity=POLICY_MIN_LIQUIDITY,
                extra_inputs=quiver_summary.get("policy_flags"),
            )
        )
        rationale, policy = await asyncio.gather(rationale_task, policy_task)
        llm_meta["rationale"] = {
            "text": rationale.get("text"),
            "cached": rationale.get("cached", False),
            "prompt_key": rationale.get("prompt_key"),
            "prompt_version": rationale.get("prompt_version"),
            "prompt_hash": rationale.get("prompt_hash"),
        }
        decision = policy.get("json") or {}
        llm_meta["policy"] = {
            "raw": policy.get("text"),
            "decision": decision,
            "cached": policy.get("cached", False),
            "prompt_key": policy.get("prompt_key"),
            "prompt_version": policy.get("prompt_version"),
            "prompt_hash": policy.get("prompt_hash"),
            "success": policy.get("success", True),
            "error": policy.get("error"),
            "shadow_mode": policy.get("shadow", False),
            "shadow_allow": decision.get("allow") if policy.get("shadow") else None,
            "shadow_flags": decision.get("flags") if policy.get("shadow") else None,
            "extra_inputs": policy.get("extra_inputs", {}),
        }
    except Exception as exc:  # pragma: no cover - defensive fallback
        llm_meta["error"] = str(exc)

    llm_meta.update(
        {
            "inputs": {
                "as_of": as_of,
                "headlines": headline_items,
                "earnings": earnings_date,
                "liquidity": liquidity,
                "factor_dispersion": factor_dispersion,
                "coverage_pct": coverage_pct,
                "quiver": quiver_summary.get("aggregates", {}),
            }
        }
    )
    llm_meta["quiver"] = quiver_summary
    return llm_meta

async def _load_price_series(conn, ticker: str) -> pd.Series | None:
    trade_rows = await conn.fetch(
        """SELECT ts, price FROM trades WHERE ticker=$1 AND ts > NOW() - INTERVAL '10 minutes' ORDER BY ts ASC""",
        ticker,
    )
    if len(trade_rows) >= 5:
        return pd.Series([r["price"] for r in trade_rows], index=[r["ts"] for r in trade_rows])

    daily_rows = await conn.fetch(
        """SELECT ds, y FROM daily_returns WHERE symbol=$1 ORDER BY ds DESC LIMIT 256""",
        ticker,
    )
    if not daily_rows:
        return None
    daily_rows = list(reversed(daily_rows))
    prices: List[float] = []
    current = 1.0
    for row in daily_rows:
        try:
            current *= math.exp(float(row["y"]))
        except Exception:
            continue
        prices.append(current)
    if not prices:
        return None
    series = pd.Series(prices, index=[row["ds"] for row in daily_rows[: len(prices)]])
    last_price = await conn.fetchval(
        """SELECT NULLIF(meta->>'last_price','')::numeric FROM symbols WHERE ticker=$1""",
        ticker,
    )
    if last_price:
        try:
            scale = float(last_price) / float(series.iloc[-1])
            series = series * scale
        except Exception:
            pass
    return series


async def forecast_once(conn, ticker: str):
    s = await _load_price_series(conn, ticker)
    if s is None or s.empty:
        return
    covariates = await load_covariates(conn, ticker)
    kronos_bundle = await _fetch_kronos_bundle(ticker, s)
    factors = _extract_factors(covariates)
    feature_snapshot = {
        "social": [
            {
                "window": item["window"],
                "rate": item["rate"],
                "senti_mean": item["senti_mean"],
                "senti_std": item["senti_std"],
            }
            for item in covariates.get("social", [])
        ],
        "kronos": kronos_bundle,
        "factors": factors,
    }
    latest_price = float(s.iloc[-1])
    mean: float | None = None
    lower: float | None = None
    upper: float | None = None
    model_name = MODEL_NAME
    kronos_used = False

    if kronos_bundle.get("status") == "ok":
        horizon_key = str(KRONOS_HORIZON[0])
        yhat = kronos_bundle.get("yhat") or {}
        quantiles = kronos_bundle.get("q") or {}
        meta = kronos_bundle.get("meta") or {}

        def _as_price(ret: float | None) -> float | None:
            if ret is None:
                return None
            try:
                return latest_price * math.exp(float(ret))
            except Exception:
                return None

        mean = _as_price(yhat.get(horizon_key))
        quantile_row = quantiles.get(horizon_key) or {}
        p05 = _as_price(quantile_row.get("p05"))
        p50 = _as_price(quantile_row.get("p50"))
        p95 = _as_price(quantile_row.get("p95"))
        lower = p05 or p50
        upper = p95 or p50
        if lower and upper and lower > upper:
            lower, upper = upper, lower
        model_name = str(meta.get("model") or "kronos-nbeats")
        kronos_used = mean is not None and lower is not None and upper is not None

    if not kronos_used:
        mean, lower, upper = MODEL.predict(s, covariates=covariates)
        kronos_used = False
        model_name = MODEL_NAME

    signal_label = "BUY" if (mean or latest_price) >= latest_price else "SELL"
    llm_meta = await _attach_llm_context(conn, ticker, float(mean or 0.0), signal_label, covariates)
    feature_snapshot["llm"] = llm_meta
    feature_snapshot.setdefault("kronos", kronos_bundle)
    feature_snapshot["kronos"]["used_for_forecast"] = kronos_used
    await conn.execute("""INSERT INTO forecasts(ts,ticker,horizon,model,mean,lower,upper,features)
                          VALUES(NOW(), $1, $2, $3, $4, $5, $6, $7)""",
                       ticker, HORIZON, model_name, mean, lower, upper, json.dumps(feature_snapshot))

def _load_file_universe() -> List[str]:
    if not UNIVERSE_FILE.exists():
        return []
    symbols: List[str] = []
    try:
        with UNIVERSE_FILE.open("r", encoding="utf-8") as handle:
            for line in handle:
                sym = line.strip().upper()
                if sym and not sym.startswith("#"):
                    symbols.append(sym)
    except Exception:
        return []
    # Preserve order, drop duplicates
    deduped: Dict[str, None] = {}
    for sym in symbols:
        deduped.setdefault(sym, None)
    return list(deduped.keys())


async def main():
    logging.basicConfig(level=logging.INFO)
    while True:
        try:
            conn = await asyncpg.connect(dsn=DB_DSN)
        except Exception as exc:
            logger.warning("DB connection unavailable, retrying: %s", exc)
            await asyncio.sleep(5)
            continue
        try:
            # Dynamic universe: union of env and discovered equities from DB symbols
            try:
                rows = await conn.fetch("SELECT ticker FROM symbols WHERE class='equity' ORDER BY ticker ASC LIMIT 500")
                dyn = [r[0] for r in rows]
            except Exception:
                dyn = []

            universe_ticks = _load_file_universe()
            if not universe_ticks:
                source_list = ENV_TICKERS
            else:
                source_list = universe_ticks

            merged = source_list + [sym for sym in ENV_TICKERS if sym not in source_list] + [sym for sym in dyn if sym not in source_list]
            all_ticks = []
            seen = set()
            for sym in merged:
                if sym not in seen:
                    seen.add(sym)
                    all_ticks.append(sym)
            if len(all_ticks) > FORECAST_MAX_TICKERS:
                random.shuffle(all_ticks)
                all_ticks = all_ticks[:FORECAST_MAX_TICKERS]
            for t in all_ticks:
                await forecast_once(conn, t)
        finally:
            await conn.close()
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
