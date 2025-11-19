#!/usr/bin/env python3
"""Nightly planner: pick daily candidates, evaluate plans, and output active symbols."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import logging
import math
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

import psycopg2

from libs.llm.reviews import LLMReviewStore
from libs.llm.run_and_log import LLMRunner
from libs.llm.settings import is_llm_enabled
from libs.llm.schemas import DAILY_GO_NOGO_SCHEMA
from libs.planner.daily_plan_eval import (
    DailyPlan,
    PlanAction,
    PlanEvaluation,
    evaluate_plan,
)
from libs.planner.llm_filters import get_plan_llm
from libs.universe.signals import SignalMetrics, compute_signal_metrics

# Locked-in defaults
WEEKLY_UNIVERSE_SIZE = int(os.getenv("WEEKLY_UNIVERSE_SIZE", "100"))
DAILY_CANDIDATE_COUNT = int(os.getenv("DAILY_CANDIDATE_COUNT", "10"))
DAILY_MAX_TRADES = int(os.getenv("DAILY_MAX_TRADES", "20"))
MAX_ACTIONS_PER_STOCK_PER_DAY = int(os.getenv("MAX_ACTIONS_PER_STOCK_PER_DAY", "2"))
MAX_SYMBOLS_TODAY = DAILY_MAX_TRADES // max(1, MAX_ACTIONS_PER_STOCK_PER_DAY)
DAILY_GO_NOGO_LIMIT = int(os.getenv("DAILY_GO_NOGO_LIMIT", "10"))
DAILY_GO_NOGO_LOOKBACK_HOURS = int(os.getenv("DAILY_GO_NOGO_LOOKBACK_HOURS", "48"))
DAILY_GO_NOGO_NEWS_LIMIT = int(os.getenv("DAILY_GO_NOGO_NEWS_LIMIT", "5"))
DAILY_GO_NOGO_SOCIAL_LIMIT = int(os.getenv("DAILY_GO_NOGO_SOCIAL_LIMIT", "5"))

ALPHA = float(os.getenv("PLAN_ALPHA", "0.05"))
EPS_MU = float(os.getenv("PLAN_EPS_MU", "0.0005"))
EPS_P = float(os.getenv("PLAN_EPS_P", "0.05"))
P_LOSS_MAX = float(os.getenv("PLAN_P_LOSS_MAX", "0.45"))
MU_MIN = float(os.getenv("PLAN_MU_MIN", "0.0001"))
MU_MIN_SHORT = float(os.getenv("PLAN_MU_MIN_SHORT", os.getenv("PLAN_MU_MIN", "0.0001")))
LAMBDA_RISK = float(os.getenv("PLAN_LAMBDA_RISK", "1.0"))

REPORTS_DIR = Path("reports")
FORECAST_HORIZON = os.getenv("PLAN_FORECAST_HORIZON", "5d")
PLAN_LLM = get_plan_llm()
logger = logging.getLogger("daily_plan_builder")


SECTOR_ETF = {
    "COMMUNICATION SERVICES": "XLC",
    "CONSUMER DISCRETIONARY": "XLY",
    "CONSUMER STAPLES": "XLP",
    "ENERGY": "XLE",
    "FINANCIALS": "XLF",
    "HEALTH CARE": "XLV",
    "INDUSTRIALS": "XLI",
    "INFORMATION TECHNOLOGY": "XLK",
    "MATERIALS": "XLB",
    "REAL ESTATE": "XLRE",
    "UTILITIES": "XLU",
}


def database_url() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    user = os.getenv("POSTGRES_USER", "pulse")
    password = os.getenv("POSTGRES_PASSWORD", "pulsepass")
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    name = os.getenv("POSTGRES_DB", "pulse")
    return f"postgresql://{user}:{password}@{host}:{port}/{name}"


def fetch_weekly_universe(conn) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol
        FROM signal_universe_100
        WHERE as_of = (SELECT MAX(as_of) FROM signal_universe_100)
        ORDER BY rank ASC
        LIMIT %s
        """,
        (WEEKLY_UNIVERSE_SIZE,),
    )
    rows = [row[0] for row in cur.fetchall()]
    cur.close()
    return rows



def fetch_positions(conn) -> Dict[str, float]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT DISTINCT ON (ticker) ticker, qty
        FROM positions
        ORDER BY ticker, ts DESC
        """
    )
    rows = cur.fetchall()
    cur.close()
    return {row[0]: float(row[1]) for row in rows if row[1] not in (None, 0, 0.0)}


def fetch_recent_news(conn, symbol: str, hours: int, limit: int) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(title, text)
        FROM fmp_news
        WHERE symbol=%s AND ts >= NOW() - make_interval(hours => %s::int)
        ORDER BY ts DESC
        LIMIT %s
        """,
        (symbol, hours, limit),
    )
    rows = [row[0] for row in cur.fetchall() if row and row[0]]
    cur.close()
    return rows


def fetch_social_headlines(conn, symbol: str, hours: int, limit: int) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT text
        FROM social_messages
        WHERE ticker=%s AND ts >= NOW() - make_interval(hours => %s::int)
        ORDER BY ts DESC
        LIMIT %s
        """,
        (symbol, hours, limit),
    )
    rows = [row[0] for row in cur.fetchall() if row and row[0]]
    cur.close()
    return rows


def fetch_active_breakers(conn, symbol: str) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT reason
        FROM circuit_breakers
        WHERE scope='symbol'
          AND key=%s
          AND active=TRUE
          AND (expires_at IS NULL OR expires_at > NOW())
        """,
        (symbol,),
    )
    rows = [row[0] for row in cur.fetchall() if row and row[0]]
    cur.close()
    return rows


def fetch_last_price(conn, symbol: str) -> float:
    cur = conn.cursor()
    cur.execute(
        "SELECT COALESCE((meta->>'last_price')::numeric, 0) FROM symbols WHERE ticker=%s",
        (symbol,),
    )
    row = cur.fetchone()
    cur.close()
    if not row or row[0] in (None, 0):
        return 0.0
    return float(row[0])


def fetch_market_movement(conn, symbol: str, hours: int) -> str:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT price
        FROM trades
        WHERE ticker=%s AND ts >= NOW() - make_interval(hours => %s::int)
        ORDER BY ts ASC
        LIMIT 1
        """,
        (symbol, hours),
    )
    row_start = cur.fetchone()
    cur.execute(
        """
        SELECT price
        FROM trades
        WHERE ticker=%s
        ORDER BY ts DESC
        LIMIT 1
        """,
        (symbol,),
    )
    row_end = cur.fetchone()
    cur.close()
    start_price = float(row_start[0]) if row_start and row_start[0] else None
    end_price = float(row_end[0]) if row_end and row_end[0] else None
    if end_price is None or end_price <= 0:
        end_price = fetch_last_price(conn, symbol)
    if start_price is None or start_price <= 0 or not end_price:
        return "insufficient trade data"
    delta = (end_price / start_price) - 1.0
    return f"{delta:+.2%} over {hours}h (from {start_price:.2f} -> {end_price:.2f})"


def summarize_social_activity(conn, symbol: str) -> str:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT window_minutes, msg_rate, senti_mean, senti_std
        FROM social_features
        WHERE ticker=%s
        ORDER BY ts DESC, window_minutes ASC
        LIMIT 2
        """,
        (symbol,),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return "no social data"
    parts = []
    for window, rate, senti_mean, senti_std in rows:
        parts.append(
            f"{window}m rate {float(rate or 0.0):.2f} sentiment {float(senti_mean or 0.0):+.2f}±{float(senti_std or 0.0):.2f}"
        )
    return "; ".join(parts)


def load_signal_meta(conn) -> Dict[str, Dict[str, Any]]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT symbol, meta
        FROM signal_universe_100
        WHERE as_of = (SELECT MAX(as_of) FROM signal_universe_100)
        """
    )
    rows = cur.fetchall()
    cur.close()
    meta: Dict[str, Dict[str, Any]] = {}
    for symbol, payload in rows:
        try:
            meta[symbol] = json.loads(payload) if isinstance(payload, str) else (payload or {})
        except Exception:
            meta[symbol] = {}
    return meta


def load_forecast_predictions(target_date: dt.date) -> Dict[str, Dict[str, Any]]:
    key = target_date.isoformat()
    reports_dir = REPORTS_DIR
    if not reports_dir.exists():
        return {}
    files = sorted(reports_dir.glob("top100_predictions_*.json"), reverse=True)
    for file in files:
        try:
            payload = json.loads(file.read_text())
        except Exception:
            continue
        horizons = payload.get("horizons") or {}
        entries = horizons.get(key)
        if not entries:
            continue
        mapping: Dict[str, Dict[str, Any]] = {}
        for entry in entries:
            symbol = (entry.get("unique_id") or "").upper()
            if symbol:
                mapping[symbol] = entry
        if mapping:
            return mapping
    return {}


def has_upcoming_earnings(conn, symbol: str, target_date: dt.date, days: int = 3) -> bool:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT 1
        FROM event_windows
        WHERE ticker=%s AND kind='earnings'
          AND start_ts::date BETWEEN %s AND %s
        LIMIT 1
        """,
        (symbol, target_date, target_date + dt.timedelta(days=days)),
    )
    row = cur.fetchone()
    cur.close()
    return bool(row)


def compute_signal_pair(
    conn,
    symbol: str,
    horizon: str,
    reference_date: dt.date,
) -> Tuple[Optional[SignalMetrics], Optional[SignalMetrics]]:
    today = compute_signal_metrics(conn, symbol, horizon=horizon)
    cutoff_prev = (reference_date - dt.timedelta(days=1)).isoformat() + " 23:59:59"
    previous = compute_signal_metrics(conn, symbol, horizon=horizon, cutoff_ts=cutoff_prev)
    return today, previous


def select_candidates(
    conn,
    symbols: Sequence[str],
    reference_date: dt.date,
    overnight_holds: Dict[str, float],
    signal_meta: Dict[str, Dict[str, Any]],
    forecasts: Dict[str, Dict[str, Any]],
) -> Tuple[List[str], Dict[str, Dict[str, Any]]]:
    deltas: Dict[str, Dict[str, Any]] = {}
    for sym in symbols:
        today_metrics, prev_metrics = compute_signal_pair(conn, sym, FORECAST_HORIZON, reference_date)
        if not today_metrics:
            continue
        signal_today = today_metrics.signal_strength
        signal_prev = prev_metrics.signal_strength if prev_metrics else 0.0
        delta = abs(signal_today - signal_prev)
        meta = signal_meta.get(sym, {})
        llm_meta = meta.get("llm") or {}
        risk_flag = (llm_meta.get("risk_flag") or "normal").lower()
        sentiment = float(llm_meta.get("sentiment") or 0.0)
        if risk_flag == "avoid":
            continue
        risk_multiplier = 1.0
        if risk_flag == "elevated":
            risk_multiplier = 0.6
        sentiment_boost = max(0.3, 1.0 + 0.25 * sentiment)
        actionability = delta * risk_multiplier * sentiment_boost
        earnings_soon = has_upcoming_earnings(conn, sym, reference_date, days=3)
        forecast_meta = forecasts.get(sym.upper()) or {}
        forecast_mu = float(forecast_meta.get("pred_q0.50") or 0.0)
        deltas[sym] = {
            "signal_today": signal_today,
            "signal_prev": signal_prev,
            "delta": delta,
            "actionability": actionability,
            "vol_5d": today_metrics.vol_5d,
            "mean_excess": today_metrics.excess_return,
            "llm": {
                "risk_flag": risk_flag,
                "sentiment": sentiment,
                "comment": llm_meta.get("comment", ""),
            },
            "earnings_soon": earnings_soon,
            "forecast": forecast_meta,
            "forecast_mu": forecast_mu,
        }
    base_candidates = sorted(
        (sym for sym in deltas if sym not in overnight_holds),
        key=lambda s: (abs(deltas[s].get("forecast_mu", 0.0)), deltas[s]["actionability"]),
        reverse=True,
    )
    selected = list(overnight_holds.keys())
    new_count = 0
    for sym in base_candidates:
        if sym in selected:
            continue
        if new_count >= DAILY_CANDIDATE_COUNT:
            break
        selected.append(sym)
        new_count += 1
    return selected, deltas


def build_plan(symbol: str, position: float, info: Dict[str, Any]) -> Optional[DailyPlan]:
    signal_value = info.get("signal_today", 0.0)
    llm_meta = info.get("llm", {})
    if position > 0:
        actions = [
            PlanAction(
                name="exit_close",
                kind="exit",
                window="15:45-16:00",
                direction="sell",
                sizing="full",
            )
        ]
        side: Literal["long", "short"] = "long"
        reason = "overnight_long_exit"
        return DailyPlan(symbol=symbol, side=side, actions=actions, reason=reason)
    elif position < 0:
        actions = [
            PlanAction(
                name="cover_close",
                kind="exit",
                window="15:45-16:00",
                direction="buy",
                sizing="full",
            )
        ]
        side = "short"
        reason = "overnight_short_exit"
        return DailyPlan(symbol=symbol, side=side, actions=actions, reason=reason)

    decision = PLAN_LLM.suggest_plan_type(
        {
            "symbol": symbol,
            "side_signal": "long" if signal_value >= 0 else "short",
            "signal_strength": signal_value,
            "delta_signal": info.get("delta", 0.0),
            "risk_flag": llm_meta.get("risk_flag", "normal"),
            "sentiment": llm_meta.get("sentiment", 0.0),
            "earnings_soon": info.get("earnings_soon", False),
            "overnight": False,
            "factors": "na",
        }
    )
    plan_type = decision.plan_type
    if plan_type == "NoTrade":
        return None
    desired_side = "long" if signal_value >= 0 else "short"
    if desired_side == "short" and plan_type.startswith("Long"):
        plan_type = plan_type.replace("Long", "Short", 1)
    elif desired_side == "long" and plan_type.startswith("Short"):
        plan_type = plan_type.replace("Short", "Long", 1)

    if plan_type == "ShortIntraday":
        side = "short"
        actions = [
            PlanAction("entry_open", "entry", "09:35-10:00", "sell", "notional"),
            PlanAction("exit_close", "exit", "15:45-16:00", "buy", "full"),
        ]
    elif plan_type == "LongHold":
        side = "long"
        actions = [
            PlanAction("entry_morning", "entry", "09:40-11:00", "buy", "notional"),
            PlanAction("exit_close", "exit", "15:55-16:00", "sell", "full"),
        ]
    elif plan_type == "ShortHold":
        side = "short"
        actions = [
            PlanAction("entry_morning", "entry", "09:40-11:00", "sell", "notional"),
            PlanAction("exit_close", "exit", "15:55-16:00", "buy", "full"),
        ]
    elif plan_type == "ShortIntraday":
        side = "short"
        actions = [
            PlanAction("entry_open", "entry", "09:35-10:00", "sell", "notional"),
            PlanAction("exit_close", "exit", "15:45-16:00", "buy", "full"),
        ]
    else:  # LongIntraday default
        side = "long"
        actions = [
            PlanAction("entry_open", "entry", "09:35-10:00", "buy", "notional"),
            PlanAction("exit_close", "exit", "15:45-16:00", "sell", "full"),
        ]

    reason = f"plan:{plan_type}|{decision.comment or 'llm'}"
    return DailyPlan(symbol=symbol, side=side, actions=actions, reason=reason)


def _position_context(symbol: str, plan: DailyPlan, positions: Dict[str, float], delta_info: Dict[str, Any]) -> str:
    pos = float(positions.get(symbol, 0.0))
    llm_meta = delta_info.get("llm") or {}
    sentiment = llm_meta.get("sentiment", 0.0)
    risk_flag = llm_meta.get("risk_flag", "normal")
    delta = float(delta_info.get("delta") or 0.0)
    return f"plan_side={plan.side} delta={delta:.4f} risk={risk_flag} senti={sentiment:+.2f} pos={pos:.0f}"


def build_go_nogo_context(
    conn,
    symbol: str,
    *,
    plan_eval: PlanEvaluation,
    target_date: dt.date,
    positions: Dict[str, float],
    delta_info: Dict[str, Any],
) -> Dict[str, Any]:
    headlines = fetch_recent_news(conn, symbol, DAILY_GO_NOGO_LOOKBACK_HOURS, DAILY_GO_NOGO_NEWS_LIMIT)
    social_items = fetch_social_headlines(conn, symbol, DAILY_GO_NOGO_LOOKBACK_HOURS, DAILY_GO_NOGO_SOCIAL_LIMIT)
    market_moves = fetch_market_movement(conn, symbol, DAILY_GO_NOGO_LOOKBACK_HOURS)
    breakers = fetch_active_breakers(conn, symbol)
    context = {
        "symbol": symbol,
        "as_of": target_date.isoformat(),
        "price": f"{fetch_last_price(conn, symbol):.2f}",
        "social_activity": summarize_social_activity(conn, symbol),
        "headlines": " ; ".join(headlines) if headlines else "none",
        "news": " ; ".join(social_items) if social_items else "none",
        "market_movements": market_moves,
        "circuit_breakers": ", ".join(breakers) if breakers else "none",
        "position_context": _position_context(symbol, plan_eval.plan, positions, delta_info),
    }
    return context


def run_daily_go_nogo_checks(
    conn,
    active_evals: List[PlanEvaluation],
    target_date: dt.date,
    positions: Dict[str, float],
    delta_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Dict[str, Any]]:
    if not active_evals or not is_llm_enabled():
        if not is_llm_enabled():
            logger.info("[planner] LLM disabled – skipping daily go/no-go checks.")
        return {}
    runner = LLMRunner()
    store = LLMReviewStore()
    results: Dict[str, Dict[str, Any]] = {}
    payloads: List[Dict[str, Any]] = []
    for eval_item in active_evals[: min(DAILY_GO_NOGO_LIMIT, len(active_evals))]:
        symbol = eval_item.plan.symbol
        delta_info = delta_map.get(symbol, {})
        context = build_go_nogo_context(
            conn,
            symbol,
            plan_eval=eval_item,
            target_date=target_date,
            positions=positions,
            delta_info=delta_info,
        )
        try:
            resp = runner.cached_call(
                "daily_go_nogo",
                context,
                schema=DAILY_GO_NOGO_SCHEMA,
                max_tokens=600,
                use_cache=False,
            )
        except Exception as exc:
            logger.warning("Go/No-Go LLM failed for %s: %s", symbol, exc)
            results[symbol] = {"error": str(exc), "context": context}
            continue
        decision = resp.get("json") or {}
        results[symbol] = {
            "decision": decision,
            "text": resp.get("text"),
            "cached": resp.get("cached", False),
            "prompt_version": resp.get("prompt_version"),
        }
        extra = {
            "score": eval_item.score,
            "plan_side": eval_item.plan.side,
            "plan_reason": eval_item.plan.reason,
        }
        try:
            store.upsert_review(
                scope="daily_go_nogo",
                symbol=symbol,
                as_of=target_date,
                prompt_key=resp.get("prompt_key", "daily_go_nogo"),
                prompt_version=resp.get("prompt_version", "1.0.0"),
                prompt_hash=resp.get("prompt_hash", ""),
                output_json=decision,
                output_text=resp.get("text"),
                input_payload=context,
                extra=extra,
                cached=bool(resp.get("cached", False)),
            )
        except Exception:
            pass
        payloads.append(
            {
                "symbol": symbol,
                "context": context,
                "result": {
                    "decision": decision,
                    "text": resp.get("text"),
                    "cached": resp.get("cached", False),
                    "prompt_version": resp.get("prompt_version"),
                },
            }
        )
    if payloads:
        REPORTS_DIR.mkdir(parents=True, exist_ok=True)
        out_path = REPORTS_DIR / f"daily_llm_check_{target_date.isoformat()}.json"
        out_path.write_text(json.dumps({"date": target_date.isoformat(), "results": payloads}, indent=2))
    return results


def mean_and_vol(deltas: Dict[str, Dict[str, Any]], symbol: str) -> Tuple[float, float]:
    info = deltas[symbol]
    forecast = info.get("forecast") or {}
    if forecast:
        mean_daily = float(forecast.get("pred_q0.50") or 0.0)
        q95 = forecast.get("pred_q0.95")
        q05 = forecast.get("pred_q0.05")
        vol_daily = float(forecast.get("pred_sigma", 0.0) or 0.0)
        if (vol_daily <= 0) and (q95 is not None) and (q05 is not None):
            try:
                vol_daily = abs(float(q95) - float(q05)) / 3.29
            except Exception:
                vol_daily = 0.0
        if vol_daily <= 0:
            vol_daily = abs(info.get("vol_5d", 0.02)) / (5 ** 0.5)
        return mean_daily, vol_daily
    mean_excess = info["mean_excess"]
    vol_5d = info.get("vol_5d", 0.02)
    mean_daily = mean_excess / 5.0
    vol_daily = vol_5d / (5 ** 0.5)
    return mean_daily, vol_daily


def evaluate_candidates(
    plans: Dict[str, DailyPlan],
    deltas: Dict[str, Dict[str, Any]],
) -> List[PlanEvaluation]:
    evaluations: List[PlanEvaluation] = []
    for sym, plan in plans.items():
        mean_daily, vol_daily = mean_and_vol(deltas, sym)
        eval_result = evaluate_plan(
            plan,
            mean_daily_return=mean_daily,
            vol_daily=vol_daily,
            eps_mu=EPS_MU,
            eps_p=EPS_P,
            p_loss_max=P_LOSS_MAX,
            mu_min=MU_MIN,
            lambda_risk=LAMBDA_RISK,
            mu_min_short=MU_MIN_SHORT,
        )
        if plan.reason.startswith("overnight_"):
            eval_result.ok_to_trade = True
            eval_result.score = abs(eval_result.mu)
        evaluations.append(eval_result)
    return evaluations


def select_active_plans(evaluations: List[PlanEvaluation]) -> List[PlanEvaluation]:
    exits = [ev for ev in evaluations if ev.ok_to_trade and ev.plan.reason.startswith("overnight_")]
    entries = [ev for ev in evaluations if ev.ok_to_trade and ev not in exits]
    entries.sort(key=lambda ev: ev.score, reverse=True)
    active_entries = entries[:MAX_SYMBOLS_TODAY]
    return exits + active_entries


def write_report(
    date: dt.date,
    evaluations: List[PlanEvaluation],
    active: List[PlanEvaluation],
    delta_map: Dict[str, Dict[str, Any]],
    llm_go_nogo: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    def _attach_meta(ev: PlanEvaluation) -> Dict[str, Any]:
        record = ev.serialize()
        forecast = delta_map.get(ev.plan.symbol, {}).get("forecast")
        if forecast:
            record["forecast"] = forecast
        if llm_go_nogo and ev.plan.symbol in llm_go_nogo:
            record["llm_go_nogo"] = llm_go_nogo[ev.plan.symbol]
        return record

    payload = {
        "date": date.isoformat(),
        "total_candidates": len(evaluations),
        "active_count": len(active),
        "candidates": [_attach_meta(ev) for ev in evaluations],
        "active": [_attach_meta(ev) for ev in active],
        "config": {
            "weekly_universe_size": WEEKLY_UNIVERSE_SIZE,
            "daily_candidate_count": DAILY_CANDIDATE_COUNT,
            "daily_max_trades": DAILY_MAX_TRADES,
            "max_actions_per_stock": MAX_ACTIONS_PER_STOCK_PER_DAY,
            "eps_mu": EPS_MU,
            "eps_p": EPS_P,
            "p_loss_max": P_LOSS_MAX,
            "mu_min": MU_MIN,
            "lambda_risk": LAMBDA_RISK,
        },
        "llm_go_nogo": llm_go_nogo or {},
    }
    out_path = REPORTS_DIR / f"daily_plan_selection_{date.isoformat()}.json"
    out_path.write_text(json.dumps(payload, indent=2))
    summary = REPORTS_DIR / f"daily_plan_selection_{date.isoformat()}.md"
    summary.write_text(
        "\n".join(
            [
                f"# Daily plan selection — {date.isoformat()}",
                f"Total candidates: {len(evaluations)}",
                f"Active symbols: {len(active)} (cap {MAX_SYMBOLS_TODAY})",
                "",
            ]
        )
        + "\n".join(
            f"- {ev.plan.symbol}: score={ev.score:.4f}, mu={ev.mu:.4f}, sigma={ev.sigma:.4f}, p_loss_upper={ev.p_loss_upper:.2f}"
            for ev in active
        ),
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nightly daily plan builder")
    parser.add_argument("--date", dest="target_date", type=str, default=None,
                        help="Trading date (YYYY-MM-DD). Defaults to tomorrow (America/New_York).")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.target_date:
        target_date = dt.date.fromisoformat(args.target_date)
    else:
        target_date = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)).date()
    conn = psycopg2.connect(database_url())
    try:
        weekly_universe = fetch_weekly_universe(conn)
        positions = fetch_positions(conn)
        signal_meta = load_signal_meta(conn)
        forecasts = load_forecast_predictions(target_date)
        if not forecasts:
            logger.warning("No nightly forecasts found for %s; falling back to historical metrics only.", target_date)
        candidates, delta_map = select_candidates(conn, weekly_universe, target_date, positions, signal_meta, forecasts)
        candidate_plans: Dict[str, DailyPlan] = {}
        for sym in candidates:
            info = delta_map.get(sym)
            if not info:
                continue
            plan = build_plan(sym, positions.get(sym, 0.0), info)
            if plan is None:
                logger.info("LLM vetoed daily plan for %s", sym)
                continue
            candidate_plans[sym] = plan
        evaluations = evaluate_candidates(candidate_plans, delta_map)
        active = select_active_plans(evaluations)
        go_nogo_map = run_daily_go_nogo_checks(conn, active, target_date, positions, delta_map)
        if go_nogo_map:
            filtered: List[PlanEvaluation] = []
            for ev in active:
                verdict = go_nogo_map.get(ev.plan.symbol, {}).get("decision")
                if verdict and verdict.get("go") is False:
                    logger.info("LLM go/no-go vetoed %s (%s)", ev.plan.symbol, verdict.get("reasons"))
                    continue
                filtered.append(ev)
            active = filtered
        write_report(target_date, evaluations, active, delta_map, llm_go_nogo=go_nogo_map)
        print(f"Selected {len(active)} active symbols for {target_date}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
