#!/usr/bin/env python3
"""Rank symbols by 5d Kronos signal strength and write the top N into signal_universe_100."""

from __future__ import annotations

import argparse
import datetime as dt
import json
import math
import os
import sys
import statistics
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
DEFAULT_REPORTS_DIR = ROOT_DIR / "reports"

import pandas as pd
import psycopg2
import psycopg2.extras

from libs.llm.reviews import LLMReviewStore
from libs.llm.run_and_log import LLMRunner
from libs.llm.schemas import WEEKLY_DEEP_DIVE_SCHEMA
from libs.llm.settings import is_llm_enabled
from libs.planner.llm_filters import get_plan_llm
from libs.universe.signals import SignalMetrics, compute_signal_metrics
from tools.universe.monte_carlo_sim import (
    DEFAULT_DAYS,
    DEFAULT_LOOKBACK,
    DEFAULT_MIN_HISTORY,
    DEFAULT_SEED,
    DEFAULT_SIMS,
    MonteCarloConfig,
    run_full_pipeline,
)


def _env_flag(name: str, default: str = "0") -> bool:
    raw = os.getenv(name, default)
    return raw.strip().lower() not in {"0", "false", "no", "off"}


DEFAULT_MIN_PRICE = float(os.getenv("SIGNAL_UNIVERSE_MIN_PRICE", "3"))
DEFAULT_MIN_DOLLAR_VOL = float(os.getenv("SIGNAL_UNIVERSE_MIN_DOLLAR_VOL", str(2_000_000)))
DEFAULT_LIMIT = int(os.getenv("SIGNAL_UNIVERSE_SIZE", "100"))
FORECAST_HORIZON = os.getenv("SIGNAL_UNIVERSE_FORECAST_HORIZON", "5d")
LLM_REVIEW_CANDIDATES = int(os.getenv("SIGNAL_UNIVERSE_LLM_REVIEW", "200"))
WEEKLY_DEEP_DIVE_LIMIT = int(os.getenv("WEEKLY_DEEP_DIVE_LIMIT", "100"))
WEEKLY_DEEP_DIVE_HEADLINE_HOURS = int(os.getenv("WEEKLY_DEEP_DIVE_HEADLINE_HOURS", "48"))
WEEKLY_DEEP_DIVE_HEADLINE_LIMIT = int(os.getenv("WEEKLY_DEEP_DIVE_HEADLINE_LIMIT", "6"))
MONTE_ENABLED_DEFAULT = _env_flag("ENABLE_MONTE_CARLO", "0")
MONTE_TOP_N = int(os.getenv("MONTE_CARLO_TOP_N", "10"))
PREDICTABILITY_LOOKBACK_DAYS = int(os.getenv("PREDICTABILITY_LOOKBACK_DAYS", "120"))
PREDICTABILITY_MIN_PAIRS = int(os.getenv("PREDICTABILITY_MIN_PAIRS", "60"))
PREDICTABILITY_LAST_BAR_LAG_DAYS = int(os.getenv("PREDICTABILITY_LAST_BAR_LAG_DAYS", "3"))
NBEATS_MODEL_PREFIX = os.getenv("PREDICTABILITY_NBEATS_PREFIX", "kronos-nbeats")
NBEATS_HORIZON_TAG = os.getenv("PREDICTABILITY_NBEATS_HORIZON", "5d")
PREDICTABILITY_EPS = float(os.getenv("PREDICTABILITY_EPS", "1e-6"))
PREDICTABILITY_STRATEGY = "nbeats_only_v1"

BASE_UNIVERSE_QUERY = """
SELECT
  s.ticker AS symbol,
  COALESCE(l.avg_dollar_vol_60d, 0) AS avg_dollar_vol_60d,
  COALESCE((s.meta->>'last_price')::numeric, 0) AS last_price,
  s.venue,
  COALESCE(p.sector, 'UNKNOWN') AS sector
FROM symbols s
LEFT JOIN mv_liquidity_60d l ON l.symbol = s.ticker
LEFT JOIN dim_company_profile p ON p.symbol = s.ticker
WHERE s.class = 'equity'
  AND COALESCE((s.meta->>'is_otc')::boolean, false) = false
  AND COALESCE((s.meta->>'is_active')::boolean, true) = true
  AND COALESCE((s.meta->>'last_price')::numeric, 0) >= %s
  AND COALESCE(l.avg_dollar_vol_60d, 0) >= %s
ORDER BY l.avg_dollar_vol_60d DESC
LIMIT 5000;
"""


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


def fetch_base_universe(conn, min_price: float, min_dollar_vol: float) -> pd.DataFrame:
    return pd.read_sql(BASE_UNIVERSE_QUERY, conn, params=(min_price, min_dollar_vol))


def fetch_upcoming_events(conn, symbol: str) -> str:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT kind, start_ts::date AS dt
        FROM event_windows
        WHERE ticker=%s AND start_ts BETWEEN NOW() AND NOW() + INTERVAL '10 days'
        ORDER BY start_ts ASC
        LIMIT 3
        """,
        (symbol,),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return "none"
    return "; ".join(f"{r[0]} {r[1]}" for r in rows)


def fetch_recent_headlines(conn, symbol: str, hours: int, limit: int) -> List[str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT COALESCE(title, text)
        FROM fmp_news
        WHERE symbol=%s
          AND ts >= NOW() - make_interval(hours => %s::int)
        ORDER BY ts DESC
        LIMIT %s
        """,
        (symbol, hours, limit),
    )
    rows = [row[0] for row in cur.fetchall() if row and row[0]]
    cur.close()
    return rows


def fetch_social_activity(conn, symbol: str, limit: int = 3) -> str:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT window_minutes, msg_rate, senti_mean, senti_std
        FROM social_features
        WHERE ticker=%s
        ORDER BY ts DESC, window_minutes ASC
        LIMIT %s
        """,
        (symbol, limit),
    )
    rows = cur.fetchall()
    cur.close()
    if not rows:
        return "no social data"
    parts = []
    for window, rate, senti_mean, senti_std in rows:
        parts.append(
            f"{window}m rate {float(rate or 0.0):.2f}/m sentiment {float(senti_mean or 0.0):+.2f}±{float(senti_std or 0.0):.2f}"
        )
    return "; ".join(parts)


def build_historical_metrics(entry: Dict[str, Any]) -> str:
    fields = {
        "avg_vol_60d": entry.get("avg_dollar_vol_60d"),
        "last_price": entry.get("last_price"),
        "mean_return": entry.get("mean_return"),
        "spy_mean": entry.get("spy_mean"),
    }
    monte = entry.get("monte_carlo") or {}
    if monte:
        fields.update(
            {
                "mc_best_score": monte.get("best_score"),
                "mc_prob_positive": monte.get("prob_positive"),
                "mc_rank": monte.get("rank"),
            }
        )
    parts = []
    for key, value in fields.items():
        if value is None:
            continue
        try:
            formatted = f"{float(value):.4f}"
        except Exception:
            formatted = str(value)
        parts.append(f"{key}={formatted}")
    return ", ".join(parts) if parts else "n/a"


def _recent_catalysts(headlines: Iterable[str], upcoming: str) -> str:
    catalyst_parts: List[str] = []
    if upcoming and upcoming != "none":
        catalyst_parts.append(f"Upcoming: {upcoming}")
    for item in list(headlines)[:3]:
        catalyst_parts.append(f"News: {item}")
    return " ; ".join(catalyst_parts) if catalyst_parts else "none"


def rank_signals(
    conn,
    rows: pd.DataFrame,
    horizon: str,
    limit: int,
) -> List[Dict[str, Any]]:
    results: List[Dict[str, Any]] = []
    cur = conn.cursor()
    for _, row in rows.iterrows():
        metrics: Optional[SignalMetrics] = compute_signal_metrics(conn, row.symbol, horizon=horizon)
        if metrics:
            entry = {
                "symbol": row.symbol,
                "signal_strength": metrics.signal_strength,
                "avg_dollar_vol_60d": float(row.avg_dollar_vol_60d or 0.0),
                "last_price": float(row.last_price or 0.0),
                "model": metrics.horizon,
                "forecast_ts": metrics.forecast_ts,
                "mean_return": metrics.mean_return,
                "spy_mean": metrics.spy_mean_return,
                "sector": row.sector,
            }
        else:
            # Fallback: 5-day average realized return from daily_returns.y
            cur.execute(
                """
                WITH last5 AS (
                  SELECT y, ROW_NUMBER() OVER (ORDER BY ds DESC) AS rn
                  FROM daily_returns
                  WHERE symbol=%s
                )
                SELECT AVG(y) FILTER (WHERE rn<=5) AS mean_return_5d,
                       COUNT(*) FILTER (WHERE rn<=5) AS obs_5d
                FROM last5
                """,
                (row.symbol,),
            )
            mr, obs = cur.fetchone()
            if not obs or obs < 5 or mr is None:
                continue
            entry = {
                "symbol": row.symbol,
                "signal_strength": float(mr),
                "avg_dollar_vol_60d": float(row.avg_dollar_vol_60d or 0.0),
                "last_price": float(row.last_price or 0.0),
                "model": "fallback-5d",
                "forecast_ts": None,
                "mean_return": float(mr),
                "spy_mean": 0.0,
                "sector": row.sector,
            }
        results.append(entry)
    results.sort(key=lambda item: abs(item["signal_strength"]), reverse=True)
    return results[:limit]


def apply_llm_filter(conn, ranked: List[Dict[str, Any]], limit: int) -> List[Dict[str, Any]]:
    if not ranked:
        return []
    llm = get_plan_llm()
    enriched: List[Dict[str, Any]] = []
    review_pool = ranked[:LLM_REVIEW_CANDIDATES]
    for entry in review_pool:
        info = {
            "symbol": entry["symbol"],
            "sector": entry.get("sector", "UNKNOWN"),
            "price": entry.get("last_price", 0.0),
            "avg_dollar_vol_60d": entry.get("avg_dollar_vol_60d", 0.0),
            "signal_strength": entry.get("signal_strength", 0.0),
            "upcoming_events": fetch_upcoming_events(conn, entry["symbol"]),
            "headlines": "n/a",
            "guidance": "n/a",
        }
        decision = llm.assess_universe(info)
        if decision.risk_flag == "avoid":
            continue
        adjusted_signal = entry["signal_strength"] - (decision.signal_penalty / 1e4)
        entry = {**entry,
                 "signal_strength": adjusted_signal,
                 "llm": {
                     "risk_flag": decision.risk_flag,
                     "sentiment": decision.sentiment,
                     "signal_penalty": decision.signal_penalty,
                     "comment": decision.comment,
                 }}
        enriched.append(entry)
    # Include remaining ranked names (without LLM review) if slots remain
    if len(enriched) < len(ranked):
        for entry in ranked[LLM_REVIEW_CANDIDATES:]:
            entry = {
                **entry,
                "llm": {"risk_flag": "normal", "sentiment": 0.0, "signal_penalty": 0.0, "comment": ""},
            }
            enriched.append(entry)
    enriched.sort(key=lambda item: abs(item["signal_strength"]), reverse=True)
    return enriched[:limit]


def run_weekly_deep_dives(
    conn,
    rows: List[Dict[str, Any]],
    as_of: dt.date,
    *,
    runner: Optional[LLMRunner] = None,
    store: Optional[LLMReviewStore] = None,
) -> None:
    if not rows:
        return
    runner = runner or LLMRunner()
    store = store or LLMReviewStore()
    review_payloads: List[Dict[str, Any]] = []
    review_dir = Path("reports")
    review_dir.mkdir(parents=True, exist_ok=True)
    for entry in rows[: min(WEEKLY_DEEP_DIVE_LIMIT, len(rows))]:
        symbol = entry["symbol"]
        headlines = fetch_recent_headlines(conn, symbol, WEEKLY_DEEP_DIVE_HEADLINE_HOURS, WEEKLY_DEEP_DIVE_HEADLINE_LIMIT)
        upcoming = fetch_upcoming_events(conn, symbol)
        context = {
            "symbol": symbol,
            "as_of": as_of.isoformat(),
            "price": f"{entry.get('last_price', 0.0):.2f}",
            "signal_strength": f"{entry.get('signal_strength', 0.0):+.4f}",
            "upcoming_events": upcoming,
            "headlines": " ; ".join(headlines) if headlines else "none",
            "guidance": entry.get("llm", {}).get("comment", "none"),
            "historical_metrics": build_historical_metrics(entry),
            "social_activity": fetch_social_activity(conn, symbol),
            "recent_catalysts": _recent_catalysts(headlines, upcoming),
        }
        try:
            result = runner.cached_call(
                "weekly_deep_dive",
                context,
                schema=WEEKLY_DEEP_DIVE_SCHEMA,
                max_tokens=900,
            )
        except Exception as exc:
            entry.setdefault("llm", {})
            entry["llm"]["deep_dive"] = {"error": str(exc)}
            continue
        decision = result.get("json")
        entry.setdefault("llm", {})
        agreement = 0
        try:
            agreement = int(decision.get("agreement", 0)) if decision else 0
        except Exception:
            agreement = 0
        entry["llm"]["deep_dive"] = {
            "narrative": result.get("text"),
            "decision": decision,
            "cached": result.get("cached", False),
            "prompt_version": result.get("prompt_version"),
        }
        entry["llm"]["sentiment"] = agreement
        extra = {"narrative": result.get("text"), "cached": result.get("cached", False)}
        try:
            store.upsert_review(
                scope="weekly_deep_dive",
                symbol=symbol,
                as_of=as_of,
                prompt_key=result.get("prompt_key", "weekly_deep_dive"),
                prompt_version=result.get("prompt_version", "1.0.0"),
                prompt_hash=result.get("prompt_hash", ""),
                output_json=decision,
                output_text=result.get("text"),
                input_payload=context,
                extra=extra,
                cached=bool(result.get("cached", False)),
            )
        except Exception:
            pass
        review_payloads.append(
            {
                "symbol": symbol,
                "context": context,
                "result": {
                    "decision": decision,
                    "text": result.get("text"),
                    "cached": result.get("cached", False),
                    "prompt_version": result.get("prompt_version"),
                    "prompt_key": result.get("prompt_key"),
                },
            }
        )
    if review_payloads:
        out_path = review_dir / f"llm_weekly_deep_dive_{as_of.isoformat()}.json"
        out_path.write_text(json.dumps({"as_of": as_of.isoformat(), "results": review_payloads}, indent=2))


def upsert_signal_universe(
    conn,
    as_of: dt.date,
    rows: List[Dict[str, Any]],
) -> None:
    if not rows:
        print("No signal candidates found; skipping signal_universe_100 update")
        return
    with conn.cursor() as cur:
        cur.execute("DELETE FROM signal_universe_100 WHERE as_of = %s", (as_of,))
        payload: List[Tuple[Any, ...]] = []
        for rank, row in enumerate(rows, start=1):
            meta = {
                "model": row.get("model"),
                "forecast_ts": row.get("forecast_ts"),
                "avg_dollar_vol_60d": row.get("avg_dollar_vol_60d"),
                "last_price": row.get("last_price"),
                "mean_return": row.get("mean_return"),
                "spy_mean": row.get("spy_mean"),
                "llm": row.get("llm"),
                "monte_carlo": row.get("monte_carlo"),
            }
            predictability_meta = row.get("predictability_meta")
            if predictability_meta:
                meta["predictability"] = predictability_meta
            payload.append(
                (
                    as_of,
                    row["symbol"],
                    float(row["signal_strength"] or 0.0),
                    rank,
                    json.dumps(meta, default=str),
                )
            )
        cur.executemany(
            """
            INSERT INTO signal_universe_100 (as_of, symbol, signal_strength, rank, meta)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (as_of, symbol) DO UPDATE
            SET signal_strength = EXCLUDED.signal_strength,
                rank = EXCLUDED.rank,
                meta = EXCLUDED.meta
            """,
            payload,
        )
    conn.commit()
    print(f"Wrote {len(rows)} rows to signal_universe_100 for {as_of}")


@dataclass
class ForecastRealizationPair:
    origin_date: dt.date
    target_date: dt.date
    horizon_days: int
    forecast_p50: float
    realized_return: float


@dataclass
class PredictabilityMetrics:
    mae: float
    resid_std: float
    directional_accuracy: float
    n_pairs: int
    score: float


def _load_return_map(
    conn,
    symbol: str,
    start_date: dt.date,
    end_date: dt.date,
) -> Dict[dt.date, float]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ds, y
            FROM daily_returns
            WHERE symbol=%s
              AND ds BETWEEN %s AND %s
            """,
            (symbol, start_date, end_date),
        )
        return {row[0]: float(row[1]) for row in cur.fetchall()}


def load_nbeats_pairs_for_symbol(
    conn,
    symbol: str,
    as_of: dt.date,
    eval_days: int,
    horizon_days: int,
    horizon_tag: str,
    model_prefix: str,
) -> List[ForecastRealizationPair]:
    start_date = as_of - dt.timedelta(days=eval_days)
    end_date = as_of - dt.timedelta(days=1)
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts::date AS origin_date, mean
            FROM forecasts
            WHERE ticker=%s
              AND horizon=%s
              AND model ILIKE %s
              AND ts::date BETWEEN %s AND %s
            ORDER BY origin_date ASC
            """,
            (symbol, horizon_tag, f"{model_prefix}%", start_date, end_date),
        )
        rows = cur.fetchall()
    if not rows:
        return []
    ret_map = _load_return_map(
        conn,
        symbol,
        start_date - dt.timedelta(days=2),
        as_of,
    )
    pairs: List[ForecastRealizationPair] = []
    for origin_date, mean_value in rows:
        if not isinstance(origin_date, dt.date):
            continue
        target_date = origin_date + dt.timedelta(days=horizon_days)
        if target_date > as_of:
            continue
        realized = ret_map.get(target_date)
        if realized is None:
            continue
        pairs.append(
            ForecastRealizationPair(
                origin_date=origin_date,
                target_date=target_date,
                horizon_days=horizon_days,
                forecast_p50=float(mean_value or 0.0),
                realized_return=float(realized),
            )
        )
    return pairs


def compute_nbeats_metrics(
    pairs: Sequence[ForecastRealizationPair],
    *,
    min_pairs: int,
) -> Optional[PredictabilityMetrics]:
    if len(pairs) < min_pairs:
        return None
    residuals = [pair.realized_return - pair.forecast_p50 for pair in pairs]
    mae = sum(abs(res) for res in residuals) / len(residuals)
    if len(residuals) > 1:
        mean_resid = sum(residuals) / len(residuals)
        resid_std = math.sqrt(sum((res - mean_resid) ** 2 for res in residuals) / (len(residuals) - 1))
    else:
        resid_std = 0.0

    def _sign(value: float, eps: float = 1e-8) -> int:
        if abs(value) <= eps:
            return 0
        return 1 if value > 0 else -1

    directional_hits = sum(
        1
        for pair in pairs
        if _sign(pair.forecast_p50) == _sign(pair.realized_return)
    )
    directional_accuracy = directional_hits / len(pairs)
    score = 1.0 / (PREDICTABILITY_EPS + mae)
    return PredictabilityMetrics(
        mae=mae,
        resid_std=resid_std,
        directional_accuracy=directional_accuracy,
        n_pairs=len(pairs),
        score=score,
    )


def fetch_predictability_candidates(
    conn,
    as_of: dt.date,
    min_eval_days: int,
) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT u.symbol
            FROM alpaca_universe u
            JOIN coverage_180_final cov USING (symbol)
            WHERE cov.last_bar >= %s - INTERVAL '3 days'
              AND cov.have_days >= %s
            ORDER BY u.symbol ASC
            """,
            (as_of, min_eval_days),
        )
        return [row[0] for row in cur.fetchall()]


def _update_universe_file(symbols: Sequence[str], dest: Path = Path("services/ingest/universe_symbols.txt")) -> None:
    ordered = [sym for sym in symbols if sym]
    if "SPY" not in ordered:
        ordered.append("SPY")
    deduped: Dict[str, None] = {}
    for sym in ordered:
        cleaned = sym.strip().upper()
        if cleaned and cleaned not in deduped:
            deduped[cleaned] = None
    dest.parent.mkdir(parents=True, exist_ok=True)
    with dest.open("w", encoding="utf-8") as handle:
        for sym in deduped.keys():
            handle.write(f"{sym}\n")
    print(f"Updated {dest} with {len(deduped)} symbols")


def _summary(values: Sequence[float]) -> Dict[str, Optional[float]]:
    if not values:
        return {"min": None, "max": None, "mean": None}
    return {
        "min": min(values),
        "max": max(values),
        "mean": float(sum(values) / len(values)),
    }


def write_predictability_snapshot(
    as_of: dt.date,
    ranked: Sequence[Tuple[int, str, PredictabilityMetrics]],
    *,
    snapshot_path: Path,
    eval_days: int,
    min_pairs: int,
    horizon_days: int,
    universe_size: int,
    eligible_count: int,
) -> None:
    snapshot_path.parent.mkdir(parents=True, exist_ok=True)
    scores = [metrics.score for _, _, metrics in ranked]
    maes = [metrics.mae for _, _, metrics in ranked]
    dirs = [metrics.directional_accuracy for _, _, metrics in ranked]
    payload = {
        "as_of": as_of.isoformat(),
        "generated_at": dt.datetime.utcnow().isoformat() + "Z",
        "top_k": len(ranked),
        "eval_days": eval_days,
        "min_pairs": min_pairs,
        "horizon_days": horizon_days,
        "model_prefix": NBEATS_MODEL_PREFIX,
        "universe_size": universe_size,
        "eligible_symbols": eligible_count,
        "summary": {
            "score": _summary(scores),
            "mae": _summary(maes),
            "dir_acc": _summary(dirs),
        },
        "symbols": [
            {
                "symbol": symbol,
                "rank": rank,
                "score": metrics.score,
                "mae": metrics.mae,
                "resid_std": metrics.resid_std,
                "dir_acc": metrics.directional_accuracy,
                "n_pairs": metrics.n_pairs,
            }
            for rank, symbol, metrics in ranked
        ],
    }
    snapshot_path.write_text(json.dumps(payload, indent=2))
    print(f"Wrote predictability snapshot to {snapshot_path}")


def build_predictability_universe(
    conn,
    *,
    as_of: dt.date,
    top_k: int,
    eval_days: int = PREDICTABILITY_LOOKBACK_DAYS,
    min_pairs: int = PREDICTABILITY_MIN_PAIRS,
    horizon_days: int = 5,
    snapshot_dir: Optional[Path] = None,
) -> int:
    min_eval_days = max(60, eval_days)
    candidates = fetch_predictability_candidates(conn, as_of, min_eval_days)
    if not candidates:
        print("No predictability candidates found.")
        return 0
    horizon_tag = f"{horizon_days}d"
    eligible: List[Tuple[str, PredictabilityMetrics]] = []
    for symbol in candidates:
        pairs = load_nbeats_pairs_for_symbol(
            conn,
            symbol,
            as_of=as_of,
            eval_days=eval_days,
            horizon_days=horizon_days,
            horizon_tag=horizon_tag,
            model_prefix=NBEATS_MODEL_PREFIX,
        )
        metrics = compute_nbeats_metrics(pairs, min_pairs=min_pairs)
        if metrics:
            eligible.append((symbol, metrics))
    if not eligible:
        print(
            f"Predictability: no symbols met the pairing criteria (eval_days={eval_days}, min_pairs={min_pairs})."
        )
        return 0
    eligible.sort(key=lambda item: item[1].score, reverse=True)
    ranked = [(idx + 1, symbol, metrics) for idx, (symbol, metrics) in enumerate(eligible[:top_k])]

    rows_for_upsert: List[Dict[str, Any]] = []
    for rank, symbol, metrics in ranked:
        per_model = {
            "nbeats": {
                "mae": metrics.mae,
                "resid_std": metrics.resid_std,
                "dir_acc": metrics.directional_accuracy,
                "n_pairs": metrics.n_pairs,
                "eval_days": eval_days,
                "horizon_days": horizon_days,
                "model_prefix": NBEATS_MODEL_PREFIX,
            }
        }
        meta = {
            "per_model": per_model,
            "ensemble": {
                "predictability_score": metrics.score,
                "strategy": PREDICTABILITY_STRATEGY,
            },
        }
        rows_for_upsert.append(
            {
                "symbol": symbol,
                "signal_strength": metrics.score,
                "model": NBEATS_MODEL_PREFIX,
                "forecast_ts": None,
                "avg_dollar_vol_60d": None,
                "last_price": None,
                "mean_return": None,
                "spy_mean": None,
                "llm": {},
                "monte_carlo": None,
                "predictability_meta": meta,
            }
        )
    upsert_signal_universe(conn, as_of=as_of, rows=rows_for_upsert)
    _update_universe_file([symbol for _, symbol, _ in ranked])
    if snapshot_dir:
        snapshot_dir.mkdir(parents=True, exist_ok=True)
        snapshot_path = snapshot_dir / f"predictability_snapshot_{as_of.isoformat()}.json"
        write_predictability_snapshot(
            as_of,
            ranked,
            snapshot_path=snapshot_path,
            eval_days=eval_days,
            min_pairs=min_pairs,
            horizon_days=horizon_days,
            universe_size=len(candidates),
            eligible_count=len(eligible),
        )
    return len(ranked)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build signal-ranked trading universe")
    parser.add_argument(
        "--as-of",
        "--date",
        dest="as_of",
        type=str,
        default=None,
        help="Date for the snapshot (YYYY-MM-DD). Defaults to today.",
    )
    parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT,
                        help="How many symbols to keep (default 100).")
    parser.add_argument("--min-price", type=float, default=DEFAULT_MIN_PRICE,
                        help=f"Minimum price filter (default {DEFAULT_MIN_PRICE}).")
    parser.add_argument("--min-dollar-vol", type=float, default=DEFAULT_MIN_DOLLAR_VOL,
                        help=f"Minimum 60d avg dollar volume (default {DEFAULT_MIN_DOLLAR_VOL}).")
    parser.add_argument(
        "--monte-carlo",
        dest="enable_monte_carlo",
        action="store_true",
        help="Enable Monte Carlo simulations for ranked symbols.",
    )
    parser.add_argument(
        "--no-monte-carlo",
        dest="enable_monte_carlo",
        action="store_false",
        help="Disable Monte Carlo stage.",
    )
    parser.set_defaults(enable_monte_carlo=MONTE_ENABLED_DEFAULT)
    parser.add_argument("--mc-sims", type=int, default=DEFAULT_SIMS, help="Number of simulation paths per symbol.")
    parser.add_argument("--mc-days", type=int, default=DEFAULT_DAYS, help="Simulation days per path.")
    parser.add_argument("--mc-lookback", type=int, default=DEFAULT_LOOKBACK, help="Historical days used for mu/sigma.")
    parser.add_argument("--mc-min-history", type=int, default=DEFAULT_MIN_HISTORY, help="Minimum history required.")
    parser.add_argument("--mc-seed", type=int, default=int(DEFAULT_SEED) if DEFAULT_SEED else None,
                        help="Optional RNG seed for reproducibility.")
    parser.add_argument(
        "--skip-llm",
        action="store_true",
        default=not is_llm_enabled(),
        help="Skip LLM risk filtering/deep dives (use raw Kronos rankings only).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    as_of = dt.date.fromisoformat(args.as_of) if args.as_of else dt.date.today()
    conn = psycopg2.connect(database_url())
    try:
        base_universe = fetch_base_universe(conn, args.min_price, args.min_dollar_vol)
        ranked_raw = rank_signals(conn, base_universe, horizon=FORECAST_HORIZON, limit=max(args.limit, LLM_REVIEW_CANDIDATES))
        if args.skip_llm:
            print("[build_signal_universe] LLM stage disabled; using raw Kronos rankings.")
            ranked = ranked_raw[: args.limit]
        else:
            ranked = apply_llm_filter(conn, ranked_raw, limit=args.limit)
            run_weekly_deep_dives(conn, ranked, as_of)
        monte_results: Dict[str, Any] = {}
        if args.enable_monte_carlo and ranked:
            mc_config = MonteCarloConfig(
                num_simulations=args.mc_sims,
                days=args.mc_days,
                lookback_days=args.mc_lookback,
                min_history=args.mc_min_history,
                seed=args.mc_seed,
            )
            monte_results = run_full_pipeline(conn, as_of, ranked, config=mc_config, top_n=MONTE_TOP_N)
            ordered = sorted(monte_results.values(), key=lambda res: res.best_score, reverse=True)
            rank_lookup = {res.symbol: idx for idx, res in enumerate(ordered, start=1)}
            for entry in ranked:
                res = monte_results.get(entry["symbol"])
                if not res:
                    continue
                entry["monte_carlo"] = {
                    "mean_profit": res.mean_profit,
                    "profit_std_dev": res.profit_std_dev,
                    "best_score": res.best_score,
                    "mean_return": res.mean_return,
                    "return_std_dev": res.return_std_dev,
                    "prob_positive": res.prob_positive,
                    "sim_count": res.sim_count,
                    "sim_days": res.sim_days,
                    "rank": rank_lookup.get(entry["symbol"]),
                }
        elif not args.enable_monte_carlo:
            print(
                "[build_signal_universe] Monte Carlo disabled; skipping simulation stage (manual runs available via tools/universe/monte_carlo_sim.py)."
            )
        upsert_signal_universe(conn, as_of=as_of, rows=ranked)
        if ranked:
            symbols = [row["symbol"] for row in ranked]
            if "SPY" not in symbols:
                symbols.append("SPY")
            pd.DataFrame(symbols, columns=["symbol"]).to_csv(
                "services/ingest/universe_symbols.txt", index=False, header=False
            )
            print("Updated services/ingest/universe_symbols.txt for ingest warm-start")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
