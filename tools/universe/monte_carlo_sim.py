"""Monte Carlo helpers for ranking the signal universe."""

from __future__ import annotations

import json
import math
import os
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import psycopg2
import psycopg2.extras

try:  # Prometheus is optional when running scripts locally
    from prometheus_client import CollectorRegistry, Gauge, push_to_gateway  # type: ignore
except Exception:  # pragma: no cover - prometheus_client may be absent in some environments
    CollectorRegistry = None  # type: ignore
    Gauge = None  # type: ignore
    push_to_gateway = None  # type: ignore


DEFAULT_SIMS = int(os.getenv("MONTE_CARLO_SIMS", "1000"))
DEFAULT_DAYS = int(os.getenv("MONTE_CARLO_DAYS", "5"))
DEFAULT_LOOKBACK = int(os.getenv("MONTE_CARLO_LOOKBACK_DAYS", "252"))
DEFAULT_MIN_HISTORY = int(os.getenv("MONTE_CARLO_MIN_HISTORY", "120"))
DEFAULT_SEED = os.getenv("MONTE_CARLO_SEED")
PROM_GATEWAY = os.getenv("PROM_PUSHGATEWAY")
Z_VALUE = 1.959963984540054
DEFAULT_EPS_MU = float(os.getenv("MONTE_CARLO_EPS_MU", "0.005"))
DEFAULT_EPS_P = float(os.getenv("MONTE_CARLO_EPS_P", "0.05"))
DEFAULT_BATCH_SIZE = int(os.getenv("MONTE_CARLO_BATCH_SIZE", "512"))
DEFAULT_SIGNAL_UNIVERSE_SIZE = int(os.getenv("SIGNAL_UNIVERSE_SIZE", "100"))


@dataclass
class MonteCarloConfig:
    num_simulations: int = DEFAULT_SIMS
    days: int = DEFAULT_DAYS
    lookback_days: int = DEFAULT_LOOKBACK
    min_history: int = DEFAULT_MIN_HISTORY
    seed: Optional[int] = int(DEFAULT_SEED) if DEFAULT_SEED else None
    eps_mu: float = DEFAULT_EPS_MU
    eps_p: float = DEFAULT_EPS_P
    batch_size: int = DEFAULT_BATCH_SIZE

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MonteCarloResult:
    symbol: str
    start_price: float
    mean_profit: float
    profit_std_dev: float
    best_score: float
    mean_return: float
    return_std_dev: float
    prob_positive: float
    mean_final_price: float
    sim_count: int
    sim_days: int
    history_len: int
    llm_meta: Dict[str, Any]
    certainty_score: float
    mu_margin: float
    prob_margin: float
    certified: bool
    final_score: float

    def to_dict(self) -> Dict[str, Any]:
        payload = asdict(self)
        payload["llm_meta"] = self.llm_meta
        return payload


def _fetch_log_returns(conn, symbol: str, lookback_days: int) -> List[float]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT y
            FROM daily_returns
            WHERE symbol=%s
            ORDER BY ds DESC
            LIMIT %s
            """,
            (symbol, lookback_days),
        )
        rows = cur.fetchall()
    if not rows:
        return []
    return [float(row[0]) for row in reversed(rows) if row[0] is not None]


def _resolve_last_price(entry: Dict[str, Any], conn, symbol: str) -> Optional[float]:
    last_price = entry.get("last_price")
    if last_price:
        return float(last_price)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT COALESCE((meta->>'last_price')::numeric, 0) FROM symbols WHERE ticker=%s",
            (symbol,),
        )
        row = cur.fetchone()
    if not row or row[0] in (None, 0):
        return None
    return float(row[0])


def _simulate_symbol(
    symbol: str,
    start_price: float,
    log_returns: Sequence[float],
    config: MonteCarloConfig,
    rng: np.random.Generator,
) -> Optional[MonteCarloResult]:
    if start_price <= 0:
        return None
    arr = np.asarray(log_returns, dtype=np.float64)
    history_len = int(arr.size)
    if arr.size < config.min_history:
        return None
    mu = float(arr.mean())
    sigma = float(arr.std(ddof=1)) if arr.size > 1 else 0.0
    sigma = max(sigma, 1e-5)
    days = config.days
    all_profits: List[float] = []
    n_paths = 0
    margin_mu = float("inf")
    margin_p = float("inf")
    mean_profit = 0.0
    profit_std = 0.0
    prob_positive = 0.0
    eps_mu = max(config.eps_mu, 1e-9)
    eps_p = max(config.eps_p, 1e-9)
    required_mu_paths = math.ceil(((Z_VALUE * sigma) / eps_mu) ** 2) if sigma > 0 else config.batch_size
    required_prob_paths = math.ceil(((Z_VALUE * 0.5) / eps_p) ** 2)

    while True:
        batch = rng.normal(loc=mu, scale=sigma, size=(config.batch_size, days))
        cumulative = batch.sum(axis=1)
        final_prices = start_price * np.exp(cumulative)
        profits = final_prices - start_price
        all_profits.extend(profits.tolist())
        n_paths += profits.size
        arr = np.array(all_profits)
        mean_profit = float(arr.mean())
        profit_std = float(arr.std(ddof=1)) if n_paths > 1 else 0.0
        profit_std = max(profit_std, 1e-6)
        prob_positive = float(np.mean(arr > 0))
        margin_mu = Z_VALUE * (profit_std / math.sqrt(max(n_paths, 1))) if n_paths > 1 else float("inf")
        p_var = max(prob_positive * (1 - prob_positive), 1e-6)
        margin_p = Z_VALUE * math.sqrt(p_var / max(n_paths, 1)) if n_paths > 1 else float("inf")
        if 0 < prob_positive < 1:
            std_prob = math.sqrt(prob_positive * (1 - prob_positive))
            required_prob_paths = max(required_prob_paths, math.ceil(((Z_VALUE * std_prob) / eps_p) ** 2))
        required_paths = max(required_mu_paths, required_prob_paths)
        if n_paths >= required_paths and margin_mu <= config.eps_mu and margin_p <= config.eps_p:
            break

    if not all_profits:
        return None

    profits_arr = np.array(all_profits)
    returns = profits_arr / start_price
    mean_return = float(returns.mean())
    return_std = float(returns.std(ddof=1)) if returns.size > 1 else 0.0
    mean_final_price = start_price + mean_profit
    best_score = mean_profit / profit_std if profit_std else 0.0
    certainty_score = min(
        1.0,
        min(
            config.eps_mu / max(margin_mu, 1e-9),
            config.eps_p / max(margin_p, 1e-9),
        ),
    )
    certified = (margin_mu <= config.eps_mu) and (margin_p <= config.eps_p)
    final_score = float(certainty_score + abs(mean_profit))

    return MonteCarloResult(
        symbol=symbol,
        start_price=start_price,
        mean_profit=mean_profit,
        profit_std_dev=profit_std,
        best_score=best_score,
        mean_return=mean_return,
        return_std_dev=return_std,
        prob_positive=prob_positive,
        mean_final_price=mean_final_price,
        sim_count=n_paths,
        sim_days=days,
        history_len=history_len,
        llm_meta={},
        certainty_score=float(certainty_score),
        mu_margin=float(margin_mu if math.isfinite(margin_mu) else config.eps_mu),
        prob_margin=float(margin_p if math.isfinite(margin_p) else config.eps_p),
        certified=certified,
        final_score=final_score,
    )


def run_monte_carlo_batch(
    conn,
    ranked_entries: Sequence[Dict[str, Any]],
    config: Optional[MonteCarloConfig] = None,
) -> Dict[str, MonteCarloResult]:
    cfg = config or MonteCarloConfig()
    rng = np.random.default_rng(cfg.seed)
    results: Dict[str, MonteCarloResult] = {}
    total = len(ranked_entries)
    for idx, entry in enumerate(ranked_entries, start=1):
        symbol = entry["symbol"]
        start_price = _resolve_last_price(entry, conn, symbol)
        if not start_price:
            continue
        history = _fetch_log_returns(conn, symbol, cfg.lookback_days)
        result = _simulate_symbol(symbol, start_price, history, cfg, rng)
        if result is None:
            continue
        result.llm_meta = entry.get("llm") or {}
        results[symbol] = result
        print(
            f"[MonteCarlo] processed {idx}/{total}: {symbol} sims={result.sim_count} certainty={result.certainty_score:.3f}",
            flush=True,
        )
    return results


def _load_weekly_universe_entries(conn, as_of, limit: int) -> List[Dict[str, Any]]:
    """Fetch the Top 100 weekly universe snapshot for Monte Carlo inputs."""
    target_date = as_of
    if target_date is None:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(as_of) FROM signal_universe_100")
            row = cur.fetchone()
            target_date = row[0]
    if target_date is None:
        return []
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT symbol, rank, signal_strength, meta
            FROM signal_universe_100
            WHERE as_of = %s
            ORDER BY rank ASC
            LIMIT %s
            """,
            (target_date, limit),
        )
        rows = cur.fetchall()
    entries: List[Dict[str, Any]] = []
    for row in rows:
        meta = row.get("meta") or {}
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except json.JSONDecodeError:
                meta = {}
        entry = {
            "symbol": row["symbol"],
            "rank": row["rank"],
            "signal_strength": row.get("signal_strength"),
            "last_price": (meta or {}).get("last_price"),
            "avg_dollar_vol_60d": (meta or {}).get("avg_dollar_vol_60d"),
            "mean_return": (meta or {}).get("mean_return"),
            "spy_mean": (meta or {}).get("spy_mean"),
            "llm": (meta or {}).get("llm"),
        }
        entries.append(entry)
    return entries


def persist_results(
    conn,
    as_of,
    results: Dict[str, MonteCarloResult],
) -> List[MonteCarloResult]:
    ordered = sorted(
        results.values(),
        key=lambda r: (r.certainty_score, abs(r.mean_profit)),
        reverse=True,
    )
    payload: List[Tuple[Any, ...]] = []
    for idx, res in enumerate(ordered, start=1):
        meta = {
            "start_price": res.start_price,
            "prob_positive": res.prob_positive,
            "mean_final_price": res.mean_final_price,
            "history_len": res.history_len,
            "llm": res.llm_meta,
            "certainty_score": res.certainty_score,
            "mu_margin": res.mu_margin,
            "prob_margin": res.prob_margin,
            "certified": res.certified,
            "final_score": res.final_score,
            "borrow_cost": 1.0,
        }
        payload.append(
            (
                as_of,
                res.symbol,
                idx,
                res.mean_profit,
                res.profit_std_dev,
                res.best_score,
                res.mean_return,
                res.return_std_dev,
                res.sim_count,
                res.sim_days,
                (res.llm_meta or {}).get("risk_flag"),
                float((res.llm_meta or {}).get("sentiment", 0.0) or 0.0),
                json.dumps(meta),
            )
        )
    with conn.cursor() as cur:
        cur.execute("DELETE FROM universe_monte_carlo WHERE as_of=%s", (as_of,))
        if payload:
            psycopg2.extras.execute_batch(
                cur,
                """
                INSERT INTO universe_monte_carlo (
                    as_of, symbol, rank,
                    mean_profit, profit_std_dev, best_score,
                    mean_return, return_std_dev,
                    sim_count, sim_days,
                    llm_risk_flag, llm_sentiment, meta
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                payload,
                page_size=200,
            )
    conn.commit()
    return ordered


def write_reports(
    as_of,
    ordered_results: Sequence[MonteCarloResult],
    config: MonteCarloConfig,
    out_dir: Path = Path("reports"),
    top_n: int = 10,
) -> Tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    as_of_str = as_of.isoformat()
    universe_payload = {
        "as_of": as_of_str,
        "config": config.to_dict(),
        "count": len(ordered_results),
        "results": [
            dict(res.to_dict(), rank=idx)
            for idx, res in enumerate(ordered_results, start=1)
        ],
    }
    universe_path = out_dir / f"monte_carlo_universe_{as_of_str}.json"
    universe_path.write_text(json.dumps(universe_payload, indent=2))

    top_results = ordered_results[:top_n]
    top_payload = {
        "as_of": as_of_str,
        "top_n": top_n,
        "config": config.to_dict(),
        "results": [
            dict(res.to_dict(), rank=idx)
            for idx, res in enumerate(top_results, start=1)
        ],
    }
    top_path = out_dir / f"monte_carlo_top{top_n}_{as_of_str}.json"
    top_path.write_text(json.dumps(top_payload, indent=2))
    return universe_path, top_path


def record_metrics(duration: float, result_count: int, top_results: Sequence[MonteCarloResult]) -> None:
    if not PROM_GATEWAY or CollectorRegistry is None or Gauge is None or push_to_gateway is None:
        return
    registry = CollectorRegistry()
    duration_gauge = Gauge("pulse_monte_carlo_duration_seconds", "Duration of the most recent universe Monte Carlo run", registry=registry)
    duration_gauge.set(duration)
    Gauge("pulse_monte_carlo_symbols_processed", "Number of symbols simulated in the latest run", registry=registry).set(result_count)
    best = top_results[0].best_score if top_results else 0.0
    Gauge("pulse_monte_carlo_top_best_score", "Best score across Monte Carlo universe", registry=registry).set(best)
    push_to_gateway(PROM_GATEWAY, job="pulse_universe_monte_carlo", registry=registry)


def run_full_pipeline(
    conn,
    as_of,
    ranked_entries: Optional[Sequence[Dict[str, Any]]] = None,
    *,
    config: Optional[MonteCarloConfig] = None,
    reports_dir: Path = Path("reports"),
    top_n: int = 10,
) -> Dict[str, MonteCarloResult]:
    cfg = config or MonteCarloConfig()
    weekly_entries = _load_weekly_universe_entries(conn, as_of, limit=max(top_n, DEFAULT_SIGNAL_UNIVERSE_SIZE))
    selected_entries: List[Dict[str, Any]]
    ranked_lookup = {entry["symbol"]: entry for entry in (ranked_entries or [])}
    if weekly_entries:
        selected_entries = []
        for base_entry in weekly_entries:
            merged = dict(base_entry)
            extra = ranked_lookup.get(base_entry["symbol"])
            if extra:
                merged.update(extra)
            selected_entries.append(merged)
    else:
        selected_entries = list(ranked_entries or [])
    if not selected_entries:
        print("[MonteCarlo] No eligible entries found for simulation; aborting.")
        return {}
    start = time.perf_counter()
    results = run_monte_carlo_batch(conn, selected_entries, cfg)
    ordered = persist_results(conn, as_of, results)
    write_reports(as_of, ordered, cfg, reports_dir, top_n=top_n)
    duration = time.perf_counter() - start
    record_metrics(duration, len(results), ordered[:top_n])
    return results
