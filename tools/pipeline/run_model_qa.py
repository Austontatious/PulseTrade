#!/usr/bin/env python3
"""Offline QA for Kronos N-BEATS artifacts."""

from __future__ import annotations

import argparse
import bisect
import json
import math
import os
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

import numpy as np
import psycopg2

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from libs.kronos.model_loader import LocalNBeatsArtifact, load_nbeats_artifact  # noqa: E402
from tools.universe.build_signal_universe import database_url  # noqa: E402


def _fetch_symbol_list(conn, limit: int) -> List[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT symbol
            FROM alpaca_universe
            ORDER BY symbol ASC
            LIMIT %s
            """,
            (limit,),
        )
        return [row[0] for row in cur.fetchall()]


def _fetch_returns(
    conn,
    symbol: str,
    start_date: date,
    end_date: date,
) -> Tuple[List[date], List[float]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ds, y
            FROM daily_returns
            WHERE symbol=%s
              AND ds >= %s
              AND ds <= %s
            ORDER BY ds ASC
            """,
            (symbol, start_date, end_date),
        )
        rows = cur.fetchall()
    return [row[0] for row in rows], [float(row[1]) for row in rows]


def _evaluate_symbol(
    artifact: LocalNBeatsArtifact,
    dates: List[date],
    values: List[float],
    as_of: date,
) -> Optional[Dict[str, Any]]:
    if not dates:
        return None
    idx = bisect.bisect_left(dates, as_of)
    if idx < artifact.input_size:
        return None
    history = values[:idx]
    future_dates = dates[idx:]
    future_values = values[idx:]
    if len(history) < artifact.input_size or len(future_values) < artifact.horizon:
        return None
    history_window = np.array(history[-artifact.input_size :], dtype=np.float32)
    preds = artifact.predict(history_window)
    realized = future_values[: artifact.horizon]
    realized_dates = future_dates[: artifact.horizon]
    if any(ds <= as_of for ds in realized_dates):
        return None
    residuals = [real - pred for pred, real in zip(preds, realized)]
    directional_hits = sum(1 for pred, real in zip(preds, realized) if pred == 0 or real == 0 or (pred > 0) == (real > 0))
    return {
        "residuals": residuals,
        "directional_hits": directional_hits,
        "count": len(residuals),
        "abs_errors": [abs(val) for val in residuals],
    }


def _aggregate_metrics(residuals: List[float], directional_hits: int, total_preds: int) -> Dict[str, Optional[float]]:
    if not residuals:
        return {
            "pair_count": 0,
            "mae": None,
            "resid_std": None,
            "coverage": None,
            "directional_accuracy": None,
        }
    mae = sum(abs(res) for res in residuals) / len(residuals)
    if len(residuals) > 1:
        mean_resid = sum(residuals) / len(residuals)
        resid_std = math.sqrt(sum((res - mean_resid) ** 2 for res in residuals) / (len(residuals) - 1))
    else:
        resid_std = 0.0
    coverage = None
    if resid_std is not None:
        threshold = 1.96 * max(resid_std, 1e-6)
        coverage = sum(1 for res in residuals if abs(res) <= threshold) / len(residuals)
    directional_accuracy = (directional_hits / total_preds) if total_preds else None
    return {
        "pair_count": len(residuals),
        "mae": mae,
        "resid_std": resid_std,
        "coverage": coverage,
        "directional_accuracy": directional_accuracy,
    }


def _evaluate_artifact(
    artifact: LocalNBeatsArtifact,
    symbols: Sequence[str],
    as_of_dates: Sequence[date],
    returns_cache: Dict[str, Tuple[List[date], List[float]]],
) -> Dict[str, Any]:
    residuals: List[float] = []
    directional_hits = 0
    total_preds = 0
    per_as_of: Dict[str, Dict[str, Optional[float]]] = {}
    for as_of in as_of_dates:
        as_of_abs_errors: List[float] = []
        as_of_pairs = 0
        for symbol in symbols:
            dates, values = returns_cache.get(symbol, ([], []))
            evaluation = _evaluate_symbol(artifact, dates, values, as_of)
            if not evaluation:
                continue
            residuals.extend(evaluation["residuals"])
            directional_hits += evaluation["directional_hits"]
            total_preds += evaluation["count"]
            as_of_abs_errors.extend(evaluation["abs_errors"])
            as_of_pairs += evaluation["count"]
        per_as_of[as_of.isoformat()] = {
            "mae": (sum(as_of_abs_errors) / as_of_pairs) if as_of_pairs else None,
            "pair_count": as_of_pairs,
        }
    metrics = _aggregate_metrics(residuals, directional_hits, total_preds)
    return {"metrics": metrics, "per_as_of": per_as_of}


def run_evaluation(
    artifact_paths: Sequence[Path],
    *,
    as_of_dates: Sequence[date],
    max_symbols: int,
    symbols: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    if not artifact_paths:
        raise ValueError("At least one artifact path is required.")
    artifacts = [load_nbeats_artifact(p) for p in artifact_paths]
    max_input = max(artifact.input_size for artifact in artifacts)
    max_horizon = max(artifact.horizon for artifact in artifacts)
    min_as_of = min(as_of_dates)
    max_as_of = max(as_of_dates)
    start_date = min_as_of - timedelta(days=max_input + max_horizon + 10)
    end_date = max_as_of + timedelta(days=max_horizon + 5)

    conn = psycopg2.connect(database_url())
    try:
        symbol_list = list(symbols) if symbols else _fetch_symbol_list(conn, max_symbols)
        returns_cache: Dict[str, Tuple[List[date], List[float]]] = {
            sym: _fetch_returns(conn, sym, start_date, end_date) for sym in symbol_list
        }
    finally:
        conn.close()

    artifact_reports: List[Dict[str, Any]] = []
    for artifact in artifacts:
        report = _evaluate_artifact(artifact, symbol_list, as_of_dates, returns_cache)
        artifact_reports.append(
            {
                "path": str(artifact.path),
                "version": artifact.version,
                "input_size": artifact.input_size,
                "horizon": artifact.horizon,
                **report,
            }
        )

    best = None
    best_score = float("inf")
    for entry in artifact_reports:
        mae = entry["metrics"]["mae"]
        score = mae if mae is not None else float("inf")
        if score < best_score:
            best_score = score
            best = entry["path"]

    return {
        "generated_at": datetime.utcnow().isoformat() + "Z",
        "as_of_dates": [d.isoformat() for d in as_of_dates],
        "artifacts": artifact_reports,
        "winner": best,
    }


def _parse_offsets(raw: Optional[str]) -> List[int]:
    if not raw:
        return [1, 7, 30]
    return [int(token) for token in raw.split(",") if token.strip()]


def build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Evaluate Kronos artifacts against historical returns.")
    parser.add_argument("--artifact", required=True, type=Path, help="Path to candidate artifact directory.")
    parser.add_argument(
        "--compare-against",
        type=Path,
        help="Optional reference artifact directory for side-by-side metrics.",
    )
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=150,
        help="Maximum number of symbols to sample for QA (default: 150).",
    )
    parser.add_argument(
        "--offsets",
        type=str,
        default="1,7,30",
        help="Comma-separated list of day offsets for as-of checkpoints (default: 1,7,30).",
    )
    parser.add_argument(
        "--output",
        type=Path,
        help="Optional path to write the QA report JSON.",
    )
    return parser


def main() -> None:
    parser = build_arg_parser()
    args = parser.parse_args()
    offsets = _parse_offsets(args.offsets)
    as_of_dates = [date.today() - timedelta(days=offset) for offset in offsets]
    artifact_paths = [args.artifact]
    if args.compare_against:
        artifact_paths.append(args.compare_against)
    report = run_evaluation(artifact_paths, as_of_dates=as_of_dates, max_symbols=args.max_symbols)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(report, indent=2))
        print(f"Wrote QA report to {args.output}")
    else:
        print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
