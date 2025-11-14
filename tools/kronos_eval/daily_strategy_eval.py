#!/usr/bin/env python3
"""Nightly Kronos strategy scorecard.

Usage:
    python tools/kronos_eval/daily_strategy_eval.py --date 2024-07-05

The script pulls policy fills, attaches the most recent Kronos forecast plus
strategist metadata, buckets trades by signal strength / PT score / regime,
and writes both a CSV summary table and a short Markdown narrative into
reports/.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime as dt
import json
import math
import os
from bisect import bisect_right
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

import asyncpg
import pandas as pd
from zoneinfo import ZoneInfo

EASTERN = ZoneInfo("America/New_York")
REPORTS_DIR = Path("reports")
SPY_SYMBOL = os.getenv("STRATEGY_EVAL_BASELINE", "SPY")
SECTOR_ETF_MAP = {
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
SIGNAL_BUCKETS = [
    (10, "<=10bps"),
    (25, "10-25bps"),
    (50, "25-50bps"),
    (75, "50-75bps"),
    (100, "75-100bps"),
]
SCORE_BUCKET_LABELS = ["bottom20", "20-40", "40-60", "60-80", "top20"]

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:"
    f"{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/"
    f"{os.getenv('POSTGRES_DB', 'pulse')}"
)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Daily Kronos strategy evaluation")
    today_ny = dt.datetime.now(EASTERN).date()
    default_date = today_ny - dt.timedelta(days=1)
    parser.add_argument(
        "--date",
        dest="trade_date",
        type=str,
        default=default_date.isoformat(),
        help="Trading date to evaluate (YYYY-MM-DD). Defaults to previous NY session.",
    )
    parser.add_argument(
        "--exit-window-days",
        type=int,
        default=5,
        help="How many calendar days past the target date to look for exit fills.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=REPORTS_DIR,
        help="Directory to write summary artifacts (default: reports/).",
    )
    return parser.parse_args()


async def _fetch_pre_positions(conn: asyncpg.Connection, before_ts: dt.datetime) -> Dict[str, float]:
    rows = await conn.fetch(
        """
        SELECT ticker,
               SUM(CASE WHEN side='buy' THEN qty ELSE -qty END) AS net_qty
        FROM fills
        WHERE ts < $1
          AND meta->>'source' IN ('planner', 'pt_allocator')
        GROUP BY ticker
        HAVING ABS(SUM(CASE WHEN side='buy' THEN qty ELSE -qty END)) > 1e-6
        """,
        before_ts,
    )
    return {r["ticker"]: float(r["net_qty"] or 0.0) for r in rows}


async def _fetch_fills(
    conn: asyncpg.Connection,
    start_ts: dt.datetime,
    end_ts: dt.datetime,
) -> List[asyncpg.Record]:
    rows = await conn.fetch(
        """
        SELECT
            f.ts,
            f.ticker,
            f.side,
            f.qty,
            f.price,
            f.meta,
            fc.ts AS forecast_ts,
            fc.mean AS forecast_mean,
            fc.lower AS forecast_lower,
            fc.upper AS forecast_upper,
            fc.model AS forecast_model,
            sr.ts AS strat_ts,
            sr.score AS strat_score,
            sr.side AS strat_side,
            sr.meta AS strat_meta
        FROM fills f
        LEFT JOIN LATERAL (
            SELECT ts, mean, lower, upper, model
            FROM forecasts
            WHERE ticker=f.ticker
              AND horizon='1m'
              AND ts <= f.ts
            ORDER BY ts DESC
            LIMIT 1
        ) fc ON TRUE
        LEFT JOIN LATERAL (
            SELECT ts, side, score, meta
            FROM strategist_recos
            WHERE ticker=f.ticker
              AND ts <= f.ts
            ORDER BY ts DESC
            LIMIT 1
        ) sr ON TRUE
        WHERE f.ts >= $1
          AND f.ts < $2
          AND f.meta->>'source' IN ('planner','pt_allocator')
        ORDER BY f.ts ASC
        """,
        start_ts,
        end_ts,
    )
    return rows


async def _fetch_regime_events(conn: asyncpg.Connection, cutoff: dt.datetime) -> List[asyncpg.Record]:
    rows = await conn.fetch(
        """
        SELECT ts, value
        FROM policy_knobs
        WHERE key='risk_regime'
          AND ts <= $1
        ORDER BY ts ASC
        """,
        cutoff,
    )
    return rows


def _build_regime_lookup(events: Sequence[asyncpg.Record]):
    times: List[dt.datetime] = []
    regimes: List[str] = []
    for record in events:
        raw_val = record["value"]
        regime = "unknown"
        if isinstance(raw_val, dict) and "value" in raw_val:
            regime = str(raw_val["value"]).lower()
        elif isinstance(raw_val, str):
            try:
                parsed = json.loads(raw_val)
                if isinstance(parsed, dict) and "value" in parsed:
                    regime = str(parsed["value"]).lower()
                else:
                    regime = raw_val.lower()
            except json.JSONDecodeError:
                regime = raw_val.lower()
        else:
            regime = "unknown"
        times.append(record["ts"])
        regimes.append(regime)

    def lookup(ts: dt.datetime) -> str:
        if not times:
            return "unknown"
        idx = bisect_right(times, ts) - 1
        if idx < 0:
            return regimes[0]
        return regimes[idx]

    return lookup


async def _fetch_company_profiles(conn: asyncpg.Connection, symbols: Sequence[str]) -> Dict[str, str]:
    if not symbols:
        return {}
    rows = await conn.fetch(
        """
        SELECT symbol, sector
        FROM dim_company_profile
        WHERE symbol = ANY($1::text[])
        """,
        symbols,
    )
    return {r["symbol"]: (r["sector"] or "").upper() for r in rows}


async def _fetch_daily_returns(
    conn: asyncpg.Connection,
    symbols: Sequence[str],
    start_date: dt.date,
    end_date: dt.date,
) -> Dict[tuple[str, dt.date], float]:
    if not symbols:
        return {}
    rows = await conn.fetch(
        """
        SELECT symbol, ds::date AS trade_date, y
        FROM daily_returns
        WHERE symbol = ANY($1::text[])
          AND ds BETWEEN $2 AND $3
        """,
        symbols,
        start_date,
        end_date,
    )
    return {(r["symbol"], r["trade_date"]): float(r["y"]) for r in rows if r["y"] is not None}


def _placeholder_lot(symbol: str, qty: float, direction: int) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "direction": direction,
        "qty_open": abs(qty),
        "entry_qty": abs(qty),
        "entry_price": 0.0,
        "entry_ts": None,
        "source": "carry",
        "forecast_mean": None,
        "forecast_lower": None,
        "forecast_upper": None,
        "forecast_ts": None,
        "forecast_model": None,
        "strategist_score": None,
        "strategist_side": None,
        "strategist_meta": None,
        "regime": "carry",
        "entry_day": None,
        "track": False,
        "exit_ts": None,
        "exit_price": None,
        "exit_qty": 0.0,
        "exit_notional": 0.0,
    }


def _make_lot(
    row: asyncpg.Record,
    direction: int,
    remaining: float,
    regime_lookup,
) -> dict[str, Any]:
    meta = row["meta"] or {}
    strat_meta = row["strat_meta"] or {}
    entry_ts: dt.datetime = row["ts"]
    entry_day = entry_ts.astimezone(EASTERN).date()
    return {
        "symbol": row["ticker"],
        "direction": direction,
        "qty_open": remaining,
        "entry_qty": remaining,
        "entry_price": float(row["price"]),
        "entry_ts": entry_ts,
        "source": meta.get("source"),
        "forecast_mean": float(row["forecast_mean"]) if row["forecast_mean"] is not None else None,
        "forecast_lower": float(row["forecast_lower"]) if row["forecast_lower"] is not None else None,
        "forecast_upper": float(row["forecast_upper"]) if row["forecast_upper"] is not None else None,
        "forecast_ts": row["forecast_ts"],
        "forecast_model": row["forecast_model"],
        "strategist_score": float(row["strat_score"]) if row["strat_score"] is not None else None,
        "strategist_side": row["strat_side"],
        "strategist_meta": strat_meta,
        "regime": regime_lookup(entry_ts),
        "entry_day": entry_day,
        "track": True,
        "exit_ts": None,
        "exit_price": None,
        "exit_qty": 0.0,
        "exit_notional": 0.0,
    }


def _attach_exit(lot: dict[str, Any], close_qty: float, price: float, ts: dt.datetime) -> None:
    lot["exit_qty"] += close_qty
    lot["exit_notional"] += close_qty * price
    lot["exit_ts"] = ts
    if lot["qty_open"] <= 1e-9 and lot["exit_qty"] > 0:
        lot["exit_price"] = lot["exit_notional"] / lot["exit_qty"]


def _build_entries(
    rows: Iterable[asyncpg.Record],
    start_ts: dt.datetime,
    end_ts: dt.datetime,
    pre_positions: Dict[str, float],
    regime_lookup,
) -> List[dict[str, Any]]:
    open_lots: dict[str, List[dict[str, Any]]] = defaultdict(list)
    entries: List[dict[str, Any]] = []

    for symbol, qty in pre_positions.items():
        if abs(qty) < 1e-6:
            continue
        direction = 1 if qty > 0 else -1
        open_lots[symbol].append(_placeholder_lot(symbol, abs(qty), direction))

    for row in rows:
        symbol = row["ticker"]
        side = (row["side"] or "").lower()
        if side not in {"buy", "sell"}:
            continue
        direction = 1 if side == "buy" else -1
        qty = float(row["qty"] or 0.0)
        if qty <= 0:
            continue
        lot_list = open_lots[symbol]
        remaining = qty
        idx = 0
        while remaining > 1e-9 and idx < len(lot_list):
            lot = lot_list[idx]
            if lot["direction"] != -direction:
                idx += 1
                continue
            close_qty = min(remaining, lot["qty_open"])
            lot["qty_open"] -= close_qty
            _attach_exit(lot, close_qty, float(row["price"]), row["ts"])
            remaining -= close_qty
            if lot["qty_open"] <= 1e-9:
                lot_list.pop(idx)
            else:
                idx += 1

        if remaining > 1e-9:
            track = start_ts <= row["ts"] < end_ts
            new_lot = _make_lot(row, direction, remaining, regime_lookup)
            new_lot["track"] = track
            lot_list.append(new_lot)
            if track:
                entries.append(new_lot)

    return entries


def _calc_cumulative_return(
    symbol: str,
    entry_day: dt.date,
    horizon: int,
    daily_map: Dict[tuple[str, dt.date], float],
) -> float | None:
    acc = 0.0
    steps = 0
    for offset in range(1, horizon + 1):
        day = entry_day + dt.timedelta(days=offset)
        val = daily_map.get((symbol, day))
        if val is None:
            continue
        acc += float(val)
        steps += 1
    if steps == 0:
        return None
    return math.exp(acc) - 1.0


def _bucket_signal(abs_bps: float | None) -> str:
    if abs_bps is None:
        return "missing"
    for threshold, label in SIGNAL_BUCKETS:
        if abs_bps <= threshold:
            return label
    return ">100bps"


def _assign_score_buckets(df: pd.DataFrame) -> None:
    df["score_bucket"] = "missing"
    mask = df["strategist_score"].notna()
    if mask.sum() < len(SCORE_BUCKET_LABELS):
        return
    try:
        df.loc[mask, "score_bucket"] = pd.qcut(
            df.loc[mask, "strategist_score"],
            q=len(SCORE_BUCKET_LABELS),
            labels=SCORE_BUCKET_LABELS,
            duplicates="drop",
        ).astype(str)
    except ValueError:
        pass


def _safe_mean(series: pd.Series) -> float | None:
    if series.empty:
        return None
    value = float(series.mean(skipna=True))
    if math.isnan(value):
        return None
    return value


def _fmt_pct(value: float | None) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    return f"{value * 100:.2f}%"


def _fmt_bp(value: float | None) -> str:
    if value is None or math.isnan(value):
        return "n/a"
    return f"{value * 1e4:.1f} bps"


def _make_records(
    entries: Sequence[dict[str, Any]],
    sector_map: Dict[str, str],
    daily_map: Dict[tuple[str, dt.date], float],
) -> pd.DataFrame:
    records: List[dict[str, Any]] = []
    for lot in entries:
        symbol = lot["symbol"]
        entry_price = lot["entry_price"] or 0.0
        if entry_price <= 0:
            continue
        direction = lot["direction"]
        entry_day = lot["entry_day"]
        side_mult = 1 if direction >= 0 else -1
        r_hold = None
        if lot.get("exit_price"):
            r_hold = (lot["exit_price"] / entry_price - 1.0) * side_mult
        r_1d = None
        r_3d = None
        if entry_day is not None:
            r_1d = _calc_cumulative_return(symbol, entry_day, 1, daily_map)
            r_3d = _calc_cumulative_return(symbol, entry_day, 3, daily_map)
            if r_1d is not None:
                r_1d *= side_mult
            if r_3d is not None:
                r_3d *= side_mult
        sector = sector_map.get(symbol)
        sector_symbol = SECTOR_ETF_MAP.get(sector or "")
        spy_r1d = _calc_cumulative_return(SPY_SYMBOL, entry_day, 1, daily_map) if entry_day else None
        sector_r1d = (
            _calc_cumulative_return(sector_symbol, entry_day, 1, daily_map)
            if entry_day and sector_symbol
            else None
        )
        edge_spy_1d = r_1d - spy_r1d if (r_1d is not None and spy_r1d is not None) else None
        edge_sector_1d = r_1d - sector_r1d if (r_1d is not None and sector_r1d is not None) else None
        edge_spy_hold = r_hold - spy_r1d if (r_hold is not None and spy_r1d is not None) else None
        edge_sector_hold = r_hold - sector_r1d if (r_hold is not None and sector_r1d is not None) else None
        forecast_dev = None
        if lot.get("forecast_mean"):
            forecast_dev = (lot["forecast_mean"] / entry_price) - 1.0
        abs_dev_bps = abs(forecast_dev) * 1e4 if forecast_dev is not None else None
        forecast_hit = None
        if forecast_dev is not None and r_1d is not None:
            pred_sign = 1 if forecast_dev > 0 else -1 if forecast_dev < 0 else 0
            realized_sign = 1 if r_1d > 0 else -1 if r_1d < 0 else 0
            if pred_sign != 0 and realized_sign != 0:
                forecast_hit = pred_sign == realized_sign
        records.append(
            {
                "symbol": symbol,
                "side": "long" if direction >= 0 else "short",
                "source": lot.get("source") or "unknown",
                "entry_ts": lot["entry_ts"],
                "exit_ts": lot.get("exit_ts"),
                "entry_price": entry_price,
                "exit_price": lot.get("exit_price"),
                "qty": lot["entry_qty"],
                "regime": lot.get("regime") or "unknown",
                "sector": sector or "UNKNOWN",
                "forecast_mean": lot.get("forecast_mean"),
                "forecast_lower": lot.get("forecast_lower"),
                "forecast_upper": lot.get("forecast_upper"),
                "forecast_ts": lot.get("forecast_ts"),
                "forecast_model": lot.get("forecast_model"),
                "strategist_score": lot.get("strategist_score"),
                "strategist_side": lot.get("strategist_side"),
                "entry_day": entry_day,
                "forecast_dev": forecast_dev,
                "abs_dev_bps": abs_dev_bps,
                "signal_bucket": _bucket_signal(abs_dev_bps),
                "r_hold": r_hold,
                "r_1d": r_1d,
                "r_3d": r_3d,
                "edge_vs_spy_1d": edge_spy_1d,
                "edge_vs_sector_1d": edge_sector_1d,
                "edge_vs_spy_hold": edge_spy_hold,
                "edge_vs_sector_hold": edge_sector_hold,
                "forecast_hit_1d": forecast_hit,
                "win_flag": True if (r_1d is not None and r_1d > 0) else False if r_1d is not None else None,
            }
        )
    df = pd.DataFrame.from_records(records)
    if df.empty:
        return df
    _assign_score_buckets(df)
    return df


def _summaries(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(
            columns=[
                "bucket_type",
                "bucket",
                "count",
                "win_rate",
                "mean_r1d",
                "median_r1d",
                "edge_vs_spy",
                "edge_vs_sector",
                "forecast_hit_rate",
            ]
        )
    bucket_defs = [
        ("signal_bucket", "signal_strength"),
        ("score_bucket", "pt_score"),
        ("regime", "risk_regime"),
        ("side", "side"),
    ]
    frames: List[pd.DataFrame] = []
    for col, label in bucket_defs:
        subset = df[df[col].notna()]
        if subset.empty:
            continue
        stats = (
            subset.groupby(col)
            .agg(
                count=("symbol", "size"),
                win_rate=("win_flag", "mean"),
                mean_r1d=("r_1d", "mean"),
                median_r1d=("r_1d", "median"),
                edge_vs_spy=("edge_vs_spy_1d", "mean"),
                edge_vs_sector=("edge_vs_sector_1d", "mean"),
                forecast_hit_rate=("forecast_hit_1d", "mean"),
            )
            .reset_index()
            .rename(columns={col: "bucket"})
        )
        stats.insert(0, "bucket_type", label)
        frames.append(stats)
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _write_markdown(
    path: Path,
    trade_date: dt.date,
    df: pd.DataFrame,
) -> None:
    lines: List[str] = [f"# Kronos Strategy Evaluation — {trade_date.isoformat()}", ""]
    if df.empty:
        lines.append("No qualifying trades for this session.")
    else:
        total = len(df)
        win_rate = _fmt_pct(df["win_flag"].mean(skipna=True))
        avg_edge = _fmt_bp(df["edge_vs_spy_1d"].mean(skipna=True))
        lines.append(f"- Coverage: {total} trades, win rate {win_rate}, mean edge vs SPY {avg_edge}.")
        top_mask = df["score_bucket"] == "top20"
        mid_mask = df["score_bucket"].isin({"20-40", "40-60", "60-80"})
        bot_mask = df["score_bucket"] == "bottom20"
        top_edge = _fmt_bp(df.loc[top_mask, "edge_vs_spy_1d"].mean(skipna=True)) if top_mask.any() else "n/a"
        mid_edge = _fmt_bp(df.loc[mid_mask, "edge_vs_spy_1d"].mean(skipna=True)) if mid_mask.any() else "n/a"
        bot_edge = _fmt_bp(df.loc[bot_mask, "edge_vs_spy_1d"].mean(skipna=True)) if bot_mask.any() else "n/a"
        lines.append(
            f"- PT_SCORE splits: top 20% ideas averaged {top_edge} edge vs SPY; core 60% sat at {mid_edge}; "
            f"bottom 20% delivered {bot_edge}."
        )
        long_edge = _fmt_bp(df.loc[df["side"] == "long", "edge_vs_spy_1d"].mean(skipna=True))
        short_edge = _fmt_bp(df.loc[df["side"] == "short", "edge_vs_spy_1d"].mean(skipna=True))
        lines.append(f"- Side bias: longs {long_edge} edge vs SPY; shorts {short_edge}.")
        avg_r1 = _fmt_pct(df["r_1d"].mean(skipna=True))
        avg_r3 = _fmt_pct(df["r_3d"].mean(skipna=True))
        lines.append(f"- Holding horizon: +1d averaged {avg_r1} vs +3d {avg_r3}.")
    path.write_text("\n".join(lines), encoding="utf-8")


async def main() -> None:
    args = _parse_args()
    trade_date = dt.date.fromisoformat(args.trade_date)
    start_local = dt.datetime.combine(trade_date, dt.time(0, 0), tzinfo=EASTERN)
    end_local = start_local + dt.timedelta(days=1)
    start_utc = start_local.astimezone(dt.timezone.utc)
    end_utc = end_local.astimezone(dt.timezone.utc)
    exit_cutoff = (end_local + dt.timedelta(days=args.exit_window_days)).astimezone(dt.timezone.utc)

    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        pre_positions, fills, regime_events = await asyncio.gather(
            _fetch_pre_positions(conn, start_utc),
            _fetch_fills(conn, start_utc, exit_cutoff),
            _fetch_regime_events(conn, exit_cutoff),
        )
        regime_lookup = _build_regime_lookup(regime_events)
        entries = _build_entries(fills, start_utc, end_utc, pre_positions, regime_lookup)
        symbols = sorted({lot["symbol"] for lot in entries})
        sector_map = await _fetch_company_profiles(conn, symbols)
        extra_symbols = {SPY_SYMBOL}
        for sym in symbols:
            sector = sector_map.get(sym)
            if sector:
                etf = SECTOR_ETF_MAP.get(sector)
                if etf:
                    extra_symbols.add(etf)
        lookahead_days = max(3, args.exit_window_days)
        daily_map = await _fetch_daily_returns(
            conn,
            [*symbols, *sorted(extra_symbols)],
            trade_date,
            trade_date + dt.timedelta(days=lookahead_days),
        )
    finally:
        await conn.close()

    df = _make_records(entries, sector_map, daily_map)
    summary = _summaries(df)

    output_dir: Path = args.output_dir
    output_dir.mkdir(parents=True, exist_ok=True)
    csv_path = output_dir / f"kronos_strategy_eval_{trade_date.isoformat()}.csv"
    summary.to_csv(csv_path, index=False)
    md_path = output_dir / f"kronos_strategy_eval_{trade_date.isoformat()}.md"
    _write_markdown(md_path, trade_date, df)

    detail_path = output_dir / f"kronos_strategy_eval_{trade_date.isoformat()}_trades.parquet"
    try:
        df.to_parquet(detail_path, index=False)
        detail_note = str(detail_path)
    except Exception:
        fallback = detail_path.with_suffix(".csv")
        df.to_csv(fallback, index=False)
        detail_note = f"{fallback} (csv fallback)"
    print(f"[eval] Summary -> {csv_path}")
    print(f"[eval] Trades  -> {detail_note}")
    print(f"[eval] Notes   -> {md_path}")


if __name__ == "__main__":
    asyncio.run(main())
