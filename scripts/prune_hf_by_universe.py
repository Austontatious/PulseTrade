#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Prune a financial dataset to only symbols in the Alpaca universe.

Reads:  /mnt/data/PulseTrade/db/alpaca_universe.symbols.txt
Writes: filtered copies of the input files to --out-dir (mirrors structure)

Supported formats:
- .csv (chunked)
- .parquet
- .feather
- .jsonl / .ndjson

Auto-detects symbol column from common names, or use --symbol-col.
"""

import argparse
import json
import os
import sys
import shutil
from pathlib import Path
from typing import Iterable, Optional, Set, Tuple

import pandas as pd

# Optional imports (parquet/feather) — pandas will handle if pyarrow is installed
# Ensure you have pyarrow installed in your PulseTrade environment.

UNIVERSE_SYMS_PATH = "/mnt/data/PulseTrade/db/alpaca_universe.symbols.txt"
DEFAULT_OUT_DIR = "/mnt/data/PulseTrade/db/pruned_dataset"

# Common symbol/ticker column names to try
CANDIDATE_COLS = ["symbol", "SYMBOL", "ticker", "TICKER", "Symbol", "Ticker"]

SUPPORTED_EXTS = {".csv", ".parquet", ".feather", ".jsonl", ".ndjson"}

def load_universe(path: str) -> Set[str]:
    if not os.path.exists(path):
        print(f"ERROR: Universe symbols file not found: {path}", file=sys.stderr)
        sys.exit(1)
    syms = set()
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if s:
                syms.add(s)
    if not syms:
        print("ERROR: Universe symbols list is empty.", file=sys.stderr)
        sys.exit(1)
    return syms

def detect_symbol_col(df: pd.DataFrame, preferred: Optional[str]) -> Optional[str]:
    if preferred and preferred in df.columns:
        return preferred
    for c in CANDIDATE_COLS:
        if c in df.columns:
            return c
    # Loose fallback: case-insensitive match
    lower_map = {c.lower(): c for c in df.columns}
    for c in CANDIDATE_COLS:
        if c.lower() in lower_map:
            return lower_map[c.lower()]
    return None

def ensure_outfile(in_root: Path, out_root: Path, file_path: Path) -> Path:
    rel = file_path.relative_to(in_root)
    out_path = out_root.joinpath(rel)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    return out_path

def filter_dataframe(df: pd.DataFrame, col: str, universe: Set[str]) -> pd.DataFrame:
    # Normalize column to string to avoid categorical issues
    return df[df[col].astype(str).isin(universe)]

def process_csv(file_path: Path, out_path: Path, universe: Set[str], symbol_col: Optional[str], chunksize: int = 250_000) -> Tuple[int, int]:
    rows_in = 0
    rows_out = 0
    out_tmp = out_path.with_suffix(out_path.suffix + ".tmp")

    # Remove any existing partials
    if out_tmp.exists():
        out_tmp.unlink()
    if out_path.exists():
        out_path.unlink()

    first_chunk = True
    for chunk in pd.read_csv(file_path, chunksize=chunksize):
        rows_in += len(chunk)
        col = detect_symbol_col(chunk, symbol_col)
        if not col:
            # If we can't find a symbol column, copy nothing (or copy through?)
            # Safer: skip this file with a warning
            print(f"[WARN] {file_path}: no symbol column found; skipping.", file=sys.stderr)
            return (rows_in, rows_out)

        filtered = filter_dataframe(chunk, col, universe)
        rows_out += len(filtered)

        if first_chunk:
            filtered.to_csv(out_tmp, index=False)
            first_chunk = False
        else:
            filtered.to_csv(out_tmp, index=False, mode="a", header=False)

    if not first_chunk:
        out_tmp.replace(out_path)
    return (rows_in, rows_out)

def process_parquet(file_path: Path, out_path: Path, universe: Set[str], symbol_col: Optional[str]) -> Tuple[int, int]:
    df = pd.read_parquet(file_path)
    rows_in = len(df)
    col = detect_symbol_col(df, symbol_col)
    if not col:
        print(f"[WARN] {file_path}: no symbol column found; skipping.", file=sys.stderr)
        return (rows_in, 0)
    filtered = filter_dataframe(df, col, universe)
    filtered.to_parquet(out_path, index=False)
    return (rows_in, len(filtered))

def process_feather(file_path: Path, out_path: Path, universe: Set[str], symbol_col: Optional[str]) -> Tuple[int, int]:
    df = pd.read_feather(file_path)
    rows_in = len(df)
    col = detect_symbol_col(df, symbol_col)
    if not col:
        print(f"[WARN] {file_path}: no symbol column found; skipping.", file=sys.stderr)
        return (rows_in, 0)
    filtered = filter_dataframe(df, col, universe)
    filtered.to_feather(out_path)
    return (rows_in, len(filtered))

def process_jsonl(file_path: Path, out_path: Path, universe: Set[str], symbol_col: Optional[str]) -> Tuple[int, int]:
    rows_in = 0
    rows_out = 0
    out_tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    if out_tmp.exists():
        out_tmp.unlink()
    if out_path.exists():
        out_path.unlink()

    # Stream line by line
    with open(file_path, "r", encoding="utf-8") as inp, open(out_tmp, "w", encoding="utf-8") as out:
        symbol_key = symbol_col  # may be None; detect on first valid line
        for line in inp:
            line = line.strip()
            if not line:
                continue
            rows_in += 1
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            # Detect key lazily
            if symbol_key is None:
                # try direct keys
                for c in CANDIDATE_COLS:
                    if c in obj:
                        symbol_key = c
                        break
                if symbol_key is None:
                    # try case-insensitive
                    lower_map = {k.lower(): k for k in obj.keys()}
                    for c in CANDIDATE_COLS:
                        if c.lower() in lower_map:
                            symbol_key = lower_map[c.lower()]
                            break
                if symbol_key is None:
                    # Can't detect on this line; continue
                    continue

            sym = obj.get(symbol_key)
            if sym is None:
                continue
            if str(sym) in universe:
                out.write(json.dumps(obj, ensure_ascii=False) + "\n")
                rows_out += 1

    if rows_out > 0:
        out_tmp.replace(out_path)
    else:
        # Nothing matched; clean up tmp
        if out_tmp.exists():
            out_tmp.unlink()
    return (rows_in, rows_out)

def should_process(path: Path) -> bool:
    if not path.is_file():
        return False
    return path.suffix.lower() in SUPPORTED_EXTS

def walk_inputs(in_path: Path) -> Iterable[Path]:
    if in_path.is_file():
        if should_process(in_path):
            yield in_path
        return
    for p in in_path.rglob("*"):
        if should_process(p):
            yield p

def main():
    ap = argparse.ArgumentParser(description="Prune HF dataset to Alpaca universe.")
    ap.add_argument("input", help="Path to a file or directory containing dataset files")
    ap.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help=f"Output directory (default: {DEFAULT_OUT_DIR})")
    ap.add_argument("--symbol-col", default=None, help="Explicit symbol column (overrides auto-detect)")
    ap.add_argument("--overwrite", action="store_true", help="Allow overwriting existing output directory")
    ap.add_argument("--exchanges", default=None, help="Optional CSV of allowed exchanges to double-filter (e.g., NYSE,NASDAQ,NYSEARCA)")
    args = ap.parse_args()

    in_path = Path(args.input).resolve()
    out_root = Path(args.out_dir).resolve()

    if not in_path.exists():
        print(f"ERROR: Input path not found: {in_path}", file=sys.stderr)
        sys.exit(1)

    if out_root.exists() and not args.overwrite:
        print(f"ERROR: Output dir exists: {out_root} (use --overwrite to reuse)", file=sys.stderr)
        sys.exit(1)

    out_root.mkdir(parents=True, exist_ok=True)

    universe = load_universe(UNIVERSE_SYMS_PATH)

    # Optional exchange double-filter: read exchanges from alpaca_universe.csv if provided
    allowed_ex = None
    if args.exchanges:
        allowed_ex = {e.strip().upper() for e in args.exchanges.split(",") if e.strip()}

    summary = {
        "input_root": str(in_path),
        "output_root": str(out_root),
        "universe_count": len(universe),
        "files": [],
        "total_rows_in": 0,
        "total_rows_out": 0,
    }

    # If exchanges are provided, derive a per-symbol mask from alpaca_universe.csv
    symbols_by_exchange = None
    if allowed_ex:
        csv_path = Path(UNIVERSE_SYMS_PATH).with_suffix(".csv")
        if csv_path.exists():
            dfu = pd.read_csv(csv_path)
            ex_col = "exchange" if "exchange" in dfu.columns else None
            if ex_col:
                allowed_syms = set(dfu.loc[dfu[ex_col].astype(str).str.upper().isin(allowed_ex), "symbol"].astype(str))
                universe = universe.intersection(allowed_syms)

    for file_path in walk_inputs(in_path):
        out_path = ensure_outfile(in_path, out_root, file_path)
        rows_in = rows_out = 0
        try:
            ext = file_path.suffix.lower()
            if ext == ".csv":
                rows_in, rows_out = process_csv(file_path, out_path, universe, args.symbol_col)
            elif ext == ".parquet":
                rows_in, rows_out = process_parquet(file_path, out_path, universe, args.symbol_col)
            elif ext == ".feather":
                rows_in, rows_out = process_feather(file_path, out_path, universe, args.symbol_col)
            elif ext in (".jsonl", ".ndjson"):
                rows_in, rows_out = process_jsonl(file_path, out_path, universe, args.symbol_col)
            else:
                # Shouldn't hit due to SUPPORTED_EXTS, but keep future-proof
                pass
        except Exception as e:
            print(f"[ERROR] {file_path}: {e}", file=sys.stderr)

        summary["files"].append({
            "input": str(file_path),
            "output": str(out_path),
            "rows_in": rows_in,
            "rows_out": rows_out,
        })
        summary["total_rows_in"] += rows_in
        summary["total_rows_out"] += rows_out

    # Write summary
    with open(out_root / "prune_summary.json", "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    print(f"[done] Files processed: {len(summary['files'])}")
    print(f"[done] Rows in:  {summary['total_rows_in']}")
    print(f"[done] Rows out: {summary['total_rows_out']}")
    print(f"[ok] Summary: {out_root / 'prune_summary.json'}")

if __name__ == "__main__":
    main()
