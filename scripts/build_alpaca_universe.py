#!/usr/bin/env python3
# /mnt/data/PulseTrade/scripts/build_alpaca_universe.py

import csv
import json
import os
import sys
from typing import List
import argparse
import time

try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = None

import requests

DEFAULT_OUTPUT_DIR = "/mnt/data/PulseTrade/db"

def load_env():
    # Load .env from project root if running from project root
    if load_dotenv:
        # Try current working dir .env first; fall back to project root if needed
        load_dotenv(dotenv_path=os.path.join(os.getcwd(), ".env"))
    key = os.getenv("ALPACA_API_KEY_ID")
    secret = os.getenv("ALPACA_API_SECRET_KEY")
    env = os.getenv("ALPACA_ENV", "paper").lower().strip()
    if env not in ("paper", "live"):
        env = "paper"
    if not key or not secret:
        print("ERROR: ALPACA_KEY_ID / ALPACA_SECRET_KEY not found in environment.", file=sys.stderr)
        sys.exit(1)
    return key, secret, env

def base_url_for_env(env: str) -> str:
    if env == "live":
        return "https://api.alpaca.markets"
    return "https://paper-api.alpaca.markets"

def fetch_assets(base_url: str, key: str, secret: str, exchange_filter: List[str] = None, retries: int = 3, backoff: float = 1.5):
    """
    Calls GET /v2/assets with server-supported filters:
      - status=active
      - asset_class=us_equity
      - exchange (optional, repeated per API semantics; here we call once per exchange to keep payloads small)
    Then merges results and de-duplicates by symbol.
    """
    headers = {
        "APCA-API-KEY-ID": key,
        "APCA-API-SECRET-KEY": secret,
    }

    params_base = {
        "status": "active",
        "asset_class": "us_equity",
    }

    def _do_request(p):
        attempt = 0
        while True:
            attempt += 1
            try:
                resp = requests.get(f"{base_url}/v2/assets", headers=headers, params=p, timeout=30)
                if resp.status_code == 429:
                    # Respect rate limits
                    wait = backoff ** attempt
                    time.sleep(min(wait, 30))
                    continue
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as e:
                if attempt >= retries:
                    raise
                time.sleep(backoff ** attempt)

    all_assets = []
    if exchange_filter:
        seen = set()
        for ex in exchange_filter:
            pb = dict(params_base)
            pb["exchange"] = ex
            data = _do_request(pb)
            for a in data:
                sym = a.get("symbol")
                if sym not in seen:
                    all_assets.append(a)
                    seen.add(sym)
    else:
        all_assets = _do_request(params_base)

    return all_assets

def client_filter(assets: List[dict], want_tradable=True, want_fractionable=True, want_shortable=True):
    out = []
    for a in assets:
        if want_tradable and not a.get("tradable", False):
            continue
        if want_fractionable and not a.get("fractionable", False):
            continue
        if want_shortable and not a.get("shortable", False):
            continue
        out.append(a)
    return out

def write_outputs(assets: List[dict], output_dir: str):
    os.makedirs(output_dir, exist_ok=True)
    csv_path = os.path.join(output_dir, "alpaca_universe.csv")
    symbols_path = os.path.join(output_dir, "alpaca_universe.symbols.txt")
    meta_path = os.path.join(output_dir, "alpaca_universe.meta.json")

    # Choose a consistent subset of columns
    fieldnames = [
        "symbol",
        "name",
        "exchange",
        "asset_class",
        "status",
        "tradable",
        "fractionable",
        "shortable",
        "marginable",
        "easy_to_borrow",
    ]

    # Normalize asset_class key name across responses (sometimes 'class')
    def normalize(a):
        ac = a.get("asset_class") or a.get("class")
        return {
            "symbol": a.get("symbol"),
            "name": a.get("name"),
            "exchange": a.get("exchange"),
            "asset_class": ac,
            "status": a.get("status"),
            "tradable": a.get("tradable"),
            "fractionable": a.get("fractionable"),
            "shortable": a.get("shortable"),
            "marginable": a.get("marginable"),
            "easy_to_borrow": a.get("easy_to_borrow"),
        }

    normalized = [normalize(a) for a in assets]
    normalized.sort(key=lambda r: (r["exchange"] or "", r["symbol"] or ""))

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        for row in normalized:
            w.writerow(row)

    with open(symbols_path, "w", encoding="utf-8") as f:
        for row in normalized:
            if row["symbol"]:
                f.write(row["symbol"] + "\n")

    # Write a tiny meta summary
    summary = {
        "count": len(normalized),
        "exchanges": sorted(list({r["exchange"] for r in normalized if r["exchange"]})),
        "filters": {
            "status": "active",
            "asset_class": "us_equity",
            "tradable": True,
            "fractionable": True,
            "shortable": True,
        },
    }
    with open(meta_path, "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)

    return csv_path, symbols_path, meta_path

def main():
    parser = argparse.ArgumentParser(description="Build Alpaca universe list (active US equities that are tradable, fractionable, shortable).")
    parser.add_argument("--exchanges", type=str, default="NYSE,NASDAQ,NYSEARCA",
                        help="Comma-separated exchanges to request server-side (default: NYSE,NASDAQ,NYSEARCA). Use '' to request all.")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR,
                        help=f"Directory to write outputs (default: {DEFAULT_OUTPUT_DIR})")
    args = parser.parse_args()

    key, secret, env = load_env()
    base = base_url_for_env(env)

    exchange_filter = None
    if args.exchanges.strip():
        exchange_filter = [e.strip().upper() for e in args.exchanges.split(",") if e.strip()]

    assets = fetch_assets(base, key, secret, exchange_filter=exchange_filter)
    filtered = client_filter(assets, want_tradable=True, want_fractionable=True, want_shortable=True)
    csv_path, symbols_path, meta_path = write_outputs(filtered, args.output_dir)

    print(f"[ok] {len(filtered)} symbols written")
    print(f"CSV:    {csv_path}")
    print(f"SYMS:   {symbols_path}")
    print(f"META:   {meta_path}")

if __name__ == "__main__":
    main()
