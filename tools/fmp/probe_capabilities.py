import os
import sys
import time
import datetime as dt
import requests

BASE = "https://financialmodelingprep.com/api/v3"
API_KEY = os.getenv("FMP_API_KEY")
SYMBOL = os.getenv("FMP_PROBE_SYMBOL", "AAPL")

ENDPOINTS = {
    # Fundamentals (payload tables already supported by repo)
    "income_statement": f"income-statement/{SYMBOL}",
    "balance_sheet": f"balance-sheet-statement/{SYMBOL}",
    "cash_flow": f"cash-flow-statement/{SYMBOL}",
    "key_metrics": f"key-metrics/{SYMBOL}",
    "ratios": f"ratios/{SYMBOL}",
    # Calendars and screens
    "earning_calendar": "earning_calendar",
    "stock_screens_actives": "stock_market/actives",
    "stock_screens_gainers": "stock_market/gainers",
    "stock_screens_losers": "stock_market/losers",
    # Analyst targets
    "price_target": f"price-target/{SYMBOL}",
    # Market data (basic)
    "quote": f"quote/{SYMBOL}",
    "historical_price_full": f"historical-price-full/{SYMBOL}",
    "profile": f"profile/{SYMBOL}",
    # Stretch (may require higher tiers depending on plan)
    "analyst_estimates": f"analyst-estimates/{SYMBOL}",
    "rating": f"rating/{SYMBOL}",
    "sec_filings": f"sec-filings/{SYMBOL}",
    "news": f"stock_news",
}


def probe(name: str, path: str) -> dict:
    url = f"{BASE}/{path}"
    params = {"apikey": API_KEY}
    # Calendar requires date range
    if name == "earning_calendar":
        today = dt.date.today()
        params.update({"from": (today - dt.timedelta(days=1)).isoformat(),
                       "to": (today + dt.timedelta(days=1)).isoformat()})
    if name == "stock_news":
        params.update({"tickers": SYMBOL, "limit": 5})
    t0 = time.time()
    try:
        r = requests.get(url, params=params, timeout=20)
        elapsed = time.time() - t0
        result = {
            "name": name,
            "url": url,
            "status": r.status_code,
            "ms": int(elapsed * 1000),
        }
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                data = None
            if isinstance(data, list):
                result["items"] = len(data)
                if data:
                    if isinstance(data[0], dict):
                        result["sample_keys"] = list(data[0].keys())[:6]
                    else:
                        result["sample"] = str(data[0])[:80]
            elif isinstance(data, dict):
                result["keys"] = list(data.keys())[:6]
            else:
                result["body_len"] = len(r.text)
        else:
            # Capture short error body for 4xx/5xx
            result["error"] = (r.text or "").strip()[:200]
        return result
    except Exception as e:
        return {"name": name, "url": url, "status": -1, "error": str(e)}


def main() -> None:
    if not API_KEY:
        print("FMP_API_KEY not set", file=sys.stderr)
        sys.exit(2)
    print(f"Probing FMP endpoints with symbol={SYMBOL}")
    names = list(ENDPOINTS.keys())
    for n in names:
        info = probe(n, ENDPOINTS[n])
        print(info)


if __name__ == "__main__":
    main()

