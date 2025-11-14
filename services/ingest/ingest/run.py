import asyncio
import os
import sys
from .ws_coinbase import run_coinbase
from .ws_kraken import run_kraken
from .ws_alpaca import run_alpaca
from .alpaca_rest import run_alpaca_rest_poller
from .seeders import run_universe_seeders
from .collectors.finnhub import fetch_and_store as finnhub_fetch
from .collectors.fmp import fetch_targets as fmp_targets
from .collectors.fmp_estimates import fetch_estimates as fmp_estimates
from .collectors.fmp_rating import fetch_rating as fmp_rating
from .collectors.fmp_profile import fetch_profile as fmp_profile
from .collectors.fmp_screens import fetch_screens as fmp_screens
from .collectors.stocktwits_trending import fetch_trending_symbols as st_trending
from .collectors.fmp_calendar import fetch_earnings_calendar
from .collectors.fmp_news import fetch_news_for as fmp_news
from .collectors.stocktwits import fetch_symbol as stocktwits_fetch
from .collectors.truthsocial import fetch_latest as truthsocial_fetch
from .collectors.capitoltrades import fetch_latest as capitoltrades_fetch
from .symbols import run_symbol_discovery
from .quiver_tasks import quiver_backfill as quiver_backfill_sync, quiver_update as quiver_update_sync

TICKERS = [t.strip() for t in os.getenv("SYMBOLS", "AAPL,MSFT,BTCUSD,ETHUSD").split(",") if t.strip()]

async def poll_http_sources() -> None:
    interval = int(os.getenv("HTTP_POLL_SECS", "120"))
    while True:
        coros = []
        if os.getenv("ENABLE_FINNHUB", "0") == "1":
            coros.extend(finnhub_fetch(t) for t in TICKERS)
        if os.getenv("ENABLE_FMP", "0") == "1":
            coros.extend(fmp_targets(t) for t in TICKERS)
            coros.extend(fmp_estimates(t) for t in TICKERS if not t.endswith("USD"))
            coros.extend(fmp_rating(t) for t in TICKERS if not t.endswith("USD"))
            coros.extend(fmp_profile(t) for t in TICKERS if not t.endswith("USD"))
        if os.getenv("ENABLE_FMP_SCREENS", "1") == "1":
            coros.append(fmp_screens())
        if os.getenv("ENABLE_STOCKTWITS_TRENDING", "1") == "1":
            coros.append(st_trending())
        if os.getenv("ENABLE_EARNINGS_BLACKOUT", "1") == "1":
            coros.append(fetch_earnings_calendar())
        if os.getenv("ENABLE_FMP_NEWS", "1") == "1" and os.getenv("ENABLE_FMP", "0") == "1":
            eq_syms = [t for t in TICKERS if not t.endswith("USD")]
            # include ALPACA_SYMBOLS if present
            alp = [t.strip() for t in os.getenv("ALPACA_SYMBOLS", "").split(",") if t.strip()]
            syms = list(dict.fromkeys([*alp, *eq_syms]))[:100]
            if syms:
                coros.append(fmp_news(syms))
        if os.getenv("ENABLE_STOCKTWITS", "0") == "1":
            symbols = [t for t in TICKERS if not t.endswith("USD")]
            coros.extend(stocktwits_fetch(sym) for sym in symbols)
        if os.getenv("ENABLE_TRUTHSOCIAL", "0") == "1":
            coros.append(truthsocial_fetch())
        if os.getenv("ENABLE_POLITICS", "0") == "1":
            symbols = [t for t in TICKERS if not t.endswith("USD")]
            if symbols:
                coros.append(asyncio.to_thread(quiver_update_sync, symbols))
            coros.append(capitoltrades_fetch())
        if coros:
            await asyncio.gather(*coros, return_exceptions=True)
        await asyncio.sleep(interval)

async def main() -> None:
    tasks = []
    # Run universe seeders once on startup
    await run_universe_seeders()
    if os.getenv("ENABLE_COINBASE", "1") == "1":
        tasks.append(asyncio.create_task(run_coinbase()))
    if os.getenv("ENABLE_KRAKEN", "1") == "1":
        tasks.append(asyncio.create_task(run_kraken()))
    if os.getenv("ENABLE_ALPACA", "0") == "1":
        tasks.append(asyncio.create_task(run_alpaca()))
        if os.getenv("ENABLE_ALPACA_REST", "1") == "1":
            print("Starting Alpaca REST poller")
            tasks.append(asyncio.create_task(run_alpaca_rest_poller()))
    tasks.append(asyncio.create_task(poll_http_sources()))
    if os.getenv("ENABLE_SYMBOL_DISCOVERY", "1") == "1":
        tasks.append(asyncio.create_task(run_symbol_discovery()))
    await asyncio.gather(*tasks)

def _parse_symbols() -> list[str]:
    return [t.strip().upper() for t in os.getenv("SYMBOLS", "AAPL,MSFT,BTCUSD,ETHUSD").split(",") if t.strip()]


if __name__ == "__main__":
    if len(sys.argv) > 1:
        cmd = sys.argv[1]
        symbols = [s for s in _parse_symbols() if not s.endswith("USD")]
        if cmd == "quiver_backfill":
            total = quiver_backfill_sync(symbols)
            print(f"quiver_backfill rows={total}")
            sys.exit(0)
        if cmd == "quiver_update":
            total = quiver_update_sync(symbols)
            print(f"quiver_update rows={total}")
            sys.exit(0)
    asyncio.run(main())
