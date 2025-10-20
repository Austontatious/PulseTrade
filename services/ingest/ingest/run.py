import asyncio
import os
from .ws_coinbase import run_coinbase
from .ws_kraken import run_kraken
from .ws_alpaca import run_alpaca
from .alpaca_rest import run_alpaca_rest_poller
from .seeders import run_universe_seeders
from .collectors.finnhub import fetch_and_store as finnhub_fetch
from .collectors.fmp import fetch_targets as fmp_targets
from .collectors.fmp_screens import fetch_screens as fmp_screens
from .collectors.stocktwits_trending import fetch_trending_symbols as st_trending
from .collectors.stocktwits import fetch_symbol as stocktwits_fetch
from .collectors.truthsocial import fetch_latest as truthsocial_fetch
from .collectors.quiver import fetch_congress_trades as quiver_fetch
from .collectors.capitoltrades import fetch_latest as capitoltrades_fetch
from .symbols import run_symbol_discovery

TICKERS = [t.strip() for t in os.getenv("SYMBOLS", "AAPL,MSFT,BTCUSD,ETHUSD").split(",") if t.strip()]

async def poll_http_sources() -> None:
    interval = int(os.getenv("HTTP_POLL_SECS", "120"))
    while True:
        coros = []
        if os.getenv("ENABLE_FINNHUB", "0") == "1":
            coros.extend(finnhub_fetch(t) for t in TICKERS)
        if os.getenv("ENABLE_FMP", "0") == "1":
            coros.extend(fmp_targets(t) for t in TICKERS)
        if os.getenv("ENABLE_FMP_SCREENS", "1") == "1":
            coros.append(fmp_screens())
        if os.getenv("ENABLE_STOCKTWITS_TRENDING", "1") == "1":
            coros.append(st_trending())
        if os.getenv("ENABLE_STOCKTWITS", "0") == "1":
            symbols = [t for t in TICKERS if not t.endswith("USD")]
            coros.extend(stocktwits_fetch(sym) for sym in symbols)
        if os.getenv("ENABLE_TRUTHSOCIAL", "0") == "1":
            coros.append(truthsocial_fetch())
        if os.getenv("ENABLE_POLITICS", "0") == "1":
            symbols = [t for t in TICKERS if not t.endswith("USD")]
            coros.extend(quiver_fetch(sym) for sym in symbols)
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

if __name__ == "__main__":
    asyncio.run(main())
