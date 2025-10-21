import asyncio
import os
from . import setup
from .ranker import generate_recommendations
from .tuner import tune_policy

INTERVAL_RECOS = int(os.getenv("STRAT_RECO_SECS", "120"))
INTERVAL_TUNE  = int(os.getenv("STRAT_TUNE_SECS", "300"))

async def main() -> None:
    await setup.ensure_tables()
    async def loop_recos():
        while True:
            try:
                await generate_recommendations()
            except Exception as e:
                print("strategist recos error:", e)
            await asyncio.sleep(INTERVAL_RECOS)
    async def loop_tune():
        while True:
            try:
                await tune_policy()
            except Exception as e:
                print("strategist tune error:", e)
            await asyncio.sleep(INTERVAL_TUNE)
    await asyncio.gather(loop_recos(), loop_tune())

if __name__ == "__main__":
    asyncio.run(main())

