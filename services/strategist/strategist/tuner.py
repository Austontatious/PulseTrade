import asyncpg
import json
import os
import datetime as dt

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

WINDOW_MIN = int(os.getenv("STRAT_TUNE_WINDOW_MIN", "15"))
KNOB_TTL_MIN = int(os.getenv("STRAT_KNOB_TTL_MIN", "10"))

async def tune_policy() -> None:
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        # Simple heuristic: too many ALPACA orders -> raise z-thresh; too few -> lower
        rows = await conn.fetch(
            """
            SELECT venue, COUNT(*) AS n
            FROM fills
            WHERE ts > NOW() - make_interval(mins => $1::int)
            GROUP BY venue
            """,
            WINDOW_MIN,
        )
        counts = {r["venue"]: int(r["n"]) for r in rows}
        alp = counts.get("ALPACA", 0); sim = counts.get("SIM", 0)
        target = int(os.getenv("STRAT_TARGET_ALPACA_PER_WIN", "200"))
        z = None
        if alp > target * 1.5:
            z = 2.2
        elif alp < max(20, target // 4):
            z = 1.4
        if z is not None:
            now = dt.datetime.now(dt.timezone.utc)
            exp = now + dt.timedelta(minutes=KNOB_TTL_MIN)
            await conn.execute(
                """
                INSERT INTO policy_knobs(ts, key, value, expires_at, meta)
                VALUES($1,$2,$3,$4,$5)
                """,
                now,
                "PLANNER_Z_DEV_THRESH",
                json.dumps({"value": z}),
                exp,
                json.dumps({"source": "tuner", "alp": alp, "sim": sim}),
            )
            print("tuner: set PLANNER_Z_DEV_THRESH=", z)
    finally:
        await conn.close()
