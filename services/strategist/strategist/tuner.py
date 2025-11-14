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
MIN_MKT_CAP = float(os.getenv("STRAT_MIN_MARKET_CAP", str(10_000_000_000)))

async def tune_policy() -> None:
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        # Compute simple regime signal using daily momentum breadth and fundamentals revisions
        try:
            breadth_row = await conn.fetchrow(
                """
                WITH f_latest AS (
                  SELECT symbol, market_cap
                  FROM fundamentals_factors_v1
                  WHERE ds=(SELECT MAX(ds) FROM fundamentals_factors_v1)
                ), mom AS (
                  SELECT dr.symbol, SUM(dr.y) AS mom60
                  FROM daily_returns dr
                  WHERE dr.ds >= (CURRENT_DATE - INTERVAL '60 days')
                  GROUP BY dr.symbol
                )
                SELECT
                  AVG(CASE WHEN mom.mom60 > 0 THEN 1.0 ELSE 0.0 END) AS frac_pos
                FROM mom
                JOIN f_latest f USING (symbol)
                WHERE f.market_cap >= $1
                """,
                MIN_MKT_CAP,
            )
            frac_pos = float(breadth_row["frac_pos"]) if breadth_row and breadth_row["frac_pos"] is not None else 0.0
        except Exception:
            frac_pos = 0.0

        try:
            zrev_row = await conn.fetchrow(
                """
                SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY z_rev) AS med_zrev
                FROM fundamentals_factors_v1
                WHERE ds=(SELECT MAX(ds) FROM fundamentals_factors_v1)
                AND z_rev IS NOT NULL
                """
            )
            med_zrev = float(zrev_row["med_zrev"]) if zrev_row and zrev_row["med_zrev"] is not None else 0.0
        except Exception:
            med_zrev = 0.0

        risk_on = (frac_pos >= 0.55) and (med_zrev > 0.0)
        regime = "on" if risk_on else "off"
        now = dt.datetime.now(dt.timezone.utc)
        exp = now + dt.timedelta(minutes=KNOB_TTL_MIN)
        # Write regime knob
        await conn.execute(
            """
            INSERT INTO policy_knobs(ts, key, value, expires_at, meta)
            VALUES($1,$2,$3,$4,$5)
            """,
            now,
            "risk_regime",
            json.dumps({"value": regime}),
            exp,
            json.dumps({"frac_pos": frac_pos, "med_zrev": med_zrev}),
        )
        # Adjust entry threshold in bps by regime
        thresh_bps = 10 if risk_on else 20
        await conn.execute(
            """
            INSERT INTO policy_knobs(ts, key, value, expires_at, meta)
            VALUES($1,$2,$3,$4,$5)
            """,
            now,
            "threshold_bps",
            json.dumps({"value": thresh_bps}),
            exp,
            json.dumps({"source": "tuner", "regime": regime}),
        )
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
