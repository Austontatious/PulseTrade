import asyncpg
import json
import os
import datetime as dt
from statistics import mean

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

LOOKBACK_MIN = int(os.getenv("STRAT_RECO_LOOKBACK_MIN", "30"))
MAX_RECO = int(os.getenv("STRAT_RECO_TOPK", "50"))

async def _universe(conn: asyncpg.Connection) -> list[str]:
    rows = await conn.fetch("SELECT ticker FROM symbols WHERE class='equity' ORDER BY ticker ASC LIMIT 1000")
    return [r[0] for r in rows]

async def _features(conn: asyncpg.Connection, ticker: str):
    # Simple momentum + recent sentiment mean
    rows = await conn.fetch(
        """
        SELECT ts, price FROM trades
        WHERE ticker=$1 AND ts > NOW() - make_interval(mins => $2::int)
        ORDER BY ts ASC
        LIMIT 500
        """,
        ticker,
        LOOKBACK_MIN,
    )
    if len(rows) < 5:
        return None
    p0 = float(rows[0]["price"]); p1 = float(rows[-1]["price"]) if rows else p0
    mom = (p1/p0) - 1.0 if p0 > 0 else 0.0
    srow = await conn.fetchrow(
        """
        SELECT senti_mean FROM social_features
        WHERE ticker=$1 ORDER BY ts DESC, window_minutes ASC LIMIT 1
        """,
        ticker,
    )
    smean = float(srow["senti_mean"]) if srow and srow["senti_mean"] is not None else 0.0
    return {"mom": mom, "senti": smean}

def _score(feat) -> float:
    # Blend momentum and sentiment; clamp to [-1,1]
    s = 0.7*feat["mom"] + 0.3*feat["senti"]
    if s > 1: s = 1
    if s < -1: s = -1
    return s

async def generate_recommendations() -> None:
    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        tickers = await _universe(conn)
        recos = []
        for t in tickers:
            feat = await _features(conn, t)
            if not feat:
                continue
            s = _score(feat)
            side = "buy" if s >= 0 else "sell"
            recos.append((t, side, s, feat))
        # Rank by absolute score and take top-k
        recos.sort(key=lambda r: -abs(r[2]))
        recos = recos[:MAX_RECO]
        now = dt.datetime.now(dt.timezone.utc)
        await conn.executemany(
            """
            INSERT INTO strategist_recos(ts, ticker, side, score, horizon, reason, meta)
            VALUES($1,$2,$3,$4,$5,$6,$7)
            """,
            [
                (
                    now,
                    t,
                    side,
                    float(score),
                    "15m",
                    "blend(mom,senti)",
                    json.dumps({"feat": feat}),
                )
                for (t, side, score, feat) in recos
            ],
        )
        print(f"strategist recos: wrote {len(recos)}")
    finally:
        await conn.close()
