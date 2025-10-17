import asyncio
import datetime as dt
import json
import os
from collections import defaultdict
from statistics import mean, pstdev

import asyncpg
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

DB_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER', 'pulse')}:{os.getenv('POSTGRES_PASSWORD', 'pulsepass')}@"
    f"{os.getenv('POSTGRES_HOST', 'db')}:{os.getenv('POSTGRES_PORT', '5432')}/{os.getenv('POSTGRES_DB', 'pulse')}"
)

WINDOWS = (5, 15, 60)
BATCH_SIZE = int(os.getenv("SENTIMENT_BATCH_SIZE", "200"))
SLEEP_SECS = int(os.getenv("SENTIMENT_SLEEP_SECS", "60"))

_analyzer = SentimentIntensityAnalyzer()

async def _score_batch(conn: asyncpg.Connection) -> int:
    rows = await conn.fetch(
        """
        SELECT ts, source, handle, ticker, text
        FROM social_messages
        WHERE sentiment IS NULL AND text IS NOT NULL
        ORDER BY ts ASC
        LIMIT $1
        """,
        BATCH_SIZE,
    )
    if not rows:
        return 0
    for row in rows:
        score = _analyzer.polarity_scores(row["text"])
        compound = float(score["compound"])
        await conn.execute(
            """
            UPDATE social_messages
            SET sentiment = $1
            WHERE ts = $2 AND source = $3 AND (handle IS NOT DISTINCT FROM $4)
              AND (ticker IS NOT DISTINCT FROM $5) AND text = $6 AND sentiment IS NULL
            """,
            compound,
            row["ts"],
            row["source"],
            row["handle"],
            row["ticker"],
            row["text"],
        )
    return len(rows)

async def _aggregate_sentiment(conn: asyncpg.Connection) -> None:
    rows = await conn.fetch(
        """
        SELECT ts, ticker, handle, sentiment
        FROM social_messages
        WHERE ts > NOW() - INTERVAL '60 minutes' AND sentiment IS NOT NULL
        """
    )
    if not rows:
        return
    now = dt.datetime.now(dt.timezone.utc)
    cutoffs = {window: now - dt.timedelta(minutes=window) for window in WINDOWS}

    values = {window: defaultdict(list) for window in WINDOWS}
    handles = {window: defaultdict(lambda: defaultdict(list)) for window in WINDOWS}

    for row in rows:
        ts = row["ts"]
        ticker = row["ticker"]
        handle = row["handle"]
        sentiment = row["sentiment"]
        for window, cutoff in cutoffs.items():
            if ts >= cutoff:
                values[window][ticker].append(sentiment)
                if handle:
                    handles[window][ticker][handle].append(sentiment)

    insert_rows = []
    for window in WINDOWS:
        for ticker, sentiments in values[window].items():
            if not sentiments:
                continue
            rate = len(sentiments) / window
            senti_mean = mean(sentiments)
            senti_std = pstdev(sentiments) if len(sentiments) > 1 else 0.0
            handle_stats = []
            for handle, vals in handles[window][ticker].items():
                if not vals:
                    continue
                handle_stats.append(
                    {
                        "handle": handle,
                        "avg": mean(vals),
                        "count": len(vals),
                    }
                )
            handle_stats.sort(key=lambda h: (-(h["count"]), -abs(h["avg"])))
            top_handles = json.dumps(handle_stats[:3])
            insert_rows.append(
                (
                    now,
                    ticker,
                    window,
                    rate,
                    senti_mean,
                    senti_std,
                    top_handles,
                )
            )
    if insert_rows:
        await conn.executemany(
            """
            INSERT INTO social_features(ts, ticker, window_minutes, msg_rate, senti_mean, senti_std, top_handles)
            VALUES($1,$2,$3,$4,$5,$6,$7)
            ON CONFLICT (ts, ticker, window_minutes)
            DO UPDATE SET
              msg_rate = EXCLUDED.msg_rate,
              senti_mean = EXCLUDED.senti_mean,
              senti_std = EXCLUDED.senti_std,
              top_handles = EXCLUDED.top_handles
            """,
            insert_rows,
        )

async def run_sentiment_loop() -> None:
    while True:
        try:
            conn = await asyncpg.connect(dsn=DB_DSN)
            try:
                updated = await _score_batch(conn)
                if updated:
                    await _aggregate_sentiment(conn)
                else:
                    # still refresh aggregates periodically
                    await _aggregate_sentiment(conn)
            finally:
                await conn.close()
        except Exception as exc:  # pragma: no cover - log and continue
            print("sentiment loop error:", exc)
        await asyncio.sleep(SLEEP_SECS)
