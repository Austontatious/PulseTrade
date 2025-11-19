from __future__ import annotations

import os
import json
from typing import List, Tuple

import asyncpg
import httpx

from ..config import DB_DSN

ALPACA_KEY = os.getenv("ALPACA_API_KEY_ID")
ALPACA_SECRET = os.getenv("ALPACA_API_SECRET_KEY")
ALPACA_BASE = os.getenv("ALPACA_PAPER_BASE_URL", "https://paper-api.alpaca.markets").rstrip("/")


def _assets_url() -> str:
    base = (ALPACA_BASE or "https://paper-api.alpaca.markets").rstrip("/")
    if base.endswith("/v2"):
        return f"{base}/assets"
    return f"{base}/v2/assets"


async def refresh_shortable_flags(status: str = "active") -> int:
    """Fetch Alpaca asset metadata and persist shortability flags."""
    if not ALPACA_KEY or not ALPACA_SECRET:
        return 0

    headers = {
        "APCA-API-KEY-ID": ALPACA_KEY,
        "APCA-API-SECRET-KEY": ALPACA_SECRET,
    }
    params = {
        "status": status,
        "asset_class": "us_equity",
    }
    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(_assets_url(), headers=headers, params=params)
        resp.raise_for_status()
        data = resp.json()

    if not isinstance(data, list):
        return 0

    rows: List[Tuple[str, bool, bool]] = []
    symbol_rows: List[Tuple[str, str, str, str]] = []
    for item in data:
        symbol = (item.get("symbol") or "").upper()
        if not symbol:
            continue
        shortable = bool(item.get("shortable"))
        easy_to_borrow = bool(item.get("easy_to_borrow"))
        rows.append((symbol, shortable, easy_to_borrow))

        venue = item.get("exchange") or item.get("primary_exchange") or "ALPACA"
        meta = {
            "alpaca": {
                "asset_id": item.get("id"),
                "status": item.get("status"),
                "class": item.get("asset_class"),
                "exchange": venue,
                "fractionable": item.get("fractionable"),
                "marginable": item.get("marginable"),
            }
        }
        symbol_rows.append(
            (
                symbol,
                "equity",
                venue or "ALPACA",
                json.dumps(meta),
            )
        )

    if not rows:
        return 0

    sql = """
        INSERT INTO dim_company_profile(symbol, meta, shortable, easy_to_borrow)
        VALUES (
            $1,
            jsonb_build_object(
                'alpaca',
                jsonb_build_object('shortable', to_jsonb($2::boolean), 'easy_to_borrow', to_jsonb($3::boolean))
            ),
            $2,
            $3
        )
        ON CONFLICT(symbol) DO UPDATE SET
            shortable = EXCLUDED.shortable,
            easy_to_borrow = EXCLUDED.easy_to_borrow,
            meta = jsonb_set(
                jsonb_set(COALESCE(dim_company_profile.meta, '{}'::jsonb), '{alpaca,shortable}', to_jsonb(EXCLUDED.shortable), true),
                '{alpaca,easy_to_borrow}', to_jsonb(EXCLUDED.easy_to_borrow), true
            );
    """

    conn = await asyncpg.connect(dsn=DB_DSN)
    try:
        async with conn.transaction():
            await conn.executemany(sql, rows)
            if symbol_rows:
                await conn.executemany(
                    """
                    INSERT INTO symbols(ticker, class, venue, meta)
                    VALUES($1,$2,$3, COALESCE($4::jsonb, '{}'::jsonb))
                    ON CONFLICT (ticker, class)
                    DO UPDATE SET
                        venue = EXCLUDED.venue,
                        meta = COALESCE(symbols.meta, '{}'::jsonb) || EXCLUDED.meta
                    """,
                    symbol_rows,
                )
    finally:
        await conn.close()

    return len(rows)
