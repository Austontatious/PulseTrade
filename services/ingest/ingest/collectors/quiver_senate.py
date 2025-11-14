from __future__ import annotations

import os
from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, to_float

INCLUDE_OPTIONS = os.getenv("QUIVER_INCLUDE_OPTIONS", "false").lower() == "true"


def _rows_hist(sym: str, items: Iterable[dict]) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        as_of_dt = parse_quiver_date(payload.get("Date") or payload.get("ReportDate"))
        if not as_of_dt:
            continue
        tx = (payload.get("Transaction") or "").lower()
        amount = to_float(payload.get("Amount"))
        sign = 1.0 if tx.startswith("buy") else -1.0 if tx.startswith("sell") else 0.0
        rows.append(
            {
                "symbol": sym,
                "as_of": as_of_dt.isoformat(),
                "metric": "quiver_senate_net_usd",
                "value": sign * amount,
                "window": "1d",
                "src": "quiver:senate_hist",
                "raw": payload,
            }
        )
    return rows


def backfill(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    total = 0
    for sym in symbols:
        data = qv.get(f"/beta/historical/senatetrading/{sym}")
        total += upsert_metrics(_rows_hist(sym, data))
    return total


def _rows_live(items: Iterable[dict]) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        sym = (payload.get("Ticker") or "").upper()
        if not sym:
            continue
        trade_type = (payload.get("Type") or "Stock").lower()
        if not INCLUDE_OPTIONS and trade_type.startswith("stock option"):
            continue
        as_of_dt = parse_quiver_date(payload.get("Date") or payload.get("ReportDate"))
        if not as_of_dt:
            continue
        tx = (payload.get("Transaction") or "").lower()
        amount = to_float(payload.get("Amount"))
        sign = 1.0 if tx.startswith("buy") else -1.0 if tx.startswith("sell") else 0.0
        rows.append(
            {
                "symbol": sym,
                "as_of": as_of_dt.isoformat(),
                "metric": "quiver_senate_net_usd",
                "value": sign * amount,
                "window": "1d",
                "src": "quiver:senate_live",
                "raw": payload,
            }
        )
    return rows


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    data = qv.get(
        "/beta/live/senatetrading",
        params={"options": str(INCLUDE_OPTIONS).lower(), "limit": 2000},
    )
    symset = {s.upper() for s in symbols}
    rows = [row for row in _rows_live(data) if row["symbol"] in symset]
    return upsert_metrics(rows)
