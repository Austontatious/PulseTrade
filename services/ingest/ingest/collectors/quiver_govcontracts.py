from __future__ import annotations

import os
from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, to_float


HIST_ENDPOINT = os.getenv("QUIVER_GOVCONTRACTS_HIST_ENDPOINT", "/beta/historical/govcontracts/{ticker}")
_recent_env = os.getenv("QUIVER_GOVCONTRACTS_RECENT_ENDPOINTS")
if _recent_env:
    RECENT_ENDPOINTS = [p.strip() for p in _recent_env.split(",") if p.strip()]
else:
    RECENT_ENDPOINTS = [
        "/beta/live/govcontracts",
        "/beta/recent/govcontracts",
        "/v1/live/govcontracts",
    ]


_QUARTER_END = {
    "Q1": (3, 31),
    "Q2": (6, 30),
    "Q3": (9, 30),
    "Q4": (12, 31),
}


def _format_path(template: str, ticker: str) -> str:
    return template.format(ticker=ticker, symbol=ticker)


def _rows_hist(items: Iterable[dict], sym: str) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        amount = to_float(payload.get("Amount"))
        year = int(float(payload.get("Year") or 0))
        if year <= 0:
            continue
        quarter = str(payload.get("Qtr") or "").upper()
        month, day = _QUARTER_END.get(quarter, (12, 31))
        as_of = f"{year:04d}-{month:02d}-{day:02d}"
        rows.append(
            {
                "symbol": sym,
                "as_of": as_of,
                "metric": "quiver_gov_award_usd",
                "value": amount,
                "window": quarter if quarter else "qtr",
                "src": "quiver:gov_hist",
                "raw": payload,
            }
        )
    return rows


def _rows_recent(items: Iterable[dict], symset: set[str]) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        symbol = (payload.get("Ticker") or "").upper()
        if symset and symbol not in symset:
            continue
        as_of_dt = parse_quiver_date(payload.get("Date") or payload.get("SigningDate"))
        if not as_of_dt:
            continue
        amount = to_float(payload.get("Amount") or payload.get("AmountUSD"))
        rows.append(
            {
                "symbol": symbol,
                "as_of": as_of_dt.isoformat(),
                "metric": "quiver_gov_award_usd",
                "value": amount,
                "window": "1d",
                "src": "quiver:gov_recent",
                "raw": payload,
            }
        )
    return rows


def backfill(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    total = 0
    for sym in symbols:
        data = qv.get(_format_path(HIST_ENDPOINT, sym))
        total += upsert_metrics(_rows_hist(data, sym))
    return total


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    symset = {s.upper() for s in symbols}
    for endpoint in RECENT_ENDPOINTS:
        try:
            data = qv.get(endpoint)
        except Exception:
            continue
        rows = _rows_recent(data, symset)
        if rows:
            return upsert_metrics(rows)
    return 0


def update_recent_quarter_totals(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    data = qv.get("/beta/live/govcontracts")
    symset = {s.upper() for s in symbols}
    rows: List[dict] = []
    for payload in data or []:
        sym = (payload.get("Ticker") or "").upper()
        if sym and symset and sym not in symset:
            continue
        year = int(float(payload.get("Year") or 0))
        if year <= 0:
            continue
        quarter = str(payload.get("Qtr") or "").upper()
        month, day = _QUARTER_END.get(quarter, (12, 31))
        as_of = f"{year:04d}-{month:02d}-{day:02d}"
        amount = to_float(payload.get("Amount"))
        rows.append(
            {
                "symbol": sym,
                "as_of": as_of,
                "metric": "quiver_gov_award_usd",
                "value": amount,
                "window": quarter if quarter else "qtr",
                "src": "quiver:gov_quarter_live",
                "raw": payload,
            }
        )
    return upsert_metrics(rows)
