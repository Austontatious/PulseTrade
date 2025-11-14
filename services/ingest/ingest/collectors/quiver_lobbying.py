from __future__ import annotations

import os
from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, to_float


HIST_ENDPOINT = os.getenv("QUIVER_LOBBYING_HIST_ENDPOINT", "/v1/historical/lobbying/{ticker}")
RECENT_ENDPOINT = os.getenv("QUIVER_LOBBYING_RECENT_ENDPOINT", "/v1/live/lobbying")


def _format_path(template: str, ticker: str) -> str:
    return template.format(ticker=ticker, symbol=ticker)


def _rows(items: Iterable[dict], *, src: str, ticker_hint: str | None = None) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        symbol = (payload.get("Ticker") or ticker_hint or "").upper()
        if not symbol:
            continue
        as_of_dt = parse_quiver_date(payload.get("Date") or payload.get("FilingDate"))
        if not as_of_dt:
            continue
        amount = to_float(payload.get("Amount") or payload.get("AmountUSD"))
        rows.append(
            {
                "symbol": symbol,
                "as_of": as_of_dt.isoformat(),
                "metric": "quiver_lobbying_spend_usd",
                "value": amount,
                "window": "1d",
                "src": src,
                "raw": payload,
            }
        )
    return rows


def backfill(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    total = 0
    for sym in symbols:
        data = qv.get(_format_path(HIST_ENDPOINT, sym))
        total += upsert_metrics(_rows(data, src="quiver:lobbying_hist", ticker_hint=sym))
    return total


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    symset = {s.upper() for s in symbols}
    data = qv.get(RECENT_ENDPOINT, params={"limit": 2000})
    rows = [row for row in _rows(data, src="quiver:lobbying_recent") if row["symbol"] in symset]
    return upsert_metrics(rows)
