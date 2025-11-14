from __future__ import annotations

import os
from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, sign_from_transaction, to_float


HIST_ENDPOINT = os.getenv("QUIVER_CONGRESS_HIST_ENDPOINT", "/v1/historical/congresstrading/{ticker}")
RECENT_ENDPOINT = os.getenv("QUIVER_CONGRESS_RECENT_ENDPOINT", "/v1/live/congresstrading")


def _format_path(template: str, ticker: str) -> str:
    return template.format(ticker=ticker, symbol=ticker)


def _rows_from_congress_json(items: Iterable[dict], *, src: str, ticker_hint: str | None = None) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        symbol = (payload.get("Ticker") or ticker_hint or "").upper()
        if not symbol:
            continue
        as_of_dt = parse_quiver_date(payload.get("TransactionDate") or payload.get("ReportDate"))
        if not as_of_dt:
            continue
        amount = to_float(payload.get("TransactionAmountUSD") or payload.get("Amount"))
        sign = sign_from_transaction(payload.get("Transaction"))
        value = amount * sign if sign != 0 else amount
        rows.append(
            {
                "symbol": symbol,
                "as_of": as_of_dt.isoformat(),
                "metric": "quiver_congress_net_usd",
                "value": value,
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
        total += upsert_metrics(_rows_from_congress_json(data, src="quiver:congress_hist", ticker_hint=sym))
    return total


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    data = qv.get(RECENT_ENDPOINT, params={"limit": 2000})
    symset = {s.upper() for s in symbols}
    rows = [row for row in _rows_from_congress_json(data, src="quiver:congress_recent") if row["symbol"] in symset]
    return upsert_metrics(rows)
