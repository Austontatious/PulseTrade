from __future__ import annotations

import os
from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, to_float


HIST_ENDPOINT = os.getenv("QUIVER_13F_HIST_ENDPOINT", "/v1/historical/13f/{ticker}")
RECENT_ENDPOINT = os.getenv("QUIVER_13F_RECENT_ENDPOINT", "/v1/13f/{ticker}")


def _format_path(template: str, ticker: str) -> str:
    return template.format(ticker=ticker, symbol=ticker)


def _rows(ticker: str, items: Iterable[dict], *, src: str) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        as_of = parse_quiver_date(payload.get("FilingDate") or payload.get("Date"))
        if not as_of:
            continue
        shares = to_float(payload.get("Shares"))
        delta = to_float(payload.get("ChangeShares") or payload.get("SharesChange"))
        rows.append(
            {
                "symbol": ticker,
                "as_of": as_of,
                "metric": "quiver_13f_shares",
                "value": shares,
                "window": "1d",
                "src": src,
                "raw": payload,
            }
        )
        rows.append(
            {
                "symbol": ticker,
                "as_of": as_of,
                "metric": "quiver_13f_delta_shares",
                "value": delta,
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
        total += upsert_metrics(_rows(sym, data, src="quiver:13f_hist"))
    return total


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    total = 0
    for sym in symbols:
        data = qv.get(_format_path(RECENT_ENDPOINT, sym))
        total += upsert_metrics(_rows(sym, data, src="quiver:13f_recent"))
    return total

