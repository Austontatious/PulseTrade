from __future__ import annotations

import os
from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, to_float


HIST_ENDPOINT = os.getenv("QUIVER_POLITICAL_BETA_HIST_ENDPOINT", "/v1/historical/politicalbeta/{ticker}")
RECENT_ENDPOINT = os.getenv("QUIVER_POLITICAL_BETA_RECENT_ENDPOINT", "/v1/politicalbeta/{ticker}")


def _format_path(template: str, ticker: str) -> str:
    return template.format(ticker=ticker, symbol=ticker)


def _rows(ticker: str, items: Iterable[dict], *, src: str) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        as_of = parse_quiver_date(payload.get("Date"))
        if not as_of:
            continue
        beta_today = payload.get("BetaToday")
        beta_bulk = payload.get("Beta") or payload.get("BetaValue")
        if beta_today is not None:
            rows.append(
                {
                    "symbol": ticker,
                    "as_of": as_of,
                    "metric": "quiver_political_beta_today",
                    "value": to_float(beta_today),
                    "window": "1d",
                    "src": src,
                    "raw": payload,
                }
            )
        if beta_bulk is not None:
            rows.append(
                {
                    "symbol": ticker,
                    "as_of": as_of,
                    "metric": "quiver_political_beta_bulk",
                    "value": to_float(beta_bulk),
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
        total += upsert_metrics(_rows(sym, data, src="quiver:political_beta_hist"))
    return total


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    total = 0
    for sym in symbols:
        data = qv.get(_format_path(RECENT_ENDPOINT, sym))
        total += upsert_metrics(_rows(sym, data, src="quiver:political_beta_recent"))
    return total

