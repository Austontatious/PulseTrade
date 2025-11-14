from __future__ import annotations

import os
from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, to_float


HIST_ENDPOINT = os.getenv("QUIVER_PATENTS_HIST_ENDPOINT", "/v1/historical/patents/{ticker}")
RECENT_ENDPOINT = os.getenv("QUIVER_PATENTS_RECENT_ENDPOINT", "/v1/patents/{ticker}")


METRIC_MAP = {
    "Patents": "quiver_patents_grants",
    "GrantCount": "quiver_patents_grants",
    "Momentum": "quiver_patent_momentum",
    "MomentumScore": "quiver_patent_momentum",
    "Drift": "quiver_patent_drift",
    "DriftScore": "quiver_patent_drift",
}


def _format_path(template: str, ticker: str) -> str:
    return template.format(ticker=ticker, symbol=ticker)


def _rows(ticker: str, items: Iterable[dict], *, src: str) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        as_of = parse_quiver_date(payload.get("Date"))
        if not as_of:
            continue
        for key, metric in METRIC_MAP.items():
            if key not in payload:
                continue
            rows.append(
                {
                    "symbol": ticker,
                    "as_of": as_of,
                    "metric": metric,
                    "value": to_float(payload.get(key)),
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
        total += upsert_metrics(_rows(sym, data, src="quiver:patents_hist"))
    return total


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    total = 0
    for sym in symbols:
        data = qv.get(_format_path(RECENT_ENDPOINT, sym))
        total += upsert_metrics(_rows(sym, data, src="quiver:patents_recent"))
    return total

