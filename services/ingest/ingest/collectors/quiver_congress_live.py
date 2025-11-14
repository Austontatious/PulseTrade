from __future__ import annotations

import os
from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, sign_from_transaction, to_float


LIVE_ENDPOINT = os.getenv("QUIVER_CONGRESS_LIVE_ENDPOINT", "/v1/live/congresstrading")


def _rows(items: Iterable[dict], symset: set[str]) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        sym = (payload.get("Ticker") or payload.get("ticker") or "").upper()
        if sym not in symset:
            continue
        as_of = parse_quiver_date(payload.get("TransactionDate") or payload.get("Date"))
        if not as_of:
            continue
        amt = to_float(payload.get("TransactionAmountUSD") or payload.get("Amount"))
        sign = sign_from_transaction(payload.get("Transaction"))
        value = amt * sign if sign != 0 else amt
        rows.append(
            {
                "symbol": sym,
                "as_of": as_of,
                "metric": "quiver_congress_net_usd",
                "value": value,
                "window": "1d",
                "src": "quiver:congress_live",
                "raw": payload,
            }
        )
    return rows


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    data = qv.get(LIVE_ENDPOINT)
    symset = {s.upper() for s in symbols}
    return upsert_metrics(_rows(data, symset))


def backfill(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    return 0

