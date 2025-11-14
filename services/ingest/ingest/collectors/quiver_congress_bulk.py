from __future__ import annotations

from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, to_float


def _rows(items: Iterable[dict], *, symset: set[str], src: str) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        symbol = (payload.get("Ticker") or "").upper()
        if symbol not in symset:
            continue
        as_of_dt = parse_quiver_date(payload.get("Traded") or payload.get("Filed") or payload.get("Date"))
        if not as_of_dt:
            continue
        amount = to_float(
            payload.get("Trade_Size_USD")
            or payload.get("TradeSizeUSD")
            or payload.get("AmountUSD")
            or payload.get("Amount")
        )
        transaction = (payload.get("Transaction") or "").lower()
        sign = 1.0 if transaction.startswith("buy") else -1.0 if transaction.startswith("sell") else 0.0
        if sign == 0.0 and amount == 0.0:
            continue
        rows.append(
            {
                "symbol": symbol,
                "as_of": as_of_dt.isoformat(),
                "metric": "quiver_congress_net_usd",
                "value": amount * sign,
                "window": "1d",
                "src": src,
                "raw": payload,
            }
        )
    return rows


def backfill(symbols: List[str], page_size: int = 5000, version: str = "V1", **kwargs) -> int:  # noqa: ANN001
    symset = {s.upper() for s in symbols}
    page = 1
    total = 0
    src = f"quiver:congress_bulk_{version.lower()}"
    while True:
        data = qv.get(
            "/beta/bulk/congresstrading",
            params={"page": page, "page_size": page_size, "version": version},
        )
        if not data:
            break
        rows = _rows(data, symset=symset, src=src)
        if rows:
            total += upsert_metrics(rows)
        if len(data) < page_size:
            break
        page += 1
    return total


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    return 0
