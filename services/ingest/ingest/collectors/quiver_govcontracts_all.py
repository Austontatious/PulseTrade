from __future__ import annotations

from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, to_float


def _rows_live_all(items: Iterable[dict]) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        symbol = (payload.get("Ticker") or "").upper()
        if not symbol:
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
                "src": "quiver:gov_all_live",
                "raw": payload,
            }
        )
    return rows


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    data = qv.get("/beta/live/govcontractsall", params={"page_size": 2000})
    symset = {s.upper() for s in symbols}
    rows = [row for row in _rows_live_all(data) if row["symbol"] in symset]
    return upsert_metrics(rows)


def backfill(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    total = 0
    for sym in symbols:
        data = qv.get(f"/beta/historical/govcontractsall/{sym}")
        rows: List[dict] = []
        for payload in data or []:
            as_of_dt = parse_quiver_date(payload.get("Date") or payload.get("SigningDate"))
            if not as_of_dt:
                continue
            amount = to_float(payload.get("Amount") or payload.get("AmountUSD"))
            rows.append(
                {
                    "symbol": sym,
                    "as_of": as_of_dt.isoformat(),
                    "metric": "quiver_gov_award_usd",
                    "value": amount,
                    "window": "1d",
                    "src": "quiver:gov_all_hist",
                    "raw": payload,
                }
            )
        total += upsert_metrics(rows)
    return total
