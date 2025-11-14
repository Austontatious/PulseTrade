from __future__ import annotations

from typing import Iterable, List

from libs.data.quiver import qv

from ..common import upsert_metrics
from ..util import parse_quiver_date, to_float


RECENT_ENDPOINT = "/beta/live/insiders"
HIST_ENDPOINT_TEMPLATE = "/beta/historical/insiders/{ticker}"


def _rows(items: Iterable[dict], *, src: str, ticker_hint: str | None = None) -> List[dict]:
    rows: List[dict] = []
    for payload in items or []:
        symbol = (payload.get("Ticker") or ticker_hint or "").upper()
        if not symbol:
            continue
        as_of_dt = parse_quiver_date(payload.get("Date"))
        if not as_of_dt:
            continue
        shares = to_float(payload.get("Shares"))
        price = to_float(payload.get("PricePerShare"))
        amount = shares * price
        code = (payload.get("AcquiredDisposedCode") or "").upper()
        if code.startswith("D"):
            amount *= -1
        rows.append(
            {
                "symbol": symbol,
                "as_of": as_of_dt.isoformat(),
                "metric": "quiver_insider_net_usd",
                "value": amount,
                "window": "1d",
                "src": src,
                "raw": payload,
            }
        )
    return rows


def update_recent(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    symset = {s.upper() for s in symbols}
    data = qv.get(RECENT_ENDPOINT, params={"limit": 2000})
    rows = [row for row in _rows(data, src="quiver:insiders_live") if row["symbol"] in symset]
    return upsert_metrics(rows)


def backfill(symbols: List[str], **kwargs) -> int:  # noqa: ANN001
    total = 0
    for sym in symbols:
        data = qv.get(HIST_ENDPOINT_TEMPLATE.format(ticker=sym))
        total += upsert_metrics(_rows(data, src="quiver:insiders_hist", ticker_hint=sym))
    return total
