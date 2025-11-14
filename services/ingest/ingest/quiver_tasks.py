from __future__ import annotations

import os
from typing import Iterable, Sequence

from .collectors import (
    quiver_13f,
    quiver_appratings,
    quiver_congress,
    quiver_congress_bulk,
    quiver_etf,
    quiver_govcontracts,
    quiver_govcontracts_all,
    quiver_house,
    quiver_insiders,
    quiver_lobbying,
    quiver_offexchange,
    quiver_patents,
    quiver_political_beta,
    quiver_senate,
)


PUBLIC_EXTRAS = os.getenv("QUIVER_ENABLE_PUBLIC_EXTRAS", "false").lower() == "true"


CORE_BACKFILL = [
    quiver_congress,
    quiver_lobbying,
    quiver_govcontracts,
    quiver_offexchange,
    quiver_house,
    quiver_senate,
    quiver_govcontracts_all,
]

CORE_UPDATE = [
    quiver_congress,
    quiver_lobbying,
    quiver_offexchange,
    quiver_house,
    quiver_senate,
    quiver_govcontracts_all,
]

EXTRA_COLLECTORS = [
    quiver_insiders,
    quiver_13f,
    quiver_etf,
    quiver_appratings,
    quiver_patents,
    quiver_political_beta,
]


def _run_collectors(symbols: Sequence[str], collectors: Iterable, method: str) -> int:
    total = 0
    for collector in collectors:
        fn = getattr(collector, method)
        total += fn(list(symbols))
    return total


def quiver_backfill(symbols: Sequence[str]) -> int:
    symbols = list({s.upper() for s in symbols})
    total = quiver_congress_bulk.backfill(symbols)
    total += _run_collectors(symbols, CORE_BACKFILL, "backfill")
    if PUBLIC_EXTRAS:
        total += _run_collectors(symbols, EXTRA_COLLECTORS, "backfill")
    return total


def quiver_update(symbols: Sequence[str]) -> int:
    symbols = list({s.upper() for s in symbols})
    total = _run_collectors(symbols, CORE_UPDATE, "update_recent")
    total += quiver_govcontracts.update_recent_quarter_totals(symbols)
    if PUBLIC_EXTRAS:
        total += _run_collectors(symbols, EXTRA_COLLECTORS, "update_recent")
    return total
