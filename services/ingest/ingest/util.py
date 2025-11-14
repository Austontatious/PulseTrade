"""Shared ingest utilities for Quiver collectors."""

from __future__ import annotations

import datetime as dt
from typing import Optional


def parse_quiver_date(value: str | None) -> Optional[dt.date]:
    if not value:
        return None
    value = value.strip()
    if not value:
        return None
    value = value.split("T", 1)[0]
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"):
        try:
            return dt.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def to_float(value: object, default: float = 0.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace("$", "").replace(",", "").strip()
        cleaned = cleaned.replace("%", "")
        if not cleaned:
            return default
        try:
            return float(cleaned)
        except ValueError:
            return default
    return default


def sign_from_transaction(text: str | None) -> float:
    if not text:
        return 0.0
    lower = text.lower()
    if "buy" in lower:
        return 1.0
    if "sell" in lower:
        return -1.0
    return 0.0
