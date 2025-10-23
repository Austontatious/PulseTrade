from __future__ import annotations

from typing import Dict

from pydantic import BaseModel


class ForecastResponse(BaseModel):
    symbol: str
    yhat: Dict[str, float]
    q: Dict[str, Dict[str, float]]
    meta: Dict[str, str]

