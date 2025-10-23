from __future__ import annotations

from typing import Dict, List, Optional

import httpx

from .schemas import ForecastResponse

DEFAULT_TIMEOUT = 2.0


class ForecastClient:
    def __init__(self, base_url: str, timeout: float = DEFAULT_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def forecast(
        self,
        symbol: str,
        horizon: List[int],
        series: List[float],
        features: Optional[Dict] = None,
    ) -> ForecastResponse:
        payload = {
            "symbol": symbol,
            "horizon": horizon,
            "series": series,
            "features": features or {},
        }
        with httpx.Client(timeout=self.timeout) as client:
            resp = client.post(f"{self.base_url}/forecast", json=payload)
            resp.raise_for_status()
            return ForecastResponse.model_validate(resp.json())

