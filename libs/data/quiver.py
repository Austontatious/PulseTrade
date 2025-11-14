from __future__ import annotations

import os
from typing import Any, Dict, Optional

import httpx


BASE = os.getenv("QUIVER_API_BASE", "https://api.quiverquant.com")
TOKEN = os.getenv("QUIVER_API_TOKEN") or os.getenv("QUIVER_API_KEY", "")
TIMEOUT = float(os.getenv("QUIVER_TIMEOUT", "20"))


class Quiver:
    """Thin wrapper around the QuiverQuant REST API."""

    def __init__(self, *, base_url: Optional[str] = None, token: Optional[str] = None, timeout: Optional[float] = None) -> None:
        base = (base_url or BASE).rstrip("/")
        headers: Dict[str, str] = {}
        auth_token = token or TOKEN
        if auth_token:
            headers["Authorization"] = f"Token {auth_token}"
        self._client = httpx.Client(base_url=base, timeout=timeout or TIMEOUT, headers=headers)

    def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        resp = self._client.get(path, params=params)
        resp.raise_for_status()
        return resp.json()


qv = Quiver()
