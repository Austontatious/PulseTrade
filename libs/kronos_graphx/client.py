import httpx


class GraphXClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 5.0):
        self.base = base_url.rstrip("/")
        self.headers = {"X-API-Key": api_key}
        self.timeout = timeout

    def weights(self, window):
        payload = {"window": window}
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(f"{self.base}/graph_weights", headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()
