import httpx


class ScenariosClient:
    def __init__(self, base_url: str, api_key: str, timeout: float = 5.0):
        self.base = base_url.rstrip("/")
        self.headers = {"X-API-Key": api_key}
        self.timeout = timeout

    def sample(
        self,
        symbols,
        context_days: int = 60,
        forecast_horizon: int = 20,
        num_paths: int = 5000,
        conditioning=None,
    ):
        payload = {
            "symbols": symbols,
            "context_days": context_days,
            "forecast_horizon": forecast_horizon,
            "num_paths": num_paths,
            "conditioning": conditioning or {},
        }
        with httpx.Client(timeout=self.timeout) as client:
            response = client.post(f"{self.base}/sample", headers=self.headers, json=payload)
            response.raise_for_status()
            return response.json()
