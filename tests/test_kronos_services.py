import time
from typing import Callable

import pytest

requests = pytest.importorskip("requests")


def _wait_healthy(url: str, tries: int = 5, sleep: float = 0.5) -> bool:
    for _ in range(tries):
        try:
            response = requests.get(url, timeout=0.5)
            if response.ok and response.json().get("ok"):
                return True
        except requests.RequestException:
            pass
        time.sleep(sleep)
    return False


def _skip_if_unavailable(check: Callable[[], bool], reason: str) -> None:
    if not check():
        pytest.skip(reason)


def test_health_kronos_nbeats():
    _skip_if_unavailable(lambda: _wait_healthy("http://localhost:8080/health"), "kronos-nbeats not running locally")


def test_health_kronos_graph():
    _skip_if_unavailable(lambda: _wait_healthy("http://localhost:8081/health"), "kronos-graph not running locally")


def test_forecast_kronos_nbeats():
    _skip_if_unavailable(lambda: _wait_healthy("http://localhost:8080/health"), "kronos-nbeats not running locally")
    payload = {
        "symbol": "AAPL",
        "horizon": [1, 5],
        "series": [0.0] * 64 + [0.001, -0.002, 0.0005, 0.003, -0.001],
    }
    response = requests.post("http://localhost:8080/forecast", json=payload, timeout=10)
    response.raise_for_status()
    data = response.json()
    assert "yhat" in data and "q" in data


def test_forecast_kronos_graph():
    _skip_if_unavailable(lambda: _wait_healthy("http://localhost:8081/health"), "kronos-graph not running locally")
    payload = {
        "symbol": "AAPL",
        "horizon": [1, 5],
        "series": [100.0] * 100,
    }
    response = requests.post("http://localhost:8081/forecast", json=payload, timeout=5)
    response.raise_for_status()
    data = response.json()
    assert "yhat" in data and "q" in data
