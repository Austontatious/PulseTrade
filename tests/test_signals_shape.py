import pytest
import requests


pytestmark = pytest.mark.integration


def test_signals_has_llm_fields():
    try:
        resp = requests.get("http://localhost:8000/signals/latest", timeout=10)
    except requests.RequestException as exc:  # pragma: no cover
        pytest.skip(f"signals endpoint unavailable: {exc}")

    if resp.status_code != 200:
        pytest.skip(f"unexpected status {resp.status_code}")

    data = resp.json()
    if not data:
        pytest.skip("no signals available")

    sample = data[0]
    features = sample.get("features") or {}
    llm = features.get("llm") or {}
    rationale = llm.get("rationale") or {}
    policy = llm.get("policy") or {}
    assert "text" in rationale or "raw" in policy
