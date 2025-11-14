import os

import pytest

from libs.llm.client import LLMClient


def _llm_client() -> LLMClient:
    return LLMClient(base_url=os.getenv("LLM_BASE_URL", "http://localhost:8003/v1"))


@pytest.mark.integration
def test_llm_health_minimal():
    client = _llm_client()
    try:
        out = client.chat(
            [
                LLMClient.sys("You are a test."),
                LLMClient.user("Say OK"),
            ],
            temperature=0.0,
            max_tokens=5,
        )
    except Exception as exc:  # pragma: no cover - allows test to skip when service offline
        pytest.skip(f"LLM service unavailable: {exc}")
    assert "OK" in out.upper()
