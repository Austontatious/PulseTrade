from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import httpx


class LLMClient:
    """Thin wrapper around an OpenAI-compatible chat completion endpoint."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout: float = 60.0,
        max_retries: int = 3,
    ) -> None:
        self.base_url = (base_url or os.getenv("LLM_BASE_URL", "http://llm:8003/v1")).rstrip("/")
        self.api_key = api_key or os.getenv("LLM_API_KEY", "local")
        self.model = model or os.getenv("LLM_MODEL", "finance-Llama3-8B")
        self.timeout = timeout
        self.max_retries = max_retries

    def chat(self, messages: List[Dict[str, str]], **kwargs: Any) -> str:
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": float(os.getenv("LLM_TEMPERATURE", "0.2")),
            "top_p": float(os.getenv("LLM_TOP_P", "0.9")),
            "max_tokens": int(os.getenv("LLM_MAX_OUTPUT_TOKENS", "1024")),
        }
        payload.update({k: v for k, v in kwargs.items() if v is not None})

        headers = {"Authorization": f"Bearer {self.api_key}"}
        url = f"{self.base_url}/chat/completions"

        last_exc: Optional[Exception] = None
        for attempt in range(self.max_retries):
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    resp = client.post(url, json=payload, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"]
            except Exception as exc:
                last_exc = exc
                if attempt == self.max_retries - 1:
                    raise
                time.sleep(0.5 * (attempt + 1))
        # Should never reach here due to raise above, but keeps type-checkers happy
        if last_exc:
            raise last_exc
        raise RuntimeError("LLMClient failed without exception context")

    @staticmethod
    def sys(text: str) -> Dict[str, str]:
        return {"role": "system", "content": text}

    @staticmethod
    def user(text: str) -> Dict[str, str]:
        return {"role": "user", "content": text}

    @staticmethod
    def asst(text: str) -> Dict[str, str]:
        return {"role": "assistant", "content": text}
