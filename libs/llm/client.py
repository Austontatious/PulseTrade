from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)


class LLMClient:
    """Thin wrapper around an OpenAI-compatible chat completion endpoint."""

    def __init__(
        self,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        model: Optional[str] = None,
        timeout: float = 60.0,
        max_retries: int = 3,
        backoff_factor: float = 1.0,
        max_backoff: float = 30.0,
    ) -> None:
        self.base_url = self._resolve_base_url(base_url)
        self.api_key = api_key or os.getenv("LLM_API_KEY", "local")
        self.model = model or os.getenv("LLM_MODEL", "Finance")
        self.timeout = timeout
        self.max_retries = max_retries
        self.backoff_factor = float(os.getenv("LLM_BACKOFF_FACTOR", str(backoff_factor)))
        self.max_backoff = float(os.getenv("LLM_MAX_BACKOFF", str(max_backoff)))

    @staticmethod
    def _resolve_base_url(explicit: Optional[str]) -> str:
        def _normalise(url: str) -> str:
            return url.rstrip("/")

        if explicit:
            return _normalise(explicit)
        env_url = os.getenv("LLM_BASE_URL")
        if env_url:
            return _normalise(env_url)

        host = os.getenv("VLLM_HOST")
        if host:
            scheme = os.getenv("VLLM_SCHEME", "http")
            base_path = os.getenv("VLLM_BASE_PATH", "/v1")
            port = os.getenv("VLLM_PORT") or os.getenv("LLM_PORT", "9009")
            if host.startswith("http://") or host.startswith("https://"):
                prefix = _normalise(host)
            else:
                prefix = f"{scheme}://{host}"
                if ":" not in host:
                    prefix = f"{prefix}:{port}"
            path = base_path if base_path.startswith("/") else f"/{base_path}"
            return f"{prefix}{path}"

        return "http://llm:9009/v1"

    @staticmethod
    def _validate_messages(messages: List[Dict[str, str]]) -> None:
        if not isinstance(messages, list) or not messages:
            raise ValueError("messages must be a non-empty list")
        for msg in messages:
            if not isinstance(msg, dict) or "role" not in msg or "content" not in msg:
                raise ValueError("each message must include 'role' and 'content'")

    def chat(self, messages: List[Dict[str, str]], **kwargs: Any) -> str:
        self._validate_messages(messages)
        payload: Dict[str, Any] = {
            "model": self.model,
            "messages": messages,
            "temperature": float(os.getenv("LLM_TEMPERATURE", "0.2")),
            "top_p": float(os.getenv("LLM_TOP_P", "0.9")),
            "max_tokens": int(os.getenv("LLM_MAX_OUTPUT_TOKENS", "1024")),
        }
        payload.update({k: v for k, v in kwargs.items() if v is not None})

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        url = f"{self.base_url}/chat/completions"

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                with httpx.Client(timeout=self.timeout) as client:
                    resp = client.post(url, json=payload, headers=headers)
                resp.raise_for_status()
                data = resp.json()
                return data["choices"][0]["message"]["content"]
            except httpx.HTTPStatusError as exc:
                last_exc = exc
                status = exc.response.status_code
                retryable = status in (429, 500, 502, 503, 504)
                if not retryable or attempt == self.max_retries:
                    raise
                sleep_s = min(self.backoff_factor * (2 ** (attempt - 1)), self.max_backoff)
                logger.warning(
                    "LLM HTTP %s, retrying in %.1fs (attempt %s/%s)",
                    status,
                    sleep_s,
                    attempt,
                    self.max_retries,
                )
                time.sleep(sleep_s)
            except httpx.RequestError as exc:
                last_exc = exc
                if attempt == self.max_retries:
                    raise
                sleep_s = min(self.backoff_factor * (2 ** (attempt - 1)), self.max_backoff)
                logger.warning(
                    "LLM request error %s, retrying in %.1fs (attempt %s/%s)",
                    exc,
                    sleep_s,
                    attempt,
                    self.max_retries,
                )
                time.sleep(sleep_s)
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
