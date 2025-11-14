import json
from typing import Any, Sequence

from libs.llm.client import LLMClient


def json_only_retry(llm: LLMClient, messages: Sequence[dict[str, str]], *, max_tokens: int = 512) -> str:
    """Ensure responses are valid JSON, retrying once with a stronger instruction."""

    raw = llm.chat(list(messages), max_tokens=max_tokens)
    if _is_json(raw):
        return raw

    rewritten = [messages[0]]
    rewritten.append(LLMClient.user("Return ONLY valid minified JSON, no commentary."))
    rewritten.extend(messages[1:])
    return llm.chat(rewritten, max_tokens=max_tokens)


def _is_json(payload: str) -> bool:
    try:
        json.loads(payload)
        return True
    except Exception:
        return False
