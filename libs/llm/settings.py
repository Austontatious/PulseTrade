"""Shared LLM configuration helpers."""

from __future__ import annotations

import os


def is_llm_enabled() -> bool:
    """Return True when the Finance LLM layer should be invoked."""
    flag = os.getenv("ENABLE_LLM", "1").strip().lower()
    return flag not in {"0", "false", "no", "off"}

