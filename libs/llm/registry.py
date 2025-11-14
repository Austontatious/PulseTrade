from __future__ import annotations

import hashlib
import os
from dataclasses import dataclass
from typing import Any, Dict

import yaml


REG_PATH = os.getenv("LLM_PROMPT_REGISTRY", "configs/llm_prompts.yaml")


@dataclass(frozen=True)
class PromptSpec:
    key: str
    version: str
    system: str
    user_template: str


def _hash_text(txt: str) -> str:
    return hashlib.sha256(txt.encode("utf-8")).hexdigest()[:10]


class PromptRegistry:
    def __init__(self, path: str | None = None) -> None:
        with open(path or REG_PATH, "r", encoding="utf-8") as handle:
            self._cfg: Dict[str, Any] = yaml.safe_load(handle)

    def get(self, key: str, version: str | None = None) -> PromptSpec:
        entry = self._cfg[key]
        resolved = version or entry["version"]
        data = entry["versions"][resolved]
        return PromptSpec(
            key=key,
            version=resolved,
            system=data["system"],
            user_template=data["user_template"],
        )

    @staticmethod
    def prompt_hash(system: str, user: str) -> str:
        return _hash_text(f"{system}\n---\n{user}")
