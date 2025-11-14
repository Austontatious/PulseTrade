from __future__ import annotations

import asyncio
from functools import partial

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from libs.llm.run_and_log import LLMRunner


router = APIRouter(prefix="/llm", tags=["llm"])
_runner = LLMRunner()


class ChatRequest(BaseModel):
    system: str = ""
    user: str
    temperature: float | None = None
    max_tokens: int | None = None


async def _runner_call(prompt_key: str, tpl_vars: dict[str, str], **kwargs):
    loop = asyncio.get_running_loop()
    func = partial(_runner.cached_call, prompt_key, tpl_vars, **kwargs)
    return await loop.run_in_executor(None, func)


@router.get("/health")
async def health() -> dict:
    try:
        result = await _runner_call("health", {}, max_tokens=5)
    except Exception as exc:  # pragma: no cover - network failure
        raise HTTPException(status_code=500, detail=str(exc))
    content = result.get("text", "")
    return {"ok": content.strip() == "OK", "cached": result.get("cached", False)}


@router.post("/chat")
async def chat(req: ChatRequest) -> dict:
    try:
        result = await _runner_call(
            "manual",
            {
                "system": req.system or "You are a helpful assistant.",
                "user": req.user,
            },
            temperature=req.temperature,
            max_tokens=req.max_tokens,
        )
    except Exception as exc:  # pragma: no cover
        raise HTTPException(status_code=500, detail=str(exc))
    return {
        "content": result.get("text", ""),
        "cached": result.get("cached", False),
        "prompt_version": result.get("prompt_version"),
    }
