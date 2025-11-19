from __future__ import annotations

import asyncio
from functools import partial
from datetime import date
from typing import Dict, Optional

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from libs.llm.reviews import LLMReviewRecord, LLMReviewStore
from libs.llm.run_and_log import LLMRunner


router = APIRouter(prefix="/llm", tags=["llm"])
_runner = LLMRunner()
_review_store = LLMReviewStore()


class ChatRequest(BaseModel):
    system: str = ""
    user: str
    temperature: float | None = None
    max_tokens: int | None = None


async def _runner_call(prompt_key: str, tpl_vars: dict[str, str], **kwargs):
    loop = asyncio.get_running_loop()
    func = partial(_runner.cached_call, prompt_key, tpl_vars, **kwargs)
    return await loop.run_in_executor(None, func)


def _serialize_review(record: LLMReviewRecord) -> dict:
    return {
        "symbol": record.symbol,
        "scope": record.scope,
        "as_of": record.as_of.isoformat(),
        "prompt_key": record.prompt_key,
        "prompt_version": record.prompt_version,
        "prompt_hash": record.prompt_hash,
        "decision": record.output_json,
        "text": record.output_text,
        "cached": record.cached,
        "input": record.input_payload,
        "extra": record.extra,
        "created_at": record.created_at.isoformat() if record.created_at else None,
        "updated_at": record.updated_at.isoformat() if record.updated_at else None,
    }


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


@router.get("/reviews")
async def list_reviews(
    scope: str = Query(..., description="LLM review scope, e.g., weekly_deep_dive"),
    as_of: Optional[date] = Query(None, description="As-of date (YYYY-MM-DD). Defaults to today."),
    symbol: Optional[str] = Query(None, description="Optional ticker filter"),
) -> dict:
    target_date = as_of or date.today()
    symbols = [symbol.upper()] if symbol else None
    loop = asyncio.get_running_loop()
    func = partial(_review_store.fetch_scope_for_date, scope=scope, as_of=target_date, symbols=symbols)
    records: Dict[str, LLMReviewRecord] = await loop.run_in_executor(None, func)
    data = [_serialize_review(rec) for rec in records.values()]
    return {
        "scope": scope,
        "as_of": target_date.isoformat(),
        "count": len(data),
        "results": data,
    }
