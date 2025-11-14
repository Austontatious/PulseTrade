from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, Optional

import psycopg2
import psycopg2.extras

from libs.llm.client import LLMClient
from libs.llm.guardrails import json_only_retry
from libs.llm.metrics import llm_calls, llm_latency
from libs.llm.registry import PromptRegistry
from libs.llm.scrub import redact


PG_DSN = os.getenv("PG_DSN", "postgresql://pulse:pulsepass@db:5432/pulse")


def _sanitize(obj: Any) -> Any:
    if isinstance(obj, str):
        return redact(obj)
    if isinstance(obj, dict):
        return {k: _sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize(v) for v in obj]
    return obj


class LLMRunner:
    def __init__(self) -> None:
        self.llm = LLMClient()
        self.registry = PromptRegistry()

    def _conn(self):
        return psycopg2.connect(PG_DSN)

    def cached_call(
        self,
        prompt_key: str,
        tpl_vars: Dict[str, Any],
        *,
        version: str | None = None,
        schema: Optional[Dict[str, Any]] = None,
        temperature: float | None = None,
        max_tokens: Optional[int] = None,
        use_cache: bool = True,
    ) -> Dict[str, Any]:
        spec = self.registry.get(prompt_key, version)
        user = spec.user_template.format(**tpl_vars)
        prompt_hash = self.registry.prompt_hash(spec.system, user)

        if use_cache:
            with self._conn() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT id, output_text, output_json
                        FROM llm_calls
                        WHERE prompt_hash=%s AND success=TRUE
                        ORDER BY ts DESC
                        LIMIT 1
                        """,
                        (prompt_hash,),
                    )
                    cached_row = cur.fetchone()
            if cached_row:
                return {
                    "text": cached_row["output_text"],
                    "json": cached_row.get("output_json"),
                    "cached": True,
                    "prompt_key": spec.key,
                    "prompt_version": spec.version,
                    "prompt_hash": prompt_hash,
                }

        messages = [
            LLMClient.sys(spec.system),
            LLMClient.user(user),
        ]

        start = time.time()
        if schema is not None:
            raw = json_only_retry(self.llm, messages, max_tokens=max_tokens or 1024)
        else:
            raw = self.llm.chat(messages, temperature=temperature, max_tokens=max_tokens)
        latency_ms = int((time.time() - start) * 1000)

        success = True
        error: Optional[str] = None
        parsed: Optional[Dict[str, Any]] = None
        if schema is not None:
            try:
                parsed = json.loads(raw)
                required = set(schema.get("required", []))
                if not required.issubset(parsed.keys()):
                    raise ValueError("missing required keys")
            except Exception as exc:
                success = False
                error = f"json:{type(exc).__name__}"

        metric_labels = (spec.key, spec.version)
        llm_latency.labels(*metric_labels).observe(latency_ms)
        llm_calls.labels(spec.key, spec.version, str(success)).inc()

        self._persist_call(
            spec,
            prompt_hash,
            tpl_vars,
            raw,
            parsed,
            latency_ms,
            success,
            error,
            temperature,
            max_tokens,
        )

        return {
            "text": raw,
            "json": parsed,
            "cached": False,
            "success": success,
            "error": error,
            "prompt_key": spec.key,
            "prompt_version": spec.version,
            "prompt_hash": prompt_hash,
        }

    def _persist_call(
        self,
        spec,
        prompt_hash: str,
        tpl_vars: Dict[str, Any],
        output_text: str,
        output_json: Optional[Dict[str, Any]],
        latency_ms: int,
        success: bool,
        error: Optional[str],
        temperature: Optional[float],
        max_tokens: Optional[int],
    ) -> None:
        payload = _sanitize(tpl_vars)
        sanitized_output = _sanitize(output_json) if output_json is not None else None
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO llm_calls(
                        prompt_key,
                        prompt_version,
                        prompt_hash,
                        input_payload,
                        output_text,
                        output_json,
                        model,
                        temperature,
                        top_p,
                        max_output_tokens,
                        latency_ms,
                        tokens_prompt,
                        tokens_output,
                        success,
                        error
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        spec.key,
                        spec.version,
                        prompt_hash,
                        json.dumps(payload),
                        output_text,
                        json.dumps(sanitized_output) if sanitized_output is not None else None,
                        os.getenv("LLM_MODEL", "finance-Llama3-8B"),
                        float(temperature if temperature is not None else os.getenv("LLM_TEMPERATURE", "0.2")),
                        float(os.getenv("LLM_TOP_P", "0.9")),
                        int(max_tokens if max_tokens is not None else os.getenv("LLM_MAX_OUTPUT_TOKENS", "1024")),
                        latency_ms,
                        None,
                        None,
                        success,
                        error,
                    ),
                )
