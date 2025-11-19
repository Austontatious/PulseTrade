from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Dict, Iterable, List, Optional

import psycopg2
import psycopg2.extras


PG_DSN = os.getenv("PG_DSN", "postgresql://pulse:pulsepass@db:5432/pulse")


@dataclass
class LLMReviewRecord:
    scope: str
    symbol: str
    as_of: date
    prompt_key: str
    prompt_version: str
    prompt_hash: str
    output_json: Optional[Dict[str, Any]]
    output_text: Optional[str]
    input_payload: Optional[Dict[str, Any]]
    extra: Optional[Dict[str, Any]]
    cached: bool
    created_at: Optional[datetime]
    updated_at: Optional[datetime]


class LLMReviewStore:
    def __init__(self, dsn: Optional[str] = None) -> None:
        self._dsn = dsn or PG_DSN

    def _conn(self):
        return psycopg2.connect(self._dsn)

    def upsert_review(
        self,
        *,
        scope: str,
        symbol: str,
        as_of: date,
        prompt_key: str,
        prompt_version: str,
        prompt_hash: str,
        output_json: Optional[Dict[str, Any]],
        output_text: Optional[str],
        input_payload: Optional[Dict[str, Any]],
        extra: Optional[Dict[str, Any]] = None,
        cached: bool = False,
    ) -> None:
        with self._conn() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO llm_symbol_reviews(
                        scope,
                        symbol,
                        as_of,
                        prompt_key,
                        prompt_version,
                        prompt_hash,
                        output_json,
                        output_text,
                        input_payload,
                        extra,
                        cached
                    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (scope, symbol, as_of) DO UPDATE
                    SET prompt_version=EXCLUDED.prompt_version,
                        prompt_hash=EXCLUDED.prompt_hash,
                        output_json=EXCLUDED.output_json,
                        output_text=EXCLUDED.output_text,
                        input_payload=EXCLUDED.input_payload,
                        extra=EXCLUDED.extra,
                        cached=EXCLUDED.cached,
                        updated_at=NOW()
                    """,
                    (
                        scope,
                        symbol,
                        as_of,
                        prompt_key,
                        prompt_version,
                        prompt_hash,
                        json.dumps(output_json) if output_json is not None else None,
                        output_text,
                        json.dumps(input_payload) if input_payload is not None else None,
                        json.dumps(extra) if extra is not None else None,
                        cached,
                    ),
                )

    def fetch_scope_for_date(
        self,
        *,
        scope: str,
        as_of: date,
        symbols: Optional[Iterable[str]] = None,
    ) -> Dict[str, LLMReviewRecord]:
        params: List[Any] = [scope, as_of]
        where = "scope=%s AND as_of=%s"
        if symbols:
            params.append(list(symbols))
            where += " AND symbol = ANY(%s)"
        query = f"""
            SELECT scope, symbol, as_of, prompt_key, prompt_version, prompt_hash,
                   output_json, output_text, input_payload, extra, cached,
                   created_at, updated_at
            FROM llm_symbol_reviews
            WHERE {where}
        """
        results: Dict[str, LLMReviewRecord] = {}
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, params)
                for row in cur.fetchall():
                    results[row["symbol"]] = LLMReviewRecord(
                        scope=row["scope"],
                        symbol=row["symbol"],
                        as_of=row["as_of"],
                        prompt_key=row["prompt_key"],
                        prompt_version=row["prompt_version"],
                        prompt_hash=row["prompt_hash"],
                        output_json=row["output_json"],
                        output_text=row["output_text"],
                        input_payload=row["input_payload"],
                        extra=row["extra"],
                        cached=bool(row["cached"]),
                        created_at=row.get("created_at"),
                        updated_at=row.get("updated_at"),
                    )
        return results

    def fetch_latest(
        self,
        *,
        scope: str,
        symbols: Iterable[str],
    ) -> Dict[str, LLMReviewRecord]:
        symbol_list = [sym.upper() for sym in symbols]
        if not symbol_list:
            return {}
        query = """
            SELECT DISTINCT ON (symbol)
                   scope, symbol, as_of, prompt_key, prompt_version, prompt_hash,
                   output_json, output_text, input_payload, extra, cached,
                   created_at, updated_at
            FROM llm_symbol_reviews
            WHERE scope=%s AND symbol = ANY(%s)
            ORDER BY symbol, as_of DESC, updated_at DESC
        """
        results: Dict[str, LLMReviewRecord] = {}
        with self._conn() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(query, (scope, symbol_list))
                for row in cur.fetchall():
                    results[row["symbol"]] = LLMReviewRecord(
                        scope=row["scope"],
                        symbol=row["symbol"],
                        as_of=row["as_of"],
                        prompt_key=row["prompt_key"],
                        prompt_version=row["prompt_version"],
                        prompt_hash=row["prompt_hash"],
                        output_json=row["output_json"],
                        output_text=row["output_text"],
                        input_payload=row["input_payload"],
                        extra=row["extra"],
                        cached=bool(row["cached"]),
                        created_at=row.get("created_at"),
                        updated_at=row.get("updated_at"),
                    )
        return results
