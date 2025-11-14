import json
from typing import Any

from libs.llm.run_and_log import LLMRunner
from libs.llm.schemas import POLICY_SCHEMA


class FakeCursor:
    def __init__(self, store):
        self.store = store
        self._result = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def execute(self, query: str, params: tuple[Any, ...]):
        text = query.strip().lower()
        if text.startswith("select"):
            prompt_hash = params[0]
            self._result = None
            for row in reversed(self.store):
                if row["prompt_hash"] == prompt_hash and row["success"]:
                    self._result = {
                        "output_text": row["output_text"],
                        "output_json": row["output_json"],
                    }
                    break
        elif text.startswith("insert into llm_calls"):
            entry = {
                "prompt_key": params[0],
                "prompt_version": params[1],
                "prompt_hash": params[2],
                "input_payload": json.loads(params[3]),
                "output_text": params[4],
                "output_json": json.loads(params[5]) if params[5] else None,
                "success": params[13],
            }
            self.store.append(entry)
            self._result = None
        else:
            self._result = None

    def fetchone(self):
        return self._result


class FakeConnection:
    def __init__(self, store):
        self.store = store

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def cursor(self, cursor_factory=None):  # noqa: ARG002
        return FakeCursor(self.store)


class RunnerHarness(LLMRunner):
    def __init__(self, store):
        super().__init__()
        self._store = store

    def _conn(self):
        return FakeConnection(self._store)


def test_cached_call_hits_store(monkeypatch):
    store: list[dict[str, Any]] = []
    runner = RunnerHarness(store)

    call_counter = {"count": 0}

    class StubLLM:
        def chat(self, messages, **kwargs):  # noqa: ARG002
            call_counter["count"] += 1
            return "{\"allow\": true, \"reasons\": [], \"flags\": []}"

    monkeypatch.setattr(runner, "llm", StubLLM())

    payload = {
        "ticker": "TEST",
        "as_of": "2025-10-27",
        "inputs_json": json.dumps({
            "earnings_date": "2025-10-29",
            "liquidity_usd": 1_000_000,
            "borrow_ok": True,
            "factor_dispersion": 1.5,
            "coverage_pct": 0.9,
            "macro_regime": "neutral",
            "signal_label": "BUY",
            "signal_value": 1.2,
            "min_liquidity": 2_000_000,
        }),
    }

    first = runner.cached_call("policy", payload, schema=POLICY_SCHEMA)
    assert first["cached"] is False
    assert call_counter["count"] == 1

    second = runner.cached_call("policy", payload, schema=POLICY_SCHEMA)
    assert second["cached"] is True
    assert call_counter["count"] == 1

    third = runner.cached_call("policy", payload, schema=POLICY_SCHEMA, use_cache=False)
    assert third["cached"] is False
    assert call_counter["count"] == 2
