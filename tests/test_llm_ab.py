import importlib
import os


def test_ab_bucket_stable_selection(monkeypatch):
    monkeypatch.setenv("LLM_AB_BUCKETS", "rationale:1.0.0,1.1.0")
    module = importlib.import_module("services.forecast.forecast.llm_hooks")
    importlib.reload(module)
    version_one = module._select_version("rationale", "AAPL", "2025-01-01")  # noqa: SLF001
    version_two = module._select_version("rationale", "AAPL", "2025-01-01")  # noqa: SLF001
    assert version_one == version_two


def test_ab_bucket_none_when_unset(monkeypatch):
    monkeypatch.delenv("LLM_AB_BUCKETS", raising=False)
    module = importlib.import_module("services.forecast.forecast.llm_hooks")
    importlib.reload(module)
    assert module._select_version("rationale", "AAPL", "2025-01-01") is None  # noqa: SLF001
