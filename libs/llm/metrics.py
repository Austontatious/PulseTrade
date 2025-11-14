try:
    from prometheus_client import Counter, Histogram  # type: ignore
except Exception:  # pragma: no cover - optional dependency fallback
    class _NoOpMetric:  # noqa: D401 - minimal stub
        def labels(self, *args, **kwargs):
            return self

        def inc(self, *args, **kwargs):
            return None

        def observe(self, *args, **kwargs):
            return None

    def Counter(*args, **kwargs):  # type: ignore
        return _NoOpMetric()

    def Histogram(*args, **kwargs):  # type: ignore
        return _NoOpMetric()


llm_calls = Counter(
    "llm_calls_total",
    "Total LLM chat completion calls",
    labelnames=("prompt_key", "version", "success"),
)

llm_latency = Histogram(
    "llm_latency_ms",
    "LLM call latency in milliseconds",
    labelnames=("prompt_key", "version"),
    buckets=(50, 100, 250, 500, 1000, 2000, 5000),
)
