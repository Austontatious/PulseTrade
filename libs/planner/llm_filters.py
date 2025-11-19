from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict

from libs.llm.run_and_log import LLMRunner


@dataclass
class UniverseLLMDecision:
    risk_flag: str
    sentiment: float
    signal_penalty: float
    comment: str


@dataclass
class PlanTypeDecision:
    plan_type: str
    confidence: float
    comment: str


class PlanLLM:
    def __init__(self) -> None:
        self.runner = LLMRunner()

    def assess_universe(self, info: Dict[str, Any]) -> UniverseLLMDecision:
        tpl = {
            "symbol": info.get("symbol"),
            "sector": info.get("sector", "UNKNOWN"),
            "price": f"{info.get('price', 0.0):.2f}",
            "avg_dollar_vol_60d": f"{info.get('avg_dollar_vol_60d', 0.0):.0f}",
            "signal_strength": f"{info.get('signal_strength', 0.0):.4f}",
            "upcoming_events": info.get("upcoming_events", "none"),
            "headlines": info.get("headlines", "none"),
            "guidance": info.get("guidance", "none"),
        }
        resp = self.runner.cached_call("universe_assess", tpl, schema={"required": ["risk_flag", "sentiment", "signal_penalty", "comment"]})
        data = resp.get("json") or {}
        return UniverseLLMDecision(
            risk_flag=str(data.get("risk_flag", "normal")).lower(),
            sentiment=float(data.get("sentiment", 0.0)),
            signal_penalty=float(data.get("signal_penalty", 0.0)),
            comment=str(data.get("comment", ""))[:240],
        )

    def suggest_plan_type(self, info: Dict[str, Any]) -> PlanTypeDecision:
        tpl = {
            "symbol": info.get("symbol"),
            "side_signal": info.get("side_signal"),
            "signal_strength": f"{info.get('signal_strength', 0.0):.4f}",
            "delta_signal": f"{info.get('delta_signal', 0.0):.4f}",
            "risk_flag": info.get("risk_flag", "normal"),
            "sentiment": f"{info.get('sentiment', 0.0):.3f}",
            "earnings_soon": info.get("earnings_soon", False),
            "overnight": info.get("overnight", False),
            "factors": info.get("factors", "n/a"),
        }
        resp = self.runner.cached_call("plan_type", tpl, schema={"required": ["plan_type", "confidence", "comment"]}, use_cache=False)
        data = resp.get("json") or {}
        plan_type = str(data.get("plan_type", "LongIntraday"))
        return PlanTypeDecision(
            plan_type=plan_type,
            confidence=float(data.get("confidence", 0.5)),
            comment=str(data.get("comment", ""))[:240],
        )


_default_llm: PlanLLM | None = None


def get_plan_llm() -> PlanLLM:
    global _default_llm
    if _default_llm is None:
        _default_llm = PlanLLM()
    return _default_llm
