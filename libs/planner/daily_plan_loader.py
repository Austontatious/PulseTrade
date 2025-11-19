from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import date, time
from pathlib import Path
from typing import Dict, List


@dataclass
class LoadedPlanAction:
    name: str
    kind: str
    direction: str
    sizing: str
    window_label: str
    start: time
    end: time


@dataclass
class LoadedDailyPlan:
    symbol: str
    side: str
    reason: str
    actions: List[LoadedPlanAction]


def _parse_window_label(label: str) -> tuple[time, time]:
    try:
        start_str, end_str = label.split("-")
        start_parts = [int(part) for part in start_str.split(":", 1)]
        end_parts = [int(part) for part in end_str.split(":", 1)]
        start_time = time(hour=start_parts[0], minute=start_parts[1] if len(start_parts) > 1 else 0)
        end_time = time(hour=end_parts[0], minute=end_parts[1] if len(end_parts) > 1 else 0)
    except Exception:
        return time(0, 0), time(23, 59)
    return start_time, end_time


def load_daily_plans(plan_dir: Path, trading_date: date) -> Dict[str, LoadedDailyPlan]:
    path = plan_dir / f"daily_plan_selection_{trading_date.isoformat()}.json"
    if not path.exists():
        return {}
    data = json.loads(path.read_text())
    plans: Dict[str, LoadedDailyPlan] = {}
    for entry in data.get("active", []):
        symbol = str(entry.get("symbol" or entry.get("plan", {}).get("symbol", ""))).upper()
        if not symbol:
            continue
        reason = entry.get("reason") or entry.get("plan", {}).get("reason", "")
        side = entry.get("side") or entry.get("plan", {}).get("side", "long")
        actions_data = entry.get("actions") or entry.get("plan", {}).get("actions", [])
        actions: List[LoadedPlanAction] = []
        for action in actions_data:
            label = action.get("window") or "00:00-23:59"
            start, end = _parse_window_label(label)
            actions.append(
                LoadedPlanAction(
                    name=action.get("name", "action"),
                    kind=action.get("kind", "entry"),
                    direction=action.get("direction", "buy").lower(),
                    sizing=str(action.get("sizing", "notional")),
                    window_label=label,
                    start=start,
                    end=end,
                )
            )
        plans[symbol] = LoadedDailyPlan(symbol=symbol, side=side, reason=reason or "", actions=actions)
    return plans
