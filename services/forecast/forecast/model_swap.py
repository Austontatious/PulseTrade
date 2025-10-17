import math
import os
from typing import Any, Dict, Iterable

import pandas as pd

USE_TIMESFM = os.getenv("USE_TIMESFM", "0") == "1"
USE_CHRONOS = os.getenv("USE_CHRONOS", "0") == "1"


def _sentiment_adjustment(covariates: Dict[str, Any] | None) -> float:
    """Blend short/medium/long window sentiment into a small price tilt."""
    if not covariates:
        return 0.0
    social: Iterable[Dict[str, Any]] = covariates.get("social") or []
    weight_map = {5: 0.6, 15: 0.3, 60: 0.1}
    numer = 0.0
    denom = 0.0
    for item in social:
        window = int(item.get("window") or item.get("window_minutes") or 0)
        mean = item.get("senti_mean")
        if mean is None or not isinstance(mean, (int, float)) or not math.isfinite(mean):
            continue
        weight = weight_map.get(window, 0.0)
        if weight <= 0.0:
            continue
        numer += weight * float(mean)
        denom += weight
    if denom == 0.0:
        return 0.0
    blended = numer / denom
    return max(min(blended, 1.0), -1.0) * 0.002


def _apply_adjustment(forecast: float, covariates: Dict[str, Any] | None) -> tuple[float, float, float]:
    adj = _sentiment_adjustment(covariates)
    mean = forecast * (1.0 + adj)
    band = abs(forecast) * 0.003 or 0.01
    return mean, mean - band, mean + band


class Baseline:
    def predict(self, series: pd.Series, *, covariates: dict | None = None) -> tuple[float, float, float]:
        window = series.tail(60)
        base = float(window.mean()) if not window.empty else float(series.iloc[-1])
        return _apply_adjustment(base, covariates)


try:  # pragma: no cover - optional deps
    if USE_TIMESFM:
        from timesfm import TimesFM  # type: ignore

        class TimesFMWrapper:
            def __init__(self) -> None:
                self.model = TimesFM()

            def predict(self, series: pd.Series, *, covariates: dict | None = None) -> tuple[float, float, float]:
                forecast = float(series.iloc[-1])
                if hasattr(self.model, "forecast"):
                    try:
                        result = self.model.forecast(series.values.astype("float32"), prediction_length=1)
                        if isinstance(result, (list, tuple)) and result:
                            forecast = float(result[0])
                    except Exception:  # pragma: no cover - defensive fallback
                        pass
                return _apply_adjustment(forecast, covariates)

        Model = TimesFMWrapper
    elif USE_CHRONOS:
        from chronos import ChronosPipeline  # type: ignore

        class ChronosWrapper:
            def __init__(self) -> None:
                self.pipeline = ChronosPipeline.from_pretrained("amazon/chronos-bolt-base")

            def predict(self, series: pd.Series, *, covariates: dict | None = None) -> tuple[float, float, float]:
                forecast = float(series.iloc[-1])
                try:
                    output = self.pipeline.predict(context=series.values.tolist(), prediction_length=1)
                    if isinstance(output, (list, tuple)) and output:
                        forecast = float(output[0])
                except Exception:  # pragma: no cover - defensive fallback
                    pass
                return _apply_adjustment(forecast, covariates)

        Model = ChronosWrapper
    else:
        Model = Baseline
except Exception:  # pragma: no cover - optional import failure
    Model = Baseline
